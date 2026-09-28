import { EventStoreError, TransientError, UsageError, WatchOverflowError } from "./errors.js";
import { memoryCursors, subscribe, type Subscription } from "./subscribe/index.js";
import type { EventStore, Query, RecordedEvent } from "./types.js";

/** Limits of `es.watch`, per process (per root api). */
export interface WatchLimits {
  /** Watchers alive at once across every view. Default 10 000; one more is a `UsageError`. */
  readonly maxWatchers?: number;
  /** Batches one watcher may have queued before it is dropped with `WatchOverflowError`. Default 64. */
  readonly maxPendingBatches?: number;
  /** Safety-net polling of a watch group (doorbells wake it at once). Default: `subscribe`'s. */
  readonly pollIntervalMs?: number;
}

/** Options of one watch. */
export interface WatchOptions {
  /**
   * Called once when the watch ends on its own: its handler threw, it fell behind
   * (`WatchOverflowError`), or the store refused the read (`UsageError`). Not called by `stop()`.
   */
  readonly onError?: (error: unknown) => void;
  /**
   * Ends the watch when aborted — also while `watch()` still waits for the store (the database
   * down at boot, say): it then rejects with the signal's reason. Pass the request's signal so a
   * closed SSE request always gives its place back.
   */
  readonly signal?: AbortSignal;
  /** `false`: records without data — ids and scopes only; a large payload never leaves the database. */
  readonly data?: false;
  /** `false`: records without reading the payload at all — type, sequence, time; nothing is unpacked. */
  readonly payload?: false;
}

/** A running watch: ephemeral, from about "now", no cursor that survives the process. */
export interface Watch {
  stop(): void;
  readonly stopped: boolean;
}

interface Member {
  readonly handler: (events: readonly RecordedEvent[]) => void | Promise<void>;
  readonly onError: ((error: unknown) => void) | undefined;
  pending: number;
  chain: Promise<void>;
  stopped: boolean;
  /** `watch()` has handed out the Watch; before that, an ending is reported by rejecting it. */
  resolved: boolean;
  failure?: unknown;
}

interface Group {
  readonly key: string;
  readonly sub: Subscription;
  readonly members: Set<Member>;
  readonly ready: Promise<void>;
}

/**
 * One reader per (view, query): every watcher of the same tenant and query in this process
 * shares one cursor read per doorbell, however many SSE clients hang on it. Each watcher gets
 * its own bounded queue, so a slow one is dropped instead of holding the others back or
 * filling memory. The read goes through the watcher's own view — tenant narrowing and
 * row-level security apply as to any read.
 */
export class WatchHub {
  private readonly groups = new Map<string, Group>();
  private total = 0;
  private closed = false;
  private readonly maxWatchers: number;
  private readonly maxPending: number;

  constructor(private readonly limits: WatchLimits = {}) {
    this.maxWatchers = limits.maxWatchers ?? 10_000;
    this.maxPending = limits.maxPendingBatches ?? 64;
  }

  /** Watchers alive right now (tests, metrics). */
  get size(): number {
    return this.total;
  }

  /** Readers alive right now: one per distinct (view, query). */
  get readers(): number {
    return this.groups.size;
  }

  async watch(key: string, store: EventStore, query: Query, handler: Member["handler"], options: WatchOptions = {}): Promise<Watch> {
    if (this.total >= this.maxWatchers) {
      throw new UsageError(`eventstore: at most ${this.maxWatchers} watchers per process (watch.maxWatchers)`);
    }
    if (this.closed) throw new UsageError("eventstore: the store is closed");
    const signal = options.signal;
    if (signal?.aborted) throw signal.reason;
    const group = this.groups.get(key) ?? this.open(key, store, query, options);
    const member: Member = { handler, onError: options.onError, pending: 0, chain: Promise.resolve(), stopped: false, resolved: false };
    group.members.add(member);
    this.total++;
    const onAbort = () => this.leave(group, member);
    signal?.addEventListener("abort", onAbort, { once: true });
    try {
      await (signal ? Promise.race([group.ready, aborted(signal)]) : group.ready);
    } catch (error) {
      signal?.removeEventListener("abort", onAbort);
      this.leave(group, member);
      throw error;
    }
    if (member.stopped) {
      // ended before the caller had a handle (its handler threw, it overflowed): say so here, once
      signal?.removeEventListener("abort", onAbort);
      throw member.failure ?? new UsageError("eventstore: the watch ended before it started");
    }
    member.resolved = true;
    return {
      stop: () => {
        signal?.removeEventListener("abort", onAbort);
        this.leave(group, member);
      },
      get stopped() {
        return member.stopped;
      },
    };
  }

  private open(key: string, store: EventStore, query: Query, options: WatchOptions): Group {
    let failure: unknown;
    const members = new Set<Member>();
    const sub = subscribe(
      `watch:${key}`,
      query,
      (events) => {
        // never awaited: one slow watcher must not hold the reader (or the others) back
        for (const member of [...members]) this.deliver(group, member, events);
      },
      {
        store,
        from: "now",
        cursors: memoryCursors(),
        ...(options.data === false ? { data: false as const } : {}),
        ...(options.payload === false ? { payload: false as const } : {}),
        ...(this.limits.pollIntervalMs !== undefined ? { pollIntervalMs: this.limits.pollIntervalMs } : {}),
        onError: (error) => {
          if (!isFatal(error)) return "retry"; // transient: the next doorbell or poll tries again
          failure = error;
          if (this.groups.get(key) === group) this.groups.delete(key);
          for (const member of [...members]) this.end(group, member, error);
          return "stop";
        },
      },
    );
    const group: Group = {
      key,
      sub,
      members,
      ready: sub.whenCaughtUp().then(() => {
        if (failure !== undefined) throw failure;
      }),
    };
    this.groups.set(key, group);
    return group;
  }

  private deliver(group: Group, member: Member, events: readonly RecordedEvent[]): void {
    if (member.stopped) return;
    if (member.pending >= this.maxPending) {
      this.end(group, member, new WatchOverflowError(`eventstore: a watcher fell ${this.maxPending} batches behind and was dropped`));
      return;
    }
    member.pending++;
    member.chain = member.chain
      .then(async () => {
        if (!member.stopped) await member.handler(events);
      })
      .catch((error: unknown) => this.end(group, member, error))
      .finally(() => {
        member.pending--;
      });
  }

  /** Stops every reader and ends every watcher (without `onError`: the owner closed the store). */
  async close(): Promise<void> {
    this.closed = true;
    const groups = [...this.groups.values()];
    this.groups.clear();
    for (const group of groups) {
      for (const member of [...group.members]) {
        member.failure = new UsageError("eventstore: the store was closed");
        this.leave(group, member);
      }
      await group.sub.stop();
    }
  }

  /** Ends a watcher on its own (error), telling it once — through `onError`, or by rejecting `watch()` if it has not resolved yet. */
  private end(group: Group, member: Member, error: unknown): void {
    if (member.stopped) return;
    member.failure = error;
    this.leave(group, member);
    if (!member.resolved) return;
    try {
      member.onError?.(error);
    } catch {
      // an error callback must never break the reader
    }
  }

  private leave(group: Group, member: Member): void {
    if (member.stopped) return;
    member.stopped = true;
    group.members.delete(member);
    this.total--;
    if (group.members.size === 0) {
      if (this.groups.get(group.key) === group) this.groups.delete(group.key);
      void group.sub.stop();
    }
  }
}

/**
 * An error no retry will fix: a refused read (`UsageError`: rls without a tenant, a past view),
 * or any other store error that is not transient (permissions, a failed install). A plain
 * `Error` (a dropped connection, say) is retried.
 */
function isFatal(error: unknown): boolean {
  return error instanceof UsageError || (error instanceof EventStoreError && !(error instanceof TransientError));
}

function aborted(signal: AbortSignal): Promise<never> {
  return new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), { once: true }));
}
