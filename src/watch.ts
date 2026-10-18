import { EventStoreError, TransientError, UsageError, WatchOverflowError } from "./errors.js";
import { memoryCursors, subscribe, type Subscription } from "./subscribe/index.js";
import type { EventStore, Query, RecordedEvent } from "./types.js";

/** Limits of `es.watch`, per process (per root api). Read model watchers count toward them too. */
export interface WatchLimits {
  /** Watchers alive at once across every view, read model watchers included. Default 10 000; one more is a `UsageError`. */
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

/**
 * One watcher (internal): its own bounded queue and chain, ended once. `es.watch` and read model
 * watchers both use it, so they keep the same guarantees: a slow watcher is dropped with
 * `WatchOverflowError`, a throwing one ends alone, and `onError` is called at most once.
 */
export class WatchMember<T> {
  private pending = 0;
  private chain: Promise<void> = Promise.resolve();
  stopped = false;
  /** `watch()` has handed out the Watch; before that, an ending is reported by rejecting it. */
  resolved = false;
  failure?: unknown;

  constructor(
    private readonly handler: (item: T) => void | Promise<void>,
    private readonly onError: ((error: unknown) => void) | undefined,
    private readonly maxPending: number,
    /** Called once when the watcher leaves, for whatever reason: it gives its place back. */
    private readonly onLeave: () => void,
  ) {}

  /** Queue one delivery. Never awaited: one slow watcher must not hold the reader (or the others) back. */
  deliver(item: T): void {
    if (this.stopped) return;
    if (this.pending >= this.maxPending) {
      this.end(new WatchOverflowError(`eventstore: a watcher fell ${this.maxPending} batches behind and was dropped`));
      return;
    }
    this.pending++;
    this.chain = this.chain
      .then(async () => {
        if (!this.stopped) await this.handler(item);
      })
      .catch((error: unknown) => this.end(error))
      .finally(() => {
        this.pending--;
      });
  }

  /** Ends the watcher on its own (error), telling it once — through `onError`, or by rejecting `watch()` if it has not resolved yet. */
  end(error: unknown): void {
    if (this.stopped) return;
    this.failure = error;
    this.leave();
    if (!this.resolved) return;
    try {
      this.onError?.(error);
    } catch {
      // an error callback must never break the reader
    }
  }

  /** Ends the watcher without `onError` (stopped, aborted, closed). */
  leave(): void {
    if (this.stopped) return;
    this.stopped = true;
    this.onLeave();
  }
}

/**
 * Hand out a watcher's `Watch` once `ready` resolved (internal). The signal ends it, also while it
 * waits; `onReady` runs synchronously right after the wait, so whatever it queues comes first.
 */
export async function handOut<T>(member: WatchMember<T>, ready: Promise<void>, signal: AbortSignal | undefined, onReady?: () => void): Promise<Watch> {
  const onAbort = () => member.leave();
  signal?.addEventListener("abort", onAbort, { once: true });
  try {
    await (signal ? Promise.race([ready, aborted(signal)]) : ready);
  } catch (error) {
    signal?.removeEventListener("abort", onAbort);
    member.leave();
    throw error;
  }
  if (!member.stopped) onReady?.();
  if (member.stopped) {
    // ended before the caller had a handle (its handler threw, it overflowed): say so here, once
    signal?.removeEventListener("abort", onAbort);
    throw member.failure ?? new UsageError("eventstore: the watch ended before it started");
  }
  member.resolved = true;
  return {
    stop: () => {
      signal?.removeEventListener("abort", onAbort);
      member.leave();
    },
    get stopped() {
      return member.stopped;
    },
  };
}

/** What a read model's watchers take from the root api's hub: one place each, and the queue limit. */
export interface WatchPlaces {
  admit(): () => void;
  readonly maxPendingBatches: number;
}

const places = new WeakMap<WatchHub, WatchPlaces>();

/** Internal (not exported from the package): read model watchers count toward the hub's `watch.maxWatchers`. */
export function watchPlaces(hub: WatchHub): WatchPlaces {
  return places.get(hub)!;
}

interface Group {
  readonly key: string;
  readonly sub: Subscription;
  readonly members: Set<WatchMember<readonly RecordedEvent[]>>;
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
    places.set(this, { admit: () => this.admit(), maxPendingBatches: this.maxPending });
  }

  /** Watchers alive right now (tests, metrics), read model watchers included. */
  get size(): number {
    return this.total;
  }

  /** Readers alive right now: one per distinct (view, query). */
  get readers(): number {
    return this.groups.size;
  }

  /** Take one place of `watch.maxWatchers`; the returned function gives it back, once. Throws `UsageError` at the limit or once closed. */
  private admit(): () => void {
    if (this.total >= this.maxWatchers) {
      throw new UsageError(`eventstore: at most ${this.maxWatchers} watchers per process (watch.maxWatchers)`);
    }
    if (this.closed) throw new UsageError("eventstore: the store is closed");
    this.total++;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.total--;
    };
  }

  async watch(key: string, store: EventStore, query: Query, handler: (events: readonly RecordedEvent[]) => void | Promise<void>, options: WatchOptions = {}): Promise<Watch> {
    const release = this.admit();
    const signal = options.signal;
    if (signal?.aborted) {
      release();
      throw signal.reason;
    }
    const group = this.groups.get(key) ?? this.open(key, store, query, options);
    const member: WatchMember<readonly RecordedEvent[]> = new WatchMember(handler, options.onError, this.maxPending, () => {
      group.members.delete(member);
      release();
      if (group.members.size === 0) {
        if (this.groups.get(group.key) === group) this.groups.delete(group.key);
        void group.sub.stop();
      }
    });
    group.members.add(member);
    return handOut(member, group.ready, signal);
  }

  private open(key: string, store: EventStore, query: Query, options: WatchOptions): Group {
    let failure: unknown;
    const members = new Set<WatchMember<readonly RecordedEvent[]>>();
    const sub = subscribe(
      `watch:${key}`,
      query,
      (events) => {
        // never awaited: one slow watcher must not hold the reader (or the others) back
        for (const member of [...members]) member.deliver(events);
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
          for (const member of [...members]) member.end(error);
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

  /** Stops every reader and ends every watcher (without `onError`: the owner closed the store). */
  async close(): Promise<void> {
    this.closed = true;
    const groups = [...this.groups.values()];
    this.groups.clear();
    for (const group of groups) {
      for (const member of [...group.members]) {
        member.failure = new UsageError("eventstore: the store was closed");
        member.leave();
      }
      await group.sub.stop();
    }
  }
}

/**
 * An error no retry will fix: a refused read (`UsageError`: rls without a tenant, a past view),
 * or any other store error that is not transient (permissions, a failed install). A plain
 * `Error` (a dropped connection, say) is retried.
 */
export function isFatal(error: unknown): boolean {
  return error instanceof UsageError || (error instanceof EventStoreError && !(error instanceof TransientError));
}

function aborted(signal: AbortSignal): Promise<never> {
  return new Promise((_, reject) => signal.addEventListener("abort", () => reject(signal.reason), { once: true }));
}
