import { isLiveStore, type LiveStore } from "../memory.js";
import { filtersOf, matchesFilter } from "../query.js";
import { emptySchema } from "../registry.js";
import type { Cursor, EventStore, Query, RecordedEvent, StoreSchema } from "../types.js";
import { memoryCursors, type CursorStore } from "./cursors.js";

export { fileCursors, memoryCursors } from "./cursors.js";
export type { CursorStore } from "./cursors.js";

/** Receives one batch of settled events; the cursor advances only after it resolves. */
export type SubscriptionHandler = (events: readonly RecordedEvent[]) => Promise<void> | void;

/** What to do after a failed batch: retry with back-off, skip it, or stop the subscription. */
export type ErrorDecision = "retry" | "skip" | "stop";

/** Options of a durable subscription: store, cursor persistence, start position, polling, error policy. */
export interface SubscribeOptions {
  readonly store: EventStore;
  /** Where the cursor is remembered. Default: in memory (replay after restart). */
  readonly cursors?: CursorStore;
  /** Start position when the cursor store has nothing for this name. Default `"beginning"`. */
  readonly from?: Cursor | null | "beginning" | "now";
  /** Polling interval. A `LiveStore` push wakes the poller immediately. Default 500 ms. */
  readonly pollIntervalMs?: number;
  /** Max events per handler call. Default 500. */
  readonly batchSize?: number;
  /**
   * What to do when the handler throws, or when the store/cursor store fails (then `batch` is
   * empty and `"skip"` means `"retry"`). Default: `"retry"` with back-off (100 ms → 5 s).
   */
  readonly onError?: (error: unknown, batch: readonly RecordedEvent[]) => ErrorDecision;
}

/** A running subscription: wait for catch-up, stop, inspect the cursor. */
export interface Subscription {
  readonly name: string;
  /**
   * Resolves the next time a poll returns an empty page — i.e. every settled event matching the
   * query up to that moment has been handled. Resolves immediately once the subscription is stopped.
   */
  whenCaughtUp(): Promise<void>;
  /** Stops polling; awaits an in-flight batch. */
  stop(): Promise<void>;
  /** The last persisted cursor. */
  readonly cursor: Cursor | null;
  /** `true` after `stop()` or after the handler asked to stop. */
  readonly stopped: boolean;
}

const MIN_BACKOFF_MS = 100;
const MAX_BACKOFF_MS = 5_000;

/**
 * A durable, gap-free, query-filtered subscription with a named cursor.
 *
 * Replays settled events beyond the cursor, then keeps polling (and wakes up on in-process
 * appends when the store is a `LiveStore`). The cursor advances to the batch's settled cursor
 * only after the handler resolved, so a crash re-delivers the batch (at-least-once). Pushed
 * events never bypass the query: the cursor is the only source of truth.
 */
export function subscribe(name: string, query: Query, handler: SubscriptionHandler, options: SubscribeOptions): Subscription {
  const store = options.store;
  const cursors = options.cursors ?? memoryCursors();
  const pollIntervalMs = options.pollIntervalMs ?? 500;
  const batchSize = options.batchSize ?? 500;
  const onError = options.onError ?? (() => "retry" as const);

  let cursor: Cursor | null = null;
  let stopped = false;
  let backoffMs = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let inFlight: Promise<void> | undefined;
  let wakeRequested = false;
  let caughtUpWaiters: Array<() => void> = [];
  let unsubscribeLive: (() => void) | undefined;

  const resolveCaughtUp = (): void => {
    const waiters = caughtUpWaiters;
    caughtUpWaiters = [];
    for (const resolve of waiters) resolve();
  };

  const initialCursor = async (): Promise<Cursor | null> => {
    const stored = await cursors.load(name);
    if (stored) return stored;
    const from = options.from ?? "beginning";
    if (from === "beginning" || from === null) return null;
    if (from === "now") {
      const head = await store.query(query, { settledOnly: true, order: "desc", limit: 1 });
      return head.settledCursor;
    }
    return from;
  };

  const schedule = (delayMs: number): void => {
    if (stopped) return;
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      timer = undefined;
      void run();
    }, delayMs);
  };

  /** One poll. Serialized: a second call while one is in flight only requests another poll afterwards. */
  const run = (): Promise<void> => {
    if (stopped) return Promise.resolve();
    if (inFlight) {
      wakeRequested = true;
      return inFlight;
    }
    inFlight = poll().finally(() => {
      inFlight = undefined;
    });
    return inFlight;
  };

  const poll = async (): Promise<void> => {
    wakeRequested = false;
    // only waiters registered BEFORE this read may be told "caught up" by its empty page;
    // a waiter that arrives while the read is in flight waits for the next poll
    const waitersBeforeRead = caughtUpWaiters;
    caughtUpWaiters = [];
    try {
      const result = await store.query(query, { settledOnly: true, cursor, limit: batchSize });
      if (stopped) {
        for (const w of waitersBeforeRead) w(); // stopped = caught up by contract; never leave a waiter hanging
        return;
      }
      if (result.events.length === 0) {
        backoffMs = 0;
        for (const w of waitersBeforeRead) w();
        if (caughtUpWaiters.length > 0) wakeRequested = true;
        // a push that arrived while this poll was in flight must not wait a full interval
        schedule(wakeRequested ? 0 : pollIntervalMs);
        return;
      }
      caughtUpWaiters = [...waitersBeforeRead, ...caughtUpWaiters];
      const decision = await deliver(result.events);
      if (decision === "stop") {
        shutdown(); // not `stop()`: that would await this very poll
        return;
      }
      if (decision === "retry") {
        backoffMs = backoffMs === 0 ? MIN_BACKOFF_MS : Math.min(backoffMs * 2, MAX_BACKOFF_MS);
        schedule(backoffMs);
        return;
      }
      // "ok" or "skip": advance past this batch
      const next = result.settledCursor;
      if (next) {
        await cursors.save(name, next);
        cursor = next;
      }
      backoffMs = 0;
      if (stopped) {
        for (const w of waitersBeforeRead) w();
        return;
      }
      // Poll again right away: more may be waiting (full page) or a push arrived meanwhile.
      schedule(0);
    } catch (error) {
      caughtUpWaiters = [...waitersBeforeRead, ...caughtUpWaiters]; // nobody was told "caught up"
      // store/cursor errors: ask onError (with an empty batch), default to back-off and retry
      let decision: ErrorDecision = "retry";
      try {
        decision = onError(error, []);
      } catch {
        decision = "retry";
      }
      if (decision === "stop") {
        shutdown();
        return;
      }
      backoffMs = backoffMs === 0 ? MIN_BACKOFF_MS : Math.min(backoffMs * 2, MAX_BACKOFF_MS);
      schedule(backoffMs);
    }
  };

  const deliver = async (batch: readonly RecordedEvent[]): Promise<"ok" | ErrorDecision> => {
    try {
      await handler(batch);
      return "ok";
    } catch (error) {
      try {
        return onError(error, batch);
      } catch {
        return "retry";
      }
    }
  };

  const shutdown = (): void => {
    stopped = true;
    if (timer) {
      clearTimeout(timer);
      timer = undefined;
    }
    unsubscribeLive?.();
    unsubscribeLive = undefined;
    resolveCaughtUp();
  };

  const stop = async (): Promise<void> => {
    shutdown();
    if (inFlight) {
      try {
        await inFlight;
      } catch {
        // already handled inside poll
      }
    }
  };

  // start
  inFlight = (async () => {
    try {
      cursor = await initialCursor();
    } catch {
      cursor = null;
    }
    if (isLiveStore(store)) {
      unsubscribeLive = store.onAppended(() => {
        if (stopped) return;
        wakeRequested = true;
        schedule(0);
      });
    }
  })().finally(() => {
    inFlight = undefined;
    schedule(0);
  });

  return {
    name,
    whenCaughtUp() {
      if (stopped) return Promise.resolve();
      return new Promise<void>((resolve) => {
        caughtUpWaiters.push(resolve);
        schedule(0);
      });
    },
    stop,
    get cursor() {
      return cursor;
    },
    get stopped() {
      return stopped;
    },
  };
}

export interface OnOptions {
  /** A store that pushes appended events in-process (`MemoryStore`). The Postgres store polls; use `subscribe` there. */
  readonly store: EventStore & LiveStore;
}

/**
 * In-process, fire-and-forget reaction to appended events matching `query`. Only for stores
 * that push (`LiveStore`, e.g. `MemoryStore`); no cursor, no replay, at-most-once. Use it for
 * SSE fan-out and similar conveniences — use `subscribe` for anything that must not be lost.
 */
export function on(query: Query, handler: (events: readonly RecordedEvent[]) => void, options: OnOptions): () => void {
  const store = options.store;
  if (!isLiveStore(store)) {
    throw new Error("eventstore: on() needs a LiveStore (one that pushes appended events); use subscribe() for polling stores");
  }
  const filters = filtersOf(query);
  const schema = schemaOf(store);
  return store.onAppended((events) => {
    const matching = events.filter((event) => filters.some((filter) => matchesFilter(event, filter, schema)));
    if (matching.length === 0) return;
    try {
      handler(matching);
    } catch {
      // a listener must never fail an append
    }
  });
}

/** Forget a subscription's cursor so its next start replays from `from`. */
export async function resetCursor(name: string, cursors: CursorStore): Promise<void> {
  if (cursors.delete) {
    await cursors.delete(name);
    return;
  }
  throw new Error("eventstore: this CursorStore cannot delete cursors; implement delete(name)");
}

function schemaOf(store: EventStore): StoreSchema {
  const candidate = (store as Partial<{ schema: StoreSchema }>).schema;
  if (candidate && typeof candidate.idKeyOf === "function") return candidate;
  return emptySchema();
}
