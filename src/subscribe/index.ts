import { cursorOf, isLiveStore, isWakeStore, type LiveStore } from "../memory.js";
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
  /**
   * Polling interval. A `LiveStore` push or a `WakeStore` doorbell wakes the poller at once, so
   * polling is only the safety net: default 5 000 ms with a doorbell, 500 ms without.
   */
  readonly pollIntervalMs?: number;
  /** Doorbells within this window share one read (default 25 ms). */
  readonly wakeCoalesceMs?: number;
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
/** Re-read delay while visible events wait for older transactions to end (nothing rings when they do). */
const MIN_UNSETTLED_MS = 25;
const MAX_UNSETTLED_MS = 500;

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
  const pollIntervalMs = options.pollIntervalMs ?? (isWakeStore(store) ? 5_000 : 500);
  const wakeCoalesceMs = options.wakeCoalesceMs ?? 25;
  const batchSize = options.batchSize ?? 500;
  const onError = options.onError ?? (() => "retry" as const);

  let cursor: Cursor | null = null;
  /** The start position is resolved inside the first successful poll: a failure there retries, it never falls back to replaying everything. */
  let cursorResolved = false;
  let stopped = false;
  let backoffMs = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let inFlight: Promise<void> | undefined;
  let wakeRequested = false;
  let caughtUpWaiters: Array<() => void> = [];
  let unsubscribeLive: (() => void) | undefined;
  let unsubscribeWake: (() => void) | undefined;
  let wakeTimer: ReturnType<typeof setTimeout> | undefined;
  let unsettledMs = 0;

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
      if (!cursorResolved) {
        cursor = await initialCursor();
        cursorResolved = true;
      }
      // Not `settledOnly`: the cursor read also returns the unsettled tail, and that tail says
      // "look again soon". Events become settled when an OLDER transaction ends — possibly one on
      // another table that rings no doorbell — so without this a woken reader would wait a full poll.
      const result = await store.query(query, { cursor, limit: batchSize });
      if (stopped) {
        for (const w of waitersBeforeRead) w(); // stopped = caught up by contract; never leave a waiter hanging
        return;
      }
      // in (transactionId, sequence) order every settled record precedes every unsettled one
      const firstUnsettled = result.events.findIndex((e) => !e.settled);
      const settled = firstUnsettled === -1 ? result.events : result.events.slice(0, firstUnsettled);
      if (settled.length === 0) {
        backoffMs = 0;
        for (const w of waitersBeforeRead) w();
        if (caughtUpWaiters.length > 0) wakeRequested = true;
        if (firstUnsettled !== -1) {
          unsettledMs = unsettledMs === 0 ? MIN_UNSETTLED_MS : Math.min(unsettledMs * 2, MAX_UNSETTLED_MS, pollIntervalMs);
          schedule(wakeRequested ? 0 : unsettledMs);
          return;
        }
        unsettledMs = 0;
        // a push that arrived while this poll was in flight must not wait a full interval
        schedule(wakeRequested ? 0 : pollIntervalMs);
        return;
      }
      unsettledMs = 0;
      caughtUpWaiters = [...waitersBeforeRead, ...caughtUpWaiters];
      const decision = await deliver(settled);
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
      const next = cursorOf(settled);
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
    unsubscribeWake?.();
    unsubscribeWake = undefined;
    if (wakeTimer) {
      clearTimeout(wakeTimer);
      wakeTimer = undefined;
    }
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
  /** A wake: read now — unless a back-off is running, which a commit elsewhere must not cut short. */
  const wake = (): void => {
    if (stopped) return;
    wakeRequested = true;
    if (backoffMs > 0 || inFlight) return; // the back-off timer, or the running poll, picks it up
    schedule(0);
  };

  inFlight = (async () => {
    try {
      if (isLiveStore(store)) {
        unsubscribeLive = store.onAppended(wake);
      } else if (isWakeStore(store)) {
        // a doorbell carries nothing: it only says "read now"; a burst of them shares one read
        unsubscribeWake = store.onCommitted(() => {
          if (stopped || wakeTimer) return;
          wakeTimer = setTimeout(() => {
            wakeTimer = undefined;
            wake();
          }, wakeCoalesceMs);
        });
      }
    } catch {
      // no doorbell (e.g. the store is closing): polling alone still delivers everything
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
