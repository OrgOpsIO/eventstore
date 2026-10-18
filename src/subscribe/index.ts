import { UsageError } from "../errors.js";
import { cursorOf, isLiveStore, isWakeStore, type LiveStore } from "../memory.js";
import { filtersOf, matchesFilter, passedBy } from "../query.js";
import { emptySchema } from "../registry.js";
import type { Cursor, EventStore, Query, RecordedEvent, StoreSchema } from "../types.js";
import { memoryCursors, type CursorStore } from "./cursors.js";

export { fileCursors, memoryCursors } from "./cursors.js";
export type { CursorStore } from "./cursors.js";

/**
 * Receives one batch of settled events; the cursor advances only after it resolves. In processor
 * mode (`target`) the handler commits the cursor itself: in one transaction it locks and reads the
 * target's checkpoint, applies only the events beyond it, and writes `cursorOf(events)` as the new
 * checkpoint. A redelivered batch can start below the checkpoint and end beyond it.
 */
export type SubscriptionHandler = (events: readonly RecordedEvent[]) => Promise<void> | void;

/**
 * What to do after a failed batch: retry with back-off, skip the one failing event, or stop the
 * subscription. `"skip"` skips only the event the handler fails on: the batch is split to find it,
 * and its neighbours are delivered and committed.
 */
export type ErrorDecision = "retry" | "skip" | "stop";

/**
 * Processor mode: the target keeps the checkpoint and writes it in its own transaction, with its
 * rows, instead of `CursorStore.save` after the handler. The object belongs to one subscription;
 * the application keys its checkpoint row itself.
 */
export interface CheckpointTarget {
  /** Where the target's checkpoint stands; `null` starts at `from`. Read once, at start. */
  load(): Promise<Cursor | null>;
  /**
   * Make a skipped poison event durable: under the same lock as the handler, commit `cursorOf([event])`
   * as the checkpoint, with a dead letter if you keep one; do nothing when the checkpoint is already
   * at or beyond the event (skipped before).
   */
  skip(event: RecordedEvent, error: unknown): Promise<void>;
}

/** Options of a durable subscription: store, cursor persistence, start position, polling, error policy. */
export interface SubscribeOptions {
  readonly store: EventStore;
  /** Where the cursor is remembered. Default: in memory (replay after restart). Not together with `target`. */
  readonly cursors?: CursorStore;
  /**
   * Processor mode: the handler writes the checkpoint with its rows, in one transaction, and
   * nothing is saved after it; a skipped event goes to `target.skip`. Not together with `cursors`.
   */
  readonly target?: CheckpointTarget;
  /** Start position when the cursor store (or the target) has nothing for this name. Default `"beginning"`. */
  readonly from?: Cursor | null | "beginning" | "now";
  /**
   * Polling interval. A `LiveStore` push or a `WakeStore` doorbell wakes the poller at once, so
   * polling is only the safety net: default 5 000 ms with a doorbell, 500 ms without.
   */
  readonly pollIntervalMs?: number;
  /** Doorbells within this window share one read (default 25 ms). */
  readonly wakeCoalesceMs?: number;
  /** `false`: deliver records without data (ids and scopes stay) — see `QueryOptions.data`. */
  readonly data?: false;
  /** `false`: deliver records without reading the payload at all — see `QueryOptions.payload`. */
  readonly payload?: false;
  /** Max events per handler call: a positive integer. Default 500. */
  readonly batchSize?: number;
  /**
   * What to do when the handler throws, or when the store, the cursor store or the target fails
   * (then `batch` is empty and `"skip"` means `"retry"`). Asked for the failed batch; after
   * `"skip"`, asked once more for the isolated event the handler fails on alone (`batch` is that
   * one event). Default: `"retry"` with back-off (100 ms → 5 s).
   */
  readonly onError?: (error: unknown, batch: readonly RecordedEvent[]) => ErrorDecision;
}

/** A running subscription: wait for catch-up, stop, inspect the cursor and what it has passed. */
export interface Subscription {
  readonly name: string;
  /**
   * Resolves the next time a poll returns an empty page — i.e. every settled event matching the
   * query up to that moment has been handled. Resolves immediately once the subscription is stopped.
   */
  whenCaughtUp(): Promise<void>;
  /** Stops polling; awaits an in-flight batch. */
  stop(): Promise<void>;
  /**
   * The last persisted cursor. In processor mode: the last checkpoint the target committed, as far
   * as the subscription knows (`target.load()` at start, then every resolved handler call or skip).
   */
  readonly cursor: Cursor | null;
  /**
   * The watermark of the last empty page (`QueryResult.watermark`): every matching event of a
   * transaction below it has been handled or skipped. `null` before the first empty page, or
   * when the store gives none.
   */
  readonly watermark: string | null;
  /**
   * Whether the subscription has passed a read-your-writes token
   * `{ transactionId: appended.transactionId, sequence: appended.last }`: handled or skipped it,
   * by the cursor, or by the watermark of the last empty page (for a write the query does not
   * match). As fresh as the last poll; it reads no store.
   */
  hasPassed(token: Cursor): boolean;
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
 *
 * When the handler fails and `onError` says `"skip"`, the batch is split in halves, left to right,
 * until the one failing event is found; every part that succeeds is committed, the failing event
 * is skipped. Neighbours of a failed batch are delivered again (at-least-once).
 *
 * With `target` (processor mode) the handler writes the checkpoint itself, with its rows, and
 * nothing is saved after it. The subscription stays at-least-once; exactly-once is the target's:
 * it locks its checkpoint and applies only the events beyond it, since a redelivered batch can be
 * longer than the one it already committed. A guard on the batch's end alone is not enough.
 */
export function subscribe(name: string, query: Query, handler: SubscriptionHandler, options: SubscribeOptions): Subscription {
  const store = options.store;
  const target = options.target;
  if (target && options.cursors) {
    throw new UsageError(
      "eventstore: subscribe() takes either `cursors` or `target`, not both — in processor mode (`target`) the target keeps the checkpoint, so drop `cursors`",
    );
  }
  const cursors = options.cursors ?? memoryCursors();
  const pollIntervalMs = options.pollIntervalMs ?? (isWakeStore(store) ? 5_000 : 500);
  const wakeCoalesceMs = options.wakeCoalesceMs ?? 25;
  const batchSize = options.batchSize ?? 500;
  // a page of `limit: 0` is empty whatever waits beyond the cursor: it would never deliver, and its
  // watermark would report unread events as passed
  if (!(Number.isSafeInteger(batchSize) && batchSize >= 1)) {
    throw new UsageError(`eventstore: subscribe() needs a batchSize that is a positive integer (default 500), got ${String(batchSize)}`);
  }
  const onError = options.onError ?? (() => "retry" as const);

  let cursor: Cursor | null = null;
  /** The watermark of the last empty settled page; only ever from that page's own read. */
  let watermark: string | null = null;
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
  const projection = { ...(options.data === false ? { data: false as const } : {}), ...(options.payload === false ? { payload: false as const } : {}) };

  const resolveCaughtUp = (): void => {
    const waiters = caughtUpWaiters;
    caughtUpWaiters = [];
    for (const resolve of waiters) resolve();
  };

  const initialCursor = async (): Promise<Cursor | null> => {
    const stored = await (target ? target.load() : cursors.load(name));
    if (stored) return stored;
    const from = options.from ?? "beginning";
    if (from === "beginning" || from === null) return null;
    if (from === "now") {
      const head = await store.query(query, { settledOnly: true, order: "desc", limit: 1, payload: false, version: false });
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
      // Settled records only, each exactly once: an unsettled one is never fetched (with its payload)
      // before it can be delivered. That one waits is told by the context version instead.
      const result = await store.query(query, { settledOnly: true, cursor, limit: batchSize, version: false, ...projection });
      if (stopped) {
        for (const w of waitersBeforeRead) w(); // stopped = caught up by contract; never leave a waiter hanging
        return;
      }
      const settled = result.events;
      if (settled.length === 0) {
        backoffMs = 0;
        // Everything matching below this read's watermark is at or below the cursor. Taken from
        // this very read only, never from the probe below (a second statement, a later snapshot).
        if (result.watermark !== undefined) watermark = result.watermark;
        for (const w of waitersBeforeRead) w();
        if (caughtUpWaiters.length > 0) wakeRequested = true;
        // Is something beyond the cursor waiting for an older transaction to end? It becomes
        // readable then — possibly on another table, which rings no doorbell — so look again soon
        // rather than after a full poll. One row, from the (transaction_id, sequence) index, with
        // no payload read: whatever it finds beyond the cursor is unsettled, or just settled.
        const waiting = await store.query(query, { cursor, limit: 1, payload: false, version: false });
        if (stopped) return;
        if (waiting.events.length > 0) {
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
      // commits happen inside: every handled part and every skipped event moves the cursor at once
      const outcome = await deliverPage(settled);
      if (outcome === "stop") {
        shutdown(); // not `stop()`: that would await this very poll
        return;
      }
      if (outcome === "retry") {
        // back off from the committed cursor: what was committed of this page stays committed
        backoffMs = backoffMs === 0 ? MIN_BACKOFF_MS : Math.min(backoffMs * 2, MAX_BACKOFF_MS);
        schedule(backoffMs);
        return;
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

  /**
   * Make a handled part count. Default mode: save its cursor. Processor mode: the handler already
   * wrote it with its rows. A failing save is a store error: it leaves to poll's catch.
   */
  const commit = async (part: readonly RecordedEvent[]): Promise<void> => {
    const next = cursorOf(part);
    if (!next) return;
    if (!target) await cursors.save(name, next);
    cursor = next;
  };

  /** Make a skipped event count: its own cursor, saved (default mode) or handed to `target.skip`. */
  const skipEvent = async (event: RecordedEvent, error: unknown): Promise<void> => {
    const next = cursorOf([event]);
    if (target) await target.skip(event, error);
    else if (next) await cursors.save(name, next);
    if (next) cursor = next;
  };

  /** The handler on one part; a resolved part is committed (a commit error propagates). */
  const attempt = async (part: readonly RecordedEvent[]): Promise<{ ok: true } | { ok: false; error: unknown }> => {
    try {
      await handler(part);
    } catch (error) {
      return { ok: false, error };
    }
    await commit(part);
    return { ok: true };
  };

  const ask = (error: unknown, part: readonly RecordedEvent[]): ErrorDecision => {
    try {
      return onError(error, part);
    } catch {
      return "retry";
    }
  };

  /**
   * One page: the handler on all of it; on a failure `onError` decides. `"skip"` skips the one
   * failing event: a single event is skipped at once, a longer page is isolated.
   */
  const deliverPage = async (page: readonly RecordedEvent[]): Promise<"ok" | "retry" | "stop"> => {
    const first = await attempt(page);
    if (first.ok) return "ok";
    const decision = ask(first.error, page);
    if (decision !== "skip") return decision;
    if (page.length === 1) {
      await skipEvent(page[0]!, first.error);
      return "ok";
    }
    return isolate(page);
  };

  /**
   * A failed part of more than one event, after `"skip"`: deliver its halves left to right, commit
   * each that succeeds, split each that fails again, and ask `onError` once more for an event that
   * fails alone. The cursor only moves forward: every commit covers a contiguous prefix of the page.
   */
  const isolate = async (part: readonly RecordedEvent[]): Promise<"ok" | "retry" | "stop"> => {
    const mid = Math.ceil(part.length / 2);
    for (const half of [part.slice(0, mid), part.slice(mid)]) {
      if (stopped) return "ok"; // what is committed stays; nothing more is delivered
      const result = await attempt(half);
      if (result.ok) continue;
      if (half.length > 1) {
        const outcome = await isolate(half);
        if (outcome !== "ok") return outcome;
        continue;
      }
      const decision = ask(result.error, half);
      if (decision !== "skip") return decision;
      await skipEvent(half[0]!, result.error);
    }
    return "ok";
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
    get watermark() {
      return watermark;
    },
    hasPassed(token: Cursor) {
      if (!token || typeof token.transactionId !== "string" || !/^\d+$/.test(token.transactionId) || !Number.isSafeInteger(token.sequence)) {
        throw new UsageError(
          "eventstore: hasPassed(token) needs a Cursor { transactionId, sequence } — build it from an append: { transactionId: appended.transactionId, sequence: appended.last }",
        );
      }
      return passedBy(token, cursor, watermark);
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
