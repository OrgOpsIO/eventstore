import { ContextTooLargeError } from "./errors.js";
import { cursorOf } from "./memory.js";
import { queryKey } from "./query.js";
import type { ContextHandle, Cursor, EventStore, Query, RecordedEvent } from "./types.js";

/**
 * An incremental fold: called with a batch of events (the delta) and the state so far.
 * `$foldAll` deliberately returns `(events, state?: undefined) => S`, which is not assignable
 * here, so a one-shot fold cannot slot in by arity and silently drop the cached state.
 */
export type Fold<S> = (events: readonly RecordedEvent[], state: S) => S;

export interface ContextSpec<S> {
  readonly query: Query;
  /** Pure: folds a batch of events into the state. Called with the delta only. */
  readonly fold: Fold<S>;
  readonly initial: S | (() => S);
  /** Cache key; defaults to the normalised query. */
  readonly key?: string;
}

export interface LoadedContext<S> {
  readonly state: S;
  /** Pass to `appendIf`. Its version is the CCC context version, never the read cursor. */
  readonly ctx: ContextHandle;
  /** Events folded in this call (the delta). */
  readonly delta: readonly RecordedEvent[];
  readonly cacheHit: boolean;
}

interface Entry<S> {
  state: S;
  cursor: Cursor | null;
  bytes: number;
}

export interface ContextCacheOptions {
  /** Max cached contexts per view (LRU). Default 1000. `0` disables caching. */
  readonly max?: number;
  /**
   * Optional byte budget: when `sizeOf(state)` is given, entries are evicted (oldest first)
   * until the sum of sizes is at most `maxBytes` (default 64 MiB). Without `sizeOf` only
   * `max` bounds the cache — tune it to your largest fold state.
   */
  readonly maxBytes?: number;
  readonly sizeOf?: (state: unknown) => number;
  /**
   * Refuse to fold more than this many events in one load (default 100 000). A context that
   * large needs a narrower query, a snapshot, or a read model — silently truncating it would
   * make decisions wrong.
   */
  readonly maxEvents?: number;
}

/**
 * Incremental context loading. The cache keeps, per query, the state folded over SETTLED
 * events and the settled cursor. A load reads only what is beyond that cursor (in
 * `(transactionId, sequence)` order, the one gap-free order), folds the settled part into the
 * cache, folds any unsettled events on top for this decision only, and hands back the CCC
 * context version for `appendIf`.
 *
 * Correctness never depends on the cache: the guard compares the full context version.
 */
export class ContextCache {
  private readonly entries = new Map<string, Entry<unknown>>();
  private readonly max: number;
  private readonly maxEvents: number;
  private readonly maxBytes: number;
  private readonly sizeOf: ((state: unknown) => number) | undefined;
  private bytes = 0;

  constructor(
    private readonly store: EventStore,
    options: ContextCacheOptions = {},
  ) {
    this.max = options.max ?? 1000;
    this.maxEvents = options.maxEvents ?? 100_000;
    this.maxBytes = options.maxBytes ?? 64 * 1024 * 1024;
    this.sizeOf = options.sizeOf;
  }

  async load<S>(spec: ContextSpec<S>): Promise<LoadedContext<S>> {
    const key = spec.key ?? queryKey(spec.query);
    const cached = this.max > 0 ? (this.entries.get(key) as Entry<S> | undefined) : undefined;
    const base: Entry<S> = cached ?? { state: typeof spec.initial === "function" ? (spec.initial as () => S)() : spec.initial, cursor: null, bytes: 0 };

    // `cursor: null` still selects (transactionId, sequence) order, so the settled prefix is exact.
    const result = await this.store.query(spec.query, { cursor: base.cursor ?? null, limit: this.maxEvents + 1 });
    if (result.events.length > this.maxEvents) {
      throw new ContextTooLargeError(key, this.maxEvents);
    }
    const settled: RecordedEvent[] = [];
    const unsettled: RecordedEvent[] = [];
    // In (transactionId, sequence) order every settled record precedes every unsettled one.
    for (const e of result.events) (e.settled && unsettled.length === 0 ? settled : unsettled).push(e);
    const persisted: Entry<S> =
      settled.length > 0 ? { state: spec.fold(settled, base.state), cursor: cursorOf(settled) ?? base.cursor, bytes: 0 } : base;
    if (settled.length > 0 && this.sizeOf) persisted.bytes = this.sizeOf(persisted.state);
    if (this.max > 0) this.remember(key, persisted);
    const state = unsettled.length > 0 ? spec.fold(unsettled, persisted.state) : persisted.state;
    return { state, ctx: result.ctx, delta: result.events, cacheHit: cached !== undefined };
  }

  /** Forget one query (or everything). */
  invalidate(query?: Query): void {
    if (query === undefined) {
      this.entries.clear();
      this.bytes = 0;
    } else this.evict(queryKey(query));
  }

  get size(): number {
    return this.entries.size;
  }

  /** Sum of `sizeOf(state)` over cached entries (0 without `sizeOf`). */
  get byteSize(): number {
    return this.bytes;
  }

  private evict(key: string): void {
    const old = this.entries.get(key);
    if (old) {
      this.bytes -= old.bytes;
      this.entries.delete(key);
    }
  }

  private remember(key: string, entry: Entry<unknown>): void {
    this.evict(key);
    this.entries.set(key, entry);
    this.bytes += entry.bytes;
    while (this.entries.size > this.max || (this.sizeOf && this.bytes > this.maxBytes && this.entries.size > 0)) {
      const oldest = this.entries.keys().next().value;
      if (oldest === undefined) break;
      this.evict(oldest);
    }
  }
}
