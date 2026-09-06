import { queryKey } from "./query.js";
import type { ContextHandle, Cursor, EventStore, Query, RecordedEvent } from "./types.js";

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
}

export interface ContextCacheOptions {
  /** Max cached contexts per process (LRU). Default 1000. `0` disables caching. */
  readonly max?: number;
}

/**
 * Incremental context loading. The cache keeps, per query, the state folded over SETTLED
 * events and the settled cursor. A load reads only what is beyond that cursor, folds the
 * settled part into the cache, folds any unsettled (visible but not yet gap-free) events on
 * top for this decision only, and hands back the CCC context version for `appendIf`.
 *
 * Correctness never depends on the cache: the guard compares the full context version.
 */
export class ContextCache {
  private readonly entries = new Map<string, Entry<unknown>>();
  private readonly max: number;

  constructor(
    private readonly store: EventStore,
    options: ContextCacheOptions = {},
  ) {
    this.max = options.max ?? 1000;
  }

  async load<S>(spec: ContextSpec<S>): Promise<LoadedContext<S>> {
    const key = spec.key ?? queryKey(spec.query);
    const cached = this.max > 0 ? (this.entries.get(key) as Entry<S> | undefined) : undefined;
    const base: Entry<S> = cached ?? { state: typeof spec.initial === "function" ? (spec.initial as () => S)() : spec.initial, cursor: null };

    const result = await this.store.query(spec.query, { cursor: base.cursor });
    const settled: RecordedEvent[] = [];
    const unsettled: RecordedEvent[] = [];
    // Records come back ascending; the settled prefix is everything up to the last settled one
    // only if no unsettled record sits before it — otherwise persist just the gap-free prefix.
    let gapFree = true;
    for (const e of result.events) {
      if (e.settled && gapFree) settled.push(e);
      else {
        gapFree = false;
        unsettled.push(e);
      }
    }
    const persisted: Entry<S> = settled.length > 0
      ? { state: spec.fold(settled, base.state), cursor: cursorOf(settled) ?? base.cursor }
      : base;
    if (this.max > 0) this.remember(key, persisted);
    const state = unsettled.length > 0 ? spec.fold(unsettled, persisted.state) : persisted.state;
    return {
      state,
      ctx: { query: spec.query, version: result.contextVersion },
      delta: result.events,
      cacheHit: cached !== undefined,
    };
  }

  /** Forget one query (or everything). */
  invalidate(query?: Query): void {
    if (query === undefined) this.entries.clear();
    else this.entries.delete(queryKey(query));
  }

  get size(): number {
    return this.entries.size;
  }

  private remember(key: string, entry: Entry<unknown>): void {
    this.entries.delete(key);
    this.entries.set(key, entry);
    while (this.entries.size > this.max) {
      const oldest = this.entries.keys().next().value;
      if (oldest === undefined) break;
      this.entries.delete(oldest);
    }
  }
}

function cursorOf(events: readonly RecordedEvent[]): Cursor | null {
  const last = events[events.length - 1];
  return last ? { transactionId: last.transactionId, sequence: last.sequence } : null;
}
