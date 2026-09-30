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
  /**
   * Cache key; defaults to the normalised query plus the identity of `fold` and `initial` — two
   * folds over one query are two states, never one. Define folds once (module level, a registry's
   * `$fold` result kept in a const) so repeated loads share their entry; a fold created per call is
   * correct but never cached.
   */
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
   * Byte budget of ONE view (tenant): entries are evicted, least recently used first, until
   * the sum of `sizeOf(state)` is at most `maxBytes` (default 64 MiB).
   */
  readonly maxBytes?: number;
  /**
   * Byte budget of the whole process, across every view: when exceeded, the least recently
   * used entries of ANY view are evicted (default 256 MiB). One knob to match the machine.
   */
  readonly totalMaxBytes?: number;
  /**
   * Size of a fold state in bytes. Default: `estimateSize`, a structural estimate that
   * understands plain objects, arrays, Map and Set (JSON length would report `{}` for a Map).
   */
  readonly sizeOf?: (state: unknown) => number;
  /** A shared budget (created by the runtime). Not needed when you construct a cache by hand. */
  readonly budget?: CacheBudget;
  /**
   * Refuse to fold more than this many events in one load (default 100 000). A context that
   * large needs a narrower query, a snapshot, or a read model — silently truncating it would
   * make decisions wrong.
   */
  readonly maxEvents?: number;
}

const MAX_ESTIMATE_NODES = 200_000;

const identities = new WeakMap<object, number>();
let nextIdentity = 1;
const MAX_KEY_NODES = 10_000;
const MAX_KEY_DEPTH = 64;

function referenceOf(value: object): string {
  let id = identities.get(value);
  if (id === undefined) identities.set(value, (id = nextIdentity++));
  return `#${id}`;
}

/**
 * A plain value's content as a key, or `null` when it is not plain (a function, a Map, a Date, a
 * class instance, a symbol), cyclic, deeper than 64 or larger than the node budget — then it is keyed by
 * reference. Never throws: bigint, NaN, ±Infinity, -0, undefined and holes each get their own tag.
 */
function encodePlain(value: unknown): string | null {
  const seen = new Set<object>();
  let nodes = 0;
  const walk = (v: unknown, depth = 0): string | null => {
    if (++nodes > MAX_KEY_NODES || depth > MAX_KEY_DEPTH) return null;
    switch (typeof v) {
      case "undefined":
        return "u";
      case "boolean":
        return v ? "t" : "f";
      case "bigint":
        return `b${v.toString()}`;
      case "string":
        return JSON.stringify(v);
      case "number":
        return Object.is(v, -0) ? "n-0" : `n${String(v)}`; // String() keeps NaN and ±Infinity apart from null
      case "object": {
        if (v === null) return "z";
        if (seen.has(v)) return null;
        seen.add(v);
        const parts: string[] = [];
        if (Array.isArray(v)) {
          for (let i = 0; i < v.length; i++) {
            const part = i in v ? walk(v[i], depth + 1) : "h";
            if (part === null) return null;
            parts.push(part);
          }
          return `[${parts.join(",")}]`;
        }
        const proto = Object.getPrototypeOf(v);
        if (proto !== Object.prototype && proto !== null) return null;
        for (const key of Object.keys(v)) {
          const part = walk((v as Record<string, unknown>)[key], depth + 1);
          if (part === null) return null;
          parts.push(`${JSON.stringify(key)}:${part}`);
        }
        return `{${parts.join(",")}}`;
      }
      default:
        return null; // function, symbol
    }
  };
  return walk(value);
}

/**
 * The token of an initial state. A factory (`initial: () => ({})`, often written inline and so a
 * new function on every call) is keyed by its source: the same factory text starts the same
 * state. Everything else as `identityOf`.
 */
function initialIdentityOf(value: unknown): string {
  return typeof value === "function" ? `fn:${Function.prototype.toString.call(value)}` : identityOf(value);
}

/** A stable token for a fold or an initial state: plain values by content, everything else by reference. */
function identityOf(value: unknown): string {
  if (typeof value === "function" || (typeof value === "object" && value !== null)) {
    const plain = typeof value === "function" ? null : encodePlain(value);
    return plain === null ? referenceOf(value as object) : `=${plain}`;
  }
  return `=${encodePlain(value)}`;
}

/** The cache key of a context: query, fold and initial state — the three that decide what the state is. */
export function contextKey(spec: Pick<ContextSpec<unknown>, "query" | "fold" | "initial" | "key">): string {
  return spec.key ?? `${queryKey(spec.query)}\u0000${identityOf(spec.fold)}\u0000${initialIdentityOf(spec.initial)}`;
}

/**
 * Rough in-memory size of a fold state: strings by length, 8 bytes per number, ~32 bytes per
 * object/array/Map/Set shell plus keys. Stops after 200 000 nodes and extrapolates nothing —
 * a state that large is a read model in disguise; the estimate is then a floor.
 */
export function estimateSize(value: unknown): number {
  let bytes = 0;
  let nodes = 0;
  const seen = new Set<object>();
  // an explicit stack, not recursion: a deeply nested state must not overflow the call stack
  const stack: unknown[] = [value];
  while (stack.length > 0) {
    if (nodes++ > MAX_ESTIMATE_NODES) break;
    const v = stack.pop();
    switch (typeof v) {
      case "string":
        bytes += 16 + v.length * 2;
        continue;
      case "number":
      case "bigint":
        bytes += 8;
        continue;
      case "boolean":
      case "undefined":
        bytes += 4;
        continue;
      case "object":
        break;
      default:
        continue;
    }
    if (v === null) {
      bytes += 4;
      continue;
    }
    if (seen.has(v)) continue;
    seen.add(v);
    bytes += 32;
    if (v instanceof Map) {
      for (const [k, x] of v) stack.push(k, x);
    } else if (v instanceof Set) {
      for (const x of v) stack.push(x);
    } else if (Array.isArray(v)) {
      bytes += v.length * 8;
      for (const x of v) stack.push(x);
    } else if (v instanceof Date) {
      bytes += 8;
    } else {
      for (const k of Object.keys(v as Record<string, unknown>)) {
        bytes += 16 + k.length * 2;
        stack.push((v as Record<string, unknown>)[k]);
      }
    }
  }
  return bytes;
}

/**
 * A process-wide byte budget shared by every context cache of one runtime. Keeps a global
 * least-recently-used order across caches; when the total exceeds `maxBytes`, the oldest
 * entries of any cache are dropped. Dropping is always safe: the next load re-reads the delta.
 */
export class CacheBudget {
  private readonly lru = new Map<string, { cache: ContextCache; key: string; bytes: number }>();
  private total = 0;
  private nextId = 1;

  constructor(readonly maxBytes: number) {}

  /** @internal */
  idFor(cache: ContextCache): string {
    return String(this.nextId++);
  }

  /** @internal */
  touch(cache: ContextCache, cacheId: string, key: string, bytes: number): void {
    const token = `${cacheId}|${key}`;
    const old = this.lru.get(token);
    if (old) {
      this.total -= old.bytes;
      this.lru.delete(token);
    }
    this.lru.set(token, { cache, key, bytes });
    this.total += bytes;
    while (this.total > this.maxBytes && this.lru.size > 1) {
      const oldest = this.lru.entries().next().value;
      if (!oldest) break;
      const [oldToken, entry] = oldest;
      if (oldToken === token) break; // never evict the entry just written; the next touch will
      entry.cache.dropFromBudget(entry.key);
      // The cache no longer held the entry (nothing was released): drop the token here, or the
      // loop would pick the same oldest token forever and block the event loop.
      if (this.lru.get(oldToken) === entry) {
        this.total -= entry.bytes;
        this.lru.delete(oldToken);
      }
    }
  }

  /** @internal */
  release(cacheId: string, key: string): void {
    const token = `${cacheId}|${key}`;
    const old = this.lru.get(token);
    if (!old) return;
    this.total -= old.bytes;
    this.lru.delete(token);
  }

  /** Bytes currently accounted across all caches. */
  get bytes(): number {
    return this.total;
  }

  get entries(): number {
    return this.lru.size;
  }
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
  private readonly sizeOf: (state: unknown) => number;
  private readonly budget: CacheBudget | undefined;
  private readonly budgetId: string;
  private bytes = 0;

  constructor(
    private readonly store: EventStore,
    options: ContextCacheOptions = {},
  ) {
    this.max = options.max ?? 1000;
    this.maxEvents = options.maxEvents ?? 100_000;
    this.maxBytes = options.maxBytes ?? 64 * 1024 * 1024;
    this.sizeOf = options.sizeOf ?? estimateSize;
    this.budget = options.budget;
    this.budgetId = options.budget ? options.budget.idFor(this) : "";
  }

  async load<S>(spec: ContextSpec<S>): Promise<LoadedContext<S>> {
    const key = contextKey(spec as ContextSpec<unknown>);
    const cached = this.max > 0 ? (this.entries.get(key) as Entry<S> | undefined) : undefined;
    const base: Entry<S> = cached ?? { state: freshInitial(spec.initial), cursor: null, bytes: 0 };

    // `cursor: null` still selects (transactionId, sequence) order, so the settled prefix is exact.
    const result = await this.store.query(spec.query, { cursor: base.cursor ?? null, limit: this.maxEvents + 1 });
    if (result.events.length > this.maxEvents) {
      throw new ContextTooLargeError(spec.key ?? queryKey(spec.query), this.maxEvents);
    }
    const settled: RecordedEvent[] = [];
    const unsettled: RecordedEvent[] = [];
    // In (transactionId, sequence) order every settled record precedes every unsettled one.
    for (const e of result.events) (e.settled && unsettled.length === 0 ? settled : unsettled).push(e);
    const persisted: Entry<S> =
      settled.length > 0 ? { state: spec.fold(settled, base.state), cursor: cursorOf(settled) ?? base.cursor, bytes: 0 } : base;
    if (settled.length > 0) persisted.bytes = this.sizeOf(persisted.state);
    if (this.max > 0) this.remember(key, persisted);
    const state = unsettled.length > 0 ? spec.fold(unsettled, persisted.state) : persisted.state;
    return { state, ctx: result.ctx, delta: result.events, cacheHit: cached !== undefined };
  }

  /** Forget one query — under every fold — or everything. Either way the entries leave the shared budget too. */
  invalidate(query?: Query): void {
    if (query === undefined) {
      for (const key of [...this.entries.keys()]) this.evict(key);
      return;
    }
    const prefix = `${queryKey(query)}\u0000`;
    for (const key of [...this.entries.keys()]) if (key.startsWith(prefix)) this.evict(key);
  }

  get size(): number {
    return this.entries.size;
  }

  /** Sum of `sizeOf(state)` over cached entries. */
  get byteSize(): number {
    return this.bytes;
  }

  /** @internal called by the shared budget when this cache's entry is the oldest in the process */
  dropFromBudget(key: string): void {
    const old = this.entries.get(key);
    if (old) {
      this.bytes -= old.bytes;
      this.entries.delete(key);
      this.budget?.release(this.budgetId, key);
    }
  }

  private evict(key: string): void {
    this.dropFromBudget(key);
  }

  private remember(key: string, entry: Entry<unknown>): void {
    this.evict(key);
    this.entries.set(key, entry);
    this.bytes += entry.bytes;
    while (this.entries.size > this.max || (this.bytes > this.maxBytes && this.entries.size > 1)) {
      const oldest = this.entries.keys().next().value;
      if (oldest === undefined || oldest === key) break;
      this.evict(oldest);
    }
    this.budget?.touch(this, this.budgetId, key, entry.bytes);
  }
}

/**
 * The state a cold load starts from. A plain initial value is copied, so a fold that updates its
 * state in place never changes the caller's object — whose content is part of the cache key.
 */
function freshInitial<S>(initial: S | (() => S)): S {
  if (typeof initial === "function") return (initial as () => S)();
  if (typeof initial === "object" && initial !== null && encodePlain(initial) !== null) return structuredClone(initial);
  return initial;
}
