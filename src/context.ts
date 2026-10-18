import { ContextTooLargeError, UsageError } from "./errors.js";
import { foldFingerprint, typeDefsFor, warnIfChanged } from "./fingerprint.js";
import { checkStorable, decodeState, encodeState, foldIdentityOf, type FoldIdentity } from "./foldstate.js";
import { cursorOf } from "./memory.js";
import { queryKey } from "./query.js";
import type { ContextHandle, Cursor, EventStore, FoldStateKey, FoldStateStore, Query, RecordedEvent } from "./types.js";

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
  /** A label for `onLoad` (which fold this is). Not part of the cache key. */
  readonly name?: string;
}

/** What one context load cost, reported to `ContextCacheOptions.onLoad`. Times in milliseconds. */
export interface ContextLoad {
  /** The spec's `name`, if given. */
  readonly name?: string;
  /** The tenant of the view the load ran in (undefined outside a tenant view). */
  readonly tenant?: string;
  /** The normalised query (`queryKey`): values included, so group by `name` for a per-fold view. */
  readonly query: string;
  readonly cacheHit: boolean;
  /** Events read and folded in this load (the delta). */
  readonly events: number;
  /** Time in the store: the delta query, including transfer and decoding. */
  readonly readMs: number;
  /** Time in the fold (settled and unsettled part). */
  readonly foldMs: number;
  /** Time measuring the new state for the byte budget (`sizeOf`). */
  readonly sizeMs: number;
  /** `sizeOf(state)` after the load, as accounted in the budget. */
  readonly bytes: number;
  /** Where the state this load folded onto came from: the in-memory entry, a stored state (persisted fold), or the initial state. */
  readonly start: "cache" | "stored" | "initial";
  /** Present when the persisted fold tier took part in this load (read or write). Times in milliseconds. */
  readonly stored?: {
    /** Reading the stored state and decoding it; 0 when not read (an in-memory hit). */
    readonly readMs: number;
    /** Whether a state was written in this load. */
    readonly written: boolean;
    /** The storability check, encoding and writing; 0 when no write was due. */
    readonly writeMs: number;
    /** A decode failure, a refused (non-storable) state, a failed encoding or a failed write. Never thrown. */
    readonly error?: unknown;
  };
}

export interface LoadedContext<S> {
  readonly state: S;
  /** Pass to `appendIf`. Its version is the CCC context version, never the read cursor. */
  readonly ctx: ContextHandle;
  /** Events folded in this call (the delta). A paged load (a persisted fold over `maxEvents`) keeps only its last read's events here, so memory stays bounded by one page. */
  readonly delta: readonly RecordedEvent[];
  readonly cacheHit: boolean;
}

interface Entry<S> {
  state: S;
  cursor: Cursor | null;
  bytes: number;
  /** Settled events folded in since the state was last read from or written to the persisted fold tier. */
  sinceStored: number;
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
  /**
   * Called after every load with what it cost: read time against fold time against sizing
   * time. Synchronous and cheap, please; an exception it throws is swallowed. Runs with a cache
   * of `max: 0` too, so it measures cold loads as well.
   */
  readonly onLoad?: (load: ContextLoad) => void;
  /**
   * The persisted fold tier: on a store with `persistedFolds: true`, the state of a fold made by
   * `defineFold` is also kept in the store, so a cold process folds only what came after it.
   * `after`: write the state again once at least this many settled events were folded into it
   * since it was last read or written (default 500). `false` turns the tier off.
   */
  readonly persist?: false | { readonly after?: number };
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

/** The initial state as the fingerprint sees it: a factory's text, a plain value's content, `"ref"` for anything kept by reference. */
export function initialToken(value: unknown): string {
  const token = initialIdentityOf(value);
  return token.startsWith("#") ? "ref" : token;
}

/** A stable token for a fold or an initial state: plain values by content, everything else by reference. */
function identityOf(value: unknown): string {
  if (typeof value === "function" || (typeof value === "object" && value !== null)) {
    const plain = typeof value === "function" ? null : encodePlain(value);
    return plain === null ? referenceOf(value as object) : `=${plain}`;
  }
  return `=${encodePlain(value)}`;
}

/**
 * The persisted fold tier's key text: the query and the initial state, both stable across
 * processes. A reference-keyed initial value (a shared `new Map()`) is another object in every
 * process, so it is refused rather than risking another process's state.
 */
function stateKeyText(spec: Pick<ContextSpec<unknown>, "query" | "initial">, identity: FoldIdentity): string {
  const token = initialIdentityOf(spec.initial);
  if (!token.startsWith("fn:") && !token.startsWith("=")) {
    throw new UsageError(
      `eventstore: a persisted fold needs an initial state that is a plain value or a factory — pass initial: () => new Map() instead of a shared instance (fold "${identity.name}" v${identity.version})`,
    );
  }
  // one JSON text: Postgres `text` cannot hold the NUL that separates the in-memory key's parts
  return JSON.stringify([queryKey(spec.query), token]);
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
  private readonly onLoad: ((load: ContextLoad) => void) | undefined;
  /** The persisted fold tier's write threshold; `0` when the tier is off. */
  private readonly persistAfter: number;
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
    this.onLoad = options.onLoad;
    const after = options.persist === false ? 0 : (options.persist?.after ?? 500);
    if (options.persist !== false && (!Number.isSafeInteger(after) || after < 1)) {
      throw new UsageError(`eventstore: contextCache.persist.after must be an integer of at least 1, got ${String(after)}`);
    }
    this.persistAfter = after;
  }

  /** The persisted fold tier for this load, or `undefined` when it is off (no store capability, `persist: false`, a fold without identity). */
  private tierFor(
    spec: ContextSpec<unknown>,
  ): { readonly states: FoldStateStore; readonly identity: FoldIdentity; readonly key: FoldStateKey; readonly fingerprint: () => { fingerprint?: string } } | undefined {
    if (this.persistAfter === 0) return undefined;
    const states = (this.store as EventStore & { foldStates?: FoldStateStore }).foldStates;
    if (!states) return undefined;
    const identity = foldIdentityOf(spec.fold);
    if (!identity) return undefined;
    // a cache built by hand (no runtime's event schemas) writes no fingerprint and compares none (decision 44)
    const defs = typeDefsFor(this);
    const fingerprint = () => (defs ? { fingerprint: foldFingerprint(spec.fold, spec.query, defs) } : {});
    return { states, identity, key: { name: identity.name, version: identity.version, key: stateKeyText(spec, identity) }, fingerprint };
  }

  async load<S>(spec: ContextSpec<S>): Promise<LoadedContext<S>> {
    const key = contextKey(spec as ContextSpec<unknown>);
    const tier = this.tierFor(spec as ContextSpec<unknown>); // throws on an unstable initial, before any I/O
    const cached = this.max > 0 ? (this.entries.get(key) as Entry<S> | undefined) : undefined;
    let base: Entry<S>;
    let start: ContextLoad["start"];
    let storedReadMs = 0;
    let tierError: unknown;
    let hasTierError = false;
    if (cached) {
      base = cached;
      start = "cache";
    } else if (tier) {
      const r0 = performance.now();
      const row = await tier.states.read(tier.key); // a read error propagates: a missing table or grant must be seen
      base = { state: undefined as S, cursor: null, bytes: 0, sinceStored: 0 };
      start = "initial";
      if (row) {
        try {
          base = { state: decodeState(row.state) as S, cursor: row.cursor, bytes: 0, sinceStored: 0 };
          start = "stored";
        } catch (err) {
          tierError = err; // unreadable bytes are a miss: fold from zero, the next write replaces them
          hasTierError = true;
        }
      }
      if (start === "initial") base.state = freshInitial(spec.initial);
      else {
        // a different fold wrote it under the same version: a warning, once per process; the state is used as always (decision 42)
        const mine = tier.fingerprint().fingerprint;
        if (mine !== undefined) warnIfChanged("persisted fold", tier.identity.name, tier.identity.version, row!.fingerprint, mine);
      }
      storedReadMs = performance.now() - r0;
    } else {
      base = { state: freshInitial(spec.initial), cursor: null, bytes: 0, sinceStored: 0 };
      start = "initial";
    }

    let t0 = performance.now();
    // `cursor: null` still selects (transactionId, sequence) order, so the settled prefix is exact.
    let result = await this.store.query(spec.query, { cursor: base.cursor ?? null, limit: this.maxEvents + 1 });
    // the paged fold (decision 41): with the tier on, a delta over maxEvents is folded a page of settled events at a
    // time, the state written along the way — each next read is a load from a stored state at that cursor
    let pagedReadMs = 0;
    let pagedFoldMs = 0;
    let pagedEvents = 0;
    let writeMs = 0;
    let written = false;
    let writeFailed = false;
    while (result.events.length > this.maxEvents) {
      if (!tier) throw new ContextTooLargeError(spec.key ?? queryKey(spec.query), this.maxEvents);
      const page: RecordedEvent[] = [];
      for (const e of result.events) {
        if (!e.settled || page.length === this.maxEvents) break; // every settled record precedes every unsettled one
        page.push(e);
      }
      if (page.length === 0) throw new ContextTooLargeError(spec.key ?? queryKey(spec.query), this.maxEvents);
      pagedReadMs += performance.now() - t0;
      const f0 = performance.now();
      base = { state: spec.fold(page, base.state), cursor: cursorOf(page) ?? base.cursor, bytes: 0, sinceStored: base.sinceStored + page.length };
      pagedFoldMs += performance.now() - f0;
      pagedEvents += page.length;
      if (!writeFailed && base.sinceStored >= this.persistAfter) {
        const w0 = performance.now();
        try {
          const refusal = checkStorable(base.state, tier.identity);
          if (refusal) throw refusal;
          await tier.states.write(tier.key, { cursor: base.cursor!, state: encodeState(base.state), ...tier.fingerprint() });
          written = true;
        } catch (err) {
          tierError = err; // reported, never thrown; no more writes in this load
          hasTierError = true;
          writeFailed = true;
        }
        base.sinceStored = 0;
        writeMs += performance.now() - w0;
      }
      t0 = performance.now();
      result = await this.store.query(spec.query, { cursor: base.cursor, limit: this.maxEvents + 1 });
    }
    const t1 = this.onLoad ? performance.now() : 0;
    const settled: RecordedEvent[] = [];
    const unsettled: RecordedEvent[] = [];
    // In (transactionId, sequence) order every settled record precedes every unsettled one.
    for (const e of result.events) (e.settled && unsettled.length === 0 ? settled : unsettled).push(e);
    const persisted: Entry<S> =
      settled.length > 0
        ? { state: spec.fold(settled, base.state), cursor: cursorOf(settled) ?? base.cursor, bytes: 0, sinceStored: base.sinceStored + settled.length }
        : base;
    const t2 = this.onLoad ? performance.now() : 0;
    // A write is due: encode now, before the unsettled part is folded on top and before any await.
    let encoded: Uint8Array | undefined;
    let writeDue = written || writeFailed;
    if (tier && !writeFailed && settled.length > 0 && persisted.sinceStored >= this.persistAfter) {
      writeDue = true;
      const w0 = performance.now();
      try {
        const refusal = checkStorable(persisted.state, tier.identity);
        if (refusal) throw refusal;
        encoded = encodeState(persisted.state); // throws on a state too deep for the serializer
      } catch (err) {
        tierError = err; // like a failed write: reported, never thrown
        hasTierError = true;
        persisted.sinceStored = 0; // not retried on every following load
      }
      writeMs = performance.now() - w0;
    }
    const t3 = this.onLoad ? performance.now() : 0;
    // a state read from the tier enters the cache now, so it is sized even when nothing was folded into it
    if (settled.length > 0 || start === "stored") persisted.bytes = this.sizeOf(persisted.state);
    const t4 = this.onLoad ? performance.now() : 0;
    if (this.max > 0) this.remember(key, persisted);
    const state = unsettled.length > 0 ? spec.fold(unsettled, persisted.state) : persisted.state;
    const t5 = this.onLoad ? performance.now() : 0;
    if (tier && encoded) {
      const w1 = performance.now();
      try {
        await tier.states.write(tier.key, { cursor: persisted.cursor!, state: encoded, ...tier.fingerprint() });
        written = true;
      } catch (err) {
        tierError = err; // a failed write never fails a load
        hasTierError = true;
      }
      persisted.sinceStored = 0; // the entry remembered above is this object
      writeMs += performance.now() - w1;
    }
    if (this.onLoad) {
      const tierTookPart = tier !== undefined && (start !== "cache" || writeDue);
      this.report({
        ...(spec.name !== undefined ? { name: spec.name } : {}),
        query: queryKey(spec.query),
        cacheHit: cached !== undefined,
        events: pagedEvents + result.events.length,
        readMs: pagedReadMs + (t1 - t0),
        foldMs: pagedFoldMs + (t2 - t1) + (t5 - t4),
        sizeMs: t4 - t3,
        bytes: persisted.bytes,
        start,
        ...(tierTookPart ? { stored: { readMs: storedReadMs, written, writeMs, ...(hasTierError ? { error: tierError } : {}) } } : {}),
      });
    }
    return { state, ctx: result.ctx, delta: result.events, cacheHit: cached !== undefined };
  }

  private report(load: ContextLoad): void {
    try {
      this.onLoad?.(load);
    } catch {
      // telemetry never fails a load
    }
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
export function freshInitial<S>(initial: S | (() => S)): S {
  if (typeof initial === "function") return (initial as () => S)();
  if (typeof initial === "object" && initial !== null && encodePlain(initial) !== null) return structuredClone(initial);
  return initial;
}
