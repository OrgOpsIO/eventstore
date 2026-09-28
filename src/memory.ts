import { UniqueViolationError } from "./errors.js";
import { uuidv7 } from "./ids.js";
import {
  assertOmittable,
  compareCursor,
  conditionLockKeys,
  filtersOf,
  fromPayload,
  matchesFilter,
  normaliseOptions,
  toPayload,
  trimData,
  uniquePathSegments,
  valueAtPath,
} from "./query.js";
import { emptySchema } from "./registry.js";
import type {
  AppendIfOutcome,
  AppendResult,
  ContextHandle,
  Cursor,
  EventStore,
  Filter,
  NewEvent,
  Query,
  QueryOptions,
  QueryResult,
  RecordedEvent,
  StatisticsStore,
  StoreSchema,
  TypeStatistics,
  WakeStore,
} from "./types.js";

/** Called with the records of one append batch. */
export type AppendedListener = (events: readonly RecordedEvent[]) => void;

/** Optional capability: a store that can push appended events to in-process listeners. */
export interface LiveStore {
  onAppended(listener: AppendedListener): () => void;
}

/** Options of the in-memory reference store. */
export interface MemoryStoreOptions {
  /** Declared events/scopes; default: an empty, non-strict schema. */
  readonly schema?: StoreSchema;
  /** Source of `recordedAt`. Default: wall clock. */
  readonly clock?: () => Date;
  /**
   * Simulate Postgres transaction semantics (tests only). `allocateId` decides the
   * `transactionId` of a batch (default: the batch's first sequence — monotone with the
   * sequence, so no inversion); `isSettled` decides whether a record is settled right now
   * (default: always). With these, the memory store can reproduce an xid/sequence inversion
   * and in-flight transactions, so cursor and cache behaviour can be specified here and
   * checked against Postgres.
   */
  readonly transactions?: {
    readonly allocateId?: (first: number, count: number) => string;
    readonly isSettled?: (transactionId: string, sequence: number) => boolean;
  };
}

/** A stored row: the wire payload, so `upcast` runs on read exactly as in Postgres. */
interface Row {
  readonly sequence: number;
  readonly type: string;
  readonly payload: Record<string, unknown>;
  readonly metadata: Record<string, unknown>;
  readonly recordedAt: Date;
  readonly transactionId: string;
  /** Memoised projection of the row (rows are immutable, `upcast` is deterministic). */
  recorded?: RecordedEvent;
}

/**
 * The semantic reference implementation. Every behaviour of the Postgres store is specified
 * by this one and checked by the shared conformance suite. Single-threaded, so `appendIf` is
 * atomic by construction; all events are settled; one transaction id per append batch.
 *
 * Reads are served from posting lists (per event type and per `(scopeKey, value)` pair —
 * the same pairs the Postgres store indexes and locks), so a context read costs O(matches),
 * not O(store). Every candidate is still verified with the full filter predicate, so the
 * index can only speed things up, never change an answer.
 */
export class MemoryStore implements EventStore, LiveStore, WakeStore, StatisticsStore {
  readonly schema: StoreSchema;
  private readonly clock: () => Date;
  private readonly rows: Row[] = [];
  /** row indices per event type, ascending */
  private readonly byType = new Map<string, number[]>();
  /** row indices per scope key → value, ascending */
  private readonly byScope = new Map<string, Map<string, number[]>>();
  private readonly listeners = new Set<AppendedListener>();
  private readonly doorbells = new Set<(hint: number | null) => void>();
  private readonly uniqueSeen = new Map<string, Set<string>>();
  private readonly idempotencyKeys = new Set<string>();
  private readonly idKeys = new Map<string, string>();
  private readonly allocateId: (first: number, count: number) => string;
  private readonly isSettled: ((transactionId: string, sequence: number) => boolean) | undefined;

  constructor(options: MemoryStoreOptions = {}) {
    this.schema = options.schema ?? emptySchema();
    this.clock = options.clock ?? (() => new Date());
    this.allocateId = options.transactions?.allocateId ?? ((first) => String(first));
    this.isSettled = options.transactions?.isSettled;
  }

  /** All records, in order. Handy in tests. */
  get events(): readonly RecordedEvent[] {
    return this.rows.map((r) => this.toRecorded(r));
  }

  async query(query: Query, options: QueryOptions = {}): Promise<QueryResult> {
    return this.queryNow(query, options);
  }

  queryNow(query: Query, options: QueryOptions = {}): QueryResult {
    normaliseOptions(options);
    const filters = filtersOf(query);
    const hits = new Map<RecordedEvent, boolean[]>();
    const visible: RecordedEvent[] = [];
    let contextVersion = 0;
    for (const index of this.candidates(filters)) {
      // `until` is a view of the past: it narrows the version as well as the rows
      if (options.until !== undefined && this.rows[index]!.sequence > options.until) continue;
      const record = this.toRecorded(this.rows[index]!);
      const matched = filters.map((f) => matchesFilter(record, f, this.schema));
      if (!matched.some(Boolean)) continue;
      if (record.sequence > contextVersion) contextVersion = record.sequence;
      if (this.visible(record, options)) {
        // matching and the version saw the full record; only what is returned is trimmed
        const shown = options.omit ? this.trimmed(record, options.omit) : record;
        visible.push(shown);
        hits.set(shown, matched);
      }
    }
    let events = visible;
    if (options.cursor !== undefined || options.settledOnly) events = [...events].sort(compareCursor);
    if (options.order === "desc") events = [...events].reverse();
    if (options.limit !== undefined) events = events.slice(0, options.limit);
    const lastReturned = events.reduce((m, e) => Math.max(m, e.sequence), 0);
    return {
      events,
      byFilter: filters.map((_, i) => events.filter((e) => hits.get(e)![i])),
      lastReturned,
      contextVersion,
      ctx: { query, version: contextVersion },
      settledCursor: cursorOf(events),
    };
  }

  /** Count, JSON bytes and last sequence per type, over the records a query matches (all when none is given). */
  async statistics(query: Query = {}): Promise<readonly TypeStatistics[]> {
    const filters = filtersOf(query);
    const byType = new Map<string, { count: number; bytes: number; lastSequence: number }>();
    for (const index of this.candidates(filters)) {
      const row = this.rows[index]!;
      const record = this.toRecorded(row);
      if (!filters.some((f) => matchesFilter(record, f, this.schema))) continue;
      const entry = byType.get(row.type) ?? { count: 0, bytes: 0, lastSequence: 0 };
      entry.count++;
      entry.bytes += Buffer.byteLength(JSON.stringify(row.payload));
      if (row.sequence > entry.lastSequence) entry.lastSequence = row.sequence;
      byType.set(row.type, entry);
    }
    return [...byType.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([type, s]) => ({ type, ...s }));
  }

  /** The record without the omitted data keys; the own id key is refused. */
  private trimmed(record: RecordedEvent, omit: readonly string[]): RecordedEvent {
    assertOmittable(omit, this.idKeyOf(record.type), record.type);
    const data = trimData(record.data as Record<string, unknown>, omit);
    return data === record.data ? record : Object.freeze({ ...record, data: Object.freeze(data) });
  }

  /** Ascending row indices that could match any of the filters (superset; verified by the caller). */
  private candidates(filters: readonly Filter[]): number[] {
    const lists: number[][] = [];
    for (const filter of filters) {
      const list = this.candidatesFor(filter);
      if (list === null) return this.rows.map((_, i) => i);
      lists.push(list);
    }
    return lists.length === 1 ? lists[0]! : mergeSorted(lists);
  }

  /** The shortest posting list that covers one filter, or `null` when only a scan can. */
  private candidatesFor(filter: Filter): number[] | null {
    let best: number[] | null = null;
    // merge several lists only when the union can still beat the best list found so far
    const considerUnion = (lists: number[][]) => {
      const total = lists.reduce((n, l) => n + l.length, 0);
      if (best !== null && best.length <= total && lists.length > 1) return;
      const list = lists.length === 1 ? lists[0]! : mergeSorted(lists);
      if (best === null || list.length < best.length) best = list;
    };
    if (filter.scopes) {
      for (const [key, values] of Object.entries(filter.scopes)) {
        if (!this.schema.scopeKeys.includes(key)) {
          // an undeclared key may sit flat in any row's data and is not posted → only a scan is a superset
          if (best === null) return null;
          continue;
        }
        const byValue = this.byScope.get(key);
        if (!byValue) return []; // declared keys are fully indexed (scopes, own id, flat data): nothing carries it
        considerUnion((values as readonly string[]).map((v) => byValue.get(v) ?? []));
      }
    }
    if (filter.types) considerUnion(filter.types.map((t) => this.byType.get(t) ?? []));
    return best;
  }

  private visible(record: RecordedEvent, options: QueryOptions): boolean {
    if (options.after !== undefined && record.sequence <= options.after) return false;
    if (options.settledOnly && !record.settled) return false;
    if (options.cursor && record.settled && compareCursor(record, options.cursor) <= 0) return false;
    return true;
  }

  private idKeyOf(type: string): string {
    let key = this.idKeys.get(type);
    if (key === undefined) this.idKeys.set(type, (key = this.schema.idKeyOf(type)));
    return key;
  }

  private toRecorded(row: Row): RecordedEvent {
    if (this.isSettled) {
      // settledness may change between reads: never memoise it
      const base = this.toRecordedBase(row);
      return Object.freeze({ ...base, settled: this.isSettled(row.transactionId, row.sequence) });
    }
    return this.toRecordedBase(row);
  }

  private toRecordedBase(row: Row): RecordedEvent {
    if (row.recorded) return row.recorded;
    const idKey = this.idKeyOf(row.type);
    // id and scopes come from the RAW payload — that is what the index matched on (Postgres: es_scope);
    // `upcast` may reshape data, never scope keys or the id.
    const raw = fromPayload(row.payload, idKey);
    const { data } = fromPayload(this.schema.upcast(row.type, row.payload), idKey);
    row.recorded = Object.freeze({
      type: row.type,
      data: Object.freeze(data),
      id: raw.id ?? `~${row.sequence}`,
      scopes: Object.freeze(raw.scopes),
      metadata: Object.freeze({ ...row.metadata }),
      sequence: row.sequence,
      recordedAt: row.recordedAt,
      transactionId: row.transactionId,
      settled: true,
    });
    return row.recorded;
  }

  async append(events: readonly NewEvent[]): Promise<AppendResult> {
    return this.appendNow(events);
  }

  async appendIf(events: readonly NewEvent[], ctx: ContextHandle): Promise<AppendIfOutcome> {
    // Validates lockability the same way the Postgres store does (strict mode errors).
    conditionLockKeys(ctx.query, this.schema);
    const actual = this.queryNow(ctx.query).contextVersion;
    if (actual !== ctx.version) return { ok: false, conflict: { expected: ctx.version, actual } };
    return { ok: true, appended: this.appendNow(events) };
  }

  private appendNow(events: readonly NewEvent[]): AppendResult {
    if (events.length === 0) throw new Error("eventstore: append needs at least one event");
    const now = this.clock();
    const prepared = events.map((raw) => {
      const event = this.schema.validate(raw);
      const id = event.id ?? uuidv7(now.getTime());
      const idKey = this.idKeyOf(event.type);
      // JSON round trip: the reference store must forget what JSONB forgets (Dates → strings,
      // `undefined` → missing, NaN → null) so tests against memory behave like production.
      const payload = jsonRoundTrip(toPayload({ ...event, id }, idKey)) as Record<string, unknown>;
      const metadata = jsonRoundTrip(event.metadata ?? {}) as Record<string, unknown>;
      return { event, id, payload, metadata };
    });
    // Check constraints for the whole batch before committing anything (all or nothing).
    const batchUniques = new Set<string>();
    const batchIdem = new Set<string>();
    for (const { event, payload, metadata } of prepared) {
      for (const u of this.schema.uniques) {
        if (u.type !== event.type) continue;
        const value = valueAtPath(payload, uniquePathSegments(u.path));
        if (value === undefined || value === null) continue;
        const key = `${u.type}|${u.path}`;
        const serialized = JSON.stringify(value);
        if (this.uniqueSeen.get(key)?.has(serialized) || batchUniques.has(`${key}|${serialized}`)) {
          throw new UniqueViolationError({ type: u.type, path: u.path });
        }
        batchUniques.add(`${key}|${serialized}`);
      }
      const idem = metadata.idempotencyKey;
      if (typeof idem === "string") {
        if (this.idempotencyKeys.has(idem) || batchIdem.has(idem)) throw new UniqueViolationError({ idempotencyKey: true });
        batchIdem.add(idem);
      }
    }
    const first = this.rows.length + 1;
    const transactionId = this.allocateId(first, prepared.length);
    if (!/^\d+$/.test(transactionId)) throw new Error("eventstore: transactions.allocateId must return a decimal string");
    const rows: Row[] = prepared.map(({ event, payload, metadata }, i) => ({
      sequence: first + i,
      type: event.type,
      payload,
      metadata,
      recordedAt: now,
      transactionId,
    }));
    for (const key of batchUniques) {
      const [type, path, serialized] = splitUniqueKey(key);
      let set = this.uniqueSeen.get(`${type}|${path}`);
      if (!set) this.uniqueSeen.set(`${type}|${path}`, (set = new Set()));
      set.add(serialized);
    }
    for (const k of batchIdem) this.idempotencyKeys.add(k);
    for (const row of rows) {
      const index = this.rows.length;
      this.rows.push(row);
      this.indexRow(index, row);
    }
    const result = { first, last: first + rows.length - 1, count: rows.length };
    if (this.listeners.size > 0) {
      const recorded = rows.map((r) => this.toRecorded(r));
      for (const listener of this.listeners) {
        try {
          listener(recorded);
        } catch {
          // a listener must never fail an append
        }
      }
    }
    for (const ring of this.doorbells) {
      try {
        ring(result.last);
      } catch {
        // a doorbell must never fail an append
      }
    }
    return result;
  }

  /** Posting lists mirror `scopeValueOf`: the `scopes` object, the own id, declared keys flat in data. */
  private indexRow(index: number, row: Row): void {
    push(this.byType, row.type, index);
    const scopes = row.payload.scopes;
    if (scopes && typeof scopes === "object" && !Array.isArray(scopes)) {
      for (const [k, v] of Object.entries(scopes as Record<string, unknown>)) if (typeof v === "string") this.post(k, v, index);
    }
    const idKey = this.idKeyOf(row.type);
    const own = row.payload[idKey];
    if (typeof own === "string") this.post(idKey, own, index);
    for (const key of this.schema.scopeKeys) {
      if (key === idKey) continue;
      const flat = row.payload[key];
      if (typeof flat === "string") this.post(key, flat, index);
    }
  }

  private post(key: string, value: string, index: number): void {
    let byValue = this.byScope.get(key);
    if (!byValue) this.byScope.set(key, (byValue = new Map()));
    const list = byValue.get(value);
    if (!list) byValue.set(value, [index]);
    else if (list[list.length - 1] !== index) list.push(index);
  }

  onAppended(listener: AppendedListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  onCommitted(listener: (hint: number | null) => void): () => void {
    this.doorbells.add(listener);
    return () => this.doorbells.delete(listener);
  }

  async close(): Promise<void> {
    this.listeners.clear();
    this.doorbells.clear();
  }
}

function push(map: Map<string, number[]>, key: string, index: number): void {
  const list = map.get(key);
  if (!list) map.set(key, [index]);
  else if (list[list.length - 1] !== index) list.push(index);
}

/** Merge ascending, duplicate-free lists into one ascending, duplicate-free list. */
function mergeSorted(lists: readonly number[][]): number[] {
  const nonEmpty = lists.filter((l) => l.length > 0);
  if (nonEmpty.length === 0) return [];
  if (nonEmpty.length === 1) return nonEmpty[0]!;
  const out: number[] = [];
  const cursors = nonEmpty.map(() => 0);
  for (;;) {
    let min = Infinity;
    for (let i = 0; i < nonEmpty.length; i++) {
      const c = cursors[i]!;
      const list = nonEmpty[i]!;
      if (c < list.length && list[c]! < min) min = list[c]!;
    }
    if (min === Infinity) return out;
    out.push(min);
    for (let i = 0; i < nonEmpty.length; i++) {
      const list = nonEmpty[i]!;
      if (cursors[i]! < list.length && list[cursors[i]!] === min) cursors[i]!++;
    }
  }
}

function jsonRoundTrip(value: unknown): unknown {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

/** The highest settled `(transactionId, sequence)` among the given records. */
export function cursorOf(events: readonly RecordedEvent<string, unknown>[]): Cursor | null {
  let best: RecordedEvent<string, unknown> | undefined;
  for (const e of events) if (e.settled && (!best || compareCursor(e, best) > 0)) best = e;
  return best ? { transactionId: best.transactionId, sequence: best.sequence } : null;
}

function splitUniqueKey(key: string): [string, string, string] {
  const first = key.indexOf("|");
  const second = key.indexOf("|", first + 1);
  return [key.slice(0, first), key.slice(first + 1, second), key.slice(second + 1)];
}

/** Whether a store rings a doorbell after a commit (`onCommitted`). */
export function isWakeStore(store: EventStore): store is EventStore & WakeStore {
  return typeof (store as Partial<WakeStore>).onCommitted === "function";
}

/** Whether a store can push appended events in-process (`onAppended`). */
export function isLiveStore(store: EventStore): store is EventStore & LiveStore {
  return typeof (store as Partial<LiveStore>).onAppended === "function";
}
