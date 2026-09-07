import { UniqueViolationError } from "./errors.js";
import { uuidv7 } from "./ids.js";
import {
  compareCursor,
  conditionLockKeys,
  filtersOf,
  fromPayload,
  matchesFilter,
  normaliseOptions,
  toPayload,
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
  NewEvent,
  Query,
  QueryOptions,
  QueryResult,
  RecordedEvent,
  StoreSchema,
} from "./types.js";

export type AppendedListener = (events: readonly RecordedEvent[]) => void;

/** Optional capability: a store that can push appended events to in-process listeners. */
export interface LiveStore {
  onAppended(listener: AppendedListener): () => void;
}

export interface MemoryStoreOptions {
  /** Declared events/scopes; default: an empty, non-strict schema. */
  readonly schema?: StoreSchema;
  /** Source of `recordedAt`. Default: wall clock. */
  readonly clock?: () => Date;
}

/** A stored row: the wire payload, so `upcast` runs on read exactly as in Postgres. */
interface Row {
  readonly sequence: number;
  readonly type: string;
  readonly payload: Record<string, unknown>;
  readonly metadata: Record<string, unknown>;
  readonly recordedAt: Date;
  readonly transactionId: string;
}

/**
 * The semantic reference implementation. Every behaviour of the Postgres store is specified
 * by this one and checked by the shared conformance suite. Single-threaded, so `appendIf` is
 * atomic by construction; all events are settled; one transaction id per append batch.
 */
export class MemoryStore implements EventStore, LiveStore {
  readonly schema: StoreSchema;
  private readonly clock: () => Date;
  private readonly rows: Row[] = [];
  private readonly listeners = new Set<AppendedListener>();
  private readonly uniqueSeen = new Map<string, Set<string>>();
  private readonly idempotencyKeys = new Set<string>();

  constructor(options: MemoryStoreOptions = {}) {
    this.schema = options.schema ?? emptySchema();
    this.clock = options.clock ?? (() => new Date());
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
    for (const row of this.rows) {
      const record = this.toRecorded(row);
      const matched = filters.map((f) => matchesFilter(record, f, this.schema));
      if (!matched.some(Boolean)) continue;
      if (record.sequence > contextVersion) contextVersion = record.sequence;
      if (this.visible(record, options)) {
        visible.push(record);
        hits.set(record, matched);
      }
    }
    let events = visible;
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

  private visible(record: RecordedEvent, options: QueryOptions): boolean {
    if (options.after !== undefined && record.sequence <= options.after) return false;
    if (options.settledOnly && !record.settled) return false;
    if (options.cursor && record.settled && compareCursor(record, options.cursor) <= 0) return false;
    return true;
  }

  private toRecorded(row: Row): RecordedEvent {
    const idKey = this.schema.idKeyOf(row.type);
    const { id, data, scopes } = fromPayload(this.schema.upcast(row.type, row.payload), idKey);
    return {
      type: row.type,
      data,
      id: id ?? `~${row.sequence}`,
      scopes,
      metadata: row.metadata,
      sequence: row.sequence,
      recordedAt: row.recordedAt,
      transactionId: row.transactionId,
      settled: true,
    };
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
      const idKey = this.schema.idKeyOf(event.type);
      // JSON round trip: the reference store must forget what JSONB forgets (Dates → strings,
      // `undefined` → missing, NaN → null) so tests against memory behave like production.
      const payload = jsonRoundTrip(toPayload({ ...event, id }, idKey)) as Record<string, unknown>;
      const metadata = jsonRoundTrip(event.metadata ?? {}) as Record<string, unknown>;
      return { event, payload, metadata };
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
    const transactionId = String(first);
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
    this.rows.push(...rows);
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
    return result;
  }

  onAppended(listener: AppendedListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async close(): Promise<void> {
    this.listeners.clear();
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

export function isLiveStore(store: EventStore): store is EventStore & LiveStore {
  return typeof (store as Partial<LiveStore>).onAppended === "function";
}
