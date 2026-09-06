import { UniqueViolationError } from "./errors.js";
import { uuidv7 } from "./ids.js";
import { conditionLockKeys, filtersOf, matchesFilter, toPayload, uniquePathSegments, valueAtPath } from "./query.js";
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
  readonly schema?: StoreSchema;
  readonly clock?: () => Date;
}

/**
 * The semantic reference implementation. Every behaviour of the Postgres store is specified
 * by this one and checked by the shared conformance suite. Single-threaded, so `appendIf` is
 * atomic by construction; all events are settled.
 */
export class MemoryStore implements EventStore, LiveStore {
  readonly schema: StoreSchema;
  private readonly clock: () => Date;
  private readonly records: RecordedEvent[] = [];
  private readonly listeners = new Set<AppendedListener>();
  private readonly uniqueSeen = new Map<string, Set<string>>();
  private readonly idempotencyKeys = new Set<string>();

  constructor(options: MemoryStoreOptions = {}) {
    this.schema = options.schema ?? emptySchema();
    this.clock = options.clock ?? (() => new Date());
  }

  /** All records, in order. Handy in tests. */
  get events(): readonly RecordedEvent[] {
    return this.records;
  }

  async query(query: Query, options: QueryOptions = {}): Promise<QueryResult> {
    return this.queryNow(query, options);
  }

  queryNow(query: Query, options: QueryOptions = {}): QueryResult {
    const filters = filtersOf(query);
    const byFilter: RecordedEvent[][] = filters.map(() => []);
    const all: RecordedEvent[] = [];
    let contextVersion = 0;
    for (const record of this.records) {
      let matched = false;
      filters.forEach((filter, i) => {
        if (matchesFilter(record, filter, this.schema)) {
          matched = true;
          if (this.visible(record, options)) byFilter[i]!.push(record);
        }
      });
      if (!matched) continue;
      if (record.sequence > contextVersion) contextVersion = record.sequence;
      if (this.visible(record, options)) all.push(record);
    }
    let events = all;
    if (options.order === "desc") events = [...events].reverse();
    if (options.limit !== undefined) events = events.slice(0, options.limit);
    const returnedSet = new Set(events);
    const lastReturned = events.reduce((m, e) => Math.max(m, e.sequence), 0);
    const settledCursor = cursorOf(events);
    return {
      events,
      byFilter: byFilter.map((list) => list.filter((e) => returnedSet.has(e))),
      lastReturned,
      contextVersion,
      settledCursor,
    };
  }

  private visible(record: RecordedEvent, options: QueryOptions): boolean {
    if (options.after !== undefined && record.sequence <= options.after) return false;
    if (options.cursor && record.sequence <= options.cursor.sequence) return false;
    return true;
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
      const payload = toPayload({ ...event, id }, idKey);
      return { event, id, payload };
    });
    // Check constraints for the whole batch before committing anything (all or nothing).
    const batchUniques = new Set<string>();
    const batchIdem = new Set<string>();
    for (const { event, payload } of prepared) {
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
      const idem = event.metadata?.idempotencyKey;
      if (typeof idem === "string") {
        if (this.idempotencyKeys.has(idem) || batchIdem.has(idem)) throw new UniqueViolationError({ idempotencyKey: idem });
        batchIdem.add(idem);
      }
    }
    const first = this.records.length + 1;
    const recorded: RecordedEvent[] = prepared.map(({ event, id }, i) => ({
      type: event.type,
      data: event.data,
      id,
      scopes: { ...(event.scopes ?? {}) },
      metadata: { ...(event.metadata ?? {}) },
      sequence: first + i,
      recordedAt: now,
      transactionId: String(first + i),
      settled: true,
    }));
    for (const key of batchUniques) {
      const [type, path, serialized] = splitUniqueKey(key);
      let set = this.uniqueSeen.get(`${type}|${path}`);
      if (!set) this.uniqueSeen.set(`${type}|${path}`, (set = new Set()));
      set.add(serialized);
    }
    for (const k of batchIdem) this.idempotencyKeys.add(k);
    this.records.push(...recorded);
    const result = { first, last: first + recorded.length - 1, count: recorded.length };
    for (const listener of this.listeners) {
      try {
        listener(recorded);
      } catch {
        // a listener must never fail an append
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

function cursorOf(events: readonly RecordedEvent[]): Cursor | null {
  let best: RecordedEvent | undefined;
  for (const e of events) if (e.settled && (!best || e.sequence > best.sequence)) best = e;
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
