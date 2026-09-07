import pg from "pg";
import type { Pool, PoolClient } from "pg";
import { EventStoreError, UniqueViolationError } from "../errors.js";
import { fnv1a64, uuidv7 } from "../ids.js";
import { GLOBAL_LOCK_KEY, compareCursor, conditionLockKeys, eventLockKeys, filtersOf, fromPayload, normaliseOptions, toPayload } from "../query.js";
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
} from "../types.js";
import {
  appendFunctionName,
  compileFilters,
  compileQuery,
  compileVersionSql,
  ddlStatements,
  idempotencyIndexName,
  quoteIdent,
  uniqueIndexes,
} from "./sql.js";

export interface CreatePostgresStoreOptions {
  /** Connection string, or an existing `pg.Pool` you own (then `close()` leaves it alone). */
  readonly connection: string | Pool;
  readonly schema: StoreSchema;
  /** `"auto"` (default): install table/function/indexes on first use. `"none"`: never touch the schema. */
  /** `"auto"` (default) installs table, function and indexes on first use; `"none"` never runs DDL. */
  readonly install?: "auto" | "none";
  readonly table?: string;
  readonly adhocQueries?: boolean;
  readonly rls?: boolean;
  /** Needed for `rls: true`: the scope key that identifies the tenant. */
  readonly tenantScopeKey?: string;
  readonly poolSize?: number;
  readonly clock?: () => Date;
}

const INSTALL_LOCK_KEY = fnv1a64("@orgops/eventstore:install");

interface Row {
  context_version: string;
  seq: string | null;
  event_type: string | null;
  payload: Record<string, unknown> | null;
  metadata: Record<string, unknown> | null;
  recorded_at: Date | null;
  xid: string | null;
  settled: boolean | null;
  hits: boolean[] | null;
}

export class PostgresStore implements EventStore {
  readonly schema: StoreSchema;
  readonly table: string;
  private readonly pool: Pool;
  private readonly ownsPool: boolean;
  private readonly install: "auto" | "none";
  private readonly ddl: string[];
  private readonly uniqueByIndex: Map<string, { type: string; path: string }>;
  private readonly idempotencyIndex: string;
  private readonly fn: string;
  private installed: Promise<void> | undefined;

  constructor(options: CreatePostgresStoreOptions) {
    this.schema = options.schema;
    this.table = options.table ?? "events";
    this.install = options.install ?? "auto";
    if (typeof options.connection === "string") {
      this.pool = new pg.Pool({ connectionString: options.connection, max: options.poolSize ?? 10 });
      this.ownsPool = true;
    } else {
      this.pool = options.connection;
      this.ownsPool = false;
    }
    this.ddl = ddlStatements(this.schema, {
      table: this.table,
      adhocQueries: options.adhocQueries,
      rls: options.rls,
      tenantScopeKey: options.tenantScopeKey,
    });
    this.uniqueByIndex = new Map(uniqueIndexes(this.schema, this.table).map((u) => [u.name, { type: u.type, path: u.path }]));
    this.idempotencyIndex = idempotencyIndexName(this.table);
    this.fn = quoteIdent(appendFunctionName(this.table));
  }

  /** The DDL this store installs (or would install with `install: "none"`). */
  get schemaSql(): string {
    return this.ddl.map((s) => `${s};`).join("\n\n");
  }

  /** Install table, function and indexes. Idempotent; serialised across processes by an advisory lock. */
  ensureInstalled(): Promise<void> {
    if (!this.installed) {
      this.installed = this.install().catch((err) => {
        this.installed = undefined;
        throw err;
      });
    }
    return this.installed;
  }

  private async install(): Promise<void> {
    if (this.install === "none") return;
    const client = await this.pool.connect();
    try {
      await client.query("SELECT pg_advisory_lock($1::bigint)", [INSTALL_LOCK_KEY.toString()]);
      try {
        for (const statement of this.ddl) await client.query(statement);
      } finally {
        await client.query("SELECT pg_advisory_unlock($1::bigint)", [INSTALL_LOCK_KEY.toString()]);
      }
    } finally {
      client.release();
    }
  }

  async query(query: Query, options: QueryOptions = {}): Promise<QueryResult> {
    await this.ensureInstalled();
    normaliseOptions(options);
    const compiled = compileQuery(this.table, query, options);
    const result = await this.pool.query<Row>(compiled.sql, compiled.params);
    const first = result.rows[0];
    const contextVersion = first ? toSafeInt(first.context_version) : 0;
    const events: RecordedEvent[] = [];
    const filterCount = filtersOf(query).length;
    const byFilter: RecordedEvent[][] = Array.from({ length: filterCount }, () => []);
    let lastReturned = 0;
    let settledCursor: Cursor | null = null;
    for (const row of result.rows) {
      if (row.seq === null || row.event_type === null || row.payload === null) continue;
      const event = this.toRecorded(row);
      events.push(event);
      if (event.sequence > lastReturned) lastReturned = event.sequence;
      if (event.settled && (!settledCursor || compareCursor(event, settledCursor) > 0)) {
        settledCursor = { transactionId: event.transactionId, sequence: event.sequence };
      }
      (row.hits ?? []).forEach((hit, i) => {
        if (hit) byFilter[i]?.push(event);
      });
    }
    return { events, byFilter, lastReturned, contextVersion, ctx: { query, version: contextVersion }, settledCursor };
  }

  private toRecorded(row: Row): RecordedEvent {
    const type = row.event_type as string;
    const idKey = this.schema.idKeyOf(type);
    const payload = this.schema.upcast(type, row.payload as Record<string, unknown>);
    const { id, data, scopes } = fromPayload(payload, idKey);
    const sequence = toSafeInt(row.seq as string);
    return {
      type,
      data,
      id: id ?? `~${sequence}`,
      scopes,
      metadata: (row.metadata ?? {}) as RecordedEvent["metadata"],
      sequence,
      recordedAt: row.recorded_at instanceof Date ? row.recorded_at : new Date(String(row.recorded_at)),
      transactionId: String(row.xid),
      settled: row.settled === true,
    };
  }

  async append(events: readonly NewEvent[]): Promise<AppendResult> {
    const outcome = await this.write(events, null);
    if (!outcome.ok) throw new EventStoreError("eventstore/postgres: unconditional append reported a conflict");
    return outcome.appended;
  }

  async appendIf(events: readonly NewEvent[], ctx: ContextHandle): Promise<AppendIfOutcome> {
    return this.write(events, ctx);
  }

  private async write(events: readonly NewEvent[], ctx: ContextHandle | null): Promise<AppendIfOutcome> {
    if (events.length === 0) throw new Error("eventstore: append needs at least one event");
    await this.ensureInstalled();
    const now = Date.now();
    const prepared = events.map((raw) => {
      const event = this.schema.validate(raw);
      return { ...event, id: event.id ?? uuidv7(now) };
    });
    const condition = ctx ? conditionLockKeys(ctx.query, this.schema) : { keys: [] as bigint[], exclusiveGlobal: false, exclusiveTenants: [] as bigint[] };
    const eventLocks = eventLockKeys(prepared, this.schema);
    const byValue = (a: string, b: string) => (BigInt(a) < BigInt(b) ? -1 : BigInt(a) > BigInt(b) ? 1 : 0);
    // exclusive: the condition's scope pairs, tenant-only guards (exclusive on the tenant), the events' own pairs
    const lockKeys = [...new Set([...condition.keys, ...condition.exclusiveTenants, ...eventLocks.keys].map((k) => k.toString()))].sort(byValue);
    // shared: the tenant stamp of the events (a tenant-only guard excludes them; scope-level guards ignore them)
    const sharedKeys = [...new Set(eventLocks.sharedTenants.map((k) => k.toString()))].filter((k) => !lockKeys.includes(k)).sort(byValue);
    const compiled = ctx ? compileVersionSql(this.table, ctx.query) : null;
    const params = [
      lockKeys,
      sharedKeys,
      GLOBAL_LOCK_KEY.toString(),
      condition.exclusiveGlobal,
      compiled ? compiled.sql : null,
      compiled ? compiled.texts : [],
      compiled ? compiled.jsons : [],
      ctx ? String(ctx.version) : "0",
      prepared.map((e) => e.type),
      prepared.map((e) => JSON.stringify(toPayload(e, this.schema.idKeyOf(e.type)))),
      prepared.map((e) => JSON.stringify(e.metadata ?? {})),
    ];
    const sql = `SELECT ok, actual::text AS actual, first_seq::text AS first_seq, last_seq::text AS last_seq, cnt
FROM ${this.fn}($1::bigint[], $2::bigint[], $3::bigint, $4::boolean, $5::text, $6::text[], $7::jsonb[], $8::bigint, $9::text[], $10::jsonb[], $11::jsonb[])`;
    let row: { ok: boolean; actual: string; first_seq: string | null; last_seq: string | null; cnt: number } | undefined;
    try {
      const result = await this.pool.query(sql, params);
      row = result.rows[0];
    } catch (err) {
      throw this.translate(err);
    }
    if (!row) throw new EventStoreError("eventstore/postgres: append function returned no row");
    if (!row.ok) {
      return { ok: false, conflict: { expected: ctx ? ctx.version : 0, actual: toSafeInt(row.actual) } };
    }
    return {
      ok: true,
      appended: { first: toSafeInt(row.first_seq as string), last: toSafeInt(row.last_seq as string), count: Number(row.cnt) },
    };
  }

  private translate(err: unknown): unknown {
    const e = err as { code?: string; constraint?: string; message?: string; detail?: string };
    if (e && e.code === "23505") {
      const constraint = e.constraint ?? "";
      if (constraint === this.idempotencyIndex) {
        return new UniqueViolationError({ idempotencyKey: true });
      }
      const unique = this.uniqueByIndex.get(constraint);
      if (unique) return new UniqueViolationError({ type: unique.type, path: unique.path }, { cause: err });
      return new UniqueViolationError({ path: constraint }, { cause: err });
    }
    return err;
  }

  /** Raw access for tests and tooling (e.g. `EXPLAIN`). */
  async withClient<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    await this.ensureInstalled();
    const client = await this.pool.connect();
    try {
      return await fn(client);
    } finally {
      client.release();
    }
  }

  async close(): Promise<void> {
    if (this.ownsPool) await this.pool.end();
  }
}

export async function createPostgresStore(options: CreatePostgresStoreOptions): Promise<EventStore> {
  const store = new PostgresStore(options);
  await store.ensureInstalled();
  return store;
}

/** The DDL as text, for DBAs and `install: "none"` deployments. */
export function printSchemaSql(
  schema: StoreSchema,
  options: Omit<CreatePostgresStoreOptions, "connection" | "schema" | "install" | "poolSize" | "clock"> = {},
): string {
  return ddlStatements(schema, { table: options.table ?? "events", ...options })
    .map((s) => `${s};`)
    .join("\n\n");
}

/** Run `fn` in a transaction with `app.current_tenant` set (for `rls: true`). */
export async function withTenantSession<T>(pool: Pool, tenantId: string, fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    try {
      await client.query("SELECT set_config('app.current_tenant', $1, true)", [tenantId]);
      const result = await fn(client);
      await client.query("COMMIT");
      return result;
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    }
  } finally {
    client.release();
  }
}

function toSafeInt(value: string | number): number {
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isSafeInteger(n)) throw new EventStoreError(`eventstore/postgres: sequence ${value} is not a safe integer`);
  return n;
}

function extractKey(detail: string | undefined): string | undefined {
  const m = detail?.match(/=\((.*)\) already exists/);
  return m?.[1];
}
