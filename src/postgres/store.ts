import pg from "pg";
import type { Pool, PoolClient } from "pg";
import { EventStoreError, PolicyViolationError, TransientError, UniqueViolationError } from "../errors.js";
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
  SCOPE_FUNCTION_NAME,
  appendFunctionDdl,
  appendFunctionFingerprint,
  appendFunctionName,
  compileQuery,
  compileVersionSql,
  ddlStatements,
  grantStatements,
  idempotencyIndexName,
  indexName,
  quoteIdent,
  quoteLiteral,
  scopeFunctionDdl,
  scopeFunctionFingerprint,
  scopeIndexDdl,
  scopeIndexName,
  scopeStatisticsDdl,
  tableDdl,
  uniqueIndexes,
  uniquePathSegmentsSql,
} from "./sql.js";

/** Per-connection Postgres timeouts in milliseconds. */
export interface PoolTimeouts {
  /** `statement_timeout` per connection. Default 30 000 ms. */
  readonly statementMs?: number;
  /** `lock_timeout` per connection — bounds how long an append waits for an advisory lock. Default 10 000 ms. */
  readonly lockMs?: number;
  /** `idle_in_transaction_session_timeout` per connection. Default 30 000 ms. */
  readonly idleInTransactionMs?: number;
}

/** Options of the Postgres store. */
export interface CreatePostgresStoreOptions {
  /** Connection string, or an existing `pg.Pool` you own (then `close()` leaves it alone and `timeouts` are not applied). */
  readonly connection: string | Pool;
  readonly schema: StoreSchema;
  /** `"auto"` (default) installs table, functions and indexes on first use; `"none"` never runs DDL. */
  readonly install?: "auto" | "none";
  readonly table?: string;
  /** GIN `jsonb_path_ops` index on the payload for ad-hoc `where` queries. */
  readonly adhocQueries?: boolean;
  /** Row-level security on the tenant scope. Needs `tenantScopeKey` (or `schema.tenantScopeKey`) and `withTenant()` for every statement. */
  readonly rls?: boolean;
  /** The scope key that identifies the tenant (defaults to `schema.tenantScopeKey`). */
  readonly tenantScopeKey?: string;
  /** Roles granted EXECUTE on the append function besides the installing role. */
  readonly grantExecuteTo?: readonly string[];
  /** Opt-in `(event_type, sequence_number)` index for type-only reads. Default `false`. */
  readonly typeIndex?: boolean;
  readonly poolSize?: number;
  readonly timeouts?: PoolTimeouts;
  /** Largest batch one append may carry. Default 10 000. */
  readonly maxBatchSize?: number;
}

const INSTALL_LOCK_KEY = fnv1a64("@orgops/eventstore:install");
const DEFAULT_TIMEOUTS: Required<PoolTimeouts> = { statementMs: 30_000, lockMs: 10_000, idleInTransactionMs: 30_000 };
const TRANSIENT_SQLSTATES = new Set(["40P01", "40001", "55P03", "57014"]);

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

interface AppendRow {
  ok: boolean;
  actual: string;
  first_seq: string | null;
  last_seq: string | null;
  cnt: number;
}

/** Anything that can run a parameterised statement: the pool, or a checked-out client inside a transaction. */
interface Runner {
  query<T extends pg.QueryResultRow = pg.QueryResultRow>(sql: string, params?: unknown[]): Promise<pg.QueryResult<T>>;
}

/** The PostgreSQL store: one `events` table, `es_scope()` expression indexes, `es_append_if_v2` for atomic conditional appends. */
export class PostgresStore implements EventStore {
  readonly schema: StoreSchema;
  readonly table: string;
  private readonly pool: Pool;
  private readonly ownsPool: boolean;
  private readonly installMode: "auto" | "none";
  private readonly ddlOptions: { adhocQueries?: boolean; rls?: boolean; tenantScopeKey?: string; grantExecuteTo?: readonly string[]; typeIndex?: boolean };
  private readonly uniqueByIndex: Map<string, { type: string; path: string }>;
  private readonly idempotencyIndex: string;
  private readonly fn: string;
  private readonly maxBatchSize: number;
  private installed: Promise<void> | undefined;

  constructor(options: CreatePostgresStoreOptions) {
    this.schema = options.schema;
    this.table = options.table ?? "events";
    this.installMode = options.install ?? "auto";
    this.maxBatchSize = options.maxBatchSize ?? 10_000;
    if (typeof options.connection === "string") {
      const timeouts = { ...DEFAULT_TIMEOUTS, ...(options.timeouts ?? {}) };
      // session defaults travel in the libpq `options` parameter: no extra round trip, no
      // query racing the first checkout (pg deprecates client.query() inside the connect hook)
      const sessionOptions = [
        `-c statement_timeout=${Math.floor(timeouts.statementMs)}`,
        `-c lock_timeout=${Math.floor(timeouts.lockMs)}`,
        `-c idle_in_transaction_session_timeout=${Math.floor(timeouts.idleInTransactionMs)}`,
      ].join(" ");
      this.pool = new pg.Pool({ connectionString: options.connection, max: options.poolSize ?? 10, options: sessionOptions });
      this.ownsPool = true;
    } else {
      this.pool = options.connection;
      this.ownsPool = false;
    }
    this.ddlOptions = {
      adhocQueries: options.adhocQueries,
      rls: options.rls,
      tenantScopeKey: options.tenantScopeKey ?? this.schema.tenantScopeKey,
      grantExecuteTo: options.grantExecuteTo,
      typeIndex: options.typeIndex,
    };
    if (options.rls && !this.ddlOptions.tenantScopeKey) throw new Error("eventstore/postgres: rls: true needs tenantScopeKey");
    this.uniqueByIndex = new Map(uniqueIndexes(this.schema, this.table).map((u) => [u.name, { type: u.type, path: u.path }]));
    this.idempotencyIndex = idempotencyIndexName(this.table);
    this.fn = quoteIdent(appendFunctionName(this.table));
  }

  /** The DDL this store installs (or would install with `install: "none"`), for the `public` schema. */
  get schemaSql(): string {
    return ddlStatements(this.schema, { table: this.table, ...this.ddlOptions })
      .map((s) => `${s};`)
      .join("\n\n");
  }

  /** Install table, functions and indexes. Idempotent; serialised across processes by an advisory lock. */
  ensureInstalled(): Promise<void> {
    if (!this.installed) {
      this.installed = this.runInstall().catch((err) => {
        this.installed = undefined;
        throw err;
      });
    }
    return this.installed;
  }

  private async runInstall(): Promise<void> {
    if (this.installMode === "none") return;
    const client = await this.pool.connect();
    try {
      await client.query("SELECT pg_advisory_lock($1::bigint)", [INSTALL_LOCK_KEY.toString()]);
      try {
        await this.installUnderLock(client);
      } finally {
        await client.query("SELECT pg_advisory_unlock($1::bigint)", [INSTALL_LOCK_KEY.toString()]);
      }
    } finally {
      client.release();
    }
  }

  /**
   * Table and indexes are `IF NOT EXISTS`. Functions are replaced only when their fingerprint
   * (a comment on the function) differs from ours — and when `es_scope` changed, every scope
   * index built on it is reindexed. Nothing is dropped on a routine boot.
   */
  private async installUnderLock(client: PoolClient): Promise<void> {
    const schemaName = (await client.query<{ s: string }>("SELECT current_schema() AS s")).rows[0]?.s ?? "public";
    const t = quoteIdent(this.table);
    const opts = { table: this.table, schemaName, ...this.ddlOptions };

    await client.query(tableDdl(this.table));

    const scopeFp = scopeFunctionFingerprint(schemaName);
    const currentScopeFp = await functionFingerprint(client, SCOPE_FUNCTION_NAME, schemaName);
    const scopeChanged = currentScopeFp !== null && currentScopeFp !== scopeFp;
    if (currentScopeFp !== scopeFp) {
      await client.query(scopeFunctionDdl(schemaName));
      await client.query(`COMMENT ON FUNCTION ${SCOPE_FUNCTION_NAME}(jsonb, text) IS ${quoteLiteral(scopeFp)}`);
    }

    if (this.ddlOptions.typeIndex) {
      await client.query(`CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(this.table, "type", "seq"))} ON ${t} (event_type, sequence_number)`);
    }
    await client.query(`CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(this.table, "xid", "seq"))} ON ${t} (transaction_id, sequence_number)`);
    if (scopeChanged) {
      // es_scope is inlined into index expressions at creation: a changed body means the stored
      // expression is stale, and only a rebuild (not REINDEX) picks the new one up.
      for (const key of this.schema.scopeKeys) await client.query(`DROP INDEX IF EXISTS ${quoteIdent(scopeIndexName(this.table, key))}`);
    }
    for (const key of this.schema.scopeKeys) {
      await client.query(scopeIndexDdl(this.table, key));
      await client.query(scopeStatisticsDdl(this.table, key));
    }
    for (const u of uniqueIndexes(this.schema, this.table)) {
      const expr = `(payload #>> ${uniquePathSegmentsSql(u.path)})`;
      await client.query(
        `CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdent(u.name)} ON ${t} (${expr}) WHERE event_type = ${quoteLiteral(u.type)} AND ${expr} IS NOT NULL`,
      );
    }
    const tenantKey = this.ddlOptions.tenantScopeKey;
    if (tenantKey) {
      await client.query(
        `CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdent(this.idempotencyIndex)} ON ${t} ((${SCOPE_FUNCTION_NAME}(payload, ${quoteLiteral(tenantKey)})), (metadata->>'idempotencyKey')) WHERE metadata ? 'idempotencyKey'`,
      );
    } else {
      await client.query(
        `CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdent(this.idempotencyIndex)} ON ${t} ((metadata->>'idempotencyKey')) WHERE metadata ? 'idempotencyKey'`,
      );
    }
    if (this.ddlOptions.adhocQueries) {
      await client.query(`CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(this.table, "payload", "gin"))} ON ${t} USING GIN (payload jsonb_path_ops)`);
    }

    const fnName = appendFunctionName(this.table);
    const appendFp = appendFunctionFingerprint(this.table, schemaName);
    if ((await functionFingerprint(client, fnName, schemaName)) !== appendFp) {
      await client.query(appendFunctionDdl(this.table, schemaName));
      await client.query(
        `COMMENT ON FUNCTION ${quoteIdent(fnName)}(bigint[], bigint[], bigint, boolean, text, text[], jsonb[], bigint, text[], jsonb[], jsonb[]) IS ${quoteLiteral(appendFp)}`,
      );
    }
    for (const statement of grantStatements(this.table, opts)) await client.query(statement);
    // one-off: the unversioned v1 function nobody calls any more
    const legacy = await client.query<{ sig: string }>(
      "SELECT p.oid::regprocedure::text AS sig FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname = $1 AND n.nspname = $2",
      [appendFunctionName(this.table, 1), schemaName],
    );
    for (const row of legacy.rows) await client.query(`DROP FUNCTION IF EXISTS ${row.sig}`);

    if (this.ddlOptions.rls) {
      const key = tenantKey as string;
      const policy = indexName(this.table, "tenant", "isolation");
      const expr = `${SCOPE_FUNCTION_NAME}(payload, ${quoteLiteral(key)}) = current_setting('app.current_tenant', true)`;
      await client.query(`ALTER TABLE ${t} ENABLE ROW LEVEL SECURITY`);
      await client.query(`ALTER TABLE ${t} FORCE ROW LEVEL SECURITY`);
      const existing = await client.query("SELECT 1 FROM pg_policies WHERE schemaname = $1 AND tablename = $2 AND policyname = $3", [schemaName, this.table, policy]);
      if (existing.rowCount === 0) {
        await client.query(`CREATE POLICY ${quoteIdent(policy)} ON ${t} USING (${expr}) WITH CHECK (${expr})`);
      }
    }
  }

  async query(query: Query, options: QueryOptions = {}): Promise<QueryResult> {
    await this.ensureInstalled();
    return this.queryWith(this.pool, query, options);
  }

  private async queryWith(runner: Runner, query: Query, options: QueryOptions): Promise<QueryResult> {
    normaliseOptions(options);
    const compiled = compileQuery(this.table, query, options, this.schema);
    let result: pg.QueryResult<Row>;
    try {
      result = await runner.query<Row>(compiled.sql, compiled.params);
    } catch (err) {
      throw this.translate(err);
    }
    const first = result.rows[0];
    const contextVersion = first ? toSafeInt(first.context_version) : 0;
    const events: RecordedEvent[] = [];
    const byFilter: RecordedEvent[][] = Array.from({ length: compiled.filterCount }, () => []);
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
      if (compiled.filterCount === 1) byFilter[0]!.push(event);
      else (row.hits ?? []).forEach((hit, i) => hit && byFilter[i]?.push(event));
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
    await this.ensureInstalled();
    const outcome = await this.writeWith(this.pool, events, null);
    if (!outcome.ok) throw new EventStoreError("eventstore/postgres: unconditional append reported a conflict");
    return outcome.appended;
  }

  async appendIf(events: readonly NewEvent[], ctx: ContextHandle): Promise<AppendIfOutcome> {
    await this.ensureInstalled();
    return this.writeWith(this.pool, events, ctx);
  }

  private async writeWith(runner: Runner, events: readonly NewEvent[], ctx: ContextHandle | null): Promise<AppendIfOutcome> {
    if (events.length === 0) throw new Error("eventstore: append needs at least one event");
    if (events.length > this.maxBatchSize) {
      throw new EventStoreError(`eventstore/postgres: batch of ${events.length} events exceeds maxBatchSize ${this.maxBatchSize}`);
    }
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
    const compiled = ctx ? compileVersionSql(this.table, ctx.query, undefined, this.schema) : null;
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
    let row: AppendRow | undefined;
    try {
      const result = await runner.query<AppendRow>(sql, params);
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

  /**
   * Map driver errors to the SDK's vocabulary. Unique violations never carry the offending
   * value (pg puts it into `detail`), transient conditions become `TransientError`.
   */
  private translate(err: unknown): unknown {
    const e = err as { code?: string; constraint?: string; message?: string };
    if (!e || typeof e !== "object") return err;
    if (e.code === "23505") {
      const constraint = e.constraint ?? "";
      if (constraint === this.idempotencyIndex) return new UniqueViolationError({ idempotencyKey: true });
      const unique = this.uniqueByIndex.get(constraint);
      if (unique) return new UniqueViolationError({ type: unique.type, path: unique.path });
      return new UniqueViolationError({ path: constraint });
    }
    if (e.code && TRANSIENT_SQLSTATES.has(e.code)) return new TransientError(e.code, { cause: err });
    if (e.code === "42501") return new PolicyViolationError(e.message ?? "row-level security policy violated", { cause: err });
    if (e.code === "ES001") return new EventStoreError(`eventstore/postgres: ${e.message ?? "READ COMMITTED required"}`, { cause: err });
    return err;
  }

  /**
   * A view that runs every statement in a transaction with `app.current_tenant` set — what
   * `rls: true` needs. The view narrows nothing by itself; combine it with the core tenant view
   * (`scopedToTenant`) for query narrowing and event stamping.
   */
  withTenant(tenantId: string): EventStore {
    const inTenant = <T>(fn: (runner: Runner) => Promise<T>): Promise<T> =>
      this.ensureInstalled().then(() => withTenantSession(this.pool, tenantId, (client) => fn(client)));
    return {
      query: (query, options = {}) => inTenant((r) => this.queryWith(r, query, options)),
      append: async (events) => {
        const outcome = await inTenant((r) => this.writeWith(r, events, null));
        if (!outcome.ok) throw new EventStoreError("eventstore/postgres: unconditional append reported a conflict");
        return outcome.appended;
      },
      appendIf: (events, ctx) => inTenant((r) => this.writeWith(r, events, ctx)),
      close: async () => undefined,
    };
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

/** Create a Postgres store (installs the schema lazily unless `install: "none"`). */
export async function createPostgresStore(options: CreatePostgresStoreOptions): Promise<PostgresStore> {
  const store = new PostgresStore(options);
  await store.ensureInstalled();
  return store;
}

/** The DDL as text, for DBAs and `install: "none"` deployments. */
export function printSchemaSql(
  schema: StoreSchema,
  options: Omit<CreatePostgresStoreOptions, "connection" | "schema" | "install" | "poolSize" | "timeouts" | "maxBatchSize"> & { schemaName?: string } = {},
): string {
  return ddlStatements(schema, { ...options, table: options.table ?? "events", tenantScopeKey: options.tenantScopeKey ?? schema.tenantScopeKey })
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
      await client.query("ROLLBACK").catch(() => undefined);
      throw err;
    }
  } finally {
    client.release();
  }
}

async function functionFingerprint(client: PoolClient, name: string, schemaName: string): Promise<string | null> {
  const r = await client.query<{ fp: string | null }>(
    "SELECT obj_description(p.oid, 'pg_proc') AS fp FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname = $1 AND n.nspname = $2 LIMIT 1",
    [name, schemaName],
  );
  const row = r.rows[0];
  if (!row) return null;
  return row.fp ?? "";
}

function toSafeInt(value: string | number): number {
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isSafeInteger(n)) throw new EventStoreError(`eventstore/postgres: sequence ${value} is not a safe integer`);
  return n;
}
