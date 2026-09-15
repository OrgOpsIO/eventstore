import pg from "pg";
import type { Pool, PoolClient } from "pg";
import { EventStoreError, PolicyViolationError, TransientError, UniqueViolationError, UsageError } from "../errors.js";
import { uuidv7 } from "../ids.js";
import {
  assertOmittable,
  compareCursor,
  conditionLockKeys,
  eventLockKeys,
  fromPayload,
  globalLockKey,
  installLockKey,
  normaliseOptions,
  toPayload,
  trimData,
} from "../query.js";
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
  StatisticsStore,
  StoreSchema,
  TypeStatistics,
} from "../types.js";
import {
  APPEND_SIGNATURE,
  DEFAULT_SCHEMA,
  SCOPE_FUNCTION_NAME,
  SCOPE_PROBES,
  appendFunctionBaseName,
  appendFunctionDdl,
  appendFunctionFingerprint,
  appendFunctionName,
  compileQuery,
  compileStatistics,
  ddlStatements,
  grantStatements,
  idempotencyIndexDdl,
  idempotencyIndexName,
  indexName,
  policyExpression,
  policyName,
  qualified,
  quoteIdent,
  quoteLiteral,
  scopeFn,
  scopeFunctionDdl,
  scopeFunctionFingerprint,
  scopeIndexDdl,
  scopeIndexName,
  scopeRebuildStatements,
  scopeStatisticsDdl,
  scopeStatisticsName,
  tableDdl,
  tableRef,
  targetOf,
  uniqueIndexes,
  uniquePathSegmentsSql,
  versionSpec,
  type Target,
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
  /**
   * Connection string, or an existing `pg.Pool` you own — then `close()` leaves it alone and
   * neither the timeouts nor `search_path` are applied (recorded in `warnings()`).
   */
  readonly connection: string | Pool;
  readonly schema: StoreSchema;
  /** `"auto"` (default) installs table, functions and indexes on first use; `"none"` never runs DDL. */
  readonly install?: "auto" | "none";
  readonly table?: string;
  /** Postgres namespace the table and functions live in (NOT the event schema). Default `public`. */
  readonly schemaName?: string;
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
  /** `lock_timeout` for the install transaction (it waits for DDL locks on a busy table). Default 60 000 ms. */
  readonly installLockTimeoutMs?: number;
  /** Largest batch one append may carry. Default 10 000. */
  readonly maxBatchSize?: number;
  /**
   * Most advisory locks one append may take. Beyond it the append takes the exclusive global
   * lock instead (correct, coarse) — the shared lock table is sized by `max_locks_per_transaction`
   * and a single oversized append would exhaust it for every backend. Default 512.
   */
  readonly maxLockKeys?: number;
  /**
   * When the installed `es_scope()` body or this table's scope indexes are stale (fingerprint
   * mismatch), rebuild the function, the scope indexes, the idempotency index and the statistics
   * inline during install. Blocks writes for the duration on a large table. Default `false`: the
   * installer throws the statements to run by hand instead.
   */
  readonly rebuildScopeIndexes?: boolean;
}

const DEFAULT_TIMEOUTS: Required<PoolTimeouts> = { statementMs: 30_000, lockMs: 10_000, idleInTransactionMs: 30_000 };
/** Deadlock victim, serialization failure, lock not available, statement timeout, out of shared memory (lock table). */
const TRANSIENT_SQLSTATES = new Set(["40P01", "40001", "55P03", "57014", "53200"]);

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

interface StatisticsRow {
  event_type: string;
  cnt: string;
  bytes: string;
  last_seq: string;
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

/** The advisory locks one append takes, as passed to the append function. */
export interface LockPlan {
  readonly keys: string[];
  readonly modes: boolean[];
  readonly globalKey: string;
  readonly exclusiveGlobal: boolean;
  /** `true` when the plan exceeded `maxLockKeys` and collapsed to the exclusive global lock. */
  readonly collapsed: boolean;
}

/**
 * The PostgreSQL store: one `events` table, `es_scope()` expression indexes, `es_append_if_v4`
 * for atomic conditional appends. Every statement is schema-qualified.
 */
export class PostgresStore implements EventStore, StatisticsStore {
  readonly schema: StoreSchema;
  readonly table: string;
  /** Postgres namespace of the table and functions. */
  readonly schemaName: string;
  private readonly target: Target;
  private readonly pool: Pool;
  private readonly ownsPool: boolean;
  private readonly installMode: "auto" | "none";
  private readonly ddlOptions: { adhocQueries?: boolean; rls?: boolean; tenantScopeKey?: string; grantExecuteTo?: readonly string[]; typeIndex?: boolean };
  private readonly uniqueByIndex: Map<string, { type: string; path: string }>;
  private readonly idempotencyIndex: string;
  private readonly fn: string;
  private readonly maxBatchSize: number;
  private readonly maxLockKeys: number;
  private readonly installLockTimeoutMs: number;
  private readonly rebuildScopeIndexes: boolean;
  private installed: Promise<void> | undefined;
  private readonly warningList: string[] = [];
  /** How often an append collapsed its lock plan to the global lock because it exceeded `maxLockKeys`. */
  lockPlanCollapses = 0;

  constructor(options: CreatePostgresStoreOptions) {
    this.schema = options.schema;
    this.table = options.table ?? "events";
    this.schemaName = options.schemaName ?? DEFAULT_SCHEMA;
    this.target = targetOf({ schemaName: this.schemaName, table: this.table });
    this.installMode = options.install ?? "auto";
    this.maxBatchSize = options.maxBatchSize ?? 10_000;
    this.maxLockKeys = options.maxLockKeys ?? 512;
    this.installLockTimeoutMs = options.installLockTimeoutMs ?? 60_000;
    this.rebuildScopeIndexes = options.rebuildScopeIndexes ?? false;
    if (typeof options.connection === "string") {
      const timeouts = { ...DEFAULT_TIMEOUTS, ...(options.timeouts ?? {}) };
      // session defaults travel in the libpq `options` parameter: no extra round trip, no
      // query racing the first checkout (pg deprecates client.query() inside the connect hook)
      const sessionOptions = [
        `-c statement_timeout=${Math.floor(timeouts.statementMs)}`,
        `-c lock_timeout=${Math.floor(timeouts.lockMs)}`,
        `-c idle_in_transaction_session_timeout=${Math.floor(timeouts.idleInTransactionMs)}`,
        `-c search_path=${this.schemaName}`,
      ].join(" ");
      this.pool = new pg.Pool({ connectionString: options.connection, max: options.poolSize ?? 10, options: sessionOptions });
      this.ownsPool = true;
    } else {
      this.pool = options.connection;
      this.ownsPool = false;
      this.warningList.push(
        "external pool: statement_timeout, lock_timeout, idle_in_transaction_session_timeout and search_path are NOT applied — set them on the pool yourself",
      );
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
    this.fn = qualified(this.schemaName, appendFunctionName(this.table));
  }

  /** Non-fatal findings of the constructor and the install (external pool, refused comments, collapsed lock plans). */
  warnings(): readonly string[] {
    return this.warningList;
  }

  /** The DDL this store installs (or would install with `install: "none"`). */
  get schemaSql(): string {
    return ddlStatements(this.schema, { table: this.table, schemaName: this.schemaName, ...this.ddlOptions })
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
      // One DDL transaction: the install lock is transaction-scoped (cannot leak behind a
      // transaction pooler); statement_timeout is lifted (an index build on a large table must
      // not be killed after 30 s and retried forever) but lock_timeout stays bounded — the
      // transaction holds every DDL lock it takes until COMMIT.
      await client.query("BEGIN");
      try {
        await client.query("SET LOCAL statement_timeout = 0");
        await client.query(`SET LOCAL lock_timeout = ${Math.floor(this.installLockTimeoutMs)}`);
        await client.query("SELECT pg_advisory_xact_lock($1::bigint)", [installLockKey(this.schema.lockSalt).toString()]);
        await this.installUnderLock(client);
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK").catch(() => undefined);
        throw err;
      }
    } finally {
      client.release();
    }
  }

  /** The scope indexes of this table plus, when tenant-keyed, the idempotency index: everything built on `es_scope`. */
  private scopeDependentIndexes(): string[] {
    const names = this.schema.scopeKeys.map((key) => scopeIndexName(this.table, key));
    if (this.ddlOptions.tenantScopeKey) names.push(this.idempotencyIndex);
    return names;
  }

  /**
   * Table and indexes are `IF NOT EXISTS`. `es_scope` and every index built on it carry a
   * fingerprint comment; the gate compares THIS table's index comments (the artefacts that are
   * actually stale), not only the schema-global function comment. An index or a function that
   * does not match is never applied silently: the installer throws the rebuild statements or,
   * with `rebuildScopeIndexes: true`, rebuilds inline. A function without a comment is accepted
   * only if it answers the behavioural probes exactly. Nothing is dropped on a routine boot.
   */
  private async installUnderLock(client: PoolClient): Promise<void> {
    const target = this.target;
    const t = tableRef(target);
    const opts = { table: this.table, schemaName: this.schemaName, ...this.ddlOptions };
    const scopeFp = scopeFunctionFingerprint(this.schemaName);
    const scopeSig = `${scopeFn(this.schemaName)}(jsonb, text)`;

    if (this.schemaName !== DEFAULT_SCHEMA) await client.query(`CREATE SCHEMA IF NOT EXISTS ${quoteIdent(this.schemaName)}`);
    await client.query(tableDdl(target));

    // ── es_scope: missing → create; commented ours → ok; uncommented → probe; else stale
    const fnComment = await functionComment(client, scopeSig);
    let functionOk = false;
    if (fnComment === null) {
      await client.query(scopeFunctionDdl(this.schemaName));
      await this.comment(client, `FUNCTION ${scopeSig}`, scopeFp, true);
      functionOk = true;
    } else if (fnComment === scopeFp) {
      functionOk = true;
    } else if (await this.scopeProbesPass(client)) {
      // no comment (pre-fingerprint build) or a foreign comment, but our semantics: adopt it and
      // re-stamp (a refused comment is a warning here — the probes verified the body)
      await this.comment(client, `FUNCTION ${scopeSig}`, scopeFp, false);
      functionOk = true;
    }

    // ── this table's indexes built on es_scope: stale unless their comment is ours
    const staleIndexes: string[] = [];
    const uncommented: string[] = [];
    for (const name of this.scopeDependentIndexes()) {
      const c = await indexComment(client, this.schemaName, name);
      if (c === null) continue; // not created yet
      if (c === scopeFp) continue;
      if (c === "" && functionOk) uncommented.push(name);
      else staleIndexes.push(name);
    }

    if (!functionOk || staleIndexes.length > 0) {
      if (!this.rebuildScopeIndexes) {
        throw new EventStoreError(
          `eventstore/postgres: ${functionOk ? "" : "the installed es_scope() is not this SDK's; "}${
            staleIndexes.length > 0 ? `these indexes of ${t} were built on a different es_scope body: ${staleIndexes.join(", ")}; ` : ""
          }their inlined expression no longer matches. Run these statements in a quiet window (or pass rebuildScopeIndexes: true to do it inline, blocking writes):\n` +
            scopeRebuildStatements(this.schema, target)
              .map((x) => (x.startsWith("--") ? x : `${x};`))
              .join("\n"),
        );
      }
      // inline rebuild inside the install transaction: other sessions see the old objects until COMMIT
      if (!functionOk) {
        await client.query(scopeFunctionDdl(this.schemaName));
        await this.comment(client, `FUNCTION ${scopeSig}`, scopeFp, true);
        functionOk = true;
      }
      const toRebuild = functionOk && staleIndexes.length === 0 ? [] : this.scopeDependentIndexes();
      for (const name of new Set([...toRebuild, ...staleIndexes])) {
        await client.query(`DROP INDEX IF EXISTS ${qualified(this.schemaName, name)}`);
      }
      for (const key of this.schema.scopeKeys) await client.query(`DROP STATISTICS IF EXISTS ${qualified(this.schemaName, scopeStatisticsName(this.table, key))}`);
    }

    if (this.ddlOptions.typeIndex) {
      await client.query(`CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(this.table, "type", "seq"))} ON ${t} (event_type, sequence_number)`);
    }
    await client.query(`CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(this.table, "xid", "seq"))} ON ${t} (transaction_id, sequence_number)`);
    for (const key of this.schema.scopeKeys) {
      await client.query(scopeIndexDdl(target, key));
      await client.query(scopeStatisticsDdl(target, key));
    }
    for (const u of uniqueIndexes(this.schema, this.table)) {
      const expr = `(payload #>> ${uniquePathSegmentsSql(u.path)})`;
      await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdent(u.name)} ON ${t} (${expr}) WHERE event_type = ${quoteLiteral(u.type)} AND ${expr} IS NOT NULL`);
    }
    const tenantKey = this.ddlOptions.tenantScopeKey;
    await client.query(idempotencyIndexDdl(target, tenantKey));
    // fingerprint every index built on es_scope (newly created ones and uncommented adopted ones)
    for (const name of this.scopeDependentIndexes()) {
      const c = await indexComment(client, this.schemaName, name);
      if (c !== scopeFp) await this.comment(client, `INDEX ${qualified(this.schemaName, name)}`, scopeFp, !uncommented.includes(name));
    }
    if (this.ddlOptions.adhocQueries) {
      await client.query(`CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(this.table, "payload", "gin"))} ON ${t} USING GIN (payload jsonb_path_ops)`);
    }

    // ── append function: ours by exact signature; every other overload/version is dropped
    const fnName = appendFunctionName(this.table);
    const ourSig = `${qualified(this.schemaName, fnName)}${APPEND_SIGNATURE}`;
    const appendFp = appendFunctionFingerprint(target);
    if ((await functionComment(client, ourSig)) !== appendFp) {
      await client.query(appendFunctionDdl(target));
      await this.comment(client, `FUNCTION ${ourSig}`, appendFp, true);
    }
    for (const statement of grantStatements(target, opts)) await client.query(statement);
    const others = await client.query<{ sig: string }>(
      `SELECT p.oid::regprocedure::text AS sig FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE n.nspname = $1 AND p.proname LIKE $2 || '%' AND p.oid <> to_regprocedure($3)`,
      [this.schemaName, appendFunctionBaseName(this.table), ourSig],
    );
    for (const row of others.rows) await client.query(`DROP FUNCTION IF EXISTS ${row.sig}`);

    // ── row-level security: ALTER only when needed, policy recreated when USING or WITH CHECK drifted
    if (this.ddlOptions.rls) {
      const key = tenantKey as string;
      const policy = policyName(this.table);
      const expr = policyExpression(this.schemaName, key);
      const flags = await client.query<{ rls: boolean; force: boolean }>(
        "SELECT c.relrowsecurity AS rls, c.relforcerowsecurity AS force FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = $1 AND n.nspname = $2",
        [this.table, this.schemaName],
      );
      if (!flags.rows[0]?.rls) await client.query(`ALTER TABLE ${t} ENABLE ROW LEVEL SECURITY`);
      if (!flags.rows[0]?.force) await client.query(`ALTER TABLE ${t} FORCE ROW LEVEL SECURITY`);
      const existing = await client.query<{ qual: string | null; with_check: string | null }>(
        "SELECT qual, with_check FROM pg_policies WHERE schemaname = $1 AND tablename = $2 AND policyname = $3",
        [this.schemaName, this.table, policy],
      );
      const row = existing.rows[0];
      const expected = normalisePolicyExpr(expr, this.schemaName);
      const upToDate =
        row !== undefined &&
        row.qual !== null &&
        row.with_check !== null &&
        normalisePolicyExpr(row.qual, this.schemaName) === expected &&
        normalisePolicyExpr(row.with_check, this.schemaName) === expected;
      if (!upToDate) {
        if (row !== undefined) await client.query(`DROP POLICY IF EXISTS ${quoteIdent(policy)} ON ${t}`);
        await client.query(`CREATE POLICY ${quoteIdent(policy)} ON ${t} USING (${expr}) WITH CHECK (${expr})`);
      }
    }
  }

  private async scopeProbesPass(client: PoolClient): Promise<boolean> {
    for (const probe of SCOPE_PROBES) {
      const r = await client.query<{ v: string | null }>(`SELECT ${scopeFn(this.schemaName)}($1::jsonb, $2) AS v`, [probe.payload, probe.key]);
      if ((r.rows[0]?.v ?? null) !== probe.expected) return false;
    }
    return true;
  }

  /**
   * `COMMENT ON` needs ownership. `fatal`: a refused comment aborts the install (the gate would
   * otherwise fail open next time); otherwise it is recorded in `warnings()` — used only when the
   * object was verified equivalent by other means (probes, or a verified function).
   */
  private async comment(client: PoolClient, target: string, fingerprint: string, fatal: boolean): Promise<void> {
    await client.query("SAVEPOINT es_comment");
    try {
      await client.query(`COMMENT ON ${target} IS ${quoteLiteral(fingerprint)}`);
      await client.query("RELEASE SAVEPOINT es_comment");
    } catch (err) {
      await client.query("ROLLBACK TO SAVEPOINT es_comment");
      if ((err as { code?: string }).code !== "42501") throw err;
      const message = `could not fingerprint ${target}: this role does not own it`;
      if (fatal) throw new EventStoreError(`eventstore/postgres: ${message} — install as the owner, or run printSchemaSql() as the owner and use install: "none"`, { cause: err });
      this.warningList.push(message);
    }
  }

  private assertRlsSession(): void {
    if (this.ddlOptions.rls) {
      throw new UsageError(
        "eventstore/postgres: rls is enabled — call withTenant(tenantId) (or es.forTenant()) for reads, writes and subscriptions; the root store has no tenant session",
      );
    }
  }

  async query(query: Query, options: QueryOptions = {}): Promise<QueryResult> {
    this.assertRlsSession();
    await this.ensureInstalled();
    return this.queryWith(this.pool, query, options, false);
  }

  private async queryWith(runner: Runner, query: Query, options: QueryOptions, inTenantSession: boolean): Promise<QueryResult> {
    normaliseOptions(options);
    const compiled = compileQuery(this.target, query, options, this.schema);
    let result: pg.QueryResult<Row>;
    try {
      result = await runner.query<Row>(compiled.sql, compiled.params);
    } catch (err) {
      throw this.translate(err, inTenantSession);
    }
    const first = result.rows[0];
    const contextVersion = first ? toSafeInt(first.context_version) : 0;
    const events: RecordedEvent[] = [];
    const byFilter: RecordedEvent[][] = Array.from({ length: compiled.filterCount }, () => []);
    let lastReturned = 0;
    let settledCursor: Cursor | null = null;
    for (const row of result.rows) {
      if (row.seq === null || row.event_type === null || row.payload === null) continue;
      const event = this.toRecorded(row, options.omit);
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

  private toRecorded(row: Row, omit?: readonly string[]): RecordedEvent {
    const type = row.event_type as string;
    const idKey = this.schema.idKeyOf(type);
    if (omit) assertOmittable(omit, idKey, type);
    const raw = row.payload as Record<string, unknown>;
    // id and scopes come from the RAW payload — what es_scope() matched and locked on; only
    // `data` goes through `upcast` (an upcast may reshape data, never scope keys or the id)
    const { id, scopes } = fromPayload(raw, idKey);
    const upcast = fromPayload(this.schema.upcast(type, raw), idKey);
    // SQL already stripped the keys; trim again after `upcast` so a reshaped payload cannot bring one back
    const data = omit ? trimData(upcast.data, omit) : upcast.data;
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

  /** Count, stored bytes and last sequence per event type, over the rows a query matches — no payload is fetched. */
  async statistics(query: Query = {}): Promise<readonly TypeStatistics[]> {
    this.assertRlsSession();
    await this.ensureInstalled();
    return this.statisticsWith(this.pool, query, false);
  }

  private async statisticsWith(runner: Runner, query: Query, inTenantSession: boolean): Promise<readonly TypeStatistics[]> {
    const compiled = compileStatistics(this.target, query);
    let result: pg.QueryResult<StatisticsRow>;
    try {
      result = await runner.query<StatisticsRow>(compiled.sql, compiled.params);
    } catch (err) {
      throw this.translate(err, inTenantSession);
    }
    return result.rows.map((r) => ({ type: r.event_type, count: toSafeInt(r.cnt), bytes: toSafeInt(r.bytes), lastSequence: toSafeInt(r.last_seq) }));
  }

  async append(events: readonly NewEvent[]): Promise<AppendResult> {
    this.assertRlsSession();
    await this.ensureInstalled();
    const outcome = await this.writeWith(this.pool, events, null, false);
    if (!outcome.ok) throw new EventStoreError("eventstore/postgres: unconditional append reported a conflict");
    return outcome.appended;
  }

  async appendIf(events: readonly NewEvent[], ctx: ContextHandle): Promise<AppendIfOutcome> {
    this.assertRlsSession();
    await this.ensureInstalled();
    return this.writeWith(this.pool, events, ctx, false);
  }

  private async writeWith(runner: Runner, events: readonly NewEvent[], ctx: ContextHandle | null, inTenantSession: boolean): Promise<AppendIfOutcome> {
    if (events.length === 0) throw new Error("eventstore: append needs at least one event");
    if (events.length > this.maxBatchSize) {
      throw new EventStoreError(`eventstore/postgres: batch of ${events.length} events exceeds maxBatchSize ${this.maxBatchSize}`);
    }
    const now = Date.now();
    const prepared = events.map((raw) => {
      const event = this.schema.validate(raw);
      return { ...event, id: event.id ?? uuidv7(now) };
    });
    const plan = this.lockPlan(prepared, ctx);
    const spec = ctx ? versionSpec(ctx.query, this.schema) : null;
    const params = [
      plan.keys,
      plan.modes,
      plan.globalKey,
      plan.exclusiveGlobal,
      spec ? JSON.stringify(spec) : null,
      ctx ? String(ctx.version) : "0",
      prepared.map((e) => e.type),
      prepared.map((e) => JSON.stringify(toPayload(e, this.schema.idKeyOf(e.type)))),
      prepared.map((e) => JSON.stringify(e.metadata ?? {})),
    ];
    const sql = `SELECT ok, actual::text AS actual, first_seq::text AS first_seq, last_seq::text AS last_seq, cnt
FROM ${this.fn}($1::bigint[], $2::boolean[], $3::bigint, $4::boolean, $5::jsonb, $6::bigint, $7::text[], $8::jsonb[], $9::jsonb[])`;
    let row: AppendRow | undefined;
    try {
      const result = await runner.query<AppendRow>(sql, params);
      row = result.rows[0];
    } catch (err) {
      throw this.translate(err, inTenantSession);
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
   * The advisory locks one append takes: one list sorted by key with a parallel mode list
   * (true = exclusive). Exclusive: the condition's scope pairs, tenant-only guards (on the
   * tenant key), the events' own pairs. Shared: the events' tenant stamps. Exclusive wins on a
   * duplicate. The global key is salted per deployment like every other key. A plan with more
   * than `maxLockKeys` keys collapses to the exclusive global lock.
   */
  lockPlan(events: readonly (NewEvent & { id: string })[], ctx: ContextHandle | null): LockPlan {
    const condition = ctx ? conditionLockKeys(ctx.query, this.schema) : { keys: [] as bigint[], exclusiveGlobal: false, exclusiveTenants: [] as bigint[] };
    const eventLocks = eventLockKeys(events, this.schema);
    const modeOf = new Map<bigint, boolean>();
    for (const k of eventLocks.sharedTenants) modeOf.set(k, false);
    for (const k of [...condition.keys, ...condition.exclusiveTenants, ...eventLocks.keys]) modeOf.set(k, true);
    const globalKey = globalLockKey(this.schema.lockSalt).toString();
    if (modeOf.size > this.maxLockKeys) {
      this.lockPlanCollapses++;
      if (this.lockPlanCollapses === 1) {
        this.warningList.push(`an append needed ${modeOf.size} advisory locks (> maxLockKeys ${this.maxLockKeys}) and took the global lock instead`);
      }
      return { keys: [], modes: [], globalKey, exclusiveGlobal: true, collapsed: true };
    }
    const sorted = [...modeOf.keys()].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    return {
      keys: sorted.map((k) => k.toString()),
      modes: sorted.map((k) => modeOf.get(k)!),
      globalKey,
      exclusiveGlobal: condition.exclusiveGlobal,
      collapsed: false,
    };
  }

  /**
   * Map driver errors to the SDK's vocabulary. Unique violations never carry the offending
   * value (pg puts it into `detail`), transient conditions become `TransientError`.
   */
  private translate(err: unknown, inTenantSession: boolean): unknown {
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
    if (e.code === "42501") {
      if (this.ddlOptions.rls && inTenantSession) return new PolicyViolationError(e.message ?? "row-level security policy violated", { cause: err });
      return new EventStoreError(
        `eventstore/postgres: permission denied — the application role needs SELECT, INSERT on the table, USAGE on its sequence and EXECUTE on the append function (grantExecuteTo): ${e.message ?? ""}`,
        { cause: err },
      );
    }
    if (e.code === "ES001" || e.code === "ES002") return new UsageError(`eventstore/postgres: ${e.message ?? e.code}`, { cause: err });
    return err;
  }

  /**
   * A view that runs every statement in a transaction with `app.current_tenant` set — what
   * `rls: true` needs. The view narrows nothing by itself; combine it with the core tenant view
   * (`scopedToTenant`) for query narrowing and event stamping. Under `rls` this is also the
   * store to hand to `subscribe`/`on`: the root store refuses to be polled.
   */
  withTenant(tenantId: string): EventStore & StatisticsStore {
    const inTenant = <T>(fn: (runner: Runner) => Promise<T>): Promise<T> =>
      this.ensureInstalled().then(() => withTenantSession(this.pool, tenantId, (client) => fn(client)));
    return {
      query: (query, options = {}) => inTenant((r) => this.queryWith(r, query, options, true)),
      statistics: (query = {}) => inTenant((r) => this.statisticsWith(r, query, true)),
      append: async (events) => {
        const outcome = await inTenant((r) => this.writeWith(r, events, null, true));
        if (!outcome.ok) throw new EventStoreError("eventstore/postgres: unconditional append reported a conflict");
        return outcome.appended;
      },
      appendIf: (events, ctx) => inTenant((r) => this.writeWith(r, events, ctx, true)),
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

/** The DDL as text, for DBAs and `install: "none"` deployments (the unconditional form: no drift gate). */
export function printSchemaSql(
  schema: StoreSchema,
  options: Omit<CreatePostgresStoreOptions, "connection" | "schema" | "install" | "poolSize" | "timeouts" | "maxBatchSize" | "maxLockKeys" | "installLockTimeoutMs" | "rebuildScopeIndexes"> = {},
): string {
  const header =
    "-- @orgops/eventstore schema. Idempotent, but NOT gated: CREATE OR REPLACE FUNCTION es_scope over existing scope\n" +
    "-- indexes needs those indexes rebuilt (see scopeRebuildStatements). Run as the table owner.";
  return [
    header,
    ...ddlStatements(schema, { ...options, table: options.table ?? "events", tenantScopeKey: options.tenantScopeKey ?? schema.tenantScopeKey }).map((s) => `${s};`),
  ].join("\n\n");
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

/** `null`: no such function (exact signature); `""`: no comment; else the comment. */
async function functionComment(client: PoolClient, qualifiedSignature: string): Promise<string | null> {
  const r = await client.query<{ oid: string | null; fp: string | null }>(
    "SELECT to_regprocedure($1)::text AS oid, obj_description(to_regprocedure($1), 'pg_proc') AS fp",
    [qualifiedSignature],
  );
  const row = r.rows[0];
  if (!row || row.oid === null) return null;
  return row.fp ?? "";
}

/** `null`: no such index; `""`: no comment; else the comment. */
async function indexComment(client: PoolClient, schemaName: string, name: string): Promise<string | null> {
  const r = await client.query<{ oid: string | null; fp: string | null }>(
    "SELECT to_regclass($1)::text AS oid, obj_description(to_regclass($1), 'pg_class') AS fp",
    [`${quoteIdent(schemaName)}.${quoteIdent(name)}`],
  );
  const row = r.rows[0];
  if (!row || row.oid === null) return null;
  return row.fp ?? "";
}

/** Normalise a policy expression for comparison: no whitespace, parentheses, quotes, casts or schema prefixes. */
function normalisePolicyExpr(expr: string, schemaName: string): string {
  return expr
    .toLowerCase()
    .replace(/::text/g, "")
    .replace(/\s+|\(|\)|"/g, "")
    .replace(new RegExp(`${schemaName.toLowerCase()}\\.`, "g"), "")
    .replace(/public\./g, "");
}

function toSafeInt(value: string | number): number {
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isSafeInteger(n)) throw new EventStoreError(`eventstore/postgres: sequence ${value} is not a safe integer`);
  return n;
}
