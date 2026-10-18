import { createHash } from "node:crypto";
import pg from "pg";
import type { Pool, PoolClient } from "pg";
import { EventStoreError, PolicyViolationError, TenantMismatchError, TransientError, UniqueViolationError, UsageError } from "../errors.js";
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
  FoldStateKey,
  FoldStateStore,
  NewEvent,
  Query,
  QueryOptions,
  QueryResult,
  RecordedEvent,
  StatisticsStore,
  StoredFoldState,
  StoreSchema,
  TypeStatistics,
  WakeStore,
} from "../types.js";
import {
  ADOPT_COLUMNS_SQL,
  ADOPT_INDEXES_SQL,
  ADOPT_POLICIES_SQL,
  ADOPT_TRIGGERS_SQL,
  ADOPT_WRITE_GRANTS_SQL,
  ADOPT_UNIQUE_SQL,
  adoptStatements,
  legacyXid,
  planAdoption,
  type AdoptionPlan,
  type AdoptOptions,
  type CatalogColumn,
  type CatalogTable,
} from "./adopt.js";
import type { ReadModelStore } from "../modelstore.js";
import { CommitListener, type OpenedConnection } from "./live.js";
import { PostgresReadModels } from "./models.js";
import {
  APPEND_SIGNATURE,
  DEFAULT_SCHEMA,
  SCOPE_BUILTIN_FUNCTIONS,
  SCOPE_FUNCTION_NAME,
  SCOPE_LEAKPROOF_MISSING_SQL,
  SCOPE_PROBES,
  appendFunctionBaseName,
  appendFunctionDdl,
  appendFunctionFingerprint,
  appendFunctionName,
  compileQuery,
  compileStatistics,
  ddlStatements,
  FOLD_STATE_POLICY_EXPRESSION,
  foldStatePolicyName,
  foldStateTableDdl,
  foldStateTargetOf,
  grantStatements,
  idempotencyIndexDdl,
  idempotencyIndexName,
  notifyChannel,
  notifyFunctionDdl,
  notifyFunctionFingerprint,
  notifyFunctionName,
  notifyTriggerDdl,
  notifyTriggerName,
  indexName,
  policyExpression,
  policyName,
  qualified,
  readModelIndexDdl,
  checkProcessorRole,
  processorPolicies,
  processorPolicyDdl,
  processorPolicyNames,
  readModelPolicyName,
  readModelStatePolicyName,
  readModelStateTableDdl,
  readModelTableDdl,
  readModelTargetsOf,
  type ReadModelTargets,
  quoteIdent,
  quoteLiteral,
  scopeFn,
  scopeFunctionDdl,
  scopeFunctionFingerprint,
  scopeIndexDdl,
  scopeIndexName,
  scopeIndexRowsSql,
  scopeLeakproofStatements,
  scopeRebuildStatements,
  scopeStatisticsKeys,
  scopeStatisticsDdl,
  scopeStatisticsName,
  tableDdl,
  tableRef,
  targetOf,
  uniqueIndexes,
  uniquePathSegmentsSql,
  versionSpec,
  type ScopeStatisticsPolicy,
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
  /**
   * Which scope keys get a statistics object. `"all"` (default) keeps one per key. With
   * `{ minIndexRows }` the installer reads the rows per scope index and keeps objects only where
   * the index holds at least that many (`DEFAULT_SCOPE_STATISTICS_MIN_ROWS` is the measured
   * choice), dropping the others; `exclude` names keys whose expression has statistics from a
   * non-partial index of your own. Every object costs planning time in every statement on the
   * table — see `scopeStatisticsKeys`. A key that grows past the limit gets its object at the
   * next install; run `ANALYZE` after that, a new object is empty.
   */
  readonly scopeStatistics?: ScopeStatisticsPolicy;
  /**
   * The commit doorbell. `true` (or an object) installs a statement trigger that NOTIFYs the
   * highest sequence of every insert on commit — no type, scope, tenant or payload — and gives
   * the store `onCommitted`, served by ONE extra connection per store that LISTENs outside the
   * pool. Subscriptions and `es.watch` then wake at once instead of polling.
   *
   * `connection`: where to LISTEN — needed behind PgBouncer in transaction mode (LISTEN does not
   * survive it) and when `connection` above is a pool you own (otherwise one of its connections
   * is taken for good). Default: off. The cost: Postgres serialises the commits of notifying
   * transactions on one lock, which matters only at very high write rates; and a listener that
   * stays connected but stops reading fills the notification queue until notifying commits
   * fail — monitor `pg_notification_queue_usage()`.
   */
  readonly live?: boolean | { readonly connection?: string };
  /**
   * Take an existing events table over at install, in place: legacy columns renamed
   * (`columns: { sequence: "id", type: "eventtype" }`), `metadata` and `transaction_id` added,
   * all metadata-only — `payload` is never touched, nothing is dropped. Runs inside the install
   * transaction under the install lock (all or nothing, one process at a time), checks row count
   * and highest sequence before and after, and does nothing on a table already adopted. A table
   * that does not fit (half adopted, `json` instead of `jsonb`, a NOT NULL column the store
   * cannot fill) stops the install before anything changes. See `AdoptOptions`.
   */
  readonly adopt?: AdoptOptions;
  /**
   * Keep persisted fold states in `<table>_fold_state` (installed with the events table, with
   * its own policy under `rls`), and give the store and its tenant sessions `foldStates`: a fold
   * made by `defineFold` then starts a cold process from its stored state and folds only the
   * delta. The application role needs `SELECT, INSERT, UPDATE` on that table. Default `false`:
   * no table, no change.
   */
  readonly persistedFolds?: boolean;
  /**
   * Keep persisted read models (`es.persistedReadModel(def)`) in `<table>_read_model` (one row per
   * model: checkpoint, epoch, revision) and `<table>_read_model_state` (one row per key), installed
   * with the events table, with their own policies under `rls`. A store with a running persisted
   * model holds one extra connection per process for the writer locks. The application role needs
   * `SELECT, INSERT, UPDATE` on both tables. Default `false`: no tables, no change.
   */
  readonly persistedModels?: boolean;
  /**
   * Under `rls` (with `persistedModels`): the database role a cross-tenant processor connects as (a read model
   * defined with `processor: "cross-tenant"`). The installer gives that role three policies — read every tenant's
   * events, read and write every tenant's model rows and state rows — and nothing else; the application role's
   * isolation is unchanged. Pass the same value in every process; the processor's process connects as that role
   * (`install: "none"`), and a store connected as it may read every tenant at its root. Never grant this role to the
   * application role: members inherit its policies. Default: none — no cross-tenant processor under `rls`.
   */
  readonly processorRole?: string;
}

const DEFAULT_TIMEOUTS: Required<PoolTimeouts> = { statementMs: 30_000, lockMs: 10_000, idleInTransactionMs: 30_000 };
/** Deadlock victim, serialization failure, lock not available, statement timeout, out of shared memory (lock table). */
const TRANSIENT_SQLSTATES = new Set(["40P01", "40001", "55P03", "57014", "53200"]);

interface Row {
  context_version: string;
  /** The `xmin` of this statement's own snapshot: the watermark (decision 11). */
  xmin: string;
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
  /** `pg_current_xact_id_if_assigned()` of the append statement; `NULL` when nothing was written. */
  xid: string | null;
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
 * The PostgreSQL store: one `events` table, `es_scope()` expression indexes, `es_append_if_v5`
 * for atomic conditional appends. Every statement is schema-qualified.
 */
export class PostgresStore implements EventStore, StatisticsStore, Partial<WakeStore> {
  readonly schema: StoreSchema;
  readonly table: string;
  /** Postgres namespace of the table and functions. */
  readonly schemaName: string;
  private readonly target: Target;
  private readonly pool: Pool;
  private readonly ownsPool: boolean;
  private readonly installMode: "auto" | "none";
  private readonly ddlOptions: {
    adhocQueries?: boolean;
    rls?: boolean;
    tenantScopeKey?: string;
    grantExecuteTo?: readonly string[];
    typeIndex?: boolean;
    live?: boolean;
    persistedFolds?: boolean;
    persistedModels?: boolean;
    processorRole?: string;
  };
  /** Whether this store is connected as `processorRole` (read once, `current_user`); see `processorSession`. */
  private processorCheck: Promise<boolean> | undefined;
  /** The persisted read models' tables and their lock session; present only with `persistedModels`. */
  private readonly modelTargets: ReadModelTargets | undefined;
  private readonly models: PostgresReadModels | undefined;
  /** @internal The persisted read models' backend on the root store (`persistedModels`). Under `rls` it refuses: use `withTenant(id)`. */
  readonly readModelStore?: ReadModelStore;
  /** The fold-state table; present only with `persistedFolds`. */
  private readonly foldTarget: Target | undefined;
  /** The persisted fold tier's backend on the root store (`persistedFolds`). Under `rls` it refuses: use `withTenant(id).foldStates`. */
  readonly foldStates?: FoldStateStore;
  private readonly uniqueByIndex: Map<string, { type: string; path: string }>;
  private readonly idempotencyIndex: string;
  private readonly fn: string;
  private readonly maxBatchSize: number;
  private readonly maxLockKeys: number;
  private readonly installLockTimeoutMs: number;
  private readonly rebuildScopeIndexes: boolean;
  private readonly scopeStatisticsPolicy: ScopeStatisticsPolicy;
  private readonly adopt: AdoptOptions | undefined;
  private analyzeAfterInstall = false;
  private installed: Promise<void> | undefined;
  private readonly warningList: string[] = [];
  /** How often an append collapsed its lock plan to the global lock because it exceeded `maxLockKeys`. */
  lockPlanCollapses = 0;
  private readonly listener: CommitListener | undefined;
  /** The commit doorbell; present only with `live` (see `WakeStore`). */
  readonly onCommitted?: (listener: (hint: number | null) => void) => () => void;

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
    this.scopeStatisticsPolicy = options.scopeStatistics ?? "all";
    this.adopt = options.adopt;
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
      live: options.live !== undefined && options.live !== false,
      persistedFolds: options.persistedFolds === true,
      persistedModels: options.persistedModels === true,
      ...(options.processorRole !== undefined ? { processorRole: options.processorRole } : {}),
    };
    checkProcessorRole(this.ddlOptions);
    this.modelTargets = options.persistedModels ? readModelTargetsOf(this.target) : undefined;
    this.foldTarget = options.persistedFolds ? foldStateTargetOf(this.target) : undefined;
    if (this.foldTarget) {
      this.foldStates = {
        read: async (key) => {
          this.assertFoldStateSession();
          await this.ensureInstalled();
          return this.readFoldState(this.pool, key.tenant ?? "", key, false);
        },
        write: async (key, stored) => {
          this.assertFoldStateSession();
          await this.ensureInstalled();
          return this.writeFoldState(this.pool, key.tenant ?? "", key, stored, false);
        },
      };
    }
    if (this.ddlOptions.live) {
      const liveConnection = typeof options.live === "object" ? options.live.connection : undefined;
      const direct = liveConnection ?? (typeof options.connection === "string" ? options.connection : undefined);
      const pool = this.pool;
      const open = async (): Promise<OpenedConnection> => {
        if (direct === undefined) {
          const client = await pool.connect();
          return { connection: client, release: () => client.release(true) }; // LISTEN state never goes back into the pool
        }
        const client = new pg.Client({ connectionString: direct, keepAlive: true });
        client.on("error", () => undefined); // the listener's own "error" handler decides; never crash the process
        await client.connect();
        return { connection: client, release: () => void client.end().catch(() => undefined) };
      };
      if (direct === undefined) this.warningList.push("live: no live.connection given with an external pool — the doorbell holds one connection of that pool");
      const listener = new CommitListener(open, notifyChannel(this.target), {
        verify: async () => {
          const found = await this.pool.query("SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = to_regclass($1) AND tgname = $2 AND NOT tgisinternal", [
            `${quoteIdent(this.schemaName)}.${quoteIdent(this.table)}`,
            notifyTriggerName(this.table),
          ]);
          return found.rows.length > 0;
        },
        onMissing: () =>
          this.warningList.push(
            `live: the doorbell trigger ${notifyTriggerName(this.table)} is not installed, so nothing rings — falling back to waking every 500 ms; install with install: "auto" or printSchemaSql(schema, { live: true })`,
          ),
      });
      this.listener = listener;
      this.onCommitted = (l) => listener.add(l);
    }
    if (this.modelTargets) {
      const direct = typeof options.connection === "string" ? options.connection : undefined;
      const pool = this.pool;
      this.models = new PostgresReadModels({
        pool,
        target: this.target,
        targets: this.modelTargets,
        lockSalt: this.schema.lockSalt,
        rls: options.rls === true,
        processorRole: this.ddlOptions.processorRole,
        isProcessor: () => this.processorSession(),
        listener: this.listener,
        // the writer locks' own session, outside the pool: a session lock on a pooled connection would leak to the next borrower
        openLockConnection: async () => {
          if (direct === undefined) {
            const client = await pool.connect();
            client.on("error", () => undefined);
            return { connection: client, release: () => client.release(true) }; // destroyed, never back into the pool
          }
          const client = new pg.Client({ connectionString: direct, keepAlive: true });
          client.on("error", () => undefined); // the lock session's own "error" handler decides; never crash the process
          await client.connect();
          return { connection: client, release: () => void client.end().catch(() => undefined) };
        },
        ensureInstalled: () => this.ensureInstalled(),
        translate: (err, inTenantSession) => this.translate(err, inTenantSession),
      });
      this.readModelStore = this.models.capability(null);
    }
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
      if (this.analyzeAfterInstall) {
        // outside the install transaction: it holds neither the install lock nor a DDL lock, and
        // with large payloads (every scope expression unpacks each sampled one) it can take seconds
        this.analyzeAfterInstall = false;
        try {
          await client.query("BEGIN");
          await client.query("SET LOCAL statement_timeout = 0");
          await client.query(`ANALYZE ${tableRef(this.target)}`);
          await client.query("COMMIT");
        } catch (err) {
          await client.query("ROLLBACK").catch(() => undefined);
          this.warningList.push(`could not ANALYZE ${this.schemaName}.${this.table} after creating statistics objects (${(err as Error).message}) — run it by hand`);
        }
      }
    } finally {
      client.release();
    }
  }

  /**
   * What `adopt` would do to the table right now — read-only, for tooling and DBAs. Uses the
   * store's `adopt` options unless others are given.
   */
  async adoptionPlan(options: AdoptOptions = this.adopt ?? {}): Promise<AdoptionPlan> {
    const client = await this.pool.connect();
    try {
      // read-only, and rolled back: the NULL scans of a large table must not hit statement_timeout
      await client.query("BEGIN");
      try {
        await client.query("SET LOCAL statement_timeout = 0");
        await client.query("SET LOCAL row_security = off");
        const catalog = await this.readCatalog(client, options);
        return planAdoption(catalog?.table ?? null, tableRef(this.target), options, this.adoptContext());
      } finally {
        await client.query("ROLLBACK").catch(() => undefined);
      }
    } finally {
      client.release();
    }
  }

  private adoptContext(): { rls?: boolean; ownPolicy: string; ownPolicies: readonly string[] } {
    // the processor role's read policy is the store's own too (decision 46 §2), whether or not this process names the role
    return { rls: this.ddlOptions.rls, ownPolicy: policyName(this.table), ownPolicies: [processorPolicyNames(this.table).events] };
  }

  private async readCatalog(client: PoolClient, options: AdoptOptions): Promise<{ table: CatalogTable; sequenceColumn: string | undefined } | null> {
    const ref = `${quoteIdent(this.schemaName)}.${quoteIdent(this.table)}`;
    const exists = await client.query<{ present: boolean }>("SELECT to_regclass($1) IS NOT NULL AS present", [ref]);
    if (!exists.rows[0]?.present) return null;
    const columns = (await client.query<{ name: string; type: string; base_type: string; not_null: boolean; filled: boolean; sequenced: boolean }>(ADOPT_COLUMNS_SQL, [ref])).rows.map(
      (r): CatalogColumn => ({ name: r.name, type: r.type, baseType: r.base_type, notNull: r.not_null, filled: r.filled, sequenced: r.sequenced }),
    );
    const policies = (await client.query<{ name: string; permissive: boolean }>(ADOPT_POLICIES_SQL, [ref])).rows;
    const writeGrants = (await client.query<{ grant: string }>(ADOPT_WRITE_GRANTS_SQL, [ref, this.schemaName, this.table])).rows.map((r) => r.grant);
    const unique = (await client.query<{ name: string }>(ADOPT_UNIQUE_SQL, [ref])).rows.map((r) => r.name);
    const indexes = (await client.query<{ name: string }>(ADOPT_INDEXES_SQL, [ref])).rows.map((r) => r.name);
    const triggers = (await client.query<{ name: string }>(ADOPT_TRIGGERS_SQL, [ref])).rows.map((r) => r.name);
    // NULLs matter only in the columns the store will require a value in
    const required = new Set(["sequence_number", "event_type", "payload", "recorded_at", "metadata", "transaction_id", options.columns?.sequence, options.columns?.type, options.columns?.payload, options.columns?.recordedAt, options.columns?.metadata, options.columns?.transactionId]);
    const withNulls: string[] = [];
    for (const column of columns) {
      if (column.notNull || !required.has(column.name)) continue;
      const found = await client.query(`SELECT 1 FROM ${tableRef(this.target)} WHERE ${quoteIdent(column.name)} IS NULL LIMIT 1`);
      if (found.rows.length > 0) withNulls.push(column.name);
    }
    const names = new Set(columns.map((c) => c.name));
    const sequenceColumn = names.has("sequence_number") ? "sequence_number" : options.columns?.sequence && names.has(options.columns.sequence) ? options.columns.sequence : undefined;
    return { table: { columns, uniqueColumns: unique, columnsWithNulls: withNulls, indexes, triggers, policies, writeGrants }, sequenceColumn };
  }

  /** The adoption step of the install: plan, refuse on problems, execute with a before/after check. */
  private async adoptUnderLock(client: PoolClient, options: AdoptOptions): Promise<void> {
    // every read of the adoption sees every row, or fails: under FORCE ROW LEVEL SECURITY a
    // filtered NULL scan or count would silently pass a table the store cannot use
    await client.query("SET LOCAL row_security = off");
    const catalog = await this.readCatalog(client, options);
    const plan = planAdoption(catalog?.table ?? null, tableRef(this.target), options, this.adoptContext());
    await client.query("SET LOCAL row_security = on");
    if (plan.state === "absent") {
      const where = `${this.schemaName}.${this.table}`;
      if (options.mode === "check" || options.requireExisting) {
        throw new EventStoreError(`eventstore/postgres: adopt: there is no table ${where} to adopt — nothing was changed (check table and schemaName)`);
      }
      this.warningList.push(`adopt: there was no table ${where} to adopt; a new, empty one was created — check table and schemaName if one was expected`);
      return;
    }
    if (plan.problems.length > 0) {
      throw new EventStoreError(`eventstore/postgres: cannot adopt ${this.schemaName}.${this.table} — nothing was changed:\n- ${plan.problems.join("\n- ")}`);
    }
    if (plan.state !== "legacy" || !catalog) return;
    if (options.mode === "check") {
      throw new EventStoreError(
        `eventstore/postgres: adopt (mode "check"): ${this.schemaName}.${this.table} needs adopting — nothing was changed. The install would run:\n${plan.statements.map((st) => `  ${st};`).join("\n")}` +
          (plan.notes.length > 0 ? `\nNotes:\n- ${plan.notes.join("\n- ")}` : ""),
      );
    }
    // legacy rows must sort before every transaction that can still commit, or they stay unsettled —
    // invisible to every durable reader — until the xid counter passes them
    const xid = legacyXid(options);
    const below = await client.query<{ ok: boolean }>("SELECT $1::xid8 < pg_snapshot_xmin(pg_current_snapshot()) AS ok", [xid]);
    if (!below.rows[0]?.ok) {
      throw new EventStoreError(`eventstore/postgres: adopt.legacyTransactionId ${xid} is not below the oldest running transaction — legacy rows would sort after new ones; nothing was changed`);
    }
    const t = tableRef(this.target);
    await client.query("SET LOCAL row_security = off");
    const legacySequence = quoteIdent(catalog.sequenceColumn ?? "sequence_number");
    const before = await client.query<{ n: string; last: string | null }>(`SELECT count(*)::text AS n, max(${legacySequence})::text AS last FROM ${t}`);
    for (const statement of plan.statements) await client.query(statement);
    const after = await client.query<{ n: string; last: string | null }>(`SELECT count(*)::text AS n, max(sequence_number)::text AS last FROM ${t}`);
    await client.query("SET LOCAL row_security = on");
    if (before.rows[0]?.n !== after.rows[0]?.n || before.rows[0]?.last !== after.rows[0]?.last) {
      // throwing rolls the whole install transaction back: the table is as it was
      throw new EventStoreError(
        `eventstore/postgres: adopting ${this.schemaName}.${this.table} changed its rows (count ${before.rows[0]?.n} → ${after.rows[0]?.n}, last ${before.rows[0]?.last} → ${after.rows[0]?.last}) — rolled back`,
      );
    }
    this.warningList.push(
      `adopt: took over ${this.schemaName}.${this.table} (${after.rows[0]?.n} rows, last sequence ${after.rows[0]?.last ?? 0}) with ${plan.statements.length} statements`,
      ...plan.notes.map((n) => `adopt: ${n}`),
    );
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
    if (this.adopt) await this.adoptUnderLock(client, this.adopt);
    await client.query(tableDdl(target));
    if (this.foldTarget) await client.query(foldStateTableDdl(this.foldTarget));
    if (this.modelTargets) {
      await client.query(readModelTableDdl(this.modelTargets));
      await client.query(readModelStateTableDdl(this.modelTargets));
      for (const statement of readModelIndexDdl(this.table, this.modelTargets)) await client.query(statement);
    }

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
    for (const key of this.schema.scopeKeys) await client.query(scopeIndexDdl(target, key));
    await this.installScopeStatistics(client);
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

    // ── the commit doorbell (live): the trigger function by fingerprint, the trigger once
    if (this.ddlOptions.live) {
      const notifySig = `${qualified(this.schemaName, notifyFunctionName(this.table))}()`;
      const notifyFp = notifyFunctionFingerprint(target);
      if ((await functionComment(client, notifySig)) !== notifyFp) {
        await client.query(notifyFunctionDdl(target));
        await this.comment(client, `FUNCTION ${notifySig}`, notifyFp, true);
      }
      const trigger = await client.query("SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = to_regclass($1) AND tgname = $2 AND NOT tgisinternal", [
        `${quoteIdent(this.schemaName)}.${quoteIdent(this.table)}`,
        notifyTriggerName(this.table),
      ]);
      if (trigger.rows.length === 0) await client.query(notifyTriggerDdl(target));
    }

    // ── row-level security: ALTER only when needed, policy recreated when USING or WITH CHECK drifted
    if (this.ddlOptions.rls) {
      await this.installPolicy(client, this.target, policyName(this.table), policyExpression(this.schemaName, tenantKey as string));
      // the fold states are isolated like the events: same session variable, and never the empty tenant
      if (this.foldTarget) await this.installPolicy(client, this.foldTarget, foldStatePolicyName(this.table), FOLD_STATE_POLICY_EXPRESSION);
      // the model rows and state rows likewise
      if (this.modelTargets) {
        await this.installPolicy(client, this.modelTargets.model, readModelPolicyName(this.table), FOLD_STATE_POLICY_EXPRESSION);
        await this.installPolicy(client, this.modelTargets.state, readModelStatePolicyName(this.table), FOLD_STATE_POLICY_EXPRESSION);
        await this.installProcessorPolicies(client, this.modelTargets);
      }
      await this.markScopeFunctionsLeakproof(client);
    }
  }

  /**
   * The processor role's three policies (decision 46 §2), recreated when their role, command or expressions drifted.
   * Without `processorRole` existing ones stay — a process installing without the option must not break a running
   * processor — and are named in `warnings()`.
   */
  private async installProcessorPolicies(client: PoolClient, modelTargets: ReadModelTargets): Promise<void> {
    const role = this.ddlOptions.processorRole;
    const policies = processorPolicies(this.target, modelTargets);
    const found = await client.query<{ tablename: string; policyname: string; cmd: string; roles: string[]; qual: string | null; with_check: string | null }>(
      "SELECT tablename, policyname, cmd, roles::text[] AS roles, qual, with_check FROM pg_policies WHERE schemaname = $1 AND policyname = ANY($2::text[]) ORDER BY policyname COLLATE \"C\"",
      [this.schemaName, policies.map((p) => p.name)],
    );
    if (role === undefined) {
      if (found.rows.length > 0) {
        this.warningList.push(
          `rls: the processor policies ${found.rows.map((r) => `${r.policyname} (to ${r.roles.join(", ")})`).join(", ")} exist but this store has no processorRole — they are left in place for a running cross-tenant processor; drop them deliberately if none runs`,
        );
      }
      return;
    }
    for (const policy of policies) {
      const row = found.rows.find((r) => r.tablename === policy.target.table && r.policyname === policy.name);
      const same = (actual: string | null, expected: string | null) =>
        expected === null ? actual === null : actual !== null && normalisePolicyExpr(actual, this.schemaName) === normalisePolicyExpr(expected, this.schemaName);
      const upToDate =
        row !== undefined && row.cmd === policy.command && row.roles.length === 1 && row.roles[0] === role && same(row.qual, policy.using) && same(row.with_check, policy.check);
      if (upToDate) continue;
      if (row !== undefined) await client.query(`DROP POLICY IF EXISTS ${quoteIdent(policy.name)} ON ${tableRef(policy.target)}`);
      await client.query(processorPolicyDdl(policy, role));
    }
  }

  /**
   * Whether this store is connected as `processorRole` (decision 46 §4): `current_user`, read once. Only then may its
   * root read every tenant (the database's processor policies decide what it sees) and run a cross-tenant processor.
   */
  processorSession(): Promise<boolean> {
    const role = this.ddlOptions.processorRole;
    if (!this.ddlOptions.rls || role === undefined) return Promise.resolve(false);
    if (!this.processorCheck) {
      this.processorCheck = (async () => {
        await this.ensureInstalled();
        const found = await this.pool.query<{ role: string }>("SELECT current_user::text AS role");
        return found.rows[0]?.role === role;
      })().catch((error: unknown) => {
        this.processorCheck = undefined;
        throw this.translate(error, false);
      });
    }
    return this.processorCheck;
  }

  /** The root's event reads under `rls`: refused, unless this store is connected as the processor role. */
  private async assertRootReads(): Promise<void> {
    if (!this.ddlOptions.rls) return;
    if (this.ddlOptions.processorRole !== undefined && (await this.processorSession())) return;
    this.assertRlsSession();
  }

  /** ENABLE and FORCE row-level security when needed; the policy recreated when USING or WITH CHECK drifted. */
  private async installPolicy(client: PoolClient, target: Target, policy: string, expr: string): Promise<void> {
    const t = tableRef(target);
    const flags = await client.query<{ rls: boolean; force: boolean }>(
      "SELECT c.relrowsecurity AS rls, c.relforcerowsecurity AS force FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = $1 AND n.nspname = $2",
      [target.table, target.schemaName],
    );
    if (!flags.rows[0]?.rls) await client.query(`ALTER TABLE ${t} ENABLE ROW LEVEL SECURITY`);
    if (!flags.rows[0]?.force) await client.query(`ALTER TABLE ${t} FORCE ROW LEVEL SECURITY`);
    const existing = await client.query<{ qual: string | null; with_check: string | null }>(
      "SELECT qual, with_check FROM pg_policies WHERE schemaname = $1 AND tablename = $2 AND policyname = $3",
      [target.schemaName, target.table, policy],
    );
    const row = existing.rows[0];
    const expected = normalisePolicyExpr(expr, target.schemaName);
    const upToDate =
      row !== undefined &&
      row.qual !== null &&
      row.with_check !== null &&
      normalisePolicyExpr(row.qual, target.schemaName) === expected &&
      normalisePolicyExpr(row.with_check, target.schemaName) === expected;
    if (!upToDate) {
      if (row !== undefined) await client.query(`DROP POLICY IF EXISTS ${quoteIdent(policy)} ON ${t}`);
      await client.query(`CREATE POLICY ${quoteIdent(policy)} ON ${t} USING (${expr}) WITH CHECK (${expr})`);
    }
  }

  /**
   * The statistics objects of the scope keys, per `scopeStatistics`. With a size policy the rows
   * per index decide, read after the indexes exist (a freshly built index knows its exact count).
   * Objects of keys that fell out of the policy are dropped — events are append-only, so that
   * only happens after an erasure.
   */
  private async installScopeStatistics(client: PoolClient): Promise<void> {
    const keys = this.schema.scopeKeys;
    let chosen: readonly string[] = keys;
    if (this.scopeStatisticsPolicy !== "all") {
      const { sql, params } = scopeIndexRowsSql(this.target, keys);
      const rows = await client.query<{ key: string; rows: number }>(sql, params);
      chosen = scopeStatisticsKeys(keys, new Map(rows.rows.map((r) => [r.key, Number(r.rows)])), this.scopeStatisticsPolicy);
    }
    const keep = new Set(chosen);
    const existing = new Set(
      (
        await client.query<{ name: string }>("SELECT s.stxname AS name FROM pg_catalog.pg_statistic_ext s JOIN pg_catalog.pg_namespace n ON n.oid = s.stxnamespace WHERE n.nspname = $1", [
          this.schemaName,
        ])
      ).rows.map((r) => r.name),
    );
    let created = false;
    for (const key of keys) {
      if (keep.has(key)) {
        if (!existing.has(scopeStatisticsName(this.table, key))) created = true;
        await client.query(scopeStatisticsDdl(this.target, key));
      } else await client.query(`DROP STATISTICS IF EXISTS ${qualified(this.schemaName, scopeStatisticsName(this.table, key))}`);
    }
    // a new statistics object is empty until the table is analysed; without it the planner keeps
    // misjudging the scope indexes until autovacuum happens to come by — analysed after COMMIT
    if (created) this.analyzeAfterInstall = true;
  }

  /**
   * Under `rls` the scope indexes serve a non-owner role only when the functions `es_scope`
   * inlines to are `LEAKPROOF` (see `SCOPE_BUILTIN_FUNCTIONS`). Checked at every install because
   * the mark lives in the database's catalogue and a restore or `pg_upgrade` loses it silently;
   * written only by a superuser. Anyone else gets a warning — reads stay correct, only slower.
   */
  private async markScopeFunctionsLeakproof(client: PoolClient): Promise<void> {
    const missing = await client.query<{ signature: string }>(SCOPE_LEAKPROOF_MISSING_SQL, [SCOPE_BUILTIN_FUNCTIONS]);
    if (missing.rows.length === 0) return;
    const role = await client.query<{ superuser: boolean }>("SELECT rolsuper AS superuser FROM pg_catalog.pg_roles WHERE rolname = current_user");
    if (role.rows[0]?.superuser !== true) {
      this.warningList.push(
        `rls: ${missing.rows.map((r) => r.signature).join(", ")} are not LEAKPROOF, so a non-owner role cannot use the scope indexes (only the tenant's) — ` +
          `run as a superuser, once per database and again after a restore or pg_upgrade: ${scopeLeakproofStatements().join("; ")}`,
      );
      return;
    }
    for (const statement of scopeLeakproofStatements()) await client.query(statement);
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

  private assertFoldStateSession(): void {
    if (this.ddlOptions.rls) {
      throw new UsageError(
        "eventstore/postgres: rls is enabled — persisted fold states are read and written only in a tenant session: use withTenant(tenantId) (or es.forTenant()); the root store has none",
      );
    }
  }

  /** One stored state, or `null`. A row whose key text differs (a hash collision) is a miss. */
  private async readFoldState(runner: Runner, tenant: string, key: FoldStateKey, inTenantSession: boolean): Promise<StoredFoldState | null> {
    const sql = `SELECT query_key, cursor_xid::text AS cursor_xid, cursor_seq::text AS cursor_seq, state, fingerprint FROM ${tableRef(this.foldTarget!)}
WHERE tenant = $1 AND fold_name = $2 AND fold_version = $3 AND query_hash = $4`;
    let result: pg.QueryResult<{ query_key: string; cursor_xid: string; cursor_seq: string; state: Buffer; fingerprint: string | null }>;
    try {
      result = await runner.query(sql, [tenant, key.name, key.version, foldStateHash(key.key)]);
    } catch (err) {
      throw this.translateFoldState(err, inTenantSession);
    }
    const row = result.rows[0];
    if (!row || row.query_key !== key.key) return null;
    return { cursor: { transactionId: row.cursor_xid, sequence: toSafeInt(row.cursor_seq) }, state: row.state, ...(row.fingerprint !== null ? { fingerprint: row.fingerprint } : {}) };
  }

  /** The monotone upsert: the row moves only to a further cursor; an older or equal one is a no-op. */
  private async writeFoldState(runner: Runner, tenant: string, key: FoldStateKey, stored: StoredFoldState, inTenantSession: boolean): Promise<void> {
    const sql = `INSERT INTO ${tableRef(this.foldTarget!)} AS t (tenant, fold_name, fold_version, query_hash, query_key, cursor_xid, cursor_seq, state, fingerprint)
VALUES ($1, $2, $3, $4, $5, $6::xid8, $7::bigint, $8, $9)
ON CONFLICT (tenant, fold_name, fold_version, query_hash) DO UPDATE
SET query_key = EXCLUDED.query_key, cursor_xid = EXCLUDED.cursor_xid, cursor_seq = EXCLUDED.cursor_seq, state = EXCLUDED.state, fingerprint = EXCLUDED.fingerprint, updated_at = now()
WHERE (t.cursor_xid, t.cursor_seq) < (EXCLUDED.cursor_xid, EXCLUDED.cursor_seq)`;
    const bytes = Buffer.from(stored.state.buffer, stored.state.byteOffset, stored.state.byteLength);
    try {
      await runner.query(sql, [tenant, key.name, key.version, foldStateHash(key.key), key.key, stored.cursor.transactionId, String(stored.cursor.sequence), bytes, stored.fingerprint ?? null]);
    } catch (err) {
      throw this.translateFoldState(err, inTenantSession);
    }
  }

  /** Errors of the fold-state table, in words that say what to do; everything else as `translate`. */
  private translateFoldState(err: unknown, inTenantSession: boolean): unknown {
    const e = err as { code?: string; message?: string } | null;
    const name = `${this.schemaName}.${this.foldTarget!.table}`;
    if (e?.code === "42P01") {
      return new EventStoreError(
        `eventstore/postgres: the fold-state table ${name} does not exist — let the store install it (persistedFolds: true with install: "auto"), or run printSchemaSql(schema, { persistedFolds: true }) as the owner`,
        { cause: err },
      );
    }
    if (e?.code === "42501" && !(this.ddlOptions.rls && inTenantSession)) {
      return new EventStoreError(`eventstore/postgres: permission denied — the application role needs SELECT, INSERT, UPDATE on ${name}: ${e.message ?? ""}`, { cause: err });
    }
    return this.translate(err, inTenantSession);
  }

  async query(query: Query, options: QueryOptions = {}): Promise<QueryResult> {
    await this.assertRootReads();
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
      const event = this.toRecorded(row, options);
      events.push(event);
      if (event.sequence > lastReturned) lastReturned = event.sequence;
      if (event.settled && (!settledCursor || compareCursor(event, settledCursor) > 0)) {
        settledCursor = { transactionId: event.transactionId, sequence: event.sequence };
      }
      if (compiled.filterCount === 1) byFilter[0]!.push(event);
      else (row.hits ?? []).forEach((hit, i) => hit && byFilter[i]?.push(event));
    }
    // the watermark is sound only from this very statement's snapshot, never from a second read
    const watermark = first?.xmin;
    return { events, byFilter, lastReturned, contextVersion, ctx: { query, version: contextVersion }, settledCursor, ...(watermark ? { watermark } : {}) };
  }

  private toRecorded(row: Row, options: QueryOptions = {}): RecordedEvent {
    const omit = options.omit;
    const type = row.event_type as string;
    const idKey = this.schema.idKeyOf(type);
    if (omit) assertOmittable(omit, idKey, type);
    const raw = row.payload as Record<string, unknown>;
    // id and scopes come from the RAW payload — what es_scope() matched and locked on; only
    // `data` goes through `upcast` (an upcast may reshape data, never scope keys or the id)
    const { id, scopes } = fromPayload(raw, idKey);
    let data: Record<string, unknown>;
    if (options.data === false || options.payload === false) {
      // SQL shipped ids and scopes (or nothing); data keeps only the declared scope keys carried
      // flat, so every back-link a query matched on stays readable
      data = options.payload === false ? {} : flatScopeKeys(raw, this.schema.scopeKeys, idKey);
    } else {
      const upcast = fromPayload(this.schema.upcast(type, raw), idKey);
      // SQL already stripped the keys; trim again after `upcast` so a reshaped payload cannot bring one back
      data = omit ? trimData(upcast.data, omit) : upcast.data;
    }
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
    await this.assertRootReads();
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
    // The transaction id comes from the append's own statement (decision 10); it is NULL only when
    // nothing was written, i.e. on a conflict.
    const sql = `SELECT ok, actual::text AS actual, first_seq::text AS first_seq, last_seq::text AS last_seq, cnt,
       pg_current_xact_id_if_assigned()::text AS xid
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
    if (row.xid === null) throw new EventStoreError("eventstore/postgres: append returned no transaction id");
    return {
      ok: true,
      appended: { first: toSafeInt(row.first_seq as string), last: toSafeInt(row.last_seq as string), count: Number(row.cnt), transactionId: row.xid },
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
  withTenant(tenantId: string): EventStore & StatisticsStore & Partial<WakeStore> & { readonly foldStates?: FoldStateStore; readonly readModelStore?: ReadModelStore } {
    const inTenant = <T>(fn: (runner: Runner) => Promise<T>): Promise<T> =>
      this.ensureInstalled().then(() => withTenantSession(this.pool, tenantId, (client) => fn(client)));
    /** The key's tenant is this session's, fail-closed; under `rls` the empty tenant has no rows of its own. */
    const sessionKey = (key: FoldStateKey): FoldStateKey => {
      if (key.tenant !== undefined && key.tenant !== tenantId) {
        throw new TenantMismatchError(`eventstore/postgres: a fold state key names tenant ${key.tenant} but the session is bound to ${tenantId}`);
      }
      if (this.ddlOptions.rls && tenantId === "") {
        throw new UsageError("eventstore/postgres: rls is enabled — a persisted fold needs a tenant; the empty tenant id has no fold states");
      }
      return key;
    };
    const foldStates: FoldStateStore | undefined = this.foldTarget
      ? {
          read: async (key) => {
            const k = sessionKey(key);
            return inTenant((r) => this.readFoldState(r, tenantId, k, true));
          },
          write: async (key, stored) => {
            const k = sessionKey(key);
            return inTenant((r) => this.writeFoldState(r, tenantId, k, stored, true));
          },
        }
      : undefined;
    return {
      // the doorbell is the store's: it carries no tenant, the tenant session decides what a read sees
      ...(this.onCommitted ? { onCommitted: this.onCommitted } : {}),
      ...(foldStates ? { foldStates } : {}),
      ...(this.models ? { readModelStore: this.models.capability(tenantId) } : {}),
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

  /**
   * Fill level of this database's notification queue, 0…1 (`pg_notification_queue_usage()`).
   * Near 0 when every listener reads; at 1 every commit that notifies fails — alert well before.
   */
  async notificationQueueUsage(): Promise<number> {
    const result = await this.pool.query<{ usage: number }>("SELECT pg_catalog.pg_notification_queue_usage() AS usage");
    return Number(result.rows[0]?.usage ?? 0);
  }

  async close(): Promise<void> {
    this.models?.close();
    await this.listener?.close();
    if (this.ownsPool) await this.pool.end();
  }
}

/** Create a Postgres store (installs the schema lazily unless `install: "none"`). */
export async function createPostgresStore(options: CreatePostgresStoreOptions): Promise<PostgresStore> {
  const store = new PostgresStore(options);
  await store.ensureInstalled();
  return store;
}

/**
 * The adoption statements as text, for a table in exactly the declared legacy shape (it cannot
 * look at the table; `store.adoptionPlan()` does). Run them before `printSchemaSql()`.
 */
export function printAdoptSql(options: { readonly adopt: AdoptOptions; readonly table?: string; readonly schemaName?: string } | { readonly plan: AdoptionPlan }): string {
  let statements: readonly string[];
  if ("plan" in options) {
    if (options.plan.problems.length > 0) throw new UsageError(`eventstore/postgres: the plan has problems:\n- ${options.plan.problems.join("\n- ")}`);
    statements = options.plan.statements; // from store.adoptionPlan(): it looked at the table
  } else {
    const target = { schemaName: options.schemaName ?? DEFAULT_SCHEMA, table: options.table ?? "events" };
    statements = adoptStatements(tableRef(target), options.adopt);
  }
  return ["-- @orgops/eventstore: adopt an existing table (metadata-only; payload untouched). Check row count and max sequence before and after.", ...statements.map((st) => `${st};`)].join("\n");
}

/** The DDL as text, for DBAs and `install: "none"` deployments (the unconditional form: no drift gate). */
export function printSchemaSql(
  schema: StoreSchema,
  options: Omit<CreatePostgresStoreOptions, "connection" | "schema" | "install" | "poolSize" | "timeouts" | "maxBatchSize" | "maxLockKeys" | "installLockTimeoutMs" | "rebuildScopeIndexes" | "adopt"> = {},
): string {
  const header =
    "-- @orgops/eventstore schema. Idempotent, but NOT gated: CREATE OR REPLACE FUNCTION es_scope over existing scope\n" +
    "-- indexes needs those indexes rebuilt (see scopeRebuildStatements). Run as the table owner.";
  const { scopeStatistics, live, ...ddlOptions } = options;
  const statements = ddlStatements(schema, {
    ...ddlOptions,
    live: live !== undefined && live !== false,
    ...(scopeStatistics === undefined || scopeStatistics === "all" ? {} : { scopeStatistics: scopeStatisticsKeys(schema.scopeKeys, new Map(), scopeStatistics) }),
    table: options.table ?? "events",
    tenantScopeKey: options.tenantScopeKey ?? schema.tenantScopeKey,
  }).map((s) => `${s};`);
  // under rls the scope indexes need three LEAKPROOF marks that only a superuser may set
  const leakproof = options.rls
    ? [
        "-- As a superuser, once per database and again after a restore or pg_upgrade (neither carries it): without it a\n" +
          "-- non-owner role uses no scope index but the tenant's under row-level security.\n" +
          scopeLeakproofStatements()
            .map((s) => `${s};`)
            .join("\n"),
      ]
    : [];
  return [header, ...statements, ...leakproof].join("\n\n");
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

/** The primary-key column of a state key: SHA-256 of its text, 32 bytes (the text itself can outgrow a B-tree entry). */
function foldStateHash(key: string): Buffer {
  return createHash("sha256").update(key).digest();
}

function toSafeInt(value: string | number): number {
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isSafeInteger(n)) throw new EventStoreError(`eventstore/postgres: sequence ${value} is not a safe integer`);
  return n;
}

/** The declared scope keys a payload carries flat (string values), without the own id — what a lean record keeps of its data. */
function flatScopeKeys(payload: Record<string, unknown>, scopeKeys: readonly string[], idKey: string): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const key of scopeKeys) {
    if (key === idKey || key === "scopes") continue;
    const value = payload && typeof payload === "object" ? payload[key] : undefined;
    if (typeof value === "string") out[key] = value;
  }
  return out;
}
