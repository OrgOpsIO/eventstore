/**
 * ## Postgres — what the store installs and how to operate it
 *
 * One table (`events` by default, in schema `public` unless `schemaName` says otherwise).
 * Everything else is an index or a function on it, and every reference in DDL and in every
 * compiled statement is schema-qualified (`"schema"."table"`, `"schema".es_scope(...)`), so a
 * shadowing function in an earlier `search_path` schema cannot hijack a read or a guard.
 *
 * - `es_scope(payload, key)` — SQL IMMUTABLE, string-only: `scopes->>key`, else the top-level
 *   `key` field. Every declared scope key gets a partial B-tree on
 *   `(es_scope(payload,'K'), sequence_number)`. Because the (inlined) body feeds those indexes,
 *   its identity is verified at install: by the fingerprint comment (`COMMENT ON FUNCTION …
 *   'es:<sha256>'`) and, when the comment is missing, by probing the installed function with
 *   known inputs. A body that differs is never applied silently: the installer throws the
 *   exact statements to run (`CREATE INDEX CONCURRENTLY`, statistics, comment) or rebuilds
 *   inline when `rebuildScopeIndexes: true` is passed.
 * - `es_append_if_v4(...)` — the conditional append (locks → fresh-snapshot version check →
 *   insert) in one round trip. `SECURITY INVOKER`, `search_path` pinned, `EXECUTE` revoked
 *   from `PUBLIC` and granted to the installing role plus `grantExecuteTo`. Requires READ
 *   COMMITTED (`ES001` otherwise) and a well-formed lock plan (`ES002` otherwise). The version
 *   check is built INSIDE the function from a structured JSON spec (leading scope key + values,
 *   types, other scopes, `where`), so no caller can hand it SQL text. Locks are taken in ONE
 *   pass sorted by key, each with its mode (shared for tenant stamps, exclusive otherwise).
 *   Versioned by name; every other `es_append_if*` overload in the schema is dropped once.
 * - `(transaction_id, sequence_number)` for cursor reads, optional `(event_type,
 *   sequence_number)` (`typeIndex`), one partial UNIQUE index per declared `unique` path, one
 *   UNIQUE index on the idempotency key (per tenant when a tenant scope key is configured),
 *   optional GIN `jsonb_path_ops` for ad-hoc `where`, and `CREATE STATISTICS` per scope key
 *   (partial expression indexes hide their statistics from the planner; this is what makes it
 *   pick the scope index in sequence order without a Sort node).
 * - optional row-level security on the tenant scope (`rls: true` + `tenantScopeKey`): the
 *   policy compares `es_scope(payload,'<tenantKey>')` with `current_setting('app.current_tenant')`
 *   in USING and WITH CHECK; the installer recreates it when either expression drifted.
 *   Use `store.withTenant(id)` so every statement runs with that setting; the root store
 *   refuses reads and writes under `rls`.
 *
 * Roles: install as the table owner (`install: "auto"`, the default; concurrent boots are
 * serialised by a transaction-scoped advisory lock inside one DDL transaction with
 * `statement_timeout` lifted — it also works behind a transaction pooler). Application roles
 * need `SELECT, INSERT` on the table, `USAGE` on its sequence and `EXECUTE` on the append
 * function (`grantExecuteTo`). Locked-down environments run `printSchemaSql()` through their
 * migration tool and set `install: "none"`. `printSchemaSql` prints the unconditional form
 * (DROP + CREATE POLICY, CREATE OR REPLACE FUNCTION).
 *
 * Pools the SDK creates carry `statement_timeout`, `lock_timeout`,
 * `idle_in_transaction_session_timeout` and `search_path` as libpq startup options. A pool you
 * pass in gets none of that (recorded in `installWarnings`).
 *
 * Adding a scope key later creates an index with a `ShareLock` on the table; do it in a quiet
 * window or run `CREATE INDEX CONCURRENTLY` from `printSchemaSql()` by hand. An append that
 * would need more than `maxLockKeys` advisory locks (default 512) takes the exclusive global
 * lock instead — correct, coarse, and it keeps the shared lock table (sized by
 * `max_locks_per_transaction`) out of trouble.
 *
 * Durability knobs are yours: `synchronous_commit=off` gains 40–75 % on single-event appends
 * (batches barely change) at the price of losing the last few hundred ms after a crash;
 * `commit_delay` helps only on fsync-bound storage. Raw Postgres tops out around 208k events/s
 * in 50-event batches at c=4 on the bench machine — batch when you can.
 */
import { createHash } from "node:crypto";
import { filtersOf, uniquePathSegments } from "../query.js";
import type { Filter, Query, QueryOptions, StoreSchema } from "../types.js";

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

export const APPEND_FUNCTION_VERSION = 4;

/** Argument types of the current append function, for GRANT/REVOKE/COMMENT statements. */
export const APPEND_SIGNATURE = "(bigint[], boolean[], bigint, boolean, jsonb, bigint, text[], jsonb[], jsonb[])";

export const SCOPE_FUNCTION_NAME = "es_scope";
export const DEFAULT_SCHEMA = "public";

/** Where the store lives: a Postgres namespace and a table name, both validated identifiers. */
export interface Target {
  readonly schemaName: string;
  readonly table: string;
}

export function targetOf(tableOrTarget: string | Target): Target {
  const target = typeof tableOrTarget === "string" ? { schemaName: DEFAULT_SCHEMA, table: tableOrTarget } : tableOrTarget;
  assertIdentifier(target.schemaName, "schema name");
  assertIdentifier(target.table, "table name");
  return target;
}

export function assertIdentifier(name: string, what: string): string {
  if (!IDENTIFIER.test(name)) throw new Error(`eventstore/postgres: invalid ${what} "${name}"`);
  return name;
}

/** Double-quote an SQL identifier. */
export function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/** Single-quote an SQL string literal (only used for validated identifiers / constant names). */
export function quoteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

/** `"schema"."name"` */
export function qualified(schemaName: string, name: string): string {
  return `${quoteIdent(assertIdentifier(schemaName, "schema name"))}.${quoteIdent(name)}`;
}

/** The qualified table reference of a target. */
export function tableRef(target: Target): string {
  return qualified(target.schemaName, target.table);
}

/** The qualified scope function reference of a target's schema. */
export function scopeFn(schemaName: string): string {
  return qualified(schemaName, SCOPE_FUNCTION_NAME);
}

function short(input: string): string {
  return createHash("sha256").update(input).digest("hex").slice(0, 8);
}

export function sha256(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

/** Deterministic index/function names, ≤ 63 chars. */
export function indexName(table: string, kind: string, detail: string): string {
  const base = `es_${table}_${kind}_${detail}`.replace(/[^A-Za-z0-9_]/g, "_");
  if (base.length <= 63) return base;
  return `${base.slice(0, 54)}_${short(base)}`;
}

/** Base name of the append function for a table (all versions share it as a prefix). */
export function appendFunctionBaseName(table: string): string {
  return table === "events" ? "es_append_if" : indexName(table, "append", "if").replace(/^es_/, "es_fn_");
}

/** Versioned name of the append function: `es_append_if_v4` for `events`, `es_fn_<table>_append_if_v4` otherwise. */
export function appendFunctionName(table: string, version: number = APPEND_FUNCTION_VERSION): string {
  return version === 1 ? appendFunctionBaseName(table) : `${appendFunctionBaseName(table)}_v${version}`;
}

// ── DDL ───────────────────────────────────────────────────────────────────────

export interface DdlOptions {
  readonly table: string;
  /** Postgres namespace the table and functions live in. Default `public`. */
  readonly schemaName?: string;
  readonly adhocQueries?: boolean;
  readonly rls?: boolean;
  readonly tenantScopeKey?: string;
  /** Roles granted EXECUTE on the append function (the installing role always is). */
  readonly grantExecuteTo?: readonly string[];
  /** Opt-in `(event_type, sequence_number)` index for type-only reads. No SDK code path needs it. */
  readonly typeIndex?: boolean;
}

export interface UniqueIndex {
  readonly name: string;
  readonly type: string;
  readonly path: string;
}

export function uniqueIndexes(schema: StoreSchema, table: string): UniqueIndex[] {
  return schema.uniques.map((u) => ({
    name: indexName(table, "uniq", `${u.type}_${u.path.replace(/\./g, "_")}_${short(`${u.type}|${u.path}`)}`),
    type: u.type,
    path: u.path,
  }));
}

export function idempotencyIndexName(table: string): string {
  return indexName(table, "idem", "key");
}

export function scopeIndexName(table: string, key: string): string {
  return indexName(table, "scope", key);
}

export function scopeStatisticsName(table: string, key: string): string {
  return indexName(table, "stat", key);
}

export function tableDdl(target: Target): string {
  return `CREATE TABLE IF NOT EXISTS ${tableRef(target)} (
  sequence_number BIGSERIAL PRIMARY KEY,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  transaction_id XID8 NOT NULL DEFAULT pg_current_xact_id()
)`;
}

function searchPath(schemaName: string): string {
  return `pg_catalog, ${quoteIdent(assertIdentifier(schemaName, "schema name"))}`;
}

/**
 * The scope function. String-only on purpose: a numeric or object value at a scope key is not
 * a scope value (the memory store and the envelope validation agree).
 *
 * `LANGUAGE sql` so Postgres inlines it — into queries AND into the index expressions at
 * index creation, so both sides stay identical and the partial expression index is matched by
 * the row read and by the MAX version check (verified with EXPLAIN; ~8× cheaper per call than
 * plpgsql). A function with `SET search_path` is never inlined, so the body schema-qualifies
 * every operator instead. Consequence of inlining: a body change needs the scope indexes
 * DROPPED and recreated (a REINDEX keeps the old stored expression) — see `scopeRebuildStatements`.
 */
export function scopeFunctionDdl(schemaName: string = DEFAULT_SCHEMA): string {
  return `CREATE OR REPLACE FUNCTION ${scopeFn(schemaName)}(p jsonb, k text) RETURNS text
  LANGUAGE sql IMMUTABLE PARALLEL SAFE
  RETURN ${SCOPE_FUNCTION_BODY}`;
}

/** The body expression of `es_scope` — what the fingerprint hashes (cosmetic DDL changes do not count). */
export const SCOPE_FUNCTION_BODY = `CASE
    WHEN pg_catalog.jsonb_typeof(p OPERATOR(pg_catalog.->) 'scopes' OPERATOR(pg_catalog.->) k) OPERATOR(pg_catalog.=) 'string'
      THEN p OPERATOR(pg_catalog.->) 'scopes' OPERATOR(pg_catalog.->>) k
    WHEN pg_catalog.jsonb_typeof(p OPERATOR(pg_catalog.->) k) OPERATOR(pg_catalog.=) 'string'
      THEN p OPERATOR(pg_catalog.->>) k
  END`;

/**
 * Behavioural probes of `es_scope`: an installed function without a fingerprint comment is
 * accepted as "our body" only if it answers all of these exactly. Covers: scopes object,
 * flat field, scopes wins over flat, non-string values are not scope values (the old plpgsql
 * COALESCE body returned `1` for the fourth and `x` for the fifth).
 */
export const SCOPE_PROBES: readonly { payload: string; key: string; expected: string | null }[] = [
  { payload: '{"scopes":{"k":"v"}}', key: "k", expected: "v" },
  { payload: '{"k":"flat"}', key: "k", expected: "flat" },
  { payload: '{"scopes":{"k":"s"},"k":"flat"}', key: "k", expected: "s" },
  { payload: '{"k":1}', key: "k", expected: null },
  { payload: '{"scopes":{"k":1},"k":"x"}', key: "k", expected: "x" },
  { payload: '{"scopes":"nope"}', key: "k", expected: null },
];

/** Extended statistics on the scope expression: partial indexes hide theirs from the planner. */
export function scopeStatisticsDdl(target: Target, key: string): string {
  assertIdentifier(key, "scope key");
  return `CREATE STATISTICS IF NOT EXISTS ${qualified(target.schemaName, scopeStatisticsName(target.table, key))} ON ${scopeFn(target.schemaName)}(payload, ${quoteLiteral(key)}) FROM ${tableRef(target)}`;
}

export function scopeIndexDdl(target: Target, key: string): string {
  assertIdentifier(key, "scope key");
  const expr = `${scopeFn(target.schemaName)}(payload, ${quoteLiteral(key)})`;
  return `CREATE INDEX IF NOT EXISTS ${quoteIdent(scopeIndexName(target.table, key))} ON ${tableRef(target)} ((${expr}), sequence_number) WHERE ${expr} IS NOT NULL`;
}

/**
 * The statements that bring the scope indexes and statistics in line with a changed
 * `es_scope` body, in an order that keeps reads working: concurrent index builds first.
 * Run them by hand in a quiet window — each statement outside a transaction (`CONCURRENTLY`
 * cannot run inside one) — or pass `rebuildScopeIndexes: true` to the store.
 */
export function scopeRebuildStatements(schema: StoreSchema, target: Target): string[] {
  const statements: string[] = ["-- run each statement outside a transaction (CREATE/DROP INDEX CONCURRENTLY cannot run inside one)", scopeFunctionDdl(target.schemaName)];
  for (const key of schema.scopeKeys) {
    const expr = `${scopeFn(target.schemaName)}(payload, ${quoteLiteral(key)})`;
    statements.push(`DROP STATISTICS IF EXISTS ${qualified(target.schemaName, scopeStatisticsName(target.table, key))}`);
    statements.push(`DROP INDEX CONCURRENTLY IF EXISTS ${qualified(target.schemaName, scopeIndexName(target.table, key))}`);
    statements.push(
      `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${quoteIdent(scopeIndexName(target.table, key))} ON ${tableRef(target)} ((${expr}), sequence_number) WHERE ${expr} IS NOT NULL`,
    );
    statements.push(scopeStatisticsDdl(target, key));
  }
  statements.push(`COMMENT ON FUNCTION ${scopeFn(target.schemaName)}(jsonb, text) IS ${quoteLiteral(scopeFunctionFingerprint(target.schemaName))}`);
  return statements;
}

/** Fingerprint of the `es_scope` BODY (whitespace-normalised), stamped on the function and on every index built on it. */
export function scopeFunctionFingerprint(_schemaName: string = DEFAULT_SCHEMA): string {
  return `es:${sha256(SCOPE_FUNCTION_BODY.replace(/\s+/g, " ").trim())}`;
}

export function appendFunctionFingerprint(target: Target): string {
  return `es:${sha256(appendFunctionDdl(target))}`;
}

/** `'{seg1,seg2}'` literal for `payload #>> …` on a validated unique path. */
export function uniquePathSegmentsSql(path: string): string {
  return `'{${uniquePathSegments(path).join(",")}}'`;
}

/** Statements that maintain access rights on the append function. */
export function grantStatements(target: Target, options: Pick<DdlOptions, "grantExecuteTo">): string[] {
  const signature = `${qualified(target.schemaName, appendFunctionName(target.table))}${APPEND_SIGNATURE}`;
  const statements = [`REVOKE EXECUTE ON FUNCTION ${signature} FROM PUBLIC`, `GRANT EXECUTE ON FUNCTION ${signature} TO CURRENT_USER`];
  for (const role of options.grantExecuteTo ?? []) {
    statements.push(`GRANT EXECUTE ON FUNCTION ${signature} TO ${quoteIdent(assertIdentifier(role, "role name"))}`);
  }
  return statements;
}

/** The RLS policy expression (USING and WITH CHECK are identical). */
export function policyExpression(schemaName: string, tenantKey: string): string {
  return `${scopeFn(schemaName)}(payload, ${quoteLiteral(assertIdentifier(tenantKey, "tenant scope key"))}) = current_setting('app.current_tenant', true)`;
}

export function policyName(table: string): string {
  return indexName(table, "tenant", "isolation");
}

/**
 * Every DDL statement needed, idempotent, in order — the static picture for `printSchemaSql`.
 * The installer (`PostgresStore`) runs the same statements but replaces functions only when
 * their fingerprint changed and never rebuilds scope indexes on its own.
 */
export function ddlStatements(schema: StoreSchema, options: DdlOptions): string[] {
  const target = targetOf({ schemaName: options.schemaName ?? DEFAULT_SCHEMA, table: options.table });
  const t = tableRef(target);
  const statements: string[] = [];

  if (target.schemaName !== DEFAULT_SCHEMA) statements.push(`CREATE SCHEMA IF NOT EXISTS ${quoteIdent(target.schemaName)}`);
  statements.push(tableDdl(target));
  statements.push(scopeFunctionDdl(target.schemaName));
  statements.push(`COMMENT ON FUNCTION ${scopeFn(target.schemaName)}(jsonb, text) IS ${quoteLiteral(scopeFunctionFingerprint(target.schemaName))}`);

  if (options.typeIndex) {
    statements.push(`CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(target.table, "type", "seq"))} ON ${t} (event_type, sequence_number)`);
  }
  // cursor reads order by (transaction_id, sequence_number)
  statements.push(`CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(target.table, "xid", "seq"))} ON ${t} (transaction_id, sequence_number)`);

  for (const key of schema.scopeKeys) {
    statements.push(scopeIndexDdl(target, key));
    statements.push(scopeStatisticsDdl(target, key));
  }

  for (const u of uniqueIndexes(schema, target.table)) {
    const expr = `(payload #>> ${uniquePathSegmentsSql(u.path)})`;
    statements.push(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdent(u.name)} ON ${t} (${expr}) WHERE event_type = ${quoteLiteral(u.type)} AND ${expr} IS NOT NULL`,
    );
  }

  const tenantKey = schema.tenantScopeKey ?? options.tenantScopeKey;
  statements.push(idempotencyIndexDdl(target, tenantKey));

  if (options.adhocQueries) {
    statements.push(`CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(target.table, "payload", "gin"))} ON ${t} USING GIN (payload jsonb_path_ops)`);
  }

  statements.push(appendFunctionDdl(target));
  statements.push(
    `COMMENT ON FUNCTION ${qualified(target.schemaName, appendFunctionName(target.table))}${APPEND_SIGNATURE} IS ${quoteLiteral(appendFunctionFingerprint(target))}`,
  );
  statements.push(...grantStatements(target, options));

  if (options.rls) {
    if (!tenantKey) throw new Error("eventstore/postgres: rls: true needs tenantScopeKey");
    const expr = policyExpression(target.schemaName, tenantKey);
    const policy = quoteIdent(policyName(target.table));
    statements.push(`ALTER TABLE ${t} ENABLE ROW LEVEL SECURITY`);
    statements.push(`ALTER TABLE ${t} FORCE ROW LEVEL SECURITY`);
    statements.push(`DROP POLICY IF EXISTS ${policy} ON ${t}`);
    statements.push(`CREATE POLICY ${policy} ON ${t} USING (${expr}) WITH CHECK (${expr})`);
  }

  return statements;
}

export function idempotencyIndexDdl(target: Target, tenantKey: string | undefined): string {
  const t = tableRef(target);
  const name = quoteIdent(idempotencyIndexName(target.table));
  if (tenantKey) {
    assertIdentifier(tenantKey, "tenant scope key");
    return `CREATE UNIQUE INDEX IF NOT EXISTS ${name} ON ${t} ((${scopeFn(target.schemaName)}(payload, ${quoteLiteral(tenantKey)})), (metadata->>'idempotencyKey')) WHERE metadata ? 'idempotencyKey'`;
  }
  return `CREATE UNIQUE INDEX IF NOT EXISTS ${name} ON ${t} ((metadata->>'idempotencyKey')) WHERE metadata ? 'idempotencyKey'`;
}

/**
 * The conditional append, one round trip. VOLATILE plpgsql: every statement inside takes a
 * fresh snapshot under READ COMMITTED, so the MAX read in step 2 happens strictly after the
 * advisory locks of step 1 — a single-statement CTE cannot give that guarantee. Under
 * REPEATABLE READ or SERIALIZABLE the whole transaction shares one snapshot and the guard
 * would be stale, hence the isolation check. Do not declare it STABLE.
 *
 * `p_lock_keys`/`p_lock_modes` are parallel arrays (mode true = exclusive), sorted by the
 * caller; the function sorts again anyway and refuses a malformed plan (`ES002`).
 *
 * `p_version` is the structured version spec (see `versionSpec`): the function builds the
 * `MAX(sequence_number)` statements itself from it with `format(%L)`, so the only SQL that ever
 * runs is the SDK's own shape — a role with EXECUTE gets a version check, not a SQL gadget.
 */
export function appendFunctionDdl(target: Target): string {
  const t = tableRef(target);
  const fn = qualified(target.schemaName, appendFunctionName(target.table));
  const scope = scopeFn(target.schemaName);
  return `CREATE OR REPLACE FUNCTION ${fn}(
  p_lock_keys bigint[],
  p_lock_modes boolean[],
  p_global_key bigint,
  p_exclusive_global boolean,
  p_version jsonb,
  p_expected bigint,
  p_types text[],
  p_payloads jsonb[],
  p_metadata jsonb[]
) RETURNS TABLE(ok boolean, actual bigint, first_seq bigint, last_seq bigint, cnt integer)
  LANGUAGE plpgsql VOLATILE SECURITY INVOKER
  SET search_path = ${searchPath(target.schemaName)}
  AS $fn$
DECLARE
  v_actual bigint := 0;
  v_branch_max bigint;
  v_branch jsonb;
  v_key text;
  v_value text;
  v_values text[];
  v_types text[];
  v_where jsonb[];
  v_sql text;
  v_pair record;
  v_first bigint;
  v_last bigint;
  v_count integer;
BEGIN
  -- 0. the guard is only sound when each statement sees a fresh snapshot
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION USING ERRCODE = 'ES001', MESSAGE = 'es_append_if requires READ COMMITTED';
  END IF;
  -- 0b. a malformed lock plan would silently downgrade or skip locks (NULL mode → shared, NULL key → no lock)
  IF p_global_key IS NULL OR p_exclusive_global IS NULL
     OR coalesce(array_length(p_lock_keys, 1), 0) <> coalesce(array_length(p_lock_modes, 1), 0)
     OR array_position(p_lock_keys, NULL) IS NOT NULL
     OR array_position(p_lock_modes, NULL) IS NOT NULL THEN
    RAISE EXCEPTION USING ERRCODE = 'ES002', MESSAGE = 'es_append_if: malformed lock plan';
  END IF;
  IF p_version IS NOT NULL AND jsonb_typeof(p_version) <> 'array' THEN
    RAISE EXCEPTION USING ERRCODE = 'ES002', MESSAGE = 'es_append_if: malformed version spec';
  END IF;

  -- 1. locks first, in statements of their own (sorted to avoid deadlocks)
  IF p_exclusive_global THEN
    PERFORM pg_advisory_xact_lock(p_global_key);
  ELSE
    PERFORM pg_advisory_xact_lock_shared(p_global_key);
  END IF;
  -- lock order everywhere: global first, then ONE pass over all keys sorted by key, each in its
  -- mode (true = exclusive). Two passes (all shared, then all exclusive) are not a global order
  -- and deadlock when a shared key sorts above an exclusive one.
  PERFORM CASE WHEN s.excl THEN pg_advisory_xact_lock(s.k) ELSE pg_advisory_xact_lock_shared(s.k) END
  FROM (SELECT u.k, u.excl FROM unnest(p_lock_keys, p_lock_modes) AS u(k, excl) ORDER BY u.k) s;

  -- 2. the context version, read after the locks (fresh snapshot): one MAX per leading scope
  --    value — an index-backward scan on the scope index — built from the structured spec.
  IF p_version IS NOT NULL THEN
    FOR v_branch IN SELECT value FROM jsonb_array_elements(p_version) LOOP
      v_key := v_branch ->> 'key';
      v_types := CASE WHEN jsonb_typeof(v_branch -> 'types') = 'array'
                      THEN ARRAY(SELECT jsonb_array_elements_text(v_branch -> 'types')) END;
      v_where := CASE WHEN jsonb_typeof(v_branch -> 'where') = 'array'
                      THEN ARRAY(SELECT jsonb_array_elements(v_branch -> 'where')) END;
      v_values := CASE WHEN v_key IS NULL THEN ARRAY[NULL::text]
                       ELSE ARRAY(SELECT jsonb_array_elements_text(v_branch -> 'values')) END;
      IF v_key IS NOT NULL AND v_key !~ '^[A-Za-z_][A-Za-z0-9_]*$' THEN
        RAISE EXCEPTION USING ERRCODE = 'ES002', MESSAGE = 'es_append_if: malformed version spec (scope key)';
      END IF;
      FOREACH v_value IN ARRAY v_values LOOP
        v_sql := 'SELECT max(sequence_number) FROM ${t} WHERE '
              || CASE WHEN v_key IS NULL THEN 'TRUE' ELSE format('${scope}(payload, %L) = %L', v_key, v_value) END;
        IF v_types IS NOT NULL THEN
          v_sql := v_sql || format(' AND event_type = ANY(%L::text[])', v_types);
        END IF;
        IF jsonb_typeof(v_branch -> 'scopes') = 'object' THEN
          FOR v_pair IN SELECT key AS k, ARRAY(SELECT jsonb_array_elements_text(value)) AS vals FROM jsonb_each(v_branch -> 'scopes') LOOP
            IF v_pair.k !~ '^[A-Za-z_][A-Za-z0-9_]*$' THEN
              RAISE EXCEPTION USING ERRCODE = 'ES002', MESSAGE = 'es_append_if: malformed version spec (scope key)';
            END IF;
            v_sql := v_sql || format(' AND ${scope}(payload, %L) = ANY(%L::text[])', v_pair.k, v_pair.vals);
          END LOOP;
        END IF;
        IF v_where IS NOT NULL THEN
          v_sql := v_sql || format(' AND payload @> ANY(%L::jsonb[])', v_where);
        END IF;
        EXECUTE v_sql INTO v_branch_max;
        v_actual := greatest(v_actual, coalesce(v_branch_max, 0));
      END LOOP;
    END LOOP;
    IF v_actual IS DISTINCT FROM p_expected THEN
      RETURN QUERY SELECT false, v_actual, NULL::bigint, NULL::bigint, 0;
      RETURN;
    END IF;
  END IF;

  -- 3. commit the batch: one consecutive sequence range
  WITH ins AS (
    INSERT INTO ${t} (event_type, payload, metadata)
    SELECT u.t, u.p, COALESCE(u.m, '{}'::jsonb)
    FROM unnest(p_types, p_payloads, p_metadata) WITH ORDINALITY AS u(t, p, m, ord)
    ORDER BY u.ord
    RETURNING sequence_number
  )
  SELECT MIN(sequence_number), MAX(sequence_number), COUNT(*)::integer INTO v_first, v_last, v_count FROM ins;

  RETURN QUERY SELECT true, v_actual, v_first, v_last, v_count;
END
$fn$`;
}

// ── query compilation ─────────────────────────────────────────────────────────

/** Parameter collector shared by every fragment of one statement: values → `($1::text[])[i]` / `($2::jsonb[])[i]`. */
export interface ParamCollector {
  readonly texts: string[];
  readonly jsons: string[];
  text(v: string): string;
  json(v: unknown): string;
}

export function createParamCollector(): ParamCollector {
  const texts: string[] = [];
  const jsons: string[] = [];
  return {
    texts,
    jsons,
    text(v) {
      texts.push(v);
      return `($1::text[])[${texts.length}]`;
    },
    json(v) {
      jsons.push(JSON.stringify(v));
      return `($2::jsonb[])[${jsons.length}]`;
    },
  };
}

/** Compiled condition: SQL that references the collector's `$1::text[]` / `$2::jsonb[]` arrays. */
export interface CompiledFilters {
  readonly texts: string[];
  readonly jsons: string[];
  /** One SQL condition per filter. */
  readonly perFilter: string[];
  /** All filters OR'ed. */
  readonly sql: string;
}

export function compileFilters(query: Query, collector: ParamCollector = createParamCollector(), schemaName: string = DEFAULT_SCHEMA): CompiledFilters {
  const scope = scopeFn(schemaName);
  const perFilter = filtersOf(query).map((f) => compileFilter(f, collector.text, collector.json, scope));
  return { texts: collector.texts, jsons: collector.jsons, perFilter, sql: perFilter.map((c) => `(${c})`).join(" OR ") };
}

function compileFilter(filter: Filter, text: (v: string) => string, json: (v: unknown) => string, scope: string): string {
  const parts: string[] = [];
  if (filter.types) {
    // an empty type list matches nothing (a dynamic filter that narrowed to zero types)
    if (filter.types.length === 0) return "FALSE";
    parts.push(`event_type = ANY(ARRAY[${filter.types.map((t) => text(t)).join(", ")}]::text[])`);
  }
  if (filter.scopes) {
    for (const [key, values] of Object.entries(filter.scopes)) {
      assertIdentifier(key, "scope key");
      const list = (typeof values === "string" ? [values] : values).map((v) => text(v));
      // Scalar equality lets the planner use the scope index's statistics and, for MAX(), the
      // index itself; `= ANY(ARRAY[...])` does neither.
      parts.push(list.length === 1 ? `${scope}(payload, ${quoteLiteral(key)}) = ${list[0]}` : `${scope}(payload, ${quoteLiteral(key)}) IN (${list.join(", ")})`);
    }
  }
  if (filter.where && filter.where.length > 0) {
    parts.push(`(${filter.where.map((w) => `payload @> ${json(w)}::jsonb`).join(" OR ")})`);
  }
  return parts.length > 0 ? parts.join(" AND ") : "TRUE";
}

/**
 * Which scope key leads the version check: a non-tenant key with the fewest values (most
 * selective, cheapest index walk); the tenant key only when nothing else is there — a
 * tenant-wide guard IS the whole tenant and there is no narrower index for it.
 */
export function leadingScopeKey(filter: Filter, tenantScopeKey: string | undefined): string | undefined {
  const entries = Object.entries(filter.scopes ?? {});
  if (entries.length === 0) return undefined;
  const count = (v: string | readonly string[]) => (typeof v === "string" ? 1 : v.length);
  const candidates = entries.filter(([k]) => k !== tenantScopeKey);
  const pool = candidates.length > 0 ? candidates : entries;
  return pool.reduce((best, cur) => (count(cur[1]) < count(best[1]) ? cur : best))[0];
}

/**
 * One branch of the version check: `MAX(sequence_number)` over the events that match
 * `key = value` (for every value) AND the rest of the filter. `key: null` is a filter without
 * scopes (non-strict only): one branch over types/where. This is the single structure the
 * read statement (`compileVersionSql`) and the append function (`p_version`) are both built
 * from, so the two can never disagree.
 */
export interface VersionBranch {
  readonly key: string | null;
  readonly values: readonly string[];
  readonly types: readonly string[] | null;
  readonly scopes: Readonly<Record<string, readonly string[]>> | null;
  readonly where: readonly Readonly<Record<string, unknown>>[] | null;
}

/** The version spec of a query. Filters that can match nothing (`types: []`) contribute no branch. */
export function versionSpec(query: Query, schema?: Pick<StoreSchema, "tenantScopeKey">): VersionBranch[] {
  const branches: VersionBranch[] = [];
  for (const filter of filtersOf(query)) {
    if (filter.types && filter.types.length === 0) continue;
    const types = filter.types ? [...filter.types] : null;
    const where = filter.where && filter.where.length > 0 ? filter.where.map((w) => ({ ...w })) : null;
    const leading = leadingScopeKey(filter, schema?.tenantScopeKey);
    if (leading === undefined) {
      branches.push({ key: null, values: [], types, scopes: null, where });
      continue;
    }
    assertIdentifier(leading, "scope key");
    const raw = filter.scopes![leading]!;
    const values = typeof raw === "string" ? [raw] : [...raw];
    const rest: Record<string, readonly string[]> = {};
    for (const [k, v] of Object.entries(filter.scopes!)) {
      if (k === leading) continue;
      assertIdentifier(k, "scope key");
      rest[k] = typeof v === "string" ? [v] : [...v];
    }
    branches.push({ key: leading, values, types, scopes: Object.keys(rest).length > 0 ? rest : null, where });
  }
  return branches;
}

/**
 * The CCC context version as one statement returning a single bigint, using the collector's
 * `$1::text[]` / `$2::jsonb[]` arrays — for the READ path. The append function computes the
 * same number from `versionSpec()` internally.
 */
export function compileVersionSql(
  tableOrTarget: string | Target,
  query: Query,
  collector: ParamCollector = createParamCollector(),
  schema?: Pick<StoreSchema, "tenantScopeKey">,
): { sql: string; texts: string[]; jsons: string[] } {
  const target = targetOf(tableOrTarget);
  const t = tableRef(target);
  const scope = scopeFn(target.schemaName);
  const { texts, jsons } = collector;
  const text = collector.text;
  const json = collector.json;
  const branches: string[] = [];
  for (const branch of versionSpec(query, schema)) {
    const rest: Filter = {
      ...(branch.types ? { types: branch.types } : {}),
      ...(branch.scopes ? { scopes: branch.scopes } : {}),
      ...(branch.where ? { where: branch.where } : {}),
    };
    if (branch.key === null) {
      branches.push(`SELECT MAX(sequence_number) FROM ${t} WHERE ${compileFilter(rest, text, json, scope)}`);
      continue;
    }
    for (const value of branch.values) {
      const restSql = compileFilter(rest, text, json, scope);
      branches.push(`SELECT MAX(sequence_number) FROM ${t} WHERE ${scope}(payload, ${quoteLiteral(branch.key)}) = ${text(value)}${restSql === "TRUE" ? "" : ` AND ${restSql}`}`);
    }
  }
  if (branches.length === 0) return { sql: "SELECT 0::bigint", texts, jsons };
  // One branch: the bare MAX (no UNION ALL wrapper, −19 µs per guarded append).
  const sql =
    branches.length === 1
      ? branches[0]!.replace("SELECT MAX(sequence_number)", "SELECT COALESCE(MAX(sequence_number), 0)")
      : `SELECT COALESCE(MAX(m), 0) FROM (${branches.map((b) => `(${b})`).join(" UNION ALL ")}) AS branches(m)`;
  return { sql, texts, jsons };
}

/** `settled` relative to a snapshot xmin hoisted into the `ctx` CTE. */
export const SETTLED_SQL = "transaction_id < ctx.xmin";

export interface CompiledQuery {
  readonly sql: string;
  readonly params: unknown[];
  /** Number of filters; when 1 the statement carries no `hits` column. */
  readonly filterCount: number;
}

/**
 * One statement that always returns at least one row (context version + snapshot xmin), with
 * the matching records joined laterally so `limit`/`order` apply only to the rows.
 *
 * Order: `sequence_number` for plain reads; `(transaction_id, sequence_number)` — the only
 * gap-free order across transactions — whenever `cursor` or `settledOnly` is used.
 */
export function compileQuery(tableOrTarget: string | Target, query: Query, options: QueryOptions, schema?: Pick<StoreSchema, "tenantScopeKey">): CompiledQuery {
  const target = targetOf(tableOrTarget);
  const t = tableRef(target);
  const collector = createParamCollector();
  const compiled = compileFilters(query, collector, target.schemaName);
  const version = compileVersionSql(target, query, collector, schema); // same collector: one parameter space
  const params: unknown[] = [collector.texts, collector.jsons];
  const param = (v: unknown): string => {
    params.push(v);
    return `$${params.length}`;
  };
  const conds: string[] = [`(${compiled.sql})`];
  if (options.after !== undefined) conds.push(`sequence_number > ${param(String(options.after))}::bigint`);
  if (options.settledOnly) conds.push(SETTLED_SQL);
  if (options.cursor) {
    conds.push(
      `NOT (${SETTLED_SQL} AND (transaction_id, sequence_number) <= (${param(options.cursor.transactionId)}::xid8, ${param(String(options.cursor.sequence))}::bigint))`,
    );
  }
  const dir = options.order === "desc" ? "DESC" : "ASC";
  const tupleOrder = options.cursor !== undefined || options.settledOnly === true;
  const orderBy = tupleOrder ? `transaction_id ${dir}, sequence_number ${dir}` : `sequence_number ${dir}`;
  const limit = options.limit !== undefined ? ` LIMIT ${param(String(options.limit))}::bigint` : "";
  const filterCount = compiled.perFilter.length;
  const hits = filterCount > 1 ? `ARRAY[${compiled.perFilter.map((c) => `(${c})`).join(", ")}]::boolean[]` : "NULL::boolean[]";
  // A parameter array nobody references would make the bind fail ("supplies 2 parameters …"):
  // anchor only those in the ctx CTE.
  const body = `${version.sql} ${compiled.sql} ${hits}`;
  const anchors = [
    body.includes("$1::text[]") ? null : "coalesce(array_length($1::text[], 1), 0) >= 0",
    body.includes("$2::jsonb[]") ? null : "coalesce(array_length($2::jsonb[], 1), 0) >= 0",
  ].filter((a): a is string => a !== null);
  const anchor = anchors.length > 0 ? ` WHERE ${anchors.join(" AND ")}` : "";
  // The single-row ctx CTE joined laterally to the ordered rows: a nested loop over one outer
  // row preserves the inner ORDER BY, so no outer sort is needed. int8/xid8 arrive as strings.
  const sql = `WITH ctx AS (SELECT (${version.sql})::text AS v, pg_snapshot_xmin(pg_current_snapshot()) AS xmin${anchor})
SELECT ctx.v AS context_version,
       e.sequence_number AS seq, e.event_type, e.payload, e.metadata, e.recorded_at, e.xid, e.settled, e.hits
FROM ctx
LEFT JOIN LATERAL (
  SELECT sequence_number, event_type, payload, metadata, recorded_at,
         transaction_id AS xid, (${SETTLED_SQL}) AS settled, ${hits} AS hits
  FROM ${t}
  WHERE ${conds.join(" AND ")}
  ORDER BY ${orderBy}${limit}
) e ON true`;
  return { sql, params, filterCount };
}
