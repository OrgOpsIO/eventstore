/**
 * ## Postgres — what the store installs and how to operate it
 *
 * One table (`events` by default). Everything else is an index or a function on it:
 *
 * - `es_scope(payload, key)` — plpgsql IMMUTABLE, string-only: `scopes->>key`, else the
 *   top-level `key` field. Every declared scope key gets a partial B-tree on
 *   `(es_scope(payload,'K'), sequence_number)`. Because the function body feeds those indexes,
 *   its definition is fingerprinted (`COMMENT ON FUNCTION … 'es:<sha256>'`); when a newer SDK
 *   ships a different body the installer replaces it and `REINDEX`es the scope indexes.
 * - `es_append_if_v2(...)` — the conditional append (locks → fresh-snapshot version check →
 *   insert) in one round trip. `SECURITY INVOKER`, `search_path` pinned to the table's schema,
 *   `EXECUTE` revoked from `PUBLIC` and granted to the installing role plus `grantExecuteTo`.
 *   Requires READ COMMITTED (it raises `ES001` otherwise). Versioned by name; old unversioned
 *   functions are dropped once, never on every boot.
 * - `(event_type, sequence_number)`, `(transaction_id, sequence_number)`, one partial UNIQUE
 *   index per declared `unique` path, one UNIQUE index on the idempotency key (per tenant when
 *   a tenant scope key is configured), optional GIN `jsonb_path_ops` for ad-hoc `where`.
 * - optional row-level security on the tenant scope (`rls: true` + `tenantScopeKey`): the
 *   policy compares `es_scope(payload,'<tenantKey>')` with `current_setting('app.current_tenant')`;
 *   use `store.withTenant(id)` so every statement runs with that setting.
 *
 * Roles: install as the table owner (`install: "auto"`, the default; concurrent boots are
 * serialised by an advisory lock). Application roles need `SELECT, INSERT` on the table,
 * `USAGE` on its sequence and `EXECUTE` on the append function (`grantExecuteTo`). Locked-down
 * environments run `printSchemaSql()` through their migration tool and set `install: "none"`.
 *
 * Adding a scope key later creates an index with a `ShareLock` on the table; do it in a quiet
 * window or run `CREATE INDEX CONCURRENTLY` from `printSchemaSql()` by hand.
 *
 * Statistics: partial expression indexes hide their expression statistics from the planner,
 * so every scope key also gets `CREATE STATISTICS … ON es_scope(payload,'K')` (PG 14+). That
 * is what makes the planner pick the scope index in sequence order without a Sort node
 * (context read 430 → 1100 ops/s at c=1 in our bench).
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

export const APPEND_FUNCTION_VERSION = 2;

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

/** Versioned name of the append function: `es_append_if_v2` for `events`, `es_fn_<table>_append_if_v2` otherwise. */
export function appendFunctionName(table: string, version: number = APPEND_FUNCTION_VERSION): string {
  const suffix = version === 1 ? "" : `_v${version}`;
  return table === "events" ? `es_append_if${suffix}` : indexName(table, "append", `if${suffix}`).replace(/^es_/, "es_fn_");
}

export const SCOPE_FUNCTION_NAME = "es_scope";

// ── DDL ───────────────────────────────────────────────────────────────────────

export interface DdlOptions {
  readonly table: string;
  readonly adhocQueries?: boolean;
  readonly rls?: boolean;
  readonly tenantScopeKey?: string;
  /** Schema the table lives in; pins the functions' `search_path`. Default `public`. */
  readonly schemaName?: string;
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

export function tableDdl(table: string): string {
  const t = quoteIdent(assertIdentifier(table, "table name"));
  return `CREATE TABLE IF NOT EXISTS ${t} (
  sequence_number BIGSERIAL PRIMARY KEY,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  transaction_id XID8 NOT NULL DEFAULT pg_current_xact_id()
)`;
}

function searchPath(schemaName: string | undefined): string {
  const s = assertIdentifier(schemaName ?? "public", "schema name");
  return `pg_catalog, ${quoteIdent(s)}`;
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
 * DROPPED and recreated (a REINDEX keeps the old stored expression), which the installer does
 * when the fingerprint changes — see `scopeFunctionFingerprint`.
 */
export function scopeFunctionDdl(_schemaName?: string): string {
  // No `SET search_path`: a function with a config setting is never inlined. Instead every
  // operator and function in the body is schema-qualified, which is the same protection.
  return `CREATE OR REPLACE FUNCTION ${SCOPE_FUNCTION_NAME}(p jsonb, k text) RETURNS text
  LANGUAGE sql IMMUTABLE PARALLEL SAFE
  RETURN CASE
    WHEN pg_catalog.jsonb_typeof(p OPERATOR(pg_catalog.->) 'scopes' OPERATOR(pg_catalog.->) k) OPERATOR(pg_catalog.=) 'string'
      THEN p OPERATOR(pg_catalog.->) 'scopes' OPERATOR(pg_catalog.->>) k
    WHEN pg_catalog.jsonb_typeof(p OPERATOR(pg_catalog.->) k) OPERATOR(pg_catalog.=) 'string'
      THEN p OPERATOR(pg_catalog.->>) k
  END`;
}

export function scopeStatisticsName(table: string, key: string): string {
  return indexName(table, "stat", key);
}

/** Extended statistics on the scope expression: partial indexes hide theirs from the planner. */
export function scopeStatisticsDdl(table: string, key: string): string {
  const t = quoteIdent(assertIdentifier(table, "table name"));
  assertIdentifier(key, "scope key");
  return `CREATE STATISTICS IF NOT EXISTS ${quoteIdent(scopeStatisticsName(table, key))} ON ${SCOPE_FUNCTION_NAME}(payload, ${quoteLiteral(key)}) FROM ${t}`;
}

export function scopeFunctionFingerprint(schemaName?: string): string {
  return `es:${sha256(scopeFunctionDdl(schemaName))}`;
}

export function appendFunctionFingerprint(table: string, schemaName?: string): string {
  return `es:${sha256(appendFunctionDdl(table, schemaName))}`;
}

/** `'{seg1,seg2}'` literal for `payload #>> …` on a validated unique path. */
export function uniquePathSegmentsSql(path: string): string {
  return `'{${uniquePathSegments(path).join(",")}}'`;
}

export function scopeIndexDdl(table: string, key: string): string {
  const t = quoteIdent(assertIdentifier(table, "table name"));
  assertIdentifier(key, "scope key");
  const expr = `${SCOPE_FUNCTION_NAME}(payload, ${quoteLiteral(key)})`;
  return `CREATE INDEX IF NOT EXISTS ${quoteIdent(scopeIndexName(table, key))} ON ${t} ((${expr}), sequence_number) WHERE ${expr} IS NOT NULL`;
}

/** Statements that maintain access rights on the append function. */
export function grantStatements(table: string, options: Pick<DdlOptions, "grantExecuteTo">): string[] {
  const fn = quoteIdent(appendFunctionName(table));
  const signature = `${fn}(bigint[], bigint[], bigint, boolean, text, text[], jsonb[], bigint, text[], jsonb[], jsonb[])`;
  const statements = [`REVOKE EXECUTE ON FUNCTION ${signature} FROM PUBLIC`, `GRANT EXECUTE ON FUNCTION ${signature} TO CURRENT_USER`];
  for (const role of options.grantExecuteTo ?? []) {
    statements.push(`GRANT EXECUTE ON FUNCTION ${signature} TO ${quoteIdent(assertIdentifier(role, "role name"))}`);
  }
  return statements;
}

/**
 * Every DDL statement needed, idempotent, in order — the static picture for `printSchemaSql`.
 * The installer (`PostgresStore`) runs the same statements but replaces functions only when
 * their fingerprint changed (and reindexes the scope indexes when `es_scope` changed).
 */
export function ddlStatements(schema: StoreSchema, options: DdlOptions): string[] {
  const table = assertIdentifier(options.table, "table name");
  const t = quoteIdent(table);
  const statements: string[] = [];

  statements.push(tableDdl(table));
  statements.push(scopeFunctionDdl(options.schemaName));
  statements.push(`COMMENT ON FUNCTION ${SCOPE_FUNCTION_NAME}(jsonb, text) IS ${quoteLiteral(scopeFunctionFingerprint(options.schemaName))}`);

  if (options.typeIndex) {
    statements.push(`CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(table, "type", "seq"))} ON ${t} (event_type, sequence_number)`);
  }
  // cursor reads order by (transaction_id, sequence_number)
  statements.push(`CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(table, "xid", "seq"))} ON ${t} (transaction_id, sequence_number)`);

  for (const key of schema.scopeKeys) {
    statements.push(scopeIndexDdl(table, key));
    statements.push(scopeStatisticsDdl(table, key));
  }

  for (const u of uniqueIndexes(schema, table)) {
    const expr = `(payload #>> ${uniquePathSegmentsSql(u.path)})`;
    statements.push(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdent(u.name)} ON ${t} (${expr}) WHERE event_type = ${quoteLiteral(u.type)} AND ${expr} IS NOT NULL`,
    );
  }

  const tenantKey = schema.tenantScopeKey ?? options.tenantScopeKey;
  if (tenantKey) {
    assertIdentifier(tenantKey, "tenant scope key");
    statements.push(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdent(idempotencyIndexName(table))} ON ${t} ((${SCOPE_FUNCTION_NAME}(payload, ${quoteLiteral(tenantKey)})), (metadata->>'idempotencyKey')) WHERE metadata ? 'idempotencyKey'`,
    );
  } else {
    statements.push(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdent(idempotencyIndexName(table))} ON ${t} ((metadata->>'idempotencyKey')) WHERE metadata ? 'idempotencyKey'`,
    );
  }

  if (options.adhocQueries) {
    statements.push(`CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(table, "payload", "gin"))} ON ${t} USING GIN (payload jsonb_path_ops)`);
  }

  statements.push(appendFunctionDdl(table, options.schemaName));
  statements.push(
    `COMMENT ON FUNCTION ${quoteIdent(appendFunctionName(table))}(bigint[], bigint[], bigint, boolean, text, text[], jsonb[], bigint, text[], jsonb[], jsonb[]) IS ${quoteLiteral(appendFunctionFingerprint(table, options.schemaName))}`,
  );
  statements.push(...grantStatements(table, options));

  if (options.rls) {
    if (!tenantKey) throw new Error("eventstore/postgres: rls: true needs tenantScopeKey");
    const expr = `${SCOPE_FUNCTION_NAME}(payload, ${quoteLiteral(tenantKey)}) = current_setting('app.current_tenant', true)`;
    const policy = quoteIdent(indexName(table, "tenant", "isolation"));
    statements.push(`ALTER TABLE ${t} ENABLE ROW LEVEL SECURITY`);
    statements.push(`ALTER TABLE ${t} FORCE ROW LEVEL SECURITY`);
    statements.push(`DROP POLICY IF EXISTS ${policy} ON ${t}`);
    statements.push(`CREATE POLICY ${policy} ON ${t} USING (${expr}) WITH CHECK (${expr})`);
  }

  return statements;
}

/**
 * The conditional append, one round trip. VOLATILE plpgsql: every statement inside takes a
 * fresh snapshot under READ COMMITTED, so the MAX read in step 2 happens strictly after the
 * advisory locks of step 1 — a single-statement CTE cannot give that guarantee. Under
 * REPEATABLE READ or SERIALIZABLE the whole transaction shares one snapshot and the guard
 * would be stale, hence the isolation check. Do not declare it STABLE.
 */
export function appendFunctionDdl(table: string, schemaName?: string): string {
  const t = quoteIdent(assertIdentifier(table, "table name"));
  const fn = quoteIdent(appendFunctionName(table));
  return `CREATE OR REPLACE FUNCTION ${fn}(
  p_lock_keys bigint[],
  p_shared_keys bigint[],
  p_global_key bigint,
  p_exclusive_global boolean,
  p_version_sql text,
  p_texts text[],
  p_jsons jsonb[],
  p_expected bigint,
  p_types text[],
  p_payloads jsonb[],
  p_metadata jsonb[]
) RETURNS TABLE(ok boolean, actual bigint, first_seq bigint, last_seq bigint, cnt integer)
  LANGUAGE plpgsql VOLATILE SECURITY INVOKER
  SET search_path = ${searchPath(schemaName)}
  AS $fn$
DECLARE
  v_actual bigint := 0;
  v_first bigint;
  v_last bigint;
  v_count integer;
BEGIN
  -- 0. the guard is only sound when each statement sees a fresh snapshot
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION USING ERRCODE = 'ES001', MESSAGE = 'es_append_if requires READ COMMITTED';
  END IF;

  -- 1. locks first, in statements of their own (sorted to avoid deadlocks)
  IF p_exclusive_global THEN
    PERFORM pg_advisory_xact_lock(p_global_key);
  ELSE
    PERFORM pg_advisory_xact_lock_shared(p_global_key);
  END IF;
  -- lock order everywhere: global → shared tenant stamps → exclusive keys (sorted by the caller)
  PERFORM pg_advisory_xact_lock_shared(k) FROM (SELECT unnest(p_shared_keys) AS k ORDER BY 1) s;
  PERFORM pg_advisory_xact_lock(k) FROM (SELECT unnest(p_lock_keys) AS k ORDER BY 1) s;

  -- 2. the context version, read after the locks (fresh snapshot). p_version_sql is the
  --    statement compiled by compileVersionSql(): one MAX per scope value, so every branch is
  --    an index-backward scan on the scope index instead of a filtered walk of the primary key.
  IF p_version_sql IS NOT NULL THEN
    EXECUTE p_version_sql INTO v_actual USING p_texts, p_jsons;
    v_actual := COALESCE(v_actual, 0);
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

/**
 * Compiled condition: SQL that references `($1::text[])[i]` (text values) and `($2::jsonb[])[i]` (jsonb values).
 * The same text works inside the append function (`EXECUTE … USING p_texts, p_jsons`) and
 * in a prepared statement whose first two parameters are those arrays.
 */
export interface CompiledFilters {
  readonly texts: string[];
  readonly jsons: string[];
  /** One SQL condition per filter. */
  readonly perFilter: string[];
  /** All filters OR'ed. */
  readonly sql: string;
}

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

export function compileFilters(query: Query, collector: ParamCollector = createParamCollector()): CompiledFilters {
  const perFilter = filtersOf(query).map((f) => compileFilter(f, collector.text, collector.json));
  return { texts: collector.texts, jsons: collector.jsons, perFilter, sql: perFilter.map((c) => `(${c})`).join(" OR ") };
}

function compileFilter(filter: Filter, text: (v: string) => string, json: (v: unknown) => string): string {
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
      parts.push(
        list.length === 1
          ? `${SCOPE_FUNCTION_NAME}(payload, ${quoteLiteral(key)}) = ${list[0]}`
          : `${SCOPE_FUNCTION_NAME}(payload, ${quoteLiteral(key)}) IN (${list.join(", ")})`,
      );
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
 * The CCC context version as one statement returning a single bigint, using the same
 * `$1::text[]` / `$2::jsonb[]` parameter arrays as `compileFilters`.
 *
 * For a filter with scope keys the MAX is split into one subquery per value of the leading
 * scope key, each a scalar equality — Postgres then answers each branch with a backward scan
 * of the `(es_scope(payload,'K'), sequence_number)` index (4 buffers) instead of walking the
 * primary key backwards with a filter (thousands of buffers, as measured on 800k rows).
 */
export function compileVersionSql(
  table: string,
  query: Query,
  collector: ParamCollector = createParamCollector(),
  schema?: Pick<StoreSchema, "tenantScopeKey">,
): { sql: string; texts: string[]; jsons: string[] } {
  const t = quoteIdent(assertIdentifier(table, "table name"));
  const { texts, jsons } = collector;
  const text = collector.text;
  const json = collector.json;
  const branches: string[] = [];
  for (const filter of filtersOf(query)) {
    const leading = leadingScopeKey(filter, schema?.tenantScopeKey);
    if (leading === undefined) {
      branches.push(`SELECT MAX(sequence_number) FROM ${t} WHERE ${compileFilter(filter, text, json)}`);
      continue;
    }
    assertIdentifier(leading, "scope key");
    const rest: Filter = { ...filter, scopes: Object.fromEntries(Object.entries(filter.scopes!).filter(([k]) => k !== leading)) };
    if (Object.keys(rest.scopes!).length === 0) delete (rest as { scopes?: unknown }).scopes;
    const values = filter.scopes![leading]!;
    for (const value of typeof values === "string" ? [values] : values) {
      const restSql = compileFilter(rest, text, json);
      branches.push(
        `SELECT MAX(sequence_number) FROM ${t} WHERE ${SCOPE_FUNCTION_NAME}(payload, ${quoteLiteral(leading)}) = ${text(value)}${restSql === "TRUE" ? "" : ` AND ${restSql}`}`,
      );
    }
  }
  // One branch: the bare MAX (no UNION ALL wrapper, −19 µs per guarded append). Inside the
  // append function unreferenced USING parameters are fine, so no anchor is needed here.
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
export function compileQuery(table: string, query: Query, options: QueryOptions, schema?: Pick<StoreSchema, "tenantScopeKey">): CompiledQuery {
  const t = quoteIdent(assertIdentifier(table, "table name"));
  const collector = createParamCollector();
  const compiled = compileFilters(query, collector);
  const version = compileVersionSql(table, query, collector, schema); // same collector: one parameter space
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
