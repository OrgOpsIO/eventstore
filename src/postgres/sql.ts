import { createHash } from "node:crypto";
import { filtersOf, uniquePathSegments } from "../query.js";
import type { Filter, Query, QueryOptions, StoreSchema } from "../types.js";

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

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

/** Deterministic index/function names, ≤ 63 chars. */
export function indexName(table: string, kind: string, detail: string): string {
  const base = `es_${table}_${kind}_${detail}`.replace(/[^A-Za-z0-9_]/g, "_");
  if (base.length <= 63) return base;
  return `${base.slice(0, 54)}_${short(base)}`;
}

export function appendFunctionName(table: string): string {
  return table === "events" ? "es_append_if" : indexName(table, "append", "if").replace(/^es_/, "es_fn_");
}

// ── DDL ───────────────────────────────────────────────────────────────────────

export interface DdlOptions {
  readonly table: string;
  readonly adhocQueries?: boolean;
  readonly rls?: boolean;
  readonly tenantScopeKey?: string;
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

/** Every DDL statement needed, idempotent, in order. */
export function ddlStatements(schema: StoreSchema, options: DdlOptions): string[] {
  const table = assertIdentifier(options.table, "table name");
  const t = quoteIdent(table);
  const statements: string[] = [];

  statements.push(
    `CREATE TABLE IF NOT EXISTS ${t} (
  sequence_number BIGSERIAL PRIMARY KEY,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  transaction_id XID8 NOT NULL DEFAULT pg_current_xact_id()
)`,
  );

  statements.push(
    `CREATE OR REPLACE FUNCTION es_scope(p jsonb, k text) RETURNS text
  LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $fn$
BEGIN
  RETURN COALESCE(p->'scopes'->>k, p->>k);
END
$fn$`,
  );

  statements.push(
    `CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(table, "type", "seq"))} ON ${t} (event_type, sequence_number)`,
  );
  statements.push(
    `CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(table, "xid", "seq"))} ON ${t} (transaction_id, sequence_number)`,
  );

  for (const key of schema.scopeKeys) {
    assertIdentifier(key, "scope key");
    const expr = `es_scope(payload, ${quoteLiteral(key)})`;
    statements.push(
      `CREATE INDEX IF NOT EXISTS ${quoteIdent(scopeIndexName(table, key))} ON ${t} ((${expr}), sequence_number) WHERE ${expr} IS NOT NULL`,
    );
  }

  for (const u of uniqueIndexes(schema, table)) {
    const segments = uniquePathSegments(u.path);
    const pathLiteral = `'{${segments.join(",")}}'`;
    const expr = `(payload #>> ${pathLiteral})`;
    statements.push(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdent(u.name)} ON ${t} (${expr}) WHERE event_type = ${quoteLiteral(u.type)} AND ${expr} IS NOT NULL`,
    );
  }

  statements.push(
    `CREATE UNIQUE INDEX IF NOT EXISTS ${quoteIdent(idempotencyIndexName(table))} ON ${t} ((metadata->>'idempotencyKey')) WHERE metadata ? 'idempotencyKey'`,
  );

  if (options.adhocQueries) {
    statements.push(
      `CREATE INDEX IF NOT EXISTS ${quoteIdent(indexName(table, "payload", "gin"))} ON ${t} USING GIN (payload jsonb_path_ops)`,
    );
  }

  statements.push(`DROP FUNCTION IF EXISTS ${quoteIdent(appendFunctionName(table))}(bigint[], bigint, boolean, text, text[], jsonb[], bigint, text[], jsonb[], jsonb[])`);
  statements.push(appendFunctionDdl(table));

  if (options.rls) {
    if (!options.tenantScopeKey) throw new Error("eventstore/postgres: rls: true needs tenantScopeKey");
    const key = assertIdentifier(options.tenantScopeKey, "tenant scope key");
    const expr = `es_scope(payload, ${quoteLiteral(key)}) = current_setting('app.current_tenant', true)`;
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
 * advisory locks of step 1 — a single-statement CTE cannot give that guarantee.
 */
export function appendFunctionDdl(table: string): string {
  const t = quoteIdent(assertIdentifier(table, "table name"));
  const fn = quoteIdent(appendFunctionName(table));
  return `CREATE OR REPLACE FUNCTION ${fn}(
  p_lock_keys bigint[],
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
  LANGUAGE plpgsql VOLATILE AS $fn$
DECLARE
  v_actual bigint := 0;
  v_first bigint;
  v_last bigint;
  v_count integer;
BEGIN
  -- 1. locks first, in a statement of their own (sorted to avoid deadlocks)
  IF p_exclusive_global THEN
    PERFORM pg_advisory_xact_lock(p_global_key);
  ELSE
    PERFORM pg_advisory_xact_lock_shared(p_global_key);
  END IF;
  PERFORM pg_advisory_xact_lock(k) FROM unnest(p_lock_keys) AS k ORDER BY k;

  -- 2. the context version, read after the locks (fresh snapshot). p_version_sql is the
  --    statement compiled by compileVersionSql(): one MAX per scope value, so every branch is
  --    an index-backward scan on the scope index instead of a filtered walk of the primary key.
  IF p_version_sql IS NOT NULL THEN
    EXECUTE p_version_sql INTO v_actual USING p_texts, p_jsons;
    IF v_actual <> p_expected THEN
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
          ? `es_scope(payload, ${quoteLiteral(key)}) = ${list[0]}`
          : `es_scope(payload, ${quoteLiteral(key)}) IN (${list.join(", ")})`,
      );
    }
  }
  if (filter.where && filter.where.length > 0) {
    parts.push(`(${filter.where.map((w) => `payload @> ${json(w)}::jsonb`).join(" OR ")})`);
  }
  return parts.length > 0 ? parts.join(" AND ") : "TRUE";
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
): { sql: string; texts: string[]; jsons: string[] } {
  const t = quoteIdent(assertIdentifier(table, "table name"));
  const { texts, jsons } = collector;
  const text = collector.text;
  const json = collector.json;
  const branches: string[] = [];
  for (const filter of filtersOf(query)) {
    const scopeKeys = Object.keys(filter.scopes ?? {});
    if (scopeKeys.length === 0) {
      branches.push(`SELECT MAX(sequence_number) FROM ${t} WHERE ${compileFilter(filter, text, json)}`);
      continue;
    }
    const leading = scopeKeys[0]!;
    const rest: Filter = { ...filter, scopes: Object.fromEntries(Object.entries(filter.scopes!).filter(([k]) => k !== leading)) };
    if (Object.keys(rest.scopes!).length === 0) delete (rest as { scopes?: unknown }).scopes;
    const values = filter.scopes![leading]!;
    for (const value of typeof values === "string" ? [values] : values) {
      assertIdentifier(leading, "scope key");
      const restSql = compileFilter(rest, text, json);
      branches.push(
        `SELECT MAX(sequence_number) FROM ${t} WHERE es_scope(payload, ${quoteLiteral(leading)}) = ${text(value)}${restSql === "TRUE" ? "" : ` AND ${restSql}`}`,
      );
    }
  }
  const anchor = "coalesce(array_length($1::text[], 1), 0) >= 0 AND coalesce(array_length($2::jsonb[], 1), 0) >= 0";
  const sql = `SELECT COALESCE(MAX(m), 0) FROM (${branches.map((b) => `(${b})`).join(" UNION ALL ")}) AS branches(m) WHERE ${anchor}`;
  return { sql, texts, jsons };
}

export const SETTLED_SQL = "transaction_id < pg_snapshot_xmin(pg_current_snapshot())";

export interface CompiledQuery {
  readonly sql: string;
  readonly params: unknown[];
}

/**
 * One statement that always returns at least one row (context version), with the matching
 * records joined laterally so `limit`/`order` apply only to the rows.
 */
export function compileQuery(table: string, query: Query, options: QueryOptions): CompiledQuery {
  const t = quoteIdent(assertIdentifier(table, "table name"));
  const collector = createParamCollector();
  const compiled = compileFilters(query, collector);
  const version = compileVersionSql(table, query, collector); // same collector: one parameter space
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
  const order = options.order === "desc" ? "DESC" : "ASC";
  const limit = options.limit !== undefined ? ` LIMIT ${param(String(options.limit))}::bigint` : "";
  const hits = `ARRAY[${compiled.perFilter.map((c) => `(${c})`).join(", ")}]::boolean[]`;
  // Both parameter arrays are always referenced so the statement's parameter count is stable
  // even when a query has no text or no jsonb values.
  const sql = `WITH ctx AS (SELECT (${version.sql})::text AS v)
SELECT ctx.v AS context_version,
       e.seq, e.event_type, e.payload, e.metadata, e.recorded_at, e.xid, e.settled, e.hits
FROM ctx
LEFT JOIN LATERAL (
  SELECT sequence_number::text AS seq, event_type, payload, metadata, recorded_at,
         transaction_id::text AS xid, (${SETTLED_SQL}) AS settled, ${hits} AS hits
  FROM ${t}
  WHERE ${conds.join(" AND ")}
  ORDER BY sequence_number ${order}${limit}
) e ON true
ORDER BY e.seq::bigint ${order} NULLS LAST`;
  return { sql, params };
}
