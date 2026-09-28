import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { z } from "zod";
import { buildSchema, defineEvents } from "../src/index.js";
import {
  PostgresStore,
  SCOPE_BUILTIN_FUNCTIONS,
  SCOPE_LEAKPROOF_MISSING_SQL,
  ddlStatements,
  printSchemaSql,
  scopeLeakproofStatements,
  scopeRebuildStatements,
  scopeStatisticsKeys,
} from "../src/postgres/index.js";

const url = process.env.ES_TEST_DATABASE_URL;

const ledger = defineEvents({
  LedgerOpened: { data: z.object({ name: z.string() }), scopes: ["tenantId"] },
  EntryBooked: { data: z.object({ amount: z.number() }), scopes: ["ledgerOpenedId", "tenantId"], optionalScopes: ["batchId"] },
});
const schema = buildSchema([ledger], { strict: true, tenantScopeKey: "tenantId" });

describe("scope statistics: which keys get an object", () => {
  it("all keys by default, in ddlStatements and in the size policy's static listing", () => {
    const ddl = ddlStatements(schema, { table: "events", tenantScopeKey: "tenantId" }).join("\n");
    for (const key of schema.scopeKeys) expect(ddl).toContain(`"es_events_stat_${key}"`);
    // printSchemaSql cannot see the data: every key without a known row count keeps its object
    const printed = printSchemaSql(schema, { scopeStatistics: { minIndexRows: 1000, exclude: ["tenantId"] } });
    expect(printed).not.toContain('"es_events_stat_tenantId"');
    expect(printed).toContain('"es_events_stat_ledgerOpenedId"');
  });

  it("an explicit list limits CREATE STATISTICS, never the indexes", () => {
    const ddl = ddlStatements(schema, { table: "events", tenantScopeKey: "tenantId", scopeStatistics: ["ledgerOpenedId"] });
    const statistics = ddl.filter((s) => s.startsWith("CREATE STATISTICS"));
    expect(statistics).toHaveLength(1);
    expect(statistics[0]).toContain('"es_events_stat_ledgerOpenedId"');
    for (const key of schema.scopeKeys) expect(ddl.some((s) => s.includes(`"es_events_scope_${key}"`))).toBe(true);
  });

  it("the size policy keeps large indexes and unknown counts, drops small ones and exclusions", () => {
    const rows = new Map([
      ["tenantId", 50_000],
      ["ledgerOpenedId", 4_000],
      ["batchId", 12],
    ]);
    expect(scopeStatisticsKeys(schema.scopeKeys, rows, { minIndexRows: 1000, exclude: ["tenantId"] })).toEqual(["ledgerOpenedId"]);
    // never counted (reltuples −1) or not there yet: unknown is not small
    expect(scopeStatisticsKeys(["a", "b", "c"], new Map([["a", -1], ["c", 5]]), { minIndexRows: 1000 })).toEqual(["a", "b"]);
    expect(scopeStatisticsKeys(schema.scopeKeys, rows)).toEqual([...schema.scopeKeys]);
  });

  it("a rebuild recreates only the chosen objects", () => {
    const statements = scopeRebuildStatements(schema, { schemaName: "public", table: "events" }, ["ledgerOpenedId"]);
    expect(statements.filter((s) => s.startsWith("CREATE STATISTICS"))).toHaveLength(1);
    expect(statements.filter((s) => s.startsWith("DROP STATISTICS"))).toHaveLength(schema.scopeKeys.length);
  });
});

describe("LEAKPROOF for row-level security", () => {
  it("names exactly the three builtins the inlined es_scope calls, and printSchemaSql lists them under rls", () => {
    expect(scopeLeakproofStatements()).toEqual([
      "ALTER FUNCTION pg_catalog.jsonb_typeof(jsonb) LEAKPROOF",
      "ALTER FUNCTION pg_catalog.jsonb_object_field(jsonb, text) LEAKPROOF",
      "ALTER FUNCTION pg_catalog.jsonb_object_field_text(jsonb, text) LEAKPROOF",
    ]);
    expect(printSchemaSql(schema, { rls: true })).toContain("ALTER FUNCTION pg_catalog.jsonb_typeof(jsonb) LEAKPROOF;");
    expect(printSchemaSql(schema)).not.toContain("LEAKPROOF");
  });
});

const TABLE = "es_scope_rls";
const APP_ROLE = "es_app";
const OWNER_ROLE = "es_owner_nosuper";

describe.skipIf(!url)("PostgresStore with rls: scope indexes for a non-owner role", () => {
  let admin: pg.Pool;
  let owner: PostgresStore;
  let app: PostgresStore;

  async function missingLeakproof(): Promise<string[]> {
    const r = await admin.query<{ signature: string }>(SCOPE_LEAKPROOF_MISSING_SQL, [SCOPE_BUILTIN_FUNCTIONS]);
    return r.rows.map((row) => row.signature);
  }
  async function setLeakproof(on: boolean): Promise<void> {
    for (const signature of SCOPE_BUILTIN_FUNCTIONS) await admin.query(`ALTER FUNCTION ${signature} ${on ? "" : "NOT "}LEAKPROOF`);
  }

  /** The plan of the app role's read, in a transaction the superuser opens and rolls back. */
  async function appPlan(sql: string, params: unknown[], prepare: string[] = []): Promise<string> {
    const client = await admin.connect();
    try {
      await client.query("BEGIN");
      for (const statement of prepare) await client.query(statement);
      await client.query(`SET LOCAL ROLE ${APP_ROLE}`);
      await client.query("SELECT set_config('app.current_tenant', 'tenant-a', true)");
      const r = await client.query<{ "QUERY PLAN": string }>(`EXPLAIN ${sql}`, params);
      return r.rows.map((row) => row["QUERY PLAN"]).join("\n");
    } finally {
      await client.query("ROLLBACK");
      client.release();
    }
  }

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}" CASCADE`);
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}_nosuper" CASCADE`);
    await admin.query(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${APP_ROLE}') THEN CREATE ROLE ${APP_ROLE} LOGIN PASSWORD '${APP_ROLE}'; END IF; END $$`);
    // the mark is per database: start from Postgres' default to see the installer set it
    await setLeakproof(false);
    owner = new PostgresStore({ connection: url!, schema, table: TABLE, rls: true, grantExecuteTo: [APP_ROLE], poolSize: 4 });
    await owner.ensureInstalled();
    await owner.withClient(async (c) => {
      await c.query(`GRANT USAGE ON SCHEMA public TO ${APP_ROLE}`);
      await c.query(`GRANT SELECT, INSERT ON "${TABLE}" TO ${APP_ROLE}`);
      await c.query(`GRANT USAGE, SELECT ON SEQUENCE "${TABLE}_sequence_number_seq" TO ${APP_ROLE}`);
    });
    const appUrl = new URL(url!);
    appUrl.username = APP_ROLE;
    appUrl.password = APP_ROLE;
    app = new PostgresStore({ connection: appUrl.toString(), schema, table: TABLE, rls: true, install: "none", poolSize: 2 });
    // enough rows that an index is a choice: 2 000 entries in one tenant over 400 ledgers
    const a = app.withTenant("tenant-a");
    const entries = Array.from({ length: 2000 }, (_, i) =>
      ledger.EntryBooked({ amount: i }, { ledgerOpenedId: `ledger-${i % 400}`, tenantId: "tenant-a", ...(i < 30 ? { batchId: "batch-1" } : {}) }),
    );
    await a.append(entries);
    await admin.query(`ANALYZE "${TABLE}"`);
  });

  afterAll(async () => {
    await setLeakproof(true);
    await app?.close();
    await owner?.close();
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}_nosuper" CASCADE`);
    await admin.query(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${OWNER_ROLE}') THEN REVOKE CREATE ON SCHEMA public FROM ${OWNER_ROLE}; DROP OWNED BY ${OWNER_ROLE}; DROP ROLE ${OWNER_ROLE}; END IF; END $$`);
    await admin.end();
  });

  it("the installer, running as a superuser, marks the three functions LEAKPROOF", async () => {
    expect(await missingLeakproof()).toEqual([]);
    expect(owner.warnings().some((w) => w.includes("LEAKPROOF"))).toBe(false);
  });

  it("with the mark, the app role reads a non-tenant scope through its index; without it, through the tenant's", async () => {
    const read = `SELECT sequence_number FROM "${TABLE}" WHERE es_scope(payload, 'ledgerOpenedId') = $1 ORDER BY sequence_number DESC LIMIT 1`;
    expect(await appPlan(read, ["ledger-7"])).toMatch(/Index Scan.*es_es_scope_rls_scope_ledgerOpenedId/s);
    // the premise, rolled back: Postgres' default leaves only the policy's own index usable
    const without = await appPlan(read, ["ledger-7"], SCOPE_BUILTIN_FUNCTIONS.map((s) => `ALTER FUNCTION ${s} NOT LEAKPROOF`));
    expect(without).not.toMatch(/es_es_scope_rls_scope_ledgerOpenedId/);
  });

  it("the mark does not open the policy: another tenant's session sees none of these rows", async () => {
    const b = app.withTenant("tenant-b");
    expect((await b.query({ scopes: { ledgerOpenedId: "ledger-7" } })).events).toEqual([]);
    expect((await app.withTenant("tenant-a").query({ scopes: { ledgerOpenedId: "ledger-7" } })).events).toHaveLength(5);
  });

  it("an install without superuser rights records a warning with the statements to run", async () => {
    await setLeakproof(false);
    await admin.query(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${OWNER_ROLE}') THEN CREATE ROLE ${OWNER_ROLE} LOGIN PASSWORD '${OWNER_ROLE}' NOSUPERUSER; END IF; END $$`);
    await admin.query(`GRANT CREATE ON SCHEMA public TO ${OWNER_ROLE}`);
    const ownerUrl = new URL(url!);
    ownerUrl.username = OWNER_ROLE;
    ownerUrl.password = OWNER_ROLE;
    const plain = new PostgresStore({ connection: ownerUrl.toString(), schema, table: `${TABLE}_nosuper`, rls: true, poolSize: 1 });
    try {
      await plain.ensureInstalled();
      const warning = plain.warnings().find((w) => w.includes("LEAKPROOF"));
      expect(warning).toContain("ALTER FUNCTION pg_catalog.jsonb_typeof(jsonb) LEAKPROOF");
      expect(await missingLeakproof()).toHaveLength(3);
    } finally {
      await plain.close();
      await setLeakproof(true);
    }
  });

  it("scopeStatistics by size: objects only where the index holds enough rows, the rest dropped", async () => {
    const statistics = async () =>
      (
        await admin.query<{ name: string }>(`SELECT stxname AS name FROM pg_statistic_ext WHERE stxrelid = '"${TABLE}"'::regclass ORDER BY 1`)
      ).rows.map((r) => r.name);
    expect(await statistics()).toEqual(schema.scopeKeys.map((k) => `es_es_scope_rls_stat_${k}`).sort());

    const sized = new PostgresStore({
      connection: url!,
      schema,
      table: TABLE,
      rls: true,
      grantExecuteTo: [APP_ROLE],
      poolSize: 1,
      scopeStatistics: { minIndexRows: 1000, exclude: ["tenantId"] },
    });
    try {
      await sized.ensureInstalled();
    } finally {
      await sized.close();
    }
    // ledgerOpenedId: 2 000 rows; batchId: 30; tenantId excluded
    expect(await statistics()).toEqual(["es_es_scope_rls_stat_ledgerOpenedId"]);
  });
});
