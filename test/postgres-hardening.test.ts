import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { z } from "zod";
import { buildSchema, defineEvents, UsageError, UniqueViolationError } from "../src/index.js";
import { APPEND_SIGNATURE, PostgresStore, appendFunctionName, printSchemaSql, scopeFunctionFingerprint } from "../src/postgres/index.js";

const url = process.env.ES_TEST_DATABASE_URL;

const events = defineEvents({
  AccountOpened: { data: z.object({ owner: z.string() }), unique: ["owner"] },
  MoneyDeposited: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"], optionalScopes: ["tenantId"] },
  NoteAdded: { data: z.object({ text: z.string() }), optionalScopes: ["accountOpenedId", "tenantId"] },
});
const schema = buildSchema([events], { tenantScopeKey: "tenantId", strict: false });

describe.skipIf(!url)("PostgresStore hardening", () => {
  const TABLE = "es_hard";
  let store: PostgresStore;
  let admin: pg.Pool;

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}" CASCADE`);
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}_b" CASCADE`);
    await admin.query(`DROP SCHEMA IF EXISTS es_alt CASCADE`);
    // rebuildScopeIndexes: an aborted earlier run may have left a foreign es_scope behind (scratch DB)
    store = new PostgresStore({ connection: url!, schema, table: TABLE, poolSize: 8, rebuildScopeIndexes: true });
    await store.ensureInstalled();
  });
  afterAll(async () => {
    await store.close();
    await admin.end();
  });

  it("the function's version check agrees with the read path for every query shape", async () => {
    const a = events.AccountOpened({ owner: "a1" }, {}, { id: "h-a1" });
    const b = events.AccountOpened({ owner: "b1" }, {}, { id: "h-b1" });
    await store.append([a, b]);
    await store.append([events.MoneyDeposited({ amount: 1 }, { accountOpenedId: "h-a1", tenantId: "T1" })]);
    await store.append([events.NoteAdded({ text: "n" }, { accountOpenedId: "h-a1" })]);
    await store.append([events.MoneyDeposited({ amount: 2 }, { accountOpenedId: "h-b1", tenantId: "T2" })]);
    const shapes: import("../src/index.js").Query[] = [
      { types: ["NoteAdded"] },
      { scopes: { accountOpenedId: "h-a1" } },
      { types: ["MoneyDeposited", "NoteAdded"], scopes: { accountOpenedId: "h-a1" } },
      { types: ["MoneyDeposited"], scopes: { accountOpenedId: ["h-a1", "h-b1"] } },
      { scopes: { accountOpenedId: "h-a1", tenantId: "T1" } },
      { scopes: { tenantId: "T2" } },
      { types: ["AccountOpened"], where: [{ owner: "b1" }] },
      [{ types: ["NoteAdded"] }, { scopes: { accountOpenedId: "h-b1" } }],
      { types: ["MoneyDeposited"], scopes: { accountOpenedId: "h-a1" }, where: [{ amount: 1 }] },
      { types: [] as string[] },
      { scopes: { accountOpenedId: "nobody" } },
    ];
    for (const shape of shapes) {
      const read = await store.query(shape);
      // an impossible expected version makes the function report what it computed
      const probe = await store.appendIf([events.NoteAdded({ text: "probe" })], { query: shape, version: -1 });
      expect(probe.ok).toBe(false);
      if (!probe.ok) expect(probe.conflict.actual, JSON.stringify(shape)).toBe(read.contextVersion);
    }
  });

  it("refuses a malformed lock plan (ES002 → UsageError)", async () => {
    const err = await store.withClient(async (c) => {
      try {
        await c.query(`SELECT * FROM "${appendFunctionName(TABLE)}"($1::bigint[], $2::boolean[], 1, false, NULL::jsonb, 0, $3::text[], $4::jsonb[], $5::jsonb[])`, [
          ["1", "2"],
          [true],
          ["NoteAdded"],
          ['{"noteAddedId":"m","text":"x"}'],
          ["{}"],
        ]);
        return null;
      } catch (e) {
        return e as { code?: string };
      }
    });
    expect(err?.code).toBe("ES002");
    // the SDK never builds one, but a NULL key would silently take no lock
    const nullKey = await store.withClient(async (c) => {
      try {
        await c.query(`SELECT * FROM "${appendFunctionName(TABLE)}"($1::bigint[], $2::boolean[], 1, false, NULL::jsonb, 0, $3::text[], $4::jsonb[], $5::jsonb[])`, [
          [null],
          [true],
          ["NoteAdded"],
          ['{"noteAddedId":"m2","text":"x"}'],
          ["{}"],
        ]);
        return null;
      } catch (e) {
        return e as { code?: string };
      }
    });
    expect(nullKey?.code).toBe("ES002");
  });

  it("installs into another schema with fully qualified DDL and queries, and the scope index is used there", async () => {
    const alt = new PostgresStore({ connection: url!, schema, table: "events", schemaName: "es_alt", poolSize: 4 });
    await alt.append([events.AccountOpened({ owner: "alt" }, {}, { id: "alt-1" })]);
    for (let i = 0; i < 300; i++) await alt.append([events.MoneyDeposited({ amount: i }, { accountOpenedId: `alt-${i % 30}` })]);
    const read = await alt.query(events.$scope("accountOpenedId", "alt-1"));
    expect(read.events.map((e) => e.type)).toEqual(["AccountOpened", "MoneyDeposited", ...Array(9).fill("MoneyDeposited")]);
    const fns = await admin.query("SELECT n.nspname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname = 'es_scope' ORDER BY 1");
    expect(fns.rows.map((r) => r.nspname)).toContain("es_alt");
    await alt.withClient(async (c) => {
      await c.query('ANALYZE "es_alt"."events"');
      const plan = await c.query(
        `EXPLAIN SELECT * FROM "es_alt"."events" WHERE "es_alt"."es_scope"(payload, 'accountOpenedId') = 'alt-1' ORDER BY sequence_number`,
      );
      expect(plan.rows.map((r) => r["QUERY PLAN"]).join("\n")).toMatch(/es_events_scope_accountOpenedId/);
    });
    expect(printSchemaSql(schema, { schemaName: "es_alt" })).toContain('"es_alt"."events"');
    await alt.close();
  });

  it("collapses an oversized lock plan to the global lock and records a warning", async () => {
    const small = new PostgresStore({ connection: url!, schema, table: TABLE, maxLockKeys: 5, poolSize: 2 });
    const batch = Array.from({ length: 10 }, (_, i) => events.MoneyDeposited({ amount: i }, { accountOpenedId: `cap-${i}` }));
    const plan = small.lockPlan(batch, null);
    expect(plan.collapsed).toBe(true);
    expect(plan.keys).toEqual([]);
    expect(plan.exclusiveGlobal).toBe(true);
    await small.append(batch); // still correct, just coarse
    expect(small.lockPlanCollapses).toBe(2);
    expect(small.warnings().some((w) => w.includes("maxLockKeys"))).toBe(true);
    const fine = small.lockPlan(batch.slice(0, 3), null);
    expect(fine.collapsed).toBe(false);
    await small.close();
  });

  it("recreates the RLS policy when USING or WITH CHECK drifted, leaves it alone otherwise", async () => {
    const rlsTable = "es_hard_rls";
    await admin.query(`DROP TABLE IF EXISTS "${rlsTable}" CASCADE`);
    const a = new PostgresStore({ connection: url!, schema, table: rlsTable, rls: true, poolSize: 2 });
    await a.ensureInstalled();
    const policy = `es_${rlsTable}_tenant_isolation`;
    const before = await admin.query("SELECT oid FROM pg_policy WHERE polname = $1", [policy]);
    // a WITH CHECK (true) would let cross-tenant inserts through: must be detected
    await admin.query(`DROP POLICY "${policy}" ON "${rlsTable}"`);
    await admin.query(`CREATE POLICY "${policy}" ON "${rlsTable}" USING (es_scope(payload, 'tenantId') = current_setting('app.current_tenant', true)) WITH CHECK (true)`);
    const b = new PostgresStore({ connection: url!, schema, table: rlsTable, rls: true, poolSize: 2 });
    await b.ensureInstalled();
    const after = await admin.query("SELECT with_check FROM pg_policies WHERE policyname = $1", [policy]);
    expect(String(after.rows[0]?.with_check)).toContain("app.current_tenant");
    const c = new PostgresStore({ connection: url!, schema, table: rlsTable, rls: true, poolSize: 2 });
    await c.ensureInstalled();
    const stable = await admin.query("SELECT oid FROM pg_policy WHERE polname = $1", [policy]);
    expect(stable.rows[0]).not.toEqual(before.rows[0]); // recreated once (drift)…
    const d = new PostgresStore({ connection: url!, schema, table: rlsTable, rls: true, poolSize: 2 });
    await d.ensureInstalled();
    const still = await admin.query("SELECT oid FROM pg_policy WHERE polname = $1", [policy]);
    expect(still.rows[0]).toEqual(stable.rows[0]); // …and not again
    await Promise.all([a.close(), b.close(), c.close(), d.close()]);
  });

  it("a second table on the same schema detects its own stale indexes after the function was replaced", async () => {
    const tableB = `${TABLE}_b`;
    const b = new PostgresStore({ connection: url!, schema, table: tableB, poolSize: 2 });
    await b.ensureInstalled();
    await b.append([events.MoneyDeposited({ amount: 1 }, { accountOpenedId: "b-1" })]);
    // simulate: some other deployment replaced es_scope with a different body and re-stamped it
    await admin.query(`CREATE OR REPLACE FUNCTION public.es_scope(p jsonb, k text) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE RETURN p OPERATOR(pg_catalog.->) 'scopes' OPERATOR(pg_catalog.->>) k`);
    await admin.query("COMMENT ON FUNCTION public.es_scope(jsonb, text) IS 'es:someone-else'");
    // table B's index comments still carry the old fingerprint → its boot must refuse
    const stale = new PostgresStore({ connection: url!, schema, table: tableB, poolSize: 2 });
    await expect(stale.ensureInstalled()).rejects.toThrow(/es_scope|CREATE INDEX CONCURRENTLY/);
    // …and a rebuild restores function + indexes + idempotency index + statistics
    const rebuilt = new PostgresStore({ connection: url!, schema, table: tableB, rebuildScopeIndexes: true, poolSize: 2 });
    await rebuilt.ensureInstalled();
    const fp = scopeFunctionFingerprint();
    const comments = await admin.query(
      "SELECT c.relname, obj_description(c.oid, 'pg_class') AS fp FROM pg_class c WHERE c.relname IN ($1, $2, $3) ORDER BY 1",
      [`es_${tableB}_scope_accountOpenedId`, `es_${tableB}_scope_tenantId`, `es_${tableB}_idem_key`],
    );
    expect(comments.rows.map((r) => r.fp)).toEqual([fp, fp, fp]);
    const stats = await admin.query("SELECT count(*)::int AS n FROM pg_statistic_ext WHERE stxname LIKE $1", [`es_${tableB}_stat_%`]);
    expect(stats.rows[0]?.n).toBe(2);
    // the first table's indexes were built on OUR body, which is installed again now: consistent, no rebuild
    const first = new PostgresStore({ connection: url!, schema, table: TABLE, poolSize: 2 });
    await first.ensureInstalled();
    expect(first.warnings()).toEqual([]);
    await Promise.all([b.close(), stale.close(), rebuilt.close(), first.close()]);
  });

  it("premise of the gate: a changed body over an existing (non-inlined) expression index silently loses rows", async () => {
    await admin.query("DROP TABLE IF EXISTS es_premise");
    await admin.query("CREATE TABLE es_premise (id serial primary key, payload jsonb not null)");
    await admin.query("CREATE OR REPLACE FUNCTION es_premise_scope(p jsonb, k text) RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN p->'scopes'->>k; END $$");
    await admin.query("CREATE INDEX es_premise_idx ON es_premise ((es_premise_scope(payload, 'k')))");
    await admin.query(`INSERT INTO es_premise (payload) SELECT jsonb_build_object('k', 'flat-' || g) FROM generate_series(1, 2000) g`);
    await admin.query(`INSERT INTO es_premise (payload) SELECT jsonb_build_object('scopes', jsonb_build_object('k', 's-' || g)) FROM generate_series(1, 2000) g`);
    await admin.query("ANALYZE es_premise");
    // the body changes to ALSO read flat fields — the index was built without them
    await admin.query("CREATE OR REPLACE FUNCTION es_premise_scope(p jsonb, k text) RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN COALESCE(p->'scopes'->>k, p->>k); END $$");
    const viaIndex = await admin.query("SET enable_seqscan = off; SELECT count(*)::int AS n FROM es_premise WHERE es_premise_scope(payload, 'k') = 'flat-7'");
    const truth = await admin.query("SET enable_seqscan = on; SET enable_indexscan = off; SET enable_bitmapscan = off; SELECT count(*)::int AS n FROM es_premise WHERE es_premise_scope(payload, 'k') = 'flat-7'");
    const idxN = (viaIndex as unknown as pg.QueryResult[])[1]?.rows[0]?.n ?? (viaIndex as pg.QueryResult).rows[0]?.n;
    const truthN = (truth as unknown as pg.QueryResult[])[3]?.rows[0]?.n ?? (truth as pg.QueryResult).rows[0]?.n;
    expect(truthN).toBe(1);
    expect(idxN).toBe(0); // the stale index answers for the old body
    await admin.query("DROP TABLE es_premise; DROP FUNCTION es_premise_scope(jsonb, text)");
  });

  it("an external pool is used as is and recorded as a warning; unique paths still map", async () => {
    const external = new pg.Pool({ connectionString: url, max: 2 });
    const s = new PostgresStore({ connection: external, schema, table: TABLE });
    expect(s.warnings().some((w) => w.includes("external pool"))).toBe(true);
    await s.append([events.AccountOpened({ owner: "ext-1" })]);
    await expect(s.append([events.AccountOpened({ owner: "ext-1" })])).rejects.toBeInstanceOf(UniqueViolationError);
    await s.close(); // leaves the pool open
    await external.query("SELECT 1");
    await external.end();
  });

  it("only our append function overload survives an install; ES001 maps to UsageError", async () => {
    await admin.query(`CREATE FUNCTION public."${appendFunctionName(TABLE)}"(x int) RETURNS int LANGUAGE sql AS 'SELECT 1'`);
    const s = new PostgresStore({ connection: url!, schema, table: TABLE, poolSize: 2 });
    await s.ensureInstalled();
    const overloads = await admin.query("SELECT count(*)::int AS n FROM pg_proc WHERE proname = $1", [appendFunctionName(TABLE)]);
    expect(overloads.rows[0]?.n).toBe(1);
    const grants = await admin.query(`SELECT has_function_privilege('public', '"${appendFunctionName(TABLE)}"${APPEND_SIGNATURE}', 'EXECUTE') AS pub`);
    expect(grants.rows[0]?.pub).toBe(false);
    const err = await s.withClient(async (c) => {
      await c.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
      try {
        await c.query(`SELECT * FROM "${appendFunctionName(TABLE)}"($1::bigint[], $2::boolean[], 1, false, NULL::jsonb, 0, $3::text[], $4::jsonb[], $5::jsonb[])`, [[], [], ["NoteAdded"], ['{"noteAddedId":"z","text":"x"}'], ["{}"]]);
        return null;
      } catch (e) {
        return e;
      } finally {
        await c.query("ROLLBACK");
      }
    });
    expect((err as { code?: string }).code).toBe("ES001");
    expect(() => {
      throw (s as unknown as { translate: (e: unknown, t: boolean) => unknown }).translate(err, false);
    }).toThrow(UsageError);
    await s.close();
  });
});
