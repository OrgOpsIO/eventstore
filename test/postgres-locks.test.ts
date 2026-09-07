import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { z } from "zod";
import { TransientError, buildSchema, defineEvents } from "../src/index.js";
import { UsageError } from "../src/errors.js";
import { GLOBAL_LOCK_KEY, globalLockKey, scopeLockKey } from "../src/internal.js";
import { PostgresStore } from "../src/postgres/index.js";

const url = process.env.ES_TEST_DATABASE_URL;

const events = defineEvents({
  ArticleDrafted: { data: z.object({ title: z.string() }), scopes: ["tenantId", "articleId"] },
});
const schema = buildSchema([events], { strict: true, tenantScopeKey: "tenantId" });
const TABLE = "es_lock_events";

/** An article value whose lock key sorts BELOW the tenant's — the inversion that deadlocked two-phase locking. */
function articleBelow(tenant: string): string {
  const t = scopeLockKey("tenantId", tenant);
  for (let i = 0; i < 100_000; i++) {
    const candidate = `A${i}`;
    if (scopeLockKey("articleId", candidate) < t) return candidate;
  }
  throw new Error("no inverted key found");
}

describe.skipIf(!url)("PostgresStore lock plan", () => {
  let store: PostgresStore;

  beforeAll(async () => {
    const admin = new pg.Pool({ connectionString: url, max: 1 });
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}" CASCADE`);
    await admin.end();
    store = new PostgresStore({ connection: url!, schema, table: TABLE, poolSize: 24 });
    await store.ensureInstalled();
  });
  afterAll(async () => {
    await store.close();
  });

  it("takes all locks in one sorted pass: a plain append and a tenant-wide guard never deadlock", async () => {
    const tenant = "T-lock";
    const article = articleBelow(tenant);
    const plain = () => store.append([events.ArticleDrafted({ title: "plain" }, { tenantId: tenant, articleId: article })]);
    const guarded = async () => {
      const read = await store.query({ types: ["ArticleDrafted"], scopes: { tenantId: tenant } });
      return store.appendIf([events.ArticleDrafted({ title: "guarded" }, { tenantId: tenant, articleId: article })], read.ctx);
    };
    const runs: Promise<unknown>[] = [];
    for (let i = 0; i < 40; i++) runs.push(plain(), guarded());
    const settled = await Promise.allSettled(runs);
    const failures = settled.filter((r): r is PromiseRejectedResult => r.status === "rejected");
    expect(failures.map((f) => String(f.reason))).toEqual([]);
    expect(failures.some((f) => f.reason instanceof TransientError)).toBe(false);
    const plan = store.lockPlan([{ ...events.ArticleDrafted({ title: "x" }, { tenantId: tenant, articleId: article }) }], null);
    // the tenant stamp is shared, the article pair exclusive, sorted by key
    expect(plan.keys).toHaveLength(2);
    expect(plan.modes).toEqual([true, false]); // article key sorts below the tenant key
    expect(BigInt(plan.keys[0]!) < BigInt(plan.keys[1]!)).toBe(true);
  });

  it("a tenant-only guard locks the tenant exclusively and still excludes a scoped append", async () => {
    const tenant = "T-excl";
    const read = await store.query({ types: ["ArticleDrafted"], scopes: { tenantId: tenant } });
    const guardPlan = store.lockPlan([{ ...events.ArticleDrafted({ title: "g" }, { tenantId: tenant, articleId: "A1" }) }], read.ctx);
    expect(guardPlan.keys).toContain(scopeLockKey("tenantId", tenant).toString());
    expect(guardPlan.modes[guardPlan.keys.indexOf(scopeLockKey("tenantId", tenant).toString())]).toBe(true); // exclusive wins over the shared stamp
    const results = await Promise.all(
      Array.from({ length: 8 }, () => store.appendIf([events.ArticleDrafted({ title: "g" }, { tenantId: tenant, articleId: "A1" })], read.ctx)),
    );
    expect(results.filter((r) => r.ok)).toHaveLength(1);
  });

  it("salts the global lock key per deployment", async () => {
    const salted = new PostgresStore({ connection: url!, schema: buildSchema([events], { strict: true, tenantScopeKey: "tenantId", lockSalt: "s3cret" }), table: TABLE, install: "none" });
    const plain = store.lockPlan([], null);
    const withSalt = salted.lockPlan([], null);
    expect(plain.globalKey).toBe(GLOBAL_LOCK_KEY.toString());
    expect(withSalt.globalKey).toBe(globalLockKey("s3cret").toString());
    expect(withSalt.globalKey).not.toBe(plain.globalKey);
    await salted.close();
  });

  it("reads id and scopes from the raw payload and data from the upcast payload", async () => {
    const upcasting = defineEvents({
      Renamed: { data: z.object({ value: z.string() }), scopes: ["tenantId"], upcast: (p) => ("v" in p ? { ...p, value: p.v } : p) },
    });
    const s = new PostgresStore({ connection: url!, schema: buildSchema([upcasting], { strict: true, tenantScopeKey: "tenantId" }), table: TABLE, install: "none" });
    // an old-shape row written by hand (the SDK's validation would insist on `value`)
    await store.withClient((c) =>
      c.query(`INSERT INTO "${TABLE}" (event_type, payload) VALUES ('Renamed', '{"renamedId":"r-1","v":"old","scopes":{"tenantId":"T-up"}}')`),
    );
    const read = await s.query({ types: ["Renamed"], scopes: { tenantId: "T-up" } });
    expect(read.events).toHaveLength(1);
    expect(read.events[0]!.id).toBe("r-1");
    expect(read.events[0]!.scopes).toEqual({ tenantId: "T-up" });
    expect(read.events[0]!.data).toEqual({ v: "old", value: "old" });
    await s.close();
  });

  it("refuses root reads and writes when rls is on", async () => {
    const rls = new PostgresStore({ connection: url!, schema, table: TABLE, install: "none", rls: true });
    await expect(rls.query({ types: ["ArticleDrafted"] })).rejects.toBeInstanceOf(UsageError);
    await expect(rls.append([events.ArticleDrafted({ title: "x" }, { tenantId: "t", articleId: "a" })])).rejects.toBeInstanceOf(UsageError);
    await rls.close();
  });

  it("installs in one transaction and leaves RLS flags and policy alone when they are already right", async () => {
    const rlsTable = "es_lock_rls";
    const admin = new pg.Pool({ connectionString: url, max: 1 });
    await admin.query(`DROP TABLE IF EXISTS "${rlsTable}" CASCADE`);
    const a = new PostgresStore({ connection: url!, schema, table: rlsTable, rls: true });
    await a.ensureInstalled();
    const before = await admin.query("SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = $1", [rlsTable]);
    expect(before.rows[0]).toEqual({ relrowsecurity: true, relforcerowsecurity: true });
    const policyBefore = await admin.query("SELECT oid FROM pg_policy WHERE polname = $1", [`es_${rlsTable}_tenant_isolation`]);
    const b = new PostgresStore({ connection: url!, schema, table: rlsTable, rls: true });
    await b.ensureInstalled();
    const policyAfter = await admin.query("SELECT oid FROM pg_policy WHERE polname = $1", [`es_${rlsTable}_tenant_isolation`]);
    expect(policyAfter.rows[0]).toEqual(policyBefore.rows[0]); // not recreated
    // a policy on another key is replaced
    await admin.query(`DROP POLICY "es_${rlsTable}_tenant_isolation" ON "${rlsTable}"`);
    await admin.query(`CREATE POLICY "es_${rlsTable}_tenant_isolation" ON "${rlsTable}" USING (es_scope(payload, 'otherKey') = current_setting('app.current_tenant', true))`);
    const c = new PostgresStore({ connection: url!, schema, table: rlsTable, rls: true });
    await c.ensureInstalled();
    const qual = await admin.query("SELECT qual FROM pg_policies WHERE policyname = $1", [`es_${rlsTable}_tenant_isolation`]);
    expect(String(qual.rows[0]?.qual)).toContain("'tenantId'::text");
    expect(a.warnings()).toEqual([]);
    await Promise.all([a.close(), b.close(), c.close()]);
    await admin.end();
  });

  it("gates on THIS table's index fingerprints: a stale index comment refuses, a foreign function comment with our semantics is adopted", async () => {
    const admin = new pg.Pool({ connectionString: url, max: 1 });
    const idx = `es_${TABLE}_scope_articleId`;
    // a foreign fingerprint on the function alone is not a stale index: the probes pass → adopted and re-stamped
    await admin.query("COMMENT ON FUNCTION public.es_scope(jsonb, text) IS 'es:0000'");
    const adopt = new PostgresStore({ connection: url!, schema, table: TABLE });
    await adopt.ensureInstalled();
    const fp = await admin.query("SELECT obj_description(p.oid, 'pg_proc') AS fp FROM pg_proc p WHERE proname = 'es_scope'");
    expect(String(fp.rows[0]?.fp)).toMatch(/^es:[0-9a-f]{64}$/);
    // a stale INDEX comment is the real gate
    await admin.query(`COMMENT ON INDEX "${idx}" IS 'es:0000'`);
    const s = new PostgresStore({ connection: url!, schema, table: TABLE });
    await expect(s.ensureInstalled()).rejects.toThrow(/CREATE INDEX CONCURRENTLY/);
    const rebuild = new PostgresStore({ connection: url!, schema, table: TABLE, rebuildScopeIndexes: true });
    await rebuild.ensureInstalled();
    const after = await admin.query("SELECT obj_description(to_regclass($1), 'pg_class') AS fp", [`"${idx}"`]);
    expect(String(after.rows[0]?.fp)).toMatch(/^es:[0-9a-f]{64}$/);
    // a pre-fingerprint function (no comment) with our semantics is adopted, indexes untouched
    await admin.query("COMMENT ON FUNCTION public.es_scope(jsonb, text) IS NULL");
    const quiet = new PostgresStore({ connection: url!, schema, table: TABLE });
    await quiet.ensureInstalled();
    expect(quiet.warnings()).toEqual([]);
    await Promise.all([adopt.close(), s.close(), rebuild.close(), quiet.close()]);
    await admin.end();
  });
});
