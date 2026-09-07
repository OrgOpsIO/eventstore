import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import pg from "pg";
import { z } from "zod";
import { UniqueViolationError, UnindexableContextError, buildSchema, defineEvents } from "../src/index.js";
import { PolicyViolationError, TransientError } from "../src/errors.js";
import { PostgresStore, appendFunctionName, leadingScopeKey, printSchemaSql, scopeIndexName } from "../src/postgres/index.js";

const url = process.env.ES_TEST_DATABASE_URL;

const accounts = defineEvents({
  AccountOpened: { data: z.object({ owner: z.string(), email: z.string() }), unique: ["email"] },
  MoneyDeposited: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"] },
  MoneyWithdrawn: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"] },
  // flat-id style: the scope value lives in data
  LegacyNote: { data: z.object({ accountOpenedId: z.string(), note: z.string() }) },
});

const schema = buildSchema([accounts], { strict: true });
const TABLE = "es_test_events";

describe.skipIf(!url)("PostgresStore", () => {
  let store: PostgresStore;

  beforeAll(async () => {
    store = new PostgresStore({ connection: url!, schema, table: TABLE, poolSize: 20 });
    await store.withClient(async (c) => {
      await c.query(`DROP TABLE IF EXISTS "${TABLE}" CASCADE`);
    });
    // the DROP above happened after install; reinstall for a clean table
    store = new PostgresStore({ connection: url!, schema, table: TABLE, poolSize: 20 });
    await store.ensureInstalled();
  });

  beforeEach(async () => {
    await store.withClient((c) => c.query(`TRUNCATE "${TABLE}" RESTART IDENTITY`));
  });

  afterAll(async () => {
    await store.close();
  });

  it("installs idempotently, also when several stores boot at once", async () => {
    const boots = Array.from({ length: 4 }, () => new PostgresStore({ connection: url!, schema, table: TABLE }));
    await Promise.all(boots.map((s) => s.ensureInstalled()));
    await Promise.all(boots.map((s) => s.close()));
    const ddl = printSchemaSql(schema, { table: TABLE });
    expect(ddl).toContain("CREATE TABLE IF NOT EXISTS");
    expect(ddl).toContain(scopeIndexName(TABLE, "accountOpenedId"));
    expect(ddl).toContain("es_fn_");
    expect(ddl).toContain("_v4");
    expect(ddl).toContain("REVOKE EXECUTE");
    expect(ddl).toContain("SET search_path");
    // functions are fingerprinted; a second install replaces nothing
    const fp = await store.withClient(async (c) => {
      const r = await c.query("SELECT obj_description(p.oid, 'pg_proc') AS fp FROM pg_proc p WHERE p.proname = $1", [appendFunctionName(TABLE)]);
      return r.rows[0]?.fp as string;
    });
    expect(fp).toMatch(/^es:[0-9a-f]{64}$/);
    // the unversioned v1 name is gone
    const legacy = await store.withClient(async (c) => (await c.query("SELECT 1 FROM pg_proc WHERE proname = $1", [appendFunctionName(TABLE, 1)])).rowCount);
    expect(legacy).toBe(0);
  });

  it("es_scope is string-only: a numeric flat value is not a scope value", async () => {
    await store.withClient((c) =>
      c.query(`INSERT INTO "${TABLE}" (event_type, payload) VALUES ('LegacyNote', '{"legacyNoteId":"n1","accountOpenedId":42,"note":"num"}')`),
    );
    const res = await store.query({ scopes: { accountOpenedId: "42" } });
    expect(res.events).toHaveLength(0);
  });

  it("refuses the guard outside READ COMMITTED and maps transient failures", async () => {
    await store.append([accounts.AccountOpened({ owner: "T", email: "t@example.com" }, {}, { id: "acc-t" })]);
    const err = await store.withClient(async (c) => {
      await c.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
      try {
        await c.query(`SELECT * FROM "${appendFunctionName(TABLE)}"($1::bigint[], $2::boolean[], 1, false, NULL::jsonb, 0, $3::text[], $4::jsonb[], $5::jsonb[])`, [
          [],
          [],
          ["MoneyDeposited"],
          ['{"moneyDepositedId":"x","amount":1,"scopes":{"accountOpenedId":"acc-t"}}'],
          ["{}"],
        ]);
        return null;
      } catch (e) {
        return e as { code?: string; message?: string };
      } finally {
        await c.query("ROLLBACK");
      }
    });
    expect(err?.code).toBe("ES001");
    // a lock_timeout surfaces as TransientError
    const tight = new PostgresStore({ connection: url!, schema, table: TABLE, poolSize: 2, timeouts: { lockMs: 200 } });
    await store.withClient(async (holder) => {
      await holder.query("BEGIN");
      await holder.query("SELECT pg_advisory_xact_lock($1::bigint)", [(await import("../src/internal.js")).scopeLockKey("accountOpenedId", "acc-t").toString()]);
      const query = accounts.$scope("accountOpenedId", "acc-t");
      const ctx = await tight.query(query);
      await expect(tight.appendIf([accounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: "acc-t" })], ctx.ctx)).rejects.toBeInstanceOf(TransientError);
      await holder.query("ROLLBACK");
    });
    await tight.close();
  });

  it("rejects batches above maxBatchSize", async () => {
    const small = new PostgresStore({ connection: url!, schema, table: TABLE, maxBatchSize: 2 });
    await expect(small.append([1, 2, 3].map((i) => accounts.AccountOpened({ owner: `m${i}`, email: `m${i}@example.com` })))).rejects.toThrow(/maxBatchSize/);
    await small.close();
  });

  it("chooses the most selective non-tenant scope key to lead the version check", () => {
    expect(leadingScopeKey({ scopes: { tenantId: "t", accountOpenedId: ["a", "b"] } }, "tenantId")).toBe("accountOpenedId");
    expect(leadingScopeKey({ scopes: { tenantId: "t", a: ["1", "2"], b: "3" } }, "tenantId")).toBe("b");
    expect(leadingScopeKey({ scopes: { tenantId: "t" } }, "tenantId")).toBe("tenantId");
    expect(leadingScopeKey({ types: ["X"] }, "tenantId")).toBeUndefined();
  });

  it("appends and queries with byFilter, contextVersion vs lastReturned", async () => {
    const opened = accounts.AccountOpened({ owner: "Mary", email: "mary@example.com" }, {}, { id: "acc-1" });
    const first = await store.append([opened]);
    expect(first).toEqual({ first: 1, last: 1, count: 1 });
    await store.append([
      accounts.MoneyDeposited({ amount: 10 }, { accountOpenedId: "acc-1" }),
      accounts.MoneyDeposited({ amount: 5 }, { accountOpenedId: "acc-1" }),
    ]);
    const res = await store.query([{ types: ["AccountOpened"] }, accounts.$scope("accountOpenedId", "acc-1")]);
    expect(res.events.map((e) => e.sequence)).toEqual([1, 2, 3]);
    expect(res.byFilter[0]?.map((e) => e.sequence)).toEqual([1]);
    expect(res.byFilter[1]?.map((e) => e.sequence)).toEqual([1, 2, 3]);
    expect(res.contextVersion).toBe(3);
    expect(res.events[0]?.id).toBe("acc-1");
    expect(res.events[0]?.data).toEqual({ owner: "Mary", email: "mary@example.com" });
    expect(res.events[1]?.scopes).toEqual({ accountOpenedId: "acc-1" });
    expect(res.events[1]?.recordedAt).toBeInstanceOf(Date);

    const delta = await store.query(accounts.$scope("accountOpenedId", "acc-1"), { after: 2 });
    expect(delta.events.map((e) => e.sequence)).toEqual([3]);
    expect(delta.lastReturned).toBe(3);
    expect(delta.contextVersion).toBe(3);

    const empty = await store.query(accounts.$scope("accountOpenedId", "nope"));
    expect(empty.events).toEqual([]);
    expect(empty.contextVersion).toBe(0);
    expect(empty.byFilter).toEqual([[]]);

    const limited = await store.query(accounts.$scope("accountOpenedId", "acc-1"), { limit: 1, order: "desc" });
    expect(limited.events.map((e) => e.sequence)).toEqual([3]);
    expect(limited.contextVersion).toBe(3);
  });

  it("matches scopes through scopes, own id and flat data fields", async () => {
    await store.append([
      accounts.AccountOpened({ owner: "A", email: "a@example.com" }, {}, { id: "acc-a" }),
      accounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: "acc-a" }),
      accounts.LegacyNote({ accountOpenedId: "acc-a", note: "flat" }),
      accounts.AccountOpened({ owner: "B", email: "b@example.com" }, {}, { id: "acc-b" }),
    ]);
    const res = await store.query({ scopes: { accountOpenedId: "acc-a" } });
    expect(res.events.map((e) => e.type)).toEqual(["AccountOpened", "MoneyDeposited", "LegacyNote"]);
    const multi = await store.query({ scopes: { accountOpenedId: ["acc-a", "acc-b"] }, types: ["AccountOpened"] });
    expect(multi.events.map((e) => e.id)).toEqual(["acc-a", "acc-b"]);
    const where = await store.query({ where: [{ note: "flat" }] });
    expect(where.events.map((e) => e.type)).toEqual(["LegacyNote"]);
  });

  it("enforces unique paths and idempotency keys", async () => {
    await store.append([accounts.AccountOpened({ owner: "A", email: "dup@example.com" })]);
    await expect(store.append([accounts.AccountOpened({ owner: "B", email: "dup@example.com" })])).rejects.toMatchObject({
      name: "UniqueViolationError",
      detail: { type: "AccountOpened", path: "email" },
    });
    await store.append([accounts.AccountOpened({ owner: "C", email: "c@example.com" }, {}, { metadata: { idempotencyKey: "k1" } })]);
    const again = store.append([accounts.AccountOpened({ owner: "C", email: "c2@example.com" }, {}, { metadata: { idempotencyKey: "k1" } })]);
    await expect(again).rejects.toBeInstanceOf(UniqueViolationError);
    await expect(again).rejects.toMatchObject({ detail: { idempotencyKey: true } });
  });

  it("appendIf commits on an unchanged context and reports a conflict otherwise", async () => {
    await store.append([accounts.AccountOpened({ owner: "A", email: "x@example.com" }, {}, { id: "acc-x" })]);
    const query = accounts.$scope("accountOpenedId", "acc-x");
    const ctx = await store.query(query);
    const ok = await store.appendIf([accounts.MoneyDeposited({ amount: 3 }, { accountOpenedId: "acc-x" })], { query, version: ctx.contextVersion });
    expect(ok).toEqual({ ok: true, appended: { first: 2, last: 2, count: 1 } });
    const stale = await store.appendIf([accounts.MoneyDeposited({ amount: 3 }, { accountOpenedId: "acc-x" })], { query, version: ctx.contextVersion });
    expect(stale).toEqual({ ok: false, conflict: { expected: 1, actual: 2 } });
    // unrelated scope: no false conflict
    await store.append([accounts.AccountOpened({ owner: "B", email: "y@example.com" }, {}, { id: "acc-y" })]);
    const ctxY = await store.query(accounts.$scope("accountOpenedId", "acc-y"));
    expect(ctxY.contextVersion).toBe(3);
  });

  it("strict mode rejects a guard without a declared scope key", async () => {
    await expect(store.appendIf([accounts.AccountOpened({ owner: "Z", email: "z@example.com" })], { query: { types: ["AccountOpened"] }, version: 0 })).rejects.toBeInstanceOf(UnindexableContextError);
  });

  it("race: 16 concurrent appendIf with the same expected version → exactly one succeeds", async () => {
    await store.append([accounts.AccountOpened({ owner: "R", email: "r@example.com" }, {}, { id: "acc-r" })]);
    const query = accounts.$scope("accountOpenedId", "acc-r");
    const ctx = await store.query(query);
    const outcomes = await Promise.all(
      Array.from({ length: 16 }, () =>
        store.appendIf([accounts.MoneyWithdrawn({ amount: 80 }, { accountOpenedId: "acc-r" })], { query, version: ctx.contextVersion }),
      ),
    );
    expect(outcomes.filter((o) => o.ok)).toHaveLength(1);
    const after = await store.query(query);
    expect(after.events.filter((e) => e.type === "MoneyWithdrawn")).toHaveLength(1);
  });

  it("non-strict guard without scopes falls back to the exclusive global lock and stays correct", async () => {
    const loose = new PostgresStore({ connection: url!, schema: buildSchema([accounts], { strict: false }), table: TABLE, poolSize: 20 });
    await loose.append([accounts.AccountOpened({ owner: "G", email: "g@example.com" }, {}, { id: "acc-g" })]);
    const query = { types: ["AccountOpened"] };
    const ctx = await loose.query(query);
    const outcomes = await Promise.all(
      Array.from({ length: 8 }, (_, i) =>
        loose.appendIf([accounts.AccountOpened({ owner: `G${i}`, email: `g${i}@example.com` })], { query, version: ctx.contextVersion }),
      ),
    );
    expect(outcomes.filter((o) => o.ok)).toHaveLength(1);
    await loose.close();
  });

  it("uses the scope expression index for scope queries", async () => {
    const rows = 5000;
    await store.withClient(async (c) => {
      await c.query(
        `INSERT INTO "${TABLE}" (event_type, payload)
         SELECT 'MoneyDeposited', jsonb_build_object('moneyDepositedId', gen_random_uuid()::text, 'amount', g % 50, 'scopes', jsonb_build_object('accountOpenedId', 'acc-' || (g % 500)))
         FROM generate_series(1, $1) g`,
        [rows],
      );
      await c.query(`ANALYZE "${TABLE}"`);
    });
    const plan = await store.withClient(async (c) => {
      const r = await c.query(
        `EXPLAIN SELECT * FROM "${TABLE}" WHERE es_scope(payload, 'accountOpenedId') = ANY(ARRAY['acc-7']::text[]) ORDER BY sequence_number`,
      );
      return r.rows.map((row: { "QUERY PLAN": string }) => row["QUERY PLAN"]).join("\n");
    });
    expect(plan).toMatch(/Index Scan|Bitmap Index Scan/);
    expect(plan).toContain(scopeIndexName(TABLE, "accountOpenedId"));
    // the version check (bare MAX over one scope value) walks the same index backwards — even with
    // es_scope as an inlined SQL function
    const { compileVersionSql } = await import("../src/postgres/index.js");
    const v = compileVersionSql(TABLE, accounts.$scope("accountOpenedId", "acc-7"), undefined, schema);
    const maxPlan = await store.withClient(async (c) => {
      // outside the function (EXECUTE … USING) only referenced parameter arrays may be bound
      const r = await c.query(`EXPLAIN ${v.sql}`, v.sql.includes("$2::jsonb[]") ? [v.texts, v.jsons] : [v.texts]);
      return r.rows.map((row: { "QUERY PLAN": string }) => row["QUERY PLAN"]).join("\n");
    });
    expect(maxPlan).toContain("Index Scan Backward");
    expect(maxPlan).toContain(scopeIndexName(TABLE, "accountOpenedId"));
    expect(v.sql).not.toContain("UNION ALL");
    const res = await store.query(accounts.$scope("accountOpenedId", "acc-7"));
    expect(res.events).toHaveLength(rows / 500);
  });

  it("cursor and settled semantics", async () => {
    await store.append([accounts.AccountOpened({ owner: "S", email: "s@example.com" }, {}, { id: "acc-s" })]);
    await store.append([accounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: "acc-s" })]);
    const query = accounts.$scope("accountOpenedId", "acc-s");
    const all = await store.query(query, { settledOnly: true });
    expect(all.events.map((e) => e.settled)).toEqual([true, true]);
    expect(all.settledCursor?.sequence).toBe(2);
    await store.append([accounts.MoneyDeposited({ amount: 2 }, { accountOpenedId: "acc-s" })]);
    const delta = await store.query(query, { cursor: all.settledCursor });
    expect(delta.events.map((e) => e.sequence)).toEqual([3]);
    expect(delta.contextVersion).toBe(3);
    // an open transaction holding a sequence makes later rows unsettled
    await store.withClient(async (holder) => {
      await holder.query("BEGIN");
      await holder.query(`INSERT INTO "${TABLE}" (event_type, payload) VALUES ('MoneyDeposited', '{"moneyDepositedId":"h","amount":9,"scopes":{"accountOpenedId":"acc-s"}}')`);
      await store.append([accounts.MoneyDeposited({ amount: 4 }, { accountOpenedId: "acc-s" })]);
      const unsettled = await store.query(query, { cursor: delta.settledCursor });
      expect(unsettled.events.map((e) => [e.sequence, e.settled])).toEqual([[5, false]]);
      expect(unsettled.settledCursor).toBeNull();
      await holder.query("ROLLBACK");
    });
    const settledNow = await store.query(query, { cursor: delta.settledCursor });
    expect(settledNow.events.map((e) => [e.sequence, e.settled])).toEqual([[5, true]]);
  });
});

const tenantAccounts = defineEvents({
  AccountOpened: { data: z.object({ owner: z.string() }), scopes: ["tenantId"] },
  MoneyDeposited: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId", "tenantId"] },
});
const tenantSchema = buildSchema([tenantAccounts], { strict: true, tenantScopeKey: "tenantId" });
const RLS_TABLE = "es_rls_events";
const APP_ROLE = "es_app";

describe.skipIf(!url)("PostgresStore with rls: true and a non-owner role", () => {
  let owner: PostgresStore;
  let app: PostgresStore;

  beforeAll(async () => {
    const admin = new pg.Pool({ connectionString: url, max: 1 });
    await admin.query(`DROP TABLE IF EXISTS "${RLS_TABLE}" CASCADE`);
    await admin.query(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${APP_ROLE}') THEN CREATE ROLE ${APP_ROLE} LOGIN PASSWORD '${APP_ROLE}'; END IF; END $$`);
    await admin.end();
    owner = new PostgresStore({ connection: url!, schema: tenantSchema, table: RLS_TABLE, rls: true, grantExecuteTo: [APP_ROLE], poolSize: 4 });
    await owner.ensureInstalled();
    await owner.withClient(async (c) => {
      await c.query(`GRANT USAGE ON SCHEMA public TO ${APP_ROLE}`);
      await c.query(`GRANT SELECT, INSERT ON "${RLS_TABLE}" TO ${APP_ROLE}`);
      await c.query(`GRANT USAGE, SELECT ON SEQUENCE "${RLS_TABLE}_sequence_number_seq" TO ${APP_ROLE}`);
    });
    const appUrl = new URL(url!);
    appUrl.username = APP_ROLE;
    appUrl.password = APP_ROLE;
    app = new PostgresStore({ connection: appUrl.toString(), schema: tenantSchema, table: RLS_TABLE, rls: true, install: "none", poolSize: 4 });
  });

  afterAll(async () => {
    await app.close();
    await owner.close();
  });

  it("isolates tenants: reads see only the session tenant, foreign rows are refused on write", async () => {
    const a = app.withTenant("tenant-a");
    const b = app.withTenant("tenant-b");
    await a.append([tenantAccounts.AccountOpened({ owner: "A" }, { tenantId: "tenant-a" }, { id: "acc-a" })]);
    await b.append([tenantAccounts.AccountOpened({ owner: "B" }, { tenantId: "tenant-b" }, { id: "acc-b" })]);
    expect((await a.query({ types: ["AccountOpened"] })).events.map((e) => e.id)).toEqual(["acc-a"]);
    expect((await b.query({ types: ["AccountOpened"] })).events.map((e) => e.id)).toEqual(["acc-b"]);
    // the root store refuses to run without a tenant session when rls is on (it would see nothing)
    await expect(app.query({ types: ["AccountOpened"] })).rejects.toThrow(/withTenant/);
    // a row that names tenant B written through tenant A's session violates WITH CHECK
    await expect(a.append([tenantAccounts.AccountOpened({ owner: "X" }, { tenantId: "tenant-b" })])).rejects.toBeInstanceOf(PolicyViolationError);
    // the guard works inside the tenant session
    const ctx = await a.query(tenantAccounts.$scope("accountOpenedId", "acc-a"));
    const ok = await a.appendIf([tenantAccounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: "acc-a", tenantId: "tenant-a" })], ctx.ctx);
    expect(ok.ok).toBe(true);
    const stale = await a.appendIf([tenantAccounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: "acc-a", tenantId: "tenant-a" })], ctx.ctx);
    expect(stale.ok).toBe(false);
  });

  it("idempotency keys are unique per tenant, not globally", async () => {
    const a = app.withTenant("tenant-a");
    const b = app.withTenant("tenant-b");
    await a.append([tenantAccounts.AccountOpened({ owner: "K" }, { tenantId: "tenant-a" }, { metadata: { idempotencyKey: "same" } })]);
    await b.append([tenantAccounts.AccountOpened({ owner: "K" }, { tenantId: "tenant-b" }, { metadata: { idempotencyKey: "same" } })]);
    await expect(a.append([tenantAccounts.AccountOpened({ owner: "K2" }, { tenantId: "tenant-a" }, { metadata: { idempotencyKey: "same" } })])).rejects.toBeInstanceOf(
      UniqueViolationError,
    );
  });
});
