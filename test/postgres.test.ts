import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { UniqueViolationError, UnindexableContextError, buildSchema, defineEvents } from "../src/index.js";
import { PostgresStore, printSchemaSql, scopeIndexName } from "../src/postgres/index.js";

const url = process.env.ES_TEST_DATABASE_URL;

const accounts = defineEvents({
  AccountOpened: { data: z.object({ owner: z.string(), email: z.string() }), unique: ["email"] },
  MoneyDeposited: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"] },
  MoneyWithdrawn: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"] },
  // flat-id style (an earlier project): the scope value lives in data
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
    await expect(again).rejects.toMatchObject({ detail: { idempotencyKey: "k1" } });
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
