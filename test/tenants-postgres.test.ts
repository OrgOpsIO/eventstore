import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildSchema, createEventStore, UsageError, type Cursor, type EventStoreApi } from "../src/index.js";
import { lockKeyOf } from "../src/ids.js";
import { printSchemaSql, PostgresStore } from "../src/postgres/index.js";
import { knobsOf, made, noted, onRead, pmConfig, tokenOf, until, wait, type Offer } from "./persisted-model.suite.js";
import { crossModel, tenantsSuite, type TenantsHarness } from "./tenants.suite.js";

const url = process.env.ES_TEST_DATABASE_URL;
const schema = buildSchema(pmConfig.events, { strict: true, tenantScopeKey: pmConfig.tenant.scopeKey });

const T = "t6_events";

async function drop(admin: pg.Pool, table: string): Promise<void> {
  for (const t of [`${table}_read_model_state`, `${table}_read_model`, table]) await admin.query(`DROP TABLE IF EXISTS "${t}" CASCADE`);
}

/** The pids holding the writer lock of a model row (pg_locks shows a bigint key as two 32-bit halves). */
export async function lockHolders(admin: pg.Pool, schemaName: string, table: string, name: string, version: number, tenant = ""): Promise<number[]> {
  const key = BigInt.asUintN(64, lockKeyOf(`readmodel\u0000${schemaName}\u0000${table}\u0000${tenant}\u0000${name}\u0000${version}`));
  const r = await admin.query<{ pid: number }>("SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND classid = $1 AND objid = $2 AND objsubid = 1 AND granted", [
    Number(key >> 32n),
    Number(key & 0xffffffffn),
  ]);
  return r.rows.map((row) => row.pid);
}

/** A pool whose statements are counted (every `query`, on the pool and on checked-out clients), heartbeats aside. */
function countingPool(connectionString: string): { pool: pg.Pool; count: () => number; reset: () => void } {
  const pool = new pg.Pool({ connectionString, max: 3 });
  let n = 0;
  const counted = (sql: unknown) => {
    const text = typeof sql === "string" ? sql : (sql as { text?: string } | null)?.text;
    if (text !== "SELECT 1") n++;
  };
  const query = pool.query.bind(pool) as (...args: unknown[]) => unknown;
  (pool as unknown as { query: (...args: unknown[]) => unknown }).query = (...args: unknown[]) => {
    counted(args[0]);
    return query(...args);
  };
  pool.on("connect", (client) => {
    const own = client.query.bind(client) as (...args: unknown[]) => unknown;
    (client as unknown as { query: (...args: unknown[]) => unknown }).query = (...args: unknown[]) => {
      counted(args[0]);
      return own(...args);
    };
  });
  return { pool, count: () => n, reset: () => (n = 0) };
}

describe.skipIf(!url)("one processor for every tenant on Postgres (no rls)", () => {
  let admin: pg.Pool;
  const opened: EventStoreApi[] = [];
  const open = (options = {}) => {
    const api = createEventStore({ ...pmConfig, connection: url!, postgres: { table: T, poolSize: 3, live: true, persistedModels: true }, ...options });
    opened.push(api);
    return api;
  };
  const harness: TenantsHarness = {
    async reset() {
      await this.closeAll();
      await admin.query(`TRUNCATE "${T}", "${T}_read_model", "${T}_read_model_state" RESTART IDENTITY`);
    },
    open,
    processor: open,
    async closeAll() {
      for (const api of opened.splice(0)) await api.close().catch(() => undefined);
    },
  };

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 3 });
    await drop(admin, T);
    const store = new PostgresStore({ connection: url!, schema, table: T, persistedModels: true, live: true });
    await store.ensureInstalled();
    await store.close();
  });
  afterAll(async () => {
    await harness.closeAll();
    await drop(admin, T);
    await admin.end();
  });

  tenantsSuite(harness, { numRuns: 12 });

  it("one subscription for every tenant: the processor's statements per append do not grow from 1 to 10 tenants with readers open, readers run no feed statements, and the processor holds one writer lock", async () => {
    const counts: { tenants: number; processor: number; readers: number }[] = [];
    for (const tenants of [1, 10]) {
      await harness.reset();
      const proc = countingPool(url!);
      const app = countingPool(url!);
      const procApi = createEventStore({ ...pmConfig, store: new PostgresStore({ connection: proc.pool, schema, table: T, persistedModels: true, live: { connection: url! } }) });
      const appApi = createEventStore({ ...pmConfig, store: new PostgresStore({ connection: app.pool, schema, table: T, persistedModels: true, live: { connection: url! } }) });
      const writer = open();
      const def = crossModel("one-sub");
      const processor = procApi.persistedReadModel(def);
      const names = Array.from({ length: tenants }, (_, i) => `t${i}`);
      let last: Cursor | undefined;
      for (const t of names) last = tokenOf(await writer.forTenant(t).append([made("o1", 1)]));
      for (const t of names) await appApi.forTenant(t).persistedReadModel(def).list({ atLeast: last!, timeoutMs: 10_000 });
      expect((await processor.status()).role).toBe("writer");
      await wait(300);
      proc.reset();
      app.reset();
      const appended = await writer.forTenant("t0").append([made("o2", 2)]);
      await until(async () => {
        const r = await admin.query<{ seq: string | null }>(`SELECT cursor_seq::text AS seq FROM "${T}_read_model" WHERE tenant = 't0' AND name = 'one-sub'`);
        return Number(r.rows[0]?.seq ?? 0) >= appended.last;
      });
      await wait(200);
      counts.push({ tenants, processor: proc.count(), readers: app.count() });
      expect(await lockHolders(admin, "public", T, "one-sub", 1)).toHaveLength(1);
      for (const t of names) expect(await lockHolders(admin, "public", T, "one-sub", 1, t)).toHaveLength(0);
      await procApi.close();
      await appApi.close();
    }
    console.log(`statements per append (one append to one tenant): ${counts.map((c) => `${c.tenants} tenant(s) with readers: processor ${c.processor}, readers ${c.readers}`).join("; ")}`);
    expect(counts[1]!.processor).toBe(counts[0]!.processor);
    expect(counts[0]!.readers).toBe(0);
    expect(counts[1]!.readers).toBe(0);
  }, 60_000);

  it("zombie: a processor whose lock session was cut without it noticing is fenced at its next page and writes nothing; every event is applied once", async () => {
    await harness.reset();
    const app = open();
    const a = open();
    const b = open();
    (await knobsOf(a)).noticeLoss = false;
    (await knobsOf(b)).tickMs = 100;
    const def = crossModel("zombie");
    await app.forTenant("ta").append([made("o1", 1)]);
    await app.forTenant("tb").append([made("o1", 1)]);
    const pa = a.persistedReadModel(def);
    await until(async () => (await pa.status()).role === "writer");
    const pb = b.persistedReadModel(def);
    await wait(150);
    expect((await pb.status()).role).toBe("standby");
    const holders = await lockHolders(admin, "public", T, "zombie", 1);
    expect(holders).toHaveLength(1);
    await admin.query("SELECT pg_terminate_backend($1)", [holders[0]]);
    await until(async () => (await pb.status()).role === "writer", 3_000);
    expect((await pa.status()).role).toBe("writer"); // still believes it
    const foldedA = (await pa.status()).folded;
    const t1 = await app.forTenant("ta").append([made("o1", 10), noted("o1")]);
    const t2 = await app.forTenant("tb").append([made("o1", 5)]);
    await app.forTenant("ta").persistedReadModel(def).list({ atLeast: tokenOf(t1) });
    await app.forTenant("tb").persistedReadModel(def).list({ atLeast: tokenOf(t2) });
    await until(async () => (await pa.status()).role === "standby", 3_000);
    expect((await pa.status()).folded).toBe(foldedA);
    for (const t of ["ta", "tb"]) expect(await app.forTenant(t).persistedReadModel(def).get("o1")).toEqual(((await onRead(app.forTenant(t), "o1", def)) as { state: Offer }).state);
    expect((await pb.status()).epoch).toBe(2);
    await harness.closeAll();
  }, 20_000);

  it("rows: per tenant a model row with its own checkpoint (epoch 0) and the processor row (tenant '') with the epoch and the checkpoint over every tenant; no state row of the empty tenant", async () => {
    await harness.reset();
    const app = open();
    const proc = open();
    const def = crossModel("rows");
    proc.persistedReadModel(def);
    const ta = await app.forTenant("ta").append([made("o1", 1)]);
    await app.forTenant("ta").persistedReadModel(def).list({ atLeast: tokenOf(ta) }); // two pages: a tenant's checkpoint is its part's end
    const tb = await app.forTenant("tb").append([made("o2", 2)]);
    await app.forTenant("tb").persistedReadModel(def).list({ atLeast: tokenOf(tb) });
    const rows = await admin.query<{ tenant: string; epoch: number; seq: string; revision: number }>(
      `SELECT tenant, epoch::int AS epoch, cursor_seq::text AS seq, revision::int AS revision FROM "${T}_read_model" WHERE name = 'rows' ORDER BY tenant`,
    );
    expect(rows.rows).toEqual([
      { tenant: "", epoch: 1, seq: String(tb.last), revision: 2 },
      { tenant: "ta", epoch: 0, seq: String(ta.last), revision: 1 },
      { tenant: "tb", epoch: 0, seq: String(tb.last), revision: 1 },
    ]);
    const states = await admin.query<{ tenant: string; key: string }>(`SELECT tenant, key FROM "${T}_read_model_state" WHERE name = 'rows' ORDER BY tenant, key`);
    expect(states.rows).toEqual([
      { tenant: "ta", key: "o1" },
      { tenant: "tb", key: "o2" },
    ]);
    await harness.closeAll();
  });
});

describe("processorRole: DDL and options", () => {
  it("printSchemaSql prints the three policies TO the processor role with rls and persistedModels, none without the option", () => {
    const sql = printSchemaSql(schema, { rls: true, persistedModels: true, processorRole: "es_proc" });
    expect(sql).toContain(`CREATE POLICY "es_events_processor_read" ON "public"."events" FOR SELECT TO "es_proc" USING (true)`);
    expect(sql).toContain(`CREATE POLICY "es_events_rm_processor" ON "public"."events_read_model" TO "es_proc" USING (true) WITH CHECK (true)`);
    expect(sql).toContain(`CREATE POLICY "es_events_rms_processor" ON "public"."events_read_model_state" TO "es_proc" USING (true) WITH CHECK (tenant <> '')`);
    expect(printSchemaSql(schema, { rls: true, persistedModels: true })).not.toMatch(/processor/);
  });

  it("processorRole without rls or persistedModels, or not an identifier, is a UsageError (store and printSchemaSql)", () => {
    expect(() => printSchemaSql(schema, { persistedModels: true, processorRole: "es_proc" })).toThrow(UsageError);
    expect(() => printSchemaSql(schema, { rls: true, processorRole: "es_proc" })).toThrow(/needs rls: true and persistedModels: true/);
    expect(() => printSchemaSql(schema, { rls: true, persistedModels: true, processorRole: "es proc; drop" })).toThrow(/processorRole must be a role name/);
    expect(() => new PostgresStore({ connection: "postgres://x@localhost/none", schema, persistedModels: true, processorRole: "es_proc" })).toThrow(UsageError);
    expect(() => new PostgresStore({ connection: "postgres://x@localhost/none", schema, rls: true, persistedModels: true, processorRole: "1x" })).toThrow(UsageError);
  });
});
