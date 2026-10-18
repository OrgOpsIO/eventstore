import { spawn, type ChildProcess } from "node:child_process";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import fc from "fast-check";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildSchema, createEventStore, defineReadModel, EventStoreError, MemoryStore, UsageError, type Cursor, type EventStoreApi, type ListOptions, type NewEvent } from "../src/index.js";
import { lockKeyOf } from "../src/ids.js";
import { toPayload } from "../src/internal.js";
import { printSchemaSql, PostgresStore } from "../src/postgres/index.js";
import {
  capOf,
  itemsModel,
  knobsOf,
  made,
  noted,
  offersModel,
  onRead,
  persistedModelSuite,
  pmConfig,
  set,
  source,
  tokenOf,
  until,
  wait,
  type Item,
  type Offer,
  type PersistedHarness,
} from "./persisted-model.suite.js";

const url = process.env.ES_TEST_DATABASE_URL;
const schema = buildSchema(pmConfig.events, { strict: true, tenantScopeKey: pmConfig.tenant.scopeKey });
const run = promisify(execFile);

const TABLE = "pm_events";
const tablesOf = (table: string) => [table, `${table}_read_model`, `${table}_read_model_state`];

async function drop(admin: pg.Pool, table: string): Promise<void> {
  for (const t of tablesOf(table).reverse()) await admin.query(`DROP TABLE IF EXISTS "${t}" CASCADE`);
}

/** The pids holding a model's writer lock (pg_locks shows a bigint key as two 32-bit halves). */
async function lockHolders(admin: pg.Pool, table: string, name: string, version: number, tenant = ""): Promise<number[]> {
  const key = BigInt.asUintN(64, lockKeyOf(`readmodel\u0000public\u0000${table}\u0000${tenant}\u0000${name}\u0000${version}`));
  const r = await admin.query<{ pid: number }>("SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND classid = $1 AND objid = $2 AND objsubid = 1 AND granted", [
    Number(key >> 32n),
    Number(key & 0xffffffffn),
  ]);
  return r.rows.map((row) => row.pid);
}

function postgresHarness(admin: () => pg.Pool): PersistedHarness {
  const opened: EventStoreApi[] = [];
  return {
    writers: 3,
    async reset() {
      await this.closeAll();
      await admin().query(`TRUNCATE "${TABLE}", "${TABLE}_read_model", "${TABLE}_read_model_state" RESTART IDENTITY`);
    },
    open(options = {}) {
      const api = createEventStore({ ...pmConfig, connection: url!, postgres: { table: TABLE, poolSize: 2, live: true, persistedModels: true }, ...options });
      opened.push(api);
      return api;
    },
    async closeAll() {
      // a test may have closed its runtime itself already (a pool ends once)
      for (const api of opened.splice(0)) await api.close().catch(() => undefined);
    },
  };
}

describe.skipIf(!url)("persisted read model on Postgres", () => {
  let admin: pg.Pool;
  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await drop(admin, TABLE);
    const store = new PostgresStore({ connection: url!, schema, table: TABLE, persistedModels: true, live: true });
    await store.ensureInstalled();
    await store.close();
  });
  afterAll(async () => {
    await drop(admin, TABLE);
    await admin.end();
  });

  // concurrent appends: each step appends from up to 3 runtimes with pools of their own at once
  persistedModelSuite(
    postgresHarness(() => admin),
    { numRuns: 12 },
  );
});

describe.skipIf(!url)("persisted read model on Postgres: one writer, failures, processes", () => {
  const T = "pm_proc_events";
  let admin: pg.Pool;
  const apis: EventStoreApi[] = [];
  const children: ChildProcess[] = [];
  const open = (options: { lockMs?: number } = {}) => {
    const api = createEventStore({
      ...pmConfig,
      connection: url!,
      postgres: { table: T, poolSize: 3, live: true, persistedModels: true, ...(options.lockMs ? { timeouts: { lockMs: options.lockMs } } : {}) },
    });
    apis.push(api);
    return api;
  };
  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 3 });
    await drop(admin, T);
    const store = new PostgresStore({ connection: url!, schema, table: T, persistedModels: true, live: true });
    await store.ensureInstalled();
    await store.close();
  });
  const reset = async () => {
    for (const child of children.splice(0)) if (child.exitCode === null) child.kill("SIGKILL");
    for (const api of apis.splice(0)) await api.close().catch(() => undefined); // a test may have closed it (a pool ends once)
    await admin.query(`TRUNCATE "${T}", "${T}_read_model", "${T}_read_model_state" RESTART IDENTITY`);
  };
  afterAll(async () => {
    await reset();
    await drop(admin, T);
    await admin.end();
  });

  /** A writer process (vite-node child) that prints its status every 50 ms. */
  function writerProcess(tickMs: number): { child: ChildProcess; last: () => { pid: number; role: string; folded: number; epoch: number } | undefined } {
    const child = spawn("node_modules/.bin/vite-node", ["test/fixtures/persisted-model-writer.ts", url!, T, String(tickMs)], { stdio: ["pipe", "pipe", "inherit"] });
    children.push(child);
    let last: { pid: number; role: string; folded: number; epoch: number } | undefined;
    let buffer = "";
    child.stdout!.on("data", (chunk: Buffer) => {
      buffer += chunk.toString();
      const lines = buffer.split("\n");
      buffer = lines.pop()!;
      for (const line of lines) {
        try {
          const parsed = JSON.parse(line) as { role?: string };
          if (parsed.role) last = parsed as typeof last;
        } catch {
          // vite-node's own output
        }
      }
    });
    return { child, last: () => last };
  }

  it("zombie: a writer whose lock session was cut without it noticing is fenced at its next page and writes nothing; every event is applied once", async () => {
    await reset();
    const a = open();
    const b = open();
    (await knobsOf(a)).noticeLoss = false; // A will not notice that its lock session is gone
    (await knobsOf(b)).tickMs = 100;
    await a.append([made("o1", 1), made("o2", 1)]);
    const ma = a.persistedReadModel(offersModel);
    await until(async () => (await ma.status()).role === "writer");
    const mb = b.persistedReadModel(offersModel);
    await wait(150);
    expect((await mb.status()).role).toBe("standby");
    const holders = await lockHolders(admin, T, "offers", 1);
    expect(holders).toHaveLength(1);
    await admin.query("SELECT pg_terminate_backend($1)", [holders[0]]);
    await until(async () => (await mb.status()).role === "writer", 3_000);
    expect((await ma.status()).role).toBe("writer"); // still believes it
    const foldedA = (await ma.status()).folded;
    const t1 = await a.append([made("o1", 10), noted("o2")]);
    await mb.list({ atLeast: tokenOf(t1) });
    await until(async () => (await ma.status()).role === "standby", 3_000);
    expect((await ma.status()).folded).toBe(foldedA); // fenced: its page met epoch 2 and rolled back
    for (const k of ["o1", "o2"]) expect(await mb.get(k)).toEqual(((await onRead(b, k)) as { state: Offer }).state);
    expect((await mb.list()).rows.reduce((n, r) => n + r.state.n + r.state.notes, 0)).toBe(4);
    expect((await mb.status()).epoch).toBe(2);
  }, 20_000);

  it("a database error inside a page (the model row held, lock_timeout) is retried and skips no event", async () => {
    await reset();
    const api = open({ lockMs: 150 });
    await api.append([made("o1", 1)]);
    const model = api.persistedReadModel(offersModel);
    await model.list({ atLeast: tokenOf(await api.append([made("o2", 2)])) });
    const holder = await admin.connect();
    await holder.query("BEGIN");
    await holder.query(`SELECT * FROM "${T}_read_model" WHERE name = 'offers' FOR UPDATE`);
    const t = await api.append([made("o1", 5), noted("o1"), made("o3", 7)]);
    await wait(600); // several pages fail on lock_timeout meanwhile
    const during = (await admin.query(`SELECT cursor_seq::int AS seq FROM "${T}_read_model" WHERE name = 'offers'`)).rows[0] as { seq: number };
    expect(during.seq).toBeLessThan(t.last);
    await holder.query("COMMIT");
    holder.release();
    const listed = await model.list({ atLeast: tokenOf(t) });
    expect(listed.failed).toBe(0);
    for (const k of ["o1", "o2", "o3"]) expect(await model.get(k)).toEqual(((await onRead(api, k)) as { state: Offer }).state);
    expect(listed.rows.reduce((n, r) => n + r.state.n + r.state.notes, 0)).toBe(5);
  }, 20_000);

  for (const how of ["SIGKILL of the writer process", "pg_terminate_backend of its lock session"] as const) {
    it(`two writer processes: ${how} half-way through 800 events; every key equals fold on read, each event applied once, the survivor writes`, async () => {
      await reset();
      const appender = open();
      await appender.append([made("k0", 1)]);
      const p1 = writerProcess(100);
      const p2 = writerProcess(100);
      await until(() => p1.last() !== undefined && p2.last() !== undefined, 30_000);
      await until(() => [p1.last()!.role, p2.last()!.role].filter((r) => r === "writer").length === 1, 10_000);
      const [writer, survivor] = p1.last()!.role === "writer" ? [p1, p2] : [p2, p1];
      // this process only reads: a long tick, and the lock is held by a child from the start
      (await knobsOf(appender)).tickMs = 600_000;
      const reader = appender.persistedReadModel(offersModel);
      let appended = 1;
      let last: Cursor | undefined;
      let n = 0;
      while (appended < 800) {
        const size = Math.min(1 + (n++ % 5), 800 - appended);
        const events: NewEvent[] = Array.from({ length: size }, (_, i) => made(`k${(appended + i) % 40}`, 1));
        last = tokenOf(await appender.append(events));
        appended += size;
        if (appended >= 400 && writer.child.exitCode === null && writer.child.signalCode === null) {
          if (how === "SIGKILL of the writer process") writer.child.kill("SIGKILL");
          else {
            const holders = await lockHolders(admin, T, "offers", 1);
            for (const pid of holders) await admin.query("SELECT pg_terminate_backend($1)", [pid]);
            writer.child.stdin!.end(); // it reconnects and competes again: let it leave
          }
          await wait(20);
        }
      }
      const listed = await reader.list({ atLeast: last!, limit: 100, timeoutMs: 20_000 });
      expect((await reader.status()).role).toBe("standby");
      expect(listed.total).toBe(40);
      expect(listed.rows.reduce((sum, r) => sum + r.state.n, 0)).toBe(800); // each applied exactly once
      for (const row of listed.rows) expect(row.state).toEqual(((await onRead(appender, row.key)) as { state: Offer }).state);
      await until(() => survivor.last()!.role === "writer", 5_000);
      console.log(`two writers (${how}): survivor folded ${survivor.last()!.folded}, epoch ${survivor.last()!.epoch}`);
      survivor.child.stdin!.end();
    }, 90_000);
  }

  it("two processes, watch: a standby here receives the state the writer child folded after an append by another child; this process folds nothing", async () => {
    await reset();
    const api = open();
    await api.append([made("o1", 1)]);
    const writer = writerProcess(100);
    await until(() => writer.last()?.role === "writer", 30_000);
    (await knobsOf(api)).tickMs = 600_000;
    const model = api.persistedReadModel(offersModel);
    const seen: { state: Offer; at: number }[] = [];
    await model.watch("o1", (state) => void seen.push({ state, at: Date.now() }));
    await until(() => seen.length === 1 && seen[0]!.state.total === 1, 5_000);
    const { stdout } = await run("node_modules/.bin/vite-node", ["test/fixtures/persisted-model-append.ts", url!, T, "o1", "41"], { timeout: 30_000 });
    const child = JSON.parse(stdout.trim().split("\n").at(-1)!) as { token: Cursor; at: number };
    await until(() => seen.length === 2, 2_000);
    expect(seen[1]!.state.total).toBe(42);
    const status = await model.status();
    expect(status).toMatchObject({ role: "standby", folded: 0 });
    console.log(`persisted watch across processes: append in one child → writer child → watcher here in ${seen[1]!.at - child.at} ms`);
    writer.child.stdin!.end();
  }, 60_000);

  it("the failed marker in the row holds the event's type, id, sequence, transaction and the error — never the payload; state and doc are NULL", async () => {
    await reset();
    const api = open();
    const t = await api.append([made("ok", 1), made("px", 77, "secret-status", true)]);
    const model = api.persistedReadModel(offersModel);
    await model.list({ atLeast: tokenOf(t) });
    const row = (await admin.query(`SELECT failed, state, doc FROM "${T}_read_model_state" WHERE key = 'px'`)).rows[0] as { failed: Record<string, unknown>; state: unknown; doc: unknown };
    const poisoned = (await api.query(source)).events[1]!;
    expect(row.failed).toEqual({ type: "OfferMade", id: poisoned.id, sequence: poisoned.sequence, transactionId: poisoned.transactionId, error: `poison event #${poisoned.sequence}` });
    // The payload is { amount: 77, status: "secret-status", poison: true }. Match its keys and the number 77 as a JSON
    // value, not the digits "77" anywhere: the event's random UUID holds them in about one marker in seven.
    expect(JSON.stringify(row.failed)).not.toMatch(/secret-status|"amount"|"status"|"poison"|:77[,}]/);
    expect([row.state, row.doc]).toEqual([null, null]);
  });

  it("watch across processes without the doorbell: a standby's follower polls and delivers within its 500 ms", async () => {
    await reset();
    const writerApi = createEventStore({ ...pmConfig, connection: url!, postgres: { table: T, poolSize: 2, persistedModels: true } });
    const watcherApi = createEventStore({ ...pmConfig, connection: url!, postgres: { table: T, poolSize: 2, persistedModels: true } });
    apis.push(writerApi, watcherApi);
    const t0 = await writerApi.append([made("o1", 1, "open")]);
    const writer = writerApi.persistedReadModel(offersModel);
    await writer.list({ atLeast: tokenOf(t0) });
    (await knobsOf(watcherApi)).tickMs = 600_000;
    const standby = watcherApi.persistedReadModel(offersModel);
    const seen: Offer[] = [];
    await standby.watch("o1", (s) => void seen.push(s));
    expect((await standby.status()).role).toBe("standby");
    const started = Date.now();
    await writer.list({ atLeast: tokenOf(await writerApi.append([made("o1", 5)])) });
    await until(() => seen.length === 2, 3_000);
    expect(seen.map((s) => s.total)).toEqual([1, 6]);
    expect(Date.now() - started).toBeLessThan(1_500);
  }, 20_000);

  it("es.close() releases the writer lock; a handle's lock shows in pg_locks while it writes", async () => {
    await reset();
    const api = open();
    const model = api.persistedReadModel(offersModel);
    await until(async () => (await model.status()).role === "writer");
    expect(await lockHolders(admin, T, "offers", 1)).toHaveLength(1);
    await api.close();
    expect(await lockHolders(admin, T, "offers", 1)).toEqual([]);
    expect(await capOf(open())).toBeDefined();
  });

  it("independent event tables share no writer lock: one definition is written at once beside two tables and in a second schema", async () => {
    const targets = [{ table: "pm_ns_a" }, { table: "pm_ns_b" }, { table: "pm_ns_a", schemaName: "pm_ns_other" }];
    const dropAll = async () => {
      await drop(admin, "pm_ns_a");
      await drop(admin, "pm_ns_b");
      await admin.query("DROP SCHEMA IF EXISTS pm_ns_other CASCADE");
    };
    await dropAll();
    await admin.query("CREATE SCHEMA pm_ns_other");
    const stores = targets.map((t) => createEventStore({ ...pmConfig, connection: url!, postgres: { ...t, poolSize: 2, live: true, persistedModels: true } }));
    try {
      const models = stores.map((api) => api.persistedReadModel(offersModel));
      for (const [i, api] of stores.entries()) {
        const t = await api.append([made("o1", i + 1)]);
        const listed = await models[i]!.list({ atLeast: tokenOf(t), timeoutMs: 3_000 });
        expect(listed.rows.map((r) => r.state.total)).toEqual([i + 1]);
      }
      for (const model of models) expect((await model.status())).toMatchObject({ role: "writer", epoch: 1, lag: { events: 0 } });
    } finally {
      for (const api of stores) await api.close();
      await dropAll();
    }
  }, 30_000);

  it("lag under xid/sequence inversion on Postgres: an older transaction's event applied, five of a younger one with lower sequences pending — lag.events is 5; caught up 0", async () => {
    await reset();
    const api = open();
    const older = await admin.connect();
    let open_ = true;
    let release: () => void = () => undefined;
    try {
      await older.query("BEGIN");
      await older.query("SELECT pg_current_xact_id()"); // the older transaction id, allocated before the younger appends
      await api.append(Array.from({ length: 5 }, () => made("o1", 1)));
      const late = { ...made("o2", 1), id: "pm-inverted-late" };
      await older.query(`INSERT INTO "${T}" (event_type, payload) VALUES ($1, $2::jsonb)`, [late.type, JSON.stringify(toPayload(late, schema.idKeyOf("OfferMade")))]);
      await older.query("COMMIT");
      open_ = false;
      const cap = await capOf(api);
      const page = cap.page;
      let pages = 0;
      let entered!: () => void;
      const blocked = new Promise<void>((r) => (entered = r));
      const hold = new Promise<void>((r) => (release = r));
      cap.page = async (ref, epoch, fn) => {
        if (++pages > 1) {
          entered();
          await hold;
        }
        return page(ref, epoch, fn);
      };
      const model = api.persistedReadModel(defineReadModel({ ...offersModel, name: "inverted", batchSize: 1 }));
      await blocked;
      const status = await model.status();
      expect(status.checkpoint!.sequence).toBe(6);
      const pending = (await api.query(source, { cursor: status.checkpoint, settledOnly: true })).events;
      expect(pending.map((e) => e.sequence)).toEqual([1, 2, 3, 4, 5]);
      expect(status.lag.events).toBe(5);
      release();
      cap.page = page;
      await until(async () => (await model.status()).lag.events === 0);
      expect((await model.status()).checkpoint!.sequence).toBe(5);
    } finally {
      release();
      if (open_) await older.query("ROLLBACK");
      older.release();
    }
  });

  it("many handles starting at once in a fresh process: every one writes, and pg warns about nothing (the lock session runs one statement at a time)", async () => {
    await reset();
    const child = spawn("node_modules/.bin/vite-node", ["test/fixtures/persisted-model-startup.ts", url!, T, "60"], { stdio: ["ignore", "pipe", "pipe"] });
    children.push(child);
    let out = "";
    let err = "";
    child.stdout!.on("data", (chunk: Buffer) => (out += chunk.toString()));
    child.stderr!.on("data", (chunk: Buffer) => (err += chunk.toString()));
    const code = await new Promise<number | null>((resolve) => child.on("exit", resolve));
    expect(code, err).toBe(0);
    const report = JSON.parse(out.trim().split("\n").pop()!) as { writers: number; warnings: string[] };
    expect(report).toEqual({ writers: 60, warnings: [] });
    expect(err).not.toMatch(/DeprecationWarning|already executing a query/);
  }, 60_000);
});

describe.skipIf(!url)("persisted read model on Postgres: opt-in, install, schema", () => {
  let admin: pg.Pool;
  beforeAll(() => {
    admin = new pg.Pool({ connectionString: url, max: 1 });
  });
  afterAll(async () => {
    for (const t of ["pm_off_events", "pm_none_events"]) await drop(admin, t);
    await admin.end();
  });

  it("without persistedModels: a failed handle with the UsageError, and no table created", async () => {
    await drop(admin, "pm_off_events");
    const api = createEventStore({ ...pmConfig, connection: url!, postgres: { table: "pm_off_events", poolSize: 1 } });
    await api.append([made("o1", 1)]);
    const model = api.persistedReadModel(offersModel);
    await expect(model.get("o1")).rejects.toThrow(/does not keep persisted read models — create it with persistedModels: true/);
    expect((await model.status()).phase).toBe("failed");
    const r = await admin.query<{ m: string | null; s: string | null }>("SELECT to_regclass('pm_off_events_read_model')::text AS m, to_regclass('pm_off_events_read_model_state')::text AS s");
    expect(r.rows[0]).toEqual({ m: null, s: null });
    await api.close();
  });

  it("install: \"none\" without the tables: the handle fails with an error naming printSchemaSql(schema, { persistedModels: true })", async () => {
    await drop(admin, "pm_none_events");
    const owner = new PostgresStore({ connection: url!, schema, table: "pm_none_events" });
    await owner.ensureInstalled();
    await owner.close();
    const api = createEventStore({ ...pmConfig, connection: url!, postgres: { table: "pm_none_events", install: "none", persistedModels: true, poolSize: 1 } });
    const model = api.persistedReadModel(offersModel);
    // reads reject with the same error meanwhile; the claim's failure fails the handle
    await until(async () => (await model.status().catch(() => undefined))?.phase === "failed");
    const error = (await model.status()).error;
    expect(error).toBeInstanceOf(EventStoreError);
    expect((error as Error).message).toMatch(/read model tables .* do not exist — .*printSchemaSql\(schema, \{ persistedModels: true \}\)/);
    await api.close();
  });

  it("printSchemaSql with persistedModels prints both tables, their indexes and (with rls) their policies; without it none", () => {
    const withIt = printSchemaSql(schema, { persistedModels: true, rls: true, tenantScopeKey: "tenantId" });
    for (const piece of [
      'CREATE TABLE IF NOT EXISTS "public"."events_read_model" (',
      'CREATE TABLE IF NOT EXISTS "public"."events_read_model_state" (',
      '"es_events_rm_revision" ON "public"."events_read_model_state" (tenant, name, version, revision)',
      '"es_events_rm_failed" ON "public"."events_read_model_state" (tenant, name, version) WHERE failed IS NOT NULL',
      '"es_events_rm_doc" ON "public"."events_read_model_state" USING GIN (doc jsonb_path_ops)',
      'ALTER TABLE "public"."events_read_model" FORCE ROW LEVEL SECURITY',
      'CREATE POLICY "es_events_rm_isolation" ON "public"."events_read_model" USING (tenant <> \'\' AND tenant = current_setting(\'app.current_tenant\', true))',
      'CREATE POLICY "es_events_rms_isolation" ON "public"."events_read_model_state"',
    ]) {
      expect(withIt).toContain(piece);
    }
    const without = printSchemaSql(schema, { rls: true, tenantScopeKey: "tenantId" });
    expect(without).not.toContain("read_model");
    expect(() => printSchemaSql(schema, { persistedModels: true, table: "x".repeat(50) })).toThrow(/longer than Postgres' 63 bytes/);
  });
});

describe.skipIf(!url)("persisted read model on Postgres: list cross-check against the memory store and the live model", () => {
  const T = "pm_list_events";
  let admin: pg.Pool;
  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 1 });
    await drop(admin, T);
  });
  afterAll(async () => {
    await drop(admin, T);
    await admin.end();
  });

  it("one generated dataset through a Postgres persisted model, a memory persisted model and a live model: the same keys, total and order for 20 random shapes", async () => {
    const values = fc.oneof(
      fc.record({ kind: fc.constant("num"), value: fc.oneof(fc.integer({ min: -5, max: 5 }), fc.constantFrom(0.5, -1.25, 1e21, 3)) }),
      fc.record({ kind: fc.constant("str"), value: fc.constantFrom("a", "B", "b", "1", "", "", "\u{1F600}", "z\u{1F600}", "zz", "é") }),
      fc.record({ kind: fc.constant("bool"), value: fc.boolean() }),
      fc.record({ kind: fc.constant("null"), value: fc.constant(undefined) }),
      fc.record({ kind: fc.constant("date"), value: fc.integer({ min: 0, max: 10 }) }),
      fc.record({ kind: fc.constant("del"), value: fc.constant(undefined) }),
      fc.record({ kind: fc.constant("arr"), value: fc.constant(1) }),
    );
    const sets = fc.sample(fc.array(fc.record({ key: fc.integer({ min: 0, max: 59 }), field: fc.constantFrom("a", "b", "c"), v: values }), { minLength: 300, maxLength: 300 }), { numRuns: 1, seed: 42 })[0]!;
    const events = sets.map((s) => set(`i${String(s.key).padStart(2, "0")}`, s.field, s.v.kind, s.v.value as number | string | boolean | undefined));
    const pgApi = createEventStore({ ...pmConfig, connection: url!, postgres: { table: T, poolSize: 2, persistedModels: true } });
    const memApi = createEventStore({ ...pmConfig, store: new MemoryStore({ schema, persistedModels: true }) });
    const pgLast = await pgApi.append(events);
    const memLast = await memApi.append(events);
    await admin.query(`VACUUM ANALYZE "${T}_read_model_state"`);
    const pgModel = pgApi.persistedReadModel(itemsModel);
    const memModel = memApi.persistedReadModel(itemsModel);
    const live = memApi.readModel(itemsModel);
    await pgModel.list({ atLeast: tokenOf(pgLast) });
    await memModel.list({ atLeast: tokenOf(memLast) });
    await live.list({ atLeast: tokenOf(memLast) });
    const shape = fc.record({
      where: fc.oneof(
        fc.constant(undefined),
        fc.dictionary(fc.constantFrom("a", "b", "c"), fc.oneof(fc.integer({ min: -2, max: 3 }), fc.constantFrom("a", "b", "1", ""), fc.boolean(), fc.constant(null)), { maxKeys: 2 }),
      ),
      sort: fc.oneof(fc.constant(undefined), fc.record({ by: fc.constantFrom("a", "b", "c", "zzz"), dir: fc.constantFrom("asc" as const, "desc" as const) })),
      offset: fc.integer({ min: 0, max: 40 }),
      limit: fc.integer({ min: 1, max: 25 }),
    });
    const shapes = fc.sample(shape, { numRuns: 20, seed: 7 });
    let compared = 0;
    for (const s of shapes) {
      const options = Object.fromEntries(Object.entries(s).filter(([, v]) => v !== undefined)) as ListOptions;
      const [p, m, l] = await Promise.all([pgModel.list(options), memModel.list(options), live.list(options)]);
      const keys = (r: { rows: readonly { key: string }[] }) => r.rows.map((row) => row.key);
      expect(keys(p), JSON.stringify(options)).toEqual(keys(l));
      expect(keys(m), JSON.stringify(options)).toEqual(keys(l));
      expect([p.total, m.total]).toEqual([l.total, l.total]);
      for (const row of p.rows) expect(row.state).toEqual(((await onRead<Item>(pgApi, row.key, itemsModel)) as { state: Item }).state);
      compared++;
    }
    expect(compared).toBe(20);
    await pgApi.close();
    await memApi.close();
  }, 30_000);
});

describe.skipIf(!url)("persisted read model under rls (non-owner role)", () => {
  const T = "pm_rls_events";
  const APP_ROLE = "es_pm_app";
  let owner: PostgresStore;
  let app: PostgresStore;
  let api: EventStoreApi;
  let admin: pg.Pool;

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await drop(admin, T);
    await admin.query(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${APP_ROLE}') THEN CREATE ROLE ${APP_ROLE} LOGIN PASSWORD '${APP_ROLE}'; END IF; END $$`);
    owner = new PostgresStore({ connection: url!, schema, table: T, rls: true, live: true, persistedModels: true, grantExecuteTo: [APP_ROLE], poolSize: 2 });
    await owner.ensureInstalled();
    await owner.withClient(async (c) => {
      await c.query(`GRANT USAGE ON SCHEMA public TO ${APP_ROLE}`);
      await c.query(`GRANT SELECT, INSERT ON "${T}" TO ${APP_ROLE}`);
      await c.query(`GRANT USAGE, SELECT ON SEQUENCE "${T}_sequence_number_seq" TO ${APP_ROLE}`);
      await c.query(`GRANT SELECT, INSERT, UPDATE ON "${T}_read_model", "${T}_read_model_state" TO ${APP_ROLE}`);
    });
    const appUrl = new URL(url!);
    appUrl.username = APP_ROLE;
    appUrl.password = APP_ROLE;
    app = new PostgresStore({ connection: appUrl.toString(), schema, table: T, rls: true, live: true, persistedModels: true, install: "none", poolSize: 4 });
    api = createEventStore({ ...pmConfig, store: app });
  });
  afterAll(async () => {
    await api.close();
    await owner.close();
    await drop(admin, T);
    await admin.end();
  });

  it("each tenant's get, list, watch, watchList and status see only its own rows; another tenant's session selects none of them", async () => {
    const a = api.forTenant("tenant-a");
    const b = api.forTenant("tenant-b");
    await a.append([made("o1", 1), made("o2", 2)]);
    const tb = await b.append([made("o1", 100), made("o3", 300)]);
    const ma = a.persistedReadModel(offersModel);
    const mb = b.persistedReadModel(offersModel);
    await mb.list({ atLeast: tokenOf(tb) });
    const ta = await a.append([noted("o1")]);
    expect((await ma.list({ atLeast: tokenOf(ta) })).rows.map((r) => [r.key, r.state.total])).toEqual([["o1", 1], ["o2", 2]]);
    expect((await mb.list()).rows.map((r) => [r.key, r.state.total])).toEqual([["o1", 100], ["o3", 300]]);
    for (const [view, model] of [[a, ma], [b, mb]] as const) for (const k of ["o1", "o2", "o3", "o4"]) expect(await model.get(k)).toEqual(((await onRead(view, k)) as { state: Offer }).state);
    expect((await ma.status()).revision).toBe(2);
    expect((await mb.status()).revision).toBe(1);
    const seenKey: Offer[] = [];
    const seenList: unknown[] = [];
    await ma.watch("o1", (s) => void seenKey.push(s));
    await ma.watchList({}, (c) => void seenList.push(c));
    await mb.list({ atLeast: tokenOf(await b.append([made("o1", 1000), noted("o2")])) });
    await wait(400);
    expect(seenKey.map((s) => s.total)).toEqual([1]);
    expect(seenList).toEqual([]);
    // a direct select in B's session, as the application role, returns none of A's rows
    const appUrl = new URL(url!);
    appUrl.username = APP_ROLE;
    appUrl.password = APP_ROLE;
    const appPool = new pg.Pool({ connectionString: appUrl.toString(), max: 1 });
    const c = await appPool.connect();
    await c.query("BEGIN");
    await c.query("SELECT set_config('app.current_tenant', 'tenant-b', true)");
    const states = await c.query<{ tenant: string }>(`SELECT DISTINCT tenant FROM "${T}_read_model_state"`);
    const models = await c.query<{ tenant: string }>(`SELECT DISTINCT tenant FROM "${T}_read_model"`);
    await c.query("COMMIT");
    c.release();
    await appPool.end();
    expect(states.rows.map((r) => r.tenant)).toEqual(["tenant-b"]);
    expect(models.rows.map((r) => r.tenant)).toEqual(["tenant-b"]);
  }, 20_000);

  it("the root view under rls: the handle fails at once with the store's UsageError and writes nothing", async () => {
    const model = api.persistedReadModel(offersModel);
    const error = await model.get("o1").catch((e: unknown) => e);
    expect(error).toBeInstanceOf(UsageError);
    expect((error as Error).message).toMatch(/only in a tenant session: use es\.forTenant\(tenantId\)\.persistedReadModel\(def\)/);
    expect(await model.status()).toMatchObject({ phase: "failed", folded: 0 });
    await expect(model.watch("o1", () => undefined)).rejects.toThrow(/tenant session/);
    const rows = await admin.query(`SELECT count(*)::int AS n FROM "${T}_read_model" WHERE tenant = ''`);
    expect(rows.rows[0]).toEqual({ n: 0 });
  });
});
