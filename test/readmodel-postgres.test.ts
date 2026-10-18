import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { buildSchema, createEventStore, UsageError, type Cursor, type EventStoreApi, type ListChange } from "../src/index.js";
import { PostgresStore } from "../src/postgres/index.js";
import { config, made, noted, offersModel, onRead, readModelSuite, tokenOf, type ModelHarness, type Offer } from "./readmodel.suite.js";

const url = process.env.ES_TEST_DATABASE_URL;
const schema = buildSchema(config.events, { strict: true, tenantScopeKey: config.tenant.scopeKey });
const run = promisify(execFile);
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
const until = async (check: () => boolean, ms = 3_000) => {
  const end = Date.now() + ms;
  while (!check() && Date.now() < end) await wait(5);
  expect(check()).toBe(true);
};

const TABLE = "rm_events";
const RLS_TABLE = "rm_rls_events";
const LIVE_TABLE = "rm_live_events";
const APP_ROLE = "es_rm_app";

/** Every table of this file's prefix: a live model must add none. */
async function rmTables(admin: pg.Pool): Promise<string[]> {
  const r = await admin.query<{ t: string }>(`SELECT tablename AS t FROM pg_tables WHERE schemaname = current_schema() AND tablename LIKE 'rm\\_%' ORDER BY 1`);
  return r.rows.map((row) => row.t);
}

function postgresHarness(admin: () => pg.Pool): ModelHarness {
  const opened: EventStoreApi[] = [];
  return {
    writers: 3,
    async reset() {
      for (const api of opened.splice(0)) await api.close();
      await admin().query(`TRUNCATE "${TABLE}" RESTART IDENTITY`);
    },
    open(options = {}) {
      const api = createEventStore({ ...config, connection: url!, postgres: { table: TABLE, poolSize: 2 }, ...options });
      opened.push(api);
      return api;
    },
    tables: () => rmTables(admin()),
    async closeAll() {
      for (const api of opened.splice(0)) await api.close();
    },
  };
}

describe.skipIf(!url)("live read model on Postgres", () => {
  let admin: pg.Pool;
  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}" CASCADE`);
    const store = new PostgresStore({ connection: url!, schema, table: TABLE });
    await store.ensureInstalled();
    await store.close();
  });
  afterAll(async () => {
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}" CASCADE`);
    await admin.end();
  });

  // concurrent writers: each step appends from up to 3 runtimes with pools of their own at once
  readModelSuite(
    postgresHarness(() => admin),
    { numRuns: 12 },
  );
});

describe.skipIf(!url)("live read model under rls (non-owner role)", () => {
  let owner: PostgresStore;
  let app: PostgresStore;
  let api: EventStoreApi;

  beforeAll(async () => {
    const admin = new pg.Pool({ connectionString: url, max: 1 });
    await admin.query(`DROP TABLE IF EXISTS "${RLS_TABLE}" CASCADE`);
    await admin.query(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${APP_ROLE}') THEN CREATE ROLE ${APP_ROLE} LOGIN PASSWORD '${APP_ROLE}'; END IF; END $$`);
    await admin.end();
    owner = new PostgresStore({ connection: url!, schema, table: RLS_TABLE, rls: true, live: true, grantExecuteTo: [APP_ROLE], poolSize: 2 });
    await owner.ensureInstalled();
    await owner.withClient(async (c) => {
      await c.query(`GRANT USAGE ON SCHEMA public TO ${APP_ROLE}`);
      await c.query(`GRANT SELECT, INSERT ON "${RLS_TABLE}" TO ${APP_ROLE}`);
      await c.query(`GRANT USAGE, SELECT ON SEQUENCE "${RLS_TABLE}_sequence_number_seq" TO ${APP_ROLE}`);
    });
    const appUrl = new URL(url!);
    appUrl.username = APP_ROLE;
    appUrl.password = APP_ROLE;
    app = new PostgresStore({ connection: appUrl.toString(), schema, table: RLS_TABLE, rls: true, live: true, install: "none", poolSize: 4 });
    api = createEventStore({ ...config, store: app });
  });
  afterAll(async () => {
    await api.close();
    await owner.close();
    const admin = new pg.Pool({ connectionString: url, max: 1 });
    await admin.query(`DROP TABLE IF EXISTS "${RLS_TABLE}" CASCADE`);
    await admin.end();
  });

  it("each tenant's get, list, watch and watchList hold and deliver only its own states, equal to fold on read in its view", async () => {
    const a = api.forTenant("tenant-a");
    const b = api.forTenant("tenant-b");
    await a.append([made("o1", 1), made("o2", 2)]);
    await b.append([made("o1", 100), made("o3", 300)]);
    const ma = a.readModel(offersModel);
    const mb = b.readModel(offersModel);
    expect((await ma.list()).rows.map((r) => [r.key, r.state.total])).toEqual([["o1", 1], ["o2", 2]]);
    expect((await mb.list()).rows.map((r) => [r.key, r.state.total])).toEqual([["o1", 100], ["o3", 300]]);
    for (const [view, model] of [[a, ma], [b, mb]] as const) {
      for (const k of ["o1", "o2", "o3", "o4"]) expect(await model.get(k)).toEqual((await onRead(view, k) as { state: Offer }).state);
    }
    const seenKey: Offer[] = [];
    const seenList: ListChange<Offer>[] = [];
    await ma.watch("o1", (s) => void seenKey.push(s));
    await ma.watchList({}, (c) => void seenList.push(c));
    const foldedA = ma.status.folded;
    await mb.get("o1", { atLeast: tokenOf(await b.append([made("o1", 1000), noted("o2")])) });
    await wait(500);
    expect(seenKey.map((s) => s.total)).toEqual([1]);
    expect(seenList).toEqual([]);
    expect(ma.status.folded).toBe(foldedA);
    await ma.get("o1", { atLeast: tokenOf(await a.append([made("o1", 5)])) });
    await until(() => seenKey.length === 2 && seenList.length === 1);
    expect(seenKey[1]!.total).toBe(6);
    expect(seenList[0]!.rows.map((r) => r.key)).toEqual(["o1"]);
  });

  it("the root view under rls: the model fails at start with the store's UsageError and reads nothing", async () => {
    const model = api.readModel(offersModel);
    const error = await model.get("o1").catch((e: unknown) => e);
    expect(error).toBeInstanceOf(UsageError);
    expect((error as Error).message).toMatch(/withTenant/);
    expect(model.status).toMatchObject({ phase: "failed", keys: 0, folded: 0 });
    await expect(model.watch("o1", () => undefined)).rejects.toThrow(/withTenant/);
  });
});

describe.skipIf(!url)("live read model across two processes (live: true)", () => {
  let admin: pg.Pool;
  let api: EventStoreApi;
  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 1 });
    await admin.query(`DROP TABLE IF EXISTS "${LIVE_TABLE}" CASCADE`);
    api = createEventStore({ ...config, connection: url!, postgres: { table: LIVE_TABLE, live: true, poolSize: 2 } });
  });
  afterAll(async () => {
    await api.close();
    await admin.query(`DROP TABLE IF EXISTS "${LIVE_TABLE}" CASCADE`);
    await admin.end();
  });

  it("an append in a child process reaches a watcher of the model in this one, folded once", async () => {
    await api.append([made("o1", 1)]);
    const model = api.readModel(offersModel);
    const seen: { state: Offer; at: number }[] = [];
    await model.watch("o1", (state) => void seen.push({ state, at: Date.now() }));
    await until(() => seen.length === 1);
    const folded = model.status.folded;
    // a separate OS process, the library loaded from source by vite-node
    const { stdout } = await run("node_modules/.bin/vite-node", ["test/fixtures/readmodel-append.ts", url!, LIVE_TABLE, "o1", "41"], { timeout: 30_000 });
    const child = JSON.parse(stdout.trim().split("\n").at(-1)!) as { token: Cursor; at: number };
    await until(() => seen.length === 2, 2_000);
    expect(seen[1]!.state.total).toBe(42);
    expect(model.status.folded).toBe(folded + 1);
    expect(model.status.cursor).toEqual(child.token);
    console.log(`two processes: append in the child → watcher here in ${seen[1]!.at - child.at} ms`);
  }, 40_000);
});
