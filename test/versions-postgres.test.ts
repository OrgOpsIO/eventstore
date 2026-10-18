import { spawn, type ChildProcess } from "node:child_process";
import { readFileSync } from "node:fs";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildSchema, compareCursor, createEventStore, UsageError, type Cursor, type EventStoreApi } from "../src/index.js";
import { PostgresStore, printSchemaSql } from "../src/postgres/index.js";
import { block, onRead, until, wait } from "./persisted-model.suite.js";
import { versionsSuite, type VersionsHarness } from "./versions.suite.js";
import { keysOf, made, noted, offersV1, offersV2, pmConfig, tokenOf, type Offer } from "./versions.model.js";

const url = process.env.ES_TEST_DATABASE_URL;
const schema = buildSchema(pmConfig.events, { strict: true, tenantScopeKey: pmConfig.tenant.scopeKey });

const tablesOf = (table: string) => [table, `${table}_read_model`, `${table}_read_model_state`];

async function drop(admin: pg.Pool, table: string): Promise<void> {
  for (const t of tablesOf(table).reverse()) await admin.query(`DROP TABLE IF EXISTS "${t}" CASCADE`);
}

/** The first ```sql block after `heading` in README.md, as printed. */
function readmeSql(heading: string): string {
  const text = readFileSync(new URL("../README.md", import.meta.url), "utf8");
  const from = text.indexOf(heading);
  expect(from, heading).toBeGreaterThanOrEqual(0);
  const match = /```sql\n([\s\S]*?)```/.exec(text.slice(from));
  expect(match, `a sql block after ${heading}`).not.toBeNull();
  return match![1]!;
}

/**
 * Run a README recipe against `table` on one connection: the default table names replaced, comments dropped, each
 * statement with the parameters it names.
 */
export async function runRecipe(admin: pg.Pool, sql: string, table: string, params: readonly unknown[]): Promise<void> {
  const text = sql
    .replace(/events_(read_model_state|read_model|fold_state)\b/g, (_, rest: string) => `"${table}_${rest}"`)
    .split("\n")
    .filter((line) => !line.trim().startsWith("--"))
    .join("\n");
  const statements = text
    .split(/;\s*\n/)
    .map((s) => s.trim().replace(/;$/, ""))
    .filter((s) => s.length > 0);
  const client = await admin.connect();
  try {
    for (const statement of statements) {
      const used = Math.max(0, ...[...statement.matchAll(/\$(\d+)/g)].map((m) => Number(m[1])));
      await client.query(statement, params.slice(0, used) as unknown[]);
    }
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

const RETIRE = "**Retiring an old version.**";

function postgresHarness(admin: () => pg.Pool, table: string): VersionsHarness {
  const opened: EventStoreApi[] = [];
  return {
    big: 5_000,
    writers: 3,
    async reset() {
      await this.closeAll();
      await admin().query(`TRUNCATE "${table}", "${table}_read_model", "${table}_read_model_state" RESTART IDENTITY`);
    },
    open(options = {}) {
      const api = createEventStore({ ...pmConfig, connection: url!, postgres: { table, poolSize: 2, live: true, persistedModels: true }, ...options });
      opened.push(api);
      return api;
    },
    async retire(name, newVersion) {
      await runRecipe(admin(), readmeSql(RETIRE), table, ["", name, newVersion]);
    },
    async stateRows(name, version) {
      const r = await admin().query<{ n: number }>(`SELECT count(*)::int AS n FROM "${table}_read_model_state" WHERE name = $1 AND version = $2`, [name, version]);
      return r.rows[0]!.n;
    },
    async corrupt(_api, ref, key) {
      await admin().query(`UPDATE "${table}_read_model_state" SET state = '\\x00'::bytea WHERE name = $1 AND version = $2 AND key = $3`, [ref.name, ref.version, key]);
    },
    async closeAll() {
      for (const api of opened.splice(0)) await api.close().catch(() => undefined); // a test may have closed it (a pool ends once)
    },
  };
}

describe.skipIf(!url)("versions and rebuild on Postgres", () => {
  const T = "vs_events";
  let admin: pg.Pool;
  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 3 });
    await drop(admin, T);
    const store = new PostgresStore({ connection: url!, schema, table: T, persistedModels: true, live: true });
    await store.ensureInstalled();
    await store.close();
  });
  afterAll(async () => {
    await drop(admin, T);
    await admin.end();
  });

  versionsSuite(
    postgresHarness(() => admin, T),
    { numRuns: 10 },
  );
});

describe.skipIf(!url)("versions and rebuild on Postgres: processes, snapshots, rls, DDL", () => {
  const T = "vs_proc_events";
  let admin: pg.Pool;
  const children: ChildProcess[] = [];
  let h: VersionsHarness;
  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 3 });
    await drop(admin, T);
    const store = new PostgresStore({ connection: url!, schema, table: T, persistedModels: true, live: true });
    await store.ensureInstalled();
    await store.close();
    h = postgresHarness(() => admin, T);
  });
  afterAll(async () => {
    for (const child of children.splice(0)) if (child.exitCode === null) child.kill("SIGKILL");
    await h.closeAll();
    await drop(admin, T);
    await admin.end();
  });

  type Line = { at: number; version: number; phase?: string; role?: string; building?: boolean; lag?: number; get?: string; list?: string; error?: string };
  /** One deploy: a vite-node child running only `version`'s definition, reporting a line every 100 ms. */
  function deploy(version: 1 | 2): { child: ChildProcess; lines: Line[] } {
    const child = spawn("node_modules/.bin/vite-node", ["test/fixtures/versions-child.ts", url!, T, String(version), "k1"], { stdio: ["pipe", "pipe", "inherit"] });
    children.push(child);
    const lines: Line[] = [];
    let buffer = "";
    child.stdout!.on("data", (chunk: Buffer) => {
      buffer += chunk.toString();
      const parts = buffer.split("\n");
      buffer = parts.pop()!;
      for (const part of parts) {
        try {
          const parsed = JSON.parse(part) as Line;
          if (typeof parsed.version === "number") lines.push(parsed);
        } catch {
          // vite-node's own output
        }
      }
    });
    return { child, lines };
  }
  const ended = (child: ChildProcess) => new Promise<void>((resolve) => (child.exitCode !== null ? resolve() : child.once("exit", () => resolve())));

  it("a rolling deploy in two processes: old code answers from v1 throughout; new code's v2 get equals its fold on read at once, its list waits, it switches; after the old process is gone the recipe removes v1's rows and v2 is unaffected", async () => {
    await h.reset();
    const parent = h.open();
    const keys = keysOf(20);
    for (let i = 0; i < 2_000; i += 500) await parent.append(Array.from({ length: 500 }, (_, j) => made(keys[(i + j) % 20]!, 1)));
    const old = deploy(1);
    await until(() => old.lines.some((l) => l.role === "writer" && l.list === "total 20"), 60_000);
    let streaming = true;
    let last: Cursor | undefined;
    const stream = (async () => {
      for (let i = 0; streaming; i++) {
        last = tokenOf(await parent.append([made(keys[i % 20]!, 1)]));
        await wait(10);
      }
    })();
    let fresh: ReturnType<typeof deploy>;
    try {
      // the new deploy starts; its writer is held off a moment, so the lines while it builds are certain
      const release = await block(parent, offersV2);
      fresh = deploy(2);
      try {
        await until(() => fresh.lines.filter((l) => l.building === true).length >= 3, 60_000);
      } finally {
        await release();
      }
      await until(() => fresh.lines.some((l) => l.building === false), 60_000);
      const switchedAt = fresh.lines.find((l) => l.building === false)!.at;
      const building = fresh.lines.filter((l) => l.building === true);
      for (const line of building) {
        expect(line.get).toBe("equal");
        expect(line.list).toBe("waiting");
      }
      // the old process keeps answering list from v1 after the switch
      await until(() => old.lines.some((l) => l.at > switchedAt + 300 && l.list === "total 20" && l.phase === "live"), 10_000);
      await until(() => fresh.lines.some((l) => l.at > switchedAt && l.list === "total 20"), 10_000);
      for (const line of fresh.lines) expect(line.get).toBe("equal");
      console.log(`[versions] rolling deploy: ${building.length} lines of the new process while building (get equal, list waiting); switched at lag ${fresh.lines.find((l) => l.building === false)!.lag}`);
    } finally {
      streaming = false;
      await stream;
    }
    // the old deploy is gone; the operator retires v1
    old.child.stdin!.end();
    await ended(old.child);
    await runRecipe(admin, readmeSql(RETIRE), T, ["", "offers", 2]);
    const rows = await admin.query<{ n: number }>(`SELECT count(*)::int AS n FROM "${T}_read_model_state" WHERE name = 'offers' AND version = 1`);
    expect(rows.rows[0]!.n).toBe(0);
    const model = await admin.query<{ status: string }>(`SELECT status FROM "${T}_read_model" WHERE name = 'offers' ORDER BY version`);
    expect(model.rows.map((r) => r.status)).toEqual(["retired", "active"]);
    const after = fresh.lines.length;
    await until(() => fresh.lines.length > after + 3, 10_000);
    for (const line of fresh.lines.slice(after)) {
      expect(line.list).toBe("total 20");
      expect(line.get).toBe("equal");
    }
    const v2 = parent.persistedReadModel(offersV2);
    expect((await v2.list({ atLeast: last! })).total).toBe(20);
    fresh.child.stdin!.end();
    await ended(fresh.child);
    await h.closeAll();
  }, 180_000);

  it("50 live starts from the persisted rows while 3 processes append and the writer runs: every key of every start equals fold on read (one snapshot)", async () => {
    await h.reset();
    const writer = h.open();
    const keys = keysOf(25);
    const t0 = await writer.append(keys.map((k) => made(k, 1)));
    const persisted = writer.persistedReadModel(offersV1);
    await persisted.list({ atLeast: tokenOf(t0) });
    const appenders = [h.open(), h.open(), h.open()];
    let paused = false;
    let running = true;
    let inflight = 0;
    let last: Cursor = tokenOf(t0);
    const loops = appenders.map(async (api, n) => {
      for (let i = 0; running; i++) {
        if (paused) {
          await wait(2);
          continue;
        }
        inflight++;
        try {
          const r = await api.append([made(keys[(i * 7 + n) % keys.length]!, 1), noted(keys[(i + n) % keys.length]!)]);
          if (compareCursor(tokenOf(r), last) > 0) last = tokenOf(r);
        } finally {
          inflight--;
        }
      }
    });
    let wrong = 0;
    let folded = 0;
    for (let start = 0; start < 50; start++) {
      const reader = createEventStore({ ...pmConfig, connection: url!, postgres: { table: T, poolSize: 1, live: true, persistedModels: true } });
      try {
        const live = reader.readModel(offersV1, { start: "persisted" });
        await live.get("k0", { timeoutMs: 20_000 }); // the start happened while the appenders ran
        paused = true;
        while (inflight > 0) await wait(1);
        const token = last;
        for (const k of keys) {
          const got = await live.get(k, { atLeast: token, timeoutMs: 20_000 });
          const want = await onRead<Offer>(reader, k, offersV1);
          if (!want.ok || JSON.stringify(got) !== JSON.stringify(want.state)) wrong++;
        }
        folded += live.status.folded;
        paused = false;
      } finally {
        await reader.close();
      }
    }
    running = false;
    await Promise.all(loops);
    console.log(`[versions] 50 starts under 3 appenders: ${wrong} wrong keys, ${folded} events folded after the snapshots in total`);
    expect(wrong).toBe(0);
    await h.closeAll();
  }, 180_000);

  it("printSchemaSql: the model row's status (with its CHECK) and fingerprint with persistedModels, the fold state's fingerprint with persistedFolds, none without", () => {
    const models = printSchemaSql(schema, { persistedModels: true });
    expect(models).toContain("status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('building', 'active', 'retired'))");
    expect(models).toMatch(/_read_model" \([^;]*fingerprint TEXT,/);
    const folds = printSchemaSql(schema, { persistedFolds: true });
    expect(folds).toMatch(/_fold_state" \([^;]*fingerprint TEXT,/);
    const none = printSchemaSql(schema, {});
    expect(none).not.toContain("fingerprint");
    expect(none).not.toContain("'building'");
  });
});

describe.skipIf(!url)("versions under rls (non-owner role): a live start loads only its tenant's rows", () => {
  const T = "vs_rls_events";
  const APP_ROLE = "es_vs_app";
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

  it("tenant B's live start holds B's keys only, folds nothing beyond its checkpoint, equals B's fold on read; the root view's start fails", async () => {
    const a = api.forTenant("tenant-a");
    const b = api.forTenant("tenant-b");
    const ta = await a.append([made("a1", 1), made("a2", 2), noted("a1")]);
    const tb = await b.append([made("b1", 10), made("a1", 99)]);
    await a.persistedReadModel(offersV1).list({ atLeast: tokenOf(ta) });
    await b.persistedReadModel(offersV1).list({ atLeast: tokenOf(tb) });
    // a second runtime over the same app role: its live model starts from B's rows
    const other = createEventStore({ ...pmConfig, store: app });
    const live = other.forTenant("tenant-b").readModel(offersV1, { start: "persisted" });
    expect((await live.list({ timeoutMs: 20_000 })).rows.map((r) => r.key)).toEqual(["a1", "b1"]);
    expect(live.status.folded).toBe(0);
    expect(live.status.keys).toBe(2);
    for (const k of ["a1", "a2", "b1"]) expect(await live.get(k)).toEqual(((await onRead(other.forTenant("tenant-b"), k, offersV1)) as { state: Offer }).state);
    expect((await live.get("a1")).total).toBe(99); // B's a1, never A's
    const root = other.readModel(offersV1, { start: "persisted" });
    const error = await root.get("a1").catch((e: unknown) => e);
    expect(error).toBeInstanceOf(UsageError);
    expect(root.status.phase).toBe("failed");
    await live.stop();
    // the store itself is shared with `api`: do not close it through this runtime
  }, 30_000);
});
