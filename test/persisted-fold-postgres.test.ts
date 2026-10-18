import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { readFileSync } from "node:fs";
import { buildSchema, createEventStore, defineFold, TenantMismatchError, UsageError, type ContextLoad, type Cursor, type EventStoreApi, type FoldStateStore } from "../src/index.js";
import { PostgresStore, ddlStatements, printSchemaSql } from "../src/postgres/index.js";
import { book, byCategory, byCategoryInitial, config, persistedFoldSuite, Q, recorder, type FoldHarness } from "./persisted-fold.suite.js";

const url = process.env.ES_TEST_DATABASE_URL;
const schema = buildSchema(config.events, { strict: true, tenantScopeKey: config.tenant.scopeKey });

const TABLE = "pf_events";
const FOLD_TABLE = `${TABLE}_fold_state`;

/** Drop an events table and its fold-state table (fixtures always clear both). */
async function dropBoth(admin: pg.Pool, table: string): Promise<void> {
  await admin.query(`DROP TABLE IF EXISTS "${table}_fold_state" CASCADE`);
  await admin.query(`DROP TABLE IF EXISTS "${table}" CASCADE`);
}

function postgresHarness(admin: () => pg.Pool): FoldHarness {
  const opened: EventStoreApi[] = [];
  const open: FoldHarness["open"] = (options = {}) => {
    const api = createEventStore({ ...config, connection: url!, postgres: { table: TABLE, persistedFolds: true, poolSize: 2 }, ...options });
    opened.push(api);
    return api;
  };
  return {
    async reset() {
      for (const api of opened.splice(0)) await api.close();
      await admin().query(
        `DO $$ BEGIN
           IF to_regclass('"${FOLD_TABLE}"') IS NOT NULL THEN TRUNCATE "${FOLD_TABLE}"; END IF;
           IF to_regclass('"${TABLE}"') IS NOT NULL THEN TRUNCATE "${TABLE}" RESTART IDENTITY; END IF;
         END $$`,
      );
    },
    open,
    async foldStates() {
      const store = (await open().store()) as PostgresStore;
      return store.foldStates!;
    },
    async storedRows() {
      const r = await admin().query<{ n: string }>(`SELECT count(*)::text AS n FROM "${FOLD_TABLE}"`);
      return Number(r.rows[0]!.n);
    },
    async corrupt() {
      await admin().query(`UPDATE "${FOLD_TABLE}" SET state = '\\x00'::bytea`);
    },
    async writesOf(api) {
      const store = (await api.store()) as PostgresStore;
      const inner = store.foldStates!;
      const cursors: Cursor[] = [];
      const tapped: FoldStateStore = {
        read: (key) => inner.read(key),
        write: async (key, stored) => {
          cursors.push(stored.cursor);
          return inner.write(key, stored);
        },
      };
      Object.defineProperty(store, "foldStates", { value: tapped, writable: true, configurable: true });
      return cursors;
    },
    async closeAll() {
      for (const api of opened.splice(0)) await api.close();
    },
  };
}

describe.skipIf(!url)("persisted fold on Postgres", () => {
  let admin: pg.Pool;
  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await dropBoth(admin, TABLE);
    // install once, so every harness reset can truncate
    const store = new PostgresStore({ connection: url!, schema, table: TABLE, persistedFolds: true });
    await store.ensureInstalled();
    await store.close();
  });
  afterAll(async () => {
    await dropBoth(admin, TABLE);
    await admin.end();
  });

  persistedFoldSuite(
    postgresHarness(() => admin),
    { numRuns: 15 },
  );

  it("the README's old-rows recipe, as printed, deletes the old versions' fold states and keeps the version in use and other folds", async () => {
    const h = postgresHarness(() => admin);
    await h.reset();
    const a = h.open({ contextCache: { persist: { after: 1 } } });
    await a.append([book(1), book(2)]);
    const v2 = defineFold({ name: "by-category", version: 2, fold: byCategory });
    const other = defineFold({ name: "count", version: 1, fold: (events: readonly unknown[], s: number) => s + events.length });
    await a.context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    await a.context({ query: Q, fold: v2, initial: byCategoryInitial });
    await a.context({ query: Q, fold: other, initial: 0 });
    expect(await h.storedRows()).toBe(3);
    const readme = readFileSync(new URL("../README.md", import.meta.url), "utf8");
    const recipe = /```sql\n(DELETE FROM events_fold_state[\s\S]*?)```/.exec(readme.slice(readme.indexOf("**Old versions' rows.**")))![1]!;
    await admin.query(recipe.trim().replace(/;$/, "").replace("events_fold_state", `"${FOLD_TABLE}"`), ["", "by-category", 2]);
    expect(await h.storedRows()).toBe(2);
    const { loads, onLoad } = recorder();
    const b = h.open({ contextCache: { persist: { after: 1 }, onLoad } });
    await b.context({ query: Q, fold: v2, initial: byCategoryInitial });
    await b.context({ query: Q, fold: other, initial: 0 });
    await b.context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    expect(loads.map((l) => l.start)).toEqual(["stored", "stored", "initial"]);
    await h.closeAll();
  });

  // added by the stage-5 measurer: the paged fold's settled rule on Postgres (the memory store had it, Postgres had no case)
  it("paged fold behind an older open transaction: no page is stored past the settled prefix, so the older transaction's event, committed after the load, is folded by the next process", async () => {
    const h = postgresHarness(() => admin);
    await h.reset();
    const a = h.open();
    for (let i = 0; i < 12; i++) await a.append(Array.from({ length: 10 }, (_, j) => book(1 + ((i * 10 + j) % 5), j % 2 === 0 ? "a" : "b")));
    const v2 = { query: Q, fold: defineFold({ name: "by-category", version: 2, fold: byCategory }), initial: byCategoryInitial };
    const paged = (onLoad?: (load: ContextLoad) => void) => ({ contextCache: { maxEvents: 50, persist: { after: 1 }, ...(onLoad ? { onLoad } : {}) } });
    const p = h.open(paged());
    await p.store(); // installed and connected before the older transaction: a fresh store's install would wait for its lock
    const older = await admin.connect();
    try {
      await older.query("BEGIN");
      // an event of the context in an older transaction, still open: every later append stays unsettled behind it
      await older.query(`INSERT INTO "${TABLE}" (event_type, payload, metadata) SELECT event_type, payload, '{}'::jsonb FROM "${TABLE}" ORDER BY sequence_number DESC LIMIT 1`);
      await a.append(Array.from({ length: 40 }, (_, j) => book(10 + (j % 3), "b")));
      const during = await p.context(v2);
      const visible = (await a.query(Q, { cursor: null })).events;
      expect(visible.length).toBe(160);
      expect(during.state).toEqual(byCategory(visible, byCategoryInitial()));
      await older.query("COMMIT");
    } finally {
      older.release();
    }
    const { loads, onLoad } = recorder();
    const after = await h.open(paged(onLoad)).context(v2);
    const all = (await a.query(Q, { cursor: null })).events;
    expect(all.length).toBe(161);
    expect(after.state).toEqual(byCategory(all, byCategoryInitial()));
    expect(loads[0]).toMatchObject({ start: "stored", events: 41 }); // stored at the settled prefix (120), not beyond
    await h.closeAll();
  });
});

describe.skipIf(!url)("persisted fold: Postgres install, DDL and errors", () => {
  let admin: pg.Pool;
  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
  });
  afterAll(async () => {
    for (const t of ["pf_off", "pf_none", "pf_missing"]) await dropBoth(admin, t);
    await admin.end();
  });

  it("printSchemaSql and ddlStatements carry the table (and its policy under rls) only with persistedFolds", () => {
    const on = printSchemaSql(schema, { table: "pf_x", persistedFolds: true });
    expect(on).toContain('CREATE TABLE IF NOT EXISTS "public"."pf_x_fold_state"');
    expect(on).toContain("PRIMARY KEY (tenant, fold_name, fold_version, query_hash)");
    expect(on).not.toContain("POLICY");
    const rls = printSchemaSql(schema, { table: "pf_x", persistedFolds: true, rls: true });
    expect(rls).toContain('ALTER TABLE "public"."pf_x_fold_state" FORCE ROW LEVEL SECURITY');
    expect(rls).toContain(`USING (tenant <> '' AND tenant = current_setting('app.current_tenant', true))`);
    const off = [printSchemaSql(schema, { table: "pf_x" }), printSchemaSql(schema, { table: "pf_x", rls: true }), ddlStatements(schema, { table: "pf_x" }).join("\n")];
    for (const sql of off) expect(sql).not.toContain("fold_state");
  });

  it("without persistedFolds there is no table and no foldStates", async () => {
    await dropBoth(admin, "pf_off");
    const store = new PostgresStore({ connection: url!, schema, table: "pf_off" });
    await store.ensureInstalled();
    expect(store.foldStates).toBeUndefined();
    expect((store.withTenant("A") as { foldStates?: unknown }).foldStates).toBeUndefined();
    const r = await admin.query<{ t: string | null }>("SELECT to_regclass('pf_off_fold_state')::text AS t");
    expect(r.rows[0]!.t).toBeNull();
    await store.close();
  });

  it("install: none with the printed DDL run by hand works; a missing table is a clear error that fails the load", async () => {
    await dropBoth(admin, "pf_none");
    await admin.query(printSchemaSql(schema, { table: "pf_none", persistedFolds: true }));
    const { loads, onLoad } = recorder();
    const api = createEventStore({ ...config, connection: url!, postgres: { table: "pf_none", persistedFolds: true, install: "none", poolSize: 2 }, contextCache: { persist: { after: 1 }, onLoad } });
    await api.append([book(3)]);
    await api.context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    expect(loads[0]!.stored?.written).toBe(true);
    await api.close();

    await dropBoth(admin, "pf_missing");
    await admin.query(printSchemaSql(schema, { table: "pf_missing" })); // events only
    const missing = createEventStore({ ...config, connection: url!, postgres: { table: "pf_missing", persistedFolds: true, install: "none", poolSize: 2 } });
    await missing.append([book(3)]);
    await expect(missing.context({ query: Q, fold: byCategory, initial: byCategoryInitial })).rejects.toThrow(
      /fold-state table public\.pf_missing_fold_state does not exist.*printSchemaSql\(schema, \{ persistedFolds: true \}\)/,
    );
    await missing.close();
  });

  it("a table name that would pass 63 bytes with _fold_state is refused at construction", () => {
    expect(() => new PostgresStore({ connection: url!, schema, table: "t".repeat(53), persistedFolds: true })).toThrow(UsageError);
    expect(() => new PostgresStore({ connection: url!, schema, table: "t".repeat(53), persistedFolds: true })).toThrow(/63 bytes/);
  });
});

const RLS_TABLE = "pf_rls";
const RLS_FOLD = `${RLS_TABLE}_fold_state`;
const APP_ROLE = "es_app";

describe.skipIf(!url)("persisted fold under rls with a non-owner role", () => {
  let admin: pg.Pool;
  let owner: PostgresStore;
  let appUrl: string;
  const opened: EventStoreApi[] = [];
  const open = (onLoad?: (load: ContextLoad) => void) => {
    const api = createEventStore({
      ...config,
      connection: appUrl,
      postgres: { table: RLS_TABLE, rls: true, persistedFolds: true, install: "none", poolSize: 2 },
      contextCache: { persist: { after: 1 }, ...(onLoad ? { onLoad } : {}) },
    });
    opened.push(api);
    return api;
  };

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await dropBoth(admin, RLS_TABLE);
    await admin.query(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${APP_ROLE}') THEN CREATE ROLE ${APP_ROLE} LOGIN PASSWORD '${APP_ROLE}'; END IF; END $$`);
    owner = new PostgresStore({ connection: url!, schema, table: RLS_TABLE, rls: true, persistedFolds: true, grantExecuteTo: [APP_ROLE], poolSize: 2 });
    await owner.ensureInstalled();
    await owner.withClient(async (c) => {
      await c.query(`GRANT USAGE ON SCHEMA public TO ${APP_ROLE}`);
      await c.query(`GRANT SELECT, INSERT ON "${RLS_TABLE}" TO ${APP_ROLE}`);
      await c.query(`GRANT USAGE, SELECT ON SEQUENCE "${RLS_TABLE}_sequence_number_seq" TO ${APP_ROLE}`);
      await c.query(`GRANT SELECT, INSERT, UPDATE ON "${RLS_FOLD}" TO ${APP_ROLE}`);
    });
    const u = new URL(url!);
    u.username = APP_ROLE;
    u.password = APP_ROLE;
    appUrl = u.toString();
  });

  afterAll(async () => {
    for (const api of opened.splice(0)) await api.close();
    await owner?.close();
    await dropBoth(admin, RLS_TABLE);
    await admin.end();
  });

  it("the install enabled and forced rls with the fold-state policy", async () => {
    const flags = await admin.query<{ rls: boolean; force: boolean }>("SELECT relrowsecurity AS rls, relforcerowsecurity AS force FROM pg_class WHERE relname = $1", [RLS_FOLD]);
    expect(flags.rows[0]).toEqual({ rls: true, force: true });
    const policy = await admin.query<{ qual: string }>("SELECT qual FROM pg_policies WHERE tablename = $1", [RLS_FOLD]);
    expect(policy.rows).toHaveLength(1);
    expect(policy.rows[0]!.qual).toMatch(/tenant <> ''.*current_setting\('app\.current_tenant'/);
    // a second install finds the policy up to date and keeps it
    const again = new PostgresStore({ connection: url!, schema, table: RLS_TABLE, rls: true, persistedFolds: true, grantExecuteTo: [APP_ROLE], poolSize: 1 });
    await again.ensureInstalled();
    await again.close();
    expect((await admin.query("SELECT 1 FROM pg_policies WHERE tablename = $1", [RLS_FOLD])).rows).toHaveLength(1);
  });

  it("one tenant's stored state is invisible to another; each keeps its own row", async () => {
    const { loads, onLoad } = recorder();
    const p = open(onLoad);
    await p.forTenant("A").append([book(1), book(2)]);
    await p.forTenant("A").context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    const before = await admin.query(`SELECT tenant, cursor_xid::text, cursor_seq, state FROM "${RLS_FOLD}"`);
    expect(before.rows.map((r) => r.tenant)).toEqual(["A"]);
    const q = open(onLoad);
    const b = await q.forTenant("B").context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    expect(b.state.size).toBe(0);
    expect(loads[1]).toMatchObject({ tenant: "B", start: "initial", events: 0 });
    const a = await q.forTenant("A").context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    expect(loads[2]).toMatchObject({ tenant: "A", start: "stored", events: 0 });
    expect(a.state.get("a")).toEqual({ count: 2, total: 3 });
    const after = await admin.query(`SELECT tenant, cursor_xid::text, cursor_seq, state FROM "${RLS_FOLD}" WHERE tenant = 'A'`);
    expect(after.rows).toEqual(before.rows);
  });

  it("the root store refuses fold states, and the empty tenant gets a clear error, never a '' row", async () => {
    const root = (await open().store()) as PostgresStore;
    await expect(root.foldStates!.read({ name: "x", version: 1, key: "k" })).rejects.toThrow(UsageError);
    await expect(root.foldStates!.write({ name: "x", version: 1, key: "k" }, { cursor: { transactionId: "1", sequence: 1 }, state: Uint8Array.from([1]) })).rejects.toThrow(
      /tenant session/,
    );
    await expect(open().forTenant("").context({ query: Q, fold: byCategory, initial: byCategoryInitial })).rejects.toThrow(UsageError);
    const empty = await admin.query(`SELECT count(*)::int AS n FROM "${RLS_FOLD}" WHERE tenant = ''`);
    expect(empty.rows[0]!.n).toBe(0);
    // a key naming another tenant never reaches the database
    const session = root.withTenant("A").foldStates!;
    await expect(session.read({ tenant: "B", name: "x", version: 1, key: "k" })).rejects.toThrow(TenantMismatchError);
  });

  it("a '' row stays invisible on a pooled connection after a tenant transaction ended; another tenant's row cannot be written", async () => {
    await admin.query(
      `INSERT INTO "${RLS_FOLD}" (tenant, fold_name, fold_version, query_hash, query_key, cursor_xid, cursor_seq, state) VALUES ('', 'leak', 1, '\\x01', 'k', '1', 1, '\\x00')`,
    );
    const client = new pg.Client({ connectionString: appUrl });
    await client.connect();
    try {
      await client.query("BEGIN");
      await client.query("SELECT set_config('app.current_tenant', 'A', true)");
      await client.query("COMMIT");
      // the setting is back to '' now — the condition the events policy alone would let through
      const seen = await client.query(`SELECT count(*)::int AS n FROM "${RLS_FOLD}"`);
      expect(seen.rows[0]!.n).toBe(0);
      await client.query("BEGIN");
      await client.query("SELECT set_config('app.current_tenant', 'A', true)");
      await expect(
        client.query(
          `INSERT INTO "${RLS_FOLD}" (tenant, fold_name, fold_version, query_hash, query_key, cursor_xid, cursor_seq, state) VALUES ('B', 'f', 1, '\\x02', 'k', '1', 1, '\\x00')`,
        ),
      ).rejects.toMatchObject({ code: "42501" });
      await client.query("ROLLBACK");
    } finally {
      await client.end();
      await admin.query(`DELETE FROM "${RLS_FOLD}" WHERE tenant = ''`);
    }
  });
});
