/**
 * Stage 6 under row-level security, as the scope asks: a non-superuser table owner that installs (bound by
 * FORCE), a non-owner application role, and the processor role of decision 46. The tenants suite runs here
 * too; then the isolation of every kept state (model §6.8) and the README erasure recipe, as printed, with a
 * cross-tenant processor running through it (model §6.9). Tables in a schema of their own, dropped at the end.
 */
import { readFileSync } from "node:fs";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildSchema, createEventStore, defineFold, UsageError, type Cursor, type EventStoreApi, type EventStoreConfig, type LiveReadModel, type PersistedReadModel } from "../src/index.js";
import { keyQuery } from "../src/readmodel.js";
import { PostgresStore } from "../src/postgres/index.js";
import { capOf, made, noted, offerFold, initialOffer, offersModel, onRead, pmConfig, source, tokenOf, until, wait, type Offer } from "./persisted-model.suite.js";
import { crossModel, tenantsSuite, type TenantsHarness } from "./tenants.suite.js";

const url = process.env.ES_TEST_DATABASE_URL;
const schema = buildSchema(pmConfig.events, { strict: true, tenantScopeKey: pmConfig.tenant.scopeKey });

const SCHEMA = "t6_rls";
const T = "t6r_events";
const OWNER = "es_t6_owner";
const APP = "es_t6_app";
const PROC = "es_t6_proc";
const KEPT = [`${T}_fold_state`, `${T}_read_model`, `${T}_read_model_state`] as const;
const q = (table: string) => `"${SCHEMA}"."${table}"`;

function roleUrl(role: string): string {
  const u = new URL(url!);
  u.username = role;
  u.password = role;
  return u.toString();
}

const postgresOptions = (install: "auto" | "none") =>
  ({ schemaName: SCHEMA, table: T, rls: true, live: true, persistedFolds: true, persistedModels: true, processorRole: PROC, install, poolSize: 3 }) as const;

/** The first ```sql block after `heading` in README.md, as printed. */
function readmeSql(heading: string): string {
  const text = readFileSync(new URL("../README.md", import.meta.url), "utf8");
  const from = text.indexOf(heading);
  expect(from, heading).toBeGreaterThanOrEqual(0);
  const match = /```sql\n([\s\S]*?)```/.exec(text.slice(from));
  expect(match, `a sql block after ${heading}`).not.toBeNull();
  return match![1]!;
}

/** The README erasure recipe's statements, for this schema and table, as printed (indentation and comments aside). */
function erasureStatements(options: { readonly withoutLock?: boolean } = {}): string[] {
  const text = readmeSql("**Erasing a tenant**")
    .replace("hashtext('public.events')", `hashtext('${SCHEMA}.${T}')`)
    .replace(/\bevents_(read_model_state|read_model|fold_state)\b/g, (_, rest: string) => `${T}_${rest}`)
    .replace(/\bevents\b(?!_)/g, T)
    .replace("'<tenantKey>'", "'tenantId'");
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("--"))
    .join("\n")
    .split(/;\s*\n?/)
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
    .filter((s) => !(options.withoutLock && s.includes("pg_advisory_xact_lock")));
}

/**
 * Run the recipe's step 2 as the table owner, with the step-1 count check before COMMIT; `onStarted` runs once the
 * first statement was sent (the lock may make it wait); `beforeCommit` runs after every `DELETE`, with the erasure
 * still open. The deleted row counts per table.
 */
async function eraseTenant(
  tenant: string,
  expected: number,
  options: { readonly withoutLock?: boolean; readonly onWaiting?: (pid: number) => Promise<void>; readonly beforeCommit?: () => Promise<void> } = {},
): Promise<Record<string, number>> {
  const owner = new pg.Client({ connectionString: roleUrl(OWNER), options: `-c search_path=${SCHEMA}` });
  await owner.connect();
  const deleted: Record<string, number> = {};
  try {
    const pid = ((await owner.query<{ pid: number }>("SELECT pg_backend_pid() AS pid")).rows[0]!).pid;
    for (const statement of erasureStatements(options)) {
      if (/^COMMIT$/i.test(statement)) {
        await options.beforeCommit?.();
        if (deleted[T] !== expected) {
          await owner.query("ROLLBACK");
          throw new Error(`erasure count check: the events DELETE removed ${deleted[T]} rows, step 1 counted ${expected} — rolled back`);
        }
        await owner.query("COMMIT");
        continue;
      }
      const used = Math.max(0, ...[...statement.matchAll(/\$(\d+)/g)].map((m) => Number(m[1])));
      const running = owner.query(statement, [tenant].slice(0, used));
      if (/pg_advisory_xact_lock/.test(statement) && options.onWaiting) await options.onWaiting(pid);
      const result = await running;
      const table = /^DELETE FROM (\w+)/i.exec(statement)?.[1];
      if (table) deleted[table] = result.rowCount ?? 0;
    }
  } catch (error) {
    await owner.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    await owner.end();
  }
  return deleted;
}

describe.skipIf(!url)("tenants under rls: a non-owner application role, the processor role, a non-superuser owner", () => {
  let admin: pg.Pool;
  const opened: EventStoreApi[] = [];
  const open = (options: Partial<EventStoreConfig> = {}): EventStoreApi => {
    const api = createEventStore({ ...pmConfig, connection: roleUrl(APP), postgres: postgresOptions("none"), ...options });
    opened.push(api);
    return api;
  };
  const processor = (options: Partial<EventStoreConfig> = {}): EventStoreApi => {
    const api = createEventStore({ ...pmConfig, connection: roleUrl(PROC), postgres: postgresOptions("none"), ...options });
    opened.push(api);
    return api;
  };
  const harness: TenantsHarness = {
    async reset() {
      await this.closeAll();
      await admin.query(`TRUNCATE ${q(T)}, ${KEPT.map(q).join(", ")} RESTART IDENTITY`);
    },
    open,
    processor,
    async closeAll() {
      for (const api of opened.splice(0)) await api.close().catch(() => undefined);
    },
  };

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 3 });
    await admin.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
    for (const role of [OWNER, APP, PROC]) {
      await admin.query(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${role}') THEN CREATE ROLE ${role} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '${role}'; END IF; END $$`);
    }
    await admin.query(`CREATE SCHEMA ${SCHEMA} AUTHORIZATION ${OWNER}`);
    // the installer runs CREATE SCHEMA IF NOT EXISTS, which Postgres checks against the database's CREATE privilege
    await admin.query(`GRANT CREATE ON DATABASE "${new URL(url!).pathname.slice(1)}" TO ${OWNER}`);
    const owner = new PostgresStore({ connection: roleUrl(OWNER), schema, ...postgresOptions("auto"), grantExecuteTo: [APP] });
    await owner.ensureInstalled();
    await owner.withClient(async (c) => {
      await c.query(`GRANT USAGE ON SCHEMA ${SCHEMA} TO ${APP}, ${PROC}`);
      await c.query(`GRANT SELECT, INSERT ON ${q(T)} TO ${APP}`);
      await c.query(`GRANT USAGE, SELECT ON SEQUENCE ${q(`${T}_sequence_number_seq`)} TO ${APP}`);
      await c.query(`GRANT SELECT, INSERT, UPDATE ON ${KEPT.map(q).join(", ")} TO ${APP}`);
      await c.query(`GRANT SELECT ON ${q(T)} TO ${PROC}`);
      await c.query(`GRANT SELECT, INSERT, UPDATE ON ${q(`${T}_read_model`)}, ${q(`${T}_read_model_state`)} TO ${PROC}`);
    });
    await owner.close();
  });
  afterAll(async () => {
    await harness.closeAll();
    await admin.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
    await admin.query(`REVOKE CREATE ON DATABASE "${new URL(url!).pathname.slice(1)}" FROM ${OWNER}`);
    await admin.end();
  });

  tenantsSuite(harness, { numRuns: 8 });

  it("the roles: the owner is no superuser and bound by FORCE, the application role is not a member of the processor role", async () => {
    const roles = await admin.query<{ rolname: string; rolsuper: boolean; rolbypassrls: boolean }>(
      "SELECT rolname, rolsuper, rolbypassrls FROM pg_roles WHERE rolname = ANY($1) ORDER BY rolname",
      [[OWNER, APP, PROC]],
    );
    for (const role of roles.rows) expect(role).toMatchObject({ rolsuper: false, rolbypassrls: false });
    const member = await admin.query<{ m: boolean }>("SELECT pg_has_role($1, $2, 'MEMBER') AS m", [APP, PROC]);
    expect(member.rows[0]!.m).toBe(false);
    const forced = await admin.query<{ relname: string; relforcerowsecurity: boolean }>(
      "SELECT c.relname, c.relforcerowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = ANY($2) ORDER BY 1",
      [SCHEMA, [T, ...KEPT]],
    );
    expect(forced.rows.map((r) => r.relforcerowsecurity)).toEqual([true, true, true, true]);
  });

  it("isolation of every kept state: in A's session the application role sees only A's fold states, model rows and state rows — never B's, never the processor row, nothing with the reset value; it cannot write B's; the processor role sees every tenant and cannot append", async () => {
    await harness.reset();
    const app = open({ contextCache: { persist: { after: 1 } } });
    const proc = processor();
    const byOffer = defineFold<Offer>({ name: "by-offer", version: 1, fold: offerFold });
    const cross = crossModel("iso-x");
    proc.persistedReadModel(cross);
    const last = new Map<string, Cursor>();
    for (const [t, amount] of [["ta", 1], ["tb", 100]] as const) {
      last.set(t, tokenOf(await app.forTenant(t).append([made("o1", amount), made("o2", amount + 1), noted("o1")])));
      await app.forTenant(t).context({ query: keyQuery(source, "offerId", "o1"), fold: byOffer, initial: initialOffer }); // a persisted fold row
      await app.forTenant(t).persistedReadModel(offersModel).list({ atLeast: last.get(t)! }); // a per-tenant model
      await app.forTenant(t).persistedReadModel(cross).list({ atLeast: last.get(t)! }); // the processor's rows
    }
    const all = async (table: string) => (await admin.query<{ tenant: string }>(`SELECT tenant FROM ${q(table)}`)).rows.map((r) => r.tenant);
    expect(new Set(await all(`${T}_fold_state`))).toEqual(new Set(["ta", "tb"]));
    expect(new Set(await all(`${T}_read_model`))).toEqual(new Set(["", "ta", "tb"]));
    // as the application role, by hand
    const appPool = new pg.Pool({ connectionString: roleUrl(APP), max: 1 });
    const inSession = async <R>(tenant: string | null, fn: (c: pg.PoolClient) => Promise<R>): Promise<R> => {
      const c = await appPool.connect();
      try {
        await c.query("BEGIN");
        if (tenant !== null) await c.query("SELECT set_config('app.current_tenant', $1, true)", [tenant]);
        return await fn(c);
      } finally {
        await c.query("ROLLBACK").catch(() => undefined);
        c.release();
      }
    };
    for (const table of KEPT) {
      const seen = await inSession("ta", async (c) => (await c.query<{ tenant: string }>(`SELECT tenant FROM ${q(table)}`)).rows.map((r) => r.tenant));
      expect(seen.length, `${table} in A's session`).toBeGreaterThan(0);
      expect(seen.filter((t) => t !== "ta"), `${table}: rows of another tenant or the processor row in A's session`).toEqual([]);
      for (const tenant of ["", null]) {
        const none = await inSession(tenant, async (c) => (await c.query(`SELECT 1 FROM ${q(table)}`)).rowCount);
        expect(none, `${table} with app.current_tenant ${tenant === null ? "unset" : "''"}`).toBe(0);
      }
    }
    // writing B's rows from A's session: refused by the policy
    const refused = async (sql: string, params: unknown[]) =>
      inSession("ta", (c) => c.query(sql, params)).then(
        () => "written",
        (e: { code?: string }) => e.code,
      );
    expect(await refused(`INSERT INTO ${q(`${T}_read_model`)} (tenant, name, version) VALUES ('tb', 'x', 1)`, [])).toBe("42501");
    expect(await refused(`INSERT INTO ${q(`${T}_read_model_state`)} (tenant, name, version, key_hash, key, cursor_xid, cursor_seq, revision) VALUES ('tb', 'x', 1, '\\x00', 'k', '1', 1, 1)`, [])).toBe("42501");
    expect(await refused(`INSERT INTO ${q(`${T}_fold_state`)} (tenant, fold_name, fold_version, query_hash, query_key, cursor_xid, cursor_seq, state) VALUES ('tb', 'x', 1, '\\x00', 'k', '1', 1, '\\x00')`, [])).toBe("42501");
    const updated = await inSession("ta", async (c) => (await c.query(`UPDATE ${q(`${T}_read_model`)} SET revision = revision + 100 WHERE tenant = 'tb'`)).rowCount);
    expect(updated).toBe(0);
    await appPool.end();
    // A's handles see only A
    const a = app.forTenant("ta");
    expect((await a.persistedReadModel(cross).list()).rows.map((r) => [r.key, r.state.total])).toEqual([
      ["o1", 1],
      ["o2", 2],
    ]);
    expect((await a.persistedReadModel(offersModel).get("o1")).total).toBe(1);
    expect((await a.context({ query: keyQuery(source, "offerId", "o1"), fold: byOffer, initial: initialOffer })).state.total).toBe(1);
    expect((await a.persistedReadModel(cross).status()).revision).toBe(1);
    // as the processor role: every tenant's model rows and the processor row, no fold state, no append
    const procPool = new pg.Pool({ connectionString: roleUrl(PROC), max: 1 });
    const procSees = async (table: string) => (await procPool.query<{ tenant: string }>(`SELECT DISTINCT tenant FROM ${q(table)} ORDER BY 1`)).rows.map((r) => r.tenant);
    expect(await procSees(`${T}_read_model`)).toEqual(["", "ta", "tb"]);
    expect(await procSees(`${T}_read_model_state`)).toEqual(["ta", "tb"]);
    const foldStates = await procPool.query(`SELECT 1 FROM ${q(`${T}_fold_state`)}`).then(
      (r) => `rows: ${r.rowCount}`,
      (e: { code?: string }) => e.code,
    );
    expect(foldStates).toBe("42501"); // no grant, no policy: the processor role never touches persisted folds
    const appended = await procPool.query(`INSERT INTO ${q(T)} (event_type, payload) VALUES ('OfferMade', '{"tenantId":"ta"}')`).then(
      () => "appended",
      (e: { code?: string }) => e.code,
    );
    expect(appended).toBe("42501");
    await procPool.end();
    // the application's root: the cross-tenant processor fails naming processorRole; root reads stay refused
    const appRoot = app.persistedReadModel(crossModel("iso-root"));
    const failure = await appRoot.status().then(
      async (s) => (s.phase === "failed" ? s.error : (await wait(200), (await appRoot.status()).error)),
      (e: unknown) => e,
    );
    expect(failure).toBeInstanceOf(UsageError);
    expect((failure as Error).message).toMatch(/processor role "es_t6_proc" \(postgres\.processorRole\)/);
    await expect(app.query(source)).rejects.toThrow(/rls is enabled — call withTenant/);
    // the processor's store: its root reads every tenant; a per-tenant model and a persisted fold at its root are refused
    const all2 = await proc.query(source);
    expect(new Set(all2.events.map((e) => e.scopes.tenantId))).toEqual(new Set(["ta", "tb"]));
    const perTenantRoot = proc.persistedReadModel(offersModel);
    await expect(perTenantRoot.get("o1")).rejects.toThrow(/only in a tenant session/);
    const procStore = (await proc.store()) as unknown as { foldStates: { read: (k: unknown) => Promise<unknown> } };
    await expect(procStore.foldStates.read({ name: "by-offer", version: 1, key: "x" })).rejects.toThrow(UsageError);
    expect((await capOf(proc)).refusal).toBeInstanceOf(UsageError); // the root's per-tenant refusal stands on the processor's store
    await harness.closeAll();
  }, 30_000);

  /** A, B with every kind of kept state, a cross-tenant processor in its own api; the recipe, as printed, with the processor paused per `mode`. */
  async function erasure(mode: "before-page" | "in-page" | "free", options: { readonly withoutLock?: boolean } = {}) {
    await harness.reset();
    const app = open({ contextCache: { persist: { after: 1 } } });
    const proc = processor();
    const byOffer = defineFold<Offer>({ name: "by-offer", version: 1, fold: offerFold });
    const cross = crossModel("erase-x");
    const processorHandle = proc.persistedReadModel(cross);
    const ctxOf = (t: string, key = "o1") => app.forTenant(t).context({ query: keyQuery(source, "offerId", key), fold: byOffer, initial: initialOffer });
    const live = new Map<string, LiveReadModel<Offer>>();
    const per = new Map<string, PersistedReadModel<Offer>>();
    for (const [t, amount] of [["ta", 1], ["tb", 100]] as const) {
      const token = tokenOf(await app.forTenant(t).append([made("o1", amount), made("o2", amount + 1), noted("o1")]));
      await ctxOf(t);
      live.set(t, app.forTenant(t).readModel(offersModel));
      per.set(t, app.forTenant(t).persistedReadModel(offersModel) as PersistedReadModel<Offer>);
      await per.get(t)!.list({ atLeast: token });
      await app.forTenant(t).persistedReadModel(cross).list({ atLeast: token });
      expect(((await live.get(t)!.get("o1", { atLeast: token })) as Offer).total).toBe(amount);
      expect((await app.forTenant(t).persistedReadModel(cross).get("o1")).total).toBe(amount); // a warm copy in RAM
    }
    const bBefore = {
      rows: Number((await admin.query(`SELECT count(*) FROM ${q(`${T}_read_model_state`)} WHERE tenant = 'tb'`)).rows[0].count),
      cross: (await app.forTenant("tb").persistedReadModel(cross).list()).rows,
    };
    // the processor pauses per mode with tenant A's newest event in its page
    let release: (() => void) | undefined;
    let paused: Promise<void> | undefined;
    const pause = () => {
      let reached!: () => void;
      paused = new Promise<void>((resolve) => (reached = resolve));
      const gate = new Promise<void>((resolve) => (release = resolve));
      let once = true;
      return async () => {
        if (!once) return;
        once = false;
        reached();
        await gate;
      };
    };
    const knobs = (await capOf(proc)).knobs;
    if (mode === "before-page") knobs.beforePage = pause();
    if (mode === "in-page") knobs.inPage = pause();
    const lateA = await app.forTenant("ta").append([made("o3", 7)]);
    if (paused) await paused;
    // step 1: A's traffic stops, A's per-tenant handles stop, the count
    await per.get("ta")!.stop();
    const count = (await app.forTenant("ta").statistics()).reduce((n, s) => n + s.count, 0);
    expect(count).toBe(4);
    // step 2, as printed; in-page: it must wait for the page's shared lock
    const waited: boolean[] = [];
    const erased = eraseTenant("ta", count, {
      ...options,
      onWaiting:
        mode === "in-page"
          ? async (pid) => {
              if (options.withoutLock) return;
              await until(async () => {
                const r = await admin.query("SELECT 1 FROM pg_locks WHERE pid = $1 AND locktype = 'advisory' AND NOT granted", [pid]);
                return r.rowCount === 1;
              });
              waited.push(true);
              release!();
            }
          : undefined,
    });
    if (mode === "in-page" && options.withoutLock) {
      await erased;
      release!();
    }
    const deleted = await erased;
    if (mode === "before-page") release!();
    if (mode === "in-page" && !options.withoutLock) expect(waited).toEqual([true]);
    knobs.beforePage = undefined;
    knobs.inPage = undefined;
    // the processor goes on: a page after the erasure, for B
    const lateB = tokenOf(await app.forTenant("tb").append([made("o3", 9)]));
    await app.forTenant("tb").persistedReadModel(cross).list({ atLeast: lateB, timeoutMs: 15_000 });
    // before step 3, RAM still holds A's facts: the live model and the reader's copy
    expect(((await live.get("ta")!.get("o1")) as Offer).total).toBe(1);
    // step 3
    app.forTenant("ta").invalidate();
    proc.invalidate();
    const remains: Record<string, number> = {};
    remains[T] = Number((await admin.query(`SELECT count(*) FROM ${q(T)} WHERE payload->'scopes'->>'tenantId' = 'ta' OR payload->>'tenantId' = 'ta'`)).rows[0].count);
    for (const table of KEPT) remains[table] = Number((await admin.query(`SELECT count(*) FROM ${q(table)} WHERE tenant = 'ta'`)).rows[0].count);
    const a = {
      context: (await ctxOf("ta")).state,
      live: (await live.get("ta")!.get("o1")) as Offer,
      liveList: (await live.get("ta")!.list()).total,
      reader: await app.forTenant("ta").persistedReadModel(cross).get("o1"),
      readerList: (await app.forTenant("ta").persistedReadModel(cross).list()).total,
    };
    const b = {
      rows: Number((await admin.query(`SELECT count(*) FROM ${q(`${T}_read_model_state`)} WHERE tenant = 'tb'`)).rows[0].count),
      cross: (await app.forTenant("tb").persistedReadModel(cross).list()).rows,
      context: (await ctxOf("tb", "o3")).state.total,
    };
    const status = await processorHandle.status();
    await harness.closeAll();
    return { deleted, count, remains, a, b, bBefore, status, lateA };
  }

  it("retiring a cross-tenant version: the README recipe as printed, run as the processor role, retires v1 in every tenant; v1's processor fails at its next page, its readers at their next cold read, and no v1 row comes back", async () => {
    await harness.reset();
    const app = open();
    const proc = processor();
    const v1 = crossModel("retire-x");
    const p1 = proc.persistedReadModel(v1);
    for (const t of ["ta", "tb", "tc"]) await app.forTenant(t).persistedReadModel(v1).list({ atLeast: tokenOf(await app.forTenant(t).append([made("o1", 1)])) });
    const v2 = crossModel("retire-x", { version: 2 });
    const p2 = proc.persistedReadModel(v2);
    await until(async () => (await p2.status()).building === false && (await p2.status()).role === "writer", 10_000);
    for (const t of ["ta", "tb", "tc"]) expect((await app.forTenant(t).persistedReadModel(v2).list()).total).toBe(1);
    const procClient = new pg.Client({ connectionString: roleUrl(PROC), options: `-c search_path=${SCHEMA}` });
    await procClient.connect();
    await admin.query(`GRANT DELETE ON ${q(`${T}_read_model_state`)} TO ${PROC}`);
    try {
      const recipe = readmeSql("**Retiring a cross-tenant version**").replace(/\bevents_(read_model_state|read_model)\b/g, (_, rest: string) => `${T}_${rest}`);
      const statements = recipe
        .split(/;\s*\n/)
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      for (const statement of statements) {
        const used = Math.max(0, ...[...statement.matchAll(/\$(\d+)/g)].map((m) => Number(m[1])));
        await procClient.query(statement.replace(/;$/, ""), ["retire-x", 2].slice(0, used));
      }
    } finally {
      await admin.query(`REVOKE DELETE ON ${q(`${T}_read_model_state`)} FROM ${PROC}`);
      await procClient.end();
    }
    const statuses = await admin.query<{ tenant: string; version: number; status: string }>(`SELECT tenant, version, status FROM ${q(`${T}_read_model`)} WHERE name = 'retire-x' ORDER BY version, tenant`);
    expect(statuses.rows.filter((r) => r.version === 1).map((r) => [r.tenant, r.status])).toEqual([
      ["", "retired"],
      ["ta", "retired"],
      ["tb", "retired"],
      ["tc", "retired"],
    ]);
    expect(statuses.rows.filter((r) => r.version === 2).every((r) => r.status === "active")).toBe(true);
    for (let i = 0; i < 3; i++) await app.forTenant(["ta", "tb", "tc"][i]!).append([made("o2", i)]);
    // its next page — or a status() that reads the tombstone first — fails it
    await until(async () => (await p1.status().catch(() => ({ phase: "rejected" }))).phase === "failed", 10_000);
    expect(String((await p1.status()).error)).toMatch(/was retired/);
    const fresh = open();
    await expect(fresh.forTenant("ta").persistedReadModel(v1).list()).rejects.toThrow(/was retired/);
    await expect(fresh.forTenant("tb").persistedReadModel(v1).get("o1")).rejects.toThrow(/was retired/);
    await wait(300);
    const v1Rows = await admin.query(`SELECT count(*)::int AS n FROM ${q(`${T}_read_model_state`)} WHERE name = 'retire-x' AND version = 1`);
    expect(v1Rows.rows[0]).toEqual({ n: 0 });
    expect((await fresh.forTenant("tc").persistedReadModel(v2).list({ timeoutMs: 5_000 })).total).toBe(2);
    await harness.closeAll();
  }, 30_000);

  it("the installer: a processor policy whose role changed is recreated; a store without processorRole leaves them in place and names them in warnings(); the adopt check counts them as the store's own", async () => {
    const OTHER = "es_t6_proc2";
    await admin.query(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${OTHER}') THEN CREATE ROLE ${OTHER} NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$`);
    const rolesOf = async () =>
      (
        await admin.query<{ policyname: string; roles: string[] }>("SELECT policyname, roles::text[] AS roles FROM pg_policies WHERE schemaname = $1 AND policyname LIKE '%processor%' ORDER BY 1", [SCHEMA])
      ).rows.map((r) => `${r.policyname}:${r.roles.join(",")}`);
    expect(await rolesOf()).toEqual([`es_${T}_processor_read:${PROC}`, `es_${T}_rm_processor:${PROC}`, `es_${T}_rms_processor:${PROC}`]);
    // the policies move to OTHER; every later test needs them on PROC, so they move back even when an assertion fails
    try {
      const moved = new PostgresStore({ connection: roleUrl(OWNER), schema, ...postgresOptions("auto"), processorRole: OTHER });
      await moved.ensureInstalled();
      await moved.close();
      expect(await rolesOf()).toEqual([`es_${T}_processor_read:${OTHER}`, `es_${T}_rm_processor:${OTHER}`, `es_${T}_rms_processor:${OTHER}`]);
      // the events table's policy recreated by hand: the catalog now holds it after the other two
      await admin.query(`DROP POLICY es_${T}_processor_read ON ${q(T)}`);
      await admin.query(`CREATE POLICY es_${T}_processor_read ON ${q(T)} FOR SELECT TO ${OTHER} USING (true)`);
      const { processorRole: _omitted, ...withoutRole } = postgresOptions("auto");
      const plain = new PostgresStore({ connection: roleUrl(OWNER), schema, ...withoutRole });
      await plain.ensureInstalled();
      const warning = plain.warnings().find((w) => w.includes("processor policies")) ?? "";
      for (const name of ["processor_read", "rm_processor", "rms_processor"]) expect(warning).toContain(`es_${T}_${name} (to ${OTHER})`);
      expect(warning).toMatch(/left in place/);
      // named in policy-name order, whatever the catalog's order
      expect(warning).toContain(`es_${T}_processor_read (to ${OTHER}), es_${T}_rm_processor (to ${OTHER}), es_${T}_rms_processor (to ${OTHER})`);
      expect(plain.warnings().join("\n")).toMatch(new RegExp(`processor policies es_${T}_processor_read \\(to ${OTHER}\\).*left in place`));
      expect((await plain.adoptionPlan()).problems).toEqual([]);
      await plain.close();
      expect(await rolesOf()).toHaveLength(3);
    } finally {
      const back = new PostgresStore({ connection: roleUrl(OWNER), schema, ...postgresOptions("auto") });
      await back.ensureInstalled();
      expect(back.warnings().join("\n")).not.toMatch(/processor policies/);
      await back.close();
    }
    expect(await rolesOf()).toEqual([`es_${T}_processor_read:${PROC}`, `es_${T}_rm_processor:${PROC}`, `es_${T}_rms_processor:${PROC}`]);
  }, 30_000);

  for (const mode of ["before-page", "in-page", "free"] as const) {
    it(`erasure, the README recipe as printed, with the processor running (${mode === "before-page" ? "a page with A's event read before the erasure, written after" : mode === "in-page" ? "a page holding the shared lock while the erasure starts" : "no pause"}): nothing of A remains in any kept state or in RAM, B is untouched`, async () => {
      const r = await erasure(mode);
      expect(r.deleted).toMatchObject({ [T]: 4 });
      expect(r.remains).toEqual({ [T]: 0, [`${T}_fold_state`]: 0, [`${T}_read_model`]: 0, [`${T}_read_model_state`]: 0 });
      expect(r.a.context).toEqual(initialOffer);
      expect(r.a.live).toEqual(initialOffer);
      expect(r.a.liveList).toBe(0);
      expect(r.a.reader).toEqual(initialOffer);
      expect(r.a.readerList).toBe(0);
      expect(r.b.cross.filter((row) => row.key !== "o3")).toEqual(r.bBefore.cross);
      expect(r.b.rows).toBe(r.bBefore.rows + 2); // o3 in the per-tenant and in the cross-tenant model
      expect(r.b.context).toBe(9);
      expect(r.status.phase).toBe("live");
      expect(r.status.role).toBe("writer");
    }, 40_000);
  }

  it("erasure, the README recipe as printed, while a cross-tenant version switches (decision 50): the switch waits for the erasure lock and leaves no model row of A; B's row of the new version is active", async () => {
    await harness.reset();
    const app = open();
    const proc = processor();
    const v1 = crossModel("switch-x");
    const p1 = proc.persistedReadModel(v1);
    for (const [t, amount] of [["ta", 1], ["tb", 100]] as const) {
      await app.forTenant(t).persistedReadModel(v1).list({ atLeast: tokenOf(await app.forTenant(t).append([made("o1", amount)])), timeoutMs: 15_000 });
    }
    await p1.stop();
    const count = (await app.forTenant("ta").statistics()).reduce((n, s) => n + s.count, 0);
    // v2's processor backend, as its handle uses it: claimed (building beside the active v1), switched by hand while the erasure is open
    const cap = await capOf(proc);
    const across = await cap.crossTenant!();
    const ref = { name: "switch-x", version: 2 };
    const owner = {};
    const detach = cap.attach(owner);
    let waited = false;
    try {
      const claim = await across.lock(ref, owner, null);
      expect(claim?.status).toBe("building");
      let switched: Promise<void> | undefined;
      const deleted = await eraseTenant("ta", count, {
        beforeCommit: async () => {
          let settled = false;
          switched = across.activate(ref, claim!.epoch);
          switched.then(
            () => (settled = true),
            () => (settled = true),
          );
          // the switch either finishes inside the open erasure, or waits for the erasure lock as the processor role
          await until(async () => {
            if (settled) return true;
            const r = await admin.query(
              "SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid) WHERE l.locktype = 'advisory' AND NOT l.granted AND a.usename = $1",
              [PROC],
            );
            waited = r.rowCount === 1;
            return waited;
          });
        },
      });
      expect(deleted).toMatchObject({ [T]: count, [`${T}_read_model`]: 1 });
      await switched;
    } finally {
      await cap.unlock(ref, owner);
      detach();
    }
    const remains: Record<string, number> = {};
    for (const table of [T, ...KEPT]) {
      const where = table === T ? "payload->'scopes'->>'tenantId' = 'ta' OR payload->>'tenantId' = 'ta'" : "tenant = 'ta'";
      remains[table] = Number((await admin.query(`SELECT count(*) FROM ${q(table)} WHERE ${where}`)).rows[0].count);
    }
    expect(remains).toEqual({ [T]: 0, [`${T}_fold_state`]: 0, [`${T}_read_model`]: 0, [`${T}_read_model_state`]: 0 });
    expect(waited).toBe(true);
    const v2Rows = await admin.query<{ tenant: string; status: string }>(`SELECT tenant, status FROM ${q(`${T}_read_model`)} WHERE name = 'switch-x' AND version = 2 ORDER BY tenant`);
    expect(v2Rows.rows).toEqual([
      { tenant: "", status: "active" },
      { tenant: "tb", status: "active" },
    ]);
    await harness.closeAll();
  }, 30_000);
});
