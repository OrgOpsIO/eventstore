/**
 * The live read model when something goes wrong (measurer, stage 3): bad input on every read and
 * watch, a version change (a new version beside the old one, and alone in a fresh process), and
 * erasing a tenant on Postgres (a restart, or a stopped and re-created model, holds nothing of it).
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { buildSchema, createEventStore, defineReadModel, MemoryStore, UsageError, type Fold } from "../src/index.js";
import { PostgresStore } from "../src/postgres/index.js";
import { config, initialOffer, made, noted, offerFold, offersModel, onRead, source, tokenOf, type Offer } from "./readmodel.suite.js";
import { keyQuery } from "../src/readmodel.js";

const url = process.env.ES_TEST_DATABASE_URL;
const schema = buildSchema(config.events, { strict: true, tenantScopeKey: config.tenant.scopeKey });

describe("live read model: bad input", () => {
  it("every malformed get, list, watch and watchList is a UsageError, and the model goes on unchanged", async () => {
    const api = createEventStore({ ...config, store: new MemoryStore({ schema }) });
    await api.append([made("o1", 1), made("o2", 2, "won"), noted("o1")]);
    const model = api.readModel(offersModel);
    const before = await model.get("o1");
    const { folded, revision } = model.status;
    /** The model called the way untyped JavaScript can call it. */
    const loose = model as unknown as Record<"get" | "watch" | "watchList", (...args: unknown[]) => Promise<unknown>>;
    const bad: [string, () => Promise<unknown>, RegExp][] = [
      ["get(42)", () => loose.get(42), /get\(\) needs a string key, got number/],
      ["get(undefined)", () => loose.get(undefined), /get\(\) needs a string key, got undefined/],
      ["get timeoutMs 0", () => model.get("o1", { timeoutMs: 0 }), /timeoutMs must be a positive integer/],
      ["get timeoutMs 1.5", () => model.get("o1", { timeoutMs: 1.5 }), /timeoutMs must be a positive integer/],
      ["list where null", () => model.list({ where: null as never }), /where must be an object/],
      ["list where array", () => model.list({ where: [] as never }), /where must be an object/],
      ["list where nested object", () => model.list({ where: { status: { $ne: "open" } } as never }), /where\.status must be a string, a finite number, a boolean or null/],
      ["list where NaN", () => model.list({ where: { total: Number.NaN } }), /where\.total must be/],
      ["list where Infinity", () => model.list({ where: { total: Number.POSITIVE_INFINITY } }), /where\.total must be/],
      ["list sort without by", () => model.list({ sort: {} as never }), /sort needs \{ by/],
      ["list sort by ''", () => model.list({ sort: { by: "" as never } }), /sort needs \{ by/],
      ["list sort dir", () => model.list({ sort: { by: "total", dir: "down" as never } }), /sort\.dir must be "asc" or "desc", got down/],
      ["list offset -1", () => model.list({ offset: -1 }), /offset must be an integer of at least 0/],
      ["list offset 0.5", () => model.list({ offset: 0.5 }), /offset must be an integer of at least 0/],
      ["list limit 0", () => model.list({ limit: 0 }), /limit must be an integer of at least 1/],
      ["list limit Infinity", () => model.list({ limit: Number.POSITIVE_INFINITY }), /limit must be an integer of at least 1/],
      ["watch(42, cb)", () => loose.watch(42, () => {}), /watch\(\) needs a string key, got number/],
      ["watch(k) without a callback", () => loose.watch("o1"), /watch\(\) needs a callback/],
      ["watchList({}) without a callback", () => loose.watchList({}), /watchList\(\) needs a callback/],
      ["watchList where nested", () => model.watchList({ where: { status: ["open"] } as never }, () => {}), /where\.status must be/],
    ];
    for (const [what, call, message] of bad) {
      const error = await Promise.resolve()
        .then(call)
        .then(
          () => new Error(`${what} did not throw`),
          (e: unknown) => e,
        );
      expect(error, what).toBeInstanceOf(UsageError);
      expect((error as Error).message, what).toMatch(message);
    }
    expect(model.status).toMatchObject({ phase: "live", folded, revision, failed: 0 });
    expect(await model.get("o1")).toEqual(before);
    await model.get("o1", { atLeast: tokenOf(await api.append([made("o1", 10)])) });
    expect((await model.get("o1")).total).toBe(11);
    await api.close();
  });
});

describe("live read model: a version change", () => {
  /** Version 2 of the same name: another fold (amounts doubled, notes ignored). */
  const foldV2: Fold<Offer> = (events, state) => {
    let s = state;
    for (const e of events) {
      if (e.type !== "OfferMade") continue;
      const d = e.data as { amount: number; status: string };
      s = { ...s, n: s.n + 1, total: s.total + 2 * d.amount, status: d.status, last: e.recordedAt };
    }
    return s;
  };
  const v2 = defineReadModel({ name: "offers", version: 2, source, keyBy: "offerId", fold: foldV2, initial: initialOffer });

  it("v2 started beside v1 rebuilds from the store with its own fold; v1 keeps answering with its own; a fresh process with v2 alone agrees", async () => {
    const store = new MemoryStore({ schema });
    const api = createEventStore({ ...config, store });
    await api.append([made("o1", 1), made("o2", 2), noted("o1"), made("o1", 3, "won")]);
    const m1 = api.readModel(offersModel);
    expect((await m1.get("o1")).total).toBe(4);
    const folded1 = m1.status.folded;
    const m2 = api.readModel(v2);
    expect(m2).not.toBe(m1);
    expect(await m2.get("o1")).toMatchObject({ n: 2, total: 8, notes: 0, status: "won" });
    // the rebuild of v2 folded every routed event from the store, and nothing of v1 was folded again
    expect(m2.status.folded).toBe(4);
    expect(m1.status.folded).toBe(folded1);
    const fold2 = async (k: string) => (await api.context({ query: keyQuery(source, "offerId", k), fold: foldV2, initial: initialOffer })).state;
    const token = tokenOf(await api.append([made("o2", 5), noted("o2"), made("o3", 7)]));
    for (const k of ["o1", "o2", "o3", "o4"]) {
      expect(await m1.get(k, { atLeast: token }), `v1 ${k}`).toEqual((await onRead(api, k) as { state: Offer }).state);
      expect(await m2.get(k, { atLeast: token }), `v2 ${k}`).toEqual(await fold2(k));
    }
    expect((await m1.list()).rows.map((r) => [r.key, r.state.total])).toEqual([["o1", 4], ["o2", 7], ["o3", 7]]);
    expect((await m2.list()).rows.map((r) => [r.key, r.state.total])).toEqual([["o1", 8], ["o2", 14], ["o3", 14]]);
    // a deploy with only v2: a fresh process rebuilds v2 from the store and holds the same states
    const fresh = createEventStore({ ...config, store });
    const m2b = fresh.readModel(v2);
    expect((await m2b.list()).rows).toEqual((await m2.list()).rows);
    await fresh.close();
    await api.close();
  });
});

describe.skipIf(!url)("live read model: erasing a tenant (Postgres)", () => {
  const TABLE = "rmx_events";
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

  it("after the README's delete, a restarted process and a stopped and re-created model hold nothing of the erased tenant; the other tenant is untouched", async () => {
    const open = () => createEventStore({ ...config, connection: url!, postgres: { table: TABLE, poolSize: 2 } });
    const p = open();
    await p.forTenant("A").append([made("o1", 1), made("o2", 2), noted("o1")]);
    await p.forTenant("B").append([made("o1", 100)]);
    const before = p.forTenant("A").readModel(offersModel);
    expect((await before.list()).total).toBe(2);
    // the root view without rls folds every tenant into one model: o1 holds A's and B's events
    const rootBefore = p.readModel(offersModel);
    expect((await rootBefore.get("o1")).total).toBe(101);
    const rootWatch = await rootBefore.watchList({}, () => undefined);
    const c = await admin.connect();
    try {
      await c.query("BEGIN");
      await c.query(`DELETE FROM "${TABLE}" WHERE es_scope(payload, 'tenantId') = $1`, ["A"]);
      await c.query("COMMIT");
    } finally {
      c.release();
    }
    // the same process: stop the tenant's model and declare it again — a fresh rebuild from the store
    before.stop();
    const again = p.forTenant("A").readModel(offersModel);
    expect(again).not.toBe(before);
    expect(await again.list()).toMatchObject({ total: 0, rows: [], failed: 0 });
    expect(await again.get("o1")).toEqual(initialOffer);
    expect(again.status).toMatchObject({ phase: "live", keys: 0, folded: 0 });
    // README step 3 for the root view's model too: stopped, its watchers end, declared again it holds B alone
    await rootBefore.stop();
    expect(rootWatch.stopped).toBe(true);
    await expect(rootBefore.get("o1")).rejects.toThrow(/stopped/);
    const rootAgain = p.readModel(offersModel);
    expect(await rootAgain.list()).toMatchObject({ total: 1, failed: 0 });
    expect((await rootAgain.get("o1")).total).toBe(100);
    expect(await rootAgain.get("o2")).toEqual(initialOffer);
    // a restarted process
    const fresh = open();
    const a = fresh.forTenant("A").readModel(offersModel);
    const b = fresh.forTenant("B").readModel(offersModel);
    expect(await a.list()).toMatchObject({ total: 0, rows: [] });
    for (const k of ["o1", "o2"]) {
      expect(await a.get(k)).toEqual(initialOffer);
      expect(await a.get(k)).toEqual((await onRead(fresh.forTenant("A"), k) as { state: Offer }).state);
    }
    expect((await b.get("o1")).total).toBe(100);
    expect((await b.list()).rows.map((r) => r.key)).toEqual(["o1"]);
    // the erased tenant's keys are free again: a new event folds from the initial state, not from the erased facts
    await a.get("o1", { atLeast: tokenOf(await fresh.forTenant("A").append([made("o1", 5)])) });
    expect(await a.get("o1")).toMatchObject({ n: 1, total: 5, notes: 0 });
    await fresh.close();
    await p.close();
  });
});
