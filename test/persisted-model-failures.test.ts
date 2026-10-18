/**
 * The persisted read model when something goes wrong (measurer, stage 4): bad input on every read and
 * watch, a version change (a new version rebuilds into its own rows beside the old one, which keeps
 * its rows and answers), and the cap of writer locks per store and process (decision 27) on Postgres.
 */
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildSchema, createEventStore, defineReadModel, MemoryStore, UsageError, type EventStoreApi, type Fold } from "../src/index.js";
import { MAX_WRITERS_PER_STORE } from "../src/modelstore.js";
import { PostgresStore } from "../src/postgres/index.js";
import { knobsOf, made, noted, offersModel, onRead, pmConfig, source, tokenOf, until, type Offer } from "./persisted-model.suite.js";

const url = process.env.ES_TEST_DATABASE_URL;
const schema = buildSchema(pmConfig.events, { strict: true, tenantScopeKey: pmConfig.tenant.scopeKey });
const TABLE = "pm_failures_events";
const tables = [TABLE, `${TABLE}_read_model`, `${TABLE}_read_model_state`];

type Open = () => EventStoreApi;
const memoryOpen = (): Open => {
  const store = new MemoryStore({ schema, persistedModels: true });
  return () => createEventStore({ ...pmConfig, store });
};
const postgresOpen = (): Open => () => createEventStore({ ...pmConfig, connection: url!, postgres: { table: TABLE, poolSize: 2, live: true, persistedModels: true } });

/** v2 of the offers model: another fold (it counts notes twice), the same name. */
const offerFoldV2: Fold<Offer> = (events, state) => {
  const s = offersModel.fold(events, state);
  return { ...s, notes: s.notes + events.filter((e) => e.type === "OfferNoted").length };
};
const offersV2 = defineReadModel({ ...offersModel, version: 2, fold: offerFoldV2 });

function failureCases(name: string, open: () => Open, reset: () => Promise<void>): void {
  describe(`persisted read model when something goes wrong (${name})`, () => {
    it("every malformed get, list, watch and watchList is a UsageError, and the model goes on unchanged", async () => {
      await reset();
      const api = open()();
      try {
        const t = await api.append([made("o1", 1), made("o2", 2, "won"), noted("o1")]);
        const model = api.persistedReadModel(offersModel);
        await model.list({ atLeast: tokenOf(t) });
        const before = await model.get("o1");
        const { revision, checkpoint, readFolded } = await model.status();
        /** The model called the way untyped JavaScript can call it. */
        const loose = model as unknown as Record<"get" | "watch" | "watchList", (...args: unknown[]) => Promise<unknown>>;
        const bad: [string, () => Promise<unknown>, RegExp][] = [
          ["get(42)", () => loose.get(42), /get\(\) needs a string key, got number/],
          ["get(undefined)", () => loose.get(undefined), /get\(\) needs a string key, got undefined/],
          ["list timeoutMs 0", () => model.list({ timeoutMs: 0 }), /timeoutMs must be a positive integer/],
          ["list timeoutMs 1.5", () => model.list({ timeoutMs: 1.5 }), /timeoutMs must be a positive integer/],
          ["list where null", () => model.list({ where: null as never }), /where must be an object/],
          ["list where array", () => model.list({ where: [] as never }), /where must be an object/],
          ["list where nested object", () => model.list({ where: { status: { $ne: "open" } } as never }), /where\.status must be a string, a finite number, a boolean or null/],
          ["list where NaN", () => model.list({ where: { total: Number.NaN } }), /where\.total must be/],
          ["list where Infinity", () => model.list({ where: { total: Number.POSITIVE_INFINITY } }), /where\.total must be/],
          ["list sort without by", () => model.list({ sort: {} as never }), /sort needs \{ by/],
          ["list sort dir", () => model.list({ sort: { by: "total", dir: "down" as never } }), /sort\.dir must be "asc" or "desc", got down/],
          ["list offset -1", () => model.list({ offset: -1 }), /offset must be an integer of at least 0/],
          ["list limit 0", () => model.list({ limit: 0 }), /limit must be an integer of at least 1/],
          ["list limit Infinity", () => model.list({ limit: Number.POSITIVE_INFINITY }), /limit must be an integer of at least 1/],
          ["list atLeast malformed", () => model.list({ atLeast: { transactionId: "x", sequence: 1 } }), /atLeast needs a Cursor/],
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
        expect(await model.status()).toMatchObject({ phase: "live", revision, checkpoint, readFolded, failed: 0 });
        expect(await model.get("o1")).toEqual(before);
        const next = await api.append([made("o1", 10)]);
        await model.list({ atLeast: tokenOf(next) });
        expect((await model.get("o1")).total).toBe(11);
        expect(await model.get("o1")).toEqual(((await onRead(api, "o1")) as { state: Offer }).state);
      } finally {
        await api.close();
      }
    });

    it("a version change: v2 rebuilds from zero into its own rows beside v1, which keeps its rows and answers with its own fold; a fresh runtime holds both", async () => {
      await reset();
      const opener = open();
      const a = opener();
      let b: EventStoreApi | undefined;
      try {
        const t1 = await a.append([made("o1", 1), noted("o1"), made("o2", 2), noted("o2"), noted("o2")]);
        const v1 = a.persistedReadModel(offersModel);
        await v1.list({ atLeast: tokenOf(t1) });
        const v1Before = await v1.list({ sort: { by: "total" } });
        const v2 = a.persistedReadModel(offersV2);
        await v2.list({ atLeast: tokenOf(t1) });
        // v2 folded the whole source itself, with its own fold; v1 folded nothing more
        expect((await v2.status()).folded).toBe(5);
        expect((await v1.status()).folded).toBe(5);
        expect(await v1.list({ sort: { by: "total" } })).toEqual(v1Before);
        expect((await v2.get("o2")).notes).toBe(4);
        expect((await v1.get("o2")).notes).toBe(2);
        // both follow new events, each equal to fold on read with its own fold
        const t2 = await a.append([noted("o1"), made("o3", 3)]);
        await v1.list({ atLeast: tokenOf(t2) });
        await v2.list({ atLeast: tokenOf(t2) });
        for (const k of ["o1", "o2", "o3"]) {
          expect(await v1.get(k)).toEqual(((await onRead(a, k, offersModel)) as { state: Offer }).state);
          expect(await v2.get(k)).toEqual(((await onRead(a, k, offersV2)) as { state: Offer }).state);
        }
        // a fresh runtime over the same store resumes both, rebuilding neither
        await a.close();
        b = opener();
        const v1b = b.persistedReadModel(offersModel);
        const v2b = b.persistedReadModel(offersV2);
        await v1b.list({ atLeast: tokenOf(t2) });
        await v2b.list({ atLeast: tokenOf(t2) });
        expect((await v1b.list({ sort: { by: "total" } })).rows.map((r) => [r.key, r.state.notes])).toEqual([
          ["o1", 2],
          ["o2", 2],
          ["o3", 0],
        ]);
        expect((await v2b.list({ sort: { by: "total" } })).rows.map((r) => [r.key, r.state.notes])).toEqual([
          ["o1", 4],
          ["o2", 4],
          ["o3", 0],
        ]);
        expect((await v1b.status()).folded).toBe(0);
        expect((await v2b.status()).folded).toBe(0);
      } finally {
        await a.close().catch(() => undefined);
        await b?.close();
      }
    });
  });
}

failureCases("memory store", memoryOpen, async () => undefined);

describe.skipIf(!url)("persisted read model when something goes wrong (Postgres)", () => {
  let admin: pg.Pool;
  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await admin.query(`DROP TABLE IF EXISTS ${tables.map((t) => `"${t}"`).join(", ")} CASCADE`);
    const store = new PostgresStore({ connection: url!, schema, table: TABLE, persistedModels: true, live: true });
    await store.ensureInstalled();
    await store.close();
  });
  afterAll(async () => {
    await admin.query(`DROP TABLE IF EXISTS ${tables.map((t) => `"${t}"`).join(", ")} CASCADE`);
    await admin.end();
  });
  const reset = async () => {
    await admin.query(`TRUNCATE ${tables.map((t) => `"${t}"`).join(", ")} RESTART IDENTITY`);
  };
  failureCases("Postgres", postgresOpen, reset);

  it(`the writer cap: of ${MAX_WRITERS_PER_STORE + 1} tenant handles in one process exactly ${MAX_WRITERS_PER_STORE} write, on one session; one stops, the last takes its slot`, async () => {
    await reset();
    const api = postgresOpen()();
    try {
      (await knobsOf(api)).tickMs = 100;
      const t = await api.forTenant("t0").append([made("o1", 1)]);
      const models = Array.from({ length: MAX_WRITERS_PER_STORE + 1 }, (_, i) => api.forTenant(`t${i}`).persistedReadModel(offersModel));
      const roles = async () => {
        const all = await Promise.all(models.map(async (m) => (await m.status()).role));
        return { writers: all.filter((r) => r === "writer").length, standby: all.indexOf("standby") };
      };
      await until(async () => (await roles()).writers === MAX_WRITERS_PER_STORE, 60_000);
      // a few ticks more: the one beyond the cap stays a standby
      await new Promise((r) => setTimeout(r, 500));
      const { writers, standby } = await roles();
      expect(writers).toBe(MAX_WRITERS_PER_STORE);
      expect(standby).toBeGreaterThanOrEqual(0);
      const held = await admin.query<{ n: number; pids: number }>("SELECT count(*)::int AS n, count(DISTINCT pid)::int AS pids FROM pg_locks WHERE locktype = 'advisory' AND granted");
      expect(held.rows[0]).toEqual({ n: MAX_WRITERS_PER_STORE, pids: 1 });
      // the capped handle still reads: get is current, from the store
      expect((await models[0]!.get("o1")).total).toBe(1);
      await models[0]!.list({ atLeast: tokenOf(t) });
      // one writer stops: the standby takes its slot within ticks
      const writerIndex = standby === 0 ? 1 : 0;
      await models[writerIndex]!.stop();
      await until(async () => (await models[standby]!.status()).role === "writer", 10_000);
      const after = await admin.query<{ n: number }>("SELECT count(*)::int AS n FROM pg_locks WHERE locktype = 'advisory' AND granted");
      expect(after.rows[0]!.n).toBe(MAX_WRITERS_PER_STORE);
    } finally {
      await api.close();
    }
  }, 120_000);
});
