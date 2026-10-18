/**
 * `invalidate()` on a view (decision 49): it rebuilds the view's live read models in place and drops its
 * persisted handles' `get` copies; `invalidate(query)` and the tenant-view LRU touch no model. On the memory
 * store an erasure is simulated by a store wrapper that hides one tenant's events from a switch on; on Postgres
 * the events and rows are really deleted.
 */
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildSchema, createEventStore, MemoryStore, TransientError, type EventStore, type EventStoreApi, type LiveReadModel, type QueryResult } from "../src/index.js";
import { PostgresStore } from "../src/postgres/index.js";
import { block, initialOffer, made, noted, offersModel, onRead, pmConfig, tokenOf, until, wait, type Offer } from "./persisted-model.suite.js";

const url = process.env.ES_TEST_DATABASE_URL;
const schema = buildSchema(pmConfig.events, { strict: true, tenantScopeKey: pmConfig.tenant.scopeKey });

/** A memory store whose reads leave tenant `hidden.tenant` out once `hidden.on` is set: an erasure, as far as reads can tell. */
function hiding(inner: MemoryStore, hidden: { on: boolean; tenant: string }): EventStore {
  const visible = (e: { scopes: Record<string, string> }) => !(hidden.on && e.scopes.tenantId === hidden.tenant);
  return {
    schema,
    query: async (q, o) => {
      const r = await inner.query(q, o);
      if (!hidden.on) return r;
      return { ...r, events: r.events.filter(visible), byFilter: r.byFilter.map((list) => list.filter(visible)) } satisfies QueryResult;
    },
    append: (e) => inner.append(e),
    appendIf: (e, c) => inner.appendIf(e, c),
    onCommitted: (l: (hint: number | null) => void) => inner.onCommitted(l),
    close: async () => undefined,
    readModelStore: inner.readModelStore,
  } as EventStore;
}

/**
 * A memory store whose reads are taken when they are issued and handed out only when the test releases them
 * (while `gate.held` is set): two rebuilds' reads can answer in any order.
 */
function gated(inner: MemoryStore, gate: { held?: Array<() => void> }): EventStore {
  return {
    ...hiding(inner, { on: false, tenant: "" }),
    query: async (q, o) => {
      const r = await inner.query(q, o);
      const held = gate.held;
      if (held) await new Promise<void>((resolve) => held.push(resolve));
      return r;
    },
  } as EventStore;
}

describe("invalidate() on the memory store", () => {
  it("rebuilds the view's live model in place: the same object, reads issued at once wait and answer from the rebuilt state, watchers end with read_model_rebuilt, the revision only grows", async () => {
    const hidden = { on: false, tenant: "ta" };
    const store = new MemoryStore({ schema, persistedModels: true });
    const api = createEventStore({ ...pmConfig, store: hiding(store, hidden) });
    const a = api.forTenant("ta");
    const b = api.forTenant("tb");
    await a.append([made("o1", 5), made("o2", 6), noted("o1")]);
    await b.append([made("o1", 50)]);
    const live = a.readModel(offersModel);
    const other = b.readModel(offersModel);
    expect((await live.get("o1")).total).toBe(5);
    expect((await other.get("o1")).total).toBe(50);
    const before = live.status;
    const ended: unknown[] = [];
    const seen: number[] = [];
    await live.watch("o1", (s) => void seen.push(s.total), { onError: (e) => void ended.push(e) });
    await live.watchList({}, () => undefined, { onError: (e) => void ended.push(e) });
    hidden.on = true;
    a.invalidate();
    expect(live.status.phase).toBe("starting");
    const right = live.get("o1"); // issued at once: waits for the rebuild
    expect(await right).toEqual(initialOffer);
    expect((await live.list()).total).toBe(0);
    expect(a.readModel(offersModel)).toBe(live);
    expect(live.status.phase).toBe("live");
    expect(live.status.revision).toBeGreaterThan(before.revision);
    expect(live.status.folded).toBe(before.folded); // the rebuild folded nothing: the tenant's events are gone
    expect(ended).toHaveLength(2);
    for (const e of ended) {
      expect(e).toBeInstanceOf(TransientError);
      expect((e as TransientError).code).toBe("read_model_rebuilt");
    }
    expect(seen).toEqual([5]);
    // another tenant's model is untouched
    expect(other.status.revision).toBe(1);
    expect((await other.get("o1")).total).toBe(50);
    // a watcher after the rebuild gets the rebuilt state, then new ones
    const after: number[] = [];
    await live.watch("o1", (s) => void after.push(s.total));
    hidden.on = false; // new traffic of the tenant is visible again
    hidden.tenant = "none";
    await a.append([made("o1", 1)]);
    await until(() => after.length === 2);
    expect(after[0]).toBe(0);
    await api.close();
  });

  it("invalidate(query) touches no model; the tenant-view LRU releases only the cache; a stopped or failed model stays so", async () => {
    const store = new MemoryStore({ schema, persistedModels: true });
    const api = createEventStore({ ...pmConfig, store, maxTenantViews: 1 });
    await api.forTenant("ta").append([made("o1", 1)]);
    const live = api.forTenant("ta").readModel(offersModel) as LiveReadModel<Offer>;
    expect((await live.get("o1")).total).toBe(1);
    const status = live.status;
    api.forTenant("ta").invalidate(offersModel.source);
    expect(live.status).toEqual(status);
    api.forTenant("tb"); // evicts ta's view from the memo (maxTenantViews: 1)
    await wait(20);
    expect(live.status).toEqual(status);
    expect(api.forTenant("ta").readModel(offersModel)).toBe(live);
    const stopped = api.forTenant("tc").readModel(offersModel);
    await stopped.stop();
    api.forTenant("tc").invalidate();
    expect(stopped.status.phase).toBe("stopped");
    const failing = createEventStore({ ...pmConfig, store: new MemoryStore({ schema }) });
    const failed = failing.forTenant("ta").readModel(offersModel, { start: "persisted" }); // no persistedModels: fails
    await expect(failed.get("o1")).rejects.toThrow(/persistedModels: true/);
    failing.forTenant("ta").invalidate();
    expect(failed.status.phase).toBe("failed");
    await failing.close();
    await api.close();
  });

  it("drops the persisted handles' get copies: the next get reads the row and the key's delta again; the handle keeps running", async () => {
    const store = new MemoryStore({ schema, persistedModels: true });
    const api = createEventStore({ ...pmConfig, store });
    const blocker = createEventStore({ ...pmConfig, store });
    const unblock = await block(blocker.forTenant("ta"), offersModel); // nobody writes rows: get folds the whole delta
    const model = api.forTenant("ta").persistedReadModel(offersModel);
    await api.forTenant("ta").append([made("o1", 1), made("o1", 2), noted("o1")]);
    expect((await model.get("o1")).total).toBe(3);
    const cold = (await model.status()).readFolded;
    expect(cold).toBe(3);
    await model.get("o1");
    expect((await model.status()).readFolded).toBe(3); // warm: the copy
    api.forTenant("ta").invalidate();
    expect((await model.get("o1")).total).toBe(3);
    expect((await model.status()).readFolded).toBe(6); // the copy was dropped: the key's delta again
    expect((await model.status()).phase).toBe("live");
    await unblock();
    await blocker.close();
    await api.close();
  });

  for (const order of ["the older read answers last", "the older read answers first"] as const) {
    it(`an invalidate() during a rebuild starts it again: reads wait for the newer rebuild, and the older one's read never reaches the states (${order})`, async () => {
      const gate: { held?: Array<() => void> } = {};
      const store = new MemoryStore({ schema, persistedModels: true });
      const api = createEventStore({ ...pmConfig, store: gated(store, gate) });
      const a = api.forTenant("ta");
      await a.append([made("o1", 5), made("o2", 6), noted("o1")]);
      const live = a.readModel(offersModel);
      expect((await live.get("o1")).total).toBe(5);
      const held: Array<() => void> = [];
      gate.held = held;
      a.invalidate(); // rebuild 1: its read is taken now, before the next append
      await until(() => held.length === 1);
      await a.append([made("o1", 7), made("o3", 1)]);
      a.invalidate(); // rebuild 2, during rebuild 1: its read holds the new events too
      await until(() => held.length === 2);
      expect(live.status.phase).toBe("starting");
      const during = live.get("o1"); // issued while both rebuilds wait
      const listed = live.list();
      let answered = false;
      void during.then(() => (answered = true));
      gate.held = undefined;
      const expected = await onRead(a, "o1");
      expect(expected.ok).toBe(true);
      if (order === "the older read answers first") {
        held[0]!(); // the older rebuild's read ends its (stopped) feed: the model is not live yet
        await wait(50);
        expect(answered).toBe(false);
        expect(live.status.phase).toBe("starting");
        held[1]!();
        expect(await during).toEqual(expected.ok && expected.state);
        expect((await listed).total).toBe(3);
      } else {
        held[1]!(); // rebuild 2's read answers first …
        expect(await during).toEqual(expected.ok && expected.state);
        expect((await listed).total).toBe(3);
        held[0]!(); // … rebuild 1's older read last
      }
      await wait(50);
      expect(await live.get("o1")).toEqual(expected.ok && expected.state);
      for (const key of ["o2", "o3", "never"]) {
        const want = await onRead(a, key);
        expect(await live.get(key)).toEqual(want.ok && want.state);
      }
      expect((await live.list()).total).toBe(3);
      expect(live.status.phase).toBe("live");
      // and it keeps following the store
      await a.append([made("o2", 1)]);
      const o2 = await onRead(a, "o2");
      await until(async () => JSON.stringify(await live.get("o2")) === JSON.stringify(o2.ok && o2.state));
      await api.close();
    });
  }
});

describe.skipIf(!url)("invalidate() on Postgres after a real DELETE", () => {
  const T = "inv_events";
  let admin: pg.Pool;
  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    for (const t of [`${T}_read_model_state`, `${T}_read_model`, T]) await admin.query(`DROP TABLE IF EXISTS "${t}" CASCADE`);
    const store = new PostgresStore({ connection: url!, schema, table: T, persistedModels: true, live: true });
    await store.ensureInstalled();
    await store.close();
  });
  afterAll(async () => {
    for (const t of [`${T}_read_model_state`, `${T}_read_model`, T]) await admin.query(`DROP TABLE IF EXISTS "${t}" CASCADE`);
    await admin.end();
  });

  it("after the tenant's events and rows are deleted: the live model (also one started from the persisted rows) and the persisted handle answer initial on the same handles", async () => {
    const api: EventStoreApi = createEventStore({ ...pmConfig, connection: url!, postgres: { table: T, poolSize: 3, live: true, persistedModels: true } });
    const a = api.forTenant("ta");
    const token = tokenOf(await a.append([made("o1", 5), noted("o1")]));
    await api.forTenant("tb").append([made("o1", 50)]);
    const persisted = a.persistedReadModel(offersModel);
    await persisted.list({ atLeast: token });
    const live = a.readModel(offersModel);
    const fromPersisted = createEventStore({ ...pmConfig, connection: url!, postgres: { table: T, poolSize: 2, live: true, persistedModels: true } });
    const rowsModel = fromPersisted.forTenant("ta").readModel(offersModel, { start: "persisted" });
    expect((await rowsModel.get("o1")).total).toBe(5);
    expect((await live.get("o1")).total).toBe(5);
    expect((await persisted.get("o1")).total).toBe(5);
    await persisted.stop(); // recipe step 1: the per-tenant handle stops
    const kept = fromPersisted.forTenant("ta").persistedReadModel(offersModel); // a running handle with a warm copy (here: it took over as writer)
    expect((await kept.get("o1")).total).toBe(5);
    await admin.query(`DELETE FROM "${T}" WHERE es_scope(payload, 'tenantId') = 'ta'`);
    await admin.query(`DELETE FROM "${T}_read_model_state" WHERE tenant = 'ta'`);
    await admin.query(`DELETE FROM "${T}_read_model" WHERE tenant = 'ta'`);
    expect((await live.get("o1")).total).toBe(5); // RAM still holds it
    a.invalidate();
    fromPersisted.forTenant("ta").invalidate();
    expect(await live.get("o1")).toEqual(initialOffer);
    expect(await rowsModel.get("o1")).toEqual(initialOffer); // re-snapshot found no rows: rebuilt
    expect((await rowsModel.list()).total).toBe(0);
    expect(await kept.get("o1")).toEqual(initialOffer); // the copy was dropped
    expect((await api.forTenant("tb").readModel(offersModel).get("o1")).total).toBe(50);
    await fromPersisted.close();
    await api.close();
  }, 30_000);
});
