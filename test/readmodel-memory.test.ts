import { describe, expect, it } from "vitest";
import { buildSchema, createEventStore, defineFold, defineReadModel, FailedKeyError, MemoryStore, UsageError, type EventStore, type EventStoreApi, type Query } from "../src/index.js";
import { keyQuery } from "../src/readmodel.js";
import { audited, config, initialOffer, made, noted, offerFold, offers, offersModel, onRead, readModelSuite, source, tokenOf, type ModelHarness, type Offer } from "./readmodel.suite.js";

const schema = buildSchema(config.events, { strict: true, tenantScopeKey: config.tenant.scopeKey });

function memoryHarness(): ModelHarness {
  let store = new MemoryStore({ schema });
  const opened: EventStoreApi[] = [];
  return {
    writers: 1,
    async reset() {
      for (const api of opened.splice(0)) await api.close();
      store = new MemoryStore({ schema });
    },
    open(options = {}) {
      const api = createEventStore({ ...config, store, ...options });
      opened.push(api);
      return api;
    },
    async tables() {
      return [];
    },
    async closeAll() {
      for (const api of opened.splice(0)) await api.close();
    },
  };
}

describe("live read model on the memory store", () => {
  readModelSuite(memoryHarness(), { numRuns: 60 });
});

const newApi = (store?: EventStore) => createEventStore({ ...config, ...(store ? { store } : {}) });

describe("live read model: definition and placement", () => {
  const fold = offerFold;
  const base = { name: "m", version: 1, source, keyBy: "offerId" as const, fold, initial: initialOffer };

  it("defineReadModel validates every field, naming the model; the result is frozen", () => {
    const def = defineReadModel(base);
    expect(Object.isFrozen(def)).toBe(true);
    expect(() => defineReadModel({ ...base, name: "" })).toThrow(/defineReadModel needs a name of 1 to 200/);
    expect(() => defineReadModel({ ...base, name: "a\u0000b" })).toThrow(/without NUL/);
    for (const version of [0, 1.5, 2 ** 31]) expect(() => defineReadModel({ ...base, version })).toThrow(/defineReadModel\("m"\) needs a version/);
    expect(() => defineReadModel({ ...base, keyBy: "offer-id" as never })).toThrow(/defineReadModel\("m"\) needs keyBy/);
    expect(() => defineReadModel({ ...base, keyBy: undefined as never })).toThrow(UsageError);
    expect(() => defineReadModel({ ...base, source: [] as Query })).toThrow(/defineReadModel\("m"\) has an invalid source/);
    expect(() => defineReadModel({ ...base, fold: undefined as never })).toThrow(/needs a fold function/);
    for (const batchSize of [0, -1, 1.5]) expect(() => defineReadModel({ ...base, batchSize })).toThrow(/batchSize/);
    expect(() => defineReadModel({ ...base, onFailedKey: 1 as never })).toThrow(/onFailedKey/);
    // defineFold's messages are unchanged by the shared check
    expect(() => defineFold({ name: "", version: 1, fold })).toThrow(/^eventstore: defineFold needs a name of 1 to 200 characters without NUL/);
    expect(() => defineFold({ name: "f", version: 0, fold })).toThrow('eventstore: defineFold("f") needs a version that is an integer from 1 to 2147483647, got 0');
    // a DefinedFold is accepted; its identity is not the model's
    expect(defineReadModel({ ...base, fold: defineFold({ name: "other", version: 9, fold }) }).name).toBe("m");
  });

  it("a source that pins keyBy is refused; a `not` naming keyBy is accepted and agrees with fold on read", async () => {
    expect(() => defineReadModel({ ...base, source: offers.$filter({ scopes: { offerId: "o1" } }) })).toThrow(/must not pin "offerId"; keyBy does the keying/);
    expect(() => defineReadModel({ ...base, source: [source, { types: ["OfferMade"], scopes: { offerId: ["a", "b"] } }] })).toThrow(/must not pin/);
    const def = defineReadModel({ ...base, name: "not-x", source: { ...source, not: { scopes: { offerId: "x" } } } });
    const api = newApi();
    await api.append([made("x", 1), made("y", 2)]);
    const model = api.readModel(def);
    expect(await model.get("x")).toEqual(initialOffer);
    expect((await api.context({ query: keyQuery(def.source, "offerId", "x"), fold, initial: initialOffer })).state).toEqual(initialOffer);
    expect((await model.get("y")).total).toBe(2);
    await api.close();
  });

  it("one model per (view tenant, name, version): the same definition gives the same model; another object with the same identity is refused", async () => {
    const api = newApi();
    const def = defineReadModel(base);
    const model = api.readModel(def);
    expect(api.readModel(def)).toBe(model);
    expect(() => api.readModel(defineReadModel(base))).toThrow(/two definitions of read model "m" v1; bump the version or reuse the definition/);
    const v2 = api.readModel(defineReadModel({ ...base, version: 2 }));
    expect(v2).not.toBe(model);
    // a plain object is checked once and keeps its identity
    const plain = { ...base, name: "plain" };
    expect(api.readModel(plain)).toBe(api.readModel(plain));
    expect(() => api.readModel({ ...base, name: "bad", keyBy: "1x" })).toThrow(UsageError);
    // tenant views have their own models
    const tenantApi = createEventStore({ ...config });
    expect(tenantApi.forTenant("A").readModel(def)).not.toBe(tenantApi.forTenant("B").readModel(def));
    await tenantApi.close();
    await api.close();
  });

  it("a past view refuses; stop() rejects reads and the next readModel(def) is a fresh model; close() stops every model", async () => {
    const api = newApi();
    await api.append([made("o1", 3)]);
    expect(() => api.asOf(1).readModel(offersModel)).toThrow(/a past view \(asOf\) has no live read model/);
    const model = api.readModel(offersModel);
    expect((await model.get("o1")).total).toBe(3);
    await model.stop();
    expect(model.status.phase).toBe("stopped");
    await expect(model.get("o1")).rejects.toThrow(/read model "offers" v1 stopped/);
    await expect(model.list()).rejects.toThrow(UsageError);
    const again = api.readModel(offersModel);
    expect(again).not.toBe(model);
    expect((await again.get("o1")).total).toBe(3);
    const other = api.readModel(defineReadModel({ ...offersModel, version: 2 }));
    await other.get("o1");
    await api.close();
    expect(again.status.phase).toBe("stopped");
    expect(other.status.phase).toBe("stopped");
    expect(() => api.readModel(offersModel)).toThrow(/closed/);
  });

  it("a stop while a read waits for the rebuild rejects that read", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const inner = new MemoryStore({ schema });
    const slow: EventStore = { ...wrap(inner), query: async (q, o) => (await gate, inner.query(q, o)) };
    const api = newApi(slow);
    const model = api.readModel(offersModel);
    const pending = model.get("o1");
    await model.stop();
    release();
    await expect(pending).rejects.toThrow(/stopped/);
    await api.close();
  });

  it("forTenant(t).readModel(def) after the view was dropped from the LRU returns the running model", async () => {
    const api = createEventStore({ ...config, maxTenantViews: 1 });
    const a = api.forTenant("A").readModel(offersModel);
    api.forTenant("B");
    api.forTenant("C"); // A's view is gone
    expect(api.forTenant("A").readModel(offersModel)).toBe(a);
    await api.close();
  });

  it("a transient store error at start is retried and the model becomes live; a fatal one fails it", async () => {
    const inner = new MemoryStore({ schema });
    await inner.append([made("o1", 4)]);
    let failures = 2;
    const flaky: EventStore = {
      ...wrap(inner),
      query: async (q, o) => {
        if (failures > 0) {
          failures--;
          throw new Error("connection reset");
        }
        return inner.query(q, o);
      },
    };
    const api = newApi(flaky);
    const model = api.readModel(offersModel);
    expect((await model.get("o1")).total).toBe(4);
    expect(model.status.phase).toBe("live");
    await api.close();

    const refusing: EventStore = { ...wrap(inner), query: async () => Promise.reject(new UsageError("eventstore: refused")) };
    const api2 = newApi(refusing);
    const failed = api2.readModel(offersModel);
    await expect(failed.get("o1")).rejects.toThrow("eventstore: refused");
    expect(failed.status.phase).toBe("failed");
    expect((failed.status.error as Error).message).toBe("eventstore: refused");
    await expect(failed.list()).rejects.toThrow("refused");
    expect(api2.readModel(offersModel)).toBe(failed); // a failed model stays until stop(): no silent rebuild loop
    await api2.close();
  });

  it("a store down at start costs a read timeoutMs, not forever", async () => {
    const inner = new MemoryStore({ schema });
    const down: EventStore = { ...wrap(inner), query: async () => Promise.reject(new Error("ECONNREFUSED")) };
    const api = newApi(down);
    const model = api.readModel(offersModel);
    const error = await model.get("o1", { timeoutMs: 150 }).catch((e: unknown) => e);
    expect(error).toMatchObject({ name: "TransientError", code: "read_model_timeout", httpStatus: 503 });
    expect((error as Error).message).toMatch(/has not finished its rebuild within 150 ms/);
    expect(model.status.phase).toBe("starting");
    await api.close();
  });
});

describe("live read model: memory-only cases", () => {
  it("unsettled events are not in the model until they settle; atLeast with their token waits for them", async () => {
    let settledUpTo = 1;
    const store = new MemoryStore({ schema, transactions: { isSettled: (_xid, sequence) => sequence <= settledUpTo } });
    const api = newApi(store);
    await api.append([made("o1", 1)]);
    const model = api.readModel(offersModel);
    expect((await model.get("o1")).total).toBe(1);
    const late = await api.append([made("o1", 10)]);
    await expect(model.get("o1", { atLeast: tokenOf(late), timeoutMs: 100 })).rejects.toMatchObject({ code: "read_model_timeout" });
    expect((await model.get("o1")).total).toBe(1);
    settledUpTo = 2;
    expect((await model.get("o1", { atLeast: tokenOf(late) })).total).toBe(11);
    await api.close();
  });

  it("two poison events on two keys in one page: both keys failed, the rest applied; a poison event mid-history fails a held key", async () => {
    const api = newApi();
    await api.append([made("a", 1), made("b", 1), made("p", 1, "open", true), made("c", 1), made("q", 2, "open", true), made("d", 1)]);
    await api.append([made("a", 5, "open", true)]);
    const model = api.readModel(offersModel);
    const ok = await model.list({ limit: 10 });
    expect(ok.rows.map((r) => r.key)).toEqual(["b", "c", "d"]);
    expect(ok.failed).toBe(3);
    for (const k of ["p", "q", "a"]) {
      await expect(model.get(k)).rejects.toBeInstanceOf(FailedKeyError);
      expect((await onRead(api, k)).ok).toBe(false);
    }
    expect(model.status).toMatchObject({ keys: 3, failed: 3 });
    await api.close();
  });

  it("a throwing onFailedKey is swallowed; the model goes on", async () => {
    const api = newApi();
    const def = defineReadModel({
      ...offersModel,
      name: "hook-throws",
      onFailedKey: () => {
        throw new Error("alerting is down");
      },
    });
    await api.append([made("p", 1, "open", true), made("o", 2)]);
    const model = api.readModel(def);
    expect((await model.get("o")).total).toBe(2);
    await expect(model.get("p")).rejects.toBeInstanceOf(FailedKeyError);
    await api.close();
  });

  it("the root view without rls folds every tenant into one model; tenant views keep their own", async () => {
    const api = createEventStore({ ...config });
    await api.forTenant("A").append([made("o1", 1)]);
    await api.forTenant("B").append([made("o1", 10)]);
    expect((await api.readModel(offersModel).get("o1")).total).toBe(11);
    expect((await api.forTenant("A").readModel(offersModel).get("o1")).total).toBe(1);
    expect((await api.forTenant("B").readModel(offersModel).get("o1")).total).toBe(10);
    await api.close();
  });

  it("tenants: A's model holds only A's states, and B's appends fold nothing into A", async () => {
    const api = createEventStore({ ...config });
    const a = api.forTenant("A");
    const b = api.forTenant("B");
    await a.append([made("o1", 1), made("o2", 2)]);
    await b.append([made("o1", 100), made("o3", 300)]);
    const ma = a.readModel(offersModel);
    const mb = b.readModel(offersModel);
    expect((await ma.list()).rows.map((r) => [r.key, r.state.total])).toEqual([["o1", 1], ["o2", 2]]);
    expect((await mb.list()).rows.map((r) => [r.key, r.state.total])).toEqual([["o1", 100], ["o3", 300]]);
    for (const [view, model] of [[a, ma], [b, mb]] as const) for (const k of ["o1", "o2", "o3"]) expect(await model.get(k)).toEqual((await onRead(view, k) as { state: Offer }).state);
    const foldedA = ma.status.folded;
    const appended = await b.append([made("o1", 1000), audited("o2"), noted("o2")]);
    await mb.get("o1", { atLeast: tokenOf(appended) });
    await new Promise((r) => setTimeout(r, 50));
    expect(ma.status.folded).toBe(foldedA);
    expect((await ma.get("o1")).total).toBe(1);
    await api.close();
  });
});

/** A plain EventStore over a memory store (no doorbell: the feed polls). */
function wrap(inner: MemoryStore): EventStore & { schema: typeof schema } {
  return {
    schema,
    query: (q, o) => inner.query(q, o),
    append: (e) => inner.append(e),
    appendIf: (e, c) => inner.appendIf(e, c),
    close: () => inner.close(),
  };
}
