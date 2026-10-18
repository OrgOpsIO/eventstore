import { describe, expect, it } from "vitest";
import { buildSchema, createEventStore, FailedKeyError, MemoryStore, UsageError, type EventStore, type EventStoreApi, type Fold } from "../src/index.js";
import { defineReadModel } from "../src/index.js";
import type { MemoryReadModelStore, ReadModelStore } from "../src/modelstore.js";
import { block, capOf, made, noted, offersModel, onRead, persistedModelSuite, pmConfig, source, tokenOf, until, wait, type Offer, type PersistedHarness } from "./persisted-model.suite.js";

const schema = buildSchema(pmConfig.events, { strict: true, tenantScopeKey: pmConfig.tenant.scopeKey });

function memoryHarness(): PersistedHarness {
  let store = new MemoryStore({ schema, persistedModels: true });
  const opened: EventStoreApi[] = [];
  return {
    writers: 1,
    async reset() {
      for (const api of opened.splice(0)) await api.close();
      store = new MemoryStore({ schema, persistedModels: true });
    },
    open(options = {}) {
      const api = createEventStore({ ...pmConfig, store, ...options });
      opened.push(api);
      return api;
    },
    async closeAll() {
      for (const api of opened.splice(0)) await api.close();
    },
  };
}

describe("persisted read model on the memory store", () => {
  persistedModelSuite(memoryHarness(), { numRuns: 60 });
});

/** A plain EventStore over a memory store, keeping its doorbell, with a capability of our choosing. */
function over(inner: MemoryStore, readModelStore: ReadModelStore | undefined): EventStore {
  return {
    schema,
    query: (q, o) => inner.query(q, o),
    append: (e) => inner.append(e),
    appendIf: (e, c) => inner.appendIf(e, c),
    onCommitted: (l: (hint: number | null) => void) => inner.onCommitted(l),
    close: async () => undefined,
    ...(readModelStore ? { readModelStore } : {}),
  } as EventStore;
}

const counted = (sum: number, state: Offer) => sum + state.n + state.notes;

describe("persisted read model: memory-only cases", () => {
  it("without persistedModels the handle fails with a UsageError that says what to do, and every read rejects with it", async () => {
    const api = createEventStore({ ...pmConfig, store: new MemoryStore({ schema }) });
    const model = api.persistedReadModel(offersModel);
    const error = await model.get("o1").catch((e: unknown) => e);
    expect(error).toBeInstanceOf(UsageError);
    expect((error as Error).message).toMatch(/the store does not keep persisted read models — create it with persistedModels: true/);
    await expect(model.list()).rejects.toThrow(/persistedModels: true/);
    await expect(model.watch("o1", () => undefined)).rejects.toThrow(/persistedModels: true/);
    const status = await model.status();
    expect(status).toMatchObject({ phase: "failed", role: "standby", checkpoint: null, folded: 0 });
    expect(status.error).toBe(error);
    expect(api.persistedReadModel(offersModel)).toBe(model); // stays until stop(): no silent retry loop
    await api.close();
  });

  it("zombie: a writer whose lock was taken without telling it is fenced at its next page, writes nothing, and every event is applied once", async () => {
    const store = new MemoryStore({ schema, persistedModels: true });
    const cap = store.readModelStore as MemoryReadModelStore;
    cap.knobs.tickMs = 60_000; // nobody retries on their own: the test decides who writes
    const a = createEventStore({ ...pmConfig, store });
    const b = createEventStore({ ...pmConfig, store });
    await a.append([made("o1", 1), made("o2", 1)]);
    const ma = a.persistedReadModel(offersModel);
    const t0 = await a.append([made("o1", 1)]);
    await ma.list({ atLeast: tokenOf(t0) });
    expect((await ma.status()).role).toBe("writer");
    const blocked = createEventStore({ ...pmConfig, store }).persistedReadModel(offersModel);
    await wait(20);
    expect((await blocked.status()).role).toBe("standby"); // A holds the lock
    await blocked.stop();
    // A loses its lock without noticing; B opens, becomes the writer (epoch + 1) and applies
    cap.steal({ name: "offers", version: 1 });
    const mb = b.persistedReadModel(offersModel);
    await until(async () => (await mb.status()).role === "writer");
    // A still believes it writes; its feed handles the next page too and meets B's epoch
    expect((await ma.status()).role).toBe("writer");
    const foldedA = (await ma.status()).folded;
    const t1 = await b.append([made("o1", 10), noted("o2")]);
    await mb.list({ atLeast: tokenOf(t1) });
    await until(async () => (await ma.status()).role === "standby");
    expect((await ma.status()).folded).toBe(foldedA); // fenced: it committed nothing
    const t2 = await a.append([made("o2", 100)]);
    await mb.list({ atLeast: tokenOf(t2) });
    for (const k of ["o1", "o2"]) expect(await mb.get(k)).toEqual(((await onRead(b, k)) as { state: Offer }).state);
    const rows = (await mb.list()).rows.map((r) => r.state);
    expect(rows.reduce(counted, 0)).toBe(6);
    expect((await mb.status()).epoch).toBe(2);
    await a.close();
    await b.close();
  });

  it("a database error inside a page is retried and skips no event: no failed key, every key equals fold on read", async () => {
    const inner = new MemoryStore({ schema, persistedModels: true });
    const cap = inner.readModelStore!;
    let failures = 2;
    const flaky: ReadModelStore = {
      ...cap,
      page: async (ref, epoch, fn) => {
        if (failures > 0) {
          failures--;
          throw new Error("connection reset by peer");
        }
        return cap.page(ref, epoch, fn);
      },
    };
    const api = createEventStore({ ...pmConfig, store: over(inner, flaky) });
    await api.append([made("o1", 1), made("o2", 2), noted("o1")]);
    const model = api.persistedReadModel(offersModel);
    const t = await api.append([made("o1", 3)]);
    const listed = await model.list({ atLeast: tokenOf(t) });
    expect(failures).toBe(0);
    expect(listed.failed).toBe(0);
    for (const k of ["o1", "o2"]) expect(await model.get(k)).toEqual(((await onRead(api, k)) as { state: Offer }).state);
    expect(listed.rows.map((r) => r.state).reduce(counted, 0)).toBe(4);
    await api.close();
  });

  it("watchList: a key that changed twice within one notification is reported removed although it never matched (the documented superset); once, only if it matched", async () => {
    const inner = new MemoryStore({ schema, persistedModels: true });
    const cap = inner.readModelStore!;
    let gate: Promise<void> = Promise.resolve();
    const held: ReadModelStore = { ...cap, changes: async (ref, after, limit) => (await gate, cap.changes(ref, after, limit)) };
    const api = createEventStore({ ...pmConfig, store: over(inner, held) });
    const t0 = await api.append([made("o1", 1, "open"), made("o5", 1, "won")]);
    const model = api.persistedReadModel(offersModel);
    await model.list({ atLeast: tokenOf(t0) });
    const changes: { rows: string[]; removed: string[] }[] = [];
    await model.watchList({ where: { status: "open" } }, (c) => void changes.push({ rows: c.rows.map((r) => r.key), removed: [...c.removed] }));
    const revision = (await model.status()).revision;
    let open!: () => void;
    gate = new Promise((r) => (open = r)); // the follower's next read waits until two pages of o5 committed
    await model.list({ atLeast: tokenOf(await api.append([made("o5", 1, "lost")])) });
    await model.list({ atLeast: tokenOf(await api.append([made("o5", 1, "won")])) });
    expect((await model.status()).revision).toBe(revision + 2);
    open();
    await until(() => changes.length === 1);
    expect(changes).toEqual([{ rows: [], removed: ["o5"] }]); // o5 never matched: the superset, harmless to a frontend
    gate = Promise.resolve();
    await model.list({ atLeast: tokenOf(await api.append([made("o5", 1, "lost")])) }); // once since the last read, never matched: nothing
    await model.list({ atLeast: tokenOf(await api.append([made("o1", 1, "won")])) }); // once, matched before: removed
    await until(() => changes.length === 2);
    await wait(50);
    expect(changes).toEqual([{ rows: [], removed: ["o5"] }, { rows: [], removed: ["o1"] }]);
    await api.close();
  });

  it("a fold that throws only on a large page isolates the one poison event: neighbours of the same page are applied", async () => {
    const store = new MemoryStore({ schema, persistedModels: true });
    const api = createEventStore({ ...pmConfig, store });
    const failures: FailedKeyError[] = [];
    const def = defineReadModel({ ...offersModel, name: "isolate", batchSize: 64, onFailedKey: (e) => void failures.push(e) });
    const t = await api.append([...Array.from({ length: 40 }, (_, i) => made(`k${i}`, 1)), made("p", 1, "open", true), made("q", 1, "open", true), ...Array.from({ length: 20 }, (_, i) => made(`k${i}`, 1))]);
    const model = api.persistedReadModel(def);
    const listed = await model.list({ atLeast: tokenOf(t), limit: 100 });
    expect(listed.failed).toBe(2);
    expect(listed.rows.map((r) => r.state).reduce(counted, 0)).toBe(60);
    expect(failures.map((f) => f.key).sort()).toEqual(["p", "q"]);
    await api.close();
  });

  it("tenants: A's handle holds, lists, watches and reports only A's rows; the root view without rls keeps its rows apart under tenant ''", async () => {
    const store = new MemoryStore({ schema, persistedModels: true });
    const api = createEventStore({ ...pmConfig, store });
    const a = api.forTenant("A");
    const b = api.forTenant("B");
    await a.append([made("o1", 1), made("o2", 2)]);
    const tb = await b.append([made("o1", 100), made("o3", 300)]);
    const ma = a.persistedReadModel(offersModel);
    const mb = b.persistedReadModel(offersModel);
    const root = api.persistedReadModel(offersModel);
    expect(ma).not.toBe(mb);
    await mb.list({ atLeast: tokenOf(tb) });
    const ta = await a.append([made("o2", 1)]);
    expect((await ma.list({ atLeast: tokenOf(ta) })).rows.map((r) => [r.key, r.state.total])).toEqual([["o1", 1], ["o2", 3]]);
    expect((await mb.list()).rows.map((r) => [r.key, r.state.total])).toEqual([["o1", 100], ["o3", 300]]);
    expect((await root.list({ atLeast: tokenOf(ta) })).rows.map((r) => [r.key, r.state.total])).toEqual([["o1", 101], ["o2", 3], ["o3", 300]]);
    for (const [view, model] of [[a, ma], [b, mb]] as const) for (const k of ["o1", "o2", "o3"]) expect(await model.get(k)).toEqual(((await onRead(view, k)) as { state: Offer }).state);
    const seenA: Offer[] = [];
    await ma.watch("o1", (s) => void seenA.push(s));
    await mb.list({ atLeast: tokenOf(await b.append([made("o1", 1000)])) });
    await wait(50);
    expect(seenA.map((s) => s.total)).toEqual([1]);
    expect((await ma.status()).revision).toBe(2);
    await api.close();
  });

  it("es.close() stops every handle and releases its writer lock: another process takes over at once", async () => {
    const store = new MemoryStore({ schema, persistedModels: true });
    const a = createEventStore({ ...pmConfig, store });
    const model = a.persistedReadModel(offersModel);
    await until(async () => (await model.status()).role === "writer");
    await a.close();
    expect((await model.status()).phase).toBe("stopped");
    const b = createEventStore({ ...pmConfig, store });
    const release = await block(b, offersModel); // the lock is free again
    await release();
    await b.close();
  });

  it("an unsettled event is not in a row until it settles, but get folds it on top at once", async () => {
    let settledUpTo = 1;
    const store = new MemoryStore({ schema, persistedModels: true, transactions: { isSettled: (_xid, sequence) => sequence <= settledUpTo } });
    const api = createEventStore({ ...pmConfig, store });
    await api.append([made("o1", 1)]);
    const model = api.persistedReadModel(offersModel);
    await until(async () => (await model.status()).checkpoint !== null);
    await api.append([made("o1", 10)]);
    expect((await model.get("o1")).total).toBe(11); // current: the unsettled tail on top
    expect((await model.list()).rows[0]!.state.total).toBe(1); // as current as the checkpoint
    settledUpTo = 2;
    await until(async () => (await model.list()).rows[0]!.state.total === 11);
    expect((await model.get("o1")).total).toBe(11);
    await api.close();
  });

  it("a fold that keeps a copy: two gets of a key share the copy's object when nothing is new", async () => {
    const store = new MemoryStore({ schema, persistedModels: true });
    const api = createEventStore({ ...pmConfig, store });
    const fold: Fold<Offer> = offersModel.fold;
    await api.append([made("o1", 1)]);
    const model = api.persistedReadModel(defineReadModel({ ...offersModel, name: "copies", fold }));
    const first = await model.get("o1");
    expect(await model.get("o1")).toBe(first);
    const none = createEventStore({ ...pmConfig, store, contextCache: false });
    const def2 = defineReadModel({ ...offersModel, name: "copies2", fold });
    const release = await block(api, def2); // no writer: the row stays absent
    const uncached = none.persistedReadModel(def2);
    await uncached.get("o1");
    const before = (await uncached.status()).readFolded;
    await uncached.get("o1");
    expect((await uncached.status()).readFolded).toBe(before + 1); // contextCache: false keeps no copy: every get reads the row and its delta
    await release();
    await none.close();
    await api.close();
  });

  for (const younger of [5, 1_200]) {
    it(`lag under xid/sequence inversion: the older transaction's event applied, ${younger} younger-xid events of lower sequence pending — lag.events is at least ${younger}; caught up 0`, async () => {
      // the first batch (sequences 1..n) gets the higher transaction id, the next one (n + 1) the lower
      const store = new MemoryStore({ schema, persistedModels: true, transactions: { allocateId: (first) => (first === 1 ? "200" : "100") } });
      const api = createEventStore({ ...pmConfig, store });
      let release: () => void = () => undefined;
      try {
        await api.append(Array.from({ length: younger }, () => made("o1", 1)));
        await api.append([made("o2", 1)]);
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
        expect(status.checkpoint).toEqual({ transactionId: "100", sequence: younger + 1 });
        const pending = (await api.query(source, { cursor: status.checkpoint, settledOnly: true })).events;
        expect(pending).toHaveLength(younger);
        expect(status.lag.events).toBeGreaterThanOrEqual(younger);
        if (younger <= 1_000) expect(status.lag.events).toBe(younger); // few: counted exactly
        release();
        cap.page = page;
        await until(async () => (await model.status()).lag.events === 0, 20_000);
        expect(await model.status()).toMatchObject({ checkpoint: { transactionId: "200", sequence: younger }, lag: { events: 0, ms: 0 } });
      } finally {
        release();
        await api.close();
      }
    }, 30_000);
  }
});
