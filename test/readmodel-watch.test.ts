import { afterEach, describe, expect, it } from "vitest";
import { buildSchema, createEventStore, defineReadModel, FailedKeyError, MemoryStore, UsageError, WatchOverflowError, type EventStore, type EventStoreApi, type Fold, type ListChange } from "../src/index.js";
import { audited, config, initialOffer, made, noted, offerFold, offers, offersModel, tokenOf, type Offer } from "./readmodel.suite.js";

const schema = buildSchema(config.events, { strict: true, tenantScopeKey: config.tenant.scopeKey });
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
const until = async (check: () => boolean, ms = 2_000) => {
  const end = Date.now() + ms;
  while (!check() && Date.now() < end) await wait(2);
  expect(check()).toBe(true);
};

/** The source has every type: OfferAudited reaches the fold, which returns the same state for it. */
const allTypes = defineReadModel({ name: "all-types", version: 1, source: offers.$filter(), keyBy: "offerId", fold: offerFold, initial: initialOffer });

const apis: EventStoreApi[] = [];
const newApi = (options: Partial<Parameters<typeof createEventStore>[0]> = {}) => {
  const api = createEventStore({ ...config, ...options });
  apis.push(api);
  return api;
};
afterEach(async () => {
  for (const api of apis.splice(0)) await api.close();
});

describe("read model watch(key)", () => {
  it("delivers the current state first, then one delivery per change; an event the fold ignores delivers nothing", async () => {
    const api = newApi();
    await api.append([made("o1", 2)]);
    const model = api.readModel(allTypes);
    const seen: Offer[] = [];
    const unseen: Offer[] = [];
    const w = await model.watch("o1", (s) => void seen.push(s));
    await model.watch("o9", (s) => void unseen.push(s));
    await until(() => seen.length === 1 && unseen.length === 1);
    expect(seen[0]!.total).toBe(2);
    expect(unseen[0]).toEqual(initialOffer);
    const a = await api.append([made("o1", 3)]);
    await model.get("o1", { atLeast: tokenOf(a) });
    await until(() => seen.length === 2);
    expect(seen[1]).toBe(await model.get("o1")); // the model's own object, not a copy
    expect(seen[1]!.total).toBe(5);
    // o9 enters the model with an event the fold ignores: its state is still the initial one, no push
    const b = await api.append([audited("o1"), made("o2", 1), audited("o9")]);
    await model.get("o1", { atLeast: tokenOf(b) });
    await wait(30);
    expect(seen).toHaveLength(2);
    expect(unseen).toHaveLength(1);
    w.stop();
    expect(w.stopped).toBe(true);
  });

  it("100 watchers on one key: the fold runs once per part, and every watcher gets the same state object", async () => {
    const api = newApi();
    let calls = 0;
    const counting: Fold<Offer> = (events, s) => {
      calls++;
      return offerFold(events, s);
    };
    const model = api.readModel(defineReadModel({ ...offersModel, name: "counting", fold: counting }));
    await model.get("o1");
    const got: Offer[][] = Array.from({ length: 100 }, () => []);
    for (let i = 0; i < 100; i++) await model.watch("o1", (s) => void got[i]!.push(s));
    calls = 0;
    const a = await api.append([made("o1", 7), made("o1", 1)]);
    await model.get("o1", { atLeast: tokenOf(a) });
    await until(() => got.every((g) => g.length === 2));
    expect(calls).toBe(1);
    const state = await model.get("o1");
    for (const g of got) expect(g[1]).toBe(state);
  });

  it("a callback that throws ends only that watcher, with onError; a slow watcher is dropped with WatchOverflowError", async () => {
    const api = newApi({ watch: { maxPendingBatches: 2 } });
    const model = api.readModel(offersModel);
    await model.get("o1");
    const errors: unknown[] = [];
    const healthy: number[] = [];
    const throwing = await model.watch(
      "o1",
      (s) => {
        if (s.n > 0) throw new Error("render failed");
      },
      { onError: (e) => void errors.push(e) },
    );
    await model.watch("o1", (s) => void healthy.push(s.n));
    const slowErrors: unknown[] = [];
    const slow = await model.watch("o1", () => new Promise<void>(() => undefined), { onError: (e) => void slowErrors.push(e) });
    for (let i = 0; i < 4; i++) {
      const a = await api.append([made("o1", 1)]);
      await model.get("o1", { atLeast: tokenOf(a) });
    }
    await until(() => healthy.length === 5);
    expect(throwing.stopped).toBe(true);
    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).message).toBe("render failed");
    expect(slow.stopped).toBe(true);
    expect(slowErrors).toHaveLength(1);
    expect(slowErrors[0]).toBeInstanceOf(WatchOverflowError);
  });

  it("signal: aborted before, while waiting for the rebuild, and after; each gives its place back", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const inner = new MemoryStore({ schema });
    const slow: EventStore & { schema: typeof schema } = { schema, query: async (q, o) => (await gate, inner.query(q, o)), append: (e) => inner.append(e), appendIf: (e, c) => inner.appendIf(e, c), close: () => inner.close() };
    const api = newApi({ store: slow, watch: { maxWatchers: 1 } });
    const model = api.readModel(offersModel);
    const before = new AbortController();
    before.abort(new Error("gone before"));
    await expect(model.watch("o1", () => undefined, { signal: before.signal })).rejects.toThrow("gone before");
    const during = new AbortController();
    const pending = model.watch("o1", () => undefined, { signal: during.signal });
    during.abort(new Error("gone while waiting"));
    await expect(pending).rejects.toThrow("gone while waiting");
    release();
    const after = new AbortController();
    const w = await model.watch("o1", () => undefined, { signal: after.signal });
    after.abort();
    expect(w.stopped).toBe(true);
    // the one place is free again
    (await model.watch("o1", () => undefined)).stop();
  });

  it("model watchers and es.watch share watch.maxWatchers", async () => {
    const api = newApi({ watch: { maxWatchers: 3 } });
    const model = api.readModel(offersModel);
    const w1 = await api.watch(offers.$filter(), () => undefined);
    await api.watch(offers.$filter(), () => undefined);
    const m = await model.watch("o1", () => undefined);
    await expect(api.watch(offers.$filter(), () => undefined)).rejects.toThrow(/at most 3 watchers per process/);
    await expect(model.watch("o1", () => undefined)).rejects.toThrow(UsageError);
    await expect(model.watchList({}, () => undefined)).rejects.toThrow(/watch\.maxWatchers/);
    m.stop();
    const l = await model.watchList({}, () => undefined);
    await expect(api.watch(offers.$filter(), () => undefined)).rejects.toThrow(UsageError);
    w1.stop();
    l.stop();
    (await model.watch("o1", () => undefined)).stop();
  });

  it("stop() and close() end watchers without onError; a failed key ends its watchers with onError; watch(failedKey) rejects", async () => {
    const api = newApi();
    const model = api.readModel(offersModel);
    await model.get("o1");
    const errors: unknown[] = [];
    const seen: Offer[] = [];
    const w = await model.watch("p", (s) => void seen.push(s), { onError: (e) => void errors.push(e) });
    const a = await api.append([made("p", 1), made("p", 2, "open", true)]);
    await model.get("o1", { atLeast: tokenOf(a) });
    await until(() => w.stopped);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(FailedKeyError);
    await expect(model.watch("p", () => undefined)).rejects.toBeInstanceOf(FailedKeyError);
    const quiet: unknown[] = [];
    const w2 = await model.watch("o1", () => undefined, { onError: (e) => void quiet.push(e) });
    const w3 = await model.watchList({}, () => undefined, { onError: (e) => void quiet.push(e) });
    await model.stop();
    expect([w2.stopped, w3.stopped]).toEqual([true, true]);
    await expect(model.watch("o1", () => undefined)).rejects.toThrow(/stopped/);
    const other = api.readModel(offersModel);
    const w4 = await other.watch("o1", () => undefined, { onError: (e) => void quiet.push(e) });
    await api.close();
    expect(w4.stopped).toBe(true);
    expect(quiet).toEqual([]);
  });

  it("a model that fails ends every watcher with onError(failure)", async () => {
    const inner = new MemoryStore({ schema });
    let refuse = false;
    const store: EventStore & { schema: typeof schema } = {
      schema,
      query: async (q, o) => {
        if (refuse) throw new UsageError("eventstore: permission denied");
        return inner.query(q, o);
      },
      append: (e) => inner.append(e),
      appendIf: (e, c) => inner.appendIf(e, c),
      close: () => inner.close(),
    };
    const api = newApi({ store });
    const model = api.readModel(offersModel);
    const errors: unknown[] = [];
    const w = await model.watch("o1", () => undefined, { onError: (e) => void errors.push(e) });
    refuse = true;
    await api.append([made("o1", 1)]);
    await until(() => w.stopped, 3_000);
    expect((errors[0] as Error).message).toBe("eventstore: permission denied");
    expect(model.status.phase).toBe("failed");
  });
});

describe("read model watchList({ where })", () => {
  it("delivers keys that entered or changed and match, and keys that stopped matching or failed, with a growing revision", async () => {
    const api = newApi();
    await api.append([made("o1", 1, "open"), made("o2", 1, "won")]);
    const model = api.readModel(allTypes);
    const page = await model.list({ where: { status: "open" } });
    expect(page.rows.map((r) => r.key)).toEqual(["o1"]);
    const changes: ListChange<Offer>[] = [];
    await model.watchList({ where: { status: "open" } }, (c) => void changes.push(c));
    const settle = async (events: Parameters<typeof api.append>[0]) => model.get("x", { atLeast: tokenOf(await api.append(events)) });
    await settle([made("o3", 5, "open")]); // enters, matches
    await settle([made("o1", 2, "won")]); // stops matching
    await settle([made("o2", 9, "won")]); // outside the filter before and after: nothing
    await settle([audited("o3")]); // same state reference: nothing
    await settle([made("o3", 1, "open"), made("p", 1, "open")]); // o3 changes, p enters
    await settle([made("p", 1, "open", true)]); // p fails while matching
    await until(() => changes.length === 4);
    await wait(30);
    expect(changes.map((c) => [c.rows.map((r) => [r.key, r.state.total]), c.removed])).toEqual([
      [[["o3", 5]], []],
      [[], ["o1"]],
      [[["o3", 6], ["p", 1]], []],
      [[], ["p"]],
    ]);
    const revisions = changes.map((c) => c.revision);
    expect(revisions[0]!).toBeGreaterThan(page.revision);
    for (let i = 1; i < revisions.length; i++) expect(revisions[i]!).toBeGreaterThan(revisions[i - 1]!);
    expect(changes[2]!.rows[0]!.state).toBe(await model.get("o3"));
    await expect(model.watchList({ where: { status: {} as never } }, () => undefined)).rejects.toThrow(UsageError);
  });

  it("tenants: A's watchers never receive B's states", async () => {
    const api = newApi();
    const a = api.forTenant("A");
    const b = api.forTenant("B");
    const seenKey: Offer[] = [];
    const seenList: ListChange<Offer>[] = [];
    const ma = a.readModel(offersModel);
    await ma.watch("o1", (s) => void seenKey.push(s));
    await ma.watchList({}, (c) => void seenList.push(c));
    const mb = b.readModel(offersModel);
    await mb.get("o1", { atLeast: tokenOf(await b.append([made("o1", 100), noted("o1")])) });
    await wait(100);
    expect(seenKey).toEqual([initialOffer]);
    expect(seenList).toEqual([]);
    await ma.get("o1", { atLeast: tokenOf(await a.append([made("o1", 1)])) });
    await until(() => seenKey.length === 2 && seenList.length === 1);
    expect(seenKey[1]!.total).toBe(1);
    expect(seenList[0]!.rows.map((r) => [r.key, r.state.total])).toEqual([["o1", 1]]);
  });
});
