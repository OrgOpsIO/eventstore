/**
 * The live read model, specified once and run against the memory store and Postgres
 * (readmodel-memory.test.ts, readmodel-postgres.test.ts). A "process" is a fresh runtime over the
 * same data: its models start empty and rebuild from the store.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { defineReadModel, FailedKeyError, TransientError, UsageError, WatchOverflowError, type Cursor, type EventStoreApi, type EventStoreConfig, type Fold, type ListChange, type LiveReadModel, type NewEvent, type RecordedEvent } from "../src/index.js";
import { keyQuery } from "../src/readmodel.js";
import { audited, initialOffer, made, noted, offerFold, offersModel, source, tokenOf, type Offer } from "./readmodel.model.js";

export * from "./readmodel.model.js";
export interface ModelHarness {
  /** Empty events for the next test. */
  reset(): Promise<void>;
  /** A new "process": a runtime over the same data. Closed by `closeAll`. */
  open(options?: Partial<EventStoreConfig>): EventStoreApi;
  /** Several writers on separate connections (Postgres: separate pools), for concurrent appends. */
  readonly writers: number;
  /** The tables of the test schema (Postgres), to prove a live model writes nothing; memory: `[]`. */
  tables(): Promise<string[]>;
  closeAll(): Promise<void>;
}

/** Fold on read for one key: the state, or the error it throws. */
export async function onRead(api: EventStoreApi, key: string): Promise<{ ok: true; state: Offer } | { ok: false; error: unknown }> {
  try {
    const loaded = await api.context({ query: keyQuery(source, "offerId", key), fold: offerFold, initial: initialOffer });
    return { ok: true, state: loaded.state };
  } catch (error) {
    return { ok: false, error };
  }
}

async function fromModel(model: LiveReadModel<Offer>, key: string): Promise<{ ok: true; state: Offer } | { ok: false; error: unknown }> {
  try {
    return { ok: true, state: await model.get(key) };
  } catch (error) {
    return { ok: false, error };
  }
}

const KEYS = ["o1", "o2", "o3", "o4", "o5"];

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
const until = async (check: () => boolean, ms = 3_000) => {
  const end = Date.now() + ms;
  while (!check() && Date.now() < end) await wait(5);
  expect(check()).toBe(true);
};

/**
 * A pure, batch-associative fold whose state has a computed, non-enumerable `status` getter: it
 * throws for a broken key while `computedThrows` is on. The fold itself throws only on a poison event.
 */
type Computed = { count: number; broken: boolean; readonly status?: string };
let computedThrows = true;
const computedFold: Fold<Computed> = (events, state) => {
  let count = state.count;
  let broken = state.broken;
  for (const e of events) {
    const d = e.data as { status: string; poison?: boolean };
    if (d.poison) throw new Error(`poison event #${e.sequence}`);
    count++;
    broken ||= d.status === "broken";
  }
  return Object.defineProperty({ count, broken }, "status", {
    get() {
      if (broken && computedThrows) throw new Error("computed status unavailable");
      return "open";
    },
  });
};
const computedModel = defineReadModel<Computed>({ name: "computed", version: 1, source, keyBy: "offerId", initial: { count: 0, broken: false }, fold: computedFold });
const countOnRead = async (api: EventStoreApi, key: string) =>
  (await api.context({ query: keyQuery(source, "offerId", key), fold: computedFold, initial: { count: 0, broken: false } })).state.count;

export function readModelSuite(h: ModelHarness, options: { readonly numRuns: number }): void {
  it("property: model.get(k) equals context(source ∧ offerId = k, fold) for every key, or both throw", async () => {
    const event = fc.oneof(
      { weight: 6, arbitrary: fc.record({ kind: fc.constant("made" as const), key: fc.constantFrom(...KEYS), amount: fc.integer({ min: -20, max: 50 }), status: fc.constantFrom("open", "won", "lost") }) },
      { weight: 1, arbitrary: fc.record({ kind: fc.constant("noted" as const), key: fc.constantFrom(...KEYS) }) },
      { weight: 1, arbitrary: fc.constant({ kind: "noted-no-key" as const }) },
      { weight: 2, arbitrary: fc.record({ kind: fc.constant("audited" as const), key: fc.constantFrom(...KEYS) }) },
    );
    // a step: up to `writers` concurrent appends of 1-4 events each
    const step = fc.array(fc.array(event, { minLength: 1, maxLength: 4 }), { minLength: 1, maxLength: Math.max(1, h.writers) });
    let mismatches = 0;
    let comparisons = 0;
    let poisoned = 0;
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(1, 3, 500), fc.array(step, { minLength: 1, maxLength: 6 }), fc.option(fc.nat({ max: 12 }), { nil: undefined }), async (batchSize, steps, poisonAt) => {
        await h.reset();
        const writers = Array.from({ length: Math.max(1, h.writers) }, () => h.open());
        const reader = h.open();
        const def = defineReadModel({ ...offersModel, name: `prop-${batchSize}`, batchSize });
        const model = reader.readModel(def);
        let madeCount = 0;
        const toEvent = (e: { kind: string; key?: string; amount?: number; status?: string }): NewEvent => {
          if (e.kind === "made") return made(e.key!, e.amount!, e.status, madeCount++ === poisonAt);
          if (e.kind === "noted") return noted(e.key);
          if (e.kind === "noted-no-key") return noted();
          return audited(e.key!);
        };
        for (const appends of steps) {
          const results = await Promise.all(appends.map((events, i) => writers[i % writers.length]!.append(events.map(toEvent))));
          for (const r of results) await model.get("never", { atLeast: tokenOf(r) });
          for (const key of [...KEYS, "never"]) {
            const got = await fromModel(model, key);
            const expected = await onRead(reader, key);
            comparisons++;
            if (got.ok !== expected.ok || (got.ok && expected.ok && !isEqual(got.state, expected.state))) mismatches++;
            if (!got.ok) {
              poisoned++;
              expect(got.error).toBeInstanceOf(FailedKeyError);
              expect((got.error as FailedKeyError).key).toBe(key);
            }
            expect(got.ok).toBe(expected.ok);
            if (got.ok && expected.ok) expect(got.state).toEqual(expected.state);
          }
          // list holds exactly the keys with a folded event, minus failed keys, with the same states
          const listed = await model.list({ limit: 1000 });
          const held: string[] = [];
          for (const key of KEYS) {
            const all = await reader.query(keyQuery(source, "offerId", key));
            const r = await fromModel(model, key);
            if (all.events.length > 0 && r.ok) held.push(key);
          }
          expect(listed.rows.map((row) => row.key)).toEqual(held);
          for (const row of listed.rows) expect(row.state).toEqual((await onRead(reader, row.key) as { state: Offer }).state);
        }
        await h.closeAll();
      }),
      { numRuns: options.numRuns },
    );
    expect(mismatches).toBe(0);
    expect(comparisons).toBeGreaterThan(0);
    // the generator does reach a poisoned key in some runs
    expect(poisoned).toBeGreaterThan(0);
    console.log(`read model property: ${comparisons} comparisons, ${mismatches} mismatches, ${poisoned} failed-key reads (both threw)`);
  }, 60_000); // numRuns × a reset and a rebuild: over the 5 s default under load

  it("a warm read folds 0 events; after an append it folded exactly the events routed to keys", async () => {
    await h.reset();
    const api = h.open();
    await api.append([made("o1", 1), made("o2", 2), noted(), audited("o1")]);
    const model = api.readModel(offersModel);
    expect(model.status.phase).toBe("starting");
    await model.get("o1");
    expect(model.status.phase).toBe("live");
    const before = model.status.folded;
    expect(before).toBe(2);
    for (let i = 0; i < 100; i++) await model.get(KEYS[i % 5]!);
    for (let i = 0; i < 10; i++) await model.list({ sort: { by: "total" } });
    expect(model.status.folded - before).toBe(0);
    const a = await api.append([made("o1", 5), noted("o2"), noted(), audited("o3")]);
    expect((await model.get("o1", { atLeast: tokenOf(a) })).total).toBe(6);
    expect(model.status.folded - before).toBe(2); // OfferMade o1 and OfferNoted o2; no key and outside the source are not folded
    await h.closeAll();
  });

  it("restart: a fresh process rebuilds from the store, equals the old model for every key, and writes nothing", async () => {
    await h.reset();
    const tablesBefore = await h.tables();
    const first = h.open();
    for (let i = 0; i < 30; i++) await first.append([made(KEYS[i % 5]!, i, i % 3 === 0 ? "won" : "open"), ...(i % 4 === 0 ? [noted(KEYS[i % 5]!)] : [])]);
    const old = first.readModel(offersModel);
    const oldStates = await Promise.all(KEYS.map((k) => old.get(k)));
    const second = h.open();
    const fresh = second.readModel(offersModel);
    expect(fresh).not.toBe(old);
    expect(fresh.status.phase).toBe("starting");
    expect(await Promise.all(KEYS.map((k) => fresh.get(k)))).toEqual(oldStates);
    expect(fresh.status).toMatchObject({ phase: "live", keys: 5, folded: old.status.folded });
    expect(await h.tables()).toEqual(tablesBefore);
    await h.closeAll();
  });

  it("atLeast: a matching token is included, a non-matching one passes, one that never comes times out, a malformed one is refused", async () => {
    await h.reset();
    const api = h.open();
    const model = api.readModel(offersModel);
    await model.get("o1");
    const a = await api.append([made("o1", 7)]);
    expect((await model.get("o1", { atLeast: tokenOf(a) })).total).toBe(7);
    const b = await api.append([audited("o1")]); // outside the source: passed by the cursor or the watermark
    expect((await model.get("o1", { atLeast: tokenOf(b), timeoutMs: 5_000 })).total).toBe(7);
    const future = { transactionId: String(BigInt(a.transactionId) + 1_000_000n), sequence: a.last + 1_000_000 };
    const started = Date.now();
    const timedOut = await model.get("o1", { atLeast: future, timeoutMs: 200 }).catch((e: unknown) => e);
    expect(timedOut).toBeInstanceOf(TransientError);
    expect((timedOut as TransientError).code).toBe("read_model_timeout");
    expect((timedOut as Error).message).toContain('read model "offers" v1');
    expect(Date.now() - started).toBeLessThan(2_000);
    await expect(model.get("o1", { atLeast: { transactionId: "x", sequence: 1 } })).rejects.toThrow(UsageError);
    await expect(model.list({ atLeast: { sequence: 1 } as unknown as Cursor })).rejects.toThrow(/atLeast needs a Cursor/);
    await expect(model.get("o1", { timeoutMs: 0 })).rejects.toThrow(/timeoutMs/);
    await h.closeAll();
  });

  it("list: AND of equality filters, one sort with missing values last and ties by key, paging", async () => {
    await h.reset();
    const api = h.open();
    await api.append([made("k3", 5, "open"), made("k1", 5, "open"), made("k2", 9, "won"), made("k4", 1, "open"), made("k5", 5, "lost")]);
    await api.append([noted("k6")]); // k6: total 0, status "none"
    const model = api.readModel(offersModel);
    const asc = await model.list({ sort: { by: "total" } });
    expect(asc.rows.map((r) => r.key)).toEqual(["k6", "k4", "k1", "k3", "k5", "k2"]);
    expect(asc).toMatchObject({ total: 6, failed: 0 });
    const desc = await model.list({ sort: { by: "total", dir: "desc" } });
    expect(desc.rows.map((r) => r.key)).toEqual(["k2", "k1", "k3", "k5", "k4", "k6"]); // ties stay by key ascending
    const open5 = await model.list({ where: { status: "open", total: 5 } });
    expect(open5.rows.map((r) => r.key)).toEqual(["k1", "k3"]);
    expect(open5.total).toBe(2);
    expect((await model.list({ where: { last: null } })).total).toBe(1); // null matches only null: k6
    const pages = [];
    for (let offset = 0; offset < 6; offset += 4) pages.push(...(await model.list({ sort: { by: "total" }, offset, limit: 4 })).rows.map((r) => r.key));
    expect(pages).toEqual(asc.rows.map((r) => r.key));
    // a missing field is last in both directions; a Date sorts by time
    expect((await model.list({ sort: { by: "nope" as never, dir: "desc" } })).rows.map((r) => r.key)).toEqual(["k1", "k2", "k3", "k4", "k5", "k6"]);
    expect((await model.list({ sort: { by: "last" } })).rows.at(-1)!.key).toBe("k6");
    // a list after an append reflects it (the kept result is rebuilt)
    const r1 = (await model.list({ sort: { by: "total" } })).revision;
    const a = await api.append([made("k6", 100, "won")]);
    const after = await model.list({ sort: { by: "total", dir: "desc" }, atLeast: tokenOf(a), limit: 1 });
    expect(after.rows.map((r) => [r.key, r.state.total])).toEqual([["k6", 100]]);
    expect(after.revision).toBeGreaterThan(r1);
    for (const bad of [{ limit: 0 }, { offset: -1 }, { where: { a: {} } }, { where: Object.fromEntries(Array.from({ length: 65 }, (_, i) => [`f${i}`, 1])) }, { sort: { by: "total", dir: "up" } }, { sort: { by: "" } }]) {
      await expect(model.list(bad as never)).rejects.toThrow(UsageError);
    }
    await h.closeAll();
  });

  it("list orders mixed kinds by rank: number < string < boolean < Date, then missing", async () => {
    await h.reset();
    const api = h.open();
    for (const k of ["a", "b", "c", "d", "e", "f"]) await api.append([made(k, 1)]);
    type V = { v?: unknown };
    const values: Record<string, unknown> = { a: true, b: "x", c: 3, d: new Date(5), e: null, f: Number.NaN };
    const def = defineReadModel<V>({ name: "mixed", version: 1, source, keyBy: "offerId", initial: {}, fold: (events, s) => (events.length > 0 ? { v: values[events[0]!.scopes.offerId!] } : s) });
    const model = api.readModel(def);
    expect((await model.list({ sort: { by: "v" } })).rows.map((r) => r.key)).toEqual(["c", "b", "a", "d", "e", "f"]);
    expect((await model.list({ sort: { by: "v", dir: "desc" } })).rows.map((r) => r.key)).toEqual(["d", "a", "b", "c", "e", "f"]);
    await h.closeAll();
  });

  it("a list watcher whose filter throws on a committed state ends alone; the part is never folded again and no key fails", async () => {
    await h.reset();
    computedThrows = true;
    const api = h.open();
    const model = api.readModel(computedModel);
    await model.get("healthy");
    const errors: unknown[] = [];
    const throwing = await model.watchList({ where: { status: "open" } }, () => undefined, { onError: (e) => void errors.push(e) });
    const others: ListChange<Computed>[] = [];
    await model.watchList({ where: { broken: false } }, (c) => void others.push(c));
    const states: Computed[] = [];
    await model.watch("healthy", (s) => void states.push(s));
    const a = await api.append([made("healthy", 1, "open"), made("computed", 1, "broken")]);
    expect((await model.get("healthy", { atLeast: tokenOf(a) })).count).toBe(1);
    expect(await countOnRead(api, "healthy")).toBe(1);
    expect((await model.get("computed")).count).toBe(1); // a fold that never threw: its key is served
    expect(await countOnRead(api, "computed")).toBe(1);
    expect(model.status).toMatchObject({ phase: "live", failed: 0, folded: 2 });
    await until(() => throwing.stopped && errors.length === 1);
    expect((errors[0] as Error).message).toBe("computed status unavailable");
    // the other watchers go on
    const b = await api.append([made("healthy", 1, "open")]);
    expect((await model.get("healthy", { atLeast: tokenOf(b) })).count).toBe(2);
    await until(() => states.length === 3 && others.length === 2);
    expect(states.map((s) => s.count)).toEqual([0, 1, 2]);
    expect(others.map((c) => [c.rows.map((r) => [r.key, r.state.count]), c.removed])).toEqual([
      [[["healthy", 1]], []],
      [[["healthy", 2]], []],
    ]);
    expect(errors).toHaveLength(1);
    expect(model.status).toMatchObject({ failed: 0, folded: 3 });
    await h.closeAll();
  });

  it("a poison event on a key whose last state throws in a list watcher's filter: the key fails, the hook runs once, the model goes on", async () => {
    await h.reset();
    computedThrows = false;
    try {
      const api = h.open();
      const hooked: FailedKeyError[] = [];
      const model = api.readModel(defineReadModel({ ...computedModel, name: "computed-poison", onFailedKey: (e) => void hooked.push(e) }));
      const a = await api.append([made("computed", 1, "broken"), made("healthy", 1, "open")]);
      await model.get("healthy", { atLeast: tokenOf(a) });
      const errors: unknown[] = [];
      const throwing = await model.watchList({ where: { status: "open" } }, () => undefined, { onError: (e) => void errors.push(e) });
      const all: ListChange<Computed>[] = [];
      await model.watchList({}, (c) => void all.push(c));
      computedThrows = true; // from now on, "computed"'s state throws when a filter reads its status
      const b = await api.append([made("healthy", 1, "open"), made("computed", 1, "open", true), made("healthy", 1, "open")]);
      expect((await model.get("healthy", { atLeast: tokenOf(b), timeoutMs: 5_000 })).count).toBe(3);
      const c = await api.append([made("healthy", 1, "open")]);
      expect((await model.get("healthy", { atLeast: tokenOf(c), timeoutMs: 5_000 })).count).toBe(4);
      expect(await countOnRead(api, "healthy")).toBe(4);
      const failed = await model.get("computed").catch((e: unknown) => e);
      expect(failed).toBeInstanceOf(FailedKeyError);
      expect((failed as FailedKeyError).event.transactionId).toBe(b.transactionId);
      expect(hooked).toHaveLength(1);
      expect(hooked[0]).toBe(failed);
      expect(model.status).toMatchObject({ phase: "live", failed: 1, folded: 5 });
      await until(() => throwing.stopped && errors.length === 1 && all.some((ch) => ch.removed.includes("computed")));
      expect((errors[0] as Error).message).toBe("computed status unavailable");
      await until(() => all.flatMap((ch) => ch.rows).filter((r) => r.key === "healthy").length === 3);
    } finally {
      computedThrows = true;
    }
    await h.closeAll();
  });

  it("a list kept current the README way — watch first, hold, page, newer only — misses no change committed around the page", async () => {
    await h.reset();
    const api = h.open();
    await api.append([made("o1", 1, "open"), made("o2", 1, "won")]);
    const model = api.readModel(offersModel);
    await model.get("o1");
    const where = { status: "open" };
    const frontend = new Map<string, number>();
    const merge = (change: ListChange<Offer>) => {
      for (const row of change.rows) frontend.set(row.key, row.state.total);
      for (const key of change.removed) frontend.delete(key);
    };
    let pageRevision: number | undefined;
    const early: ListChange<Offer>[] = [];
    const pushed: number[] = [];
    const feed = await model.watchList({ where }, (change) => {
      if (pageRevision === undefined) early.push(change);
      else if (change.revision > pageRevision) {
        pushed.push(change.revision);
        merge(change);
      }
    });
    // a commit between the registration and the page: listed first, this change was lost (R3.3)
    await model.get("o1", { atLeast: tokenOf(await api.append([made("o1", 2, "open"), made("o2", 1, "open")])) });
    await until(() => early.length === 1);
    const first = await model.list({ where, limit: 1000 });
    expect(feed.stopped).toBe(false);
    pageRevision = first.revision;
    for (const row of first.rows) frontend.set(row.key, row.state.total);
    expect(early[0]!.revision).toBeLessThanOrEqual(first.revision); // held, and already in the page
    for (const change of early) if (change.revision > pageRevision) merge(change);
    expect([...frontend].sort()).toEqual([["o1", 3], ["o2", 2]]);
    // commits after the page
    await model.get("o1", { atLeast: tokenOf(await api.append([made("o3", 5, "open"), made("o1", 1, "won")])) });
    await until(() => pushed.length === 1);
    const now = await model.list({ where, limit: 1000 });
    expect([...frontend].sort()).toEqual(now.rows.map((r) => [r.key, r.state.total]).sort());
    expect([...frontend].sort()).toEqual([["o2", 2], ["o3", 5]]);
    for (const [key, total] of frontend) expect(total).toBe(((await onRead(api, key)) as { state: Offer }).state.total);
    feed.stop();

    // a watch that ends while the page is read (it fell behind): stopped says so, onError said it once
    const slow = h.open({ watch: { maxPendingBatches: 1 } }).readModel(offersModel);
    await slow.get("o1");
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const errors: unknown[] = [];
    const held = await slow.watchList({}, () => gate, { onError: (e) => void errors.push(e) });
    for (let i = 0; i < 3; i++) await slow.get("o4", { atLeast: tokenOf(await api.append([made("o4", 1)])) });
    await slow.list({});
    expect(held.stopped).toBe(true);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(WatchOverflowError);
    release();
    await h.closeAll();
  });

  it("poison: the fold throws on one event in a 500-event page; only its key fails, every other key equals fold on read", async () => {
    for (const at of [0, 250, 499]) {
      await h.reset();
      const api = h.open();
      const failedKeys: FailedKeyError[] = [];
      const def = defineReadModel({ ...offersModel, name: `poison-${at}`, onFailedKey: (e) => void failedKeys.push(e) });
      const page = Array.from({ length: 500 }, (_, i) => (i === at ? made("bad", 987_654_321, "secret-status", true) : made(`k${i % 20}`, i)));
      const appended = await api.append(page);
      const later = await api.append([made("bad", 1), made("k1", 1000)]);
      const model = api.readModel(def);
      await model.get("k1", { atLeast: tokenOf(later) });
      const error = await model.get("bad").catch((e: unknown) => e);
      expect(error).toBeInstanceOf(FailedKeyError);
      const failed = error as FailedKeyError;
      const events = (await api.query(keyQuery(source, "offerId", "bad"))).events as RecordedEvent[];
      const poison = events[0]!;
      expect(failed.key).toBe("bad");
      expect(failed.model).toEqual({ name: `poison-${at}`, version: 1 });
      expect(failed.event).toEqual({ type: "OfferMade", id: poison.id, sequence: poison.sequence, transactionId: poison.transactionId });
      expect(poison.transactionId).toBe(appended.transactionId);
      expect((failed.cause as Error).message).toBe(`poison event #${poison.sequence}`);
      // no payload value in the message or the event record
      expect(failed.message).not.toContain("987654321");
      expect(failed.message).not.toContain("secret-status");
      expect(JSON.stringify(failed.event)).not.toContain("secret-status");
      expect(failed.httpStatus).toBe(500);
      expect((await onRead(api, "bad")).ok).toBe(false);
      for (let i = 0; i < 20; i++) expect(await model.get(`k${i}`)).toEqual((await onRead(api, `k${i}`) as { state: Offer }).state);
      expect(model.status.folded).toBe(500 - 1 + 1); // 499 healthy + k1's later event; the later "bad" event is passed over
      expect(model.status.failed).toBe(1);
      expect(failedKeys).toHaveLength(1);
      expect(failedKeys[0]).toBe(failed);
      const listed = await model.list({ limit: 100 });
      expect(listed.failed).toBe(1);
      expect(listed.rows.map((r) => r.key)).not.toContain("bad");
      await h.closeAll();
    }
  });
}

/** Deep equality the way `toEqual` sees it, for counting mismatches without throwing. */
function isEqual(a: unknown, b: unknown): boolean {
  try {
    expect(a).toEqual(b);
    return true;
  } catch {
    return false;
  }
}
