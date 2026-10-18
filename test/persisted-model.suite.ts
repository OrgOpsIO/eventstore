/**
 * The persisted read model, specified once and run against the memory store and Postgres
 * (persisted-model-memory.test.ts, persisted-model-postgres.test.ts). A "process" is a fresh
 * runtime over the same store data: its handles compete for the writer lock, and the rows and the
 * checkpoint survive it.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import {
  compareCursor,
  defineReadModel,
  FailedKeyError,
  TransientError,
  UsageError,
  type Cursor,
  type EventStoreApi,
  type EventStoreConfig,
  type Fold,
  type NewEvent,
  type PersistedReadModel,
  type ReadModelDefinition,
} from "../src/index.js";
import type { ReadModelStore, WriterKnobs } from "../src/modelstore.js";
import { keyQuery } from "../src/readmodel.js";
import { audited, broken, initialOffer, itemFold, itemsModel, made, noted, offerFold, offersModel, pmConfig, set, source, tokenOf, type Item, type Offer } from "./persisted-model.model.js";

export * from "./persisted-model.model.js";

export interface PersistedHarness {
  /** Empty events and model tables for the next test. */
  reset(): Promise<void>;
  /** A new "process": a runtime over the same data (`persistedModels: true`). Closed by `closeAll`. */
  open(options?: Partial<EventStoreConfig>): EventStoreApi;
  /** Concurrent appenders with connections of their own (Postgres: separate pools). */
  readonly writers: number;
  closeAll(): Promise<void>;
}

const KEYS = ["o1", "o2", "o3", "o4", "o5", "o6"];

export const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
export const until = async (check: () => boolean | Promise<boolean>, ms = 5_000) => {
  const end = Date.now() + ms;
  while (!(await check()) && Date.now() < end) await wait(10);
  expect(await check()).toBe(true);
};

/** The store's capability (internal) of a runtime. */
export async function capOf(api: EventStoreApi): Promise<ReadModelStore> {
  const cap = ((await api.store()) as { readModelStore?: ReadModelStore }).readModelStore;
  if (!cap) throw new Error("no readModelStore on this store");
  return cap;
}

/** The writer knobs of a runtime's store (memory: shared by every runtime over the one store). */
export async function knobsOf(api: EventStoreApi): Promise<WriterKnobs> {
  return (await capOf(api)).knobs;
}

/** Take a model's writer lock in a runtime of its own, so no handle elsewhere can write it. */
export async function block<S>(api: EventStoreApi, def: ReadModelDefinition<S>): Promise<() => Promise<void>> {
  const cap = await capOf(api);
  const owner = {};
  const detach = cap.attach(owner);
  const epoch = await cap.lock({ name: def.name, version: def.version }, owner, null);
  expect(epoch).not.toBeNull();
  return async () => {
    await cap.unlock({ name: def.name, version: def.version }, owner);
    detach();
  };
}

/** Fold on read for one key of the offers model: the state, or the error it throws. */
export async function onRead<S = Offer>(api: EventStoreApi, key: string, def: ReadModelDefinition<S> = offersModel as unknown as ReadModelDefinition<S>): Promise<{ ok: true; state: S } | { ok: false; error: unknown }> {
  try {
    return { ok: true, state: (await api.context({ query: keyQuery(def.source, def.keyBy, key), fold: def.fold, initial: def.initial })).state };
  } catch (error) {
    return { ok: false, error };
  }
}

async function fromModel<S>(model: PersistedReadModel<S>, key: string): Promise<{ ok: true; state: S } | { ok: false; error: unknown }> {
  try {
    return { ok: true, state: await model.get(key) };
  } catch (error) {
    return { ok: false, error };
  }
}

/** The source events with a key, counted: each must be applied exactly once (n counts OfferMade, notes OfferNoted). */
async function sourceEventsWithKey(api: EventStoreApi): Promise<number> {
  const all = await api.query(source);
  return all.events.filter((e) => typeof e.scopes.offerId === "string").length;
}

function appliedCount(rows: readonly { state: Offer }[]): number {
  return rows.reduce((n, r) => n + r.state.n + r.state.notes, 0);
}

export function persistedModelSuite(h: PersistedHarness, options: { readonly numRuns: number }): void {
  it("property: get(k) equals context(source ∧ offerId = k, fold) for every key without waiting, also while the writer lags; list({ atLeast }) holds exactly the held keys", async () => {
    const event = fc.oneof(
      { weight: 6, arbitrary: fc.record({ kind: fc.constant("made" as const), key: fc.constantFrom(...KEYS), amount: fc.integer({ min: -20, max: 50 }), status: fc.constantFrom("open", "won", "lost") }) },
      { weight: 1, arbitrary: fc.record({ kind: fc.constant("noted" as const), key: fc.constantFrom(...KEYS) }) },
      { weight: 1, arbitrary: fc.constant({ kind: "noted-no-key" as const }) },
      { weight: 2, arbitrary: fc.record({ kind: fc.constant("audited" as const), key: fc.constantFrom(...KEYS) }) },
    );
    const step = fc.array(fc.array(event, { minLength: 1, maxLength: 4 }), { minLength: 1, maxLength: Math.max(1, h.writers) });
    let mismatches = 0;
    let comparisons = 0;
    let lagging = 0;
    let bothThrew = 0;
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(1, 3, 500), fc.array(step, { minLength: 1, maxLength: 6 }), fc.option(fc.nat({ max: 12 }), { nil: undefined }), async (batchSize, steps, poisonAt) => {
        await h.reset();
        const writers = Array.from({ length: Math.max(1, h.writers) }, () => h.open());
        const host = h.open();
        const other = h.open();
        const def = defineReadModel({ ...offersModel, name: `prop-${batchSize}`, batchSize });
        const model = host.persistedReadModel(def);
        const standby = other.persistedReadModel(def);
        let madeCount = 0;
        let last: Cursor | undefined;
        const toEvent = (e: { kind: string; key?: string; amount?: number; status?: string }): NewEvent => {
          if (e.kind === "made") return made(e.key!, e.amount!, e.status, madeCount++ === poisonAt);
          if (e.kind === "noted") return noted(e.key);
          if (e.kind === "noted-no-key") return noted();
          return audited(e.key!);
        };
        for (const appends of steps) {
          const results = await Promise.all(appends.map((events, i) => writers[i % writers.length]!.append(events.map(toEvent))));
          for (const r of results) if (!last || compareCursor(tokenOf(r), last) > 0) last = tokenOf(r);
          const checkpoint = (await model.status()).checkpoint;
          if (checkpoint === null || compareCursor(checkpoint, last!) < 0) lagging++;
          // no wait at all: get is current by row + delta, whatever the writer's lag
          for (const key of [...KEYS, "never"]) {
            const expected = await onRead(host, key);
            for (const handle of [model, standby]) {
              const got = await fromModel(handle, key);
              comparisons++;
              if (got.ok !== expected.ok) mismatches++;
              else if (got.ok && expected.ok) {
                try {
                  expect(got.state).toEqual(expected.state);
                } catch {
                  mismatches++;
                }
              } else bothThrew++;
            }
          }
        }
        // after the writer passed the last token: list holds exactly the held keys, with the states fold on read gives
        const listed = await model.list({ atLeast: last!, limit: 1000 });
        const held: string[] = [];
        for (const key of KEYS) {
          const all = await host.query(keyQuery(source, "offerId", key));
          const r = await onRead(host, key);
          if (all.events.length > 0 && r.ok) held.push(key);
        }
        expect(listed.rows.map((row) => row.key)).toEqual(held);
        for (const row of listed.rows) expect(row.state).toEqual(((await onRead(host, row.key)) as { state: Offer }).state);
        await h.closeAll();
      }),
      { numRuns: options.numRuns },
    );
    console.log(`persisted model property: ${comparisons} comparisons, ${mismatches} mismatches, ${lagging} steps compared with the checkpoint behind the last append, ${bothThrew} reads where both threw`);
    expect(mismatches).toBe(0);
    expect(comparisons).toBeGreaterThan(0);
    expect(lagging).toBeGreaterThan(0);
    expect(bothThrew).toBeGreaterThan(0);
  }, 60_000); // numRuns × (reset + up to 6 steps): 8.7 s on Postgres at a load average of 46, over the 5 s default

  it("a warm get folds 0 events and a list folds none; after one append the next get folds exactly 1, the one after 0", async () => {
    await h.reset();
    const writer = h.open();
    const reader = h.open();
    await writer.append([made("o1", 1), made("o2", 2), noted("o1"), audited("o1")]);
    const w = writer.persistedReadModel(offersModel);
    const last = await writer.append([made("o3", 3)]);
    await w.list({ atLeast: tokenOf(last) });
    const r = reader.persistedReadModel(offersModel);
    for (const k of KEYS) await r.get(k);
    const before = (await r.status()).readFolded;
    for (let i = 0; i < 100; i++) await r.get(KEYS[i % KEYS.length]!);
    for (let i = 0; i < 10; i++) await r.list({ sort: { by: "total" } });
    expect((await r.status()).readFolded).toBe(before);
    expect((await r.status()).role).toBe("standby");
    await w.stop(); // the checkpoint stays behind from here on (the reader's tick is long)
    (await knobsOf(reader)).tickMs = 60_000;
    await writer.append([made("o1", 5)]);
    expect((await r.get("o1")).total).toBe(6);
    expect((await r.status()).readFolded).toBe(before + 1);
    expect((await r.get("o1")).total).toBe(6);
    expect((await r.status()).readFolded).toBe(before + 1);
    expect((await onRead(reader, "o1")) as { state: Offer }).toMatchObject({ state: { total: 6 } });
    await h.closeAll();
  });

  it("restart: a new writer resumes at the checkpoint and folds exactly the events in between, each once; a fresh process's first get folds only the key's events beyond its row", async () => {
    await h.reset();
    const a = h.open();
    for (let i = 0; i < 100; i++) await a.append([made(KEYS[i % 6]!, i), noted(KEYS[(i + 1) % 6]), made(KEYS[(i + 2) % 6]!, 1)]);
    const first = a.persistedReadModel(offersModel);
    const lastA = (await a.query(source, { order: "desc", limit: 1 })).events[0]!;
    await first.list({ atLeast: { transactionId: lastA.transactionId, sequence: lastA.sequence } });
    const sa = await first.status();
    expect(sa).toMatchObject({ role: "writer", folded: 300, readFolded: 0 });
    await first.stop();
    let lastB: Cursor | undefined;
    for (let i = 0; i < 30; i++) lastB = tokenOf(await a.append([made(KEYS[i % 6]!, 2), noted(KEYS[i % 6]), audited(KEYS[i % 6]!), made(KEYS[(i + 3) % 6]!, 3)]));
    const b = h.open();
    const second = b.persistedReadModel(offersModel);
    await second.list({ atLeast: lastB! });
    const sb = await second.status();
    expect(sb.role).toBe("writer");
    expect(sb.folded).toBe(90); // the 90 source events in between, nothing before them
    expect(sb.epoch).toBe(sa.epoch + 1);
    expect(sb.checkpoint).toEqual(lastB);
    expect(sb.lag).toEqual({ events: 0, ms: 0 });
    for (const k of KEYS) expect(await second.get(k)).toEqual(((await onRead(b, k)) as { state: Offer }).state);
    const listed = await second.list({ limit: 100 });
    expect(appliedCount(listed.rows)).toBe(await sourceEventsWithKey(b)); // each applied exactly once
    await second.stop();
    for (let i = 0; i < 7; i++) await b.append([made("o1", 1)]);
    const blocker = h.open();
    const release = await block(blocker, offersModel);
    const c = h.open();
    const third = c.persistedReadModel(offersModel);
    expect((await third.get("o1")).total).toBe(((await onRead(c, "o1")) as { state: Offer }).state.total);
    expect((await third.status()).readFolded).toBe(7); // the key's events beyond its row, not its history
    expect((await third.get("o2")).total).toBe(((await onRead(c, "o2")) as { state: Offer }).state.total);
    expect((await third.status()).readFolded).toBe(7);
    expect((await third.status()).role).toBe("standby");
    await release();
    await h.closeAll();
  });

  for (const position of ["start", "middle", "end"] as const) {
    it(`poison at the ${position} of a 500-event page: that key carries the marker, its neighbours and later pages are applied; get throws FailedKeyError here and in a fresh process`, async () => {
      await h.reset();
      const failures: FailedKeyError[] = [];
      const def = defineReadModel({ ...offersModel, name: `poison-${position}`, batchSize: 500, onFailedKey: (e) => void failures.push(e) });
      const a = h.open();
      const at = position === "start" ? 0 : position === "middle" ? 250 : 499;
      const events = Array.from({ length: 500 }, (_, i) => (i === at ? made("px", 1, "open", true) : made(`k${i % 50}`, 1)));
      const page = await a.append(events);
      const model = a.persistedReadModel(def);
      const listed = await model.list({ atLeast: tokenOf(page), limit: 100 });
      expect(listed.failed).toBe(1);
      expect(listed.total).toBe(50);
      expect(listed.rows.map((r) => r.key)).not.toContain("px");
      expect((await model.status()).failed).toBe(1);
      expect(appliedCount(listed.rows)).toBe(499);
      const poisoned = (await a.query(source)).events[at]!;
      for (const api of [a, h.open()]) {
        const handle = api.persistedReadModel(def);
        const error = await handle.get("px").catch((e: unknown) => e);
        expect(error).toBeInstanceOf(FailedKeyError);
        expect(error).toMatchObject({ key: "px", model: { name: def.name, version: 1 }, event: { type: "OfferMade", id: poisoned.id, sequence: poisoned.sequence, transactionId: poisoned.transactionId } });
        expect(((error as FailedKeyError).cause as Error).message).toBe(`poison event #${poisoned.sequence}`);
        expect((await onRead(api, "px")).ok).toBe(false);
      }
      expect(failures).toHaveLength(1);
      expect(failures[0]!.key).toBe("px");
      // a restarted writer passes the key's later events over and applies the rest
      await model.stop();
      const b = h.open();
      const after = await b.append([made("px", 5), made("k1", 100)]);
      const again = b.persistedReadModel(def);
      const later = await again.list({ atLeast: tokenOf(after), limit: 100 });
      expect(later.failed).toBe(1);
      expect(later.rows.find((r) => r.key === "k1")!.state).toEqual(((await onRead(b, "k1")) as { state: Offer }).state);
      await expect(again.get("px")).rejects.toBeInstanceOf(FailedKeyError);
      expect(failures).toHaveLength(1);
      await h.closeAll();
    });
  }

  it("a fold result that cannot be stored fails that key like a poison event; the cause names the refusal", async () => {
    await h.reset();
    const api = h.open();
    await api.append([set("i1", "a", "num", 1), set("i2", "a", "num", 2)]);
    const last = await api.append([broken("i1"), set("i2", "b", "str", "x")]);
    const model = api.persistedReadModel(itemsModel);
    const listed = await model.list({ atLeast: tokenOf(last) });
    expect(listed.rows.map((r) => [r.key, r.state])).toEqual([["i2", { a: 2, b: "x" }]]);
    expect(listed.failed).toBe(1);
    // no copy of i1 in either process yet, so both read the row and its marker
    for (const handle of [model, h.open().persistedReadModel(itemsModel)]) {
      const error = await handle.get("i1").catch((e: unknown) => e);
      expect(error).toBeInstanceOf(FailedKeyError);
      expect(error).toMatchObject({ key: "i1", event: { type: "ItemBroken" } });
      expect(((error as FailedKeyError).cause as Error).message).toMatch(/cannot be persisted — state\.broken is a class instance \(NotStorable\)/);
    }
    await h.closeAll();
  });

  it("one writer: of two handles in two processes exactly one writes; stop() it and the other takes over within ticks and resumes at the checkpoint", async () => {
    await h.reset();
    const a = h.open();
    const b = h.open();
    (await knobsOf(a)).tickMs = 100;
    (await knobsOf(b)).tickMs = 100;
    await a.append([made("o1", 1)]);
    const ma = a.persistedReadModel(offersModel);
    const mb = b.persistedReadModel(offersModel);
    await until(async () => [(await ma.status()).role, (await mb.status()).role].filter((r) => r === "writer").length === 1);
    expect([(await ma.status()).role, (await mb.status()).role].sort()).toEqual(["standby", "writer"]);
    const [writer, standby, standbyApi] = (await ma.status()).role === "writer" ? [ma, mb, b] : [mb, ma, a];
    const t1 = await a.append([made("o2", 2)]);
    await standby.list({ atLeast: tokenOf(t1) }); // a standby's list waits for the writer in another process
    const epoch = (await writer.status()).epoch;
    const stopped = Date.now();
    await writer.stop();
    await until(async () => (await standby.status()).role === "writer", 3_000);
    const took = Date.now() - stopped;
    expect(took).toBeLessThan(1_000);
    const t2 = await standbyApi.append([made("o1", 10), noted("o2")]);
    const listed = await standby.list({ atLeast: tokenOf(t2) });
    expect((await standby.status()).epoch).toBe(epoch + 1);
    for (const k of ["o1", "o2"]) expect(await standby.get(k)).toEqual(((await onRead(standbyApi, k)) as { state: Offer }).state);
    expect(appliedCount(listed.rows)).toBe(await sourceEventsWithKey(standbyApi));
    console.log(`takeover after stop(): ${took} ms (tick 100 ms)`);
    await h.closeAll();
  });

  it("list: two-field where, null, asc/desc with ties by key, missing values last both ways, mixed kinds by rank, pages, total; invalid options are refused", async () => {
    await h.reset();
    const api = h.open();
    await api.append([
      set("a", "n", "num", 3), set("a", "s", "str", "x"), set("a", "t", "str", "p"),
      set("b", "n", "num", 1), set("b", "s", "str", "x"), set("b", "t", "null"),
      set("c", "n", "num", 3), set("c", "s", "str", "y"),
      set("d", "n", "str", "zz"), set("d", "s", "str", "x"),
      set("e", "n", "bool", true),
      set("f", "n", "date", 1_000),
      set("g", "s", "str", "x"),
      set("h", "n", "num", 1.5),
    ]);
    const last = await api.append([set("i", "n", "bool", false)]);
    const model = api.persistedReadModel(itemsModel);
    const keys = async (o: Parameters<typeof model.list>[0]) => (await model.list({ atLeast: tokenOf(last), ...o })).rows.map((r) => r.key);
    expect(await keys({})).toEqual(["a", "b", "c", "d", "e", "f", "g", "h", "i"]);
    expect(await keys({ where: { s: "x", n: 3 } })).toEqual(["a"]);
    expect(await keys({ where: { t: null } })).toEqual(["b"]);
    expect(await keys({ sort: { by: "n" } })).toEqual(["b", "h", "a", "c", "d", "i", "e", "f", "g"]);
    expect(await keys({ sort: { by: "n", dir: "desc" } })).toEqual(["f", "e", "i", "d", "a", "c", "h", "b", "g"]);
    const page1 = await model.list({ sort: { by: "n" }, limit: 4 });
    const page2 = await model.list({ sort: { by: "n" }, limit: 4, offset: 4 });
    const page3 = await model.list({ sort: { by: "n" }, limit: 4, offset: 8 });
    expect([...page1.rows, ...page2.rows, ...page3.rows].map((r) => r.key)).toEqual(["b", "h", "a", "c", "d", "i", "e", "f", "g"]);
    expect([page1.total, page2.total, page3.total]).toEqual([9, 9, 9]);
    expect((await model.list({ offset: 100 })).rows).toEqual([]);
    expect((await model.list({ offset: 100 })).total).toBe(9);
    expect((await model.list({ where: { s: "x" } })).total).toBe(4);
    expect((await model.list({ where: { s: "x" } })).revision).toBeGreaterThan(0);
    for (const bad of [{ where: [] }, { where: { n: Number.POSITIVE_INFINITY } }, { where: { n: {} } }, { sort: { by: "" } }, { sort: { by: "n", dir: "up" } }, { offset: -1 }, { limit: 0 }, { timeoutMs: 0 }, { atLeast: { sequence: 1 } }]) {
      await expect(model.list(bad as never)).rejects.toThrow(UsageError);
    }
    await h.closeAll();
  });

  it("list doc rules: 1 and \"1\" kept apart, a Date sorts after booleans, Infinity, NaN, arrays and objects count as missing, -0 is 0, astral before U+E000 is wrong: code point order", async () => {
    await h.reset();
    const api = h.open();
    await api.append([
      set("num", "v", "num", 1), set("str", "v", "str", "1"),
      set("inf", "v", "inf"), set("nan", "v", "nan"), set("arr", "v", "arr", 1), set("obj", "v", "obj", 1),
      set("date", "v", "date", 5), set("bool", "v", "bool", true), set("zero", "v", "neg0"),
      set("nul", "v", "nul", "a"), set("lone", "v", "lone", "a"), // strings Postgres' jsonb cannot hold: missing
      set("bmp", "w", "str", ""), set("astral", "w", "str", "\u{1F600}"), set("ascii", "w", "str", "z"),
    ]);
    const last = await api.append([set("plain", "v", "str", "a")]);
    const model = api.persistedReadModel(itemsModel);
    await model.list({ atLeast: tokenOf(last) });
    const keys = async (o: Parameters<typeof model.list>[0]) => (await model.list(o)).rows.map((r) => r.key);
    expect(await keys({ where: { v: 1 } })).toEqual(["num"]);
    expect(await keys({ where: { v: "1" } })).toEqual(["str"]);
    expect(await keys({ where: { v: 0 } })).toEqual(["zero"]);
    expect(await keys({ sort: { by: "v" }, limit: 100 })).toEqual(["zero", "num", "str", "plain", "bool", "date", "arr", "ascii", "astral", "bmp", "inf", "lone", "nan", "nul", "obj"]);
    expect(await keys({ sort: { by: "v", dir: "desc" }, limit: 100 })).toEqual(["date", "bool", "plain", "str", "num", "zero", "arr", "ascii", "astral", "bmp", "inf", "lone", "nan", "nul", "obj"]);
    // code point: z < U+E000 < U+1F600 (UTF-16 code units would put the astral one before U+E000)
    expect(await keys({ sort: { by: "w" }, where: {}, limit: 100 })).toEqual(["ascii", "bmp", "astral", "arr", "bool", "date", "inf", "lone", "nan", "nul", "num", "obj", "plain", "str", "zero"]);
    expect((await model.list({ limit: 100 })).rows.find((r) => r.key === "nul")!.state.v).toBe("a\u0000x"); // stored exactly, only left out of the doc
    // the state list returns is the exact stored one, not the doc
    const inf = (await model.list({ where: {}, limit: 100 })).rows.find((r) => r.key === "inf")!.state;
    expect(inf.v).toBe(Number.POSITIVE_INFINITY);
    expect((await model.list({ limit: 100 })).rows.find((r) => r.key === "date")!.state.v).toEqual(new Date(5));
    await h.closeAll();
  });

  it("atLeast: a matching token is included, a non-matching one passes by the probe, one that never comes times out, a malformed one is refused", async () => {
    await h.reset();
    const api = h.open();
    const model = api.persistedReadModel(offersModel);
    const a = await api.append([made("o1", 7)]);
    expect((await model.list({ atLeast: tokenOf(a) })).rows.map((r) => r.state.total)).toEqual([7]);
    const b = await api.append([audited("o1")]); // outside the source: passed by the checkpoint or the probe's watermark
    expect((await model.list({ atLeast: tokenOf(b), timeoutMs: 5_000 })).total).toBe(1);
    const future = { transactionId: String(BigInt(a.transactionId) + 1_000_000n), sequence: a.last + 1_000_000 };
    const started = Date.now();
    const timedOut = await model.list({ atLeast: future, timeoutMs: 200 }).catch((e: unknown) => e);
    expect(timedOut).toBeInstanceOf(TransientError);
    expect((timedOut as TransientError).code).toBe("read_model_timeout");
    expect((timedOut as Error).message).toContain('persisted read model "offers" v1 has not passed token');
    expect(Date.now() - started).toBeLessThan(2_000);
    await expect(model.list({ atLeast: { transactionId: "x", sequence: 1 } })).rejects.toThrow(/atLeast needs a Cursor/);
    await h.closeAll();
  });

  it("lag: with no writer N events appended show as lag in events and ms; caught up both are 0; no model row yet → checkpoint null", async () => {
    await h.reset();
    const blocker = h.open();
    const def = defineReadModel({ ...offersModel, name: "lag" });
    const noneDef = defineReadModel({ ...offersModel, name: "lag-none" });
    const release = await block(blocker, def);
    const releaseNone = await block(blocker, noneDef);
    const api = h.open();
    const fresh = api.persistedReadModel(noneDef);
    for (let i = 0; i < 12; i++) await api.append([made(KEYS[i % 6]!, 1)]);
    const model = api.persistedReadModel(def);
    await wait(15);
    const lagging = await model.status();
    expect(lagging.role).toBe("standby");
    expect(lagging.checkpoint).toBeNull();
    expect(lagging.lag.events).toBeGreaterThanOrEqual(12);
    expect(lagging.lag.ms).toBeGreaterThan(0);
    const none = await fresh.status();
    expect(none).toMatchObject({ role: "standby", checkpoint: null, revision: 0, failed: 0, lag: { events: 12 } });
    await releaseNone();
    await release();
    (await knobsOf(api)).tickMs = 50;
    await until(async () => (await model.status()).role === "writer");
    const last = (await api.query(source, { order: "desc", limit: 1 })).events[0]!;
    await model.list({ atLeast: { transactionId: last.transactionId, sequence: last.sequence } });
    expect((await model.status()).lag).toEqual({ events: 0, ms: 0 });
    await h.closeAll();
  });

  it("placement: one handle per (tenant, name, version); another definition object is refused; a past view refuses; a live and a persisted model of one definition answer the same", async () => {
    await h.reset();
    const api = h.open();
    const model = api.persistedReadModel(offersModel);
    expect(api.persistedReadModel(offersModel)).toBe(model);
    expect(() => api.persistedReadModel(defineReadModel({ ...offersModel }))).toThrow(/two definitions of persisted read model "offers" v1/);
    expect(() => api.asOf(1).persistedReadModel(offersModel)).toThrow(/a past view \(asOf\) has no persisted read model/);
    const live = api.readModel(offersModel);
    expect(live).not.toBe(model);
    const t = await api.append([made("o1", 2), made("o2", 3), noted("o1")]);
    expect(await model.get("o1")).toEqual(await live.get("o1", { atLeast: tokenOf(t) }));
    expect((await model.list({ atLeast: tokenOf(t) })).rows).toEqual((await live.list()).rows);
    await model.stop();
    await expect(model.get("o1")).rejects.toThrow(/persisted read model "offers" v1 stopped/);
    expect((await model.status()).phase).toBe("stopped");
    const again = api.persistedReadModel(offersModel);
    expect(again).not.toBe(model);
    expect((await again.get("o2")).total).toBe(3);
    await api.close();
    expect((await again.status()).phase).toBe("stopped");
    expect(() => api.persistedReadModel(offersModel)).toThrow(/closed/);
    await h.closeAll();
  });

  it("watch: first the stored state (or initial), then one delivery per page that changed the key; 100 watchers get the same object; a failed key ends its watcher", async () => {
    await h.reset();
    const api = h.open();
    await api.append([made("o1", 1)]);
    const model = api.persistedReadModel(offersModel);
    const t0 = await api.append([made("o2", 1)]);
    await model.list({ atLeast: tokenOf(t0) });
    const seen: Offer[] = [];
    const fresh: Offer[] = [];
    await model.watch("o1", (s) => void seen.push(s));
    await model.watch("o9", (s) => void fresh.push(s));
    expect(seen.map((s) => s.total)).toEqual([1]);
    expect(fresh).toEqual([initialOffer]);
    const many: Offer[][] = Array.from({ length: 100 }, () => []);
    await Promise.all(many.map((list) => model.watch("o1", (s) => void list.push(s))));
    // one page each (the writer passes every token before the next append)
    await model.list({ atLeast: tokenOf(await api.append([made("o1", 10)])) });
    await model.list({ atLeast: tokenOf(await api.append([audited("o1"), made("o2", 1)])) }); // nothing for o1 in the source
    await model.list({ atLeast: tokenOf(await api.append([made("o1", 100)])) });
    await until(() => seen.length === 3 && many.every((l) => l.length === 3));
    expect(seen.map((s) => s.total)).toEqual([1, 11, 111]);
    for (const list of many) {
      expect(list[1]).toBe(many[0]![1]); // decoded once, the same object for every watcher
      expect(list[2]).toBe(many[0]![2]);
    }
    const ended: unknown[] = [];
    await model.watch("o1", () => undefined, { onError: (e) => void ended.push(e) });
    const tp = await api.append([made("o1", 1, "open", true)]);
    await model.list({ atLeast: tokenOf(tp) });
    await until(() => ended.length === 1);
    expect(ended[0]).toBeInstanceOf(FailedKeyError);
    await expect(model.watch("o1", () => undefined)).rejects.toBeInstanceOf(FailedKeyError);
    await h.closeAll();
  });

  it("watchList: rows, removed and revision per page; the handoff (register, hold, list, apply newer revisions) gives the fresh list", async () => {
    await h.reset();
    const api = h.open();
    const model = api.persistedReadModel(offersModel);
    const t0 = await api.append([made("o1", 1, "open"), made("o2", 2, "open"), made("o3", 3, "won")]);
    await model.list({ atLeast: tokenOf(t0) });
    const held: { revision: number; rows: string[]; removed: string[] }[] = [];
    await model.watchList({ where: { status: "open" } }, (c) => void held.push({ revision: c.revision, rows: c.rows.map((r) => r.key), removed: [...c.removed] }));
    const page = await model.list({ where: { status: "open" } });
    const t1 = await api.append([made("o1", 5, "won")]);
    await model.list({ atLeast: tokenOf(t1) });
    const t2 = await api.append([made("o4", 1, "open"), made("o3", 1, "open")]);
    await model.list({ atLeast: tokenOf(t2) });
    const t3 = await api.append([made("o9", 1, "won")]); // never matched: no delivery
    await model.list({ atLeast: tokenOf(t3) });
    await until(() => held.length === 2);
    await wait(100);
    expect(held).toEqual([
      { revision: page.revision + 1, rows: [], removed: ["o1"] },
      { revision: page.revision + 2, rows: ["o3", "o4"], removed: [] },
    ]);
    // the handoff: the page plus the newer changes equals a fresh list
    const keys = new Set(page.rows.map((r) => r.key));
    for (const change of held.filter((c) => c.revision > page.revision)) {
      for (const k of change.removed) keys.delete(k);
      for (const k of change.rows) keys.add(k);
    }
    expect([...keys].sort()).toEqual((await model.list({ where: { status: "open" } })).rows.map((r) => r.key));
    await h.closeAll();
  });

  it("watch limits: overflow, signal, a throwing callback, stop(); maxWatchers shared with es.watch and live models", async () => {
    await h.reset();
    const api = h.open({ watch: { maxWatchers: 3, maxPendingBatches: 1 } });
    const model = api.persistedReadModel(offersModel);
    const t0 = await api.append([made("o1", 1)]);
    await model.list({ atLeast: tokenOf(t0) });
    const controller = new AbortController();
    const w1 = await model.watch("o1", () => undefined, { signal: controller.signal });
    const live = api.readModel(offersModel);
    await live.watch("o1", () => undefined);
    await api.watch(source, () => undefined);
    await expect(model.watch("o1", () => undefined)).rejects.toThrow(/at most 3 watchers/);
    controller.abort();
    expect(w1.stopped).toBe(true);
    // overflow: a slow watcher with one pending batch falls behind and is dropped
    let releaseSlow!: () => void;
    const gate = new Promise<void>((r) => (releaseSlow = r));
    const errors: unknown[] = [];
    await model.watch("o1", () => gate, { onError: (e) => void errors.push(e) });
    for (let i = 0; i < 3; i++) await model.list({ atLeast: tokenOf(await api.append([made("o1", 1)])) });
    await until(() => errors.length === 1);
    expect((errors[0] as Error).name).toBe("WatchOverflowError");
    releaseSlow();
    // a throwing callback ends that watcher alone
    const thrown: unknown[] = [];
    let calls = 0;
    const w3 = await model.watch(
      "o1",
      () => {
        if (++calls > 1) throw new Error("boom");
      },
      { onError: (e) => void thrown.push(e) },
    );
    await model.list({ atLeast: tokenOf(await api.append([made("o1", 1)])) });
    await until(() => thrown.length === 1);
    expect(w3.stopped).toBe(true);
    const w4 = await model.watch("o1", () => undefined, { onError: () => thrown.push("stop must not call onError") });
    await model.stop();
    expect(w4.stopped).toBe(true);
    expect(thrown).toHaveLength(1);
    await h.closeAll();
  });

  it("a cold get is bounded by the context limit for the delta only, not for the key's history", async () => {
    await h.reset();
    const api = h.open({ contextCache: { maxEvents: 50 } });
    for (let i = 0; i < 4; i++) await api.append(Array.from({ length: 40 }, () => made("big", 1)));
    const model = api.persistedReadModel(offersModel);
    const t = await api.append([made("big", 1)]);
    await model.list({ atLeast: tokenOf(t) });
    await expect(api.context({ query: keyQuery(source, "offerId", "big"), fold: offerFold, initial: initialOffer })).rejects.toThrow(/more than 50/);
    expect((await h.open({ contextCache: { maxEvents: 50 } }).persistedReadModel(offersModel).get("big")).total).toBe(161);
    await h.closeAll();
  });

  it("an active model's get pages a delta over maxEvents (its writer not yet past the key): it answers, equal to fold on read, folding the delta once (decision 58)", async () => {
    await h.reset();
    const api = h.open({ contextCache: { maxEvents: 50 } });
    for (let i = 0; i < 4; i++) await api.append(Array.from({ length: 40 }, () => made("big", 1)));
    const release = await block(api, offersModel); // no writer: the model stays at no checkpoint
    try {
      const model = api.persistedReadModel(offersModel);
      expect(await model.status()).toMatchObject({ building: false, checkpoint: null });
      const got = await model.get("big");
      expect(got).toEqual(((await onRead(h.open(), "big")) as { state: unknown }).state);
      expect(got.total).toBe(160);
      expect((await model.status()).readFolded).toBe(160);
      expect(await model.get("big")).toEqual(got); // warm: the copy kept the paged state
      expect((await model.status()).readFolded).toBe(160);
    } finally {
      await release();
    }
    await h.closeAll();
  });

  it("the items fold: get equals fold on read for every value kind", async () => {
    await h.reset();
    const api = h.open();
    const model = api.persistedReadModel(itemsModel);
    await api.append([set("x", "a", "date", 7), set("x", "b", "inf"), set("x", "c", "arr", 2), set("x", "d", "nul", "q"), set("x", "e", "neg0")]);
    const got = await model.get("x");
    expect(got).toEqual(((await onRead<Item>(api, "x", itemsModel)) as { state: Item }).state);
    expect(Object.is(got.e, -0)).toBe(true);
    expect(itemFold).toBeDefined();
    await h.closeAll();
  });

  it("poison whose message jsonb cannot hold as cut or as given (an emoji at the 1 000th unit, a NUL, lone surrogates): the key fails durably with a bounded, well-formed message; its neighbours are applied, also after a restart", async () => {
    await h.reset();
    const messages = ["x".repeat(999) + "💥", "a\u0000b\ud800c\udc00", "y".repeat(998) + "💥zz"];
    const kept = ["x".repeat(999), "a�b�c�", "y".repeat(998) + "💥"];
    const fold: Fold<Offer> = (events, state) => {
      for (const e of events) {
        const d = e.data as { amount?: number; poison?: boolean };
        if (e.type === "OfferMade" && d.poison) throw new Error(messages[d.amount!]);
      }
      return offerFold(events, state);
    };
    const def = defineReadModel({ ...offersModel, name: "poison-text", fold });
    const a = h.open();
    await a.append([made("before", 1)]);
    const last = await a.append([made("p0", 0, "open", true), made("ok1", 1), made("p1", 1, "open", true), made("ok2", 2), made("p2", 2, "open", true), made("ok3", 3)]);
    const model = a.persistedReadModel(def);
    const listed = await model.list({ atLeast: tokenOf(last), timeoutMs: 3_000 });
    expect(listed.failed).toBe(3);
    expect(listed.rows.map((r) => r.key)).toEqual(["before", "ok1", "ok2", "ok3"]);
    for (const api of [a, h.open()]) {
      const handle = api.persistedReadModel(def);
      for (const [i, text] of kept.entries()) {
        const error = await handle.get(`p${i}`).catch((e: unknown) => e);
        expect(error).toBeInstanceOf(FailedKeyError);
        expect(((error as FailedKeyError).cause as Error).message).toBe(text);
      }
      for (const k of ["ok1", "ok2", "ok3"]) expect(await handle.get(k)).toEqual(((await onRead(api, k, def)) as { state: Offer }).state);
    }
    // a restarted writer passes the failed keys' later events over and applies the rest
    await model.stop();
    const b = h.open();
    const after = await b.append([made("p0", 5), made("ok1", 10)]);
    const later = await b.persistedReadModel(def).list({ atLeast: tokenOf(after), timeoutMs: 3_000 });
    expect(later.failed).toBe(3);
    expect(later.rows.find((r) => r.key === "ok1")!.state.total).toBe(11);
    await h.closeAll();
  });

  it("watch: cancelling one registration while another waits on the same follower leaves the other's pushes intact (key/key, key/list); the last watcher leaving stops the follower, a new one starts it again", async () => {
    await h.reset();
    const api = h.open();
    const model = api.persistedReadModel(offersModel);
    await model.list({ atLeast: tokenOf(await api.append([made("o1", 1)])) });
    const cap = await capOf(api);
    const original = cap.head;
    const followerOf = () => (model as unknown as { follower?: unknown }).follower;
    let total = 1;
    for (const second of ["key", "list"] as const) {
      let entered!: () => void;
      const inHead = new Promise<void>((r) => (entered = r));
      let release!: () => void;
      const held = new Promise<void>((r) => (release = r));
      let once = true;
      // the follower's first head read waits: both registrations are pending on it
      cap.head = async (ref) => {
        const head = await original(ref);
        if (once) {
          once = false;
          entered();
          await held;
        }
        return head;
      };
      try {
        const ac = new AbortController();
        const first = model.watch("o1", () => undefined, { signal: ac.signal }).catch((e: unknown) => e);
        await inHead;
        const seen: unknown[] = [];
        const other =
          second === "key"
            ? model.watch("o1", (s) => void seen.push(s.total))
            : model.watchList({ where: { status: "open" } }, (c) => void seen.push(c.rows.map((r) => r.state.total)));
        await wait(20);
        ac.abort(new Error("the first registration is cancelled"));
        await first;
        release();
        const watch = await other;
        cap.head = original;
        expect(followerOf()).toBeDefined();
        total += 2;
        await model.list({ atLeast: tokenOf(await api.append([made("o1", 2)])) });
        await until(() => seen.length === (second === "key" ? 2 : 1));
        expect(seen).toEqual(second === "key" ? [total - 2, total] : [[total]]);
        await watch.stop();
        expect(followerOf()).toBeUndefined();
      } finally {
        cap.head = original;
        release();
      }
    }
    const again: number[] = [];
    const w = await model.watch("o1", (s) => void again.push(s.total));
    await model.list({ atLeast: tokenOf(await api.append([made("o1", 4)])) });
    await until(() => again.length === 2);
    expect(again).toEqual([total, total + 4]);
    await w.stop();
    await h.closeAll();
  });

  it("events that left a key's state unchanged are not refolded: a fresh process's get starts at the model's checkpoint, folds 0 and stays within a small context limit; no watcher was told of them", async () => {
    await h.reset();
    const sum = defineReadModel({ name: "sum", version: 1, source, keyBy: "offerId", initial: 0, fold: (events, n: number) => events.reduce((s, e) => s + ((e.data as { amount?: number }).amount ?? 0), n) });
    const a = h.open({ contextCache: { maxEvents: 10 } });
    const model = a.persistedReadModel(sum);
    await model.list({ atLeast: tokenOf(await a.append([made("k", 1)])) });
    const seen: number[] = [];
    const w = await model.watch("k", (s) => void seen.push(s));
    await model.list({ atLeast: tokenOf(await a.append(Array.from({ length: 20 }, () => made("k", 0)))) });
    await model.list({ atLeast: tokenOf(await a.append(Array.from({ length: 5 }, () => made("k", 0)))) });
    await wait(50);
    expect(seen).toEqual([1]);
    await w.stop();
    await model.stop();
    const b = h.open({ contextCache: { maxEvents: 10 } });
    const fresh = b.persistedReadModel(sum);
    expect((await fresh.status()).lag).toEqual({ events: 0, ms: 0 });
    expect(await fresh.get("k")).toBe(1);
    expect((await fresh.status()).readFolded).toBe(0);
    await b.append([made("k", 2)]);
    expect(await fresh.get("k")).toBe(3);
    expect((await fresh.status()).readFolded).toBe(1);
    await h.closeAll();
  });
}

export { pmConfig, offerFold };
