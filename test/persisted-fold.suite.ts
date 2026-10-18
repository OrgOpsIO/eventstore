/**
 * The persisted fold, specified once and run against the memory store and Postgres
 * (persisted-fold-memory.test.ts, persisted-fold-postgres.test.ts). A "process" is a fresh
 * runtime over the same data: its in-memory cache is empty, so a load starts from the stored
 * state or from zero.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { z } from "zod";
import {
  CacheBudget,
  compareCursor,
  ContextCache,
  ContextTooLargeError,
  createEventStore,
  estimateSize,
  defineEvents,
  defineFold,
  UsageError,
  type ContextLoad,
  type ContextSpec,
  type Cursor,
  type EventStoreApi,
  type EventStoreConfig,
  type FoldStateStore,
  type Query,
} from "../src/index.js";
import { resetFingerprintWarnings } from "../src/fingerprint.js";

export const ledger = defineEvents({
  EntryBooked: { data: z.object({ amount: z.number(), category: z.string() }), scopes: ["ledgerId"], optionalScopes: ["tenantId"] },
});
export const TENANT = { scopeKey: "tenantId" } as const;
export const config = { events: [ledger], tenant: TENANT } satisfies EventStoreConfig;

export const Q: Query = ledger.$filter({ scopes: { ledgerId: "L1" } });
export const book = (amount: number, category = "a", ledgerId = "L1") => ledger.EntryBooked({ amount, category }, { ledgerId });

type Category = { count: number; total: number };
/** A `$foldBy`: its state is a Map, which JSON would lose. */
export const byCategory = defineFold({
  name: "by-category",
  version: 1,
  fold: ledger.$foldBy<string, Category>((e) => e.data.category, {
    EntryBooked: (d, s) => ({ count: (s?.count ?? 0) + 1, total: (s?.total ?? 0) + d.amount }),
  }),
});
export const byCategoryInitial = () => new Map<string, Category>();

type Timeline = { n: number; last: Date | null; seqs: number[] };
/** A plain fold that keeps `recordedAt` (a Date) and the order it saw. */
export const timeline = defineFold<Timeline>({
  name: "timeline",
  version: 1,
  fold: (events, s) => ({
    n: s.n + events.length,
    last: events.length > 0 ? events[events.length - 1]!.recordedAt : s.last,
    seqs: [...s.seqs, ...events.map((e) => e.sequence)],
  }),
});
export const timelineInitial: Timeline = { n: 0, last: null, seqs: [] };

export interface FoldHarness {
  /** Empty events and fold states for the next test. */
  reset(): Promise<void>;
  /** A new "process": a runtime over the same data, with an empty in-memory cache. Closed by `closeAll`. */
  open(options?: Pick<EventStoreConfig, "contextCache">): EventStoreApi;
  /** The root store's fold-state backend. */
  foldStates(): Promise<FoldStateStore>;
  /** How many stored states there are. */
  storedRows(): Promise<number>;
  /** Make every stored state unreadable, keeping its cursor. */
  corrupt(): Promise<void>;
  /** From now on, the cursors of the states written through `api`'s store (the memory store's are shared by every runtime). */
  writesOf(api: EventStoreApi): Promise<Cursor[]>;
  closeAll(): Promise<void>;
}

/** What every load reported, in order. */
export function recorder(): { loads: ContextLoad[]; onLoad: (load: ContextLoad) => void } {
  const loads: ContextLoad[] = [];
  return { loads, onLoad: (load) => void loads.push(load) };
}

/** The fold from zero over every matching event, in `(transactionId, sequence)` order: what any load must return. */
export async function fromZero<S>(api: EventStoreApi, spec: ContextSpec<S>): Promise<S> {
  const all = await api.query(spec.query, { cursor: null });
  const initial = typeof spec.initial === "function" ? (spec.initial as () => S)() : structuredClone(spec.initial);
  return spec.fold(all.events, initial);
}

const persist = (after: number, onLoad?: (load: ContextLoad) => void): Pick<EventStoreConfig, "contextCache"> => ({
  contextCache: { persist: { after }, ...(onLoad ? { onLoad } : {}) },
});

export function persistedFoldSuite(h: FoldHarness, options: { readonly numRuns: number }): void {
  it("property: every load — warm, cold, from a stored state — equals the fold from zero", async () => {
    const op = fc.oneof(
      fc.record({ kind: fc.constant("append" as const), amounts: fc.array(fc.integer({ min: -50, max: 50 }), { minLength: 1, maxLength: 4 }), category: fc.constantFrom("a", "b", "c") }),
      fc.constant({ kind: "warm" as const }),
      fc.constant({ kind: "cold" as const }),
    );
    let mismatches = 0;
    let fromStored = 0;
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(1, 2, 5, 500), fc.array(op, { minLength: 1, maxLength: 12 }), async (after, ops) => {
        await h.reset();
        const { loads, onLoad } = recorder();
        let api = h.open(persist(after, onLoad));
        for (const step of [...ops, { kind: "cold" as const }]) {
          if (step.kind === "append") {
            await api.append(step.amounts.map((a) => book(a, step.category)));
            continue;
          }
          if (step.kind === "cold") api = h.open(persist(after, onLoad));
          for (const spec of [
            { query: Q, fold: byCategory, initial: byCategoryInitial },
            { query: Q, fold: timeline, initial: timelineInitial },
          ] as ContextSpec<unknown>[]) {
            const loaded = await api.context(spec);
            const expected = await fromZero(api, spec);
            if (!isDeepEqual(loaded.state, expected)) mismatches++;
            expect(loaded.state).toEqual(expected);
          }
        }
        await h.closeAll();
        fromStored += loads.filter((l) => l.start === "stored").length;
      }),
      { numRuns: options.numRuns },
    );
    expect(mismatches).toBe(0);
    // the property exercised the stored path, not only the in-memory one
    expect(fromStored).toBeGreaterThan(0);
  }, 120_000);

  it("a cold process starts from the stored state and folds only the delta", async () => {
    await h.reset();
    const { loads, onLoad } = recorder();
    const spec = { query: Q, fold: byCategory, initial: byCategoryInitial };
    const a = h.open(persist(10, onLoad));
    await a.append(Array.from({ length: 12 }, (_, i) => book(i, i % 2 ? "a" : "b")));
    await a.context(spec);
    expect(loads[0]).toMatchObject({ start: "initial", events: 12, cacheHit: false, stored: { written: true } });

    const b = h.open(persist(10, onLoad));
    const fromStored = await b.context(spec);
    expect(loads[1]).toMatchObject({ start: "stored", events: 0, cacheHit: false, stored: { written: false } });
    expect(loads[1]!.stored!.readMs).toBeGreaterThanOrEqual(0);
    expect(fromStored.state).toEqual(await fromZero(b, spec));

    await b.append([book(100, "c"), book(1, "a"), book(2, "b")]);
    const c = h.open(persist(10, onLoad));
    const delta = await c.context(spec);
    expect(loads[2]).toMatchObject({ start: "stored", events: 3 });
    expect(delta.state).toEqual(await fromZero(c, spec));
    // warm with nothing new: no events, and the tier is not asked
    await c.context(spec);
    expect(loads[3]).toMatchObject({ start: "cache", events: 0, cacheHit: true });
    expect(loads[3]!.stored).toBeUndefined();
    await h.closeAll();
  });

  it("writes once sinceStored reaches persist.after, never for a delta of 0, and counts again after a write", async () => {
    await h.reset();
    const { loads, onLoad } = recorder();
    const api = h.open(persist(5, onLoad));
    const spec = { query: Q, fold: timeline, initial: timelineInitial };
    for (let i = 0; i < 5; i++) {
      await api.append([book(i)]);
      await api.context(spec);
    }
    expect(loads.map((l) => l.stored?.written ?? null)).toEqual([false, null, null, null, true]);
    expect(await h.storedRows()).toBe(1);
    await api.context(spec); // delta 0
    expect(loads[5]).toMatchObject({ start: "cache", events: 0 });
    expect(loads[5]!.stored).toBeUndefined();
    for (let i = 0; i < 5; i++) {
      await api.append([book(i)]);
      await api.context(spec);
    }
    expect(loads.slice(6).map((l) => l.stored?.written ?? null)).toEqual([null, null, null, null, true]);
    await h.closeAll();
  });

  it("the backend is monotone: an older cursor never replaces a newer state", async () => {
    await h.reset();
    const states = await h.foldStates();
    const key = { name: "monotone", version: 1, key: "k" };
    await states.write(key, { cursor: { transactionId: "10", sequence: 10 }, state: Uint8Array.from([1, 2, 3]) });
    await states.write(key, { cursor: { transactionId: "5", sequence: 5 }, state: Uint8Array.from([9]) });
    await states.write(key, { cursor: { transactionId: "10", sequence: 10 }, state: Uint8Array.from([8]) });
    let read = await states.read(key);
    expect(read?.cursor).toEqual({ transactionId: "10", sequence: 10 });
    expect([...read!.state]).toEqual([1, 2, 3]);
    await states.write(key, { cursor: { transactionId: "10", sequence: 11 }, state: Uint8Array.from([4]) });
    read = await states.read(key);
    expect(read?.cursor).toEqual({ transactionId: "10", sequence: 11 });
    expect([...read!.state]).toEqual([4]);
    expect(await states.read({ ...key, key: "other" })).toBeNull();
    expect(await states.read({ ...key, version: 2 })).toBeNull();
  });

  it("an initial value keyed by reference is refused with the fix; a factory works; without the tier it loads as before", async () => {
    await h.reset();
    const api = h.open(persist(1));
    await api.append([book(1)]);
    await expect(api.context({ query: Q, fold: byCategory, initial: new Map<string, Category>() })).rejects.toThrow(UsageError);
    await expect(api.context({ query: Q, fold: byCategory, initial: new Map<string, Category>() })).rejects.toThrow(/initial: \(\) => new Map\(\).*by-category/);
    expect((await api.context({ query: Q, fold: byCategory, initial: byCategoryInitial })).state.get("a")).toEqual({ count: 1, total: 1 });
    const off = h.open({ contextCache: { persist: false } });
    expect((await off.context({ query: Q, fold: byCategory, initial: new Map<string, Category>() })).state.get("a")).toEqual({ count: 1, total: 1 });
    await h.closeAll();
  });

  it("a state that is not plain data is not written: the error names the path, the load is correct", async () => {
    await h.reset();
    class Money {
      constructor(readonly cents: number) {}
    }
    const wallet = defineFold<{ items: Money[] }>({ name: "wallet", version: 1, fold: (events, s) => ({ items: [...s.items, ...events.map((e) => new Money(e.sequence))] }) });
    const { loads, onLoad } = recorder();
    const api = h.open(persist(2, onLoad));
    await api.append([book(1), book(2)]);
    const loaded = await api.context({ query: Q, fold: wallet, initial: () => ({ items: [] as Money[] }) });
    expect(loaded.state.items).toHaveLength(2);
    expect(loads[0]!.stored).toMatchObject({ written: false });
    expect(loads[0]!.stored!.error).toBeInstanceOf(UsageError);
    expect(String((loads[0]!.stored!.error as Error).message)).toMatch(/state\.items\[\d\] is a class instance \(Money\)/);
    expect(await h.storedRows()).toBe(0);
    // the count went back to 0: one more event is below the threshold, so nothing is due
    await api.append([book(3)]);
    await api.context({ query: Q, fold: wallet, initial: () => ({ items: [] as Money[] }) });
    expect(loads[1]!.stored).toBeUndefined();
    await h.closeAll();
  });

  it("unreadable stored bytes are a miss: the load folds from zero and reports the error", async () => {
    await h.reset();
    const spec = { query: Q, fold: byCategory, initial: byCategoryInitial };
    const { loads, onLoad } = recorder();
    const a = h.open(persist(1, onLoad));
    await a.append([book(1), book(2, "b")]);
    await a.context(spec);
    expect(loads[0]!.stored?.written).toBe(true);
    await h.corrupt();
    const b = h.open(persist(1, onLoad));
    const loaded = await b.context(spec);
    expect(loads[1]).toMatchObject({ start: "initial", events: 2 });
    expect(loads[1]!.stored!.error).toBeInstanceOf(Error);
    expect(loaded.state).toEqual(await fromZero(b, spec));
    await h.closeAll();
  });

  it("appendIf still decides on the store: a process that started from a stored state gets a conflict, and command() retries", async () => {
    await h.reset();
    const spec = { query: Q, fold: byCategory, initial: byCategoryInitial };
    const a = h.open(persist(1));
    await a.append([book(5)]);
    await a.context(spec);
    const { loads, onLoad } = recorder();
    const p1 = h.open(persist(1, onLoad));
    const p2 = h.open(persist(1));
    const loaded = await p1.context(spec);
    expect(loads[0]!.start).toBe("stored");
    await p2.append([book(7)]); // another process writes into the context
    const outcome = await p1.appendIf([book(1)], loaded.ctx);
    expect(outcome.ok).toBe(false);
    let seen: Category | undefined;
    const done = await p1.command({
      context: Q,
      fold: byCategory,
      initial: byCategoryInitial,
      decide: (state: Map<string, Category>) => {
        seen = state.get("a");
        return { events: [book(1)] };
      },
    });
    expect(done.ok).toBe(true);
    expect(seen).toEqual({ count: 2, total: 12 });
    await h.closeAll();
  });

  it("off: persist: false and contextCache: false read and write nothing; a fold without identity is never stored", async () => {
    await h.reset();
    const plain = (events: readonly unknown[], s: number) => s + events.length;
    const { loads, onLoad } = recorder();
    const seed = h.open();
    await seed.append([book(1), book(2)]);
    const off = h.open({ contextCache: { persist: false, onLoad } });
    await off.context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    const none = h.open({ contextCache: false });
    await none.context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    const on = h.open(persist(1, onLoad));
    await on.context({ query: Q, fold: plain, initial: 0 });
    expect(loads.map((l) => l.start)).toEqual(["initial", "initial"]);
    for (const load of loads) expect(load.stored).toBeUndefined();
    expect(await h.storedRows()).toBe(0);
    await h.closeAll();
  });

  it("a version bump is a miss with its own row; the old version's row stays", async () => {
    await h.reset();
    const v2 = defineFold({ name: "by-category", version: 2, fold: byCategory });
    const { loads, onLoad } = recorder();
    const a = h.open(persist(1, onLoad));
    await a.append([book(1), book(2)]);
    await a.context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    const b = h.open(persist(1, onLoad));
    await b.context({ query: Q, fold: v2, initial: byCategoryInitial });
    expect(loads[1]).toMatchObject({ start: "initial", events: 2, stored: { written: true } });
    expect(await h.storedRows()).toBe(2);
    const c = h.open(persist(1, onLoad));
    await c.context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    expect(loads[2]).toMatchObject({ start: "stored", events: 0 });
    await h.closeAll();
  });

  it("tenants: each tenant's state is its own row; one tenant never starts from another's", async () => {
    await h.reset();
    const spec = { query: Q, fold: byCategory, initial: byCategoryInitial };
    const { loads, onLoad } = recorder();
    const p = h.open(persist(1, onLoad));
    await p.forTenant("A").append([book(1), book(2)]);
    await p.forTenant("B").append([book(100)]);
    await p.forTenant("A").context(spec);
    await p.forTenant("B").context(spec);
    expect(loads.map((l) => [l.tenant, l.start, l.stored?.written])).toEqual([
      ["A", "initial", true],
      ["B", "initial", true],
    ]);
    expect(await h.storedRows()).toBe(2);
    const q = h.open(persist(1, onLoad));
    const b = await q.forTenant("B").context(spec);
    const a = await q.forTenant("A").context(spec);
    expect(loads.slice(2).map((l) => [l.tenant, l.start, l.events])).toEqual([
      ["B", "stored", 0],
      ["A", "stored", 0],
    ]);
    expect(b.state.get("a")).toEqual({ count: 1, total: 100 });
    expect(a.state.get("a")).toEqual({ count: 2, total: 3 });
    await h.closeAll();
  });

  it("a state the codec would change is not written: a Map's own property, a non-enumerable property", async () => {
    await h.reset();
    type Counted = Map<string, number> & { total?: number };
    const mapWithTotal = defineFold<Counted>({
      name: "map-with-total",
      version: 1,
      fold: (events, s) => Object.assign(new Map(s), { total: (s.total ?? 0) + events.length }),
    });
    const hiddenTotal = defineFold<{ n: number; total?: number }>({
      name: "hidden-total",
      version: 1,
      fold: (events, s) => {
        const next = { n: s.n + events.length };
        Object.defineProperty(next, "total", { value: next.n, enumerable: false });
        return next;
      },
    });
    const cases = [
      [{ query: Q, fold: mapWithTotal, initial: () => new Map() as Counted }, /state is a Map with an own property "total"/],
      [{ query: Q, fold: hiddenTotal, initial: { n: 0 } }, /state has a non-enumerable property "total"/],
    ] as unknown as [ContextSpec<{ total?: number }>, RegExp][];
    const { loads, onLoad } = recorder();
    const a = h.open(persist(1, onLoad));
    await a.append([book(1)]);
    for (const [spec, message] of cases) {
      expect((await a.context(spec)).state.total).toBe(1);
      const load = loads.at(-1)!;
      expect(load.stored).toMatchObject({ written: false });
      expect(load.stored!.error).toBeInstanceOf(UsageError);
      expect(String((load.stored!.error as Error).message)).toMatch(message);
    }
    expect(await h.storedRows()).toBe(0);
    // a fresh process folds from zero and keeps the property, never a stored state without it
    const b = h.open(persist(1, onLoad));
    for (const [spec] of cases) {
      expect((await b.context(spec)).state.total).toBe(1);
      expect(loads.at(-1)!.start).toBe("initial");
    }
    await h.closeAll();
  });

  it("a state the encoder cannot take (10 000 levels deep) is reported, never thrown, and not retried on every load", async () => {
    await h.reset();
    type Link = { next: Link | null };
    const chain = (length: number): Link => {
      let head: Link | null = null;
      for (let i = 0; i < length; i++) head = { next: head };
      return head!;
    };
    const depth = (link: Link | null): number => {
      let n = 0;
      for (; link; link = link.next) n++;
      return n;
    };
    const deep = defineFold<{ n: number; chain: Link | null }>({
      name: "deep-chain",
      version: 1,
      fold: (events, s) => ({ n: s.n + events.length, chain: s.chain ?? chain(10_000) }),
    });
    const spec = { query: Q, fold: deep, initial: { n: 0, chain: null } };
    const { loads, onLoad } = recorder();
    const a = h.open(persist(2, onLoad));
    await a.append([book(1), book(2)]);
    const loaded = await a.context(spec);
    expect(loaded.state.n).toBe(2);
    expect(depth(loaded.state.chain)).toBe(10_000);
    expect(loads[0]!.stored).toMatchObject({ written: false });
    expect(loads[0]!.stored!.error).toBeInstanceOf(Error);
    expect(await h.storedRows()).toBe(0);
    // the count went back to 0: one more event is below the threshold, so nothing is due
    await a.append([book(3)]);
    expect((await a.context(spec)).state.n).toBe(3);
    expect(loads[1]!.stored).toBeUndefined();
    const b = h.open(persist(2, onLoad));
    expect((await b.context(spec)).state.n).toBe(3);
    expect(loads[2]).toMatchObject({ start: "initial", events: 3 });
    await h.closeAll();
  });

  it("a state restored from the tier is sized for both byte budgets, also when the delta is empty", async () => {
    await h.reset();
    const blob = defineFold<{ blob: string; n: number }>({ name: "blob", version: 1, fold: (events, s) => ({ blob: "x".repeat(2048), n: s.n + events.length }) });
    const queries = [Q, ledger.$filter({ scopes: { ledgerId: "L2" } })];
    const initial = { blob: "", n: 0 };
    const a = h.open(persist(1));
    await a.append([book(1, "a", "L1"), book(2, "a", "L2")]);
    for (const query of queries) await a.context({ query, fold: blob, initial });
    expect(await h.storedRows()).toBe(2);
    const store = await h.open().store();
    const expected = estimateSize({ blob: "x".repeat(2048), n: 1 });
    expect(expected).toBeGreaterThan(1000);

    const { loads, onLoad } = recorder();
    const perView = new ContextCache(store, { maxBytes: 1000, persist: { after: 1 }, onLoad });
    for (const query of queries) await perView.load({ query, fold: blob, initial });
    expect(loads.map((l) => [l.start, l.events, l.bytes])).toEqual([
      ["stored", 0, expected],
      ["stored", 0, expected],
    ]);
    expect(perView.size).toBe(1);
    expect(perView.byteSize).toBe(expected);

    const budget = new CacheBudget(1000);
    const shared = new ContextCache(store, { budget, persist: { after: 1 } });
    for (const query of queries) await shared.load({ query, fold: blob, initial });
    expect(budget.entries).toBe(1);
    expect(budget.bytes).toBe(expected);
    expect(shared.size).toBe(1);
    await h.closeAll();
  });

  // ── stage 5: a version bump and the paged fold (decision 41) ──────────────

  /** 120 events in one context, in 12 appends. */
  const seed120 = async (api: EventStoreApi) => {
    for (let i = 0; i < 12; i++) await api.append(Array.from({ length: 10 }, (_, j) => book(1 + ((i * 10 + j) % 5), j % 2 === 0 ? "a" : "b")));
  };
  const paged = (onLoad?: (load: ContextLoad) => void, extra: { persist?: false } = {}): Pick<EventStoreConfig, "contextCache"> => ({
    contextCache: { maxEvents: 50, persist: extra.persist ?? { after: 1 }, ...(onLoad ? { onLoad } : {}) },
  });

  it("paged fold: after a bump, a context over maxEvents loads in pages written along the way, equal to a fold from zero; a fresh process starts stored and folds nothing; the old version's row stays", async () => {
    await h.reset();
    const seed = h.open();
    await seed120(seed);
    const v1 = { query: Q, fold: byCategory, initial: byCategoryInitial };
    await h.open(persist(1)).context(v1); // v1 stored, maxEvents default
    const v2 = { query: Q, fold: defineFold({ name: "by-category", version: 2, fold: byCategory }), initial: byCategoryInitial };
    const { loads, onLoad } = recorder();
    const p = h.open(paged(onLoad));
    const writes = await h.writesOf(p);
    const loaded = await p.context(v2);
    expect(loads[0]).toMatchObject({ start: "initial", events: 120, stored: { written: true } });
    expect(loads[0]!.stored!.error).toBeUndefined();
    expect(writes.length).toBeGreaterThanOrEqual(2);
    for (let i = 1; i < writes.length; i++) expect(compareCursor(writes[i]!, writes[i - 1]!)).toBeGreaterThan(0);
    expect(loaded.state).toEqual(await fromZero(seed, v2));
    // a fresh process with v2 starts from the last page's state: nothing folded
    const q = h.open(paged(onLoad));
    expect((await q.context(v2)).state).toEqual(loaded.state);
    expect(loads[1]).toMatchObject({ start: "stored", events: 0 });
    expect(await h.storedRows()).toBe(2); // v1's row and v2's
    await h.closeAll();
  });

  it("paged fold only with the tier: a plain fold and persist: false still throw ContextTooLargeError over maxEvents; a non-storable state loads right and writes nothing", async () => {
    await h.reset();
    const seed = h.open();
    await seed120(seed);
    const plain = (events: readonly unknown[], s: number) => s + events.length;
    await expect(h.open(paged()).context({ query: Q, fold: plain, initial: 0 })).rejects.toThrow(ContextTooLargeError);
    await expect(h.open(paged(undefined, { persist: false })).context({ query: Q, fold: byCategory, initial: byCategoryInitial })).rejects.toThrow(ContextTooLargeError);
    class Tally {
      constructor(readonly n: number) {}
    }
    const odd = defineFold<Tally>({ name: "odd", version: 1, fold: (events, s) => new Tally(s.n + events.length) });
    const { loads, onLoad } = recorder();
    const p = h.open(paged(onLoad));
    const writes = await h.writesOf(p);
    const loaded = await p.context({ query: Q, fold: odd, initial: () => new Tally(0) });
    expect(loaded.state.n).toBe(120);
    expect(loads[0]!.stored!.written).toBe(false);
    expect(String(loads[0]!.stored!.error)).toMatch(/cannot be persisted — state is a class instance \(Tally\)/);
    expect(writes).toEqual([]);
    await h.closeAll();
  });

  it("paged fold: appendIf with the ctx of a paged load still decides on the store — another process's append conflicts", async () => {
    await h.reset();
    const seed = h.open();
    await seed120(seed);
    const spec = { query: Q, fold: byCategory, initial: byCategoryInitial };
    const p = h.open(paged());
    const loaded = await p.context(spec);
    await h.open().append([book(7)]);
    const outcome = await p.appendIf([book(1)], loaded.ctx);
    expect(outcome.ok).toBe(false);
    const fresh = await h.open(paged()).appendIf([book(1)], (await h.open(paged()).context(spec)).ctx);
    expect(fresh.ok).toBe(true);
    await h.closeAll();
  });

  // added by the stage-5 measurer: two processes paging the same bumped context at once, with appends arriving
  it("paged fold under concurrency: two processes page the same bumped context while a third appends; each answer is the fold of a prefix of the store's events (none twice, none skipped); a fourth starts stored, folds only the rest and equals the fold from zero", async () => {
    await h.reset();
    const seed = h.open();
    await seed120(seed);
    const v1 = { query: Q, fold: byCategory, initial: byCategoryInitial };
    await h.open(persist(1)).context(v1);
    const v2 = { query: Q, fold: defineFold({ name: "by-category", version: 2, fold: byCategory }), initial: byCategoryInitial };
    // two appenders of their own, so on Postgres their transactions can commit out of sequence order
    let streaming = true;
    let appended = 0;
    const streams = [h.open(), h.open()].map((appender) =>
      (async () => {
        while (streaming && appended < 200) {
          const i = appended++;
          await appender.append([book(1 + (i % 9), i % 3 === 0 ? "a" : "b")]);
          await new Promise((r) => setTimeout(r, 1));
        }
      })(),
    );
    let answers: Map<string, Category>[];
    try {
      answers = (await Promise.all([h.open(paged()).context(v2), h.open(paged()).context(v2)])).map((l) => l.state);
    } finally {
      streaming = false;
      await Promise.all(streams);
    }
    const all = (await seed.query(Q, { cursor: null })).events;
    for (const state of answers) {
      const n = [...state.values()].reduce((sum, c) => sum + c.count, 0);
      expect(n).toBeGreaterThanOrEqual(120);
      expect(state).toEqual(byCategory(all.slice(0, n), byCategoryInitial()));
    }
    const { loads, onLoad } = recorder();
    const last = await h.open(paged(onLoad)).context(v2);
    expect(last.state).toEqual(await fromZero(seed, v2));
    expect(loads[0]!.start).toBe("stored");
    expect(loads[0]!.events).toBeLessThan(all.length);
    await h.closeAll();
  });

  it("fingerprint: a state written by another fold under the same version warns once per process (ES_FOLD_CHANGED), and is used as always; the same fold never warns", async () => {
    await h.reset();
    const a = h.open(persist(1));
    await a.append([book(1), book(2)]);
    const v1 = defineFold({ name: "by-amount", version: 1, fold: ledger.$fold<number>({ EntryBooked: (d, s) => s + d.amount }) });
    const v1Changed = defineFold({ name: "by-amount", version: 1, fold: ledger.$fold<number>({ EntryBooked: (d, s) => s + 2 * d.amount }) });
    await a.context({ query: Q, fold: v1, initial: 0 });
    const seen: { name: string; code?: string; message: string }[] = [];
    // only this fold's: a warning an earlier test gave may still be on its way to the listeners
    const listener = (w: Error & { code?: string }) => void (w.code === "ES_FOLD_CHANGED" && w.message.includes('"by-amount"') && seen.push({ name: w.name, code: w.code, message: w.message }));
    resetFingerprintWarnings();
    process.on("warning", listener);
    try {
      await h.open(persist(1)).context({ query: Q, fold: v1, initial: 0 });
      await new Promise((r) => setTimeout(r, 20));
      expect(seen.map((s) => s.message)).toEqual([]);
      const { loads, onLoad } = recorder();
      for (let i = 0; i < 10; i++) {
        const b = h.open(persist(1, onLoad));
        expect((await b.context({ query: Q, fold: v1Changed, initial: 0 })).state).toBe(3); // the stored state, as always
      }
      expect(loads.every((l) => l.start === "stored" && l.events === 0)).toBe(true);
      await new Promise((r) => setTimeout(r, 20));
      expect(seen.length).toBe(1);
      expect(seen[0]).toMatchObject({ name: "EventStoreWarning", code: "ES_FOLD_CHANGED" });
      expect(seen[0]!.message).toMatch(/^eventstore: persisted fold "by-amount" v1: the stored state was written by a different fold/);
    } finally {
      process.off("warning", listener);
    }
    await h.closeAll();
  });

  it("fingerprint: a hand-built ContextCache (no event schemas) and a runtime sharing the same fold never warn, in either direction; a row the hand-built cache wrote carries no fingerprint and is never compared", async () => {
    await h.reset();
    const a = h.open(persist(1));
    await a.append([book(1), book(2)]);
    const fold = defineFold({ name: "by-hand", version: 1, fold: ledger.$fold<number>({ EntryBooked: (d, s) => s + d.amount }) });
    const changed = defineFold({ name: "by-hand", version: 1, fold: ledger.$fold<number>({ EntryBooked: (d, s) => s + 2 * d.amount }) });
    const seen: string[] = [];
    const listener = (w: Error & { code?: string }) => void (w.code === "ES_FOLD_CHANGED" && w.message.includes('"by-hand"') && seen.push(w.message));
    resetFingerprintWarnings();
    process.on("warning", listener);
    try {
      await a.context({ query: Q, fold, initial: 0 }); // the runtime writes the row with its fingerprint
      const store = await h.open().store();
      const { loads, onLoad } = recorder();
      expect((await new ContextCache(store, { persist: { after: 1 }, onLoad }).load({ query: Q, fold, initial: 0 })).state).toBe(3);
      expect(loads[0]).toMatchObject({ start: "stored", events: 0 });
      await a.append([book(4)]);
      expect((await new ContextCache(store, { persist: { after: 1 }, onLoad }).load({ query: Q, fold, initial: 0 })).state).toBe(7);
      expect(loads[1]).toMatchObject({ start: "stored", events: 1, stored: { written: true } }); // the hand-built cache wrote the row
      expect((await h.open(persist(1)).context({ query: Q, fold, initial: 0 })).state).toBe(7); // a runtime reads it
      await new Promise((r) => setTimeout(r, 20));
      expect(seen).toEqual([]);
      // the documented consequence: nothing to compare against, so a changed fold under the same version is not seen on this row
      expect((await h.open(persist(1)).context({ query: Q, fold: changed, initial: 0 })).state).toBe(7);
      await new Promise((r) => setTimeout(r, 20));
      expect(seen).toEqual([]);
    } finally {
      process.off("warning", listener);
    }
    await h.closeAll();
  });
}

/** Deep equality as `toEqual` sees it, counted for the mismatch total. */
function isDeepEqual(a: unknown, b: unknown): boolean {
  try {
    expect(a).toEqual(b);
    return true;
  } catch {
    return false;
  }
}
