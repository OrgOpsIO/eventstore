import { describe, expect, it } from "vitest";
import { serialize } from "node:v8";
import { buildSchema, ContextCache, ContextTooLargeError, createEventStore, defineFold, MemoryStore, scopedToTenant, UsageError, type Cursor, type EventStoreApi, type FoldStateKey, type FoldStateStore, type StoredFoldState } from "../src/index.js";
import { checkStorable, decodeState, encodeState, foldIdentityOf } from "../src/foldstate.js";
import { book, byCategory, byCategoryInitial, config, fromZero, ledger, persistedFoldSuite, Q, recorder, timeline, timelineInitial, type FoldHarness } from "./persisted-fold.suite.js";

const schema = buildSchema(config.events, { strict: true, tenantScopeKey: config.tenant.scopeKey });

/** A backend that counts its calls and remembers the last key and cursor written. */
function spied(inner: FoldStateStore) {
  const keys = new Set<string>();
  const spy = {
    reads: 0,
    writes: 0,
    last: undefined as { key: FoldStateKey; stored: StoredFoldState } | undefined,
    keys,
    /** Lists that get the cursor of every write from now on. */
    taps: new Set<Cursor[]>(),
    states: {
      read: async (key: FoldStateKey) => {
        spy.reads++;
        return inner.read(key);
      },
      write: async (key: FoldStateKey, stored: StoredFoldState) => {
        spy.writes++;
        for (const tap of spy.taps) tap.push(stored.cursor);
        keys.add(JSON.stringify([key.tenant ?? "", key.name, key.version, key.key]));
        spy.last = { key, stored };
        return inner.write(key, stored);
      },
    } satisfies FoldStateStore,
  };
  return spy;
}

/** A memory store with the tier, its backend wrapped in a spy. */
function memoryWithSpy(options: ConstructorParameters<typeof MemoryStore>[0] = {}) {
  const store = new MemoryStore({ schema, persistedFolds: true, ...options });
  const spy = spied(store.foldStates!);
  Object.defineProperty(store, "foldStates", { value: spy.states });
  return { store, spy };
}

function memoryHarness(): FoldHarness {
  let current = memoryWithSpy();
  const opened: EventStoreApi[] = [];
  return {
    async reset() {
      current = memoryWithSpy();
    },
    open(options = {}) {
      const api = createEventStore({ ...config, store: current.store, ...options });
      opened.push(api);
      return api;
    },
    async foldStates() {
      return current.store.foldStates!;
    },
    async storedRows() {
      return current.spy.keys.size;
    },
    async writesOf() {
      const tap: Cursor[] = [];
      current.spy.taps.add(tap);
      return tap;
    },
    async corrupt() {
      const last = current.spy.last!;
      // newer cursor, so the monotone write takes it: bytes v8 cannot read
      await current.store.foldStates!.write(last.key, { cursor: { ...last.stored.cursor, sequence: last.stored.cursor.sequence + 1 }, state: Uint8Array.from([0]) });
    },
    async closeAll() {
      for (const api of opened.splice(0)) await api.close();
    },
  };
}

describe("persisted fold on the memory store", () => {
  persistedFoldSuite(memoryHarness(), { numRuns: 60 });
});

describe("persisted fold: memory-only cases", () => {
  it("unsettled events are folded for the answer but never stored; the cold process re-reads them", async () => {
    let settledUpTo = 2;
    const { store, spy } = memoryWithSpy({ transactions: { isSettled: (_xid, sequence) => sequence <= settledUpTo } });
    const { loads, onLoad } = recorder();
    const a = createEventStore({ ...config, store, contextCache: { persist: { after: 1 }, onLoad } });
    await a.append([book(1)]);
    await a.append([book(2)]);
    await a.append([book(3)]);
    const spec = { query: Q, fold: timeline, initial: timelineInitial };
    expect((await a.context(spec)).state.seqs).toEqual([1, 2, 3]);
    expect(spy.last?.stored.cursor).toEqual({ transactionId: "2", sequence: 2 });
    const b = createEventStore({ ...config, store, contextCache: { persist: { after: 1 }, onLoad } });
    expect((await b.context(spec)).state.seqs).toEqual([1, 2, 3]);
    expect(loads[1]).toMatchObject({ start: "stored", events: 1 });
    settledUpTo = 3;
    const c = createEventStore({ ...config, store, contextCache: { persist: { after: 1 }, onLoad } });
    expect((await c.context(spec)).state.seqs).toEqual([1, 2, 3]);
    expect(spy.last?.stored.cursor).toEqual({ transactionId: "3", sequence: 3 });
  });

  it("a past view (asOf) neither reads nor writes fold states", async () => {
    const { store, spy } = memoryWithSpy();
    const api = createEventStore({ ...config, store, contextCache: { persist: { after: 1 } } });
    await api.append([book(1), book(2), book(3)]);
    await api.context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    const before = { reads: spy.reads, writes: spy.writes };
    const past = await api.asOf(2).context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    expect(past.state.get("a")).toEqual({ count: 2, total: 3 });
    expect({ reads: spy.reads, writes: spy.writes }).toEqual(before);
  });

  it("contextCache: false never touches the backend", async () => {
    const { store, spy } = memoryWithSpy();
    const api = createEventStore({ ...config, store, contextCache: false });
    await api.append([book(1)]);
    await api.context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    await api.context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    expect([spy.reads, spy.writes]).toEqual([0, 0]);
  });

  it("a store without persistedFolds has no foldStates, and loads report only start", async () => {
    const store = new MemoryStore({ schema });
    expect(store.foldStates).toBeUndefined();
    const { loads, onLoad } = recorder();
    const api = createEventStore({ ...config, store, contextCache: { onLoad } });
    await api.append([book(1)]);
    await api.context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    await api.context({ query: Q, fold: byCategory, initial: new Map() }); // a shared instance is fine without the tier
    expect(loads.map((l) => [l.start, l.stored])).toEqual([
      ["initial", undefined],
      ["initial", undefined],
    ]);
  });

  it("with max: 0 every load starts from the tier", async () => {
    const { store } = memoryWithSpy();
    const { loads, onLoad } = recorder();
    const api = createEventStore({ ...config, store, contextCache: { max: 0, persist: { after: 1 }, onLoad } });
    await api.append([book(1), book(2)]);
    const spec = { query: Q, fold: byCategory, initial: byCategoryInitial };
    await api.context(spec);
    await api.context(spec);
    await api.append([book(3)]);
    const third = await api.context(spec);
    expect(loads.map((l) => [l.start, l.events, l.stored?.written])).toEqual([
      ["initial", 2, true],
      ["stored", 0, false],
      ["stored", 1, true],
    ]);
    expect(third.state).toEqual(await fromZero(api, spec));
  });

  it("a write that fails is reported, never thrown, and the load is correct", async () => {
    const store = new MemoryStore({ schema, persistedFolds: true });
    const failing: FoldStateStore = { read: async () => null, write: async () => Promise.reject(new Error("disk full")) };
    Object.defineProperty(store, "foldStates", { value: failing });
    const { loads, onLoad } = recorder();
    const api = createEventStore({ ...config, store, contextCache: { persist: { after: 1 }, onLoad } });
    await api.append([book(4)]);
    expect((await api.context({ query: Q, fold: byCategory, initial: byCategoryInitial })).state.get("a")).toEqual({ count: 1, total: 4 });
    expect(loads[0]!.stored).toMatchObject({ written: false });
    expect((loads[0]!.stored!.error as Error).message).toBe("disk full");
  });

  it("a read that fails fails the load (decision 06, O7): a missing tier is never silent", async () => {
    const store = new MemoryStore({ schema, persistedFolds: true });
    Object.defineProperty(store, "foldStates", { value: { read: async () => Promise.reject(new Error("no table")), write: async () => undefined } });
    const api = createEventStore({ ...config, store });
    await api.append([book(4)]);
    await expect(api.context({ query: Q, fold: byCategory, initial: byCategoryInitial })).rejects.toThrow("no table");
  });

  it("the tenant view stamps its tenant on every key and refuses a key naming another", async () => {
    const { store, spy } = memoryWithSpy();
    const api = createEventStore({ ...config, store, contextCache: { persist: { after: 1 } } });
    const a = api.forTenant("A");
    await a.append([book(1)]);
    await a.context({ query: Q, fold: byCategory, initial: byCategoryInitial });
    expect(spy.last?.key.tenant).toBe("A");
    const scoped = scopedToTenant(store, config.tenant, "A") as { foldStates?: FoldStateStore };
    await expect(scoped.foldStates!.read({ tenant: "B", name: "x", version: 1, key: "k" })).rejects.toThrow(/names tenant B/);
  });

  it("persist.after must be an integer of at least 1", () => {
    const store = new MemoryStore({ schema, persistedFolds: true });
    expect(() => new ContextCache(store, { persist: { after: 0 } })).toThrow(UsageError);
    expect(() => new ContextCache(store, { persist: { after: 1.5 } })).toThrow(/persist\.after/);
    expect(() => new ContextCache(store, { persist: false })).not.toThrow();
  });
});

describe("the paged fold: memory-only cases", () => {
  it("more than maxEvents unsettled events beyond the settled prefix: ContextTooLargeError, after the settled page was folded and written", async () => {
    let settledUpTo = 10;
    const { store, spy } = memoryWithSpy({ transactions: { isSettled: (_xid, sequence) => sequence <= settledUpTo } });
    const loads: { start: string; events: number }[] = [];
    const api = createEventStore({ ...config, store, contextCache: { maxEvents: 50, persist: { after: 1 }, onLoad: (l) => void loads.push(l) } });
    for (let i = 0; i < 12; i++) await api.append(Array.from({ length: 10 }, () => book(1)));
    const spec = { query: Q, fold: byCategory, initial: byCategoryInitial };
    await expect(api.context(spec)).rejects.toThrow(ContextTooLargeError);
    expect(spy.writes).toBe(1); // the settled first page (10 events) was stored before the rest refused
    settledUpTo = 1_000;
    const fresh = createEventStore({ ...config, store, contextCache: { maxEvents: 50, persist: { after: 1 }, onLoad: (l) => void loads.push(l) } });
    expect((await fresh.context(spec)).state.get("a")).toEqual({ count: 120, total: 120 });
    expect(loads.at(-1)).toMatchObject({ start: "stored", events: 110 });
  });
});

describe("defineFold and the codec", () => {
  it("defineFold gives a new function with a frozen, read-only identity and validates name and version", () => {
    const fold = (events: readonly unknown[], s: number) => s + events.length;
    const a = defineFold({ name: "count", version: 3, fold });
    expect(a).not.toBe(fold);
    expect(a.identity).toEqual({ name: "count", version: 3 });
    expect(Object.isFrozen(a.identity)).toBe(true);
    expect(Object.keys(a)).toEqual([]);
    expect(a([1, 2] as never, 1)).toBe(3);
    expect(foldIdentityOf(a)).toEqual({ name: "count", version: 3 });
    expect(foldIdentityOf(fold)).toBeUndefined();
    expect(foldIdentityOf(ledger.$fold<number>({}))).toBeUndefined();
    expect(() => defineFold({ name: "", version: 1, fold })).toThrow(UsageError);
    expect(() => defineFold({ name: "x".repeat(201), version: 1, fold })).toThrow(/1 to 200/);
    expect(() => defineFold({ name: "a\u0000b", version: 1, fold })).toThrow(/without NUL/);
    for (const version of [0, -1, 1.5, 2 ** 31, Number.NaN]) expect(() => defineFold({ name: "n", version, fold })).toThrow(UsageError);
    expect(() => defineFold({ name: "n", version: 2_147_483_647, fold })).not.toThrow();
  });

  it("checkStorable accepts plain data, Map, Set and Date, and names the first path that would not survive", () => {
    const id = { name: "f", version: 1 };
    const ok = { a: [1, "x", null, undefined, true, 10n], m: new Map<unknown, unknown>([[{ k: 1 }, new Set([new Date(0)])]]), d: new Date(5) };
    expect(checkStorable(ok, id)).toBeNull();
    const cases: [unknown, RegExp][] = [
      [{ a: { b: () => 1 } }, /state\.a\.b is a function/],
      [{ list: [1, Object.create(null)] }, /state\.list\[1\] is an object without prototype/],
      [new Map([["k", new (class Money {})()]]), /state\.get\("k"\) is a class instance \(Money\)/],
      [{ s: Symbol("x") }, /state\.s is a symbol/],
      [{ [Symbol("x")]: 1 }, /state is an object with symbol keys/],
      [{ r: /x/ }, /state\.r is a class instance \(RegExp\)/],
      [{ b: new Uint8Array(2) }, /state\.b is a class instance \(Uint8Array\)/],
    ];
    for (const [state, message] of cases) {
      const error = checkStorable(state, id);
      expect(error).toBeInstanceOf(UsageError);
      expect(error!.message).toMatch(message);
      expect(error!.message).toContain('fold "f" v1');
    }
    // cyclic plain data is fine: v8 keeps the cycle
    const cyclic: Record<string, unknown> = { n: 1 };
    cyclic.self = cyclic;
    expect(checkStorable(cyclic, id)).toBeNull();
    const back = decodeState(encodeState(cyclic)) as Record<string, unknown>;
    expect(back.self).toBe(back);
  });

  it("checkStorable refuses every property the codec would drop or change, and what it accepts comes back with the same properties", () => {
    const id = { name: "f", version: 1 };
    const hidden = (target: object, name: string, value: unknown) => Object.defineProperty(target, name, { value, enumerable: false });
    const refused: [unknown, RegExp][] = [
      [Object.assign(new Map([["a", 1]]), { total: 1 }), /state is a Map with an own property "total"/],
      [{ s: Object.assign(new Set([1]), { total: 1 }) }, /state\.s is a Set with an own property "total"/],
      [{ d: Object.assign(new Date(0), { total: 1 }) }, /state\.d is a Date with an own property "total"/],
      [hidden({ n: 1 }, "total", 1), /state has a non-enumerable property "total"/],
      [{ list: hidden([1, 2], "total", 1) }, /state\.list has a non-enumerable property "total"/],
      [{ n: 1, get double() { return 2; } }, /state has a getter or setter "double"/],
      [{ list: Object.defineProperty([1], "0", { get: () => 1, enumerable: true }) }, /state\.list has a getter or setter "0"/],
      // an array's own enumerable property survives the codec, so its value is checked like any other
      [{ list: Object.assign([1], { extra: new (class Money {})() }) }, /state\.list\.extra is a class instance \(Money\)/],
    ];
    for (const [state, message] of refused) {
      const error = checkStorable(state, id);
      expect(error).toBeInstanceOf(UsageError);
      expect(error!.message).toMatch(message);
    }
    const accepted: unknown[] = [
      { a: 1, list: Object.assign([1, , 3], { extra: "kept" }) },
      new Map([["k", { n: [new Set([1]), new Date(3)] }]]),
      Object.freeze({ frozen: Object.freeze([1]) }), // frozen-ness is not kept; the values are, and a fold reads values
    ];
    // what is accepted comes back with the same own properties, each enumerable data with the same value
    const shape = (value: unknown): unknown => {
      if (typeof value !== "object" || value === null) return value;
      const own = Object.getOwnPropertyNames(value).map((name) => {
        const d = Object.getOwnPropertyDescriptor(value, name)!;
        return [name, d.enumerable, "value" in d ? shape(d.value) : "accessor"];
      });
      const inner = value instanceof Map ? [...value].map(([k, v]) => [shape(k), shape(v)]) : value instanceof Set ? [...value].map(shape) : value instanceof Date ? value.getTime() : null;
      return [Object.prototype.toString.call(value), own, inner];
    };
    for (const state of accepted) {
      expect(checkStorable(state, id)).toBeNull();
      expect(shape(decodeState(encodeState(state)))).toEqual(shape(state));
    }
  });

  it("the codec round-trips what JSON would lose, and refuses bytes it cannot read", () => {
    const state = { m: new Map([["a", { n: 1 }]]), s: new Set([1, 2]), d: new Date(1234), u: undefined, b: 7n, holes: [1, , 3] };
    expect(decodeState(encodeState(state))).toEqual(state);
    for (const bytes of [Uint8Array.from([0]), new TextEncoder().encode("garbage"), new Uint8Array(0)]) expect(() => decodeState(bytes)).toThrow();
    expect([...encodeState(1)]).toEqual([...serialize(1)]);
  });
});
