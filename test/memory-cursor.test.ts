import { describe, expect, it } from "vitest";
import { ContextCache, MemoryStore, type RecordedEvent } from "../src/index.js";

/**
 * The memory store can simulate what Postgres does for real in test/postgres-cursor.test.ts:
 * a batch that grabbed its transaction id early but commits late (xid/sequence inversion),
 * and rows that are visible but not yet settled. The same invariants must hold.
 */
describe("MemoryStore with simulated transactions", () => {
  // batch 1 (seq 1..2) gets xid 100 but "commits late"; batch 2 (seq 3) gets xid 101 and commits first
  const xids = new Map<number, string>([[1, "100"], [3, "101"], [4, "102"]]);
  const settledXids = new Set<string>(["101"]); // 100 still in flight at first

  function makeStore() {
    return new MemoryStore({
      transactions: {
        allocateId: (first) => xids.get(first) ?? String(first + 1000),
        isSettled: (xid) => settledXids.has(xid),
      },
    });
  }

  it("cursor reads order by (transactionId, sequence) and never lose an event that settles late", async () => {
    settledXids.clear(); // nothing settled yet: xid 100 is in flight, so 101 cannot be settled either (xmin ≤ 100)
    const store = makeStore();
    await store.append([{ type: "A", data: { n: 1 } }, { type: "A", data: { n: 2 } }]); // seq 1,2 xid 100
    await store.append([{ type: "A", data: { n: 3 } }]); // seq 3 xid 101
    const none = await store.query({ types: ["A"] }, { settledOnly: true, cursor: null, limit: 10 });
    expect(none.events).toEqual([]);
    expect(none.settledCursor).toBeNull();
    // a plain read still returns sequence order and the full context version
    const plain = await store.query({ types: ["A"] });
    expect(plain.events.map((e) => e.sequence)).toEqual([1, 2, 3]);
    expect(plain.contextVersion).toBe(3);
    // the oldest transaction settles first (settled is downward-closed in xid)
    settledXids.add("100");
    const page1 = await store.query({ types: ["A"] }, { settledOnly: true, cursor: null, limit: 10 });
    expect(page1.events.map((e) => e.sequence)).toEqual([1, 2]);
    expect(page1.settledCursor).toEqual({ transactionId: "100", sequence: 2 });
    settledXids.add("101");
    const page2 = await store.query({ types: ["A"] }, { settledOnly: true, cursor: page1.settledCursor, limit: 10 });
    expect(page2.events.map((e) => e.sequence)).toEqual([3]);
    // paging with limit 1 in tuple order delivers every event exactly once
    const seen: number[] = [];
    let cursor = null as typeof page1.settledCursor;
    for (;;) {
      const page = await store.query({ types: ["A"] }, { settledOnly: true, cursor, limit: 1 });
      if (page.events.length === 0) break;
      seen.push(...page.events.map((e) => e.sequence));
      cursor = page.settledCursor;
    }
    expect(seen).toEqual([1, 2, 3]);
  });

  it("ContextCache folds every event exactly once across loads when settledness is monotone in xid", async () => {
    const monotone = new Set<string>();
    const store = new MemoryStore({
      transactions: {
        allocateId: (first) => xids.get(first) ?? String(first + 1000),
        isSettled: (xid) => monotone.has(xid),
      },
    });
    await store.append([{ type: "A", data: { n: 1 } }, { type: "A", data: { n: 2 } }]); // xid 100
    await store.append([{ type: "A", data: { n: 3 } }]); // xid 101
    const cache = new ContextCache(store);
    const fold = (events: readonly RecordedEvent[], state: number[]) => [...state, ...events.map((e) => e.sequence)];
    const load = () => cache.load({ query: { types: ["A"] }, fold, initial: [] as number[] });
    const first = await load();
    expect(first.state).toEqual([1, 2, 3]); // nothing settled: all folded for this decision, none persisted
    expect(first.ctx.version).toBe(3);
    monotone.add("100"); // oldest settles first — settled is downward-closed in xid
    const second = await load();
    expect(second.state).toEqual([1, 2, 3]);
    monotone.add("101");
    await store.append([{ type: "A", data: { n: 4 } }]); // xid 102, in flight
    const third = await load();
    expect(third.state).toEqual([1, 2, 3, 4]);
    expect(third.delta.map((e) => e.sequence)).toEqual([3, 4]); // 1,2 were persisted after the second load
  });
});
