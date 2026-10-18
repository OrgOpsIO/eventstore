import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { MemoryStore, UsageError, createEventStore, defineEvents } from "../src/index.js";
import { subscribe, type Subscription } from "../src/subscribe/index.js";

// Stage 2: the read-your-writes token (AppendResult.transactionId), the watermark (QueryResult and
// Subscription) and hasPassed, on the memory store (model §6, tests 1, 4, 9, 10, 11).

function ev(type: string, data: Record<string, unknown> = {}) {
  return { type, data, scopes: {} };
}

const stops: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const s of stops.splice(0)) await s();
});

function started(sub: Subscription): Subscription {
  stops.push(sub.stop);
  return sub;
}

/** Loop `whenCaughtUp()` until `hasPassed(token)` or a deadline (no fixed sleep). */
async function untilPassed(sub: Subscription, token: { transactionId: string; sequence: number }, ms = 2_000): Promise<boolean> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    await sub.whenCaughtUp();
    if (sub.hasPassed(token)) return true;
  }
  return sub.hasPassed(token);
}

describe("AppendResult.transactionId (memory)", () => {
  it("is the transaction id every record of the batch carries", async () => {
    const store = new MemoryStore();
    await store.append([ev("A")]);
    const appended = await store.append([ev("A"), ev("B"), ev("A")]);
    expect(appended).toEqual({ first: 2, last: 4, count: 3, transactionId: "2" });
    const records = store.events.filter((e) => e.sequence >= appended.first && e.sequence <= appended.last);
    expect(records.map((e) => e.transactionId)).toEqual(["2", "2", "2"]);
  });

  it("is the allocateId hook's value when one is given", async () => {
    const store = new MemoryStore({ transactions: { allocateId: (first) => String(1000 - first) } });
    const appended = await store.append([ev("A"), ev("A")]);
    expect(appended.transactionId).toBe("999");
    expect(store.events.map((e) => e.transactionId)).toEqual(["999", "999"]);
  });

  it("comes back from appendIf and from command()", async () => {
    const store = new MemoryStore();
    const ok = await store.appendIf([ev("A")], { query: { types: ["A"] }, version: 0 });
    expect(ok.ok && ok.appended.transactionId).toBe("1");

    const things = defineEvents({ ThingAdded: { data: z.object({ n: z.number() }) } });
    const es = createEventStore({ events: [things], strict: false, store: (schema) => new MemoryStore({ schema }) });
    await es.append([things.ThingAdded({ n: 1 })]);
    const outcome = await es.command<number, void>({
      context: things.$filter(),
      fold: (events, state) => state + events.length,
      initial: () => 0,
      decide: (count) => ({ events: [things.ThingAdded({ n: count + 1 })] }),
    });
    if (!outcome.ok || !outcome.appended) throw new Error("expected an append");
    expect(outcome.appended.transactionId).toBe("2");
    const recorded = (await es.query(things.$filter())).events.find((e) => e.sequence === outcome.appended!.last);
    expect(recorded?.transactionId).toBe(outcome.appended.transactionId);
  });
});

describe("QueryResult.watermark (memory)", () => {
  it("default hooks: one beyond the highest transaction id, also for an empty result", async () => {
    const store = new MemoryStore();
    expect((await store.query({ types: ["A"] })).watermark).toBe("1");
    await store.append([ev("A"), ev("A")]);
    await store.append([ev("B")]);
    const result = await store.query({ types: ["Nothing"] });
    expect(result.events).toEqual([]);
    expect(result.watermark).toBe(String(store.events.length + 1));
    for (const e of store.events) expect(BigInt(e.transactionId) < BigInt(result.watermark!)).toBe(true);
  });

  it("is absent with either transactions hook", async () => {
    const withAllocate = new MemoryStore({ transactions: { allocateId: (first) => String(first) } });
    await withAllocate.append([ev("A")]);
    expect((await withAllocate.query({})).watermark).toBeUndefined();
    const withSettled = new MemoryStore({ transactions: { isSettled: () => true } });
    await withSettled.append([ev("A")]);
    expect((await withSettled.query({})).watermark).toBeUndefined();
  });
});

describe("Subscription.hasPassed (memory)", () => {
  it("passes a non-matching write by the watermark, a matching one by the cursor", async () => {
    const store = new MemoryStore();
    const sub = started(subscribe("rw", { types: ["A"] }, () => {}, { store, pollIntervalMs: 20 }));
    await sub.whenCaughtUp();
    const b = await store.append([ev("B")]);
    const tokenB = { transactionId: b.transactionId, sequence: b.last };
    expect(await untilPassed(sub, tokenB)).toBe(true);
    expect(sub.cursor).toBeNull(); // not by the cursor: nothing matched
    expect(BigInt(sub.watermark!) > BigInt(tokenB.transactionId)).toBe(true);

    const a = await store.append([ev("A")]);
    const tokenA = { transactionId: a.transactionId, sequence: a.last };
    expect(await untilPassed(sub, tokenA)).toBe(true);
    expect(sub.cursor).toEqual(tokenA);
  });

  it("without a watermark (hooks) passes a matched token by the cursor, never a non-matched one", async () => {
    const store = new MemoryStore({ transactions: { isSettled: () => true } });
    const sub = started(subscribe("rw-hooks", { types: ["A"] }, () => {}, { store, pollIntervalMs: 20 }));
    const b = await store.append([ev("B")]);
    const a = await store.append([ev("A")]);
    await sub.whenCaughtUp();
    await sub.whenCaughtUp();
    expect(sub.watermark).toBeNull();
    expect(sub.hasPassed({ transactionId: a.transactionId, sequence: a.last })).toBe(true);
    // B is older than A, so the cursor beyond A has passed B as well — order is (transactionId, sequence)
    expect(sub.hasPassed({ transactionId: b.transactionId, sequence: b.last })).toBe(true);
    const later = await store.append([ev("B")]);
    await sub.whenCaughtUp();
    expect(sub.hasPassed({ transactionId: later.transactionId, sequence: later.last })).toBe(false);
  });

  it("follows (transactionId, sequence) order, not sequence order", async () => {
    // batch 1 gets xid 20 (sequence 1), batch 2 gets xid 10 (sequence 2): the inversion of E1 check 5
    const ids = ["20", "10"];
    const store = new MemoryStore({ transactions: { allocateId: () => ids.shift() ?? "30" } });
    const first = await store.append([ev("A")]);
    const second = await store.append([ev("A")]);
    expect(first.last).toBeLessThan(second.last);
    const seen: number[] = [];
    const sub = started(subscribe("order", { types: ["A"] }, (events) => {
      seen.push(...events.map((e) => e.sequence));
    }, { store, batchSize: 1, pollIntervalMs: 20, onError: () => "stop" }));
    // stop after the first delivered event: the older transaction (10) comes first
    await new Promise<void>((resolve) => {
      const tick = () => (seen.length >= 1 ? resolve() : setTimeout(tick, 2));
      tick();
    });
    await sub.stop();
    expect(seen[0]).toBe(second.last);
    expect(sub.cursor).toEqual({ transactionId: "10", sequence: second.last });
    // the higher sequence (2) is passed, the lower one (1, xid 20) is not
    expect(sub.hasPassed({ transactionId: second.transactionId, sequence: second.last })).toBe(true);
    expect(sub.hasPassed({ transactionId: first.transactionId, sequence: first.last })).toBe(false);
  });

  it("a stopped subscription keeps its watermark and cursor and still answers", async () => {
    const store = new MemoryStore();
    await store.append([ev("A")]);
    const sub = subscribe("kept", { types: ["A"] }, () => {}, { store, pollIntervalMs: 20 });
    await sub.whenCaughtUp();
    const b = await store.append([ev("B")]);
    await sub.whenCaughtUp();
    const watermark = sub.watermark;
    const cursor = sub.cursor;
    expect(watermark).not.toBeNull();
    await sub.stop();
    expect(sub.watermark).toBe(watermark);
    expect(sub.cursor).toEqual(cursor);
    expect(sub.hasPassed({ transactionId: b.transactionId, sequence: b.last })).toBe(true);
    expect(sub.hasPassed({ transactionId: "99", sequence: 99 })).toBe(false);
  });

  it("refuses a token that is not a Cursor, with what to pass instead", async () => {
    const sub = started(subscribe("bad-token", { types: ["A"] }, () => {}, { store: new MemoryStore() }));
    expect(() => sub.hasPassed({ transactionId: "x", sequence: 1 })).toThrow(UsageError);
    expect(() => sub.hasPassed({ transactionId: 5, sequence: 1 } as never)).toThrow(/appended\.transactionId/);
  });
});

// R2.2 (stage-2 review): a page read with `limit: 0` is empty whatever is beyond the cursor, so its
// watermark proves nothing; such a batch size must never start polling.
describe("subscribe: batchSize (memory)", () => {
  it("refuses a batchSize that is not a positive safe integer, at once, before reading anything", () => {
    const store = new MemoryStore();
    let reads = 0;
    const query = store.query.bind(store);
    store.query = ((...args: Parameters<MemoryStore["query"]>) => (reads++, query(...args))) as MemoryStore["query"];
    for (const batchSize of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, "10" as never]) {
      expect(() => subscribe("bad-batch", { types: ["A"] }, () => {}, { store, batchSize })).toThrow(UsageError);
      expect(() => subscribe("bad-batch", { types: ["A"] }, () => {}, { store, batchSize })).toThrow(/batchSize/);
    }
    expect(reads).toBe(0);
  });

  it("a query with limit 0 keeps its meaning: an empty page", async () => {
    const store = new MemoryStore();
    await store.append([ev("A")]);
    expect((await store.query({ types: ["A"] }, { limit: 0 })).events).toEqual([]);
  });

  it("batchSize 1 delivers a matching event before it passes its token", async () => {
    const store = new MemoryStore();
    const a = await store.append([ev("A", { n: 1 })]);
    const token = { transactionId: a.transactionId, sequence: a.last };
    const handled: number[] = [];
    const sub = started(subscribe("one", { types: ["A"] }, (events) => void handled.push(...events.map((e) => e.sequence)), { store, batchSize: 1, pollIntervalMs: 20 }));
    expect(await untilPassed(sub, token)).toBe(true);
    expect(handled).toEqual([a.last]);
    expect(sub.cursor).toEqual(token);
  });
});
