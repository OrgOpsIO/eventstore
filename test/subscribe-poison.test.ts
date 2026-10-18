import { afterEach, describe, expect, it } from "vitest";
import { MemoryStore, type RecordedEvent } from "../src/index.js";
import { memoryCursors, subscribe, type CursorStore, type ErrorDecision, type Subscription } from "../src/subscribe/index.js";

// Stage 2, decision 12: "skip" skips the one poison event, found by bisection; its neighbours are
// applied (model §6, tests 12–19). Memory store.

const stops: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const s of stops.splice(0)) await s();
});

/** n events of type A in one batch (one transaction), `poison` marked by sequence. */
async function page(n: number): Promise<MemoryStore> {
  const store = new MemoryStore();
  await store.append(Array.from({ length: n }, (_, i) => ({ type: "A", data: { n: i + 1 }, scopes: {} })));
  return store;
}

/**
 * A handler that behaves like one transaction per call: it applies a part only when the part
 * holds no poison event, and otherwise throws having applied nothing (a rollback).
 */
function allOrNothing(poison: ReadonlySet<number>) {
  const applied: number[] = [];
  const calls: number[][] = [];
  let succeeded = 0;
  const handler = (events: readonly RecordedEvent[]) => {
    calls.push(events.map((e) => e.sequence));
    const bad = events.find((e) => poison.has(e.sequence));
    if (bad) throw new Error(`poison ${bad.sequence}`);
    applied.push(...events.map((e) => e.sequence));
    succeeded++;
  };
  return { handler, applied, calls, succeeded: () => succeeded };
}

function recordingOnError(answer: (batch: readonly RecordedEvent[], call: number) => ErrorDecision = () => "skip") {
  const asked: number[][] = [];
  const onError = (_error: unknown, batch: readonly RecordedEvent[]): ErrorDecision => {
    asked.push(batch.map((e) => e.sequence));
    return answer(batch, asked.length);
  };
  return { onError, asked };
}

/** A CursorStore that counts its saves and can fail chosen ones. */
function countingCursors(failOnSave: ReadonlySet<number> = new Set()) {
  const inner = memoryCursors();
  let saves = 0;
  const cursors: CursorStore = {
    load: (name) => inner.load(name),
    save: async (name, cursor) => {
      saves++;
      if (failOnSave.has(saves)) throw new Error(`save ${saves} failed`);
      await inner.save(name, cursor);
    },
  };
  return { cursors, inner, saves: () => saves };
}

function started(sub: Subscription): Subscription {
  stops.push(sub.stop);
  return sub;
}

const range = (from: number, to: number) => Array.from({ length: to - from + 1 }, (_, i) => from + i);

describe("subscribe: 'skip' skips the one poison event", () => {
  for (const poison of [1, 5, 9]) {
    it(`poison at sequence ${poison} of a 9-event page: every neighbour applied once, in order`, async () => {
      const store = await page(9);
      const h = allOrNothing(new Set([poison]));
      const e = recordingOnError();
      const sub = started(subscribe(`p-${poison}`, { types: ["A"] }, h.handler, { store, batchSize: 10, pollIntervalMs: 20, onError: e.onError }));
      await sub.whenCaughtUp();
      expect(h.applied).toEqual(range(1, 9).filter((s) => s !== poison));
      expect(sub.cursor).toEqual({ transactionId: "1", sequence: 9 });
      expect(e.asked).toEqual([range(1, 9), [poison]]);
      // about 2·log2(n) handler calls, not one per event
      expect(h.calls.length).toBeLessThanOrEqual(2 * Math.ceil(Math.log2(9)) + 1);
    });
  }

  it("two poison events in one page: both isolated, all others applied", async () => {
    const store = await page(9);
    const h = allOrNothing(new Set([3, 7]));
    const e = recordingOnError();
    const sub = started(subscribe("two", { types: ["A"] }, h.handler, { store, batchSize: 10, pollIntervalMs: 20, onError: e.onError }));
    await sub.whenCaughtUp();
    expect(h.applied).toEqual([1, 2, 4, 5, 6, 8, 9]);
    expect(e.asked).toEqual([range(1, 9), [3], [7]]);
    expect(sub.cursor?.sequence).toBe(9);
  });

  it("the isolated event answered 'retry', then 'skip': backs off from the event before the poison", async () => {
    const store = await page(9);
    const h = allOrNothing(new Set([5]));
    const cursorAtRetriedPage: Array<number | undefined> = [];
    let sub!: Subscription;
    const e = recordingOnError((batch, call) => {
      if (call === 3) cursorAtRetriedPage.push(sub.cursor?.sequence); // the page read after the back-off
      if (batch.length === 1 && call === 2) return "retry";
      return "skip";
    });
    sub = started(subscribe("retry-then-skip", { types: ["A"] }, h.handler, { store, batchSize: 10, pollIntervalMs: 20, onError: e.onError }));
    const deadline = Date.now() + 3_000;
    while (sub.cursor?.sequence !== 9 && Date.now() < deadline) await sub.whenCaughtUp();
    expect(e.asked[0]).toEqual(range(1, 9));
    expect(e.asked[1]).toEqual([5]);
    expect(e.asked[2]).toEqual(range(5, 9)); // the retried page starts at the poison
    expect(cursorAtRetriedPage).toEqual([4]);
    expect(e.asked[3]).toEqual([5]);
    expect([...new Set(h.applied)].sort((a, b) => a - b)).toEqual([1, 2, 3, 4, 6, 7, 8, 9]);
    expect(sub.cursor?.sequence).toBe(9);
  });

  it("the isolated event answered 'stop': stopped at the event before the poison, nothing after it", async () => {
    const store = await page(9);
    const h = allOrNothing(new Set([5]));
    const e = recordingOnError((batch) => (batch.length === 1 ? "stop" : "skip"));
    const sub = started(subscribe("stop-isolated", { types: ["A"] }, h.handler, { store, batchSize: 10, pollIntervalMs: 20, onError: e.onError }));
    await sub.whenCaughtUp();
    expect(sub.stopped).toBe(true);
    expect(sub.cursor?.sequence).toBe(4);
    expect(h.applied).toEqual([1, 2, 3, 4]);
  });

  it("the skip is durable: a fresh subscription over the same cursors delivers nothing of the old page", async () => {
    const store = await page(9);
    const h = allOrNothing(new Set([5]));
    const c = countingCursors();
    const sub = subscribe("durable", { types: ["A"] }, h.handler, { store, cursors: c.cursors, batchSize: 10, pollIntervalMs: 20, onError: () => "skip" });
    await sub.whenCaughtUp();
    await sub.stop();
    // one save per committed part: every part the handler applied, and the one skipped event
    expect(c.saves()).toBe(h.succeeded() + 1);
    expect(await c.inner.load("durable")).toEqual({ transactionId: "1", sequence: 9 });

    const again: number[] = [];
    const fresh = started(subscribe("durable", { types: ["A"] }, (events) => {
      again.push(...events.map((e) => e.sequence));
    }, { store, cursors: c.inner, pollIntervalMs: 20 }));
    await fresh.whenCaughtUp();
    expect(again).toEqual([]);
  });

  it("a cursor save that fails during isolation is a store error: onError(error, []), retry, nothing lost", async () => {
    const store = await page(9);
    const poison = new Set([5]);
    const applied = new Set<number>();
    const handler = (events: readonly RecordedEvent[]) => {
      if (events.some((e) => poison.has(e.sequence))) throw new Error("poison");
      for (const e of events) applied.add(e.sequence); // idempotent: a redelivered part is not applied twice
    };
    const c = countingCursors(new Set([2])); // the second save is the one for event 4
    const e = recordingOnError();
    const sub = started(subscribe("save-fails", { types: ["A"] }, handler, { store, cursors: c.cursors, batchSize: 10, pollIntervalMs: 20, onError: e.onError }));
    const deadline = Date.now() + 3_000;
    while (sub.cursor?.sequence !== 9 && Date.now() < deadline) await sub.whenCaughtUp();
    expect(e.asked).toContainEqual([]);
    expect([...applied].sort((a, b) => a - b)).toEqual([1, 2, 3, 4, 6, 7, 8, 9]);
    expect(sub.cursor).toEqual({ transactionId: "1", sequence: 9 });
  });

  it("'stop' on a failed page of several events commits nothing of it", async () => {
    const store = await page(5);
    const h = allOrNothing(new Set([3]));
    const e = recordingOnError(() => "stop");
    const sub = started(subscribe("stop-page", { types: ["A"] }, h.handler, { store, batchSize: 10, pollIntervalMs: 20, onError: e.onError }));
    await sub.whenCaughtUp();
    expect(sub.stopped).toBe(true);
    expect(sub.cursor).toBeNull();
    expect(h.calls).toEqual([range(1, 5)]);
    expect(e.asked).toEqual([range(1, 5)]);
  });

  it("a page that fails once and then succeeds in halves is applied whole, nothing skipped", async () => {
    const store = await page(6);
    const applied: number[] = [];
    let failures = 0;
    const handler = (events: readonly RecordedEvent[]) => {
      if (events.length === 6 && failures++ === 0) throw new Error("transient");
      applied.push(...events.map((e) => e.sequence));
    };
    const e = recordingOnError();
    const sub = started(subscribe("transient", { types: ["A"] }, handler, { store, batchSize: 10, pollIntervalMs: 20, onError: e.onError }));
    await sub.whenCaughtUp();
    expect(applied).toEqual(range(1, 6));
    expect(e.asked).toEqual([range(1, 6)]);
    expect(sub.cursor?.sequence).toBe(6);
  });

  it("stop() during isolation ends the delivery; what is committed stays", async () => {
    const store = await page(8);
    const h = allOrNothing(new Set([8]));
    let sub!: Subscription;
    let stopping: Promise<void> | undefined;
    const handler = (events: readonly RecordedEvent[]) => {
      h.handler(events);
      // the first half succeeded: stop now, before the second half is delivered
      if (events.length === 4 && events[0]!.sequence === 1) stopping = sub.stop();
    };
    sub = subscribe("stop-during", { types: ["A"] }, handler, { store, batchSize: 10, pollIntervalMs: 20, onError: () => "skip" });
    stops.push(sub.stop);
    const deadline = Date.now() + 2_000;
    while (!stopping && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    await stopping;
    expect(sub.stopped).toBe(true);
    expect(h.applied).toEqual([1, 2, 3, 4]);
    expect(sub.cursor?.sequence).toBe(4);
    expect(h.calls).toEqual([range(1, 8), [1, 2, 3, 4]]);
  });
});
