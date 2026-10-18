import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { MemoryStore, UsageError, compareCursor, createEventStore, cursorOf, defineEvents, type Cursor, type RecordedEvent } from "../src/index.js";
import { memoryCursors, subscribe, type CheckpointTarget, type Subscription } from "../src/subscribe/index.js";

// Stage 2, decision 13: processor mode — the target writes the checkpoint with its rows; nothing is
// saved after the handler; a skip goes to target.skip (model §6, tests 20–26). Memory store.

const stops: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const s of stops.splice(0)) await s();
});

function started(sub: Subscription): Subscription {
  stops.push(sub.stop);
  return sub;
}

/**
 * An in-memory "read store": rows and checkpoint change together, like one transaction; the
 * handler applies only the events beyond its checkpoint (a redelivered page can start below it and
 * end beyond it), then moves the checkpoint to the page's end. A skip at or below it is a no-op.
 */
function readStore(poison: ReadonlySet<number> = new Set(), hooks: { afterCommit?: (events: readonly RecordedEvent[]) => Promise<void> | void } = {}) {
  const rows: number[] = [];
  const deadLetters: Array<{ sequence: number; type: string; error: string }> = [];
  let checkpoint: Cursor | null = null;
  const calls = { load: 0, skip: 0, handler: 0 };
  const handler = (events: readonly RecordedEvent[]) => {
    calls.handler++;
    if (events.some((e) => poison.has(e.sequence))) throw new Error("poison");
    const fresh = events.filter((e) => checkpoint === null || compareCursor(e, checkpoint) > 0);
    if (fresh.length === 0) return; // all applied already: a redelivery
    rows.push(...fresh.map((e) => e.sequence));
    checkpoint = cursorOf(events);
    return hooks.afterCommit?.(events);
  };
  const target: CheckpointTarget & { failLoad: number; failSkip: number } = {
    failLoad: 0,
    failSkip: 0,
    async load() {
      calls.load++;
      if (target.failLoad > 0) {
        target.failLoad--;
        throw new Error("checkpoint table unreachable");
      }
      return checkpoint;
    },
    async skip(event, error) {
      calls.skip++;
      if (target.failSkip > 0) {
        target.failSkip--;
        throw new Error("dead-letter write failed");
      }
      const next = cursorOf([event])!;
      if (checkpoint && compareCursor(checkpoint, next) >= 0) return;
      checkpoint = next;
      deadLetters.push({ sequence: event.sequence, type: event.type, error: String(error) });
    },
  };
  return { handler, target, rows, deadLetters, calls, checkpoint: () => checkpoint };
}

async function seeded(n: number): Promise<MemoryStore> {
  const store = new MemoryStore();
  for (let i = 1; i <= n; i++) await store.append([{ type: "A", data: { n: i }, scopes: {} }]);
  return store;
}

describe("subscribe: processor mode (target)", () => {
  it("refuses cursors and target together, at once", () => {
    const r = readStore();
    expect(() => subscribe("both", { types: ["A"] }, r.handler, { store: new MemoryStore(), cursors: memoryCursors(), target: r.target })).toThrow(UsageError);
    expect(() => subscribe("both", { types: ["A"] }, r.handler, { store: new MemoryStore(), cursors: memoryCursors(), target: r.target })).toThrow(/not both/);
  });

  it("starts after target.load()'s checkpoint; null starts at `from`", async () => {
    const store = await seeded(4);
    const resumed = readStore();
    const at2: Cursor = { transactionId: "2", sequence: 2 };
    resumed.target.load = async () => at2;
    const sub = started(subscribe("resume", { types: ["A"] }, resumed.handler, { store, target: resumed.target, pollIntervalMs: 20 }));
    await sub.whenCaughtUp();
    expect(resumed.rows).toEqual([3, 4]);

    const beginning = readStore();
    const fromStart = started(subscribe("begin", { types: ["A"] }, beginning.handler, { store, target: beginning.target, pollIntervalMs: 20 }));
    await fromStart.whenCaughtUp();
    expect(beginning.rows).toEqual([1, 2, 3, 4]);

    const now = readStore();
    const fromNow = started(subscribe("now", { types: ["A"] }, now.handler, { store, target: now.target, from: "now", pollIntervalMs: 20 }));
    await fromNow.whenCaughtUp();
    expect(now.rows).toEqual([]);
    await store.append([{ type: "A", data: { n: 5 }, scopes: {} }]);
    await fromNow.whenCaughtUp();
    expect(now.rows).toEqual([5]);
  });

  it("saves nothing: the cursor follows the handler's committed parts", async () => {
    const store = await seeded(3);
    const r = readStore();
    const sub = started(subscribe("nosave", { types: ["A"] }, r.handler, { store, target: r.target, batchSize: 2, pollIntervalMs: 20 }));
    await sub.whenCaughtUp();
    expect(r.rows).toEqual([1, 2, 3]);
    expect(sub.cursor).toEqual({ transactionId: "3", sequence: 3 });
    expect(sub.cursor).toEqual(r.checkpoint());
    expect(r.calls.load).toBe(1);
  });

  it("a longer redelivery after a lost acknowledgement applies only the events beyond the checkpoint", async () => {
    const store = await seeded(3);
    let lost = false;
    const r = readStore(new Set(), {
      afterCommit: async () => {
        if (lost) return;
        lost = true;
        await store.append([4, 5].map((n) => ({ type: "A", data: { n }, scopes: {} }))); // arrive before the retry
        throw new Error("acknowledgement of COMMIT lost");
      },
    });
    const pages: number[][] = [];
    const handler = (events: readonly RecordedEvent[]) => (pages.push(events.map((e) => e.sequence)), r.handler(events));
    const sub = started(subscribe("longer", { types: ["A"] }, handler, { store, target: r.target, pollIntervalMs: 20, onError: () => "retry" }));
    const deadline = Date.now() + 3_000;
    while (sub.cursor?.sequence !== 5 && Date.now() < deadline) await sub.whenCaughtUp();
    // the retried page starts at the same event and is longer: its first three events are applied already
    expect(pages.slice(0, 2)).toEqual([
      [1, 2, 3],
      [1, 2, 3, 4, 5],
    ]);
    expect(r.rows).toEqual([1, 2, 3, 4, 5]);
    expect(r.checkpoint()).toEqual(sub.cursor);
  });

  it("a poison event mid-page goes to target.skip once; a fresh subscription delivers nothing again", async () => {
    const store = new MemoryStore();
    await store.append(Array.from({ length: 7 }, (_, i) => ({ type: "A", data: { n: i + 1 }, scopes: {} })));
    const r = readStore(new Set([4]));
    const sub = subscribe("poison", { types: ["A"] }, r.handler, { store, target: r.target, batchSize: 10, pollIntervalMs: 20, onError: () => "skip" });
    await sub.whenCaughtUp();
    await sub.stop();
    expect(r.rows).toEqual([1, 2, 3, 5, 6, 7]);
    expect(r.calls.skip).toBe(1);
    expect(r.deadLetters).toEqual([{ sequence: 4, type: "A", error: "Error: poison" }]);
    expect(sub.cursor).toEqual({ transactionId: "1", sequence: 7 });
    expect(r.checkpoint()).toEqual(sub.cursor);

    const before = r.calls.handler;
    const fresh = started(subscribe("poison", { types: ["A"] }, r.handler, { store, target: r.target, batchSize: 10, pollIntervalMs: 20 }));
    await fresh.whenCaughtUp();
    expect(r.calls.handler).toBe(before);
    expect(fresh.cursor).toEqual(sub.cursor);
  });

  it("target.skip that throws once is a store error: onError(error, []), retry, skip again", async () => {
    const store = new MemoryStore();
    await store.append([1, 2, 3].map((n) => ({ type: "A", data: { n }, scopes: {} })));
    const r = readStore(new Set([2]));
    r.target.failSkip = 1;
    const asked: number[] = [];
    const sub = started(subscribe("skip-fails", { types: ["A"] }, r.handler, {
      store,
      target: r.target,
      batchSize: 10,
      pollIntervalMs: 20,
      onError: (_e, batch) => {
        asked.push(batch.length);
        return "skip";
      },
    }));
    const deadline = Date.now() + 3_000;
    while (sub.cursor?.sequence !== 3 && Date.now() < deadline) await sub.whenCaughtUp();
    expect(asked).toContain(0);
    expect(r.calls.skip).toBe(2);
    expect(r.rows).toEqual([1, 3]);
    expect(r.deadLetters.map((d) => d.sequence)).toEqual([2]);
  });

  it("target.load that throws is retried and never falls back to `from`", async () => {
    const store = await seeded(3);
    const r = readStore();
    const at3: Cursor = { transactionId: "3", sequence: 3 };
    let loads = 0;
    r.target.load = async () => {
      if (loads++ === 0) throw new Error("checkpoint table unreachable");
      return at3;
    };
    const errors: number[] = [];
    const sub = started(subscribe("load-fails", { types: ["A"] }, r.handler, {
      store,
      target: r.target,
      pollIntervalMs: 20,
      onError: (_e, batch) => {
        errors.push(batch.length);
        return "retry";
      },
    }));
    const deadline = Date.now() + 3_000;
    while (loads < 2 && Date.now() < deadline) await new Promise((res) => setTimeout(res, 10));
    await sub.whenCaughtUp();
    expect(errors).toEqual([0]);
    expect(r.rows).toEqual([]); // resumed after 3, nothing replayed from the beginning
    expect(sub.cursor).toEqual(at3);
  });

  it("es.subscribe on a tenant view passes the target through", async () => {
    const things = defineEvents({ ThingAdded: { data: z.object({ n: z.number() }), scopes: ["tenantId"] } });
    const es = createEventStore({ events: [things], tenant: { scopeKey: "tenantId" }, store: (schema) => new MemoryStore({ schema }) });
    const t1 = es.forTenant("t1");
    await t1.append([things.ThingAdded({ n: 1 }, { tenantId: "t1" })]);
    await es.forTenant("t2").append([things.ThingAdded({ n: 2 }, { tenantId: "t2" })]);
    const r = readStore();
    const sub = await t1.subscribe("tenant-proc", things.$filter(), r.handler, { target: r.target, pollIntervalMs: 20 });
    stops.push(sub.stop);
    await sub.whenCaughtUp();
    expect(r.calls.load).toBe(1);
    expect(r.rows).toEqual([1]);
    expect(sub.cursor).toEqual(r.checkpoint());
    await sub.stop();

    // the checkpoint lives in the target only: a fresh subscription over it resumes, replays nothing
    const before = r.calls.handler;
    const again = await t1.subscribe("tenant-proc", things.$filter(), r.handler, { target: r.target, pollIntervalMs: 20 });
    stops.push(again.stop);
    await again.whenCaughtUp();
    expect(r.calls.handler).toBe(before);
    expect(r.calls.load).toBe(2);
  });
});
