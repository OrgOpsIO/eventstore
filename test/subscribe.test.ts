import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MemoryStore, type RecordedEvent } from "../src/index.js";
import { fileCursors, memoryCursors, on, resetCursor, subscribe, type CursorStore } from "../src/subscribe/index.js";

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

function ev(type: string, data: Record<string, unknown> = {}, scopes: Record<string, string> = {}) {
  return { type, data, scopes };
}

async function seeded(n = 3): Promise<MemoryStore> {
  const store = new MemoryStore();
  for (let i = 1; i <= n; i++) await store.append([ev("AccountOpened", { n: i }, { accountOpenedId: `a${i}` })]);
  return store;
}

const stops: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const s of stops.splice(0)) await s();
});

describe("subscribe", () => {
  it("replays from the beginning, then delivers live appends", async () => {
    const store = await seeded(3);
    const seen: number[] = [];
    const sub = subscribe("p1", { types: ["AccountOpened"] }, (events) => {
      seen.push(...events.map((e) => e.sequence));
    }, { store, pollIntervalMs: 50 });
    stops.push(sub.stop);
    await sub.whenCaughtUp();
    expect(seen).toEqual([1, 2, 3]);
    expect(sub.cursor).toEqual({ transactionId: "3", sequence: 3 });

    await store.append([ev("AccountOpened", { n: 4 })]);
    await sub.whenCaughtUp();
    expect(seen).toEqual([1, 2, 3, 4]);
  });

  it("filters by the query and honours batchSize", async () => {
    const store = await seeded(5);
    await store.append([ev("Other", {})]);
    const batches: number[][] = [];
    const sub = subscribe("p2", { types: ["AccountOpened"] }, (events) => {
      batches.push(events.map((e) => e.sequence));
    }, { store, batchSize: 2, pollIntervalMs: 20 });
    stops.push(sub.stop);
    await sub.whenCaughtUp();
    expect(batches).toEqual([[1, 2], [3, 4], [5]]);
  });

  it("persists the cursor and resumes from it (memoryCursors)", async () => {
    const store = await seeded(2);
    const cursors = memoryCursors();
    const first: number[] = [];
    const sub1 = subscribe("p3", { types: ["AccountOpened"] }, (events) => {
      first.push(...events.map((e) => e.sequence));
    }, { store, cursors, pollIntervalMs: 20 });
    await sub1.whenCaughtUp();
    await sub1.stop();
    expect(first).toEqual([1, 2]);
    expect(await cursors.load("p3")).toEqual({ transactionId: "2", sequence: 2 });

    await store.append([ev("AccountOpened", { n: 3 })]);
    const second: number[] = [];
    const sub2 = subscribe("p3", { types: ["AccountOpened"] }, (events) => {
      second.push(...events.map((e) => e.sequence));
    }, { store, cursors, pollIntervalMs: 20 });
    stops.push(sub2.stop);
    await sub2.whenCaughtUp();
    expect(second).toEqual([3]);
  });

  it("persists the cursor in a JSON file (fileCursors) and resetCursor() replays", async () => {
    const dir = mkdtempSync(join(tmpdir(), "es-cursors-"));
    try {
      const path = join(dir, "nested", "cursors.json");
      const cursors: CursorStore = fileCursors(path);
      const store = await seeded(2);
      const a: number[] = [];
      const sub1 = subscribe("file", { types: ["AccountOpened"] }, (events) => {
        a.push(...events.map((e) => e.sequence));
      }, { store, cursors, pollIntervalMs: 20 });
      await sub1.whenCaughtUp();
      await sub1.stop();
      expect(a).toEqual([1, 2]);
      expect(await fileCursors(path).load("file")).toEqual({ transactionId: "2", sequence: 2 });

      const b: number[] = [];
      const sub2 = subscribe("file", { types: ["AccountOpened"] }, (events) => {
        b.push(...events.map((e) => e.sequence));
      }, { store, cursors, pollIntervalMs: 20 });
      await sub2.whenCaughtUp();
      await sub2.stop();
      expect(b).toEqual([]);

      await resetCursor("file", cursors);
      expect(await cursors.load("file")).toBeNull();
      const c: number[] = [];
      const sub3 = subscribe("file", { types: ["AccountOpened"] }, (events) => {
        c.push(...events.map((e) => e.sequence));
      }, { store, cursors, pollIntervalMs: 20 });
      stops.push(sub3.stop);
      await sub3.whenCaughtUp();
      expect(c).toEqual([1, 2]);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("from: 'now' skips history", async () => {
    const store = await seeded(3);
    const seen: number[] = [];
    const sub = subscribe("now", { types: ["AccountOpened"] }, (events) => {
      seen.push(...events.map((e) => e.sequence));
    }, { store, from: "now", pollIntervalMs: 20 });
    stops.push(sub.stop);
    await sub.whenCaughtUp();
    expect(seen).toEqual([]);
    await store.append([ev("AccountOpened", { n: 4 })]);
    await sub.whenCaughtUp();
    expect(seen).toEqual([4]);
  });

  it("retries a failing handler with back-off and does not advance the cursor until it succeeds", async () => {
    const store = await seeded(1);
    let calls = 0;
    const delivered: number[] = [];
    const sub = subscribe("retry", { types: ["AccountOpened"] }, (events) => {
      calls++;
      if (calls < 3) throw new Error("boom");
      delivered.push(...events.map((e) => e.sequence));
    }, { store, pollIntervalMs: 20 });
    stops.push(sub.stop);
    await wait(60);
    expect(sub.cursor).toBeNull();
    await sub.whenCaughtUp();
    await wait(400);
    expect(calls).toBeGreaterThanOrEqual(3);
    expect(delivered).toEqual([1]);
    expect(sub.cursor).toEqual({ transactionId: "1", sequence: 1 });
  });

  it("onError 'skip' advances past the failing batch", async () => {
    const store = await seeded(2);
    const delivered: number[] = [];
    const skipped: number[][] = [];
    const sub = subscribe("skip", { types: ["AccountOpened"] }, (events) => {
      if (events.some((e) => e.sequence === 1)) throw new Error("bad batch");
      delivered.push(...events.map((e) => e.sequence));
    }, {
      store,
      batchSize: 1,
      pollIntervalMs: 20,
      onError: (_err, batch) => {
        skipped.push(batch.map((e) => e.sequence));
        return "skip";
      },
    });
    stops.push(sub.stop);
    await sub.whenCaughtUp();
    expect(skipped).toEqual([[1]]);
    expect(delivered).toEqual([2]);
    expect(sub.cursor).toEqual({ transactionId: "2", sequence: 2 });
  });

  it("onError 'stop' stops the subscription", async () => {
    const store = await seeded(1);
    const sub = subscribe("stop", { types: ["AccountOpened"] }, () => {
      throw new Error("fatal");
    }, { store, pollIntervalMs: 20, onError: () => "stop" });
    await sub.whenCaughtUp();
    expect(sub.stopped).toBe(true);
    expect(sub.cursor).toBeNull();
  });

  it("stop() awaits the in-flight batch and delivers nothing afterwards", async () => {
    const store = await seeded(1);
    const seen: number[] = [];
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const sub = subscribe("stopping", { types: ["AccountOpened"] }, async (events) => {
      await gate;
      seen.push(...events.map((e) => e.sequence));
    }, { store, pollIntervalMs: 20 });
    await wait(20);
    const stopping = sub.stop();
    release();
    await stopping;
    expect(seen).toEqual([1]);
    await store.append([ev("AccountOpened", { n: 2 })]);
    await wait(60);
    expect(seen).toEqual([1]);
    expect(sub.stopped).toBe(true);
  });

  it("on() reacts in-process to matching appends only", async () => {
    const store = new MemoryStore();
    const seen: RecordedEvent[] = [];
    const off = on({ types: ["AccountOpened"], scopes: { accountOpenedId: "x" } }, (events) => seen.push(...events), { store });
    await store.append([ev("AccountOpened", {}, {}), ev("Other", {}, { accountOpenedId: "x" })]);
    await store.append([{ type: "AccountOpened", data: {}, id: "x" }]);
    expect(seen.map((e) => [e.type, e.id])).toEqual([["AccountOpened", "x"]]);
    off();
    await store.append([{ type: "AccountOpened", data: {}, id: "x2" }]);
    expect(seen).toHaveLength(1);
  });

  it("on() rejects stores that cannot push", () => {
    const polling = {
      query: async () => ({ events: [], byFilter: [], lastReturned: 0, contextVersion: 0, ctx: { query: {}, version: 0 }, settledCursor: null }),
      append: async () => ({ first: 0, last: 0, count: 0 }),
      appendIf: async () => ({ ok: true as const, appended: { first: 0, last: 0, count: 0 } }),
      close: async () => {},
    };
    expect(() => on({ types: ["X"] }, () => {}, { store: polling })).toThrow(/LiveStore/);
  });
});
