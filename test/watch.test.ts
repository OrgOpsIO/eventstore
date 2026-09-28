import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import {
  MemoryStore,
  UsageError,
  WatchOverflowError,
  buildSchema,
  createEventStore,
  defineEvents,
  type EventStore,
  type RecordedEvent,
  type WakeStore,
} from "../src/index.js";
import { memoryCursors, subscribe, type CursorStore } from "../src/subscribe/index.js";

const notes = defineEvents({
  NoteAdded: { data: z.object({ text: z.string() }), scopes: ["workspaceProvisionedId"] },
});
const schema = buildSchema([notes], { tenantScopeKey: "workspaceProvisionedId", strict: true });
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
const until = async (check: () => boolean, ms = 2_000) => {
  const end = Date.now() + ms;
  while (!check() && Date.now() < end) await wait(5);
  expect(check()).toBe(true);
};

/** A memory store that counts reads, so a test can see how many readers a doorbell costs. */
function counting(): { store: EventStore & WakeStore; reads: () => number } {
  const inner = new MemoryStore({ schema });
  let reads = 0;
  const store: EventStore & WakeStore & { schema: typeof schema } = {
    schema,
    query: (q, o) => {
      reads++;
      return inner.query(q, o);
    },
    append: (e) => inner.append(e),
    appendIf: (e, c) => inner.appendIf(e, c),
    onCommitted: (l) => inner.onCommitted(l),
    close: () => inner.close(),
  };
  return { store, reads: () => reads };
}

const newApi = (store?: EventStore, limits?: { maxWatchers?: number; maxPendingBatches?: number }) =>
  createEventStore({ events: [notes], tenant: { scopeKey: "workspaceProvisionedId" }, ...(store ? { store } : {}), ...(limits ? { watch: limits } : {}) });

const stops: Array<() => unknown> = [];
afterEach(async () => {
  for (const s of stops.splice(0)) await s();
});

describe("es.watch", () => {
  it("delivers appends of its own tenant only, from now — not the history", async () => {
    const api = newApi();
    const a = api.forTenant("ws-a");
    const b = api.forTenant("ws-b");
    await a.append([notes.NoteAdded({ text: "before" }, { workspaceProvisionedId: "ws-a" })]);
    const seen: string[] = [];
    const w = await a.watch(notes.$filter(), (events) => {
      seen.push(...events.map((e) => (e.data as { text: string }).text));
    });
    stops.push(() => w.stop());
    await b.append([notes.NoteAdded({ text: "other tenant" }, { workspaceProvisionedId: "ws-b" })]);
    await a.append([notes.NoteAdded({ text: "after" }, { workspaceProvisionedId: "ws-a" })]);
    await until(() => seen.length > 0);
    await wait(60);
    expect(seen).toEqual(["after"]);
  });

  it("shares one reader per view and query, however many watchers hang on it", async () => {
    const { store, reads } = counting();
    const api = newApi(store);
    const view = api.forTenant("ws-a");
    const got = new Array<number>(100).fill(0);
    for (let i = 0; i < 100; i++) {
      const w = await view.watch(notes.$filter(), (events) => {
        got[i]! += events.length;
      });
      stops.push(() => w.stop());
    }
    const before = reads();
    await view.append([notes.NoteAdded({ text: "x" }, { workspaceProvisionedId: "ws-a" })]);
    await until(() => got.every((n) => n === 1));
    // one reader: a handful of reads for the doorbell, not one per watcher
    expect(reads() - before).toBeLessThan(5);
  });

  it("drops a watcher that falls behind with WatchOverflowError; the others go on", async () => {
    const api = newApi(undefined, { maxPendingBatches: 2 });
    const view = api.forTenant("ws-a");
    const errors: unknown[] = [];
    let fast = 0;
    const stuck = await view.watch(notes.$filter(), () => new Promise<void>(() => undefined), { onError: (e) => errors.push(e) });
    const ok = await view.watch(notes.$filter(), (events) => {
      fast += events.length;
    });
    stops.push(() => ok.stop());
    for (let i = 0; i < 5; i++) {
      await view.append([notes.NoteAdded({ text: `n${i}` }, { workspaceProvisionedId: "ws-a" })]);
      await wait(40); // let each append become its own batch
    }
    await until(() => errors.length === 1 && fast === 5);
    expect(errors[0]).toBeInstanceOf(WatchOverflowError);
    expect(stuck.stopped).toBe(true);
  });

  it("ends a watcher whose handler throws, once, and keeps the reader for the others", async () => {
    const api = newApi();
    const view = api.forTenant("ws-a");
    const errors: unknown[] = [];
    let other = 0;
    const bad = await view.watch(notes.$filter(), () => {
      throw new Error("boom");
    }, { onError: (e) => errors.push(e) });
    const good = await view.watch(notes.$filter(), (events) => {
      other += events.length;
    });
    stops.push(() => good.stop());
    await view.append([notes.NoteAdded({ text: "1" }, { workspaceProvisionedId: "ws-a" })]);
    await view.append([notes.NoteAdded({ text: "2" }, { workspaceProvisionedId: "ws-a" })]);
    await until(() => other === 2);
    expect(errors).toHaveLength(1);
    expect(bad.stopped).toBe(true);
  });

  it("refuses more than maxWatchers, and a stopped watch frees its place and gets nothing more", async () => {
    const api = newApi(undefined, { maxWatchers: 2 });
    const view = api.forTenant("ws-a");
    let seen = 0;
    const one = await view.watch(notes.$filter(), (events) => {
      seen += events.length;
    });
    const two = await view.watch({ types: ["NoteAdded"], scopes: { workspaceProvisionedId: "ws-a" } }, () => undefined);
    await expect(view.watch(notes.$filter(), () => undefined)).rejects.toBeInstanceOf(UsageError);
    one.stop();
    two.stop();
    const three = await view.watch(notes.$filter(), () => undefined);
    stops.push(() => three.stop());
    await view.append([notes.NoteAdded({ text: "x" }, { workspaceProvisionedId: "ws-a" })]);
    await wait(80);
    expect(seen).toBe(0);
  });

  it("is refused on a past view", async () => {
    const api = newApi();
    await expect(api.forTenant("ws-a").asOf(0).watch(notes.$filter(), () => undefined)).rejects.toThrow(/past view/);
    await expect(api.forTenant("ws-a").asOf(0).subscribe("s", notes.$filter(), () => undefined)).rejects.toThrow(/past view/);
  });
});

describe("es.watch — ending well", () => {
  const failingStore = (error: () => Error): EventStore => ({
    query: async () => {
      throw error();
    },
    append: async () => {
      throw error();
    },
    appendIf: async () => {
      throw error();
    },
    close: async () => undefined,
  });

  it("a watch waiting on a store that is down gives its place back when its signal aborts", async () => {
    const api = newApi(failingStore(() => new Error("connection refused")), { maxWatchers: 1 });
    const controller = new AbortController();
    const pending = api.watch(notes.$filter(), () => undefined, { signal: controller.signal });
    await wait(50);
    controller.abort(new Error("request closed"));
    await expect(pending).rejects.toThrow(/request closed/);
    // the place is free again
    const again = new AbortController();
    const second = api.watch(notes.$filter(), () => undefined, { signal: again.signal });
    again.abort(new Error("done"));
    await expect(second).rejects.toThrow(/done/);
  });

  it("a non-transient store error ends the watch instead of retrying forever", async () => {
    const api = newApi(failingStore(() => new UsageError("refused")));
    await expect(api.watch(notes.$filter(), () => undefined)).rejects.toThrow(/refused/);
  });

  it("a watch whose handler fails before watch() resolved rejects once and does not call onError", async () => {
    const store = new MemoryStore({ schema });
    const api = newApi(store);
    const errors: unknown[] = [];
    // an append racing the start: the handler may fail before the caller has a handle
    const pending = api.forTenant("ws-a").watch(notes.$filter(), () => {
      throw new Error("boom");
    }, { onError: (e) => errors.push(e) });
    const watch = await pending.catch((e: unknown) => e);
    if (watch instanceof Error) expect(watch.message).toBe("boom");
    expect(errors).toEqual([]);
  });

  it("api.close() stops every watcher and its reader", async () => {
    const api = newApi();
    const view = api.forTenant("ws-a");
    const w = await view.watch(notes.$filter(), () => undefined);
    await api.close();
    expect(w.stopped).toBe(true);
    await expect(view.watch(notes.$filter(), () => undefined)).rejects.toThrow(/closed/);
  });
});

describe("subscribe — doorbells never cut a back-off short", () => {
  it("a failing handler is retried at back-off pace, however often other commits ring", async () => {
    const store = new MemoryStore();
    const wakeOnly: EventStore & WakeStore = {
      query: (q, o) => store.query(q, o),
      append: (e) => store.append(e),
      appendIf: (e, c) => store.appendIf(e, c),
      onCommitted: (l) => store.onCommitted(l),
      close: () => store.close(),
    };
    let attempts = 0;
    const sub = subscribe("poison", { types: ["X"] }, () => {
      attempts++;
      throw new Error("poison");
    }, { store: wakeOnly });
    stops.push(sub.stop);
    await store.append([{ type: "X", data: {} }]);
    const noise = setInterval(() => void store.append([{ type: "Y", data: {} }]), 10);
    await wait(1_500);
    clearInterval(noise);
    // back-off 100 → 200 → 400 → 800 ms: about 4 attempts; every doorbell retrying would be ~50
    expect(attempts).toBeLessThanOrEqual(6);
  });
});

describe("es.subscribe", () => {
  it("reads through the tenant view and keeps its cursor as name@tenant", async () => {
    const api = newApi();
    const a = api.forTenant("ws-a");
    await a.append([notes.NoteAdded({ text: "a1" }, { workspaceProvisionedId: "ws-a" })]);
    await api.forTenant("ws-b").append([notes.NoteAdded({ text: "b1" }, { workspaceProvisionedId: "ws-b" })]);
    const cursors = memoryCursors();
    const seen: RecordedEvent[] = [];
    const sub = await a.subscribe("indexer", notes.$filter(), (events) => {
      seen.push(...events);
    }, { cursors });
    stops.push(sub.stop);
    await sub.whenCaughtUp();
    expect(seen.map((e) => (e.data as { text: string }).text)).toEqual(["a1"]);
    expect(sub.name).toBe("indexer@ws-a");
    expect(await cursors.load("indexer@ws-a")).toEqual(sub.cursor);
  });
});

describe("subscribe start position", () => {
  it("retries a failing cursor load instead of replaying from the beginning", async () => {
    const store = new MemoryStore();
    for (let i = 0; i < 3; i++) await store.append([{ type: "NoteAdded", data: { text: `old${i}` } }]);
    const saved = (await store.query({}, { settledOnly: true })).settledCursor!;
    let failures = 1;
    const inner = memoryCursors();
    await inner.save("s", saved);
    const flaky: CursorStore = {
      load: async (name) => {
        if (failures-- > 0) throw new Error("cursor store down");
        return inner.load(name);
      },
      save: (name, c) => inner.save(name, c),
    };
    const seen: string[] = [];
    const sub = subscribe("s", {}, (events) => {
      seen.push(...events.map((e) => (e.data as { text: string }).text));
    }, { store, cursors: flaky, pollIntervalMs: 20 });
    stops.push(sub.stop);
    await store.append([{ type: "NoteAdded", data: { text: "new" } }]);
    await until(() => seen.length > 0);
    await sub.whenCaughtUp();
    expect(seen).toEqual(["new"]);
  });
});
