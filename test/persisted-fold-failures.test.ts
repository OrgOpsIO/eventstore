/**
 * The persisted fold when something goes wrong (measurer, stage 1): two writers at once, a fold
 * that throws on one event, and erasing a tenant. Memory store and Postgres. A "process" is a
 * fresh runtime with its own in-memory cache — on Postgres with its own connection pool, so two
 * of them write to the database concurrently like two processes would.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { buildSchema, createEventStore, defineFold, MemoryStore, type ContextLoad, type EventStoreApi, type EventStoreConfig, type FoldStateStore } from "../src/index.js";
import { PostgresStore } from "../src/postgres/index.js";
import { book, byCategory, byCategoryInitial, config, fromZero, Q, recorder } from "./persisted-fold.suite.js";

const url = process.env.ES_TEST_DATABASE_URL;
const schema = buildSchema(config.events, { strict: true, tenantScopeKey: config.tenant.scopeKey });
const TABLE = "pfx_events";
const FOLD_TABLE = `${TABLE}_fold_state`;
const spec = { query: Q, fold: byCategory, initial: byCategoryInitial };

interface Backend {
  /** Empty events and fold states. */
  reset(): Promise<void>;
  /** A new "process" with an empty cache. */
  open(contextCache?: EventStoreConfig["contextCache"]): EventStoreApi;
  /** A root fold-state backend of its own (on Postgres: its own pool). */
  foldStates(): Promise<FoldStateStore>;
  closeAll(): Promise<void>;
}

const persist = (after: number, onLoad?: (load: ContextLoad) => void): EventStoreConfig["contextCache"] => ({ persist: { after }, ...(onLoad ? { onLoad } : {}) });

function failureSuite(b: Backend): void {
  it("two writers at once: concurrent appends and cold loads in two processes leave the newest state, equal to the fold from zero", async () => {
    await b.reset();
    const { loads, onLoad } = recorder();
    for (let round = 0; round < 8; round++) {
      // two fresh processes per round: both read cold, both fold a delta, both write the same state key at the same time
      const p1 = b.open(persist(1, onLoad));
      const p2 = b.open(persist(1, onLoad));
      await Promise.all([p1.append([book(round, "a"), book(1, "b")]), p2.append([book(-round, "c")]), p1.append([book(2, "a")])]);
      const [r1, r2] = await Promise.all([p1.context(spec), p2.context(spec)]);
      const truth = await fromZero(p1, spec);
      expect(r1.state).toEqual(truth);
      expect(r2.state).toEqual(truth);
      // and one more load in each, racing an append: both may write again, while the store moves on
      await Promise.all([p2.append([book(7, "b")]), p1.context(spec), p2.context(spec)]);
    }
    expect(loads.filter((l) => l.stored?.error !== undefined)).toEqual([]);
    expect(loads.filter((l) => l.stored?.written).length).toBeGreaterThan(8);
    // a fresh process starts from a stored state at the very end: the newest write won
    const last = recorder();
    const fresh = b.open(persist(1, last.onLoad));
    const r = await fresh.context(spec);
    expect(last.loads[0]).toMatchObject({ start: "stored", events: 0 });
    expect(r.state).toEqual(await fromZero(fresh, spec));
    expect(r.state.get("a")!.count).toBe(16);
    await b.closeAll();
  });

  it("two writers at once on the backend: many parallel writes of one key keep the highest cursor", async () => {
    await b.reset();
    const [s1, s2] = await Promise.all([b.foldStates(), b.foldStates()]);
    const key = { name: "race", version: 1, key: "k" };
    const seqs = Array.from({ length: 60 }, (_, i) => ((i * 37) % 60) + 1); // 1..60, shuffled
    await Promise.all(seqs.map((seq, i) => (i % 2 ? s1 : s2).write(key, { cursor: { transactionId: "100", sequence: seq }, state: Uint8Array.from([seq]) })));
    const read = await s1.read(key);
    expect(read?.cursor).toEqual({ transactionId: "100", sequence: 60 });
    expect([...read!.state]).toEqual([60]);
    await b.closeAll();
  });

  it("a fold that throws on one event: the load fails with its error, nothing is stored past the event before it, a fresh process fails the same way", async () => {
    await b.reset();
    const poisoned = defineFold({
      name: "by-category",
      version: 1,
      fold: (events: Parameters<typeof byCategory>[0], state: ReturnType<typeof byCategoryInitial>) => {
        for (const e of events) if (e.data.amount === 13) throw new Error(`poison: event ${e.sequence}`);
        return byCategory(events, state);
      },
    });
    const pspec = { query: Q, fold: poisoned, initial: byCategoryInitial };
    const { loads, onLoad } = recorder();
    const p1 = b.open(persist(1, onLoad));
    await p1.append([book(1), book(2)]);
    await p1.context(pspec); // stored at the second event
    expect(loads[0]).toMatchObject({ start: "initial", events: 2, stored: { written: true } });
    await p1.append([book(3), book(13), book(4)]);
    await expect(p1.context(pspec)).rejects.toThrow(/^poison: event \d+$/);
    await expect(p1.context(pspec)).rejects.toThrow(/^poison/); // warm: the same, nothing half-applied
    const p2 = b.open(persist(1, onLoad));
    await expect(p2.context(pspec)).rejects.toThrow(/^poison/);
    // the stored state is still the one before the poison: the same fold without the throw (same
    // name and version, so the same state key) starts there and folds the 3 events after it
    const check = recorder();
    const p3 = b.open(persist(1, check.onLoad));
    const r = await p3.context(spec);
    expect(check.loads[0]).toMatchObject({ start: "stored", events: 3 });
    expect(r.state).toEqual(await fromZero(p3, spec));
    await b.closeAll();
  });
}

describe("persisted fold failures on the memory store", () => {
  let store: MemoryStore;
  const opened: EventStoreApi[] = [];
  failureSuite({
    async reset() {
      store = new MemoryStore({ schema, persistedFolds: true });
    },
    open(contextCache) {
      const api = createEventStore({ ...config, store, ...(contextCache ? { contextCache } : {}) });
      opened.push(api);
      return api;
    },
    async foldStates() {
      return store.foldStates!;
    },
    async closeAll() {
      for (const api of opened.splice(0)) await api.close();
    },
  });
});

describe.skipIf(!url)("persisted fold failures on Postgres", () => {
  let admin: pg.Pool;
  const opened: EventStoreApi[] = [];
  const stores: PostgresStore[] = [];
  const closeAll = async () => {
    for (const api of opened.splice(0)) await api.close();
    for (const s of stores.splice(0)) await s.close();
  };
  const open = (contextCache?: EventStoreConfig["contextCache"]) => {
    const api = createEventStore({ ...config, connection: url!, postgres: { table: TABLE, persistedFolds: true, poolSize: 3 }, ...(contextCache ? { contextCache } : {}) });
    opened.push(api);
    return api;
  };

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await admin.query(`DROP TABLE IF EXISTS "${FOLD_TABLE}" CASCADE; DROP TABLE IF EXISTS "${TABLE}" CASCADE`);
    const store = new PostgresStore({ connection: url!, schema, table: TABLE, persistedFolds: true });
    await store.ensureInstalled();
    await store.close();
  });
  afterAll(async () => {
    await closeAll();
    await admin.query(`DROP TABLE IF EXISTS "${FOLD_TABLE}" CASCADE; DROP TABLE IF EXISTS "${TABLE}" CASCADE`);
    await admin.end();
  });

  failureSuite({
    async reset() {
      await closeAll();
      await admin.query(`TRUNCATE "${FOLD_TABLE}"; TRUNCATE "${TABLE}" RESTART IDENTITY`);
    },
    open,
    async foldStates() {
      const s = new PostgresStore({ connection: url!, schema, table: TABLE, persistedFolds: true, poolSize: 4 });
      stores.push(s);
      return s.foldStates!;
    },
    closeAll,
  });

  it("erasing a tenant needs the README step: without it a fresh process starts from the erased facts; with it, from zero", async () => {
    await closeAll();
    await admin.query(`TRUNCATE "${FOLD_TABLE}"; TRUNCATE "${TABLE}" RESTART IDENTITY`);
    const p = open(persist(1));
    await p.forTenant("A").append([book(1), book(2)]);
    await p.forTenant("B").append([book(5)]);
    await p.forTenant("C").append([book(9)]);
    for (const t of ["A", "B", "C"]) await p.forTenant(t).context(spec);
    const tx = async (statements: string[], tenant: string) => {
      const c = await admin.connect();
      try {
        await c.query("BEGIN");
        for (const s of statements) await c.query(s, [tenant]);
        await c.query("COMMIT");
      } finally {
        c.release();
      }
    };
    const deleteEvents = `DELETE FROM "${TABLE}" WHERE es_scope(payload, 'tenantId') = $1`;
    // A: the recipe as the README has it now, events and fold states in one transaction
    await tx([deleteEvents, `DELETE FROM "${FOLD_TABLE}" WHERE tenant = $1`], "A");
    // C: the recipe as it was before stage 1, events only
    await tx([deleteEvents], "C");

    const { loads, onLoad } = recorder();
    const fresh = open(persist(1, onLoad));
    const a = await fresh.forTenant("A").context(spec);
    const bState = await fresh.forTenant("B").context(spec);
    const c = await fresh.forTenant("C").context(spec);
    expect(loads.map((l) => [l.tenant, l.start, l.events])).toEqual([
      ["A", "initial", 0],
      ["B", "stored", 0],
      ["C", "stored", 0],
    ]);
    expect(a.state.size).toBe(0);
    expect(bState.state.get("a")).toEqual({ count: 1, total: 5 });
    // the gap the README step closes: C's events are gone, its stored state still answers with them
    expect(c.state.get("a")).toEqual({ count: 1, total: 9 });
    expect(await fromZero(fresh.forTenant("C"), spec)).toEqual(new Map());
    const left = await admin.query<{ tenant: string }>(`SELECT tenant FROM "${FOLD_TABLE}" ORDER BY tenant`);
    expect(left.rows.map((r) => r.tenant)).toEqual(["B", "C"]);
    await closeAll();
  });
});
