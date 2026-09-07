import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { z } from "zod";
import { ContextCache, buildSchema, defineEvents, type RecordedEvent } from "../src/index.js";
import { toPayload } from "../src/internal.js";
import { PostgresStore } from "../src/postgres/index.js";

const url = process.env.ES_TEST_DATABASE_URL;

const accounts = defineEvents({
  AccountOpened: { data: z.object({ owner: z.string() }) },
  MoneyDeposited: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"] },
});
const schema = buildSchema([accounts], { strict: true });
const TABLE = "es_cursor_events";
const ACC = "acc-inv";

/** Insert the same wire payload the store would write, through a raw client. */
async function rawInsert(client: pg.PoolClient, amount: number): Promise<{ seq: number; xid: string }> {
  const ev = accounts.MoneyDeposited({ amount }, { accountOpenedId: ACC });
  const r = await client.query<{ seq: string; xid: string }>(
    `INSERT INTO "${TABLE}" (event_type, payload) VALUES ($1, $2::jsonb) RETURNING sequence_number::text AS seq, transaction_id::text AS xid`,
    [ev.type, JSON.stringify(toPayload(ev, "moneyDepositedId"))],
  );
  return { seq: Number(r.rows[0]!.seq), xid: r.rows[0]!.xid };
}

describe.skipIf(!url)("PostgresStore: xid/sequence inversion and gap-free cursors", () => {
  let store: PostgresStore;
  let pool: pg.Pool;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: url, max: 4 });
    await pool.query(`DROP TABLE IF EXISTS "${TABLE}" CASCADE`);
    store = new PostgresStore({ connection: url!, schema, table: TABLE, poolSize: 8 });
    await store.ensureInstalled();
    await store.append([accounts.AccountOpened({ owner: "I" }, {}, { id: ACC })]);
  });

  afterAll(async () => {
    await store.close();
    await pool.end();
  });

  /**
   * Builds: A(xid X) takes seq N, B(xid X+1) takes and commits seq N+1, A takes seq N+2 and
   * commits. Sequence order is N, N+1, N+2 but transaction order is (X,N), (X,N+2), (X+1,N+1).
   */
  async function inversion(): Promise<{ a: pg.PoolClient; n: number; xa: string; xb: string; committed: () => Promise<void> }> {
    const a = await pool.connect();
    await a.query("BEGIN");
    const first = await rawInsert(a, 1);
    const bClient = await pool.connect();
    let b: { seq: number; xid: string };
    try {
      b = await rawInsert(bClient, 2); // autocommit: its own, younger transaction
    } finally {
      bClient.release();
    }
    expect(b.seq).toBe(first.seq + 1);
    expect(BigInt(b.xid)).toBeGreaterThan(BigInt(first.xid));
    const third = await rawInsert(a, 3);
    expect(third.seq).toBe(first.seq + 2);
    expect(third.xid).toBe(first.xid);
    return {
      a,
      n: first.seq,
      xa: first.xid,
      xb: b.xid,
      committed: async () => {
        await a.query("COMMIT");
        a.release();
      },
    };
  }

  it("settledOnly never returns an unsettled row while an older transaction is open; plain reads flag it", async () => {
    const inv = await inversion();
    try {
      const query = accounts.$scope("accountOpenedId", ACC);
      const settled = await store.query(query, { settledOnly: true });
      // B's row (xid X+1) is visible but not settled: A (xid X) is still running
      expect(settled.events.map((e) => e.sequence)).not.toContain(inv.n + 1);
      const plain = await store.query(query);
      const b = plain.events.find((e) => e.sequence === inv.n + 1);
      expect(b?.settled).toBe(false);
      expect(plain.settledCursor === null || plain.settledCursor.sequence < inv.n).toBe(true);
    } finally {
      await inv.committed();
    }
  });

  it("ContextCache folds every event exactly once across loads despite the inversion", async () => {
    const inv = await inversion();
    await inv.committed();
    // wait until everything is settled (no other transaction in flight on the pool)
    const cache = new ContextCache(store, { max: 10 });
    const query = accounts.$scope("accountOpenedId", ACC);
    const seen: number[] = [];
    const fold = (events: readonly RecordedEvent[], state: number) => {
      for (const e of events) seen.push(e.sequence);
      return state + events.length;
    };
    const first = await cache.load({ query, fold, initial: 0 });
    expect(first.cacheHit).toBe(false);
    expect(new Set(seen).size).toBe(seen.length);
    expect(seen).toContain(inv.n);
    expect(seen).toContain(inv.n + 1);
    expect(seen).toContain(inv.n + 2);
    const before = seen.length;
    const second = await cache.load({ query, fold, initial: 0 });
    expect(second.cacheHit).toBe(true);
    expect(seen.length).toBe(before); // nothing re-folded
    expect(second.state).toBe(first.state);
    // an append after the inversion arrives exactly once as a delta
    await store.append([accounts.MoneyDeposited({ amount: 9 }, { accountOpenedId: ACC })]);
    const third = await cache.load({ query, fold, initial: 0 });
    expect(third.delta.map((e) => e.data.amount)).toEqual([9]);
    expect(third.state).toBe(first.state + 1);
    expect(new Set(seen).size).toBe(seen.length);
  });

  it("paging with a limit and the returned settledCursor delivers every event exactly once", async () => {
    const inv = await inversion();
    await inv.committed();
    const query = { types: ["MoneyDeposited"], scopes: { accountOpenedId: ACC } };
    const all = await store.query(query);
    const delivered: number[] = [];
    let cursor = null as { transactionId: string; sequence: number } | null;
    for (let page = 0; page < 100; page++) {
      const r = await store.query(query, { cursor, settledOnly: true, limit: 2 });
      if (r.events.length === 0) break;
      delivered.push(...r.events.map((e) => e.sequence));
      // the cursor is the max (xid, seq) tuple, not the max sequence
      cursor = r.settledCursor;
      expect(cursor).not.toBeNull();
    }
    expect(delivered.sort((a, b) => a - b)).toEqual(all.events.map((e) => e.sequence).sort((a, b) => a - b));
    expect(new Set(delivered).size).toBe(delivered.length);
    expect(delivered).toContain(inv.n + 2);
  });

  it("cursor reads are ordered by (transactionId, sequence), plain reads by sequence", async () => {
    const inv = await inversion();
    await inv.committed();
    const query = { types: ["MoneyDeposited"], scopes: { accountOpenedId: ACC } };
    const plain = await store.query(query);
    const seqs = plain.events.map((e) => e.sequence);
    expect([...seqs].sort((a, b) => a - b)).toEqual(seqs);
    const tuple = await store.query(query, { cursor: null });
    const idx = (s: number) => tuple.events.findIndex((e) => e.sequence === s);
    expect(idx(inv.n)).toBeLessThan(idx(inv.n + 2));
    expect(idx(inv.n + 2)).toBeLessThan(idx(inv.n + 1)); // A's rows come before B's row
    expect(tuple.contextVersion).toBe(plain.contextVersion);
  });
});
