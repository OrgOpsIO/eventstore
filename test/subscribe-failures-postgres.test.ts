import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { z } from "zod";
import { buildSchema, compareCursor, cursorOf, defineEvents, type Cursor, type RecordedEvent } from "../src/index.js";
import { PostgresStore } from "../src/postgres/index.js";
import { subscribe, type CheckpointTarget, type Subscription } from "../src/subscribe/index.js";

// Stage 2, measurer: processor mode when things go wrong, on real Postgres.
//  - A redelivered page can be LONGER than the one already committed (a lost COMMIT acknowledgement,
//    new events before the retry), and two concurrent subscribers (separate stores and pools, one
//    Node process) running the same target read pages of different lengths. A target stays
//    exactly-once only when it applies just the events beyond its checkpoint, under a lock on that
//    checkpoint. The guard "move the checkpoint if it is behind, then write every event of the
//    page" applies the overlap twice (measured: .ziv/lab/stage-02-measurer/longer-redelivery.mjs).
//  - These tests use the README's locking target (decision 14) and show that subscribe gives it what
//    it needs: pages in (transactionId, sequence) order, each commit a contiguous prefix, the start
//    from target.load().
// Tables of their own, dropped before and after.

const url = process.env.ES_TEST_DATABASE_URL;

const jobs = defineEvents({ Job: { data: z.object({ n: z.number(), poison: z.boolean().optional() }) } });
const schema = buildSchema([jobs], { strict: false });
const TABLE = "es_sub2m_events";
const ROWS = "es_sub2m_rows";
const CHECKPOINT = "es_sub2m_checkpoint";
const DEAD = "es_sub2m_dead_letters";
const JQ = { types: ["Job"] };
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function untilAll(subs: readonly Subscription[], token: Cursor, ms = 10_000): Promise<boolean> {
  const deadline = Date.now() + ms;
  for (const s of subs) while (!s.hasPassed(token) && Date.now() < deadline) await s.whenCaughtUp();
  return subs.every((s) => s.hasPassed(token));
}

describe.skipIf(!url)("subscribe, processor mode: a target that applies only what is beyond its checkpoint (Postgres)", () => {
  let admin: pg.Pool;
  const stores: PostgresStore[] = [];
  const storeOf = () => {
    const s = new PostgresStore({ connection: url!, schema, table: TABLE, poolSize: 2 });
    stores.push(s);
    return s;
  };
  const drop = () => admin.query(`DROP TABLE IF EXISTS "${TABLE}", "${ROWS}", "${CHECKPOINT}", "${DEAD}"`);

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 6 });
    await drop();
    // `applied` counts: a second application is visible, not refused by the key
    await admin.query(`CREATE TABLE "${ROWS}" (name text NOT NULL, seq bigint NOT NULL, applied int NOT NULL DEFAULT 1, PRIMARY KEY (name, seq))`);
    await admin.query(`CREATE TABLE "${CHECKPOINT}" (name text PRIMARY KEY, xid xid8, seq bigint)`);
    await admin.query(`CREATE TABLE "${DEAD}" (name text NOT NULL, xid xid8 NOT NULL, seq bigint NOT NULL, event_type text NOT NULL, event_id text NOT NULL, error text NOT NULL)`);
  });
  afterAll(async () => {
    for (const s of stores) await s.close();
    await drop();
    await admin.end();
  });

  async function inTransaction(work: (client: pg.PoolClient) => Promise<boolean>): Promise<void> {
    const client = await admin.connect();
    try {
      await client.query("BEGIN");
      await client.query((await work(client)) ? "COMMIT" : "ROLLBACK");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  /** The README pattern: this target's checkpoint row, created on first use, locked for the transaction and read. */
  async function lockCheckpoint(client: pg.PoolClient, name: string): Promise<Cursor | null> {
    await client.query(`INSERT INTO "${CHECKPOINT}" (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, [name]);
    const r = await client.query<{ transactionId: string | null; sequence: string | null }>(
      `SELECT xid::text AS "transactionId", seq::text AS sequence FROM "${CHECKPOINT}" WHERE name = $1 FOR UPDATE`,
      [name],
    );
    const row = r.rows[0]!;
    return row.transactionId === null ? null : { transactionId: row.transactionId, sequence: Number(row.sequence) };
  }
  const moveCheckpoint = (client: pg.PoolClient, name: string, c: Cursor) =>
    client.query(`UPDATE "${CHECKPOINT}" SET xid = $2::xid8, seq = $3 WHERE name = $1`, [name, c.transactionId, c.sequence]);
  const beyond = (events: readonly RecordedEvent[], cp: Cursor | null) => events.filter((e) => cp === null || compareCursor(e, cp) > 0);

  function processor(name: string, store: PostgresStore, hooks: { afterCommit?: (events: readonly RecordedEvent[]) => Promise<void> } = {}) {
    const deliveries: number[][] = [];
    const target: CheckpointTarget = {
      async load() {
        const r = await admin.query<{ transactionId: string | null; sequence: string | null }>(
          `SELECT xid::text AS "transactionId", seq::text AS sequence FROM "${CHECKPOINT}" WHERE name = $1`,
          [name],
        );
        const row = r.rows[0];
        return row?.transactionId ? { transactionId: row.transactionId, sequence: Number(row.sequence) } : null;
      },
      async skip(event, error) {
        await inTransaction(async (client) => {
          if (beyond([event], await lockCheckpoint(client, name)).length === 0) return false; // already past it
          await moveCheckpoint(client, name, cursorOf([event])!);
          await client.query(`INSERT INTO "${DEAD}" VALUES ($1, $2::xid8, $3, $4, $5, $6)`, [name, event.transactionId, event.sequence, event.type, event.id, String(error)]);
          return true;
        });
      },
    };
    const handler = async (events: readonly RecordedEvent[]) => {
      deliveries.push(events.map((e) => e.sequence));
      await inTransaction(async (client) => {
        const fresh = beyond(events, await lockCheckpoint(client, name));
        if (fresh.length === 0) return false; // all applied already: a redelivery
        await moveCheckpoint(client, name, cursorOf(events)!);
        await client.query(`INSERT INTO "${ROWS}" AS r (name, seq) SELECT $1, unnest($2::bigint[]) ON CONFLICT (name, seq) DO UPDATE SET applied = r.applied + 1`, [
          name,
          fresh.map((e) => e.sequence),
        ]);
        if (fresh.some((e) => (e.data as { poison?: boolean }).poison)) throw new Error("poison"); // the fold fails while applying: rolled back
        return true;
      });
      await hooks.afterCommit?.(events);
    };
    const sub = subscribe(name, JQ, handler, {
      store,
      target,
      batchSize: 7,
      pollIntervalMs: 5,
      onError: (error) => (error instanceof Error && error.message === "poison" ? "skip" : "retry"),
    });
    return { sub, deliveries, target };
  }

  const rowsOf = async (name: string) =>
    (await admin.query<{ s: string; applied: number }>(`SELECT seq::text AS s, applied FROM "${ROWS}" WHERE name = $1 ORDER BY seq`, [name])).rows.map((r) => [Number(r.s), r.applied]);
  const deadOf = async (name: string) => (await admin.query<{ s: string }>(`SELECT seq::text AS s FROM "${DEAD}" WHERE name = $1 ORDER BY seq`, [name])).rows.map((r) => Number(r.s));

  it("a lost COMMIT acknowledgement with new events before the retry: the longer page is redelivered, and applied once", async () => {
    const store = storeOf();
    const first = await store.append([1, 2, 3].map((n) => jobs.Job({ n })));
    let lost = false;
    let more!: Awaited<ReturnType<PostgresStore["append"]>>;
    const { sub, deliveries, target } = processor("longer-redelivery", store, {
      afterCommit: async () => {
        if (lost) return;
        lost = true;
        more = await store.append([4, 5].map((n) => jobs.Job({ n }))); // arrive during the back-off
        throw new Error("acknowledgement of COMMIT lost");
      },
    });
    await untilAll([sub], { transactionId: first.transactionId, sequence: first.last });
    while (!more) await wait(5);
    expect(await untilAll([sub], { transactionId: more.transactionId, sequence: more.last })).toBe(true);
    await sub.stop();
    const all = [1, 2, 3, 4, 5].map((i) => first.first + i - 1);
    // what makes a "checkpoint behind? then write the page" guard apply 1–3 twice: the retry's page starts at the same event and is longer
    expect(deliveries[0]).toEqual(all.slice(0, 3));
    expect(deliveries[1]).toEqual(all);
    expect(await rowsOf("longer-redelivery")).toEqual(all.map((s) => [s, 1]));
    expect(await target.load()).toEqual(sub.cursor);
    expect(sub.cursor).toEqual({ transactionId: more.transactionId, sequence: more.last });
  });

  it("two concurrent subscribers (separate stores and pools) run the same target while appends stream in, one of them poison: every event applied once, one dead letter", async () => {
    const writer = storeOf();
    const name = "two-subscribers";
    // the target starts at the table's head: only the events appended below are this test's
    const head = (await writer.query(JQ, { settledOnly: true, order: "desc", limit: 1, payload: false, version: false })).settledCursor;
    if (head) await admin.query(`INSERT INTO "${CHECKPOINT}" VALUES ($1, $2::xid8, $3)`, [name, head.transactionId, head.sequence]);
    const one = processor(name, storeOf());
    const two = processor(name, storeOf());
    const sequences: number[] = [];
    let poisonAt = 0;
    let last!: Awaited<ReturnType<PostgresStore["append"]>>;
    for (let i = 1; i <= 40; i++) {
      const size = 1 + (i % 3);
      last = await writer.append(Array.from({ length: size }, (_, j) => jobs.Job({ n: i * 10 + j, ...(i === 17 && j === 1 ? { poison: true } : {}) })));
      for (let s = last.first; s <= last.last; s++) sequences.push(s);
      if (i === 17) poisonAt = last.first + 1;
      await wait(Math.random() * 4);
    }
    expect(await untilAll([one.sub, two.sub], { transactionId: last.transactionId, sequence: last.last })).toBe(true);
    await one.sub.stop();
    await two.sub.stop();
    expect(one.deliveries.length).toBeGreaterThan(0);
    expect(two.deliveries.length).toBeGreaterThan(0);
    expect(await rowsOf(name)).toEqual(sequences.filter((s) => s !== poisonAt).map((s) => [s, 1]));
    expect(await deadOf(name)).toEqual([poisonAt]);
    expect(await one.target.load()).toEqual({ transactionId: last.transactionId, sequence: last.last });
  });
});
