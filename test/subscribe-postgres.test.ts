import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { z } from "zod";
import { buildSchema, compareCursor, cursorOf, defineEvents, type Cursor, type RecordedEvent } from "../src/index.js";
import { toPayload } from "../src/internal.js";
import { PostgresStore, printSchemaSql } from "../src/postgres/index.js";
import { subscribe, type CheckpointTarget, type Subscription } from "../src/subscribe/index.js";

// Stage 2 against real Postgres: AppendResult.transactionId (decision 10), the watermark and
// hasPassed (decision 11), processor mode with a poison event (decisions 12, 13). Model §6: tests
// 2, 3, 5–8, 27. Tables of their own, dropped before and after.

const url = process.env.ES_TEST_DATABASE_URL;

const ab = defineEvents({
  A: { data: z.object({ n: z.number(), poison: z.boolean().optional() }) },
  B: { data: z.object({ n: z.number() }) },
});
const schema = buildSchema([ab], { strict: false });
const TABLE = "es_sub2_events";
const ROWS = "es_sub2_rows";
const CHECKPOINT = "es_sub2_checkpoint";
const DEAD = "es_sub2_dead_letters";

const tenantEvents = defineEvents({ Noted: { data: z.object({ n: z.number() }), scopes: ["tenantId"] } });
const tenantSchema = buildSchema([tenantEvents], { strict: true, tenantScopeKey: "tenantId" });
const RLS_TABLE = "es_sub2_rls";
const APP_ROLE = "es_app";

/** Insert the wire payload of an `A` the store would write, through a raw client (inside its transaction). */
async function rawInsertA(client: pg.PoolClient, n: number): Promise<{ seq: number; xid: string }> {
  const event = ab.A({ n });
  const r = await client.query<{ seq: string; xid: string }>(
    `INSERT INTO "${TABLE}" (event_type, payload) VALUES ($1, $2::jsonb) RETURNING sequence_number::text AS seq, transaction_id::text AS xid`,
    [event.type, JSON.stringify(toPayload(event, "aId"))],
  );
  return { seq: Number(r.rows[0]!.seq), xid: r.rows[0]!.xid };
}

/** Loop `whenCaughtUp()` until `hasPassed(token)` or a deadline. */
async function untilPassed(sub: Subscription, token: Cursor, ms = 2_000): Promise<boolean> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    await sub.whenCaughtUp();
    if (sub.hasPassed(token)) return true;
  }
  return sub.hasPassed(token);
}

describe("printSchemaSql: the append function is unchanged", () => {
  it("still installs es_append_if_v5 and no _v6", () => {
    const sql = printSchemaSql(schema);
    expect(sql).toContain("es_append_if_v5");
    expect(sql).not.toContain("_v6");
  });
});

describe.skipIf(!url)("PostgresStore: transaction id, watermark and processor mode", () => {
  let admin: pg.Pool;
  let store: PostgresStore;
  const xidsOf = async (first: number, last: number): Promise<string[]> =>
    (await admin.query<{ x: string }>(`SELECT DISTINCT transaction_id::text AS x FROM "${TABLE}" WHERE sequence_number BETWEEN $1 AND $2`, [first, last])).rows.map((r) => r.x);

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 6 });
    for (const t of [TABLE, ROWS, CHECKPOINT, DEAD]) await admin.query(`DROP TABLE IF EXISTS "${t}" CASCADE`);
    store = new PostgresStore({ connection: url!, schema, table: TABLE, poolSize: 6 });
    await store.ensureInstalled();
  });

  afterAll(async () => {
    await store?.close();
    for (const t of [TABLE, ROWS, CHECKPOINT, DEAD]) await admin.query(`DROP TABLE IF EXISTS "${t}" CASCADE`);
    await admin.end();
  });

  it("transactionId is the rows' transaction id: append, appendIf, a 500-event batch", async () => {
    const one = await store.append([ab.B({ n: 1 })]);
    expect(await xidsOf(one.first, one.last)).toEqual([one.transactionId]);
    const query = { types: ["B"] };
    const ctx = await store.query(query);
    const conditional = await store.appendIf([ab.B({ n: 2 })], ctx.ctx);
    if (!conditional.ok) throw new Error("expected the append to commit");
    expect(await xidsOf(conditional.appended.first, conditional.appended.last)).toEqual([conditional.appended.transactionId]);
    const stale = await store.appendIf([ab.B({ n: 3 })], ctx.ctx);
    expect(stale.ok).toBe(false); // a conflict has no token
    const batch = await store.append(Array.from({ length: 500 }, (_, i) => ab.B({ n: i })));
    expect(batch.count).toBe(500);
    expect(await xidsOf(batch.first, batch.last)).toEqual([batch.transactionId]);
    expect(BigInt(batch.transactionId) > BigInt(conditional.appended.transactionId)).toBe(true);
  });

  it("the watermark comes from the read's own snapshot: settled exactly below it", async () => {
    const older = await admin.connect();
    let open = true;
    try {
      await older.query("BEGIN");
      const held = await rawInsertA(older, 100); // an open transaction with an xid
      const younger = await store.append([ab.A({ n: 101 })]);
      const result = await store.query({ types: ["A", "B"] });
      expect(result.watermark).toBeDefined();
      const watermark = BigInt(result.watermark!);
      expect(watermark <= BigInt(held.xid)).toBe(true);
      expect(result.events.some((e) => e.sequence === younger.last && !e.settled)).toBe(true);
      for (const event of result.events) expect(event.settled).toBe(BigInt(event.transactionId) < watermark);
      await older.query("COMMIT");
      open = false;
      const after = await store.query({ types: ["Nothing"] });
      expect(after.events).toEqual([]);
      expect(BigInt(after.watermark!) > BigInt(held.xid)).toBe(true);
    } finally {
      if (open) await older.query("ROLLBACK");
      older.release();
    }
  });

  it("a subscriber passes a write it does not match by the watermark of its empty page", async () => {
    await store.ensureInstalled();
    const sub = subscribe("pg-rw", { types: ["Unmatched"] }, () => {}, { store, pollIntervalMs: 50 });
    try {
      await sub.whenCaughtUp();
      const b = await store.append([ab.B({ n: 7 })]);
      const token = { transactionId: b.transactionId, sequence: b.last };
      expect(await untilPassed(sub, token)).toBe(true);
      expect(sub.cursor).toBeNull();
      expect(BigInt(sub.watermark!) > BigInt(token.transactionId)).toBe(true);
    } finally {
      await sub.stop();
    }
  });

  it("an older open transaction holds the watermark back until it ends", async () => {
    const sub = subscribe("pg-held", { types: ["A"] }, () => {}, { store, pollIntervalMs: 50 });
    const older = await admin.connect();
    let open = true;
    try {
      await older.query("BEGIN");
      const olderXid = (await older.query<{ x: string }>("SELECT pg_current_xact_id()::text AS x")).rows[0]!.x;
      const younger = await store.append([ab.B({ n: 8 })]); // not matched
      const token = { transactionId: younger.transactionId, sequence: younger.last };
      await sub.whenCaughtUp();
      await sub.whenCaughtUp();
      expect(sub.hasPassed(token)).toBe(false);
      expect(BigInt(sub.watermark!) <= BigInt(olderXid)).toBe(true);
      const late = await rawInsertA(older, 9); // matched: lower xid, higher sequence
      expect(late.xid).toBe(olderXid);
      expect(late.seq).toBeGreaterThan(younger.last);
      await older.query("COMMIT");
      open = false;
      expect(await untilPassed(sub, token)).toBe(true);
      expect(sub.cursor).toEqual({ transactionId: olderXid, sequence: late.seq });
    } finally {
      if (open) await older.query("ROLLBACK");
      older.release();
      await sub.stop();
    }
  });

  it("strict: a matching event in flight at the watermark is not passed until delivered", async () => {
    const sub = subscribe("pg-strict", { types: ["A"] }, () => {}, { store, pollIntervalMs: 50 });
    const inFlight = await admin.connect();
    let open = true;
    try {
      await sub.whenCaughtUp();
      await inFlight.query("BEGIN");
      const held = await rawInsertA(inFlight, 10);
      const token = { transactionId: held.xid, sequence: held.seq };
      await sub.whenCaughtUp();
      await sub.whenCaughtUp();
      expect(BigInt(sub.watermark!) <= BigInt(held.xid)).toBe(true);
      expect(sub.hasPassed(token)).toBe(false);
      await inFlight.query("COMMIT");
      open = false;
      expect(await untilPassed(sub, token)).toBe(true);
      expect(sub.cursor).toEqual(token); // by the cursor
    } finally {
      if (open) await inFlight.query("ROLLBACK");
      inFlight.release();
      await sub.stop();
    }
  });

  it("batchSize 0 is refused before polling (R2.2); batchSize 1 delivers a matching event before passing its token", async () => {
    const a = await store.append([ab.A({ n: 1 })]);
    const token = { transactionId: a.transactionId, sequence: a.last };
    // a `limit: 0` page is empty and carries a watermark above the token: it must never be read as "caught up"
    const zeroPage = await store.query({ types: ["A"] }, { settledOnly: true, cursor: null, limit: 0, version: false });
    expect(zeroPage.events).toEqual([]);
    expect(BigInt(zeroPage.watermark!) > BigInt(token.transactionId)).toBe(true);
    let handled = 0;
    expect(() => subscribe("zero", { types: ["A"] }, () => void handled++, { store, batchSize: 0 })).toThrow(/batchSize/);

    const seen: number[] = [];
    const one = subscribe("one", { types: ["A"] }, (events) => void seen.push(...events.map((e) => e.sequence)), { store, batchSize: 1, pollIntervalMs: 20 });
    try {
      expect(await untilPassed(one, token)).toBe(true);
      expect(seen).toContain(a.last);
      expect(one.cursor!.sequence).toBeGreaterThanOrEqual(a.last);
    } finally {
      await one.stop();
    }
    expect(handled).toBe(0);
  });

  describe("processor mode: rows and checkpoint in one transaction (the README pattern: lock, apply what is beyond)", () => {
    const AQ = { types: ["A"] };

    beforeAll(async () => {
      await admin.query(`TRUNCATE "${TABLE}" RESTART IDENTITY`);
      await admin.query(`CREATE TABLE "${ROWS}" (name text NOT NULL, seq bigint NOT NULL, applied int NOT NULL DEFAULT 1, PRIMARY KEY (name, seq))`);
      await admin.query(`CREATE TABLE "${CHECKPOINT}" (name text PRIMARY KEY, xid xid8, seq bigint)`);
      await admin.query(`CREATE TABLE "${DEAD}" (name text NOT NULL, xid xid8 NOT NULL, seq bigint NOT NULL, event_type text NOT NULL, event_id text NOT NULL, error text NOT NULL)`);
    });

    /**
     * The README pattern: the checkpoint row, created on first use, locked for this transaction (a
     * second writer waits here) and read. `null` = nothing applied yet.
     */
    async function lockCheckpoint(client: pg.PoolClient, name: string): Promise<Cursor | null> {
      await client.query(`INSERT INTO "${CHECKPOINT}" (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, [name]);
      const r = await client.query<{ transactionId: string | null; sequence: string | null }>(
        `SELECT xid::text AS "transactionId", seq::text AS sequence FROM "${CHECKPOINT}" WHERE name = $1 FOR UPDATE`,
        [name],
      );
      const row = r.rows[0]!;
      return row.transactionId === null ? null : { transactionId: row.transactionId, sequence: Number(row.sequence) };
    }
    /** Only what is beyond the checkpoint is new: a redelivered page can start below it and end beyond it. */
    const beyond = (events: readonly RecordedEvent[], checkpoint: Cursor | null) => events.filter((e) => checkpoint === null || compareCursor(e, checkpoint) > 0);
    const moveCheckpoint = (client: pg.PoolClient, name: string, c: Cursor) =>
      client.query(`UPDATE "${CHECKPOINT}" SET xid = $2::xid8, seq = $3 WHERE name = $1`, [name, c.transactionId, c.sequence]);

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

    function processor(name: string, options: { loseFirstAck?: boolean; beforeRetry?: () => Promise<unknown> } = {}) {
      let lose = options.loseFirstAck ?? false;
      const stats = { deliveries: 0, guardRollbacks: 0, skips: 0, onError: [] as number[] };
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
          stats.skips++;
          await inTransaction(async (client) => {
            if (beyond([event], await lockCheckpoint(client, name)).length === 0) return false; // skipped already
            await moveCheckpoint(client, name, cursorOf([event])!);
            // a dead letter: cursor, type, id and the error — never the payload
            await client.query(`INSERT INTO "${DEAD}" (name, xid, seq, event_type, event_id, error) VALUES ($1, $2::xid8, $3, $4, $5, $6)`, [
              name,
              event.transactionId,
              event.sequence,
              event.type,
              event.id,
              String(error),
            ]);
            return true;
          });
        },
      };
      const handler = async (events: readonly RecordedEvent[]) => {
        stats.deliveries++;
        if (events.some((e) => (e.data as { poison?: boolean }).poison)) throw new Error("poison");
        await inTransaction(async (client) => {
          const fresh = beyond(events, await lockCheckpoint(client, name));
          if (fresh.length === 0) {
            stats.guardRollbacks++; // all applied already: a redelivery
            return false;
          }
          // `applied` counts: a second application is visible, not refused by the key
          await client.query(`INSERT INTO "${ROWS}" AS r (name, seq) SELECT $1, unnest($2::bigint[]) ON CONFLICT (name, seq) DO UPDATE SET applied = r.applied + 1`, [
            name,
            fresh.map((e) => e.sequence),
          ]);
          await moveCheckpoint(client, name, cursorOf(events)!);
          return true;
        });
        if (lose) {
          lose = false;
          await options.beforeRetry?.();
          throw new Error("acknowledgement of COMMIT lost");
        }
      };
      const sub = subscribe(name, AQ, handler, {
        store,
        target,
        batchSize: 5,
        pollIntervalMs: 50,
        onError: (error, batch) => {
          stats.onError.push(batch.length);
          return error instanceof Error && error.message === "poison" ? "skip" : "retry";
        },
      });
      return { sub, stats, target };
    }

    const rowsOf = async (name: string) =>
      (await admin.query<{ s: string; applied: number }>(`SELECT seq::text AS s, applied FROM "${ROWS}" WHERE name = $1 ORDER BY seq`, [name])).rows.map((r) => [Number(r.s), r.applied]);

    it("(a) a lost COMMIT acknowledgement is redelivered and applied once", async () => {
      const appended = await store.append(Array.from({ length: 10 }, (_, i) => ab.A({ n: i })));
      const { sub, stats } = processor("lost-ack", { loseFirstAck: true });
      try {
        while (sub.cursor?.sequence !== appended.last) await sub.whenCaughtUp();
      } finally {
        await sub.stop();
      }
      expect(await rowsOf("lost-ack")).toEqual(Array.from({ length: 10 }, (_, i) => [i + 1, 1]));
      expect(stats.guardRollbacks).toBe(1);
    });

    it("(b) a poison event mid-page: neighbours applied, one dead letter, target checkpoint = sub.cursor", async () => {
      const appended = await store.append(Array.from({ length: 7 }, (_, i) => ab.A({ n: i, ...(i === 3 ? { poison: true } : {}) })));
      const poisonSeq = appended.first + 3;
      // start where (a) left off, so this page holds exactly the 7 new events
      await admin.query(`INSERT INTO "${CHECKPOINT}" (name, xid, seq) SELECT 'poison', xid, seq FROM "${CHECKPOINT}" WHERE name = 'lost-ack'`);
      const { sub, stats, target } = processor("poison");
      try {
        while (sub.cursor?.sequence !== appended.last) await sub.whenCaughtUp();
      } finally {
        await sub.stop();
      }
      const expected = Array.from({ length: 7 }, (_, i) => appended.first + i).filter((s) => s !== poisonSeq);
      expect((await rowsOf("poison")).map(([seq]) => seq)).toEqual(expected);
      expect((await rowsOf("poison")).every(([, applied]) => applied === 1)).toBe(true);
      const dead = (await admin.query(`SELECT seq::int AS seq, event_type, error FROM "${DEAD}" WHERE name = 'poison'`)).rows;
      expect(dead).toEqual([{ seq: poisonSeq, event_type: "A", error: "Error: poison" }]);
      expect(stats.skips).toBe(1);
      expect(stats.onError).toEqual([5, 1]); // the page (batchSize 5 holds the poison), then the isolated event
      expect(await target.load()).toEqual(sub.cursor);
    });

    it("(c) a fresh subscription resumes at the target checkpoint and delivers exactly the 3 new events", async () => {
      const more = await store.append([ab.A({ n: 1 }), ab.A({ n: 2 }), ab.A({ n: 3 })]);
      const { sub, stats } = processor("poison");
      try {
        const token = { transactionId: more.transactionId, sequence: more.last };
        expect(await untilPassed(sub, token)).toBe(true);
      } finally {
        await sub.stop();
      }
      expect(stats.deliveries).toBe(1);
      const rows = (await rowsOf("poison")).map(([seq]) => seq);
      expect(rows.slice(-3)).toEqual([more.first, more.first + 1, more.last]);
      expect(rows).toHaveLength(9);
    });

    it("(d) a lost COMMIT acknowledgement with new events before the retry: the longer page is applied once", async () => {
      // start at the table's head, so the first page holds exactly the 3 events below
      await admin.query(`INSERT INTO "${CHECKPOINT}" (name, xid, seq) SELECT 'longer', xid, seq FROM "${CHECKPOINT}" WHERE name = 'poison'`);
      const first = await store.append([ab.A({ n: 1 }), ab.A({ n: 2 }), ab.A({ n: 3 })]);
      let more: Awaited<ReturnType<PostgresStore["append"]>> | undefined;
      const { sub, stats, target } = processor("longer", {
        loseFirstAck: true,
        beforeRetry: async () => (more = await store.append([ab.A({ n: 4 }), ab.A({ n: 5 })])),
      });
      try {
        const deadline = Date.now() + 5_000;
        while (!more && Date.now() < deadline) await sub.whenCaughtUp();
        expect(await untilPassed(sub, { transactionId: more!.transactionId, sequence: more!.last }, 5_000)).toBe(true);
      } finally {
        await sub.stop();
      }
      // batchSize 5: the retried page is [1..5], longer than the committed [1..3]; only 4 and 5 are new
      expect(stats.deliveries).toBe(2);
      const all = [first.first, first.first + 1, first.last, more!.first, more!.last];
      expect(await rowsOf("longer")).toEqual(all.map((seq) => [seq, 1]));
      expect(await target.load()).toEqual(sub.cursor);
      expect(sub.cursor).toEqual({ transactionId: more!.transactionId, sequence: more!.last });
    });
  });
});

describe.skipIf(!url)("PostgresStore with rls: the transaction id of a tenant session (non-owner role)", () => {
  let admin: pg.Pool;
  let owner: PostgresStore;
  let app: PostgresStore;

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await admin.query(`DROP TABLE IF EXISTS "${RLS_TABLE}" CASCADE`);
    await admin.query(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${APP_ROLE}') THEN CREATE ROLE ${APP_ROLE} LOGIN PASSWORD '${APP_ROLE}'; END IF; END $$`);
    owner = new PostgresStore({ connection: url!, schema: tenantSchema, table: RLS_TABLE, rls: true, grantExecuteTo: [APP_ROLE], poolSize: 2 });
    await owner.ensureInstalled();
    await owner.withClient(async (c) => {
      await c.query(`GRANT USAGE ON SCHEMA public TO ${APP_ROLE}`);
      await c.query(`GRANT SELECT, INSERT ON "${RLS_TABLE}" TO ${APP_ROLE}`);
      await c.query(`GRANT USAGE, SELECT ON SEQUENCE "${RLS_TABLE}_sequence_number_seq" TO ${APP_ROLE}`);
    });
    const appUrl = new URL(url!);
    appUrl.username = APP_ROLE;
    appUrl.password = APP_ROLE;
    app = new PostgresStore({ connection: appUrl.toString(), schema: tenantSchema, table: RLS_TABLE, rls: true, install: "none", poolSize: 2 });
  });

  afterAll(async () => {
    await app?.close();
    await owner?.close();
    await admin.query(`DROP TABLE IF EXISTS "${RLS_TABLE}" CASCADE`);
    await admin.end();
  });

  it("append through withTenant returns the rows' transaction id; its reads carry a watermark", async () => {
    const tenantA = app.withTenant("tenant-a");
    const appended = await tenantA.append([tenantEvents.Noted({ n: 1 }, { tenantId: "tenant-a" }), tenantEvents.Noted({ n: 2 }, { tenantId: "tenant-a" })]);
    const xids = (await admin.query<{ x: string }>(`SELECT DISTINCT transaction_id::text AS x FROM "${RLS_TABLE}" WHERE sequence_number BETWEEN $1 AND $2`, [appended.first, appended.last])).rows.map((r) => r.x);
    expect(xids).toEqual([appended.transactionId]);
    const read = await tenantA.query(tenantEvents.$filter());
    expect(read.events.map((e) => e.transactionId)).toEqual([appended.transactionId, appended.transactionId]);
    expect(BigInt(read.watermark!) > BigInt(appended.transactionId)).toBe(true);
    // another tenant sees none of tenant-a's rows; only the database-wide watermark
    const other = await app.withTenant("tenant-b").query(tenantEvents.$filter());
    expect(other.events).toEqual([]);
    expect(other.watermark).toBeDefined();
  });
});
