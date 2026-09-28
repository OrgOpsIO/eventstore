import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { z } from "zod";
import { buildSchema, createEventStore, defineEvents, type EventStoreApi, type RecordedEvent } from "../src/index.js";
import { PostgresStore } from "../src/postgres/index.js";

const url = process.env.ES_TEST_DATABASE_URL;
const TABLE = "es_lean_events";

const pages = defineEvents({
  DocumentPagesRead: { data: z.object({ html: z.string() }), scopes: ["documentCreatedId", "tenantId"] },
});
const schema = buildSchema([pages], { tenantScopeKey: "tenantId", strict: true });
const HTML = `<html>${"<p>Tarifrechner Seite mit sehr viel Inhalt.</p>".repeat(9_000)}</html>`; // ~400 kB
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
const until = async (check: () => boolean, ms = 5_000) => {
  const end = Date.now() + ms;
  while (!check() && Date.now() < end) await wait(10);
  expect(check()).toBe(true);
};

/** Counts the bytes of every row the pg client receives, in this process — what the database ships to the app. */
function meterRows(): { bytes: () => number; reset: () => void; restore: () => void } {
  const proto = pg.Client.prototype as unknown as { query: (...args: unknown[]) => unknown };
  const original = proto.query;
  let bytes = 0;
  proto.query = function (this: unknown, ...args: unknown[]) {
    const callback = typeof args[args.length - 1] === "function" ? (args.pop() as (err: unknown, result?: unknown) => void) : undefined;
    const pending = (original.apply(this, args) as Promise<{ rows?: unknown[] }>).then((result) => {
      bytes += JSON.stringify(result?.rows ?? []).length;
      return result;
    });
    if (!callback) return pending;
    pending.then(
      (result) => callback(null, result),
      (err) => callback(err),
    );
    return undefined;
  };
  return { bytes: () => bytes, reset: () => (bytes = 0), restore: () => (proto.query = original) };
}

describe.skipIf(!url)("lean live reads: a large payload travels once, or not at all", () => {
  let writer: PostgresStore;
  let reader: PostgresStore;
  let api: EventStoreApi;
  let admin: pg.Pool;
  let meter: ReturnType<typeof meterRows>;

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}" CASCADE`);
    await admin.query("CREATE TABLE IF NOT EXISTS es_lean_other (x int)");
    writer = new PostgresStore({ connection: url!, schema, table: TABLE, live: true, poolSize: 2 });
    await writer.ensureInstalled();
    reader = new PostgresStore({ connection: url!, schema, table: TABLE, live: true, poolSize: 4 });
    await reader.ensureInstalled();
    api = createEventStore({ events: [pages], tenant: { scopeKey: "tenantId" }, store: reader });
    meter = meterRows();
  });
  afterAll(async () => {
    meter.restore();
    await writer.close();
    await reader.close();
    await admin.end();
  });

  const page = (doc: string, tenant: string) => pages.DocumentPagesRead({ html: HTML }, { documentCreatedId: doc, tenantId: tenant });

  it("while an older transaction keeps it unsettled, a waiting watcher does not fetch the payload again and again", async () => {
    const seen: RecordedEvent[] = [];
    const w = await api.forTenant("t-1").watch(pages.$filter(), (events) => {
      seen.push(...events);
    });
    const other = await admin.connect();
    try {
      await other.query("BEGIN");
      await other.query("INSERT INTO es_lean_other VALUES (1)"); // an older transaction: the append stays unsettled
      meter.reset();
      await writer.append([page("doc-1", "t-1")]);
      await wait(1_000); // the watcher re-reads soon and often in this window
      const whileWaiting = meter.bytes();
      await other.query("COMMIT");
      await until(() => seen.length === 1);
      await wait(200);
      // the payload travels when it is delivered — once — not on every re-read while it waited
      expect(whileWaiting).toBeLessThan(HTML.length / 4);
      expect(meter.bytes()).toBeLessThan(HTML.length * 2);
      expect((seen[0]!.data as { html: string }).html.length).toBe(HTML.length);
    } finally {
      other.release();
      w.stop();
    }
  });

  it("data: false delivers ids and scopes, and the payload never leaves the database", async () => {
    const seen: RecordedEvent[] = [];
    const w = await api.forTenant("t-2").watch(pages.$filter(), (events) => {
      seen.push(...events);
    }, { data: false });
    meter.reset();
    await writer.append([page("doc-2", "t-2")]);
    await until(() => seen.length === 1);
    await wait(100);
    expect(meter.bytes()).toBeLessThan(HTML.length / 20);
    expect(seen[0]!.data).toEqual({});
    expect(seen[0]!.scopes).toEqual({ documentCreatedId: "doc-2", tenantId: "t-2" });
    expect(seen[0]!.id.startsWith("~")).toBe(false);
    w.stop();
  });

  it("payload: false delivers type and sequence without reading the payload at all", async () => {
    const seen: RecordedEvent[] = [];
    const w = await api.forTenant("t-3").watch(pages.$filter(), (events) => {
      seen.push(...events);
    }, { payload: false });
    meter.reset();
    const appended = await writer.append([page("doc-3", "t-3")]);
    await until(() => seen.length === 1);
    await wait(100);
    expect(meter.bytes()).toBeLessThan(HTML.length / 20);
    expect(seen.map((e) => [e.type, e.sequence, e.data, e.scopes])).toEqual([["DocumentPagesRead", appended.last, {}, {}]]);
    w.stop();
  });

  it("a lean and a full watcher on the same query do not share a reader", async () => {
    const lean: RecordedEvent[] = [];
    const full: RecordedEvent[] = [];
    const a = await api.forTenant("t-4").watch(pages.$filter(), (events) => {
      lean.push(...events);
    }, { data: false });
    const b = await api.forTenant("t-4").watch(pages.$filter(), (events) => {
      full.push(...events);
    });
    await writer.append([page("doc-4", "t-4")]);
    await until(() => lean.length === 1 && full.length === 1);
    expect(lean[0]!.data).toEqual({});
    expect((full[0]!.data as { html: string }).html.length).toBe(HTML.length);
    a.stop();
    b.stop();
  });

  it("after a sequence/transaction inversion a waiting event is still read again soon, not after the safety-net poll", async () => {
    // E2 gets the lower sequence but the higher transaction id; E1 commits first and is delivered;
    // E2 then waits on an older transaction on another table, whose end rings no doorbell
    await admin.query("CREATE TABLE IF NOT EXISTS es_lean_other (x int)");
    const got: { seq: number; at: number }[] = [];
    const w = await api.forTenant("t-6").watch(pages.$filter(), (events) => {
      for (const e of events) got.push({ seq: e.sequence, at: Date.now() });
    }, { payload: false });
    const pool = new pg.Pool({ connectionString: url, max: 3 });
    const [t1, blocker, t2] = [await pool.connect(), await pool.connect(), await pool.connect()];
    const insert = (client: pg.PoolClient, n: number) =>
      client.query(`INSERT INTO "${TABLE}" (event_type, payload) VALUES ('DocumentPagesRead', jsonb_build_object('html', 'x', 'documentPagesReadId', 'inv-${n}', 'scopes', jsonb_build_object('documentCreatedId', 'd', 'tenantId', 't-6')))`);
    try {
      await t1.query("BEGIN");
      await t1.query("SELECT pg_current_xact_id()"); // t1 takes the lower transaction id
      await blocker.query("BEGIN");
      await blocker.query("INSERT INTO es_lean_other VALUES (1)");
      await t2.query("BEGIN");
      await insert(t2, 2); // lower sequence, higher transaction id
      await insert(t1, 1); // higher sequence, lower transaction id
      await t1.query("COMMIT");
      await until(() => got.length === 1);
      await t2.query("COMMIT");
      await wait(300);
      const released = Date.now();
      await blocker.query("COMMIT");
      await until(() => got.length === 2, 6_000);
      expect(got[1]!.at - released).toBeLessThan(1_500); // the safety-net poll is 5 s
    } finally {
      t1.release();
      blocker.release();
      t2.release();
      await pool.end();
      w.stop();
    }
  });

  it("data: false keeps a custom own id, flat scope back-links, and survives a payload that is not an object", async () => {
    const custom = defineEvents({
      SlugClaimed: { data: z.object({ body: z.string(), accountOpenedId: z.string() }), idKey: "slug" },
    });
    const TABLE2 = "es_lean_custom";
    await admin.query(`DROP TABLE IF EXISTS "${TABLE2}" CASCADE`);
    const customSchema = buildSchema([custom], { scopeKeys: ["accountOpenedId"], strict: false });
    const store = new PostgresStore({ connection: url!, schema: customSchema, table: TABLE2, poolSize: 2 });
    try {
      await store.append([{ type: "SlugClaimed", id: "my-slug", data: { body: HTML, accountOpenedId: "acc-1" } }]);
      await admin.query(`INSERT INTO "${TABLE2}" (event_type, payload) VALUES ('Legacy', '[1,2,3]'::jsonb)`);
      const lean = await store.query({}, { data: false });
      expect(lean.events.map((e) => [e.type, e.id, e.data])).toEqual([
        ["SlugClaimed", "my-slug", { accountOpenedId: "acc-1" }],
        ["Legacy", `~${lean.events[1]!.sequence}`, {}],
      ]);
      expect((await store.query({ scopes: { accountOpenedId: "acc-1" } }, { data: false })).events).toHaveLength(1);
    } finally {
      await store.close();
    }
  });

  it("the cursor read uses the (transaction_id, sequence_number) index instead of scanning the tenant", async () => {
    // enough rows that a scan would show; the delta beyond the cursor is one row
    for (let i = 0; i < 20; i++) await writer.append(Array.from({ length: 50 }, (_, j) => pages.DocumentPagesRead({ html: "x" }, { documentCreatedId: `bulk-${i}-${j}`, tenantId: "t-5" })));
    await admin.query(`ANALYZE "${TABLE}"`);
    const head = await reader.withTenant("t-5").query(pages.$filter(), { settledOnly: true, order: "desc", limit: 1 });
    const { compileQuery } = await import("../src/postgres/sql.js");
    const compiled = compileQuery({ schemaName: "public", table: TABLE }, { types: ["DocumentPagesRead"], scopes: { tenantId: "t-5" } }, { settledOnly: true, cursor: head.settledCursor, limit: 500 }, schema);
    const plan = (await admin.query(`EXPLAIN (FORMAT JSON) ${compiled.sql}`, compiled.params)).rows[0]["QUERY PLAN"];
    expect(JSON.stringify(plan)).toMatch(/xid_seq/);
  });
});
