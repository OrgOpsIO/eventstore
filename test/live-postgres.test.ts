import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { z } from "zod";
import { buildSchema, createEventStore, defineEvents, type EventStoreApi } from "../src/index.js";
import { PostgresStore, notifyChannel, printSchemaSql } from "../src/postgres/index.js";

const url = process.env.ES_TEST_DATABASE_URL;
const TABLE = "es_live_events";
const RLS_TABLE = "es_live_rls_events";
const APP_ROLE = "es_live_app";

const notes = defineEvents({
  NoteAdded: { data: z.object({ text: z.string() }), scopes: ["tenantId"] },
});
const schema = buildSchema([notes], { tenantScopeKey: "tenantId", strict: true });
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
const until = async (check: () => boolean, ms = 3_000) => {
  const end = Date.now() + ms;
  while (!check() && Date.now() < end) await wait(10);
  expect(check()).toBe(true);
};
const text = (e: { data: unknown }) => (e.data as { text: string }).text;

describe.skipIf(!url)("the commit doorbell on Postgres (live: true)", () => {
  // two stores on one table stand for two processes
  let writer: PostgresStore;
  let reader: PostgresStore;
  let admin: pg.Pool;

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}" CASCADE`);
    writer = new PostgresStore({ connection: url!, schema, table: TABLE, live: true, poolSize: 4 });
    await writer.ensureInstalled();
    reader = new PostgresStore({ connection: url!, schema, table: TABLE, live: true, poolSize: 4 });
    await reader.ensureInstalled();
  });
  afterAll(async () => {
    await writer.close();
    await reader.close();
    await admin.end();
  });

  const ringsOn = async (store: PostgresStore): Promise<{ hints: (number | null)[]; off: () => void }> => {
    const hints: (number | null)[] = [];
    const off = store.onCommitted!((h) => hints.push(h));
    await until(() => hints.includes(null)); // LISTEN is up once it rang with "unknown"
    return { hints, off };
  };

  it("another process wakes at once, long before its poll interval, and reads the events through its own view", async () => {
    const api = createEventStore({ events: [notes], tenant: { scopeKey: "tenantId" }, store: reader });
    const seen: string[] = [];
    const w = await api.forTenant("t-1").watch(notes.$filter(), (events) => {
      seen.push(...events.map(text));
    });
    const started = Date.now();
    await writer.append([notes.NoteAdded({ text: "hello" }, { tenantId: "t-1" })]);
    await until(() => seen.length === 1);
    expect(Date.now() - started).toBeLessThan(1_000); // the safety-net poll is 5 s
    w.stop();
  });

  it("rings once per append batch, and never for a rolled-back insert", async () => {
    const { hints, off } = await ringsOn(reader);
    hints.length = 0;
    const client = await admin.connect();
    try {
      await client.query("BEGIN");
      await client.query(`INSERT INTO "${TABLE}" (event_type, payload) VALUES ('NoteAdded', '{"noteAddedId":"x","text":"rolled back","scopes":{"tenantId":"t-1"}}')`);
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
    await wait(300);
    expect(hints).toEqual([]);
    await writer.append([notes.NoteAdded({ text: "a" }, { tenantId: "t-1" }), notes.NoteAdded({ text: "b" }, { tenantId: "t-1" })]);
    await until(() => hints.length > 0);
    await wait(100);
    expect(hints).toEqual([null]); // one ring, and it carries nothing
    off();
  });

  it("the notification carries nothing — no sequence, tenant, type, scope or payload", async () => {
    const raw = new pg.Client({ connectionString: url });
    await raw.connect();
    const payloads: string[] = [];
    raw.on("notification", (m) => payloads.push(m.payload ?? ""));
    await raw.query(`LISTEN "${notifyChannel({ schemaName: "public", table: TABLE })}"`);
    await writer.append([notes.NoteAdded({ text: "secret text" }, { tenantId: "tenant-secret" })]);
    await until(() => payloads.length > 0);
    await raw.end();
    expect(payloads.every((p) => p === "")).toBe(true);
  });

  it("a dropped LISTEN connection reconnects and loses no event committed while it was down", async () => {
    const api = createEventStore({ events: [notes], tenant: { scopeKey: "tenantId" }, store: reader });
    const seen: string[] = [];
    const w = await api.forTenant("t-2").watch(notes.$filter(), (events) => {
      seen.push(...events.map(text));
    });
    const channel = notifyChannel({ schemaName: "public", table: TABLE });
    // kill every backend that LISTENs on the channel (the reader's and the writer's doorbell) —
    // once it is there: watch() resolves when the reader has caught up, LISTEN may still be on its way
    let killed = 0;
    const deadline = Date.now() + 3_000;
    while (killed === 0 && Date.now() < deadline) {
      const result = await admin.query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND query ILIKE $1",
        [`%LISTEN "${channel}"%`],
      );
      killed = result.rowCount ?? 0;
      if (killed === 0) await wait(20);
    }
    expect(killed).toBeGreaterThan(0);
    await writer.append([notes.NoteAdded({ text: "while down" }, { tenantId: "t-2" })]);
    await until(() => seen.includes("while down"), 5_000);
    await writer.append([notes.NoteAdded({ text: "after reconnect" }, { tenantId: "t-2" })]);
    await until(() => seen.includes("after reconnect"), 3_000);
    w.stop();
  });

  it("a woken reader does not wait a full poll when an older transaction on another table ends later", async () => {
    // events become settled when an OLDER transaction ends; that end rings nothing
    const api = createEventStore({ events: [notes], tenant: { scopeKey: "tenantId" }, store: reader });
    let seenAt = 0;
    const w = await api.forTenant("t-3").watch(notes.$filter(), () => {
      seenAt = Date.now();
    });
    const other = await admin.connect();
    try {
      await other.query("CREATE TABLE IF NOT EXISTS es_live_other (x int)");
      await other.query("BEGIN");
      await other.query("INSERT INTO es_live_other VALUES (1)"); // holds an xid older than the append
      const started = Date.now();
      await writer.append([notes.NoteAdded({ text: "late settled" }, { tenantId: "t-3" })]);
      await wait(50);
      await other.query("COMMIT");
      await until(() => seenAt > 0, 3_000);
      expect(seenAt - started).toBeLessThan(1_500); // the safety-net poll is 5 s
    } finally {
      other.release();
      w.stop();
    }
  });

  it("without the trigger (install: none, DDL not run) it warns and still wakes at polling's pace", async () => {
    const BARE = "es_live_bare_events";
    await admin.query(`DROP TABLE IF EXISTS "${BARE}" CASCADE`);
    const installer = new PostgresStore({ connection: url!, schema, table: BARE, poolSize: 1 }); // not live: no trigger
    await installer.ensureInstalled();
    const bare = new PostgresStore({ connection: url!, schema, table: BARE, live: true, install: "none", poolSize: 2 });
    try {
      const api = createEventStore({ events: [notes], tenant: { scopeKey: "tenantId" }, store: bare });
      const seen: string[] = [];
      const w = await api.forTenant("t-4").watch(notes.$filter(), (events) => {
        seen.push(...events.map(text));
      });
      await until(() => bare.warnings().some((m) => m.includes("not installed")));
      const started = Date.now();
      await installer.append([notes.NoteAdded({ text: "no trigger" }, { tenantId: "t-4" })]);
      await until(() => seen.length === 1, 3_000);
      expect(Date.now() - started).toBeLessThan(1_500);
      w.stop();
    } finally {
      await bare.close();
      await installer.close();
    }
  });

  it("reports the notification queue fill level for health checks", async () => {
    const usage = await reader.notificationQueueUsage();
    expect(usage).toBeGreaterThanOrEqual(0);
    expect(usage).toBeLessThan(0.01);
  });

  it("printSchemaSql({ live: true }) lists the trigger; without live it does not", () => {
    expect(printSchemaSql(schema, { table: TABLE, live: true })).toContain("AFTER INSERT");
    expect(printSchemaSql(schema, { table: TABLE })).not.toContain("pg_notify");
  });
});

describe.skipIf(!url)("watch under row-level security (live: true, non-owner role)", () => {
  let owner: PostgresStore;
  let app: PostgresStore;
  let api: EventStoreApi;

  beforeAll(async () => {
    const admin = new pg.Pool({ connectionString: url, max: 1 });
    await admin.query(`DROP TABLE IF EXISTS "${RLS_TABLE}" CASCADE`);
    await admin.query(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${APP_ROLE}') THEN CREATE ROLE ${APP_ROLE} LOGIN PASSWORD '${APP_ROLE}'; END IF; END $$`);
    await admin.end();
    owner = new PostgresStore({ connection: url!, schema, table: RLS_TABLE, rls: true, live: true, grantExecuteTo: [APP_ROLE], poolSize: 2 });
    await owner.ensureInstalled();
    await owner.withClient(async (c) => {
      await c.query(`GRANT USAGE ON SCHEMA public TO ${APP_ROLE}`);
      await c.query(`GRANT SELECT, INSERT ON "${RLS_TABLE}" TO ${APP_ROLE}`);
      await c.query(`GRANT USAGE, SELECT ON SEQUENCE "${RLS_TABLE}_sequence_number_seq" TO ${APP_ROLE}`);
    });
    const appUrl = new URL(url!);
    appUrl.username = APP_ROLE;
    appUrl.password = APP_ROLE;
    app = new PostgresStore({ connection: appUrl.toString(), schema, table: RLS_TABLE, rls: true, live: true, install: "none", poolSize: 4 });
    api = createEventStore({ events: [notes], tenant: { scopeKey: "tenantId" }, store: app });
  });
  afterAll(async () => {
    await app.close();
    await owner.close();
  });

  it("a tenant's watcher is woken by any commit but reads only its own tenant, through its session", async () => {
    const seenA: string[] = [];
    const seenB: string[] = [];
    const a = await api.forTenant("tenant-a").watch(notes.$filter(), (events) => {
      seenA.push(...events.map(text));
    });
    const b = await api.forTenant("tenant-b").watch(notes.$filter(), (events) => {
      seenB.push(...events.map(text));
    });
    await api.forTenant("tenant-b").append([notes.NoteAdded({ text: "for b" }, { tenantId: "tenant-b" })]);
    await api.forTenant("tenant-a").append([notes.NoteAdded({ text: "for a" }, { tenantId: "tenant-a" })]);
    await until(() => seenA.length === 1 && seenB.length === 1);
    await wait(100);
    expect(seenA).toEqual(["for a"]);
    expect(seenB).toEqual(["for b"]);
    a.stop();
    b.stop();
  });

  it("the root view under rls cannot watch: the read is refused and the watch rejects", async () => {
    await expect(api.watch(notes.$filter(), () => undefined)).rejects.toThrow(/withTenant/);
  });
});
