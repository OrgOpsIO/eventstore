import { afterEach, beforeEach, describe, expect, it } from "vitest";
import pg from "pg";
import { z } from "zod";
import { buildSchema, createEventStore, defineEvents, type RecordedEvent } from "../src/index.js";
import { PostgresStore, printAdoptSql } from "../src/postgres/index.js";

const url = process.env.ES_TEST_DATABASE_URL;
const TABLE = "es_adopt_events";

const docs = defineEvents({
  DocumentCreated: { data: z.object({ title: z.string() }), scopes: ["tenantId"] },
  DocumentEdited: { data: z.object({ body: z.string() }), scopes: ["documentCreatedId", "tenantId"] },
});
const schema = buildSchema([docs], { tenantScopeKey: "tenantId", strict: true });
const adopt = { columns: { sequence: "id", type: "eventtype" } } as const;
const ROWS = 300;

/** The consumer's table as it is today: id / eventtype / payload / recorded_at, its own indexes, rows in the convention. */
async function legacyTable(admin: pg.Pool): Promise<void> {
  await admin.query(`DROP TABLE IF EXISTS "${TABLE}" CASCADE`);
  await admin.query(`CREATE TABLE "${TABLE}" (id BIGSERIAL PRIMARY KEY, eventtype TEXT NOT NULL, payload JSONB NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
  await admin.query(`CREATE INDEX "${TABLE}_payload_gin" ON "${TABLE}" USING GIN (payload)`);
  await admin.query(`CREATE UNIQUE INDEX "${TABLE}_doc_title" ON "${TABLE}" ((payload->>'title')) WHERE eventtype = 'DocumentCreated'`);
  const values: string[] = [];
  for (let i = 1; i <= ROWS; i++) {
    const doc = `doc-${Math.ceil(i / 3)}`;
    const payload =
      i % 3 === 1
        ? { documentCreatedId: doc, title: `T${i}`, scopes: { tenantId: `t-${i % 2}` } }
        : { documentEditedId: `e-${i}`, body: `b${i}`, scopes: { documentCreatedId: doc, tenantId: `t-${Math.ceil(i / 3) % 2 === 0 ? 0 : 1}` } };
    values.push(`('${i % 3 === 1 ? "DocumentCreated" : "DocumentEdited"}', '${JSON.stringify(payload)}'::jsonb, timestamptz '2025-01-01' + interval '${i} minutes')`);
  }
  await admin.query(`INSERT INTO "${TABLE}" (eventtype, payload, recorded_at) VALUES ${values.join(", ")}`);
}

const legacySnapshot = async (admin: pg.Pool) =>
  (await admin.query<{ id: string; eventtype: string; payload: unknown; at: string }>(`SELECT id::text, eventtype, payload, recorded_at::text AS at FROM "${TABLE}" ORDER BY "${TABLE}".id`)).rows;

describe.skipIf(!url)("adopting an existing events table (postgres: { adopt })", () => {
  let admin: pg.Pool;
  const open: PostgresStore[] = [];
  const store = (extra: Record<string, unknown> = {}) => {
    const s = new PostgresStore({ connection: url!, schema, table: TABLE, adopt, poolSize: 3, ...extra });
    open.push(s);
    return s;
  };

  beforeEach(async () => {
    admin = new pg.Pool({ connectionString: url, max: 2 });
    await legacyTable(admin);
  });
  afterEach(async () => {
    for (const s of open.splice(0)) await s.close();
    await admin.end();
  });

  it("takes the table over in place: same rows, payloads, types, times and order — and no row rewritten", async () => {
    const before = await legacySnapshot(admin);
    const fileBefore = (await admin.query(`SELECT pg_relation_filenode('"${TABLE}"') AS f`)).rows[0].f;
    const s = store();
    await s.ensureInstalled();
    expect((await admin.query(`SELECT pg_relation_filenode('"${TABLE}"') AS f`)).rows[0].f).toBe(fileBefore); // metadata-only
    const read = await s.query({ types: ["DocumentCreated", "DocumentEdited"] });
    expect(read.events).toHaveLength(ROWS);
    const after = (await admin.query<{ id: string; event_type: string; payload: unknown; at: string }>(`SELECT sequence_number::text AS id, event_type, payload, recorded_at::text AS at FROM "${TABLE}" ORDER BY sequence_number`)).rows;
    expect(after.map((r) => [r.id, r.event_type, r.payload, r.at])).toEqual(before.map((r) => [r.id, r.eventtype, r.payload, r.at]));
    expect(read.events.map((e) => e.sequence)).toEqual(before.map((r) => Number(r.id)));
    expect(s.warnings().some((w) => w.includes(`took over public.${TABLE} (${ROWS} rows`))).toBe(true);
    expect(s.warnings().some((w) => w.includes(`${TABLE}_payload_gin is kept`))).toBe(true);
  });

  it("serves the adopted rows through the store: scopes, guards, and new appends after every legacy row", async () => {
    const s = store();
    const api = createEventStore({ events: [docs], tenant: { scopeKey: "tenantId" }, store: s });
    const t1 = api.forTenant("t-1");
    const ctx = await t1.query(docs.$scope("documentCreatedId", "doc-1"));
    expect(ctx.events.map((e) => e.type)).toEqual(["DocumentCreated", "DocumentEdited", "DocumentEdited"]);
    expect(ctx.events[0]!.transactionId).toBe("3");
    const appended = await t1.appendIfOrThrow([docs.DocumentEdited({ body: "new" }, { documentCreatedId: "doc-1", tenantId: "t-1" })], ctx.ctx);
    expect(appended.first).toBe(ROWS + 1);
    await expect(t1.appendIfOrThrow([docs.DocumentEdited({ body: "stale" }, { documentCreatedId: "doc-1", tenantId: "t-1" })], ctx.ctx)).rejects.toThrow();
    // a durable subscriber from the beginning sees every legacy row once, then the new one, in order
    const seen: RecordedEvent[] = [];
    const sub = await api.subscribe("all", { types: ["DocumentCreated", "DocumentEdited"] }, (events) => {
      seen.push(...events);
    }, { from: "beginning" });
    await sub.whenCaughtUp();
    await sub.stop();
    expect(seen.map((e) => e.sequence)).toEqual(Array.from({ length: ROWS + 1 }, (_, i) => i + 1));
    // the legacy unique index still guards what it guarded
    await expect(api.forTenant("t-0").append([docs.DocumentCreated({ title: "T1" }, { tenantId: "t-0" })])).rejects.toThrow();
  });

  it("is idempotent: the next boot finds an adopted table and does nothing", async () => {
    await store().ensureInstalled();
    const again = store();
    await again.ensureInstalled();
    expect(again.warnings().filter((w) => w.startsWith("adopt:"))).toEqual([]);
    expect((await again.adoptionPlan()).state).toBe("current");
  });

  it("concurrent boots adopt exactly once", async () => {
    const stores = [store(), store(), store()];
    await Promise.all(stores.map((s) => s.ensureInstalled()));
    expect(stores.filter((s) => s.warnings().some((w) => w.includes("took over"))).length).toBe(1);
    expect((await stores[0]!.query({ types: ["DocumentCreated"] })).events.length).toBe(ROWS / 3);
  });

  it('mode "check" changes nothing and fails the install with the plan', async () => {
    const s = store({ adopt: { ...adopt, mode: "check" } });
    await expect(s.ensureInstalled()).rejects.toThrow(/needs adopting — nothing was changed[\s\S]*RENAME COLUMN "id" TO sequence_number/);
    const cols = (await admin.query(`SELECT column_name FROM information_schema.columns WHERE table_name = '${TABLE}' ORDER BY ordinal_position`)).rows.map((r) => r.column_name);
    expect(cols).toEqual(["id", "eventtype", "payload", "recorded_at"]);
  });

  it("refuses a table it cannot adopt and leaves it exactly as it was", async () => {
    await admin.query(`ALTER TABLE "${TABLE}" ADD COLUMN owner_id TEXT NOT NULL DEFAULT 'x'`);
    await admin.query(`ALTER TABLE "${TABLE}" ALTER COLUMN owner_id DROP DEFAULT`); // NOT NULL, no default: every append would fail
    const s = store();
    await expect(s.ensureInstalled()).rejects.toThrow(/cannot adopt[\s\S]*"owner_id" is NOT NULL without a default/);
    const cols = (await admin.query(`SELECT column_name FROM information_schema.columns WHERE table_name = '${TABLE}' ORDER BY ordinal_position`)).rows.map((r) => r.column_name);
    expect(cols).toEqual(["id", "eventtype", "payload", "recorded_at", "owner_id"]);
  });

  it("refuses a legacy transaction id that would sort legacy rows after new ones, and changes nothing", async () => {
    const s = store({ adopt: { ...adopt, legacyTransactionId: "999999999999" } });
    await expect(s.ensureInstalled()).rejects.toThrow(/not below the oldest running transaction/);
    const cols = (await admin.query(`SELECT column_name FROM information_schema.columns WHERE table_name = '${TABLE}' ORDER BY ordinal_position`)).rows.map((r) => r.column_name);
    expect(cols).toEqual(["id", "eventtype", "payload", "recorded_at"]);
  });

  it("refuses a sequence column holding NULLs — such a row would be invisible to every cursor read", async () => {
    await admin.query(`ALTER TABLE "${TABLE}" DROP CONSTRAINT "${TABLE}_pkey"`);
    await admin.query(`ALTER TABLE "${TABLE}" ALTER COLUMN id DROP NOT NULL`);
    await admin.query(`INSERT INTO "${TABLE}" (id, eventtype, payload) VALUES (NULL, 'DocumentCreated', '{"title":"orphan"}')`);
    await expect(store().ensureInstalled()).rejects.toThrow(/"id" holds NULLs/);
  });

  it("under rls, a foreign permissive policy on the legacy table stops the adoption", async () => {
    await admin.query(`ALTER TABLE "${TABLE}" ENABLE ROW LEVEL SECURITY`);
    await admin.query(`CREATE POLICY legacy_reporting ON "${TABLE}" FOR SELECT USING (true)`);
    await expect(store({ rls: true }).ensureInstalled()).rejects.toThrow(/policy "legacy_reporting" is permissive/);
  });

  it("an absent table: check mode and requireExisting refuse; migrate creates it and says so", async () => {
    await admin.query(`DROP TABLE "${TABLE}" CASCADE`);
    await expect(store({ adopt: { ...adopt, mode: "check" } }).ensureInstalled()).rejects.toThrow(/no table .* to adopt — nothing was changed/);
    await expect(store({ adopt: { ...adopt, requireExisting: true } }).ensureInstalled()).rejects.toThrow(/no table/);
    expect((await admin.query(`SELECT to_regclass('"${TABLE}"') AS t`)).rows[0].t).toBeNull();
    const s = store();
    await s.ensureInstalled();
    expect(s.warnings().some((w) => w.includes("a new, empty one was created"))).toBe(true);
  });

  it("printAdoptSql({ plan }) prints exactly what the installer would run", async () => {
    const plan = await store().adoptionPlan();
    expect(printAdoptSql({ plan }).split("\n").slice(1)).toEqual(plan.statements.map((st) => `${st};`));
  });

  it("printAdoptSql lists the same statements the installer plans", async () => {
    const plan = await store().adoptionPlan();
    const text = printAdoptSql({ adopt, table: TABLE });
    for (const statement of plan.statements) expect(text).toContain(`${statement};`);
  });
});
