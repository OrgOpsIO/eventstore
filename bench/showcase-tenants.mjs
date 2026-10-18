// The showcase's stage-6 part: tenants, rls and erasure. Under row-level security with three roles of its
// own — showcase_owner (installs, no superuser, bound by FORCE), showcase_app (the application) and
// showcase_proc (the cross-tenant processor, postgres.processorRole) — in a schema of its own, showcase_rls,
// dropped with the roles at the start and the end. Two tenants with the same offer ids; one processor writes
// both tenants' rows; each tenant sees only its own; then tenant A is erased with the README recipe as
// printed while the processor runs, and nothing of A remains in any kept state. Imported by bench/showcase.mjs.
import { readFileSync } from "node:fs";
import { isDeepStrictEqual } from "node:util";
import pg from "pg";
import { z } from "zod";
import { buildSchema, createEventStore, defineEvents, defineFold, defineReadModel } from "../dist/index.js";
import { PostgresStore } from "../dist/postgres/index.js";

const SCHEMA = "showcase_rls";
const TABLE = "showcase_events";
const KEPT = [`${TABLE}_fold_state`, `${TABLE}_read_model`, `${TABLE}_read_model_state`];
const ROLES = { owner: "showcase_owner", app: "showcase_app", proc: "showcase_proc" };
const KEYS = 50;
const EVENTS = 1_000;
const A = "tenant-a";
const B = "tenant-b";

const sales = defineEvents({
  OfferMade: { data: z.object({ amount: z.number() }), scopes: ["offerId"], optionalScopes: ["tenantId"] },
  OfferRevised: { data: z.object({ amount: z.number() }), scopes: ["offerId"], optionalScopes: ["tenantId"] },
});
const source = sales.$filter();
const initial = { status: "none", amount: 0, revisions: 0 };
const fold = sales.$fold({
  OfferMade: (d, s) => ({ ...s, status: "open", amount: d.amount }),
  OfferRevised: (d, s) => ({ ...s, amount: d.amount, revisions: s.revisions + 1 }),
});
const offers = defineReadModel({ name: "offers", version: 1, source, keyBy: "offerId", fold, initial, processor: "cross-tenant" });
const liveOffers = defineReadModel({ name: "offers-live", version: 1, source, keyBy: "offerId", fold, initial });
const byOffer = defineFold({ name: "by-offer", version: 1, fold });
const keyQuery = (k) => ({ ...source, scopes: { offerId: k } });
const tokenOf = (a) => ({ transactionId: a.transactionId, sequence: a.last });
const config = { events: [sales], tenant: { scopeKey: "tenantId" } };
const schema = buildSchema(config.events, { strict: true, tenantScopeKey: "tenantId" });
const options = (install) => ({ schemaName: SCHEMA, table: TABLE, rls: true, live: true, persistedFolds: true, persistedModels: true, processorRole: ROLES.proc, install, poolSize: 3 });
const fmt = (n) => n.toFixed(n < 0.1 ? 4 : n < 10 ? 2 : 1);
const q = (t) => `"${SCHEMA}"."${t}"`;
const clientQuery = pg.Client.prototype.query;

function roleUrl(url, role) {
  const u = new URL(url);
  u.username = role;
  u.password = role;
  return u.toString();
}

/** A's and B's events: the same 50 offer ids, other amounts. */
function events(tenantOffset) {
  return Array.from({ length: EVENTS }, (_, i) => {
    const offerId = `offer-${i % KEYS}`;
    return i < KEYS ? sales.OfferMade({ amount: tenantOffset + i }, { offerId }) : sales.OfferRevised({ amount: tenantOffset + i }, { offerId });
  });
}

/** The README's erasure recipe (the first sql block after "**Erasing a tenant**"), for this schema and table. */
function erasureStatements() {
  const text = readFileSync(new URL("../README.md", import.meta.url), "utf8");
  const block = /```sql\n([\s\S]*?)```/.exec(text.slice(text.indexOf("**Erasing a tenant**")))[1];
  return block
    .replace("hashtext('public.events')", `hashtext('${SCHEMA}.${TABLE}')`)
    .replace(/\bevents_(read_model_state|read_model|fold_state)\b/g, (_, rest) => `${TABLE}_${rest}`)
    .replace(/\bevents\b(?!_)/g, TABLE)
    .replace("'<tenantKey>'", "'tenantId'")
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length > 0 && !l.startsWith("--"))
    .join("\n")
    .split(/;\s*\n?/)
    .map((s) => s.trim())
    .filter(Boolean);
}

async function dropAll(admin) {
  await admin.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
  for (const role of Object.values(ROLES)) {
    const exists = await admin.query("SELECT 1 FROM pg_roles WHERE rolname = $1", [role]);
    if (exists.rowCount === 0) continue;
    await admin.query(`DROP OWNED BY ${role}`);
    await admin.query(`DROP ROLE ${role}`);
  }
}

export async function stage6(url, admin) {
  console.log("@orgops/eventstore showcase — stage 6: tenants, rls and erasure");
  const fails = [];
  const expect = (ok, what) => {
    if (!ok) fails.push(what);
    return ok;
  };
  const apis = [];
  await dropAll(admin);
  try {
    // ── 1. setup ──
    for (const role of Object.values(ROLES)) await admin.query(`CREATE ROLE ${role} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '${role}'`);
    await admin.query(`CREATE SCHEMA ${SCHEMA} AUTHORIZATION ${ROLES.owner}`);
    await admin.query(`GRANT CREATE ON DATABASE "${new URL(url).pathname.slice(1)}" TO ${ROLES.owner}`);
    const owner = new PostgresStore({ connection: roleUrl(url, ROLES.owner), schema, ...options("auto"), grantExecuteTo: [ROLES.app] });
    await owner.ensureInstalled();
    await owner.withClient(async (c) => {
      await c.query(`GRANT USAGE ON SCHEMA ${SCHEMA} TO ${ROLES.app}, ${ROLES.proc}`);
      await c.query(`GRANT SELECT, INSERT ON ${q(TABLE)} TO ${ROLES.app}`);
      await c.query(`GRANT USAGE, SELECT ON SEQUENCE ${q(`${TABLE}_sequence_number_seq`)} TO ${ROLES.app}`);
      await c.query(`GRANT SELECT, INSERT, UPDATE ON ${KEPT.map(q).join(", ")} TO ${ROLES.app}`);
      await c.query(`GRANT SELECT ON ${q(TABLE)} TO ${ROLES.proc}`);
      await c.query(`GRANT SELECT, INSERT, UPDATE ON ${q(`${TABLE}_read_model`)}, ${q(`${TABLE}_read_model_state`)} TO ${ROLES.proc}`);
    });
    await owner.close();
    const policies = await admin.query("SELECT policyname, roles::text[] AS roles FROM pg_policies WHERE schemaname = $1 ORDER BY policyname", [SCHEMA]);
    console.log(`1. installed by ${ROLES.owner} (no superuser, bound by FORCE) with rls, persistedFolds, persistedModels, live, processorRole: "${ROLES.proc}" — ${policies.rowCount} policies:`);
    for (const p of policies.rows) console.log(`   ${p.policyname.padEnd(42)} to ${p.roles.join(", ")}`);
    const member = (await admin.query("SELECT pg_has_role($1, $2, 'MEMBER') AS m", [ROLES.app, ROLES.proc])).rows[0].m;
    console.log(`   the application api connects as ${ROLES.app}, the processor api as ${ROLES.proc}; ${ROLES.app} a member of ${ROLES.proc}: ${member ? "YES" : "no"}`);
    expect(!member, "the app role is not a member of the processor role");
    const app = createEventStore({ ...config, connection: roleUrl(url, ROLES.app), postgres: options("none"), contextCache: { persist: { after: 1 } } });
    const proc = createEventStore({ ...config, connection: roleUrl(url, ROLES.proc), postgres: options("none") });
    apis.push(app, proc);

    // ── 2. two tenants, one processor ──
    // a subscription's feed is the only reader of settled events WITH their payload (src/subscribe `poll`); readers'
    // probes read none (lab .ziv/lab/stage-06-fixer/feed-statements.txt) — counted per role while the model builds
    const feedReads = new Map();
    const feedRead = new RegExp(`payload AS payload.*FROM "${SCHEMA}"\\."${TABLE}" WHERE .*transaction_id < ctx\\.xmin`);
    pg.Client.prototype.query = function (...args) {
      const text = (typeof args[0] === "string" ? args[0] : (args[0]?.text ?? "")).replace(/\s+/g, " ");
      const role = this.connectionParameters?.user ?? "";
      if (feedRead.test(text)) feedReads.set(role, (feedReads.get(role) ?? 0) + 1);
      return clientQuery.apply(this, args);
    };
    const processor = proc.persistedReadModel(offers);
    const last = {};
    for (const [t, offset] of [[A, 0], [B, 100_000]]) {
      for (let i = 0; i < EVENTS; i += 500) last[t] = tokenOf(await app.forTenant(t).append(events(offset).slice(i, i + 500)));
    }
    const t0 = performance.now();
    for (const t of [A, B]) await app.forTenant(t).persistedReadModel(offers).list({ atLeast: last[t], timeoutMs: 30_000 });
    const built = performance.now() - t0;
    pg.Client.prototype.query = clientQuery;
    const feedsOf = (role) => feedReads.get(role) ?? 0;
    const ps = await processor.status();
    const locks = await admin.query(
      "SELECT a.usename AS role, count(*)::int AS n FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid WHERE l.locktype = 'advisory' AND l.granted AND a.usename = ANY($1) GROUP BY 1 ORDER BY 1",
      [[ROLES.app, ROLES.proc]],
    );
    const lockOf = (role) => locks.rows.find((r) => r.role === role)?.n ?? 0;
    const readerRoles = await Promise.all([A, B].map(async (t) => (await app.forTenant(t).persistedReadModel(offers).status()).role));
    console.log(`2. tenants ${A} and ${B}: ${EVENTS} events each on the same ${KEYS} offer ids; one cross-tenant processor (processor: "cross-tenant") built both in ${fmt(built)} ms`);
    console.log(`   processor: role ${ps.role}, epoch ${ps.epoch}, folded ${ps.folded}, checkpoint seq ${ps.checkpoint?.sequence}; writer locks held: ${ROLES.proc} ${lockOf(ROLES.proc)}, ${ROLES.app} ${lockOf(ROLES.app)}; readers' role: ${readerRoles.join(", ")}`);
    console.log(`   subscription feed reads while building (settled events with payload, counted per role): ${ROLES.proc} ${feedsOf(ROLES.proc)}, ${ROLES.app} ${feedsOf(ROLES.app)} — ${feedsOf(ROLES.app) === 0 ? "only the processor subscribes; readers do not" : "the application role subscribes too"}`);
    expect(ps.role === "writer" && ps.folded === 2 * EVENTS && lockOf(ROLES.proc) === 1 && lockOf(ROLES.app) === 0 && readerRoles.every((r) => r === "standby"), "one processor, one lock, readers standby");
    expect(feedsOf(ROLES.proc) > 0 && feedsOf(ROLES.app) === 0, "only the processor's feed reads the events");

    // ── 3. each sees only its own rows ──
    console.log("3. each tenant sees only its own rows (as the application role):");
    const loads = [];
    const fresh = createEventStore({ ...config, connection: roleUrl(url, ROLES.app), postgres: options("none"), contextCache: { persist: { after: 1 }, onLoad: (l) => void loads.push(l) } });
    apis.push(fresh);
    const before = {};
    for (const t of [A, B]) {
      const view = app.forTenant(t);
      const reader = view.persistedReadModel(offers);
      let same = true;
      for (let k = 0; k < KEYS; k++) {
        const key = `offer-${k}`;
        if (!isDeepStrictEqual(await reader.get(key), (await view.context({ query: keyQuery(key), fold, initial })).state)) same = false;
      }
      const g0 = performance.now();
      const got = await reader.get("offer-7");
      const getMs = performance.now() - g0;
      const page = await reader.list({ sort: { by: "amount", dir: "desc" }, limit: 2 });
      await view.context({ query: keyQuery("offer-7"), fold: byOffer, initial }); // stores a persisted fold of the tenant
      loads.length = 0;
      const ctx = await fresh.forTenant(t).context({ query: keyQuery("offer-7"), fold: byOffer, initial });
      const load = loads.at(-1);
      const live = await view.readModel(liveOffers).get("offer-7");
      before[t] = { got, total: page.total, live, ctx: ctx.state };
      console.log(
        `   ${t}: reader get(offer-7) amount ${got.amount} (${fmt(getMs)} ms, warm), list total ${page.total}, top ${page.rows.map((r) => `${r.key}=${r.state.amount}`).join(" ")}; same answer as es.context() for all ${KEYS} keys: ${same ? "yes" : "NO"}`,
      );
      console.log(`   ${" ".repeat(t.length)}  persisted fold in a fresh process: start ${load?.start}, events ${load?.events}; live model get(offer-7) amount ${live.amount}`);
      expect(same && page.total === KEYS && load?.start === "stored" && load?.events === 0 && live.amount === got.amount, `${t} sees its own state`);
    }
    expect(before[A].got.amount !== before[B].got.amount, "the two tenants differ");
    const appPool = new pg.Pool({ connectionString: roleUrl(url, ROLES.app), max: 1 });
    const c = await appPool.connect();
    let foreign = 0;
    let processorRow = 0;
    const counts = [];
    try {
      await c.query("BEGIN");
      await c.query("SELECT set_config('app.current_tenant', $1, true)", [A]);
      for (const table of KEPT) {
        const r = await c.query(`SELECT count(*)::int AS n, count(*) FILTER (WHERE tenant <> current_setting('app.current_tenant'))::int AS foreign FROM ${q(table)}`);
        counts.push(`${table.slice(TABLE.length + 1)} ${r.rows[0].n}`);
        foreign += r.rows[0].foreign;
      }
      processorRow = (await c.query(`SELECT count(*)::int AS n FROM ${q(`${TABLE}_read_model`)} WHERE tenant = ''`)).rows[0].n;
      await c.query("ROLLBACK");
    } finally {
      c.release();
      await appPool.end();
    }
    console.log(`   a direct SELECT as ${ROLES.app} in ${A}'s session: rows ${counts.join(", ")}; ${A}'s session sees ${B}'s rows: ${foreign}; processor row visible: ${processorRow ? "YES" : "no"}`);
    expect(foreign === 0 && processorRow === 0, "A's session sees nothing foreign");

    // ── 4. the erasure, with the processor running ──
    console.log(`4. erasing ${A} with the README recipe "Erasing a tenant", as printed, as ${ROLES.owner}, the processor running:`);
    const bSeq = (await admin.query(`SELECT count(*)::int AS n FROM ${q(`${TABLE}_read_model_state`)} WHERE tenant = $1`, [B])).rows[0].n;
    const count = (await app.forTenant(A).statistics()).reduce((n, s) => n + s.count, 0);
    const client = new pg.Client({ connectionString: roleUrl(url, ROLES.owner), options: `-c search_path=${SCHEMA}` });
    await client.connect();
    const deleted = {};
    let checked = false;
    const e0 = performance.now();
    try {
      for (const statement of erasureStatements()) {
        if (/^COMMIT$/i.test(statement)) {
          checked = deleted[TABLE] === count;
          if (!checked) {
            await client.query("ROLLBACK");
            break;
          }
          await client.query("COMMIT");
          continue;
        }
        const r = await client.query(statement, statement.includes("$1") ? [A] : []);
        const table = /^DELETE FROM (\w+)/i.exec(statement)?.[1];
        if (table) deleted[table] = r.rowCount;
      }
    } finally {
      await client.end();
    }
    const eraseMs = performance.now() - e0;
    console.log(`   step 1 count (statistics): ${count}; deleted: ${Object.entries(deleted).map(([t, n]) => `${t} ${n}`).join(", ")}; count check: ${checked ? "ok" : "MISMATCH — rolled back"} (${fmt(eraseMs)} ms)`);
    expect(checked, "count check");
    app.forTenant(A).invalidate();
    fresh.forTenant(A).invalidate();
    const lateB = tokenOf(await app.forTenant(B).append([sales.OfferRevised({ amount: 1 }, { offerId: "offer-0" })]));
    await app.forTenant(B).persistedReadModel(offers).list({ atLeast: lateB, timeoutMs: 15_000 });
    const left = [];
    let leftover = Number((await admin.query(`SELECT count(*)::int AS n FROM ${q(TABLE)} WHERE ${SCHEMA}.es_scope(payload, 'tenantId') = $1`, [A])).rows[0].n);
    left.push(`${TABLE} ${leftover}`);
    for (const table of KEPT) {
      const n = (await admin.query(`SELECT count(*)::int AS n FROM ${q(table)} WHERE tenant = $1`, [A])).rows[0].n;
      leftover += n;
      left.push(`${table.slice(TABLE.length + 1)} ${n}`);
    }
    console.log(`   after es.forTenant("${A}").invalidate() and one more processor page: ${A} rows left: ${left.join(", ")}`);
    const aReader = await app.forTenant(A).persistedReadModel(offers).get("offer-7");
    const aLive = await app.forTenant(A).readModel(liveOffers).get("offer-7");
    const aCtx = (await app.forTenant(A).context({ query: keyQuery("offer-7"), fold: byOffer, initial })).state;
    const initialAll = [aReader, aLive, aCtx].every((s) => isDeepStrictEqual(s, initial));
    console.log(`   ${A}: reader get(offer-7) ${JSON.stringify(aReader)}, live model get (same handle) ${JSON.stringify(aLive)}, context ${JSON.stringify(aCtx)} — initial: ${initialAll ? "yes" : "NO"}`);
    const bReader = await app.forTenant(B).persistedReadModel(offers).get("offer-7");
    const bRows = (await admin.query(`SELECT count(*)::int AS n FROM ${q(`${TABLE}_read_model_state`)} WHERE tenant = $1`, [B])).rows[0].n;
    const bSame = isDeepStrictEqual(bReader, before[B].got) && bRows === bSeq;
    console.log(`   ${B} answers unchanged: ${bSame ? "yes" : "NO"} (get(offer-7) amount ${bReader.amount}, ${bRows} state rows); processor still ${(await processor.status()).role}`);
    expect(leftover === 0 && initialAll && bSame, "nothing of A remains, B unchanged");
  } finally {
    pg.Client.prototype.query = clientQuery;
    for (const api of apis) await api.close().catch(() => undefined);
    await dropAll(admin).catch(() => undefined);
  }
  const all = fails.length === 0;
  console.log(`stage 6 expectations (one processor, each tenant sees only its own rows, the erasure leaves nothing of A, B unchanged): ${all ? "all met" : `NOT MET — ${fails.join("; ")}`}\n`);
  return all;
}
