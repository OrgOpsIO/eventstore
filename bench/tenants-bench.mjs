// Tenants, rls and erasure (stage 6) on the local Postgres test database. Build first, or run
// `sh bench/tenants.sh`, which builds and then runs this.
//   node bench/tenants-bench.mjs fanout <tenants> [appends=7]      statements and latency per append: per-tenant handles vs one processor
//   node bench/tenants-bench.mjs page <tenants> [runs=3] [fresh|analyzed]   the processor's page time vs tenants per page (1 000-event pages)
//   node bench/tenants-bench.mjs rebuild [runs=3] [fresh|analyzed]   100 000 × 1 KB over 100 tenants × 10 keys: one processor vs 100 per-tenant writers
//   node bench/tenants-bench.mjs erasure <events> [runs=3]         the README recipe for a tenant with <events> events, processor stopped / running
//   node bench/tenants-bench.mjs invalidate [runs=11]              invalidate() → the rebuilt live model's first answer, 10 000 events in the tenant
//   node bench/tenants-bench.mjs read [runs=11]                    a reader of a processor-written model, 100 000 × 1 KB, K = 1, cold and warm, vs fold on read
//
// Everything runs under row-level security (knowledge/model.md stage 6 §9), in a schema of its own (bench_t6) with
// three roles of its own, created at the start and dropped (DROP OWNED BY, DROP ROLE, DROP SCHEMA … CASCADE) at the
// end: bench_t6_owner installs (no superuser, bound by FORCE), bench_t6_app is the application, bench_t6_proc the
// processor role (postgres.processorRole). Every event 1 KB (the "small" payload of the matrix).
//
// Statements are counted in this process by wrapping pg.Client.prototype.query (the library's pools are pg.Pools of the
// same module): every statement of a client counts once, the heartbeat "SELECT 1" aside, per application_name, which
// each api sets through its connection string: bench_app (the application's handles), bench_proc (the processor),
// bench_writer (the appender: its own statements are not counted), bench_admin (the superuser that observes: not
// counted). The processor's page transactions are recognised by their shared erasure lock and timed statement by
// statement (BEGIN → COMMIT wall time on the client, the fold's JavaScript between statements included).
//
// fanout: N tenants, each with 4 events on 2 keys. (a) "per tenant": every tenant's handle es.forTenant(t)
//   .persistedReadModel(def) without processor — a writer each (stage 4); (b) "processor": one processor
//   (processor: "cross-tenant") on the processor api's root and a reader es.forTenant(t).persistedReadModel(def) per
//   tenant. After all are caught up and 2 s of quiet: statements in 6 s idle; then `appends` + 1 single-event appends
//   (the first not kept), each to another tenant: "applied" = the append's return → the tenant's model row checkpoint
//   (polled by the superuser every 5 ms) at or beyond the event; statements counted from the append to 300 ms after
//   "applied". Writer locks = granted advisory locks per role in pg_locks.
// page: 10 000 events over T tenants, appended in 10 rounds so that every 1 000 consecutive events hold every tenant;
//   a processor with batchSize 1 000 builds from zero (a fresh model name per run): 10 pages per run. Per page: the
//   transaction, the erasure lock statement, the count check statement, the notify statement, the statements.
//   "fresh" (default): the kept tables carry the statistics of their empty state (a fresh install before autovacuum
//   analyzes them); "analyzed": one build first, then VACUUM ANALYZE before every run. The same for rebuild.
//   Afterwards 20 tenants' reader get(k) against es.context() (0 mismatches expected).
// rebuild: 100 000 × 1 KB, 100 tenants × 10 keys, appended in 1 000 rounds of 10 events per tenant (interleaved).
//   Per run a fresh model name: (b) one processor from zero until its row's checkpoint passes the last event;
//   (a) 100 per-tenant handles until every tenant's row passes its last event. Default batchSize (500).
// erasure: background 9 tenants × 1 000 events; per run a fresh tenant X with <events> events on 10 keys, a processor
//   model built over all, X's persisted folds stored (10 keys). Then the README recipe "Erasing a tenant" as printed
//   (schema and table substituted), as the owner, with the step-1 count (statistics()) checked before COMMIT; timed
//   from the lock statement to COMMIT. Variant "stopped": the processor api is closed first; variant "running": the
//   processor runs and an appender writes one event every 5 ms to the 9 other tenants; its page transactions during
//   the recipe are compared with those of 2 s of the stream alone right after it. Then X's rows in events and the three kept tables (0
//   expected) and, running, again after one more page.
// invalidate: one tenant with 10 000 × 1 KB on 10 keys and a live model of it in the application api (rls); per run
//   view.invalidate() and the time to the first get(k) after it, its state compared with es.context().
// read: one tenant with 100 000 × 1 KB on one key (K = 1), a processor-written cross-tenant model (built by the
//   processor api, batchSize 5 000, which then closes). Per run fresh node processes, the order rotated, one warm-up
//   round not kept: fold on read (es.context(), a plain fold: no persisted fold) and the reader's get, cold (the first
//   call; store connected before the clock) and warm (the next call). The states are compared.

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { cpus, loadavg, totalmem } from "node:os";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import pg from "pg";
import { z } from "zod";
import { buildSchema, createEventStore, defineEvents, defineFold, defineReadModel } from "../dist/index.js";
import { PostgresStore } from "../dist/postgres/index.js";

const url = process.env.ES_BENCH_DATABASE_URL ?? "postgres://es:es@localhost:5432/es_sdk_test";
const self = fileURLToPath(import.meta.url);
const SCHEMA = "bench_t6";
const ROLES = { owner: "bench_t6_owner", app: "bench_t6_app", proc: "bench_t6_proc" };
const PAYLOAD = 1024;
const CATEGORIES = 40;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length === 0 ? NaN : s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const f = (n) => (n === undefined || Number.isNaN(n) ? "–" : Number.isInteger(n) ? String(n) : n < 0.1 ? n.toFixed(4) : n < 10 ? n.toFixed(2) : n.toFixed(1));
const range = (xs) => (xs.length === 0 ? "–" : `${f(median(xs))} (${f(Math.min(...xs))}–${f(Math.max(...xs))})`);
const factor = (a, b) => {
  const x = a / b;
  return x >= 100 ? Math.round(x).toLocaleString("en") : x.toFixed(2);
};
const verdict = (x) => (x >= 10 ? "reached" : '"not reached"');
const q = (t) => `"${SCHEMA}"."${t}"`;
const kept = (table) => [table, `${table}_fold_state`, `${table}_read_model`, `${table}_read_model_state`];

// ── statements, counted and timed per application_name ──────────────────────────
const counts = new Map();
const pages = [];
const countOf = (tag) => counts.get(tag) ?? 0;
const resetCounts = () => counts.clear();
const originalQuery = pg.Client.prototype.query;
pg.Client.prototype.query = function (...args) {
  const tag = this.connectionParameters?.application_name ?? "";
  const text = typeof args[0] === "string" ? args[0] : (args[0]?.text ?? "");
  if (text !== "SELECT 1") counts.set(tag, (counts.get(tag) ?? 0) + 1);
  const t0 = performance.now();
  const result = originalQuery.apply(this, args);
  if (tag === "bench_proc" && result && typeof result.then === "function") trackPage(this, text, t0, result);
  return result;
};
/** The page statements of src/postgres/models.ts (the cross-tenant `page`), by what they do. */
const LABELS = [
  [/^BEGIN/, "BEGIN"],
  [/pg_advisory_xact_lock_shared/, "erasure lock"],
  [/WHERE sequence_number = ANY/, "count check"],
  [/WHERE tenant = '' AND name = \$1 AND version = \$2 FOR UPDATE|tenant = '' .*FOR UPDATE/s, "processor row lock"],
  [/^INSERT INTO \S+ \(tenant, name, version, status\)/, "tenant rows insert"],
  [/^SELECT tenant, cursor_xid/, "tenant rows lock"],
  [/^SELECT tenant, key, state/, "state rows read"],
  [/^INSERT INTO \S+ AS t/, "state rows upsert"],
  [/^UPDATE \S+ AS t SET/, "tenant checkpoints"],
  [/^UPDATE \S+ SET cursor_xid/, "processor row update"],
  [/pg_notify/, "notify"],
  [/^(COMMIT|ROLLBACK)/, "COMMIT"],
];
const labelOf = (text) => LABELS.find(([re]) => re.test(text))?.[1] ?? `other: ${text.replace(/\s+/g, " ").slice(0, 50)}`;

function trackPage(client, text, t0, promise) {
  if (/^BEGIN/i.test(text)) client.__benchTx = { t0, statements: [] };
  const tx = client.__benchTx;
  if (!tx) return;
  promise.then(
    () => {
      const end = performance.now();
      tx.statements.push({ text, ms: end - t0 });
      if (!/^(COMMIT|ROLLBACK)/i.test(text)) return;
      client.__benchTx = undefined;
      const of = (re) => tx.statements.filter((s) => re.test(s.text)).reduce((n, s) => n + s.ms, 0);
      if (!tx.statements.some((s) => s.text.includes("pg_advisory_xact_lock_shared"))) return;
      pages.push({
        start: tx.t0,
        end,
        ms: end - tx.t0,
        lock: of(/pg_advisory_xact_lock_shared/),
        check: of(/count\(\*\)::text AS n FROM .* WHERE sequence_number = ANY/s),
        notify: of(/pg_notify\(\$1, k\) FROM unnest/),
        statements: tx.statements.length,
        parts: tx.statements.map((s) => ({ label: labelOf(s.text), ms: s.ms })),
        committed: /^COMMIT/i.test(text),
      });
    },
    () => {
      client.__benchTx = undefined;
    },
  );
}

// ── events and folds ─────────────────────────────────────────────────────────────
const ledger = defineEvents({
  EntryBooked: { data: z.object({ amount: z.number(), category: z.string(), note: z.string() }), scopes: ["accountId"], optionalScopes: ["tenantId"] },
});
const source = ledger.$filter();
const config = { events: [ledger], tenant: { scopeKey: "tenantId" } };
const schema = buildSchema(config.events, { strict: true, tenantScopeKey: "tenantId" });
const statsFold = (events, state) => {
  const next = new Map(state);
  for (const e of events) {
    const { amount, category, note } = e.data;
    const s = next.get(category) ?? { count: 0, sum: 0, min: Infinity, max: -Infinity, digest: 0 };
    let digest = s.digest;
    for (let i = 0; i < note.length; i += 16) digest = (digest * 31 + note.charCodeAt(i)) >>> 0;
    next.set(category, { count: s.count + 1, sum: s.sum + amount, min: Math.min(s.min, amount), max: Math.max(s.max, amount), digest });
  }
  return next;
};
const statsInitial = () => new Map();
const keyQuery = (k) => ({ ...source, scopes: { accountId: k } });
const models = new Map();
/** One definition object per name (a runtime refuses two definitions of one name and version). */
const model = (name, cross, extra = {}) => {
  if (!models.has(name)) models.set(name, defineReadModel({ name, version: 1, source, keyBy: "accountId", fold: statsFold, initial: statsInitial, ...(cross ? { processor: "cross-tenant" } : {}), ...extra }));
  return models.get(name);
};
const namedFold = defineFold({ name: "bench-stats", version: 1, fold: statsFold });

let noise = "";
const textOf = (i, length) => {
  if (noise.length < length + 4096) {
    let seed = 42;
    noise = Array.from({ length: length + 4096 }, () => {
      seed = (Math.imul(seed, 1103515245) + 12345) >>> 0;
      return "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"[seed >>> 26];
    }).join("");
  }
  const prefix = `entry ${i} `;
  const at = (i * 7919) % 4096;
  return prefix + noise.slice(at, at + Math.max(0, length - prefix.length));
};
let serial = 0;
/** One 1 KB event of account `account` (the tenant comes from the view it is appended through). */
const entry = (account) => {
  const i = serial++;
  return ledger.EntryBooked({ amount: (i * 37) % 1000, category: `c${i % CATEGORIES}`, note: textOf(i, PAYLOAD - 100) }, { accountId: account });
};

// ── roles, schema, apis ──────────────────────────────────────────────────────────
function roleUrl(role, tag) {
  const u = new URL(url);
  u.username = role;
  u.password = role;
  u.searchParams.set("application_name", tag);
  return u.toString();
}
const options = (table, install, extra = {}) => ({
  schemaName: SCHEMA,
  table,
  rls: true,
  live: true,
  persistedFolds: true,
  persistedModels: true,
  processorRole: ROLES.proc,
  install,
  ...extra,
});
const apis = [];
const open = (api) => (apis.push(api), api);
const appApi = (table, extra = {}, tag = "bench_app") =>
  open(createEventStore({ ...config, connection: roleUrl(ROLES.app, tag), postgres: options(table, "none"), contextCache: { maxEvents: 1_000_000 }, maxTenantViews: 100_000, ...extra }));
const procApi = (table) => open(createEventStore({ ...config, connection: roleUrl(ROLES.proc, "bench_proc"), postgres: options(table, "none") }));
const closeApi = async (api) => {
  apis.splice(apis.indexOf(api), 1);
  await api.close();
};

async function dropAll(admin) {
  await admin.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
  for (const role of Object.values(ROLES)) {
    if ((await admin.query("SELECT 1 FROM pg_roles WHERE rolname = $1", [role])).rowCount === 0) continue;
    await admin.query(`DROP OWNED BY ${role}`);
    await admin.query(`DROP ROLE ${role}`);
  }
}
async function createRoles(admin) {
  await dropAll(admin);
  for (const role of Object.values(ROLES)) await admin.query(`CREATE ROLE ${role} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '${role}'`);
  await admin.query(`CREATE SCHEMA ${SCHEMA} AUTHORIZATION ${ROLES.owner}`);
  await admin.query(`GRANT CREATE ON DATABASE "${new URL(url).pathname.slice(1)}" TO ${ROLES.owner}`);
}
/** Installs `table` as the owner (rls, persistedFolds, persistedModels, processorRole) and grants the two roles what README "Roles for RLS" lists. */
async function install(table) {
  const owner = new PostgresStore({ connection: roleUrl(ROLES.owner, "bench_owner"), schema, ...options(table, "auto"), grantExecuteTo: [ROLES.app] });
  await owner.ensureInstalled();
  await owner.withClient(async (c) => {
    await c.query(`GRANT USAGE ON SCHEMA ${SCHEMA} TO ${ROLES.app}, ${ROLES.proc}`);
    await c.query(`GRANT SELECT, INSERT ON ${q(table)} TO ${ROLES.app}`);
    await c.query(`GRANT USAGE, SELECT ON SEQUENCE ${q(`${table}_sequence_number_seq`)} TO ${ROLES.app}`);
    await c.query(`GRANT SELECT, INSERT, UPDATE ON ${q(`${table}_fold_state`)}, ${q(`${table}_read_model`)}, ${q(`${table}_read_model_state`)} TO ${ROLES.app}`);
    await c.query(`GRANT SELECT ON ${q(table)} TO ${ROLES.proc}`);
    await c.query(`GRANT SELECT, INSERT, UPDATE ON ${q(`${table}_read_model`)}, ${q(`${table}_read_model_state`)} TO ${ROLES.proc}`);
  });
  await owner.close();
}
const analyze = async (admin, table) => {
  for (const t of kept(table)) await admin.query(`VACUUM ANALYZE ${q(t)}`);
};
/** The tenant's model row checkpoint (sequence), read by the superuser. */
const checkpointOf = async (admin, table, tenant, name) =>
  Number((await admin.query(`SELECT cursor_seq::text AS s FROM ${q(`${table}_read_model`)} WHERE tenant = $1 AND name = $2 AND version = 1`, [tenant, name])).rows[0]?.s ?? -1);
async function waitCheckpoint(admin, table, tenant, name, sequence, timeoutMs = 600_000, everyMs = 5) {
  const t0 = performance.now();
  while ((await checkpointOf(admin, table, tenant, name)) < sequence) {
    if (performance.now() - t0 > timeoutMs) throw new Error(`model ${name} of tenant "${tenant}" did not pass ${sequence} within ${timeoutMs} ms`);
    await sleep(everyMs);
  }
  return performance.now() - t0;
}
const lockCounts = async (admin) => {
  const r = await admin.query(
    "SELECT a.usename AS role, count(*)::int AS n FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid WHERE l.locktype = 'advisory' AND l.granted AND a.usename = ANY($1) GROUP BY 1",
    [[ROLES.app, ROLES.proc]],
  );
  const of = (role) => r.rows.find((x) => x.role === role)?.n ?? 0;
  return { app: of(ROLES.app), proc: of(ROLES.proc) };
};
const rowsOf = async (admin, table, tenant) => {
  const out = {};
  out[table] = Number((await admin.query(`SELECT count(*)::int AS n FROM ${q(table)} WHERE ${SCHEMA}.es_scope(payload, 'tenantId') = $1`, [tenant])).rows[0].n);
  for (const t of kept(table).slice(1)) out[t] = (await admin.query(`SELECT count(*)::int AS n FROM ${q(t)} WHERE tenant = $1`, [tenant])).rows[0].n;
  return out;
};
/** Appends `perTenant` events to every tenant in `rounds` interleaved rounds (`concurrency` tenants at a time); the last append per tenant. */
async function seed(api, tenants, perTenant, keys, rounds = 1, concurrency = 10) {
  const last = {};
  const per = Math.ceil(perTenant / rounds);
  for (let r = 0; r < rounds; r++) {
    for (let i = 0; i < tenants.length; i += concurrency) {
      await Promise.all(
        tenants.slice(i, i + concurrency).map(async (t, j) => {
          const n = Math.min(per, perTenant - r * per);
          if (n <= 0) return;
          for (let at = 0; at < n; at += 1000) {
            const batch = Array.from({ length: Math.min(1000, n - at) }, (_, e) => entry(`a${(r * per + at + e + j) % keys}`));
            const a = await api.forTenant(t).append(batch);
            last[t] = { transactionId: a.transactionId, sequence: a.last };
          }
        }),
      );
    }
  }
  return last;
}
async function machineLine(admin) {
  const v = (await admin.query("SHOW server_version")).rows[0].server_version;
  return `${cpus()[0]?.model ?? "?"}, ${cpus().length} cores, ${Math.round(totalmem() / 2 ** 30)} GiB, ${process.platform} ${process.arch}, Node ${process.version}, Postgres ${v}; load average ${loadavg()
    .map((x) => x.toFixed(1))
    .join(" / ")}`;
}

/** The README's erasure recipe (the first sql block after "**Erasing a tenant**"), for this schema and table. */
function erasureStatements(table) {
  const text = readFileSync(new URL("../README.md", import.meta.url), "utf8");
  const block = /```sql\n([\s\S]*?)```/.exec(text.slice(text.indexOf("**Erasing a tenant**")))[1];
  return block
    .replace("hashtext('public.events')", `hashtext('${SCHEMA}.${table}')`)
    .replace(/\bevents_(read_model_state|read_model|fold_state)\b/g, (_, rest) => `${table}_${rest}`)
    .replace(/\bevents\b(?!_)/g, table)
    .replace("'<tenantKey>'", "'tenantId'")
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length > 0 && !l.startsWith("--"))
    .join("\n")
    .split(/;\s*\n?/)
    .map((s) => s.trim())
    .filter(Boolean);
}
/** Runs the recipe as the owner for `tenant`; COMMIT only when the deleted events equal `count`. */
async function runErasure(table, tenant, count) {
  const client = new pg.Client({ connectionString: roleUrl(ROLES.owner, "bench_owner"), options: `-c search_path=${SCHEMA}` });
  await client.connect();
  const deleted = {};
  let checked = false;
  let t0 = NaN;
  let end = NaN;
  try {
    for (const statement of erasureStatements(table)) {
      if (Number.isNaN(t0) && /pg_advisory_xact_lock/.test(statement)) t0 = performance.now();
      if (/^COMMIT$/i.test(statement)) {
        checked = deleted[table] === count;
        await client.query(checked ? "COMMIT" : "ROLLBACK");
        end = performance.now();
        break;
      }
      const r = await client.query(statement, statement.includes("$1") ? [tenant] : []);
      const t = /^DELETE FROM (\w+)/i.exec(statement)?.[1];
      if (t) deleted[t] = r.rowCount;
    }
  } finally {
    await client.end();
  }
  return { ms: end - t0, start: t0, end, deleted, checked };
}

// ════ child: one cold read in a fresh process ═══════════════════════════════════
async function readChild(table, kind, tenant, key) {
  const api = appApi(table, {}, "bench_child");
  await api.store();
  const view = api.forTenant(tenant);
  const initial = statsInitial(); // one instance: the context cache's key holds the identity of fold and initial (src/context.ts:21)
  const once = async () => {
    const t0 = performance.now();
    const state = kind === "fold" ? (await view.context({ query: keyQuery(key), fold: statsFold, initial })).state : await view.persistedReadModel(model("read", true)).get(key);
    return { ms: performance.now() - t0, state };
  };
  const cold = await once();
  const warm = await once();
  const folded = kind === "read" ? (await view.persistedReadModel(model("read", true)).status()).readFolded : undefined;
  await api.close();
  process.stdout.write(JSON.stringify({ cold: cold.ms, warm: warm.ms, folded, state: [...cold.state], same: isDeepStrictEqual(cold.state, warm.state) }));
  process.exit(0);
}
if (process.argv[2] === "--child") {
  const [, , , kind, ...a] = process.argv;
  if (kind === "read") await readChild(...a);
  process.exit(2);
}

// ════ parent ════════════════════════════════════════════════════════════════════
const admin = new pg.Pool({ connectionString: roleUrl("es", "bench_admin").replace("es:es@", "es:es@"), max: 3, connectionTimeoutMillis: 3000 });
try {
  await admin.query("SELECT 1");
} catch (err) {
  console.error(`bench: Postgres is not reachable at ${url} (${err.code ?? (err.message || err.name)}). Start the Docker container es-sdk-test-pg.`);
  await admin.end().catch(() => undefined);
  process.exit(1);
}
let exitCode = 0;
const red = (message) => {
  console.log(`RED: ${message}`);
  exitCode = 1;
};

async function fanout(n, appends) {
  const table = `fan_${n}`;
  await install(table);
  const tenants = Array.from({ length: n }, (_, i) => `t${i}`);
  const writer = appApi(table, {}, "bench_writer");
  const lastSeed = await seed(writer, tenants, 4, 2);
  await analyze(admin, table);
  console.log(`fanout, ${n} tenant(s), 4 events each on 2 keys; ${appends} single-event appends kept (+1 warm-up)`);
  for (const mode of ["per tenant", "processor"]) {
    const name = mode === "processor" ? "fan-proc" : "fan-tenant";
    const def = model(name, mode === "processor");
    const app = appApi(table);
    const proc = mode === "processor" ? procApi(table) : undefined;
    const t0 = performance.now();
    proc?.persistedReadModel(def);
    const handles = tenants.map((t) => app.forTenant(t).persistedReadModel(def));
    for (const t of tenants) await waitCheckpoint(admin, table, t, name, lastSeed[t].sequence, 600_000, 20);
    const ready = performance.now() - t0;
    await sleep(2000);
    const locks = await lockCounts(admin);
    const roles = await Promise.all(handles.slice(0, 3).map(async (h) => (await h.status()).role));
    resetCounts();
    await sleep(6000);
    const idle = { app: countOf("bench_app"), proc: countOf("bench_proc") };
    const kept = { applied: [], app: [], proc: [], window: [] };
    for (let r = 0; r <= appends; r++) {
      const t = tenants[(r * 7 + 3) % n];
      resetCounts();
      const a0 = performance.now();
      const res = await writer.forTenant(t).append([entry("a0")]);
      const applied = await waitCheckpoint(admin, table, t, name, res.last, 60_000, 5);
      const appliedMs = performance.now() - a0;
      await sleep(300);
      const window = performance.now() - a0;
      if (r === 0) continue;
      kept.applied.push(appliedMs);
      kept.app.push(countOf("bench_app"));
      kept.proc.push(countOf("bench_proc"));
      kept.window.push(window);
      void applied;
    }
    const idleRate = (idle.app + idle.proc) / 6000; // statements per ms while idle
    const adjusted = kept.app.map((x, i) => x + kept.proc[i] - idleRate * kept.window[i]);
    console.log(
      `  ${mode.padEnd(10)} ready ${f(ready)} ms (first build of ${n} tenant(s)); readers' role ${roles.join("/")}; writer locks held: app ${locks.app}, processor ${locks.proc}; idle statements per 6 s: app ${idle.app}, processor ${idle.proc}`,
    );
    console.log(
      `  ${" ".repeat(10)} per append: statements app ${range(kept.app)}, processor ${range(kept.proc)}, both minus the idle rate over the window ${range(adjusted)}; append → applied ${range(kept.applied)} ms; window ${range(kept.window)} ms`,
    );
    for (const h of handles) await h.stop().catch(() => undefined);
    await closeApi(app);
    if (proc) await closeApi(proc);
  }
  await closeApi(writer);
}

/**
 * "fresh": the kept tables' statistics are those of VACUUM ANALYZE after seeding the events (no state row yet) — a
 * fresh install before autovacuum's first ANALYZE; "analyzed": one build first (not kept), then VACUUM ANALYZE of
 * every table before each kept run.
 */
async function warmStatistics(stats, table, build) {
  if (stats !== "analyzed") return;
  await build();
  await analyze(admin, table);
}

async function page(tenantsPerPage, runs, stats) {
  const table = `page_${tenantsPerPage}`;
  await install(table);
  const tenants = Array.from({ length: tenantsPerPage }, (_, i) => `t${i}`);
  const writer = appApi(table, {}, "bench_writer");
  const last = await seed(writer, tenants, 10_000 / tenantsPerPage, 10, 10, 20);
  const end = Math.max(...Object.values(last).map((l) => l.sequence));
  await analyze(admin, table);
  await warmStatistics(stats, table, async () => {
    const proc = procApi(table);
    proc.persistedReadModel(model("page-warm", true, { batchSize: 1000 }));
    await waitCheckpoint(admin, table, "", "page-warm", end, 600_000, 20);
    await closeApi(proc);
  });
  const all = [];
  const firsts = [];
  for (let run = 1; run <= runs; run++) {
    if (stats === "analyzed") await analyze(admin, table);
    const name = `page-r${run}`;
    const proc = procApi(table);
    pages.length = 0;
    proc.persistedReadModel(model(name, true, { batchSize: 1000 }));
    await waitCheckpoint(admin, table, "", name, end, 600_000, 20);
    await sleep(100);
    const committed = pages.filter((p) => p.committed);
    if (committed.length !== 10) red(`page ${tenantsPerPage}: run ${run} committed ${committed.length} pages, 10 expected (${pages.length} page transactions)`);
    firsts.push(committed[0]);
    all.push(...committed.slice(1));
    await closeApi(proc);
  }
  // equivalence: 20 tenants' readers against fold on read
  const app = appApi(table);
  let compared = 0;
  let mismatches = 0;
  for (const t of tenants.slice(0, 20)) {
    for (const k of ["a0", "a3", "never"]) {
      const got = await app.forTenant(t).persistedReadModel(model(`page-r${runs}`, true, { batchSize: 1000 })).get(k);
      const want = (await app.forTenant(t).context({ query: keyQuery(k), fold: statsFold, initial: statsInitial() })).state;
      compared++;
      if (!isDeepStrictEqual(got, want)) mismatches++;
    }
  }
  await closeApi(app);
  await closeApi(writer);
  if (mismatches > 0) red(`page ${tenantsPerPage}: ${mismatches} of ${compared} reader gets differ from es.context()`);
  const pick = (k) => all.map((p) => p[k]);
  const share = (k) => all.map((p) => (100 * p[k]) / p.ms);
  console.log(
    `page, ${tenantsPerPage} tenant(s) per 1 000-event page, statistics ${stats}, ${runs} runs × 9 pages after the first: page ${range(pick("ms"))} ms; erasure lock ${range(pick("lock"))} ms (${range(share("lock"))} %); count check ${range(pick("check"))} ms (${range(share("check"))} %); notify ${range(pick("notify"))} ms (${range(share("notify"))} %); statements ${range(pick("statements"))}; first page (inserts the tenant rows) ${range(firsts.map((p) => p.ms))} ms; equivalence ${compared} gets, ${mismatches} mismatches`,
  );
  // where a page's time goes: per statement kind (median over the pages, ms), and the rest (the fold and the client between statements)
  const labels = [...new Set(all.flatMap((p) => p.parts.map((x) => x.label)))];
  const per = labels.map((label) => [label, median(all.map((p) => p.parts.filter((x) => x.label === label).reduce((n, x) => n + x.ms, 0)))]);
  const between = median(all.map((p) => p.ms - p.parts.reduce((n, x) => n + x.ms, 0)));
  console.log(`  per statement (median ms): ${per.map(([l, ms]) => `${l} ${f(ms)}`).join("; ")}; between statements (fold, client) ${f(between)}`);
}

async function rebuild(runs, stats) {
  const table = "rebuild";
  await install(table);
  const tenants = Array.from({ length: 100 }, (_, i) => `t${i}`);
  const writer = appApi(table, {}, "bench_writer");
  const s0 = performance.now();
  const last = await seed(writer, tenants, 1000, 10, 100, 20);
  const seedMs = performance.now() - s0;
  const end = Math.max(...Object.values(last).map((l) => l.sequence));
  await closeApi(writer);
  await analyze(admin, table);
  await warmStatistics(stats, table, async () => {
    const proc = procApi(table);
    proc.persistedReadModel(model("rb-warm", true));
    await waitCheckpoint(admin, table, "", "rb-warm", end, 1_800_000, 20);
    await closeApi(proc);
  });
  const out = { processor: [], "per tenant": [] };
  for (let run = 1; run <= runs; run++) {
    for (const mode of run % 2 ? ["processor", "per tenant"] : ["per tenant", "processor"]) {
      if (stats === "analyzed") await analyze(admin, table);
      const name = `rb-${mode === "processor" ? "proc" : "tenant"}-r${run}`;
      const t0 = performance.now();
      if (mode === "processor") {
        const proc = procApi(table);
        proc.persistedReadModel(model(name, true));
        await waitCheckpoint(admin, table, "", name, end, 1_800_000, 20);
        out[mode].push(performance.now() - t0);
        await closeApi(proc);
      } else {
        const app = appApi(table);
        const handles = tenants.map((t) => app.forTenant(t).persistedReadModel(model(name, false)));
        for (const t of tenants) await waitCheckpoint(admin, table, t, name, last[t].sequence, 1_800_000, 20);
        out[mode].push(performance.now() - t0);
        for (const h of handles) await h.stop().catch(() => undefined);
        await closeApi(app);
      }
    }
  }
  console.log(`rebuild, 100 000 × 1 KB over 100 tenants × 10 keys (seeded in ${f(seedMs / 1000)} s), statistics ${stats}, ${runs} runs each, default batchSize:`);
  for (const mode of ["processor", "per tenant"]) {
    const ms = out[mode];
    console.log(`  ${mode.padEnd(10)} zero → caught up ${range(ms.map((x) => x / 1000))} s = ${range(ms.map((x) => 100_000 / (x / 1000)))} events/s`);
  }
  console.log(`  processor vs per tenant (medians): ${factor(median(out["per tenant"]), median(out.processor))}× the per-tenant writers' time`);
}

async function erasure(events, runs) {
  const table = `erase_${events}`;
  await install(table);
  const background = Array.from({ length: 9 }, (_, i) => `b${i}`);
  const writer = appApi(table, {}, "bench_writer");
  await seed(writer, background, 1000, 10, 10, 9);
  const name = "erase-proc";
  const def = model(name, true);
  const out = { stopped: [], running: [] };
  const pageDuring = [];
  const pageBefore = [];
  let rolledBack = 0;
  for (let run = 1; run <= runs; run++) {
    for (const variant of run % 2 ? ["stopped", "running"] : ["running", "stopped"]) {
      const x = `x-${variant}-${run}`;
      const lastX = (await seed(writer, [x], events, 10, 1, 1))[x];
      await analyze(admin, table);
      let proc = procApi(table);
      proc.persistedReadModel(def);
      const head = Number((await admin.query(`SELECT max(sequence_number)::text AS s FROM ${q(table)}`)).rows[0].s);
      await waitCheckpoint(admin, table, "", name, head, 1_800_000, 20);
      await waitCheckpoint(admin, table, x, name, lastX.sequence, 60_000, 20);
      const app = appApi(table, { contextCache: { maxEvents: 1_000_000, persist: { after: 1 } } });
      for (let k = 0; k < 10; k++) await app.forTenant(x).context({ query: keyQuery(`a${k}`), fold: namedFold, initial: statsInitial });
      await closeApi(app);
      const before = await rowsOf(admin, table, x);
      if (before[`${table}_fold_state`] < 10 || before[`${table}_read_model_state`] < 10) red(`erasure ${events}: ${x} has not every kept state before the recipe: ${JSON.stringify(before)}`);
      const count = (await writer.forTenant(x).statistics()).reduce((n, s) => n + s.count, 0);
      let stream;
      let streaming = false;
      if (variant === "stopped") {
        await closeApi(proc);
        proc = undefined;
      } else {
        streaming = true;
        stream = (async () => {
          for (let i = 0; streaming; i++) {
            await writer.forTenant(background[i % background.length]).append([entry(`a${i % 10}`)]);
            await sleep(5);
          }
        })();
        await sleep(2000);
      }
      pages.length = 0;
      const r = await runErasure(table, x, count);
      if (!r.checked) red(`erasure ${events} ${variant}: count check failed: ${count} counted, ${JSON.stringify(r.deleted)}`);
      out[variant].push(r.ms);
      if (variant === "running") {
        await sleep(1000);
        for (const p of pages) if (p.end >= r.start && p.start <= r.end) pageDuring.push(p.ms);
        rolledBack += pages.filter((p) => !p.committed).length;
        streaming = false;
        await stream;
        const tail = Number((await admin.query(`SELECT max(sequence_number)::text AS s FROM ${q(table)}`)).rows[0].s);
        await waitCheckpoint(admin, table, "", name, tail, 60_000, 20);
      }
      const left = await rowsOf(admin, table, x);
      const leftover = Object.values(left).reduce((a, b) => a + b, 0);
      if (leftover !== 0) red(`erasure ${events} ${variant}: rows of ${x} left: ${JSON.stringify(left)}`);
      if (run === 1) console.log(`  erasure ${events} ${variant}: deleted ${JSON.stringify(r.deleted)}; ${x} rows left after${variant === "running" ? " one more page" : ""}: ${leftover}`);
      if (proc) {
        // the processor's pages with the stream alone (2 s right after the recipe), for comparison
        pages.length = 0;
        streaming = true;
        stream = (async () => {
          for (let i = 0; streaming; i++) {
            await writer.forTenant(background[i % background.length]).append([entry(`a${i % 10}`)]);
            await sleep(5);
          }
        })();
        await sleep(2000);
        streaming = false;
        await stream;
        pageBefore.push(...pages.filter((p) => p.committed).map((p) => p.ms));
        await closeApi(proc);
      }
    }
  }
  await closeApi(writer);
  console.log(
    `erasure, a tenant with ${events.toLocaleString("en")} × 1 KB events (9 other tenants × 1 000), the README recipe as the owner, ${runs} runs each: processor stopped ${range(out.stopped)} ms; processor running with a stream ${range(out.running)} ms; the processor's page while the recipe runs ${range(pageDuring)} ms (${pageDuring.length} pages, ${rolledBack} rolled back) vs with the stream alone (2 s right after it) ${range(pageBefore)} ms (${pageBefore.length} pages)`,
  );
}

async function invalidate(runs) {
  const table = "inval";
  await install(table);
  const writer = appApi(table, {}, "bench_writer");
  await seed(writer, ["ti"], 10_000, 10, 1, 1);
  await closeApi(writer);
  await analyze(admin, table);
  const app = appApi(table);
  const view = app.forTenant("ti");
  const live = view.readModel(model("inval-live", false));
  const b0 = performance.now();
  await live.get("a0", { timeoutMs: 600_000 });
  const first = performance.now() - b0;
  const want = (await view.context({ query: keyQuery("a0"), fold: statsFold, initial: statsInitial() })).state;
  const ms = [];
  let same = 0;
  let folded = [];
  for (let r = 0; r < runs; r++) {
    const before = live.status.folded;
    const t0 = performance.now();
    view.invalidate();
    const got = await live.get("a0", { timeoutMs: 600_000 });
    ms.push(performance.now() - t0);
    folded.push(live.status.folded - before);
    if (isDeepStrictEqual(got, want)) same++;
  }
  await closeApi(app);
  if (same !== runs) red(`invalidate: ${runs - same} of ${runs} rebuilt answers differ from es.context()`);
  console.log(
    `invalidate, one tenant with 10 000 × 1 KB on 10 keys, live model under rls, ${runs} runs: invalidate() → first get ${range(ms)} ms (events folded per rebuild ${range(folded)}); the model's first build ${f(first)} ms; ${same}/${runs} equal to es.context()`,
  );
}

async function read(runs) {
  const table = "read";
  await install(table);
  const writer = appApi(table, {}, "bench_writer");
  const s0 = performance.now();
  const last = (await seed(writer, ["tr"], 100_000, 1, 1, 1)).tr;
  const seedMs = performance.now() - s0;
  await closeApi(writer);
  const proc = procApi(table);
  const b0 = performance.now();
  proc.persistedReadModel(model("read", true, { batchSize: 5000 }));
  await waitCheckpoint(admin, table, "tr", "read", last.sequence, 1_800_000, 20);
  const built = performance.now() - b0;
  await closeApi(proc);
  await analyze(admin, table);
  const child = (kind) =>
    JSON.parse(
      execFileSync(process.execPath, [self, "--child", "read", table, kind, "tr", "a0"], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", "inherit"], env: { ...process.env, ES_BENCH_DATABASE_URL: url } }),
    );
  const out = { fold: { cold: [], warm: [] }, read: { cold: [], warm: [] } };
  let mismatches = 0;
  let readFolded = [];
  for (let r = 0; r <= runs; r++) {
    const order = r % 2 ? ["fold", "read"] : ["read", "fold"];
    const got = {};
    for (const kind of order) got[kind] = child(kind);
    if (!isDeepStrictEqual(got.fold.state, got.read.state) || !got.fold.same || !got.read.same) mismatches++;
    if (r === 0) continue; // warm-up round, not kept
    for (const kind of order) {
      out[kind].cold.push(got[kind].cold);
      out[kind].warm.push(got[kind].warm);
    }
    readFolded.push(got.read.folded);
  }
  if (mismatches > 0) red(`read: ${mismatches} rounds where the reader's state differs from fold on read`);
  const cold = median(out.fold.cold) / median(out.read.cold);
  const warm = median(out.fold.warm) / median(out.read.warm);
  console.log(`read, one tenant with 100 000 × 1 KB on one key (K = 1), processor-written (built in ${f(built / 1000)} s; seeded in ${f(seedMs / 1000)} s), ${runs} fresh processes per kind:`);
  console.log(`  fold on read  cold ${range(out.fold.cold)} ms, warm ${range(out.fold.warm)} ms`);
  console.log(`  reader get    cold ${range(out.read.cold)} ms, warm ${range(out.read.warm)} ms; events folded by the reader's two gets (readFolded) ${range(readFolded)}`);
  console.log(`  10× target at 100 000 events: cold ${factor(median(out.fold.cold), median(out.read.cold))}× ${verdict(cold)}; warm ${factor(median(out.fold.warm), median(out.read.warm))}× ${verdict(warm)}; states equal in ${runs + 1 - mismatches}/${runs + 1} rounds`);
}

const [cmd, a1, a2, a3] = process.argv.slice(2);
console.log(`bench/tenants-bench.mjs ${process.argv.slice(2).join(" ")} — ${new Date().toISOString()} — ${await machineLine(admin)}`);
try {
  await createRoles(admin);
  if (cmd === "fanout") await fanout(Number(a1 ?? 10), Number(a2 ?? 7));
  else if (cmd === "page") await page(Number(a1 ?? 10), Number(a2 ?? 3), a3 ?? "fresh");
  else if (cmd === "rebuild") await rebuild(Number(a1 ?? 3), a2 ?? "fresh");
  else if (cmd === "erasure") await erasure(Number(a1 ?? 1000), Number(a2 ?? 3));
  else if (cmd === "invalidate") await invalidate(Number(a1 ?? 11));
  else if (cmd === "read") await read(Number(a1 ?? 11));
  else {
    console.error("usage: node bench/tenants-bench.mjs fanout <tenants> [appends] | page <tenants> [runs] | rebuild [runs] | erasure <events> [runs] | invalidate [runs] | read [runs]");
    exitCode = 2;
  }
} catch (error) {
  red(`${cmd}: ${error?.stack ?? error}`);
} finally {
  for (const api of apis.splice(0)) await api.close().catch(() => undefined);
  await dropAll(admin).catch((e) => red(`cleanup: ${e.message}`));
  const left = (await admin.query("SELECT count(*)::int AS n FROM pg_roles WHERE rolname LIKE 'bench_t6_%'")).rows[0].n + (await admin.query("SELECT count(*)::int AS n FROM pg_namespace WHERE nspname = $1", [SCHEMA])).rows[0].n;
  console.log(`cleanup: schema ${SCHEMA} and roles bench_t6_* dropped (left: ${left})`);
  await admin.end();
}
process.exit(exitCode);
