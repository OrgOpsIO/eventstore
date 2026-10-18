// The persisted read model vs. the live read model vs. fold on read, on the local Postgres test
// database (and the memory store for the steady-state latency). Build first, or run
// `sh bench/persisted-model.sh`, which builds and then runs this.
//   node bench/persisted-model-bench.mjs matrix <events> <payloadBytes> [reps=11]
//   node bench/persisted-model-bench.mjs list <rows> [reps=5]
//   node bench/persisted-model-bench.mjs rebuild [events=100000] [reps=3]
//   node bench/persisted-model-bench.mjs steady [appends=200]
//   node bench/persisted-model-bench.mjs takeover [reps=5]
//   node bench/persisted-model-bench.mjs locks
// Tables of its own (bench_pm_*, with their _read_model and _read_model_state), dropped at the start and the end.
//
// matrix (knowledge/model.md stage 4 §9): one events table, two key layouts over the same events:
//   K = 1      keyBy ledgerId: one key holds every event, so get(k) and context() are the same N-event state
//   K = 1 000  keyBy accountId: one key holds N / 1 000 events
// The persisted model of each layout is built once by a process of its own (batchSize 5 000), which ends.
// Then, each cell in `reps` fresh processes, the order rotated per round:
//   fold on read     cold = the first es.context() of the key; warm = further loads with nothing new
//   live (500)       cold = readModel(def) → the first awaited get(k) (the rebuild); warm = awaited gets
//   persisted        cold = persistedReadModel(def) → the first get(k) (row + the key's delta, here 0);
//                    warm = awaited gets (copy present, one store query each)
// Then 100 events for the measured key are appended while no writer runs, and two more cells:
//   fold on read     again, over N + 100 events
//   persisted, lagging   the process holds the model's writer lock itself (the store's internal
//                    capability, as bench/showcase-persisted.mjs does), so its handle stays a standby
//                    and the row stays 100 events behind: the first get folds the delta 100, the
//                    second folds 0 more (the copy moved forward), warm gets fold 0.
// "cold" = a fresh node process with the store installed and connected before the clock runs (the
// database itself warm, as in bench/persisted-fold-bench.mjs and bench/readmodel-bench.mjs).
//
// list: R rows (one ~1 KB OfferMade each, 25 % open), list({ where: { status: "open" }, sort: amount
//   desc, limit: 50 }) first page and last page with total: persisted (cold = the first call of a fresh
//   process, warm = further calls) vs. fold on read + list (the whole source into a Map, filter, sort,
//   page in JavaScript) vs. the live model's kept list, each page compared with fold on read + list.
//
// rebuild: N × 1 KB (default 100 000) from zero to caught up, the persisted model (Postgres jsonb rows, the write side
//   included) and the live model (in memory) of the same definition, a fresh process and model name per run. At
//   100 000: batchSize 500 and 5 000, K = 1 / 1 000 / 100 000; above (decision 56 §2: 1 000 000): K = 1 000 at both
//   batch sizes and one key per event (keyBy entryId, K = N) at 5 000. pg_database_size before, with, after. At 100 000
//   only: a fresh writer after 1 000 new events (time to caught up, folded).
// steady: one append every 10 ms; the page transaction's time (the capability's page() timed in the
//   writer's process), append → watcher, append → get (memory; Postgres in one process; Postgres with
//   the appends from a second OS process).
// takeover: a writer process SIGKILLed → this process's standby becomes writer and applies a new event.
// locks: N tenant handles (N = 10 / 100) in one process: advisory locks held and connections used.
import { execFileSync, spawn } from "node:child_process";
import { writeFileSync } from "node:fs";
import { cpus, tmpdir, totalmem } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { deserialize, serialize } from "node:v8";
import pg from "pg";
import { z } from "zod";
import { buildSchema, createEventStore, defineEvents, defineReadModel, MemoryStore } from "../dist/index.js";
import { PostgresStore } from "../dist/postgres/index.js";

const url = process.env.ES_BENCH_DATABASE_URL ?? "postgres://es:es@localhost:5432/es_sdk_test";
// Bench timeouts (decision 54): every store is opened with statement_timeout 600 000 ms and every cold model read
// passes timeoutMs 600 000, so a slow cold read gives a number instead of 57014 / read_model_timeout. The library's
// defaults (statement_timeout 30 000 ms, timeoutMs 10 000) are unchanged.
const BENCH_TIMEOUTS = { statementMs: 600_000 };
const COLD_TIMEOUT_MS = 600_000;
const self = fileURLToPath(import.meta.url);
const MATRIX_TABLE = "bench_pm_events";
const LIST_TABLE = "bench_pm_list_events";
const REBUILD_TABLE = "bench_pm_rebuild_events";
const STEADY_TABLE = "bench_pm_steady_events";
const TAKEOVER_TABLE = "bench_pm_takeover_events";
const LOCKS_TABLE = "bench_pm_locks_events";
const CATEGORIES = 40;
const ACCOUNTS = 1_000;
const WARM = 20;
const GET_SEGMENTS = 20;
const LIVE_GETS = 1_000;
const PM_GETS = 100;
const DELTA = 100;

const now = () => performance.timeOrigin + performance.now(); // wall clock in ms with sub-ms precision, comparable across processes
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length === 0 ? NaN : s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const pct = (xs, p) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
};
const f = (n) => (n === undefined || Number.isNaN(n) ? "–" : n < 0.01 ? `${(n * 1000).toFixed(2)} µs` : n < 0.1 ? n.toFixed(4) : n < 10 ? n.toFixed(2) : n.toFixed(1));
const fms = (n) => {
  const s = f(n);
  return s.endsWith("µs") ? s : `${s} ms`;
};
const factor = (a, b) => {
  const x = a / b;
  return x >= 100 ? Math.round(x).toLocaleString("en") : x.toFixed(2);
};
/** A watcher gets one delivery per page, with the newest state: an append is delivered by the first delivery that holds it or a later one. */
const covering = (sent, deliveries) =>
  sent.flatMap((s) => {
    const d = deliveries.find((x) => x.amount >= s.amount);
    return d ? [d.at - s.at] : [];
  });
const verdict = (x) => (x >= 10 ? "reached" : '"not reached"');
const tokenOf = (a) => ({ transactionId: a.transactionId, sequence: a.last });
const pmTables = (table) => [table, `${table}_read_model`, `${table}_read_model_state`];

// ── the matrix and rebuild events, fold, models ─────────────────────────────────
const ledger = defineEvents({
  EntryBooked: { data: z.object({ amount: z.number(), category: z.string(), note: z.string() }), scopes: ["ledgerId", "accountId", "entryId"] },
});
const matrixSource = ledger.$filter();
/** bench/persisted-fold-bench.mjs's fold: running statistics per category, some work per event. */
const statsFold = (events, state) => {
  const next = new Map(state);
  for (const e of events) {
    const { amount, category, note } = e.data;
    const s = next.get(category) ?? { count: 0, sum: 0, min: Infinity, max: -Infinity, mean: 0, m2: 0, last: null, digest: 0 };
    const count = s.count + 1;
    const delta = amount - s.mean;
    const mean = s.mean + delta / count;
    let digest = s.digest;
    for (let i = 0; i < note.length; i += 16) digest = (digest * 31 + note.charCodeAt(i)) >>> 0;
    next.set(category, { count, sum: s.sum + amount, min: Math.min(s.min, amount), max: Math.max(s.max, amount), mean, m2: s.m2 + delta * (amount - mean), last: e.recordedAt, digest });
  }
  return next;
};
const statsInitial = () => new Map();
const LAYOUTS = { 1: { keyBy: "ledgerId", key: "bench" }, 1000: { keyBy: "accountId", key: "a7" }, 100000: { keyBy: "entryId", key: "e7" } };
const keyQueryOf = (source, keyBy, key) => ({ ...source, scopes: { ...(source.scopes ?? {}), [keyBy]: key } });
const statsModel = (name, keyBy, batchSize) => defineReadModel({ name, version: 1, source: matrixSource, keyBy, fold: statsFold, initial: statsInitial, batchSize });

let noise = "";
const noiseFor = (length) => {
  if (noise.length >= length + 4096) return noise;
  let seed = 42;
  noise = Array.from({ length: length + 4096 }, () => {
    seed = (Math.imul(seed, 1103515245) + 12345) >>> 0;
    return "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"[seed >>> 26];
  }).join("");
  return noise;
};
/** Pseudo-random text of `length` characters, so Postgres cannot compress a large payload. */
const textOf = (i, length) => {
  const prefix = `entry ${i} `;
  const at = (i * 7919) % 4096;
  return prefix + noiseFor(length).slice(at, at + Math.max(0, length - prefix.length));
};
const entry = (i, payload, accountId = `a${i % ACCOUNTS}`) =>
  ledger.EntryBooked({ amount: (i * 37) % 1000, category: `c${i % CATEGORIES}`, note: textOf(i, Math.max(16, payload - 100)) }, { ledgerId: "bench", accountId, entryId: `e${i}` });

// ── the list, steady and takeover cells: offers ────────────────────────────────
const sales = defineEvents({
  OfferMade: { data: z.object({ amount: z.number(), status: z.string(), note: z.string() }), scopes: ["offerId"], optionalScopes: ["tenantId"] },
  OfferRevised: { data: z.object({ amount: z.number() }), scopes: ["offerId"], optionalScopes: ["tenantId"] },
});
const offerSource = sales.$filter();
const offerInitial = { status: "none", amount: 0, revisions: 0 };
const offerFold = sales.$fold({
  OfferMade: (d, s) => ({ ...s, status: d.status, amount: d.amount }),
  OfferRevised: (d, s) => ({ ...s, amount: d.amount, revisions: s.revisions + 1 }),
});
const offersModel = defineReadModel({ name: "bench-offers", version: 1, source: offerSource, keyBy: "offerId", fold: offerFold, initial: offerInitial, batchSize: 5000 });
const STATUSES = ["open", "accepted", "draft", "rejected"]; // 25 % open
const offerMade = (i) => sales.OfferMade({ amount: (i * 7_919) % 100_000, status: STATUSES[i % 4], note: textOf(i, 900) }, { offerId: `o${i}` });
const offersMapFold = (events, state) => {
  const next = new Map(state);
  for (const e of events) {
    const k = e.scopes.offerId;
    next.set(k, offerFold([e], next.get(k) ?? offerInitial));
  }
  return next;
};
const LIST = { where: { status: "open" }, sort: { by: "amount", dir: "desc" }, limit: 50 };
/** The model's list order: amount desc, ties by key ascending (code points), then the page. */
const listOf = (map, offset) => {
  const rows = [];
  for (const [key, state] of map) if (state.status === "open") rows.push({ key, state });
  rows.sort((a, b) => b.state.amount - a.state.amount || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  return { rows: rows.slice(offset, offset + LIST.limit), total: rows.length };
};
const samePage = (p, e) => p.total === e.total && isDeepStrictEqual(p.rows.map((r) => [r.key, r.state]), e.rows.map((r) => [r.key, r.state]));

const machineLine = async (admin) => {
  const pgVersion = admin ? (await admin.query("SHOW server_version")).rows[0].server_version : "–";
  return `${cpus()[0]?.model ?? "?"}, ${cpus().length} cores, ${Math.round(totalmem() / 2 ** 30)} GiB, ${process.platform} ${process.arch}, Node ${process.version}, Postgres ${pgVersion}`;
};
const pgApi = (table, extra = {}, postgres = {}) =>
  createEventStore({ events: [ledger, sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table, poolSize: 3, persistedModels: true, ...postgres }, contextCache: { maxEvents: 1_000_000 }, ...extra });

/** The store's internal capability: hold the model's writer lock in this process so a handle here stays a standby. */
async function holdWriterLock(api, name) {
  const cap = (await api.store()).readModelStore;
  const holder = {};
  const detach = cap.attach(holder);
  const ref = { name, version: 1 };
  for (let i = 0; i < 200; i++) {
    const epoch = await cap.lock(ref, holder);
    if (epoch !== null) return async () => {
      await cap.unlock(ref, holder);
      detach();
    };
    await sleep(50); // the previous process's lock session may still be ending
  }
  throw new Error(`could not take the writer lock of ${name} within 10 s`);
}

/** Time every page transaction of the writer in this process (the capability's page(), wrapped in place). */
async function timePages(api) {
  const cap = (await api.store()).readModelStore;
  const times = [];
  const page = cap.page.bind(cap);
  cap.page = async (...args) => {
    const t = performance.now();
    try {
      return await page(...args);
    } finally {
      times.push(performance.now() - t);
    }
  };
  return times;
}

// ════ children ══════════════════════════════════════════════════════════════════

/** One fresh process, one cell of the matrix. */
async function matrixChild(mode, layoutArg) {
  const { keyBy, key } = LAYOUTS[layoutArg];
  const loads = [];
  const api = pgApi(MATRIX_TABLE, { contextCache: { maxEvents: 1_000_000, onLoad: (l) => void loads.push(l) } });
  await api.store(); // install check and the first connection are not part of the load
  const query = keyQueryOf(matrixSource, keyBy, key);
  const onRead = async () => (await api.context({ query, fold: statsFold, initial: statsInitial })).state;
  let out;
  if (mode === "fold") {
    const t0 = performance.now();
    const state = await onRead();
    const cold = performance.now() - t0;
    const warm = [];
    for (let i = 0; i < WARM; i++) {
      const t = performance.now();
      await onRead();
      warm.push(performance.now() - t);
    }
    out = { cold, coldEvents: loads[0].events, warm: median(warm), warmEvents: Math.max(...loads.slice(1).map((l) => l.events)), state };
  } else if (mode === "live") {
    const model = api.readModel(defineReadModel({ name: "bench-live", version: 1, source: matrixSource, keyBy, fold: statsFold, initial: statsInitial, batchSize: 500 }));
    const t0 = performance.now();
    const state = await model.get(key, { timeoutMs: COLD_TIMEOUT_MS });
    const cold = performance.now() - t0;
    const folded = model.status.folded;
    const segments = [];
    for (let s = 0; s < GET_SEGMENTS; s++) {
      const t = performance.now();
      for (let i = 0; i < LIVE_GETS; i++) await model.get(key);
      segments.push((performance.now() - t) / LIVE_GETS);
    }
    out = { cold, coldEvents: folded, warm: median(segments), warmEvents: (model.status.folded - folded) / (GET_SEGMENTS * LIVE_GETS), same: isDeepStrictEqual(state, await onRead()), state };
  } else {
    // persisted | lagging
    const release = mode === "lagging" ? await holdWriterLock(api, `bench-pm-k${layoutArg}`) : undefined;
    const model = api.persistedReadModel(statsModel(`bench-pm-k${layoutArg}`, keyBy, 5000));
    const t0 = performance.now();
    const state = await model.get(key);
    const cold = performance.now() - t0;
    const s1 = await model.status();
    const t1 = performance.now();
    await model.get(key);
    const second = performance.now() - t1;
    const s2 = await model.status();
    const segments = [];
    for (let s = 0; s < GET_SEGMENTS; s++) {
      const t = performance.now();
      for (let i = 0; i < PM_GETS; i++) await model.get(key);
      segments.push((performance.now() - t) / PM_GETS);
    }
    const s3 = await model.status();
    const same = isDeepStrictEqual(state, await onRead());
    out = { cold, coldEvents: s1.readFolded, second, secondEvents: s2.readFolded - s1.readFolded, warm: median(segments), warmEvents: (s3.readFolded - s2.readFolded) / (GET_SEGMENTS * PM_GETS), role: s1.role, lag: s1.lag.events, same, state };
    if (release) await release();
  }
  await api.close();
  process.stdout.write(JSON.stringify({ ...out, state: serialize(out.state).toString("base64") }));
  process.exit(0);
}

/** A fresh process opens the model and waits until it has passed `token`: the build, a rebuild, a resume. */
async function buildChild(table, name, keyBy, batchSize, txid, seq, offers) {
  const api = pgApi(table);
  await api.store();
  const def = offers === "offers" ? offersModel : statsModel(name, keyBy, Number(batchSize));
  const t0 = performance.now();
  const model = api.persistedReadModel(def);
  await model.list({ atLeast: { transactionId: txid, sequence: Number(seq) }, limit: 1, timeoutMs: 1_800_000 });
  const ms = performance.now() - t0;
  const s = await model.status();
  await api.close();
  process.stdout.write(JSON.stringify({ ms, folded: s.folded, foldMs: s.foldMs, role: s.role, epoch: s.epoch, revision: s.revision, checkpoint: s.checkpoint }));
  process.exit(0);
}

/** A fresh process opens the LIVE model of the same definition and waits until its feed has passed `token`: the rebuild in memory. */
async function liveBuildChild(table, name, keyBy, batchSize, txid, seq) {
  const api = pgApi(table);
  await api.store();
  const t0 = performance.now();
  const model = api.readModel(statsModel(name, keyBy, Number(batchSize)));
  await model.list({ atLeast: { transactionId: txid, sequence: Number(seq) }, limit: 1, timeoutMs: 1_800_000 });
  const ms = performance.now() - t0;
  const s = model.status;
  const heapMb = process.memoryUsage().heapUsed / 2 ** 20;
  await api.close();
  process.stdout.write(JSON.stringify({ ms, folded: s.folded, foldMs: s.foldMs, keys: s.keys, revision: s.revision, heapMb }));
  process.exit(0);
}

/** One fresh process: the persisted list, cold (the first call) and warm, first and last page. */
async function listChild(firstCall, lastOffsetArg) {
  const api = pgApi(LIST_TABLE);
  await api.store();
  const model = api.persistedReadModel(offersModel);
  const call = (offset) => model.list({ ...LIST, offset });
  let t = performance.now();
  const probe = firstCall === "first" ? await call(0) : await call(Number(lastOffsetArg));
  const cold = performance.now() - t;
  const lastOffset = Math.max(0, probe.total - LIST.limit);
  const pages = { first: await call(0), last: await call(lastOffset) };
  const warm = { first: [], last: [] };
  for (let i = 0; i < 50; i++) {
    for (const which of ["first", "last"]) {
      t = performance.now();
      await call(which === "first" ? 0 : lastOffset);
      warm[which].push(performance.now() - t);
    }
  }
  const s = await model.status();
  await api.close();
  process.stdout.write(JSON.stringify({ cold, firstCall, warmFirst: median(warm.first), warmLast: median(warm.last), total: probe.total, pages, role: s.role }));
  process.exit(0);
}

/** One fresh process: fold on read + list and the live model's list, first and last page. */
async function listRefChild() {
  const api = pgApi(LIST_TABLE, {}, { live: true });
  await api.store();
  const onRead = async () => (await api.context({ query: offerSource, fold: offersMapFold, initial: () => new Map() })).state;
  let t = performance.now();
  let map = await onRead();
  const foldColdFirst = performance.now() - t;
  const total = listOf(map, 0).total;
  const lastOffset = Math.max(0, total - LIST.limit);
  const expected = { first: listOf(map, 0), last: listOf(map, lastOffset) };
  const warm = { first: [], last: [] };
  for (let i = 0; i < 10; i++) {
    for (const which of ["first", "last"]) {
      t = performance.now();
      map = await onRead();
      listOf(map, which === "first" ? 0 : lastOffset);
      warm[which].push(performance.now() - t);
    }
  }
  t = performance.now();
  const model = api.readModel(defineReadModel({ name: "bench-offers-live", version: 1, source: offerSource, keyBy: "offerId", fold: offerFold, initial: offerInitial, batchSize: 5000 }));
  const livePage = await model.list(LIST);
  const liveRebuild = performance.now() - t;
  const liveWarm = { first: [], last: [] };
  for (let i = 0; i < 200; i++) {
    for (const which of ["first", "last"]) {
      t = performance.now();
      await model.list({ ...LIST, offset: which === "first" ? 0 : lastOffset });
      liveWarm[which].push(performance.now() - t);
    }
  }
  const liveSame = samePage(livePage, expected.first) && samePage(await model.list({ ...LIST, offset: lastOffset }), expected.last);
  await api.close();
  process.stdout.write(
    JSON.stringify({ foldColdFirst, foldWarmFirst: median(warm.first), foldWarmLast: median(warm.last), liveRebuild, liveWarmFirst: median(liveWarm.first), liveWarmLast: median(liveWarm.last), liveSame, expected, lastOffset }),
  );
  process.exit(0);
}

/** Appends `count` OfferRevised for one key, 10 ms apart, and prints when each append returned. */
async function appenderChild(table, count, key) {
  const api = createEventStore({ events: [sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table, live: true, poolSize: 1 } });
  await api.store();
  for (let i = 1; i <= count; i++) {
    await sleep(10);
    const a = await api.append([sales.OfferRevised({ amount: 1_000_000 + i }, { offerId: key })]);
    process.stdout.write(`${JSON.stringify({ i, at: now(), token: tokenOf(a) })}\n`);
  }
  await api.close();
  process.exit(0);
}

/** The writer of the takeover cell: opens the model, says when it is the writer, then waits to be killed. */
async function takeoverWriterChild() {
  const api = pgApi(TAKEOVER_TABLE, {}, { live: true });
  await api.store();
  const model = api.persistedReadModel(offersModel);
  for (;;) {
    const s = await model.status();
    if (s.role === "writer") break;
    await sleep(20);
  }
  process.stdout.write(`${JSON.stringify({ role: "writer", pid: process.pid })}\n`);
  await new Promise(() => undefined);
}

/** N tenant handles in one process: says when all are writers, then waits for "exit" on stdin. */
async function locksChild(n) {
  const api = createEventStore({ events: [sales], connection: url, tenant: { scopeKey: "tenantId" }, postgres: { timeouts: BENCH_TIMEOUTS, table: LOCKS_TABLE, poolSize: 3, persistedModels: true } });
  await api.store();
  const models = Array.from({ length: n }, (_, i) => api.forTenant(`t${i}`).persistedReadModel(offersModel));
  const t0 = performance.now();
  for (;;) {
    const roles = await Promise.all(models.map(async (m) => (await m.status()).role));
    if (roles.every((r) => r === "writer")) break;
    if (performance.now() - t0 > 60_000) throw new Error(`not all ${n} handles became writers within 60 s`);
    await sleep(50);
  }
  process.stdout.write(`${JSON.stringify({ ready: true, ms: performance.now() - t0 })}\n`);
  const lines = createInterface({ input: process.stdin });
  for await (const line of lines) if (line === "exit") break;
  await api.close();
  process.exit(0);
}

if (process.argv[2] === "--child") {
  const [, , , kind, ...a] = process.argv;
  if (kind === "matrix") await matrixChild(a[0], a[1]);
  else if (kind === "build") await buildChild(...a);
  else if (kind === "livebuild") await liveBuildChild(...a);
  else if (kind === "list") await listChild(a[0], a[1]);
  else if (kind === "listref") await listRefChild();
  else if (kind === "appender") await appenderChild(a[0], Number(a[1]), a[2]);
  else if (kind === "takeover-writer") await takeoverWriterChild();
  else if (kind === "locks") await locksChild(Number(a[0]));
  process.exit(2);
}

// ════ parent ════════════════════════════════════════════════════════════════════

const admin = new pg.Pool({ connectionString: url, max: 2, connectionTimeoutMillis: 3000 });
try {
  await admin.query("SELECT 1");
} catch (err) {
  console.error(`bench: Postgres is not reachable at ${url} (${err.code ?? (err.message || err.name)}). Start the Docker container es-sdk-test-pg.`);
  await admin.end().catch(() => undefined);
  process.exit(1);
}
const drop = (table) => admin.query(`DROP TABLE IF EXISTS ${pmTables(table).map((t) => `"${t}"`).join(", ")} CASCADE`);
const analyze = async (table) => {
  for (const t of pmTables(table)) if ((await admin.query("SELECT to_regclass($1) AS r", [`"${t}"`])).rows[0].r) await admin.query(`VACUUM ANALYZE "${t}"`);
};
let failedRuns = 0;
let exitCode = 0;
const red = (message) => {
  console.error(`bench: ${message}`);
  exitCode = 1;
};
const child = (args) => {
  try {
    return JSON.parse(execFileSync(process.execPath, [self, "--child", ...args.map(String)], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, ES_BENCH_DATABASE_URL: url } }));
  } catch (error) {
    const text = String(error.stderr ?? error.message ?? error);
    const file = join(tmpdir(), `bench-pm-failed-${process.pid}-${++failedRuns}.txt`);
    writeFileSync(file, text);
    const first = text.split("\n").find((l) => /(Error|error:)/.test(l)) ?? text.trim().split("\n")[0] ?? "?";
    return { failed: `${first.trim().slice(0, 200)} (stderr: ${file})` };
  }
};
/** JSON lines of a spawned child's stdout, one at a time. */
function linesOf(proc) {
  const lines = createInterface({ input: proc.stdout });
  const queue = [];
  const waiting = [];
  lines.on("line", (line) => {
    if (!line.startsWith("{")) return;
    const parsed = JSON.parse(line);
    const w = waiting.shift();
    if (w) w(parsed);
    else queue.push(parsed);
  });
  return () => (queue.length > 0 ? Promise.resolve(queue.shift()) : new Promise((r) => waiting.push(r)));
}
const seedStore = (table, events) => new PostgresStore({ connection: url, timeouts: BENCH_TIMEOUTS, schema: buildSchema(events, { strict: true }), table, poolSize: 2 });

// ── matrix ──────────────────────────────────────────────────────────────────────
async function matrix(events, payload, reps) {
  await drop(MATRIX_TABLE);
  const seeder = seedStore(MATRIX_TABLE, [ledger, sales]);
  const batch = Math.max(1, Math.min(1000, Math.floor((4 * 1024 * 1024) / payload)));
  const t0 = performance.now();
  let last;
  for (let i = 0; i < events; i += batch) last = await seeder.append(Array.from({ length: Math.min(batch, events - i) }, (_, j) => entry(i + j, payload)));
  await admin.query(`VACUUM ANALYZE "${MATRIX_TABLE}"`);
  await seeder.query(matrixSource);
  const seedS = (performance.now() - t0) / 1000;
  const facts = (await admin.query(`SELECT count(*)::int AS n, round(avg(octet_length(payload::text)))::int AS bytes, pg_size_pretty(pg_total_relation_size('"${MATRIX_TABLE}"')) AS size FROM "${MATRIX_TABLE}"`)).rows[0];
  // the persisted models, built by a process of their own each, which ends
  const builds = {};
  for (const layout of [1, 1000]) {
    builds[layout] = child(["build", MATRIX_TABLE, `bench-pm-k${layout}`, LAYOUTS[layout].keyBy, 5000, last.transactionId, last.last]);
    if (builds[layout].failed) red(`build of the persisted model, K = ${layout}: ${builds[layout].failed}`);
  }
  await analyze(MATRIX_TABLE);

  const runRounds = async (modes, phase) => {
    const cells = [];
    for (const layout of [1, 1000]) for (const m of modes) cells.push({ ...m, layout, cid: `${m.id}@${layout}` });
    const results = Object.fromEntries(cells.map((c) => [c.cid, []]));
    const failures = Object.fromEntries(cells.map((c) => [c.cid, []]));
    const reference = {};
    let unequal = 0;
    for (let round = -1; round < reps; round++) {
      for (let k = 0; k < cells.length; k++) {
        const cell = cells[(k + Math.max(0, round)) % cells.length];
        const r = child(["matrix", cell.id, cell.layout]);
        if (r.failed) {
          failures[cell.cid].push(`${round < 0 ? "warm-up round: " : ""}${r.failed}`);
          red(`${phase}: ${cell.label}, K = ${cell.layout}: a run failed${round < 0 ? " (warm-up round)" : ""}: ${r.failed}`);
          continue;
        }
        const state = deserialize(Buffer.from(r.state, "base64"));
        reference[cell.layout] ??= state;
        if (!isDeepStrictEqual(state, reference[cell.layout])) {
          unequal++;
          red(`${phase}: ${cell.label}, K = ${cell.layout}: a state that differs from the other cells`);
        }
        if (r.same === false) {
          unequal++;
          red(`${phase}: ${cell.label}, K = ${cell.layout}: model.get(k) differs from es.context() in the same process`);
        }
        if (r.warmEvents !== 0) red(`${phase}: ${cell.label}, K = ${cell.layout}: a warm read folded ${r.warmEvents} events per read`);
        if (round >= 0) results[cell.cid].push(r);
      }
    }
    const summary = {};
    for (const cell of cells) {
      const rs = results[cell.cid];
      if (rs.length === 0) {
        summary[cell.cid] = { label: cell.label, layout: cell.layout, failed: failures[cell.cid] };
        continue;
      }
      const pick = (field) => rs.map((r) => r[field]);
      const stat = (xs) => ({ med: median(xs), min: Math.min(...xs), max: Math.max(...xs) });
      summary[cell.cid] = {
        label: cell.label,
        layout: cell.layout,
        n: rs.length,
        fails: failures[cell.cid],
        cold: stat(pick("cold")),
        warm: stat(pick("warm")),
        second: rs[0].second === undefined ? undefined : stat(pick("second")),
        coldEvents: [...new Set(pick("coldEvents"))].join("/"),
        secondEvents: rs[0].secondEvents === undefined ? undefined : [...new Set(pick("secondEvents"))].join("/"),
        warmEvents: Math.max(...pick("warmEvents")),
        roles: rs[0].role === undefined ? undefined : [...new Set(pick("role"))].join("/"),
        lag: rs[0].lag === undefined ? undefined : [...new Set(pick("lag"))].join("/"),
      };
    }
    return { summary, unequal };
  };

  const main = await runRounds(
    [
      { id: "fold", label: "fold on read" },
      { id: "live", label: "live, batchSize 500" },
      { id: "persisted", label: "persisted" },
    ],
    "main",
  );
  // DELTA events for the measured key of both layouts, while no writer runs
  const tail = Array.from({ length: DELTA }, (_, j) => entry(events + j, payload, "a7"));
  for (let i = 0; i < tail.length; i += batch) await seeder.append(tail.slice(i, i + batch));
  await seeder.close();
  await admin.query(`ANALYZE "${MATRIX_TABLE}"`);
  const lag = await runRounds(
    [
      { id: "fold", label: `fold on read (N + ${DELTA})` },
      { id: "lagging", label: `persisted, writer ${DELTA} behind` },
    ],
    "lagging",
  );

  const machine = await machineLine(admin);
  console.log(`persisted read model vs. live read model vs. fold on read — ${events.toLocaleString("en")} events of ${payload} bytes (stored payload ${facts.bytes} bytes as JSON, table ${facts.size})`);
  console.log(`${machine}; ${new Date().toISOString()}; ${reps} fresh processes per cell (+1 warm-up round, not kept), order rotated; seed ${seedS.toFixed(1)} s`);
  console.log(`command: node bench/persisted-model-bench.mjs matrix ${events} ${payload} ${reps}`);
  for (const layout of [1, 1000]) {
    const b = builds[layout];
    console.log(`build of the persisted model, K = ${layout} (a process of its own, batchSize 5 000, from zero to caught up): ${b.failed ? `FAILED ${b.failed}` : `${f(b.ms)} ms, folded ${b.folded}, role ${b.role}, revision ${b.revision}`}`);
  }
  console.log("");
  console.log("| phase | K | cell | cold ms median (min–max) | events folded cold | 2nd get ms median (min–max) | events folded 2nd | warm ms median (min–max of the per-process medians) | events folded per warm read | role | lag.events at open | runs failed |");
  console.log("|---|---|---|---|---|---|---|---|---|---|---|---|");
  for (const [phase, run] of [
    ["main", main],
    [`writer ${DELTA} behind`, lag],
  ]) {
    for (const s of Object.values(run.summary)) {
      if (!s.n) {
        console.log(`| ${phase} | ${s.layout} | ${s.label} | **failed** | – | – | – | – | – | – | – | ${s.failed.length}: ${s.failed[0] ?? "?"} |`);
        red(`${phase}: ${s.label}, K = ${s.layout}: every run failed`);
        continue;
      }
      const r = (x) => `**${f(x.med)}** (${f(x.min)}–${f(x.max)})`;
      console.log(
        `| ${phase} | ${s.layout} | ${s.label} | ${r(s.cold)} | ${s.coldEvents} | ${s.second ? r(s.second) : "–"} | ${s.secondEvents ?? "–"} | ${r(s.warm)} | ${s.warmEvents} | ${s.roles ?? "–"} | ${s.lag ?? "–"} | ${s.fails.length ? `${s.fails.length}: ${s.fails[0]}` : "0"} |`,
      );
    }
  }
  console.log("");
  console.log("10× target (constitution): a read of the model vs. fold on read of the same state, cold vs. cold, warm vs. warm (fold on read ÷ model)");
  for (const layout of [1, 1000]) {
    const fold = main.summary[`fold@${layout}`];
    for (const id of ["persisted", "live"]) {
      const s = main.summary[`${id}@${layout}`];
      if (!fold?.n || !s?.n) {
        console.log(`  K = ${layout}, ${id}: not measured (runs failed)`);
        continue;
      }
      const w = fold.warm.med / s.warm.med;
      const c = fold.cold.med / s.cold.med;
      console.log(`  K = ${layout}, ${id}: warm ${verdict(w)}, ${factor(fold.warm.med, s.warm.med)}× (${fms(fold.warm.med)} → ${fms(s.warm.med)}); cold ${verdict(c)}, ${factor(fold.cold.med, s.cold.med)}× (${fms(fold.cold.med)} → ${fms(s.cold.med)})`);
    }
    const lf = lag.summary[`fold@${layout}`];
    const ls = lag.summary[`lagging@${layout}`];
    if (lf?.n && ls?.n)
      console.log(
        `  K = ${layout}, persisted with the writer ${DELTA} behind: cold ${verdict(lf.cold.med / ls.cold.med)}, ${factor(lf.cold.med, ls.cold.med)}× (${fms(lf.cold.med)} → ${fms(ls.cold.med)}, ${ls.coldEvents} events folded); warm ${verdict(lf.warm.med / ls.warm.med)}, ${factor(lf.warm.med, ls.warm.med)}×`,
      );
  }
  const unequal = main.unequal + lag.unequal;
  console.log(`states equal across cells and rounds of a phase, and model.get(k) equal to es.context() in every model process that finished: ${unequal === 0 ? "yes" : `NO (${unequal})`}`);
  if (process.env.BENCH_JSON) process.stdout.write(`\nJSON ${JSON.stringify({ cell: "matrix", events, payload, reps, machine, facts, builds, main: main.summary, lag: lag.summary })}\n`);
  await drop(MATRIX_TABLE);
}

// ── list ────────────────────────────────────────────────────────────────────────
async function list(rows, reps) {
  await drop(LIST_TABLE);
  const seeder = seedStore(LIST_TABLE, [ledger, sales]);
  const t0 = performance.now();
  let last;
  for (let i = 0; i < rows; i += 1000) last = await seeder.append(Array.from({ length: Math.min(1000, rows - i) }, (_, j) => offerMade(i + j)));
  await admin.query(`VACUUM ANALYZE "${LIST_TABLE}"`);
  await seeder.query(offerSource);
  await seeder.close();
  const build = child(["build", LIST_TABLE, "bench-offers", "offerId", 5000, last.transactionId, last.last, "offers"]);
  if (build.failed) red(`list ${rows}: build failed: ${build.failed}`);
  await analyze(LIST_TABLE);
  const seedS = (performance.now() - t0) / 1000;
  const machine = await machineLine(admin);
  const pm = { first: [], last: [] };
  const warmFirst = [];
  const warmLast = [];
  const refs = [];
  let mismatches = 0;
  let total;
  let lastOffset;
  for (let rep = 0; rep < reps; rep++) {
    const ref = child(["listref"]);
    if (ref.failed) {
      red(`list ${rows}: reference run failed: ${ref.failed}`);
      continue;
    }
    refs.push(ref);
    if (!ref.liveSame) {
      mismatches++;
      red(`list ${rows}: the live model's page differs from fold on read + list`);
    }
    total = ref.expected.first.total;
    lastOffset = ref.lastOffset;
    for (const which of ["first", "last"]) {
      const r = child(["list", which, lastOffset]);
      if (r.failed) {
        red(`list ${rows}: persisted run failed: ${r.failed}`);
        continue;
      }
      pm[which].push(r.cold);
      warmFirst.push(r.warmFirst);
      warmLast.push(r.warmLast);
      if (!samePage(r.pages.first, ref.expected.first) || !samePage(r.pages.last, ref.expected.last)) {
        mismatches++;
        red(`list ${rows}: the persisted page differs from fold on read + list`);
      }
    }
  }
  const m = (xs) => median(xs);
  const mm = (xs) => `**${f(m(xs))}** (${f(Math.min(...xs))}–${f(Math.max(...xs))})`;
  console.log(`list of the persisted read model vs. fold on read + list vs. the live model's kept list — ${rows.toLocaleString("en")} rows, one ~1 KB OfferMade each (25 % open: total ${total}), last page offset ${lastOffset}`);
  console.log(`${machine}; ${new Date().toISOString()}; ${reps} reps (persisted: ${reps} fresh processes with the first page first, ${reps} with the last page first); seed + build ${seedS.toFixed(1)} s (build ${build.failed ? "failed" : `${f(build.ms)} ms, folded ${build.folded}`})`);
  console.log(`command: node bench/persisted-model-bench.mjs list ${rows} ${reps}\n`);
  console.log("| rows | persisted cold, first page | persisted cold, last page | persisted warm, first page | persisted warm, last page | fold on read + list, cold (first page) | fold on read + list, warm first / last | live kept list, warm first / last | live rebuild to the first list |");
  console.log("|---|---|---|---|---|---|---|---|---|");
  const fw = m(refs.map((r) => r.foldWarmFirst));
  const fl = m(refs.map((r) => r.foldWarmLast));
  console.log(
    `| ${rows.toLocaleString("en")} | ${mm(pm.first)} | ${mm(pm.last)} | ${mm(warmFirst)} | ${mm(warmLast)} | ${mm(refs.map((r) => r.foldColdFirst))} | ${f(fw)} / ${f(fl)} | ${fms(m(refs.map((r) => r.liveWarmFirst)))} / ${fms(m(refs.map((r) => r.liveWarmLast)))} | ${f(m(refs.map((r) => r.liveRebuild)))} |`,
  );
  console.log("");
  console.log(`warm, first page: persisted vs. fold on read + list ${verdict(fw / m(warmFirst))}, ${factor(fw, m(warmFirst))}×; last page ${verdict(fl / m(warmLast))}, ${factor(fl, m(warmLast))}×`);
  const fc = m(refs.map((r) => r.foldColdFirst));
  console.log(`cold, first page: persisted vs. fold on read + list ${verdict(fc / m(pm.first))}, ${factor(fc, m(pm.first))}×; vs. the live model's rebuild ${factor(m(refs.map((r) => r.liveRebuild)), m(pm.first))}×`);
  console.log(`pages different from fold on read + list (persisted first and last page per process, live first and last page): ${mismatches}`);
  await drop(LIST_TABLE);
}

// ── rebuild and resume ─────────────────────────────────────────────────────────
async function rebuild(events, reps) {
  const dbSize = async () => Number((await admin.query("SELECT pg_database_size(current_database()) AS b")).rows[0].b);
  const gb = (b) => `${(b / 2 ** 30).toFixed(2)} GB`;
  const sizeBefore = await dbSize();
  await drop(REBUILD_TABLE);
  const seeder = seedStore(REBUILD_TABLE, [ledger, sales]);
  let last;
  const t0 = performance.now();
  for (let i = 0; i < events; i += 1000) last = await seeder.append(Array.from({ length: Math.min(1000, events - i) }, (_, j) => entry(i + j, 1024)));
  const seedS = (performance.now() - t0) / 1000;
  await admin.query(`VACUUM ANALYZE "${REBUILD_TABLE}"`);
  await seeder.query(matrixSource);
  const machine = await machineLine(admin);
  // the cells (decision 56 §2): at 100 000 today's six; above it K = 1 000 at both batch sizes and the write-heavy end
  // (one key per event: keyBy entryId, so K = the number of events) at batchSize 5 000
  const cells = events <= 100_000 ? [1, 1000, 100000].flatMap((layout) => [500, 5000].map((batchSize) => [layout, batchSize])) : [[1000, 500], [1000, 5000], [100000, 5000]];
  const keysOf = (layout) => (layout === 100000 ? events : Math.min(layout, events));
  console.log(`rebuild from zero to caught up — ${events.toLocaleString("en")} events × 1 KB: the persisted read model (Postgres jsonb rows, the write side included) and the live read model (in memory); a fresh process and a fresh model name per run`);
  console.log(`${machine}; ${new Date().toISOString()}; ${reps} runs per cell; seeded through append in batches of 1 000 in ${f(seedS)} s; database ${gb(sizeBefore)} before`);
  console.log(`command: node bench/persisted-model-bench.mjs rebuild ${events} ${reps}\n`);
  console.log("| K (keys) | batchSize | model | to caught up, ms median (min–max) | events/s (median) | folded | fold time, ms (median) | revision | live heap after, MB |");
  console.log("|---|---|---|---|---|---|---|---|---|");
  let run = 0;
  for (const [layout, batchSize] of cells) {
    const K = keysOf(layout);
    for (const kind of ["persisted", "live"]) {
      const rs = [];
      for (let rep = 0; rep < reps; rep++) {
        const r = child([kind === "persisted" ? "build" : "livebuild", REBUILD_TABLE, `rebuild-${layout}-${batchSize}-${++run}`, LAYOUTS[layout].keyBy, batchSize, last.transactionId, last.last]);
        if (r.failed) red(`rebuild ${kind} K = ${K}, batchSize ${batchSize}: ${r.failed}`);
        else {
          rs.push(r);
          if (r.folded !== events) red(`rebuild ${kind} K = ${K}, batchSize ${batchSize}: folded ${r.folded}, not ${events}`);
          if (kind === "live" && r.keys !== K) red(`rebuild live K = ${K}, batchSize ${batchSize}: holds ${r.keys} keys, not ${K}`);
        }
        if (kind === "persisted") await analyze(REBUILD_TABLE);
      }
      const ms = rs.map((r) => r.ms);
      console.log(
        `| ${K.toLocaleString("en")} | ${batchSize} | ${kind} | ${rs.length ? `**${f(median(ms))}** (${f(Math.min(...ms))}–${f(Math.max(...ms))})` : "red: no run finished"} | ${rs.length ? Math.round(events / (median(ms) / 1000)).toLocaleString("en") : "–"} | ${[...new Set(rs.map((r) => r.folded))].join("/")} | ${f(median(rs.map((r) => r.foldMs)))} | ${[...new Set(rs.map((r) => r.revision))].join("/")} | ${kind === "live" && rs.length ? f(median(rs.map((r) => r.heapMb))) : "–"} |`,
      );
    }
  }
  const sizeWith = await dbSize();
  if (events > 100_000) {
    await seeder.close();
    await drop(REBUILD_TABLE);
    console.log(`\ndatabase ${gb(sizeBefore)} before, ${gb(sizeWith)} with the events and every built model's rows, ${gb(await dbSize())} after the drop (the resume cell runs at 100 000 only)`);
    return;
  }
  console.log(`\ndatabase ${gb(sizeBefore)} before, ${gb(sizeWith)} with the events and every built model's rows`);
  // resume: a model caught up, 1 000 new events, a fresh writer
  const name = "resume-k1000";
  const built = child(["build", REBUILD_TABLE, name, "accountId", 500, last.transactionId, last.last]);
  if (built.failed) red(`resume: build failed: ${built.failed}`);
  console.log("");
  console.log(`resume (K = 1 000, batchSize 500; built first in ${f(built.ms)} ms): 1 000 new events appended while no writer runs, then a fresh process opens the model`);
  console.log("| run | to caught up, ms | folded | epoch | role |");
  console.log("|---|---|---|---|---|");
  for (let rep = 0; rep < reps; rep++) {
    for (let i = 0; i < 1000; i += 500) last = await seeder.append(Array.from({ length: 500 }, (_, j) => entry(events + rep * 1000 + i + j, 1024)));
    await admin.query(`ANALYZE "${REBUILD_TABLE}"`);
    const r = child(["build", REBUILD_TABLE, name, "accountId", 500, last.transactionId, last.last]);
    if (r.failed) {
      red(`resume ${rep + 1}: ${r.failed}`);
      continue;
    }
    if (r.folded !== 1000) red(`resume ${rep + 1}: folded ${r.folded}, not 1000`);
    console.log(`| ${rep + 1} | **${f(r.ms)}** | ${r.folded} | ${r.epoch} | ${r.role} |`);
  }
  await seeder.close();
  await drop(REBUILD_TABLE);
  console.log(`database ${gb(await dbSize())} after the drop`);
}

// ── steady state ───────────────────────────────────────────────────────────────
async function steady(appends) {
  const machine = await machineLine(admin);
  console.log(`steady state: one append every 10 ms, ${appends} appends per path; the writer and a watch("o1") in this process`);
  console.log(`${machine}; ${new Date().toISOString()}`);
  console.log(`command: node bench/persisted-model-bench.mjs steady ${appends}\n`);
  console.log("| path | page transaction ms, median (p90) | pages | append → the delivery that holds it, median (p90, max) | appends never delivered | deliveries | append → get(k) returned, median (p90) | events folded per get after an append | get equal to es.context() |");
  console.log("|---|---|---|---|---|---|---|---|---|");
  const row = (label, r) =>
    console.log(
      `| ${label} | ${r.pages.length ? `**${f(median(r.pages))}** (${f(pct(r.pages, 90))})` : "not measured (page() not reached through the store object)"} | ${r.pages.length} | **${f(median(r.watch))}** (${f(pct(r.watch, 90))}, ${f(Math.max(...r.watch))}) | ${r.missed} | ${r.deliveries} | ${r.get.length ? `**${f(median(r.get))}** (${f(pct(r.get, 90))})` : "–"} | ${r.getFolded} | ${r.same ? "yes" : "NO"} |`,
    );
  const inProcess = async (api) => {
    const pages = await timePages(api);
    const model = api.persistedReadModel(offersModel);
    await api.append([offerMade(1)]);
    const seen = [];
    await model.watch("o1", (s) => void seen.push({ amount: s.amount, at: now() }));
    while ((await model.status()).role !== "writer") await sleep(10);
    const sent = [];
    const get = [];
    let folded = 0;
    for (let i = 1; i <= appends; i++) {
      await sleep(10);
      await api.append([sales.OfferRevised({ amount: 1_000_000 + i }, { offerId: "o1" })]);
      const at = now();
      sent.push({ amount: 1_000_000 + i, at });
      const before = (await model.status()).readFolded;
      const t = performance.now();
      await model.get("o1");
      get.push(performance.now() - t);
      folded += (await model.status()).readFolded - before;
    }
    await sleep(1_000);
    const watch = covering(sent, seen);
    const same = isDeepStrictEqual(await model.get("o1"), (await api.context({ query: keyQueryOf(offerSource, "offerId", "o1"), fold: offerFold, initial: offerInitial })).state);
    return { pages: pages.slice(1), watch, missed: sent.length - watch.length, deliveries: seen.length, get, getFolded: (folded / appends).toFixed(2), same };
  };
  // memory
  {
    const store = new MemoryStore({ schema: buildSchema([sales], { strict: true }), persistedModels: true });
    const api = createEventStore({ events: [sales], store });
    row("memory store, one process", await inProcess(api));
    await api.close();
  }
  // Postgres, one process
  await drop(STEADY_TABLE);
  {
    const api = pgApi(STEADY_TABLE, {}, { live: true });
    await api.store();
    row("Postgres, one process, doorbell (`live: true`)", await inProcess(api));
    await api.close();
  }
  // Postgres, appends from a second OS process
  await drop(STEADY_TABLE);
  {
    const api = pgApi(STEADY_TABLE, {}, { live: true });
    await api.store();
    const pages = await timePages(api);
    const model = api.persistedReadModel(offersModel);
    await api.append([offerMade(1)]);
    const seen = [];
    await model.watch("o1", (s) => void seen.push({ amount: s.amount, at: now() }));
    while ((await model.status()).role !== "writer") await sleep(10);
    const proc = spawn(process.execPath, [self, "--child", "appender", STEADY_TABLE, String(appends), "o1"], { stdio: ["ignore", "pipe", "inherit"] });
    const next = linesOf(proc);
    const sent = [];
    for (let i = 0; i < appends; i++) sent.push(await next());
    await new Promise((r) => (proc.exitCode !== null ? r() : proc.on("exit", r)));
    await sleep(1_000);
    const watch = covering(
      sent.map((s) => ({ amount: 1_000_000 + s.i, at: s.at })),
      seen,
    );
    const same = isDeepStrictEqual(await model.get("o1"), (await api.context({ query: keyQueryOf(offerSource, "offerId", "o1"), fold: offerFold, initial: offerInitial })).state);
    row("Postgres, appends from a second OS process, doorbell", { pages: pages.slice(1), watch, missed: sent.length - watch.length, deliveries: seen.length, get: [], getFolded: "–", same });
    await api.close();
  }
  await drop(STEADY_TABLE);
}

// ── takeover ───────────────────────────────────────────────────────────────────
async function takeover(reps) {
  await drop(TAKEOVER_TABLE);
  const machine = await machineLine(admin);
  console.log("takeover: the writer process is SIGKILLed; this process holds a standby handle; one event is appended right after the kill");
  console.log(`${machine}; ${new Date().toISOString()}; default tick (1 s), live: true`);
  console.log(`command: node bench/persisted-model-bench.mjs takeover ${reps}\n`);
  console.log("| run | kill → standby becomes writer, ms | kill → the new event applied (list atLeast returned), ms | epoch before → after | the event applied once |");
  console.log("|---|---|---|---|---|");
  const roles = [];
  const applied = [];
  for (let rep = 0; rep < reps; rep++) {
    const proc = spawn(process.execPath, [self, "--child", "takeover-writer"], { stdio: ["ignore", "pipe", "inherit"] });
    const next = linesOf(proc);
    await next();
    const api = pgApi(TAKEOVER_TABLE, {}, { live: true });
    await api.store();
    const model = api.persistedReadModel(offersModel);
    const before = await model.status();
    if (before.role !== "standby") red(`takeover ${rep + 1}: this handle is ${before.role}, not standby`);
    await api.append([offerMade(1000 + rep)]);
    const t0 = performance.now();
    proc.kill("SIGKILL");
    const a = await api.append([sales.OfferRevised({ amount: 4242 }, { offerId: `o${1000 + rep}` })]);
    let becameWriter;
    const waiting = model.list({ atLeast: tokenOf(a), limit: 1, timeoutMs: 60_000 }).then(() => performance.now() - t0);
    while (becameWriter === undefined) {
      if ((await model.status()).role === "writer") becameWriter = performance.now() - t0;
      else await sleep(5);
    }
    const appliedMs = await waiting;
    const after = await model.status();
    const state = await model.get(`o${1000 + rep}`);
    const once = state.revisions === 1 && state.amount === 4242;
    if (!once) red(`takeover ${rep + 1}: state ${JSON.stringify(state)}`);
    roles.push(becameWriter);
    applied.push(appliedMs);
    console.log(`| ${rep + 1} | **${f(becameWriter)}** | **${f(appliedMs)}** | ${before.epoch} → ${after.epoch} | ${once ? "yes" : "NO"} |`);
    await api.close();
    await new Promise((r) => (proc.exitCode !== null || proc.signalCode !== null ? r() : proc.on("exit", r)));
  }
  console.log(`\nmedian: kill → writer ${f(median(roles))} ms, kill → applied ${f(median(applied))} ms`);
  await drop(TAKEOVER_TABLE);
}

// ── lock slots ─────────────────────────────────────────────────────────────────
async function locks() {
  await drop(LOCKS_TABLE);
  // install the tables once, so the child's handles do not race the install
  {
    const api = createEventStore({ events: [sales], connection: url, tenant: { scopeKey: "tenantId" }, postgres: { timeouts: BENCH_TIMEOUTS, table: LOCKS_TABLE, poolSize: 1, persistedModels: true } });
    await api.store();
    await api.close();
  }
  const machine = await machineLine(admin);
  const count = async () =>
    (
      await admin.query(
        "SELECT (SELECT count(*)::int FROM pg_locks WHERE locktype = 'advisory' AND granted) AS advisory, (SELECT count(DISTINCT pid)::int FROM pg_locks WHERE locktype = 'advisory' AND granted) AS holders, (SELECT count(*)::int FROM pg_stat_activity WHERE datname = current_database()) AS backends",
      )
    ).rows[0];
  const settings = (await admin.query("SELECT current_setting('max_locks_per_transaction') AS mlpt, current_setting('max_connections') AS mc, current_setting('max_prepared_transactions') AS mpt")).rows[0];
  console.log(`lock slots: N tenant handles of one model in one process (forTenant(t).persistedReadModel(def)), all writers`);
  console.log(`${machine}; ${new Date().toISOString()}; server: max_locks_per_transaction ${settings.mlpt}, max_connections ${settings.mc}, max_prepared_transactions ${settings.mpt}`);
  console.log("command: node bench/persisted-model-bench.mjs locks\n");
  console.log("| N handles | advisory locks held (before → with the handles) | sessions holding them | backends of the database (before → with the handles) | all writers after, ms |");
  console.log("|---|---|---|---|---|");
  for (const n of [10, 100]) {
    const before = await count();
    const proc = spawn(process.execPath, [self, "--child", "locks", String(n)], { stdio: ["pipe", "pipe", "inherit"] });
    const next = linesOf(proc);
    const ready = await next();
    const during = await count();
    const extra = during.advisory - before.advisory;
    if (extra !== n) red(`locks N = ${n}: ${extra} advisory locks held, not ${n}`);
    console.log(`| ${n} | ${before.advisory} → **${during.advisory}** (+${extra}) | ${before.holders} → ${during.holders} | ${before.backends} → ${during.backends} | ${f(ready.ms)} |`);
    proc.stdin.write("exit\n");
    await new Promise((r) => (proc.exitCode !== null ? r() : proc.on("exit", r)));
    await sleep(200);
  }
  await drop(LOCKS_TABLE);
}

try {
  const [what, a, b, c] = process.argv.slice(2);
  if (what === "matrix") await matrix(Number(a ?? 10000), Number(b ?? 1024), Number(c ?? 11));
  else if (what === "list") await list(Number(a ?? 10000), Number(b ?? 5));
  else if (what === "rebuild") await rebuild(Number(a ?? 100000), Number(b ?? 3));
  else if (what === "steady") await steady(Number(a ?? 200));
  else if (what === "takeover") await takeover(Number(a ?? 5));
  else if (what === "locks") await locks();
  else {
    console.error("usage: node bench/persisted-model-bench.mjs matrix <events> <payloadBytes> [reps] | list <rows> [reps] | rebuild [events] [reps] | steady [appends] | takeover [reps] | locks");
    exitCode = 2;
  }
} catch (error) {
  red(`${error?.stack ?? error}`);
} finally {
  await admin.end();
}
process.exit(exitCode);
