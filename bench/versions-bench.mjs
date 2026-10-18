// Versions and rebuild (stage 5) on the local Postgres test database. Build first, or run
// `sh bench/versions.sh`, which builds and then runs this.
//   node bench/versions-bench.mjs beside <K> [reps=3]           rebuild beside the old version, 100 000 × 1 KB
//   node bench/versions-bench.mjs live <events> <payloadBytes> [reps=11]   the live model's start: "persisted", cold
//   node bench/versions-bench.mjs paged [reps=3]                 the persisted fold after a bump, 100 000 and 100 500 events
//   node bench/versions-bench.mjs retire [reps=3]                the README's retire recipe at 1 000 and 100 000 rows
// Tables of its own (bench_v_*, with their _read_model, _read_model_state and _fold_state), dropped at the start and the end.
// (The fingerprint's cost is internal, not in dist/: bench/fingerprint-bench.mjs, run with vite-node.)
//
// beside (knowledge/model.md stage 5 §9): 100 000 × 1 KB, K = 1 000 (keyBy accountId) or 100 000 (keyBy entryId);
//   per run a fresh model name; v1 (batchSize 5 000) is built by a process of its own, which ends. Then this process
//   opens v1 (it becomes v1's writer), a second process appends one event every 5 ms, and this process samples
//   v1.get(key) and v1.list({ limit: 50 }) back to back with 10 ms pauses: 3 s "before", then v2 (same fold, version 2,
//   batchSize 5 000) is opened —
//     one process:  in this process's runtime (the same pool as v1's writer and the sampler)
//     two processes: in a fresh node process of its own (the rolling-deploy shape)
//   — and the samples until v2's status says building: false are "during". Time open → switch is from the
//   persistedReadModel(v2) call (store connected before) to the first status() poll (every 25 ms) that says
//   building: false; v2's lag.events is that poll's. After the switch the stream stops and v2.get of three keys is
//   compared with fold on read (v2's fold) in this process. `postgres: { live: true }`, pool 10 (the default).
//
// live: N events × payload, layouts K = 1 (ledgerId), 1 000 (accountId) and, at 100 000 × 1 KB, 100 000 (entryId);
//   the persisted model of each layout built beforehand by a process of its own (batchSize 500), which ends.
//   Then per cell `reps` fresh processes after one warm-up round (not kept), the order rotated per round:
//     fold on read          the first es.context() of the key
//     live, rebuild         readModel(def) → the first awaited get(key) (the whole source)
//     live, persisted       readModel(def, { start: "persisted" }) → the first awaited get(key) (one snapshot + the delta)
//   "cold" = a fresh node process with the store installed and connected before the clock. get uses
//   timeoutMs 600 000, so a long rebuild gives a number instead of read_model_timeout (stages 3/4 used the default
//   10 000 and recorded those timeouts red; they stay recorded). Then DELTA events for the measured key of K = 1 and
//   K = 1 000 are appended while no writer runs, and two cells again: fold on read (N + DELTA) and live, persisted
//   (it folds the DELTA). Every state is compared with fold on read in the same process.
//
// paged: one key holds every event (1 KB), contextCache default (maxEvents 100 000, persist.after 500); every step a
//   fresh process. Run A at 100 000 events: v1 first, v1 again, v2 (the bump) first, v2 again. Run B: v1 first at
//   100 000, then 500 events appended, then v1 (stored + 500), v2 first (100 500 > maxEvents: paged), v2 again.
//   Each step's onLoad: start, events, readMs, foldMs, stored.readMs, stored.writeMs, written; the state compared
//   with a fold from zero.
//
// retire: 100 000 × 1 KB; K = 1 000 / 100 000 rows; per run a fresh name: v1 built, then v2 built beside it (it
//   switches), both processes end; then the README's recipe as printed ("Retiring an old version"), as the owner, in
//   one transaction — timed — and the rows left per version counted.

import { execFileSync, spawn } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { cpus, loadavg, tmpdir, totalmem } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { deserialize, serialize } from "node:v8";
import pg from "pg";
import { z } from "zod";
import { buildSchema, createEventStore, defineEvents, defineFold, defineReadModel } from "../dist/index.js";
import { PostgresStore } from "../dist/postgres/index.js";

const url = process.env.ES_BENCH_DATABASE_URL ?? "postgres://es:es@localhost:5432/es_sdk_test";
// Bench timeouts (decision 54): every store is opened with statement_timeout 600 000 ms and every cold model read
// passes timeoutMs 600 000, so a slow cold read gives a number instead of 57014 / read_model_timeout. The library's
// defaults (statement_timeout 30 000 ms, timeoutMs 10 000) are unchanged.
const BENCH_TIMEOUTS = { statementMs: 600_000 };
const COLD_TIMEOUT_MS = 600_000;
const self = fileURLToPath(import.meta.url);
const BESIDE_TABLE = "bench_v_beside";
const LIVE_TABLE = "bench_v_live";
const PAGED_TABLE = "bench_v_paged";
const RETIRE_TABLE = "bench_v_retire";
const CATEGORIES = 40;
const ACCOUNTS = 1_000;
const DELTA = 100;
const BEFORE_MS = 3_000;

const now = () => performance.timeOrigin + performance.now(); // wall clock in ms, comparable across processes
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length === 0 ? NaN : s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const pct = (xs, p) => {
  const s = [...xs].sort((a, b) => a - b);
  return s.length === 0 ? NaN : s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
};
const f = (n) => (n === undefined || Number.isNaN(n) ? "–" : n < 0.01 ? `${(n * 1000).toFixed(2)} µs` : n < 0.1 ? n.toFixed(4) : n < 10 ? n.toFixed(2) : n.toFixed(1));
const factor = (a, b) => {
  const x = a / b;
  return x >= 100 ? Math.round(x).toLocaleString("en") : x.toFixed(2);
};
const verdict = (x) => (x >= 10 ? "reached" : '"not reached"');
const range = (xs) => `${f(median(xs))} (${f(Math.min(...xs))}–${f(Math.max(...xs))})`;
const tokenOf = (a) => ({ transactionId: a.transactionId, sequence: a.last });
const modelTables = (table) => [`${table}_read_model_state`, `${table}_read_model`, `${table}_fold_state`, table];

// ── events and folds (bench/persisted-model-bench.mjs's ledger) ──────────────────
const ledger = defineEvents({
  EntryBooked: { data: z.object({ amount: z.number(), category: z.string(), note: z.string() }), scopes: ["ledgerId", "accountId", "entryId"] },
});
const source = ledger.$filter();
const schema = buildSchema([ledger], { strict: true });
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
const keyQueryOf = (keyBy, key) => ({ ...source, scopes: { ...(source.scopes ?? {}), [keyBy]: key } });
const modelDef = (name, version, keyBy, batchSize) => defineReadModel({ name, version, source, keyBy, fold: statsFold, initial: statsInitial, batchSize });

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
const textOf = (i, length) => {
  const prefix = `entry ${i} `;
  const at = (i * 7919) % 4096;
  return prefix + noiseFor(length).slice(at, at + Math.max(0, length - prefix.length));
};
/** Event i: ledger "bench", account a{i % 1 000}, entry e{i} — or the scopes given. */
const entry = (i, payload, scopes = {}) =>
  ledger.EntryBooked({ amount: (i * 37) % 1000, category: `c${i % CATEGORIES}`, note: textOf(i, Math.max(16, payload - 100)) }, { ledgerId: "bench", accountId: `a${i % ACCOUNTS}`, entryId: `e${i}`, ...scopes });

const pgApi = (table, extra = {}) => createEventStore({ events: [ledger], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table, live: true, persistedModels: true, persistedFolds: true }, contextCache: { maxEvents: 1_000_000 }, ...extra });
const machineLine = async (admin) => {
  const pgVersion = (await admin.query("SHOW server_version")).rows[0].server_version;
  return `${cpus()[0]?.model ?? "?"}, ${cpus().length} cores, ${Math.round(totalmem() / 2 ** 30)} GiB, ${process.platform} ${process.arch}, Node ${process.version}, Postgres ${pgVersion}; load average ${loadavg().map((x) => x.toFixed(1)).join(" / ")}`;
};
const sendState = (state) => serialize(state).toString("base64");

// ════ children ══════════════════════════════════════════════════════════════════

/** A fresh process opens a model and waits until it has passed `token` (and, for a building version, its switch). */
async function buildChild(table, name, version, keyBy, batchSize, txid, seq) {
  const api = pgApi(table);
  await api.store();
  const t0 = performance.now();
  const model = api.persistedReadModel(modelDef(name, Number(version), keyBy, Number(batchSize)));
  await model.list({ atLeast: { transactionId: txid, sequence: Number(seq) }, limit: 1, timeoutMs: 1_800_000 });
  const ms = performance.now() - t0;
  const s = await model.status();
  await api.close();
  process.stdout.write(JSON.stringify({ ms, folded: s.folded, building: s.building, checkpoint: s.checkpoint }));
  process.exit(0);
}

/** Appends one event every `everyMs` until "exit" on stdin; prints the count and the last token. */
async function appenderChild(table, everyMs, start) {
  const api = pgApi(table);
  await api.store();
  let stop = false;
  let n = 0;
  let last = null;
  createInterface({ input: process.stdin }).on("line", (line) => {
    if (line === "exit") stop = true;
  });
  process.stdout.write(`${JSON.stringify({ ready: true })}\n`);
  let i = Number(start);
  while (!stop) {
    const t = performance.now();
    last = tokenOf(await api.append([entry(i++, 1024)]));
    n++;
    const rest = Number(everyMs) - (performance.now() - t);
    if (rest > 0) await sleep(rest);
  }
  process.stdout.write(`${JSON.stringify({ n, last })}\n`);
  await api.close();
  process.exit(0);
}

/** The rolling-deploy shape: v2 in a process of its own; prints when it was opened and when it switched, then stays its writer until "exit". */
async function v2Child(table, name, keyBy) {
  const api = pgApi(table);
  await api.store();
  const openedAt = now();
  const v2 = api.persistedReadModel(modelDef(name, 2, keyBy, 5000));
  const polls = [];
  for (;;) {
    const s = await v2.status();
    polls.push(s.building);
    if (!s.building) {
      process.stdout.write(`${JSON.stringify({ openedAt, switchedAt: now(), lag: s.lag.events, role: s.role, checkpoint: s.checkpoint, polls: polls.length })}\n`);
      break;
    }
    await sleep(25);
  }
  const lines = createInterface({ input: process.stdin });
  for await (const line of lines) if (line === "exit") break;
  await api.close();
  process.exit(0);
}

/** One cold cell of the live matrix. */
async function liveChild(mode, layoutArg, name) {
  const { keyBy, key } = LAYOUTS[layoutArg];
  const loads = [];
  const api = pgApi(LIVE_TABLE, { contextCache: { maxEvents: 1_000_000, onLoad: (l) => void loads.push(l) } });
  await api.store(); // installed and connected before the clock
  const onRead = async () => (await api.context({ query: keyQueryOf(keyBy, key), fold: statsFold, initial: statsInitial })).state;
  let out;
  if (mode === "fold") {
    const t0 = performance.now();
    const state = await onRead();
    out = { cold: performance.now() - t0, events: loads[0].events, state, same: true };
  } else {
    const def = modelDef(name, 1, keyBy, 500);
    const t0 = performance.now();
    const model = mode === "persisted" ? api.readModel(def, { start: "persisted" }) : api.readModel(def);
    const state = await model.get(key, { timeoutMs: COLD_TIMEOUT_MS });
    const cold = performance.now() - t0;
    const folded = model.status.folded;
    out = { cold, events: folded, state, same: isDeepStrictEqual(state, await onRead()) };
  }
  await api.close();
  process.stdout.write(JSON.stringify({ ...out, state: sendState(out.state) }));
  process.exit(0);
}

/** One step of the paged-fold run: a fresh process, one load of the defined fold `name` v`version`. */
async function pagedChild(name, version) {
  const loads = [];
  const api = createEventStore({ events: [ledger], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: PAGED_TABLE, persistedFolds: true }, contextCache: { onLoad: (l) => void loads.push(l) } });
  await api.store();
  const fold = defineFold({ name, version: Number(version), fold: statsFold });
  const t0 = performance.now();
  let state;
  let error;
  try {
    state = (await api.context({ query: keyQueryOf("ledgerId", "bench"), fold, initial: statsInitial })).state;
  } catch (e) {
    error = `${e.name}: ${e.message}`.slice(0, 200);
  }
  const ms = performance.now() - t0;
  const l = loads[0];
  await api.close();
  process.stdout.write(JSON.stringify({ ms, error, start: l?.start, events: l?.events, readMs: l?.readMs, foldMs: l?.foldMs, storedReadMs: l?.stored?.readMs, writeMs: l?.stored?.writeMs, written: l?.stored?.written, storedError: l?.stored?.error ? String(l.stored.error) : undefined, state: state ? sendState(state) : null }));
  process.exit(0);
}

if (process.argv[2] === "--child") {
  const [, , , kind, ...a] = process.argv;
  if (kind === "build") await buildChild(...a);
  else if (kind === "appender") await appenderChild(...a);
  else if (kind === "v2") await v2Child(...a);
  else if (kind === "live") await liveChild(...a);
  else if (kind === "paged") await pagedChild(...a);
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
const drop = (table) => admin.query(`DROP TABLE IF EXISTS ${modelTables(table).map((t) => `"${t}"`).join(", ")} CASCADE`);
const analyze = async (table) => {
  for (const t of modelTables(table)) if ((await admin.query("SELECT to_regclass($1) AS r", [`"${t}"`])).rows[0].r) await admin.query(`VACUUM ANALYZE "${t}"`);
};
let exitCode = 0;
let failedRuns = 0;
const red = (message) => {
  console.error(`bench: RED: ${message}`);
  exitCode = 1;
};
const child = (args) => {
  try {
    return JSON.parse(execFileSync(process.execPath, [self, "--child", ...args.map(String)], { encoding: "utf8", maxBuffer: 256 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, ES_BENCH_DATABASE_URL: url } }));
  } catch (error) {
    const text = String(error.stderr ?? error.message ?? error);
    const file = join(tmpdir(), `bench-v-failed-${process.pid}-${++failedRuns}.txt`);
    writeFileSync(file, text);
    const first = text.split("\n").find((l) => /(Error|error:)/.test(l)) ?? text.trim().split("\n")[0] ?? "?";
    return { failed: `${first.trim().slice(0, 200)} (stderr: ${file})` };
  }
};
/** A spawned child and its JSON stdout lines, one at a time. */
function spawned(args) {
  const proc = spawn(process.execPath, [self, "--child", ...args.map(String)], { stdio: ["pipe", "pipe", "inherit"], env: { ...process.env, ES_BENCH_DATABASE_URL: url } });
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
  const next = () => (queue.length > 0 ? Promise.resolve(queue.shift()) : new Promise((r) => waiting.push(r)));
  const exited = new Promise((r) => proc.on("exit", r));
  return { proc, next, exited, send: (line) => proc.stdin.write(`${line}\n`) };
}
async function seed(table, events, payload) {
  await drop(table);
  const seeder = new PostgresStore({ connection: url, timeouts: BENCH_TIMEOUTS, schema, table, persistedModels: true, persistedFolds: true, poolSize: 2 });
  const batch = Math.max(1, Math.min(1000, Math.floor((4 * 1024 * 1024) / payload)));
  const t0 = performance.now();
  let last;
  for (let i = 0; i < events; i += batch) last = await seeder.append(Array.from({ length: Math.min(batch, events - i) }, (_, j) => entry(i + j, payload)));
  await admin.query(`VACUUM ANALYZE "${table}"`);
  const facts = (await admin.query(`SELECT count(*)::int AS n, round(avg(octet_length(payload::text)))::int AS bytes, pg_size_pretty(pg_total_relation_size('"${table}"')) AS size FROM "${table}"`)).rows[0];
  console.log(`seeded ${facts.n} events, payload ~${facts.bytes} B as text, table ${facts.size}, in ${((performance.now() - t0) / 1000).toFixed(1)} s`);
  return { seeder, last: tokenOf(last) };
}

// ── beside ──────────────────────────────────────────────────────────────────────
async function beside(K, reps) {
  const { keyBy, key } = LAYOUTS[K];
  const { seeder, last: seeded } = await seed(BESIDE_TABLE, 100_000, 1024);
  await seeder.close();
  const rows = [];
  for (let run = 0; run < reps; run++) {
    for (const shape of ["one", "two"]) {
      const name = `bv-k${K}-${shape}-r${run}`;
      const head = (await admin.query(`SELECT transaction_id::text AS x, sequence_number AS s FROM "${BESIDE_TABLE}" ORDER BY transaction_id DESC, sequence_number DESC LIMIT 1`)).rows[0];
      const built = child(["build", BESIDE_TABLE, name, 1, keyBy, 5000, head.x, head.s]);
      if (built.failed) {
        red(`beside K = ${K} ${shape}: v1 build failed: ${built.failed}`);
        continue;
      }
      await analyze(BESIDE_TABLE);
      const api = pgApi(BESIDE_TABLE);
      await api.store();
      const v1 = api.persistedReadModel(modelDef(name, 1, keyBy, 5000));
      await v1.list({ limit: 1 });
      for (let i = 0; i < 200 && (await v1.status()).role !== "writer"; i++) await sleep(50);
      const v1Role = (await v1.status()).role;
      const appender = spawned(["appender", BESIDE_TABLE, 5, 1_000_000 + run * 100_000 + (shape === "two" ? 50_000 : 0)]);
      await appender.next();
      const samples = []; // { at, op, ms }
      let sampling = true;
      const sampler = (async () => {
        while (sampling) {
          let t = now();
          await v1.get(key);
          samples.push({ at: t, op: "get", ms: now() - t });
          t = now();
          await v1.list({ limit: 50 });
          samples.push({ at: t, op: "list", ms: now() - t });
          await sleep(10);
        }
      })();
      await sleep(BEFORE_MS);
      let openedAt;
      let switchedAt;
      let lag;
      let v2Proc;
      let v2Local;
      if (shape === "one") {
        openedAt = now();
        v2Local = api.persistedReadModel(modelDef(name, 2, keyBy, 5000));
        for (;;) {
          const s = await v2Local.status();
          if (!s.building) {
            switchedAt = now();
            lag = s.lag.events;
            break;
          }
          if (now() - openedAt > 600_000) break;
          await sleep(25);
        }
      } else {
        v2Proc = spawned(["v2", BESIDE_TABLE, name, keyBy]);
        const r = await v2Proc.next();
        openedAt = r.openedAt;
        switchedAt = r.switchedAt;
        lag = r.lag;
      }
      await sleep(200); // a few samples after the switch
      sampling = false;
      await sampler;
      appender.send("exit");
      const appended = await appender.next();
      await appender.exited;
      // equivalence after the switch: v2.get(k) vs fold on read with v2's fold, for three keys
      const check = v2Local ?? api.persistedReadModel(modelDef(name, 2, keyBy, 5000));
      let mismatches = 0;
      const keys = K === 1000 ? [key, "a500", "a999"] : [key, "e50000", "e99999"];
      for (const k of keys) {
        const want = (await api.context({ query: keyQueryOf(keyBy, k), fold: statsFold, initial: statsInitial })).state;
        if (!isDeepStrictEqual(await check.get(k), want)) mismatches++;
      }
      const listed = await check.list({ atLeast: appended.last, limit: 1, timeoutMs: 120_000 });
      if (v2Proc) {
        v2Proc.send("exit");
        await v2Proc.exited;
      }
      await api.close();
      if (switchedAt === undefined) red(`beside K = ${K} ${shape} run ${run}: v2 did not switch within 600 s`);
      if (mismatches > 0) red(`beside K = ${K} ${shape} run ${run}: ${mismatches} of ${keys.length} v2.get differ from fold on read`);
      const pick = (op, from, to) => samples.filter((s) => s.op === op && s.at >= from && s.at < to).map((s) => s.ms);
      const before = { get: pick("get", openedAt - BEFORE_MS, openedAt), list: pick("list", openedAt - BEFORE_MS, openedAt) };
      const during = { get: pick("get", openedAt, switchedAt ?? Infinity), list: pick("list", openedAt, switchedAt ?? Infinity) };
      const row = { K, shape, run, v1Role, ms: switchedAt - openedAt, lag, appended: appended.n, before, during, mismatches, total: listed.total, v1BuildMs: built.ms };
      rows.push(row);
      console.log(
        `K = ${K}, ${shape === "one" ? "one process" : "two processes"}, run ${run + 1}: open → switch ${f(row.ms)} ms, v2 lag at the switch ${lag}; v1 (role ${v1Role}) get p50/p95 ${f(median(before.get))}/${f(pct(before.get, 95))} → ${f(median(during.get))}/${f(pct(during.get, 95))} ms (${before.get.length} → ${during.get.length} samples), list p50/p95 ${f(median(before.list))}/${f(pct(before.list, 95))} → ${f(median(during.list))}/${f(pct(during.list, 95))} ms; ${appended.n} appends in the run; v2.get = fold on read for ${keys.length - mismatches} of ${keys.length} keys; v2 list total ${listed.total}; v1 build ${f(built.ms)} ms`,
      );
    }
  }
  console.log("");
  console.log(`| K | shape | open → switch, ms: median (min–max) | v2 lag.events at the switch | v1 get p50 / p95 before → during (ms, medians over runs) | v1 list p50 / p95 before → during | runs |`);
  console.log(`|---|---|---|---|---|---|---|`);
  for (const shape of ["one", "two"]) {
    const rs = rows.filter((r) => r.shape === shape);
    if (rs.length === 0) continue;
    const m = (fn) => f(median(rs.map(fn)));
    console.log(
      `| ${K.toLocaleString("en")} | ${shape === "one" ? "one process" : "two processes"} | ${range(rs.map((r) => r.ms))} | ${rs.map((r) => r.lag).join(" / ")} | ${m((r) => median(r.before.get))} / ${m((r) => pct(r.before.get, 95))} → ${m((r) => median(r.during.get))} / ${m((r) => pct(r.during.get, 95))} | ${m((r) => median(r.before.list))} / ${m((r) => pct(r.before.list, 95))} → ${m((r) => median(r.during.list))} / ${m((r) => pct(r.during.list, 95))} | ${rs.length} |`,
    );
  }
  await drop(BESIDE_TABLE);
}

// ── live: start "persisted" ─────────────────────────────────────────────────────
async function live(events, payload, reps) {
  const { seeder, last } = await seed(LIVE_TABLE, events, payload);
  const layouts = events === 100_000 && payload === 1024 ? [1, 1000, 100000] : [1, 1000];
  const name = (K) => `bv-live-k${K}`;
  for (const K of layouts) {
    const b = child(["build", LIVE_TABLE, name(K), 1, LAYOUTS[K].keyBy, 500, last.transactionId, last.sequence]);
    if (b.failed) red(`live ${events} × ${payload}: the persisted build, K = ${K}, failed: ${b.failed}`);
    else console.log(`persisted model K = ${K} built in ${f(b.ms)} ms (folded ${b.folded})`);
  }
  await analyze(LIVE_TABLE);
  const runRounds = async (modes, phase, ks) => {
    const cells = [];
    for (const K of ks) for (const m of modes) cells.push({ ...m, K, cid: `${m.id}@${K}` });
    const results = Object.fromEntries(cells.map((c) => [c.cid, []]));
    const fails = Object.fromEntries(cells.map((c) => [c.cid, 0]));
    const reference = {};
    for (let round = -1; round < reps; round++) {
      for (let k = 0; k < cells.length; k++) {
        const cell = cells[(k + Math.max(0, round)) % cells.length];
        const r = child(["live", cell.id, cell.K, name(cell.K)]);
        if (r.failed) {
          fails[cell.cid]++;
          red(`live ${events} × ${payload} ${phase}: ${cell.label}, K = ${cell.K}: a run failed${round < 0 ? " (warm-up round)" : ""}: ${r.failed}`);
          continue;
        }
        const state = deserialize(Buffer.from(r.state, "base64"));
        reference[cell.K] ??= state;
        if (!isDeepStrictEqual(state, reference[cell.K]) || r.same === false) red(`live ${events} × ${payload} ${phase}: ${cell.label}, K = ${cell.K}: a state that differs from fold on read`);
        if (round >= 0) results[cell.cid].push(r);
      }
    }
    return { results, fails };
  };
  const out = (res, id, K) => res.results[`${id}@${K}`] ?? [];
  const main = await runRounds(
    [
      { id: "fold", label: "fold on read" },
      { id: "rebuild", label: "live, rebuild" },
      { id: "persisted", label: 'live, start: "persisted"' },
    ],
    "main",
    layouts,
  );
  // DELTA events for the measured key of K = 1 and K = 1 000, while no writer runs
  const tail = Array.from({ length: DELTA }, (_, j) => entry(events + j, payload, { accountId: "a7" }));
  const batch = Math.max(1, Math.min(1000, Math.floor((4 * 1024 * 1024) / payload)));
  for (let i = 0; i < tail.length; i += batch) await seeder.append(tail.slice(i, i + batch));
  await seeder.close();
  await admin.query(`ANALYZE "${LIVE_TABLE}"`);
  const lag = await runRounds(
    [
      { id: "fold", label: `fold on read (N + ${DELTA})` },
      { id: "persisted", label: `live, start: "persisted", ${DELTA} behind` },
    ],
    "delta",
    [1, 1000],
  );
  const cold = (rs) => rs.map((r) => r.cold);
  const ev = (rs) => [...new Set(rs.map((r) => r.events))].join("/");
  console.log("");
  console.log(`| events × payload | K | fold on read cold (events) | live rebuild cold (events) | live start: "persisted" cold (events) | persisted vs fold on read | persisted vs rebuild | fold on read N + ${DELTA} (events) | start: "persisted", ${DELTA} behind (events) | behind: vs fold on read |`);
  console.log(`|---|---|---|---|---|---|---|---|---|---|`);
  for (const K of layouts) {
    const fo = out(main, "fold", K);
    const rb = out(main, "rebuild", K);
    const ps = out(main, "persisted", K);
    const fo2 = K === 100000 ? [] : out(lag, "fold", K);
    const ps2 = K === 100000 ? [] : out(lag, "persisted", K);
    const x = median(cold(fo)) / median(cold(ps));
    const x2 = median(cold(fo2)) / median(cold(ps2));
    console.log(
      `| ${events.toLocaleString("en")} × ${payload === 1024 ? "1 KB" : `${payload / 1024} KB`} | ${K.toLocaleString("en")} | ${fo.length ? `${range(cold(fo))} (${ev(fo)})` : "failed"} | ${rb.length ? `${range(cold(rb))} (${ev(rb)})` : "failed"}${main.fails[`rebuild@${K}`] ? ` — ${main.fails[`rebuild@${K}`]} failed` : ""} | ${ps.length ? `${range(cold(ps))} (${ev(ps)})` : "failed"} | **${Number.isNaN(x) ? "–" : `${verdict(x)}: ${factor(median(cold(fo)), median(cold(ps)))}×`}** | ${factor(median(cold(rb)), median(cold(ps)))}× | ${K === 100000 ? "not measured (no delta key at K = 100 000)" : `${range(cold(fo2))} (${ev(fo2)})`} | ${K === 100000 ? "–" : `${range(cold(ps2))} (${ev(ps2)})`} | ${K === 100000 || Number.isNaN(x2) ? "–" : `${verdict(x2)}: ${factor(median(cold(fo2)), median(cold(ps2)))}×`} |`,
    );
  }
  console.log(`(median of ${reps} fresh processes per cell, min–max in brackets; ms)`);
  await drop(LIVE_TABLE);
}

// ── paged: the persisted fold after a bump ──────────────────────────────────────
async function paged(reps) {
  const { seeder } = await seed(PAGED_TABLE, 100_000, 1024);
  const step = (label, name, version, want) => {
    const r = child(["paged", name, version]);
    if (r.failed) {
      red(`paged ${label}: ${r.failed}`);
      return r;
    }
    if (r.error) red(`paged ${label}: ${r.error}`);
    if (r.storedError) red(`paged ${label}: stored.error ${r.storedError}`);
    if (want && r.state && !isDeepStrictEqual(deserialize(Buffer.from(r.state, "base64")), want)) red(`paged ${label}: the state differs from a fold from zero`);
    return { ...r, label };
  };
  const fromZero = async () => statsFold((await seeder.query(keyQueryOf("ledgerId", "bench"))).events, statsInitial());
  const want100000 = await fromZero();
  const runs = [];
  for (let run = 0; run < reps; run++) {
    const a = `bv-paged-a${run}`;
    const b = `bv-paged-b${run}`;
    runs.push({
      run,
      steps: [step("A v1 first (100 000)", a, 1, want100000), step("A v1 again", a, 1, want100000), step("A v2 first, the bump (100 000)", a, 2, want100000), step("A v2 again", a, 2, want100000), step("B v1 first (100 000)", b, 1, want100000)],
    });
  }
  const t500 = Array.from({ length: 500 }, (_, j) => entry(100_000 + j, 1024));
  await seeder.append(t500);
  await admin.query(`ANALYZE "${PAGED_TABLE}"`);
  const want100500 = await fromZero();
  for (const r of runs) {
    const b = `bv-paged-b${r.run}`;
    r.steps.push(step("B v1 after 500 more (100 500)", b, 1, want100500), step("B v2 first, the bump (100 500: paged)", b, 2, want100500), step("B v2 again", b, 2, want100500));
  }
  const pages = await admin.query(`SELECT fold_name, fold_version, cursor_seq FROM "${PAGED_TABLE}_fold_state" ORDER BY fold_name, fold_version`);
  await seeder.close();
  console.log("");
  console.log(`| step | ms (load, wall) | start | events | readMs | foldMs | stored.readMs | stored.writeMs | written |`);
  console.log(`|---|---|---|---|---|---|---|---|---|`);
  const labels = runs[0].steps.map((s) => s.label);
  for (let i = 0; i < labels.length; i++) {
    const ss = runs.map((r) => r.steps[i]).filter((s) => !s.failed && !s.error);
    const failed = runs.length - ss.length;
    const col = (k) => (ss.every((s) => s[k] !== undefined) ? range(ss.map((s) => s[k])) : "–");
    console.log(`| ${labels[i]} | ${col("ms")} | ${[...new Set(ss.map((s) => s.start))].join("/")} | ${[...new Set(ss.map((s) => s.events))].join("/")} | ${col("readMs")} | ${col("foldMs")} | ${col("storedReadMs")} | ${col("writeMs")} | ${[...new Set(ss.map((s) => s.written))].join("/")}${failed ? ` — ${failed} failed: ${runs.map((r) => r.steps[i].error ?? r.steps[i].failed).filter(Boolean)[0]}` : ""} |`);
  }
  console.log(`(median of ${reps} runs, each step a fresh process; min–max in brackets)`);
  console.log(`stored rows after the runs: ${pages.rows.map((r) => `${r.fold_name} v${r.fold_version} @${r.cursor_seq}`).join(", ")}`);
  await drop(PAGED_TABLE);
}

// ── retire ──────────────────────────────────────────────────────────────────────
async function retire(reps) {
  const readme = readFileSync(new URL("../README.md", import.meta.url), "utf8");
  const at = readme.indexOf("**Retiring an old version.**");
  const recipe = /```sql\n([\s\S]*?)```/.exec(readme.slice(at))[1];
  const statements = recipe
    .split("\n")
    .filter((l) => !l.trim().startsWith("--"))
    .join("\n")
    .split(";")
    .map((s) => s.trim())
    .filter((s) => s && s !== "BEGIN" && s !== "COMMIT")
    .map((s) => s.replaceAll("events_read_model_state", `"${RETIRE_TABLE}_read_model_state"`).replaceAll("events_read_model ", `"${RETIRE_TABLE}_read_model" `).replaceAll("events_read_model\n", `"${RETIRE_TABLE}_read_model"\n`));
  console.log(`the README recipe, ${statements.length} statements in one transaction (table names replaced):\n${statements.map((s) => `  ${s.replace(/\n\s*/g, " ")}`).join("\n")}`);
  const { seeder, last } = await seed(RETIRE_TABLE, 100_000, 1024);
  await seeder.close();
  console.log("");
  console.log(`| rows (K) | recipe, ms: median (min–max) | v1 rows before → after | v2 rows before → after | v1 status after | runs |`);
  console.log(`|---|---|---|---|---|---|`);
  for (const K of [1000, 100000]) {
    const ms = [];
    const counts = [];
    for (let run = 0; run < reps; run++) {
      const name = `bv-retire-k${K}-r${run}`;
      for (const version of [1, 2]) {
        const b = child(["build", RETIRE_TABLE, name, version, LAYOUTS[K].keyBy, 5000, last.transactionId, last.sequence]);
        if (b.failed) red(`retire K = ${K}: build of v${version} failed: ${b.failed}`);
        else if (b.building) red(`retire K = ${K}: v${version} still building after its build`);
      }
      await analyze(RETIRE_TABLE);
      const rowsOf = async (version) => Number((await admin.query(`SELECT count(*)::int AS n FROM "${RETIRE_TABLE}_read_model_state" WHERE tenant = '' AND name = $1 AND version = $2`, [name, version])).rows[0].n);
      const before = [await rowsOf(1), await rowsOf(2)];
      const c = await admin.connect();
      const t0 = performance.now();
      try {
        await c.query("BEGIN");
        for (const s of statements) {
          const used = Math.max(0, ...[...s.matchAll(/\$(\d)/g)].map((m) => Number(m[1])));
          await c.query(s, ["", name, 2].slice(0, used));
        }
        await c.query("COMMIT");
      } finally {
        c.release();
      }
      ms.push(performance.now() - t0);
      const after = [await rowsOf(1), await rowsOf(2)];
      const status = (await admin.query(`SELECT status FROM "${RETIRE_TABLE}_read_model" WHERE tenant = '' AND name = $1 AND version = 1`, [name])).rows[0]?.status;
      counts.push({ before, after, status });
      if (after[0] !== 0 || after[1] !== before[1] || status !== "retired") red(`retire K = ${K} run ${run}: v1 rows ${before[0]} → ${after[0]}, v2 ${before[1]} → ${after[1]}, v1 status ${status}`);
    }
    console.log(`| ${K.toLocaleString("en")} | ${range(ms)} | ${[...new Set(counts.map((x) => `${x.before[0]} → ${x.after[0]}`))].join(", ")} | ${[...new Set(counts.map((x) => `${x.before[1]} → ${x.after[1]}`))].join(", ")} | ${[...new Set(counts.map((x) => x.status))].join("/")} | ${ms.length} |`);
  }
  await drop(RETIRE_TABLE);
}

const [mode, ...args] = process.argv.slice(2);
console.log(`machine: ${await machineLine(admin)}; ${new Date().toISOString()}`);
console.log(`command: node bench/versions-bench.mjs ${process.argv.slice(2).join(" ")}`);
const started = performance.now();
try {
  if (mode === "beside") await beside(Number(args[0] ?? 1000), Number(args[1] ?? 3));
  else if (mode === "live") await live(Number(args[0] ?? 10000), Number(args[1] ?? 1024), Number(args[2] ?? 11));
  else if (mode === "paged") await paged(Number(args[0] ?? 3));
  else if (mode === "retire") await retire(Number(args[0] ?? 3));
  else {
    console.error("usage: node bench/versions-bench.mjs beside <K> [reps] | live <events> <payloadBytes> [reps] | paged [reps] | retire [reps]");
    exitCode = 2;
  }
} finally {
  for (const t of [BESIDE_TABLE, LIVE_TABLE, PAGED_TABLE, RETIRE_TABLE]) await drop(t).catch(() => undefined);
  await admin.end();
}
console.log(`${mode}: ${exitCode === 0 ? "exit 0" : "exit 1 (red above)"}, ${((performance.now() - started) / 1000).toFixed(1)} s`);
process.exit(exitCode);
