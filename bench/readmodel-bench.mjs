// The live read model vs. fold on read, on the local Postgres test database (and the memory store
// for the watch latency). Build first, or run `sh bench/readmodel.sh`, which builds and then runs this.
//   node bench/readmodel-bench.mjs matrix <events> <payloadBytes> [reps=11]
//   node bench/readmodel-bench.mjs list <keys> [reps=5]
//   node bench/readmodel-bench.mjs watch [samples=30]
// Tables of its own (bench_rm_*), dropped at the start and the end.
//
// matrix: one events table, two key layouts over the same events:
//   K = 1      keyBy ledgerId: one key holds every event, so get(k) and context() are the same N-event state
//   K = 1 000  keyBy accountId: one key holds N / 1 000 events; the model still holds 1 000 keys
// and three cells per layout, each in `reps` fresh processes, the order rotated per round:
//   fold on read           cold = the first es.context() of the key in the process; warm = WARM further loads, nothing new
//   live, batchSize 500    cold = readModel(def) → the first awaited get(k) returns (the rebuild); warm = awaited gets
//   live, batchSize 5000   the same with pages of 5 000 events
// "cold" = a fresh node process with the store installed and connected before the clock runs (the
// database itself warm, as in bench/persisted-fold-bench.mjs).
//
// list: K keys (one 1 KB event each), list({ where: { status: "open" }, sort: amount desc, limit: 50 })
//   vs. fold on read of the whole source into a Map plus the same filter, sort and page; warm at a
//   stable revision, the first list after a change, under a steady write load (one append per 10 ms);
//   heap per key of the caught-up model in a child process with --expose-gc.
//
// watch: latency from an append to the watcher's callback (memory; Postgres in one process with and
//   without the doorbell; Postgres from a second OS process), fan-out over 1 / 100 / 1 000 watchers
//   on one key (fold calls per change), read-your-writes get(k, { atLeast }), and atLeast behind an
//   older open transaction.
import { execFileSync, spawn } from "node:child_process";
import { writeFileSync } from "node:fs";
import { cpus, tmpdir, totalmem } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { deserialize, serialize } from "node:v8";
import pg from "pg";
import { z } from "zod";
import { buildSchema, createEventStore, defineEvents, defineReadModel } from "../dist/index.js";
import { PostgresStore } from "../dist/postgres/index.js";

const url = process.env.ES_BENCH_DATABASE_URL ?? "postgres://es:es@localhost:5432/es_sdk_test";
// Bench timeouts (decision 54): every store is opened with statement_timeout 600 000 ms and every cold model read
// passes timeoutMs 600 000, so a slow cold read gives a number instead of 57014 / read_model_timeout. The library's
// defaults (statement_timeout 30 000 ms, timeoutMs 10 000) are unchanged.
const BENCH_TIMEOUTS = { statementMs: 600_000 };
const COLD_TIMEOUT_MS = 600_000;
const self = fileURLToPath(import.meta.url);
const MATRIX_TABLE = "bench_rm_events";
const LIST_TABLE = "bench_rm_list_events";
const WATCH_TABLE = "bench_rm_watch_events";
const CATEGORIES = 40;
const ACCOUNTS = 1_000;
const WARM = 20;
const GET_SEGMENTS = 20;
const GETS_PER_SEGMENT = 1_000;

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
/** f(n) with its unit: "0.18 µs" stays, a plain number gets " ms". */
const fms = (n) => {
  const s = f(n);
  return s.endsWith("µs") ? s : `${s} ms`;
};
const factor = (a, b) => {
  const x = a / b;
  return x >= 100 ? Math.round(x).toLocaleString("en") : x.toFixed(2);
};

// ── the matrix: events, fold, model ─────────────────────────────────────────────
const ledger = defineEvents({
  EntryBooked: { data: z.object({ amount: z.number(), category: z.string(), note: z.string() }), scopes: ["ledgerId", "accountId"] },
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
const LAYOUTS = { 1: { keyBy: "ledgerId", key: "bench" }, 1000: { keyBy: "accountId", key: "a7" } };
/** What es.context() reads for one key of the model: the source with the key pinned (source ∧ keyBy = k). */
const keyQueryOf = (source, keyBy, key) => ({ ...source, scopes: { ...(source.scopes ?? {}), [keyBy]: key } });

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
/** Pseudo-random text of `length` characters, so Postgres cannot compress a large payload (as bench/persisted-fold-bench.mjs). */
const textOf = (i, length) => {
  const prefix = `entry ${i} `;
  const at = (i * 7919) % 4096;
  return prefix + noiseFor(length).slice(at, at + Math.max(0, length - prefix.length));
};
const entry = (i, payload) =>
  ledger.EntryBooked({ amount: (i * 37) % 1000, category: `c${i % CATEGORIES}`, note: textOf(i, Math.max(16, payload - 100)) }, { ledgerId: "bench", accountId: `a${i % ACCOUNTS}` });

// ── the list and watch cells: offers ────────────────────────────────────────────
const sales = defineEvents({
  OfferMade: { data: z.object({ amount: z.number(), status: z.string(), note: z.string() }), scopes: ["offerId"] },
  OfferRevised: { data: z.object({ amount: z.number() }), scopes: ["offerId"] },
});
const offerSource = sales.$filter();
const offerInitial = { status: "none", amount: 0, revisions: 0 };
const offerFold = sales.$fold({
  OfferMade: (d, s) => ({ ...s, status: d.status, amount: d.amount }),
  OfferRevised: (d, s) => ({ ...s, amount: d.amount, revisions: s.revisions + 1 }),
});
const STATUSES = ["open", "accepted", "draft", "rejected"]; // 25 % open
const offerMade = (i) => sales.OfferMade({ amount: (i * 7_919) % 100_000, status: STATUSES[i % 4], note: textOf(i, 900) }, { offerId: `o${i}` });
/** Fold on read of the whole source into one Map key → state: the list a fold-on-read app keeps doing. */
const offersMapFold = (events, state) => {
  const next = new Map(state);
  for (const e of events) {
    const k = e.scopes.offerId;
    next.set(k, offerFold([e], next.get(k) ?? offerInitial));
  }
  return next;
};
const LIST = { where: { status: "open" }, sort: { by: "amount", dir: "desc" }, limit: 50 };
/** The model's list order (decision 19): amount desc, ties by key ascending, then the page. */
const listOf = (map) => {
  const rows = [];
  for (const [key, state] of map) if (state.status === "open") rows.push({ key, state });
  rows.sort((a, b) => b.state.amount - a.state.amount || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  return { rows: rows.slice(0, LIST.limit), total: rows.length };
};

const machineLine = async (admin) => {
  const pgVersion = admin ? (await admin.query("SHOW server_version")).rows[0].server_version : "–";
  return `${cpus()[0]?.model ?? "?"}, ${cpus().length} cores, ${Math.round(totalmem() / 2 ** 30)} GiB, ${process.platform} ${process.arch}, Node ${process.version}, Postgres ${pgVersion}`;
};

// ════ children ══════════════════════════════════════════════════════════════════

/** One fresh process, one cell of the matrix. */
async function matrixChild(mode, layoutArg) {
  const { keyBy, key } = LAYOUTS[layoutArg];
  const loads = [];
  const api = createEventStore({ events: [ledger], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: MATRIX_TABLE, poolSize: 2 }, contextCache: { onLoad: (l) => void loads.push(l) } });
  await api.store(); // install check and the first connection are not part of the load
  const query = keyQueryOf(matrixSource, keyBy, key);
  let out;
  if (mode === "fold") {
    const t0 = performance.now();
    const { state } = await api.context({ query, fold: statsFold, initial: statsInitial });
    const cold = performance.now() - t0;
    const warm = [];
    for (let i = 0; i < WARM; i++) {
      const t = performance.now();
      await api.context({ query, fold: statsFold, initial: statsInitial });
      warm.push(performance.now() - t);
    }
    out = { cold, coldEvents: loads[0].events, warm: median(warm), warmEvents: Math.max(...loads.slice(1).map((l) => l.events)), state };
  } else {
    const batchSize = Number(mode.slice(4)); // live500 | live5000
    const model = api.readModel(defineReadModel({ name: "bench-stats", version: 1, source: matrixSource, keyBy, fold: statsFold, initial: statsInitial, batchSize }));
    const t0 = performance.now();
    const state = await model.get(key, { timeoutMs: COLD_TIMEOUT_MS });
    const cold = performance.now() - t0;
    const coldEvents = model.status.folded;
    const folded = model.status.folded;
    const segments = [];
    for (let s = 0; s < GET_SEGMENTS; s++) {
      const t = performance.now();
      for (let i = 0; i < GETS_PER_SEGMENT; i++) await model.get(key);
      segments.push((performance.now() - t) / GETS_PER_SEGMENT);
    }
    const warmFolded = (model.status.folded - folded) / (GET_SEGMENTS * GETS_PER_SEGMENT);
    // the same answer as fold on read, in the same process (not timed)
    const onRead = (await api.context({ query, fold: statsFold, initial: statsInitial })).state;
    out = { cold, coldEvents, warm: median(segments), warmEvents: warmFolded, keys: model.status.keys, same: isDeepStrictEqual(state, onRead), state };
  }
  await api.close();
  process.stdout.write(JSON.stringify({ ...out, state: serialize(out.state).toString("base64") }));
  process.exit(0);
}

/** Heap of a caught-up model over the list table, in a process run with --expose-gc. */
async function heapChild(keys) {
  const api = createEventStore({ events: [sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: LIST_TABLE, poolSize: 2 } });
  await api.store();
  const gc = async () => {
    for (let i = 0; i < 4; i++) {
      globalThis.gc();
      await sleep(5);
    }
    return process.memoryUsage().heapUsed;
  };
  const before = await gc();
  const model = api.readModel(defineReadModel({ name: "bench-offers-heap", version: 1, source: offerSource, keyBy: "offerId", fold: offerFold, initial: offerInitial }));
  await model.get("o0");
  const after = await gc();
  const held = model.status.keys;
  process.stdout.write(JSON.stringify({ before, after, keys: held, expected: keys, perKey: (after - before) / held }));
  await api.close();
  process.exit(0);
}

/** A second OS process that appends OfferRevised for one key, `count` times at random pauses, and prints when each append returned. */
async function writerChild(count, key) {
  const api = createEventStore({ events: [sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: WATCH_TABLE, live: true, poolSize: 1 } });
  await api.store();
  for (let i = 1; i <= count; i++) {
    await sleep(100 + Math.random() * 500);
    await api.append([sales.OfferRevised({ amount: 1_000_000 + i }, { offerId: key })]);
    process.stdout.write(`${JSON.stringify({ i, at: now() })}\n`);
  }
  await api.close();
  process.exit(0);
}

if (process.argv[2] === "--child") {
  const [, , , kind, a, b] = process.argv;
  if (kind === "matrix") await matrixChild(a, b);
  else if (kind === "heap") await heapChild(Number(a));
  else if (kind === "writer") await writerChild(Number(a), b);
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
const drop = (table) => admin.query(`DROP TABLE IF EXISTS "${table}" CASCADE`);
const child = (args, nodeArgs = []) => {
  try {
    return JSON.parse(execFileSync(process.execPath, [...nodeArgs, self, "--child", ...args.map(String)], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, ES_BENCH_DATABASE_URL: url } }));
  } catch (error) {
    // the whole stderr to a file of its own; the table gets the error's first line (its name and message)
    const text = String(error.stderr ?? error.message ?? error);
    const file = join(tmpdir(), `bench-rm-failed-${process.pid}-${++failedRuns}.txt`);
    writeFileSync(file, text);
    const first = text.split("\n").find((l) => /(Error|error:)/.test(l)) ?? text.trim().split("\n")[0] ?? "?";
    return { failed: `${first.trim().slice(0, 200)} (stderr: ${file})` };
  }
};
let failedRuns = 0;
let exitCode = 0;
const red = (message) => {
  console.error(`bench: ${message}`);
  exitCode = 1;
};

// ── matrix ──────────────────────────────────────────────────────────────────────
async function matrix(events, payload, reps) {
  await drop(MATRIX_TABLE);
  const schema = buildSchema([ledger], { strict: true });
  const seeder = new PostgresStore({ connection: url, timeouts: BENCH_TIMEOUTS, schema, table: MATRIX_TABLE, poolSize: 2 });
  const batch = Math.max(1, Math.min(1000, Math.floor((4 * 1024 * 1024) / payload)));
  const t0 = performance.now();
  for (let i = 0; i < events; i += batch) await seeder.append(Array.from({ length: Math.min(batch, events - i) }, (_, j) => entry(i + j, payload)));
  await admin.query(`VACUUM ANALYZE "${MATRIX_TABLE}"`);
  await seeder.query(matrixSource); // hint bits and buffers, untimed
  await seeder.close();
  const seedS = (performance.now() - t0) / 1000;
  const facts = (await admin.query(`SELECT count(*)::int AS n, round(avg(octet_length(payload::text)))::int AS bytes, pg_size_pretty(pg_total_relation_size('"${MATRIX_TABLE}"')) AS size FROM "${MATRIX_TABLE}"`)).rows[0];
  const MODES = [
    { id: "fold", label: "fold on read" },
    { id: "live500", label: "live, batchSize 500" },
    { id: "live5000", label: "live, batchSize 5000" },
  ];
  const cells = [];
  for (const layout of [1, 1000]) for (const m of MODES) cells.push({ ...m, layout, cid: `${m.id}@${layout}` });
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
        red(`${cell.label}, K = ${cell.layout}: a run failed${round < 0 ? " (warm-up round)" : ""}: ${r.failed}`);
        continue;
      }
      const state = deserialize(Buffer.from(r.state, "base64"));
      reference[cell.layout] ??= state;
      if (!isDeepStrictEqual(state, reference[cell.layout])) {
        unequal++;
        red(`${cell.label}, K = ${cell.layout}: a state that differs from the other cells`);
      }
      if (r.same === false) {
        unequal++;
        red(`${cell.label}, K = ${cell.layout}: model.get(k) differs from es.context() in the same process`);
      }
      if (r.warmEvents !== 0) red(`${cell.label}, K = ${cell.layout}: a warm read folded ${r.warmEvents} events`);
      if (round >= 0) results[cell.cid].push(r);
    }
  }
  const machine = await machineLine(admin);
  console.log(`live read model vs. fold on read — ${events.toLocaleString("en")} events of ${payload} bytes (stored payload ${facts.bytes} bytes as JSON, table ${facts.size})`);
  console.log(`${machine}; ${new Date().toISOString()}; ${reps} fresh processes per cell (+1 warm-up round, not kept), order rotated; seed ${seedS.toFixed(1)} s`);
  console.log(`command: node bench/readmodel-bench.mjs matrix ${events} ${payload} ${reps}\n`);
  console.log("| K | cell | cold ms median (min–max) | events folded cold | warm ms median (min–max of the per-process medians) | events folded per warm read | keys held | runs failed |");
  console.log("|---|---|---|---|---|---|---|---|");
  const summary = {};
  for (const cell of cells) {
    const rs = results[cell.cid];
    const fails = failures[cell.cid];
    if (rs.length === 0) {
      console.log(`| ${cell.layout} | ${cell.label} | **failed** | – | – | – | – | ${fails.length}: ${fails[0] ?? "?"} |`);
      summary[cell.cid] = null;
      red(`${cell.label}, K = ${cell.layout}: every run failed (${fails[0]})`);
      continue;
    }
    const cold = rs.map((r) => r.cold);
    const warm = rs.map((r) => r.warm);
    summary[cell.cid] = { cold: median(cold), coldMin: Math.min(...cold), coldMax: Math.max(...cold), warm: median(warm), warmMin: Math.min(...warm), warmMax: Math.max(...warm), n: rs.length, fails: fails.length };
    const warmEv = Math.max(...rs.map((r) => r.warmEvents));
    console.log(
      `| ${cell.layout} | ${cell.label} | **${f(median(cold))}** (${f(Math.min(...cold))}–${f(Math.max(...cold))}) | ${[...new Set(rs.map((r) => r.coldEvents))].join("/")} | **${f(median(warm))}** (${f(Math.min(...warm))}–${f(Math.max(...warm))}) | ${warmEv} | ${rs[0].keys ?? "–"} | ${fails.length ? `${fails.length}: ${fails[0]}` : "0"} |`,
    );
  }
  console.log("");
  console.log("10× target (constitution): a read of the live model vs. fold on read of the same state, cold vs. cold, warm vs. warm");
  for (const layout of [1, 1000]) {
    const fold = summary[`fold@${layout}`];
    for (const live of ["live500", "live5000"]) {
      const s = summary[`${live}@${layout}`];
      if (!fold || !s) {
        console.log(`  K = ${layout}, ${live}: not measured (runs failed)`);
        continue;
      }
      const warmX = fold.warm / s.warm;
      const coldX = fold.cold / s.cold;
      const verdict = (x) => (x >= 10 ? "reached" : '"not reached"');
      console.log(
        `  K = ${layout}, ${live}: warm ${verdict(warmX)}, ${factor(fold.warm, s.warm)}× (${fms(fold.warm)} → ${fms(s.warm)}); cold ${verdict(coldX)}, ${factor(fold.cold, s.cold)}× (${fms(fold.cold)} → ${fms(s.cold)})`,
      );
    }
  }
  console.log(`states equal across all cells and rounds, and model.get(k) equal to es.context() in every live process that finished: ${unequal === 0 ? "yes" : `NO (${unequal})`}`);
  if (process.env.BENCH_JSON) process.stdout.write(`\nJSON ${JSON.stringify({ cell: "matrix", events, payload, reps, machine, facts, summary })}\n`);
  await drop(MATRIX_TABLE);
}

// ── list ────────────────────────────────────────────────────────────────────────
async function list(keys, reps) {
  await drop(LIST_TABLE);
  const schema = buildSchema([sales], { strict: true });
  const seeder = new PostgresStore({ connection: url, timeouts: BENCH_TIMEOUTS, schema, table: LIST_TABLE, poolSize: 2 });
  const t0 = performance.now();
  for (let i = 0; i < keys; i += 1000) await seeder.append(Array.from({ length: Math.min(1000, keys - i) }, (_, j) => offerMade(i + j)));
  await admin.query(`VACUUM ANALYZE "${LIST_TABLE}"`);
  await seeder.query(offerSource);
  await seeder.close();
  const seedS = (performance.now() - t0) / 1000;
  const machine = await machineLine(admin);
  console.log(`list of the live read model vs. fold on read + list — ${keys.toLocaleString("en")} keys, one ~1 KB OfferMade each (25 % open)`);
  console.log(`${machine}; ${new Date().toISOString()}; seed ${seedS.toFixed(1)} s`);
  console.log(`command: node bench/readmodel-bench.mjs list ${keys} ${reps}`);
  console.log(`list = { where: { status: "open" }, sort: { by: "amount", dir: "desc" }, limit: 50 }; reader with the doorbell (live: true); fold on read + list = es.context() of the whole source into a Map, then the same filter, sort and page in JavaScript\n`);

  const rows = { foldCold: [], foldWarm: [], liveRebuild: [], liveWarm: [], liveAfterChange: [], loadLive: [], loadLiveAfterChange: [], loadFold: [] };
  let mismatches = 0;
  let foldedByLists = 0;
  let lists = 0;
  let changesSeen = 0;
  let writes = 0;
  for (let rep = 0; rep < reps; rep++) {
    // the reader with the doorbell, as a live model is meant to run: it folds each write within ~27 ms
    // maxEvents raised above the default 100 000: fold on read + list reads the whole source (keys + the revisions of the load)
    const api = createEventStore({ events: [sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: LIST_TABLE, live: true, poolSize: 4 }, contextCache: { maxEvents: 1_000_000 } });
    const writer = createEventStore({ events: [sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: LIST_TABLE, poolSize: 2 } });
    await api.store();
    await writer.store();
    const onRead = async () => listOf((await api.context({ query: offerSource, fold: offersMapFold, initial: () => new Map() })).state);
    let t = performance.now();
    let expected = await onRead();
    rows.foldCold.push(performance.now() - t);
    const warmFold = [];
    for (let i = 0; i < 10; i++) {
      t = performance.now();
      expected = await onRead();
      warmFold.push(performance.now() - t);
    }
    rows.foldWarm.push(median(warmFold));

    t = performance.now();
    const model = api.readModel(defineReadModel({ name: "bench-offers", version: 1, source: offerSource, keyBy: "offerId", fold: offerFold, initial: offerInitial }));
    let page = await model.list(LIST);
    rows.liveRebuild.push(performance.now() - t);
    const same = (p, e) => p.total === e.total && isDeepStrictEqual(p.rows.map((r) => [r.key, r.state]), e.rows.map((r) => [r.key, r.state]));
    if (!same(page, expected)) mismatches++;
    const folded0 = model.status.folded;
    const warmLive = [];
    for (let i = 0; i < 200; i++) {
      t = performance.now();
      page = await model.list(LIST);
      warmLive.push(performance.now() - t);
    }
    foldedByLists += model.status.folded - folded0;
    lists += 200;
    rows.liveWarm.push(median(warmLive));

    // the first list after a change: one revision of an open offer, waited for, then one list (the kept result is rebuilt)
    const after = [];
    for (let i = 0; i < 30; i++) {
      const k = `o${4 * ((rep * 31 + i * 97) % Math.floor(keys / 4))}`; // an open offer
      const appended = await writer.append([sales.OfferRevised({ amount: (i * 104_729) % 100_000 }, { offerId: k })]);
      await model.get(k, { atLeast: { transactionId: appended.transactionId, sequence: appended.last } });
      const rev = model.status.revision;
      t = performance.now();
      page = await model.list(LIST);
      after.push(performance.now() - t);
      if (page.revision !== rev) mismatches++;
    }
    rows.liveAfterChange.push(median(after));
    if (!same(page, await onRead())) mismatches++;

    // a steady write load: one append every 10 ms from the writer, lists back to back for 3 s
    let stop = false;
    const load = (async () => {
      let i = 0;
      while (!stop) {
        const k = `o${4 * ((i * 7_919) % Math.floor(keys / 4))}`;
        await writer.append([sales.OfferRevised({ amount: (i * 15_485_863) % 100_000 }, { offerId: k })]);
        writes++;
        i++;
        await sleep(10);
      }
    })();
    const liveTimes = [];
    const liveAfter = [];
    let lastRevision = model.status.revision;
    const endAt = performance.now() + 3_000;
    while (performance.now() < endAt) {
      t = performance.now();
      page = await model.list(LIST);
      const ms = performance.now() - t;
      liveTimes.push(ms);
      if (page.revision !== lastRevision) {
        liveAfter.push(ms);
        changesSeen++;
      }
      lastRevision = page.revision;
      await sleep(1);
    }
    const foldTimes = [];
    const endFold = performance.now() + 3_000;
    while (performance.now() < endFold) {
      t = performance.now();
      await onRead();
      foldTimes.push(performance.now() - t);
      await sleep(1);
    }
    stop = true;
    await load;
    rows.loadLive.push(...liveTimes);
    rows.loadLiveAfterChange.push(...liveAfter);
    rows.loadFold.push(...foldTimes);
    await writer.close();
    await api.close();
  }
  const line = (label, xs, note = "") => console.log(`| ${label} | **${f(median(xs))}** | ${f(Math.min(...xs))}–${f(Math.max(...xs))} | ${xs.length} | ${note} |`);
  console.log("| cell | ms median | min–max | n | note |");
  console.log("|---|---|---|---|---|");
  line("fold on read + list, cold (first in a fresh runtime of this process)", rows.foldCold, "one per rep");
  line("fold on read + list, warm (ContextCache hit, then filter + sort + page)", rows.foldWarm, "median of 10 per rep");
  line("live: rebuild to the first list", rows.liveRebuild, "one per rep");
  line("live list, warm at a stable revision (the kept result)", rows.liveWarm, "median of 200 per rep");
  line("live list, the first after a change (scan + sort)", rows.liveAfterChange, "median of 30 per rep");
  line("under load (1 append / 10 ms): live list, every call", rows.loadLive, `p90 ${f(pct(rows.loadLive, 90))}`);
  line("under load: live list, the calls that met a new revision", rows.loadLiveAfterChange, `p90 ${f(pct(rows.loadLiveAfterChange, 90))}`);
  line("under load: fold on read + list, every call", rows.loadFold, `p90 ${f(pct(rows.loadFold, 90))}`);
  console.log("");
  const warmX = median(rows.foldWarm) / median(rows.liveWarm);
  const changeX = median(rows.foldWarm) / median(rows.liveAfterChange);
  const loadX = median(rows.loadFold) / median(rows.loadLive);
  const loadChangeX = median(rows.loadFold) / median(rows.loadLiveAfterChange);
  const v = (x) => (x >= 10 ? "reached" : '"not reached"');
  console.log(`10× (warm vs. warm), list at ${keys.toLocaleString("en")} keys: kept result ${v(warmX)} ${warmX.toFixed(1)}×; first list after a change ${v(changeX)} ${changeX.toFixed(1)}×`);
  console.log(`under a steady write load: every call ${v(loadX)} ${loadX.toFixed(1)}×; calls that met a new revision ${v(loadChangeX)} ${loadChangeX.toFixed(1)}× (${changesSeen} of ${rows.loadLive.length} calls met a new revision; ${writes} writes)`);
  console.log(`events folded by ${lists} warm lists: ${foldedByLists}; pages different from fold on read + list: ${mismatches}`);
  if (mismatches) red(`list: ${mismatches} pages differ from fold on read + list`);
  if (foldedByLists) red(`list: warm lists folded ${foldedByLists} events`);

  const heap = child(["heap", keys], ["--expose-gc"]);
  if (heap.failed) red(`heap: ${heap.failed}`);
  else console.log(`heap of the caught-up model (--expose-gc, heapUsed after gc, model held vs. before it): ${(heap.after - heap.before) / 2 ** 20 < 0.01 ? "<0.01" : ((heap.after - heap.before) / 2 ** 20).toFixed(2)} MiB for ${heap.keys.toLocaleString("en")} keys = ${Math.round(heap.perKey)} bytes per key (state { status, amount, revisions })`);
  if (process.env.BENCH_JSON) process.stdout.write(`\nJSON ${JSON.stringify({ cell: "list", keys, reps, rows: Object.fromEntries(Object.entries(rows).map(([k, xs]) => [k, { median: median(xs), min: Math.min(...xs), max: Math.max(...xs), n: xs.length }])), heap })}\n`);
  await drop(LIST_TABLE);
}

// ── watch ───────────────────────────────────────────────────────────────────────
async function watch(samples) {
  const machine = await machineLine(admin);
  console.log(`push on change: model.watch latency, fan-out, read-your-writes`);
  console.log(`${machine}; ${new Date().toISOString()}`);
  console.log(`command: node bench/readmodel-bench.mjs watch ${samples}\n`);
  const def = (name, fold = offerFold) => defineReadModel({ name, version: 1, source: offerSource, keyBy: "offerId", fold, initial: offerInitial });
  const table = [];
  const row = (label, xs, extra = "") => {
    table.push(`| ${label} | **${f(median(xs))}** | ${f(pct(xs, 90))} | ${f(Math.min(...xs))}–${f(Math.max(...xs))} | ${xs.length} | ${extra} |`);
  };
  /** Appends with a pause of 0–600 ms before each (a random point of the poll cycle), and the time from the append's return to the delivery. */
  const latency = async (writer, model, key, n) => {
    const delivered = new Map();
    const w = await model.watch(key, (s) => void delivered.set(s.amount, now()));
    const out = [];
    let missed = 0;
    for (let i = 1; i <= n; i++) {
      await sleep(Math.random() * 600);
      const amount = 2_000_000 + i + Math.floor(Math.random() * 1e6) * 10;
      await writer.append([sales.OfferRevised({ amount }, { offerId: key })]);
      const ret = now();
      const end = ret + 3_000;
      while (!delivered.has(amount) && now() < end) await sleep(0.2);
      if (delivered.has(amount)) out.push(delivered.get(amount) - ret);
      else missed++;
    }
    w.stop();
    return { out, missed };
  };

  // memory: one store, one process
  {
    const api = createEventStore({ events: [sales] });
    await api.append([offerMade(1)]);
    const model = api.readModel(def("bench-watch-mem"));
    await model.get("o1");
    const { out, missed } = await latency(api, model, "o1", samples);
    row("memory store, same process", out, `missed ${missed}`);
    if (missed) red(`watch, memory: ${missed} deliveries missed`);
    await api.close();
  }

  // Postgres, one process, with and without the doorbell
  for (const live of [true, false]) {
    await drop(WATCH_TABLE);
    const api = createEventStore({ events: [sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: WATCH_TABLE, live, poolSize: 2 } });
    const writer = createEventStore({ events: [sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: WATCH_TABLE, live: true, poolSize: 1 } });
    await writer.append([offerMade(1)]);
    const model = api.readModel(def("bench-watch-pg"));
    await model.get("o1");
    const { out, missed } = await latency(writer, model, "o1", live ? samples : Math.min(samples, 20));
    row(`Postgres, same process, ${live ? "doorbell (live: true)" : "no doorbell (poll 500 ms)"}`, out, `missed ${missed}`);
    if (missed) red(`watch, Postgres live=${live}: ${missed} deliveries missed`);

    // read-your-writes: the time of get(k, { atLeast }) right after the append returned
    const ryw = [];
    for (let i = 0; i < samples; i++) {
      await sleep(Math.random() * 200);
      const appended = await writer.append([sales.OfferRevised({ amount: 3_000_000 + i }, { offerId: "o1" })]);
      const t = performance.now();
      const s = await model.get("o1", { atLeast: { transactionId: appended.transactionId, sequence: appended.last } });
      ryw.push(performance.now() - t);
      if (s.amount !== 3_000_000 + i) red("read-your-writes: get with atLeast missed the append");
    }
    row(`Postgres, get(k, { atLeast }) after the append returned, ${live ? "doorbell" : "no doorbell"}`, ryw);
    await writer.close();
    await api.close();
  }

  // Postgres, the append in a second OS process, the watcher here (doorbell)
  {
    await drop(WATCH_TABLE);
    const api = createEventStore({ events: [sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: WATCH_TABLE, live: true, poolSize: 2 } });
    await api.append([offerMade(1)]);
    const model = api.readModel(def("bench-watch-2p"));
    await model.get("o1");
    const delivered = new Map();
    const w = await model.watch("o1", (s) => void delivered.set(s.amount, now()));
    const appended = new Map();
    const proc = spawn(process.execPath, [self, "--child", "writer", String(samples), "o1"], { stdio: ["ignore", "pipe", "inherit"], env: { ...process.env, ES_BENCH_DATABASE_URL: url } });
    let buffer = "";
    proc.stdout.on("data", (d) => {
      buffer += d;
      let nl;
      while ((nl = buffer.indexOf("\n")) >= 0) {
        const { i, at } = JSON.parse(buffer.slice(0, nl));
        appended.set(1_000_000 + i, at);
        buffer = buffer.slice(nl + 1);
      }
    });
    const code = await new Promise((r) => proc.on("exit", r));
    await sleep(1_000);
    w.stop();
    const out = [];
    for (const [amount, at] of appended) if (delivered.has(amount)) out.push(delivered.get(amount) - at);
    const missed = appended.size - out.length;
    row("Postgres, append in a second OS process, doorbell", out, `missed ${missed}; writer exit ${code}`);
    if (code !== 0 || missed || appended.size !== samples) red(`watch across processes: exit ${code}, ${appended.size} appends, ${missed} missed`);
    await api.close();
  }

  // fan-out: N watchers on one key; fold calls per change and the latency of the last delivery
  const fan = [];
  for (const watchers of [1, 100, 1000]) {
    await drop(WATCH_TABLE);
    let calls = 0;
    const counting = (events, state) => {
      calls++;
      return offerFold(events, state);
    };
    const api = createEventStore({ events: [sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: WATCH_TABLE, live: true, poolSize: 2 } });
    const writer = createEventStore({ events: [sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: WATCH_TABLE, live: true, poolSize: 1 } });
    await writer.append([offerMade(1)]);
    const model = api.readModel(def(`bench-fan-${watchers}`, counting));
    await model.get("o1");
    let delivered = 0;
    let last = 0;
    const ws = [];
    for (let i = 0; i < watchers; i++)
      ws.push(
        await model.watch("o1", () => {
          delivered++;
          last = now();
        }),
      );
    const per = [];
    const lat = [];
    const n = Math.min(samples, 20);
    for (let i = 0; i < n; i++) {
      await sleep(50 + Math.random() * 200);
      delivered = 0;
      const c0 = calls;
      await writer.append([sales.OfferRevised({ amount: 4_000_000 + i }, { offerId: "o1" })]);
      const ret = now();
      const end = ret + 3_000;
      while (delivered < watchers && now() < end) await sleep(0.2);
      if (delivered < watchers) red(`fan-out ${watchers}: ${delivered} of ${watchers} delivered`);
      per.push(calls - c0);
      lat.push(last - ret);
    }
    for (const w of ws) w.stop();
    fan.push(`| ${watchers} | ${Math.max(...per)} (every change: ${[...new Set(per)].join("/")}) | **${f(median(lat))}** | ${f(pct(lat, 90))} | ${n} |`);
    if (Math.max(...per) !== 1) red(`fan-out ${watchers}: ${Math.max(...per)} fold calls for one change`);
    await writer.close();
    await api.close();
  }

  // atLeast behind an older open transaction: the token's event settles only when the older one ends
  const older = [];
  for (const live of [true, false]) {
    await drop(WATCH_TABLE);
    const api = createEventStore({ events: [sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: WATCH_TABLE, live, poolSize: 2 } });
    const writer = createEventStore({ events: [sales], connection: url, postgres: { timeouts: BENCH_TIMEOUTS, table: WATCH_TABLE, live: true, poolSize: 1 } });
    await writer.append([offerMade(1)]);
    const model = api.readModel(def("bench-older"));
    await model.get("o1");
    const ms = [];
    for (let i = 0; i < Math.min(samples, 10); i++) {
      const tx = await admin.connect();
      try {
        await tx.query("BEGIN");
        await tx.query("SELECT pg_current_xact_id()"); // an xid: the snapshot xmin stays at it while it is open
        const appended = await writer.append([sales.OfferRevised({ amount: 5_000_000 + i }, { offerId: "o1" })]);
        let resolved = 0;
        const read = model.get("o1", { atLeast: { transactionId: appended.transactionId, sequence: appended.last }, timeoutMs: 5_000 }).then((s) => {
          resolved = now();
          return s;
        });
        await sleep(300);
        if (resolved) red("atLeast resolved while an older transaction was open");
        await tx.query("COMMIT");
        const ended = now();
        const s = await read;
        if (s.amount !== 5_000_000 + i) red("atLeast behind an older transaction returned a state without the append");
        ms.push(resolved - ended);
      } finally {
        tx.release();
      }
    }
    older.push(`| ${live ? "doorbell (live: true)" : "no doorbell"} | **${f(median(ms))}** | ${f(Math.min(...ms))}–${f(Math.max(...ms))} | ${ms.length} |`);
    await writer.close();
    await api.close();
  }
  await drop(WATCH_TABLE);

  console.log("Latency from the append's return to the watcher's callback (and of an atLeast read), ms:\n");
  console.log("| path | median | p90 | min–max | n | note |");
  console.log("|---|---|---|---|---|---|");
  for (const l of table) console.log(l);
  console.log("\nFan-out: N watchers on one key (Postgres, doorbell, one process), one change at a time:\n");
  console.log("| watchers | fold calls per change (max) | last delivery after the append's return, ms median | p90 | changes |");
  console.log("|---|---|---|---|---|");
  for (const l of fan) console.log(l);
  console.log("\nget(k, { atLeast }) behind an older open transaction (held 300 ms): from the older transaction's COMMIT to the read's return, ms:\n");
  console.log("| store | median | min–max | n |");
  console.log("|---|---|---|---|");
  for (const l of older) console.log(l);
}

try {
  const [cell, a, b, c] = process.argv.slice(2);
  if (cell === "matrix") await matrix(Number(a ?? 10_000), Number(b ?? 1024), Number(c ?? 11));
  else if (cell === "list") await list(Number(a ?? 10_000), Number(b ?? 5));
  else if (cell === "watch") await watch(Number(a ?? 30));
  else {
    console.error("usage: node bench/readmodel-bench.mjs matrix <events> <payloadBytes> [reps] | list <keys> [reps] | watch [samples]");
    exitCode = 2;
  }
} finally {
  for (const t of [MATRIX_TABLE, LIST_TABLE, WATCH_TABLE]) await drop(t).catch(() => undefined);
  await admin.end();
}
process.exit(exitCode);
