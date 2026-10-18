// Persisted fold vs. fold on read: one context, cold (a fresh node process) and warm (the same
// process, nothing new), on the local Postgres test database. Build first, or run
// `sh bench/persisted-fold.sh`, which builds and then runs this.
//   node bench/persisted-fold-bench.mjs [events=10000] [payloadBytes=1024] [reps=11]
// Tables of its own (bench_pf_events, bench_pf_events_fold_state), dropped at the start and the end.
//
// Cells, each measured `reps` times in fresh processes, the order rotated per round:
//   fold on read                    plain fold, persistedFolds off: reads and folds every event
//   persisted fold, delta 0         defined fold, a stored state at the context's end
//   persisted fold, delta D         the stored state D events behind (D = min(100, events/10))
//   persisted fold, nothing stored  the first cold load: folds from zero and writes the state
// "cold" = the first load in the process (the store is installed and connected before the clock
// runs); "warm" = the median of WARM further loads in the same process, with nothing new.
import { execFileSync } from "node:child_process";
import { cpus, totalmem } from "node:os";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { deserialize, serialize } from "node:v8";
import pg from "pg";
import { z } from "zod";
import { buildSchema, createEventStore, defineEvents, defineFold } from "../dist/index.js";
import { PostgresStore } from "../dist/postgres/index.js";

const url = process.env.ES_BENCH_DATABASE_URL ?? "postgres://es:es@localhost:5432/es_sdk_test";
// Bench timeouts (decision 54): every store is opened with statement_timeout 600 000 ms and every cold model read
// passes timeoutMs 600 000, so a slow cold read gives a number instead of 57014 / read_model_timeout. The library's
// defaults (statement_timeout 30 000 ms, timeoutMs 10 000) are unchanged.
const BENCH_TIMEOUTS = { statementMs: 600_000 };
const COLD_TIMEOUT_MS = 600_000;
const TABLE = "bench_pf_events";
const CATEGORIES = 40;
const WARM = 20;

const ledger = defineEvents({
  EntryBooked: { data: z.object({ amount: z.number(), category: z.string(), note: z.string() }), scopes: ["ledgerId"] },
});
const schema = buildSchema([ledger], { strict: true });
const query = ledger.$filter({ scopes: { ledgerId: "bench" } });

/** The showcase's fold: running statistics per category, some work per event (scout: with a trivial fold the cold cost is almost all read). */
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
const stats = defineFold({ name: "bench-ledger-stats", version: 1, fold: statsFold });
const initial = () => new Map();

// ── child: one fresh process, one cold load, then WARM warm loads ───────────────
if (process.argv[2] === "--child") {
  const [, , , mode, afterArg] = process.argv; // mode: "fold-on-read" | "persisted"
  const loads = [];
  const api = createEventStore({
    events: [ledger],
    connection: url,
    postgres: { timeouts: BENCH_TIMEOUTS, table: TABLE, persistedFolds: mode === "persisted", poolSize: 2 },
    contextCache: { onLoad: (l) => loads.push(l), ...(afterArg ? { persist: { after: Number(afterArg) } } : {}) },
  });
  await api.store(); // install check and the first connection are not part of the load
  const fold = mode === "persisted" ? stats : statsFold;
  const t0 = performance.now();
  const { state } = await api.context({ query, fold, initial });
  const cold = { totalMs: performance.now() - t0, load: loads[0] };
  const warm = [];
  for (let i = 0; i < WARM; i++) {
    const t = performance.now();
    await api.context({ query, fold, initial });
    warm.push({ totalMs: performance.now() - t, load: loads[i + 1] });
  }
  await api.close();
  process.stdout.write(JSON.stringify({ cold, warm, state: serialize(state).toString("base64") }));
  process.exit(0);
}

// ── parent ────────────────────────────────────────────────────────────────────
const EVENTS = Number(process.argv[2] ?? 10_000);
const PAYLOAD = Number(process.argv[3] ?? 1024);
const REPS = Number(process.argv[4] ?? 11);
const DELTA = Math.min(100, Math.floor(EVENTS / 10));
const BATCH = Math.max(1, Math.min(1000, Math.floor((4 * 1024 * 1024) / PAYLOAD)));

const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const f1 = (n) => (n === undefined || Number.isNaN(n) ? "–" : n < 10 ? n.toFixed(2) : n.toFixed(1));

const admin = new pg.Pool({ connectionString: url, max: 2, connectionTimeoutMillis: 3000 });
try {
  await admin.query("SELECT 1");
} catch (err) {
  console.error(`bench: Postgres is not reachable at ${url} (${err.code ?? (err.message || err.name)}). Start the Docker container es-sdk-test-pg.`);
  await admin.end().catch(() => undefined);
  process.exit(1);
}
const dropTables = () => admin.query(`DROP TABLE IF EXISTS "${TABLE}_fold_state"; DROP TABLE IF EXISTS "${TABLE}"`);
// the note is pseudo-random text, so Postgres cannot compress a large payload (TOAST, from ~2 KB)
// into a fraction of its size and make the read look cheaper than the application's would be
const NOTE = Math.max(16, PAYLOAD - 100);
let seedState = 42;
const noise = Array.from({ length: NOTE + 4096 }, () => {
  seedState = (Math.imul(seedState, 1103515245) + 12345) >>> 0; // Math.imul: a plain * loses bits and cycles early
  return "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"[seedState >>> 26];
}).join("");
const entry = (i) => {
  const prefix = `entry ${i} `;
  const at = (i * 7919) % 4096;
  return ledger.EntryBooked({ amount: (i * 37) % 1000, category: `c${i % CATEGORIES}`, note: prefix + noise.slice(at, at + NOTE - prefix.length) }, { ledgerId: "bench" });
};

const self = fileURLToPath(import.meta.url);
const child = (mode, after) =>
  JSON.parse(execFileSync(process.execPath, [self, "--child", mode, ...(after ? [String(after)] : [])], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024, env: { ...process.env, ES_BENCH_DATABASE_URL: url } }));

let exitCode = 0;
try {
  await dropTables();
  const pgVersion = (await admin.query("SHOW server_version")).rows[0].server_version;
  const seeder = new PostgresStore({ connection: url, timeouts: BENCH_TIMEOUTS, schema, table: TABLE, persistedFolds: true, poolSize: 2 });
  const seed = async (from, to) => {
    for (let i = from; i < to; i += BATCH) await seeder.append(Array.from({ length: Math.min(BATCH, to - i) }, (_, j) => entry(i + j)));
  };
  const t0 = performance.now();
  await seed(0, EVENTS - DELTA);
  // a stored state DELTA events behind the end, then one at the end: written by the library itself
  // (persist.after 1), kept, and put back before each cell that needs it
  child("persisted", 1);
  const snap = async () => (await admin.query(`SELECT state, cursor_xid::text AS xid, cursor_seq FROM "${TABLE}_fold_state"`)).rows;
  const [behind] = await snap();
  await seed(EVENTS - DELTA, EVENTS);
  child("persisted", 1);
  const [atEnd] = await snap();
  if (!behind || !atEnd || Number(behind.cursor_seq) >= Number(atEnd.cursor_seq)) throw new Error("bench: the stored states were not written as expected");
  // the first read after a bulk insert sets hint bits and fills the buffers: done here, untimed
  await admin.query(`VACUUM ANALYZE "${TABLE}"`);
  await seeder.query(query);
  await seeder.close();
  const seedS = (performance.now() - t0) / 1000;
  const facts = (await admin.query(`SELECT count(*)::int AS n, round(avg(octet_length(payload::text)))::int AS bytes, pg_size_pretty(pg_total_relation_size('"${TABLE}"')) AS size FROM "${TABLE}"`)).rows[0];

  // put a kept state back as the only row (or none): plain SQL as the owner, so the next cell
  // meets exactly the stored state it is named for, whatever the cell before it wrote
  const keyRow = (await admin.query(`SELECT tenant, fold_name, fold_version, query_hash, query_key FROM "${TABLE}_fold_state"`)).rows[0];
  const putRow = async (row) => {
    await admin.query(`DELETE FROM "${TABLE}_fold_state"`);
    if (row)
      await admin.query(`INSERT INTO "${TABLE}_fold_state" (tenant, fold_name, fold_version, query_hash, query_key, state, cursor_xid, cursor_seq) VALUES ($1, $2, $3, $4, $5, $6, $7::xid8, $8)`, [
        keyRow.tenant, keyRow.fold_name, keyRow.fold_version, keyRow.query_hash, keyRow.query_key, row.state, row.xid, row.cursor_seq,
      ]);
  };
  const CELLS = [
    { id: "fold", label: "fold on read", mode: "fold-on-read" },
    { id: "pf0", label: "persisted fold, delta 0", mode: "persisted", row: () => putRow(atEnd) },
    { id: "pfD", label: `persisted fold, delta ${DELTA}`, mode: "persisted", row: () => putRow(behind) },
    { id: "pfW", label: "persisted fold, nothing stored (writes)", mode: "persisted", row: () => putRow(null) },
  ];
  const results = Object.fromEntries(CELLS.map((c) => [c.id, []]));
  let reference;

  for (let round = -1; round < REPS; round++) {
    // round -1 is a warm-up round: run, compared, not kept
    for (let k = 0; k < CELLS.length; k++) {
      const cell = CELLS[(k + Math.max(0, round)) % CELLS.length];
      if (cell.row) await cell.row();
      const r = child(cell.mode);
      const state = deserialize(Buffer.from(r.state, "base64"));
      reference ??= state;
      if (!isDeepStrictEqual(state, reference)) {
        console.error(`bench: ${cell.label} returned a state that differs from the other cells`);
        exitCode = 1;
      }
      if (round >= 0) results[cell.id].push(r);
    }
  }

  // ── report ──
  const machine = `${cpus()[0]?.model ?? "?"}, ${cpus().length} cores, ${Math.round(totalmem() / 2 ** 30)} GiB, ${process.platform} ${process.arch}, Node ${process.version}, Postgres ${pgVersion}`;
  console.log(`persisted fold vs. fold on read — ${EVENTS.toLocaleString("en")} events of ${PAYLOAD} bytes (stored payload ${facts.bytes} bytes as JSON, table ${facts.size}), one context`);
  console.log(`${machine}; ${new Date().toISOString()}; ${REPS} fresh processes per cell (+1 warm-up round, not kept), order rotated; seed ${seedS.toFixed(1)} s`);
  console.log(`command: node bench/persisted-fold-bench.mjs ${EVENTS} ${PAYLOAD} ${REPS}\n`);
  console.log("| cell | start | events | cold totalMs median (min–max) | readMs | foldMs | stored.readMs | stored.writeMs | warm totalMs median (min–max) | warm events |");
  console.log("|---|---|---|---|---|---|---|---|---|---|");
  const summary = {};
  for (const cell of CELLS) {
    const rs = results[cell.id];
    const cold = rs.map((r) => r.cold.totalMs);
    const starts = [...new Set(rs.map((r) => r.cold.load.start))].join("/");
    const events = [...new Set(rs.map((r) => r.cold.load.events))].join("/");
    const warmPerProcess = rs.map((r) => median(r.warm.map((w) => w.totalMs)));
    const warmEvents = Math.max(...rs.flatMap((r) => r.warm.map((w) => w.load.events)));
    const warmStarts = [...new Set(rs.flatMap((r) => r.warm.map((w) => w.load.start)))].join("/");
    const st = (pick) => (rs.every((r) => r.cold.load.stored && pick(r.cold.load.stored) !== undefined) ? median(rs.map((r) => pick(r.cold.load.stored))) : undefined);
    const written = rs.filter((r) => r.cold.load.stored?.written).length;
    const errors = rs.map((r) => r.cold.load.stored?.error).filter(Boolean);
    if (errors.length) {
      console.error(`bench: ${cell.label}: stored.error ${errors[0]}`);
      exitCode = 1;
    }
    summary[cell.id] = { cold: median(cold), coldMin: Math.min(...cold), coldMax: Math.max(...cold), warm: median(warmPerProcess), events, warmEvents, written };
    console.log(
      `| ${cell.label} | ${starts} | ${events} | **${f1(median(cold))}** (${f1(Math.min(...cold))}–${f1(Math.max(...cold))}) | ${f1(median(rs.map((r) => r.cold.load.readMs)))} | ${f1(median(rs.map((r) => r.cold.load.foldMs)))} | ${f1(st((s) => s.readMs))} | ${written ? `${f1(st((s) => s.writeMs))} (written ${written}/${rs.length})` : "– (not written)"} | ${f1(median(warmPerProcess))} (${f1(Math.min(...warmPerProcess))}–${f1(Math.max(...warmPerProcess))}) | ${warmEvents} (${warmStarts}) |`,
    );
  }
  console.log("");
  const verdict = (id) => {
    const factor = summary.fold.cold / summary[id].cold;
    const faster = summary[id].cold < summary.fold.cold;
    const apart = summary[id].coldMax < summary.fold.coldMin; // every run of the one below every run of the other
    return `${faster ? "faster" : "NOT faster (red)"}, ${factor.toFixed(2)}× (median ${f1(summary.fold.cold)} → ${f1(summary[id].cold)} ms${faster && !apart ? "; the min–max ranges overlap" : ""})`;
  };
  console.log(`limit "cold read: persisted fold faster than fold on read" — delta 0: ${verdict("pf0")}; delta ${DELTA}: ${verdict("pfD")}`);
  console.log(`limit "warm read with nothing new folds 0 events" — max warm events per cell: ${CELLS.map((c) => `${c.label} ${summary[c.id].warmEvents}`).join("; ")}`);
  console.log(`warm, persisted fold vs. fold on read: ${(summary.fold.warm / summary.pf0.warm).toFixed(2)}× (median ${f1(summary.fold.warm)} vs. ${f1(summary.pf0.warm)} ms)`);
  console.log(`states equal across all cells and rounds: ${exitCode === 0 ? "yes" : "NO"}`);
  if (summary.pf0.cold >= summary.fold.cold || summary.pfD.cold >= summary.fold.cold) exitCode = 1;
  if (CELLS.some((c) => summary[c.id].warmEvents !== 0)) exitCode = 1;
  if (process.env.BENCH_JSON) process.stdout.write(`\nJSON ${JSON.stringify({ events: EVENTS, payload: PAYLOAD, reps: REPS, delta: DELTA, machine, facts, summary })}\n`);
} finally {
  await dropTables().catch(() => undefined);
  await admin.end();
}
process.exit(exitCode);
