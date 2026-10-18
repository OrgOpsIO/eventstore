// The showcase: what a read of kept state costs, per kind. Run it with `sh bench/showcase.sh`
// (it builds first). Uses the local Postgres test database (Docker container es-sdk-test-pg) in
// tables of its own — showcase_events and showcase_events_fold_state; showcase_subs,
// showcase_rows, showcase_checkpoint and showcase_dead_letters — dropped at the start and at the
// end. Stage 1: the persisted fold, against fold on read, in fresh processes. Stage 2: an append's
// read-your-writes token, a subscriber that reports it has passed it, and one poison event skipped
// while its neighbours are applied (processor mode). Stage 3 (bench/showcase-readmodel.mjs, table
// showcase_rm): a live read model's get and list, events folded per read, the same answer as
// es.context(), and a watcher receiving the new state after an append in another process. Stage 4
// (bench/showcase-persisted.mjs, tables showcase_pm, showcase_pm_read_model, showcase_pm_read_model_state):
// a persisted read model built by one process, resumed by a fresh one that folds only what came in
// between; get cold and warm beside es.context(), list, and a watcher in a standby process. Stage 5
// (bench/showcase-versions.mjs): a version bump, the rebuild beside the old version, the switch. Stage 6
// (bench/showcase-tenants.mjs, schema showcase_rls and roles showcase_owner, showcase_app, showcase_proc):
// under rls, one cross-tenant processor for two tenants, each tenant seeing only its own rows, and the
// README erasure of one tenant with the processor running, after which nothing of it remains. Stage 7 (no
// database): typed definitions — the project's tsc on bench/showcase-types.ts, which imports the built package and
// holds three mistakes on purpose; exactly those three compiler messages are expected. At the end, the "## Summary"
// section of bench/RESULTS.md (the first table when there is no summary yet). Every stage ends with its expectations.
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { deserialize, serialize } from "node:v8";
import pg from "pg";
import { z } from "zod";
import { compareCursor, createEventStore, cursorOf, defineEvents, defineFold, MemoryStore, buildSchema } from "../dist/index.js";
import { PostgresStore } from "../dist/postgres/index.js";
import { subscribe } from "../dist/subscribe/index.js";
import { stage4 } from "./showcase-persisted.mjs";
import { stage5 } from "./showcase-versions.mjs";
import { stage6 } from "./showcase-tenants.mjs";
import { RM_TABLE, stage3 } from "./showcase-readmodel.mjs";

const url = process.env.ES_SHOWCASE_DATABASE_URL ?? "postgres://es:es@localhost:5432/es_sdk_test";
const TABLE = "showcase_events";
const EVENTS = 10_000;
const PAYLOAD = 1024; // bytes of the stored payload, about
const CATEGORIES = 40;

const ledger = defineEvents({
  EntryBooked: { data: z.object({ amount: z.number(), category: z.string(), note: z.string() }), scopes: ["ledgerId"] },
});
const schema = buildSchema([ledger], { strict: true });
const query = ledger.$filter({ scopes: { ledgerId: "showcase" } });

/** A fold that costs something per event, like the applications' folds: running statistics per category. */
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
/** The same fold with an identity: a name and a version. Only this one is persisted. */
const stats = defineFold({ name: "ledger-stats", version: 1, fold: statsFold });
const initial = () => new Map();

const entry = (i) =>
  ledger.EntryBooked({ amount: (i * 37) % 1000, category: `c${i % CATEGORIES}`, note: `entry ${i} `.padEnd(PAYLOAD - 100, "-") }, { ledgerId: "showcase" });

// ── child: one fresh process, one cold load ───────────────────────────────────
if (process.argv[2] === "--child") {
  const mode = process.argv[3]; // "fold-on-read" | "persisted"
  let load;
  const api = createEventStore({
    events: [ledger],
    connection: url,
    postgres: { table: TABLE, persistedFolds: mode === "persisted", poolSize: 2 },
    contextCache: { onLoad: (l) => (load ??= l) },
  });
  await api.store(); // install check and first connection before the clock runs, as in a process that already served a request
  const t0 = performance.now();
  const { state } = await api.context({ query, fold: mode === "persisted" ? stats : statsFold, initial });
  const totalMs = performance.now() - t0;
  await api.close();
  process.stdout.write(JSON.stringify({ load, totalMs, state: serialize(state).toString("base64") }));
  process.exit(0);
}

// ── parent ──────────────────────────────────────────────────────────────────
const fmt = (n) => (n === undefined ? "–" : n.toFixed(1));
const row = (cells, widths) => cells.map((c, i) => String(c).padEnd(widths[i])).join("  ");
const WIDTHS = [34, 8, 7, 8, 8, 14, 8, 9];
const HEADER = ["read", "start", "events", "readMs", "foldMs", "stored.readMs", "written", "totalMs"];
const line = (label, load, totalMs) =>
  row([label, load.start, load.events, fmt(load.readMs), fmt(load.foldMs), load.stored ? fmt(load.stored.readMs) : "–", load.stored ? (load.stored.written ? "yes" : "no") : "–", fmt(totalMs)], WIDTHS);

const admin = new pg.Pool({ connectionString: url, max: 2, connectionTimeoutMillis: 3000 });
try {
  await admin.query("SELECT 1");
} catch (err) {
  console.error(`showcase: Postgres is not reachable at ${url} (${err.code ?? (err.message || err.name)}).`);
  console.error("Start the Docker container es-sdk-test-pg (docker start es-sdk-test-pg) and run sh bench/showcase.sh again.");
  await admin.end().catch(() => undefined);
  process.exit(1);
}
const SUBS = "showcase_subs";
const ROWS = "showcase_rows";
const CHECKPOINT = "showcase_checkpoint";
const DEAD = "showcase_dead_letters";
const dropTables = () =>
  admin.query(`DROP TABLE IF EXISTS "${TABLE}_fold_state"; DROP TABLE IF EXISTS "${TABLE}"; DROP TABLE IF EXISTS "${ROWS}", "${CHECKPOINT}", "${DEAD}", "${SUBS}", "${RM_TABLE}" CASCADE`);

// ── stage 2: what read models need from subscriptions ─────────────────────────
const work = defineEvents({
  A: { data: z.object({ n: z.number() }) },
  B: { data: z.object({ n: z.number() }) },
  Job: { data: z.object({ n: z.number(), poison: z.boolean().optional() }) },
});
const show = (c) => (c ? `(${c.transactionId}, ${c.sequence})` : "null");

/** Loop whenCaughtUp() until hasPassed(token) or a 2 s deadline. */
async function untilPassed(sub, token) {
  const deadline = Date.now() + 2_000;
  while (Date.now() < deadline) {
    await sub.whenCaughtUp();
    if (sub.hasPassed(token)) return true;
  }
  return sub.hasPassed(token);
}

/** An append returns its token; a subscriber on A passes a B by the watermark, an A by the cursor. Returns false on a miss. */
async function tokenAndPassed(label, store) {
  const sub = subscribe(`showcase-rw-${label}`, { types: ["A"] }, () => {}, { store, pollIntervalMs: 50 });
  try {
    await sub.whenCaughtUp();
    console.log(`${label}: a subscriber on type A`);
    const b = await store.append([work.B({ n: 1 })]);
    const tokenB = { transactionId: b.transactionId, sequence: b.last };
    console.log(`  append([B])          token ${show(tokenB)}   hasPassed right after the append: ${sub.hasPassed(tokenB) ? "yes" : "no"}`);
    const passedB = await untilPassed(sub, tokenB);
    console.log(`  after whenCaughtUp() hasPassed: ${passedB ? "yes" : "NO"}   cursor ${show(sub.cursor)}   watermark ${sub.watermark}`);
    const cursorReachedB = sub.cursor !== null && (BigInt(sub.cursor.transactionId) > BigInt(tokenB.transactionId) || (sub.cursor.transactionId === tokenB.transactionId && sub.cursor.sequence >= tokenB.sequence));
    console.log(`  → passed ${cursorReachedB ? "by the cursor" : "by the watermark: the cursor did not reach it, B is not matched"}`);
    const a = await store.append([work.A({ n: 2 })]);
    const tokenA = { transactionId: a.transactionId, sequence: a.last };
    const passedA = await untilPassed(sub, tokenA);
    const byCursor = sub.cursor !== null && sub.cursor.transactionId === tokenA.transactionId && sub.cursor.sequence === tokenA.sequence;
    console.log(`  append([A])          token ${show(tokenA)}   hasPassed: ${passedA ? "yes" : "NO"}   cursor ${show(sub.cursor)} → ${byCursor ? "passed by the cursor" : "NOT by the cursor"}`);
    return passedB && !cursorReachedB && passedA && byCursor;
  } finally {
    await sub.stop();
  }
}

/** Processor mode on Postgres: rows and checkpoint in one transaction; one poison event skipped. Returns false on a miss. */
async function poisonSkipped(store) {
  // no primary key on purpose: a second application shows as a second row instead of a refused insert
  await admin.query(`CREATE TABLE "${ROWS}" (seq bigint NOT NULL, n int NOT NULL)`);
  await admin.query(`CREATE TABLE "${CHECKPOINT}" (name text PRIMARY KEY, xid xid8, seq bigint)`);
  await admin.query(`CREATE TABLE "${DEAD}" (xid xid8 NOT NULL, seq bigint NOT NULL, event_type text NOT NULL, event_id text NOT NULL, error text NOT NULL)`);
  const name = "showcase-jobs";
  const inTransaction = async (work) => {
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
  };
  // the README pattern: lock the checkpoint row, apply only the events beyond it, move it
  const lockCheckpoint = async (client) => {
    await client.query(`INSERT INTO "${CHECKPOINT}" (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, [name]);
    const c = (await client.query(`SELECT xid::text AS "transactionId", seq::int AS sequence FROM "${CHECKPOINT}" WHERE name = $1 FOR UPDATE`, [name])).rows[0];
    return c.transactionId === null ? null : c;
  };
  const beyond = (events, cp) => events.filter((e) => cp === null || compareCursor(e, cp) > 0);
  const moveCheckpoint = (client, c) => client.query(`UPDATE "${CHECKPOINT}" SET xid = $2::xid8, seq = $3 WHERE name = $1`, [name, c.transactionId, c.sequence]);
  const target = {
    async load() {
      const c = (await admin.query(`SELECT xid::text AS "transactionId", seq::int AS sequence FROM "${CHECKPOINT}" WHERE name = $1`, [name])).rows[0];
      return c?.transactionId ? c : null;
    },
    async skip(event, error) {
      await inTransaction(async (client) => {
        if (beyond([event], await lockCheckpoint(client)).length === 0) return false; // skipped already
        await moveCheckpoint(client, cursorOf([event]));
        await client.query(`INSERT INTO "${DEAD}" VALUES ($1::xid8, $2, $3, $4, $5)`, [event.transactionId, event.sequence, event.type, event.id, String(error)]);
        return true;
      });
    },
  };
  const counts = { handler: 0, onError: 0 };
  const handler = async (events) => {
    counts.handler++;
    if (events.some((e) => e.data.poison)) throw new Error("poison event");
    await inTransaction(async (client) => {
      const fresh = beyond(events, await lockCheckpoint(client));
      if (fresh.length === 0) return false; // all applied already: a redelivery
      await client.query(`INSERT INTO "${ROWS}" SELECT unnest($1::bigint[]), unnest($2::int[])`, [fresh.map((e) => e.sequence), fresh.map((e) => e.data.n)]);
      await moveCheckpoint(client, cursorOf(events));
      return true;
    });
  };
  const options = { store, target, batchSize: 10, pollIntervalMs: 50, onError: () => (counts.onError++, "skip") };
  const batch = await store.append(Array.from({ length: 7 }, (_, i) => work.Job({ n: i + 1, ...(i === 3 ? { poison: true } : {}) })));
  const sub = subscribe(name, { types: ["Job"] }, handler, options);
  await untilPassed(sub, { transactionId: batch.transactionId, sequence: batch.last });
  await sub.stop();
  const applied = (await admin.query(`SELECT seq::int AS seq FROM "${ROWS}" ORDER BY seq`)).rows.map((r) => r.seq);
  const dead = (await admin.query(`SELECT event_type, seq::int AS seq, error FROM "${DEAD}"`)).rows;
  const checkpoint = await target.load();
  console.log(`Postgres, processor mode: one append of 7 Job events (sequences ${batch.first}–${batch.last}), the 4th poison, batchSize 10, onError → "skip"`);
  console.log(`  events applied: ${applied.length} (sequences ${applied.join(", ")})`);
  console.log(`  dead letters:   ${dead.length} (${dead.map((d) => `${d.event_type} at sequence ${d.seq}: ${d.error}`).join("; ")})`);
  console.log(`  handler calls:  ${counts.handler}   onError calls: ${counts.onError} (the page, then the isolated event)`);
  console.log(`  target checkpoint ${show(checkpoint)}   sub.cursor ${show(sub.cursor)}   ${show(checkpoint) === show(sub.cursor) ? "equal" : "NOT EQUAL"}`);
  let again = 0;
  const fresh = subscribe(name, { types: ["Job"] }, (events) => void (again += events.length), { ...options, onError: () => "stop" });
  await fresh.whenCaughtUp();
  await fresh.stop();
  console.log(`  a fresh subscribe over the same target: delivered again: ${again}`);

  // A lost COMMIT acknowledgement: the handler's transaction committed, the subscription never heard
  // so, and two more events arrive before its retry, so the redelivered page is longer than the
  // committed one. The target applies only what is beyond its checkpoint.
  const first = await store.append([1, 2, 3].map((n) => work.Job({ n: 100 + n })));
  let more = null;
  const pages = [];
  const lossy = subscribe(name, { types: ["Job"] }, async (events) => {
    pages.push(events.length);
    await handler(events);
    if (more === null) {
      more = await store.append([4, 5].map((n) => work.Job({ n: 100 + n })));
      throw new Error("acknowledgement of COMMIT lost");
    }
  }, { ...options, onError: () => "retry" });
  const deadline = Date.now() + 5_000;
  while (more === null && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10));
  const lossyPassed = more !== null && (await untilPassed(lossy, { transactionId: more.transactionId, sequence: more.last }));
  await lossy.stop();
  const counted = (await admin.query(`SELECT count(*)::int AS rows, count(DISTINCT seq)::int AS events FROM "${ROWS}" WHERE seq >= $1`, [first.first])).rows[0];
  const once = lossyPassed && counted.rows === 5 && counted.events === 5;
  console.log(`  a lost COMMIT acknowledgement, 2 new events before the retry: pages of ${pages.join(", then ")} events`);
  console.log(`  rows for those 5 events: ${counted.rows} → ${once ? "each applied once" : "NOT once each"}`);

  const expected = [1, 2, 3, 5, 6, 7].map((i) => batch.first + i - 1);
  return (
    isDeepStrictEqual(applied, expected) &&
    dead.length === 1 &&
    dead[0].seq === batch.first + 3 &&
    show(checkpoint) === show(sub.cursor) &&
    again === 0 &&
    isDeepStrictEqual(pages, [3, 5]) &&
    once
  );
}

/**
 * Stage 7: typed definitions. The project's tsc on bench/showcase-types.ts against the built package (dist), as a
 * consumer's editor sees it; exactly the three lines marked `// mistake:` must be reported, and nothing else.
 */
function stage7() {
  console.log("@orgops/eventstore showcase — stage 7: typed read model definitions and reads");
  const fixture = fileURLToPath(new URL("./showcase-types.ts", import.meta.url));
  const project = fileURLToPath(new URL("..", import.meta.url));
  const args = ["--noEmit", "--strict", "--target", "es2022", "--module", "nodenext", "--moduleResolution", "nodenext", "--skipLibCheck", "false", "bench/showcase-types.ts"];
  console.log(`tsc ${args.join(" ")}   (the fixture imports ../dist/index.js, the built package)`);
  const t0 = performance.now();
  const run = spawnSync(process.execPath, [fileURLToPath(new URL("../node_modules/typescript/bin/tsc", import.meta.url)), ...args], { cwd: project, encoding: "utf8" });
  const ms = performance.now() - t0;
  const marked = readFileSync(fixture, "utf8")
    .split("\n")
    .flatMap((l, i) => (l.includes("// mistake:") && !l.trimStart().startsWith("//") ? [{ line: i + 1, what: l.slice(l.indexOf("// mistake:") + 12).trim() }] : []));
  const errors = (run.stdout ?? "")
    .split("\n")
    .map((l) => l.match(/^(.+?)\((\d+),(\d+)\): error (TS\d+): (.*)$/))
    .filter(Boolean)
    .map((m) => ({ file: m[1], line: Number(m[2]), code: m[4], text: m[5] }));
  for (const e of errors) console.log(`  ${e.file}:${e.line}  ${e.code}  ${e.text}`);
  const expected = errors.length === marked.length && marked.every((m) => errors.some((e) => e.file === "bench/showcase-types.ts" && e.line === m.line));
  console.log(`${errors.length} compiler messages in ${(ms / 1000).toFixed(1)} s; the fixture's marked mistakes: ${marked.map((m) => `line ${m.line} ${m.what}`).join("; ")}`);
  console.log(`each mistake reported on its line, and no other error: ${expected ? "yes" : "NO"}`);
  if (run.error) console.log(`tsc did not run: ${run.error.message}`);
  console.log(`stage 7 expectations: ${expected ? "all met" : "NOT MET"}\n`);
  return expected;
}

let exitCode = 0;
try {
  await dropTables();
  console.log("@orgops/eventstore showcase — stages 1–7, one run\n");
  console.log(`@orgops/eventstore showcase — stage 1: the persisted fold`);
  console.log(`One context of ${EVENTS.toLocaleString("en")} events of ~${PAYLOAD} bytes; the fold keeps running statistics per category (a Map of ${CATEGORIES}).\n`);

  // ── memory store ──
  const memory = new MemoryStore({ schema, persistedFolds: true });
  for (let i = 0; i < EVENTS; i += 1000) await memory.append(Array.from({ length: 1000 }, (_, j) => entry(i + j)));
  const memoryLoad = async () => {
    let load;
    const api = createEventStore({ events: [ledger], store: memory, contextCache: { onLoad: (l) => (load ??= l) } });
    const t0 = performance.now();
    const { state } = await api.context({ query, fold: stats, initial });
    return { load, totalMs: performance.now() - t0, state };
  };
  const m1 = await memoryLoad();
  const m2 = await memoryLoad();
  console.log("Memory store (persistedFolds: true), two fresh runtimes over the same store:");
  console.log(row(HEADER, WIDTHS));
  console.log(line("1 persisted fold, nothing stored", m1.load, m1.totalMs));
  console.log(line("2 persisted fold, fresh runtime", m2.load, m2.totalMs));
  console.log(`states equal: ${isDeepStrictEqual(m1.state, m2.state) ? "yes" : "NO"}\n`);
  if (!isDeepStrictEqual(m1.state, m2.state)) exitCode = 1;

  // ── Postgres ──
  const seeder = new PostgresStore({ connection: url, schema, table: TABLE, persistedFolds: true, poolSize: 2 });
  for (let i = 0; i < EVENTS; i += 1000) await seeder.append(Array.from({ length: 1000 }, (_, j) => entry(i + j)));
  // the first read after a bulk insert sets hint bits and fills the buffers: done here, untimed, so
  // the three processes below all meet the same, warm database and differ only in what they fold
  await admin.query(`VACUUM ANALYZE "${TABLE}"`);
  await seeder.query(query);
  await seeder.close();
  const size = await admin.query(`SELECT round(avg(octet_length(payload::text))) AS s FROM "${TABLE}"`);

  const self = fileURLToPath(import.meta.url);
  const child = (mode) => JSON.parse(execFileSync(process.execPath, [self, "--child", mode], { encoding: "utf8", env: { ...process.env, ES_SHOWCASE_DATABASE_URL: url } }));
  const p1 = child("fold-on-read");
  const p2 = child("persisted");
  const p3 = child("persisted");
  console.log(`Postgres (${TABLE}, payload ${size.rows[0].s} bytes per event as JSON), each row a fresh node process with an empty cache:`);
  console.log(row(HEADER, WIDTHS));
  console.log(line("1 fold on read (plain fold)", p1.load, p1.totalMs));
  console.log(line("2 persisted fold, table empty", p2.load, p2.totalMs));
  console.log(line("3 persisted fold, from stored", p3.load, p3.totalMs));
  const states = [p1, p2, p3].map((p) => deserialize(Buffer.from(p.state, "base64")));
  const equal = isDeepStrictEqual(states[0], states[1]) && isDeepStrictEqual(states[0], states[2]);
  console.log(`states equal: ${equal ? "yes (all three)" : "NO"}`);
  if (!equal) exitCode = 1;
  const stored = await admin.query(`SELECT fold_name, fold_version, cursor_seq, octet_length(state) AS bytes FROM "${TABLE}_fold_state"`);
  for (const r of stored.rows) console.log(`stored state: ${r.fold_name} v${r.fold_version}, checkpoint at sequence ${r.cursor_seq}, ${r.bytes} bytes`);
  console.log("start: where the load began — initial (from zero), stored (persisted fold), cache (in memory). events: folded in this load.");
  console.log("Times vary per run; bench/RESULTS.md holds the measured numbers with their commands.");
  // stage 1 expectations: equal states; fold on read folds every event; a fresh runtime and a fresh process start from
  // the stored state and fold only the delta, here nothing
  const stage1Ok =
    isDeepStrictEqual(m1.state, m2.state) &&
    equal &&
    m2.load.start === "stored" &&
    m2.load.events === 0 &&
    p1.load.events === EVENTS &&
    p3.load.start === "stored" &&
    p3.load.events === 0;
  console.log(`stage 1 expectations: ${stage1Ok ? "all met" : "NOT MET"}\n`);
  if (!stage1Ok) exitCode = 1;

  // ── stage 2 ──
  console.log("@orgops/eventstore showcase — stage 2: what read models need from subscriptions");
  console.log("A read-your-writes token is { transactionId: appended.transactionId, sequence: appended.last }; sub.hasPassed(token) says whether a subscriber has handled or skipped it.\n");
  const memoryOk = await tokenAndPassed("Memory store", new MemoryStore({ schema: buildSchema([work], { strict: false }) }));
  const subsStore = new PostgresStore({ connection: url, schema: buildSchema([work], { strict: false }), table: SUBS, poolSize: 2 });
  try {
    const postgresOk = await tokenAndPassed(`Postgres (${SUBS})`, subsStore);
    console.log("");
    const poisonOk = await poisonSkipped(subsStore);
    console.log(`stage 2 expectations: ${memoryOk && postgresOk && poisonOk ? "all met" : "NOT MET"}\n`);
    if (!(memoryOk && postgresOk && poisonOk)) exitCode = 1;
  } finally {
    await subsStore.close();
  }

  // ── stage 3 ──
  if (!(await stage3(url, admin))) exitCode = 1;

  // ── stage 4 ──
  if (!(await stage4(url, admin))) exitCode = 1;

  // ── stage 5 ──
  if (!(await stage5(url, admin))) exitCode = 1;

  // ── stage 6 ──
  if (!(await stage6(url, admin))) exitCode = 1;

  // ── stage 7 ──
  if (!stage7()) exitCode = 1;
} finally {
  await dropTables().catch(() => undefined);
  await admin.end();
}

// ── where bench/RESULTS.md stands ──
const results = fileURLToPath(new URL("./RESULTS.md", import.meta.url));
if (!existsSync(results)) {
  console.log("bench/RESULTS.md: missing (the measurer writes it).");
} else {
  const lines = readFileSync(results, "utf8").split("\n");
  const summary = lines.findIndex((l) => l.startsWith("## Summary"));
  if (summary >= 0) {
    let end = summary + 1;
    while (end < lines.length && !lines[end].startsWith("## ")) end++;
    console.log("bench/RESULTS.md, summary section:");
    console.log(lines.slice(summary, end).join("\n").trimEnd());
  } else {
    const start = lines.findIndex((l) => l.trimStart().startsWith("|"));
    let end = start;
    while (end >= 0 && end < lines.length && lines[end].trimStart().startsWith("|")) end++;
    console.log("bench/RESULTS.md, first table (no summary section yet):");
    console.log(start < 0 ? "(no table yet)" : lines.slice(start, end).join("\n"));
  }
}
process.exit(exitCode);
