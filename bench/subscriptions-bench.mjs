// What the stage-2 subscription additions cost, on the local Postgres test database (and the memory
// store where it applies). Build first, or run `sh bench/subscriptions.sh`, which builds and then
// runs this.
//   node bench/subscriptions-bench.mjs [cell=all] [reps] [--before=<dist dir>]
//     cell: append | query | poison | latency | all
//     --before: a build of the library before the change (its dist/), for the append and query
//               cells; they then run before and after in alternating fresh processes. Without it
//               only "after" is measured.
// Tables of its own (bench_sub_*), dropped at the start and the end.
//
// Cells:
//   append   single-event append, `per` timed appends per fresh process after a warm-up; the
//            statement now carries `pg_current_xact_id_if_assigned()::text` (decision 10)
//   query    the subscriber's settled read with `ctx.xmin::text` (decision 11): an empty page at the
//            head, and a full 500-event page
//   poison   one page of 500 events with one poison event at 0 / 250 / 499 (and none): memory
//            (default mode, in-process all-or-nothing handler) and Postgres (processor mode, one
//            transaction per handler call); time from the first delivery until the page is fully
//            committed, handler calls, transactions, events applied (decision 12, 13)
//   latency  read-your-writes: a write from another store (another pool), time until
//            `sub.hasPassed(token)`, without and with the doorbell (`live: true`), for a write the
//            subscriber does not match (passed by the watermark) and one it matches (by the cursor)
import { execFileSync } from "node:child_process";
import { cpus, totalmem } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import pg from "pg";
import { z } from "zod";

const url = process.env.ES_BENCH_DATABASE_URL ?? "postgres://es:es@localhost:5432/es_sdk_test";
const self = fileURLToPath(import.meta.url);
const AFTER = fileURLToPath(new URL("../dist", import.meta.url));
const T = { append: "bench_sub_append", query: "bench_sub_query", poison: "bench_sub_poison", rows: "bench_sub_rows", checkpoint: "bench_sub_checkpoint", dead: "bench_sub_dead", poll: "bench_sub_rw_poll", live: "bench_sub_rw_live" };

const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const quantile = (xs, q) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(q * s.length))];
};
const f = (n) => (n === undefined || Number.isNaN(n) ? "–" : n < 10 ? n.toFixed(3) : n < 100 ? n.toFixed(2) : n.toFixed(1));
const range = (xs) => `${f(Math.min(...xs))}–${f(Math.max(...xs))}`;
const lib = async (dist) => ({
  core: await import(pathToFileURL(resolve(dist, "index.js")).href),
  postgres: await import(pathToFileURL(resolve(dist, "postgres/index.js")).href),
  subscribe: await import(pathToFileURL(resolve(dist, "subscribe/index.js")).href),
});
const eventsOf = (core) => core.defineEvents({ Job: { data: z.object({ n: z.number(), poison: z.boolean().optional(), note: z.string().optional() }) }, A: { data: z.object({ n: z.number() }) }, B: { data: z.object({ n: z.number() }) } });
const NOTE = "x".repeat(900); // ~1 KB stored payload per event for the query cell

// ── children: one fresh process, one library build ─────────────────────────────
if (process.argv[2] === "--child") {
  const [, , , kind, dist, ...rest] = process.argv;
  const { core, postgres } = await lib(dist);
  const work = eventsOf(core);
  const schema = core.buildSchema([work], { strict: false });
  if (kind === "append") {
    const per = Number(rest[0]);
    const store = new postgres.PostgresStore({ connection: url, schema, table: T.append, poolSize: 2 });
    for (let i = 0; i < 30; i++) await store.append([work.Job({ n: i })]); // warm-up: install check, connection, plan cache
    const ms = [];
    let withId = 0;
    for (let i = 0; i < per; i++) {
      const t = performance.now();
      const r = await store.append([work.Job({ n: i })]);
      ms.push(performance.now() - t);
      if (typeof r.transactionId === "string") withId++;
    }
    await store.close();
    process.stdout.write(JSON.stringify({ median: median(ms), p90: quantile(ms, 0.9), withId }));
    process.exit(0);
  }
  if (kind === "query") {
    const per = Number(rest[0]);
    const store = new postgres.PostgresStore({ connection: url, schema, table: T.query, poolSize: 2 });
    const q = { types: ["Job"] };
    const head = (await store.query(q, { settledOnly: true, order: "desc", limit: 1, payload: false, version: false })).settledCursor;
    const middle = (await store.query(q, { settledOnly: true, limit: 1000, payload: false, version: false })).settledCursor;
    const time = async (opts, n, expect) => {
      for (let i = 0; i < 20; i++) await store.query(q, opts); // warm-up
      const ms = [];
      let watermarks = 0;
      for (let i = 0; i < n; i++) {
        const t = performance.now();
        const r = await store.query(q, opts);
        ms.push(performance.now() - t);
        if (r.events.length !== expect) throw new Error(`query returned ${r.events.length} events, expected ${expect}`);
        if (r.watermark !== undefined) watermarks++;
      }
      return { median: median(ms), p90: quantile(ms, 0.9), watermarks };
    };
    const empty = await time({ settledOnly: true, cursor: head, limit: 500, version: false }, per, 0);
    const page = await time({ settledOnly: true, cursor: middle, limit: 500, version: false }, Math.max(20, Math.floor(per / 3)), 500);
    await store.close();
    process.stdout.write(JSON.stringify({ empty, page }));
    process.exit(0);
  }
  throw new Error(`unknown child ${kind}`);
}

// ── parent ─────────────────────────────────────────────────────────────────────
const args = process.argv.slice(2).filter((a) => !a.startsWith("--"));
const beforeArg = process.argv.slice(2).find((a) => a.startsWith("--before="));
const BEFORE = beforeArg ? resolve(beforeArg.slice("--before=".length)) : null;
const CELL = args[0] ?? "all";
const REPS = Number(args[1] ?? 0) || undefined;

const admin = new pg.Pool({ connectionString: url, max: 3, connectionTimeoutMillis: 3000 });
try {
  await admin.query("SELECT 1");
} catch (err) {
  console.error(`bench: Postgres is not reachable at ${url} (${err.code ?? (err.message || err.name)}). Start the Docker container es-sdk-test-pg.`);
  await admin.end().catch(() => undefined);
  process.exit(1);
}
const dropTables = () => admin.query(Object.values(T).map((t) => `DROP TABLE IF EXISTS "${t}" CASCADE;`).join(" "));
const child = (kind, dist, ...rest) => JSON.parse(execFileSync(process.execPath, [self, "--child", kind, dist, ...rest.map(String)], { encoding: "utf8", env: { ...process.env, ES_BENCH_DATABASE_URL: url } }));
const builds = BEFORE ? [["before", BEFORE], ["after", AFTER]] : [["after", AFTER]];
const { core, postgres, subscribe: sub } = await lib(AFTER);
const work = eventsOf(core);
const schema = core.buildSchema([work], { strict: false });
let exitCode = 0;
const red = (why) => {
  exitCode = 1;
  console.log(`RED: ${why}`);
};

/** Before/after in alternating fresh processes; returns per build the list of child results. */
function alternate(kind, reps, ...rest) {
  const out = Object.fromEntries(builds.map(([name]) => [name, []]));
  child(kind, AFTER, ...rest); // warm-up round, not kept
  for (let r = 0; r < reps; r++) {
    const order = r % 2 === 0 ? builds : [...builds].reverse();
    for (const [name, dist] of order) out[name].push(child(kind, dist, ...rest));
  }
  return out;
}

async function appendCell() {
  const reps = REPS ?? 9;
  const per = 300;
  console.log(`## append: one event per append, median of ${per} appends per fresh process, ${reps} processes per build, alternating`);
  console.log(`command: node bench/subscriptions-bench.mjs append ${reps}${BEFORE ? " --before=<dist of 0f04d25>" : ""}`);
  const out = alternate("append", reps, per);
  console.log("| build | append median ms (median of process medians) | min–max of process medians | p90 ms (median) | results with transactionId |");
  console.log("|---|---|---|---|---|");
  for (const [name] of builds) {
    const xs = out[name];
    console.log(`| ${name} | ${f(median(xs.map((x) => x.median)))} | ${range(xs.map((x) => x.median))} | ${f(median(xs.map((x) => x.p90)))} | ${xs.reduce((s, x) => s + x.withId, 0)} of ${xs.length * per} |`);
  }
  if (out.after.some((x) => x.withId !== per)) red("an append after the change returned no transactionId");
  if (BEFORE) {
    const b = median(out.before.map((x) => x.median));
    const a = median(out.after.map((x) => x.median));
    console.log(`after − before: ${(a - b >= 0 ? "+" : "") + (a - b).toFixed(3)} ms (${(((a - b) / b) * 100).toFixed(1)}%)`);
  }
  console.log("");
}

async function queryCell() {
  const reps = REPS ?? 9;
  const per = 300;
  await admin.query(`DROP TABLE IF EXISTS "${T.query}"`);
  const seeder = new postgres.PostgresStore({ connection: url, schema, table: T.query, poolSize: 2 });
  for (let i = 0; i < 2000; i += 500) await seeder.append(Array.from({ length: 500 }, (_, j) => work.Job({ n: i + j, note: NOTE })));
  await seeder.close();
  await admin.query(`VACUUM ANALYZE "${T.query}"`);
  console.log(`## query: the subscriber's settled read (settledOnly, cursor, limit 500, version: false) on 2 000 events of ~1 KB; ${reps} processes per build, alternating`);
  console.log(`command: node bench/subscriptions-bench.mjs query ${reps}${BEFORE ? " --before=<dist of 0f04d25>" : ""}`);
  const out = alternate("query", reps, per);
  console.log("| build | empty page at the head, median ms (min–max of process medians) | 500-event page, median ms (min–max) | reads with a watermark |");
  console.log("|---|---|---|---|");
  for (const [name] of builds) {
    const xs = out[name];
    const marks = xs.reduce((s, x) => s + x.empty.watermarks + x.page.watermarks, 0);
    const total = xs.reduce((s) => s + per + Math.max(20, Math.floor(per / 3)), 0);
    console.log(`| ${name} | ${f(median(xs.map((x) => x.empty.median)))} (${range(xs.map((x) => x.empty.median))}) | ${f(median(xs.map((x) => x.page.median)))} (${range(xs.map((x) => x.page.median))}) | ${marks} of ${total} |`);
    if (name === "after" && marks !== total) red("a read after the change carried no watermark");
  }
  if (BEFORE) {
    for (const k of ["empty", "page"]) {
      const b = median(out.before.map((x) => x[k].median));
      const a = median(out.after.map((x) => x[k].median));
      console.log(`${k === "empty" ? "empty page" : "500-event page"}: after − before ${(a - b >= 0 ? "+" : "") + (a - b).toFixed(3)} ms (${(((a - b) / b) * 100).toFixed(1)}%)`);
    }
  }
  console.log("");
}

/** One page of 500 events with a poison event at `at` (or none), delivered and isolated; returns what happened. */
async function poisonRun(kind, at, stores) {
  const PAGE = 500;
  const counts = { handler: 0, onError: 0, transactions: 0 };
  const applied = [];
  let t0 = 0;
  let tEnd = 0;
  let last = 0;
  const done = (cursor) => {
    if (cursor && cursor.sequence === last && tEnd === 0) tEnd = performance.now();
  };
  const name = `bench-poison-${kind}`;
  const query = { types: ["Job"] };
  const onError = () => (counts.onError++, "skip");
  let options;
  let handler;
  let deadLetters = () => Promise.resolve(0);
  let appliedCount = () => Promise.resolve(applied.length);
  if (kind === "memory") {
    const store = new core.MemoryStore({ schema });
    const inner = sub.memoryCursors();
    const cursors = { load: (n) => inner.load(n), save: async (n, c) => (await inner.save(n, c), done(c)) };
    let skipped = 0;
    handler = (events) => {
      counts.handler++;
      if (counts.handler === 1) t0 = performance.now();
      if (events.some((e) => e.data.poison)) throw new Error("poison event");
      for (const e of events) applied.push(e.sequence); // all or nothing, like a rolled-back transaction
    };
    options = { store, cursors, batchSize: PAGE, pollIntervalMs: 1000, onError: (e, b) => (b.length === 1 && b[0].data.poison && skipped++, onError()) };
    deadLetters = () => Promise.resolve(skipped);
    stores.memory = store;
  } else {
    const store = stores.postgres;
    const { rows, checkpoint, dead } = T;
    await admin.query(`TRUNCATE "${rows}", "${dead}"`);
    const inTransaction = async (fn) => {
      const client = await admin.connect();
      counts.transactions++;
      try {
        await client.query("BEGIN");
        await client.query((await fn(client)) ? "COMMIT" : "ROLLBACK");
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      } finally {
        client.release();
      }
    };
    // the README pattern (decision 14): lock the checkpoint row, apply only the events beyond it, move it
    const lockCheckpoint = async (client) => {
      await client.query(`INSERT INTO "${checkpoint}" (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, [name]);
      const c = (await client.query(`SELECT xid::text AS "transactionId", seq::int AS sequence FROM "${checkpoint}" WHERE name = $1 FOR UPDATE`, [name])).rows[0];
      return c.transactionId === null ? null : c;
    };
    const beyond = (events, cp) => events.filter((e) => cp === null || core.compareCursor(e, cp) > 0);
    const moveCheckpoint = (client, c) => client.query(`UPDATE "${checkpoint}" SET xid = $2::xid8, seq = $3 WHERE name = $1`, [name, c.transactionId, c.sequence]);
    const target = {
      async load() {
        const c = (await admin.query(`SELECT xid::text AS "transactionId", seq::int AS sequence FROM "${checkpoint}" WHERE name = $1`, [name])).rows[0];
        return c?.transactionId ? c : null;
      },
      async skip(event, error) {
        await inTransaction(async (client) => {
          if (beyond([event], await lockCheckpoint(client)).length === 0) return false; // skipped already
          await moveCheckpoint(client, core.cursorOf([event]));
          await client.query(`INSERT INTO "${dead}" VALUES ($1::xid8, $2, $3, $4, $5)`, [event.transactionId, event.sequence, event.type, event.id, String(error)]);
          return true;
        });
        done(core.cursorOf([event]));
      },
    };
    handler = async (events) => {
      counts.handler++;
      if (counts.handler === 1) t0 = performance.now();
      await inTransaction(async (client) => {
        const fresh = beyond(events, await lockCheckpoint(client));
        if (fresh.length === 0) return false; // all applied already: a redelivery
        await client.query(`INSERT INTO "${rows}" SELECT unnest($1::bigint[]), unnest($2::int[])`, [fresh.map((e) => e.sequence), fresh.map((e) => e.data.n)]);
        if (fresh.some((e) => e.data.poison)) throw new Error("poison event"); // the fold fails while applying: the transaction rolls back
        await moveCheckpoint(client, core.cursorOf(events));
        return true;
      });
      done(core.cursorOf(events));
    };
    options = { store, target, batchSize: PAGE, pollIntervalMs: 1000, onError };
    deadLetters = async () => Number((await admin.query(`SELECT count(*) AS n FROM "${dead}"`)).rows[0].n);
    appliedCount = async () => Number((await admin.query(`SELECT count(*) AS n FROM "${rows}"`)).rows[0].n);
  }
  const store = options.store;
  // the subscriber starts after everything already there: the page is exactly these 500 events
  const before = (await store.query(query, { settledOnly: true, order: "desc", limit: 1, payload: false, version: false })).settledCursor;
  if (kind === "postgres") {
    await admin.query(`DELETE FROM "${T.checkpoint}" WHERE name = $1`, [name]);
    if (before) await admin.query(`INSERT INTO "${T.checkpoint}" VALUES ($1, $2::xid8, $3)`, [name, before.transactionId, before.sequence]);
  } else if (before) {
    await options.cursors.save(name, before);
    tEnd = 0;
  }
  const batch = await store.append(Array.from({ length: PAGE }, (_, i) => work.Job({ n: i, ...(i === at ? { poison: true } : {}) })));
  last = batch.last;
  const s = sub.subscribe(name, query, handler, options);
  const deadline = Date.now() + 30_000;
  while (!s.hasPassed({ transactionId: batch.transactionId, sequence: batch.last }) && Date.now() < deadline) await s.whenCaughtUp();
  await s.stop();
  const ok = s.hasPassed({ transactionId: batch.transactionId, sequence: batch.last });
  return { ms: tEnd - t0, ...counts, applied: await appliedCount(), dead: await deadLetters(), ok, cursorAtEnd: s.cursor?.sequence === batch.last };
}

async function poisonCell() {
  const reps = REPS ?? 11;
  console.log(`## poison: one page of 500 events, one poison event, onError → "skip"; median of ${reps} runs per row (min–max)`);
  console.log(`command: node bench/subscriptions-bench.mjs poison ${reps}`);
  await admin.query(`DROP TABLE IF EXISTS "${T.poison}"`);
  await admin.query(`CREATE TABLE IF NOT EXISTS "${T.rows}" (seq bigint PRIMARY KEY, n int NOT NULL)`);
  await admin.query(`CREATE TABLE IF NOT EXISTS "${T.checkpoint}" (name text PRIMARY KEY, xid xid8, seq bigint)`);
  await admin.query(`CREATE TABLE IF NOT EXISTS "${T.dead}" (xid xid8 NOT NULL, seq bigint NOT NULL, event_type text NOT NULL, event_id text NOT NULL, error text NOT NULL)`);
  const stores = { postgres: new postgres.PostgresStore({ connection: url, schema, table: T.poison, poolSize: 4 }) };
  try {
    await poisonRun("postgres", null, stores); // warm-up: install, connections, plans
    await poisonRun("memory", null, stores);
    console.log("| store, mode | poison at | page committed after, ms | handler calls | onError calls | transactions | events applied | dead letters |");
    console.log("|---|---|---|---|---|---|---|---|");
    for (const kind of ["memory", "postgres"]) {
      for (const at of [null, 0, 250, 499]) {
        const runs = [];
        for (let r = 0; r < reps; r++) runs.push(await poisonRun(kind, at, stores));
        const expectApplied = at === null ? 500 : 499;
        const expectDead = at === null ? 0 : 1;
        const bad = runs.filter((x) => !x.ok || !x.cursorAtEnd || x.applied !== expectApplied || x.dead !== expectDead);
        const one = (k) => {
          const vs = [...new Set(runs.map((x) => x[k]))];
          return vs.length === 1 ? String(vs[0]) : `${Math.min(...vs)}–${Math.max(...vs)}`;
        };
        const label = kind === "memory" ? "memory, default mode (in-process handler)" : "Postgres, processor mode (a transaction per handler call)";
        console.log(`| ${label} | ${at === null ? "none" : at} | ${f(median(runs.map((x) => x.ms)))} (${range(runs.map((x) => x.ms))}) | ${one("handler")} | ${one("onError")} | ${kind === "memory" ? "–" : one("transactions")} | ${one("applied")} of 500 | ${one("dead")} |`);
        if (bad.length > 0) red(`${kind}, poison at ${at}: ${bad.length} run(s) did not apply ${expectApplied}, skip ${expectDead} and end at the page's last cursor`);
      }
    }
  } finally {
    await stores.postgres.close();
  }
  console.log("");
}

async function latencyCell() {
  const reps = REPS ?? 30;
  console.log(`## latency: read-your-writes on an idle database, a write from another store (own pool), time until sub.hasPassed(token); ${reps} writes per row, each after a random pause of 0–600 ms`);
  console.log(`command: node bench/subscriptions-bench.mjs latency ${reps}`);
  console.log("| doorbell | subscriber polls every | write | passed by | median ms | p90 ms | min–max ms | passed |");
  console.log("|---|---|---|---|---|---|---|---|");
  for (const live of [false, true]) {
    const table = live ? T.live : T.poll;
    const reader = new postgres.PostgresStore({ connection: url, schema, table, poolSize: 2, ...(live ? { live: true } : {}) });
    const writer = new postgres.PostgresStore({ connection: url, schema, table, poolSize: 2, ...(live ? { live: true } : {}) });
    await writer.append([work.A({ n: 0 })]); // install (and the doorbell trigger) before the subscriber starts
    const s = sub.subscribe(`bench-rw-${live}`, { types: ["A"] }, () => {}, { store: reader });
    try {
      await s.whenCaughtUp();
      for (const [type, by] of [["B", "watermark"], ["A", "cursor"]]) {
        const ms = [];
        let passed = 0;
        let byWatermark = 0;
        for (let r = 0; r < reps; r++) {
          await new Promise((res) => setTimeout(res, Math.random() * 600));
          const appended = await writer.append([work[type]({ n: r })]);
          const token = { transactionId: appended.transactionId, sequence: appended.last };
          const t0 = performance.now();
          const deadline = t0 + 10_000;
          while (!s.hasPassed(token) && performance.now() < deadline) await new Promise((res) => setImmediate(res));
          ms.push(performance.now() - t0);
          if (s.hasPassed(token)) passed++;
          const c = s.cursor;
          if (c === null || core.compareCursor(c, token) < 0) byWatermark++;
        }
        const seenBy = byWatermark === reps ? "watermark" : byWatermark === 0 ? "cursor" : `mixed (${byWatermark} by watermark)`;
        console.log(`| ${live ? "on (live: true)" : "off"} | ${live ? "5 000 ms (default with a doorbell)" : "500 ms (default without)"} | ${type === "B" ? "not matched (B)" : "matched (A)"} | ${seenBy} | ${f(median(ms))} | ${f(quantile(ms, 0.9))} | ${range(ms)} | ${passed} of ${reps} |`);
        if (passed !== reps) red(`latency, live ${live}, ${type}: ${reps - passed} token(s) not passed within 10 s`);
        if (seenBy !== by) red(`latency, live ${live}, ${type}: passed by ${seenBy}, expected ${by}`);
      }
    } finally {
      await s.stop();
      await reader.close();
      await writer.close();
    }
  }
  console.log("");
}

try {
  await dropTables();
  const pgVersion = (await admin.query("SHOW server_version")).rows[0].server_version;
  console.log(`subscriptions bench — ${new Date().toISOString().slice(0, 10)}, ${cpus()[0]?.model ?? "?"} ×${cpus().length}, ${Math.round(totalmem() / 2 ** 30)} GiB, Node ${process.version}, Postgres ${pgVersion}${BEFORE ? `, before = ${BEFORE}` : ""}\n`);
  if (CELL === "append" || CELL === "all") await appendCell();
  if (CELL === "query" || CELL === "all") await queryCell();
  if (CELL === "poison" || CELL === "all") await poisonCell();
  if (CELL === "latency" || CELL === "all") await latencyCell();
} finally {
  await dropTables().catch(() => undefined);
  await admin.end();
}
console.log(exitCode === 0 ? "subscriptions bench: every check held" : "subscriptions bench: RED (see above)");
process.exit(exitCode);
