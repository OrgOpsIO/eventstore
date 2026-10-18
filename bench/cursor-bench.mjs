// The processor's cursor read when only 1 % of the events match (synthesis §7, research 04 §4.4 and §6.2; decision
// 56), on the local Postgres test database. Build first, or run `sh bench/cursor.sh`, which builds and then runs this.
//   node bench/cursor-bench.mjs <events> [reps=3]
// One table of its own (bench_cursor_<events>), dropped at the start and the end. N events, every 100th `Rare`, the
// rest `Common`, ~1 KB each, in transactions of 1 000 events (so transaction ids vary), then VACUUM ANALYZE.
// Seeding is SQL in the store's wire shape (`{ ...data, <idKey>: id, scopes }`; 10⁶ in seconds instead of minutes
// through append); before anything is measured the events are read back through the library: statistics() counts N
// and one page through es.query() parses.
// Three index layouts on the same table:
//   A  the store as the library installs it by default: (transaction_id, sequence_number), no type index
//   B  postgres: { typeIndex: true }: the library adds (event_type, sequence_number)
//   C  (event_type, transaction_id, sequence_number) instead of B — NOT IN THE LIBRARY, created by this bench's own SQL
// Per layout: EXPLAIN (ANALYZE, BUFFERS) of the page a subscriber sends for the 1 % source (compileQuery with the
// options of src/subscribe/index.ts: settledOnly, cursor, limit, no version) at the first page, half-way and the empty
// page at the end, printed in full; then the catch-up of the library's subscribe on the 1 % source from the
// beginning, batchSize 500 and 5 000, median of `reps`. Reference: the 100 % source on layout A (the read side of a
// rebuild). Checks print ok / FAIL: every subscription delivered exactly the matching events.
import { cpus, totalmem } from "node:os";
import pg from "pg";
import { z } from "zod";
import { buildSchema, createEventStore, defineEvents } from "../dist/index.js";
import { compileQuery } from "../dist/postgres/index.js";

const url = process.env.ES_BENCH_DATABASE_URL ?? "postgres://es:es@localhost:5432/es_sdk_test";
const BENCH_TIMEOUTS = { statementMs: 600_000 }; // as the matrix benches (decision 54); the library default is 30 000
const N = Number(process.argv[2] ?? 100_000);
const REPS = Number(process.argv[3] ?? 3);
const TABLE = `bench_cursor_${N}`;
const C_INDEX = `bench_cursor_${N}_type_xid_seq`;

const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length === 0 ? NaN : s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const f = (n) => (n === undefined || Number.isNaN(n) ? "–" : n < 10 ? n.toFixed(2) : n < 100 ? n.toFixed(1) : n.toFixed(0));
let failed = 0;
const check = (name, ok, seen) => {
  if (!ok) failed++;
  console.log(`${ok ? "ok  " : "FAIL"} ${name} — ${seen}`);
};

const events = defineEvents({
  Common: { data: z.object({ n: z.number(), note: z.string() }), scopes: ["ledgerId"] },
  Rare: { data: z.object({ n: z.number(), note: z.string() }), scopes: ["ledgerId"] },
});
const schema = buildSchema([events], { strict: true });
const rare = events.$filter({ types: ["Rare"] });
const all = events.$filter();
const apiFor = (typeIndex) => createEventStore({ events: [events], connection: url, postgres: { table: TABLE, poolSize: 4, typeIndex, timeouts: BENCH_TIMEOUTS } });

const admin = new pg.Client({ connectionString: url });
await admin.connect();
const sql = (text, params) => admin.query(text, params);
const drop = () => sql(`DROP TABLE IF EXISTS "${TABLE}" CASCADE`);

async function seed() {
  for (let from = 1; from <= N; from += 1000) {
    const to = Math.min(N, from + 999);
    await sql(
      `INSERT INTO "${TABLE}" (event_type, payload)
       SELECT CASE WHEN i % 100 = 0 THEN 'Rare' ELSE 'Common' END,
              jsonb_build_object(CASE WHEN i % 100 = 0 THEN 'rareId' ELSE 'commonId' END, gen_random_uuid()::text,
                'n', i, 'note', substr(repeat(md5(i::text) || md5((i * 7)::text), 15), 1, 900),
                'scopes', jsonb_build_object('ledgerId', 'l' || (i % 10)))
       FROM generate_series($1::int, $2::int) AS i`,
      [from, to],
    );
  }
}

async function explain(label, query, cursor, limit) {
  const q = compileQuery({ schemaName: "public", table: TABLE }, query, { settledOnly: true, cursor, limit, version: false }, schema);
  const plan = (await sql(`EXPLAIN (ANALYZE, BUFFERS) ${q.sql}`, q.params)).rows.map((x) => x["QUERY PLAN"]);
  const exec = Number(plan.find((l) => l.startsWith("Execution Time"))?.match(/([\d.]+) ms/)?.[1]);
  const removed = plan.map((l) => Number(l.match(/Rows Removed by Filter: (\d+)/)?.[1] ?? 0)).reduce((a, b) => a + b, 0);
  const hit = plan.map((l) => l.match(/shared hit=(\d+)(?: read=(\d+))?/)).find(Boolean);
  const buffers = hit ? Number(hit[1]) + Number(hit[2] ?? 0) : NaN;
  const node = (plan.find((l) => /Scan/.test(l)) ?? "?").trim().replace(/^->\s*/, "").replace(/\s+\(cost=.*$/, "").replace(new RegExp(`"?${TABLE}"?`, "g"), "t");
  console.log(`--- ${label}: execution ${f(exec)} ms, rows removed by filter ${removed}, buffers ${buffers}`);
  for (const line of plan) console.log(`    ${line}`);
  return { exec, removed, buffers, node };
}

async function catchUp(api, query, batchSize) {
  const xs = [];
  let n = 0;
  for (let i = 0; i < REPS; i++) {
    n = 0;
    const t0 = performance.now();
    const sub = await api.subscribe(`bench-cursor-${batchSize}-${i}-${Date.now()}`, query, (batch) => void (n += batch.length), { from: "beginning", batchSize, pollIntervalMs: 60_000 });
    await sub.whenCaughtUp();
    xs.push(performance.now() - t0);
    await sub.stop();
  }
  return { ms: median(xs), min: Math.min(...xs), max: Math.max(...xs), n };
}

try {
  await drop();
  const pgVersion = (await sql("SHOW server_version")).rows[0].server_version;
  console.log(`cursor read at 1 % — ${N.toLocaleString("en")} events × ~1 KB, every 100th Rare; ${REPS} catch-ups per cell`);
  console.log(`${cpus()[0]?.model ?? "?"}, ${cpus().length} cores, ${Math.round(totalmem() / 2 ** 30)} GiB, ${process.platform} ${process.arch}, Node ${process.version}, Postgres ${pgVersion}; ${new Date().toISOString()}`);
  console.log(`command: node bench/cursor-bench.mjs ${N} ${REPS}\n`);

  // layout A: the library's default install
  const apiA = apiFor(false);
  await apiA.store();
  const t0 = performance.now();
  await seed();
  const seedS = (performance.now() - t0) / 1000;
  await sql(`VACUUM ANALYZE "${TABLE}"`);
  const size = (await sql(`SELECT pg_size_pretty(pg_total_relation_size('"${TABLE}"')) AS s, (SELECT count(*)::int FROM "${TABLE}" WHERE event_type = 'Rare') AS rare`)).rows[0];
  console.log(`seeded by SQL in the store's wire shape in ${f(seedS)} s (${size.rare} Rare = ${((100 * size.rare) / N).toFixed(1)} %); table + indexes ${size.s}; VACUUM ANALYZEd`);
  // read back through the library before measuring anything
  const counted = (await apiA.statistics()).reduce((a, s) => a + s.count, 0);
  check("read back through the library: statistics() counts every seeded event", counted === N, `${counted} of ${N}`);
  const page = await apiA.query(rare, { limit: 3 });
  check("read back through the library: a page of the 1 % source parses", page.events.length === 3 && page.events.every((e) => e.type === "Rare" && typeof e.data.n === "number"), page.events.map((e) => `${e.type} n=${e.data.n}`).join(", "));

  const at = async (where) => {
    const r = (await sql(`SELECT transaction_id::text AS x, sequence_number AS s FROM "${TABLE}" ${where}`)).rows[0];
    return { transactionId: r.x, sequence: Number(r.s) };
  };
  const half = await at(`WHERE sequence_number = ${Math.floor(N / 2)}`);
  const end = await at("ORDER BY transaction_id DESC, sequence_number DESC LIMIT 1");
  const indexes = async () => (await sql("SELECT indexname FROM pg_indexes WHERE tablename = $1 ORDER BY 1", [TABLE])).rows.map((r) => r.indexname);

  const rows = [];
  async function layout(name, api) {
    await sql(`ANALYZE "${TABLE}"`);
    console.log(`\n## layout ${name}\nindexes: ${(await indexes()).join(", ")}`);
    const first = await explain("1 % source, first page (cursor null, limit 500)", rare, null, 500);
    const mid = await explain("1 % source, page half-way (limit 500)", rare, half, 500);
    const last = await explain("1 % source, the empty page at the end (limit 500)", rare, end, 500);
    const c500 = await catchUp(api, rare, 500);
    const c5000 = await catchUp(api, rare, 5000);
    console.log(`catch-up of the 1 % source from the beginning: batchSize 500 ${f(c500.ms)} ms (${f(c500.min)}–${f(c500.max)}), batchSize 5 000 ${f(c5000.ms)} ms (${f(c5000.min)}–${f(c5000.max)})`);
    check(`${name}: the 1 % subscription delivered every Rare event, and nothing else`, c500.n === size.rare && c5000.n === size.rare, `${c500.n}, ${c5000.n} of ${size.rare}`);
    rows.push({ name, first, mid, last, c500, c5000 });
  }

  await layout("A default (the library's install: transaction_id, sequence_number)", apiA);
  // the 100 % source on layout A: what a full rebuild reads
  const allFirst = await explain("reference: 100 % source, first page (limit 500), layout A", all, null, 500);
  const a500 = await catchUp(apiA, all, 500);
  const a5000 = await catchUp(apiA, all, 5000);
  check("reference: the 100 % subscription delivered every event", a500.n === N && a5000.n === N, `${a500.n}, ${a5000.n} of ${N}`);
  await apiA.close();

  // layout B: a store opened with typeIndex: true installs (event_type, sequence_number)
  const apiB = apiFor(true);
  await apiB.store();
  check("layout B: the library installed its type index", (await indexes()).some((i) => /type.*seq/.test(i)), (await indexes()).join(", "));
  await layout("B postgres.typeIndex: true (the library adds event_type, sequence_number)", apiB);
  await apiB.close();

  // layout C: NOT in the library — this bench's own index, instead of B's
  const typeIdx = (await indexes()).filter((i) => /type.*seq/.test(i));
  for (const i of typeIdx) await sql(`DROP INDEX "${i}"`);
  await sql(`CREATE INDEX "${C_INDEX}" ON "${TABLE}" (event_type, transaction_id, sequence_number)`);
  const apiC = apiFor(false);
  await apiC.store();
  await layout("C (event_type, transaction_id, sequence_number) instead of B — not in the library", apiC);
  await apiC.close();

  console.log(`\n| ${N.toLocaleString("en")} events, 1 % match | first page (500): plan node, ms, rows removed by filter | half-way page: ms, removed | empty end page: ms | catch-up, batchSize 500, ms median (min–max) | catch-up, batchSize 5 000, ms median (min–max) | vs. A (batchSize 5 000) |`);
  console.log("|---|---|---|---|---|---|---|");
  const aRef = rows[0].c5000.ms;
  for (const r of rows) {
    console.log(
      `| ${r.name} | ${r.first.node}, ${f(r.first.exec)}, ${r.first.removed} | ${f(r.mid.exec)}, ${r.mid.removed} | ${f(r.last.exec)} | **${f(r.c500.ms)}** (${f(r.c500.min)}–${f(r.c500.max)}) | **${f(r.c5000.ms)}** (${f(r.c5000.min)}–${f(r.c5000.max)}) | ${(aRef / r.c5000.ms).toFixed(2)}× |`,
    );
  }
  console.log(`| reference: 100 % source, layout A | ${allFirst.node}, ${f(allFirst.exec)}, ${allFirst.removed} | | | **${f(a500.ms)}** (${f(a500.min)}–${f(a500.max)}) | **${f(a5000.ms)}** (${f(a5000.min)}–${f(a5000.max)}) | ${Math.round(N / (a5000.ms / 1000)).toLocaleString("en")} events/s |`);
  console.log(`\nchecks: ${failed === 0 ? "all ok" : `${failed} FAILED`}`);
} finally {
  await drop();
  await admin.end();
}
process.exitCode = failed === 0 ? 0 : 1;
