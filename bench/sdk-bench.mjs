// Throughput of the SDK against a local Postgres. Build first: `npm run build`.
// Usage: ES_BENCH_DATABASE_URL=postgres://es:es@localhost:5432/es_sdk_test node bench/sdk-bench.mjs
import { z } from "zod";
import pg from "pg";
import { defineEvents, buildSchema, MemoryStore } from "../dist/index.js";
import { PostgresStore } from "../dist/postgres/index.js";

const url = process.env.ES_BENCH_DATABASE_URL ?? "postgres://es:es@localhost:5432/es_sdk_test";
const TABLE = "bench_events";
const ACCOUNTS = 5000;
const SEED = 200_000;

const accounts = defineEvents({
  AccountOpened: { data: z.object({ owner: z.string() }) },
  MoneyDeposited: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"] },
  MoneyWithdrawn: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"] },
});
const schema = buildSchema([accounts], { strict: true });

const admin = new pg.Pool({ connectionString: url });
await admin.query(`DROP TABLE IF EXISTS "${TABLE}"`);
const store = new PostgresStore({ connection: url, schema, table: TABLE, poolSize: 32 });
await store.append([accounts.AccountOpened({ owner: "warmup" })]);

// ── seed ───────────────────────────────────────────────────────────────────────
const accountIds = [];
console.log(`seeding ${SEED} events across ${ACCOUNTS} accounts …`);
let t0 = Date.now();
for (let i = 0; i < ACCOUNTS; i += 250) {
  const batch = Array.from({ length: Math.min(250, ACCOUNTS - i) }, (_, j) => accounts.AccountOpened({ owner: `acc-${i + j}` }));
  const r = await store.append(batch);
  const opened = await store.query({ types: ["AccountOpened"], where: batch.map((b) => ({ owner: b.data.owner })) });
  for (const e of opened.events) accountIds.push(e.id);
  void r;
}
for (let i = 0; i < SEED; i += 500) {
  const batch = Array.from({ length: 500 }, (_, j) => accounts.MoneyDeposited({ amount: ((i + j) % 97) + 1 }, { accountOpenedId: accountIds[(i + j) % ACCOUNTS] }));
  await store.append(batch);
}
await admin.query(`ANALYZE "${TABLE}"`);
console.log(`seeded in ${((Date.now() - t0) / 1000).toFixed(1)}s (${Math.round(SEED / ((Date.now() - t0) / 1000))} events/s incl. queries)\n`);

async function run(name, concurrency, seconds, fn) {
  let ops = 0;
  let conflicts = 0;
  const end = Date.now() + seconds * 1000;
  const workers = Array.from({ length: concurrency }, async (_, w) => {
    let i = w;
    while (Date.now() < end) {
      const r = await fn(i);
      i += concurrency; // disjoint account sequences per worker: conflicts only when the hash collides
      ops++;
      if (r === "conflict") conflicts++;
    }
  });
  const start = Date.now();
  await Promise.all(workers);
  const secs = (Date.now() - start) / 1000;
  console.log(`${name.padEnd(46)} c=${String(concurrency).padStart(2)}  ${Math.round(ops / secs).toString().padStart(6)} ops/s   avg ${((secs * 1000 * concurrency) / ops).toFixed(2)} ms${conflicts ? `   conflicts ${conflicts}` : ""}`);
}

const pick = (i) => accountIds[(i * 7919) % ACCOUNTS];

for (const c of [1, 16]) {
  await run("context read (~40 events, scope index)", c, 6, async (i) => {
    await store.query(accounts.$scope("accountOpenedId", pick(i)));
  });
  await run("command: read → decide → appendIf (guarded)", c, 6, async (i) => {
    const id = pick(i);
    const q = accounts.$scope("accountOpenedId", id);
    const read = await store.query(q);
    const out = await store.appendIf([accounts.MoneyWithdrawn({ amount: 1 }, { accountOpenedId: id })], { query: q, version: read.contextVersion });
    return out.ok ? "ok" : "conflict";
  });
  await run("plain append, 1 event", c, 6, async (i) => {
    await store.append([accounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: pick(i) })]);
  });
}
await run("plain append, 50-event batches", 1, 6, async (i) => {
  await store.append(Array.from({ length: 50 }, (_, j) => accounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: pick(i * 50 + j) })));
});
await run("plain append, 50-event batches", 4, 6, async (i) => {
  await store.append(Array.from({ length: 50 }, (_, j) => accounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: pick(i * 50 + j) })));
});

// ── race: 16 writers, same context, same expected version ─────────────────────
{
  const id = pick(1);
  const q = accounts.$scope("accountOpenedId", id);
  const read = await store.query(q);
  const results = await Promise.all(
    Array.from({ length: 16 }, () => store.appendIf([accounts.MoneyWithdrawn({ amount: 80 }, { accountOpenedId: id })], { query: q, version: read.contextVersion })),
  );
  console.log(`\nrace: 16 concurrent appendIf with the same context version → ${results.filter((r) => r.ok).length} committed (must be 1)`);
}

const { rows } = await admin.query(`SELECT count(*)::int AS n, pg_size_pretty(pg_relation_size('${TABLE}')) AS tbl, pg_size_pretty(pg_indexes_size('${TABLE}')) AS idx FROM "${TABLE}"`);
console.log(`table: ${rows[0].n} events, ${rows[0].tbl} data, ${rows[0].idx} indexes`);

const mem = new MemoryStore({ schema });
const memId = (await mem.append([accounts.AccountOpened({ owner: "m" })])).first;
const memAcc = (await mem.query({ types: ["AccountOpened"] })).events[0].id;
void memId;
t0 = Date.now();
let n = 0;
while (Date.now() - t0 < 2000) {
  const q = accounts.$scope("accountOpenedId", memAcc);
  const r = await mem.query(q);
  await mem.appendIf([accounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: memAcc })], { query: q, version: r.contextVersion });
  n++;
}
console.log(`memory store reference: ${Math.round(n / 2)} guarded commands/s on one growing context (${n} events)`);

await store.close();
await admin.end();
