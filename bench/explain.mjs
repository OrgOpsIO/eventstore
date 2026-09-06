import { z } from "zod";
import pg from "pg";
import { defineEvents, buildSchema } from "../dist/index.js";
import { compileQuery, compileFilters } from "../dist/postgres/index.js";
const accounts = defineEvents({
  AccountOpened: { data: z.object({ owner: z.string() }) },
  MoneyDeposited: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"] },
  MoneyWithdrawn: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"] },
});
const pool = new pg.Pool({ connectionString: process.env.ES_BENCH_DATABASE_URL ?? "postgres://es:es@localhost:5432/es_sdk_test" });
const { rows: [acc] } = await pool.query(`SELECT payload->>'accountOpenedId' AS id FROM bench_events WHERE event_type='AccountOpened' OFFSET 77 LIMIT 1`);
const q = accounts.$scope("accountOpenedId", acc.id);
const c = compileQuery("bench_events", q, {});
console.log("--- READ SQL ---\n" + (c.text ?? c.sql));
const keys = Object.keys(c); console.log("keys:", keys);
const params = c.params ?? c.values;
const r = await pool.query("EXPLAIN (ANALYZE, BUFFERS) " + (c.text ?? c.sql), params);
for (const row of r.rows) console.log(row["QUERY PLAN"]);
const f = compileFilters(q);
console.log("--- GUARD WHERE ---\n" + (f.where ?? f.sql ?? JSON.stringify(f)));
const r2 = await pool.query(`EXPLAIN (ANALYZE, BUFFERS) SELECT COALESCE(MAX(sequence_number),0) FROM bench_events WHERE ${f.where ?? f.sql}`, [f.texts, f.jsons]);
for (const row of r2.rows) console.log(row["QUERY PLAN"]);
await pool.end();
