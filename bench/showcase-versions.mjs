// The showcase's stage-5 part: versions and rebuild. Old code and new code are separate node processes:
//   1. v1 is built by a process running only v1's code, which stays as v1's writer; events keep coming.
//   2. A process with only v2's code (a bump) starts: v2 builds beside v1 — its get answers at once,
//      its list waits — while v1 keeps answering.
//   3. v2 switches once it has caught up.
//   4. A process with another fold under the SAME version 2 warns (ES_FOLD_CHANGED).
//   5. The old deploy ends; the README recipe retires v1; a straggler with v1's code fails and never rebuilds.
//   6. A live model starts from v2's persisted rows, beside one that rebuilds.
//   7. A persisted fold bump over a context larger than maxEvents loads in pages.
// Every cold read is timed after one warm-up store call in its process (O18). Imported by
// bench/showcase.mjs; its children run this file: node bench/showcase-versions.mjs --child <mode> <connection> …
import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { z } from "zod";
import { createEventStore, defineEvents, defineFold, defineReadModel } from "../dist/index.js";

export const BG_TABLE = "showcase_bg";
const FOLD_TABLE = "showcase_bgf";
const TABLES = [BG_TABLE, `${BG_TABLE}_read_model`, `${BG_TABLE}_read_model_state`, FOLD_TABLE, `${FOLD_TABLE}_fold_state`];
const KEYS = 1_000;
const EVENTS = 20_000;
const FOLD_EVENTS = 12_000;
const MAX_EVENTS = 5_000;
const PAD = "x".repeat(1_000); // ~1 KB per event

const sales = defineEvents({
  OfferMade: { data: z.object({ amount: z.number(), pad: z.string() }), scopes: ["offerId"] },
  OfferRevised: { data: z.object({ amount: z.number(), pad: z.string() }), scopes: ["offerId"] },
});
const ledger = defineEvents({ EntryBooked: { data: z.object({ amount: z.number() }), scopes: ["ledgerId"] } });
const source = sales.$filter();
const initial = { status: "draft", amount: 0, revisions: 0 };
// v1: the amount in euros
const offersV1 = defineReadModel({
  name: "offers",
  version: 1,
  source,
  keyBy: "offerId",
  initial,
  fold: sales.$fold({ OfferMade: (d, s) => ({ ...s, status: "open", amount: d.amount }), OfferRevised: (d, s) => ({ ...s, amount: d.amount, revisions: s.revisions + 1 }) }),
});
// v2, a bump: the amount in cents
const offersV2 = defineReadModel({
  name: "offers",
  version: 2,
  source,
  keyBy: "offerId",
  initial,
  batchSize: 250,
  fold: sales.$fold({ OfferMade: (d, s) => ({ ...s, status: "open", amount: d.amount * 100 }), OfferRevised: (d, s) => ({ ...s, amount: d.amount * 100, revisions: s.revisions + 1 }) }),
});
// the same version 2 with another handler body: a forgotten bump
const offersV2Changed = defineReadModel({
  name: "offers",
  version: 2,
  source,
  keyBy: "offerId",
  initial,
  batchSize: 250,
  fold: sales.$fold({ OfferMade: (d, s) => ({ ...s, status: "open", amount: d.amount * 1000 }), OfferRevised: (d, s) => ({ ...s, amount: d.amount * 1000, revisions: s.revisions + 1 }) }),
});
const balanceV1 = defineFold({ name: "balance", version: 1, fold: ledger.$fold({ EntryBooked: (d, s) => s + d.amount }) });
const balanceV2 = defineFold({ name: "balance", version: 2, fold: ledger.$fold({ EntryBooked: (d, s) => s + 2 * d.amount }) });

const keyOf = (i) => `offer-${i % KEYS}`;
const event = (i) => (i < KEYS ? sales.OfferMade({ amount: (i * 37) % 900, pad: PAD }, { offerId: keyOf(i) }) : sales.OfferRevised({ amount: (i * 53) % 900, pad: PAD }, { offerId: keyOf(i) }));
const keyQuery = (k) => ({ ...source, scopes: { offerId: k } });
const tokenOf = (a) => ({ transactionId: a.transactionId, sequence: a.last });
const fmt = (n) => (n < 10 ? n.toFixed(2) : n.toFixed(0));
const self = fileURLToPath(import.meta.url);
const pgApi = (url, extra = {}) => createEventStore({ events: [sales], connection: url, postgres: { table: BG_TABLE, live: true, persistedModels: true, poolSize: 4 }, ...extra });
const out = (o) => process.stdout.write(JSON.stringify(o) + "\n");
/** One store call before any timed read: the pool connects and the store opens here, untimed (O18). */
const warmUp = async (api) => void (await api.query({ types: ["OfferMade"], scopes: { offerId: "none" } }, { limit: 1 }));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Every key's state from `list`, by key. */
async function allRows(model) {
  const rows = new Map();
  for (let offset = 0; ; offset += 500) {
    const page = await model.list({ offset, limit: 500 });
    for (const row of page.rows) rows.set(row.key, row.state);
    if (page.rows.length < 500) return rows;
  }
}

// ── the children ─────────────────────────────────────────────────────────────
if (process.argv[1] === self && process.argv[2] === "--child") {
  const [mode, url, arg] = process.argv.slice(3);
  if (mode === "v1" || mode === "v2") {
    // one deploy: only this version's code; reports every 250 ms until stdin closes
    const def = mode === "v1" ? offersV1 : offersV2;
    const api = pgApi(url);
    await warmUp(api);
    const opened = Date.now();
    const model = api.persistedReadModel(def);
    let switched = false;
    const report = async () => {
      // list before status: building only ever goes true → false, so a line that says building had a list that waited
      let list;
      try {
        list = `total ${(await model.list({ timeoutMs: 100 })).total}`;
      } catch (error) {
        list = error?.code === "read_model_timeout" ? "waiting (read_model_timeout)" : `error: ${error.message}`;
      }
      const s = await model.status();
      const newest = (await api.query(source, { order: "desc", limit: 1, payload: false, version: false })).events[0];
      const key = keyOf(Math.floor(Math.random() * KEYS));
      // an append to this key may land between the two reads: up to 3 tries before the line says NO
      let got;
      let want;
      for (let i = 0; i < 3 && (got === undefined || !isDeepStrictEqual(got, want.state)); i++) {
        [got, want] = await Promise.all([model.get(key), api.context({ query: keyQuery(key), fold: def.fold, initial })]);
      }
      const line = { v: def.version, ms: Date.now() - opened, phase: s.phase, role: s.role, building: s.building, checkpoint: s.checkpoint?.sequence ?? null, newest: newest?.sequence ?? null, lag: s.lag.events, get: isDeepStrictEqual(got, want.state), list };
      if (!s.building && !switched && mode === "v2") {
        switched = true;
        const top = await model.list({ where: { status: "open" }, sort: { by: "amount", dir: "desc" }, limit: 5 });
        Object.assign(line, { switched: true, top: top.rows.map((r) => [r.key, r.state.amount]), total: top.total });
      }
      out(line);
    };
    const timer = setInterval(() => void report().catch((e) => out({ v: def.version, error: String(e) })), 250);
    process.stdin.resume();
    process.stdin.on("end", async () => {
      clearInterval(timer);
      await api.close();
      process.exit(0);
    });
  } else if (mode === "v2-changed") {
    const api = pgApi(url);
    const model = api.persistedReadModel(offersV2Changed);
    await model.get(keyOf(1));
    await model.status();
    await sleep(100);
    await api.close();
    process.exit(0);
  } else if (mode === "v1-late") {
    const api = pgApi(url);
    const model = api.persistedReadModel(offersV1);
    let answer;
    try {
      answer = `total ${(await model.list()).total}`;
    } catch (error) {
      answer = `${error.name}: ${error.message}`;
    }
    out({ answer, phase: (await model.status()).phase });
    await api.close();
    process.exit(0);
  } else if (mode === "live") {
    const api = pgApi(url);
    await warmUp(api);
    const t0 = performance.now();
    const model = api.readModel(offersV2, arg === "persisted" ? { start: "persisted" } : undefined);
    await model.get(keyOf(3), { timeoutMs: 60_000 });
    const ms = performance.now() - t0;
    const rows = await allRows(model);
    out({ start: arg, ms, folded: model.status.folded, keys: model.status.keys, rows: [...rows].sort(([a], [b]) => (a < b ? -1 : 1)) });
    await api.close();
    process.exit(0);
  } else if (mode === "fold") {
    const loads = [];
    const api = createEventStore({
      events: [ledger],
      connection: url,
      postgres: { table: FOLD_TABLE, persistedFolds: true, poolSize: 2 },
      contextCache: { maxEvents: MAX_EVENTS, onLoad: (l) => void loads.push(l) },
    });
    await api.query(ledger.$filter(), { limit: 1 }); // warm-up (O18)
    let result;
    try {
      const loaded = await api.context({ query: ledger.$scope("ledgerId", "L1"), fold: arg === "2" ? balanceV2 : balanceV1, initial: 0 });
      const l = loads.at(-1);
      result = { state: loaded.state, start: l.start, events: l.events, readMs: l.readMs, foldMs: l.foldMs, written: l.stored?.written ?? false, writeMs: l.stored?.writeMs ?? 0, error: l.stored?.error ? String(l.stored.error) : null };
    } catch (error) {
      result = { thrown: `${error.name}: ${error.message}` };
    }
    out(result);
    await api.close();
    process.exit(0);
  }
}

/** A child of this file; its JSON lines go to `onLine`, its stderr to `onErr`. */
function child(mode, url, arg, onLine, onErr) {
  const proc = spawn(process.execPath, [self, "--child", mode, url, ...(arg !== undefined ? [arg] : [])], { stdio: ["pipe", "pipe", "pipe"] });
  createInterface({ input: proc.stdout }).on("line", (line) => {
    if (line.startsWith("{")) onLine(JSON.parse(line));
  });
  createInterface({ input: proc.stderr }).on("line", (line) => onErr?.(line));
  proc.done = new Promise((resolve) => proc.on("exit", resolve));
  return proc;
}

/** A child that prints one line and exits: that line, and its stderr lines. */
async function once(mode, url, arg) {
  let result;
  const errs = [];
  const proc = child(mode, url, arg, (l) => (result = l), (l) => errs.push(l));
  proc.stdin.end();
  await proc.done;
  return { result, errs };
}

/** The README's retire recipe, as printed, for `table`, tenant '' (no tenant view). */
async function retire(admin, table, name, newVersion) {
  const readme = readFileSync(new URL("../README.md", import.meta.url), "utf8");
  const sql = /```sql\n([\s\S]*?)```/.exec(readme.slice(readme.indexOf("**Retiring an old version.**")))[1];
  const statements = sql
    .replace(/events_(read_model_state|read_model)\b/g, (_, rest) => `"${table}_${rest}"`)
    .split("\n")
    .filter((l) => !l.trim().startsWith("--"))
    .join("\n")
    .split(/;\s*\n/)
    .map((s) => s.trim().replace(/;$/, ""))
    .filter(Boolean);
  const client = await admin.connect();
  try {
    for (const statement of statements) {
      const used = Math.max(0, ...[...statement.matchAll(/\$(\d+)/g)].map((m) => Number(m[1])));
      await client.query(statement, ["", name, newVersion].slice(0, used));
    }
  } finally {
    client.release();
  }
}

const rowsOf = async (admin, version) => (await admin.query(`SELECT count(*)::int AS n FROM "${BG_TABLE}_read_model_state" WHERE name = 'offers' AND version = $1`, [version])).rows[0].n;

/** The stage-5 part of the showcase; `admin` drops the tables. Returns whether every expectation held. */
export async function stage5(url, admin) {
  console.log("@orgops/eventstore showcase — stage 5: versions and rebuild");
  console.log("Old code and new code run as separate node processes over one Postgres table; every cold read is timed after one warm-up store call in its process.\n");
  const dropAll = () => admin.query(`DROP TABLE IF EXISTS ${TABLES.map((t) => `"${t}"`).join(", ")} CASCADE`);
  await dropAll();
  const procs = [];
  const fails = [];
  const expect = (ok, what) => {
    if (!ok) fails.push(what);
    return ok;
  };
  let streaming = false;
  let stream;
  try {
    // ── 1. v1 built by the old deploy, which keeps running as its writer ──
    const here = pgApi(url);
    for (let i = 0; i < EVENTS; i += 500) await here.append(Array.from({ length: 500 }, (_, j) => event(i + j)));
    await admin.query(`VACUUM ANALYZE "${BG_TABLE}"`);
    const v1Lines = [];
    const v1 = child("v1", url, undefined, (l) => v1Lines.push(l));
    procs.push(v1);
    const v1Start = Date.now();
    while (!v1Lines.some((l) => l.list === `total ${KEYS}` && l.role === "writer") && Date.now() - v1Start < 60_000) await sleep(50);
    console.log(`1. v1 built by process A (only v1's code): ${EVENTS} events × ~1 KB on ${KEYS} offers, v1.list: ${v1Lines.at(-1)?.list ?? "—"}, role ${v1Lines.at(-1)?.role}; A stays v1's writer`);
    streaming = true;
    let appended = 0;
    stream = (async () => {
      while (streaming) {
        await here.append([event(EVENTS + appended++)]);
        await sleep(10);
      }
    })();
    console.log("   from now on one event is appended every 10 ms\n");

    // ── 2. the bump: process B with only v2's code ──
    console.log("2. the bump: process B starts with ONLY v2's definition (same name, version 2, amounts in cents)");
    console.log("   ms     building  v2 checkpoint / newest  v2.get = context  v2.list                         | process A: v1.list");
    const v2Lines = [];
    const v2 = child("v2", url, undefined, (l) => {
      v2Lines.push(l);
      if (v2Lines.some((x) => x.switched && x !== l)) return; // printed up to the switch
      const a = v1Lines.at(-1);
      if (l.error) return console.log(`   ${l.error}`);
      console.log(
        `   ${String(l.ms).padEnd(6)} ${String(l.building).padEnd(9)} ${`${l.checkpoint ?? "—"} / ${l.newest}`.padEnd(23)} ${(l.get ? "yes" : "NO").padEnd(17)} ${l.list.padEnd(31)} | ${a?.list ?? "—"}, answers: ${a?.list?.startsWith("total") ? "yes" : "NO"}`,
      );
    });
    procs.push(v2);
    const v2Start = Date.now();
    while (!v2Lines.some((l) => l.switched) && Date.now() - v2Start < 60_000) await sleep(50);
    const sw = v2Lines.find((l) => l.switched);
    const during = v2Lines.filter((l) => l.building === true);
    expect(sw !== undefined, "v2 switched within 60 s");
    expect(during.every((l) => l.get), "v2.get = context while building");
    expect(during.every((l) => l.list.startsWith("waiting")), "v2.list waits while building");
    expect(v1Lines.filter((l) => l.list).every((l) => l.list === `total ${KEYS}`), "v1 answers list throughout");

    // ── 3. the switch ──
    if (sw) {
      console.log(`\n3. the switch: building → false ${sw.ms} ms after process B opened v2 (lag at that report: ${sw.lag} events); nothing happened to v1`);
      console.log(`   v2.list({ where: { status: "open" }, sort: amount desc, limit: 5 }) in process B: total ${sw.total}`);
      for (const [k, amount] of sw.top) console.log(`     ${k.padEnd(10)} amount ${amount} (cents)`);
    }
    streaming = false;
    await stream;
    stream = undefined;
    // every key, checked here once the stream stopped: v2's rows as of the last append against es.context() with v2's fold
    const last = await here.append([event(EVENTS + appended++)]);
    const check = pgApi(url);
    const v2Here = check.persistedReadModel(offersV2);
    await v2Here.list({ atLeast: tokenOf(last), timeoutMs: 30_000 });
    const rows = await allRows(v2Here);
    let everyKey = rows.size === KEYS;
    for (const [k, state] of rows) if (!isDeepStrictEqual(state, (await check.context({ query: keyQuery(k), fold: offersV2.fold, initial })).state)) everyKey = false;
    await check.close();
    console.log(`   every one of the ${KEYS} keys of v2 (list as of the last append) equals es.context() with v2's fold: ${everyKey ? "yes" : "NO"}`);
    expect(everyKey, "every v2 key equals context after the switch");
    const afterSwitch = v1Lines.at(-1);
    console.log(`   process A still answers from v1 after the switch: ${afterSwitch?.list} (role ${afterSwitch?.role})\n`);

    // ── 4. a forgotten bump ──
    const changed = await once("v2-changed", url);
    const warning = changed.errs.find((l) => l.includes("ES_FOLD_CHANGED"));
    console.log("4. a forgotten bump: process C opens version 2 with another handler body (amounts × 1000), same version number");
    console.log(`   its stderr: ${warning ? warning.slice(0, 160) + "…" : "(no warning)"}`);
    expect(warning !== undefined, "the fingerprint warning");
    console.log("   it only warns: C read v2's rows as they are\n");

    // ── 5. retire ──
    v1.stdin.end();
    await v1.done;
    const before = await rowsOf(admin, 1);
    const r0 = performance.now();
    await retire(admin, BG_TABLE, "offers", 2);
    const retireMs = performance.now() - r0;
    const afterRetire = await rowsOf(admin, 1);
    console.log(`5. retire: process A (the old deploy) ended; the README recipe ran in ${fmt(retireMs)} ms: v1 state rows ${before} → ${afterRetire}`);
    const late = await once("v1-late", url);
    console.log(`   a straggler with v1's code (process D): list → ${late.result.answer.slice(0, 150)}…  (phase ${late.result.phase})`);
    for (let i = 0; i < 6; i++) {
      await here.append([event(EVENTS + appended++)]);
      await sleep(500);
    }
    const after3s = await rowsOf(admin, 1);
    console.log(`   v1 rows after 3 s with appends: ${after3s} (not rebuilt)\n`);
    expect(afterRetire === 0 && after3s === 0, "v1 rows gone and not rebuilt");
    expect(/was retired/.test(late.result.answer) && late.result.phase === "failed", "the straggler fails with the retirement error");

    // ── 6. a live model from the persisted state ──
    const fromRows = await once("live", url, "persisted");
    const rebuilt = await once("live", url, "rebuild");
    console.log("6. a live model (RAM only) of v2 in a fresh process:");
    console.log(`   start: "persisted"  first get after ${fmt(fromRows.result.ms)} ms, events folded ${fromRows.result.folded} (only those beyond the rows' checkpoint), keys ${fromRows.result.keys}`);
    console.log(`   start: "rebuild"    first get after ${fmt(rebuilt.result.ms)} ms, events folded ${rebuilt.result.folded} (the whole source), keys ${rebuilt.result.keys}`);
    const same = isDeepStrictEqual(fromRows.result.rows, rebuilt.result.rows);
    console.log(`   same answer for every key: ${same ? "yes" : "NO"}\n`);
    expect(same, "the live start's states equal the rebuild's");
    expect(fromRows.result.folded < rebuilt.result.folded, "the live start folds only the delta");
    v2.stdin.end();
    await v2.done;

    // ── 7. a persisted fold bump over a context larger than maxEvents ──
    const ledgerApi = createEventStore({ events: [ledger], connection: url, postgres: { table: FOLD_TABLE, persistedFolds: true, poolSize: 2 } });
    for (let i = 0; i < FOLD_EVENTS; i += 1_000) await ledgerApi.append(Array.from({ length: 1_000 }, (_, j) => ledger.EntryBooked({ amount: ((i + j) % 9) + 1 }, { ledgerId: "L1" })));
    await ledgerApi.close();
    await admin.query(`VACUUM ANALYZE "${FOLD_TABLE}"`);
    console.log(`7. a persisted fold bump: one context of ${FOLD_EVENTS} events, contextCache.maxEvents ${MAX_EVENTS}, each line a fresh process:`);
    const f1 = (await once("fold", url, "1")).result;
    const f2 = (await once("fold", url, "2")).result;
    const f3 = (await once("fold", url, "2")).result;
    const show = (label, f) =>
      console.log(f.thrown ? `   ${label} ${f.thrown}` : `   ${label} start ${f.start.padEnd(7)} events ${String(f.events).padEnd(6)} readMs ${fmt(f.readMs).padEnd(6)} foldMs ${fmt(f.foldMs).padEnd(6)} written ${f.written}  state ${f.state}`);
    show("v1 (first load): ", f1);
    show("v2 (the bump):   ", f2);
    show("v2 again:        ", f3);
    console.log(`   no ContextTooLargeError: ${!f1.thrown && !f2.thrown && !f3.thrown ? "yes — the delta was folded in pages of " + MAX_EVENTS + ", written after each" : "NO"}\n`);
    expect(!f1.thrown && !f2.thrown && !f3.thrown, "no ContextTooLargeError");
    expect(f2.start === "initial" && f2.events === FOLD_EVENTS && f2.written && f3.start === "stored" && f3.events === 0 && f3.state === f2.state, "v2 built in pages, then stored");
    await here.close();
  } finally {
    streaming = false;
    await stream?.catch(() => undefined);
    for (const p of procs) if (p.exitCode === null) p.kill("SIGKILL");
    await dropAll();
  }
  const all = fails.length === 0;
  console.log(`stage 5 expectations (v2 get = context while building, list waits, switch, every key equal, warning, v1 retired and not rebuilt, live start = rebuild, paged fold): ${all ? "all met" : `NOT MET — ${fails.join("; ")}`}\n`);
  return all;
}
