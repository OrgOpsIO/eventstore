// The showcase's stage-4 part: a persisted read model. Process 1 builds it and ends; 50 events come in
// while no writer runs; process 2 resumes at the stored checkpoint and folds only those 50. Then a cold
// and a warm get beside es.context(), a list, and a watcher in this process (a standby) receiving the
// state process 2 folded. Imported by bench/showcase.mjs; its children run this file:
//   node bench/showcase-persisted.mjs --writer <connection>     (process 1)
//   node bench/showcase-persisted.mjs --resume <connection>     (process 2; then "append" / "exit" on stdin)
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { z } from "zod";
import { buildSchema, createEventStore, defineEvents, defineReadModel, MemoryStore } from "../dist/index.js";

export const PM_TABLE = "showcase_pm";
const PM_TABLES = [PM_TABLE, `${PM_TABLE}_read_model`, `${PM_TABLE}_read_model_state`];
const OFFERS = 100;
const EVENTS = 1_000;
const BETWEEN = 50;
const READS = 1_000;

const sales = defineEvents({
  OfferMade: { data: z.object({ amount: z.number() }), scopes: ["offerId"] },
  OfferRevised: { data: z.object({ amount: z.number() }), scopes: ["offerId"] },
  OfferAccepted: { data: z.object({}), scopes: ["offerId"] },
});
const source = sales.$filter();
const initial = { status: "draft", amount: 0, revisions: 0 };
const fold = sales.$fold({
  OfferMade: (d, s) => ({ ...s, status: "open", amount: d.amount }),
  OfferRevised: (d, s) => ({ ...s, amount: d.amount, revisions: s.revisions + 1 }),
  OfferAccepted: (_, s) => ({ ...s, status: "accepted" }),
});
const offers = defineReadModel({ name: "offers", version: 1, source, keyBy: "offerId", fold, initial });
const keyQuery = (k) => ({ ...source, scopes: { offerId: k } });
const tokenOf = (a) => ({ transactionId: a.transactionId, sequence: a.last });
const show = (c) => (c ? `(${c.transactionId}, ${c.sequence})` : "null");
const fmt = (n) => n.toFixed(n < 0.1 ? 4 : n < 10 ? 2 : 1);
const median = (xs) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];

const event = (i) => {
  const offerId = `offer-${i % OFFERS}`;
  const round = Math.floor(i / OFFERS);
  if (round === 0) return sales.OfferMade({ amount: ((i * 37) % 900) + 100 }, { offerId });
  if (round === 9 && i % 3 === 0) return sales.OfferAccepted({}, { offerId });
  return sales.OfferRevised({ amount: ((i * 53) % 900) + 100 }, { offerId });
};
/** The 50 events in between: 10 revisions each for offer-0 … offer-4. */
const between = (j) => sales.OfferRevised({ amount: 5_000 + j }, { offerId: `offer-${j % 5}` });

const self = fileURLToPath(import.meta.url);
const pgApi = (url, extra = {}) => createEventStore({ events: [sales], connection: url, postgres: { table: PM_TABLE, live: true, persistedModels: true, poolSize: 3 }, ...extra });

// ── process 1: build the model, then end ─────────────────────────────────────
if (process.argv[1] === self && process.argv[2] === "--writer") {
  const api = pgApi(process.argv[3]);
  let last;
  for (let i = 0; i < EVENTS; i += 250) last = await api.append(Array.from({ length: 250 }, (_, j) => event(i + j)));
  const model = api.persistedReadModel(offers);
  const t0 = performance.now();
  await model.list({ atLeast: tokenOf(last) });
  const ms = performance.now() - t0;
  const s = await model.status();
  await api.close();
  process.stdout.write(JSON.stringify({ pid: process.pid, role: s.role, checkpoint: s.checkpoint, folded: s.folded, epoch: s.epoch, ms }) + "\n");
  process.exit(0);
}

// ── process 2: resume, read, and append on request ───────────────────────────
if (process.argv[1] === self && process.argv[2] === "--resume") {
  const [url, txid, seq] = process.argv.slice(3);
  const loads = [];
  const api = pgApi(url, { contextCache: { onLoad: (l) => void loads.push(l) } });
  const out = (o) => process.stdout.write(JSON.stringify(o) + "\n");
  // one store call first, untimed: the pool connects and the store opens here, not inside the cold get (O18)
  await api.query({ types: ["OfferMade"], scopes: { offerId: "none" } }, { limit: 1 });
  const model = api.persistedReadModel(offers);
  const key = "offer-3";
  // a cold get first, in this fresh process: the key's row plus its events beyond the model's checkpoint
  const c0 = performance.now();
  const cold = await model.get(key);
  const coldMs = performance.now() - c0;
  const coldFolded = (await model.status()).readFolded;
  const t0 = performance.now();
  await model.list({ atLeast: { transactionId: txid, sequence: Number(seq) } });
  const catchUpMs = performance.now() - t0;
  const s = await model.status();
  const before = s.readFolded;
  const times = [];
  let warm;
  for (let i = 0; i < READS; i++) {
    const r0 = performance.now();
    warm = await model.get(key);
    times.push(performance.now() - r0);
  }
  const warmFolded = (await model.status()).readFolded - before;
  const x0 = performance.now();
  const viaContext = (await api.context({ query: keyQuery(key), fold, initial })).state;
  const contextMs = performance.now() - x0;
  const load = loads.at(-1);
  const l0 = performance.now();
  const page = await model.list({ where: { status: "open" }, sort: { by: "amount", dir: "desc" }, limit: 5 });
  const listMs = performance.now() - l0;
  out({ step: "resumed", role: s.role, folded: s.folded, checkpoint: s.checkpoint, epoch: s.epoch, catchUpMs, key, cold, coldMs, coldFolded, warmMs: median(times), warmFolded, warm, viaContext, contextMs, load, page, listMs });
  const lines = createInterface({ input: process.stdin });
  for await (const line of lines) {
    if (line === "append") {
      const appended = await api.append([sales.OfferRevised({ amount: 4242 }, { offerId: key })]);
      out({ step: "appended", at: Date.now(), token: tokenOf(appended) });
    } else if (line === "exit") break;
  }
  await api.close();
  process.exit(0);
}

/** One JSON line from a child's stdout. */
function nextLine(child) {
  const lines = createInterface({ input: child.stdout });
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

/** The memory store's restart: two runtimes over one MemoryStore are two processes. */
async function memoryRestart() {
  const store = new MemoryStore({ schema: buildSchema([sales], { strict: true }), persistedModels: true });
  const a = createEventStore({ events: [sales], store });
  let last;
  for (let i = 0; i < EVENTS; i += 250) last = await a.append(Array.from({ length: 250 }, (_, j) => event(i + j)));
  const first = a.persistedReadModel(offers);
  await first.list({ atLeast: tokenOf(last) });
  const s1 = await first.status();
  await a.close();
  for (let j = 0; j < BETWEEN; j++) last = await createEventStore({ events: [sales], store }).append([between(j)]);
  const b = createEventStore({ events: [sales], store });
  const second = b.persistedReadModel(offers);
  await second.list({ atLeast: tokenOf(last) });
  const s2 = await second.status();
  const same = isDeepStrictEqual(await second.get("offer-3"), (await b.context({ query: keyQuery("offer-3"), fold, initial })).state);
  await b.close();
  console.log("Memory store (persistedModels: true), two runtimes over one store:");
  console.log(`  runtime 1: role ${s1.role}, folded ${s1.folded}, checkpoint ${show(s1.checkpoint)}, then closed`);
  console.log(`  ${BETWEEN} events appended with no writer running`);
  console.log(`  runtime 2: role ${s2.role}, folded ${s2.folded} (only what came in between), epoch ${s1.epoch} → ${s2.epoch}, checkpoint ${show(s2.checkpoint)}`);
  console.log(`  same answer as es.context(): ${same ? "yes" : "NO"}`);
  return s1.folded === EVENTS && s2.folded === BETWEEN && s2.epoch === s1.epoch + 1 && same;
}

/** The stage-4 part of the showcase; `admin` drops the tables. Returns whether every expectation held. */
export async function stage4(url, admin) {
  console.log("@orgops/eventstore showcase — stage 4: the persisted read model");
  console.log("The same definition; es.persistedReadModel(def) keeps it as rows plus a checkpoint, written in one transaction by the one writer.\n");
  const memoryOk = await memoryRestart();
  console.log("");
  const dropAll = () => admin.query(`DROP TABLE IF EXISTS ${PM_TABLES.map((t) => `"${t}"`).join(", ")} CASCADE`);
  await dropAll();
  let pgOk = false;
  let resume;
  try {
    console.log(`Postgres (${PM_TABLE}, ${PM_TABLE}_read_model, ${PM_TABLE}_read_model_state; live: true):`);
    // process 1
    const p1 = await new Promise((resolve, reject) => {
      const child = spawn(process.execPath, [self, "--writer", url], { stdio: ["ignore", "pipe", "inherit"] });
      const next = nextLine(child);
      next().then(resolve, reject);
      child.on("error", reject);
    });
    console.log(`  process 1 (pid ${p1.pid}): appended ${EVENTS} events on ${OFFERS} offers; role ${p1.role}, folded ${p1.folded} in ${fmt(p1.ms)} ms, checkpoint ${show(p1.checkpoint)}, epoch ${p1.epoch} — ended`);
    // in between: no writer runs
    const here = pgApi(url);
    let last;
    for (let j = 0; j < BETWEEN; j++) last = await here.append([between(j)]);
    await admin.query(`VACUUM ANALYZE "${PM_TABLE}"`);
    await admin.query(`VACUUM ANALYZE "${PM_TABLE}_read_model_state"`);
    // the lag, from a handle in this process: for this one line the showcase holds the model's writer
    // lock itself (through the store's internal capability), so the handle stays a standby and writes nothing
    const cap = (await here.store()).readModelStore;
    const holder = {};
    const detach = cap.attach(holder);
    await cap.lock({ name: offers.name, version: offers.version }, holder, null);
    const watcherModel = here.persistedReadModel(offers);
    const lagging = await watcherModel.status();
    await cap.unlock({ name: offers.name, version: offers.version }, holder);
    detach();
    console.log(`  ${BETWEEN} events appended while no writer runs (10 revisions each for offer-0 … offer-4)`);
    console.log(`  status() of a standby handle here (the showcase held the writer lock for this line): checkpoint ${show(lagging.checkpoint)}, lag ${lagging.lag.events} events, ${lagging.lag.ms} ms`);
    // process 2
    const child = spawn(process.execPath, [self, "--resume", url, last.transactionId, String(last.last)], { stdio: ["pipe", "pipe", "inherit"] });
    resume = child;
    const next = nextLine(child);
    const r = await next();
    console.log(`  process 2 (pid ${child.pid}, fresh): resumed at the stored checkpoint ${show(lagging.checkpoint)}; role ${r.role}, folded ${r.folded} (only what came in between) in ${fmt(r.catchUpMs)} ms`);
    console.log(`    new checkpoint ${show(r.checkpoint)}, epoch ${p1.epoch} → ${r.epoch} (epoch ${lagging.epoch} was the showcase's own hold of the lock for the lag line)`);
    console.log(`    get("${r.key}") cold, first read of the model in this process (after one warm-up store call): ${fmt(r.coldMs)} ms, events folded ${r.coldFolded} (the key's events beyond the stored checkpoint, of ${BETWEEN / 5} new; its history is ${EVENTS / OFFERS + BETWEEN / 5} events)`);
    console.log(`    get("${r.key}") warm, median of ${READS}: ${fmt(r.warmMs)} ms, events folded per read: ${r.warmFolded / READS}`);
    console.log(`    es.context(${r.key}) in that process: ${fmt(r.contextMs)} ms (events ${r.load.events}, readMs ${fmt(r.load.readMs)}, foldMs ${fmt(r.load.foldMs)}, cacheHit ${r.load.cacheHit})`);
    const same = isDeepStrictEqual(r.warm, r.viaContext) && isDeepStrictEqual(r.cold, r.viaContext);
    console.log(`    state: ${JSON.stringify(r.warm)}`);
    console.log(`    same answer as es.context(): ${same ? "yes" : "NO"}`);
    console.log(`    list({ where: { status: "open" }, sort: amount desc, limit: 5 }): total ${r.page.total}, revision ${r.page.revision}, ${fmt(r.listMs)} ms`);
    for (const row of r.page.rows) console.log(`      ${row.key.padEnd(10)} ${row.state.status.padEnd(9)} amount ${row.state.amount}`);
    // watch: a standby here, the writer in process 2
    const seen = [];
    await watcherModel.watch(r.key, (state) => void seen.push({ state, at: Date.now() }));
    while (seen.length < 1) await new Promise((res) => setTimeout(res, 5));
    const role = (await watcherModel.status()).role;
    child.stdin.write("append\n");
    const appended = await next();
    const end = Date.now() + 3_000;
    while (seen.length < 2 && Date.now() < end) await new Promise((res) => setTimeout(res, 2));
    const got = seen[1];
    const delivered = got !== undefined && got.state.amount === 4242;
    console.log(`  watch("${r.key}") here (role ${role}): process 2 appended OfferRevised(4242) and folded it; delivered here ${got ? `${got.at - appended.at} ms after the append` : "—"}`);
    console.log(`    watcher received the new state: ${delivered ? "yes" : "NO"}   folded in this process: ${(await watcherModel.status()).folded}`);
    child.stdin.write("exit\n");
    await new Promise((res) => child.on("exit", res));
    resume = undefined;
    await here.close();
    pgOk = p1.folded === EVENTS && r.role === "writer" && r.folded === BETWEEN && r.warmFolded === 0 && r.coldFolded <= BETWEEN / 5 && same && delivered && role === "standby";
  } finally {
    if (resume && resume.exitCode === null) resume.kill("SIGKILL");
    await dropAll();
  }
  const all = memoryOk && pgOk;
  console.log(`stage 4 expectations (folded ${BETWEEN} on resume, 0 events per warm get, same answer as es.context(), the watcher got the new state): ${all ? "all met" : "NOT MET"}\n`);
  return all;
}
