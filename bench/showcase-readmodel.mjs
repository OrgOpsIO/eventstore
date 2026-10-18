// The showcase's stage-3 part: a live read model's get and list, the events it folds per read (0
// when warm), the same answer as es.context(), and a watcher receiving the new state after an
// append from another process. Imported by bench/showcase.mjs; run as a child it appends one event:
//   node bench/showcase-readmodel.mjs --append <connection> <table> <offerId> <amount>
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { z } from "zod";
import { createEventStore, defineEvents, defineReadModel } from "../dist/index.js";

export const RM_TABLE = "showcase_rm";
const OFFERS = 100;
const EVENTS = 1_000;
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
/** What `es.context()` reads for one key of the model: the source with the key pinned (`source ∧ offerId = k`). */
const keyQuery = (k) => ({ ...source, scopes: { offerId: k } });

const event = (i) => {
  const offerId = `offer-${i % OFFERS}`;
  const round = Math.floor(i / OFFERS);
  if (round === 0) return sales.OfferMade({ amount: (i * 37) % 900 + 100 }, { offerId });
  if (round === 9 && i % 3 === 0) return sales.OfferAccepted({}, { offerId });
  return sales.OfferRevised({ amount: (i * 53) % 900 + 100 }, { offerId });
};

// ── child: another process appends one revision ─────────────────────────────
if (process.argv[1] === fileURLToPath(import.meta.url) && process.argv[2] === "--append") {
  const [connection, table, offerId, amount] = process.argv.slice(3);
  const api = createEventStore({ events: [sales], connection, postgres: { table, live: true, poolSize: 1 } });
  await api.append([sales.OfferRevised({ amount: Number(amount) }, { offerId })]);
  const at = Date.now();
  await api.close();
  process.stdout.write(JSON.stringify({ at }));
  process.exit(0);
}

const fmt = (n) => n.toFixed(n < 0.1 ? 4 : n < 10 ? 2 : 1);
const median = (xs) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];

/** One store: rebuild, warm get beside es.context(), list. Returns whether every expectation held. */
async function readsOn(label, api) {
  let ok = true;
  for (let i = 0; i < EVENTS; i += 250) await api.append(Array.from({ length: 250 }, (_, j) => event(i + j)));
  const t0 = performance.now();
  const model = api.readModel(offers);
  const key = "offer-7";
  await model.get(key);
  const rebuildMs = performance.now() - t0;
  console.log(`${label}: ${EVENTS.toLocaleString("en")} events on ${OFFERS} offers`);
  console.log(`  rebuild (readModel → first get): ${fmt(rebuildMs)} ms, ${model.status.folded} events folded, ${model.status.keys} keys`);

  const before = model.status.folded;
  const times = [];
  let state;
  for (let i = 0; i < READS; i++) {
    const s = performance.now();
    state = await model.get(key);
    times.push(performance.now() - s);
  }
  const foldedPerRead = (model.status.folded - before) / READS;
  const loads = [];
  const viaContext = async () => {
    const s = performance.now();
    const loaded = await api.context({ query: keyQuery(key), fold, initial });
    return { state: loaded.state, ms: performance.now() - s };
  };
  const cold = await viaContext();
  const warm = await viaContext();
  const same = isDeepStrictEqual(state, cold.state) && isDeepStrictEqual(state, warm.state);
  const [coldLoad, warmLoad] = api.__loads.slice(-2);
  console.log(`  model.get("${key}")  median of ${READS}: ${fmt(median(times))} ms, events folded per read: ${foldedPerRead}`);
  console.log(`  es.context(${key})   cold: ${fmt(cold.ms)} ms (events ${coldLoad.events}, readMs ${fmt(coldLoad.readMs)}, foldMs ${fmt(coldLoad.foldMs)}); warm: ${fmt(warm.ms)} ms (events ${warmLoad.events}, cacheHit ${warmLoad.cacheHit})`);
  console.log(`  state: ${JSON.stringify(state)}`);
  console.log(`  same answer as es.context(): ${same ? "yes" : "NO"}`);
  if (!same || foldedPerRead !== 0) ok = false;

  const lb = model.status.folded;
  const s = performance.now();
  const page = await model.list({ where: { status: "open" }, sort: { by: "amount", dir: "desc" }, limit: 5 });
  const listMs = performance.now() - s;
  const listFolded = model.status.folded - lb;
  console.log(`  list({ where: { status: "open" }, sort: amount desc, limit: 5 }): total ${page.total} of ${model.status.keys}, ${fmt(listMs)} ms, events folded: ${listFolded}`);
  for (const r of page.rows) console.log(`    ${r.key.padEnd(10)} ${r.state.status.padEnd(9)} amount ${r.state.amount}`);
  // the same page by fold on read, key by key, to show it is the same answer
  const byContext = [];
  for (let k = 0; k < OFFERS; k++) {
    const offerId = `offer-${k}`;
    const st = (await api.context({ query: keyQuery(offerId), fold, initial })).state;
    if (st.status === "open") byContext.push({ key: offerId, state: st });
  }
  byContext.sort((a, b) => b.state.amount - a.state.amount || (a.key < b.key ? -1 : 1));
  const listSame = isDeepStrictEqual(page.rows.map((r) => [r.key, r.state]), byContext.slice(0, 5).map((r) => [r.key, r.state])) && page.total === byContext.length;
  console.log(`  same page as fold on read over every key: ${listSame ? "yes" : "NO"}`);
  if (!listSame || listFolded !== 0) ok = false;
  return { ok, model };
}

/** Postgres with the doorbell: a watcher here, an append in a child process. */
async function watchAcrossProcesses(url, api, model) {
  const key = "offer-7";
  const seen = [];
  const w = await model.watch(key, (state) => void seen.push({ state, at: Date.now() }));
  while (seen.length < 1) await new Promise((r) => setTimeout(r, 5));
  console.log(`  watch("${key}") first delivery (current state): ${JSON.stringify(seen[0].state)}`);
  const self = fileURLToPath(import.meta.url);
  const child = JSON.parse(execFileSync(process.execPath, [self, "--append", url, RM_TABLE, key, "4242"], { encoding: "utf8" }));
  const end = Date.now() + 3_000;
  while (seen.length < 2 && Date.now() < end) await new Promise((r) => setTimeout(r, 2));
  w.stop();
  const got = seen[1];
  const ok = got !== undefined && got.state.amount === 4242;
  if (got) console.log(`  a child process appended OfferRevised(4242); delivered here ${got.at - child.at} ms after its commit: ${JSON.stringify(got.state)}`);
  console.log(`  watcher received the new state: ${ok ? "yes" : "NO"}`);
  const viaContext = (await api.context({ query: keyQuery(key), fold, initial })).state;
  const same = ok && isDeepStrictEqual(got.state, viaContext);
  console.log(`  the pushed state equals es.context(): ${same ? "yes" : "NO"}`);
  return ok && same;
}

/** The stage-3 part of the showcase; `admin` drops the table. Returns whether every expectation held. */
export async function stage3(url, admin) {
  console.log("@orgops/eventstore showcase — stage 3: the live read model");
  console.log("defineReadModel({ name, version, source, keyBy: \"offerId\", fold, initial }); es.readModel(def) keeps it in memory, current by its feed.\n");
  const withLoads = (config) => {
    const loads = [];
    const api = createEventStore({ ...config, contextCache: { onLoad: (l) => void loads.push(l) } });
    api.__loads = loads;
    return api;
  };
  const memoryApi = withLoads({ events: [sales] });
  let memoryOk;
  try {
    ({ ok: memoryOk } = await readsOn("Memory store", memoryApi));
  } finally {
    await memoryApi.close();
  }
  console.log("");
  await admin.query(`DROP TABLE IF EXISTS "${RM_TABLE}" CASCADE`);
  const pgApi = withLoads({ events: [sales], connection: url, postgres: { table: RM_TABLE, live: true, poolSize: 2 } });
  let pgOk = false;
  let watchOk = false;
  try {
    const { ok, model } = await readsOn(`Postgres (${RM_TABLE}, live: true)`, pgApi);
    pgOk = ok;
    watchOk = await watchAcrossProcesses(url, pgApi, model);
  } finally {
    await pgApi.close();
    await admin.query(`DROP TABLE IF EXISTS "${RM_TABLE}" CASCADE`);
  }
  const all = memoryOk && pgOk && watchOk;
  console.log(`stage 3 expectations (0 events folded per warm read, same answers as es.context(), the watcher got the new state): ${all ? "all met" : "NOT MET"}\n`);
  return all;
}
