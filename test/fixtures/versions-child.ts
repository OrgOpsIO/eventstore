/**
 * A process that runs ONE version of the offers model, persisted — the code of one deploy — and
 * reports one JSON line every 100 ms until its stdin closes: building, phase, role, whether
 * `get(key)` equals fold on read with its own fold, and what `list` answers (a total, "waiting"
 * while the version builds, or the error). Run with vite-node:
 *   vite-node test/fixtures/versions-child.ts <connection> <table> <version 1|2> <key>
 */
import { createEventStore, type ReadModelDefinition } from "../../src/index.js";
import type { ReadModelStore } from "../../src/modelstore.js";
import { keyQuery } from "../../src/readmodel.js";
import { offersV1, offersV2, pmConfig, type Offer } from "../versions.model.js";

const [connection, table, version, key] = process.argv.slice(2);
if (!connection || !table || !version || !key) {
  console.error("usage: versions-child.ts <connection> <table> <version 1|2> <key>");
  process.exit(2);
}
const def = (version === "2" ? offersV2 : offersV1) as ReadModelDefinition<Offer>;
const api = createEventStore({ ...pmConfig, connection, postgres: { table, live: true, persistedModels: true, poolSize: 3 } });
((await api.store()) as unknown as { readModelStore: ReadModelStore }).readModelStore.knobs.tickMs = 100;
const model = api.persistedReadModel(def);
let busy = false;
const report = async () => {
  if (busy) return;
  busy = true;
  try {
    // list before status: building only ever goes true → false, so a line that says building had a list that waited
    let list: string;
    try {
      list = `total ${(await model.list({ timeoutMs: 50 })).total}`;
    } catch (error) {
      list = /still building/.test(String(error)) ? "waiting" : `error: ${String(error)}`;
    }
    const status = await model.status();
    let get: "equal" | "DIFFERENT" | string = "?";
    try {
      // an append to the key may land between the two reads: up to 3 tries before the line says DIFFERENT
      for (let i = 0; i < 3 && get !== "equal"; i++) {
        const [got, want] = await Promise.all([model.get(key), api.context({ query: keyQuery(def.source, def.keyBy, key), fold: def.fold, initial: def.initial })]);
        get = JSON.stringify(got) === JSON.stringify(want.state) ? "equal" : "DIFFERENT";
      }
    } catch (error) {
      get = `error: ${String(error)}`;
    }
    console.log(JSON.stringify({ at: Date.now(), version: def.version, phase: status.phase, role: status.role, building: status.building, lag: status.lag.events, get, list }));
  } catch (error) {
    console.log(JSON.stringify({ at: Date.now(), version: def.version, error: String(error) }));
  } finally {
    busy = false;
  }
};
const timer = setInterval(() => void report(), 100);
process.stdin.resume();
process.stdin.on("end", async () => {
  clearInterval(timer);
  await api.close();
  process.exit(0);
});
