/**
 * A process that holds a handle of the offers model, persisted (writer or standby), and reports its
 * status as one JSON line every 50 ms until it is killed or its stdin closes. Run with vite-node:
 *   vite-node test/fixtures/persisted-model-writer.ts <connection> <table> <tickMs> [tenant]
 */
import { createEventStore } from "../../src/index.js";
import type { ReadModelStore } from "../../src/modelstore.js";
import { offersModel, pmConfig } from "../persisted-model.model.js";

const [connection, table, tickMs, tenant] = process.argv.slice(2);
if (!connection || !table || !tickMs) {
  console.error("usage: persisted-model-writer.ts <connection> <table> <tickMs> [tenant]");
  process.exit(2);
}
const api = createEventStore({ ...pmConfig, connection, postgres: { table, live: true, persistedModels: true, poolSize: 2 } });
const view = tenant ? api.forTenant(tenant) : api;
((await api.store()) as unknown as { readModelStore: ReadModelStore }).readModelStore.knobs.tickMs = Number(tickMs);
const model = view.persistedReadModel(offersModel);
const report = async () => {
  try {
    const s = await model.status();
    console.log(JSON.stringify({ pid: process.pid, role: s.role, folded: s.folded, epoch: s.epoch, checkpoint: s.checkpoint, phase: s.phase }));
  } catch (error) {
    console.log(JSON.stringify({ pid: process.pid, error: String(error) }));
  }
};
const timer = setInterval(() => void report(), 50);
process.stdin.resume();
process.stdin.on("end", async () => {
  clearInterval(timer);
  await api.close();
  process.exit(0);
});
