/**
 * A fresh process that opens N tenant handles of the offers model at once on one runtime (their
 * writer locks share the store's one lock session), waits until every one writes, stops them all at
 * once, and prints one JSON line: how many wrote, and every process warning seen. Run with vite-node:
 *   vite-node test/fixtures/persisted-model-startup.ts <connection> <table> <n>
 */
import { createEventStore } from "../../src/index.js";
import type { ReadModelStore } from "../../src/modelstore.js";
import { offersModel, pmConfig } from "../persisted-model.model.js";

const [connection, table, n] = process.argv.slice(2);
if (!connection || !table || !n) {
  console.error("usage: persisted-model-startup.ts <connection> <table> <n>");
  process.exit(2);
}
const warnings: string[] = [];
process.on("warning", (warning) => warnings.push(`${warning.name}: ${warning.message}`));
const api = createEventStore({ ...pmConfig, connection, postgres: { table, persistedModels: true, poolSize: 4 } });
((await api.store()) as unknown as { readModelStore: ReadModelStore }).readModelStore.knobs.tickMs = 50;
const models = Array.from({ length: Number(n) }, (_, i) => api.forTenant(`startup-${i}`).persistedReadModel(offersModel));
const writers = async () => (await Promise.all(models.map(async (m) => (await m.status()).role))).filter((r) => r === "writer").length;
const deadline = Date.now() + 30_000;
let wrote = await writers();
while (wrote < models.length && Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, 50));
  wrote = await writers();
}
await new Promise((r) => setTimeout(r, 200)); // a few heartbeats beside them
await Promise.all(models.map((m) => m.stop()));
await api.close();
console.log(JSON.stringify({ writers: wrote, warnings }));
process.exit(0);
