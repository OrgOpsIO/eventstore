/**
 * A process that appends one OfferMade through the library and exits. Run with vite-node:
 *   vite-node test/fixtures/persisted-model-append.ts <connection> <table> <offerId> <amount>
 * Prints the append's read-your-writes token as JSON.
 */
import { createEventStore } from "../../src/index.js";
import { made, pmConfig, tokenOf } from "../persisted-model.model.js";

const [connection, table, offerId, amount] = process.argv.slice(2);
if (!connection || !table || !offerId || !amount) {
  console.error("usage: persisted-model-append.ts <connection> <table> <offerId> <amount>");
  process.exit(2);
}
const api = createEventStore({ ...pmConfig, connection, postgres: { table, live: true, poolSize: 1, persistedModels: true } });
const appended = await api.append([made(offerId, Number(amount))]);
console.log(JSON.stringify({ token: tokenOf(appended), at: Date.now() }));
await api.close();
