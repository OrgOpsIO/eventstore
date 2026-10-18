/**
 * The second process of the two-process read model test: appends one OfferMade through the library
 * and exits. Run with vite-node (vitest's own runner, no new dependency):
 *   vite-node test/fixtures/readmodel-append.ts <connection> <table> <offerId> <amount> [tenant]
 * Prints the append's read-your-writes token as JSON.
 */
import { createEventStore } from "../../src/index.js";
import { config, made, tokenOf } from "../readmodel.model.js";

const [connection, table, offerId, amount, tenant] = process.argv.slice(2);
if (!connection || !table || !offerId || !amount) {
  console.error("usage: readmodel-append.ts <connection> <table> <offerId> <amount> [tenant]");
  process.exit(2);
}
const api = createEventStore({ ...config, connection, postgres: { table, live: true, poolSize: 1 } });
const view = tenant ? api.forTenant(tenant) : api;
const appended = await view.append([made(offerId, Number(amount))]);
console.log(JSON.stringify({ token: tokenOf(appended), at: Date.now() }));
await api.close();
