// MemoryStore throughput: guarded commands on many contexts and on one growing context.
import { z } from "zod";
import { defineEvents, buildSchema, MemoryStore } from "../dist/index.js";

const accounts = defineEvents({
  AccountOpened: { data: z.object({ owner: z.string() }) },
  MoneyDeposited: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"] },
});
const store = new MemoryStore({ schema: buildSchema([accounts], { strict: true }) });
const N = 2000;
const ids = [];
for (let i = 0; i < N; i++) ids.push(accounts.AccountOpened({ owner: `acc-${i}` }));
await store.append(ids);
const accountIds = ids.map((e) => e.id);
for (let round = 0; round < 25; round++) {
  await store.append(accountIds.map((id) => accounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: id })));
}
console.log(`store: ${store.events.length} events, ${N} contexts of ~26 events`);

let t0 = Date.now(), n = 0;
while (Date.now() - t0 < 2000) {
  const id = accountIds[n % N];
  const q = accounts.$scope("accountOpenedId", id);
  const r = await store.query(q);
  await store.appendIf([accounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: id })], r.ctx);
  n++;
}
console.log(`guarded commands across ${N} contexts: ${Math.round(n / 2)} /s`);

const hot = accountIds[0];
t0 = Date.now(); n = 0;
while (Date.now() - t0 < 2000) {
  const q = accounts.$scope("accountOpenedId", hot);
  const r = await store.query(q);
  await store.appendIf([accounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: hot })], r.ctx);
  n++;
}
console.log(`guarded commands on one growing context (${n} events at the end): ${Math.round(n / 2)} /s`);
