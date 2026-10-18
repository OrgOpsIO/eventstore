// The fingerprint (decision 42): its cost per definition and that it is computed once. The fingerprint is
// internal (src/fingerprint.ts, not in dist/), so this runs on the sources through vite-node:
//   node_modules/.bin/vite-node bench/fingerprint-bench.mjs [definitions=2000] [reps=5]
// No database. Per rep: `definitions` fresh definitions (a fold through $fold, defineFold over a plain fold, and a read
// model, each a new object with the same text, so nothing is served from the WeakMap memo), hashed once each — the
// first call, which hashes; then every definition again — served from memory; the counter fingerprintsComputed()
// before and after says how many hashes ran. The defs are the runtime's merged $defs of three event types
// (what src/runtime.ts:184 builds), so each first call also reads the zod JSON schema of the types the source names.
import { z } from "zod";
import { defineEvents, defineFold, defineReadModel } from "../src/index.ts";
import { fingerprintsComputed, foldFingerprint, modelFingerprint } from "../src/fingerprint.ts";

const n = Number(process.argv[2] ?? 2000);
const reps = Number(process.argv[3] ?? 5);
const sales = defineEvents({
  OfferMade: { data: z.object({ amount: z.number(), status: z.enum(["open", "won", "lost"]), note: z.string().max(2000), lines: z.array(z.object({ sku: z.string(), qty: z.number().int() })) }), scopes: ["offerId"], optionalScopes: ["tenantId"] },
  OfferRevised: { data: z.object({ amount: z.number(), reason: z.string().optional() }), scopes: ["offerId"], optionalScopes: ["tenantId"] },
  OfferNoted: { data: z.object({ text: z.string() }), scopes: ["offerId"], optionalScopes: ["tenantId"] },
});
const defs = Object.freeze(Object.assign({}, sales.$defs));
const source = sales.$filter({ types: ["OfferMade", "OfferRevised", "OfferNoted"] });
const initial = { status: "none", amount: 0, revisions: 0, notes: 0 };

const kinds = {
  "read model ($fold, 3 handlers)": () =>
    defineReadModel({
      name: "offers",
      version: 1,
      source,
      keyBy: "offerId",
      initial,
      fold: sales.$fold({
        OfferMade: (d, s) => ({ ...s, status: d.status, amount: d.amount }),
        OfferRevised: (d, s) => ({ ...s, amount: d.amount, revisions: s.revisions + 1 }),
        OfferNoted: (_d, s) => ({ ...s, notes: s.notes + 1 }),
      }),
    }),
  "persisted fold (defineFold over a plain fold)": () =>
    defineFold({
      name: "offer-total",
      version: 1,
      fold: (events, s) => {
        let t = s;
        for (const e of events) if (e.type === "OfferMade" || e.type === "OfferRevised") t = e.data.amount;
        return t;
      },
    }),
};
const print = (kind, def) => (kind.startsWith("read model") ? modelFingerprint(def, defs) : foldFingerprint(def, source, defs));

console.log(`command: node_modules/.bin/vite-node bench/fingerprint-bench.mjs ${n} ${reps}; Node ${process.version}; ${new Date().toISOString()}`);
console.log("");
console.log("| definition | first call (hashes), µs per definition: median of reps (min–max) | again (memo), µs | hashes computed per definition, first call / again | prints equal |");
console.log("|---|---|---|---|---|");
for (const [kind, make] of Object.entries(kinds)) {
  const first = [];
  const again = [];
  let counts = "";
  let equal = true;
  for (let r = -1; r < reps; r++) {
    const ds = Array.from({ length: n }, make);
    const c0 = fingerprintsComputed();
    let t = performance.now();
    const ps = ds.map((d) => print(kind, d));
    const t1 = (performance.now() - t) / n;
    const c1 = fingerprintsComputed();
    t = performance.now();
    const ps2 = ds.map((d) => print(kind, d));
    const t2 = (performance.now() - t) / n;
    const c2 = fingerprintsComputed();
    equal &&= ps.every((p) => p === ps[0]) && ps2.every((p, i) => p === ps[i]);
    if (r >= 0) {
      first.push(t1 * 1000);
      again.push(t2 * 1000);
      counts = `${(c1 - c0) / n} / ${(c2 - c1) / n}`;
    }
  }
  const med = (xs) => [...xs].sort((a, b) => a - b)[xs.length >> 1];
  const fmt = (x) => (x < 1 ? x.toFixed(3) : x.toFixed(2));
  console.log(`| ${kind} | ${fmt(med(first))} (${fmt(Math.min(...first))}–${fmt(Math.max(...first))}) | ${fmt(med(again))} (${fmt(Math.min(...again))}–${fmt(Math.max(...again))}) | ${counts} | ${equal ? "yes" : "NO"} |`);
}
