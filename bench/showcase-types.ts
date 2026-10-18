// The showcase's stage-7 fixture (not in the package, not in tsconfig's include): a read model declared and read
// against the BUILT package, with three mistakes on purpose. `sh bench/showcase.sh` type-checks it with
//   tsc --noEmit --strict --module nodenext --moduleResolution nodenext --skipLibCheck false bench/showcase-types.ts
// and expects exactly the three errors marked `// mistake:` below, and nothing else (an error inside the emitted
// .d.ts would count as another one). It is never run.
import { z } from "zod";
import { createEventStore, defineEvents, defineReadModel } from "../dist/index.js";

const sales = defineEvents({
  OfferMade: { data: z.object({ amount: z.number() }), scopes: ["offerId"], optionalScopes: ["customerId"] },
  OfferAccepted: { data: z.object({}), scopes: ["offerId"] },
});

interface Offer {
  status: "draft" | "open" | "accepted";
  amount: number;
}

const initial: Offer = { status: "draft", amount: 0 };
const fold = sales.$fold<Offer>({
  OfferMade: (d, s) => ({ ...s, status: "open", amount: d.amount }),
  OfferAccepted: (_, s) => ({ ...s, status: "accepted" }),
});

export const offers = defineReadModel({
  name: "offers",
  version: 1,
  source: sales.$filter(),
  keyBy: "offerID", // mistake: keyBy misspelt (the declared scope key is offerId)
  fold,
  initial,
});

export const byCustomer = defineReadModel({ name: "by-customer", version: 1, source: sales.$filter({ types: ["OfferMade"] }), keyBy: "customerId", fold, initial });

export async function read(): Promise<void> {
  const es = createEventStore({ events: [sales] });
  const model = es.readModel(byCustomer);
  await model.list({ where: { stauts: "open" } }); // mistake: a where field misspelt
  await model.list({ where: { status: "closed" } }); // mistake: a value outside the literal union
  await model.list({ where: { status: "open", amount: 5 }, sort: { by: "amount", dir: "desc" } });
  const offer: Offer = await model.get("c1");
  void offer;
}
