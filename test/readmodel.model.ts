/**
 * The events, fold and read model of the read model tests — without vitest, so the child process
 * of the two-process test (fixtures/readmodel-append.ts) can load it too.
 */
import { z } from "zod";
import { defineEvents, defineReadModel, type AppendResult, type Cursor, type EventStoreConfig, type Fold } from "../src/index.js";

export const offers = defineEvents({
  OfferMade: { data: z.object({ amount: z.number(), status: z.string(), poison: z.boolean().optional() }), scopes: ["offerId"], optionalScopes: ["tenantId"] },
  /** In the source; carries a key only sometimes. */
  OfferNoted: { data: z.object({ text: z.string() }), optionalScopes: ["offerId", "tenantId"] },
  /** Outside the source. */
  OfferAudited: { data: z.object({ by: z.string() }), scopes: ["offerId"], optionalScopes: ["tenantId"] },
});
export const TENANT = { scopeKey: "tenantId" } as const;
export const config = { events: [offers], tenant: TENANT } satisfies EventStoreConfig;

export type Offer = { n: number; total: number; status: string; notes: number; last: Date | null };
export const initialOffer: Offer = { n: 0, total: 0, status: "none", notes: 0, last: null };
export const source = offers.$filter({ types: ["OfferMade", "OfferNoted"] });

/** A pure, batch-associative fold that keeps a counter, a sum, a status, a Date — and throws on a poison event. */
export const offerFold: Fold<Offer> = (events, state) => {
  let s = state;
  for (const e of events) {
    if (e.type === "OfferMade") {
      const d = e.data as { amount: number; status: string; poison?: boolean };
      if (d.poison) throw new Error(`poison event #${e.sequence}`);
      s = { ...s, n: s.n + 1, total: s.total + d.amount, status: d.status, last: e.recordedAt };
    } else if (e.type === "OfferNoted") {
      s = { ...s, notes: s.notes + 1 };
    }
  }
  return s;
};

export const offersModel = defineReadModel({ name: "offers", version: 1, source, keyBy: "offerId", fold: offerFold, initial: initialOffer });

export const made = (offerId: string, amount: number, status = "open", poison = false) =>
  offers.OfferMade({ amount, status, ...(poison ? { poison: true } : {}) }, { offerId });
export const noted = (offerId?: string) => offers.OfferNoted({ text: "n" }, offerId ? { offerId } : {});
export const audited = (offerId: string) => offers.OfferAudited({ by: "x" }, { offerId });

export const tokenOf = (a: AppendResult): Cursor => ({ transactionId: a.transactionId, sequence: a.last });
