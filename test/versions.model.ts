/**
 * The versions of the offers model the stage-5 tests use — without vitest, so the child processes
 * (fixtures/versions-child.ts) can load them too. v1 is the persisted model of stage 4; v2 changes
 * what the fold computes (a bump); `offersV2Changed` changes it again under the SAME version 2 —
 * a forgotten bump, for the fingerprint warning.
 */
import { defineReadModel, type Fold } from "../src/index.js";
import { offersModel, type Offer } from "./persisted-model.model.js";

export * from "./persisted-model.model.js";

/** v2: an offer's total counts each amount twice. Pure, batch-associative, throws on a poison event like v1. */
export const offerFoldV2: Fold<Offer> = (events, state) => {
  let s = state;
  for (const e of events) {
    if (e.type === "OfferMade") {
      const d = e.data as { amount: number; status: string; poison?: boolean };
      if (d.poison) throw new Error(`poison event #${e.sequence}`);
      s = { ...s, n: s.n + 1, total: s.total + 2 * d.amount, status: d.status, last: e.recordedAt };
    } else if (e.type === "OfferNoted") {
      s = { ...s, notes: s.notes + 1 };
    }
  }
  return s;
};

/** Another body under version 2: each amount three times. */
export const offerFoldV2Changed: Fold<Offer> = (events, state) => {
  let s = state;
  for (const e of events) {
    if (e.type === "OfferMade") {
      const d = e.data as { amount: number; status: string; poison?: boolean };
      if (d.poison) throw new Error(`poison event #${e.sequence}`);
      s = { ...s, n: s.n + 1, total: s.total + 3 * d.amount, status: d.status, last: e.recordedAt };
    } else if (e.type === "OfferNoted") {
      s = { ...s, notes: s.notes + 1 };
    }
  }
  return s;
};

export const offersV1 = offersModel;
export const offersV2 = defineReadModel({ ...offersModel, version: 2, fold: offerFoldV2 });
export const offersV2Changed = defineReadModel({ ...offersModel, version: 2, fold: offerFoldV2Changed });
export const offersV3 = defineReadModel({ ...offersModel, version: 3, fold: offerFoldV2 });

/** `n` keys `k0` … `k{n-1}`. */
export const keysOf = (n: number): string[] => Array.from({ length: n }, (_, i) => `k${i}`);
