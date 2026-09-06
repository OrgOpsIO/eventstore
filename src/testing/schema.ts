import { z } from "zod";
import { buildSchema, defineEvents, type StoreSchema } from "../index.js";

/** The registry the conformance suite is written against. */
export const conformanceEvents = defineEvents({
  AccountOpened: { data: z.object({ owner: z.string() }), unique: ["owner"] },
  MoneyDeposited: { data: z.object({ amount: z.number().positive() }), scopes: ["accountOpenedId"] },
  MoneyWithdrawn: { data: z.object({ amount: z.number().positive() }), scopes: ["accountOpenedId"] },
  NoteAdded: { data: z.object({ text: z.string() }), optionalScopes: ["accountOpenedId"] },
  /** Flat-id compatibility: no `scopes`, the id lives in the data. */
  LegacyThing: { data: z.object({ thingId: z.string(), value: z.string() }) },
});

export interface ConformanceSchemaOptions {
  readonly strict?: boolean;
}

/** The schema every conforming store is exercised with: declared scope keys + a flat extra key `thingId`. */
export function conformanceSchema(options: ConformanceSchemaOptions = {}): StoreSchema {
  return buildSchema([conformanceEvents], { scopeKeys: ["thingId"], strict: options.strict ?? true });
}
