import { z } from "zod";
import { buildSchema, defineEvents, type StoreSchema } from "../index.js";

/** The registry the conformance suite is written against. */
export const conformanceEvents = defineEvents({
  AccountOpened: { data: z.object({ owner: z.string() }), unique: ["owner"] },
  MoneyDeposited: { data: z.object({ amount: z.number().positive() }), scopes: ["accountOpenedId"] },
  MoneyWithdrawn: { data: z.object({ amount: z.number().positive() }), scopes: ["accountOpenedId"] },
  NoteAdded: { data: z.object({ text: z.string() }), optionalScopes: ["accountOpenedId"] },
  /** Flat-id compatibility: no `scopes`, the id lives in the data. */
  LegacyThing: { data: z.object({ thingId: z.string().nullable(), value: z.string() }) },
});

/**
 * A registry whose stored payloads may still carry the old field `v`; `upcast` renames it to
 * `value` on read. Both shapes pass the schema so the old shape can be appended in a test.
 */
export const conformanceUpcastEvents = defineEvents({
  Renamed: {
    data: z.object({ value: z.string().optional(), v: z.string().optional() }),
    upcast: (payload) => {
      if (!("v" in payload)) return payload;
      const { v, ...rest } = payload;
      return { ...rest, value: v };
    },
  },
});

/** The schema for the upcast case: non-strict, no scope keys. */
export function conformanceUpcastSchema(): StoreSchema {
  return buildSchema([conformanceUpcastEvents], { strict: false });
}

/** Options of `conformanceSchema()`: strictness. */
export interface ConformanceSchemaOptions {
  readonly strict?: boolean;
}

/** The schema every conforming store is exercised with: declared scope keys + a flat extra key `thingId`. */
export function conformanceSchema(options: ConformanceSchemaOptions = {}): StoreSchema {
  return buildSchema([conformanceEvents], { scopeKeys: ["thingId"], strict: options.strict ?? true });
}
