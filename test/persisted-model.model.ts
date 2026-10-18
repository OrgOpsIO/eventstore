/**
 * The events, folds and models of the persisted read model tests — without vitest, so the child
 * processes (fixtures/persisted-model-*.ts) and the showcase can load them too. The offers model is
 * the live model's (readmodel.model.ts); `items` lets a test put any value into a state field, for
 * the doc rules of `list`.
 */
import { z } from "zod";
import { defineEvents, defineReadModel, type EventStoreConfig, type Fold } from "../src/index.js";
import { offers, TENANT } from "./readmodel.model.js";

export * from "./readmodel.model.js";

export const items = defineEvents({
  /** Sets one field of an item's state: `kind` says what value it becomes. */
  ItemSet: {
    data: z.object({ field: z.string(), kind: z.enum(["num", "str", "bool", "null", "date", "inf", "nan", "arr", "obj", "nul", "lone", "del", "neg0"]), num: z.number().optional(), str: z.string().optional(), bool: z.boolean().optional() }),
    scopes: ["itemId"],
    optionalScopes: ["tenantId"],
  },
  /** Makes the fold return a value that cannot be stored (a class instance). */
  ItemBroken: { data: z.object({}), scopes: ["itemId"], optionalScopes: ["tenantId"] },
});

export const pmConfig = { events: [offers, items], tenant: TENANT } satisfies EventStoreConfig;

export type Item = Record<string, unknown>;

class NotStorable {
  readonly x = 1;
}

/** Every value kind a state field can hold; pure, batch-associative, a new object per changed state. */
export const itemFold: Fold<Item> = (events, state) => {
  let s = state;
  for (const e of events) {
    if (e.type === "ItemBroken") {
      s = { ...s, broken: new NotStorable() };
      continue;
    }
    const d = e.data as { field: string; kind: string; num?: number; str?: string; bool?: boolean };
    const next: Item = { ...s };
    switch (d.kind) {
      case "num":
        next[d.field] = d.num;
        break;
      case "str":
        next[d.field] = d.str;
        break;
      case "bool":
        next[d.field] = d.bool;
        break;
      case "null":
        next[d.field] = null;
        break;
      case "date":
        next[d.field] = new Date(d.num ?? 0);
        break;
      case "inf":
        next[d.field] = Number.POSITIVE_INFINITY;
        break;
      case "nan":
        next[d.field] = Number.NaN;
        break;
      case "arr":
        next[d.field] = [d.num ?? 0];
        break;
      case "obj":
        next[d.field] = { n: d.num ?? 0 };
        break;
      case "nul":
        next[d.field] = `${d.str ?? ""}\u0000x`; // a string Postgres' jsonb cannot hold
        break;
      case "lone":
        next[d.field] = `${d.str ?? ""}\ud800`; // a lone surrogate
        break;
      case "neg0":
        next[d.field] = -0;
        break;
      case "del":
        delete next[d.field];
        break;
    }
    s = next;
  }
  return s;
};

export const itemsModel = defineReadModel<Item>({ name: "items", version: 1, source: items.$filter(), keyBy: "itemId", fold: itemFold, initial: () => ({}) });

export const set = (itemId: string, field: string, kind: string, value?: number | string | boolean) =>
  items.ItemSet(
    {
      field,
      kind: kind as "num",
      ...(typeof value === "number" ? { num: value } : typeof value === "string" ? { str: value } : typeof value === "boolean" ? { bool: value } : {}),
    },
    { itemId },
  );
export const broken = (itemId: string) => items.ItemBroken({}, { itemId });
