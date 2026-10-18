import { describe, expect, expectTypeOf, it } from "vitest";
import { z } from "zod";
import {
  createEventStore,
  defineEvents,
  defineReadModel,
  scope,
  type Filter,
  type ListChange,
  type ListResult,
  type LiveReadModel,
  type PersistedReadModel,
  type Query,
  type ReadModelDefinition,
} from "../src/index.js";

// Compile-time pins of the typed read model definition and reads (decisions 51–53). Every refusal is an
// `@ts-expect-error` inside a closure that never runs: `npm run typecheck` checks them, vitest reports the groups.

const sales = defineEvents({
  OfferMade: { data: z.object({ amount: z.number() }), scopes: ["offerId"], optionalScopes: ["customerId"] },
  OfferNoted: { data: z.object({ text: z.string() }), scopes: ["offerId"], idKey: "noteId" },
  Shipped: { data: z.object({ carrier: z.string() }), scopes: ["shipmentId"] },
});
const billing = defineEvents({
  Invoiced: { data: z.object({ total: z.number() }), scopes: ["invoiceId"] },
});

interface Offer {
  status: "draft" | "open" | "accepted";
  amount: number;
  note?: string;
  at: Date;
  tags: string[];
  flag: boolean;
  meta: { a: number };
  anything: unknown;
}
const initial = (): Offer => ({ status: "draft", amount: 0, at: new Date(0), tags: [], flag: false, meta: { a: 0 }, anything: null });
const fold = sales.$fold<Offer>({
  OfferMade: (d, s) => ({ ...s, status: "open", amount: s.amount + d.amount }),
  OfferNoted: (d, s) => ({ ...s, note: d.text }),
});
const made = sales.$filter({ types: ["OfferMade"] });
const base = { name: "offers", version: 1, fold, initial } as const;

/** A closure the type checker reads and nobody calls. */
const typeOnly = (check: () => unknown): void => void check;

const live = null as unknown as LiveReadModel<Offer>;
const persisted = null as unknown as PersistedReadModel<Offer>;

describe("typed read model definitions: keyBy", () => {
  it("T1 admits a declared scope key of a source type, a type's own id key, and the union over several filters", () => {
    const key = "offerId";
    const defs = [
      defineReadModel({ ...base, source: made, keyBy: "offerId" }), // a required scope
      defineReadModel({ ...base, source: made, keyBy: "customerId" }), // an optional scope
      defineReadModel({ ...base, source: made, keyBy: "offerMadeId" }), // the default id key
      defineReadModel({ ...base, source: sales.$filter({ types: ["OfferNoted"] }), keyBy: "noteId" }), // an explicit idKey
      defineReadModel({ ...base, source: sales.$filter({ types: ["OfferMade", "Shipped"] }), keyBy: "shipmentId" }), // a key of one of two types
      defineReadModel({ ...base, source: sales.$filter(), keyBy: "shipmentId" }), // the whole registry
      defineReadModel({ ...base, source: [made, billing.$filter()], keyBy: "invoiceId" }), // two typed filters of two registries
      defineReadModel({ ...base, source: made, keyBy: key }), // a const holding the literal
    ];
    expectTypeOf(defs[0]!).toEqualTypeOf<ReadModelDefinition<Offer>>();
    expect(defs.map((d) => d.keyBy)).toEqual(["offerId", "customerId", "offerMadeId", "noteId", "shipmentId", "shipmentId", "invoiceId", "offerId"]);
  });

  it("T2 refuses a typo, a key of a type outside the source, a replaced id key, a data field and a widened string", () => {
    typeOnly(() => {
      // @ts-expect-error a typo in keyBy
      defineReadModel({ ...base, source: made, keyBy: "ofer" });
      // @ts-expect-error a key only a type outside the source declares
      defineReadModel({ ...base, source: made, keyBy: "shipmentId" });
      // @ts-expect-error the default id key of a type that declares its own idKey
      defineReadModel({ ...base, source: sales.$filter({ types: ["OfferNoted"] }), keyBy: "offerNotedId" });
      // @ts-expect-error a top-level data field is not a key
      defineReadModel({ ...base, source: sales.$filter({ types: ["OfferNoted"] }), keyBy: "text" });
      const widened: string = "offerId";
      // @ts-expect-error a widened string with a typed source
      defineReadModel({ ...base, source: made, keyBy: widened });
      const held = { ...base, source: made, keyBy: "offerId" };
      // @ts-expect-error a definition held in a variable widens keyBy to string: write `as const`
      defineReadModel(held);
      defineReadModel({ ...held, keyBy: "offerId" as const });
      // @ts-expect-error a spread with `not` keeps the registry, and so the check
      defineReadModel({ ...base, source: { ...made, not: { scopes: { offerId: "x" } } }, keyBy: "ofer" });
    });
    expect(true).toBe(true);
  });

  it("T3 an untyped source keeps plain strings", () => {
    const plain: Filter = { types: ["OfferMade"] };
    const widened: string = "anyKey";
    const query: Query = sales.$filter();
    const defs = [
      defineReadModel({ ...base, source: plain, keyBy: "anyKey" }), // a Filter variable
      defineReadModel({ ...base, source: { types: ["OfferMade"] }, keyBy: "anyKey" }), // a literal filter
      defineReadModel({ ...base, source: scope("customerId", "c1"), keyBy: "anyKey" }), // scope()
      defineReadModel({ ...base, source: sales.$scope("customerId", "c1"), keyBy: "anyKey" }), // $scope()
      defineReadModel({ ...base, source: [made, plain], keyBy: "anyKey" }), // an array holding an untyped filter
      defineReadModel({ ...base, source: query, keyBy: widened }), // a Query-typed variable, a widened string
    ];
    expect(defs.every((d) => d.keyBy === "anyKey")).toBe(true);
  });
});

describe("typed reads: where and sort.by from the state", () => {
  it("T4 where on the live handle: comparable fields with their exact types", async () => {
    typeOnly(() => {
      live.list({ where: { status: "open", amount: 5, flag: true, note: "x", anything: null } });
      const where = { status: "open" } as const;
      live.list({ where });
      // @ts-expect-error a typo in where
      live.list({ where: { stauts: "open" } });
      // @ts-expect-error a wrong value type
      live.list({ where: { amount: "5" } });
      // @ts-expect-error a value outside the literal union
      live.list({ where: { status: "closed" } });
      // @ts-expect-error a widened string for a literal-union field
      live.list({ where: { status: "open" as string } });
      // @ts-expect-error a Date field is no key of where
      live.list({ where: { at: new Date() } });
      // @ts-expect-error an array field is no key of where
      live.list({ where: { tags: "a" } });
      // @ts-expect-error an object field is no key of where
      live.list({ where: { meta: null } });
    });
    // the typed form is the runtime form: a live model on the memory store answers it
    const api = createEventStore({ events: [sales] });
    await api.append([sales.OfferMade({ amount: 3 }, { offerId: "o1" }), sales.OfferMade({ amount: 4 }, { offerId: "o2" })]);
    const model = api.readModel(defineReadModel({ ...base, source: made, keyBy: "offerId" }));
    const page = await model.list({ where: { status: "open", amount: 4 }, sort: { by: "amount", dir: "desc" } });
    expect(page.rows.map((r) => r.key)).toEqual(["o2"]);
    await api.close();
  });

  it("T5 the same where on the persisted handle and on both handles' watchList", () => {
    typeOnly(() => {
      persisted.list({ where: { status: "accepted" }, sort: { by: "at" } });
      // @ts-expect-error a typo in where (persisted list)
      persisted.list({ where: { stauts: "open" } });
      // @ts-expect-error a wrong value type (persisted list)
      persisted.list({ where: { flag: "yes" } });
      live.watchList({ where: { status: "open" } }, () => {});
      // @ts-expect-error a typo in where (live watchList)
      live.watchList({ where: { stauts: "open" } }, () => {});
      // @ts-expect-error a wrong value type (live watchList)
      live.watchList({ where: { amount: "1" } }, () => {});
      persisted.watchList({ where: { amount: 1 } }, () => {});
      // @ts-expect-error a typo in where (persisted watchList)
      persisted.watchList({ where: { stauts: "open" } }, () => {});
      // @ts-expect-error a wrong value type (persisted watchList)
      persisted.watchList({ where: { status: 1 } }, () => {});
    });
    expect(true).toBe(true);
  });

  it("T6 sort.by is any field of an object state; untyped states take strings; a number state has no field", () => {
    typeOnly(() => {
      for (const by of ["status", "amount", "note", "at", "tags", "flag", "meta", "anything"] as const) live.list({ sort: { by } });
      // @ts-expect-error a typo in sort.by
      live.list({ sort: { by: "amout" } });
      const counter = null as unknown as LiveReadModel<number>;
      counter.list({ limit: 5 });
      // @ts-expect-error keyof number holds its methods; a number state has no field to sort by
      counter.list({ sort: { by: "toFixed" } });
      // @ts-expect-error nor a field to filter on
      counter.list({ where: { n: 1 } });
      const loose = null as unknown as PersistedReadModel<Record<string, unknown>>;
      loose.list({ where: { whatever: 1 }, sort: { by: "whatever" } });
      const anyModel = null as unknown as LiveReadModel<any>;
      anyModel.list({ where: { whatever: 1 }, sort: { by: "whatever" } });
      const unknownModel = null as unknown as LiveReadModel<unknown>;
      unknownModel.list({ where: { whatever: 1 }, sort: { by: "whatever" } });
      type Shape = { kind: "a"; x: number } | { kind: "b"; y: string };
      const shapes = null as unknown as LiveReadModel<Shape>;
      shapes.list({ where: { kind: "a", x: 1 }, sort: { by: "y" } });
      // @ts-expect-error a field of no member of a union state
      shapes.list({ where: { z: 1 } });
      // @ts-expect-error a sort field of no member of a union state
      shapes.list({ sort: { by: "z" } });
    });
    expect(true).toBe(true);
  });

  it("T8 get, watch, list and watchList stay typed by the state", async () => {
    expectTypeOf<LiveReadModel<Offer>["get"]>().returns.resolves.toEqualTypeOf<Offer>();
    expectTypeOf<LiveReadModel<Offer>["watch"]>().parameter(1).parameter(0).toEqualTypeOf<Offer>();
    expectTypeOf<LiveReadModel<Offer>["list"]>().returns.resolves.toEqualTypeOf<ListResult<Offer>>();
    expectTypeOf<LiveReadModel<Offer>["watchList"]>().parameter(1).parameter(0).toEqualTypeOf<ListChange<Offer>>();
    expectTypeOf<PersistedReadModel<Offer>["get"]>().returns.resolves.toEqualTypeOf<Offer>();
    expectTypeOf<PersistedReadModel<Offer>["watch"]>().parameter(1).parameter(0).toEqualTypeOf<Offer>();
    expectTypeOf<PersistedReadModel<Offer>["list"]>().returns.resolves.toEqualTypeOf<ListResult<Offer>>();
    expectTypeOf<PersistedReadModel<Offer>["watchList"]>().parameter(1).parameter(0).toEqualTypeOf<ListChange<Offer>>();
    const api = createEventStore({ events: [sales] });
    expectTypeOf(api.readModel(defineReadModel({ ...base, source: made, keyBy: "offerId" }))).toEqualTypeOf<LiveReadModel<Offer>>();
    expect(typeof api.readModel).toBe("function");
    await api.close();
  });
});
