/**
 * The fingerprint itself (decision 42): what changes it and what does not. The warnings it gives are
 * specified in versions.suite.ts (persisted and live read models) and persisted-fold.suite.ts.
 */
import { describe, expect, it } from "vitest";
import { z } from "zod";
import { defineEvents, defineFold, defineReadModel, MemoryStore, buildSchema, createEventStore } from "../src/index.js";
import { foldFingerprint, fingerprintsComputed, modelFingerprint, resetFingerprintWarnings, warnIfChanged, type TypeDefs } from "../src/fingerprint.js";

const make = (amount: z.ZodNumber = z.number(), upcast?: (p: Record<string, unknown>) => Record<string, unknown>) =>
  defineEvents({
    Booked: { data: z.object({ amount, note: z.string().optional() }), scopes: ["accountId"], ...(upcast ? { upcast } : {}) },
    Closed: { data: z.object({}), scopes: ["accountId"] },
  });
const ledger = make();
const defsOf = (registry: { $defs: TypeDefs }): TypeDefs => registry.$defs;
const defs = defsOf(ledger);
const source = ledger.$filter();

const sum = ledger.$fold<number>({ Booked: (d, s) => s + d.amount });
const model = (over: Partial<Parameters<typeof defineReadModel<number>>[0]> = {}) =>
  defineReadModel<number>({ name: "balance", version: 1, source, keyBy: "accountId", fold: sum, initial: 0, ...over });

describe("fingerprint", () => {
  it("the same definition gives the same fingerprint, also a second object with the same text and the same schemas", () => {
    expect(modelFingerprint(model(), defs)).toBe(modelFingerprint(model(), defs));
    const again = make();
    const sumAgain = again.$fold<number>({ Booked: (d, s) => s + d.amount });
    expect(modelFingerprint(defineReadModel<number>({ name: "balance", version: 1, source: again.$filter(), keyBy: "accountId", fold: sumAgain, initial: 0 }), defsOf(again))).toBe(
      modelFingerprint(model(), defs),
    );
    expect(modelFingerprint(model(), defs)).toMatch(/^[0-9a-f]{64}$/);
  });

  it("a changed handler body, plain fold, source, keyBy, initial factory, event schema or upcaster changes it", () => {
    const base = modelFingerprint(model(), defs);
    const changed = [
      modelFingerprint(model({ fold: ledger.$fold<number>({ Booked: (d, s) => s + 2 * d.amount }) }), defs),
      modelFingerprint(model({ fold: (events, s) => s + events.length }), defs),
      modelFingerprint(model({ source: ledger.$filter({ types: ["Booked"] }) }), defs),
      modelFingerprint(model({ keyBy: "otherId" }), defs),
      modelFingerprint(model({ initial: () => 0 }), defs),
      modelFingerprint(model({ initial: 1 }), defs),
      modelFingerprint(model(), defsOf(make(z.number().int()))),
      modelFingerprint(model(), defsOf(make(z.number(), (p) => ({ ...p, amount: 0 })))),
    ];
    for (const print of changed) expect(print).not.toBe(base);
    expect(new Set(changed).size).toBe(changed.length);
  });

  it("the wrappers are seen through: two defineFold or $fold results with different handlers differ, the same handlers do not", () => {
    const a = defineFold({ name: "f", version: 1, fold: ledger.$fold<number>({ Booked: (d, s) => s + d.amount }) });
    const b = defineFold({ name: "f", version: 1, fold: ledger.$fold<number>({ Booked: (d, s) => s - d.amount }) });
    const c = defineFold({ name: "f", version: 1, fold: ledger.$fold<number>({ Booked: (d, s) => s + d.amount }) });
    expect(foldFingerprint(a, source, defs)).not.toBe(foldFingerprint(b, source, defs));
    expect(foldFingerprint(a, source, defs)).toBe(foldFingerprint(c, source, defs));
    const byA = ledger.$foldBy<string, number>((e) => e.scopes.accountId, { Booked: (d, s) => (s ?? 0) + d.amount });
    const byB = ledger.$foldBy<string, number>((e) => e.data.note, { Booked: (d, s) => (s ?? 0) + d.amount });
    expect(foldFingerprint(byA, source, defs)).not.toBe(foldFingerprint(byB, source, defs)); // keyOf is hashed too
  });

  it("the documented miss: a changed captured constant or imported helper leaves it equal", () => {
    let factor = 1;
    const scaled = () => ledger.$fold<number>({ Booked: (d, s) => s + factor * d.amount });
    const before = foldFingerprint(defineFold({ name: "g", version: 1, fold: scaled() }), source, defs);
    factor = 3;
    expect(foldFingerprint(defineFold({ name: "g", version: 1, fold: scaled() }), source, defs)).toBe(before);
  });

  it("a fold reads only the schemas of the types its query names; with empty defs (a runtime without event registries) it hashes the fold alone", () => {
    const fold = defineFold({ name: "h", version: 1, fold: sum });
    const onlyClosed = ledger.$filter({ types: ["Closed"] });
    expect(foldFingerprint(fold, onlyClosed, defs)).toBe(foldFingerprint(fold, onlyClosed, defsOf(make(z.number().int()))));
    expect(foldFingerprint(fold, source, defs)).not.toBe(foldFingerprint(fold, source, defsOf(make(z.number().int()))));
    expect(foldFingerprint(fold, source, {})).not.toBe(foldFingerprint(fold, source, defs));
  });

  it("is computed once per definition and schema, then served from memory", () => {
    const def = model({ initial: 5 });
    const before = fingerprintsComputed();
    for (let i = 0; i < 100; i++) modelFingerprint(def, defs);
    expect(fingerprintsComputed() - before).toBe(1);
    const fold = defineFold({ name: "k", version: 1, fold: sum });
    const mid = fingerprintsComputed();
    for (let i = 0; i < 100; i++) foldFingerprint(fold, ledger.$scope("accountId", `a${i}`), defs);
    expect(fingerprintsComputed() - mid).toBe(1); // another scope value, the same types
  });

  it("warns once per process per (kind, name, version), never for a null stored fingerprint or an equal one; the warning names its code", async () => {
    resetFingerprintWarnings();
    const seen: (Error & { code?: string })[] = [];
    const listener = (w: Error & { code?: string }) => void (w.message.includes('"once"') && seen.push(w));
    process.on("warning", listener);
    try {
      expect(warnIfChanged("persisted fold", "once", 1, null, "b")).toBe(false);
      expect(warnIfChanged("persisted fold", "once", 1, undefined, "b")).toBe(false);
      expect(warnIfChanged("persisted fold", "once", 1, "b", "b")).toBe(false);
      expect(warnIfChanged("persisted fold", "once", 1, "a", "b")).toBe(true);
      expect(warnIfChanged("persisted fold", "once", 1, "c", "b")).toBe(false);
      expect(warnIfChanged("persisted fold", "once", 2, "c", "b")).toBe(true);
      expect(warnIfChanged("persisted read model", "once", 1, "c", "b")).toBe(true);
      await new Promise((r) => setTimeout(r, 20));
      expect(seen.map((w) => [w.name, w.code])).toEqual([
        ["EventStoreWarning", "ES_FOLD_CHANGED"],
        ["EventStoreWarning", "ES_FOLD_CHANGED"],
        ["EventStoreWarning", "ES_FOLD_CHANGED"],
      ]);
    } finally {
      process.off("warning", listener);
    }
  });

  it("the runtime hashes with its registries: a store written by a runtime with another schema for the same fold version warns once", async () => {
    const store = new MemoryStore({ schema: buildSchema([ledger], { strict: false }), persistedFolds: true });
    const fold = defineFold({ name: "schema-change", version: 1, fold: sum });
    const query = ledger.$scope("accountId", "a1");
    const first = createEventStore({ events: [ledger], store, strict: false, contextCache: { persist: { after: 1 } } });
    await first.append([ledger.Booked({ amount: 2 }, { accountId: "a1" })]);
    await first.context({ query, fold, initial: 0 });
    resetFingerprintWarnings();
    const seen: string[] = [];
    const listener = (w: Error & { code?: string }) => void (w.message.includes('"schema-change"') && seen.push(w.message));
    process.on("warning", listener);
    try {
      const changedSchema = make(z.number().int());
      const second = createEventStore({ events: [changedSchema], store, strict: false, contextCache: { persist: { after: 1 } } });
      expect((await second.context({ query, fold, initial: 0 })).state).toBe(2);
      const same = createEventStore({ events: [ledger], store, strict: false, contextCache: { persist: { after: 1 } } });
      await same.context({ query, fold, initial: 0 });
      await new Promise((r) => setTimeout(r, 20));
      expect(seen.length).toBe(1);
    } finally {
      process.off("warning", listener);
    }
  });
});
