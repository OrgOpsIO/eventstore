/**
 * The fingerprint of a kept fold (decision 42), internal: a hash of the handlers' own text, the
 * source, keyBy, the initial state and the event schemas and upcasters it reads. It may warn about
 * a forgotten version bump — once per process per (kind, name, version) — and never decides
 * anything: the version is the only signal.
 */
import { createHash } from "node:crypto";
import { initialToken } from "./context.js";
import { wrappedFoldOf } from "./foldstate.js";
import { filtersOf, queryKey } from "./query.js";
import { foldPartsOf } from "./registry.js";
import type { Query } from "./types.js";

/** What the fingerprint reads of an event type: its data schema (zod 4: `toJSONSchema`) and its upcaster. */
export type TypeDefs = Readonly<Record<string, { readonly data?: unknown; readonly upcast?: unknown } | undefined>>;

/**
 * The defs a runtime threads to its context caches (the schema seam). A cache built by hand has none: its fingerprint
 * could not see the schemas a runtime's sees, so it neither writes nor compares one (a false warning otherwise, R5.3).
 */
const typeDefsOf = new WeakMap<object, TypeDefs>();

export function setTypeDefs(owner: object, defs: TypeDefs): void {
  typeDefsOf.set(owner, defs);
}

/** The defs threaded to `owner`, or `undefined` for a cache built by hand. */
export function typeDefsFor(owner: object): TypeDefs | undefined {
  return typeDefsOf.get(owner);
}

/** Fingerprints computed in this process (each definition once per schema): for the measurer. */
let computed = 0;
export function fingerprintsComputed(): number {
  return computed;
}

const textOf = (fn: unknown): string => (typeof fn === "function" ? Function.prototype.toString.call(fn) : String(fn));

function handlersText(handlers: Readonly<Record<string, unknown>>): [string, string][] {
  return Object.keys(handlers)
    .filter((type) => typeof handlers[type] === "function")
    .sort()
    .map((type) => [type, textOf(handlers[type])]);
}

/** The fold's own text: through `defineFold`'s wrapper to the fold it wraps, through `$fold`/`$foldBy` to their handlers. */
export function foldText(fold: unknown): unknown {
  const inner = wrappedFoldOf(fold);
  if (inner !== undefined) return foldText(inner);
  const parts = foldPartsOf(fold);
  if (parts) return parts.keyOf === undefined ? ["$fold", handlersText(parts.handlers)] : ["$foldBy", textOf(parts.keyOf), handlersText(parts.handlers)];
  return ["fn", textOf(fold)];
}

function schemaText(schema: unknown): string | null {
  if (schema === undefined || schema === null) return null;
  try {
    const toJSONSchema = (schema as { toJSONSchema?: (options: unknown) => unknown }).toJSONSchema;
    if (typeof toJSONSchema !== "function") return "?";
    return JSON.stringify(toJSONSchema.call(schema, { unrepresentable: "any" }));
  } catch {
    return "?";
  }
}

/** The event types the query's positive filters name (every declared type for a filter without `types`), sorted; `null` for an invalid query. */
function typeNames(query: Query, defs: TypeDefs): string[] | null {
  try {
    const names = new Set<string>();
    for (const filter of filtersOf(query)) for (const type of filter.types ?? Object.keys(defs)) names.add(type);
    return [...names].sort();
  } catch {
    return null;
  }
}

/** Per event type the query reads: its schema and upcaster text. */
export function typesText(query: Query, defs: TypeDefs): unknown[] {
  const names = typeNames(query, defs);
  if (names === null) return ["?"];
  return names.map((type) => {
    const def = defs[type];
    return [type, schemaText(def?.data), typeof def?.upcast === "function" ? textOf(def.upcast) : null];
  });
}

function hash(parts: unknown[]): string {
  computed++;
  return createHash("sha256").update(JSON.stringify(parts)).digest("hex");
}

const foldPrints = new WeakMap<object, WeakMap<TypeDefs, Map<string, string>>>();
const modelPrints = new WeakMap<object, WeakMap<TypeDefs, string>>();

/** A defined fold over a query (the persisted fold tier): the query and the initial are in the state key already. */
export function foldFingerprint(fold: object, query: Query, defs: TypeDefs): string {
  let byDefs = foldPrints.get(fold);
  if (!byDefs) foldPrints.set(fold, (byDefs = new WeakMap()));
  let byTypes = byDefs.get(defs);
  if (!byTypes) byDefs.set(defs, (byTypes = new Map()));
  // keyed by the type names, so a load reads no schema once its fold and types were hashed
  const id = JSON.stringify(typeNames(query, defs));
  let print = byTypes.get(id);
  if (print === undefined) byTypes.set(id, (print = hash(["fold/1", foldText(fold), typesText(query, defs)])));
  return print;
}

/** A read model definition (persisted model, live start): fold, source, keyBy, initial and the types it reads. */
export function modelFingerprint(
  definition: { readonly fold: unknown; readonly source: Query; readonly keyBy: string; readonly initial: unknown; readonly processor?: string },
  defs: TypeDefs,
): string {
  let byDefs = modelPrints.get(definition);
  if (!byDefs) modelPrints.set(definition, (byDefs = new WeakMap()));
  let print = byDefs.get(defs);
  if (print === undefined) {
    // who writes (decision 45 §1) enters only when set: a definition without it hashes exactly as before
    const writer = definition.processor === undefined ? [] : [["processor", definition.processor]];
    print = hash(["model/1", foldText(definition.fold), queryKey(definition.source), definition.keyBy, initialToken(definition.initial), typesText(definition.source, defs), ...writer]);
    byDefs.set(defs, print);
  }
  return print;
}

const warned = new Set<string>();

/** Tests only: forget which warnings this process gave. */
export function resetFingerprintWarnings(): void {
  warned.clear();
}

/**
 * Compare a stored fingerprint with this process's: warn once per process per (kind, name, version) when they differ.
 * A stored `null` / absent fingerprint is never compared. Returns whether it warned now.
 */
export function warnIfChanged(kind: "persisted fold" | "persisted read model" | "read model", name: string, version: number, stored: string | null | undefined, mine: string): boolean {
  if (stored === null || stored === undefined || stored === mine) return false;
  const id = JSON.stringify([kind, name, version]);
  if (warned.has(id)) return false;
  warned.add(id);
  const sees = kind === "persisted fold" ? "the fold's handlers and the event schemas and upcasters it reads" : "the fold's handlers, the source, keyBy, the initial state and the event schemas and upcasters it reads";
  const message =
    `eventstore: ${kind} "${name}" v${version}: the stored state was written by a different fold (fingerprint ${stored.slice(0, 4)}…≠${mine.slice(0, 4)}…) under the same version — ` +
    "if what the fold computes changed, bump the version (the version is the only signal; a stale state is served until you do). " +
    `This check sees ${sees}; it cannot see imported helpers or captured values, and a different build of the same code may warn once.`;
  const proc = (globalThis as { process?: { emitWarning?: (message: string, options: { type: string; code: string }) => void } }).process;
  if (typeof proc?.emitWarning === "function") proc.emitWarning(message, { type: "EventStoreWarning", code: "ES_FOLD_CHANGED" });
  return true;
}
