import { deserialize, serialize } from "node:v8";
import type { Fold } from "./context.js";
import { UsageError } from "./errors.js";

/** The stable name and version of a kept fold. A version change means another state: a rebuild. */
export interface FoldIdentity {
  readonly name: string;
  readonly version: number;
}

/** A fold that carries a fold identity — the only kind the persisted fold tier keeps. */
export type DefinedFold<S> = Fold<S> & { readonly identity: FoldIdentity };

const MAX_NAME_LENGTH = 200;
const MAX_VERSION = 2_147_483_647; // stored in an `integer` column

/**
 * Give a fold an identity, so its state can be kept across processes (the persisted fold tier,
 * on a store with `persistedFolds: true`). Define it once, at module level, like every fold.
 *
 * Bump `version` whenever what the fold computes changes: its code, the event schemas or
 * upcasters it sees, or what the initial state means. The version is the only signal — a stored
 * state of another version is never used.
 *
 * ```ts
 * export const balance = defineFold({ name: "balance", version: 1, fold: ledger.$fold<number>({ EntryBooked: (d, s) => s + d.amount }) });
 * await es.context({ query, fold: balance, initial: 0 });
 * ```
 */
export function defineFold<S>(definition: { readonly name: string; readonly version: number; readonly fold: Fold<S> }): DefinedFold<S> {
  const { name, version, fold } = definition ?? ({} as typeof definition);
  checkIdentity("defineFold", name, version);
  if (typeof fold !== "function") throw new UsageError(`eventstore: defineFold("${name}") needs a fold function`);
  // a new function: the given fold stays untouched, so one fold can sit under two definitions
  const defined = ((events, state) => fold(events, state)) as Fold<S>;
  Object.defineProperty(defined, "identity", { value: Object.freeze({ name, version }), enumerable: false, writable: false, configurable: false });
  wrapped.set(defined, fold);
  return defined as DefinedFold<S>;
}

/** `defineFold`'s result → the fold it wraps: the fingerprint hashes that one, not the wrapper (the same text for every fold). */
const wrapped = new WeakMap<object, unknown>();

/** The fold a `defineFold` result wraps, or `undefined` for any other value. */
export function wrappedFoldOf(fold: unknown): unknown {
  return typeof fold === "function" ? wrapped.get(fold) : undefined;
}

/** The name and version rules of every kept fold (`defineFold`, `defineReadModel`); `by` names the caller in the message. */
export function checkIdentity(by: string, name: unknown, version: unknown): void {
  if (typeof name !== "string" || name.length === 0 || name.length > MAX_NAME_LENGTH || name.includes("\u0000")) {
    throw new UsageError(
      `eventstore: ${by} needs a name of 1 to ${MAX_NAME_LENGTH} characters without NUL (it is stored as Postgres text), got ${typeof name === "string" ? `${name.length} characters` : String(name)}`,
    );
  }
  if (!Number.isSafeInteger(version) || (version as number) < 1 || (version as number) > MAX_VERSION) {
    throw new UsageError(`eventstore: ${by}("${name}") needs a version that is an integer from 1 to ${MAX_VERSION}, got ${String(version)}`);
  }
}

/** The identity of a fold made by `defineFold`, or `undefined` for any other fold. */
export function foldIdentityOf(fold: unknown): FoldIdentity | undefined {
  if (typeof fold !== "function") return undefined;
  const identity = (fold as { identity?: unknown }).identity;
  if (typeof identity !== "object" || identity === null) return undefined;
  const { name, version } = identity as { name?: unknown; version?: unknown };
  if (typeof name !== "string" || name.length === 0 || !Number.isSafeInteger(version)) return undefined;
  return identity as FoldIdentity;
}

/**
 * Whether a state survives the codec unchanged. Accepts primitives (not symbols), plain objects
 * (prototype `Object.prototype`), arrays, `Map`, `Set` and `Date`, nested. Anything else — a
 * class instance, a function, a null-prototype object, a symbol key, a typed array — would come
 * back different, so it is refused; so is a property the codec drops or changes: any own property
 * of a `Map`, `Set` or `Date` (only entries and time are kept), and a non-enumerable property or a
 * getter/setter on an object or array (only enumerable data properties are kept, as values).
 * Returns the `UsageError` naming the first offending path, or `null` when the state is storable.
 * An explicit stack: a deep state cannot overflow the call stack.
 */
export function checkStorable(state: unknown, identity: FoldIdentity): UsageError | null {
  const seen = new Set<object>();
  const stack: [unknown, string][] = [[state, "state"]];
  while (stack.length > 0) {
    const [value, path] = stack.pop()!;
    const problem = refusalOf(value);
    if (problem !== null) return refused(identity, path, `is ${problem}`);
    if (typeof value !== "object" || value === null || seen.has(value)) continue;
    seen.add(value);
    if (value instanceof Map) {
      let i = 0;
      for (const [k, v] of value) {
        stack.push([k, `${path}.keys()[${i}]`], [v, `${path}.get(${describeKey(k)})`]);
        i++;
      }
    } else if (value instanceof Set) {
      let i = 0;
      for (const v of value) stack.push([v, `${path}.values()[${i++}]`]);
    } else if (!(value instanceof Date)) {
      // an object or an array: every own property, so one the codec would drop or change is named
      const isArray = Array.isArray(value);
      for (const k of Object.getOwnPropertyNames(value)) {
        if (isArray && k === "length") continue;
        const d = Object.getOwnPropertyDescriptor(value, k)!;
        const problem = d.get || d.set ? "getter or setter" : !d.enumerable ? "non-enumerable property" : null;
        if (problem !== null) return refused(identity, path, `has a ${problem} ${JSON.stringify(k)}`);
        stack.push([d.value, isArray && isIndex(k) ? `${path}[${k}]` : `${path}.${k}`]);
      }
    }
  }
  return null;
}

function refused(identity: FoldIdentity, path: string, problem: string): UsageError {
  return new UsageError(
    `eventstore: the state of fold "${identity.name}" v${identity.version} cannot be persisted — ${path} ${problem}. ` +
      "A persisted fold keeps plain objects, arrays, Map, Set, Date and primitives; the state stays correct in memory but is not stored",
  );
}

function isIndex(key: string): boolean {
  return /^(?:0|[1-9]\d*)$/.test(key);
}

/** Why one value (not its children) is not storable, or `null`. */
function refusalOf(value: unknown): string | null {
  switch (typeof value) {
    case "function":
      return "a function";
    case "symbol":
      return "a symbol";
    case "object":
      break;
    default:
      return null;
  }
  if (value === null) return null;
  const proto = Object.getPrototypeOf(value);
  const allowed = proto === Object.prototype || proto === Array.prototype || proto === Map.prototype || proto === Set.prototype || proto === Date.prototype;
  if (!allowed) {
    if (proto === null) return "an object without prototype (Object.create(null))";
    const name = (proto as { constructor?: { name?: unknown } }).constructor?.name;
    return `a class instance (${typeof name === "string" && name.length > 0 ? name : "anonymous"})`;
  }
  if (Object.getOwnPropertySymbols(value).length > 0) return "an object with symbol keys";
  if (proto === Map.prototype || proto === Set.prototype || proto === Date.prototype) {
    // the codec keeps a Map's and a Set's entries and a Date's time, nothing else
    const own = Object.getOwnPropertyNames(value);
    if (own.length > 0) return `a ${proto === Map.prototype ? "Map" : proto === Set.prototype ? "Set" : "Date"} with an own property ${JSON.stringify(own[0])}`;
  }
  return null;
}

function describeKey(key: unknown): string {
  return typeof key === "string" ? JSON.stringify(key).slice(0, 60) : typeof key === "object" && key !== null ? "<object>" : String(key);
}

/** The bytes of a storable state (`node:v8` serialization). */
export function encodeState(state: unknown): Uint8Array {
  return serialize(state);
}

/** A state back from its bytes. Throws on bytes it cannot read (corrupt, or written by a newer Node) — the caller treats that as a miss. */
export function decodeState(bytes: Uint8Array): unknown {
  return deserialize(bytes);
}
