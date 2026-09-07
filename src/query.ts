import { fnv1a64, lockKeyOf } from "./ids.js";
import { UnindexableContextError, UsageError, ValidationError } from "./errors.js";
import type { Filter, NewEvent, Query, QueryOptions, RecordedEvent, StoreSchema } from "./types.js";

export const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const MAX_CONTAINS_DEPTH = 64;
/** Caps on query shapes: every scope value becomes an advisory lock and an index probe. */
export const QUERY_LIMITS = { filters: 64, scopeValuesPerKey: 256, wherePredicates: 64 } as const;

/** Normalise a query into a non-empty list of filters with canonical shapes. */
export function filtersOf(query: Query): Filter[] {
  const list = Array.isArray(query) ? (query as readonly Filter[]) : [query as Filter];
  if (list.length === 0) throw new UsageError("eventstore: a query needs at least one filter");
  if (list.length > QUERY_LIMITS.filters) throw new UsageError(`eventstore: a query may have at most ${QUERY_LIMITS.filters} filters`);
  return list.map(normaliseFilter);
}

export function normaliseFilter(filter: Filter): Filter {
  const out: { types?: string[]; scopes?: Record<string, string[]>; where?: Record<string, unknown>[] } = {};
  if (filter.types !== undefined) {
    // an empty list is a legitimate dynamic result and matches nothing
    out.types = [...new Set(filter.types)].sort();
  }
  if (filter.scopes !== undefined) {
    const scopes: Record<string, string[]> = {};
    for (const key of Object.keys(filter.scopes).sort()) {
      if (!IDENTIFIER.test(key)) throw new UsageError(`eventstore: invalid scope key "${key}"`);
      const raw = filter.scopes[key];
      if (typeof raw !== "string" && !Array.isArray(raw)) throw new UsageError(`eventstore: scope "${key}" must be a string or an array of strings`);
      const values = typeof raw === "string" ? [raw] : [...new Set(raw)].sort();
      if (values.length === 0) throw new UsageError(`eventstore: scope "${key}" needs at least one value`);
      if (values.length > QUERY_LIMITS.scopeValuesPerKey) throw new UsageError(`eventstore: scope "${key}" may have at most ${QUERY_LIMITS.scopeValuesPerKey} values`);
      for (const v of values) if (typeof v !== "string") throw new UsageError(`eventstore: scope "${key}" values must be strings`);
      scopes[key] = values;
    }
    if (Object.keys(scopes).length > 0) out.scopes = scopes;
  }
  if (filter.where !== undefined && filter.where.length > QUERY_LIMITS.wherePredicates) {
    throw new UsageError(`eventstore: a filter may have at most ${QUERY_LIMITS.wherePredicates} where predicates`);
  }
  if (filter.where !== undefined && filter.where.length > 0) {
    // the wire format is JSON: `undefined` keys vanish, Dates become strings, NaN becomes null
    out.where = filter.where.map((w) => JSON.parse(JSON.stringify(w)) as Record<string, unknown>);
  }
  return out;
}

/** Validate query options so both stores reject the same inputs. */
export function normaliseOptions(options: QueryOptions = {}): QueryOptions {
  if (options.after !== undefined && !(Number.isSafeInteger(options.after) && options.after >= 0)) {
    throw new Error(`eventstore: \`after\` must be a non-negative safe integer, got ${String(options.after)}`);
  }
  if (options.limit !== undefined && !(Number.isSafeInteger(options.limit) && options.limit >= 0)) {
    throw new Error(`eventstore: \`limit\` must be a non-negative safe integer, got ${String(options.limit)}`);
  }
  if (options.cursor) {
    const c = options.cursor;
    if (typeof c.transactionId !== "string" || !/^\d+$/.test(c.transactionId) || !Number.isSafeInteger(c.sequence) || c.sequence < 0) {
      throw new Error("eventstore: `cursor` must be { transactionId: decimal string, sequence: non-negative safe integer }");
    }
  }
  if (options.order !== undefined && options.order !== "asc" && options.order !== "desc") {
    throw new Error(`eventstore: \`order\` must be "asc" or "desc"`);
  }
  return options;
}

/** Stable string key of a query — used for context caches and lock derivation. */
export function queryKey(query: Query): string {
  return JSON.stringify(filtersOf(query));
}

/** A filter over every event type that happened in relation to one event (cross-registry). */
export function scope(key: string, value: string | readonly string[]): Filter {
  return { scopes: { [key]: value } };
}

/** The scope value of a recorded event for a key (see `Filter.scopes` docs). */
export function scopeValueOf(event: RecordedEvent<string, unknown>, key: string, idKey: string): string | undefined {
  const fromScopes = event.scopes[key];
  if (fromScopes !== undefined) return fromScopes;
  if (key === idKey) return event.id;
  const data = event.data as Record<string, unknown> | null;
  const flat = data && typeof data === "object" ? data[key] : undefined;
  return typeof flat === "string" ? flat : undefined;
}

/** Wire payload = `{ [idKey]: id, ...data, scopes? }` (the *Scoping Events* convention). The id always wins. */
export function toPayload(event: NewEvent & { id: string }, idKey: string): Record<string, unknown> {
  const { scopes: _ignored, ...data } = event.data as Record<string, unknown>;
  const payload: Record<string, unknown> = { ...data, [idKey]: event.id };
  if (event.scopes && Object.keys(event.scopes).length > 0) payload.scopes = { ...event.scopes };
  return payload;
}

export function fromPayload(
  payload: Record<string, unknown>,
  idKey: string,
): { id: string | undefined; data: Record<string, unknown>; scopes: Record<string, string> } {
  const { [idKey]: rawId, scopes: rawScopes, ...data } = payload;
  const scopes: Record<string, string> = {};
  if (rawScopes && typeof rawScopes === "object" && !Array.isArray(rawScopes)) {
    for (const [k, v] of Object.entries(rawScopes as Record<string, unknown>)) {
      if (typeof v === "string") scopes[k] = v;
    }
  }
  return { id: typeof rawId === "string" ? rawId : undefined, data, scopes };
}

/**
 * Envelope rules every store enforces before writing, so memory and Postgres agree and the
 * wire payload can never lie about an event's identity:
 * - `data` must not contain the event's own id key or a `scopes` key
 * - a value in `data` at a declared scope key must be a string (it is a scope value)
 * - `scopes` values must be non-empty strings with identifier keys
 */
export function validateEnvelope(event: NewEvent, schema: Pick<StoreSchema, "idKeyOf" | "scopeKeys">): void {
  const issues: { message: string; path?: (string | number)[] }[] = [];
  const idKey = schema.idKeyOf(event.type);
  const data = event.data as Record<string, unknown> | null | undefined;
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    throw new ValidationError(event.type, [{ message: "data must be an object" }]);
  }
  if (Object.prototype.hasOwnProperty.call(data, idKey)) issues.push({ message: `data must not contain the event's own id key "${idKey}"`, path: [idKey] });
  if (Object.prototype.hasOwnProperty.call(data, "scopes")) issues.push({ message: "data must not contain a `scopes` key; pass scopes separately", path: ["scopes"] });
  for (const key of schema.scopeKeys) {
    // `null`/`undefined` mean "no such scope" in both stores (es_scope yields NULL, memory ignores it)
    if (Object.prototype.hasOwnProperty.call(data, key) && data[key] !== undefined && data[key] !== null && typeof data[key] !== "string") {
      issues.push({ message: `"${key}" is a declared scope key; its value must be a string or null`, path: [key] });
    }
  }
  for (const [key, value] of Object.entries(event.scopes ?? {})) {
    if (!IDENTIFIER.test(key)) issues.push({ message: `invalid scope key "${key}"`, path: ["scopes", key] });
    if (typeof value !== "string" || value === "") issues.push({ message: `scope "${key}" must be a non-empty string`, path: ["scopes", key] });
  }
  if (event.id !== undefined && (typeof event.id !== "string" || event.id === "")) issues.push({ message: "id must be a non-empty string", path: ["id"] });
  if (issues.length > 0) throw new ValidationError(event.type, issues);
}

/** Rico Fritzsche's containment semantics (`payload @> predicate`), reimplemented for memory. */
export function contains(payload: unknown, predicate: unknown, depth = 0): boolean {
  if (depth > MAX_CONTAINS_DEPTH) throw new Error("eventstore: `where` predicate nested too deeply");
  if (predicate === null) return payload === null;
  if (predicate === undefined) return true;
  if (payload === null || payload === undefined) return false;
  if (typeof predicate !== "object" || typeof payload !== "object") return predicate === payload;
  if (Array.isArray(predicate) && Array.isArray(payload)) {
    return predicate.every((p) => payload.some((v) => contains(v, p, depth + 1)));
  }
  if (Array.isArray(predicate) !== Array.isArray(payload)) return false;
  const p = predicate as Record<string, unknown>;
  const v = payload as Record<string, unknown>;
  for (const key of Object.keys(p)) {
    if (!Object.prototype.hasOwnProperty.call(v, key)) return false;
    if (!contains(v[key], p[key], depth + 1)) return false;
  }
  return true;
}

/** In-memory evaluation of one normalised filter against a recorded event. */
export function matchesFilter(event: RecordedEvent<string, unknown>, filter: Filter, schema: StoreSchema): boolean {
  if (filter.types && !filter.types.includes(event.type)) return false;
  if (filter.scopes) {
    const idKey = schema.idKeyOf(event.type);
    for (const [key, values] of Object.entries(filter.scopes)) {
      const value = scopeValueOf(event, key, idKey);
      if (value === undefined || !(values as readonly string[]).includes(value)) return false;
    }
  }
  if (filter.where) {
    const payload = toPayload({ ...event, data: event.data as Record<string, unknown> }, schema.idKeyOf(event.type));
    if (!filter.where.some((w) => contains(payload, w))) return false;
  }
  return true;
}

/**
 * Advisory-lock keys for a condition query.
 *
 * A filter that names at least one declared scope key is locked on every `(key, value)` pair
 * of its declared keys: any event that can enter that context must carry one of those pairs
 * and therefore takes the same lock when it is appended. A filter without a declared scope key
 * cannot be locked that way; in strict mode that is an error, otherwise the append takes the
 * exclusive global lock while every other append holds it shared.
 *
 * The tenant scope key is special: a filter that has other declared keys locks only those
 * (the tenant key would serialise the whole tenant); a filter that has ONLY the tenant key
 * locks the tenant key exclusively, while every append holds it shared — same pattern as the
 * global lock, one level down.
 */
export function conditionLockKeys(
  query: Query,
  schema: StoreSchema,
): { keys: bigint[]; exclusiveGlobal: boolean; exclusiveTenants: bigint[] } {
  const keys = new Set<bigint>();
  const exclusiveTenants = new Set<bigint>();
  let exclusiveGlobal = false;
  const tenantKey = schema.tenantScopeKey;
  for (const filter of filtersOf(query)) {
    const declared = Object.entries(filter.scopes ?? {}).filter(([k]) => schema.scopeKeys.includes(k));
    const nonTenant = declared.filter(([k]) => k !== tenantKey);
    if (nonTenant.length > 0) {
      for (const [key, values] of nonTenant) for (const value of values as readonly string[]) keys.add(scopeLockKey(key, value, schema.lockSalt));
      continue;
    }
    const tenantOnly = declared.find(([k]) => k === tenantKey);
    if (tenantOnly) {
      for (const value of tenantOnly[1] as readonly string[]) exclusiveTenants.add(scopeLockKey(tenantKey!, value, schema.lockSalt));
      continue;
    }
    if (schema.strict) {
      throw new UnindexableContextError(
        filter.scopes
          ? `filter uses only undeclared scope keys ${Object.keys(filter.scopes).join(", ")}`
          : `filter ${JSON.stringify(filter)} has no scope`,
      );
    }
    exclusiveGlobal = true;
  }
  return { keys: [...keys].sort(compareBigint), exclusiveGlobal, exclusiveTenants: [...exclusiveTenants].sort(compareBigint) };
}

/**
 * Advisory-lock keys an append must hold so that every condition it could affect sees it:
 * exclusive on each `(declaredKey, value)` pair the event carries (own id, scopes, flat data
 * field) — except the tenant key, which appends hold shared (`sharedTenants`).
 */
export function eventLockKeys(
  events: readonly (NewEvent & { id: string })[],
  schema: StoreSchema,
): { keys: bigint[]; sharedTenants: bigint[] } {
  const keys = new Set<bigint>();
  const sharedTenants = new Set<bigint>();
  const tenantKey = schema.tenantScopeKey;
  const add = (key: string, value: string) => {
    if (key === tenantKey) sharedTenants.add(scopeLockKey(key, value, schema.lockSalt));
    else keys.add(scopeLockKey(key, value, schema.lockSalt));
  };
  for (const event of events) {
    const idKey = schema.idKeyOf(event.type);
    if (schema.scopeKeys.includes(idKey)) add(idKey, event.id);
    for (const key of schema.scopeKeys) {
      const inScopes = event.scopes?.[key];
      if (inScopes !== undefined) add(key, inScopes);
      const flat = (event.data as Record<string, unknown>)[key];
      if (typeof flat === "string") add(key, flat);
    }
  }
  return { keys: [...keys].sort(compareBigint), sharedTenants: [...sharedTenants].sort(compareBigint) };
}

/** The unsalted global lock key. Stores should prefer `globalLockKey(schema.lockSalt)`. */
export const GLOBAL_LOCK_KEY = fnv1a64("@orgops/eventstore:global");

/** The global advisory-lock key, salted per deployment when `lockSalt` is configured. */
export function globalLockKey(salt?: string): bigint {
  return salt ? lockKeyOf("@orgops/eventstore:global", salt) : GLOBAL_LOCK_KEY;
}

/** The install advisory-lock key (session-independent, salted like the others). */
export function installLockKey(salt?: string): bigint {
  return lockKeyOf("@orgops/eventstore:install", salt);
}

/** The advisory-lock key of one `(scopeKey, value)` pair, salted per deployment when `lockSalt` is configured. */
export function scopeLockKey(key: string, value: string, salt?: string): bigint {
  return lockKeyOf(`scope:${key}=${value}`, salt);
}

function compareBigint(a: bigint, b: bigint): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/** Compare two `(transactionId, sequence)` cursors. */
export function compareCursor(a: { transactionId: string; sequence: number }, b: { transactionId: string; sequence: number }): number {
  const x = BigInt(a.transactionId);
  const y = BigInt(b.transactionId);
  if (x !== y) return x < y ? -1 : 1;
  return a.sequence - b.sequence;
}

/** Split a unique path like `scopes.magicLinkRequestedId` into JSON path segments. */
export function uniquePathSegments(path: string): string[] {
  const segments = path.split(".");
  for (const s of segments) if (!IDENTIFIER.test(s)) throw new Error(`eventstore: invalid unique path "${path}"`);
  return segments;
}

export function valueAtPath(payload: Record<string, unknown>, segments: readonly string[]): unknown {
  let cur: unknown = payload;
  for (const s of segments) {
    if (cur === null || typeof cur !== "object") return undefined;
    cur = (cur as Record<string, unknown>)[s];
  }
  return cur;
}
