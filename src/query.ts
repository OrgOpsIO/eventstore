import { fnv1a64 } from "./ids.js";
import { UnindexableContextError } from "./errors.js";
import type { Filter, NewEvent, Query, RecordedEvent, StoreSchema } from "./types.js";

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** Normalise a query into a non-empty list of filters with canonical shapes. */
export function filtersOf(query: Query): Filter[] {
  const list = Array.isArray(query) ? (query as readonly Filter[]) : [query as Filter];
  if (list.length === 0) throw new Error("eventstore: a query needs at least one filter");
  return list.map(normaliseFilter);
}

export function normaliseFilter(filter: Filter): Filter {
  const out: { types?: string[]; scopes?: Record<string, string[]>; where?: Record<string, unknown>[] } = {};
  if (filter.types !== undefined) {
    if (filter.types.length === 0) throw new Error("eventstore: `types` must not be empty (omit it to match all types)");
    out.types = [...new Set(filter.types)].sort();
  }
  if (filter.scopes !== undefined) {
    const scopes: Record<string, string[]> = {};
    for (const key of Object.keys(filter.scopes).sort()) {
      if (!IDENTIFIER.test(key)) throw new Error(`eventstore: invalid scope key "${key}"`);
      const raw = filter.scopes[key];
      const values = typeof raw === "string" ? [raw] : [...new Set(raw ?? [])].sort();
      if (values.length === 0) throw new Error(`eventstore: scope "${key}" needs at least one value`);
      scopes[key] = values;
    }
    if (Object.keys(scopes).length > 0) out.scopes = scopes;
  }
  if (filter.where !== undefined && filter.where.length > 0) {
    out.where = filter.where.map((w) => ({ ...w }));
  }
  return out;
}

/** Stable string key of a query — used for context caches and lock derivation. */
export function queryKey(query: Query): string {
  return JSON.stringify(filtersOf(query));
}

/** The scope value of a recorded event for a key (see `Filter.scopes` docs). */
export function scopeValueOf(event: RecordedEvent, key: string, idKey: string): string | undefined {
  const fromScopes = event.scopes[key];
  if (fromScopes !== undefined) return fromScopes;
  if (key === idKey) return event.id;
  const flat = (event.data as Record<string, unknown>)[key];
  return typeof flat === "string" ? flat : undefined;
}

/** Wire payload = `{ [idKey]: id, ...data, scopes? }` (Westphal / an earlier in-house store convention). */
export function toPayload(event: NewEvent & { id: string }, idKey: string): Record<string, unknown> {
  const payload: Record<string, unknown> = { [idKey]: event.id, ...event.data };
  if (event.scopes && Object.keys(event.scopes).length > 0) payload.scopes = { ...event.scopes };
  return payload;
}

export function fromPayload(
  payload: Record<string, unknown>,
  idKey: string,
): { id: string; data: Record<string, unknown>; scopes: Record<string, string> } {
  const { [idKey]: rawId, scopes: rawScopes, ...data } = payload;
  const scopes: Record<string, string> = {};
  if (rawScopes && typeof rawScopes === "object" && !Array.isArray(rawScopes)) {
    for (const [k, v] of Object.entries(rawScopes as Record<string, unknown>)) {
      if (typeof v === "string") scopes[k] = v;
    }
  }
  return { id: typeof rawId === "string" ? rawId : "", data, scopes };
}

/** Rico Fritzsche's containment semantics (`payload @> predicate`), reimplemented for memory. */
export function contains(payload: unknown, predicate: unknown): boolean {
  if (predicate === null || predicate === undefined) return true;
  if (payload === null || payload === undefined) return false;
  if (typeof predicate !== "object" || typeof payload !== "object") return predicate === payload;
  if (Array.isArray(predicate) && Array.isArray(payload)) {
    return predicate.every((p) => payload.some((v) => contains(v, p)));
  }
  if (Array.isArray(predicate) !== Array.isArray(payload)) return false;
  const p = predicate as Record<string, unknown>;
  const v = payload as Record<string, unknown>;
  for (const key of Object.keys(p)) {
    if (!(key in v)) return false;
    if (!contains(v[key], p[key])) return false;
  }
  return true;
}

/** In-memory evaluation of one normalised filter against a recorded event. */
export function matchesFilter(event: RecordedEvent, filter: Filter, schema: StoreSchema): boolean {
  if (filter.types && !filter.types.includes(event.type)) return false;
  if (filter.scopes) {
    const idKey = schema.idKeyOf(event.type);
    for (const [key, values] of Object.entries(filter.scopes)) {
      const value = scopeValueOf(event, key, idKey);
      if (value === undefined || !(values as readonly string[]).includes(value)) return false;
    }
  }
  if (filter.where) {
    const payload = toPayload({ ...event, id: event.id }, schema.idKeyOf(event.type));
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
 */
export function conditionLockKeys(
  query: Query,
  schema: StoreSchema,
): { keys: bigint[]; exclusiveGlobal: boolean } {
  const keys = new Set<bigint>();
  let exclusiveGlobal = false;
  for (const filter of filtersOf(query)) {
    const declared = Object.entries(filter.scopes ?? {}).filter(([k]) => schema.scopeKeys.includes(k));
    if (declared.length === 0) {
      if (schema.strict) {
        throw new UnindexableContextError(
          filter.scopes
            ? `filter uses only undeclared scope keys ${Object.keys(filter.scopes).join(", ")}`
            : `filter ${JSON.stringify(filter)} has no scope`,
        );
      }
      exclusiveGlobal = true;
      continue;
    }
    for (const [key, values] of declared) {
      for (const value of values as readonly string[]) keys.add(scopeLockKey(key, value));
    }
  }
  return { keys: [...keys].sort(compareBigint), exclusiveGlobal };
}

/** Advisory-lock keys an append must hold so that every condition it could affect sees it. */
export function eventLockKeys(events: readonly (NewEvent & { id: string })[], schema: StoreSchema): bigint[] {
  const keys = new Set<bigint>();
  for (const event of events) {
    const idKey = schema.idKeyOf(event.type);
    if (schema.scopeKeys.includes(idKey)) keys.add(scopeLockKey(idKey, event.id));
    for (const key of schema.scopeKeys) {
      const inScopes = event.scopes?.[key];
      if (inScopes !== undefined) keys.add(scopeLockKey(key, inScopes));
      const flat = (event.data as Record<string, unknown>)[key];
      if (typeof flat === "string") keys.add(scopeLockKey(key, flat));
    }
  }
  return [...keys].sort(compareBigint);
}

export const GLOBAL_LOCK_KEY = fnv1a64("@orgops/eventstore:global");

export function scopeLockKey(key: string, value: string): bigint {
  return fnv1a64(`scope:${key}=${value}`);
}

function compareBigint(a: bigint, b: bigint): number {
  return a < b ? -1 : a > b ? 1 : 0;
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
