import type { ZodType, output } from "zod";
import { defaultIdKey, uuidv7 } from "./ids.js";
import { UsageError, ValidationError } from "./errors.js";
import { IDENTIFIER, uniquePathSegments, validateEnvelope } from "./query.js";
import type { Fold } from "./context.js";
import type { Filter, Metadata, NewEvent, RecordedEvent, Scopes, StoreSchema } from "./types.js";

/** One event type declaration. */
export interface EventDefinition<S extends ZodType = ZodType, K extends string = string> {
  /** Zod schema of the domain data (everything except the own id and `scopes`). */
  readonly data: S;
  /** Scope keys this event MUST carry. They are indexed and locked by the store. */
  readonly scopes?: readonly K[];
  /** Scope keys this event MAY carry. Also indexed and locked. */
  readonly optionalScopes?: readonly K[];
  /** Payload paths that must be unique across all events of this type, e.g. `"email"` or `"scopes.magicLinkRequestedId"`. */
  readonly unique?: readonly string[];
  /** Own-id key on the wire; defaults to `<lowerFirst(Type)>Id`. */
  readonly idKey?: string;
  /**
   * Upcast an older stored payload to the current shape (runs on read, before parsing). It may
   * reshape `data` only: the own id and the `scopes` object are read from the stored payload,
   * because that is what the store indexed, locked and matched on.
   */
  readonly upcast?: (payload: Record<string, unknown>) => Record<string, unknown>;
}

/** The shape `defineEvents` takes: event type name → definition. */
export type Definitions = Record<string, EventDefinition>;

type ScopeKeysOf<Def extends EventDefinition> = Def extends EventDefinition<ZodType, infer K> ? K : never;
type RequiredScopeKeys<Def extends EventDefinition> = Def["scopes"] extends readonly (infer K extends string)[] ? K : never;
type OptionalScopeKeys<Def extends EventDefinition> = Def["optionalScopes"] extends readonly (infer K extends string)[] ? K : never;

/** The exact scopes a creator accepts: required keys, optional keys, nothing else. */
export type ScopesInputOf<Def extends EventDefinition> = Record<RequiredScopeKeys<Def>, string> & Partial<Record<OptionalScopeKeys<Def>, string>>;

/** The scopes a recorded event of this type carries (extra keys such as the tenant stamp are tolerated). */
export type ScopesOf<Def extends EventDefinition> = ScopesInputOf<Def> & Scopes;

type ScopesArg<Def extends EventDefinition> = [RequiredScopeKeys<Def>] extends [never]
  ? [OptionalScopeKeys<Def>] extends [never]
    ? [scopes?: Record<string, never>] // no scope keys at all: only `{}` is accepted
    : [scopes?: Partial<Record<OptionalScopeKeys<Def>, string>>]
  : [scopes: ScopesInputOf<Def>];

/** The parsed data type of one event definition. */
export type DataOf<Def extends EventDefinition> = output<Def["data"]>;

/** The union of events a registry's creators produce. */
export type NewEventOf<D extends Definitions, N extends keyof D & string = keyof D & string> = {
  [K in N]: NewEvent<K, DataOf<D[K]>, ScopesOf<D[K]>> & { readonly id: string; readonly scopes: ScopesOf<D[K]> };
}[N];

/** The union of recorded events of a registry (narrow on `type`). */
export type RecordedEventOf<D extends Definitions, N extends keyof D & string = keyof D & string> = {
  [K in N]: RecordedEvent<K, DataOf<D[K]>, ScopesOf<D[K]>>;
}[N];

/** Every scope key any event of the registry declares. */
export type AllScopeKeys<D extends Definitions> = { [K in keyof D]: ScopeKeysOf<D[K]> }[keyof D];

/** Optional explicit `id` and envelope `metadata` for a creator call. */
export interface CreateOptions {
  readonly id?: string;
  readonly metadata?: Metadata;
}

/** A typed event constructor: `(data, scopes?, options?) => NewEvent` with the id generated. */
export type Creator<D extends Definitions, N extends keyof D & string> = (
  data: DataOf<D[N]>,
  ...rest: [...ScopesArg<D[N]>, options?: CreateOptions]
) => NewEventOf<D, N>;

/** One handler per event type, each receiving typed `data`, the state, and the typed recorded event. */
export type FoldHandlers<D extends Definitions, S> = {
  readonly [N in keyof D & string]?: (data: DataOf<D[N]>, state: S, event: RecordedEventOf<D, N>) => S;
};

/** A filter restricted to this registry's types and scope keys. */
export interface RegistryFilter<D extends Definitions> {
  readonly types?: readonly (keyof D & string)[];
  readonly scopes?: Partial<Record<AllScopeKeys<D> | (string & {}), string | readonly string[]>>;
  readonly where?: readonly Readonly<Record<string, unknown>>[];
}

/** The structural part of a registry that stores and configs need — accepts any `defineEvents(...)` result. */
export interface RegistryLike {
  readonly $defs: Definitions;
  readonly $scopeKeys: readonly string[];
  $validate(event: NewEvent): NewEvent;
}

/** The result of `defineEvents`: one typed creator per event type plus the `$`-prefixed helpers. */
export type EventRegistry<D extends Definitions> = {
  readonly [N in keyof D & string]: Creator<D, N>;
} & {
  readonly $defs: D;
  readonly $types: readonly (keyof D & string)[];
  /** All scope keys declared by any event in this registry. */
  readonly $scopeKeys: readonly string[];
  /** A filter over this registry's types (all of them unless `types` is given). */
  $filter(filter?: RegistryFilter<D>): Filter;
  /**
   * Everything OF THIS REGISTRY that happened in relation to one event: `$scope("articleDraftedId", id)`.
   * For a cross-registry scope use the free `scope()` helper.
   */
  $scope(key: AllScopeKeys<D> | (string & {}), value: string | readonly string[]): Filter;
  /**
   * The incremental fold `es.context()` and `es.command()` want: `(delta, state) => state`.
   * Unknown or unhandled types are skipped. A handler must return the next state.
   */
  $fold<S>(handlers: FoldHandlers<D, S>): Fold<S>;
  /**
   * One-shot: fold a complete list from `initial`. Deliberately NOT assignable to `Fold<S>`
   * (its second parameter is typed `undefined`), so it cannot slot into `es.context()`/`es.command()`
   * by accident and silently ignore the cached state.
   */
  $foldAll<S>(initial: S | (() => S), handlers: FoldHandlers<D, S>): (events: readonly RecordedEvent[], state?: undefined) => S;
  /** Narrow a recorded event to this registry's union, re-validating its data against the schema. Throws for unknown types. */
  $parse(event: RecordedEvent<string, unknown>): RecordedEventOf<D>;
  $is(event: RecordedEvent<string, unknown>): event is RecordedEventOf<D>;
  $idKey(type: keyof D & string): string;
  /** Validate a new event of this registry (schema + envelope rules). */
  $validate(event: NewEvent): NewEvent;
};

const RESERVED = /^\$/;
const TYPE_NAME = /^[A-Za-z][A-Za-z0-9_.]*$/;

function zodIssues(issues: readonly { message: string; path: PropertyKey[]; code: string }[]) {
  return issues.map((i) => ({ message: i.message, path: i.path.map(String), code: i.code }));
}

/**
 * Declare events once. You get typed creators, typed folds, typed filters — and the store
 * derives its indexes, locks and unique constraints from the same declaration.
 *
 * ```ts
 * const articles = defineEvents({
 *   ArticleDrafted: { data: z.object({ title: z.string(), slug: z.string() }), scopes: ["workspaceProvisionedId"] },
 *   ArticleArchived: { data: z.object({ reason: z.string().optional() }), scopes: ["articleDraftedId"] },
 * });
 * const drafted = articles.ArticleDrafted({ title, slug }, { workspaceProvisionedId });   // drafted.id is a UUIDv7
 * ```
 */
export function defineEvents<const D extends Definitions>(defs: D): EventRegistry<D> {
  const types = Object.keys(defs) as (keyof D & string)[];
  for (const t of types) {
    if (RESERVED.test(t)) throw new Error(`eventstore: event type "${t}" must not start with "$"`);
    if (!TYPE_NAME.test(t)) throw new Error(`eventstore: invalid event type "${t}"`);
    for (const u of defs[t]?.unique ?? []) uniquePathSegments(u);
    for (const k of [...(defs[t]?.scopes ?? []), ...(defs[t]?.optionalScopes ?? [])]) {
      if (!IDENTIFIER.test(k)) throw new Error(`eventstore: invalid scope key "${k}" on ${t}`);
    }
  }
  const scopeKeys = [...new Set(types.flatMap((t) => [...(defs[t]?.scopes ?? []), ...(defs[t]?.optionalScopes ?? [])]))].sort();
  const idKeyOf = (type: string): string => defs[type]?.idKey ?? defaultIdKey(type);
  const envelope = { idKeyOf, scopeKeys };

  const validate = (event: NewEvent): NewEvent => {
    const def = defs[event.type];
    if (!def) {
      validateEnvelope(event, envelope);
      return event;
    }
    const parsed = def.data.safeParse(event.data);
    if (!parsed.success) throw new ValidationError(event.type, zodIssues(parsed.error.issues));
    const withData = { ...event, data: parsed.data as Record<string, unknown> };
    validateEnvelope(withData, envelope);
    for (const key of def.scopes ?? []) {
      if (typeof event.scopes?.[key] !== "string" || event.scopes[key] === "") {
        throw new ValidationError(event.type, [{ message: `missing required scope "${key}"`, path: ["scopes", key] }]);
      }
    }
    return withData;
  };

  const foldWith = <S>(handlers: FoldHandlers<D, S>) => {
    const table = handlers as Record<string, ((d: unknown, s: S, e: RecordedEvent) => S) | undefined>;
    return (events: readonly RecordedEvent[], state: S): S => {
      for (const event of events) {
        const handler = table[event.type];
        if (!handler) continue;
        const next = handler(event.data, state, event);
        if (next === undefined) throw new Error(`eventstore: fold handler for "${event.type}" returned undefined; return the next state`);
        state = next;
      }
      return state;
    };
  };

  const registry: Record<string, unknown> = {
    $defs: defs,
    $types: types,
    $scopeKeys: scopeKeys,
    $filter(filter: RegistryFilter<D> = {}): Filter {
      const out: { types: string[]; scopes?: Record<string, string | readonly string[]>; where?: readonly Record<string, unknown>[] } = {
        types: [...(filter.types ?? types)],
      };
      if (filter.scopes) out.scopes = filter.scopes as Record<string, string | readonly string[]>;
      if (filter.where) out.where = filter.where;
      return out;
    },
    $scope(key: string, value: string | readonly string[]): Filter {
      return { types: [...types], scopes: { [key]: value } };
    },
    $fold: <S>(handlers: FoldHandlers<D, S>) => foldWith(handlers),
    $foldAll<S>(initial: S | (() => S), handlers: FoldHandlers<D, S>) {
      const fold = foldWith(handlers);
      return (events: readonly RecordedEvent[]): S =>
        fold(events, typeof initial === "function" ? (initial as () => S)() : initial);
    },
    $parse(event: RecordedEvent<string, unknown>) {
      const def = defs[event.type];
      if (!def) throw new UsageError(`eventstore: "${event.type}" is not declared in this registry`);
      const parsed = def.data.safeParse(event.data);
      if (!parsed.success) throw new ValidationError(event.type, zodIssues(parsed.error.issues));
      return { ...event, data: parsed.data };
    },
    $is(event: RecordedEvent<string, unknown>): boolean {
      return event.type in defs;
    },
    $idKey: idKeyOf,
    $validate: validate,
  };

  for (const type of types) {
    registry[type] = (data: unknown, ...rest: unknown[]) => {
      const scopes = (rest[0] ?? {}) as Scopes;
      const options = (rest[1] ?? {}) as CreateOptions;
      const valid = validate({ type, data: data as Record<string, unknown>, scopes, ...(options.metadata ? { metadata: options.metadata } : {}) });
      return { ...valid, id: options.id ?? uuidv7(), scopes };
    };
  }
  return registry as EventRegistry<D>;
}

/** Store-level options merged into a schema: extra scope keys, the tenant key, the lock salt, strictness. */
export interface SchemaOptions {
  readonly scopeKeys?: readonly string[];
  readonly tenantScopeKey?: string;
  /** Per-deployment secret mixed into advisory-lock keys (see `StoreSchema.lockSalt`). */
  readonly lockSalt?: string;
  readonly strict?: boolean;
}

/** Merge registries + options into the schema a store works with. */
export function buildSchema(registries: readonly RegistryLike[], options: SchemaOptions = {}): StoreSchema {
  const defs: Definitions = {};
  for (const r of registries) {
    for (const [type, def] of Object.entries(r.$defs)) {
      if (!TYPE_NAME.test(type)) throw new Error(`eventstore: invalid event type "${type}"`);
      if (defs[type] && defs[type] !== def) throw new Error(`eventstore: event type "${type}" is declared twice`);
      defs[type] = def;
    }
  }
  const scopeKeys = [
    ...new Set([...registries.flatMap((r) => r.$scopeKeys), ...(options.scopeKeys ?? []), ...(options.tenantScopeKey ? [options.tenantScopeKey] : [])]),
  ].sort();
  for (const k of scopeKeys) if (!IDENTIFIER.test(k)) throw new Error(`eventstore: invalid scope key "${k}"`);
  const uniques = Object.entries(defs).flatMap(([type, def]) => (def.unique ?? []).map((path) => ({ type, path })));
  const idKeyOf = (type: string): string => defs[type]?.idKey ?? defaultIdKey(type);
  const envelope = { idKeyOf, scopeKeys };
  const validateAll = (event: NewEvent): NewEvent => {
    for (const r of registries) if (event.type in r.$defs) return r.$validate(event);
    validateEnvelope(event, envelope);
    return event;
  };
  return {
    scopeKeys,
    ...(options.tenantScopeKey ? { tenantScopeKey: options.tenantScopeKey } : {}),
    ...(options.lockSalt ? { lockSalt: options.lockSalt } : {}),
    uniques,
    strict: options.strict ?? true,
    idKeyOf,
    upcast: (type, payload) => defs[type]?.upcast?.(payload) ?? payload,
    validate: validateAll,
  };
}

/** A schema with no registries — for quick starts and tests. */
export function emptySchema(options: SchemaOptions = {}): StoreSchema {
  return buildSchema([], { strict: false, ...options });
}
