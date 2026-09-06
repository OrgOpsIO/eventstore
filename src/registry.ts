import type { ZodType, output } from "zod";
import { defaultIdKey } from "./ids.js";
import { ValidationError } from "./errors.js";
import { uniquePathSegments } from "./query.js";
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
  /** Upcast an older stored payload to the current shape (runs before parsing on read). */
  readonly upcast?: (payload: Record<string, unknown>) => Record<string, unknown>;
}

export type Definitions = Record<string, EventDefinition>;

type ScopeKeysOf<Def extends EventDefinition> = Def extends EventDefinition<ZodType, infer K> ? K : never;
type RequiredScopeKeys<Def extends EventDefinition> = Def["scopes"] extends readonly (infer K extends string)[] ? K : never;
type OptionalScopeKeys<Def extends EventDefinition> = Def["optionalScopes"] extends readonly (infer K extends string)[] ? K : never;

type ScopesArg<Def extends EventDefinition> = [RequiredScopeKeys<Def>] extends [never]
  ? [scopes?: Partial<Record<OptionalScopeKeys<Def>, string>> & Scopes]
  : [scopes: Record<RequiredScopeKeys<Def>, string> & Partial<Record<OptionalScopeKeys<Def>, string>> & Scopes];

export type DataOf<Def extends EventDefinition> = output<Def["data"]>;

export type NewEventOf<D extends Definitions, N extends keyof D & string = keyof D & string> = {
  [K in N]: NewEvent<K, DataOf<D[K]>> & { readonly id: string; readonly scopes: Scopes };
}[N];

export type RecordedEventOf<D extends Definitions, N extends keyof D & string = keyof D & string> = {
  [K in N]: RecordedEvent<K, DataOf<D[K]>>;
}[N];

export type AllScopeKeys<D extends Definitions> = { [K in keyof D]: ScopeKeysOf<D[K]> }[keyof D];

export interface CreateOptions {
  readonly id?: string;
  readonly metadata?: Metadata;
}

export type Creator<D extends Definitions, N extends keyof D & string> = (
  data: DataOf<D[N]>,
  ...rest: [...ScopesArg<D[N]>, options?: CreateOptions]
) => NewEventOf<D, N>;

export type FoldHandlers<D extends Definitions, S> = {
  readonly [N in keyof D & string]?: (data: DataOf<D[N]>, state: S, event: RecordedEventOf<D, N>) => S;
};

export interface RegistryFilter<D extends Definitions> {
  readonly types?: readonly (keyof D & string)[];
  readonly scopes?: Partial<Record<AllScopeKeys<D> | (string & {}), string | readonly string[]>>;
  readonly where?: readonly Readonly<Record<string, unknown>>[];
}

export type EventRegistry<D extends Definitions = Definitions> = {
  readonly [N in keyof D & string]: Creator<D, N>;
} & {
  readonly $defs: D;
  readonly $types: readonly (keyof D & string)[];
  /** All scope keys declared by any event in this registry. */
  readonly $scopeKeys: readonly string[];
  /** A filter over this registry's types (all of them unless `types` is given). */
  $filter(filter?: RegistryFilter<D>): Filter;
  /** Everything of this registry that happened in relation to one event: `$scope("articleDraftedId", id)`. */
  $scope(key: AllScopeKeys<D> | (string & {}), value: string | readonly string[]): Filter;
  /** A typed fold: unknown or unhandled types are skipped. */
  $fold<S>(initial: S | (() => S), handlers: FoldHandlers<D, S>): (events: readonly RecordedEvent[]) => S;
  /** Narrow a recorded event to this registry's union (throws for unknown types). */
  $parse(event: RecordedEvent): RecordedEventOf<D>;
  $is(event: RecordedEvent<string, unknown>): event is RecordedEventOf<D>;
  $idKey(type: keyof D & string): string;
  /** Validate the data of a new event against its schema. */
  $validate(event: NewEvent): NewEvent;
};

const RESERVED = /^\$/;

/**
 * Declare events once. You get typed creators, typed folds, typed filters — and the store
 * derives its indexes, locks and unique constraints from the same declaration.
 *
 * ```ts
 * const articles = defineEvents({
 *   ArticleDrafted: { data: z.object({ title: z.string(), slug: z.string() }), scopes: ["workspaceProvisionedId"] },
 *   ArticleArchived: { data: z.object({ reason: z.string().optional() }), scopes: ["articleDraftedId"] },
 * });
 * const drafted = articles.ArticleDrafted({ title, slug }, { workspaceProvisionedId });
 * ```
 */
export function defineEvents<const D extends Definitions>(defs: D): EventRegistry<D> {
  const types = Object.keys(defs) as (keyof D & string)[];
  for (const t of types) {
    if (RESERVED.test(t)) throw new Error(`eventstore: event type "${t}" must not start with "$"`);
    if (!/^[A-Za-z][A-Za-z0-9_.]*$/.test(t)) throw new Error(`eventstore: invalid event type "${t}"`);
    for (const u of defs[t]?.unique ?? []) uniquePathSegments(u);
  }
  const scopeKeys = [...new Set(types.flatMap((t) => [...(defs[t]?.scopes ?? []), ...(defs[t]?.optionalScopes ?? [])]))].sort();

  const idKeyOf = (type: string): string => defs[type]?.idKey ?? defaultIdKey(type);

  const validate = (event: NewEvent): NewEvent => {
    const def = defs[event.type];
    if (!def) return event;
    const parsed = def.data.safeParse(event.data);
    if (!parsed.success) throw new ValidationError(event.type, parsed.error.issues);
    for (const key of def.scopes ?? []) {
      if (typeof event.scopes?.[key] !== "string" || event.scopes[key] === "") {
        throw new ValidationError(event.type, [{ message: `missing required scope "${key}"` }]);
      }
    }
    return { ...event, data: parsed.data as Record<string, unknown> };
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
    $fold<S>(initial: S | (() => S), handlers: FoldHandlers<D, S>) {
      return (events: readonly RecordedEvent[]): S => {
        let state = typeof initial === "function" ? (initial as () => S)() : initial;
        for (const event of events) {
          const handler = (handlers as Record<string, ((d: unknown, s: S, e: RecordedEvent) => S) | undefined>)[event.type];
          if (handler) state = handler(event.data, state, event);
        }
        return state;
      };
    },
    $parse(event: RecordedEvent) {
      if (!(event.type in defs)) throw new Error(`eventstore: "${event.type}" is not declared in this registry`);
      return event;
    },
    $is(event: RecordedEvent): boolean {
      return event.type in defs;
    },
    $idKey: idKeyOf,
    $validate: validate,
  };

  for (const type of types) {
    registry[type] = (data: unknown, ...rest: unknown[]) => {
      const scopes = (rest[0] ?? {}) as Scopes;
      const options = (rest[1] ?? {}) as CreateOptions;
      const event: NewEvent = { type, data: data as Record<string, unknown>, scopes, ...(options.metadata ? { metadata: options.metadata } : {}) };
      const valid = validate(event);
      return { ...valid, id: options.id ?? undefined, scopes };
    };
  }
  return registry as EventRegistry<D>;
}

/** The structural part of a registry that stores and configs need — accepts any `defineEvents(...)` result. */
export interface RegistryLike {
  readonly $defs: Definitions;
  readonly $scopeKeys: readonly string[];
  $validate(event: NewEvent): NewEvent;
}

export interface SchemaOptions {
  readonly scopeKeys?: readonly string[];
  readonly strict?: boolean;
}

/** Merge registries + options into the schema a store works with. */
export function buildSchema(registries: readonly RegistryLike[], options: SchemaOptions = {}): StoreSchema {
  const defs: Definitions = {};
  for (const r of registries) {
    for (const [type, def] of Object.entries(r.$defs)) {
      if (defs[type] && defs[type] !== def) throw new Error(`eventstore: event type "${type}" is declared twice`);
      defs[type] = def;
    }
  }
  const scopeKeys = [...new Set([...registries.flatMap((r) => r.$scopeKeys), ...(options.scopeKeys ?? [])])].sort();
  const uniques = Object.entries(defs).flatMap(([type, def]) => (def.unique ?? []).map((path) => ({ type, path })));
  const validateAll = (event: NewEvent): NewEvent => {
    for (const r of registries) if (event.type in r.$defs) return r.$validate(event);
    return event;
  };
  return {
    scopeKeys,
    uniques,
    strict: options.strict ?? true,
    idKeyOf: (type) => defs[type]?.idKey ?? defaultIdKey(type),
    upcast: (type, payload) => defs[type]?.upcast?.(payload) ?? payload,
    validate: validateAll,
  };
}

/** A schema with no registries — for quick starts and tests. */
export function emptySchema(options: SchemaOptions = {}): StoreSchema {
  return buildSchema([], { strict: false, ...options });
}
