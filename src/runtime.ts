import { runCommand, type CommandOutcome, type CommandSpec, type RawCommandSpec } from "./command.js";
import { ContextCache, type ContextCacheOptions, type ContextSpec, type LoadedContext } from "./context.js";
import { ConflictError, NotConfiguredError, UsageError } from "./errors.js";
import { MemoryStore } from "./memory.js";
import { buildSchema, type Definitions, type EventRegistry, type RecordedEventOf, type RegistryLike } from "./registry.js";
import type { CreatePostgresStoreOptions } from "./postgres/store.js";
import { PLATFORM_TENANT_ID, scopedToTenant, type TenantConfig } from "./tenant.js";
import type {
  AppendIfOutcome,
  AppendResult,
  ContextHandle,
  EventStore,
  NewEvent,
  Query,
  QueryOptions,
  QueryResult,
  StoreSchema,
} from "./types.js";

/**
 * Options of the Postgres store, as accepted by `configure({ postgres })`. Derived from the
 * store's own option type (type-only import: the core stays driver-free).
 */
export type PostgresOptions = Omit<CreatePostgresStoreOptions, "connection" | "schema" | "tenantScopeKey">;

/** What `configure()`/`createEventStore()` take. Nothing here is required: no `connection` means an in-memory store. */
export interface EventStoreConfig {
  /** Postgres connection string. Loads `@orgops/eventstore/postgres` lazily (peer dependency `pg`). */
  readonly connection?: string;
  /** Or bring your own store (e.g. `new MemoryStore()`), or a factory receiving the schema. */
  readonly store?: EventStore | ((schema: StoreSchema) => EventStore | Promise<EventStore>);
  /** Your `defineEvents(...)` registries. They drive typing, validation, indexes, locks and unique constraints. */
  readonly events?: readonly RegistryLike[];
  /** Extra scope keys to index and lock (for flat-id payloads that are not declared in a registry). */
  readonly scopeKeys?: readonly string[];
  readonly tenant?: TenantConfig;
  /** Strict mode: guard queries must be lockable through declared scope keys. Default `true`. */
  readonly strict?: boolean;
  /**
   * Per-deployment secret mixed into every advisory-lock key. Without it the keys are plain
   * hashes of public strings and any database role can compute and hold them. Set it from a
   * secret; changing it later is safe (locks are transaction-scoped).
   */
  readonly lockSalt?: string;
  readonly postgres?: PostgresOptions;
  /** In-process incremental context cache, one per tenant view. `false` disables it. */
  readonly contextCache?: ContextCacheOptions | false;
  readonly clock?: () => Date;
}

/** The thing you use. `es` is one of these; `createEventStore()` gives you your own. */
export interface EventStoreApi extends EventStore {
  readonly schema: StoreSchema;
  /**
   * A typed read: every record narrowed to the registry's union and re-validated through
   * `$parse`. `query` defaults to `registry.$filter()`; a record of an undeclared type throws.
   */
  read<D extends Definitions>(registry: EventRegistry<D>, query?: Query, options?: QueryOptions): Promise<QueryResult<RecordedEventOf<D>>>;
  /** Run a CCC command: read → decide → `appendIf`, retrying on conflict. */
  command<R = void>(spec: RawCommandSpec<R>): Promise<CommandOutcome<R>>;
  command<S, R = void>(spec: CommandSpec<S, R>): Promise<CommandOutcome<R>>;
  /** Load a context incrementally (cached per query in this view). */
  context<S>(spec: ContextSpec<S>): Promise<LoadedContext<S>>;
  /** Like `appendIf`, but throws `ConflictError` instead of returning the conflict. */
  appendIfOrThrow(events: readonly NewEvent[], ctx: ContextHandle): Promise<AppendResult>;
  /**
   * A view bound to one tenant: queries narrowed, events stamped, its own context cache.
   * Memoised per tenant id, so calling it per request is free. Requires `tenant` in the config.
   */
  forTenant(tenantId: string): EventStoreApi;
  /** The platform tenant (accounts, registrations, …). */
  forPlatform(): EventStoreApi;
  /** The underlying store (memory or Postgres), initialised. Not narrowed to a tenant. */
  store(): Promise<EventStore>;
  /** Forget this view's cached contexts (all, or one query). */
  invalidate(query?: Query): void;
  /** Closes the underlying store. On a tenant view this is a no-op — close the root api. */
  close(): Promise<void>;
}

/** A store that can bind a database session to a tenant (e.g. the Postgres store with `rls: true`). */
interface TenantSessionStore extends EventStore {
  withTenant(tenantId: string): EventStore;
}

function hasTenantSessions(store: EventStore): store is TenantSessionStore {
  return typeof (store as Partial<TenantSessionStore>).withTenant === "function";
}

/** An explicit, non-global instance (libraries, tests, several stores in one process). */
export function createEventStore(config: EventStoreConfig): EventStoreApi {
  const schema = buildSchema(config.events ?? [], {
    scopeKeys: config.scopeKeys ?? [],
    tenantScopeKey: config.tenant?.scopeKey,
    lockSalt: config.lockSalt,
    strict: config.strict ?? true,
  });
  let storePromise: Promise<EventStore> | undefined;
  const store = (): Promise<EventStore> => {
    if (!storePromise) {
      storePromise = resolveStore(config, schema).catch((err) => {
        storePromise = undefined;
        throw err;
      });
    }
    return storePromise;
  };
  return buildApi({ schema, store, created: () => storePromise !== undefined, config, tenantId: undefined, views: new Map() });
}

interface ApiParts {
  readonly schema: StoreSchema;
  readonly store: () => Promise<EventStore>;
  /** Whether the lazy store has been created — `close()` must not connect just to disconnect. */
  readonly created: () => boolean;
  readonly config: EventStoreConfig;
  readonly tenantId: string | undefined;
  /** Tenant views, shared by the root api and every view, so `forTenant(id)` is memoised. */
  readonly views: Map<string, EventStoreApi>;
}

function buildApi(parts: ApiParts): EventStoreApi {
  const { schema, config, tenantId } = parts;
  let viewPromise: Promise<EventStore> | undefined;
  const view = (): Promise<EventStore> => {
    if (!viewPromise) {
      viewPromise = parts.store().then((inner) => {
        if (tenantId === undefined || !config.tenant) return inner;
        // A store with tenant sessions (RLS) binds the session; the wrapper narrows and stamps.
        const bound = hasTenantSessions(inner) ? inner.withTenant(tenantId) : inner;
        return scopedToTenant(bound, config.tenant, tenantId, schema.idKeyOf);
      });
    }
    return viewPromise;
  };
  // One cache per view, allocated once: tenant views never share entries. `false` → `max: 0` (no caching, same code path).
  let cache: ContextCache | undefined;
  const cacheFor = async (): Promise<ContextCache> => {
    if (!cache) cache = new ContextCache(await view(), config.contextCache === false ? { max: 0 } : (config.contextCache ?? {}));
    return cache;
  };

  const api: EventStoreApi = {
    schema,
    async query(query: Query, options?: QueryOptions): Promise<QueryResult> {
      return (await view()).query(query, options);
    },
    async read(registry, query, options) {
      const result = await (await view()).query(query ?? registry.$filter(), options);
      const parsed = new Map(result.events.map((e) => [e, registry.$parse(e)] as const));
      return {
        ...result,
        events: result.events.map((e) => parsed.get(e)!),
        byFilter: result.byFilter.map((list) => list.map((e) => parsed.get(e)!)),
      };
    },
    async append(events: readonly NewEvent[]): Promise<AppendResult> {
      return (await view()).append(events);
    },
    async appendIf(events: readonly NewEvent[], ctx: ContextHandle): Promise<AppendIfOutcome> {
      return (await view()).appendIf(events, ctx);
    },
    async appendIfOrThrow(events, ctx) {
      const outcome = await api.appendIf(events, ctx);
      if (!outcome.ok) throw new ConflictError(outcome.conflict);
      return outcome.appended;
    },
    command: (async (spec: CommandSpec<unknown, unknown>) => {
      return runCommand({ store: await view(), cache: await cacheFor(), clock: config.clock }, spec);
    }) as EventStoreApi["command"],
    async context(spec) {
      return (await cacheFor()).load(spec);
    },
    forTenant(id: string): EventStoreApi {
      if (!config.tenant) throw new Error("eventstore: forTenant() needs configure({ tenant: { scopeKey } })");
      if (id === tenantId) return api;
      let existing = parts.views.get(id);
      if (!existing) {
        existing = buildApi({ ...parts, tenantId: id });
        parts.views.set(id, existing);
      }
      return existing;
    },
    forPlatform(): EventStoreApi {
      return api.forTenant(config.tenant?.platformId ?? PLATFORM_TENANT_ID);
    },
    store: () => parts.store(),
    invalidate(query?: Query): void {
      cache?.invalidate(query);
    },
    async close(): Promise<void> {
      if (tenantId === undefined && parts.created()) await (await parts.store()).close();
    },
  };
  return api;
}

async function resolveStore(config: EventStoreConfig, schema: StoreSchema): Promise<EventStore> {
  if (config.store) {
    const store = typeof config.store === "function" ? await config.store(schema) : config.store;
    assertSchemaAgrees(store, schema);
    return store;
  }
  if (config.connection) {
    const { createPostgresStore } = await import("./postgres/index.js");
    return createPostgresStore({
      connection: config.connection,
      schema,
      ...(config.postgres ?? {}),
      tenantScopeKey: config.tenant?.scopeKey,
    });
  }
  return new MemoryStore({ schema, clock: config.clock });
}

/**
 * A pre-built store carries its own schema (that is what installed its indexes and uniques);
 * silently using it with different registries would enforce a different set of rules than
 * the one the caller declared.
 */
function assertSchemaAgrees(store: EventStore, schema: StoreSchema): void {
  const own = (store as { schema?: StoreSchema }).schema;
  if (!own) return;
  const problems: string[] = [];
  // the store may enforce MORE than these registries declare (one shared table, several services);
  // it must not enforce less, and the non-negotiables must match exactly
  const missingKeys = schema.scopeKeys.filter((k) => !own.scopeKeys.includes(k));
  if (missingKeys.length > 0) problems.push(`the store does not index/lock scope keys [${missingKeys.join(", ")}]`);
  const uniq = (u: { type: string; path: string }) => `${u.type}.${u.path}`;
  const ownUniques = new Set(own.uniques.map(uniq));
  const missingUniques = schema.uniques.map(uniq).filter((u) => !ownUniques.has(u));
  if (missingUniques.length > 0) problems.push(`the store does not enforce uniques [${missingUniques.join(", ")}]`);
  if ((own.tenantScopeKey ?? "") !== (schema.tenantScopeKey ?? "")) problems.push(`tenant key ${own.tenantScopeKey ?? "none"} vs ${schema.tenantScopeKey ?? "none"}`);
  if (own.strict !== schema.strict) {
    problems.push(
      `strict ${own.strict} vs ${schema.strict}` +
        (own.strict === false ? " (a bare `new MemoryStore()` uses emptySchema(), which is non-strict; pass the schema: `store: (schema) => new MemoryStore({ schema })`)" : ""),
    );
  }
  if ((own.lockSalt ?? "") !== (schema.lockSalt ?? "")) problems.push("lockSalt differs (two salts = two lock key spaces = no mutual exclusion)");
  for (const t of new Set(schema.uniques.map((u) => u.type))) {
    if (own.idKeyOf(t) !== schema.idKeyOf(t)) problems.push(`idKey of ${t}: ${own.idKeyOf(t)} vs ${schema.idKeyOf(t)}`);
  }
  if (problems.length > 0) {
    throw new UsageError(
      `eventstore: the store you passed was built with a different schema than configure() derives from your registries — ${problems.join("; ")}. ` +
        `Build the store with buildSchema(events, { scopeKeys, tenantScopeKey, strict, lockSalt }) from the same registries, or omit \`store\` and let configure() create it.`,
    );
  }
}

// ── ambient instance ──────────────────────────────────────────────────────────

let ambient: EventStoreApi | undefined;

/**
 * Configure the ambient store ONCE at startup (a Nuxt/Nitro server plugin, your `main.ts`).
 * Then `import { es } from "@orgops/eventstore"` anywhere.
 *
 * ```ts
 * configure({ connection: process.env.DATABASE_URL!, events: [articles, accounts] });
 * ```
 */
export function configure(config: EventStoreConfig): EventStoreApi {
  ambient = createEventStore(config);
  return ambient;
}

/** Whether `configure()` has been called in this process. */
export function isConfigured(): boolean {
  return ambient !== undefined;
}

/** Reset the ambient instance (tests). Does not close the store. */
export function reset(): void {
  ambient = undefined;
}

function current(): EventStoreApi {
  if (!ambient) throw new NotConfiguredError();
  return ambient;
}

/** The ambient store — delegates to whatever `configure()` set. */
export const es: EventStoreApi = {
  get schema() {
    return current().schema;
  },
  query: (q, o) => current().query(q, o),
  read: (r, q, o) => current().read(r, q, o),
  append: (e) => current().append(e),
  appendIf: (e, c) => current().appendIf(e, c),
  appendIfOrThrow: (e, c) => current().appendIfOrThrow(e, c),
  command: ((s: CommandSpec<unknown, unknown>) => current().command(s)) as EventStoreApi["command"],
  context: (s) => current().context(s),
  forTenant: (id) => current().forTenant(id),
  forPlatform: () => current().forPlatform(),
  store: () => current().store(),
  invalidate: (q) => current().invalidate(q),
  close: () => current().close(),
};
