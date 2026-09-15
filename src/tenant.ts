import { TenantMismatchError } from "./errors.js";
import { filtersOf } from "./query.js";
import type { AppendIfOutcome, AppendResult, ContextHandle, EventStore, Filter, NewEvent, Query, QueryOptions, QueryResult, StatisticsStore } from "./types.js";

/** How the tenant is represented: as a scope key in every event's payload. */
export interface TenantConfig {
  /** The scope key that identifies the tenant, e.g. `"workspaceProvisionedId"`. Indexed and locked like any scope key. */
  readonly scopeKey: string;
  /** Tenant id used for platform-level events (accounts, registrations). Default: the nil UUID. */
  readonly platformId?: string;
}

/** The tenant id of platform-level events (accounts, registrations) when none is configured: the nil UUID. */
export const PLATFORM_TENANT_ID = "00000000-0000-0000-0000-000000000000";

/**
 * A store view bound to one tenant. Every query is narrowed to the tenant's scope; every
 * appended event is stamped with it — and rejected, fail-closed, if it claims another tenant
 * The tenant is a scope in the payload, not a column.
 *
 * `close()` on a view is a no-op: views share the underlying store, which the root api closes.
 */
export function scopedToTenant(
  inner: EventStore & Partial<StatisticsStore>,
  config: TenantConfig,
  tenantId: string,
  idKeyOf?: (type: string) => string,
): EventStore & Partial<StatisticsStore> {
  const key = config.scopeKey;
  const narrow = (query: Query): Filter[] =>
    filtersOf(query).map((f) => {
      const claimed = f.scopes?.[key];
      if (claimed !== undefined) {
        const values = typeof claimed === "string" ? [claimed] : claimed;
        if (values.length !== 1 || values[0] !== tenantId) {
          throw new TenantMismatchError(`eventstore: query names tenant ${values.join(",")} but the store is bound to ${tenantId}`);
        }
        return f;
      }
      return { ...f, scopes: { ...(f.scopes ?? {}), [key]: tenantId } };
    });
  /**
   * Stamp the tenant onto every event, fail-closed: an event that names another tenant —
   * in `scopes` or as a flat data field — is rejected before anything is written.
   */
  const stamp = (events: readonly NewEvent[]): NewEvent[] =>
    events.map((e) => {
      const data = (e.data ?? {}) as Record<string, unknown>;
      const flat = Object.prototype.hasOwnProperty.call(data, key) ? data[key] : undefined;
      if (flat !== undefined && flat !== tenantId) {
        throw new TenantMismatchError(`eventstore: "${e.type}" carries ${key}=${String(flat)} in its data but is appended to ${tenantId}`);
      }
      const claimed = e.scopes?.[key];
      if (claimed !== undefined && claimed !== tenantId) {
        throw new TenantMismatchError(`eventstore: "${e.type}" names tenant ${claimed} but is appended to ${tenantId}`);
      }
      if (claimed === tenantId) return e;
      if (idKeyOf && idKeyOf(e.type) === key) {
        // the tenant's own root event (its id IS the tenant id) carries no back-link to itself;
        // a root-keyed event with ANOTHER id would be shadowed by the stamp — refuse instead
        if (e.id === tenantId) return e;
        throw new TenantMismatchError(`eventstore: "${e.type}" is a tenant root with id ${e.id ?? "<generated>"} but is appended to tenant ${tenantId}`);
      }
      return { ...e, scopes: { ...(e.scopes ?? {}), [key]: tenantId } };
    });
  const view: EventStore & Partial<StatisticsStore> = {
    query(query: Query, options?: QueryOptions): Promise<QueryResult> {
      return inner.query(narrow(query), options);
    },
    append(events: readonly NewEvent[]): Promise<AppendResult> {
      return inner.append(stamp(events));
    },
    appendIf(events: readonly NewEvent[], ctx: ContextHandle): Promise<AppendIfOutcome> {
      return inner.appendIf(stamp(events), { query: narrow(ctx.query), version: ctx.version });
    },
    close(): Promise<void> {
      return Promise.resolve();
    },
  };
  // statistics narrow like any read
  if (typeof inner.statistics === "function") view.statistics = (query: Query = {}) => inner.statistics!(narrow(query));
  return view;
}
