# Changelog

## 0.3.1 (2026-09-28)

- **A tenant view refused an event that names its tenant in `scopes` when the data carried another value under the tenant key.** The append check looked at the flat data field before `scopes`, while every read (`es_scope`, the memory store) lets `scopes` win. An event recorded in the platform tenant that announces a new tenant with its key as a top-level field (`scopes.tenantId = "system"`, `data.tenantId = "<new>"`) threw `TenantMismatchError`. Now `scopes` decides when it names the view's tenant; without it the flat field still decides, fail-closed. Reads were never affected.

## 0.3.0 (2026-09-28)

Two things a non-owner role under row-level security paid for on every read.

- **`LEAKPROOF` for the scope indexes under `rls`**: the installer marks `jsonb_typeof`, `jsonb_object_field` and `jsonb_object_field_text` `LEAKPROOF` when it runs as a superuser, otherwise it records a warning with the statements (`warnings()`). Without them Postgres may not use a scope index as an index condition for a non-owner role and filters every scope but the tenant over the whole tenant. New: `SCOPE_BUILTIN_FUNCTIONS`, `scopeLeakproofStatements()`, `SCOPE_LEAKPROOF_MISSING_SQL`; `printSchemaSql({ rls: true })` lists the statements.
- **`scopeStatistics`** (installer) and `DdlOptions.scopeStatistics` (static DDL): statistics objects only for the keys that need them. Every object costs planning time in every statement on the table; `{ minIndexRows, exclude }` keeps them where the scope index holds at least that many rows (`DEFAULT_SCOPE_STATISTICS_MIN_ROWS` = 1 000, measured) and drops the others. New: `scopeStatisticsKeys()`, `scopeIndexRowsSql()`; `scopeRebuildStatements()` takes the chosen keys. Default unchanged (`"all"`).

## 0.2.1 (2026-09-28)

- **`invalidate()` without a query released nothing from the process-wide budget.** It cleared the view's entries but left their bytes in `CacheBudget` as ghosts. Once a ghost was the oldest entry and the budget was exceeded, `CacheBudget.touch` evicted it again and again without freeing anything — a synchronous endless loop that blocked the event loop for every tenant. Affected: every `invalidate()` on a view or on the api, and the eviction of a tenant view from the `maxTenantViews` LRU. Now `invalidate()` releases each entry, and the budget drops a token itself when its cache no longer holds the entry.

## 0.2.0 (2026-09-15)

Two things applications had to do with raw SQL beside the package, now inside it. Both are reads; the store stays append-only. Deliberately still outside: cross-tenant directory functions and tenant erasure.

- **`omit`** on `query`/`read`: top-level `data` keys every returned record leaves out — a projection for lists that do not need a large field (an article body). The Postgres store strips them in SQL (`payload - '{…}'`), so the field never travels. `where` and `contextVersion` see the full record; `scopes` and the own id key are refused; `es.read` re-validates without the omitted keys and types `data` as `Omit<…>`. Decisions (`command`, `context`) have no `omit`.
- **`statistics(query?)`** on both stores, on the api and on tenant views (narrowed): count, stored bytes (`pg_column_size`, no payload fetched) and last sequence per event type.
- New optional capability interface `StatisticsStore`; `EventStore` is unchanged, so custom stores keep compiling. `TypeStatistics` and `TrimmedDataOf` exported.
- Operations: a recipe for erasing a tenant (the one write the package deliberately does not offer).

## 0.1.1 (2026-09-08)

- A declared scope key may be `null` in `data`: it then simply is not a scope (`es_scope` yields NULL, the memory store ignores it). Previously the envelope check rejected it; existing stores that write `categoryDefinedId: null` for "no category" need no change.

## 0.1.0 (2026-09-08)

First cut of `@orgops/eventstore`, built in one night from the research in `research/`. Every concept is the joint work of Ralf Westphal and Rico Fritzsche (event-orientation, Command Context Consistency, scoping events); this package is an implementation of it on PostgreSQL.

- **Core**: `defineEvents` (Zod v4) with typed creators, `$fold`/`$foldAll`, `$filter`/`$scope`, `$parse`; envelope rules (own id and `scopes` never inside `data`, string scope values); `configure()` + ambient `es`, `createEventStore()`; `es.command()` (read → decide → `appendIf`, retries on conflicts and transient failures with jittered back-off); `es.context()` incremental context cache over settled events with a gap-free `(transactionId, sequence)` cursor; `es.read(registry)` typed reads; tenant views (`forTenant`, `forPlatform`, fail-closed stamping, memoised per tenant); typed errors with `httpStatus`.
- **Postgres**: one `events` table; `es_scope()` inlinable expression function; one B-tree per declared scope key with `CREATE STATISTICS`; partial unique indexes per `unique` path; idempotency-key index (per tenant when a tenant key is configured); `es_append_if_v4` — advisory locks in one sorted pass (shared tenant stamps, exclusive scope pairs, shared/exclusive global), fresh-snapshot version check, batched `unnest` insert, READ COMMITTED enforced, `search_path` pinned, `EXECUTE` revoked from PUBLIC; fingerprinted installer under a transaction-scoped advisory lock; per-connection timeouts; transient-error mapping; optional RLS with per-transaction `set_config`.
- **Memory**: the semantic reference store with posting lists per type and per `(scopeKey, value)`; every candidate re-verified with the full predicate.
- **Subscribe**: durable, gap-free, at-least-once subscriptions with pluggable cursor stores (`memoryCursors`, `fileCursors`); in-process `on()` for live stores.
- **Testing**: shared conformance suite (runs against memory and Postgres), `given/when/then`, `interferingStore`/`slowStore`/`recordingStore`, `makeEvent`.

Measured on Docker Postgres 18 with 1.9 M events (see `research/30-journal.md`): 6 600 full commands/s at 16 clients (read + decide + guarded append), 10 600 context reads/s, 208 000 events/s in 50-event batches; 16 concurrent guarded appends on one context → exactly one commits. 163 tests: shared conformance suite against memory and Postgres, constructed xid/sequence inversions, deadlock schedules, RLS with a non-owner role, an end-to-end example app.
