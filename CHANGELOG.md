# Changelog

## 0.6.0 (2026-09-28)

- **`postgres: { adopt }` takes an existing events table over at install**, in place: `adopt: { columns: { sequence: "id", type: "eventtype" } }` renames the legacy columns, adds `metadata` and `transaction_id` (legacy rows get `adopt.legacyTransactionId`, default `3`, so they sort before every new row), makes `sequence_number` unique when it is not, then installs as always. Metadata-only — no row rewritten, `payload` never touched — inside the install transaction under the install lock, with row count and highest sequence compared before and after; idempotent on every later boot. A table that does not fit (half adopted, `json` payload, `integer` sequence, NULLs — also in the sequence column —, a `NOT NULL` column without a default, one column named for two roles, a `legacyTransactionId` not below every running transaction, under `rls` a foreign permissive policy) stops the install before anything changes; the adoption's reads run with `row_security = off`, so they never count a filtered table. Legacy indexes, columns, triggers, restrictive policies and write grants are kept and listed in `warnings()`. `mode: "check"` fails the install with the plan instead (also when the table is missing); `requireExisting` refuses to create a missing table; `store.adoptionPlan()` reads the plan, `printAdoptSql({ plan })` prints it. New: `planAdoption`, `adoptStatements` and their types.

## 0.5.0 (2026-09-28)

Live pushes across processes, with hard limits instead of options.

- **`es.subscribe(name, query, handler, options?)`** and **`es.watch(query, handler, options?)`** on the api, next to `append`: both read through the view they are called on (a tenant view: that tenant only, under its session). `subscribe` is durable (named cursor, `name@tenantId` on a tenant view, at-least-once); `watch` is ephemeral (from about now, at-most-once) and shares one reader per view and query in the process. A watcher that falls `watch.maxPendingBatches` (64) behind is dropped with the new `WatchOverflowError`; `watch.maxWatchers` (10 000) caps them per process. Past views (`asOf`) refuse both.
- **The commit doorbell on Postgres: `postgres: { live: true }`** installs a statement trigger that `pg_notify`s after every insert statement on commit with an **empty payload** — no sequence, tenant, type, scope or data — and one `LISTEN` connection per store (`live: { connection }` for a direct one behind PgBouncer). New optional store capability `WakeStore` (`onCommitted`), implemented by both stores and passed through tenant views; `subscribe` wakes on it (doorbells within 25 ms share one read, and never cut a retry back-off short), re-reads soon while events wait on an older transaction, and polls only as a safety net (5 s). The listener reconnects with back-off and wakes everyone once after every (re)connect; without the trigger it warns and wakes every 500 ms. `watch` takes an `AbortSignal`, ends on store errors no retry will fix, and `es.close()` ends every watch. New: `notifyChannel`, `notifyFunctionDdl`, `notifyTriggerDdl`, `isWakeStore`, and `PostgresStore.notificationQueueUsage()` for health checks — a listener that stays connected but stops reading fills the notification queue until notifying commits fail.
- **Fix: a subscription whose start position could not be loaded replayed everything.** When the cursor store (or the store, for `from: "now"`) failed at start, `subscribe` fell back to the beginning. It now retries resolving the start position like any failed poll.
- `NegatedFilter` (0.4.0) is now exported, as the 0.4.0 notes said.

## 0.4.0 (2026-09-28)

Two gaps a consumer migration ran into: a guard over a whole scope that leaves some facts out, and reading the store as it was.

- **`not` in a filter**: a filter or a list of filters whose matching records are left out of the rows and of the context version; `appendIf` re-checks exactly that. Locks come from the positive part only, a negated scope key need not be declared, a record lacking a negated key stays in. Nested `not`, an empty negation, and a `where` predicate inside `not` that is empty or holds `undefined` (it would turn into `{}` on the way to JSON and leave out the whole context, so its version would never move) are `UsageError`. New type `NegatedFilter`.
- **`es.asOf(sequence)`**: a read-only view of the store as it was at that event (inclusive). Reads, `read` and `context` see only records up to it, and their context version is computed within the cutoff; `append`, `appendIf`, `appendIfOrThrow` and `command` throw `UsageError`; nested views only narrow; no context cache; `statistics()` refused; a custom store that ignores `until` is refused instead of answering with the live state. `store()` stays the unrestricted escape hatch. Underneath: `QueryOptions.until`, which — unlike `after` and `cursor` — caps the context version too.
- **Postgres: the append function is now `es_append_if_v5`** (the version spec carries the negations). The installer creates it and drops `_v4`, as it did for every earlier version: a process still on 0.3 cannot append once a 0.4 process has installed. Roll out all processes together, or install by hand with `printSchemaSql()` and `install: "none"` and drop `_v4` after the rollout.

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
