# Changelog

## 0.1.0 (unreleased, 2026-09-08)

First cut of `@orgops/eventstore`, built in one night from the research in `research/`.

- **Core**: `defineEvents` (Zod v4) with typed creators, `$fold`/`$foldAll`, `$filter`/`$scope`, `$parse`; envelope rules (own id and `scopes` never inside `data`, string scope values); `configure()` + ambient `es`, `createEventStore()`; `es.command()` (read → decide → `appendIf`, retries on conflicts and transient failures with jittered back-off); `es.context()` incremental context cache over settled events with a gap-free `(transactionId, sequence)` cursor; `es.read(registry)` typed reads; tenant views (`forTenant`, `forPlatform`, fail-closed stamping, memoised per tenant); typed errors with `httpStatus`.
- **Postgres**: one `events` table; `es_scope()` inlinable expression function; one B-tree per declared scope key with `CREATE STATISTICS`; partial unique indexes per `unique` path; idempotency-key index (per tenant when a tenant key is configured); `es_append_if_v3` — advisory locks in one sorted pass (shared tenant stamps, exclusive scope pairs, shared/exclusive global), fresh-snapshot version check, batched `unnest` insert, READ COMMITTED enforced, `search_path` pinned, `EXECUTE` revoked from PUBLIC; fingerprinted installer under a transaction-scoped advisory lock; per-connection timeouts; transient-error mapping; optional RLS with per-transaction `set_config`.
- **Memory**: the semantic reference store with posting lists per type and per `(scopeKey, value)`; every candidate re-verified with the full predicate.
- **Subscribe**: durable, gap-free, at-least-once subscriptions with pluggable cursor stores (`memoryCursors`, `fileCursors`); in-process `on()` for live stores.
- **Testing**: shared conformance suite (runs against memory and Postgres), `given/when/then`, `interferingStore`/`slowStore`/`recordingStore`, `makeEvent`.

Measured on Docker Postgres 18 with 1.9 M events (see `research/30-journal.md`): 6 450 full commands/s at 16 clients (read + decide + guarded append), 10 600 context reads/s, 198 000 events/s in 50-event batches; 16 concurrent guarded appends on one context → exactly one commits.
