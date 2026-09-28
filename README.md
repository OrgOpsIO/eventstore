# @orgops/eventstore

**One `events` table in PostgreSQL. Typed events. An atomic conditional append that is actually atomic. Indexes derived from your event declarations.**

## Whose ideas these are

This library is a realisation of the thinking that **Ralf Westphal** and **Rico Fritzsche** developed together and published as *event-orientation* and *Command Context Consistency*: record what happened instead of what is; no entities, no aggregates; a command reads the facts it needs, decides, and appends only if those facts are still what it read — the command's own context is the consistency boundary; events refer to each other through scopes, not through invented ids; one append-only table is enough. Every concept in this package is theirs, including the event store contract (`query`, `append`, `append_if`, the two sequence numbers that must not be confused) and the reference implementation [`eventstore-typescript`](https://github.com/ricofritzsche/eventstore-typescript) they wrote together.

What this package adds is an implementation on PostgreSQL: it makes the conditional append atomic under concurrency, derives indexes and advisory locks from the event declaration, keeps read cursors gap-free, and wraps it in a typed, configure-once API. None of that would exist without their work. Read them first:

- Ralf Westphal, *Event-Orientation* — https://ralfwestphal.substack.com/s/event-orientation
- Rico Fritzsche, *Architecture Knowledge Base* and the CCC specification — https://architecture.ricofritzsche.me/
- Rico Fritzsche, *Autonomous Domain Capabilities* — https://leanpub.com/autonomous-domain-capabilities

Configured once and used anywhere — the way [`@orgops/coax`](https://github.com/OrgOpsIO/coax) does it for LLMs.

```bash
npm install @orgops/eventstore zod pg        # plus @types/pg if you use TypeScript with the Postgres store
```

`zod` (v4) is a peer dependency (its types appear in the public API). `pg` is only needed for the Postgres store.

## Configure once, use `es` everywhere

```ts
// server/plugins/eventstore.ts — once at startup (Nuxt/Nitro shown; any entry point works)
import { configure, es } from "@orgops/eventstore";
import { articles } from "../features/articles/events";
import { accounts } from "../features/accounts/events";

export default defineNitroPlugin(async (nitro) => {
  configure({
    connection: process.env.DATABASE_URL!,
    events: [articles, accounts],                       // typing, validation, indexes, locks, uniques
    tenant: { scopeKey: "workspaceProvisionedId" },     // the tenant is a scope in the payload, not a column
    lockSalt: process.env.EVENTSTORE_LOCK_SALT,         // per-deployment secret for advisory-lock keys
  });
  await es.store();                                     // install the schema now, fail fast at boot
  nitro.hooks.hook("close", () => es.close());          // one pool per process
});
```

```ts
// anywhere
import { es, reject, rejectMissing } from "@orgops/eventstore";

const outcome = await es.forTenant(workspaceId).command({
  context: articles.$scope("articleDraftedId", articleId),   // the facts this decision reads = the consistency boundary
  fold: foldArticle,
  initial: null,
  decide: (article, { now, id }) => {
    if (!article) return rejectMissing("not_found", "unknown article");
    if (article.archived) return reject("archived", "article is archived");
    return { events: [articles.ArticleContentEdited({ body }, { articleDraftedId: articleId })] };
  },
});
// { ok: true, result, appended, attempts } | { ok: false, code, reason, missing?, conflict?, attempts }
```

On first use the SDK creates the table, its function and every index it needs — idempotently, under an advisory lock, so concurrent boots are safe. In locked-down environments pass `postgres: { install: "none" }` and hand the DDL to a DBA — with the same Postgres options, so the printed DDL matches what the store expects:

```ts
import { printSchemaSql } from "@orgops/eventstore/postgres";
const pg = { table: "events", rls: true, grantExecuteTo: ["app"] } as const;
configure({ connection, events: [articles], tenant: { scopeKey: "workspaceProvisionedId" }, postgres: { ...pg, install: "none" } });
console.log(printSchemaSql(es.schema, pg));
```

Mapping outcomes to HTTP in an h3 handler:

```ts
import { EventStoreError, httpStatusOf, type CommandOutcome } from "@orgops/eventstore";

export function unwrap<R>(outcome: CommandOutcome<R>): R {
  if (outcome.ok) return outcome.result;
  throw createError({ statusCode: httpStatusOf(outcome), statusMessage: outcome.reason, data: { code: outcome.code, conflict: outcome.conflict } });
}
export function toHttpError(error: unknown): unknown {
  return error instanceof EventStoreError ? createError({ statusCode: error.httpStatus, statusMessage: error.message, data: { name: error.name } }) : error;
}
```

### What a scope is

An event carries its own id (`articleDraftedId`) and, in `scopes`, the ids of the events it happened *in relation to*. `ArticleDrafted` happens in relation to a workspace; `ArticleContentEdited` happens in relation to the drafted article. There are no streams and no aggregate ids — a scope key is a back-link to another event, and `articles.$scope("articleDraftedId", id)` reads exactly the events that link back to it, root event included. Because the store indexes and *locks* every declared scope key, "the events this decision reads" and "the rows the guard locks" are the same set. Pick scope keys from the questions your decisions ask, not from your tables.

## Declare events once

```ts
import { z } from "zod";
import { defineEvents } from "@orgops/eventstore";

export const articles = defineEvents({
  ArticleDrafted: {
    data: z.object({ title: z.string().min(1), slug: z.string() }),
    scopes: ["workspaceProvisionedId"],     // required back-links; indexed and locked
    unique: ["slug"],                        // a partial unique index on the events table
  },
  ArticleContentEdited: {
    data: z.object({ body: z.string() }),
    scopes: ["articleDraftedId"],
  },
  ArticleArchived: {
    data: z.object({ reason: z.string().optional() }),
    scopes: ["articleDraftedId"],
  },
  ArticleImported: {
    data: z.object({ title: z.string() }),
    scopes: ["workspaceProvisionedId"],
    optionalScopes: ["importRunStartedId"],   // may carry it; indexed and locked all the same
    idKey: "articleId",                       // default would be articleImportedId
    upcast: (p) => ("headline" in p ? { ...p, title: p.headline } : p),   // runs on read, before parsing
  },
});
```

From this one declaration you get:

- **typed constructors**: `articles.ArticleDrafted({ title, slug }, { workspaceProvisionedId })` — wrong fields, missing scopes and unknown scope keys are compile errors; invalid values throw `ValidationError`. The constructor generates the event's own id (`articleDraftedId`, a UUIDv7) unless you pass `{ id }` in the third argument, so `drafted.id` is usable right away.
- **typed folds**: `articles.$fold({ ArticleDrafted: (data, state, event) => … })` builds the incremental fold `(delta, state) => state` that `es.context()` and `es.command()` take; `event.scopes.workspaceProvisionedId` is a `string`, not `string | undefined`. `articles.$foldAll(initial, handlers)` folds a complete list in one go and is deliberately not assignable to the incremental shape. `articles.$foldBy(keyOf, handlers)` maintains a `Map` of entries keyed by the event's subject — each handler gets the entry for its key and returns the next one, or `null` to delete it — with one Map copy per batch.
- **typed filters**: `articles.$scope("articleDraftedId", id)` is everything of this registry that happened in relation to that article, root event included; the free `scope(key, id)` does the same across registries; `articles.$filter({ types: [...], scopes: {...} })` for anything else.
- **typed reads**: `es.read(articles, query?)` returns the registry's union, re-validated through `$parse`, so `event.type === "ArticleDrafted"` narrows `event.data`.
- **the store schema**: a B-tree per declared scope key (plus `CREATE STATISTICS` on the same expression), a unique index per `unique` path, an idempotency-key index (per tenant when a tenant scope key is configured), `(transaction_id, sequence_number)` for cursor reads. `postgres: { typeIndex: true }` adds `(event_type, sequence_number)` for type-only reads; `adhocQueries: true` adds a GIN index for `where`.

A pre-built store passed as `store` must have been built from the same registries (`buildSchema(events, { tenantScopeKey, strict })`); `configure()` refuses a store whose scope keys, uniques, tenant key or strictness disagree, because those are what its indexes enforce.

On the wire an event is `{ articleDraftedId, title, slug, scopes: { workspaceProvisionedId } }` in a JSONB `payload` column — the convention from Westphal's and Fritzsche's *Scoping Events*. Flat ids (`employeeId` as a plain field) keep working: a scope key matches `scopes.K`, the event's own id, or a top-level string field `K`. The envelope rules hold for every event, declared or not: `data` may not contain the own id key or a `scopes` key, and a value at a declared scope key must be a string.

### Uniqueness

A `unique` path becomes a partial unique index on the events table, scoped to that event type: `unique: ["slug"]` on `ArticleDrafted` means no two `ArticleDrafted` rows share a slug, across all tenants. It is enforced by Postgres, not by your decision — a concurrent duplicate raises `UniqueViolationError` (409) *after* the guard passed, and `es.command()` deliberately does not retry it. So: check it in `decide` for the good error message, and let the index be the truth. `unique: ["scopes.magicLinkRequestedId"]` makes a back-link single-use — the standard way to model "consume this token once". A value that can be released and re-claimed (a per-tenant slug that frees up on archive) is not a `unique` path; it is a CCC rule. Make the claimed value itself a scope key so the guard is narrow and lockable: `SlugClaimed { }` with `scopes: ["workspaceProvisionedId", "slug"]` and `SlugReleased` likewise; the drafting command's context is then `{ types: ["SlugClaimed", "SlugReleased"], scopes: { workspaceProvisionedId, slug } }` — two drafts with different slugs never contend, two with the same slug are serialised on exactly that pair. A tenant-wide context (`scopes: { workspaceProvisionedId }` only) also works but serialises every draft of the tenant.

## The store contract

```ts
const read = await es.query(query, { after?, cursor?, settledOnly?, limit?, order?, omit? });
// read.events            matching records (sequence order; (transactionId, sequence) order with `cursor`/`settledOnly`)
// read.byFilter          the same records grouped per filter, in the order of `events` (DCB-style multi-filter contexts)
// read.contextVersion    highest sequence of ALL matching records — the guard. Never narrowed by `after`, `cursor`, `limit`.
// read.lastReturned      highest sequence among returned records — a read cursor. Not the guard.
// read.ctx               { query, version: contextVersion } — pass it straight to appendIf
// read.settledCursor     the gap-free (transactionId, sequence) cursor for durable consumers

await es.append(events);                          // one batch, one consecutive range, all or nothing
await es.appendIf(events, read.ctx);              // { ok: true, appended } | { ok: false, conflict: { expected, actual } }
await es.appendIfOrThrow(events, read.ctx);       // throws ConflictError (httpStatus 409)
```

`contextVersion` and `lastReturned` are two different numbers on purpose (CCC spec, Draft 0.1). Using the read cursor as the expected version silently defeats the guard; `read.ctx` exists so you never build the pair by hand.

**One invariant every deployment must keep:** every writer goes through `append`/`appendIf`. A row inserted by anything else (a migration script, a second service, a DBA) takes no advisory lock, so a concurrent guard can miss it. If you must bulk-load, do it through the SDK's batch `append`.

### Reads that leave a field out

```ts
const list = await es.read(articles, articles.$filter({ types: ["ArticleDrafted", "ArticleContentEdited"] }), { omit: ["body"] });
list.events[0].data.body;   // compile error: omitted
```

`omit` names top-level `data` keys that every returned record leaves out — a projection for lists and overviews that do not need the large field. The Postgres store strips the keys in SQL (`payload - '{body}'`), so the field never travels; `where` and `contextVersion` still see the whole record. Identity is never trimmed: `scopes` and the event's own id key are refused with `UsageError`. `es.read` re-validates against the schema without the omitted keys and types `data` accordingly. Decisions have no `omit`: `command()` and `context()` read complete facts.

### Why `appendIf` is a PL/pgSQL function

The well-known single-statement CTE guard (`WITH context AS (SELECT MAX(...)) INSERT ... WHERE max = $expected`) is **not atomic** under Postgres' default isolation: the statement's snapshot is taken before any lock, so two concurrent writers both see the old version and both commit. Measured: 16 clients, one allowed append — 11 went through.

`@orgops/eventstore` takes sorted advisory locks on every `(scopeKey, value)` pair of the condition *and* of the events being appended, in a statement **before** the version check, inside one function call (`es_append_if_v4`). The function takes a structured description of the context, never SQL text, and builds the version check itself; a role with `EXECUTE` on it cannot run anything else through it. Every append also holds a shared global lock; a condition that cannot be expressed through declared scope keys takes it exclusively. Correct, one round trip, and — because the version check runs on a B-tree on the scope key instead of a GIN bitmap scan — about 16× faster than a correctly locked GIN variant in our benchmarks.

**Strict mode** (default) refuses a guard query that has no declared scope key: the query would be neither indexable nor lockable. Set `strict: false` to accept the global lock instead. The tenant key is treated specially: a guard that names only the tenant serialises that tenant (exclusive tenant lock), while every append holds the tenant lock shared — so scope-level guards never contend on it.

**Lock salt.** Without a salt, advisory-lock keys are FNV-1a hashes of public strings, so any database role could compute and hold them (a held lock stalls that entity's appends until `lock_timeout`). With `lockSalt` (a per-deployment secret) keys are HMAC-SHA256 values; a key observed in `pg_locks` reveals nothing about the others. Every writer on the same database must use the same salt — introduce or change it with a full restart, not a rolling one.

### Commands that read nothing

A pure create (register an account, provision a workspace) has no facts to protect. Omit `context`: `decide` receives the initial state and the append is unconditional — no guard, no lock beyond the events' own scope pairs. Whatever must be unique about the created thing belongs to a `unique` path, or to a command that does declare a context.

```ts
await es.forPlatform().command({
  decide: () => ({ events: [accounts.AccountRegistered({ email })] }),   // UniqueViolationError if the email is taken
});
```

## Contexts, incrementally

```ts
const { state, ctx } = await es.context({ query: articles.$scope("articleDraftedId", id), fold, initial });
await es.appendIf(newEvents, ctx);
```

`es.context()` keeps, per query and per tenant view, the state folded over *settled* events plus a gap-free cursor in an in-process LRU. Each call reads only the delta. Unsettled events (visible, but a transaction that started earlier is still in flight) are folded on top for this decision and not cached. Correctness never depends on the cache: `appendIf` compares the full context version. `es.command()` uses it automatically when you pass a `fold`.

A context larger than `contextCache.maxEvents` (default 100 000) is refused rather than silently truncated — a decision over a truncated context would be wrong. Narrow the query, or raise the limit deliberately.

**Sizing.** The cache holds fold states, not events, so its size follows your folds. Three bounds keep a process under a fixed ceiling: `contextCache.max` entries per tenant view (default 1 000), `contextCache.maxBytes` per view (64 MiB) and `contextCache.totalMaxBytes` for the whole process (256 MiB) — when the total is exceeded the least recently used entries of *any* view are dropped. Sizes come from `estimateSize` (a structural estimate that understands Map/Set; override with `sizeOf`). Tenant views themselves are a bounded LRU too (`maxTenantViews`, default 10 000). A dropped entry or view costs one cold context read on its next command, nothing else. Transient memory during a load is the delta: roughly 0.5–1 KB per event with small payloads, so a cold read of a 10 000-event context uses ~10 MB for a moment.

## Tenants

```ts
configure({ ..., tenant: { scopeKey: "workspaceProvisionedId" } });
const ws = es.forTenant(workspaceId);   // every query narrowed, every event stamped — fail-closed if it claims another tenant
const platform = es.forPlatform();       // accounts, registrations: the nil-UUID tenant
```

`forTenant(id)` is memoised: calling it per request returns the same view with its own context cache. `close()` on a view is a no-op; close the root api. Optionally `postgres: { rls: true }` installs a row-level-security policy on the same expression the index uses and binds every statement of a tenant view to `app.current_tenant` (needs a non-owner application role; see the Operations section).

### Statistics

```ts
await es.forTenant(ws).statistics();                       // [{ type, count, bytes, lastSequence }, …] per event type
await es.statistics(articles.$filter({ types: ["ArticleContentEdited"] }));
```

Count, stored bytes and last sequence per event type, narrowed by a query like any read and by the tenant on a tenant view. Postgres sums `pg_column_size(payload)` — the on-disk size, compressed where TOASTed — without fetching a payload; the memory store reports JSON text length. `statistics()` is an optional store capability (`StatisticsStore`); a custom store may lack it, and the api then throws `UsageError`. Deliberately not in the package: a cross-tenant directory ("whose account, token or key is this?") — under RLS that is a `SECURITY DEFINER` function of the application, owned and audited there.

## Subscriptions (`@orgops/eventstore/subscribe`)

```ts
import { subscribe, fileCursors, resetCursor, on } from "@orgops/eventstore/subscribe";

const cursors = fileCursors("./data/cursors.json");   // default: memoryCursors() (replays from `from` after a restart)
const sub = subscribe("search-index", articles.$filter(), async (events) => { … }, {
  store: await es.store(),
  cursors,
  onError: (error, batch) => "retry",            // or "skip" | "stop"; batch is empty for store errors
});
await sub.whenCaughtUp();                        // resolves on the next empty page
await resetCursor("search-index", cursors);      // forget the cursor → replay from `from`

on(articles.$filter(), (events) => sse.push(events), { store: memoryStore });   // in-process, fire-and-forget; the store must be a LiveStore (MemoryStore)
```

Durable, gap-free, at-least-once: reads settled events beyond a `(transactionId, sequence)` cursor, advances only after your handler resolved, retries with back-off. `events` stays the only table — the cursor store is pluggable. Subscriptions are not owned by `es`: stop them (`await sub.stop()`) before `es.close()`, or their next poll fails against a closed pool and is retried until you stop it.

## Testing (`@orgops/eventstore/testing`)

```ts
import { given, conformanceSuite, interferingStore } from "@orgops/eventstore/testing";

const drafted = articles.ArticleDrafted({ title: "A", slug: "a" }, { workspaceProvisionedId: ws });
await given([drafted], { schema: buildSchema([articles]) })     // pass the schema, or scopes/uniques/strict are not enforced
  .when(archiveArticle(drafted.id))                              // a function returning a CommandSpec
  .then([{ type: "ArticleArchived", scopes: { articleDraftedId: drafted.id } }]);   // ids are never compared

// your own store implementation? run the same behavioural spec the SDK runs against MemoryStore and Postgres
describe("my store", () => conformanceSuite((schema) => new MyStore({ schema }), { test: it }));
```

`MemoryStore` is the semantic reference; the conformance suite runs the same behavioural spec against it and against Postgres. With `new MemoryStore({ transactions: { allocateId, isSettled } })` it can even simulate what Postgres does for real — a batch whose transaction id sorts below a later one, rows that are visible but not yet settled — so cursor and cache behaviour is specified in memory and checked against Postgres. `interferingStore` injects competing appends between read and write so your retry paths are exercised.

## Errors and outcomes

| | |
|---|---|
| business rejection | `{ ok: false, code, reason, missing? }` — `httpStatusOf()` gives 422 / 404 |
| conflict after retries | `{ ok: false, code: "conflict", conflict }` — 409 |
| `ConflictError`, `UniqueViolationError` | 409 (`UniqueViolationError.detail` names type and path; the value is never echoed) |
| `ValidationError` | 400 — `issues` lists message, path and code |
| `UnindexableContextError` | 400 — a guard query without a declared scope key in strict mode |
| `TransientError` | 503 — deadlock victim, serialization failure, lock timeout; `es.command()` retries these |
| `PolicyViolationError` | 403 — the database refused the statement: an RLS policy, or a missing `GRANT` on the table or the append function |
| `TenantMismatchError` | 403 — a query or event names a tenant other than the view's |
| `UsageError` | 400 — a programming error surfaced at request time (undeclared type in `$parse`, invalid query shape) |
| `ContextTooLargeError` | 500 — a context exceeds `contextCache.maxEvents` |
| `NotConfiguredError` | 500 — `es` used before `configure()` |
| `EventStoreError` | 500 — base class; unmapped store failure |

Every error the store raises extends `EventStoreError` and carries `httpStatus` and, where there is one, `cause`. Invalid declarations (`defineEvents` with a bad type name) throw plain `Error`s at module load.

## What it deliberately does not do

- No streams, no aggregates, no expected-stream-version: the context is a query.
- No read models in the core; indexes are not read models.
- No `CREATE DATABASE` at runtime.
- No DCB tags: scopes in the payload carry the same information, and the store indexes them.
- No delete. The store is append-only; erasing a tenant is an operator's act with its own proof — see "Erasing a tenant" under Operations.

## Explicit instance

```ts
import { createEventStore } from "@orgops/eventstore";
const store = createEventStore({ connection, events: [articles] });   // no global state
```

## Internals

`@orgops/eventstore/internal` exposes the lock-key derivation and the wire payload helpers for people implementing their own store. They are not part of the semver-stable surface.

## Operations

- **Install.** With `install: "auto"` (default) the first query or append creates the table, the `es_scope()` function, the append function and every index, idempotently and under a session advisory lock, so concurrent boots do not race. With `install: "none"` nothing is touched; print the DDL with `printSchemaSql(es.schema)` and run it yourself.
- **Adding a scope key or a unique path** adds an index. On a large table prefer running the printed `CREATE INDEX` statement as `CONCURRENTLY` yourself before deploying the declaration.
- **`es_scope()`** is the expression every scope index, the tenant-keyed idempotency index and the RLS policy are built on. It is an inlinable `IMMUTABLE` SQL function, so the *inlined expression* is what the indexes store. The SDK fingerprints its body on the function and on every index built from it; when the installed body differs from what an index was built with, the installer refuses to continue and throws an error listing the exact statements (`DROP/CREATE INDEX CONCURRENTLY`, statistics, comments) — run them yourself, or pass `postgres: { rebuildScopeIndexes: true }` to rebuild inline during a maintenance window. With the inlined body a stale index is a performance problem (the planner stops matching it); with an opaque body it would be a correctness problem, which is why the gate exists.
- **`upcast`** may reshape an event's `data` on read; the own id and the `scopes` object always come from the stored payload, because that is what the store indexed, locked and matched on.
- **Roles for RLS.** The install runs as the table owner; the application connects as a non-owner role (`FORCE ROW LEVEL SECURITY` applies to everyone but a superuser). Tenant views bind `app.current_tenant` per transaction with `set_config(..., true)`, never session-wide, so pooled connections cannot leak a tenant.
- **Scope indexes under RLS need three `LEAKPROOF` marks.** Under row-level security Postgres uses a condition as an index condition only when every function in it is `LEAKPROOF`; `es_scope` inlines to `jsonb_typeof`, `->` and `->>`, which it does not ship as such, and no jsonb function or operator is (`@>` included). Without the marks a non-owner role uses the tenant's index only and filters every other scope over the whole tenant (measured: 40 ms instead of 0.1 ms for a four-key read over 27 000 events). The installer sets them when it runs as a superuser and records a warning in `warnings()` otherwise; `scopeLeakproofStatements()` and `printSchemaSql({ rls: true })` give the statements. They are per database and neither `pg_dump` nor `pg_upgrade` carries them — rerun after a restore. The three never raise an error on any input, so they reveal nothing but their result; the policy still decides what anyone sees.
- **Statistics objects cost planning time.** Postgres loads every statistics object of a table for every reference to it in every statement, named key or not: with 43 scope keys they were two thirds of the planning time of a four-key read (7 ms of 11 on the bench machine, execution 0.1 ms). `postgres: { scopeStatistics: { minIndexRows: DEFAULT_SCOPE_STATISTICS_MIN_ROWS } }` keeps objects only for keys whose index holds at least 1 000 rows (below that the planner never costs the index above its size, so a wrong estimate costs at most that index); a key that grows past the limit gets its object at the next install — `ANALYZE` afterwards. `exclude` names keys whose expression already has statistics from a non-partial expression index of your own. The default stays `"all"`.
- **Erasing a tenant** (a right-to-erasure request, a closed workspace) is not an SDK call: the store is append-only, and deletion should stay a deliberate, logged act of the operator. The recipe: (1) stop the tenant's traffic; (2) as the table owner, `DELETE FROM events WHERE es_scope(payload, '<tenantKey>') = $1` — the tenant scope index makes it a range delete; under `rls` run it in a transaction with `set_config('app.current_tenant', $1, true)`; (3) `es.forTenant(id).invalidate()` in every process, or restart them — a context cache may still hold the folded state; (4) durable subscribers need nothing: cursors are `(transactionId, sequence)` positions, and gaps are ordinary; (5) idempotency keys and unique values of the tenant are free again — decide whether that is what you want; (6) record the proof as an event on the platform tenant (a `TenantErased` with the count from `statistics()` taken before the delete) — that is the Löschnachweis, and it is the only row about the tenant that remains.
- **Timeouts.** The SDK sets `lock_timeout` (10 s), `statement_timeout` (30 s) and `idle_in_transaction_session_timeout` (30 s) per connection, so a stuck lock holder degrades to `TransientError` instead of a hang; tune them with `postgres: { timeouts: { lockMs, statementMs, idleInTransactionMs } }`. If you pass your own `pg.Pool` as `connection`, set them yourself — the SDK does not touch a pool it did not create, and `close()` leaves it open.
- **Durability knobs.** Single-event appends are bound by commit durability: `synchronous_commit = off` (per session or transaction) gives +40–75 % on them and loses at most the last ~600 ms on a crash — acceptable for many event stores, never for money. Batches of 50 barely change (group commit already amortises fsync); at four writers the raw ceiling on our test box was ~200 000 events/s.
- **Migrating an existing `events` table** (one with a `sequence` column and JSONB payloads in the same convention): rename `sequence` → `sequence_number`, add `metadata jsonb NOT NULL DEFAULT '{}'`, add `transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id()` (backfill legacy rows with one low constant such as `'3'::xid8` so they sort before every future row), then run `printSchemaSql(es.schema, pg)` for functions and indexes (`CREATE INDEX … CONCURRENTLY` outside a transaction) and boot with `install: "none"`. Legacy rows without an own id read back as `id: "~<sequence>"`; a durable subscriber must start at `{ transactionId: "3", sequence: 0 }`, not `"now"`.
- **Every writer through the SDK.** See the invariant under "The store contract".

## License

MIT
