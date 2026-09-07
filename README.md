# @orgops/eventstore

**One `events` table in PostgreSQL. Typed events. An atomic conditional append that is actually atomic. Indexes derived from your event declarations.**

Command Context Consistency (Rico Fritzsche) with Westphal scopes, configured once and used anywhere — the way [`@orgops/coax`](https://github.com/OrgOpsIO/coax) does it for LLMs.

```bash
npm install @orgops/eventstore zod pg
```

`zod` (v4) is a peer dependency. `pg` is only needed for the Postgres store.

## Configure once, use `es` everywhere

```ts
// server/plugins/eventstore.ts — once at startup
import { configure } from "@orgops/eventstore";
import { articles } from "../features/articles/events";
import { accounts } from "../features/accounts/events";

configure({
  connection: process.env.DATABASE_URL!,
  events: [articles, accounts],                       // typing, validation, indexes, locks, uniques
  tenant: { scopeKey: "workspaceProvisionedId" },     // the tenant is a scope in the payload, not a column
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
// { ok: true, appended } | { ok: false, code, reason, missing? } | conflict after retries
```

On first use the SDK creates the table, its function and every index it needs — idempotently, under an advisory lock, so concurrent boots are safe. In locked-down environments pass `postgres: { install: "none" }` and hand the DDL to a DBA:

```ts
import { printSchemaSql } from "@orgops/eventstore/postgres";
console.log(printSchemaSql(es.schema, { table: "events" }));
```

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
});
```

From this one declaration you get:

- **typed constructors**: `articles.ArticleDrafted({ title, slug }, { workspaceProvisionedId })` — wrong fields, missing scopes and unknown scope keys are compile errors; invalid values throw `ValidationError`. The constructor generates the event's own id (`articleDraftedId`, a UUIDv7) unless you pass `{ id }` in the third argument, so `drafted.id` is usable right away.
- **typed folds**: `articles.$fold({ ArticleDrafted: (data, state, event) => … })` is the incremental fold `(delta, state) => state` that `es.context()` and `es.command()` take; `event.scopes.workspaceProvisionedId` is a `string`, not `string | undefined`. `articles.$foldAll(initial, handlers)` folds a complete list in one go and is deliberately not assignable to the incremental shape.
- **typed filters**: `articles.$scope("articleDraftedId", id)` is everything of this registry that happened in relation to that article, root event included; the free `scope(key, id)` does the same across registries; `articles.$filter({ types: [...], scopes: {...} })` for anything else.
- **the store schema**: a B-tree per declared scope key, a unique index per `unique` path, an idempotency-key index, `(event_type, sequence_number)`, `(transaction_id, sequence_number)`.

On the wire an event is `{ articleDraftedId, title, slug, scopes: { workspaceProvisionedId } }` in a JSONB `payload` column — Ralf Westphal's convention as used in an earlier in-house store. Flat ids (`employeeId` as a plain field) keep working: a scope key matches `scopes.K`, the event's own id, or a top-level string field `K`. The envelope rules hold for every event, declared or not: `data` may not contain the own id key or a `scopes` key, and a value at a declared scope key must be a string.

## The store contract

```ts
const read = await es.query(query, { after?, cursor?, settledOnly?, limit?, order? });
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

### Why `appendIf` is a PL/pgSQL function

The well-known single-statement CTE guard (`WITH context AS (SELECT MAX(...)) INSERT ... WHERE max = $expected`) is **not atomic** under Postgres' default isolation: the statement's snapshot is taken before any lock, so two concurrent writers both see the old version and both commit. Measured: 16 clients, one allowed append — 11 went through.

`@orgops/eventstore` takes sorted advisory locks on every `(scopeKey, value)` pair of the condition *and* of the events being appended, in a statement **before** the version check, inside one function call. Every append also holds a shared global lock; a condition that cannot be expressed through declared scope keys takes it exclusively. Correct, one round trip, and — because the version check runs on a B-tree on the scope key instead of a GIN bitmap scan — about 16× faster than a correctly locked GIN variant in our benchmarks.

**Strict mode** (default) refuses a guard query that has no declared scope key: the query would be neither indexable nor lockable. Set `strict: false` to accept the global lock instead. The tenant key is treated specially: a guard that names only the tenant serialises that tenant (exclusive tenant lock), while every append holds the tenant lock shared — so scope-level guards never contend on it.

**Lock salt.** Advisory-lock keys are hashes of public strings, so any database role could compute and hold them. Pass `lockSalt` (a per-deployment secret) in `configure()` to make them unguessable.

## Contexts, incrementally

```ts
const { state, ctx } = await es.context({ query: articles.$scope("articleDraftedId", id), fold, initial });
await es.appendIf(newEvents, ctx);
```

`es.context()` keeps, per query and per tenant view, the state folded over *settled* events plus a gap-free cursor in an in-process LRU. Each call reads only the delta. Unsettled events (visible, but a transaction that started earlier is still in flight) are folded on top for this decision and not cached. Correctness never depends on the cache: `appendIf` compares the full context version. `es.command()` uses it automatically when you pass a `fold`.

A context larger than `contextCache.maxEvents` (default 100 000) is refused rather than silently truncated — a decision over a truncated context would be wrong. Narrow the query, or raise the limit deliberately.

## Tenants

```ts
configure({ ..., tenant: { scopeKey: "workspaceProvisionedId" } });
const ws = es.forTenant(workspaceId);   // every query narrowed, every event stamped — fail-closed if it claims another tenant
const platform = es.forPlatform();       // accounts, registrations: the nil-UUID tenant
```

`forTenant(id)` is memoised: calling it per request returns the same view with its own context cache. `close()` on a view is a no-op; close the root api. Optionally `postgres: { rls: true }` installs a row-level-security policy on the same expression the index uses and binds every statement of a tenant view to `app.current_tenant` (needs a non-owner application role; see the Operations section).

## Subscriptions (`@orgops/eventstore/subscribe`)

```ts
import { subscribe, fileCursors, resetCursor, on } from "@orgops/eventstore/subscribe";

const sub = subscribe("search-index", articles.$filter(), async (events) => { … }, {
  store: await es.store(),
  cursors: fileCursors("./data/cursors.json"),   // default: in memory (replays from `from` after a restart)
  onError: (error, batch) => "retry",            // or "skip" | "stop"; batch is empty for store errors
});
await sub.whenCaughtUp();                        // resolves on the next empty page
await resetCursor("search-index", cursors);      // forget the cursor → replay from `from`

on(articles.$filter(), (events) => sse.push(events), { store: memoryStore });   // in-process, fire-and-forget, LiveStore only
```

Durable, gap-free, at-least-once: reads settled events beyond a `(transactionId, sequence)` cursor, advances only after your handler resolved, retries with back-off. `events` stays the only table — the cursor store is pluggable.

## Testing (`@orgops/eventstore/testing`)

```ts
import { given, conformanceSuite, interferingStore } from "@orgops/eventstore/testing";

await given([articles.ArticleDrafted({ title: "A", slug: "a" }, { workspaceProvisionedId: ws })])
  .when(archiveArticle(id))
  .then([articles.ArticleArchived({}, { articleDraftedId: id })]);
```

`MemoryStore` is the semantic reference; the conformance suite runs the same behavioural spec against it and against Postgres. `interferingStore` injects competing appends between read and write so your retry paths are exercised.

## Errors and outcomes

| | |
|---|---|
| business rejection | `{ ok: false, code, reason, missing? }` — `httpStatusOf()` gives 422 / 404 |
| conflict after retries | `{ ok: false, code: "conflict", conflict }` — 409 |
| `ConflictError`, `UniqueViolationError` | 409 (`UniqueViolationError.detail` names type and path; the value is never echoed) |
| `ValidationError` | 400 — `issues` lists message, path and code |
| `UnindexableContextError` | 400 — a guard query without a declared scope key in strict mode |
| `TransientError` | 503 — deadlock victim, serialization failure, lock timeout; `es.command()` retries these |
| `PolicyViolationError` | 403 — row-level security refused the statement |

Every error extends `EventStoreError` and carries `httpStatus` and, where there is one, `cause`.

## What it deliberately does not do

- No streams, no aggregates, no expected-stream-version: the context is a query.
- No read models in the core; indexes are not read models.
- No `CREATE DATABASE` at runtime.
- No DCB tags: scopes in the payload carry the same information, and the store indexes them.

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
- **`es_scope()`** is the expression every scope index, the unique indexes and the RLS policy are built on. It is `IMMUTABLE` by contract: if its body ever changes, `REINDEX` the table. The SDK does not change it in place.
- **Roles for RLS.** The install runs as the table owner; the application connects as a non-owner role (`FORCE ROW LEVEL SECURITY` applies to everyone but a superuser). Tenant views bind `app.current_tenant` per transaction with `set_config(..., true)`, never session-wide, so pooled connections cannot leak a tenant.
- **Advisory locks** are transaction-scoped and released on commit or rollback; set `lock_timeout` on the application role so a stuck holder degrades to `TransientError` instead of a hang.
- **Every writer through the SDK.** See the invariant under "The store contract".

## License

MIT
