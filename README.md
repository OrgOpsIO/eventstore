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

On first use the SDK creates the table, its function and every index it needs — idempotently, under an advisory lock, so concurrent boots are safe. Pass `postgres: { schema: "none" }` and `printSchemaSql()` to hand the DDL to a DBA instead.

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

- **typed constructors**: `articles.ArticleDrafted({ title, slug }, { workspaceProvisionedId })` — wrong fields and missing scopes are compile errors; invalid values throw `ValidationError`. The event's own id (`articleDraftedId`, UUIDv7) is generated unless you pass one.
- **typed folds**: `articles.$fold(initial, { ArticleDrafted: (data, state, event) => … })` — no `payload.x as string` anywhere.
- **typed filters**: `articles.$scope("articleDraftedId", id)` is everything that happened in relation to that article, root event included; `articles.$filter({ types: [...], scopes: {...} })` for anything else.
- **the store schema**: a B-tree per declared scope key, a unique index per `unique` path, an idempotency-key index, `(event_type, sequence_number)`, `(transaction_id, sequence_number)`.

On the wire an event is `{ articleDraftedId, title, slug, scopes: { workspaceProvisionedId } }` in a JSONB `payload` column — Ralf Westphal's convention as used in an earlier in-house store. Flat ids (`employeeId` as a plain field) keep working: a scope key matches `scopes.K`, the event's own id, or a top-level field `K`.

## The store contract

```ts
const read = await es.query(query, { after?, cursor?, settledOnly?, limit?, order? });
// read.events            matching records, ascending
// read.byFilter          the same records grouped per filter (for DCB-style multi-filter contexts)
// read.contextVersion    highest sequence of ALL matching records — the guard. Never narrowed by `after`.
// read.lastReturned      highest sequence among returned records — a read cursor. Not the guard.

await es.append(events);                          // one batch, one consecutive range, all or nothing
await es.appendIf(events, { query, version });    // { ok: true, appended } | { ok: false, conflict: { expected, actual } }
await es.appendIfOrThrow(events, ctx);            // throws ConflictError (httpStatus 409)
```

`contextVersion` and `lastReturned` are two different numbers on purpose (CCC spec, Draft 0.1). Using the read cursor as the expected version silently defeats the guard; the SDK never lets you confuse them.

### Why `appendIf` is a PL/pgSQL function

The well-known single-statement CTE guard (`WITH context AS (SELECT MAX(...)) INSERT ... WHERE max = $expected`) is **not atomic** under Postgres' default isolation: the statement's snapshot is taken before any lock, so two concurrent writers both see the old version and both commit. Measured: 16 clients, one allowed append — 11 went through.

`@orgops/eventstore` takes sorted advisory locks on every `(scopeKey, value)` pair of the condition *and* of the events being appended, in a statement **before** the version check, inside one function call. Every append also holds a shared global lock; a condition that cannot be expressed through declared scope keys takes it exclusively. Correct, one round trip, and — because the version check runs on a B-tree on the scope key instead of a GIN bitmap scan — about 16× faster than a correctly locked GIN variant in our benchmarks.

**Strict mode** (default) refuses a guard query that has no declared scope key: the query would be neither indexable nor lockable. Set `strict: false` to accept the global lock instead.

## Contexts, incrementally

```ts
const { state, ctx } = await es.context({ query: articles.$scope("articleDraftedId", id), fold, initial });
await es.appendIf(newEvents, ctx);
```

`es.context()` keeps, per query, the state folded over *settled* events plus a gap-free cursor in an in-process LRU. Each call reads only the delta. Unsettled events (visible, but a transaction that started earlier is still in flight) are folded on top for this decision and not cached. Correctness never depends on the cache: `appendIf` compares the full context version. `es.command()` uses it automatically when you pass a `fold`.

## Tenants

```ts
configure({ ..., tenant: { scopeKey: "workspaceProvisionedId" } });
const ws = es.forTenant(workspaceId);   // every query narrowed, every event stamped — fail-closed if it claims another tenant
const platform = es.forPlatform();       // accounts, registrations: the nil-UUID tenant
```

Optionally `postgres: { rls: true }` installs a row-level-security policy on the same expression the index uses (needs a non-owner application role; see `withTenantSession`).

## Subscriptions (`@orgops/eventstore/subscribe`)

```ts
import { subscribe, fileCursors, on } from "@orgops/eventstore/subscribe";

const sub = subscribe("search-index", articles.$filter(), async (events) => { … }, {
  store: await es.store(),
  cursors: fileCursors("./data/cursors.json"),   // default: in memory (replays from `from` after a restart)
});
await sub.whenCaughtUp();

on(articles.$filter(), (events) => sse.push(events), memoryStore);   // in-process, fire-and-forget
```

Durable, gap-free: reads settled events beyond a `(transactionId, sequence)` cursor, advances only after your handler resolved, retries with back-off. `events` stays the only table — the cursor store is pluggable.

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
| `ConflictError`, `UniqueViolationError` | 409 |
| `ValidationError` | 400 |
| `UnindexableContextError` | a guard query without a declared scope key in strict mode |

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

## License

MIT
