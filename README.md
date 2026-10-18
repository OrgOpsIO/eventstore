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
const read = await es.query(query, { after?, until?, cursor?, settledOnly?, limit?, order?, omit?, data?: false, payload?: false, version?: false });
// read.events            matching records (sequence order; (transactionId, sequence) order with `cursor`/`settledOnly`)
// read.byFilter          the same records grouped per filter, in the order of `events` (DCB-style multi-filter contexts)
// read.contextVersion    highest sequence of ALL matching records — the guard. Never narrowed by `after`, `cursor`, `limit`.
// read.lastReturned      highest sequence among returned records — a read cursor. Not the guard.
// read.ctx               { query, version: contextVersion } — pass it straight to appendIf
// read.settledCursor     the gap-free (transactionId, sequence) cursor for durable consumers
// read.watermark?        every transaction below this id had ended when the read ran (absent when the store cannot tell)

await es.append(events);                          // one batch, one consecutive range, all or nothing: { first, last, count, transactionId }
await es.appendIf(events, read.ctx);              // { ok: true, appended } | { ok: false, conflict: { expected, actual } }
await es.appendIfOrThrow(events, read.ctx);       // throws ConflictError (httpStatus 409)
```

`appended.transactionId` is the transaction every record of the batch carries; with `appended.last` it is the read-your-writes token a subscriber can be asked about (`sub.hasPassed(token)`, below). A custom `EventStore` must return it; `watermark` is optional, and a store that leaves it out only makes `hasPassed` answer by the cursor alone.

`contextVersion` and `lastReturned` are two different numbers on purpose (CCC spec, Draft 0.1). Using the read cursor as the expected version silently defeats the guard; `read.ctx` exists so you never build the pair by hand.

**One invariant every deployment must keep:** every writer goes through `append`/`appendIf`. A row inserted by anything else (a migration script, a second service, a DBA) takes no advisory lock, so a concurrent guard can miss it. If you must bulk-load, do it through the SDK's batch `append`.

### Reads that leave a field out

```ts
const list = await es.read(articles, articles.$filter({ types: ["ArticleDrafted", "ArticleContentEdited"] }), { omit: ["body"] });
list.events[0].data.body;   // compile error: omitted
```

`omit` names top-level `data` keys that every returned record leaves out — a projection for lists and overviews that do not need the large field. The Postgres store strips the keys in SQL (`payload - '{body}'`), so the field never travels; `where` and `contextVersion` still see the whole record. Identity is never trimmed: `scopes` and the event's own id key are refused with `UsageError`. `es.read` re-validates against the schema without the omitted keys and types `data` accordingly. Decisions have no `omit`: `command()` and `context()` read complete facts.

### Lean reads

`omit` leaves named fields out; three switches go further, for reads that route, count or decide nothing:

- `data: false` — records come back without their data: id and `scopes` stay, and so do declared scope keys carried flat in data (every back-link a query matched on). The Postgres store ships `scopes` and top-level strings up to 1 kB, so a large field (a whole HTML page) never leaves the database — it still unpacks the payload to find them.
- `payload: false` — the payload column is not read at all: type, sequence, time and metadata only; `scopes` is `{}` and the id reads `~<sequence>`. Nothing is unpacked.
- `version: false` — no context version is computed (one index lookup per scope value); `contextVersion` is `-1` and the `ctx` can never guard.

A read may list up to 10 000 values of a scope key (beyond eight they become one `IN (…)`); a guard is held to 256, because every value of a guard is an advisory lock. `es.read` refuses `data: false` and `payload: false` — it validates data against the registry.

### Everything in a scope except …

```ts
// a reader that has no use for the large usage records
await es.query({ scopes: { documentCreatedId: id }, not: { types: ["ModelUsageRecorded"] } });

// a long-running job's guard: its own progress events must not conflict with its final append
const ctx = (await es.query({ scopes: { documentCreatedId: id }, not: [{ types: ["ModelUsageRecorded"] }, { scopes: { jobStartedId: job } }] })).ctx;
```

`not` takes a filter or a list of filters; a record that matches any of them is left out — of the rows **and** of the context version, so `appendIf` re-checks exactly what was read. That is why it beats a positive type list for a guard: a type added later is inside the guard until someone excludes it, instead of silently outside. A record that lacks a negated scope key stays in. Locks come from the positive part only (a negation never widens or narrows them), so the guard is as lockable as it was without `not`, and a negated scope key does not have to be declared. `not` cannot be nested; an empty negation is refused, and so is a `where` predicate inside it that is empty or holds `undefined` — `{ jobId: undefined }` would become `{}`, match every record and leave the guard with a version that never moves.

### The store as it was at event n

```ts
const past = es.forTenant(t).asOf(sequence);   // read-only
await past.query(q);                           // only records with sequence <= n; contextVersion within the cutoff
await past.context({ query, fold, initial });  // the same fold code as live, over the past
await past.append(events);                     // UsageError — and so do appendIf, appendIfOrThrow, command
past.asOf(m);                                  // min(n, m): a nested view only narrows
```

For questions like "since which event has this rule been broken?": fold the store as it was at a handful of candidate events with the same code that reads it live. Underneath is `QueryOptions.until` (inclusive), which — unlike `after` and `cursor` — caps the context version as well. A past view has no context cache, so live state never answers a past read and past state is never kept as live; `forTenant` on a past view stays in the past, and `statistics()` is refused there. The cutoff is on `sequence`: a record with a lower sequence can still commit after `n` became visible, so a read that must give the same answer every time adds `settledOnly: true`.

### Why `appendIf` is a PL/pgSQL function

The well-known single-statement CTE guard (`WITH context AS (SELECT MAX(...)) INSERT ... WHERE max = $expected`) is **not atomic** under Postgres' default isolation: the statement's snapshot is taken before any lock, so two concurrent writers both see the old version and both commit. Measured: 16 clients, one allowed append — 11 went through.

`@orgops/eventstore` takes sorted advisory locks on every `(scopeKey, value)` pair of the condition *and* of the events being appended, in a statement **before** the version check, inside one function call (`es_append_if_v5`). The function takes a structured description of the context, never SQL text, and builds the version check itself; a role with `EXECUTE` on it cannot run anything else through it. Every append also holds a shared global lock; a condition that cannot be expressed through declared scope keys takes it exclusively. Correct, one round trip, and — because the version check runs on a B-tree on the scope key instead of a GIN bitmap scan — about 16× faster than a correctly locked GIN variant in our benchmarks.

**Strict mode** (default) refuses a guard query that has no declared scope key: the query would be neither indexable nor lockable. Set `strict: false` to accept the global lock instead. The tenant key is treated specially: a guard that names only the tenant serialises that tenant (exclusive tenant lock), while every append holds the tenant lock shared — so scope-level guards never contend on it.

**Lock salt.** Without a salt, advisory-lock keys are FNV-1a hashes of public strings, so any database role could compute and hold them (a held lock stalls that entity's appends until `lock_timeout`). With `lockSalt` (a per-deployment secret) keys are HMAC-SHA256 values; a key observed in `pg_locks` reveals nothing about the others. Every writer on the same database must use the same salt — introduce or change it with a full restart, not a rolling one.

### Commands that read nothing

A pure create (register an account, provision a workspace) has no facts to protect. Omit `context`: `decide` receives the initial state and the append is unconditional — no guard, no lock beyond the events' own scope pairs. Whatever must be unique about the created thing belongs to a `unique` path, or to a command that does declare a context.

```ts
await es.forPlatform().command({
  decide: () => ({ events: [accounts.AccountRegistered({ email })] }),   // UniqueViolationError if the email is taken
});
```

## Kept state: four kinds

Folds may be materialized; a materialized fold is a cache of your fold, never a second source of truth. Every kind below answers what folding the events from zero would answer, and **decisions stay on the store**: a command reads `es.context` / `es.command`, and `appendIf` checks the context version — never a read model.

| kind | where the state lives | a warm read folds | after a restart | decide on it? | use it when |
|---|---|---|---|---|---|
| fold on read (`es.context`) | the process, per query | the delta | folds from zero | yes — it carries the context version | every decision; reads of a few contexts |
| persisted fold (`defineFold` + `persistedFolds`) | the process, and the store beside the events | the delta | folds only what came after the stored state | yes — same as fold on read | cold processes (deploys, new containers) fold large contexts from zero |
| live read model (`defineReadModel` + `readModel`) | the process, per key | nothing | rebuilds from the store | no — read it, decide with `es.command` | one process reads the same states often: screens, lists, SSE |
| persisted read model (`defineReadModel` + `persistedReadModel` + `persistedModels`) | the store, per key, with its checkpoint | `get`: the key's delta (0 events when nothing is new); `list`: nothing | resumes at the checkpoint and folds only what came after it | no — read it, decide with `es.command` | many processes read the same states, or a rebuild per restart costs too much |

Every kept fold has a **name and a version**; a new version is another state, built from zero (beside the old one, for persisted read models).

### 1. Fold on read

The state of one query, folded from the events and kept in the process's context cache; each call folds only what is new. The one kind a command decides on. More in "Contexts, incrementally".

```ts
const { state, ctx } = await es.context({ query: sales.$scope("offerId", offerId), fold, initial });
await es.appendIf([sales.OfferAccepted({}, { offerId })], ctx); // decided on the store: the context version
```

### 2. Persisted fold

The same fold with a name and a version, its state also kept in the store beside the events: a cold process starts from it and folds only the delta. More in "Persisted folds".

```ts
const offerState = defineFold({ name: "offer", version: 1, fold }); // a name and a version
configure({ connection, events: [sales], postgres: { persistedFolds: true } });
const { state: fromStored } = await es.context({ query: sales.$scope("offerId", offerId), fold: offerState, initial });
```

### 3. Live read model

One state per key, kept in memory by one feed in the process; a read folds nothing. After a restart it rebuilds. More in "Live read models".

```ts
const offers = defineReadModel({ name: "offers", version: 1, source: sales.$filter(), keyBy: "offerId", fold, initial });
const live = es.forTenant(t).readModel(offers);
const offer = await live.get(offerId); // from memory: 0 events folded
```

### 4. Persisted read model

The same definition, kept as rows plus a checkpoint in the store, written in one transaction: it resumes after a restart and never applies an event twice. More in "Persisted read models".

```ts
configure({ connection, events: [sales], postgres: { persistedModels: true } });
const stored = es.forTenant(t).persistedReadModel(offers); // the definition of the live model
const page = await stored.list({ where: { status: "open" }, sort: { by: "amount", dir: "desc" } });
```

## Contexts, incrementally

*Kind 1 of "Kept state: four kinds": fold on read.*

```ts
const { state, ctx } = await es.context({ query: articles.$scope("articleDraftedId", id), fold, initial });
await es.appendIf(newEvents, ctx);
```

`es.context()` keeps, per query and per tenant view, the state folded over *settled* events plus a gap-free cursor in an in-process LRU. Each call reads only the delta. Unsettled events (visible, but a transaction that started earlier is still in flight) are folded on top for this decision and not cached. Correctness never depends on the cache: `appendIf` compares the full context version. `es.command()` uses it automatically when you pass a `fold`.

A context larger than `contextCache.maxEvents` (default 100 000) is refused rather than silently truncated — a decision over a truncated context would be wrong. Narrow the query, or raise the limit deliberately. With the persisted fold tier on (see "Persisted folds"), such a context loads in pages of `maxEvents` instead, and `ContextTooLargeError` remains only for more than `maxEvents` events still to fold that are unsettled (an earlier transaction in flight).

**Sizing.** The cache holds fold states, not events, so its size follows your folds. Three bounds keep a process under a fixed ceiling: `contextCache.max` entries per tenant view (default 1 000), `contextCache.maxBytes` per view (64 MiB) and `contextCache.totalMaxBytes` for the whole process (256 MiB) — when the total is exceeded the least recently used entries of *any* view are dropped. Sizes come from `estimateSize` (a structural estimate that understands Map/Set; override with `sizeOf`). Tenant views themselves are a bounded LRU too (`maxTenantViews`, default 10 000). A dropped entry or view costs one cold context read on its next command, nothing else. Transient memory during a load is the delta: roughly 0.5–1 KB per event with small payloads, so a cold read of a 10 000-event context uses ~10 MB for a moment.

**Where the time goes.** `contextCache.onLoad` is called after every load (contexts and commands alike) with what it cost: `readMs` in the store, `foldMs` in your fold, `sizeMs` measuring the state for the budget, plus `events` folded, `cacheHit`, `bytes`, the normalised `query`, the view's `tenant` and the `name` you gave the spec (`es.context({ name: "workspaceRoot", … })`, `es.command({ name, … })`). `start` says where the state came from — `"cache"` (in memory), `"stored"` (a persisted fold, below) or `"initial"` (folded from zero) — and `stored` what the persisted fold tier cost when it took part. Group by `name` to find the folds worth optimising; an exception in the callback never fails a load.

## Persisted folds

*Kind 2 of "Kept state: four kinds": the persisted fold.*

The context cache lives in the process: after a deploy, in a new container, every context is folded from zero again. A **persisted fold** is a second tier behind it: the state of a fold is also kept in the store, beside the events, with the cursor it was folded up to — so a cold process starts from that state and folds only what came after it.

```ts
import { configure, defineFold, es } from "@orgops/eventstore";

// 1. the fold gets an identity: a name and a version
export const ledgerStats = defineFold({
  name: "ledger-stats",
  version: 1,
  fold: ledger.$foldBy<string, { count: number; total: number }>((e) => e.data.category, {
    EntryBooked: (d, s) => ({ count: (s?.count ?? 0) + 1, total: (s?.total ?? 0) + d.amount }),
  }),
});

// 2. the store keeps fold states (memory: new MemoryStore({ schema, persistedFolds: true }))
configure({ connection, events: [ledger], postgres: { persistedFolds: true } });

const { state, ctx } = await es.context({ query: ledger.$scope("ledgerId", id), fold: ledgerStats, initial: () => new Map() });
```

It is on only when both hold: the store has `persistedFolds: true`, and the fold was made by `defineFold`. Everything else loads exactly as before. `contextCache: { persist: false }` or `contextCache: false` turns it off for a runtime; a past view (`asOf`) never reads or writes it.

**A cache of your fold, never a second source of truth.** Every load still reads the delta beyond the stored cursor from the store, and the context version with it, so `appendIf` and `es.command()` decide exactly as before: a process that started from a stored state and missed a concurrent write gets a conflict and retries. Only *settled* events are ever stored (the same rule as the in-memory entry); events of a transaction still in flight are folded on top for the answer and read again next time.

**The version is the only signal that the fold changed.** Bump it when the fold's code changes, when the event schemas or upcasters it sees change, or when what the initial state means changes. Another version is another state, in its own row: a stored state of another version is never used. During a rolling deploy each process loads the version its code has — old processes keep reading and writing the old rows, new ones build the new version's state on their first load of each context, from zero, and store it. A bump so refolds each context once, on its first load in new code (`onLoad` reports it as `start: "initial"` with its `events`); to warm the new version before traffic, load your hottest contexts once at deploy time.

**Large contexts load in pages.** With the tier on, a load whose delta exceeds `contextCache.maxEvents` (default 100 000) no longer throws `ContextTooLargeError`: it folds the delta a page of `maxEvents` settled events at a time and writes the state after each page (the same write rule), so memory stays bounded by one page and the next load starts from the last page. `maxEvents` then bounds a page, not the context. The answer, and its context version for `appendIf`, are the same as an unpaged fold's. Without the tier (a fold without `defineFold`, `persist: false`, a store without `persistedFolds`) a context over `maxEvents` still throws.

**Old versions' rows.** They stay until you delete them. Once no process runs the old code any more, the operator deletes them (`$1` the tenant, `''` outside a tenant view; `$2` the fold's name; `$3` the version in use — `DELETE` is the operator's grant, and under `rls` set `app.current_tenant` first, as in "Erasing a tenant"):

```sql
DELETE FROM events_fold_state WHERE tenant = $1 AND fold_name = $2 AND fold_version < $3;
```

A process with old code that loads afterwards folds from zero and writes a correct old-version row again; the next run of the recipe removes it.

**A forgotten version bump.** Each stored state carries a fingerprint of the fold: a hash of its handlers' own source text (through `defineFold`, `$fold` and `$foldBy` to the handlers you wrote) and of the JSON schema and upcaster of every event type the query reads. When a load starts from a state that a different fold wrote under the same version, the process warns once — `process.emitWarning` with type `EventStoreWarning` and code `ES_FOLD_CHANGED`, one line on stderr; route it with `process.on("warning", …)`, silence it with `node --no-warnings`. It only warns: the stored state is used as always, and the version stays the only signal. It cannot see a change in an imported helper or a captured constant, and a different build of the same code (another bundler, a minifier) may warn once without a change. A `ContextCache` you construct yourself knows no event schemas, so it writes no fingerprint and compares none: a state it wrote is not checked until a runtime writes the row again.

**What is stored.** States are encoded with `node:v8` (a Node built-in, no dependency): plain objects, arrays, `Map`, `Set`, `Date`, `bigint`, `undefined` and every other primitive but symbols survive — JSON would turn a `$foldBy` Map into `{}`. A state holding anything else (a class instance, a function, an `Object.create(null)` object) would come back different, so it is not stored: the load stays correct from memory and the store, and `onLoad` reports `stored.error` naming the path (`state.items[3] is a class instance (Money)`). The same holds for a property the encoding would drop or change: an own property on a `Map`, `Set` or `Date` (`Object.assign(new Map(), { total })`), a non-enumerable property, a getter or setter. A state the encoder cannot take at all (nested thousands of levels deep) is reported the same way, never thrown. Bytes that cannot be read back (corrupt, or written by a newer Node) are a miss: folded from zero, reported, replaced by the next write.

**The initial state is part of the key**, so it must mean the same in every process: a plain value or a factory (`initial: () => new Map()`). A shared instance (`initial: new Map()`) is a different object in every process, and with a defined fold on a store with `persistedFolds` it throws a `UsageError` that says so.

**When a state is written.** Once at least `contextCache.persist.after` settled events (default 500) were folded into the in-memory state since it was last read from or written to the store — never for a load that folded nothing new. The write is awaited inside the load, is an upsert that only ever moves a row to a further cursor (two processes writing the same state need no lock), and never fails a load: a failed write is reported in `onLoad` as `stored.error`. Reading the stored state is different: an error there (a missing table, a missing grant) fails the load, so a deployment never runs without the tier unnoticed.

**On Postgres** the states live in `<table>_fold_state` (`events_fold_state` by default) in the same schema, created by the install with `persistedFolds: true` or printed by `printSchemaSql(schema, { persistedFolds: true })`. One row per tenant, fold name, fold version and state key; the application role needs `SELECT, INSERT, UPDATE` on it. Under `rls` the table gets its own policy on the same session variable as the events, and a stored state is reached only through a tenant view — the root store refuses, and so does the empty tenant id: one tenant never reads another's state. Whoever deletes events must delete the matching fold states too (see "Erasing a tenant"); a stored state still holds the facts it folded.

**What it saves.** A cold load (a fresh process) of one context, median of 11 processes, local Postgres 18 on a loaded machine (`sh bench/persisted-fold.sh matrix`, stage 7; all cells in `bench/RESULTS.md`):

| events × payload | fold on read | persisted fold, nothing new | persisted fold, 100 new |
|---|---|---|---|
| 10 000 × 1 KB | 77 ms | 5.9 ms | 8.3 ms |
| 100 000 × 1 KB | 766 ms | 6.0 ms | 9.3 ms |
| 10 000 × 200 KB | 10.6 s | 4.8 ms | 120 ms |

Warm loads are unchanged (~1 ms, nothing folded). A small context gains nothing: at 100 events of 1 KB a load from the stored state took as long as folding (1.00×), and with 10 new events longer (0.85×). What it costs: the write (~2 ms for a small state, ~10 ms for 570 KB), and on every cold load a read of the stored state, which grows with its size (~0.8 ms small, ~8 ms at 570 KB, ~78 ms at 5.7 MB plus ~5 ms to size it — more than folding 10 000 small events; "What the tier itself costs" in `bench/RESULTS.md`). A context below `persist.after` events is never stored and pays the ~0.7 ms read of a miss. Keep stored states small against the events they fold.

## Live read models

*Kind 3 of "Kept state: four kinds": the live read model.*

A context answers one query for one decision. A screen that shows a hundred offers, or one offer to a hundred browser tabs, would fold the same events again and again. A **live read model** keeps one fold **per key** in memory, current through one feed in the process, so a read folds nothing:

```ts
import { defineReadModel, es } from "@orgops/eventstore";

type Offer = { status: "draft" | "open" | "accepted"; amount: number };

// once, at module level: a name, a version, the events, the key, the fold
export const offers = defineReadModel({
  name: "offers",
  version: 1,
  source: sales.$filter(),
  keyBy: "offerId", // a scope key: one state per offerId
  initial: (): Offer => ({ status: "draft", amount: 0 }),
  fold: sales.$fold<Offer>({
    OfferMade: (d, s) => ({ ...s, status: "open", amount: d.amount }),
    OfferAccepted: (_, s) => ({ ...s, status: "accepted" }),
  }),
});

const model = es.forTenant(t).readModel(offers);  // one per tenant, name and version in this process
const offer = await model.get(offerId);           // from memory: 0 events folded
const page = await model.list({ where: { status: "open" }, sort: { by: "amount", dir: "desc" }, limit: 20 });
```

**A cache of your fold, never a second source of truth.** For every key, `model.get(k)` is what fold on read gives for the same events: `es.context({ query: source ∧ offerId = k, fold, initial }).state` (the test suite checks it on the memory store and on Postgres, with concurrent writers). The model hands out no context: **never decide on it** — a command reads `es.context` / `es.command`, and `appendIf` checks the store, as before.

**How it is kept.** `readModel(def)` returns at once and starts one feed: a `subscribe` through the view, its cursor in memory only. It replays the source from the beginning (the rebuild), then follows new commits — at once with the doorbell (`postgres: { live: true }`, or the memory store), within the poll interval (500 ms) without it. Nothing is written to the database: no table, no row. After a restart the model rebuilds from the store; the first read waits for that. An event's key is its scope value for `keyBy` (as in queries: `scopes[keyBy]`, the event's own id for its id key, or a flat data field); an event without one is not folded. The source must not pin `keyBy` itself. The same definition object always gives the same model; another definition object with the same name and version is a `UsageError` (bump the version). Another version is another model, rebuilt from zero — or started from the persisted model of that version (below).

**The fold.** Pure, never mutating its input state, and batch-associative — folding `a` then `b` equals folding `a ++ b` — the same contract as for contexts. The feed folds a page of events at once, and commits all the keys it touched or none.

**`get(key, { atLeast, timeoutMs })`** returns the key's state, or `initial` for a key without an event. It is the model's own object: treat it as read-only. Without options it is as fresh as the feed. To read your own write, pass its token, and the read waits until the feed has passed it:

```ts
const appended = await es.forTenant(t).append([sales.OfferAccepted({}, { offerId })]);
const accepted = await model.get(offerId, { atLeast: { transactionId: appended.transactionId, sequence: appended.last } });
```

The wait — the first rebuild included — is bounded by `timeoutMs` (default 10 000); beyond it the read throws `TransientError` with code `read_model_timeout` (HTTP 503, retry). Events of a transaction still in flight are not in the model until it ends. The first read waits for the rebuild: over a large source (measured: 10 000 events of 200 KB took ~8–10 s, and most first reads with the default timed out — `bench/RESULTS.md`, stages 3 and 4; with `timeoutMs: 600_000` every first read answered, after 9.0–35.4 s on a loaded machine, stage 7) pass a `timeoutMs` above the default, or start from a persisted state (below).

**`list({ where, sort, offset, limit, atLeast, timeoutMs })`** pages over the keys that have at least one event. `where` is equality on top-level fields of the state, all entries together (AND); values are strings, finite numbers, booleans or `null` (which matches only `null`). `sort` is one field: numbers before strings before booleans before `Date`s, strings by Unicode code point (not locale; what Postgres' `COLLATE "C"` gives, so a persisted model orders the same), a missing field, `null`, `NaN` or `±Infinity` last in both directions, ties by key ascending (by code point too) — so pages never shuffle. `offset` (default 0) and `limit` (default 50) page; `total` counts the matches. A result is kept per `where` and `sort` until the model changes, so repeated pages cost a slice. That is the whole query surface: no joins, no ranges, no query language.

**Typed definitions and reads.** With a `$filter(...)` source, `keyBy` takes only a scope key that one of the chosen event types declares (`scopes` or `optionalScopes`), or a chosen type's own id key — your editor offers them, and a typo is a compile error instead of an empty model. `list`'s and `watchList`'s `where` takes the state's fields that it can compare (strings, numbers, booleans, `null`), each with the field's own type — a literal union stays one, and `Date`, array and object fields are not offered; `sort.by` takes any field of the state. `get`, `watch` and `watchList` hand out the state's type. These are types only; what runs is unchanged.

```ts
// @ts-expect-error a typo: keyBy is one of offerId, offerMadeId, offerAcceptedId
defineReadModel({ name: "offers-typo", version: 1, source: sales.$filter(), keyBy: "offerID", initial: offers.initial, fold: offers.fold });
const accepted = await model.list({ where: { status: "accepted" }, sort: { by: "amount" } });
// @ts-expect-error "closed" is not a status of Offer
await model.list({ where: { status: "closed" } });
```

A definition object built in a variable before `defineReadModel` needs `keyBy: "offerId" as const`, and a `where` object in a variable `as const`, or they widen to `string`. A value from a request is a `string`: narrow it to the field's type first (a check or a schema parse). A source that is not a `$filter(...)` — a `Filter` literal, `scope()`, `$scope()`, a `Query` variable, an array holding any of them — keeps `keyBy` a plain string, and a state typed `any`, `unknown` or `Record<string, …>` keeps plain strings for `where` and `sort.by`. A typed handle is no longer assignable to `LiveReadModel<unknown>` (its `list` would then take any field): type a collection of models as `LiveReadModel<any>`.

**Push on change, for SSE.** `watch(key, onState)` delivers the key's current state first, then the new state after every folded page that changed it. `watchList({ where }, onChange)` delivers `{ revision, rows, removed }`: the keys that entered or changed and match `where` now, and those that matched before and no longer do. It is a change feed from the moment it is registered: no initial delivery, no replay of what came before — see "A list that stays current" below.

```ts
const watch = await model.watch(offerId, (offer) => sse.push(offer), {
  signal: request.signal,   // a closed request gives its place back
  onError: () => sse.close(), // it ended on its own: the callback threw, it fell behind, the key failed
});
```

**A list that stays current.** **Register the watch first**, holding what it delivers, then take the page with `list`, then apply only changes with `change.revision > page.revision`. The other way round, a change that commits between `list` and `watchList` is in neither and never arrives.

```ts
import type { ListChange } from "@orgops/eventstore";

async function streamOpenOffers(): Promise<void> {
  const where = { status: "open" } as const; // as const: "open", not string (where takes the field's own type)
  let pageRevision: number | undefined;
  const early: ListChange<Offer>[] = [];
  const feed = await model.watchList({ where }, (change) => {
    if (pageRevision === undefined) early.push(change); // before the page: hold it
    else if (change.revision > pageRevision) sse.push(change); // newer than the page only
  }, { signal: request.signal, onError: () => sse.close() });
  const first = await model.list({ where, sort: { by: "amount", dir: "desc" }, limit: 20 });
  if (feed.stopped) return; // it ended while the page was read (fell behind, the request closed): onError or the signal took care
  pageRevision = first.revision;
  sse.push(first);
  for (const change of early) if (change.revision > pageRevision) sse.push(change);
}
```

The frontend merges by key: a row replaces the key's state, `removed` drops the key. The feed does not keep a sorted, paged result: a changed row may now belong on another page, and a new key on a later one; re-read the page with `list` when its order or its bounds matter.

Every event is folded once, by the feed, however many watch; every watcher gets the same state object. Model watchers have the guarantees of `es.watch` and share its limits: each has its own queue and is dropped with `WatchOverflowError` after `watch.maxPendingBatches`, and they count toward `watch.maxWatchers`. A model on a tenant view folds only that tenant's events, under its session with `rls`, so a watcher never sees another tenant's state. An append in another process reaches the watcher through the doorbell (`live: true`).

**A fold that throws on one event.** That event is skipped (the stage-2 rule: the page is split until the one event the fold fails on alone is found) and every other key goes on. Its key becomes **failed**: `get` throws `FailedKeyError` (the model, the key, the event's type, id, sequence and transaction — never its payload — with the fold's error as `cause`), `list` leaves it out and counts it in `failed`, its watchers end with `onError`. Fold on read throws for that key too, so the model never answers what the fold cannot. `onFailedKey(error)` in the definition is called once per failed key: log or alert there. Fix the fold and bump the version; the new version rebuilds.

**`status`** says `phase` (`"starting"`, `"live"`, `"failed"`, `"stopped"`), `cursor`, `keys`, `failed`, `folded` (events folded since the start; a read adds nothing), `foldMs` and `revision`. A store error no retry fixes (a refused read, a missing grant) fails the model, and every read rejects with it; under `rls` the root view's model fails so — use `forTenant(t).readModel(def)`. On the root view without `rls` one model folds every tenant's events. A past view (`asOf`) has no read model. `model.stop()` ends the feed and its watchers; `es.close()` stops every model. **`invalidate()`** on a view (without a query) rebuilds that view's live models from the store **in place** — the same handle: reads issued meanwhile wait for the rebuild, `revision` keeps growing, and every watcher ends with `onError(TransientError)` code `read_model_rebuilt` (open it again); a failed or stopped model stays so. It is the step that follows an erasure (Operations, "Erasing a tenant"). `invalidate(query)` only forgets that query's cached contexts, and a tenant view dropped from the `maxTenantViews` memo keeps its models running.

**Starting from a persisted state.** Where a persisted read model of the same definition is kept (see "Persisted read models"), a live model can start from its rows instead of replaying the source:

```ts
const live = es.forTenant(t).readModel(offers, { start: "persisted" });
const fromRows = await live.get(offerId); // waits for the start: the rows, then only the events beyond their checkpoint
```

It reads the persisted model's checkpoint and every one of its state rows in **one snapshot** (one `REPEATABLE READ` transaction on Postgres, in pages of 5 000 rows — two separate reads would mix rows and a checkpoint of different moments), then follows with its own feed from that checkpoint, so `status.folded` counts only what came after it. Keys the persisted writer failed are failed here too (`FailedKeyError`; `onFailedKey` is not called again — it ran in the writer's process). Nothing to start from — no persisted rows, no checkpoint yet, a retired version, a row that does not decode — falls back to the ordinary rebuild. On a store without `persistedModels` the model fails with a `UsageError`; under `rls` use a tenant view, which loads only that tenant's rows. After the start it is an ordinary live model: RAM only, its own feed, it writes nothing and takes no writer lock. Every key equals what a rebuild gives, with one exception: a key the persisted writer failed because its state was not storable stays failed here. The options apply when the model is created; `readModel(def, …)` for a model already running returns it as it is.

**Cost.** Memory: one state per key, per tenant, for as long as the process runs — the library does not bound it; `status.keys` shows it. One feed per tenant and model: each commit wakes every feed (a cross-tenant feed is planned). A rebuild reads and folds the whole source, at `batchSize` events per page (default 500). Measured on a local Postgres 18 at 100 000 events of 1 KB (`sh bench/readmodel.sh all`, every cell in `bench/RESULTS.md`): an awaited `get` ~0.2 µs against ~0.9 ms for a warm `es.context()` of the same state; the rebuild to the first `get` ~1.1 s (pages of 500) or ~0.9 s (5 000), about what folding from zero costs; a `list` at a stable revision ~2 µs, the first `list` after a change a scan of every key (~5.6 ms at 100 000 keys); a watcher gets a change ~29 ms after the append with the doorbell, also from another process; ~120–180 bytes of heap per key for a small state.

## Persisted read models

*Kind 4 of "Kept state: four kinds": the persisted read model.*

A live read model lives in one process and rebuilds after every restart; every process that wants it folds the whole source again. A **persisted read model** keeps the same states in the store instead — one row per key, plus one checkpoint per model, written **in the same transaction** — so it survives restarts and deploys, resumes where it stopped and never applies an event twice. It is the same definition, opened by its own method:

```ts
// the store keeps persisted read models (memory: new MemoryStore({ schema, persistedModels: true }))
configure({ connection, events: [sales], tenant: { scopeKey: "tenantId" }, postgres: { persistedModels: true, live: true } });

const model = es.forTenant(t).persistedReadModel(offers); // the definition from "Live read models"
const offer = await model.get(offerId);                    // the stored row + the key's events beyond it: always current
const page = await model.list({ where: { status: "open" }, sort: { by: "amount", dir: "desc" }, limit: 20 });
const status = await model.status();                       // role, checkpoint, epoch, lag
```

It is on only when both hold: the store has `persistedModels: true`, and you call `persistedReadModel(def)`. Without the option the handle fails with a `UsageError` that says so, and nothing is created. `persistedModels` and `persistedFolds` are independent.

**A cache of your fold, never a second source of truth.** For every key, `get(k)` equals `es.context({ query: source ∧ offerId = k, fold, initial }).state` — or both throw (the test suite checks it on the memory store and on Postgres, with concurrent writers, also while the writer lags). The model hands out no context: **never decide on it**; commands read `es.context` / `es.command`, and `appendIf` checks the store, as before.

**`get(key)`** reads the key's row and folds the key's events beyond it from the store on top (Marten's `FetchLatest`): it is current the moment your `append` returned, in any process, whatever the writer's lag — so it takes no `atLeast`. The result is kept in the process as a copy that moves forward by what it folded (bounded like the context cache: `contextCache.max` keys, `contextCache.maxBytes`; `contextCache: false` keeps none), so a warm `get` with nothing new is one store query and folds **0** events, and a cold one folds only the key's events beyond the model's checkpoint — the row holds the key up to it, also the events that left its state unchanged; the two are read in one snapshot — never the key's history. A delta of more than `contextCache.maxEvents` events beyond the checkpoint (a version early in its build, a writer far behind) is folded **in pages** of `maxEvents` settled events, as the persisted fold tier does, so memory stays bounded by one page and the answer is the unpaged fold's; only more than `maxEvents` unsettled events still to fold (an earlier transaction in flight) throw `ContextTooLargeError`. Treat the result as read-only.

**`list({ where, sort, offset, limit, atLeast, timeoutMs })`** reads the stored rows, so it is **as current as the checkpoint**. The query surface is the live model's — equality filters, one sort, paging, nothing more — with the same order. Filters and sort work on the state's JSON-representable top-level fields: strings, finite numbers, booleans, `null` and `Date`s; any other field value (an array, an object, `Infinity`, a string with a NUL or a lone surrogate) counts as missing. `1` and `"1"` stay apart. The states it returns are the exact stored ones. `total`, `failed`, `revision` and the page come from one snapshot. To read your own write, pass its token; the call waits until the model has passed it (decided from the store, in any process — not only the writer's), at most `timeoutMs`, then throws `TransientError` `read_model_timeout`:

```ts
const appended = await es.forTenant(t).append([sales.OfferAccepted({}, { offerId })]);
const fresh = await model.list({ where: { status: "accepted" }, atLeast: { transactionId: appended.transactionId, sequence: appended.last } });
```

The library creates a GIN index on the doc for `where`. No index serves the sort: the order is the live model's (by value kind, then value, then key), computed over the doc with the field as a parameter, so a sorted `list` sorts every row that matches `where` on every call, and an expression index of your own such as `(doc->'amount')` leaves the plan unchanged (checked with `EXPLAIN`). Measured at 100 000 rows, 25 000 of them matching: a warm sorted page takes ~28 ms against ~5.5 ms for fold on read plus the same filter and sort in JavaScript — "not reached" in `bench/RESULTS.md`. Narrow a large `list` with `where`; where a sorted list must be fast, a live read model keeps it in memory.

**`watch(key)` and `watchList({ where })`** are the live model's (first delivery, one delivery per changed page, limits, `signal`, `onError`, the handoff "register first, then `list`, then apply newer revisions"), fed by the writer's committed rows: every process that opened the handle gets the change, folded once by the writer and decoded once per process. With `postgres: { live: true }` a change arrives ~a doorbell after the commit, also in other processes; without it, within 500 ms. A watcher sees the model as of its checkpoint (a `get` may be ahead by the writer's lag), and deliveries never go backwards. One difference: `removed` may name a key that did not match before when that key changed twice within one notification — a frontend that drops an absent key does nothing.

**One writer per model and version.** Every process with a handle competes for the model's writer lock (a session advisory lock, keyed by the events table, tenant, name and version — stores of other tables in one database never compete); the winner claims the next **epoch** and runs the model's processor, every other process is a **standby** that reads and takes over within about a second when the writer stops or dies. Every page transaction locks the model row and checks the epoch: a writer that lost its lock without noticing (a network partition, a paused process) is fenced at its next page and writes nothing. The locks of one store live on **one extra connection** per process (with TCP keepalives, so the server frees a vanished host's locks in ~25 s); it cannot sit behind PgBouncer in transaction mode. At most 1 000 writer locks per store and process — beyond that a handle stays standby. Every held lock uses a slot of the server's shared lock table (`max_locks_per_transaction × (max_connections + max_prepared_transactions)`, shared with every append's locks), and one model per tenant means one lock per tenant: size it, or keep the number of tenant models in one process bounded.

**Processor, poison and storable states.** The writer reads the source from the stored checkpoint in pages (`batchSize`, default 500) and applies each in one transaction that reads its keys' rows, folds, writes the changed rows and moves the checkpoint — all of it or nothing. A rebuild (a new model, a version bump) is this processor from the start of the source, and at the default `batchSize` it slows down over a long one: 1 KB events over 1 000 keys took 3.4 s for 10⁵ events but 76.0 s for 10⁶ with `batchSize` 500, against 1.1 s and 10.6 s with `batchSize` 5 000 (`sh bench/persisted-model.sh rebuild 1000000 3`, `bench/RESULTS.md` S3; the cause is not measured) — give a model over a large source a larger `batchSize` (it is not part of the fingerprint, so changing it needs no version bump). A fold that throws on one event fails that key, as in the live model: the event is found by splitting the page, the key's row gets a **failed marker** (the event's type, id, sequence, transaction and the error message, cut to at most 1 000 UTF-16 units without splitting a character, NUL and lone surrogates as U+FFFD — never the payload; it is the dead letter), its neighbours are applied, `get` throws `FailedKeyError` in every process, `list` counts it in `failed`, and `onFailedKey` runs once, in the writer's process. A database error never skips an event: the page is retried. A persisted model's fold must return **storable** states (what `node:v8` round-trips: plain objects, arrays, `Map`, `Set`, `Date`, primitives — see "Persisted folds"); a result that is not fails its key the same way, with the refusal as the cause.

**`status()`** reads the model from the store: `role` of this process, `checkpoint`, `epoch`, `revision`, `failed`, and `lag` — `events` (the source events beyond the checkpoint: counted up to 1 000; beyond, an upper bound — one per sequence above the checkpoint's up to the newest, plus the events of younger transactions that took lower sequences — never fewer than are pending) and `ms` (the age of the oldest of them, by this process's clock against the stored time, so clock skew shows) — plus this process's `folded`, `foldMs` and `readFolded` (events `get` folded; it grows by 0 on a warm read). `stop()` releases the writer lock and ends the handle's watchers; the rows stay. `es.close()` stops every handle. `invalidate()` on the handle's view drops its `get` copies, so the next `get` reads the row again; the handle keeps running. A past view (`asOf`) refuses; under `rls` use `forTenant(t).persistedReadModel(def)` — the root view's handle fails (the processor of a cross-tenant model aside, below).

**On Postgres** the model rows live in `<table>_read_model` (checkpoint, epoch, revision) and the states in `<table>_read_model_state` (`events_read_model` and `events_read_model_state` by default), created by the install with `persistedModels: true` or printed by `printSchemaSql(schema, { persistedModels: true })`. The application role needs `SELECT, INSERT, UPDATE` on both. Under `rls` both get a policy on the tenant's session variable, like the fold states. A new version is another model with its own rows — see the next paragraphs. A database installed by an earlier unreleased build of this branch lacks the `status` and `fingerprint` columns: reinstall it.

**A new version: blue-green.** Bump the version and deploy. The new version builds **beside** the old one, into its own rows, from the beginning of the source; the old version keeps answering in every process whose code knows it, and its writer keeps it current. While it builds, the new version is `building` (`(await model.status()).building`):

- its `get(key)` answers at once and is current (the stored row plus the key's events beyond it — correct from the first moment), for every key: a key with more than `contextCache.maxEvents` events beyond the new version's checkpoint — early in a build, the key's whole history — is folded in pages of `maxEvents` (memory bounded by one page), so a bump never makes a key unreadable and never throws `ContextTooLargeError`; until the build has passed the key, such a read costs a fold of the key's history;
- its `list` waits until it has caught up, at most `timeoutMs` (then `TransientError` `read_model_timeout`, naming the build), so a page is never served from half-built rows;
- its `watch` and `watchList` registrations wait the same way (no timeout; `signal` ends the wait), so no build page reaches an SSE client.

It **switches** when its own writer has passed the newest event of its source as of the moment it began building: the status turns `active` in one transaction that rings every process. Nothing happens to the old version at the switch — each process reads the version its code knows, so in a rolling deploy old processes answer from the old rows and new ones from the new rows. The first version of a name, or a version with nothing active beside it, is `active` from the start. To serve `list` from the old version while the new one builds, keep the old definition one deploy longer and read `(await v2.status()).building ? v1 : v2`. While both build, both writers run: the old version's reads slow down for the length of the rebuild.

**Retiring an old version.** The old rows stay until the operator retires them — after the new version has switched and every process running the old code is gone. Run per tenant (`$1` the tenant, `''` outside a tenant view; `$2` the name; `$3` the new version), as a role with `UPDATE` on `events_read_model` and `DELETE` on `events_read_model_state` (the owner, or an operator role — the application role has no `DELETE`); under `rls` set `app.current_tenant` first inside the transaction (`SELECT set_config('app.current_tenant', $1, true)`), or run it as a role that bypasses RLS, as in "Erasing a tenant":

```sql
BEGIN;
-- only versions below one that has switched (status 'active') in this tenant
UPDATE events_read_model SET status = 'retired', updated_at = now()
 WHERE tenant = $1 AND name = $2 AND version < $3 AND status <> 'retired'
   AND EXISTS (SELECT 1 FROM events_read_model
                WHERE tenant = $1 AND name = $2 AND version = $3 AND status = 'active');
DELETE FROM events_read_model_state s USING events_read_model m
 WHERE s.tenant = m.tenant AND s.name = m.name AND s.version = m.version
   AND m.tenant = $1 AND m.name = $2 AND m.status = 'retired';
COMMIT;
```

Before the new version has switched it changes nothing. The model row of a retired version stays as a **tombstone**: no process can bring the version back, and a straggler still running the old code fails loudly with a `UsageError` ("… was retired …") at its writer's next page, its standby's next claim, its next cold `get`, `list` or `status()` — it never rebuilds the old version and never serves an empty `list`. A live model with `start: "persisted"` on it rebuilds instead. A rollback after retiring is a new version (bump again); an operator who wants the number back deletes the tombstone row by hand.

**A forgotten version bump.** The writer stores a fingerprint of the definition with its claim: a hash of the fold's handlers' own source text, the source, `keyBy`, the initial state, and the JSON schema and upcaster of every event type the source reads. A process whose definition has another fingerprint under the same version — at its claim, at its first read of the model row, or a live model starting from the rows — warns once: `process.emitWarning` with type `EventStoreWarning` and code `ES_FOLD_CHANGED` (route it with `process.on("warning", …)`, silence it with `--no-warnings`). It only warns, it never rebuilds: bump the version. It cannot see imported helpers or captured constants, and a different build of the same code may warn once.

**One processor for every tenant.** One writer per tenant means one subscription, one writer lock and one page transaction per tenant and model, and every commit wakes all of them. A definition that says `processor: "cross-tenant"` has **one** writer for every tenant instead:

```ts
export const offersAll = defineReadModel({ ...offers, processor: "cross-tenant" }); // offers: the definition from "Live read models"

const processor = es.persistedReadModel(offersAll);                     // the root view's handle IS the processor
const offer = await es.forTenant(t).persistedReadModel(offersAll).get(offerId); // every tenant handle only reads
```

The root view's handle elects like any writer (the processor row's lock and epoch; standbys in other processes take over), runs **one** subscription over the whole source and writes, in one transaction per page, every tenant's state rows and every tenant's model row — exactly the rows a per-tenant writer would leave — plus one **processor row** (tenant `''`: its epoch and its checkpoint over every tenant; no fact of any tenant). An event belongs to the tenant its tenant scope key names (an event without one belongs to no tenant view and is passed over); a tenant's checkpoint moves when a page holds that tenant's events. Measured in the test (`tenants-postgres.test.ts`): one append to one tenant costs the processor the same 18 statements with readers open in 1 and in 10 tenants, and the readers none. What it costs, measured (`sh bench/tenants.sh rebuild` and `sh bench/tenants.sh page`, numbers in `bench/RESULTS.md`, "Stage 6"): **a rebuild is one feed instead of one per tenant** — 100 000 events × 1 KB over 100 tenants took the processor 5.38 s / 4.16 s against 0.96 s / 1.03 s for 100 per-tenant writers (fresh / analyzed statistics), **4.0–5.6× as long**; and **a page reads its tenants' rows with a plan that depends on the statistics of `events_read_model_state`**: while they do not know the model's rows — a first build, and every new version, until autovacuum analyzes the table — that read took 211.5 ms of a 247 ms page with 1 000 tenants per page (the same statement after `ANALYZE`: 2.2 ms for 1 000 pairs, lab). With many tenants, run `ANALYZE events_read_model_state` (as the owner) once a build has written its first pages, or expect slow pages until autovacuum does. Every `es.forTenant(t).persistedReadModel(def)` is a **reader**: it never takes the writer lock and never subscribes (`role` is always `"standby"`), and its `get`, `list` (`atLeast` included), `watch`, `watchList` and `status()` are the per-tenant ones, on its tenant's rows — the processor notifies each tenant it wrote. The processor handle's `get`, `list`, `watch` and `watchList` reject with a `UsageError` (a key without a tenant means nothing); its `status()` reports the processor row, `failed` counted over every tenant and `lag` over the whole source. It needs `configure({ tenant })`. Without `rls` the application's own root view is the processor. Under `rls` the processor runs on an api **connected as the processor role** (`postgres: { …, processorRole }`, the same options in every process, `install: "none"` there — see "Roles for RLS"); on the application's api its root handle fails with a `UsageError` naming `processorRole`.

A per-tenant writer of the same name and version — old code during a deploy — stays correct beside the processor: both lock the tenant's model row and apply only the events beyond its checkpoint, so each event lands once per tenant (the two definitions' fingerprints differ, so `ES_FOLD_CHANGED` names the disagreement). A fold that throws on one event fails that key in its tenant only; `onFailedKey` runs once, in the processor's process, and `FailedKeyError.tenant` says which tenant. **Blue-green** works across tenants: the new version's processor row is `building` while any version of the name is active in any tenant (so a move from a per-tenant v1 to a cross-tenant v2 builds beside v1 everywhere), and at the switch every tenant that has any version of the name gets an active row of the new version in one transaction — also a tenant without an event in the new source, whose `list` then answers an empty page. **Retiring a cross-tenant version** is the recipe above without the tenant conditions (`$1` the name, `$2` the new version), run as the processor role with `DELETE` on `events_read_model_state` granted for it (it sees every tenant's rows and the processor row; under `FORCE` the owner sees none without a tenant):

```sql
BEGIN;
UPDATE events_read_model SET status = 'retired', updated_at = now()
 WHERE name = $1 AND version < $2 AND status <> 'retired'
   AND EXISTS (SELECT 1 FROM events_read_model
                WHERE tenant = '' AND name = $1 AND version = $2 AND status = 'active');
DELETE FROM events_read_model_state s USING events_read_model m
 WHERE s.tenant = m.tenant AND s.name = m.name AND s.version = m.version
   AND m.name = $1 AND m.status = 'retired';
COMMIT;
```

The old version's processor fails at its next page, its readers at their next cold read, as with a per-tenant version. An erasure needs nothing from a running processor: its pages and its version switch are fenced (Operations, "Erasing a tenant").

The four kinds side by side: the table in "Kept state: four kinds".

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

## Subscriptions and live pushes

```ts
// durable: named cursor, gap-free, at-least-once — a projection, a search index, an outbox
const sub = await es.forTenant(t).subscribe("search-index", articles.$filter(), async (events) => { … }, {
  cursors: fileCursors("./data/cursors.json"),   // default: memoryCursors() (replays from `from` after a restart)
  onError: (error, batch) => "retry",            // or "skip" (the one failing event) | "stop"; batch is empty for store errors
});
await sub.whenCaughtUp();                        // resolves on the next empty page
const appended = await es.forTenant(t).append([articles.ArticleDrafted({ … })]);
sub.hasPassed({ transactionId: appended.transactionId, sequence: appended.last }); // handled or skipped yet?
await sub.stop();

// ephemeral: from about now, at-most-once — an SSE stream to one browser tab
const watch = await es.forTenant(t).watch(articles.$filter(), (events) => sse.push(events), {
  data: false,                                   // a relay forwards type, id, scopes: the payload never leaves the database
  signal: request.signal,                        // a closed request gives its place back, even before the watch started
  onError: () => sse.close(),                    // it ended on its own: handler threw, fell behind, or the read was refused
});
watch.stop();
```

Both read through the view they are called on: a tenant view delivers that tenant's events only, under its session when `rls` is on (the root view under `rls` cannot watch — the read is refused). `subscribe` reads settled events beyond a `(transactionId, sequence)` cursor, advances it only after your handler resolved, retries with back-off, and on a tenant view keeps its cursor as `name@tenantId`. `watch` shares **one reader per view and query** in the process: a hundred tabs on the same tenant cost one read per commit, not a hundred. Both read settled records only, each exactly once: a record still waiting on an older transaction is not fetched until it can be delivered (a one-row read without payload tells the reader it exists, and it looks again within 25–500 ms). Pass `data: false` or `payload: false` to either for a relay that forwards only type, ids and scopes — the rule for any live stream of large events. Every watcher has its own queue; one that falls `watch.maxPendingBatches` behind (default 64) is dropped with `WatchOverflowError` instead of holding the others back or filling memory, and at most `watch.maxWatchers` (default 10 000) live at once: `configure({ watch: { maxWatchers, maxPendingBatches } })`. A store error that no retry will fix (a refused read, missing permissions) ends the watch; a dropped connection is retried. `es.close()` ends every watch. To push folded state instead of events — the new state of one offer to every tab that shows it, folded once — watch a live read model (`model.watch(key)`, `model.watchList({ where })`, see "Live read models").

**Read your writes.** The token of an append is `{ transactionId: appended.transactionId, sequence: appended.last }`, a plain `Cursor`; pass it wherever the reader is (a return value, a response header). `sub.hasPassed(token)` is `true` once the subscription has handled or skipped it: by its cursor, or — for a write its query does not match, which never moves the cursor — by `sub.watermark`, the snapshot horizon of its last empty page (every matching event of a transaction below it had been delivered). It reads no store, so it is as fresh as the last poll (about the doorbell's 25 ms with `live`, up to 500 ms without; measured on an idle database: 27 ms and at most 495 ms, `bench/RESULTS.md`); a token is passed only once every older transaction has ended. On a store without a watermark it answers by the cursor alone, so possibly never for a write it does not match.

**One poison event.** `onError` returning `"skip"` skips only the event the handler fails on: the failed batch is split in halves, left to right, each half that succeeds is committed, and `onError` is asked once more with just the event that fails alone (answer `"retry"` there if the error was transient). Its neighbours are applied, at about 2·log₂(batch) extra handler calls (measured on a page of 500: 17–19 calls; with a Postgres transaction per call and the locking target below, medians of 26–39 ms instead of 2–3 ms for a clean page, `bench/RESULTS.md`). Neighbours of a failed batch are delivered again — fine for a handler that rolled back, and what at-least-once already allows.

### Processor mode: the checkpoint in your transaction (`target`)

A durable projection writes its rows and its cursor in one transaction, so a crash between the two can never apply a batch twice. Pass a `target` instead of `cursors`: the subscription loads the checkpoint from it at start, saves nothing after the handler, and hands a skipped poison event to `target.skip`. In one transaction your handler **locks its checkpoint row, applies only the events beyond it, and moves it** to `cursorOf(events)`:

```ts
import { compareCursor, cursorOf, type Cursor } from "@orgops/eventstore";

// CREATE TABLE checkpoints (name text PRIMARY KEY, xid xid8, seq bigint);  -- xid/seq NULL: nothing applied yet
async function lockCheckpoint(tx, name: string): Promise<Cursor | null> {
  await tx.query("INSERT INTO checkpoints (name) VALUES ($1) ON CONFLICT (name) DO NOTHING", [name]);
  const { rows: [c] } = await tx.query(
    `SELECT xid::text AS "transactionId", seq FROM checkpoints WHERE name = $1 FOR UPDATE`, [name]); // a second writer waits here
  return c.transactionId === null ? null : { transactionId: c.transactionId, sequence: Number(c.seq) };
}
const beyond = (events, checkpoint: Cursor | null) => events.filter((e) => checkpoint === null || compareCursor(e, checkpoint) > 0);
const moveCheckpoint = (tx, name: string, c: Cursor) =>
  tx.query("UPDATE checkpoints SET xid = $2::xid8, seq = $3 WHERE name = $1", [name, c.transactionId, c.sequence]);

const sub = await es.subscribe("search-index", articles.$filter(), async (events) => {
  await db.transaction(async (tx) => {
    const fresh = beyond(events, await lockCheckpoint(tx, "search-index"));
    if (fresh.length === 0) return;                                        // all applied already: a redelivery
    await writeRows(tx, fresh);                                            // never the events at or below the checkpoint
    await moveCheckpoint(tx, "search-index", cursorOf(events)!);
  });
}, {
  target: {
    load: async () => readCheckpoint("search-index"),                     // Cursor | null; null starts at `from`
    skip: async (event, error) => db.transaction(async (tx) => {           // the checkpoint at the event, and a dead letter:
      if (beyond([event], await lockCheckpoint(tx, "search-index")).length === 0) return; // skipped already
      await moveCheckpoint(tx, "search-index", cursorOf([event])!);       //   cursor, type, id, error — never the payload
      await tx.query("INSERT INTO dead_letters …", [event.transactionId, event.sequence, event.type, event.id, String(error)]);
    }),
  },
  onError: () => "skip",
});
```

Exactly-once is the target's, not the subscription's: `subscribe` stays at-least-once, and it redelivers from the cursor it knows. **A redelivered batch can be longer than the one already committed** — a COMMIT whose acknowledgement was lost, with new events before the retry, or two subscribers on one target — so it can start below the checkpoint and end beyond it. Filtering by the checkpoint under its row lock makes every redelivery apply only what is new; a guard that only compares the batch's end with the checkpoint lets such a batch through and applies its first part twice. The lock is `FOR UPDATE` under Postgres's default `READ COMMITTED`; under `REPEATABLE READ` or `SERIALIZABLE` a concurrent writer gets a serialization error instead, a transient error your `onError` should answer with `"retry"`. `sub.cursor` is the last checkpoint the target committed, as far as the subscription knows. `cursors` and `target` together are refused. The target object belongs to one subscription; key its checkpoint row yourself (on a tenant view the subscription's name becomes `name@tenantId`, the target is not told).

A processor whose source is a small share of a large table catches up faster with `postgres: { typeIndex: true }`: over 1 % of 1 000 000 events it took 66 ms instead of 199 ms with the default indexes (3.0×; 2.2× at 100 000 events; `batchSize` 5 000), against 4.9 s for the whole table — the default is no trap, the type index is the cheaper walk (`sh bench/cursor.sh`, `bench/RESULTS.md` S4).

Subscriptions are not owned by `es`: stop them before `es.close()`. The low-level `subscribe(name, query, handler, { store })` from `@orgops/eventstore/subscribe` is what `es.subscribe` is built on and stays available; `on()` there is superseded by `es.watch` (it only ever worked with the memory store).

### The commit doorbell (`postgres: { live: true }`)

Without it, subscriptions on Postgres poll (500 ms). With it, the installer adds a statement trigger that runs `pg_notify` after every insert statement, and the store keeps **one extra connection** that `LISTEN`s — so every subscription and watch in every process wakes as soon as anything commits, and polls only as a safety net (5 s). The limits are built in, not options:

- **The notification is a doorbell, never the data.** Its payload is empty — no sequence, tenant, type, scope or payload. Whoever can `LISTEN` on the database learns *that* and *when* something was written to the table, not by whom or what. Delivery is always a cursor read through the subscriber's own view, so tenant narrowing and row-level security decide what anyone sees.
- **Only on commit.** `NOTIFY` is transactional: a rolled-back batch rings nothing. One notification per append batch.
- **Losing one costs latency, never events.** Notifications sent while the listener is disconnected are gone; the cursor read is gap-free regardless. The listener reconnects with back-off (100 ms → 5 s) and, every time `LISTEN` is up again, wakes everyone once to read what committed meanwhile.
- **Doorbells within 25 ms share one read** per reader (`wakeCoalesceMs`), and they never cut a retry back-off short: a failing handler is not retried on every commit elsewhere.
- **Events waiting on an older transaction are re-read soon** (25 ms, doubling to 500 ms), not after the safety-net poll: they become readable when that transaction ends, and its end rings nothing when it wrote to another table.
- **Without the trigger** (`live` with `install: "none"` and the DDL not run) nothing would ring: the store records a warning and wakes its subscribers every 500 ms — polling's pace, not the safety net's.

Operations: `LISTEN` does not survive PgBouncer in transaction mode — give it a direct connection with `postgres: { live: { connection } }`, which you also want when `connection` is a pool of your own (otherwise the doorbell takes one of its connections for good). Postgres serialises the commits of notifying transactions on one lock; that matters only at very high write rates (our raw ceiling was ~200 000 events/s without it) — leave `live` off for a bulk-loading process, the trigger rings for everyone's inserts anyway. The one real risk: Postgres keeps undelivered notifications in a queue (8 GB by default), and when a listener stays connected but stops reading, the queue fills and **commits that notify fail** — every append, not just the listener's process. The SDK's listener reads on Node's event loop and never opens a transaction, so this takes a process that is alive but stuck; watch `store.notificationQueueUsage()` (`pg_notification_queue_usage()`; it should stay near 0) and alert well below 1. With `install: "none"`, `printSchemaSql(schema, { live: true })` lists the trigger. The trigger needs Postgres 14 or later.

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
| `ContextTooLargeError` | 500 — a context exceeds `contextCache.maxEvents`; with the persisted fold tier on a context loads in pages instead, as a persisted read model's `get` delta always does, and it is thrown only for more than `maxEvents` unsettled events still to fold |
| `NotConfiguredError` | 500 — `es` used before `configure()` |
| `EventStoreError` | 500 — base class; unmapped store failure |

Every error the store raises extends `EventStoreError` and carries `httpStatus` and, where there is one, `cause`. Invalid declarations (`defineEvents` with a bad type name) throw plain `Error`s at module load.

## What it deliberately does not do

- No streams, no aggregates, no expected-stream-version: the context is a query.
- No second source of truth. Folds may be materialized; a materialized fold is a cache of your fold, never a second source of truth (see "Persisted folds", "Live read models" and "Persisted read models"). Indexes are not read models.
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

- **Install.** With `install: "auto"` (default) the first query or append creates the table, the `es_scope()` function, the append function and every index, idempotently and under a session advisory lock, so concurrent boots do not race. With `install: "none"` nothing is touched; print the DDL with `printSchemaSql(es.schema)` (with `{ persistedFolds: true }` for the fold-state table, `{ persistedModels: true }` for the persisted read models' two tables) and run it yourself.
- **Adding a scope key or a unique path** adds an index. On a large table prefer running the printed `CREATE INDEX` statement as `CONCURRENTLY` yourself before deploying the declaration.
- **`es_scope()`** is the expression every scope index, the tenant-keyed idempotency index and the RLS policy are built on. It is an inlinable `IMMUTABLE` SQL function, so the *inlined expression* is what the indexes store. The SDK fingerprints its body on the function and on every index built from it; when the installed body differs from what an index was built with, the installer refuses to continue and throws an error listing the exact statements (`DROP/CREATE INDEX CONCURRENTLY`, statistics, comments) — run them yourself, or pass `postgres: { rebuildScopeIndexes: true }` to rebuild inline during a maintenance window. With the inlined body a stale index is a performance problem (the planner stops matching it); with an opaque body it would be a correctness problem, which is why the gate exists.
- **`upcast`** may reshape an event's `data` on read; the own id and the `scopes` object always come from the stored payload, because that is what the store indexed, locked and matched on.
- **Roles for RLS.** The install runs as the table owner; the application connects as a non-owner role (`FORCE ROW LEVEL SECURITY` applies to everyone but a superuser). Tenant views bind `app.current_tenant` per transaction with `set_config(..., true)`, never session-wide, so pooled connections cannot leak a tenant. With `persistedFolds: true` the role also needs `SELECT, INSERT, UPDATE` on `<table>_fold_state`, whose policy is `tenant <> '' AND tenant = current_setting('app.current_tenant', true)` — the extra `tenant <> ''` keeps a pooled connection whose setting was reset to `''` from seeing anything. With `persistedModels: true` the same on `<table>_read_model` and `<table>_read_model_state` (same policy expression); the writer locks need no grant. Set a `lockSalt`: a persisted model's notifications carry its lock key, and without a salt that key is a plain hash of the events table's schema and name, the tenant id, the model's name and version — a role that can `LISTEN` and guesses them could tell when that model's writer committed (timing, never data). **The processor role** of a cross-tenant model (`processor: "cross-tenant"`): with `postgres: { rls: true, persistedModels: true, processorRole: "es_processor" }` the installer (and `printSchemaSql`) adds three policies **`TO` that role only** — `FOR SELECT USING (true)` on `<table>`, `USING (true) WITH CHECK (true)` on `<table>_read_model`, `USING (true) WITH CHECK (tenant <> '')` on `<table>_read_model_state` — and none on the fold states; the application role's policies are untouched. Create it `NOSUPERUSER NOBYPASSRLS LOGIN` and grant it `USAGE` on the schema, `SELECT` on `<table>`, `SELECT, INSERT, UPDATE` on both model tables — no `EXECUTE` on the append function: it never appends. It sees **every tenant's** events and model rows by design. **Never grant it to the application role**: a member of a role inherits that role's policies, so the application would read every tenant. Pass `processorRole` in every process; the store reads `current_user` once, and only a store connected as that role may read every tenant at its root and run the processor — any other connection is refused as before. A process that installs without the option leaves existing processor policies in place and names them in `warnings()`. (A `BYPASSRLS` role, which only a superuser can create and which bypasses every policy of the database, works too: name it in `processorRole`.)
- **Scope indexes under RLS need three `LEAKPROOF` marks.** Under row-level security Postgres uses a condition as an index condition only when every function in it is `LEAKPROOF`; `es_scope` inlines to `jsonb_typeof`, `->` and `->>`, which it does not ship as such, and no jsonb function or operator is (`@>` included). Without the marks a non-owner role uses the tenant's index only and filters every other scope over the whole tenant (measured: 40 ms instead of 0.1 ms for a four-key read over 27 000 events). The installer sets them when it runs as a superuser and records a warning in `warnings()` otherwise; `scopeLeakproofStatements()` and `printSchemaSql({ rls: true })` give the statements. They are per database and neither `pg_dump` nor `pg_upgrade` carries them — rerun after a restore. The three never raise an error on any input, so they reveal nothing but their result; the policy still decides what anyone sees.
- **Statistics objects cost planning time.** Postgres loads every statistics object of a table for every reference to it in every statement, named key or not: with 43 scope keys they were two thirds of the planning time of a four-key read (7 ms of 11 on the bench machine, execution 0.1 ms). `postgres: { scopeStatistics: { minIndexRows: DEFAULT_SCOPE_STATISTICS_MIN_ROWS } }` keeps objects only for keys whose index holds at least 1 000 rows (below that the planner never costs the index above its size, so a wrong estimate costs at most that index); a key that grows past the limit gets its object at the next install — `ANALYZE` afterwards. `exclude` names keys whose expression already has statistics from a non-partial expression index of your own. The default stays `"all"`.
- **Erasing a tenant** (a right-to-erasure request, a closed workspace) is not an SDK call: the store is append-only, and deletion should stay a deliberate, logged act of the operator. Every kept state of the tenant goes with its events — persisted folds, persisted model rows and checkpoints, and what processes hold in memory. The recipe (`events` stands for your table, `public` for its schema, `<tenantKey>` for the tenant scope key, `$1` for the tenant id):

  1. **Stop the tenant's traffic** — requests, jobs, anything that appends or loads its contexts — and wait for what is in flight. `stop()` every **per-tenant** persisted handle of the tenant (`es.forTenant(id).persistedReadModel(def)` of a definition without `processor`) in every process. A cross-tenant processor keeps running, and its tenant readers need nothing here. Take the count: the sum of `count` over `await es.forTenant(id).statistics()`.
  2. **One transaction, as the table owner** (or an operator role with `DELETE` on the four tables). The lock line comes first: it waits for a cross-tenant processor's page or version switch in flight, makes every later page re-read without the tenant and every later switch list the tenants without it (it costs nothing without one). Under `rls`, `FORCE` binds the owner too, so set the tenant before deleting. Leave out the `DELETE` of a table you do not have (`persistedFolds`, `persistedModels`):

     ```sql
     BEGIN;
     SELECT pg_advisory_xact_lock(hashtext('eventstore:erasure'), hashtext('public.events'));
     SELECT set_config('app.current_tenant', $1, true);
     DELETE FROM events WHERE es_scope(payload, '<tenantKey>') = $1;
     DELETE FROM events_fold_state WHERE tenant = $1;
     DELETE FROM events_read_model_state WHERE tenant = $1;
     DELETE FROM events_read_model WHERE tenant = $1;
     COMMIT;
     ```

     **Before `COMMIT`, compare the events `DELETE`'s row count with the count of step 1**; if it differs, `ROLLBACK` and look — 0 under `rls` means the `set_config` line is missing: the policy hides every row, and Postgres raises nothing. The tenant scope index makes the events `DELETE` a range delete.
  3. **In every process, or restart them: `es.forTenant(id).invalidate()`.** It forgets the view's cached contexts, rebuilds the view's live read models from the store in place (their watchers end with `TransientError` `read_model_rebuilt`: open them again) and drops the view's persisted handles' `get` copies. (`stop()` and `readModel(def)` again, or a restart, does the same.)
  4. **Without `rls`, the root view's kept states fold every tenant**, so also: `es.invalidate()` on the root view in every process (root live models rebuild); `stop()` every root persisted model of a per-tenant definition (`es.persistedReadModel(def)` without `processor`) in every process and delete its rows by name — `DELETE FROM events_read_model_state WHERE tenant = '' AND name = $2`, the same on `events_read_model` — it rebuilds when opened again; and after step 3 `DELETE FROM events_fold_state WHERE tenant = ''` (root persisted folds rebuild on their next load; a root load already in flight while it runs can store one state that still holds the tenant — run the `DELETE` once more after every process has done step 3). A cross-tenant model's processor row (`tenant = ''`, its name) holds no fact: leave it. Under `rls` none of these exist: the root keeps no state.
  5. Durable subscribers need nothing: cursors are `(transactionId, sequence)` positions, and gaps are ordinary.
  6. Idempotency keys and unique values of the tenant are free again — decide whether that is what you want.
  7. Record the proof as an event on the platform tenant (a `TenantErased` with the count of step 1) — that is the Löschnachweis, and it is the only row about the tenant that remains.

  Then nothing of the tenant remains: 0 rows in the events table and the three kept-state tables, and no process answers one of its facts from memory — also with a cross-tenant processor running through the erasure (each of its pages takes the lock shared and checks that its events still exist, so a page read before the erasure is read again, never written; its switch to a new version takes the lock shared too, so it lists the tenants after the erasure or before its `DELETE`s). A handle of the tenant opened afterwards is new traffic: its first claim writes a model row with no fact in it. The erasing role needs no policy beyond the tenant's and no grant for the lock.
- **Timeouts.** The SDK sets `lock_timeout` (10 s), `statement_timeout` (30 s) and `idle_in_transaction_session_timeout` (30 s) per connection, so a stuck lock holder degrades to `TransientError` instead of a hang; tune them with `postgres: { timeouts: { lockMs, statementMs, idleInTransactionMs } }`. If you pass your own `pg.Pool` as `connection`, set them yourself — the SDK does not touch a pool it did not create, and `close()` leaves it open.
- **Durability knobs.** Single-event appends are bound by commit durability: `synchronous_commit = off` (per session or transaction) gives +40–75 % on them and loses at most the last ~600 ms on a crash — acceptable for many event stores, never for money. Batches of 50 barely change (group commit already amortises fsync); at four writers the raw ceiling on our test box was ~200 000 events/s.
- **Adopting an existing events table.** Tell the store which columns are which, and the installer takes the table over in place at boot — no mapping that lives on, no copy:

  ```ts
  configure({ connection, events, postgres: { adopt: { columns: { sequence: "id", type: "eventtype" } } } });
  ```

  It renames the legacy columns (`id` → `sequence_number`, `eventtype` → `event_type`), adds `metadata` and `transaction_id` — every legacy row gets `transaction_id = 3`, so all of them sort before every new row — and then installs indexes and functions as always. No row is rewritten and `payload` is never touched (renames and the added columns are metadata-only; a `SET NOT NULL` on a nullable legacy column scans the table once, and a missing unique index on the sequence is built), all of it runs in the install transaction under the install lock (all or nothing, exactly one of several booting processes adopts), and the row count and highest sequence are compared before and after. The next boot finds the table adopted and does nothing — so it fits machines that migrate offline on start. The store refuses a table it cannot adopt and changes nothing: half adopted (`id` *and* `sequence_number`), `json` instead of `jsonb`, an `integer` sequence, NULLs where it needs values (a row without a sequence would be invisible to every cursor read), a `NOT NULL` column without a default that every append would violate, one column named for two roles, a `legacyTransactionId` that is not below every running transaction — and, under `rls`, a foreign permissive policy, which Postgres would OR with the tenant policy and so open every tenant's rows. Nothing is dropped: legacy indexes, extra columns, triggers, restrictive policies and grants stay, and `warnings()` lists them for you to review (the scope indexes often make a payload-wide GIN index redundant; an `UPDATE`/`DELETE` grant on an append-only table deserves a look). `adopt: { …, mode: "check" }` fails the install with the plan instead of running it; `store.adoptionPlan()` returns it read-only, and `printAdoptSql({ plan })` prints exactly that for `install: "none"` (`printAdoptSql({ adopt })` guesses the shape without looking). A missing table is created with a warning — set `requireExisting: true` on machines that must adopt, so a mistyped `table` or `schemaName` fails instead of starting on a fresh, empty table. Two things to plan for: the first boot counts the rows twice and builds the scope indexes (writes wait meanwhile, proportional to the table; other processes booting at the same time wait on the install lock — raise `installLockTimeoutMs` for a large table), and code still using the old column names stops working the moment the table is adopted. Legacy rows without an own id read back as `id: "~<sequence>"`; a durable subscriber starting `from: "beginning"` sees every legacy row.
- **Every writer through the SDK.** See the invariant under "The store contract".

## License

MIT
