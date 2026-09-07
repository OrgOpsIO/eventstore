# Friction log — writing `examples/articles` against the public API only

Ordered by how much it cost me. Nothing here required a cast in the example itself.

1. **A pre-built store does not inherit the registries you pass to `configure()`.**
   ```ts
   const store = new MemoryStore({ schema: buildSchema([workspace, articles], { tenantScopeKey }) }); // forgot `accounts`
   configure({ store, events: [workspace, accounts, articles], tenant: { scopeKey } });
   ```
   Result: `unique: ["email"]` was silently not enforced on the memory store (the Postgres run enforced it), because the store's schema is what installs uniques/indexes and `configure` cannot reach into a store it did not build. Expected: `configure()` to throw when `store.schema` (MemoryStore/PostgresStore expose it) disagrees with `events`/`tenant`, or a `createMemoryStore(config)` helper that derives the schema from the same config. Workaround: pass the same registries to `buildSchema`.

2. **`unique` cannot express "unique per tenant, released on archive".** Documented in the README; still, the slug rule ended up as a tenant-wide CCC guard (`articles.$filter({ types: [...] })` under `forTenant`), which serialises every draft in a workspace. Fine for this size; I expected a hint in the docs on how to narrow it (a `SlugClaimed`/`SlugReleased` event pair with `unique: ["scopes.slugClaimedId"]`?) — not obvious how to model "release" with an index.

3. **`command<S, R>` needs both generics spelled out when there is no `fold`.**
   ```ts
   es.forTenant(id).command<readonly unknown[], string>({ context, decide: () => ({ events: [e], result: e.id }) })
   ```
   Without them `S` infers as `unknown` and `result` as `void`. Expected: `S` to default to `readonly RecordedEvent[]` (the documented no-fold state) and `R` to infer from `result`. An overload `command<R>(spec: CommandSpec<readonly RecordedEvent[], R>)` would remove the noise.

4. **The workspace root event is stamped with its own id as a scope.** `es.forTenant(provisioned.id).command(...)` writes `WorkspaceProvisioned` with `scopes: { workspaceProvisionedId: <own id> }` in addition to the top-level own id. Harmless (`es_scope` reads the same value) but surprising on the wire; an earlier in-house store's convention says the root carries no `scopes`. Expected: the tenant stamp to be skipped when the event's own id key IS the tenant key and the ids are equal.

5. **`registerAccount` on the platform tenant needs a context to satisfy strict mode.** A create-only command has no facts to read, yet `command()` requires `context`. I used `accounts.$scope("accountRegisteredId", registered.id)` (always empty, version 0) which reads as a ritual. Expected: `context` optional for pure appends, or a documented idiom (`createOnly()`?).

6. **`$fold` handlers for optional scope keys / `e.scopes.articleDraftedId` are typed `string` only because the key is required on that event** — good — but `foldArticles` needed a `Map`-copy helper for every case; a `$reduceBy(keyOf, handlers)` keyed-fold helper would remove three near-identical handlers. Nice-to-have, not a blocker.

7. **Shutdown order is on the caller.** The scenario stops the subscription before `es.close()`; nothing documents what a still-running poller does against a closed store (I did not test it). Expected: `es.close()` to stop subscriptions created from `await es.store()`, or a README note that subscriptions must be stopped first.

8. **Running the example needs path aliases twice** (`tsconfig.json` `paths` for tsc, `vitest.config.ts` `resolve.alias` for vitest). Expected for a monorepo-less repo; a published package would not have this. Noting it because `examples/` now leaks into the root `tsconfig` `include`.

Not friction, worth keeping: `read(registry)` narrowing on `event.type`, `httpStatusOf`, `reject`/`rejectMissing`, `$parse` inside the subscription handler, `fileCursors`, the memoised `forTenant` — all did what I expected on the first try, on both stores.
