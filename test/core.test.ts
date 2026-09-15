import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
  ConflictError,
  ContextCache,
  MemoryStore,
  estimateSize,
  UnindexableContextError,
  UniqueViolationError,
  ValidationError,
  buildSchema,
  createEventStore,
  defineEvents,
  reject,
  type RecordedEventOf,
} from "../src/index.js";
import { GLOBAL_LOCK_KEY, globalLockKey, lockKeyOf, scopeLockKey } from "../src/internal.js";

const articles = defineEvents({
  ArticleDrafted: {
    data: z.object({ title: z.string().min(1), slug: z.string() }),
    scopes: ["workspaceProvisionedId"],
    unique: ["slug"],
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

type Article = { id: string; title: string; body: string; archived: boolean };

// incremental fold (delta, state) => state — what es.context()/es.command() take
const foldInto = articles.$fold<Map<string, Article>>({
  ArticleDrafted: (data, state, e) => new Map(state).set(e.id, { id: e.id, title: data.title, body: "", archived: false }),
  ArticleContentEdited: (data, state, e) => {
    const a = state.get(e.scopes.articleDraftedId); // typed: required scope → string
    return a ? new Map(state).set(a.id, { ...a, body: data.body }) : state;
  },
  ArticleArchived: (_data, state, e) => {
    const a = state.get(e.scopes.articleDraftedId);
    return a ? new Map(state).set(a.id, { ...a, archived: true }) : state;
  },
});
// one-shot fold over a complete list
const foldArticles = (events: readonly import("../src/index.js").RecordedEvent[]) => foldInto(events, new Map<string, Article>());

const WS = "ws-1";

function newApi() {
  return createEventStore({ events: [articles], tenant: { scopeKey: "workspaceProvisionedId" }, strict: true });
}

describe("registry", () => {
  it("creates typed, validated events with the <eventName>Id convention", () => {
    const e = articles.ArticleDrafted({ title: "Hello", slug: "hello" }, { workspaceProvisionedId: WS });
    expect(e.type).toBe("ArticleDrafted");
    expect(e.id).toMatch(/^[0-9a-f-]{36}$/); // creators generate the id
    // @ts-expect-error unknown scope key alongside the required one
    articles.ArticleDrafted({ title: "x", slug: "y" }, { workspaceProvisionedId: WS, bogusId: "z" });
    expect(e.scopes.workspaceProvisionedId).toBe(WS);
    expect(articles.$idKey("ArticleDrafted")).toBe("articleDraftedId");
    expect(() => articles.ArticleDrafted({ title: "", slug: "x" }, { workspaceProvisionedId: WS })).toThrow(ValidationError);
    // @ts-expect-error missing required scope
    expect(() => articles.ArticleContentEdited({ body: "b" })).toThrow(ValidationError);
    expect(articles.$scopeKeys).toEqual(["articleDraftedId", "workspaceProvisionedId"]);
  });

  it("builds a store schema with uniques and scope keys", () => {
    const schema = buildSchema([articles], { scopeKeys: ["thingId"] });
    expect(schema.scopeKeys).toEqual(["articleDraftedId", "thingId", "workspaceProvisionedId"]);
    expect(schema.uniques).toEqual([{ type: "ArticleDrafted", path: "slug" }]);
  });
});

describe("memory store + api", () => {
  it("append, query by scope, fold, appendIf", async () => {
    const es = newApi().forTenant(WS);
    const drafted = articles.ArticleDrafted({ title: "Hello", slug: "hello" }, { workspaceProvisionedId: WS });
    const r1 = await es.append([drafted]);
    expect(r1).toEqual({ first: 1, last: 1, count: 1 });
    const stored = (await es.query(articles.$filter())).events[0]!;
    expect(stored.scopes.workspaceProvisionedId).toBe(WS);
    const id = stored.id;
    expect(id).toMatch(/^[0-9a-f-]{36}$/);

    const ctxQuery = articles.$scope("articleDraftedId", id);
    const read = await es.query(ctxQuery);
    expect(read.events.map((e) => e.type)).toEqual(["ArticleDrafted"]); // root matches via own id
    expect(read.contextVersion).toBe(1);

    const ok = await es.appendIf([articles.ArticleContentEdited({ body: "text" }, { articleDraftedId: id })], { query: ctxQuery, version: read.contextVersion });
    expect(ok.ok).toBe(true);
    const stale = await es.appendIf([articles.ArticleArchived({}, { articleDraftedId: id })], { query: ctxQuery, version: 1 });
    expect(stale).toEqual({ ok: false, conflict: { expected: 1, actual: 2 } });
    await expect(es.appendIfOrThrow([articles.ArticleArchived({}, { articleDraftedId: id })], { query: ctxQuery, version: 1 })).rejects.toBeInstanceOf(ConflictError);

    const all = await es.query(ctxQuery);
    const state = foldArticles(all.events);
    expect(state.get(id)).toEqual({ id, title: "Hello", body: "text", archived: false });
  });

  it("enforces unique paths and idempotency keys", async () => {
    const es = newApi().forTenant(WS);
    await es.append([articles.ArticleDrafted({ title: "A", slug: "same" }, { workspaceProvisionedId: WS })]);
    await expect(es.append([articles.ArticleDrafted({ title: "B", slug: "same" }, { workspaceProvisionedId: WS })])).rejects.toBeInstanceOf(UniqueViolationError);
    const withKey = articles.ArticleDrafted({ title: "C", slug: "c" }, { workspaceProvisionedId: WS }, { metadata: { idempotencyKey: "k1" } });
    await es.append([withKey]);
    await expect(es.append([articles.ArticleDrafted({ title: "D", slug: "d" }, { workspaceProvisionedId: WS }, { metadata: { idempotencyKey: "k1" } })])).rejects.toBeInstanceOf(UniqueViolationError);
    expect((await es.query(articles.$filter())).events).toHaveLength(2);
  });

  it("tenant view narrows queries and stamps events, fail-closed", async () => {
    const api = newApi();
    const a = api.forTenant("ws-a");
    const b = api.forTenant("ws-b");
    await a.append([articles.ArticleDrafted({ title: "A", slug: "a" }, { workspaceProvisionedId: "ws-a" })]);
    await b.append([articles.ArticleDrafted({ title: "B", slug: "b" }, { workspaceProvisionedId: "ws-b" })]);
    expect((await a.query(articles.$filter())).events.map((e) => e.data.title)).toEqual(["A"]);
    expect((await b.query(articles.$filter())).events.map((e) => e.data.title)).toEqual(["B"]);
    await expect(a.append([articles.ArticleDrafted({ title: "X", slug: "x" }, { workspaceProvisionedId: "ws-b" })])).rejects.toThrow(/tenant/);
    // events without the tenant scope get it stamped
    const edited = await a.query(articles.$filter());
    expect(edited.events[0]!.scopes.workspaceProvisionedId).toBe("ws-a");
  });

  it("strict mode rejects unlockable guards", async () => {
    const es = newApi();
    await expect(es.appendIf([articles.ArticleDrafted({ title: "A", slug: "a" }, { workspaceProvisionedId: WS })], { query: { types: ["ArticleDrafted"] }, version: 0 })).rejects.toBeInstanceOf(UnindexableContextError);
    const lax = createEventStore({ events: [articles], strict: false });
    const out = await lax.appendIf([articles.ArticleDrafted({ title: "A", slug: "a" }, { workspaceProvisionedId: WS })], { query: { types: ["ArticleDrafted"] }, version: 0 });
    expect(out.ok).toBe(true);
  });

  it("runs commands with retry on conflict and rejection mapping", async () => {
    const es = newApi().forTenant(WS);
    const outcome = await es.command<Map<string, Article>, string>({
      context: articles.$filter({ types: ["ArticleDrafted"] }),
      fold: foldInto,
      initial: () => new Map(),
      decide: (state, { id }) => {
        if ([...state.values()].some((a) => a.title === "Hello")) return reject("duplicate", "already drafted");
        const articleId = id();
        return { events: [articles.ArticleDrafted({ title: "Hello", slug: "hello" }, { workspaceProvisionedId: WS }, { id: articleId })], result: articleId };
      },
    });
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) throw new Error("unexpected");
    expect(outcome.appended?.count).toBe(1);
    const again = await es.command<Map<string, Article>, string>({
      context: articles.$filter({ types: ["ArticleDrafted"] }),
      fold: foldInto,
      initial: () => new Map(),
      decide: (state) => ([...state.values()].some((a) => a.title === "Hello") ? reject("duplicate", "already drafted") : { events: [] }),
    });
    expect(again.ok).toBe(false);
    if (again.ok) throw new Error("unexpected");
    expect(again.code).toBe("duplicate");
  });

  it("context cache folds incrementally", async () => {
    const api = newApi();
    const es = api.forTenant(WS);
    const spec = {
      query: articles.$filter(),
      fold: (events: readonly import("../src/index.js").RecordedEvent[], state: number) => state + events.length,
      initial: 0,
    };
    await es.append([articles.ArticleDrafted({ title: "A", slug: "a" }, { workspaceProvisionedId: WS })]);
    const first = await es.context(spec);
    expect(first.state).toBe(1);
    expect(first.cacheHit).toBe(false);
    await es.append([articles.ArticleDrafted({ title: "B", slug: "b" }, { workspaceProvisionedId: WS })]);
    const second = await es.context(spec);
    expect(second.state).toBe(2);
    expect(second.cacheHit).toBe(true);
    expect(second.delta).toHaveLength(1);
    expect(second.ctx.version).toBe(2);
  });

  it("es.read(registry) narrows and re-validates", async () => {
    const es = newApi().forTenant(WS);
    await es.append([articles.ArticleDrafted({ title: "R", slug: "r" }, { workspaceProvisionedId: WS })]);
    const read = await es.read(articles);
    const first = read.events[0]!;
    if (first.type === "ArticleDrafted") expect(first.data.title).toBe("R");
    expect(read.byFilter[0]).toHaveLength(1);
    expect(read.ctx.version).toBe(1);
  });

  it("es.read(registry, query, { omit }) trims typed data; a projection never reaches a decision", async () => {
    const es = newApi().forTenant(WS);
    const drafted = articles.ArticleDrafted({ title: "R", slug: "r" }, { workspaceProvisionedId: WS });
    await es.append([drafted, articles.ArticleContentEdited({ body: "x".repeat(100) }, { articleDraftedId: drafted.id })]);
    const read = await es.read(articles, undefined, { omit: ["body"] });
    const edited = read.events[1]!;
    if (edited.type === "ArticleContentEdited") {
      // @ts-expect-error `body` was omitted — the type says so
      void edited.data.body;
      expect(edited.data).toEqual({});
      expect(edited.scopes.articleDraftedId).toBe(drafted.id);
    }
    const first = read.events[0]!;
    if (first.type === "ArticleDrafted") expect(first.data.title).toBe("R");
    // a key the schema does not know is ignored by the trimmed re-validation
    expect((await es.read(articles, undefined, { omit: ["nope"] })).events).toHaveLength(2);
    // the full read still re-validates completely
    const full = await es.read(articles);
    if (full.events[1]!.type === "ArticleContentEdited") expect(full.events[1]!.data.body).toHaveLength(100);
  });

  it("es.statistics() narrows to the tenant view", async () => {
    const api = newApi();
    const ws1 = api.forTenant(WS);
    const ws2 = api.forTenant("ws-2");
    await ws1.append([articles.ArticleDrafted({ title: "A", slug: "a" }, { workspaceProvisionedId: WS })]);
    await ws2.append([articles.ArticleDrafted({ title: "B", slug: "b" }, { workspaceProvisionedId: "ws-2" })]);
    expect((await ws1.statistics()).map((s) => [s.type, s.count])).toEqual([["ArticleDrafted", 1]]);
    expect((await api.statistics()).map((s) => [s.type, s.count])).toEqual([["ArticleDrafted", 2]]);
  });

  it("close() never connects a store that was never used", async () => {
    let created = 0;
    const api = createEventStore({ store: (schema) => (created++, new MemoryStore({ schema })) });
    await api.close();
    expect(created).toBe(0);
    await api.append([{ type: "Ping", data: {} }]);
    await api.close();
    expect(created).toBe(1);
  });

  it("refuses a pre-built store whose schema disagrees with the registries", async () => {
    const api = createEventStore({ events: [articles], store: new MemoryStore() }); // default schema: no scope keys, non-strict
    await expect(api.append([articles.ArticleDrafted({ title: "A", slug: "a" }, { workspaceProvisionedId: WS })])).rejects.toThrow(/different schema/);
  });

  it("accepts a pre-built store that enforces a superset, refuses one that enforces less", async () => {
    const superset = buildSchema([articles], { scopeKeys: ["extraKey"], tenantScopeKey: "workspaceProvisionedId" });
    const ok = createEventStore({ events: [articles], tenant: { scopeKey: "workspaceProvisionedId" }, store: new MemoryStore({ schema: superset }) });
    await ok.forTenant(WS).append([articles.ArticleDrafted({ title: "S", slug: "s" }, { workspaceProvisionedId: WS })]);
    const salted = createEventStore({ events: [articles], lockSalt: "x", store: new MemoryStore({ schema: buildSchema([articles]) }) });
    await expect(salted.append([articles.ArticleDrafted({ title: "S", slug: "s2" }, { workspaceProvisionedId: WS })])).rejects.toThrow(/lockSalt/);
  });

  it("a tenant root event with a foreign id is refused instead of being shadowed by the stamp", async () => {
    const ws = defineEvents({ WorkspaceProvisioned: { data: z.object({ name: z.string() }) } });
    const api = createEventStore({ events: [ws], tenant: { scopeKey: "workspaceProvisionedId" }, strict: false });
    const root = ws.WorkspaceProvisioned({ name: "A" });
    await api.forTenant(root.id).append([root]); // its own tenant: no self back-link
    expect((await api.forTenant(root.id).query(ws.$filter())).events[0]!.scopes).toEqual({});
    await expect(api.forTenant("other").append([ws.WorkspaceProvisioned({ name: "B" })])).rejects.toThrow(/tenant root/);
  });

  it("a command without context appends unconditionally", async () => {
    const es = newApi().forTenant(WS);
    const out = await es.command({ decide: () => ({ events: [articles.ArticleDrafted({ title: "A", slug: "a" }, { workspaceProvisionedId: WS })], result: "made" }) });
    expect(out.ok && out.result).toBe("made");
    expect((await es.query(articles.$filter())).events).toHaveLength(1);
  });

  it("$foldBy keeps a Map of entries keyed by the event's subject", async () => {
    const byArticle = articles.$foldBy<string, Article>(
      (e) => (e.type === "ArticleDrafted" ? e.id : e.scopes.articleDraftedId),
      {
        ArticleDrafted: (data, _entry, e) => ({ id: e.id, title: data.title, body: "", archived: false }),
        ArticleContentEdited: (data, entry) => (entry ? { ...entry, body: data.body } : null),
        ArticleArchived: (_data, entry) => (entry ? { ...entry, archived: true } : null),
      },
    );
    const es = newApi().forTenant(WS);
    const d = articles.ArticleDrafted({ title: "T", slug: "t" }, { workspaceProvisionedId: WS });
    await es.append([d, articles.ArticleContentEdited({ body: "b" }, { articleDraftedId: d.id })]);
    const loaded = await es.context({ query: articles.$filter(), fold: byArticle, initial: () => new Map<string, Article>() });
    expect(loaded.state.get(d.id)).toEqual({ id: d.id, title: "T", body: "b", archived: false });
    const empty = new Map<string, Article>();
    expect(byArticle([], empty)).toBe(empty); // no events → same Map instance
  });

  it("context cache honours a byte budget", async () => {
    const store = new MemoryStore({ schema: buildSchema([articles], { strict: false }) });
    const cache = new ContextCache(store, { maxBytes: 100, sizeOf: (s) => JSON.stringify(s).length });
    await store.append([articles.ArticleDrafted({ title: "A", slug: "a" }, { workspaceProvisionedId: WS })]);
    const fold = (events: readonly import("../src/index.js").RecordedEvent[], state: string[]) => [...state, ...events.map((e) => e.id + "-".repeat(60))];
    await cache.load({ query: { types: ["ArticleDrafted"] }, fold, initial: [] as string[] });
    expect(cache.size).toBe(1);
    await cache.load({ query: { types: ["ArticleDrafted", "ArticleArchived"] }, fold, initial: [] as string[] });
    expect(cache.size).toBe(1); // the second entry (>100 bytes each) evicted the first
    expect(cache.byteSize).toBeLessThanOrEqual(200);
  });

  it("lock keys are keyed hashes when a salt is configured", () => {
    expect(scopeLockKey("k", "v")).toBe(scopeLockKey("k", "v"));
    expect(scopeLockKey("k", "v", "s1")).not.toBe(scopeLockKey("k", "v"));
    expect(scopeLockKey("k", "v", "s1")).not.toBe(scopeLockKey("k", "v", "s2"));
    expect(globalLockKey("s1")).not.toBe(GLOBAL_LOCK_KEY);
    expect(typeof lockKeyOf("x", "s")).toBe("bigint");
  });

  it("estimateSize understands Maps, arrays and strings", () => {
    const small = estimateSize({ a: 1 });
    const map = estimateSize(new Map(Array.from({ length: 100 }, (_, i) => [`k${i}`, { title: "x".repeat(100) }])));
    expect(map).toBeGreaterThan(100 * 200);
    expect(estimateSize("x".repeat(1000))).toBeGreaterThan(2000);
    expect(small).toBeGreaterThan(0);
  });

  it("the process-wide budget evicts across tenant views, oldest first", async () => {
    const api = createEventStore({
      events: [articles],
      tenant: { scopeKey: "workspaceProvisionedId" },
      contextCache: { totalMaxBytes: 3000, sizeOf: () => 1000 },
    });
    const fold = (events: readonly import("../src/index.js").RecordedEvent[], state: number) => state + events.length;
    for (const ws of ["a", "b", "c", "d"]) {
      await api.forTenant(ws).append([articles.ArticleDrafted({ title: ws, slug: ws }, { workspaceProvisionedId: ws })]);
      await api.forTenant(ws).context({ query: articles.$filter(), fold, initial: 0 });
    }
    // four entries of 1000 bytes against a 3000-byte budget: the oldest view's entry is gone
    const a = await api.forTenant("a").context({ query: articles.$filter(), fold, initial: 0 });
    const d = await api.forTenant("d").context({ query: articles.$filter(), fold, initial: 0 });
    expect(a.cacheHit).toBe(false);
    expect(d.cacheHit).toBe(true);
  });

  it("tenant views are a bounded LRU", async () => {
    const api = createEventStore({ events: [articles], tenant: { scopeKey: "workspaceProvisionedId" }, maxTenantViews: 2 });
    const first = api.forTenant("t1");
    api.forTenant("t2");
    api.forTenant("t3"); // evicts t1
    expect(api.forTenant("t1")).not.toBe(first);
    expect(api.forTenant("t3")).toBe(api.forTenant("t3"));
  });

  it("$parse narrows types", async () => {
    const store = new MemoryStore({ schema: buildSchema([articles], { strict: false }) });
    await store.append([articles.ArticleDrafted({ title: "A", slug: "a" }, { workspaceProvisionedId: WS })]);
    const e: RecordedEventOf<typeof articles.$defs> = articles.$parse((await store.query({ types: ["ArticleDrafted"] })).events[0]!);
    if (e.type === "ArticleDrafted") expect(e.data.title).toBe("A");
  });
});
