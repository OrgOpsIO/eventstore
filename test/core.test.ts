import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
  ConflictError,
  MemoryStore,
  UnindexableContextError,
  UniqueViolationError,
  ValidationError,
  buildSchema,
  createEventStore,
  defineEvents,
  reject,
  type RecordedEventOf,
} from "../src/index.js";

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

  it("$parse narrows types", async () => {
    const store = new MemoryStore({ schema: buildSchema([articles], { strict: false }) });
    await store.append([articles.ArticleDrafted({ title: "A", slug: "a" }, { workspaceProvisionedId: WS })]);
    const e: RecordedEventOf<typeof articles.$defs> = articles.$parse((await store.query({ types: ["ArticleDrafted"] })).events[0]!);
    if (e.type === "ArticleDrafted") expect(e.data.title).toBe("A");
  });
});
