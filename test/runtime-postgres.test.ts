import { afterAll, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { z } from "zod";
import { createEventStore, defineEvents, reject, rejectMissing, httpStatusOf, type EventStoreApi } from "../src/index.js";

const url = process.env.ES_TEST_DATABASE_URL;

const articles = defineEvents({
  ArticleDrafted: { data: z.object({ title: z.string().min(1), slug: z.string() }), scopes: ["workspaceProvisionedId"], unique: ["slug"] },
  ArticleArchived: { data: z.object({ reason: z.string().optional() }), scopes: ["articleDraftedId"] },
});

type Article = { id: string; title: string; archived: boolean };
const foldOne = (events: readonly import("../src/index.js").RecordedEvent[], state: Article | null): Article | null =>
  events.reduce<Article | null>((s, e) => {
    if (e.type === "ArticleDrafted") return { id: e.id, title: (e.data as { title: string }).title, archived: false };
    if (e.type === "ArticleArchived" && s) return { ...s, archived: true };
    return s;
  }, state);

describe.skipIf(!url)("configure({ connection }) end to end on Postgres", () => {
  const TABLE = "rt_events";
  let api: EventStoreApi;

  beforeAll(async () => {
    const admin = new pg.Pool({ connectionString: url });
    await admin.query(`DROP TABLE IF EXISTS "${TABLE}"`);
    await admin.end();
    api = createEventStore({
      connection: url!,
      events: [articles],
      tenant: { scopeKey: "workspaceProvisionedId" },
      postgres: { table: TABLE, poolSize: 8 },
    });
  });
  afterAll(async () => {
    await api.close();
  });

  it("runs a full command cycle per tenant with retries, rejections and conflicts", async () => {
    const ws = api.forTenant("ws-1");
    const other = api.forTenant("ws-2");

    const drafted = await ws.command<Article | null, string>({
      context: articles.$filter({ types: ["ArticleDrafted"], scopes: { workspaceProvisionedId: "ws-1" } }),
      decide: (_events, { id }) => {
        const articleId = id();
        return { events: [articles.ArticleDrafted({ title: "Hello", slug: "hello" }, { workspaceProvisionedId: "ws-1" }, { id: articleId })], result: articleId };
      },
    });
    if (!drafted.ok) throw new Error(JSON.stringify(drafted));
    const id = drafted.result;
    expect(httpStatusOf(drafted)).toBe(201);

    // the other tenant does not see it
    expect((await other.query(articles.$filter())).events).toHaveLength(0);
    expect((await ws.query(articles.$filter())).events).toHaveLength(1);

    // archive via context + fold, twice: second is a business rejection
    const archive = () =>
      ws.command<Article | null, void>({
        context: articles.$scope("articleDraftedId", id),
        fold: foldOne,
        initial: null,
        decide: (article) => {
          if (!article) return rejectMissing("not_found", "unknown article");
          if (article.archived) return reject("archived", "already archived");
          return { events: [articles.ArticleArchived({ reason: "done" }, { articleDraftedId: id })] };
        },
      });
    const first = await archive();
    expect(first.ok).toBe(true);
    const second = await archive();
    expect(second.ok).toBe(false);
    if (second.ok) throw new Error("unexpected");
    expect(second.code).toBe("archived");
    expect(httpStatusOf(second)).toBe(422);

    const missing = await ws.command<Article | null, void>({
      context: articles.$scope("articleDraftedId", "does-not-exist"),
      fold: foldOne,
      initial: null,
      decide: (article) => (article ? { events: [] } : rejectMissing("not_found", "unknown article")),
    });
    expect(httpStatusOf(missing)).toBe(404);

    // unique slug across the tenant's events
    await expect(ws.append([articles.ArticleDrafted({ title: "Again", slug: "hello" }, { workspaceProvisionedId: "ws-1" })])).rejects.toThrow(/unique/);

    // context cache: second load is a cache hit with an empty delta
    const spec = { query: articles.$scope("articleDraftedId", id), fold: foldOne, initial: null };
    const a = await ws.context(spec);
    const b = await ws.context(spec);
    // the archive commands above already warmed this query's entry
    expect(b.cacheHit).toBe(true);
    expect(b.delta).toHaveLength(0);
    expect(b.state?.archived).toBe(true);
    expect(b.ctx.version).toBe(a.ctx.version);
  });

  it("16 concurrent commands on one context: exactly one wins per round, all others retry and reject", async () => {
    const ws = api.forTenant("ws-3");
    const created = await ws.append([articles.ArticleDrafted({ title: "Race", slug: "race" }, { workspaceProvisionedId: "ws-3" })]);
    const id = (await ws.query({ types: ["ArticleDrafted"], scopes: { workspaceProvisionedId: "ws-3" } })).events[0]!.id;
    expect(created.count).toBe(1);
    const outcomes = await Promise.all(
      Array.from({ length: 16 }, () =>
        ws.command<Article | null, void>({
          context: articles.$scope("articleDraftedId", id),
          fold: foldOne,
          initial: null,
          retries: 5,
          decide: (article) => (article?.archived ? reject("archived", "already") : { events: [articles.ArticleArchived({}, { articleDraftedId: id })] }),
        }),
      ),
    );
    if (outcomes.filter((o) => o.ok).length !== 1) throw new Error(JSON.stringify(outcomes.slice(0, 3)));
    expect(outcomes.filter((o) => !o.ok && o.code === "archived")).toHaveLength(15);
    expect((await ws.query(articles.$scope("articleDraftedId", id))).events.map((e) => e.type)).toEqual(["ArticleDrafted", "ArticleArchived"]);
  });
});
