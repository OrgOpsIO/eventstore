import { describe, expect, it } from "vitest";
import pg from "pg";
import { buildSchema, MemoryStore, reset } from "../src/index.js";
import { given } from "../src/testing/index.js";
import { runScenario } from "../examples/articles/scenario.js";
import { accounts, articles, workspace, TENANT_KEY } from "../examples/articles/events.js";

const url = process.env.ES_TEST_DATABASE_URL;

describe("example: articles", () => {
  it("runs end to end on the memory store", async () => {
    reset();
    const report = await runScenario({ store: new MemoryStore({ schema: buildSchema([workspace, accounts, articles], { tenantScopeKey: TENANT_KEY }) }) });
    expect(report.duplicateEmailRejected).toBe(true);
    expect(report.isolation).toBe(true);
    expect(report.concurrentEdits.ok + report.concurrentEdits.conflicts).toBe(10);
    expect(report.concurrentEdits.rejected).toBe(0);
    expect(report.concurrentEdits.ok).toBeGreaterThan(0);
    expect(report.indexed).toBe(2); // a1 (published) and b2 (draft); b1 was archived
    expect(report.titles).toEqual(["Hello", "Hello, world"]);
  });

  it.skipIf(!url)("runs end to end on Postgres", async () => {
    reset();
    const admin = new pg.Pool({ connectionString: url });
    await admin.query('DROP TABLE IF EXISTS "example_events"');
    await admin.end();
    const report = await runScenario({ connection: url!, postgres: { table: "example_events", poolSize: 12 } });
    expect(report.duplicateEmailRejected).toBe(true);
    expect(report.isolation).toBe(true);
    expect(report.concurrentEdits.ok + report.concurrentEdits.conflicts).toBe(10);
    expect(report.concurrentEdits.rejected).toBe(0);
    expect(report.indexed).toBe(2);
    expect(report.titles).toEqual(["Hello", "Hello, world"]);
  });

  it("draft-article: the slug rule as a given/when/then spec", async () => {
    const ws = "ws-1";
    const taken = articles.ArticleDrafted({ title: "Taken", slug: "hello" }, { workspaceProvisionedId: ws });
    const spec = (slug: string) => ({
      context: articles.$filter({ types: ["ArticleDrafted", "ArticleContentEdited", "ArticleArchived"], scopes: { workspaceProvisionedId: ws } }),
      fold: articles.$fold<string[]>({ ArticleDrafted: (d, s) => [...s, d.slug] }),
      initial: [] as string[],
      decide: (slugs: string[]) =>
        slugs.includes(slug)
          ? { ok: false as const, code: "slug_taken", reason: "in use" }
          : { events: [articles.ArticleDrafted({ title: "New", slug }, { workspaceProvisionedId: ws })] },
    });
    await given([taken], { schema: buildSchema([articles], { tenantScopeKey: TENANT_KEY }) }).when(spec("hello")).thenRejects("slug_taken");
    await given([taken], { schema: buildSchema([articles], { tenantScopeKey: TENANT_KEY }) })
      .when(spec("fresh"))
      .then([{ type: "ArticleDrafted", data: { title: "New", slug: "fresh" }, scopes: { workspaceProvisionedId: ws } }]);
  });
});
