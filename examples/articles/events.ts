import { z } from "zod";
import { defineEvents } from "@orgops/eventstore";

/** The tenant root: a workspace IS the id of its WorkspaceProvisioned event. */
export const workspace = defineEvents({
  WorkspaceProvisioned: {
    data: z.object({ slug: z.string().min(1), name: z.string().min(1) }),
    unique: ["slug"], // enforced by Postgres across all workspaces
  },
});

/** Platform-level: accounts live in the platform tenant, not in a workspace. */
export const accounts = defineEvents({
  AccountRegistered: {
    data: z.object({ email: z.string().email(), displayName: z.string() }),
    unique: ["email"],
  },
});

export const articles = defineEvents({
  ArticleDrafted: {
    data: z.object({ title: z.string().min(1), slug: z.string().min(1) }),
    scopes: ["workspaceProvisionedId"],
    // legacy rows stored the title as `headline`
    upcast: (p) => ("headline" in p && !("title" in p) ? { ...p, title: p.headline } : p),
  },
  ArticleContentEdited: {
    data: z.object({ title: z.string().min(1).optional(), body: z.string() }),
    scopes: ["articleDraftedId"],
  },
  ArticlePublished: {
    data: z.object({ at: z.string() }),
    scopes: ["articleDraftedId"],
  },
  ArticleArchived: {
    data: z.object({ reason: z.string().optional() }),
    scopes: ["articleDraftedId"],
  },
});

export const TENANT_KEY = "workspaceProvisionedId";

export type Article = {
  readonly id: string;
  readonly title: string;
  readonly slug: string;
  readonly body: string;
  readonly status: "draft" | "published" | "archived";
};

/** Incremental fold of ONE article (context: `articles.$scope("articleDraftedId", id)`). */
export const foldArticle = articles.$fold<Article | null>({
  ArticleDrafted: (data, _state, e) => ({ id: e.id, title: data.title, slug: data.slug, body: "", status: "draft" }),
  ArticleContentEdited: (data, state) => (state ? { ...state, body: data.body, title: data.title ?? state.title } : state),
  ArticlePublished: (_data, state) => (state ? { ...state, status: "published" } : state),
  ArticleArchived: (_data, state) => (state ? { ...state, status: "archived" } : state),
});

/** Incremental fold of ALL articles of a workspace, keyed by id. */
export const foldArticles = articles.$fold<Map<string, Article>>({
  ArticleDrafted: (data, state, e) => new Map(state).set(e.id, { id: e.id, title: data.title, slug: data.slug, body: "", status: "draft" }),
  ArticleContentEdited: (data, state, e) => update(state, e.scopes.articleDraftedId, (a) => ({ ...a, body: data.body, title: data.title ?? a.title })),
  ArticlePublished: (_data, state, e) => update(state, e.scopes.articleDraftedId, (a) => ({ ...a, status: "published" })),
  ArticleArchived: (_data, state, e) => update(state, e.scopes.articleDraftedId, (a) => ({ ...a, status: "archived" })),
});

function update(state: Map<string, Article>, id: string, fn: (a: Article) => Article): Map<string, Article> {
  const a = state.get(id);
  return a ? new Map(state).set(id, fn(a)) : state;
}
