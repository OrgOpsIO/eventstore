import { es } from "@orgops/eventstore";
import { articles, type Article } from "./events.js";

const listFold = articles.$foldAll<Map<string, Article>>(() => new Map(), {
  ArticleDrafted: (data, state, e) => new Map(state).set(e.id, { id: e.id, title: data.title, slug: data.slug, body: "", status: "draft" }),
  ArticleContentEdited: (data, state, e) => patch(state, e.scopes.articleDraftedId, { body: data.body, ...(data.title ? { title: data.title } : {}) }),
  ArticlePublished: (_d, state, e) => patch(state, e.scopes.articleDraftedId, { status: "published" }),
  ArticleArchived: (_d, state, e) => patch(state, e.scopes.articleDraftedId, { status: "archived" }),
});

function patch(state: Map<string, Article>, id: string, changes: Partial<Article>): Map<string, Article> {
  const a = state.get(id);
  return a ? new Map(state).set(id, { ...a, ...changes }) : state;
}

/** All articles of a workspace: a typed read (narrowed + re-validated) folded in one go. */
export async function listArticles(workspaceId: string): Promise<Article[]> {
  const read = await es.forTenant(workspaceId).read(articles);
  return [...listFold(read.events).values()];
}

/** One article's title history, typed: `event.type === "ArticleDrafted"` narrows `data`. */
export async function titleHistory(workspaceId: string, articleId: string): Promise<string[]> {
  const read = await es.forTenant(workspaceId).read(articles, articles.$scope("articleDraftedId", articleId));
  const titles: string[] = [];
  for (const event of read.events) {
    if (event.type === "ArticleDrafted") titles.push(event.data.title);
    else if (event.type === "ArticleContentEdited" && event.data.title) titles.push(event.data.title);
  }
  return titles;
}
