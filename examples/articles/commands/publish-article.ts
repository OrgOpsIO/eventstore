import { es, reject, rejectMissing } from "@orgops/eventstore";
import { articles, foldArticle } from "../events.js";

export async function publishArticle(workspaceId: string, articleId: string) {
  return es.forTenant(workspaceId).command({
    context: articles.$scope("articleDraftedId", articleId),
    fold: foldArticle,
    initial: null,
    decide: (article, { now }) => {
      if (!article) return rejectMissing("not_found", "unknown article");
      if (article.status === "published") return { events: [] }; // idempotent: nothing to record
      if (article.status === "archived") return reject("archived", "an archived article cannot be published");
      if (article.body.trim() === "") return reject("empty", "write something first");
      return { events: [articles.ArticlePublished({ at: now.toISOString() }, { articleDraftedId: articleId })] };
    },
  });
}
