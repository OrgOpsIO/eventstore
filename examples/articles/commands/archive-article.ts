import { es, reject, rejectMissing } from "@orgops/eventstore";
import { articles, foldArticle } from "../events.js";

export async function archiveArticle(workspaceId: string, articleId: string, reason?: string) {
  return es.forTenant(workspaceId).command({
    context: articles.$scope("articleDraftedId", articleId),
    fold: foldArticle,
    initial: null,
    decide: (article) => {
      if (!article) return rejectMissing("not_found", "unknown article");
      if (article.status === "archived") return reject("archived", "already archived");
      return { events: [articles.ArticleArchived({ reason }, { articleDraftedId: articleId })] };
    },
  });
}
