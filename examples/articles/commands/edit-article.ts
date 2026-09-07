import { es, reject, rejectMissing } from "@orgops/eventstore";
import { articles, foldArticle } from "../events.js";

export async function editArticle(workspaceId: string, articleId: string, input: { body: string; title?: string }, retries = 3) {
  return es.forTenant(workspaceId).command({
    context: articles.$scope("articleDraftedId", articleId),
    fold: foldArticle,
    initial: null,
    retries,
    decide: (article) => {
      if (!article) return rejectMissing("not_found", "unknown article");
      if (article.status === "archived") return reject("archived", "an archived article cannot be edited");
      return { events: [articles.ArticleContentEdited({ body: input.body, title: input.title }, { articleDraftedId: articleId })] };
    },
  });
}
