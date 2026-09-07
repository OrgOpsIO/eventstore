import { es, reject } from "@orgops/eventstore";
import { articles, foldArticles, type Article } from "../events.js";

/**
 * The per-tenant slug rule is a CCC rule, not a unique index: slugs are freed when an article
 * is archived, so the context is every article of the workspace (a tenant-wide guard → the
 * whole workspace is serialised while drafting, and the context grows with the workspace; the
 * narrower idiom is a SlugClaimed/SlugReleased pair with `slug` declared as a scope key — see
 * README "Uniqueness" — or a global `unique: ["slug"]` when slugs never need to be released;
 * tenant is locked exclusively while this decides).
 */
export async function draftArticle(workspaceId: string, input: { title: string; slug: string }) {
  return es.forTenant(workspaceId).command<Map<string, Article>, string>({
    context: articles.$filter({ types: ["ArticleDrafted", "ArticleContentEdited", "ArticleArchived"] }),
    fold: foldArticles,
    initial: () => new Map(),
    decide: (all) => {
      const taken = [...all.values()].some((a) => a.status !== "archived" && a.slug === input.slug);
      if (taken) return reject("slug_taken", `slug "${input.slug}" is in use in this workspace`);
      const drafted = articles.ArticleDrafted({ title: input.title, slug: input.slug }, { workspaceProvisionedId: workspaceId });
      return { events: [drafted], result: drafted.id };
    },
  });
}
