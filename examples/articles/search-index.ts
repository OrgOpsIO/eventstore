import type { EventStore } from "@orgops/eventstore";
import { fileCursors, subscribe, type Subscription } from "@orgops/eventstore/subscribe";
import { articles } from "./events.js";

export interface SearchIndex {
  readonly entries: Map<string, { workspaceId: string; title: string; published: boolean }>;
  readonly subscription: Subscription;
}

/** A durable projection: the cursor survives restarts in a file, `events` stays the only table. */
export function startSearchIndex(store: EventStore, cursorFile: string): SearchIndex {
  const entries = new Map<string, { workspaceId: string; title: string; published: boolean }>();
  const subscription = subscribe(
    "search-index",
    articles.$filter({ types: ["ArticleDrafted", "ArticleContentEdited", "ArticlePublished", "ArticleArchived"] }),
    (events) => {
      for (const raw of events) {
        const event = articles.$parse(raw);
        switch (event.type) {
          case "ArticleDrafted":
            entries.set(event.id, { workspaceId: event.scopes.workspaceProvisionedId, title: event.data.title, published: false });
            break;
          case "ArticleContentEdited": {
            const e = entries.get(event.scopes.articleDraftedId);
            if (e && event.data.title) entries.set(event.scopes.articleDraftedId, { ...e, title: event.data.title });
            break;
          }
          case "ArticlePublished": {
            const e = entries.get(event.scopes.articleDraftedId);
            if (e) entries.set(event.scopes.articleDraftedId, { ...e, published: true });
            break;
          }
          case "ArticleArchived":
            entries.delete(event.scopes.articleDraftedId);
            break;
        }
      }
    },
    { store, cursors: fileCursors(cursorFile), pollIntervalMs: 25 },
  );
  return { entries, subscription };
}
