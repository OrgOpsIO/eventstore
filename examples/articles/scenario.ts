import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { configure, es, httpStatusOf, UniqueViolationError, type EventStoreConfig } from "@orgops/eventstore";
import { accounts, articles, workspace, TENANT_KEY } from "./events.js";
import { provisionWorkspace } from "./commands/provision-workspace.js";
import { registerAccount } from "./commands/register-account.js";
import { draftArticle } from "./commands/draft-article.js";
import { editArticle } from "./commands/edit-article.js";
import { publishArticle } from "./commands/publish-article.js";
import { archiveArticle } from "./commands/archive-article.js";
import { listArticles, titleHistory } from "./queries.js";
import { startSearchIndex } from "./search-index.js";

export interface ScenarioReport {
  readonly workspaces: number;
  readonly duplicateEmailRejected: boolean;
  readonly isolation: boolean;
  readonly concurrentEdits: { ok: number; conflicts: number; rejected: number };
  readonly indexed: number;
  readonly titles: string[];
}

/** Runs the whole application flow against whatever store `base` selects (memory or Postgres). */
export async function runScenario(base: Pick<EventStoreConfig, "store" | "connection" | "postgres">): Promise<ScenarioReport> {
  configure({ ...base, events: [workspace, accounts, articles], tenant: { scopeKey: TENANT_KEY }, contextCache: { max: 100 } });
  const store = await es.store();
  const index = startSearchIndex(store, join(mkdtempSync(join(tmpdir(), "es-example-")), "cursors.json"));

  // workspaces (each its own tenant) and platform-level accounts
  const wsA = unwrap(await provisionWorkspace({ slug: "acme", name: "Acme" }));
  const wsB = unwrap(await provisionWorkspace({ slug: "globex", name: "Globex" }));
  unwrap(await registerAccount({ email: "mary@example.com", displayName: "Mary" }));
  let duplicateEmailRejected = false;
  try {
    await registerAccount({ email: "mary@example.com", displayName: "Mary again" });
  } catch (error) {
    duplicateEmailRejected = error instanceof UniqueViolationError && error.httpStatus === 409;
  }

  // articles in both workspaces; the slug rule is per workspace
  const a1 = unwrap(await draftArticle(wsA, { title: "Hello", slug: "hello" }));
  const b1 = unwrap(await draftArticle(wsB, { title: "Hello too", slug: "hello" })); // same slug, other tenant: fine
  const dup = await draftArticle(wsA, { title: "Hello again", slug: "hello" });
  if (dup.ok || dup.code !== "slug_taken" || httpStatusOf(dup) !== 422) throw new Error("slug rule did not fire");

  unwrap(await editArticle(wsA, a1, { body: "First draft", title: "Hello, world" }));
  unwrap(await publishArticle(wsA, a1));
  unwrap(await publishArticle(wsA, a1)); // idempotent: appends nothing
  unwrap(await editArticle(wsB, b1, { body: "B body" }));
  unwrap(await archiveArticle(wsB, b1, "superseded"));
  const editArchived = await editArticle(wsB, b1, { body: "too late" });
  if (editArchived.ok || editArchived.code !== "archived") throw new Error("archived article accepted an edit");
  const b2 = unwrap(await draftArticle(wsB, { title: "Hello 2", slug: "hello" })); // slug freed by the archive
  const missing = await publishArticle(wsA, "does-not-exist");
  if (missing.ok || httpStatusOf(missing) !== 404) throw new Error("missing article did not map to 404");

  // tenant isolation: B's view does not see A's article, and A's id cannot be edited through B
  const listA = await listArticles(wsA);
  const listB = await listArticles(wsB);
  const crossTenant = await editArticle(wsB, a1, { body: "sneaky" });
  const isolation = listA.map((a) => a.id).join() === a1 && listB.map((a) => a.id).sort().join() === [b1, b2].sort().join() && !crossTenant.ok && crossTenant.code === "not_found";

  // 10 concurrent edits on one article: every one either commits (after retries) or gives up with a conflict
  const outcomes = await Promise.all(Array.from({ length: 10 }, (_, i) => editArticle(wsA, a1, { body: `edit ${i}` }, 10)));
  const concurrentEdits = {
    ok: outcomes.filter((o) => o.ok).length,
    conflicts: outcomes.filter((o) => !o.ok && o.code === "conflict").length,
    rejected: outcomes.filter((o) => !o.ok && o.code !== "conflict").length,
  };

  await index.subscription.whenCaughtUp();
  await index.subscription.stop();
  const titles = await titleHistory(wsA, a1);
  await es.close();
  return {
    workspaces: 2,
    duplicateEmailRejected,
    isolation,
    concurrentEdits,
    indexed: index.entries.size,
    titles,
  };
}

function unwrap<R>(outcome: { ok: true; result: R } | { ok: false; code: string; reason: string }): R {
  if (!outcome.ok) throw new Error(`${outcome.code}: ${outcome.reason}`);
  return outcome.result;
}
