/**
 * The live read model examples in README.md and CHANGELOG.md compile against the library, behind a
 * preamble that declares what the prose assumes (the `sales` registry, a tenant, an id, an SSE
 * stream, a request, an append).
 */
import { expect, it } from "vitest";
import { snippet, typecheck } from "./docs.js";

const SALES = `import { defineEvents, type AppendResult } from "@orgops/eventstore";
import { z } from "zod";
const sales = defineEvents({
  OfferMade: { data: z.object({ amount: z.number() }), scopes: ["offerId"] },
  OfferAccepted: { data: z.object({}), scopes: ["offerId"] },
});
declare const t: string;
declare const offerId: string;
declare const sse: { push(value: unknown): void; close(): void };
declare const request: { signal: AbortSignal };
`;

it("the live read model examples in README.md and CHANGELOG.md compile", () => {
  const define = snippet("README.md", "## Live read models");
  const atLeast = snippet("README.md", "**`get(key, { atLeast, timeoutMs })`**");
  const watch = snippet("README.md", "**Push on change, for SSE.**");
  const current = snippet("README.md", "**A list that stays current.**");
  const changelog = snippet("CHANGELOG.md", "**Live read models:");
  const diagnostics = typecheck({
    "__docs_readme_read_model.ts": `${SALES}${define}\n${atLeast}\n${watch}\n${current}\nexport { offer, page, accepted, watch, streamOpenOffers };\n`,
    "__docs_changelog_read_model.ts": `${SALES}import { defineReadModel, es } from "@orgops/eventstore";\ndeclare const appended: AppendResult;\n${changelog}\nexport { offer };\n`,
  });
  expect(diagnostics).toEqual([]);
}, 60_000);
