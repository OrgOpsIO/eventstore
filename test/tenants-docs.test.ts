/**
 * The cross-tenant processor examples in README.md and CHANGELOG.md compile against the library, behind
 * a preamble that declares what the prose assumes (the `sales` registry and the `offers` definition of
 * "Live read models", a tenant, an id). And the CHANGELOG agrees with itself on `invalidate()`.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";
import { snippet, typecheck } from "./docs.js";

const PREAMBLE = `import { defineEvents, defineReadModel, es } from "@orgops/eventstore";
import { z } from "zod";
const sales = defineEvents({
  OfferMade: { data: z.object({ amount: z.number() }), scopes: ["offerId"] },
  OfferAccepted: { data: z.object({}), scopes: ["offerId"] },
});
type Offer = { status: "draft" | "open" | "accepted"; amount: number };
const offers = defineReadModel({
  name: "offers", version: 1, source: sales.$filter(), keyBy: "offerId", initial: (): Offer => ({ status: "draft", amount: 0 }),
  fold: sales.$fold<Offer>({ OfferMade: (d, s) => ({ ...s, status: "open", amount: d.amount }), OfferAccepted: (_, s) => ({ ...s, status: "accepted" }) }),
});
declare const t: string;
declare const offerId: string;
`;

it("the cross-tenant processor examples in README.md and CHANGELOG.md compile", () => {
  const readme = snippet("README.md", "**One processor for every tenant.**");
  const changelog = snippet("CHANGELOG.md", "**One processor for every tenant:");
  const diagnostics = typecheck({
    "__docs_readme_cross_tenant.ts": `${PREAMBLE}${readme}\nconst _o: Offer = offer;\nexport { processor, _o };\n`,
    "__docs_changelog_cross_tenant.ts": `${PREAMBLE}${changelog}\nconst _p: readonly { readonly key: string; readonly state: Offer }[] = page.rows;\nexport { processor, _p };\n`,
  });
  expect(diagnostics).toEqual([]);
}, 60_000);

it("the CHANGELOG says one thing about invalidate() and live read models: it rebuilds them, as the stage-6 entry and the README recipe say (R7.2)", () => {
  const changelog = readFileSync(resolve(__dirname, "..", "CHANGELOG.md"), "utf8");
  const unreleased = changelog.slice(0, changelog.indexOf("\n## ", changelog.indexOf("## 0.7.0") + 1));
  expect(unreleased).not.toMatch(/invalidate\(\)` does not reach live read models/);
  expect(unreleased).toMatch(/`invalidate\(\)` on a view now also rebuilds that view's live read models/);
  const readme = readFileSync(resolve(__dirname, "..", "README.md"), "utf8");
  expect(readme).toMatch(/\*\*In every process, or restart them: `es\.forTenant\(id\)\.invalidate\(\)`\.\*\* It forgets the view's cached contexts, rebuilds the view's live read models/);
});
