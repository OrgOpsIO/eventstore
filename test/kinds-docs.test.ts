/**
 * The examples of README.md "Kept state: four kinds" (one per kind), the typed definitions paragraph of "Live read
 * models", and the CHANGELOG's typed block compile against the library, behind a preamble that declares what the
 * prose assumes (the `sales` registry, a tenant, an id, a connection string, and for the overview and the CHANGELOG
 * the `Offer` state and its fold). The `@ts-expect-error` lines in them must be errors: an unused directive is a
 * diagnostic too.
 */
import { expect, it } from "vitest";
import { snippet, typecheck } from "./docs.js";

const SALES = `import { defineEvents } from "@orgops/eventstore";
import { z } from "zod";
const sales = defineEvents({
  OfferMade: { data: z.object({ amount: z.number() }), scopes: ["offerId"] },
  OfferAccepted: { data: z.object({}), scopes: ["offerId"] },
});
declare const t: string;
declare const offerId: string;
declare const connection: string;
`;

/** What the overview and the CHANGELOG assume beyond the registry; "Live read models" imports and declares its own. */
const OFFER = `import { configure, defineFold, defineReadModel, es } from "@orgops/eventstore";
type Offer = { status: "draft" | "open" | "accepted"; amount: number };
const initial = (): Offer => ({ status: "draft", amount: 0 });
const fold = sales.$fold<Offer>({
  OfferMade: (d, s) => ({ ...s, status: "open", amount: d.amount }),
  OfferAccepted: (_, s) => ({ ...s, status: "accepted" }),
});
`;

it("the four kinds' examples, the typed definitions paragraph and the CHANGELOG's typed block compile", () => {
  const kinds = ["### 1. Fold on read", "### 2. Persisted fold", "### 3. Live read model", "### 4. Persisted read model"].map((h) => snippet("README.md", h));
  const define = snippet("README.md", "## Live read models");
  const typed = snippet("README.md", "**Typed definitions and reads.**");
  const changelog = snippet("CHANGELOG.md", "**Typed read model definitions and reads:");
  const diagnostics = typecheck({
    "__docs_readme_kinds.ts": `${SALES}${OFFER}${kinds.join("\n")}\nexport { state, ctx, fromStored, live, offer, stored, page };\n`,
    "__docs_readme_typed.ts": `${SALES}${define}\n${typed}\nexport { offer, page, accepted };\n`,
    "__docs_changelog_typed.ts": `${SALES}${OFFER}${changelog}\nexport { page };\n`,
  });
  expect(diagnostics).toEqual([]);
}, 60_000);
