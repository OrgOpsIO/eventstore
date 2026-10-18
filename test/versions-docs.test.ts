/**
 * The stage-5 examples in README.md and CHANGELOG.md compile against the library, behind a preamble
 * that declares what the prose assumes (the `sales` registry and the `offers` definition of "Live
 * read models", a tenant, an id). And the stage-5 prose names the limits the code keeps.
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

it("the version and live-start examples in README.md and CHANGELOG.md compile", () => {
  const start = snippet("README.md", "**Starting from a persisted state.**");
  const changelog = snippet("CHANGELOG.md", "**Versions and rebuild:");
  const diagnostics = typecheck({
    "__docs_readme_versions.ts": `${PREAMBLE}${start}\nconst _o: Offer = fromRows;\nexport { live, _o };\n`,
    "__docs_changelog_versions.ts": `${PREAMBLE}${changelog}\nconst _b: boolean = status;\nexport { live, offer, _b };\n`,
  });
  expect(diagnostics).toEqual([]);
}, 60_000);

/** The paragraph of `file` that starts with `start`, up to the next blank line. */
function paragraph(file: string, start: string): string {
  const text = readFileSync(resolve(__dirname, "..", file), "utf8");
  const from = text.indexOf(start);
  expect(from, `${start} in ${file}`).toBeGreaterThanOrEqual(0);
  const end = text.indexOf("\n\n", from);
  return text.slice(from, end < 0 ? undefined : end);
}

it("where the docs promise a building version's get, they say a delta over maxEvents is folded in pages and a bump never throws ContextTooLargeError (R5.1, O24 b)", () => {
  const bullet = paragraph("README.md", "- its `get(key)` answers at once").split("\n")[0]!;
  expect(bullet).toMatch(/in pages of `maxEvents`/);
  expect(bullet).toMatch(/never throws `ContextTooLargeError`/);
  expect(bullet).not.toMatch(/throws `ContextTooLargeError` until/);
  const entry = paragraph("CHANGELOG.md", "- **Versions and rebuild:");
  expect(entry).toMatch(/its `get` answers at once \([^)]*in pages of `maxEvents`[^)]*never throws `ContextTooLargeError`/);
  expect(paragraph("README.md", "**`get(key)`** reads the key's row")).toMatch(/in pages\*\* of `maxEvents`/);
  expect(paragraph("README.md", "| `ContextTooLargeError` |").split("\n")[0]).not.toMatch(/persisted read model's `get` delta, exceeds/);
});

it("the general sentences on ContextTooLargeError name the persisted fold tier's paged load (R5.2)", () => {
  expect(paragraph("README.md", "A context larger than `contextCache.maxEvents`")).toMatch(/in pages/);
  expect(paragraph("README.md", "| `ContextTooLargeError` |").split("\n")[0]).toMatch(/in pages/);
});
