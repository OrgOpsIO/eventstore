/**
 * The persisted fold examples in README.md and CHANGELOG.md compile against the library: each
 * `ts` block is taken from the file as it stands, put behind a preamble that declares what the
 * prose assumes (the `ledger` registry, a connection, an id, a query), and type-checked with the
 * repository's compiler options. An example that does not compile is a red test, not a reader's
 * surprise.
 */
import { expect, it } from "vitest";
import { snippet, typecheck } from "./docs.js";

const LEDGER = `import { defineEvents } from "@orgops/eventstore";
import { z } from "zod";
const ledger = defineEvents({
  EntryBooked: { data: z.object({ amount: z.number(), category: z.string() }), scopes: ["ledgerId"] },
});
`;

it("the persisted fold examples in README.md and CHANGELOG.md compile", () => {
  const readme = snippet("README.md", "## Persisted folds");
  const changelog = snippet("CHANGELOG.md", "**Persisted folds:");
  const diagnostics = typecheck({
    "__docs_readme_persisted_fold.ts": `${LEDGER}declare const connection: string;\ndeclare const id: string;\n${readme}\nexport { state, ctx };\n`,
    "__docs_changelog_persisted_fold.ts": `${LEDGER}import { defineFold, es, type Query } from "@orgops/eventstore";\ndeclare const query: Query;\n${changelog}\nexport { ledgerStats };\n`,
  });
  expect(diagnostics).toEqual([]);
}, 60_000);
