/**
 * The persisted read model examples in README.md and CHANGELOG.md compile against the library,
 * behind a preamble that declares what the prose assumes (the `sales` registry and the `offers`
 * definition of "Live read models", a connection, a tenant, an id, an append). And the README names
 * the measured rebuild cost.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { expect, it } from "vitest";
import { snippet, typecheck } from "./docs.js";

const PREAMBLE = `import { configure, defineEvents, defineReadModel, es, type AppendResult } from "@orgops/eventstore";
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
declare const connection: string;
declare const t: string;
declare const offerId: string;
`;

it("the persisted read model examples in README.md and CHANGELOG.md compile", () => {
  const open = snippet("README.md", "## Persisted read models");
  const atLeast = snippet("README.md", "**`list({ where, sort, offset, limit, atLeast, timeoutMs })`** reads the stored rows");
  const changelog = snippet("CHANGELOG.md", "**Persisted read models:");
  const diagnostics = typecheck({
    "__docs_readme_persisted_model.ts": `${PREAMBLE}${open}\n${atLeast}\nconst _o: Offer = offer;\nexport { page, status, fresh, _o };\n`,
    "__docs_changelog_persisted_model.ts": `${PREAMBLE}declare const appended: AppendResult;\nvoid configure;\n${changelog}\nexport { offer, page };\n`,
  });
  expect(diagnostics).toEqual([]);
}, 60_000);

it("the README states what a persisted rebuild costs at the default batchSize, with its command, and that a larger batchSize pages faster (R7.3, bench/RESULTS.md S3)", () => {
  const read = (file: string) => readFileSync(resolve(__dirname, "..", file), "utf8");
  const readme = read("README.md");
  const from = readme.indexOf("**Processor, poison and storable states.**");
  expect(from).toBeGreaterThanOrEqual(0);
  const processor = readme.slice(from, readme.indexOf("\n\n", from));
  expect(processor).toMatch(/3\.4 s for 10⁵ events but 76\.0 s for 10⁶ with `batchSize` 500, against 1\.1 s and 10\.6 s with `batchSize` 5 000/);
  expect(processor).toMatch(/`sh bench\/persisted-model\.sh rebuild 1000000 3`/);
  expect(processor).toMatch(/a larger `batchSize`/);
  // the numbers are S3's
  const results = read("bench/RESULTS.md");
  const s3 = results.slice(results.indexOf("### S3 "), results.indexOf("\n### S4 "));
  expect(s3).toContain("`sh bench/persisted-model.sh rebuild 1000000 3`");
  expect(s3).toMatch(/\| 100 000 \| 1 000 \| 500 \| persisted \| \*\*3 350\.2\*\*/);
  expect(s3).toMatch(/\| 100 000 \| 1 000 \| 5 000 \| persisted \| \*\*1 136\.9\*\*/);
  expect(s3).toMatch(/\| 1 000 000 \| 1 000 \| 500 \| persisted \| \*\*76 000\.5\*\*/);
  expect(s3).toMatch(/\| 1 000 000 \| 1 000 \| 5 000 \| persisted \| \*\*10 554\.3\*\*/);
});
