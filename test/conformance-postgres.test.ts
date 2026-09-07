import { describe, it } from "vitest";
import pg from "pg";
import { PostgresStore } from "../src/postgres/index.js";
import { conformanceSuite } from "../src/testing/index.js";
import type { StoreSchema } from "../src/index.js";

const url = process.env.ES_TEST_DATABASE_URL;

describe.skipIf(!url)("PostgresStore conformance", () => {
  const installed = new Set<string>();
  // The suite closes every store it receives, so each case gets its own pool on a shared table.
  conformanceSuite(
    async (schema: StoreSchema) => {
      const table = `conf_${schema.strict ? "strict" : "lax"}`;
      const admin = new pg.Pool({ connectionString: url });
      if (!installed.has(table)) {
        await admin.query(`DROP TABLE IF EXISTS "${table}"`);
        installed.add(table);
      } else {
        await admin.query(`TRUNCATE "${table}" RESTART IDENTITY`);
      }
      await admin.end();
      // rebuildScopeIndexes: repairs a foreign es_scope left behind by an aborted run (scratch DB)
      return new PostgresStore({ connection: url!, schema, table, poolSize: 25, rebuildScopeIndexes: true });
    },
    { test: it },
  );
});
