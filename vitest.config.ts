import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const src = (p: string) => fileURLToPath(new URL(`./src/${p}`, import.meta.url));

export default defineConfig({
  // the examples import the package by name, like a consumer would
  resolve: {
    alias: [
      { find: "@orgops/eventstore/postgres", replacement: src("postgres/index.ts") },
      { find: "@orgops/eventstore/subscribe", replacement: src("subscribe/index.ts") },
      { find: "@orgops/eventstore/testing", replacement: src("testing/index.ts") },
      { find: "@orgops/eventstore/internal", replacement: src("internal.ts") },
      { find: "@orgops/eventstore", replacement: src("index.ts") },
    ],
  },
  test: { include: ["test/**/*.test.ts"], fileParallelism: false },
});
