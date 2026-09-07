import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    internal: "src/internal.ts",
    "postgres/index": "src/postgres/index.ts",
    "subscribe/index": "src/subscribe/index.ts",
    "testing/index": "src/testing/index.ts",
  },
  format: ["esm", "cjs"],
  dts: true,
  clean: true,
  sourcemap: true,
  splitting: true,
  treeshake: true,
  external: ["pg", "zod"],
});
