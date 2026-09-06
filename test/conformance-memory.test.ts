import { describe, it } from "vitest";
import { MemoryStore } from "../src/index.js";
import { conformanceSuite } from "../src/testing/index.js";

describe("MemoryStore conformance", () => {
  conformanceSuite((schema) => new MemoryStore({ schema }), { test: it });
});
