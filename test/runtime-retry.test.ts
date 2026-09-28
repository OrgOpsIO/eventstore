import { describe, expect, it } from "vitest";
import { createEventStore, MemoryStore } from "../src/index.js";

describe("a store that fails at first use", () => {
  it("is attempted again by the next call on the root view", async () => {
    let attempts = 0;
    const api = createEventStore({
      strict: false,
      store: (schema) => {
        attempts += 1;
        if (attempts === 1) throw new Error("connection refused");
        return new MemoryStore({ schema });
      },
    });

    await expect(api.query({ types: ["Opened"] })).rejects.toThrow("connection refused");
    await api.append([{ type: "Opened", id: "o-1", data: {}, scopes: {} }]);
    expect((await api.query({ types: ["Opened"] })).events.map((e) => e.id)).toEqual(["o-1"]);
    expect(attempts).toBe(2);
  });

  it("is attempted again by the next call on a tenant view", async () => {
    let attempts = 0;
    const api = createEventStore({
      strict: false,
      tenant: { scopeKey: "tenantId" },
      store: (schema) => {
        attempts += 1;
        if (attempts === 1) throw new Error("connection refused");
        return new MemoryStore({ schema });
      },
    });
    const view = api.forTenant("t-1");

    await expect(view.query({ types: ["Opened"] })).rejects.toThrow("connection refused");
    await view.append([{ type: "Opened", id: "o-1", data: {}, scopes: {} }]);
    expect((await view.query({ types: ["Opened"] })).events.map((e) => e.id)).toEqual(["o-1"]);
  });
});
