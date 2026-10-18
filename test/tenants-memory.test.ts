import { describe, expect, it } from "vitest";
import { buildSchema, createEventStore, defineReadModel, MemoryStore, UsageError, type EventStoreApi } from "../src/index.js";
import { modelFingerprint, foldText, typesText } from "../src/fingerprint.js";
import { initialToken } from "../src/context.js";
import { queryKey } from "../src/query.js";
import { createHash } from "node:crypto";
import type { MemoryReadModelStore } from "../src/modelstore.js";
import { made, offersModel, onRead, pmConfig, tokenOf, until, type Offer } from "./persisted-model.suite.js";
import { crossModel, tenantsSuite, type TenantsHarness } from "./tenants.suite.js";

const schema = buildSchema(pmConfig.events, { strict: true, tenantScopeKey: pmConfig.tenant.scopeKey });

function memoryHarness(): TenantsHarness {
  let store = new MemoryStore({ schema, persistedModels: true });
  const opened: EventStoreApi[] = [];
  const open = (options = {}) => {
    const api = createEventStore({ ...pmConfig, store, ...options });
    opened.push(api);
    return api;
  };
  return {
    async reset() {
      for (const api of opened.splice(0)) await api.close();
      store = new MemoryStore({ schema, persistedModels: true });
    },
    open,
    processor: open,
    async closeAll() {
      for (const api of opened.splice(0)) await api.close();
    },
  };
}

describe("one processor for every tenant on the memory store", () => {
  tenantsSuite(memoryHarness(), { numRuns: 40 });
});

describe("cross-tenant definitions: memory-only cases", () => {
  it("defineReadModel refuses any processor but \"cross-tenant\"; the fingerprint differs with it, and a definition without it hashes exactly as before this stage", () => {
    expect(() => defineReadModel({ ...offersModel, processor: "sideways" as "cross-tenant" })).toThrow(UsageError);
    expect(() => defineReadModel({ ...offersModel, processor: "sideways" as "cross-tenant" })).toThrow(/processor must be "cross-tenant"/);
    const cross = crossModel("offers");
    const plain = defineReadModel<Offer>({ ...offersModel });
    expect(cross.processor).toBe("cross-tenant");
    expect(plain.processor).toBeUndefined();
    expect(modelFingerprint(cross, {})).not.toBe(modelFingerprint(plain, {}));
    // the stage-5 formula, by hand: a fingerprint stored by stage-5 code still matches (no false ES_FOLD_CHANGED)
    const before = createHash("sha256")
      .update(JSON.stringify(["model/1", foldText(plain.fold), queryKey(plain.source), plain.keyBy, initialToken(plain.initial), typesText(plain.source, {})]))
      .digest("hex");
    expect(modelFingerprint(plain, {})).toBe(before);
  });

  it("a cross-tenant root handle without configure({ tenant }) fails with a UsageError that says so, and writes nothing", async () => {
    const plain = buildSchema(pmConfig.events, { strict: true });
    const store = new MemoryStore({ schema: plain, persistedModels: true });
    const api = createEventStore({ events: pmConfig.events, store });
    await api.append([made("o1", 1)]);
    const lone = api.persistedReadModel(crossModel("untenanted"));
    const error = await lone.status().then(
      (s) => s.error,
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(UsageError);
    expect((error as Error).message).toMatch(/needs configure\(\{ tenant: \{ scopeKey \} \}\)/);
    expect((await lone.status()).phase).toBe("failed");
    expect(await store.readModelStore!.head({ name: "untenanted", version: 1 })).toBeNull();
    await api.close();
  });

  it("a zombie processor (its lock taken without its knowledge) is fenced at its next page and writes nothing; the new processor's rows equal context", async () => {
    const store = new MemoryStore({ schema, persistedModels: true });
    const app = createEventStore({ ...pmConfig, store });
    const p1 = createEventStore({ ...pmConfig, store });
    const p2 = createEventStore({ ...pmConfig, store });
    const def = crossModel("zombie");
    const cap = store.readModelStore as MemoryReadModelStore;
    cap.knobs.tickMs = 60_000;
    const first = p1.persistedReadModel(def);
    await until(async () => (await first.status()).role === "writer");
    await app.forTenant("ta").persistedReadModel(def).list({ atLeast: tokenOf(await app.forTenant("ta").append([made("o1", 1)])) });
    cap.steal({ name: def.name, version: def.version }); // p1 does not know it lost the lock
    const second = p2.persistedReadModel(def);
    await until(async () => (await second.status()).role === "writer");
    const last = tokenOf(await app.forTenant("ta").append([made("o1", 2)]));
    await app.forTenant("ta").persistedReadModel(def).list({ atLeast: last });
    await until(async () => (await first.status()).role === "standby");
    expect(await app.forTenant("ta").persistedReadModel(def).get("o1")).toEqual(((await onRead(app.forTenant("ta"), "o1", def)) as { state: Offer }).state);
    expect((await app.forTenant("ta").persistedReadModel(def).get("o1")).total).toBe(3);
    for (const api of [app, p1, p2]) await api.close();
  });
});
