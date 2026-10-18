import { describe, expect, it } from "vitest";
import { buildSchema, createEventStore, MemoryStore, UsageError, type EventStoreApi } from "../src/index.js";
import { RetiredError, type MemoryReadModelStore } from "../src/modelstore.js";
import { capOf } from "./persisted-model.suite.js";
import { versionsSuite, type VersionsHarness } from "./versions.suite.js";
import { made, offersV1, pmConfig, tokenOf } from "./versions.model.js";

const schema = buildSchema(pmConfig.events, { strict: true, tenantScopeKey: pmConfig.tenant.scopeKey });

function memoryHarness(): VersionsHarness {
  let store = new MemoryStore({ schema, persistedModels: true });
  const opened: EventStoreApi[] = [];
  const cap = () => store.readModelStore as MemoryReadModelStore;
  const headOf = async (name: string, version: number) => {
    try {
      return await cap().head({ name, version });
    } catch (error) {
      if (error instanceof RetiredError) return { status: "retired" as const };
      throw error;
    }
  };
  return {
    big: 500,
    writers: 1,
    async reset() {
      for (const api of opened.splice(0)) await api.close();
      store = new MemoryStore({ schema, persistedModels: true });
    },
    open(options = {}) {
      const api = createEventStore({ ...pmConfig, store, ...options });
      opened.push(api);
      return api;
    },
    async retire(name, newVersion) {
      // the recipe's condition: only below a version that has switched
      if ((await headOf(name, newVersion))?.status !== "active") return;
      for (let version = 1; version < newVersion; version++) {
        const head = await headOf(name, version);
        if (head && head.status !== "retired") await cap().retire({ name, version });
      }
    },
    async stateRows(name, version) {
      return (await cap().changes({ name, version }, { revision: -1, key: null }, 1_000_000)).length;
    },
    async corrupt(api, ref, key) {
      const c = await capOf(api);
      const owner = {};
      const claim = await c.lock(ref, owner, null);
      if (!claim) throw new Error("the model has a writer: stop it first");
      const head = await c.head(ref);
      await c.page(ref, claim.epoch, async () => ({ writes: [{ key, state: Uint8Array.from([0]), doc: {}, failed: null }], cursor: head!.cursor! }));
      await c.unlock(ref, owner);
    },
    async closeAll() {
      for (const api of opened.splice(0)) await api.close();
    },
  };
}

describe("versions and rebuild on the memory store", () => {
  versionsSuite(memoryHarness(), { numRuns: 40 });
});

describe("versions and rebuild: memory-only cases", () => {
  it("start: 'persisted' on a store without persistedModels: the live model fails with a UsageError that says what to do", async () => {
    const api = createEventStore({ ...pmConfig, store: new MemoryStore({ schema }) });
    const model = api.readModel(offersV1, { start: "persisted" });
    const error = await model.get("o1").catch((e: unknown) => e);
    expect(error).toBeInstanceOf(UsageError);
    expect((error as Error).message).toMatch(/start: "persisted" needs a store that keeps persisted read models — create it with persistedModels: true/);
    expect(model.status.phase).toBe("failed");
    await api.close();
  });

  it("a retired version's tombstone: claim, page, activate, head, latest and list all throw the retirement error; the snapshot reports it with no rows", async () => {
    const store = new MemoryStore({ schema, persistedModels: true });
    const api = createEventStore({ ...pmConfig, store });
    const t = await api.append([made("o1", 1)]);
    const model = api.persistedReadModel(offersV1);
    await model.list({ atLeast: tokenOf(t) });
    await model.stop();
    const cap = store.readModelStore as MemoryReadModelStore;
    const ref = { name: "offers", version: 1 };
    await cap.retire(ref);
    const retired = /persisted read model "offers" v1 was retired/;
    await expect(cap.lock(ref, {}, null)).rejects.toThrow(retired);
    await expect(cap.page(ref, 1, async () => null)).rejects.toThrow(retired);
    await expect(cap.activate(ref, 1)).rejects.toThrow(retired);
    await expect(cap.head(ref)).rejects.toThrow(retired);
    await expect(cap.latest(ref, "o1")).rejects.toThrow(retired);
    await expect(cap.list(ref, { where: [], sort: null, offset: 0, limit: 10 })).rejects.toThrow(retired);
    const rows: unknown[] = [];
    expect(await cap.snapshot(ref, (r) => void rows.push(...r))).toMatchObject({ status: "retired" });
    expect(rows).toEqual([]);
    await api.close();
  });
});
