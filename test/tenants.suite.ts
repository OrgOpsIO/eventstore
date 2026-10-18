/**
 * One processor for every tenant (decision 45), specified once and run against the memory store, Postgres without
 * rls and Postgres under rls with a non-owner application role and a processor role (tenants-memory.test.ts,
 * tenants-postgres.test.ts, tenants-rls.test.ts). A "process" is a fresh runtime over the same data; the processor
 * runs on the root view of its own runtime, every tenant reads through `forTenant(t)` of another.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { compareCursor, defineReadModel, FailedKeyError, TransientError, UsageError, type Cursor, type EventStoreApi, type EventStoreConfig, type NewEvent, type PersistedReadModel, type ReadModelDefinition } from "../src/index.js";
import { keyQuery } from "../src/readmodel.js";
import { knobsOf, made, noted, audited, offerFold, offersModel, onRead, source, tokenOf, until, wait, type Offer } from "./persisted-model.suite.js";
import { offers } from "./readmodel.model.js";

export interface TenantsHarness {
  /** Empty events and model tables for the next test. */
  reset(): Promise<void>;
  /** A new application "process": its tenant views read and append (under rls: the application role). */
  open(options?: Partial<EventStoreConfig>): EventStoreApi;
  /** A new processor "process": its root view runs the processor (under rls: connected as the processor role). */
  processor(options?: Partial<EventStoreConfig>): EventStoreApi;
  closeAll(): Promise<void>;
}

const KEYS = ["o1", "o2", "o3", "o4", "o5"];
const TENANTS = ["ta", "tb", "tc", "td", "te"];

/** A cross-tenant offers model under a name of its own. */
export function crossModel(name: string, extra: Partial<ReadModelDefinition<Offer>> = {}): ReadModelDefinition<Offer> {
  return defineReadModel<Offer>({ ...offersModel, ...extra, name, processor: "cross-tenant" });
}

async function fromModel<S>(model: PersistedReadModel<S>, key: string): Promise<{ ok: true; state: S } | { ok: false; error: unknown }> {
  try {
    return { ok: true, state: await model.get(key) };
  } catch (error) {
    return { ok: false, error };
  }
}

/** Every key of every tenant against fold on read: the mismatches. */
async function compareAll(app: EventStoreApi, def: ReadModelDefinition<Offer>, tenants: readonly string[], keys: readonly string[] = [...KEYS, "never"]): Promise<{ compared: number; mismatches: string[] }> {
  const mismatches: string[] = [];
  let compared = 0;
  for (const t of tenants) {
    const reader = app.forTenant(t).persistedReadModel(def);
    for (const key of keys) {
      const expected = await onRead(app.forTenant(t), key, def);
      const got = await fromModel(reader, key);
      compared++;
      if (got.ok !== expected.ok) mismatches.push(`${t}/${key}: model ${got.ok ? "answered" : "threw"}, fold on read ${expected.ok ? "answered" : "threw"}`);
      else if (got.ok && expected.ok && JSON.stringify(got.state) !== JSON.stringify(expected.state)) mismatches.push(`${t}/${key}: ${JSON.stringify(got.state)} ≠ ${JSON.stringify(expected.state)}`);
    }
  }
  return { compared, mismatches };
}

/** The tenant's keys that have a source event and fold without throwing: what its `list` must hold. */
async function heldKeys(app: EventStoreApi, tenant: string, def: ReadModelDefinition<Offer>): Promise<string[]> {
  const held: string[] = [];
  for (const key of KEYS) {
    const all = await app.forTenant(tenant).query(keyQuery(def.source, def.keyBy, key));
    const r = await onRead(app.forTenant(tenant), key, def);
    if (all.events.length > 0 && r.ok) held.push(key);
  }
  return held;
}

export function tenantsSuite(h: TenantsHarness, options: { readonly numRuns: number }): void {
  it("property: every tenant's reader get(k) equals that tenant's context(source ∧ offerId = k) without waiting, whatever the processor's lag; list({ atLeast }) holds exactly the tenant's keys", async () => {
    const event = fc.oneof(
      { weight: 6, arbitrary: fc.record({ kind: fc.constant("made" as const), key: fc.constantFrom(...KEYS), amount: fc.integer({ min: -20, max: 50 }) }) },
      { weight: 1, arbitrary: fc.record({ kind: fc.constant("noted" as const), key: fc.constantFrom(...KEYS) }) },
      { weight: 1, arbitrary: fc.constant({ kind: "noted-no-key" as const }) },
      { weight: 1, arbitrary: fc.record({ kind: fc.constant("audited" as const), key: fc.constantFrom(...KEYS) }) },
    );
    let mismatches: string[] = [];
    let comparisons = 0;
    let lagging = 0;
    let bothThrew = 0;
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(1, 3, 500),
        fc.integer({ min: 3, max: 5 }),
        fc.array(fc.array(fc.record({ tenant: fc.nat({ max: 4 }), events: fc.array(event, { minLength: 1, maxLength: 4 }) }), { minLength: 1, maxLength: 3 }), { minLength: 1, maxLength: 5 }),
        fc.option(fc.nat({ max: 10 }), { nil: undefined }),
        async (batchSize, tenantCount, steps, poisonAt) => {
          await h.reset();
          const tenants = TENANTS.slice(0, tenantCount);
          const app = h.open();
          const proc = h.processor();
          const def = crossModel(`xprop-${batchSize}`, { batchSize });
          const processor = proc.persistedReadModel(def);
          const readers = tenants.map((t) => app.forTenant(t).persistedReadModel(def));
          let madeCount = 0;
          const last = new Map<string, Cursor>();
          const toEvent = (e: { kind: string; key?: string; amount?: number }): NewEvent => {
            if (e.kind === "made") return made(e.key!, e.amount!, "open", madeCount++ === poisonAt);
            if (e.kind === "noted") return noted(e.key);
            if (e.kind === "noted-no-key") return noted();
            return audited(e.key!);
          };
          for (const appends of steps) {
            for (const { tenant, events } of appends) {
              const t = tenants[tenant % tenants.length]!;
              const appended = await app.forTenant(t).append(events.map(toEvent));
              const before = last.get(t);
              if (!before || compareCursor(tokenOf(appended), before) > 0) last.set(t, tokenOf(appended));
            }
            const checkpoint = (await processor.status()).checkpoint;
            const newest = [...last.values()].reduce<Cursor | null>((a, b) => (a === null || compareCursor(b, a) > 0 ? b : a), null);
            if (checkpoint === null || (newest !== null && compareCursor(checkpoint, newest) < 0)) lagging++;
            // no wait at all: a reader's get is current by its tenant row + the tenant's delta
            for (const t of tenants) {
              const reader = app.forTenant(t).persistedReadModel(def);
              for (const key of [...KEYS, "never"]) {
                const expected = await onRead(app.forTenant(t), key, def);
                const got = await fromModel(reader, key);
                comparisons++;
                if (got.ok !== expected.ok) mismatches.push(`${t}/${key}: model ${got.ok ? "answered" : "threw"}`);
                else if (got.ok && expected.ok) {
                  if (JSON.stringify(got.state) !== JSON.stringify(expected.state)) mismatches.push(`${t}/${key}: ${JSON.stringify(got.state)} ≠ ${JSON.stringify(expected.state)}`);
                } else bothThrew++;
              }
            }
          }
          // once the processor passed each tenant's last token: list holds exactly that tenant's keys, with fold on read's states
          for (const [t, token] of last) {
            const listed = await app.forTenant(t).persistedReadModel(def).list({ atLeast: token, limit: 1000 });
            expect(listed.rows.map((row) => row.key)).toEqual(await heldKeys(app, t, def));
            for (const row of listed.rows) expect(row.state).toEqual(((await onRead(app.forTenant(t), row.key, def)) as { state: Offer }).state);
          }
          for (const reader of readers) expect((await reader.status()).role).toBe("standby");
          expect((await processor.status()).role).toBe("writer");
          // a warm reader get folds nothing
          const warm = readers[0]!;
          for (const key of KEYS) await warm.get(key).catch(() => undefined);
          const before = (await warm.status()).readFolded;
          for (let i = 0; i < 100; i++) await warm.get(KEYS[i % KEYS.length]!).catch(() => undefined);
          expect((await warm.status()).readFolded).toBe(before);
          await h.closeAll();
        },
      ),
      { numRuns: options.numRuns },
    );
    console.log(`cross-tenant property: ${comparisons} comparisons, ${mismatches.length} mismatches, ${lagging} steps with the processor behind the last append, ${bothThrew} reads where both threw`);
    expect(mismatches).toEqual([]);
    expect(comparisons).toBeGreaterThan(0);
    expect(lagging + bothThrew).toBeGreaterThan(0);
    mismatches = [];
  }, 120_000);

  it("exactly once per tenant beside a per-tenant writer of the same name and version (a mixed deploy): every key equals context, every source event counted once", async () => {
    await h.reset();
    const app = h.open();
    const old = h.open();
    const proc = h.processor();
    const def = crossModel("mixed");
    const perTenant = defineReadModel<Offer>({ ...offersModel, name: "mixed" }); // the old code: no processor
    const processor = proc.persistedReadModel(def);
    const oldWriter = old.forTenant("ta").persistedReadModel(perTenant);
    let lastA: Cursor | undefined;
    let lastB: Cursor | undefined;
    for (let i = 0; i < 150; i++) {
      lastA = tokenOf(await app.forTenant("ta").append([made(KEYS[i % 5]!, i), noted(KEYS[(i + 1) % 5])]));
      lastB = tokenOf(await app.forTenant("tb").append([made(KEYS[(i + 2) % 5]!, 1)]));
    }
    expect((await oldWriter.status()).role).toBe("writer");
    await app.forTenant("ta").persistedReadModel(def).list({ atLeast: lastA!, timeoutMs: 20_000 });
    await app.forTenant("tb").persistedReadModel(def).list({ atLeast: lastB!, timeoutMs: 20_000 });
    const { mismatches, compared } = await compareAll(app, def, ["ta", "tb"]);
    expect(mismatches).toEqual([]);
    expect(compared).toBe(12);
    for (const t of ["ta", "tb"]) {
      const listed = await app.forTenant(t).persistedReadModel(def).list({ limit: 100 });
      const applied = listed.rows.reduce((n, r) => n + r.state.n + r.state.notes, 0);
      const events = (await app.forTenant(t).query(source)).events.filter((e) => typeof e.scopes.offerId === "string").length;
      expect(applied, `tenant ${t}: events applied vs source events with a key`).toBe(events);
    }
    expect((await processor.status()).role).toBe("writer");
    await h.closeAll();
  }, 60_000);

  it("poison across tenants: tenant A's key fails with the tenant on its FailedKeyError, B's same key value stays healthy, later pages are applied", async () => {
    await h.reset();
    const seen: FailedKeyError[] = [];
    const app = h.open();
    const proc = h.processor();
    const def = crossModel("poison", { onFailedKey: (error: FailedKeyError) => void seen.push(error) });
    proc.persistedReadModel(def);
    await app.forTenant("ta").append([made("o1", 1), made("o2", 2)]);
    await app.forTenant("tb").append([made("o1", 10)]);
    await app.forTenant("ta").append([made("o1", 3, "open", true)]);
    const lastB = tokenOf(await app.forTenant("tb").append([made("o1", 20), made("o2", 30)]));
    const lastA = tokenOf(await app.forTenant("ta").append([made("o2", 4)]));
    const ra = app.forTenant("ta").persistedReadModel(def);
    const rb = app.forTenant("tb").persistedReadModel(def);
    await ra.list({ atLeast: lastA });
    await rb.list({ atLeast: lastB });
    const failed = await ra.get("o1").catch((e: unknown) => e);
    expect(failed).toBeInstanceOf(FailedKeyError);
    expect((await ra.list()).failed).toBe(1);
    expect((await ra.get("o2")).total).toBe(6);
    expect((await rb.get("o1")).total).toBe(30);
    expect((await rb.list()).failed).toBe(0);
    await until(() => seen.length === 1);
    expect(seen[0]!.tenant).toBe("ta");
    expect(seen[0]!.key).toBe("o1");
    expect(seen[0]!.message).toMatch(/key "o1" of tenant ta failed/);
    const { mismatches } = await compareAll(app, def, ["ta", "tb"]);
    expect(mismatches).toEqual([]);
    await wait(100);
    expect(seen).toHaveLength(1);
    await h.closeAll();
  }, 30_000);

  it("one processor at a time: a second processor runtime is a standby; after the writer stops it resumes at the processor row's checkpoint and folds only what came after", async () => {
    await h.reset();
    const app = h.open();
    const p1 = h.processor();
    const p2 = h.processor();
    (await knobsOf(p2)).tickMs = 100;
    const def = crossModel("takeover");
    const first = p1.persistedReadModel(def);
    await until(async () => (await first.status()).role === "writer");
    const second = p2.persistedReadModel(def);
    let last: Cursor | undefined;
    for (let i = 0; i < 20; i++) last = tokenOf(await app.forTenant(TENANTS[i % 3]!).append([made(KEYS[i % 5]!, i)]));
    await app.forTenant(TENANTS[19 % 3]!).persistedReadModel(def).list({ atLeast: last! });
    await wait(300);
    expect((await second.status()).role).toBe("standby");
    expect((await first.status()).folded).toBe(20);
    await first.stop();
    for (let i = 0; i < 7; i++) last = tokenOf(await app.forTenant(TENANTS[i % 3]!).append([made(KEYS[i % 5]!, 1), noted(KEYS[i % 5])]));
    await until(async () => (await second.status()).role === "writer", 10_000);
    await app.forTenant(TENANTS[6 % 3]!).persistedReadModel(def).list({ atLeast: last!, timeoutMs: 10_000 });
    expect((await second.status()).folded).toBe(14);
    const { mismatches } = await compareAll(app, def, TENANTS.slice(0, 3));
    expect(mismatches).toEqual([]);
    await h.closeAll();
  }, 30_000);

  it("blue-green across tenants: v2 builds beside v1, readers' v2 list waits, then every tenant switches — also one without an event in v2's source, which answers an empty page", async () => {
    await h.reset();
    const app = h.open();
    const proc1 = h.processor();
    const v1 = crossModel("bg");
    proc1.persistedReadModel(v1);
    await app.forTenant("ta").append([made("o1", 1), made("o2", 2)]);
    await app.forTenant("tb").append([made("o1", 5)]);
    const lastC = tokenOf(await app.forTenant("tc").append([noted("o1"), noted("o3")])); // C: no OfferMade at all
    await app.forTenant("tc").persistedReadModel(v1).list({ atLeast: lastC });
    expect((await app.forTenant("tc").persistedReadModel(v1).list()).total).toBe(2);
    // v2: another fold, a source that C has no event in
    const v2 = defineReadModel<Offer>({ ...v1, version: 2, source: offers.$filter({ types: ["OfferMade"] }), fold: (events, s) => offerFold(events, { ...s, status: "v2" }) });
    let gate: (() => void) | undefined;
    const held = new Promise<void>((resolve) => (gate = resolve));
    const proc2 = h.processor();
    (await knobsOf(proc2)).beforePage = () => held; // every processor page of this store waits until the gate opens
    const p2 = proc2.persistedReadModel(v2);
    await until(async () => (await p2.status()).role === "writer");
    expect((await p2.status()).building).toBe(true);
    for (const t of ["ta", "tb", "tc"]) {
      const error = await app.forTenant(t).persistedReadModel(v2).list({ timeoutMs: 50 }).catch((e: unknown) => e);
      expect(error).toBeInstanceOf(TransientError);
      expect((error as TransientError).code).toBe("read_model_timeout");
      expect((await app.forTenant(t).persistedReadModel(v1).list()).total).toBeGreaterThan(0); // v1 keeps answering
    }
    gate!();
    (await knobsOf(proc2)).beforePage = undefined;
    await until(async () => !(await p2.status()).building, 10_000);
    expect((await app.forTenant("ta").persistedReadModel(v2).list({ timeoutMs: 5_000 })).total).toBe(2);
    expect((await app.forTenant("tb").persistedReadModel(v2).list({ timeoutMs: 5_000 })).total).toBe(1);
    expect(await app.forTenant("tc").persistedReadModel(v2).list({ timeoutMs: 5_000 })).toMatchObject({ total: 0, rows: [] });
    const { mismatches } = await compareAll(app, v2, ["ta", "tb", "tc"]);
    expect(mismatches).toEqual([]);
    await h.closeAll();
  }, 30_000);

  it("migration: a per-tenant v1 (tenant writers) stays answering while a cross-tenant v2 builds in every tenant, and all tenants switch together once the processor passed the switch token", async () => {
    await h.reset();
    const app = h.open();
    const v1 = defineReadModel<Offer>({ ...offersModel, name: "mig" });
    const a1 = app.forTenant("ta").persistedReadModel(v1);
    const b1 = app.forTenant("tb").persistedReadModel(v1);
    await a1.list({ atLeast: tokenOf(await app.forTenant("ta").append([made("o1", 1)])) });
    await b1.list({ atLeast: tokenOf(await app.forTenant("tb").append([made("o2", 2)])) });
    const v2 = crossModel("mig", { version: 2 });
    let gate: (() => void) | undefined;
    const held = new Promise<void>((resolve) => (gate = resolve));
    const proc = h.processor();
    (await knobsOf(proc)).beforePage = () => held;
    const p2 = proc.persistedReadModel(v2);
    await until(async () => (await p2.status()).role === "writer");
    expect((await p2.status()).building).toBe(true); // v1 is active in tenant rows: v2 builds, not active at once
    for (const t of ["ta", "tb"]) {
      await expect(app.forTenant(t).persistedReadModel(v2).list({ timeoutMs: 50 })).rejects.toThrow(/still building/);
    }
    expect((await a1.list()).total).toBe(1);
    gate!();
    (await knobsOf(proc)).beforePage = undefined;
    await until(async () => !(await p2.status()).building, 10_000);
    expect((await app.forTenant("ta").persistedReadModel(v2).list()).rows.map((r) => r.key)).toEqual(["o1"]);
    expect((await app.forTenant("tb").persistedReadModel(v2).list()).rows.map((r) => r.key)).toEqual(["o2"]);
    await h.closeAll();
  }, 30_000);

  it("watch through the processor: a reader's watch in A receives A's new state; B's watcher of the same key value receives nothing; watchList in A gets A's rows only", async () => {
    await h.reset();
    const app = h.open();
    const other = h.open();
    const proc = h.processor();
    const def = crossModel("watched");
    proc.persistedReadModel(def);
    await app.forTenant("ta").persistedReadModel(def).list({ atLeast: tokenOf(await app.forTenant("ta").append([made("o1", 1)])) });
    await app.forTenant("tb").persistedReadModel(def).list({ atLeast: tokenOf(await app.forTenant("tb").append([made("o1", 100)])) });
    const seenA: number[] = [];
    const seenB: number[] = [];
    const listA: string[][] = [];
    await other.forTenant("ta").persistedReadModel(def).watch("o1", (s) => void seenA.push(s.total));
    await other.forTenant("tb").persistedReadModel(def).watch("o1", (s) => void seenB.push(s.total));
    await other.forTenant("ta").persistedReadModel(def).watchList({}, (c) => void listA.push(c.rows.map((r) => `${r.key}:${r.state.total}`)));
    await app.forTenant("ta").append([made("o1", 2), made("o2", 7)]);
    await until(() => seenA.length === 2, 2_000);
    expect(seenA).toEqual([1, 3]);
    await until(() => listA.length >= 1, 2_000);
    expect(listA.flat().sort()).toEqual(["o1:3", "o2:7"]);
    await wait(300);
    expect(seenB).toEqual([100]);
    await h.closeAll();
  }, 20_000);

  it("the processor handle has no reads; a past view refuses", async () => {
    await h.reset();
    const proc = h.processor();
    const def = crossModel("refusals");
    const processor = proc.persistedReadModel(def);
    await expect(processor.get("o1")).rejects.toThrow(/a cross-tenant model is read per tenant: es\.forTenant\(t\)\.persistedReadModel\(def\)/);
    await expect(processor.list()).rejects.toThrow(UsageError);
    await expect(processor.watch("o1", () => undefined)).rejects.toThrow(UsageError);
    await expect(processor.watchList({}, () => undefined)).rejects.toThrow(UsageError);
    expect((await processor.status()).phase).toBe("live");
    expect(() => proc.asOf(1).persistedReadModel(def)).toThrow(/past view/);
    await h.closeAll();
  });
}
