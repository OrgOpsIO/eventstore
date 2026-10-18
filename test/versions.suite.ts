/**
 * Versions and rebuild (stage 5, decisions 38–43), specified once and run against the memory store
 * and Postgres (versions-memory.test.ts, versions-postgres.test.ts). A "process" is a fresh runtime
 * over the same store data. v1 → v2 is a bump of the offers model (versions.model.ts).
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import {
  compareCursor,
  defineReadModel,
  FailedKeyError,
  TransientError,
  UsageError,
  type Cursor,
  type EventStoreApi,
  type EventStoreConfig,
  type Fold,
  type NewEvent,
  type PersistedReadModel,
  type ReadModelDefinition,
} from "../src/index.js";
import { resetFingerprintWarnings } from "../src/fingerprint.js";
import type { ModelRef, ReadModelStore } from "../src/modelstore.js";
import { block, capOf, onRead, until, wait } from "./persisted-model.suite.js";
import { audited, keysOf, made, noted, offerFoldV2, offersV1, offersV2, offersV2Changed, offersV3, source, tokenOf, type Offer } from "./versions.model.js";

export interface VersionsHarness {
  /** Empty events and model tables for the next test. */
  reset(): Promise<void>;
  /** A new "process": a runtime over the same data (`persistedModels: true`). Closed by `closeAll`. */
  open(options?: Partial<EventStoreConfig>): EventStoreApi;
  /** The README's retire recipe (Postgres, as printed) / the memory hook behind the same condition: versions of `name` below `newVersion`, once `newVersion` is active. */
  retire(name: string, newVersion: number): Promise<void>;
  /** State rows of one version (held and failed), whatever its status. */
  stateRows(name: string, version: number): Promise<number>;
  /** Make one stored state row undecodable (no writer may hold the model). */
  corrupt(api: EventStoreApi, ref: ModelRef, key: string): Promise<void>;
  /** Events v1 is built over in the blue-green test (memory 500, Postgres 5 000). */
  readonly big: number;
  /** Concurrent appenders with connections of their own (Postgres: separate pools). */
  readonly writers: number;
  closeAll(): Promise<void>;
}

/** Fold on read with a definition's fold: the state, or the error. */
const contextOf = <S = Offer>(api: EventStoreApi, def: ReadModelDefinition<S>, key: string) => onRead<S>(api, key, def);

async function settle<T>(p: Promise<T>): Promise<{ ok: true; value: T } | { ok: false; error: unknown }> {
  try {
    return { ok: true, value: await p };
  } catch (error) {
    return { ok: false, error };
  }
}

/**
 * Whether `read` (a model's `get`, always current) equals the key's fold on read taken just before or just after it:
 * under a stream of appends an append may land between the two reads, so the comparison is bracketed.
 */
async function bracketed<S>(api: EventStoreApi, def: ReadModelDefinition<S>, key: string, read: () => Promise<S>): Promise<boolean> {
  const before = await contextOf(api, def, key);
  const got = await settle(read());
  const after = await contextOf(api, def, key);
  if (!got.ok) return false;
  const value = JSON.stringify(got.value);
  return [before, after].some((want) => want.ok && JSON.stringify(want.state) === value);
}

/** Until the model has switched (or `ms` passed); returns how long it took. */
async function switched<S>(model: PersistedReadModel<S>, ms = 60_000): Promise<number> {
  const started = Date.now();
  await until(async () => !(await model.status()).building, ms);
  return Date.now() - started;
}

/** Append `n` OfferMade events over `keys`, in appends of at most 500. */
async function seed(api: EventStoreApi, n: number, keys: readonly string[]): Promise<Cursor> {
  let last: Cursor | undefined;
  for (let i = 0; i < n; i += 500) {
    const events = Array.from({ length: Math.min(500, n - i) }, (_, j) => made(keys[(i + j) % keys.length]!, ((i + j) % 7) + 1));
    last = tokenOf(await api.append(events));
  }
  return last!;
}

/** Warnings with code ES_FOLD_CHANGED while `run` runs (the once-per-process memory reset first). */
async function foldWarnings(run: () => Promise<void>): Promise<string[]> {
  resetFingerprintWarnings();
  const seen: string[] = [];
  const listener = (warning: Error & { code?: string }) => {
    // only the model under test: a warning an earlier test gave may still be on its way to the listeners
    if (warning.code === "ES_FOLD_CHANGED" && warning.message.includes('"offers" v2')) seen.push(`${warning.name}: ${warning.message}`);
  };
  process.on("warning", listener);
  try {
    await run();
    await wait(20); // emitWarning delivers on the next tick
  } finally {
    process.off("warning", listener);
  }
  return seen;
}

/** Make a capability claim nothing: the handle stays a standby, its version keeps no model row. Returns the undo. */
function noClaims(cap: ReadModelStore): () => void {
  const lock = cap.lock;
  cap.lock = async () => null;
  return () => {
    cap.lock = lock;
  };
}

export function versionsSuite(h: VersionsHarness, options: { readonly numRuns: number }): void {
  it("creation status: a first version is active from the start; v2 beside an active v1 is building, before its writer claimed and after; v3 after v1 and v2 are retired is active", async () => {
    await h.reset();
    const a = h.open();
    const t0 = await a.append([made("o1", 1), noted("o1")]);
    const v1 = a.persistedReadModel(offersV1);
    expect((await v1.status()).building).toBe(false);
    expect((await v1.list({ atLeast: tokenOf(t0) })).total).toBe(1); // stage 4 unchanged: a first build answers list
    const b = h.open();
    const undo = noClaims(await capOf(b));
    try {
      const v2 = b.persistedReadModel(offersV2);
      await wait(50);
      const before = await v2.status();
      expect(before).toMatchObject({ role: "standby", building: true, checkpoint: null, epoch: 0 }); // no row yet
      await expect(v2.list({ timeoutMs: 30 })).rejects.toThrow(/still building beside an active version/);
      undo(); // the next tick claims
      await until(async () => (await v2.status()).epoch > 0, 10_000);
      await switched(v2);
      expect((await v2.list({ atLeast: tokenOf(t0) })).rows.map((r) => r.state.total)).toEqual([2]);
      await h.retire("offers", 2);
      const v3 = b.persistedReadModel(offersV3);
      expect((await v3.status()).building).toBe(true); // v2 is active
      await switched(v3);
      await h.retire("offers", 3);
      // with nothing active below or beside it, a fresh name's first version is active, and so is a version after a retirement of all others
      const c = h.open();
      const solo = c.persistedReadModel(defineReadModel({ ...offersV2, name: "solo" }));
      expect((await solo.status()).building).toBe(false);
    } finally {
      undo();
      await h.closeAll();
    }
  }, 60_000);

  it("blue-green under a stream of appends: v1 answers get, list and watch throughout, v2's get equals its fold on read at every sample, v2's list waits, v2 switches, then lists every key as fold on read gives it", async () => {
    await h.reset();
    const keys = keysOf(50);
    const a = h.open();
    const b = h.open();
    const appender = h.open();
    for (const api of [a, b]) (await capOf(api)).knobs.tickMs = 50;
    let last = await seed(appender, h.big, keys);
    const v1 = a.persistedReadModel(offersV1);
    await v1.list({ atLeast: last, timeoutMs: 60_000 });
    const v1Seen: Offer[] = [];
    await v1.watch("k1", (s) => void v1Seen.push(s));
    // a steady stream: one append every 5 ms, until the switch is checked
    let streaming = true;
    let appended = 0;
    const stream = (async () => {
      while (streaming) {
        last = tokenOf(await appender.append([made(keys[appended++ % keys.length]!, 1)]));
        await wait(5);
      }
    })();
    // v2 opened in a second runtime; its writer is held off first, so the build window is certain
    const release = await block(b, offersV2);
    const v2 = b.persistedReadModel(offersV2);
    await wait(60);
    expect((await v2.status()).building).toBe(true);
    const timeout = await v2.list({ timeoutMs: 50 }).catch((e: unknown) => e);
    expect(timeout).toBeInstanceOf(TransientError);
    expect((timeout as TransientError).code).toBe("read_model_timeout");
    expect((timeout as Error).message).toMatch(/v2 is still building beside an active version \(blue-green\)/);
    let samples = 0;
    let mismatches = 0;
    const sample = async () => {
      for (const k of ["k0", "k7", "k33"]) {
        samples++;
        if (!(await bracketed(b, offersV2, k, () => v2.get(k)))) mismatches++;
      }
      // v1 keeps answering: list as current as the latest token, get equal to its fold on read
      expect((await v1.list({ atLeast: last, limit: 100 })).total).toBe(50);
      expect(await bracketed(a, offersV1, "k5", () => v1.get("k5"))).toBe(true);
    };
    try {
      await sample();
      await release(); // v2's writer claims at its next tick and builds
      const opened = Date.now();
      let lagAtSwitch = -1;
      while ((await v2.status()).building && Date.now() - opened < 60_000) {
        await sample();
        await wait(20);
      }
      const status = await v2.status();
      lagAtSwitch = status.lag.events;
      expect(status.building).toBe(false);
      console.log(`[versions] v2 switched ${Date.now() - opened} ms after its writer was released; lag.events at the switch ${lagAtSwitch}; ${samples} get samples, ${mismatches} mismatches; ${appended} appends streamed so far`);
      expect(mismatches).toBe(0);
    } finally {
      // also on a failure: no append may outlive the test into closed pools
      streaming = false;
      await stream;
    }
    const page = await v2.list({ atLeast: last, limit: 100 });
    expect(page.total).toBe(50);
    for (const row of page.rows) expect(row.state).toEqual(((await contextOf(b, offersV2, row.key)) as { state: Offer }).state);
    // v1 is untouched by the switch: still active, still current, its watcher still fed
    expect((await v1.status()).building).toBe(false);
    expect((await v1.list({ atLeast: last, limit: 100 })).total).toBe(50);
    expect(v1Seen.length).toBeGreaterThan(1);
    await h.closeAll();
  }, 120_000);

  it("a building version's get of a key whose events beyond its checkpoint exceed maxEvents pages its delta (O24 b): it answers while building, equals fold on read, never folds more than maxEvents events at once; v1 answers the key, after the switch v2 does", async () => {
    await h.reset();
    const max = { contextCache: { maxEvents: 50 } };
    const a = h.open(max);
    for (let i = 0; i < 3; i++) await a.append(Array.from({ length: 40 }, () => made("big", 1)));
    const last = tokenOf(await a.append([made("small", 1)]));
    const v1 = a.persistedReadModel(offersV1);
    await v1.list({ atLeast: last, timeoutMs: 30_000 });
    expect((await v1.get("big")).n).toBe(120); // its row plus an empty delta: within maxEvents
    const b = h.open(max);
    const unbounded = h.open(); // fold on read with the default maxEvents
    let widest = 0; // the most events one fold call was given: one page at most, so memory stays bounded by maxEvents
    const paged = defineReadModel({
      ...offersV2,
      fold: (events, state) => {
        widest = Math.max(widest, events.length);
        return offerFoldV2(events, state);
      },
    });
    const release = await block(b, offersV2); // v2 stays building with no rows and no checkpoint
    try {
      const v2 = b.persistedReadModel(paged);
      expect(await v2.status()).toMatchObject({ building: true, checkpoint: null });
      const small = await v2.get("small");
      expect(small).toEqual(((await contextOf(b, offersV2, "small")) as { state: Offer }).state);
      const before = (await v2.status()).readFolded;
      const big = await v2.get("big"); // 120 events beyond no checkpoint: three pages of at most 50
      expect(big).toEqual(((await contextOf(unbounded, offersV2, "big")) as { state: Offer }).state);
      expect(big.n).toBe(120);
      expect((await v2.status()).readFolded - before).toBe(120);
      expect(widest).toBeGreaterThan(0);
      expect(widest).toBeLessThanOrEqual(50);
      expect((await v1.get("big")).n).toBe(120); // the old version answers the key in old-code processes
      // a second delta over maxEvents, now from the kept copy: paged again, folding only the new events
      await a.append(Array.from({ length: 60 }, () => made("big", 1)));
      const again = await v2.get("big");
      expect(again).toEqual(((await contextOf(unbounded, offersV2, "big")) as { state: Offer }).state);
      expect(again.n).toBe(180);
      expect((await v2.status()).readFolded - before).toBe(180);
      expect(widest).toBeLessThanOrEqual(50);
      expect(await v2.status()).toMatchObject({ building: true, checkpoint: null });
    } finally {
      await release();
    }
    const v2 = b.persistedReadModel(paged); // the same handle, now free to claim and build
    await switched(v2);
    expect(await v2.get("big")).toEqual(((await contextOf(unbounded, offersV2, "big")) as { state: Offer }).state);
    expect((await v2.get("big")).total).toBe(360);
    await h.closeAll();
  }, 60_000);

  it("switch only when caught up: activate is called once, after the feed passed T, also across a takeover of the building writer", async () => {
    await h.reset();
    const a = h.open();
    const t = await seed(a, 300, keysOf(10));
    const v1 = a.persistedReadModel(offersV1);
    await v1.list({ atLeast: t });
    const slowV2 = defineReadModel({ ...offersV2, batchSize: 1 });
    const T = t; // nothing is appended from here on: T is the newest source event when v2 begins building
    const calls: { checkpoint: Cursor | null }[] = [];
    const spy = async (api: EventStoreApi) => {
      const cap = await capOf(api);
      cap.knobs.tickMs = 50;
      if ((cap.activate as { spied?: true }).spied) return; // the memory store's capability is one object for every runtime
      const activate = cap.activate;
      const wrapped = async (ref: ModelRef, epoch: number) => {
        calls.push({ checkpoint: (await cap.head(ref))?.cursor ?? null });
        return activate(ref, epoch);
      };
      cap.activate = Object.assign(wrapped, { spied: true as const });
      // every page a little slower, so the takeover lands half-way on the memory store too
      const page = cap.page;
      cap.page = async (ref, epoch, fn) => {
        await wait(1);
        return page(ref, epoch, fn);
      };
    };
    const b = h.open();
    const c = h.open();
    await spy(b);
    await spy(c);
    const vb = b.persistedReadModel(slowV2);
    // the invariant, sampled: below T it is building and its list never answers
    const listed = settle(vb.list({ timeoutMs: 20 }));
    let below = 0;
    await until(async () => {
      const s = await vb.status();
      if (s.checkpoint === null || compareCursor(s.checkpoint, T) < 0) {
        below++;
        expect(s.building).toBe(true);
      }
      return s.checkpoint !== null && s.checkpoint.sequence >= 100;
    }, 30_000);
    const early = await listed;
    if (early.ok) expect(compareCursor((await vb.status()).checkpoint!, T)).toBeGreaterThanOrEqual(0);
    expect(below).toBeGreaterThan(0);
    // a takeover half-way: the building writer's process stops; the standby in c takes over with a new T and switches once
    const vc = c.persistedReadModel(slowV2);
    await b.close();
    await switched(vc);
    expect(calls.length).toBe(1);
    expect(compareCursor(calls[0]!.checkpoint!, T)).toBeGreaterThanOrEqual(0);
    expect((await vc.list({ atLeast: T })).total).toBe(10);
    await h.closeAll();
  }, 90_000);

  it("watch and watchList on a building version wait for the switch; then the first delivery is v2's fold on read, and list changes come only after the switch's revision", async () => {
    await h.reset();
    const a = h.open();
    const b = h.open();
    for (const api of [a, b]) (await capOf(api)).knobs.tickMs = 50;
    const t = await seed(a, 40, keysOf(4));
    await a.persistedReadModel(offersV1).list({ atLeast: t });
    const release = await block(b, offersV2);
    const v2 = b.persistedReadModel(offersV2);
    const states: Offer[] = [];
    const changes: { revision: number; keys: string[] }[] = [];
    let keyResolved = false;
    let listResolved = false;
    const keyWatch = v2.watch("k1", (s) => void states.push(s)).then((w) => ((keyResolved = true), w));
    const listWatch = v2.watchList({}, (c) => void changes.push({ revision: c.revision, keys: c.rows.map((r) => r.key) })).then((w) => ((listResolved = true), w));
    const aborter = new AbortController();
    const aborted = v2.watch("k2", () => undefined, { signal: aborter.signal });
    await wait(150);
    expect((await v2.status()).building).toBe(true);
    expect(keyResolved).toBe(false);
    expect(listResolved).toBe(false);
    aborter.abort(new Error("the request closed"));
    await expect(aborted).rejects.toThrow("the request closed");
    await release();
    const watch = await keyWatch;
    const feed = await listWatch;
    const switchRevision = (await v2.status()).revision;
    expect((await v2.status()).building).toBe(false);
    expect(states[0]).toEqual(((await contextOf(b, offersV2, "k1")) as { state: Offer }).state);
    expect(changes).toEqual([]); // no build page reached the list watcher
    // decision 24's handoff: registered first, then list, then newer revisions
    const page = await v2.list({ limit: 10 });
    const next = await a.append([made("k1", 100), made("k9", 5)]);
    await v2.list({ atLeast: tokenOf(next) });
    await until(() => changes.length > 0 && states.length > 1);
    for (const change of changes) expect(change.revision).toBeGreaterThan(switchRevision);
    const merged = new Map(page.rows.map((r) => [r.key, r.state]));
    for (const change of changes) if (change.revision > page.revision) for (const key of change.keys) merged.set(key, ((await contextOf(b, offersV2, key)) as { state: Offer }).state);
    const fresh = await v2.list({ limit: 10 });
    expect([...merged.keys()].sort()).toEqual(fresh.rows.map((r) => r.key).sort());
    expect(states.at(-1)).toEqual(((await contextOf(b, offersV2, "k1")) as { state: Offer }).state);
    watch.stop();
    feed.stop();
    await h.closeAll();
  }, 60_000);

  it("retire: before the switch it changes nothing; after it, v1's writer and standby fail at their next page and claim, v1 is never rebuilt, its list and cold get reject, its watchers end, v2 answers unchanged, a live start on v1 rebuilds", async () => {
    await h.reset();
    const a = h.open(); // v1 writer
    const s = h.open(); // v1 standby
    const b = h.open(); // v2
    for (const api of [a, s, b]) (await capOf(api)).knobs.tickMs = 50;
    const t = await seed(a, 60, keysOf(6));
    const v1 = a.persistedReadModel(offersV1);
    await v1.list({ atLeast: t });
    const standby = s.persistedReadModel(offersV1);
    expect((await standby.status()).role).toBe("standby");
    const release = await block(b, offersV2);
    const v2 = b.persistedReadModel(offersV2);
    // before v2 has switched the recipe changes nothing
    await h.retire("offers", 2);
    expect(await h.stateRows("offers", 1)).toBe(6);
    expect((await v1.list()).total).toBe(6);
    await release();
    await switched(v2);
    const v2Before = await v2.list({ atLeast: t });
    // watchers on both v1 handles: they end with onError when their handle fails — observed without reading the model row
    const ended: unknown[] = [];
    const standbyEnded: unknown[] = [];
    await v1.watch("k1", () => undefined, { onError: (e) => void ended.push(e) });
    await standby.watch("k1", () => undefined, { onError: (e) => void standbyEnded.push(e) });
    await h.retire("offers", 2);
    expect(await h.stateRows("offers", 1)).toBe(0);
    // the writer meets the tombstone at its next page (and writes nothing); then the standby at its next claim
    const t2 = await b.append([made("k1", 1)]);
    await until(() => ended.length === 1, 10_000);
    expect(await h.stateRows("offers", 1)).toBe(0);
    await until(() => standbyEnded.length === 1, 10_000);
    for (const model of [v1, standby]) {
      const status = await model.status();
      expect(status.phase).toBe("failed");
      expect(status.error).toBeInstanceOf(UsageError);
      expect((status.error as Error).message).toMatch(/persisted read model "offers" v1 was retired \(a newer version replaced it and its rows were deleted\)/);
    }
    // never rebuilt: 3 ticks with appends in between, still 0 rows
    for (let i = 0; i < 3; i++) {
      await b.append([made("k2", 1)]);
      await wait(60);
      expect(await h.stateRows("offers", 1)).toBe(0);
    }
    // a fresh v1 handle: list rejects (never total 0), a cold get rejects
    const c = h.open();
    const late = c.persistedReadModel(offersV1);
    await expect(late.list()).rejects.toThrow(/was retired/);
    const d = h.open();
    await expect(d.persistedReadModel(offersV1).get("k1")).rejects.toThrow(/was retired/);
    // v2 unchanged, and current
    await v2.list({ atLeast: tokenOf(t2) });
    expect((await v2.list()).total).toBe(v2Before.total);
    expect(await v2.get("k1")).toEqual(((await contextOf(b, offersV2, "k1")) as { state: Offer }).state);
    // a live model on the retired version rebuilds
    const e = h.open();
    const live = e.readModel(offersV1, { start: "persisted" });
    const all = await e.query(source);
    await live.get("k1", { atLeast: tokenOf(await e.append([noted("k1")])) });
    expect(live.status.folded).toBe(all.events.filter((ev) => typeof ev.scopes.offerId === "string").length + 1);
    expect(await live.get("k1")).toEqual(((await contextOf(e, offersV1, "k1")) as { state: Offer }).state);
    await h.closeAll();
  }, 60_000);

  it("property: a live model with start: 'persisted' equals fold on read for every key (or both throw), folds only what is beyond the snapshot, carries failed keys without onFailedKey", async () => {
    const KEYS = ["o1", "o2", "o3", "o4"];
    const event = fc.oneof(
      { weight: 6, arbitrary: fc.record({ kind: fc.constant("made" as const), key: fc.constantFrom(...KEYS), amount: fc.integer({ min: -9, max: 30 }) }) },
      { weight: 1, arbitrary: fc.record({ kind: fc.constant("noted" as const), key: fc.constantFrom(...KEYS) }) },
      { weight: 1, arbitrary: fc.constant({ kind: "noted-no-key" as const }) },
      { weight: 1, arbitrary: fc.record({ kind: fc.constant("audited" as const), key: fc.constantFrom(...KEYS) }) },
    );
    let mismatches = 0;
    let comparisons = 0;
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(1, 3, 500),
        fc.array(fc.array(event, { minLength: 1, maxLength: 4 }), { minLength: 1, maxLength: 6 }),
        fc.option(fc.nat({ max: 10 }), { nil: undefined }),
        fc.nat({ max: 6 }),
        fc.boolean(),
        async (batchSize, appends, poisonAt, startAt, writerRunning) => {
          await h.reset();
          let madeCount = 0;
          const toEvent = (e: { kind: string; key?: string; amount?: number }): NewEvent => {
            if (e.kind === "made") return made(e.key!, e.amount!, "open", madeCount++ === poisonAt);
            if (e.kind === "noted") return noted(e.key);
            if (e.kind === "noted-no-key") return noted();
            return audited(e.key!);
          };
          const failedHook: string[] = [];
          const def = defineReadModel({ ...offersV1, name: `start-${batchSize}`, batchSize, onFailedKey: (err) => void failedHook.push(err.key) });
          const writer = h.open();
          const persisted = writer.persistedReadModel(def);
          let last: Cursor | undefined;
          for (let i = 0; i < appends.length; i++) {
            last = tokenOf(await writer.append(appends[i]!.map(toEvent)));
            if (i === Math.min(startAt, appends.length - 1)) {
              if (!writerRunning) {
                await persisted.list({ atLeast: last });
                await persisted.stop();
              }
              break;
            }
          }
          const head = (await (await capOf(writer)).head({ name: def.name, version: def.version }))?.cursor ?? null;
          // the rest of the schedule after the start point
          const reader = h.open();
          const hooked: string[] = [];
          const liveDef = defineReadModel({ ...def, onFailedKey: (err) => void hooked.push(err.key) });
          const live = reader.readModel(liveDef, { start: "persisted" });
          for (const events of appends.slice(Math.min(startAt, appends.length - 1) + 1)) last = tokenOf(await writer.append(events.map(toEvent)));
          for (const key of [...KEYS, "never"]) {
            const got = await settle(live.get(key, { atLeast: last, timeoutMs: 20_000 }));
            const want = await contextOf(reader, def, key);
            comparisons++;
            const same = got.ok && want.ok ? JSON.stringify(got.value) === JSON.stringify(want.state) : !got.ok && !want.ok;
            if (!same) mismatches++;
            if (!got.ok) expect(got.error).toBeInstanceOf(FailedKeyError);
          }
          // only the delta beyond the snapshot: never more than the routed events beyond the checkpoint read before the start
          const all = await reader.query(source);
          const routed = all.events.filter((e) => typeof e.scopes.offerId === "string" && (head === null || compareCursor(e, head) > 0)).length;
          expect(live.status.folded).toBeLessThanOrEqual(routed);
          if (!writerRunning && poisonAt === undefined) {
            const total = all.events.filter((e) => typeof e.scopes.offerId === "string").length;
            const beyond = all.events.filter((e) => typeof e.scopes.offerId === "string" && head !== null && compareCursor(e, head) > 0).length;
            expect(live.status.folded).toBe(head === null ? total : beyond);
          }
          // failed keys carried over from the rows are not reported again
          for (const key of hooked) expect(failedHook.includes(key) && writerRunning === false).toBe(false);
          await h.closeAll();
        },
      ),
      { numRuns: options.numRuns },
    );
    expect(comparisons).toBeGreaterThan(0);
    expect(mismatches).toBe(0);
  }, 300_000);

  it("live start fallbacks: no row, no checkpoint, a corrupt row each rebuild and equal fold on read; no capability fails; a bad start is a UsageError; a running model is returned as it is", async () => {
    await h.reset();
    const a = h.open();
    const t = await a.append([made("o1", 1), noted("o1"), made("o2", 2), noted(), audited("o1")]);
    const routed = 3; // a noted event without a key is not folded; an audited one is not in the source
    // no model row
    const r1 = h.open().readModel(offersV1, { start: "persisted" });
    await r1.get("o1", { atLeast: tokenOf(t) });
    expect(r1.status.folded).toBe(routed);
    // a model row without a checkpoint: claimed, never paged
    const holder = h.open();
    const release = await block(holder, offersV1);
    const r2 = h.open().readModel(offersV1, { start: "persisted" });
    expect(await r2.get("o1", { atLeast: tokenOf(t) })).toEqual(((await contextOf(a, offersV1, "o1")) as { state: Offer }).state);
    expect(r2.status.folded).toBe(routed);
    await release();
    // a corrupt row: everything loaded is dropped, the model rebuilds
    const writer = h.open();
    const persisted = writer.persistedReadModel(offersV1);
    await persisted.list({ atLeast: tokenOf(t) });
    await persisted.stop();
    await h.corrupt(writer, { name: offersV1.name, version: offersV1.version }, "o2");
    const r3 = h.open().readModel(offersV1, { start: "persisted" });
    for (const k of ["o1", "o2"]) expect(await r3.get(k, { atLeast: tokenOf(t) })).toEqual(((await contextOf(a, offersV1, k)) as { state: Offer }).state);
    expect(r3.status.folded).toBe(routed);
    // the good path for contrast: the rows, and nothing folded
    await h.reset();
    const w = h.open();
    const t2 = await w.append([made("o1", 1), noted("o1"), made("o2", 2)]);
    const p = w.persistedReadModel(offersV1);
    await p.list({ atLeast: tokenOf(t2) });
    const fresh = h.open();
    const r4 = fresh.readModel(offersV1, { start: "persisted" });
    expect(await r4.get("o1", { atLeast: tokenOf(t2) })).toEqual(((await contextOf(w, offersV1, "o1")) as { state: Offer }).state);
    expect(r4.status.folded).toBe(0);
    // a running model is returned whatever its start
    expect(fresh.readModel(offersV1)).toBe(r4);
    expect(fresh.readModel(offersV1, { start: "rebuild" })).toBe(r4);
    expect(() => fresh.readModel(offersV1, { start: "sideways" as never })).toThrow(UsageError);
    expect(() => fresh.readModel(offersV1, { start: "sideways" as never })).toThrow(/start must be "rebuild" \(default\) or "persisted", got "sideways"/);
    await h.closeAll();
  }, 60_000);

  it("fingerprint: a second runtime claiming with another fold under the same version warns once (claim and a standby's first read); the same definition never; rows are unchanged by it; a live start warns once", async () => {
    await h.reset();
    const a = h.open();
    const t = await a.append([made("o1", 2), noted("o1")]);
    const same = await foldWarnings(async () => {
      const v2 = a.persistedReadModel(offersV2);
      await v2.list({ atLeast: tokenOf(t) });
      const b = h.open();
      const again = b.persistedReadModel(offersV2);
      await again.get("o1");
      await again.list();
    });
    expect(same).toEqual([]);
    const rowsBefore = await a.persistedReadModel(offersV2).list();
    const c = h.open();
    const changed = await foldWarnings(async () => {
      const standby = c.persistedReadModel(offersV2Changed);
      await standby.get("o1");
      await standby.list();
      await standby.status();
    });
    expect(changed.length).toBe(1);
    expect(changed[0]).toMatch(/^EventStoreWarning: eventstore: persisted read model "offers" v2: the stored state was written by a different fold \(fingerprint [0-9a-f]{4}…≠[0-9a-f]{4}…\) under the same version — if what the fold computes changed, bump the version/);
    expect(await a.persistedReadModel(offersV2).list()).toEqual(rowsBefore);
    // the writer's claim: a runtime with the changed fold becomes the writer and claims over the stored fingerprint
    await c.close();
    await a.close();
    const claimed = await foldWarnings(async () => {
      const d = h.open();
      (await capOf(d)).knobs.tickMs = 50;
      const writer = d.persistedReadModel(offersV2Changed);
      await until(async () => (await writer.status()).role === "writer", 10_000);
    });
    expect(claimed.length).toBe(1);
    const live = await foldWarnings(async () => {
      const e = h.open();
      const model = e.readModel(offersV2, { start: "persisted" });
      await model.get("o1");
    });
    expect(live.length).toBe(1);
    expect(live[0]).toMatch(/read model "offers" v2/);
    await h.closeAll();
  }, 60_000);

  // ── added by the stage-5 measurer: what goes wrong during a build ─────────

  it("a stream that outpaces the build: v2's lag never reaches 0, yet it switches once its feed passed the newest event of its start; then every key equals fold on read", async () => {
    await h.reset();
    const keys = keysOf(10);
    const a = h.open();
    const b = h.open();
    const appender = h.open();
    for (const api of [a, b]) (await capOf(api)).knobs.tickMs = 50;
    let last = await seed(appender, 40, keys);
    const v1 = a.persistedReadModel(offersV1);
    await v1.list({ atLeast: last, timeoutMs: 60_000 });
    // v2's writer: one event per page and every v2 page 25 ms slower (at most ~40 events/s); the stream below appends faster
    const cap = await capOf(b);
    const page = cap.page;
    cap.page = async (ref, epoch, fn) => {
      if (ref.version === 2) await wait(25);
      return page(ref, epoch, fn);
    };
    const slowV2 = defineReadModel({ ...offersV2, batchSize: 1 });
    let streaming = true;
    let appended = 0;
    const stream = (async () => {
      while (streaming) {
        last = tokenOf(await appender.append([made(keys[appended++ % keys.length]!, 1)]));
        await wait(2);
      }
    })();
    try {
      const before = last; // T is at least this: v2 begins building after it
      const v2 = b.persistedReadModel(slowV2);
      const opened = Date.now();
      let lagAtSwitch = -1;
      let minLagBuilding = Number.POSITIVE_INFINITY;
      await until(async () => {
        const s = await v2.status();
        if (s.building) {
          if (s.checkpoint !== null) minLagBuilding = Math.min(minLagBuilding, s.lag.events);
          return false;
        }
        if (lagAtSwitch < 0) lagAtSwitch = s.lag.events;
        expect(compareCursor(s.checkpoint!, before)).toBeGreaterThanOrEqual(0);
        return true;
      }, 30_000);
      console.log(`[versions/measurer] outpacing stream: v2 switched ${Date.now() - opened} ms after open with lag ${lagAtSwitch}; smallest lag seen while building ${minLagBuilding}; ${appended} appends`);
      // the premise: the stream did outpace the build, so a switch on lag 0 would not have come
      expect(lagAtSwitch).toBeGreaterThan(0);
    } finally {
      streaming = false;
      await stream;
      cap.page = page;
    }
    const listed = await b.persistedReadModel(slowV2).list({ atLeast: last, limit: 100, timeoutMs: 60_000 });
    expect(listed.total).toBe(10);
    for (const row of listed.rows) expect(row.state).toEqual(((await contextOf(b, offersV2, row.key)) as { state: Offer }).state);
    await h.closeAll();
  }, 90_000);

  it("a poison event in the build: v2's fold throws on an event v1 took; v2 fails that key durably and once, applies its neighbours, still switches; v1 keeps answering the key", async () => {
    await h.reset();
    const a = h.open();
    const b = h.open();
    for (const api of [a, b]) (await capOf(api)).knobs.tickMs = 50;
    await seed(a, 50, keysOf(5));
    // the poison is the newest event when v2 begins building: its switch token T, passed only by the skip
    const bad = await a.append([made("k1", 2), made("k3", 13)]);
    const last = tokenOf(bad);
    const v1 = a.persistedReadModel(offersV1);
    await v1.list({ atLeast: last });
    // the new version's fold has a bug v1 does not have: it cannot take an amount of 13
    const buggy: Fold<Offer> = (events, state) => {
      for (const e of events) if (e.type === "OfferMade" && (e.data as { amount: number }).amount === 13) throw new Error("v2 cannot take 13");
      return offerFoldV2(events, state);
    };
    const failures: FailedKeyError[] = [];
    const v2Def = defineReadModel({ ...offersV2, fold: buggy, onFailedKey: (e) => void failures.push(e) });
    const v2 = b.persistedReadModel(v2Def);
    await switched(v2, 30_000);
    const page = await v2.list({ atLeast: last, limit: 100 });
    expect(page.rows.map((r) => r.key)).not.toContain("k3");
    expect(page.rows.length).toBe(4);
    for (const row of page.rows) expect(row.state).toEqual(((await contextOf(b, v2Def, row.key)) as { state: Offer }).state);
    const got = await v2.get("k3").catch((e: unknown) => e);
    expect(got).toBeInstanceOf(FailedKeyError);
    expect(got).toMatchObject({ key: "k3", event: { sequence: bad.last } });
    expect((await contextOf(b, v2Def, "k3")).ok).toBe(false); // fold on read throws too: the same answer
    expect(failures.length).toBe(1);
    expect((await v2.status()).failed).toBe(1);
    // v1 is untouched by v2's failure
    expect(await v1.get("k3")).toEqual(((await contextOf(a, offersV1, "k3")) as { state: Offer }).state);
    // durable: a fresh process's v2 fails the key the same way, without calling onFailedKey again
    const c = h.open();
    await expect(c.persistedReadModel(v2Def).get("k3")).rejects.toBeInstanceOf(FailedKeyError);
    expect(failures.length).toBe(1);
    await h.closeAll();
  }, 60_000);
}
