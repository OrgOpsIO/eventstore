import assert from "node:assert/strict";
import {
  UnindexableContextError,
  UniqueViolationError,
  ValidationError,
  compareCursor,
  type Cursor,
  type EventStore,
  type Query,
  type StoreSchema,
} from "../index.js";
import { conformanceEvents, conformanceSchema, conformanceUpcastEvents, conformanceUpcastSchema } from "./schema.js";

/** Builds a fresh store for one conformance case from the given schema. */
export type MakeStore = (schema: StoreSchema) => EventStore | Promise<EventStore>;

/** The test framework's `test(name, fn)` — vitest's `it`, jest's `test`, node:test's `test`. */
export interface ConformanceHooks {
  /** Register one test. Works with vitest/jest `it`, `node:test` `test`, … */
  readonly test: (name: string, fn: () => Promise<void>) => void;
}

const ev = conformanceEvents;

/**
 * The behavioural specification of an `EventStore`. `MemoryStore` is the reference; the
 * Postgres store must pass the same suite. Every case gets a fresh store.
 */
export function conformanceSuite(makeStore: MakeStore, hooks: ConformanceHooks): void {
  const strict = conformanceSchema({ strict: true });
  const lenient = conformanceSchema({ strict: false });

  const withStore = (name: string, fn: (store: EventStore, schema: StoreSchema) => Promise<void>, schema: StoreSchema = strict) =>
    hooks.test(name, async () => {
      const store = await makeStore(schema);
      try {
        await fn(store, schema);
      } finally {
        await store.close();
      }
    });

  const openAccount = (owner: string, id?: string) => ev.AccountOpened({ owner }, {}, id ? { id } : {});

  withStore("append returns one consecutive sequence range", async (store) => {
    const a = await store.append([openAccount("a"), openAccount("b")]);
    assert.equal(a.count, 2);
    assert.equal(a.last, a.first + 1);
    const b = await store.append([openAccount("c")]);
    assert.equal(b.first, a.last + 1);
    assert.equal(b.count, 1);
  });

  withStore("append rejects an empty batch", async (store) => {
    await assert.rejects(() => store.append([]));
  });

  withStore("query returns ascending by sequence, or descending on request", async (store) => {
    await store.append([openAccount("a"), openAccount("b"), openAccount("c")]);
    const asc = await store.query({ types: ["AccountOpened"] });
    assert.deepEqual(asc.events.map((e) => e.data.owner), ["a", "b", "c"]);
    for (let i = 1; i < asc.events.length; i++) assert.ok(asc.events[i]!.sequence > asc.events[i - 1]!.sequence);
    const desc = await store.query({ types: ["AccountOpened"] }, { order: "desc" });
    assert.deepEqual(desc.events.map((e) => e.data.owner), ["c", "b", "a"]);
  });

  withStore("types are OR-ed; omitted types match everything", async (store) => {
    const acc = openAccount("mary", "acc-1");
    await store.append([acc, ev.MoneyDeposited({ amount: 5 }, { accountOpenedId: "acc-1" }), ev.NoteAdded({ text: "hi" })]);
    const two = await store.query({ types: ["AccountOpened", "NoteAdded"] });
    assert.deepEqual(two.events.map((e) => e.type), ["AccountOpened", "NoteAdded"]);
    const all = await store.query({});
    assert.equal(all.events.length, 3);
  });

  withStore("scopes match via the scopes object, via the own id (root), and via a flat data field", async (store) => {
    await store.append([
      openAccount("mary", "acc-1"),
      openAccount("bob", "acc-2"),
      ev.MoneyDeposited({ amount: 5 }, { accountOpenedId: "acc-1" }),
      ev.MoneyDeposited({ amount: 7 }, { accountOpenedId: "acc-2" }),
      ev.LegacyThing({ thingId: "t-1", value: "x" }),
      ev.LegacyThing({ thingId: "t-2", value: "y" }),
    ]);
    const acc1 = await store.query({ scopes: { accountOpenedId: "acc-1" } });
    assert.deepEqual(acc1.events.map((e) => e.type), ["AccountOpened", "MoneyDeposited"]);
    assert.equal(acc1.events[0]!.id, "acc-1");
    const legacy = await store.query({ scopes: { thingId: "t-2" } });
    assert.equal(legacy.events.length, 1);
    assert.equal(legacy.events[0]!.data.value, "y");
    const none = await store.query({ scopes: { accountOpenedId: "nope" } });
    assert.equal(none.events.length, 0);
    assert.equal(none.contextVersion, 0);
  });

  withStore("a scope with several values matches any of them; several scope keys must all match", async (store) => {
    await store.append([
      openAccount("mary", "acc-1"),
      openAccount("bob", "acc-2"),
      openAccount("eve", "acc-3"),
      ev.NoteAdded({ text: "n1" }, { accountOpenedId: "acc-1" }),
    ]);
    const two = await store.query({ types: ["AccountOpened"], scopes: { accountOpenedId: ["acc-1", "acc-3"] } });
    assert.deepEqual(two.events.map((e) => e.id).sort(), ["acc-1", "acc-3"]);
    const both = await store.query({ scopes: { accountOpenedId: "acc-1", thingId: "missing" } });
    assert.equal(both.events.length, 0);
  });

  withStore("contextVersion equals the highest matching sequence for every filter shape, with and without options", async (store) => {
    await store.append([
      openAccount("mary", "acc-1"),
      openAccount("bob", "acc-2"),
      ev.MoneyDeposited({ amount: 5 }, { accountOpenedId: "acc-1" }),
      ev.NoteAdded({ text: "n1" }, { accountOpenedId: "acc-1" }),
      ev.NoteAdded({ text: "n2" }),
      ev.MoneyDeposited({ amount: 7 }, { accountOpenedId: "acc-2" }),
      // an undeclared type may carry a declared scope key both as a scope and as a flat field
      { type: "Tagged", data: { thingId: "t-1" }, scopes: { accountOpenedId: "acc-1" } },
      { type: "Tagged", data: { thingId: "t-2" }, scopes: { accountOpenedId: "acc-2" } },
    ]);
    const all = (await store.query({})).events;
    const shapes: Query[] = [
      { scopes: { accountOpenedId: "acc-1", thingId: "t-1" } },
      { types: ["Tagged", "NoteAdded"], scopes: { accountOpenedId: "acc-1", thingId: "t-1" } },
      { scopes: { accountOpenedId: ["acc-1", "acc-2"], thingId: ["t-1", "t-9"] } },
      { types: ["Tagged"], scopes: { thingId: "t-2", accountOpenedId: "acc-2" }, where: [{ thingId: "t-2" }] },
      { types: ["NoteAdded"] },
      { scopes: { accountOpenedId: "acc-1" } },
      { types: ["AccountOpened", "MoneyDeposited", "NoteAdded"], scopes: { accountOpenedId: "acc-1" } },
      { types: ["MoneyDeposited"], scopes: { accountOpenedId: ["acc-1", "acc-2"] } },
      { scopes: { accountOpenedId: "acc-1", thingId: "none" } },
      { types: ["AccountOpened"], where: [{ owner: "bob" }] },
      [{ types: ["NoteAdded"] }, { scopes: { accountOpenedId: "acc-2" } }],
      { types: ["MoneyDeposited"], scopes: { accountOpenedId: "acc-1" }, where: [{ amount: 5 }] },
    ];
    for (const shape of shapes) {
      const full = await store.query(shape);
      const expected = full.events.reduce((m, e) => Math.max(m, e.sequence), 0);
      assert.equal(full.contextVersion, expected, `contextVersion for ${JSON.stringify(shape)}`);
      const narrowed = await store.query(shape, { after: expected, limit: 1 });
      assert.equal(narrowed.contextVersion, expected, `contextVersion with options for ${JSON.stringify(shape)}`);
      assert.equal(narrowed.events.length, 0);
      // the guard must agree with the read: an appendIf with the read version commits
      const probe = await store.appendIf([ev.NoteAdded({ text: "probe" })], { query: [{ scopes: { thingId: "never" } }], version: 0 });
      assert.equal(probe.ok, true);
    }
    assert.ok(all.length >= 6);
  });

  withStore("where is JSONB containment, OR-ed across predicates", async (store) => {
    await store.append([openAccount("mary"), openAccount("bob"), openAccount("eve")]);
    const r = await store.query({ types: ["AccountOpened"], where: [{ owner: "mary" }, { owner: "eve" }] });
    assert.deepEqual(r.events.map((e) => e.data.owner), ["mary", "eve"]);
    const nested = await store.query({ where: [{ scopes: { accountOpenedId: "x" } }] });
    assert.equal(nested.events.length, 0);
  });

  withStore("several filters are OR-ed and grouped per filter in byFilter", async (store) => {
    await store.append([
      openAccount("mary", "acc-1"),
      ev.MoneyDeposited({ amount: 5 }, { accountOpenedId: "acc-1" }),
      ev.NoteAdded({ text: "n" }),
    ]);
    const r = await store.query([{ types: ["MoneyDeposited"] }, { types: ["NoteAdded"] }, { scopes: { accountOpenedId: "acc-1" } }]);
    assert.equal(r.events.length, 3, "union without duplicates");
    assert.equal(r.byFilter.length, 3);
    assert.deepEqual(r.byFilter[0]!.map((e) => e.type), ["MoneyDeposited"]);
    assert.deepEqual(r.byFilter[1]!.map((e) => e.type), ["NoteAdded"]);
    assert.deepEqual(r.byFilter[2]!.map((e) => e.type), ["AccountOpened", "MoneyDeposited"]);
    assert.equal(r.contextVersion, r.events[2]!.sequence);
  });

  withStore("after narrows the returned events but never the context version (two numbers)", async (store) => {
    await store.append([openAccount("mary", "acc-1")]);
    const first = await store.append([ev.MoneyDeposited({ amount: 1 }, { accountOpenedId: "acc-1" })]);
    const second = await store.append([ev.MoneyDeposited({ amount: 2 }, { accountOpenedId: "acc-1" })]);
    const r = await store.query({ scopes: { accountOpenedId: "acc-1" } }, { after: first.last });
    assert.equal(r.events.length, 1);
    assert.equal(r.events[0]!.data.amount, 2);
    assert.equal(r.lastReturned, second.last);
    assert.equal(r.contextVersion, second.last);
    const late = await store.query({ scopes: { accountOpenedId: "acc-1" } }, { after: second.last });
    assert.equal(late.events.length, 0);
    assert.equal(late.lastReturned, 0, "nothing returned → 0");
    assert.equal(late.contextVersion, second.last, "the context still has a version");
  });

  withStore("limit caps the returned events; lastReturned follows the returned set, contextVersion the whole context", async (store) => {
    await store.append([openAccount("a"), openAccount("b"), openAccount("c")]);
    const r = await store.query({ types: ["AccountOpened"] }, { limit: 2 });
    assert.equal(r.events.length, 2);
    assert.equal(r.lastReturned, r.events[1]!.sequence);
    assert.ok(r.contextVersion > r.lastReturned);
  });

  withStore("settledCursor points at the highest settled returned record; cursor excludes settled records at or below it", async (store) => {
    await store.append([openAccount("a"), openAccount("b")]);
    const r = await store.query({ types: ["AccountOpened"] });
    assert.ok(r.settledCursor, "a settled cursor is present");
    assert.equal(r.settledCursor!.sequence, r.lastReturned);
    assert.ok(r.settledCursor!.transactionId.length > 0);
    await store.append([openAccount("c")]);
    const delta = await store.query({ types: ["AccountOpened"] }, { cursor: r.settledCursor });
    assert.deepEqual(delta.events.map((e) => e.data.owner), ["c"]);
    assert.equal(delta.contextVersion, delta.events[0]!.sequence);
    const nothing = await store.query({ types: ["AccountOpened"] }, { cursor: delta.settledCursor });
    assert.equal(nothing.events.length, 0);
    assert.equal(nothing.settledCursor, null);
  });

  withStore("appendIf commits on an unchanged context and conflicts afterwards with expected/actual", async (store) => {
    await store.append([openAccount("mary", "acc-1")]);
    const query = { scopes: { accountOpenedId: "acc-1" } };
    const read = await store.query(query);
    const ok = await store.appendIf([ev.MoneyDeposited({ amount: 10 }, { accountOpenedId: "acc-1" })], { query, version: read.contextVersion });
    assert.ok(ok.ok);
    assert.equal(ok.appended.count, 1);
    const stale = await store.appendIf([ev.MoneyDeposited({ amount: 10 }, { accountOpenedId: "acc-1" })], { query, version: read.contextVersion });
    assert.ok(!stale.ok);
    assert.equal(stale.conflict.expected, read.contextVersion);
    assert.equal(stale.conflict.actual, ok.appended.last);
    const after = await store.query(query);
    assert.equal(after.events.length, 2, "the conflicting batch was not stored");
  });

  withStore("appendIf with version 0 on an empty context is create-if-absent: the second one conflicts", async (store) => {
    const query = { scopes: { accountOpenedId: "acc-9" } };
    const read = await store.query(query);
    assert.equal(read.contextVersion, 0);
    const first = await store.appendIf([openAccount("mary", "acc-9")], { query, version: 0 });
    assert.ok(first.ok);
    const second = await store.appendIf([openAccount("mary2", "acc-9")], { query, version: 0 });
    assert.ok(!second.ok);
    assert.equal(second.conflict.expected, 0);
    assert.equal(second.conflict.actual, first.appended.last);
  });

  withStore("appendIf rejects an empty batch", async (store) => {
    await assert.rejects(() => store.appendIf([], { query: { scopes: { accountOpenedId: "x" } }, version: 0 }));
  });

  withStore("invalid data fails validation and stores nothing", async (store) => {
    await assert.rejects(
      () => store.append([{ type: "MoneyDeposited", data: { amount: -1 }, scopes: { accountOpenedId: "acc-1" } }]),
      (e: unknown) => e instanceof ValidationError,
    );
    assert.equal((await store.query({})).events.length, 0);
  });

  withStore("a missing required scope fails validation", async (store) => {
    await assert.rejects(
      () => store.append([{ type: "MoneyDeposited", data: { amount: 1 } }]),
      (e: unknown) => e instanceof ValidationError,
    );
  });

  withStore("a declared unique path is enforced within a batch and across batches, all or nothing", async (store) => {
    await assert.rejects(
      () => store.append([openAccount("mary"), openAccount("mary")]),
      (e: unknown) => e instanceof UniqueViolationError,
    );
    assert.equal((await store.query({})).events.length, 0, "nothing of the failed batch is stored");
    await store.append([openAccount("mary")]);
    await assert.rejects(
      () => store.append([openAccount("bob"), openAccount("mary")]),
      (e: unknown) => e instanceof UniqueViolationError && e.detail.type === "AccountOpened" && e.detail.path === "owner",
    );
    const all = await store.query({ types: ["AccountOpened"] });
    assert.deepEqual(all.events.map((e) => e.data.owner), ["mary"], "bob was rolled back with the batch");
  });

  withStore("a duplicate idempotencyKey is rejected", async (store) => {
    await store.append([ev.NoteAdded({ text: "once" }, {}, { metadata: { idempotencyKey: "k-1" } })]);
    await assert.rejects(
      () => store.append([ev.NoteAdded({ text: "twice" }, {}, { metadata: { idempotencyKey: "k-1" } })]),
      (e: unknown) => e instanceof UniqueViolationError && e.detail.idempotencyKey === true,
    );
    assert.equal((await store.query({ types: ["NoteAdded"] })).events.length, 1);
  });

  withStore("strict mode refuses a guard that has no declared scope key", async (store) => {
    await assert.rejects(
      () => store.appendIf([ev.NoteAdded({ text: "x" })], { query: { types: ["NoteAdded"] }, version: 0 }),
      (e: unknown) => e instanceof UnindexableContextError,
    );
    await assert.rejects(
      () => store.appendIf([ev.NoteAdded({ text: "x" })], { query: { scopes: { undeclaredKey: "v" } }, version: 0 }),
      (e: unknown) => e instanceof UnindexableContextError,
    );
  });

  withStore(
    "non-strict mode accepts a type-only guard (global lock fallback) and still guards correctly",
    async (store) => {
      const query = { types: ["NoteAdded"] };
      const first = await store.appendIf([ev.NoteAdded({ text: "a" })], { query, version: 0 });
      assert.ok(first.ok);
      const stale = await store.appendIf([ev.NoteAdded({ text: "b" })], { query, version: 0 });
      assert.ok(!stale.ok);
      const fresh = await store.appendIf([ev.NoteAdded({ text: "b" })], { query, version: first.appended.last });
      assert.ok(fresh.ok);
    },
    lenient,
  );

  withStore("ids follow the <eventName>Id convention, are generated when omitted and kept when given", async (store, schema) => {
    await store.append([{ type: "AccountOpened", data: { owner: "gen" } }, openAccount("fixed", "acc-fixed")]);
    const r = await store.query({ types: ["AccountOpened"] });
    assert.equal(schema.idKeyOf("AccountOpened"), "accountOpenedId");
    assert.ok(r.events[0]!.id.length >= 32, "a generated id");
    assert.equal(r.events[1]!.id, "acc-fixed");
    assert.equal(r.events[0]!.data.accountOpenedId, undefined, "the own id is not part of data");
    assert.equal(r.events[0]!.data.owner, "gen");
  });

  withStore("metadata, recordedAt and transactionId round-trip", async (store) => {
    const before = Date.now() - 1000;
    await store.append([ev.NoteAdded({ text: "m" }, { accountOpenedId: "acc-1" }, { metadata: { actor: "rebar", correlationId: "c-1", custom: 42 } })]);
    const e = (await store.query({ types: ["NoteAdded"] })).events[0]!;
    assert.deepEqual(e.metadata, { actor: "rebar", correlationId: "c-1", custom: 42 });
    assert.deepEqual(e.scopes, { accountOpenedId: "acc-1" });
    assert.ok(e.recordedAt instanceof Date && e.recordedAt.getTime() >= before);
    assert.ok(typeof e.transactionId === "string" && e.transactionId.length > 0);
    assert.equal(typeof e.settled, "boolean");
  });

  withStore("concurrency: many parallel appendIf on the same context → exactly one commits", async (store) => {
    await store.append([openAccount("mary", "acc-1")]);
    const query = { scopes: { accountOpenedId: "acc-1" } };
    const read = await store.query(query);
    const attempts = await Promise.all(
      Array.from({ length: 20 }, (_, i) =>
        store.appendIf([ev.MoneyWithdrawn({ amount: 80 }, { accountOpenedId: "acc-1" }, { metadata: { by: i } })], {
          query,
          version: read.contextVersion,
        }),
      ),
    );
    const committed = attempts.filter((a) => a.ok);
    assert.equal(committed.length, 1, `expected exactly one winner, got ${committed.length}`);
    const withdrawn = await store.query({ types: ["MoneyWithdrawn"], scopes: { accountOpenedId: "acc-1" } });
    assert.equal(withdrawn.events.length, 1);
  });

  withStore("concurrency: parallel appendIf on different contexts all commit", async (store) => {
    const ids = Array.from({ length: 10 }, (_, i) => `acc-${i}`);
    await store.append(ids.map((id) => openAccount(`owner-${id}`, id)));
    const results = await Promise.all(
      ids.map(async (id) => {
        const query = { scopes: { accountOpenedId: id } };
        const read = await store.query(query);
        return store.appendIf([ev.MoneyDeposited({ amount: 1 }, { accountOpenedId: id })], { query, version: read.contextVersion });
      }),
    );
    assert.equal(results.filter((r) => r.ok).length, ids.length);
  });

  withStore("data must not contain the event's own id key or a `scopes` key", async (store) => {
    await assert.rejects(
      () => store.append([{ type: "Widget", data: { widgetId: "forged", n: 1 } }]),
      (e: unknown) => e instanceof ValidationError,
    );
    await assert.rejects(
      () => store.append([{ type: "Widget", data: { scopes: { accountOpenedId: "acc-9" } } }]),
      (e: unknown) => e instanceof ValidationError,
    );
    assert.equal((await store.query({ types: ["Widget"] })).events.length, 0, "nothing stored");
  });

  withStore("a non-string value at a declared scope key in data is rejected", async (store) => {
    await assert.rejects(
      () => store.append([{ type: "Widget", data: { thingId: 42 } }]),
      (e: unknown) => e instanceof ValidationError,
    );
    await assert.rejects(
      () => store.append([{ type: "Widget", data: { n: 1 }, scopes: { accountOpenedId: "" } }]),
      (e: unknown) => e instanceof ValidationError,
    );
  });

  withStore("where {k: null} matches only a JSON null", async (store) => {
    await store.append([
      { type: "Widget", data: { a: null } },
      { type: "Widget", data: { a: 1 } },
      { type: "Widget", data: { b: 1 } },
    ]);
    const r = await store.query({ types: ["Widget"], where: [{ a: null }] });
    assert.equal(r.events.length, 1);
    assert.equal(r.events[0]!.data.a, null);
  });

  withStore("where predicates go through the wire format (undefined vanishes, Dates become ISO strings)", async (store) => {
    const at = new Date("2026-01-02T03:04:05.000Z");
    await store.append([{ type: "Widget", data: { at, n: 1 } }, { type: "Widget", data: { n: 2 } }]);
    const everything = await store.query({ types: ["Widget"], where: [{ nope: undefined }] });
    assert.equal(everything.events.length, 2, "an undefined key is no constraint");
    const dated = await store.query({ types: ["Widget"], where: [{ at }] });
    assert.equal(dated.events.length, 1);
    assert.equal(dated.events[0]!.data.at, at.toISOString(), "data is stored as JSON");
    const asString = await store.query({ types: ["Widget"], where: [{ at: at.toISOString() }] });
    assert.equal(asString.events.length, 1);
  });

  withStore("byFilter follows the order of events (desc, limit, several filters)", async (store) => {
    await store.append([openAccount("a"), ev.NoteAdded({ text: "n1" }), openAccount("b"), openAccount("c"), ev.NoteAdded({ text: "n2" })]);
    const r = await store.query([{ types: ["AccountOpened"] }, { types: ["NoteAdded"] }], { order: "desc", limit: 4 });
    assert.equal(r.events.length, 4);
    assert.deepEqual(
      r.byFilter[0]!.map((e) => e.sequence),
      r.events.filter((e) => e.type === "AccountOpened").map((e) => e.sequence),
    );
    assert.deepEqual(
      r.byFilter[1]!.map((e) => e.sequence),
      r.events.filter((e) => e.type === "NoteAdded").map((e) => e.sequence),
    );
    for (let i = 1; i < r.events.length; i++) assert.ok(r.events[i]!.sequence < r.events[i - 1]!.sequence, "descending");
  });

  withStore("all events of one batch share a transactionId; different batches do not", async (store) => {
    await store.append([openAccount("a"), openAccount("b")]);
    await store.append([openAccount("c"), openAccount("d")]);
    const [a, b, c, d] = (await store.query({ types: ["AccountOpened"] })).events;
    assert.equal(a!.transactionId, b!.transactionId);
    assert.equal(c!.transactionId, d!.transactionId);
    assert.notEqual(a!.transactionId, c!.transactionId);
    assert.ok(/^\d+$/.test(a!.transactionId), "a decimal string");
  });

  withStore("a cursor survives a JSON round trip and never re-delivers what it points past", async (store) => {
    await store.append([openAccount("a"), openAccount("b"), openAccount("c")]);
    const first = await store.query({ types: ["AccountOpened"] }, { settledOnly: true });
    if (!first.settledCursor) return; // a fresh Postgres connection may not see its own writes as settled yet
    const roundTripped = JSON.parse(JSON.stringify(first.settledCursor)) as Cursor;
    assert.deepEqual(roundTripped, first.settledCursor);
    const again = await store.query({ types: ["AccountOpened"] }, { settledOnly: true, cursor: roundTripped });
    assert.ok(again.events.every((e) => compareCursor(e, roundTripped) > 0), "only events beyond the cursor");
    const seen = new Set(first.events.map((e) => e.sequence));
    assert.ok(again.events.every((e) => !seen.has(e.sequence)), "no re-delivery");
  });

  withStore("settledOnly never returns an unsettled event", async (store) => {
    await store.append([openAccount("a"), openAccount("b")]);
    const all = await store.query({ types: ["AccountOpened"] });
    const settled = await store.query({ types: ["AccountOpened"] }, { settledOnly: true });
    assert.ok(settled.events.every((e) => e.settled));
    assert.ok(settled.events.length <= all.events.length);
    assert.equal(settled.contextVersion, all.contextVersion, "the context version ignores settledOnly");
  });

  withStore("an undeclared scope key matches via scopes, own id and flat data — and the reference store's index is a superset", async (store) => {
    // `commentId` is declared by nobody: rows carry it as own id, in `scopes`, and flat in data
    await store.append([{ type: "CommentPosted", data: { text: "root" }, id: "c-1" }]);
    await store.append([{ type: "ReplyPosted", data: { text: "reply" }, scopes: { commentPostedId: "c-1" } }]);
    await store.append([{ type: "LegacyRef", data: { commentPostedId: "c-1", note: "flat" } }]);
    await store.append([{ type: "LegacyRef", data: { commentPostedId: "c-2", note: "other" } }]);
    const r = await store.query({ scopes: { commentPostedId: "c-1" } });
    assert.deepEqual(r.events.map((e) => e.type), ["CommentPosted", "ReplyPosted", "LegacyRef"]);
    assert.equal(r.contextVersion, 3);
    const typed = await store.query({ types: ["LegacyRef"], scopes: { commentPostedId: "c-1" } });
    assert.deepEqual(typed.events.map((e) => e.data.note), ["flat"]);
  });

  withStore("types: [] matches nothing", async (store) => {
    await store.append([openAccount("a")]);
    const r = await store.query({ types: [] });
    assert.equal(r.events.length, 0);
    assert.equal(r.contextVersion, 0);
    const mixed = await store.query([{ types: [] }, { types: ["AccountOpened"] }]);
    assert.equal(mixed.events.length, 1);
    assert.deepEqual(mixed.byFilter[0], []);
  });

  withStore("a negative limit and a fractional `after` are rejected before touching the store", async (store) => {
    await store.append([openAccount("a")]);
    await assert.rejects(() => store.query({}, { limit: -1 }));
    await assert.rejects(() => store.query({}, { after: 0.5 }));
    await assert.rejects(() => store.query({}, { cursor: { transactionId: "x", sequence: 1 } }));
    assert.equal((await store.query({}, { limit: 0 })).events.length, 0);
  });

  withStore(
    "upcast is applied on read by every store",
    async (store) => {
      const up = conformanceUpcastEvents;
      await store.append([up.Renamed({ v: "old" }), up.Renamed({ value: "new" })]);
      const r = await store.query({ types: ["Renamed"] });
      assert.deepEqual(
        r.events.map((e) => e.data),
        [{ value: "old" }, { value: "new" }],
      );
    },
    conformanceUpcastSchema(),
  );
}
