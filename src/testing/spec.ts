import assert from "node:assert/strict";
import {
  emptySchema,
  MemoryStore,
  runCommand,
  type CommandOutcome,
  type CommandSpec,
  type EventStore,
  type NewEvent,
  type RecordedEvent,
  type StoreSchema,
} from "../index.js";

export interface SpecOptions {
  /** Schema for the memory store (default: `emptySchema({ strict: false })`). */
  readonly schema?: StoreSchema;
  /** Bring your own store instead of a fresh `MemoryStore`. */
  readonly store?: () => EventStore | Promise<EventStore>;
  readonly clock?: () => Date;
}

/** What `then` compares: type, data and scopes. Ids and metadata are compared only when given. */
export interface ExpectedEvent {
  readonly type: string;
  readonly data?: Record<string, unknown>;
  readonly scopes?: Record<string, string>;
  readonly id?: string;
}

export interface Then<R> {
  /** Exactly these events were appended (in order), or run your own assertions on them. */
  then(expected: readonly ExpectedEvent[] | ((appended: readonly RecordedEvent[], outcome: CommandOutcome<R>) => void | Promise<void>)): Promise<CommandOutcome<R>>;
  /** The command was rejected (optionally with this code). */
  thenRejects(code?: string): Promise<CommandOutcome<R>>;
  /** The command succeeded and appended nothing. */
  thenNothingHappened(): Promise<CommandOutcome<R>>;
  /** The command threw. */
  thenThrows(check?: (error: unknown) => void): Promise<void>;
}

export interface When {
  when<S, R = void>(spec: CommandSpec<S, R>): Then<R>;
}

/**
 * given / when / then for CCC commands over a fresh in-memory store:
 *
 * ```ts
 * await given([accounts.AccountOpened({ owner: "mary" }, {}, { id })])
 *   .when(withdraw({ accountOpenedId: id, amount: 50 }))
 *   .then([{ type: "MoneyWithdrawn", data: { amount: 50 }, scopes: { accountOpenedId: id } }]);
 * ```
 */
export function given(events: readonly NewEvent[] = [], options: SpecOptions = {}): When {
  const run = async <S, R>(spec: CommandSpec<S, R>) => {
    const store = options.store ? await options.store() : new MemoryStore({ schema: options.schema ?? emptySchema({ strict: false }), clock: options.clock });
    if (events.length > 0) await store.append(events);
    const before = (await store.query({}, {})).lastReturned;
    const outcome = await runCommand({ store, clock: options.clock }, spec);
    const appended = (await store.query({}, { after: before })).events;
    await store.close();
    return { outcome, appended };
  };
  return {
    when<S, R = void>(spec: CommandSpec<S, R>): Then<R> {
      return {
        async then(expected) {
          const { outcome, appended } = await run(spec);
          if (typeof expected === "function") {
            await expected(appended, outcome);
            return outcome;
          }
          assert.ok(outcome.ok, `expected the command to succeed, got rejection ${!outcome.ok ? `${outcome.code}: ${outcome.reason}` : ""}`);
          assert.equal(appended.length, expected.length, `expected ${expected.length} appended event(s), got ${appended.length}: ${appended.map((e) => e.type).join(", ")}`);
          expected.forEach((exp, i) => {
            const got = appended[i]!;
            assert.equal(got.type, exp.type, `event #${i}: type`);
            if (exp.data !== undefined) assert.deepEqual(got.data, exp.data, `event #${i}: data`);
            if (exp.scopes !== undefined) assert.deepEqual(got.scopes, exp.scopes, `event #${i}: scopes`);
            if (exp.id !== undefined) assert.equal(got.id, exp.id, `event #${i}: id`);
          });
          return outcome;
        },
        async thenRejects(code) {
          const { outcome, appended } = await run(spec);
          assert.ok(!outcome.ok, "expected a rejection, but the command succeeded");
          if (code !== undefined) assert.equal(outcome.code, code, `rejection code (reason: ${outcome.reason})`);
          assert.equal(appended.length, 0, "a rejected command must append nothing");
          return outcome;
        },
        async thenNothingHappened() {
          const { outcome, appended } = await run(spec);
          assert.ok(outcome.ok, `expected success without events, got rejection ${!outcome.ok ? outcome.code : ""}`);
          assert.equal(appended.length, 0, `expected no events, got ${appended.map((e) => e.type).join(", ")}`);
          return outcome;
        },
        async thenThrows(check) {
          let thrown: unknown;
          try {
            await run(spec);
          } catch (error) {
            thrown = error;
          }
          assert.ok(thrown !== undefined, "expected the command to throw");
          check?.(thrown);
        },
      };
    },
  };
}
