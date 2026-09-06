import { describe, expect, it } from "vitest";
import { z } from "zod";
import { buildSchema, decision, defineEvents, MemoryStore, reject, runCommand, type CommandSpec } from "../src/index.js";
import { given, interferingStore, makeEvent, recordingStore, resetSequence, slowStore } from "../src/testing/index.js";

const accounts = defineEvents({
  AccountOpened: { data: z.object({ owner: z.string() }) },
  MoneyDeposited: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"] },
  MoneyWithdrawn: { data: z.object({ amount: z.number() }), scopes: ["accountOpenedId"] },
});
const schema = buildSchema([accounts]);

const balance = accounts.$fold(0, {
  MoneyDeposited: (d, s) => s + d.amount,
  MoneyWithdrawn: (d, s) => s - d.amount,
});

function withdraw(accountOpenedId: string, amount: number): CommandSpec<number, { balance: number }> {
  return {
    context: accounts.$scope("accountOpenedId", accountOpenedId),
    initial: 0,
    fold: (events, state) => state + balance(events),
    decide: (state) => {
      if (state < amount) return reject("insufficient-funds", `balance ${state} < ${amount}`);
      return decision(accounts.MoneyWithdrawn({ amount }, { accountOpenedId }), { balance: state - amount });
    },
  };
}

describe("given / when / then", () => {
  const opened = accounts.AccountOpened({ owner: "mary" }, {}, { id: "acc-1" });
  const deposited = accounts.MoneyDeposited({ amount: 100 }, { accountOpenedId: "acc-1" });

  it("asserts the appended events", async () => {
    const outcome = await given([opened, deposited], { schema })
      .when(withdraw("acc-1", 40))
      .then([{ type: "MoneyWithdrawn", data: { amount: 40 }, scopes: { accountOpenedId: "acc-1" } }]);
    expect(outcome.ok && outcome.result.balance).toBe(60);
  });

  it("asserts a rejection with its code", async () => {
    const outcome = await given([opened, deposited], { schema }).when(withdraw("acc-1", 500)).thenRejects("insufficient-funds");
    expect(outcome.ok).toBe(false);
  });

  it("fails when the expectation does not match", async () => {
    await expect(
      given([opened, deposited], { schema }).when(withdraw("acc-1", 40)).then([{ type: "MoneyWithdrawn", data: { amount: 41 } }]),
    ).rejects.toThrow(/data/);
    await expect(given([opened, deposited], { schema }).when(withdraw("acc-1", 40)).thenRejects()).rejects.toThrow(/expected a rejection/);
  });

  it("thenNothingHappened for a no-op decision", async () => {
    await given([opened], { schema })
      .when({ context: accounts.$scope("accountOpenedId", "acc-1"), decide: () => decision([]) })
      .thenNothingHappened();
  });

  it("thenThrows when decide throws", async () => {
    await given([opened], { schema })
      .when({
        context: accounts.$scope("accountOpenedId", "acc-1"),
        decide: () => {
          throw new Error("boom");
        },
      })
      .thenThrows((e) => expect((e as Error).message).toBe("boom"));
  });

  it("accepts a custom assertion callback", async () => {
    await given([opened, deposited], { schema })
      .when(withdraw("acc-1", 10))
      .then((appended, outcome) => {
        expect(appended).toHaveLength(1);
        expect(outcome.ok).toBe(true);
      });
  });
});

describe("decorators", () => {
  it("interferingStore makes the guard fire and the runner retry", async () => {
    const inner = new MemoryStore({ schema });
    await inner.append([accounts.AccountOpened({ owner: "mary" }, {}, { id: "acc-1" }), accounts.MoneyDeposited({ amount: 100 }, { accountOpenedId: "acc-1" })]);
    const store = interferingStore(inner, () => [accounts.MoneyWithdrawn({ amount: 90 }, { accountOpenedId: "acc-1" })]);
    const outcome = await runCommand({ store }, withdraw("acc-1", 50));
    expect(store.interferences).toBe(1);
    // After the interference the balance is 10, so the retried decision must reject.
    expect(outcome.ok).toBe(false);
    expect(!outcome.ok && outcome.code).toBe("insufficient-funds");
    expect(outcome.attempts).toBe(2);
    expect((await inner.query({ types: ["MoneyWithdrawn"] })).events.map((e) => e.data.amount)).toEqual([90]);
  });

  it("interferingStore exhausts retries when interference keeps coming", async () => {
    const inner = new MemoryStore({ schema });
    await inner.append([accounts.AccountOpened({ owner: "mary" }, {}, { id: "acc-1" }), accounts.MoneyDeposited({ amount: 1000 }, { accountOpenedId: "acc-1" })]);
    const store = interferingStore(inner, () => [accounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: "acc-1" })], { times: 10 });
    const outcome = await runCommand({ store }, { ...withdraw("acc-1", 5), retries: 2 });
    expect(outcome.ok).toBe(false);
    expect(!outcome.ok && outcome.code).toBe("conflict");
    expect(outcome.attempts).toBe(3);
  });

  it("slowStore widens the append window so a second writer wins", async () => {
    const inner = new MemoryStore({ schema });
    await inner.append([accounts.AccountOpened({ owner: "mary" }, {}, { id: "acc-1" })]);
    const slow = slowStore(inner, 30);
    const query = accounts.$scope("accountOpenedId", "acc-1");
    const read = await slow.query(query);
    const slowAppend = slow.appendIf([accounts.MoneyDeposited({ amount: 1 }, { accountOpenedId: "acc-1" })], { query, version: read.contextVersion });
    const fast = await inner.appendIf([accounts.MoneyDeposited({ amount: 2 }, { accountOpenedId: "acc-1" })], { query, version: read.contextVersion });
    expect(fast.ok).toBe(true);
    expect((await slowAppend).ok).toBe(false);
  });

  it("recordingStore records calls", async () => {
    const store = recordingStore(new MemoryStore({ schema }));
    await store.append([accounts.AccountOpened({ owner: "mary" }, {}, { id: "acc-1" })]);
    await runCommand({ store }, withdraw("acc-1", 1));
    expect(store.calls.map((c) => c.op)).toEqual(["append", "query"]);
    store.clear();
    expect(store.calls).toHaveLength(0);
  });
});

describe("makeEvent", () => {
  it("builds typed recorded events with increasing sequences", () => {
    resetSequence();
    const a = makeEvent("AccountOpened", { owner: "mary" });
    const b = makeEvent("MoneyDeposited", { amount: 5 }, { scopes: { accountOpenedId: a.id } });
    expect(a.sequence).toBe(1);
    expect(b.sequence).toBe(2);
    expect(b.scopes.accountOpenedId).toBe(a.id);
    expect(b.settled).toBe(true);
    expect(balance([a, b])).toBe(5);
    const c = makeEvent("X", {}, { sequence: 10 });
    expect(makeEvent("Y", {}).sequence).toBe(11);
    expect(c.transactionId).toBe("10");
  });
});
