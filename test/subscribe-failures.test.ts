import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { compareCursor, MemoryStore, type Cursor, type RecordedEvent } from "../src/index.js";
import { memoryCursors, subscribe, type CursorStore, type Subscription } from "../src/subscribe/index.js";

// Stage 2, measurer: the isolation invariant of decision 12 as a property, memory store. Any
// appends, any poison events, any batch size, and a stop at any handler call followed by a fresh
// subscription over the same cursors (a restart): every healthy event is applied exactly once and
// in order, every poison event is asked about alone exactly once and skipped, and the saved cursor
// only moves forward (model §2, "Invariant").

/** Loop `whenCaughtUp()` until `hasPassed(token)`, the subscription stopped, or a deadline. */
async function until(sub: Subscription, token: Cursor, ms = 5_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!sub.stopped && !sub.hasPassed(token) && Date.now() < deadline) await sub.whenCaughtUp();
}

describe("subscribe: 'skip' isolation, as a property (memory)", () => {
  it("applies every healthy event once, in order, skips each poison event once, and only moves the cursor forward — across a restart", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: 1, max: 8 }), { minLength: 1, maxLength: 10 }), // sizes of the appends (one transaction each)
        fc.array(fc.integer({ min: 0, max: 6 }).map((x) => x === 0), { minLength: 80, maxLength: 80 }), // poison by sequence, ~1 in 7
        fc.integer({ min: 1, max: 16 }), // batchSize
        fc.option(fc.integer({ min: 1, max: 30 }), { nil: null }), // stop the first subscription at this handler call
        async (sizes, poisonMask, batchSize, stopAt) => {
          const store = new MemoryStore();
          let last!: { transactionId: string; last: number };
          let n = 0;
          for (const size of sizes) {
            last = await store.append(Array.from({ length: size }, () => ({ type: "A", data: { n: ++n }, scopes: {} })));
          }
          const poison = new Set(Array.from({ length: n }, (_, i) => i + 1).filter((s) => poisonMask[s - 1]));
          const token = { transactionId: last.transactionId, sequence: last.last };

          const inner = memoryCursors();
          const saved: Cursor[] = [];
          const cursors: CursorStore = { load: (name) => inner.load(name), save: async (name, c) => (saved.push(c), inner.save(name, c)) };
          const applied: number[] = [];
          const askedAlone: number[] = [];
          let calls = 0;
          let first!: Subscription;
          let stopping: Promise<void> | undefined;
          const handler = (events: readonly RecordedEvent[]) => {
            calls++;
            if (stopAt !== null && calls === stopAt && !stopping) stopping = first.stop(); // not awaited: it awaits this very delivery
            if (events.some((e) => poison.has(e.sequence))) throw new Error("poison"); // all or nothing, like a rolled-back transaction
            applied.push(...events.map((e) => e.sequence));
          };
          const onError = (_error: unknown, batch: readonly RecordedEvent[]) => {
            if (batch.length === 1 && poison.has(batch[0]!.sequence)) askedAlone.push(batch[0]!.sequence);
            return "skip" as const;
          };
          const options = { store, cursors, batchSize, pollIntervalMs: 5, onError };

          first = subscribe("prop", { types: ["A"] }, handler, options);
          await until(first, token);
          await stopping;
          await first.stop();
          const second = subscribe("prop", { types: ["A"] }, handler, options); // the restart: same cursors
          await until(second, token);
          await second.stop();

          const healthy = Array.from({ length: n }, (_, i) => i + 1).filter((s) => !poison.has(s));
          expect(applied).toEqual(healthy);
          expect(askedAlone).toEqual([...poison]);
          expect(second.cursor).toEqual({ transactionId: token.transactionId, sequence: token.sequence });
          for (let i = 1; i < saved.length; i++) expect(compareCursor(saved[i - 1]!, saved[i]!)).toBeLessThan(0);
        },
      ),
      { numRuns: 150 },
    );
  });
});
