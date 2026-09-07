import type {
  AppendIfOutcome,
  AppendResult,
  ContextHandle,
  EventStore,
  NewEvent,
  Query,
  QueryOptions,
  QueryResult,
} from "../index.js";

/** How often the interfering store injects its events before delegating `appendIf`. */
export interface InterferenceOptions {
  /** How many `appendIf` calls get interference. Default 1. */
  readonly times?: number;
}

/**
 * Appends competing events right before the first N `appendIf` calls — after the caller has
 * read its context, before it records. A correct guard must fire; a correct command runner
 * must retry.
 */
export function interferingStore(
  inner: EventStore,
  interference: readonly NewEvent[] | (() => readonly NewEvent[]),
  options: InterferenceOptions = {},
): EventStore & { readonly interferences: number } {
  let remaining = options.times ?? 1;
  let fired = 0;
  return {
    get interferences() {
      return fired;
    },
    query: (q: Query, o?: QueryOptions) => inner.query(q, o),
    append: (e: readonly NewEvent[]) => inner.append(e),
    async appendIf(events: readonly NewEvent[], ctx: ContextHandle): Promise<AppendIfOutcome> {
      if (remaining > 0) {
        remaining--;
        fired++;
        const competing = typeof interference === "function" ? interference() : interference;
        if (competing.length > 0) await inner.append(competing);
      }
      return inner.appendIf(events, ctx);
    },
    close: () => inner.close(),
  };
}

/** Delays every `appendIf` by `ms` so a second writer can slip into the window. */
export function slowStore(inner: EventStore, ms: number): EventStore {
  return {
    query: (q: Query, o?: QueryOptions) => inner.query(q, o),
    append: (e: readonly NewEvent[]) => inner.append(e),
    async appendIf(events: readonly NewEvent[], ctx: ContextHandle): Promise<AppendIfOutcome> {
      await new Promise((resolve) => setTimeout(resolve, ms));
      return inner.appendIf(events, ctx);
    },
    close: () => inner.close(),
  };
}

export type RecordedCall =
  | { readonly op: "query"; readonly query: Query; readonly options: QueryOptions | undefined; readonly result: QueryResult }
  | { readonly op: "append"; readonly events: readonly NewEvent[]; readonly result: AppendResult }
  | { readonly op: "appendIf"; readonly events: readonly NewEvent[]; readonly ctx: ContextHandle; readonly result: AppendIfOutcome };

/** Records every call for assertions about what a command actually did. */
export function recordingStore(inner: EventStore): EventStore & { readonly calls: readonly RecordedCall[]; clear(): void } {
  const calls: RecordedCall[] = [];
  return {
    calls,
    clear: () => {
      calls.length = 0;
    },
    async query(query: Query, options?: QueryOptions): Promise<QueryResult> {
      const result = await inner.query(query, options);
      calls.push({ op: "query", query, options, result });
      return result;
    },
    async append(events: readonly NewEvent[]): Promise<AppendResult> {
      const result = await inner.append(events);
      calls.push({ op: "append", events, result });
      return result;
    },
    async appendIf(events: readonly NewEvent[], ctx: ContextHandle): Promise<AppendIfOutcome> {
      const result = await inner.appendIf(events, ctx);
      calls.push({ op: "appendIf", events, ctx, result });
      return result;
    },
    close: () => inner.close(),
  };
}
