import type { ContextCache, LoadedContext } from "./context.js";
import { uuidv7 } from "./ids.js";
import type { AppendResult, Conflict, EventStore, NewEvent, Query, RecordedEvent } from "./types.js";

/** A business rejection. Not an error: the command ran and said no. */
export interface Rejection {
  readonly ok: false;
  readonly code: string;
  readonly reason: string;
  /** The subject does not exist → HTTP 404 instead of 422. */
  readonly missing?: boolean;
  readonly conflict?: Conflict;
}

export function reject(code: string, reason: string, options: { missing?: boolean } = {}): Rejection {
  return { ok: false, code, reason, ...(options.missing ? { missing: true } : {}) };
}

export function rejectMissing(code: string, reason: string): Rejection {
  return reject(code, reason, { missing: true });
}

/** What `decide` returns when it accepts. `events` may be empty (a no-op decision). */
export interface Decision<R = void> {
  readonly ok?: true;
  readonly events: readonly NewEvent[];
  readonly result?: R;
}

export type Decided<R> = Decision<R> | Rejection;

export function isRejection(decided: Decided<unknown>): decided is Rejection {
  return (decided as Rejection).ok === false;
}

/** Uncomputable values handed into the pure `decide` (Rico Fritzsche's RPU rule). */
export interface DecideTools {
  readonly now: Date;
  id(): string;
  /** Number of the current attempt, starting at 0. */
  readonly attempt: number;
}

export interface CommandSpec<S, R> {
  /** The context: all events the decision depends on. Also the consistency boundary. */
  readonly context: Query;
  /** Optional fold. Without it, `decide` receives the raw context events. */
  readonly fold?: (events: readonly RecordedEvent[], state: S) => S;
  readonly initial?: S | (() => S);
  readonly decide: (state: S, tools: DecideTools) => Decided<R> | Promise<Decided<R>>;
  /** Retries on a conditional-append conflict. Default 3. */
  readonly retries?: number;
  /** Bypass the context cache for this command. */
  readonly noCache?: boolean;
}

export type CommandOutcome<R = void> =
  | { readonly ok: true; readonly result: R; readonly appended: AppendResult | null; readonly attempts: number }
  | (Rejection & { readonly attempts: number });

export const CONFLICT_CODE = "conflict";

export interface CommandRuntime {
  readonly store: EventStore;
  readonly cache?: ContextCache;
  readonly clock?: () => Date;
}

/**
 * The CCC command cycle: read the context → decide (pure) → record with `appendIf` →
 * on conflict re-read and decide again. Lifted from an earlier in-house store's `run-command`, with the
 * re-read replaced by the store's atomic guard.
 */
export async function runCommand<S, R>(runtime: CommandRuntime, spec: CommandSpec<S, R>): Promise<CommandOutcome<R>> {
  const retries = spec.retries ?? 3;
  const clock = runtime.clock ?? (() => new Date());
  let lastConflict: Conflict | undefined;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const loaded = await loadContext(runtime, spec);
    const now = clock();
    const decided = await spec.decide(loaded.state, { now, id: () => uuidv7(now.getTime()), attempt });
    if (isRejection(decided)) return { ...decided, attempts: attempt + 1 };
    if (decided.events.length === 0) {
      return { ok: true, result: decided.result as R, appended: null, attempts: attempt + 1 };
    }
    const outcome = await runtime.store.appendIf(decided.events, loaded.ctx);
    if (outcome.ok) return { ok: true, result: decided.result as R, appended: outcome.appended, attempts: attempt + 1 };
    lastConflict = outcome.conflict;
  }
  return {
    ok: false,
    code: CONFLICT_CODE,
    reason: `context changed concurrently on all ${retries + 1} attempts`,
    conflict: lastConflict,
    attempts: retries + 1,
  };
}

async function loadContext<S, R>(runtime: CommandRuntime, spec: CommandSpec<S, R>): Promise<LoadedContext<S>> {
  const fold = spec.fold ?? ((events: readonly RecordedEvent[], state: S) => [...((state as unknown as RecordedEvent[]) ?? []), ...events] as unknown as S);
  // `null` is a legitimate initial state ("nothing exists yet"); only `undefined` means "not given".
  const initial = spec.initial !== undefined ? spec.initial : ([] as unknown as S);
  if (runtime.cache && !spec.noCache && spec.fold) {
    return runtime.cache.load<S>({ query: spec.context, fold, initial });
  }
  const result = await runtime.store.query(spec.context);
  const base = typeof initial === "function" ? (initial as () => S)() : initial;
  return {
    state: fold(result.events, base),
    ctx: { query: spec.context, version: result.contextVersion },
    delta: result.events,
    cacheHit: false,
  };
}

/** Map an outcome to an HTTP status: 200/201 ok, 404 missing, 409 conflict, 422 rejection. */
export function httpStatusOf(outcome: CommandOutcome<unknown> | Rejection): number {
  if (outcome.ok) return "appended" in outcome && outcome.appended ? 201 : 200;
  if (outcome.code === CONFLICT_CODE) return 409;
  if (outcome.missing) return 404;
  return 422;
}

/**
 * Typed per-type decision helpers are deliberately absent: `decide` is your function. The
 * `Decision` and `Rejection` shapes are the whole contract.
 */
export function decision<R = void>(events: readonly NewEvent[] | NewEvent, result?: R): Decision<R> {
  return { ok: true, events: Array.isArray(events) ? events : [events as NewEvent], ...(result !== undefined ? { result } : {}) } as Decision<R>;
}
