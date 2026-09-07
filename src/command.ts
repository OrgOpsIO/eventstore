import type { ContextCache, Fold, LoadedContext } from "./context.js";
import { TransientError } from "./errors.js";
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

/** What `decide` returns when it accepts: `{ events }`, optionally with a `result`. `events` may be empty (a no-op decision). */
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
  /** A fresh UUIDv7 (registry creators generate their own; use this for ids you need before creating the event). */
  id(): string;
  /** Number of the current attempt, starting at 0. */
  readonly attempt: number;
}

export interface CommandSpec<S, R> {
  /** The context: all events the decision depends on. Also the consistency boundary. */
  readonly context: Query;
  /** Optional incremental fold. Without it, `decide` receives the raw context events (`S` = `readonly RecordedEvent[]`). */
  readonly fold?: Fold<S>;
  readonly initial?: S | (() => S);
  readonly decide: (state: S, tools: DecideTools) => Decided<R> | Promise<Decided<R>>;
  /** Retries on a conditional-append conflict or a transient store failure. Default 3. */
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
  /** Wait between attempts (jittered exponential back-off by default). Tests pass `() => Promise.resolve()`. */
  readonly backoff?: (attempt: number) => Promise<void>;
}

/**
 * The CCC command cycle: read the context → decide (pure) → record with `appendIf` →
 * on conflict re-read and decide again. Lifted from an earlier in-house store's `run-command`, with the
 * re-read replaced by the store's atomic guard.
 *
 * Retries are for version conflicts and transient store failures. A `UniqueViolationError`
 * is not retried: it is a constraint, not a version. If a retry might re-append after a lost
 * acknowledgement, give the events an `idempotencyKey`.
 */
export async function runCommand<S, R>(runtime: CommandRuntime, spec: CommandSpec<S, R>): Promise<CommandOutcome<R>> {
  const retries = spec.retries ?? 3;
  const clock = runtime.clock ?? (() => new Date());
  const backoff = runtime.backoff ?? defaultBackoff;
  let lastConflict: Conflict | undefined;
  for (let attempt = 0; attempt <= retries; attempt++) {
    if (attempt > 0) await backoff(attempt);
    let loaded: LoadedContext<S>;
    try {
      loaded = await loadContext(runtime, spec);
    } catch (err) {
      if (err instanceof TransientError && attempt < retries) continue;
      throw err;
    }
    const now = clock();
    const decided = await spec.decide(loaded.state, { now, id: () => uuidv7(now.getTime()), attempt });
    if (isRejection(decided)) return { ...decided, attempts: attempt + 1 };
    if (decided.events.length === 0) {
      return { ok: true, result: decided.result as R, appended: null, attempts: attempt + 1 };
    }
    try {
      const outcome = await runtime.store.appendIf(decided.events, loaded.ctx);
      if (outcome.ok) return { ok: true, result: decided.result as R, appended: outcome.appended, attempts: attempt + 1 };
      lastConflict = outcome.conflict;
    } catch (err) {
      if (err instanceof TransientError && attempt < retries) continue;
      throw err;
    }
  }
  return {
    ok: false,
    code: CONFLICT_CODE,
    reason: `context changed concurrently on all ${retries + 1} attempts`,
    ...(lastConflict ? { conflict: lastConflict } : {}),
    attempts: retries + 1,
  };
}

const rawFold: Fold<readonly RecordedEvent[]> = (events, state) => [...state, ...events];

async function loadContext<S, R>(runtime: CommandRuntime, spec: CommandSpec<S, R>): Promise<LoadedContext<S>> {
  if (spec.fold) {
    // `null` is a legitimate initial state ("nothing exists yet"); only `undefined` means "not given".
    const initial = (spec.initial !== undefined ? spec.initial : ([] as unknown)) as S | (() => S);
    if (runtime.cache && !spec.noCache) return runtime.cache.load<S>({ query: spec.context, fold: spec.fold, initial });
    const result = await runtime.store.query(spec.context);
    const base = typeof initial === "function" ? (initial as () => S)() : initial;
    return { state: spec.fold(result.events, base), ctx: result.ctx, delta: result.events, cacheHit: false };
  }
  const result = await runtime.store.query(spec.context);
  return { state: rawFold(result.events, []) as unknown as S, ctx: result.ctx, delta: result.events, cacheHit: false };
}

function defaultBackoff(attempt: number): Promise<void> {
  const ms = Math.min(1000, 20 * 2 ** attempt) * (0.5 + Math.random());
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Map an outcome to an HTTP status: 201 with events, 200 without, 404 missing, 409 conflict, 422 rejection. */
export function httpStatusOf(outcome: CommandOutcome<unknown> | Rejection): number {
  if (outcome.ok) return outcome.appended ? 201 : 200;
  if (outcome.code === CONFLICT_CODE) return 409;
  if (outcome.missing) return 404;
  return 422;
}
