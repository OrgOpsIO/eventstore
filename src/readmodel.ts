import { freshInitial, type Fold } from "./context.js";
import { FailedKeyError, TransientError, UsageError } from "./errors.js";
import { modelFingerprint, warnIfChanged, type TypeDefs } from "./fingerprint.js";
import { checkIdentity, decodeState } from "./foldstate.js";
import { readModelStoreOf, type ModelSnapshot, type ReadModelStore } from "./modelstore.js";
import type { KeyByOf } from "./registry.js";
import { filtersOf, IDENTIFIER, scopeValueOf } from "./query.js";
import { memoryCursors, subscribe, type ErrorDecision, type Subscription } from "./subscribe/index.js";
import type { Cursor, EventStore, Filter, Query, RecordedEvent, StoreSchema } from "./types.js";
import { handOut, isFatal, WatchMember, type Watch, type WatchOptions, type WatchPlaces } from "./watch.js";

/**
 * A declared read model: a fold kept per key, in memory, current through its feed. `get(k)` is
 * always what fold on read gives for `source ∧ keyBy = k` — a cache of your fold, never a second
 * source of truth.
 */
export interface ReadModelDefinition<S> {
  /** 1 to 200 characters without NUL — the rules of `defineFold`. */
  readonly name: string;
  /** An integer from 1 to 2 147 483 647. Bump it whenever what the fold computes changes: another version is another model. */
  readonly version: number;
  /** The events the model folds. It must not pin `keyBy` itself: `keyBy` does the keying. */
  readonly source: Query;
  /** A scope key: one state per value. An event's key is its scope value (`scopes[keyBy]`, its id for the id key, or a flat data field). */
  readonly keyBy: string;
  /** Per key: pure, never mutates its input state, and batch-associative — `fold(a ++ b, s)` equals `fold(b, fold(a, s))`. */
  readonly fold: Fold<S>;
  /** The state of a key before its first event (as `ContextSpec.initial`). */
  readonly initial: S | (() => S);
  /** Events per page of the feed (the rebuild included): a positive integer. Default 500. */
  readonly batchSize?: number;
  /** Called once, synchronously, when a key fails (its fold threw on one event): the dead letter. What it throws is swallowed. */
  readonly onFailedKey?: (error: FailedKeyError) => void;
  /**
   * Who writes a persisted model of this definition. Absent: one writer per view tenant (each
   * `es.forTenant(t).persistedReadModel(def)` elects its own). `"cross-tenant"`: ONE processor for every
   * tenant — the root view's handle (`es.persistedReadModel(def)`) writes each tenant's rows from one
   * subscription, and every `es.forTenant(t).persistedReadModel(def)` only reads. Live read models ignore it.
   */
  readonly processor?: "cross-tenant";
}

/** How a live read model is opened (`es.readModel(definition, options)`). */
export interface LiveReadModelOptions {
  /** Where the model starts: "rebuild" (default) replays the source from the beginning; "persisted" loads the persisted model of the same name and version (rows + checkpoint, one snapshot) and folds only what came after — falling back to a rebuild when there is nothing to start from. */
  readonly start?: "rebuild" | "persisted";
}

/** How fresh a read must be: wait until the feed has passed `atLeast` (your append's token), at most `timeoutMs`. */
export interface ReadModelReadOptions {
  /** A read-your-writes token `{ transactionId: appended.transactionId, sequence: appended.last }`. */
  readonly atLeast?: Cursor;
  /** Bounds the whole wait (the first rebuild included). Default 10 000; on expiry `TransientError` with code `read_model_timeout`. */
  readonly timeoutMs?: number;
}

/** A value `where` compares with `===`. */
type Equatable = string | number | boolean | null;

/** A state whose fields are not known to the type: `any`, `unknown`, or one with a `string` index. */
type Untyped<S> = 0 extends 1 & S ? true : unknown extends S ? true : string extends keyof S ? true : false;

/**
 * The `where` of a list over states `S` (decision 53): the fields with a value `where` can compare, each with
 * exactly that part of its type (a literal union stays one); `Date`, array and object fields are no keys. Plain
 * strings for an untyped state; no key for a state that is not an object. Distributes over a union state.
 */
type WhereOf<S> =
  Untyped<S> extends true
    ? Readonly<Record<string, Equatable>>
    : S extends object
      ? {
          readonly [F in keyof S & string as unknown extends S[F] ? F : [Extract<S[F], Equatable>] extends [never] ? never : F]?: unknown extends S[F]
            ? Equatable
            : Extract<S[F], Equatable>;
        }
      : Readonly<Record<string, never>>;

/** The `sort.by` of a list over states `S`: any field of an object state; `string` for an untyped state; none otherwise. */
type SortKeyOf<S> = Untyped<S> extends true ? string : S extends object ? keyof S & string : never;

/** A page of a read model: equality filters on top-level state fields, one sort, offset paging. Typed by the state `S`. */
export interface ListOptions<S = any> extends ReadModelReadOptions {
  /** AND over the entries: a key matches when its state has an own field with exactly this value (`===`). */
  readonly where?: WhereOf<S>;
  /** One field. Numbers < strings < booleans < Dates, strings by code point; missing values (and NaN, ±Infinity) last in both directions; ties by key ascending. */
  readonly sort?: { readonly by: SortKeyOf<S>; readonly dir?: "asc" | "desc" };
  /** Default 0. */
  readonly offset?: number;
  /** Default 50. */
  readonly limit?: number;
}

export interface ListRow<S> {
  readonly key: string;
  readonly state: S;
}

export interface ListResult<S> {
  readonly rows: readonly ListRow<S>[];
  /** Matching keys before paging. */
  readonly total: number;
  /** Failed keys of the model; never listed. */
  readonly failed: number;
  /** The model revision this answer is of: merge only `watchList` changes with a higher revision, from a `watchList` registered before this `list`. */
  readonly revision: number;
}

/** What one folded part changed for a list watcher. */
export interface ListChange<S> {
  /** The model revision after the change. */
  readonly revision: number;
  /** Keys that entered or changed and match `where` now, in the order the part first touched them. */
  readonly rows: readonly ListRow<S>[];
  /** Keys that matched before and no longer do, or that failed. */
  readonly removed: readonly string[];
}

export type ReadModelWatchOptions = Pick<WatchOptions, "onError" | "signal">;

export interface ReadModelStatus {
  readonly phase: "starting" | "live" | "failed" | "stopped";
  /** The feed's cursor (in RAM). */
  readonly cursor: Cursor | null;
  /** States held (failed keys not counted). */
  readonly keys: number;
  /** Failed keys. */
  readonly failed: number;
  /** Events handed to the fold in committed parts since the model started, the rebuild included. A read never adds to it. */
  readonly folded: number;
  /** Time spent in the fold since the model started, in milliseconds. */
  readonly foldMs: number;
  /** +1 per folded part that entered, changed or failed a key. */
  readonly revision: number;
  /** The failure, when `phase` is `"failed"`. */
  readonly error?: unknown;
}

/**
 * A live read model: kept in memory by one feed per (view, name, version), its position in RAM
 * only, rebuilt from the store after a restart. Reads fold nothing. Never decide on it: it hands
 * out no context; decisions stay on `es.context` / `es.command`.
 */
export interface LiveReadModel<S> {
  /** The key's state from RAM (`initial` for a key without an event); waits for the first rebuild, and for `atLeast`. Treat it as read-only. */
  get(key: string, options?: ReadModelReadOptions): Promise<S>;
  /** A page of the held keys. */
  list(options?: ListOptions<S>): Promise<ListResult<S>>;
  /** The key's current state first, then its new state after every folded part that changed it. Folded once, however many watch. */
  watch(key: string, onState: (state: S) => void | Promise<void>, options?: ReadModelWatchOptions): Promise<Watch>;
  /**
   * The keys that entered, changed or left `where`, after every folded part, from its registration on: no initial
   * delivery, no replay. To keep a page current, await this first and hold its changes, then take the page with
   * `list`, then apply only changes with a higher `revision` than the page's — the other way round, a change
   * between the two calls is lost.
   */
  watchList(where: Pick<ListOptions<S>, "where">, onChange: (change: ListChange<S>) => void | Promise<void>, options?: ReadModelWatchOptions): Promise<Watch>;
  readonly status: ReadModelStatus;
  /** Stops the feed, ends every watcher (without `onError`) and forgets the model: the next `readModel(def)` rebuilds. */
  stop(): Promise<void>;
}

export const DEFAULT_TIMEOUT_MS = 10_000;
export const DEFAULT_LIMIT = 50;
const MAX_WHERE = 64;
const KEPT_LISTS = 16;
export const START_BACKOFF_MS =[100, 200, 400, 800, 1_600, 3_200, 5_000];

/** Every definition `defineReadModel` returned; `readModel` takes any other object through it once. */
const defined = new WeakSet<object>();
const normalised = new WeakMap<object, ReadModelDefinition<unknown>>();

/**
 * Declare a read model. Define it once, at module level, like every fold; then
 * `es.forTenant(t).readModel(definition)` keeps it live in this process.
 *
 * ```ts
 * export const offers = defineReadModel({
 *   name: "offers", version: 1, source: sales.$filter(), keyBy: "offerId", initial: (): Offer => ({ status: "draft", amount: 0 }),
 *   fold: sales.$fold<Offer>({ OfferMade: (d, s) => ({ ...s, status: "open", amount: d.amount }), OfferAccepted: (_, s) => ({ ...s, status: "accepted" }) }),
 * });
 * ```
 *
 * With a `$filter(...)` source, `keyBy` must be a scope key one of the chosen event types declares, or a chosen type's
 * own id key: a typo is a compile error. Any other source (a `Filter` literal, `scope()`, `$scope()`) takes a plain string.
 */
export function defineReadModel<S, Q extends Query = Query>(
  definition: ReadModelDefinition<S> & { readonly source: Q; readonly keyBy: KeyByOf<Q> },
): ReadModelDefinition<S>;
export function defineReadModel<S>(definition: ReadModelDefinition<S>): ReadModelDefinition<S> {
  const { name, version, source, keyBy, fold, initial, batchSize, onFailedKey, processor } = definition ?? ({} as ReadModelDefinition<S>);
  checkIdentity("defineReadModel", name, version);
  const label = `eventstore: defineReadModel("${name}")`;
  if (typeof keyBy !== "string" || !IDENTIFIER.test(keyBy)) {
    throw new UsageError(`${label} needs keyBy, a scope key (letters, digits and _, not starting with a digit), got ${typeof keyBy === "string" ? `"${keyBy}"` : String(keyBy)}`);
  }
  let filters: Filter[];
  try {
    filters = filtersOf(source);
  } catch (error) {
    throw new UsageError(`${label} has an invalid source — ${error instanceof Error ? error.message : String(error)}`, { cause: error });
  }
  for (const filter of filters) {
    if (filter.scopes && Object.prototype.hasOwnProperty.call(filter.scopes, keyBy)) {
      throw new UsageError(`${label}: the source must not pin "${keyBy}"; keyBy does the keying — drop scopes.${keyBy} from the source`);
    }
  }
  if (typeof fold !== "function") throw new UsageError(`${label} needs a fold function`);
  if (batchSize !== undefined && !(Number.isSafeInteger(batchSize) && batchSize >= 1)) {
    throw new UsageError(`${label} needs a batchSize that is a positive integer (default 500), got ${String(batchSize)}`);
  }
  if (onFailedKey !== undefined && typeof onFailedKey !== "function") throw new UsageError(`${label}: onFailedKey must be a function`);
  if (processor !== undefined && processor !== "cross-tenant") {
    throw new UsageError(`${label}: processor must be "cross-tenant" (one processor writes every tenant's rows) or absent (one writer per tenant), got ${typeof processor === "string" ? `"${processor}"` : String(processor)}`);
  }
  const out: ReadModelDefinition<S> = Object.freeze({
    name,
    version,
    source,
    keyBy,
    fold,
    initial,
    ...(batchSize !== undefined ? { batchSize } : {}),
    ...(onFailedKey !== undefined ? { onFailedKey } : {}),
    ...(processor !== undefined ? { processor } : {}),
  });
  defined.add(out);
  return out;
}

/** The checked, frozen definition behind any definition object: the same object always gives the same one. */
export function definitionOf<S>(definition: ReadModelDefinition<S>): ReadModelDefinition<S> {
  if (typeof definition === "object" && definition !== null && defined.has(definition)) return definition;
  if (typeof definition !== "object" || definition === null) throw new UsageError("eventstore: readModel() needs a definition from defineReadModel({ name, version, source, keyBy, fold, initial })");
  let out = normalised.get(definition) as ReadModelDefinition<S> | undefined;
  if (!out) {
    out = defineReadModel(definition);
    normalised.set(definition, out as ReadModelDefinition<unknown>);
  }
  return out;
}

/** The key query `source ∧ keyBy = k`, filter by filter: what fold on read reads for one key of the model. */
export function keyQuery(source: Query, keyBy: string, key: string): Query {
  const pin = (filter: Filter): Filter => ({ ...filter, scopes: { ...(filter.scopes ?? {}), [keyBy]: key } });
  return Array.isArray(source) ? (source as readonly Filter[]).map(pin) : pin(source as Filter);
}

/** An event's key: its scope value for `keyBy` — the same semantics as the SQL matcher, so routing and the key query agree. */
export function keyOf(event: RecordedEvent, keyBy: string, schema: Pick<StoreSchema, "idKeyOf">): string | undefined {
  return scopeValueOf(event, keyBy, schema.idKeyOf(event.type));
}

/** What a model needs from the view it lives on. */
export interface ReadModelHost {
  readonly schema: Pick<StoreSchema, "idKeyOf">;
  /** The view's store: tenant narrowing and the rls session apply to every read of the feed. */
  readonly view: () => Promise<EventStore>;
  /** Places of the root api's watch hub: model watchers share `watch.maxWatchers` and `watch.maxPendingBatches` with `es.watch`. */
  readonly places: WatchPlaces;
  /** Called once when the model stops: it leaves the registry. */
  readonly onStop: () => void;
  /** Where it starts (decision 43); default `"rebuild"`. */
  readonly start?: "rebuild" | "persisted";
  /** The declared event types, for the fingerprint of a persisted start (decision 42). */
  readonly typeDefs?: TypeDefs;
}

/** A state row that would not decode: the start drops everything loaded and rebuilds (decision 43 §2.3). */
class Undecodable extends Error {}

interface Change<S> {
  readonly key: string;
  readonly had: boolean;
  readonly prev: S | undefined;
  readonly next: S;
  /** The state reference changed (an event the fold ignores keeps it). */
  readonly changed: boolean;
}

interface ListWatcher<S> {
  readonly where: readonly (readonly [string, unknown])[];
  readonly member: WatchMember<ListChange<S>>;
}

type Phase = ReadModelStatus["phase"];

/** The live read model (decisions 16–22). */
export class LiveModel<S> implements LiveReadModel<S> {
  private phase: Phase = "starting";
  private failure: unknown;
  private sub: Subscription | undefined;
  private readonly entries = new Map<string, S>();
  private readonly failedKeys = new Map<string, FailedKeyError>();
  private folded = 0;
  private foldMs = 0;
  private revision = 0;
  private ready!: Promise<void>;
  private settleReady!: (error?: unknown) => void;
  /** +1 per rebuild (decision 49): a feed, a start or a snapshot of an older one changes nothing. */
  private generation = 0;
  private readonly keyWatchers = new Map<string, Set<WatchMember<S>>>();
  private readonly listWatchers = new Set<ListWatcher<S>>();
  /** Every watcher, resolved or still waiting: ended on stop and on failure. */
  private readonly members = new Set<WatchMember<never>>();
  /** Sorted keys per (where, sort) at a revision; least recently used dropped beyond `KEPT_LISTS`. */
  private readonly lists = new Map<string, { revision: number; keys: string[] }>();

  constructor(
    readonly definition: ReadModelDefinition<S>,
    private readonly host: ReadModelHost,
  ) {
    this.newReady();
    void this.start();
  }

  private newReady(): void {
    this.ready = new Promise<void>((resolve, reject) => {
      this.settleReady = (error?: unknown) => (error === undefined ? resolve() : reject(error));
    });
    this.ready.catch(() => undefined); // a failure is reported to every read; nobody need be waiting
  }

  private get label(): string {
    return `read model "${this.definition.name}" v${this.definition.version}`;
  }

  /**
   * The in-place rebuild (decision 49 §2), what `invalidate()` on the model's view does: the feed stops, every state
   * and failed key is dropped, every watcher ends with `TransientError("read_model_rebuilt")`, and the start runs
   * again; reads wait for it. The handle stays the same object; the revision only grows. A failed or stopped model
   * stays so; a rebuild during a rebuild starts it again.
   */
  rebuild(): void {
    if (this.phase === "failed" || this.phase === "stopped") return;
    this.generation++;
    const sub = this.sub;
    this.sub = undefined;
    void sub?.stop();
    this.entries.clear();
    this.failedKeys.clear();
    this.lists.clear();
    this.revision++;
    const rebuilt = new TransientError("read_model_rebuilt");
    rebuilt.message = `eventstore: ${this.label} was rebuilt from the store (invalidate()) — a state you were given may be gone; watch again`;
    for (const member of [...this.members]) member.end(rebuilt);
    if (this.phase === "live") {
      this.phase = "starting";
      this.newReady();
    }
    void this.start();
  }

  private async start(): Promise<void> {
    const generation = this.generation;
    /** This start is still the model's own: not stopped, failed or replaced by a rebuild. */
    const current = () => this.phase === "starting" && this.generation === generation;
    let store: EventStore | undefined;
    for (let attempt = 0; store === undefined; attempt++) {
      try {
        store = await this.host.view();
      } catch (error) {
        if (!current()) return;
        if (isFatal(error)) return this.fail(error);
        await sleep(START_BACKOFF_MS[Math.min(attempt, START_BACKOFF_MS.length - 1)]!);
      }
      if (!current()) return;
    }
    let from: Cursor | "beginning" = "beginning";
    if (this.host.start === "persisted") {
      const cap = readModelStoreOf(store);
      if (!cap) {
        return this.fail(
          new UsageError(
            `eventstore: ${this.label}: start: "persisted" needs a store that keeps persisted read models — create it with persistedModels: true (new MemoryStore({ schema, persistedModels: true }), or configure({ postgres: { persistedModels: true } }))`,
          ),
        );
      }
      if (cap.refusal) return this.fail(cap.refusal);
      for (let attempt = 0; ; attempt++) {
        try {
          from = (await this.fromPersisted(cap, current)) ?? "beginning";
          break;
        } catch (error) {
          if (!current()) return;
          if (isFatal(error)) return this.fail(error);
          await sleep(START_BACKOFF_MS[Math.min(attempt, START_BACKOFF_MS.length - 1)]!);
        }
        if (!current()) return;
      }
      if (!current()) return;
    }
    const { name, version, source, batchSize } = this.definition;
    // a feed of an older generation (a rebuild stopped it) never touches the states again
    const sub = subscribe(`readmodel:${name}@v${version}`, source, (events) => (this.generation === generation ? this.apply(events) : undefined), {
      store,
      cursors: memoryCursors(), // position in RAM only: a restart is a rebuild (or a start from persisted rows)
      from,
      ...(batchSize !== undefined ? { batchSize } : {}),
      onError: (error, batch) => (this.generation === generation ? this.onFeedError(error, batch) : "stop"),
    });
    this.sub = sub;
    await sub.whenCaughtUp(); // resolves on a stop or a failure too: the phase says which
    if (current()) {
      this.phase = "live";
      this.settleReady();
    }
  }

  /**
   * Load the persisted model of this name and version (decision 43): every row and the checkpoint from one snapshot.
   * The checkpoint to follow from, or `null` — nothing to start from (no row, no checkpoint, retired, a row that does
   * not decode) — for the ordinary rebuild; then nothing loaded is kept. A start replaced meanwhile keeps nothing.
   */
  private async fromPersisted(cap: ReadModelStore, current: () => boolean): Promise<Cursor | null> {
    const { name, version } = this.definition;
    const entries = new Map<string, S>();
    const failed = new Map<string, FailedKeyError>();
    let snap: ModelSnapshot | null;
    try {
      snap = await cap.snapshot({ name, version }, (rows) => {
        for (const row of rows) {
          if (row.failed) {
            const m = row.failed;
            // the stored dead letter, carried over: onFailedKey ran in the writer's process already
            failed.set(row.key, new FailedKeyError({ name, version }, row.key, { type: m.type, id: m.id, sequence: m.sequence, transactionId: m.transactionId }, { cause: new Error(m.error) }));
            continue;
          }
          try {
            entries.set(row.key, decodeState(row.state!) as S);
          } catch (error) {
            throw new Undecodable("a state row does not decode", { cause: error });
          }
        }
      });
    } catch (error) {
      if (error instanceof Undecodable) return null;
      throw error;
    }
    if (snap === null || snap.checkpoint === null || snap.status === "retired" || !current()) return null;
    warnIfChanged("read model", name, version, snap.fingerprint, modelFingerprint(this.definition, this.host.typeDefs ?? {}));
    for (const [key, state] of entries) this.entries.set(key, state);
    for (const [key, error] of failed) this.failedKeys.set(key, error);
    return snap.checkpoint;
  }

  /** One part (a page, or a half during isolation): fold every key, then commit all of them or none. */
  private apply(events: readonly RecordedEvent[]): void {
    const { keyBy, fold, initial } = this.definition;
    const groups = new Map<string, RecordedEvent[]>();
    for (const event of events) {
      const key = keyOf(event, keyBy, this.host.schema);
      if (key === undefined || this.failedKeys.has(key)) continue;
      const group = groups.get(key);
      if (group) group.push(event);
      else groups.set(key, [event]);
    }
    if (groups.size === 0) return;
    const staged: Change<S>[] = [];
    let count = 0;
    const started = performance.now();
    try {
      for (const [key, group] of groups) {
        const had = this.entries.has(key);
        const prev = had ? (this.entries.get(key) as S) : freshInitial(initial);
        const next = fold(group, prev);
        staged.push({ key, had, prev: had ? prev : undefined, next, changed: !Object.is(next, prev) });
        count += group.length;
      }
    } finally {
      this.foldMs += performance.now() - started;
    }
    // every fold returned: commit, synchronously — a reader never sees half a part
    for (const change of staged) this.entries.set(change.key, change.next);
    this.folded += count;
    const changes = staged.filter((c) => !c.had || c.changed);
    if (changes.length === 0) return;
    this.revision++;
    this.notify(changes);
  }

  /** Hand one part's changes to the watchers: the same state objects to all, folded once. */
  private notify(changes: readonly Change<S>[]): void {
    for (const change of changes) {
      if (!change.changed) continue;
      const watchers = this.keyWatchers.get(change.key);
      if (watchers) for (const member of [...watchers]) member.deliver(change.next);
    }
    for (const watcher of [...this.listWatchers]) {
      const rows: ListRow<S>[] = [];
      const removed: string[] = [];
      const changed = this.filterFor(watcher, () => {
        for (const change of changes) {
          if (matches(change.next, watcher.where)) rows.push({ key: change.key, state: change.next });
          else if (change.had && matches(change.prev, watcher.where)) removed.push(change.key);
        }
        return rows.length > 0 || removed.length > 0;
      });
      if (changed) watcher.member.deliver({ revision: this.revision, rows, removed });
    }
  }

  /**
   * One list watcher's filter over committed states (decision 23): what `read` returns, or `false`
   * when it threw (a state field that throws when read). The part is committed, so that error ends
   * this watcher alone, with `onError`, and never reaches the feed — the feed would take it for a
   * fold error and fold the part again.
   */
  private filterFor(watcher: ListWatcher<S>, read: () => boolean): boolean {
    try {
      return read();
    } catch (error) {
      watcher.member.end(error);
      return false;
    }
  }

  /** The feed's error policy (decision 17): a page is isolated, the one event the fold fails on fails its key, a store error stops or retries. */
  private onFeedError(error: unknown, batch: readonly RecordedEvent[]): ErrorDecision {
    if (batch.length > 1) return "skip";
    if (batch.length === 1) {
      this.failKey(batch[0]!, error);
      return "skip";
    }
    if (isFatal(error)) {
      this.fail(error);
      return "stop";
    }
    return "retry";
  }

  private failKey(event: RecordedEvent, cause: unknown): void {
    const key = keyOf(event, this.definition.keyBy, this.host.schema);
    if (key === undefined || this.failedKeys.has(key)) return; // never folded, so it cannot fail
    const { name, version } = this.definition;
    const error = new FailedKeyError({ name, version }, key, { type: event.type, id: event.id, sequence: event.sequence, transactionId: event.transactionId }, { cause });
    const had = this.entries.has(key);
    const prev = this.entries.get(key);
    this.entries.delete(key);
    this.failedKeys.set(key, error);
    this.revision++;
    for (const member of [...(this.keyWatchers.get(key) ?? [])]) member.end(error);
    if (had) {
      for (const watcher of [...this.listWatchers]) {
        if (this.filterFor(watcher, () => matches(prev, watcher.where))) watcher.member.deliver({ revision: this.revision, rows: [], removed: [key] });
      }
    }
    try {
      this.definition.onFailedKey?.(error);
    } catch {
      // the dead-letter hook must never break the feed
    }
  }

  /** An error no retry fixes: every read rejects with it, every watcher ends with `onError`. */
  private fail(error: unknown): void {
    if (this.phase === "failed" || this.phase === "stopped") return;
    this.phase = "failed";
    this.failure = error;
    this.settleReady(error);
    for (const member of [...this.members]) member.end(error);
  }

  private stoppedError(): UsageError {
    return new UsageError(`eventstore: ${this.label} stopped — call readModel(definition) again for a fresh one`);
  }

  async stop(): Promise<void> {
    if (this.phase === "stopped") return;
    this.phase = "stopped";
    const stopped = this.stoppedError();
    this.settleReady(stopped);
    for (const member of [...this.members]) {
      member.failure = stopped;
      member.leave();
    }
    this.host.onStop();
    await this.sub?.stop();
  }

  private assertRunning(): void {
    if (this.phase === "failed") throw this.failure;
    if (this.phase === "stopped") throw this.stoppedError();
  }

  /** Wait for readiness and, with `atLeast`, until the feed has passed the token — all within `timeoutMs`. */
  private async settle(options: ReadModelReadOptions | undefined): Promise<void> {
    const token = options?.atLeast;
    const timeoutMs = options?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1) {
      throw new UsageError(`eventstore: ${this.label}: timeoutMs must be a positive integer (default ${DEFAULT_TIMEOUT_MS}), got ${String(timeoutMs)}`);
    }
    if (token !== undefined) checkToken(token);
    this.assertRunning();
    if (this.phase === "live" && (token === undefined || this.sub!.hasPassed(token))) return; // the warm path: no timer, no wait
    let timer: ReturnType<typeof setTimeout> | undefined;
    let expired = false;
    const deadline = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        expired = true;
        const error = new TransientError("read_model_timeout");
        const what = this.phase === "starting" ? "finished its rebuild" : `passed token ${token!.transactionId}/${token!.sequence}`;
        error.message = `eventstore: ${this.label} has not ${what} within ${timeoutMs} ms (timeoutMs) — the store is slow or down, or an older transaction is still open; retry`;
        reject(error);
      }, timeoutMs);
    });
    const wait = async (): Promise<void> => {
      for (let pause = 1; ; pause = Math.min(pause * 2, 50)) {
        await this.ready;
        this.assertRunning();
        if (this.phase !== "live") continue; // rebuilt meanwhile (invalidate()): wait for the new start
        if (token === undefined || expired || this.sub!.hasPassed(token)) return;
        await this.sub!.whenCaughtUp();
        this.assertRunning();
        if (this.phase !== "live") continue;
        if (expired || this.sub!.hasPassed(token)) return;
        // caught up, yet not passed: the token's transaction waits behind an older one; look again soon
        await sleep(pause);
      }
    };
    try {
      await Promise.race([wait(), deadline]);
    } finally {
      clearTimeout(timer);
    }
    this.assertRunning();
  }

  async get(key: string, options?: ReadModelReadOptions): Promise<S> {
    if (typeof key !== "string") throw new UsageError(`eventstore: ${this.label}: get() needs a string key, got ${typeof key}`);
    await this.settle(options);
    const failed = this.failedKeys.get(key);
    if (failed) throw failed;
    return this.entries.has(key) ? (this.entries.get(key) as S) : freshInitial(this.definition.initial);
  }

  async list(options: ListOptions = {}): Promise<ListResult<S>> {
    const where = whereOf(options.where, this.label);
    const sort = sortOf(options.sort, this.label);
    const offset = options.offset ?? 0;
    const limit = options.limit ?? DEFAULT_LIMIT;
    if (!Number.isSafeInteger(offset) || offset < 0) throw new UsageError(`eventstore: ${this.label}: list offset must be an integer of at least 0, got ${String(offset)}`);
    if (!Number.isSafeInteger(limit) || limit < 1) throw new UsageError(`eventstore: ${this.label}: list limit must be an integer of at least 1, got ${String(limit)}`);
    await this.settle(options);
    const shape = JSON.stringify([where, sort]);
    let kept = this.lists.get(shape);
    if (kept) this.lists.delete(shape); // to the most recently used end
    if (!kept || kept.revision !== this.revision) kept = { revision: this.revision, keys: this.sortedKeys(where, sort) };
    this.lists.set(shape, kept);
    while (this.lists.size > KEPT_LISTS) this.lists.delete(this.lists.keys().next().value!);
    const rows = kept.keys.slice(offset, offset + limit).map((key) => ({ key, state: this.entries.get(key) as S }));
    return { rows, total: kept.keys.length, failed: this.failedKeys.size, revision: this.revision };
  }

  private sortedKeys(where: readonly (readonly [string, unknown])[], sort: { by: string; dir: "asc" | "desc" } | null): string[] {
    const picked: { key: string; rank: number; value: unknown }[] = [];
    for (const [key, state] of this.entries) {
      if (!matches(state, where)) continue;
      if (!sort) {
        picked.push({ key, rank: 0, value: undefined });
        continue;
      }
      const value = state !== null && typeof state === "object" && Object.prototype.hasOwnProperty.call(state, sort.by) ? (state as Record<string, unknown>)[sort.by] : undefined;
      picked.push({ key, rank: rankOf(value), value });
    }
    const desc = sort?.dir === "desc" ? -1 : 1;
    picked.sort((a, b) => compareListed(a, b, desc));
    return picked.map((p) => p.key);
  }

  async watch(key: string, onState: (state: S) => void | Promise<void>, options: ReadModelWatchOptions = {}): Promise<Watch> {
    if (typeof key !== "string") throw new UsageError(`eventstore: ${this.label}: watch() needs a string key, got ${typeof key}`);
    if (typeof onState !== "function") throw new UsageError(`eventstore: ${this.label}: watch() needs a callback`);
    this.assertRunning();
    const release = this.host.places.admit();
    if (options.signal?.aborted) {
      release();
      throw options.signal.reason;
    }
    const member: WatchMember<S> = new WatchMember<S>(onState, options.onError, this.host.places.maxPendingBatches, () => {
      this.members.delete(member as WatchMember<never>);
      const set = this.keyWatchers.get(key);
      set?.delete(member);
      if (set?.size === 0) this.keyWatchers.delete(key);
      release();
    });
    this.members.add(member as WatchMember<never>);
    return handOut(member, this.ready, options.signal, () => {
      const failed = this.failedKeys.get(key);
      if (failed) return member.end(failed);
      // the current state first, in the same tick as the registration: no change falls in between
      member.deliver(this.entries.has(key) ? (this.entries.get(key) as S) : freshInitial(this.definition.initial));
      const set = this.keyWatchers.get(key);
      if (set) set.add(member);
      else this.keyWatchers.set(key, new Set([member]));
    });
  }

  async watchList(filter: Pick<ListOptions, "where">, onChange: (change: ListChange<S>) => void | Promise<void>, options: ReadModelWatchOptions = {}): Promise<Watch> {
    const where = whereOf(filter?.where, this.label);
    if (typeof onChange !== "function") throw new UsageError(`eventstore: ${this.label}: watchList() needs a callback`);
    this.assertRunning();
    const release = this.host.places.admit();
    if (options.signal?.aborted) {
      release();
      throw options.signal.reason;
    }
    const member: WatchMember<ListChange<S>> = new WatchMember<ListChange<S>>(onChange, options.onError, this.host.places.maxPendingBatches, () => {
      this.members.delete(member as WatchMember<never>);
      this.listWatchers.delete(watcher);
      release();
    });
    const watcher: ListWatcher<S> = { where, member };
    this.members.add(member as WatchMember<never>);
    return handOut(member, this.ready, options.signal, () => {
      this.listWatchers.add(watcher);
    });
  }

  get status(): ReadModelStatus {
    return {
      phase: this.phase,
      cursor: this.sub?.cursor ?? null,
      keys: this.entries.size,
      failed: this.failedKeys.size,
      folded: this.folded,
      foldMs: this.foldMs,
      revision: this.revision,
      ...(this.phase === "failed" ? { error: this.failure } : {}),
    };
  }
}

/** A persisted handle as the registry sees it (`PersistedModel` in persisted.ts). */
export interface PersistedHandle {
  readonly definition: ReadModelDefinition<unknown>;
  stop(): Promise<void>;
  /** Forget the in-process `get` copies (decision 49 §1.3). */
  dropCopies(): void;
}

/** The models of one root api: live ones per (tenant of the view, name, version), persisted handles likewise, keyed apart. */
export class ReadModelRegistry {
  private readonly models = new Map<string, LiveModel<unknown>>();
  private readonly persisted = new Map<string, PersistedHandle>();
  private closed = false;

  /** The persisted handle of (tenant, name, version): the same definition gives the same handle; `create` makes a new one. */
  openPersisted<H extends PersistedHandle>(tenantId: string | undefined, definition: ReadModelDefinition<unknown>, create: (def: ReadModelDefinition<unknown>, onStop: () => void) => H): H {
    if (this.closed) throw new UsageError("eventstore: the store is closed");
    const def = definitionOf(definition);
    const id = JSON.stringify(["persisted", tenantId ?? null, def.name, def.version]);
    const existing = this.persisted.get(id);
    if (existing) {
      if (existing.definition === def) return existing as H;
      throw new UsageError(`eventstore: two definitions of persisted read model "${def.name}" v${def.version}; bump the version or reuse the definition`);
    }
    const handle: H = create(def, () => {
      if (this.persisted.get(id) === handle) this.persisted.delete(id);
    });
    this.persisted.set(id, handle);
    return handle;
  }

  open<S>(tenantId: string | undefined, definition: ReadModelDefinition<S>, host: Omit<ReadModelHost, "onStop">): LiveReadModel<S> {
    if (this.closed) throw new UsageError("eventstore: the store is closed");
    const def = definitionOf(definition);
    const id = JSON.stringify([tenantId ?? null, def.name, def.version]);
    const existing = this.models.get(id);
    if (existing) {
      if (existing.definition === def) return existing as unknown as LiveReadModel<S>;
      throw new UsageError(`eventstore: two definitions of read model "${def.name}" v${def.version}; bump the version or reuse the definition`);
    }
    const model: LiveModel<S> = new LiveModel<S>(def, {
      ...host,
      onStop: () => {
        if (this.models.get(id) === (model as LiveModel<unknown>)) this.models.delete(id);
      },
    });
    this.models.set(id, model as LiveModel<unknown>);
    return model;
  }

  /**
   * `invalidate()` of one view (decision 49): its live models rebuilt in place, its persisted handles' `get` copies
   * dropped. `tenantId` is the view's (`undefined`: the root view); other views' models are left alone.
   */
  invalidateView(tenantId: string | undefined): void {
    const tenant = tenantId ?? null;
    for (const [id, model] of [...this.models]) if ((JSON.parse(id) as unknown[])[0] === tenant) model.rebuild();
    for (const [id, handle] of [...this.persisted]) if ((JSON.parse(id) as unknown[])[1] === tenant) handle.dropCopies();
  }

  /** Stops every model (the root api closes): persisted handles first (their writer locks released), then the live ones. */
  async close(): Promise<void> {
    this.closed = true;
    const handles = [...this.persisted.values()];
    this.persisted.clear();
    await Promise.all(handles.map((handle) => handle.stop()));
    const models = [...this.models.values()];
    this.models.clear();
    await Promise.all(models.map((model) => model.stop()));
  }
}

export const MISSING = 4;

/** number 0 < string 1 < boolean 2 < Date 3; anything else (absent, null, NaN, ±Infinity, an invalid Date) is missing. */
export function rankOf(value: unknown): number {
  if (typeof value === "number") return Number.isFinite(value) ? 0 : MISSING;
  if (typeof value === "string") return 1;
  if (typeof value === "boolean") return 2;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? MISSING : 3;
  return MISSING;
}

export function compareSame(a: unknown, b: unknown, rank: number): number {
  switch (rank) {
    case 0:
      return (a as number) < (b as number) ? -1 : (a as number) > (b as number) ? 1 : 0;
    case 1:
      return compareCodePoints(a as string, b as string); // never locale: the order of Postgres' COLLATE "C"
    case 2:
      return Number(a) - Number(b);
    default:
      return (a as Date).getTime() - (b as Date).getTime();
  }
}

/**
 * Two strings by Unicode code point (not UTF-16 code unit): the order of Postgres' `COLLATE "C"` in
 * a UTF-8 database. The two differ only where a surrogate pair (above U+FFFF) meets U+E000–U+FFFF.
 */
export function compareCodePoints(a: string, b: string): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a.charCodeAt(i);
    const y = b.charCodeAt(i);
    if (x !== y) return codePointRank(x) - codePointRank(y);
  }
  return a.length - b.length;
}

/** Surrogates (U+D800–U+DFFF) move above U+E000–U+FFFF, so code units compare as code points do. */
function codePointRank(unit: number): number {
  return unit < 0xd800 ? unit : unit >= 0xe000 ? unit - 0x800 : unit + 0x2000;
}

/** List order of two entries: rank first (missing last both ways), then the value, ties by key ascending. */
export function compareListed(a: { key: string; rank: number; value: unknown }, b: { key: string; rank: number; value: unknown }, desc: 1 | -1): number {
  if (a.rank !== b.rank) return a.rank === MISSING ? 1 : b.rank === MISSING ? -1 : (a.rank - b.rank) * desc;
  const byValue = a.rank === MISSING ? 0 : compareSame(a.value, b.value, a.rank) * desc;
  return byValue !== 0 ? byValue : compareCodePoints(a.key, b.key);
}

/** A NUL or a lone surrogate: what a Postgres `jsonb` string cannot hold. */
const JSONB_UNSAFE = /\u0000|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

/**
 * The list form of a state (decision 30): its own enumerable top-level fields that are a string
 * Postgres can hold, a finite number (`-0` as `0`), a boolean, `null`, or a valid Date (as
 * `{ $date: ms }`). Everything else is left out, so it counts as missing for `where` and `sort`.
 */
export function docOf(state: unknown): Record<string, unknown> {
  const doc: Record<string, unknown> = {};
  if (state === null || typeof state !== "object" || Array.isArray(state)) return doc;
  for (const field of Object.keys(state)) {
    if (JSONB_UNSAFE.test(field)) continue;
    const value = (state as Record<string, unknown>)[field];
    let kept: unknown;
    if (typeof value === "string") {
      if (JSONB_UNSAFE.test(value)) continue;
      kept = value;
    } else if (typeof value === "number") {
      if (!Number.isFinite(value)) continue;
      kept = value === 0 ? 0 : value;
    } else if (typeof value === "boolean" || value === null) {
      kept = value;
    } else if (value instanceof Date) {
      const time = value.getTime();
      if (Number.isNaN(time)) continue;
      kept = { $date: time };
    } else {
      continue;
    }
    // defineProperty: a field named "__proto__" stays a field
    Object.defineProperty(doc, field, { value: kept, enumerable: true, writable: true, configurable: true });
  }
  return doc;
}

/** One field of a doc as `rankOf` / `compareSame` read it: `{ $date }` back to a Date. */
export function docValue(doc: Record<string, unknown>, field: string): unknown {
  if (!Object.prototype.hasOwnProperty.call(doc, field)) return undefined;
  const value = doc[field];
  return value !== null && typeof value === "object" ? new Date((value as { $date: number }).$date) : value;
}

export function matches(state: unknown, where: readonly (readonly [string, unknown])[]): boolean {
  if (where.length === 0) return true;
  if (state === null || typeof state !== "object") return false;
  for (const [field, value] of where) {
    if (!Object.prototype.hasOwnProperty.call(state, field) || (state as Record<string, unknown>)[field] !== value) return false;
  }
  return true;
}

/** `where` checked and in a canonical order (sorted fields), so equal filters share a kept list. */
export function whereOf(where: ListOptions["where"], label: string): (readonly [string, unknown])[] {
  if (where === undefined) return [];
  if (where === null || typeof where !== "object" || Array.isArray(where)) throw new UsageError(`eventstore: ${label}: where must be an object of field → value`);
  const entries = Object.entries(where);
  if (entries.length > MAX_WHERE) throw new UsageError(`eventstore: ${label}: where may have at most ${MAX_WHERE} fields, got ${entries.length}`);
  for (const [field, value] of entries) {
    const ok = value === null || typeof value === "string" || typeof value === "boolean" || (typeof value === "number" && Number.isFinite(value));
    if (!ok) throw new UsageError(`eventstore: ${label}: where.${field} must be a string, a finite number, a boolean or null — list() filters by equality only`);
  }
  return entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
}

export function sortOf(sort: ListOptions["sort"], label: string): { by: string; dir: "asc" | "desc" } | null {
  if (sort === undefined) return null;
  if (sort === null || typeof sort !== "object" || typeof sort.by !== "string" || sort.by.length === 0) {
    throw new UsageError(`eventstore: ${label}: sort needs { by: "<field>", dir?: "asc" | "desc" }`);
  }
  const dir = sort.dir ?? "asc";
  if (dir !== "asc" && dir !== "desc") throw new UsageError(`eventstore: ${label}: sort.dir must be "asc" or "desc", got ${String(dir)}`);
  return { by: sort.by, dir };
}

export function checkToken(token: Cursor): void {
  if (!token || typeof token.transactionId !== "string" || !/^\d+$/.test(token.transactionId) || !Number.isSafeInteger(token.sequence)) {
    throw new UsageError(
      "eventstore: atLeast needs a Cursor { transactionId, sequence } — build it from an append: { transactionId: appended.transactionId, sequence: appended.last }",
    );
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
