import { freshInitial } from "./context.js";
import { ContextTooLargeError, FailedKeyError, TransientError, UsageError } from "./errors.js";
import { checkStorable, decodeState, encodeState } from "./foldstate.js";
import { cursorOf } from "./memory.js";
import { warnIfChanged } from "./fingerprint.js";
import {
  FencedError,
  RetiredError,
  docMatches,
  failedText,
  pairKey,
  readModelStoreOf,
  type ChangedRow,
  type Claim,
  type CrossTenantStore,
  type FailedMarker,
  type ModelRef,
  type ReadModelStore,
  type RowWrite,
  type StoredRow,
  type TenantRowWrite,
} from "./modelstore.js";
import { compareCursor, queryKey } from "./query.js";
import {
  checkToken,
  DEFAULT_LIMIT,
  DEFAULT_TIMEOUT_MS,
  docOf,
  keyOf,
  keyQuery,
  sleep,
  sortOf,
  START_BACKOFF_MS,
  whereOf,
  type ListChange,
  type ListOptions,
  type ListResult,
  type ListRow,
  type ReadModelDefinition,
  type ReadModelWatchOptions,
} from "./readmodel.js";
import { subscribe, type ErrorDecision, type Subscription } from "./subscribe/index.js";
import type { Cursor, EventStore, RecordedEvent, StoreSchema } from "./types.js";
import { handOut, isFatal, WatchMember, type Watch, type WatchPlaces } from "./watch.js";

/** What `status()` reports: this handle, and the model as the store holds it (decision 32). */
export interface PersistedReadModelStatus {
  /** This handle: `"live"` from the start (reads work at once), `"failed"`, or `"stopped"`. */
  readonly phase: "live" | "failed" | "stopped";
  /** This process: the one writer of the model, or a standby that reads and takes over. */
  readonly role: "writer" | "standby";
  /** Stored: the last event applied or skipped; `null` before the first page. */
  readonly checkpoint: Cursor | null;
  /** Stored: the current writer's claim (+1 per new writer). */
  readonly epoch: number;
  /** Stored: +1 per page that entered, changed or failed a key. */
  readonly revision: number;
  /** Stored: failed keys. */
  readonly failed: number;
  /** Stored: this version builds beside an active version of the same name — `list` and watches wait until it has caught up (blue-green). `false` once it switched, or when it had nothing to replace. */
  readonly building: boolean;
  readonly lag: {
    /**
     * Source events beyond the checkpoint: counted when at most 1 000, else an upper bound (one per sequence above the
     * checkpoint's up to the head's, plus those of younger transactions below it). Never fewer than are pending; 0 when caught up.
     */
    readonly events: number;
    /** Age of the oldest source event beyond the checkpoint (this process's clock against the stored `recordedAt`). 0 when caught up. */
    readonly ms: number;
  };
  /** Events this process's writer folded in committed pages since the handle opened. */
  readonly folded: number;
  /** Its fold time, in milliseconds. */
  readonly foldMs: number;
  /** Events `get` folded from the store's delta in this process since the handle opened: 0 more per warm read. */
  readonly readFolded: number;
  /** The failure, when `phase` is `"failed"`. */
  readonly error?: unknown;
}

/**
 * A persisted read model: state rows plus a checkpoint in the store, written in one transaction by
 * the one writer of the model, so it resumes after a restart and never double-applies. Any process
 * reads it. Never decide on it: it hands out no context; decisions stay on `es.context` / `es.command`.
 */
export interface PersistedReadModel<S> {
  /** The stored row plus the key's events beyond the model's checkpoint, folded on top (Marten's `FetchLatest`): always current, no waiting. Treat it as read-only. */
  get(key: string): Promise<S>;
  /** A page of the stored rows: as current as the checkpoint; `atLeast` waits until the model has passed your append's token. */
  list(options?: ListOptions<S>): Promise<ListResult<S>>;
  /** The key's stored state first, then its new state after every page that changed it — in any process, folded once by the writer. */
  watch(key: string, onState: (state: S) => void | Promise<void>, options?: ReadModelWatchOptions): Promise<Watch>;
  /**
   * The keys that entered, changed or left `where`, after every committed page, from its registration on. Register it
   * first, then take the page with `list`, then apply only changes with a higher `revision`. `removed` may name a key
   * that did not match before when it changed twice within one notification.
   */
  watchList(where: Pick<ListOptions<S>, "where">, onChange: (change: ListChange<S>) => void | Promise<void>, options?: ReadModelWatchOptions): Promise<Watch>;
  /** The checkpoint, epoch, revision and lag from the store, and this process's counters. */
  status(): Promise<PersistedReadModelStatus>;
  /** Stops this process's writer (the writer lock is released), its follower and its watchers; forgets the handle. The rows stay. */
  stop(): Promise<void>;
}

/** What a persisted model needs from the view it lives on. */
export interface PersistedModelHost {
  readonly schema: Pick<StoreSchema, "idKeyOf">;
  /** The view's store: tenant narrowing, the rls session and the tenant-stamped `readModelStore` apply. */
  readonly view: () => Promise<EventStore>;
  readonly tenantId: string | undefined;
  readonly places: WatchPlaces;
  /** The in-process copies of `get` (decision 29 §2) and the delta limit, from `contextCache`. */
  readonly copies: { readonly max: number; readonly maxBytes: number; readonly sizeOf: (state: unknown) => number; readonly maxEvents: number };
  /** The definition's fingerprint (decision 42): written at the claim, compared with the stored one. */
  readonly fingerprint: string;
  /** The tenant scope key (`configure({ tenant })`): a cross-tenant processor writes each event to the tenant it names. */
  readonly tenantKey: string | undefined;
  readonly onStop: () => void;
}

/** A fold, storability check or encoding threw for one key: isolate the page, fail the key (decision 28 §3.4). */
class FoldError extends Error {
  override readonly name = "FoldError";
  constructor(
    readonly key: string,
    cause: unknown,
  ) {
    super(`the fold of key "${key}" threw`, { cause });
  }
}

const FOLLOW_POLL_MS = 500;
/** The longest pause of a wait for the switch between two looks at the model row (a ring ends it sooner). */
const SWITCH_POLL_MS = 500;
const CHANGES_PAGE = 1_000;
const FOLLOW_RETRY_MS = 500;
/** Pending events `status()` counts one by one; beyond, `lag.events` is an upper bound. */
const LAG_COUNTED = 1_000;

interface Copy<S> {
  readonly state: S;
  readonly cursor: Cursor | null;
  readonly bytes: number;
}

/** The bounded copies of `get`, least recently used evicted first. */
class Copies<S> {
  private readonly entries = new Map<string, Copy<S>>();
  private bytes = 0;

  constructor(private readonly limits: PersistedModelHost["copies"]) {}

  get(key: string): Copy<S> | undefined {
    const copy = this.entries.get(key);
    if (copy) {
      this.entries.delete(key);
      this.entries.set(key, copy);
    }
    return copy;
  }

  set(key: string, state: S, cursor: Cursor | null): void {
    if (this.limits.max <= 0) return;
    this.drop(key);
    const copy = { state, cursor, bytes: this.limits.sizeOf(state) };
    this.entries.set(key, copy);
    this.bytes += copy.bytes;
    while (this.entries.size > this.limits.max || (this.bytes > this.limits.maxBytes && this.entries.size > 1)) {
      const oldest = this.entries.keys().next().value;
      if (oldest === undefined || oldest === key) break;
      this.drop(oldest);
    }
  }

  clear(): void {
    this.entries.clear();
    this.bytes = 0;
  }

  private drop(key: string): void {
    const old = this.entries.get(key);
    if (!old) return;
    this.bytes -= old.bytes;
    this.entries.delete(key);
  }
}

interface KeyWatcher<S> {
  readonly key: string;
  member: WatchMember<S>;
  /** The revision of the last state delivered; changes at or below it are dropped. */
  last: number;
  /** Changes the follower handed over before the first delivery; `null` after it. */
  held: ChangedRow[] | null;
}

interface ListWatcher<S> {
  readonly where: readonly (readonly [string, unknown])[];
  readonly member: WatchMember<ListChange<S>>;
}

interface Follower {
  seen: number;
  ready: Promise<void>;
  off: () => void;
  timer: ReturnType<typeof setInterval> | undefined;
  reading: boolean;
  again: boolean;
}

type Phase = PersistedReadModelStatus["phase"];

/** The persisted read model (decisions 25–32): one handle per (view tenant, name, version) per process. */
export class PersistedModel<S> implements PersistedReadModel<S> {
  private phase: Phase = "live";
  private role: "writer" | "standby" = "standby";
  private failure: unknown;
  private store: EventStore | undefined;
  private cap: ReadModelStore | undefined;
  private readonly ref: ModelRef;
  private readonly ready: Promise<void>;
  private settleReady!: (error?: unknown) => void;
  private epoch: number | null = null;
  private sub: Subscription | undefined;
  private tickTimer: ReturnType<typeof setTimeout> | undefined;
  private claiming = false;
  private detach: (() => void) | undefined;
  private folded = 0;
  private foldMs = 0;
  private readFolded = 0;
  private readonly copies: Copies<S>;
  private follower: Follower | undefined;
  private readonly keyWatchers = new Map<string, Set<KeyWatcher<S>>>();
  private readonly listWatchers = new Set<ListWatcher<S>>();
  /** Every watcher, resolved or still waiting: ended on stop and on failure. */
  private readonly members = new Set<WatchMember<never>>();
  /** The model row was seen `active`: it never goes back to `building` (decision 38), so the gate reads nothing more. */
  private activeSeen = false;
  /** The writer of a `building` version: the switch token `T` it must pass (decision 39); `null` while it is being read. */
  private switchToken: Cursor | null | undefined;
  private activating = false;
  /**
   * Who this handle is (decision 45 §2): `"own"` — the one writer of its view's tenant or a standby of it (stage 4);
   * `"processor"` — the root handle of a cross-tenant definition, writing every tenant; `"reader"` — a tenant handle of
   * a cross-tenant definition, which never writes.
   */
  private readonly mode: "own" | "processor" | "reader";
  /** The processor's backend, once started. */
  private cross: CrossTenantStore | undefined;

  constructor(
    readonly definition: ReadModelDefinition<S>,
    private readonly host: PersistedModelHost,
  ) {
    this.mode = definition.processor !== "cross-tenant" ? "own" : host.tenantId === undefined ? "processor" : "reader";
    this.ref = { ...(host.tenantId !== undefined ? { tenant: host.tenantId } : {}), name: definition.name, version: definition.version };
    this.copies = new Copies<S>(host.copies);
    this.ready = new Promise<void>((resolve, reject) => {
      this.settleReady = (error?: unknown) => (error === undefined ? resolve() : reject(error));
    });
    this.ready.catch(() => undefined); // a failure is reported to every read; nobody need be waiting
    void this.start();
  }

  private get label(): string {
    return `persisted read model "${this.definition.name}" v${this.definition.version}`;
  }

  private async start(): Promise<void> {
    let store: EventStore | undefined;
    for (let attempt = 0; store === undefined; attempt++) {
      try {
        store = await this.host.view();
      } catch (error) {
        if (isFatal(error)) return this.fail(error);
        await sleep(START_BACKOFF_MS[Math.min(attempt, START_BACKOFF_MS.length - 1)]!);
      }
      if (this.phase !== "live") return;
    }
    const cap = readModelStoreOf(store);
    if (!cap) {
      return this.fail(
        new UsageError(
          `eventstore: ${this.label}: the store does not keep persisted read models — create it with persistedModels: true (new MemoryStore({ schema, persistedModels: true }), or configure({ postgres: { persistedModels: true } }))`,
        ),
      );
    }
    if (this.mode === "processor") {
      // the root refuses per-tenant models under rls; the processor asks for its own backend instead (decision 46 §4)
      if (this.host.tenantKey === undefined) {
        return this.fail(
          new UsageError(
            `eventstore: ${this.label} is cross-tenant (processor: "cross-tenant"): its processor writes each event to the tenant it names, so it needs configure({ tenant: { scopeKey } })`,
          ),
        );
      }
      if (typeof cap.crossTenant !== "function") return this.fail(new UsageError(`eventstore: ${this.label}: this store cannot run a cross-tenant processor`));
      for (let attempt = 0; this.cross === undefined; attempt++) {
        try {
          this.cross = await cap.crossTenant();
        } catch (error) {
          if (isFatal(error)) return this.fail(error);
          await sleep(START_BACKOFF_MS[Math.min(attempt, START_BACKOFF_MS.length - 1)]!);
        }
        if (this.phase !== "live") return;
      }
    } else if (cap.refusal) return this.fail(cap.refusal);
    this.store = store;
    this.cap = cap;
    const detachHandle = cap.attach(this);
    const offLost = cap.onLost(() => this.lost());
    this.detach = () => {
      this.detach = undefined;
      offLost();
      detachHandle();
    };
    this.settleReady();
    if (this.mode !== "reader") void this.tick(); // a reader never takes the writer lock: the processor writes its rows
  }

  /** The model row's backend: the processor's (its row, failed keys over every tenant) or the view's. */
  private heads(): Pick<CrossTenantStore, "head" | "activeSibling" | "activate"> {
    return this.cross ?? this.cap!;
  }

  /** The event's tenant: its value for the tenant scope key, as a tenant view narrows by; `undefined` (and `''`): no tenant view reads it. */
  private tenantOf(event: RecordedEvent): string | undefined {
    const tenant = keyOf(event, this.host.tenantKey!, this.host.schema);
    return tenant === "" ? undefined : tenant;
  }

  /** The reads of a cross-tenant model are per tenant: the processor handle has none. */
  private refuseRead(what: string): void {
    if (this.mode !== "processor") return;
    throw new UsageError(`eventstore: ${this.label}: ${what}() on the processor — a cross-tenant model is read per tenant: es.forTenant(t).persistedReadModel(def)`);
  }

  // ── one writer (decision 27) ──────────────────────────────────────────────

  /** A standby's try: take the writer lock and claim the next epoch, or try again at the next tick. */
  private async tick(): Promise<void> {
    this.tickTimer = undefined;
    if (this.phase !== "live" || this.role === "writer" || this.claiming || !this.cap) return;
    this.claiming = true;
    try {
      const claim = await (this.cross ?? this.cap).lock(this.ref, this, this.host.fingerprint);
      if (claim !== null) {
        if (this.phase !== "live") {
          await this.cap.unlock(this.ref, this).catch(() => undefined);
          return;
        }
        this.compareFingerprint(claim.fingerprint);
        this.becomeWriter(claim);
        return;
      }
    } catch (error) {
      if (isFatal(error)) return this.fail(error);
      // transient: the next tick tries again
    } finally {
      this.claiming = false;
    }
    this.scheduleTick();
  }

  private scheduleTick(): void {
    if (this.phase !== "live" || this.role === "writer" || this.tickTimer || !this.cap) return;
    this.tickTimer = setTimeout(() => void this.tick(), this.cap.knobs.tickMs);
    this.tickTimer.unref?.();
  }

  /** The processor (decision 28): processor-mode `subscribe` from the stored checkpoint, every page fenced by `epoch`. */
  private becomeWriter(claim: Claim): void {
    const epoch = claim.epoch;
    this.role = "writer";
    this.epoch = epoch;
    const { name, version, source, batchSize } = this.definition;
    const heads = this.heads();
    const across = this.mode === "processor";
    const sub = subscribe(`readmodel:${name}@v${version}`, source, (events) => (across ? this.applyAcross(events, epoch) : this.applyPage(events, epoch)), {
      store: this.store!,
      target: {
        load: async () => (await heads.head(this.ref))?.cursor ?? null,
        skip: (event, error) => (across ? this.skipAcross(event, error, epoch) : this.skipEvent(event, error, epoch)),
      },
      from: "beginning",
      ...(batchSize !== undefined ? { batchSize } : {}),
      onError: (error, batch) => this.onFeedError(error, batch, epoch),
    });
    this.sub = sub;
    if (claim.status === "building") void this.buildBeside(sub, epoch);
  }

  /**
   * The writer of a `building` version (decision 39): take the switch token `T` — the newest event of the source
   * now — and activate the version once the feed has passed it, checked after every committed part and whenever the
   * feed caught up.
   */
  private async buildBeside(sub: Subscription, epoch: number): Promise<void> {
    this.switchToken = null;
    const mine = () => this.epoch === epoch && this.sub === sub && this.phase === "live";
    for (let attempt = 0; mine(); attempt++) {
      try {
        const top = await this.store!.query(this.definition.source, { order: "desc", limit: 1, payload: false, version: false });
        const newest = top.events[0];
        if (!mine()) return;
        if (newest === undefined) return void this.activateNow(epoch); // nothing to catch up with
        this.switchToken = { transactionId: newest.transactionId, sequence: newest.sequence };
        break;
      } catch (error) {
        if (isFatal(error)) return this.fail(error);
        await sleep(START_BACKOFF_MS[Math.min(attempt, START_BACKOFF_MS.length - 1)]!);
      }
    }
    this.checkSwitch(epoch, null);
    while (mine() && this.switchToken) {
      await sub.whenCaughtUp(); // resolves on a stop too
      if (!mine()) return;
      this.checkSwitch(epoch, null);
      if (this.switchToken) await sleep(50); // caught up, not passed yet: the token's transaction waits behind an older one
    }
  }

  /** After a committed part (its cursor) or a catch-up: the feed has passed `T` → activate, once. */
  private checkSwitch(epoch: number, committed: Cursor | null): void {
    const token = this.switchToken;
    if (!token || this.epoch !== epoch || this.activating) return;
    const passed = (committed !== null && compareCursor(committed, token) >= 0) || this.sub?.hasPassed(token) === true;
    if (passed) this.activateNow(epoch);
  }

  private activateNow(epoch: number): void {
    if (this.activating) return;
    this.activating = true;
    this.heads().activate(this.ref, epoch).then(
      () => {
        this.activating = false;
        if (this.epoch === epoch) this.switchToken = undefined;
        this.activeSeen = true;
      },
      (error: unknown) => {
        this.activating = false;
        if (this.epoch !== epoch) return;
        if (error instanceof FencedError) return this.demote();
        if (isFatal(error)) return this.fail(error);
        // transient: retried at the next committed part or catch-up
      },
    );
  }

  /** Back to standby: the feed stops, the lock is released if still held, the next tick competes again. */
  private demote(): void {
    if (this.role !== "writer") return;
    this.role = "standby";
    this.epoch = null;
    this.switchToken = undefined;
    const sub = this.sub;
    this.sub = undefined;
    void sub?.stop(); // never awaited here: onError runs inside that feed's own poll
    void this.cap?.unlock(this.ref, this).catch(() => undefined);
    this.scheduleTick();
  }

  /** The lock session was lost: every writer lock of the store is gone. A page in flight is fenced by the epoch if another process claimed. */
  private lost(): void {
    this.demote();
  }

  /** The feed's error policy (decision 28 §6): fenced → standby; a fold error isolates and fails the key; a store error never skips. */
  private onFeedError(error: unknown, batch: readonly RecordedEvent[], epoch: number): ErrorDecision {
    if (this.epoch !== epoch || this.phase !== "live") return "stop";
    if (error instanceof FencedError) {
      this.demote();
      return "stop";
    }
    if (error instanceof FoldError && batch.length >= 1) return "skip";
    if (isFatal(error)) {
      this.fail(error);
      return "stop";
    }
    return "retry";
  }

  /** One part (a page, or a half during isolation): one fenced transaction that reads its rows and writes every key of the part, or none. */
  private async applyPage(events: readonly RecordedEvent[], epoch: number): Promise<void> {
    const { keyBy, fold, initial, name, version } = this.definition;
    let count = 0;
    let foldMs = 0;
    let committed: Cursor | null = null;
    const outcome = await this.cap!.page(this.ref, epoch, async ({ head, rows }) => {
      const fresh = head.cursor === null ? events : events.filter((e) => compareCursor(e, head.cursor!) > 0);
      if (fresh.length === 0) return null; // a redelivery: applied before
      committed = cursorOf(fresh);
      const groups = new Map<string, RecordedEvent[]>();
      for (const event of fresh) {
        const key = keyOf(event, keyBy, this.host.schema);
        if (key === undefined) continue;
        const group = groups.get(key);
        if (group) group.push(event);
        else groups.set(key, [event]);
      }
      const stored = groups.size > 0 ? await rows([...groups.keys()]) : new Map<string, StoredRow>();
      const writes: RowWrite[] = [];
      count = 0;
      const started = performance.now();
      try {
        for (const [key, group] of groups) {
          const row = stored.get(key);
          if (row?.failed) continue; // a failed key's later events are passed over
          const prev = row ? (decodeState(row.state!) as S) : freshInitial(initial);
          let next: S;
          let bytes: Uint8Array | undefined;
          try {
            next = fold(group, prev);
            if (!row || !Object.is(next, prev)) {
              const refusal = checkStorable(next, { name, version });
              if (refusal) throw refusal;
              bytes = encodeState(next);
            }
          } catch (cause) {
            throw new FoldError(key, cause);
          }
          count += group.length;
          if (bytes) writes.push({ key, state: bytes, doc: docOf(next), failed: null });
        }
      } finally {
        foldMs = performance.now() - started;
      }
      // the checkpoint moves to the end of the part even when no row changed (decision 14 §1.4)
      return { writes, cursor: cursorOf(fresh)! };
    });
    if (outcome.committed) {
      this.folded += count;
      this.foldMs += foldMs;
      this.ring();
      this.checkSwitch(epoch, committed);
    }
  }

  /** The durable poison skip (decision 28 §5): the key's row gets the failed marker, the checkpoint moves to the event. */
  private async skipEvent(event: RecordedEvent, error: unknown, epoch: number): Promise<void> {
    const cause = error instanceof FoldError ? error.cause : error;
    const key = keyOf(event, this.definition.keyBy, this.host.schema);
    const marker: FailedMarker = {
      type: event.type,
      id: event.id,
      sequence: event.sequence,
      transactionId: event.transactionId,
      error: failedText(cause instanceof Error ? cause.message : String(cause)),
    };
    const outcome = await this.cap!.page(this.ref, epoch, async ({ head }) => {
      if (head.cursor !== null && compareCursor(event, head.cursor) <= 0) return null; // skipped before
      return { writes: key === undefined ? [] : [{ key, state: null, doc: null, failed: marker }], cursor: cursorOf([event])! };
    });
    if (outcome.committed) this.checkSwitch(epoch, cursorOf([event]));
    if (!outcome.committed || key === undefined) return;
    this.ring();
    try {
      const { name, version } = this.definition;
      this.definition.onFailedKey?.(new FailedKeyError({ name, version }, key, { type: event.type, id: event.id, sequence: event.sequence, transactionId: event.transactionId }, { cause }));
    } catch {
      // the dead-letter hook must never break the feed
    }
  }

  /**
   * One part of the cross-tenant processor (decision 45 §5): one fenced transaction over every tenant the part
   * names. Each tenant gets only its events beyond ITS checkpoint, so an event is applied once per tenant even beside
   * a per-tenant writer of the same version; all keys of all tenants of the part, or none.
   */
  private async applyAcross(events: readonly RecordedEvent[], epoch: number): Promise<void> {
    const { keyBy, fold, initial, name, version } = this.definition;
    const sequences = events.map((e) => e.sequence);
    await this.cap!.knobs.beforePage?.(sequences);
    let count = 0;
    let foldMs = 0;
    let committed: Cursor | null = null;
    const outcome = await this.cross!.page(this.ref, epoch, sequences, async ({ head, tenants, rows }) => {
      const fresh = head.cursor === null ? events : events.filter((e) => compareCursor(e, head.cursor!) > 0);
      if (fresh.length === 0) return null; // a redelivery: applied before
      committed = cursorOf(fresh);
      const byTenant = new Map<string, RecordedEvent[]>();
      for (const event of fresh) {
        const tenant = this.tenantOf(event);
        if (tenant === undefined) continue; // no tenant view reads it; its cursor still counts for the processor
        const list = byTenant.get(tenant);
        if (list) list.push(event);
        else byTenant.set(tenant, [event]);
      }
      const checkpoints = byTenant.size > 0 ? await tenants([...byTenant.keys()]) : new Map<string, Cursor | null>();
      const groups = new Map<string, { readonly tenant: string; readonly key: string; readonly events: RecordedEvent[] }>();
      for (const [tenant, list] of byTenant) {
        const checkpoint = checkpoints.get(tenant) ?? null;
        for (const event of list) {
          if (checkpoint !== null && compareCursor(event, checkpoint) <= 0) continue; // this tenant has it already
          const key = keyOf(event, keyBy, this.host.schema);
          if (key === undefined) continue;
          const id = pairKey(tenant, key);
          const group = groups.get(id);
          if (group) group.events.push(event);
          else groups.set(id, { tenant, key, events: [event] });
        }
      }
      const stored = groups.size > 0 ? await rows([...groups.values()].map(({ tenant, key }) => ({ tenant, key }))) : new Map<string, StoredRow>();
      const writes: TenantRowWrite[] = [];
      count = 0;
      const started = performance.now();
      try {
        for (const [id, { tenant, key, events: group }] of groups) {
          const row = stored.get(id);
          if (row?.failed) continue; // a failed key's later events are passed over
          const prev = row ? (decodeState(row.state!) as S) : freshInitial(initial);
          let next: S;
          let bytes: Uint8Array | undefined;
          try {
            next = fold(group, prev);
            if (!row || !Object.is(next, prev)) {
              const refusal = checkStorable(next, { name, version });
              if (refusal) throw refusal;
              bytes = encodeState(next);
            }
          } catch (cause) {
            throw new FoldError(key, cause);
          }
          count += group.length;
          if (bytes) writes.push({ tenant, key, state: bytes, doc: docOf(next), failed: null });
        }
      } finally {
        foldMs = performance.now() - started;
      }
      return { writes, cursor: cursorOf(fresh)! };
    });
    if (outcome.committed) {
      this.folded += count;
      this.foldMs += foldMs;
      this.checkSwitch(epoch, committed);
    }
  }

  /** The poison skip of the processor (decision 45 §6): the failed marker in the event's tenant, both checkpoints moved to it. */
  private async skipAcross(event: RecordedEvent, error: unknown, epoch: number): Promise<void> {
    const cause = error instanceof FoldError ? error.cause : error;
    const tenant = this.tenantOf(event);
    const key = keyOf(event, this.definition.keyBy, this.host.schema);
    const marker: FailedMarker = {
      type: event.type,
      id: event.id,
      sequence: event.sequence,
      transactionId: event.transactionId,
      error: failedText(cause instanceof Error ? cause.message : String(cause)),
    };
    let failed = false;
    await this.cap!.knobs.beforePage?.([event.sequence]);
    const outcome = await this.cross!.page(this.ref, epoch, [event.sequence], async ({ head, tenants }) => {
      if (head.cursor !== null && compareCursor(event, head.cursor) <= 0) return null; // skipped before
      if (tenant === undefined) return { writes: [], cursor: cursorOf([event])! };
      const checkpoint = (await tenants([tenant])).get(tenant) ?? null;
      failed = key !== undefined && (checkpoint === null || compareCursor(event, checkpoint) > 0);
      return { writes: failed ? [{ tenant, key: key!, state: null, doc: null, failed: marker }] : [], cursor: cursorOf([event])! };
    });
    if (outcome.committed) this.checkSwitch(epoch, cursorOf([event]));
    if (!outcome.committed || !failed) return;
    try {
      const { name, version } = this.definition;
      this.definition.onFailedKey?.(new FailedKeyError({ name, version }, key!, { type: event.type, id: event.id, sequence: event.sequence, transactionId: event.transactionId }, { cause, tenant: tenant! }));
    } catch {
      // the dead-letter hook must never break the feed
    }
  }

  // ── lifecycle ─────────────────────────────────────────────────────────────

  /** Forget the `get` copies (decision 49 §1.3): the next `get` reads row + delta again. The handle keeps running. */
  dropCopies(): void {
    this.copies.clear();
  }

  /** A read of the model row met the tombstone (decision 40 §2): the handle fails with it, and the read rejects with it. */
  private async guard<T>(read: Promise<T>): Promise<T> {
    try {
      return await read;
    } catch (error) {
      if (error instanceof RetiredError) this.fail(error);
      throw error;
    }
  }

  /** The stored fingerprint against this definition's: a warning at most once per process (decision 42); nothing else. */
  private compareFingerprint(stored: string | null | undefined): void {
    warnIfChanged("persisted read model", this.definition.name, this.definition.version, stored, this.host.fingerprint);
  }

  /** Whether this version builds beside an active one (decision 38 §3): the model row's status, or — without a row — whether another version is active. */
  private async isBuilding(cap: ReadModelStore): Promise<boolean> {
    if (this.activeSeen) return false;
    const head = await this.guard(cap.head(this.ref));
    if (head) {
      this.compareFingerprint(head.fingerprint);
      if (head.status === "active") this.activeSeen = true;
      return head.status === "building";
    }
    return this.guard(cap.activeSibling(this.ref));
  }

  /**
   * Wait until the version is no longer `building` — the switch rings the model's notification — within `deadline`
   * (`list`: `TransientError` `read_model_timeout`) or until `member` stops (a watch: no deadline).
   */
  private async untilSwitched(cap: ReadModelStore, deadline: number | null, timeoutMs: number, member?: WatchMember<never>): Promise<void> {
    for (let pause = 50; ; pause = Math.min(pause * 2, SWITCH_POLL_MS)) {
      this.assertRunning();
      if (member?.stopped) return;
      if (!(await this.isBuilding(cap))) return;
      this.assertRunning();
      let left = pause;
      if (deadline !== null) {
        left = deadline - Date.now();
        if (left <= 0) {
          const error = new TransientError("read_model_timeout");
          error.message = `eventstore: ${this.label} is still building beside an active version (blue-green); it answers list once it has caught up (waited ${timeoutMs} ms, timeoutMs) — retry, or read the old version while it builds`;
          throw error;
        }
      }
      await this.ringOrPause(cap, Math.min(pause, left));
    }
  }

  /** Until the model's notification rings, at most `ms`. */
  private ringOrPause(cap: ReadModelStore, ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
      const timer = setTimeout(done, ms);
      const off = cap.onChanged(this.ref, done);
      function done() {
        clearTimeout(timer);
        off();
        resolve();
      }
    });
  }

  /** An error no retry fixes in this process: the writer stops, every read rejects with it, every watcher ends with `onError`. */
  private fail(error: unknown): void {
    if (this.phase !== "live") return;
    this.phase = "failed";
    this.failure = error;
    this.settleReady(error);
    this.release();
    for (const member of [...this.members]) member.end(error);
    this.stopFollower();
  }

  /** Stop the election, the feed and the lock; detach from the store. */
  private release(): Promise<void> {
    if (this.tickTimer) clearTimeout(this.tickTimer);
    this.tickTimer = undefined;
    const sub = this.sub;
    this.sub = undefined;
    this.role = "standby";
    this.epoch = null;
    const cap = this.cap;
    return (async () => {
      await sub?.stop();
      await cap?.unlock(this.ref, this).catch(() => undefined);
      this.detach?.();
    })();
  }

  private stoppedError(): UsageError {
    return new UsageError(`eventstore: ${this.label} stopped — call persistedReadModel(definition) again for a fresh handle`);
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
    this.stopFollower();
    this.host.onStop();
    await this.release();
    this.copies.clear();
  }

  private assertRunning(): void {
    if (this.phase === "failed") throw this.failure;
    if (this.phase === "stopped") throw this.stoppedError();
  }

  /** The capability, once the handle has started; rejects with the failure or the stop. */
  private async running(): Promise<ReadModelStore> {
    this.assertRunning();
    await this.ready;
    this.assertRunning();
    return this.cap!;
  }

  private failedError(key: string, marker: FailedMarker): FailedKeyError {
    const { name, version } = this.definition;
    return new FailedKeyError({ name, version }, key, { type: marker.type, id: marker.id, sequence: marker.sequence, transactionId: marker.transactionId }, { cause: new Error(marker.error) });
  }

  // ── reads ─────────────────────────────────────────────────────────────────

  async get(key: string): Promise<S> {
    this.refuseRead("get");
    if (typeof key !== "string") throw new UsageError(`eventstore: ${this.label}: get() needs a string key, got ${typeof key}`);
    const cap = await this.running();
    const { source, keyBy, fold, initial } = this.definition;
    let copy = this.copies.get(key);
    let fromRow = false;
    if (!copy) {
      // the row holds every event of the key up to the model's checkpoint — also those that changed nothing and wrote
      // no row — so the delta starts there, not at the row's own older cursor (decision 36)
      const { row, checkpoint, fingerprint } = await this.guard(cap.latest(this.ref, key));
      this.compareFingerprint(fingerprint);
      if (row?.failed) throw this.failedError(key, row.failed);
      copy = { state: row ? (decodeState(row.state!) as S) : freshInitial(initial), cursor: checkpoint ?? row?.cursor ?? null, bytes: 0 };
      fromRow = true;
    }
    // the key's delta from the view store, settled and unsettled: the read fold on read makes
    const query = keyQuery(source, keyBy, key);
    const maxEvents = this.host.copies.maxEvents;
    let result = await this.store!.query(query, { cursor: copy.cursor, limit: maxEvents + 1 });
    let state = copy.state;
    let cursor = copy.cursor;
    // a delta over maxEvents — a building version early in its build, a writer far behind — is folded a page of
    // settled events at a time, as the persisted fold does (decision 41, O24 b): memory stays bounded by one page
    let paged = false;
    while (result.events.length > maxEvents) {
      const page: RecordedEvent[] = [];
      for (const e of result.events) {
        if (!e.settled || page.length === maxEvents) break; // every settled record precedes every unsettled one
        page.push(e);
      }
      if (page.length === 0) throw new ContextTooLargeError(queryKey(query), maxEvents); // more than maxEvents unsettled
      state = fold(page, state);
      cursor = cursorOf(page) ?? cursor;
      this.readFolded += page.length;
      paged = true;
      result = await this.store!.query(query, { cursor, limit: maxEvents + 1 });
    }
    const settled: RecordedEvent[] = [];
    const unsettled: RecordedEvent[] = [];
    // in (transactionId, sequence) order every settled record precedes every unsettled one
    for (const e of result.events) (e.settled && unsettled.length === 0 ? settled : unsettled).push(e);
    if (settled.length > 0) {
      state = fold(settled, state);
      cursor = cursorOf(settled) ?? cursor;
    }
    const answer = unsettled.length > 0 ? fold(unsettled, state) : state;
    this.readFolded += result.events.length;
    if (fromRow || paged || settled.length > 0) this.copies.set(key, state, cursor); // the copy moves forward, never back
    return answer;
  }

  async list(options: ListOptions = {}): Promise<ListResult<S>> {
    this.refuseRead("list");
    const where = whereOf(options.where, this.label);
    const sort = sortOf(options.sort, this.label);
    const offset = options.offset ?? 0;
    const limit = options.limit ?? DEFAULT_LIMIT;
    if (!Number.isSafeInteger(offset) || offset < 0) throw new UsageError(`eventstore: ${this.label}: list offset must be an integer of at least 0, got ${String(offset)}`);
    if (!Number.isSafeInteger(limit) || limit < 1) throw new UsageError(`eventstore: ${this.label}: list limit must be an integer of at least 1, got ${String(limit)}`);
    const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1) {
      throw new UsageError(`eventstore: ${this.label}: timeoutMs must be a positive integer (default ${DEFAULT_TIMEOUT_MS}), got ${String(timeoutMs)}`);
    }
    const token = options.atLeast;
    if (token !== undefined) checkToken(token);
    const cap = await this.running();
    // the whole wait — for the switch of a building version, then for the token — within timeoutMs
    const deadline = Date.now() + timeoutMs;
    await this.untilSwitched(cap, deadline, timeoutMs);
    if (token !== undefined) await this.waitFor(cap, token, deadline, timeoutMs);
    const page = await this.guard(cap.list(this.ref, { where, sort, offset, limit }));
    this.compareFingerprint(page.fingerprint);
    return { rows: page.rows.map((row) => ({ key: row.key, state: decodeState(row.state) as S })), total: page.total, failed: page.failed, revision: page.revision };
  }

  /** Wait until the model has passed `token` (decision 30 §6), decided from the store alone, until `deadline`. */
  private async waitFor(cap: ReadModelStore, token: Cursor, deadline: number, timeoutMs: number): Promise<void> {
    for (let pause = 50; ; pause = Math.min(pause * 2, 500)) {
      if (await this.hasPassed(cap, token)) return;
      this.assertRunning();
      const left = deadline - Date.now();
      if (left <= 0) {
        const error = new TransientError("read_model_timeout");
        error.message = `eventstore: ${this.label} has not passed token ${token.transactionId}/${token.sequence} within ${timeoutMs} ms (timeoutMs) — its writer is behind or not running, or an older transaction is still open; retry`;
        throw error;
      }
      await this.ringOrPause(cap, Math.min(pause, left));
    }
  }

  private async hasPassed(cap: ReadModelStore, token: Cursor): Promise<boolean> {
    if (this.role === "writer" && this.sub?.hasPassed(token)) return true; // free in the writer's process
    const head = await this.guard(cap.head(this.ref));
    if (head?.cursor && compareCursor(head.cursor, token) >= 0) return true;
    // the source beyond the checkpoint, settled, with this statement's own watermark (decision 11)
    const probe = await this.store!.query(this.definition.source, { cursor: head?.cursor ?? null, limit: 1, settledOnly: true, payload: false, version: false });
    if (probe.watermark === undefined || BigInt(probe.watermark) <= BigInt(token.transactionId)) return false;
    const first = probe.events[0];
    return first === undefined || compareCursor(first, token) > 0;
  }

  async status(): Promise<PersistedReadModelStatus> {
    const local = { phase: this.phase, role: this.role, folded: this.folded, foldMs: this.foldMs, readFolded: this.readFolded };
    if (this.phase !== "live") {
      return { ...local, checkpoint: null, epoch: 0, revision: 0, failed: 0, building: false, lag: { events: 0, ms: 0 }, ...(this.phase === "failed" ? { error: this.failure } : {}) };
    }
    await this.running();
    const heads = this.heads(); // the processor: its row, failed keys over every tenant, lag over every tenant's source
    const head = await this.guard(heads.head(this.ref));
    if (head) this.compareFingerprint(head.fingerprint);
    const building = head ? head.status === "building" : await this.guard(heads.activeSibling(this.ref));
    const checkpoint = head?.cursor ?? null;
    const source = this.definition.source;
    let lag = { events: 0, ms: 0 };
    // up to LAG_COUNTED pending events are counted; beyond, bounded without reading them all (decision 37)
    const pending = await this.store!.query(source, { cursor: checkpoint, settledOnly: true, limit: LAG_COUNTED + 1, payload: false, version: false });
    const first = pending.events[0];
    if (first) {
      let events = pending.events.length;
      if (events > LAG_COUNTED) {
        // pending above the checkpoint's sequence: at most one per sequence up to the head's; below it (younger
        // transactions that took lower sequences): counted
        const top = await this.store!.query(source, { order: "desc", limit: 1, payload: false, version: false });
        const below = checkpoint ? (await this.store!.query(source, { cursor: checkpoint, settledOnly: true, until: checkpoint.sequence, payload: false, version: false })).events.length : 0;
        events = Math.max(events, (top.events[0]?.sequence ?? 0) - (checkpoint?.sequence ?? 0) + below);
      }
      lag = { events, ms: Math.max(0, Date.now() - first.recordedAt.getTime()) };
    }
    return { ...local, role: this.role, checkpoint, epoch: head?.epoch ?? 0, revision: head?.revision ?? 0, failed: head?.failed ?? 0, building, lag };
  }

  // ── follower and watchers (decision 31) ───────────────────────────────────

  /** Ring this process's follower: a page committed here (no round trip needed). */
  private ring(): void {
    if (this.follower) void this.follow();
  }

  private ensureFollower(cap: ReadModelStore): Promise<void> {
    if (this.follower) return this.follower.ready;
    const follower: Follower = { seen: 0, ready: Promise.resolve(), off: () => undefined, timer: undefined, reading: false, again: false };
    this.follower = follower;
    follower.ready = (async () => {
      follower.seen = (await this.guard(cap.head(this.ref)))?.revision ?? 0;
    })();
    follower.ready.catch(() => {
      if (this.follower === follower) this.stopFollower(); // the next watcher starts a new one
    });
    follower.off = cap.onChanged(this.ref, () => void this.follow());
    if (!cap.pushes) {
      follower.timer = setInterval(() => void this.follow(), FOLLOW_POLL_MS);
      follower.timer.unref?.();
    }
    return follower.ready;
  }

  private stopFollower(): void {
    const follower = this.follower;
    if (!follower) return;
    this.follower = undefined;
    follower.off();
    if (follower.timer) clearInterval(follower.timer);
  }

  /** The follower stops with the last watcher — counting those still waiting for it, not only those it already feeds. */
  private maybeStopFollower(): void {
    if (this.members.size === 0) this.stopFollower();
  }

  /** A registration joins the follower; one stopped while it waited (and not by the end of every watcher) is started again. */
  private async joinFollower(cap: ReadModelStore, member: WatchMember<never>): Promise<void> {
    do await this.ensureFollower(cap);
    while (!member.stopped && this.follower === undefined && this.phase === "live");
  }

  /** One ring: read every row changed since `seen`, decode each once, hand it to the watchers. */
  private async follow(): Promise<void> {
    const follower = this.follower;
    const cap = this.cap;
    if (!follower || !cap) return;
    if (follower.reading) {
      follower.again = true;
      return;
    }
    follower.reading = true;
    try {
      await follower.ready;
      do {
        follower.again = false;
        const before = follower.seen;
        const changed: ChangedRow[] = [];
        let after: { revision: number; key: string | null } = { revision: before, key: null };
        for (;;) {
          const batch = await cap.changes(this.ref, after, CHANGES_PAGE);
          changed.push(...batch);
          if (batch.length < CHANGES_PAGE) break;
          const last = batch[batch.length - 1]!;
          after = { revision: last.revision, key: last.key };
        }
        if (this.follower !== follower) return;
        if (changed.length === 0) continue;
        follower.seen = changed[changed.length - 1]!.revision;
        this.dispatch(changed, before);
      } while (follower.again && this.follower === follower);
    } catch (error) {
      if (this.follower !== follower) return;
      if (isFatal(error)) {
        for (const member of [...this.members]) member.end(error);
        this.stopFollower();
        return;
      }
      setTimeout(() => void this.follow(), FOLLOW_RETRY_MS).unref?.(); // transient: try again soon, a ring may not come
    } finally {
      follower.reading = false;
    }
  }

  private dispatch(changed: readonly ChangedRow[], before: number): void {
    const decoded = new Map<ChangedRow, S>();
    const stateOf = (row: ChangedRow): S => {
      if (!decoded.has(row)) decoded.set(row, decodeState(row.state!) as S);
      return decoded.get(row) as S;
    };
    for (const row of changed) {
      const watchers = this.keyWatchers.get(row.key);
      if (!watchers) continue;
      for (const watcher of [...watchers]) {
        if (watcher.held) watcher.held.push(row);
        else this.deliverKey(watcher, row, stateOf);
      }
    }
    if (this.listWatchers.size === 0) return;
    const byRevision: ChangedRow[][] = [];
    for (const row of changed) {
      const group = byRevision[byRevision.length - 1];
      if (group && group[0]!.revision === row.revision) group.push(row);
      else byRevision.push([row]);
    }
    for (const watcher of [...this.listWatchers]) {
      for (const group of byRevision) {
        const rows: ListRow<S>[] = [];
        const removed: string[] = [];
        for (const row of group) {
          if (!row.failed && docMatches(row.doc, watcher.where)) rows.push({ key: row.key, state: stateOf(row) });
          // matched before: the previous doc this follower knew, or — changed twice since — unknown, so reported
          else if (row.prevRevision !== null && (row.prevRevision > before || docMatches(row.prevDoc, watcher.where))) removed.push(row.key);
        }
        if (rows.length > 0 || removed.length > 0) watcher.member.deliver({ revision: group[0]!.revision, rows, removed });
      }
    }
  }

  private deliverKey(watcher: KeyWatcher<S>, row: ChangedRow, stateOf: (row: ChangedRow) => S): void {
    if (row.revision <= watcher.last) return;
    watcher.last = row.revision;
    if (row.failed) return watcher.member.end(this.failedError(row.key, row.failed));
    watcher.member.deliver(stateOf(row));
  }

  async watch(key: string, onState: (state: S) => void | Promise<void>, options: ReadModelWatchOptions = {}): Promise<Watch> {
    this.refuseRead("watch");
    if (typeof key !== "string") throw new UsageError(`eventstore: ${this.label}: watch() needs a string key, got ${typeof key}`);
    if (typeof onState !== "function") throw new UsageError(`eventstore: ${this.label}: watch() needs a callback`);
    this.assertRunning();
    const release = this.host.places.admit();
    if (options.signal?.aborted) {
      release();
      throw options.signal.reason;
    }
    const watcher: KeyWatcher<S> = { key, member: undefined as unknown as WatchMember<S>, last: 0, held: [] };
    const member: WatchMember<S> = new WatchMember<S>(onState, options.onError, this.host.places.maxPendingBatches, () => {
      this.members.delete(member as WatchMember<never>);
      const set = this.keyWatchers.get(key);
      set?.delete(watcher);
      if (set?.size === 0) this.keyWatchers.delete(key);
      release();
      this.maybeStopFollower();
    });
    watcher.member = member;
    this.members.add(member as WatchMember<never>);
    let row: StoredRow | null = null;
    const prepared = (async () => {
      const cap = await this.running();
      await this.untilSwitched(cap, null, 0, member as WatchMember<never>); // a building version: no build page reaches a watcher
      if (member.stopped) return;
      await this.joinFollower(cap, member as WatchMember<never>); // join the follower before reading the row: no change falls in between
      if (member.stopped) return;
      const set = this.keyWatchers.get(key);
      if (set) set.add(watcher);
      else this.keyWatchers.set(key, new Set([watcher]));
      row = await cap.row(this.ref, key);
    })();
    return handOut(member, prepared, options.signal, () => {
      const held = watcher.held ?? [];
      watcher.held = null;
      const first = row as StoredRow | null;
      if (first?.failed) return member.end(this.failedError(key, first.failed));
      member.deliver(first ? (decodeState(first.state!) as S) : freshInitial(this.definition.initial));
      watcher.last = first?.revision ?? 0;
      for (const change of held) this.deliverKey(watcher, change, (r) => decodeState(r.state!) as S);
    });
  }

  async watchList(filter: Pick<ListOptions, "where">, onChange: (change: ListChange<S>) => void | Promise<void>, options: ReadModelWatchOptions = {}): Promise<Watch> {
    this.refuseRead("watchList");
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
      this.maybeStopFollower();
    });
    const watcher: ListWatcher<S> = { where, member };
    this.members.add(member as WatchMember<never>);
    const prepared = (async () => {
      const cap = await this.running();
      await this.untilSwitched(cap, null, 0, member as WatchMember<never>);
      if (member.stopped) return;
      await this.joinFollower(cap, member as WatchMember<never>);
    })();
    return handOut(member, prepared, options.signal, () => {
      this.listWatchers.add(watcher);
    });
  }
}
