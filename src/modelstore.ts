/**
 * The capability a store needs to keep persisted read models (decisions 26–28, 31), internal: only
 * the two built-in stores implement it, and nothing of it is exported from the package. A store
 * carries it as `readModelStore` only when created with `persistedModels: true`.
 */
import { EventStoreError, UsageError } from "./errors.js";
import { compareCursor } from "./query.js";
import { compareCodePoints, compareListed, docValue, rankOf } from "./readmodel.js";
import type { Cursor } from "./types.js";

/** One model: its tenant (stamped by the view; `''` or absent outside a tenant view), name and version. */
export interface ModelRef {
  readonly tenant?: string;
  readonly name: string;
  readonly version: number;
}

/**
 * Where one version stands (decision 38): `building` beside an active version of the name (its `list` and watches
 * wait), `active` (answers everything), `retired` (a tombstone: rows deleted, every handle of it fails).
 */
export type VersionStatus = "building" | "active" | "retired";

/** The model row: checkpoint, the current writer's epoch, the model revision, the number of failed keys, the status and the stored fingerprint. */
export interface ModelHead {
  readonly cursor: Cursor | null;
  readonly epoch: number;
  readonly revision: number;
  readonly failed: number;
  readonly status: VersionStatus;
  /** The fingerprint the last writer claimed with (decision 42); `null` before. */
  readonly fingerprint: string | null;
}

/** What a writer's claim returns: its epoch, the version's status, and the fingerprint it replaced. */
export interface Claim {
  readonly epoch: number;
  readonly status: VersionStatus;
  readonly fingerprint: string | null;
}

/** The model row as one snapshot read it, with every state row (decision 43). */
export interface ModelSnapshot {
  readonly checkpoint: Cursor | null;
  readonly status: VersionStatus;
  readonly fingerprint: string | null;
}

/**
 * The version was retired (decision 40): a `UsageError`, so `isFatal` holds — the handle fails and no
 * retry loop starts. Every place that reads the model row throws it.
 */
export class RetiredError extends UsageError {
  constructor(ref: ModelRef) {
    super(
      `eventstore: persisted read model "${ref.name}" v${ref.version} was retired (a newer version replaced it and its rows were deleted) — ` +
        `this process runs code that only knows v${ref.version}; deploy the code that knows the new version, or bump the version to rebuild`,
    );
  }
}

/** The failed marker of a key: the dead letter of the event its fold threw on. Never the payload. */
export interface FailedMarker {
  readonly type: string;
  readonly id: string;
  readonly sequence: number;
  readonly transactionId: string;
  /** The cause's message as `failedText` keeps it: at most 1 000 UTF-16 units, a pair never cut, NUL and lone surrogates as U+FFFD. */
  readonly error: string;
}

/** A state row as read. */
export interface StoredRow {
  readonly key: string;
  /** The encoded state (`node:v8`); `null` when failed. */
  readonly state: Uint8Array | null;
  readonly doc: Record<string, unknown> | null;
  readonly failed: FailedMarker | null;
  /** The checkpoint of the page that last wrote the row. */
  readonly cursor: Cursor;
  readonly revision: number;
}

/** A state row as the follower reads it: with the doc before its last change. */
export interface ChangedRow extends StoredRow {
  readonly prevDoc: Record<string, unknown> | null;
  /** The revision of that previous change; `null` when the row is new. */
  readonly prevRevision: number | null;
}

/** One row a page writes: a held state (bytes and doc) or a failed key (marker). */
export interface RowWrite {
  readonly key: string;
  readonly state: Uint8Array | null;
  readonly doc: Record<string, unknown> | null;
  readonly failed: FailedMarker | null;
}

/** What a page's function sees inside its transaction. */
export interface PageScope {
  /** The model row, locked for this transaction. */
  readonly head: Pick<ModelHead, "cursor" | "epoch" | "revision">;
  /** The rows of these keys, read inside the same transaction. */
  rows(keys: readonly string[]): Promise<Map<string, StoredRow>>;
}

/** What a page commits: rows (all with the next revision) and the checkpoint. `null`: commit nothing. */
export type PageCommit = { readonly writes: readonly RowWrite[]; readonly cursor: Cursor } | null;

export interface ListQuery {
  /** Equality on doc fields, AND (validated by the caller). */
  readonly where: readonly (readonly [string, unknown])[];
  readonly sort: { readonly by: string; readonly dir: "asc" | "desc" } | null;
  readonly offset: number;
  readonly limit: number;
}

export interface ListPage {
  readonly rows: readonly { readonly key: string; readonly state: Uint8Array }[];
  readonly total: number;
  readonly failed: number;
  readonly revision: number;
  /** The model row's fingerprint, read by the same statement; `null` when none. */
  readonly fingerprint: string | null;
}

/** Internal knobs, for tests only: never in a public options type (decision 27 §9). */
export interface WriterKnobs {
  /** The writer tick: heartbeat of the lock session and the standbys' next try. Default `WRITER_TICK_MS`. */
  tickMs: number;
  /** `false`: the lock session's loss goes unnoticed (no heartbeat, no reaction to its end) — makes a zombie writer. */
  noticeLoss: boolean;
  /** Awaited by a cross-tenant processor after its feed read a part and before its page transaction (the erasure tests pause it here). */
  beforePage?: ((sequences: readonly number[]) => Promise<void>) | undefined;
  /** Awaited inside a cross-tenant page on Postgres, after the erasure lock and the count check. */
  inPage?: ((sequences: readonly number[]) => Promise<void>) | undefined;
}

/** One key of one tenant. */
export interface TenantKey {
  readonly tenant: string;
  readonly key: string;
}

/** The map key of a (tenant, key) pair in `AcrossScope.rows`. */
export function pairKey(tenant: string, key: string): string {
  return `${tenant}\u0000${key}`;
}

/** What a cross-tenant page's function sees inside its transaction (decision 45 §5). */
export interface AcrossScope {
  /** The processor row (tenant `''`), locked for this transaction. */
  readonly head: Pick<ModelHead, "cursor" | "epoch" | "revision">;
  /**
   * The model rows of these tenants — created when missing with the processor row's status, then locked in tenant
   * order (code point): each tenant's checkpoint. Every tenant locked here moves to `greatest(checkpoint, cursor)` at
   * commit. Never `''`.
   */
  tenants(tenants: readonly string[]): Promise<Map<string, Cursor | null>>;
  /** The rows of these (tenant, key) pairs, read inside the same transaction, keyed by `pairKey`. */
  rows(pairs: readonly TenantKey[]): Promise<Map<string, StoredRow>>;
}

/** One row a cross-tenant page writes, in a tenant locked by `tenants()`. */
export interface TenantRowWrite extends RowWrite {
  readonly tenant: string;
}

/** What a cross-tenant page commits: rows (each with its tenant's next revision), the part's end. `null`: commit nothing. */
export type AcrossCommit = { readonly writes: readonly TenantRowWrite[]; readonly cursor: Cursor } | null;

/**
 * The cross-tenant processor's backend (decision 45), internal: only a root capability has it, and under
 * `rls` only on a store connected as the processor role (decision 46). Every `ref` names the processor row
 * (tenant `''`); the tenants' rows are reached through a page.
 */
export interface CrossTenantStore {
  /** The writer lock of the processor row and its next epoch; created `building` when another version of the name is active in ANY row. */
  lock(ref: ModelRef, owner: object, fingerprint: string | null): Promise<Claim | null>;
  /** The processor row, its failed keys counted over every tenant; `null` without a row; retired → `RetiredError`. */
  head(ref: ModelRef): Promise<ModelHead | null>;
  /** Whether another version of the name is `active` in any row (any tenant): the status of a version without a processor row. */
  activeSibling(ref: ModelRef): Promise<boolean>;
  /**
   * One page across tenants: [Postgres: the erasure lock shared, then every event of `sequences` must still exist,
   * else `TransientError("read_model_page_stale")`] → the processor row locked (created when missing), retired /
   * fenced as a page → `fn` → rows, tenant checkpoints and revisions, the processor checkpoint, one notification per
   * tenant locked — or nothing when anything throws.
   */
  page(ref: ModelRef, epoch: number, sequences: readonly number[], fn: (scope: AcrossScope) => Promise<AcrossCommit>): Promise<{ readonly committed: boolean }>;
  /**
   * The switch across tenants (decision 45 §7): every tenant that has any version of the name gets an active row of this
   * one. [Postgres: the erasure lock shared first (decision 50), so an erased tenant is never listed.]
   */
  activate(ref: ModelRef, epoch: number): Promise<void>;
}

export const WRITER_TICK_MS = 1_000;
export const MAX_WRITERS_PER_STORE = 1_000;
export const FAILED_ERROR_LENGTH = 1_000;

/** A lone surrogate: a high one not followed by a low one, or a low one not preceded by a high one. */
const LONE_SURROGATE = /[\ud800-\udbff](?![\udc00-\udfff])|(?<![\ud800-\udbff])[\udc00-\udfff]/g;

/**
 * The marker's text of an error message (decision 33): at most `FAILED_ERROR_LENGTH` UTF-16 units,
 * never cut inside a surrogate pair, a NUL and every lone surrogate as U+FFFD — so jsonb holds it
 * and the poison skip can always commit, and both stores keep the same text.
 */
export function failedText(message: string): string {
  const text = message.slice(0, FAILED_ERROR_LENGTH + 1).replace(/\u0000/g, "�").replace(LONE_SURROGATE, "�");
  if (text.length <= FAILED_ERROR_LENGTH) return text;
  // the unit at the limit is the low half of a pair: keep the pair whole by dropping its high half too
  const cut = /[\ud800-\udbff]/.test(text[FAILED_ERROR_LENGTH - 1]!) ? FAILED_ERROR_LENGTH - 1 : FAILED_ERROR_LENGTH;
  return text.slice(0, cut);
}

export interface ReadModelStore {
  /** Set when every call would be refused (the root of an `rls` store): the handle fails with it at once. */
  readonly refusal?: Error;
  /** `true` when a page committed in another process rings `onChanged` here (Postgres with `live`, the memory store). */
  readonly pushes: boolean;
  readonly knobs: WriterKnobs;
  /** A handle of this store is alive (the lock session stays open until the last one detaches). */
  attach(owner: object): () => void;
  /**
   * Try to become the model's writer: take the writer lock without waiting and claim the next epoch, writing
   * `fingerprint` into the model row (created when missing, `building` beside an active version, decision 38 §2).
   * The claim, or `null` when another session (or another handle of this process) holds the lock. A retired version
   * throws `RetiredError` and keeps no lock.
   */
  lock(ref: ModelRef, owner: object, fingerprint: string | null): Promise<Claim | null>;
  /** Release the writer lock if `owner` holds it. */
  unlock(ref: ModelRef, owner: object): Promise<void>;
  /** Called when the lock session was lost: every writer lock of this store is gone. */
  onLost(listener: () => void): () => void;
  /** The model row, `null` when the version has none yet; a retired version throws `RetiredError`. */
  head(ref: ModelRef): Promise<ModelHead | null>;
  /** Whether another version of the name is `active` in the tenant: the status of a version without a row (decision 38 §3). */
  activeSibling(ref: ModelRef): Promise<boolean>;
  /**
   * One page transaction: lock the model row (created when missing), throw `RetiredError` when it is retired and
   * `FencedError` unless its epoch is `epoch`, run `fn`, and commit what it returns — rows with revision + 1, the
   * checkpoint, the notification — or roll everything back when anything throws.
   */
  page(ref: ModelRef, epoch: number, fn: (scope: PageScope) => Promise<PageCommit>): Promise<{ readonly revision: number; readonly committed: boolean }>;
  /**
   * The switch (decision 39 §4): lock the model row, `RetiredError` / `FencedError` as a page, `building` → `active`,
   * ring the model's notification. Already `active`: nothing.
   */
  activate(ref: ModelRef, epoch: number): Promise<void>;
  /**
   * The model row and every state row of the version from ONE snapshot (decision 43 §2.2), handed to `onRows` page by
   * page; `null` when the version has no model row. A retired version returns its row and no state rows. What
   * `onRows` throws aborts the read and rejects with it.
   */
  snapshot(ref: ModelRef, onRows: (rows: readonly StoredRow[]) => void): Promise<ModelSnapshot | null>;
  row(ref: ModelRef, key: string): Promise<StoredRow | null>;
  /**
   * The row of a key and the model's checkpoint, read from ONE snapshot (decision 36): the row holds every event of
   * the key up to that checkpoint, also those that left its state unchanged and so wrote nothing.
   */
  latest(ref: ModelRef, key: string): Promise<{ readonly row: StoredRow | null; readonly checkpoint: Cursor | null; readonly fingerprint: string | null }>;
  list(ref: ModelRef, query: ListQuery): Promise<ListPage>;
  /** Rows changed after `(revision, key)`, ordered by revision and key (code point), at most `limit`. */
  changes(ref: ModelRef, after: { readonly revision: number; readonly key: string | null }, limit: number): Promise<ChangedRow[]>;
  /** Rings after a page of the model committed (payload-free). */
  onChanged(ref: ModelRef, listener: () => void): () => void;
  /**
   * The cross-tenant processor's backend (decision 45): only on a root capability; rejects with a `UsageError` where
   * this store may not run one (under `rls` unless connected as the processor role, decision 46).
   */
  crossTenant?(): Promise<CrossTenantStore>;
}

/** A page met another writer's epoch: this writer was fenced and must stop (decision 27 §4). */
export class FencedError extends EventStoreError {
  override readonly name = "FencedError";
  constructor(ref: ModelRef, mine: number, current: number) {
    super(`eventstore: persisted read model "${ref.name}" v${ref.version}: fenced — epoch ${current} has replaced this writer's ${mine}`);
  }
}

/** The capability of a store, if it has one. */
export function readModelStoreOf(store: unknown): ReadModelStore | undefined {
  const cap = (store as { readModelStore?: ReadModelStore } | null)?.readModelStore;
  return cap && typeof cap.page === "function" ? cap : undefined;
}

/** Stamp the view's tenant onto every reference, fail-closed against one naming another tenant. */
export function stampedModelStore(inner: ReadModelStore, tenantId: string, onMismatch: (claimed: string) => Error): ReadModelStore {
  const stamp = (ref: ModelRef): ModelRef => {
    if (ref.tenant !== undefined && ref.tenant !== tenantId) throw onMismatch(ref.tenant);
    return { name: ref.name, version: ref.version, tenant: tenantId };
  };
  return {
    ...(inner.refusal ? { refusal: inner.refusal } : {}),
    pushes: inner.pushes,
    knobs: inner.knobs,
    attach: (owner) => inner.attach(owner),
    lock: async (ref, owner, fingerprint) => inner.lock(stamp(ref), owner, fingerprint),
    unlock: async (ref, owner) => inner.unlock(stamp(ref), owner),
    onLost: (listener) => inner.onLost(listener),
    head: async (ref) => inner.head(stamp(ref)),
    activeSibling: async (ref) => inner.activeSibling(stamp(ref)),
    page: async (ref, epoch, fn) => inner.page(stamp(ref), epoch, fn),
    activate: async (ref, epoch) => inner.activate(stamp(ref), epoch),
    snapshot: async (ref, onRows) => inner.snapshot(stamp(ref), onRows),
    row: async (ref, key) => inner.row(stamp(ref), key),
    latest: async (ref, key) => inner.latest(stamp(ref), key),
    list: async (ref, query) => inner.list(stamp(ref), query),
    changes: async (ref, after, limit) => inner.changes(stamp(ref), after, limit),
    onChanged: (ref, listener) => inner.onChanged(stamp(ref), listener),
  };
}

/** Whether a doc matches `where`: every field present with exactly that value (jsonb containment). */
export function docMatches(doc: Record<string, unknown> | null, where: readonly (readonly [string, unknown])[]): boolean {
  if (doc === null) return false;
  for (const [field, value] of where) {
    if (!Object.prototype.hasOwnProperty.call(doc, field) || doc[field] !== value) return false;
  }
  return true;
}

// ── the memory store's implementation ────────────────────────────────────────

interface MemoryModel {
  readonly ref: { readonly tenant: string; readonly name: string; readonly version: number };
  /** `undefined` while the version has no model row (nobody claimed or paged it yet). */
  status: VersionStatus | undefined;
  fingerprint: string | null;
  epoch: number;
  cursor: Cursor | null;
  revision: number;
  owner: object | undefined;
  /** One page at a time: the critical section of a page transaction. */
  chain: Promise<unknown>;
  readonly rows: Map<string, MemoryRow>;
  readonly listeners: Set<() => void>;
}

interface MemoryRow {
  readonly key: string;
  readonly state: Uint8Array | null;
  readonly doc: Record<string, unknown> | null;
  readonly failed: FailedMarker | null;
  readonly cursor: Cursor;
  readonly revision: number;
  readonly prevDoc: Record<string, unknown> | null;
  readonly prevRevision: number | null;
}

/** The memory store's capability, with test hooks: take a lock from its owner without telling it; retire a version. */
export interface MemoryReadModelStore extends ReadModelStore {
  /** Tests only: the model's writer lock is taken from its owner, who is not told (a zombie). */
  steal(ref: ModelRef): void;
  /** Tests only: what the README's retire recipe does for one version (decision 40 §4): status `retired`, rows cleared, listeners rung. */
  retire(ref: ModelRef): Promise<void>;
}

/**
 * Persisted read models in maps (decision 26 §7): rows and model rows inside the `MemoryStore`
 * object, states as copies of the encoded bytes. Two runtimes over one store are two processes:
 * one writes, the other is a standby.
 */
export function memoryReadModelStore(): MemoryReadModelStore {
  const models = new Map<string, MemoryModel>();
  const knobs: WriterKnobs = { tickMs: WRITER_TICK_MS, noticeLoss: true };
  const idOf = (ref: ModelRef) => JSON.stringify([ref.tenant ?? "", ref.name, ref.version]);
  const modelOf = (ref: ModelRef): MemoryModel => {
    const id = idOf(ref);
    let model = models.get(id);
    if (!model) {
      const key = { tenant: ref.tenant ?? "", name: ref.name, version: ref.version };
      models.set(id, (model = { ref: key, status: undefined, fingerprint: null, epoch: 0, cursor: null, revision: 0, owner: undefined, chain: Promise.resolve(), rows: new Map(), listeners: new Set() }));
    }
    return model;
  };
  const activeSibling = (ref: ModelRef): boolean => {
    for (const other of models.values()) {
      if (other.ref.tenant === (ref.tenant ?? "") && other.ref.name === ref.name && other.ref.version !== ref.version && other.status === "active") return true;
    }
    return false;
  };
  /** The model row comes into being (claim or page): `building` beside an active version, else `active` (decision 38 §2). */
  const created = (model: MemoryModel): MemoryModel => {
    if (model.status === undefined) model.status = activeSibling(model.ref) ? "building" : "active";
    return model;
  };
  const existing = (ref: ModelRef): MemoryModel | undefined => {
    const model = models.get(idOf(ref));
    if (model?.status === "retired") throw new RetiredError(ref);
    return model?.status === undefined ? undefined : model;
  };
  const copyRow = (row: MemoryRow): StoredRow => ({
    key: row.key,
    state: row.state ? Uint8Array.from(row.state) : null,
    doc: row.doc,
    failed: row.failed,
    cursor: { ...row.cursor },
    revision: row.revision,
  });
  const failedCount = (model: MemoryModel) => {
    let n = 0;
    for (const row of model.rows.values()) if (row.failed) n++;
    return n;
  };
  const headOf = (model: MemoryModel): ModelHead => ({
    cursor: model.cursor ? { ...model.cursor } : null,
    epoch: model.epoch,
    revision: model.revision,
    failed: failedCount(model),
    status: model.status ?? "active",
    fingerprint: model.fingerprint,
  });
  /** One critical section per model: a page, an activation, a retirement. */
  const exclusive = <T>(model: MemoryModel, run: () => Promise<T>): Promise<T> => {
    const result = model.chain.then(run, run);
    model.chain = result.catch(() => undefined);
    return result;
  };
  const ring = (model: MemoryModel) => {
    for (const listener of [...model.listeners]) {
      try {
        listener();
      } catch {
        // a listener never breaks a page
      }
    }
  };
  /** Hold the model's critical section until the returned release is called (several at once: the cross-tenant page). */
  const acquire = (model: MemoryModel): Promise<() => void> => {
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    const prior = model.chain;
    model.chain = prior.then(
      () => held,
      () => held,
    );
    return prior.then(
      () => release,
      () => release,
    );
  };
  /** Another version of the name `active` in ANY row, any tenant's or a processor row (decision 45 §5.2). */
  const activeAnywhere = (ref: ModelRef): boolean => {
    for (const other of models.values()) {
      if (other.ref.name === ref.name && other.ref.version !== ref.version && other.status === "active") return true;
    }
    return false;
  };
  const processorRef = (ref: ModelRef): ModelRef => ({ tenant: "", name: ref.name, version: ref.version });
  /** The tenants' rows of the processor's name and version (every row but the processor row). */
  const tenantModels = (ref: ModelRef): MemoryModel[] =>
    [...models.values()].filter((m) => m.ref.tenant !== "" && m.ref.name === ref.name && m.ref.version === ref.version && m.status !== undefined);
  const cross: CrossTenantStore = {
    async lock(ref, owner, fingerprint) {
      const model = modelOf(processorRef(ref));
      if (model.owner !== undefined && model.owner !== owner) return null;
      if (model.status === "retired") throw new RetiredError(ref);
      if (model.status === undefined) model.status = activeAnywhere(ref) ? "building" : "active";
      model.owner = owner;
      model.epoch++;
      const replaced = model.fingerprint;
      model.fingerprint = fingerprint;
      return { epoch: model.epoch, status: model.status, fingerprint: replaced };
    },
    async head(ref) {
      const model = existing(processorRef(ref));
      if (!model) return null;
      let failed = 0;
      for (const tenant of tenantModels(ref)) failed += failedCount(tenant);
      return { ...headOf(model), failed };
    },
    async activeSibling(ref) {
      return activeAnywhere(ref);
    },
    async page(ref, epoch, _sequences, fn) {
      const proc = modelOf(processorRef(ref));
      const releases = [await acquire(proc)];
      const locked = new Map<string, MemoryModel>();
      let committed = false;
      try {
        if (proc.status === "retired") throw new RetiredError(ref);
        if (proc.status === undefined) proc.status = activeAnywhere(ref) ? "building" : "active";
        if (proc.epoch !== epoch) throw new FencedError(ref, epoch, proc.epoch);
        const commit = await fn({
          head: headOf(proc),
          tenants: async (tenants) => {
            const out = new Map<string, Cursor | null>();
            for (const tenant of [...new Set(tenants)].sort(compareCodePoints)) {
              if (tenant === "") throw new UsageError("eventstore: a cross-tenant page has no row for the empty tenant");
              let model = locked.get(tenant);
              if (!model) {
                model = modelOf({ tenant, name: ref.name, version: ref.version });
                releases.push(await acquire(model));
                locked.set(tenant, model);
                // created with the processor row's status: building while it builds (decision 45 §5.5)
                if (model.status === undefined) model.status = proc.status;
              }
              out.set(tenant, model.cursor ? { ...model.cursor } : null);
            }
            return out;
          },
          rows: async (pairs) => {
            const out = new Map<string, StoredRow>();
            for (const { tenant, key } of pairs) {
              const row = models.get(idOf({ tenant, name: ref.name, version: ref.version }))?.rows.get(key);
              if (row) out.set(pairKey(tenant, key), copyRow(row));
            }
            return out;
          },
        });
        if (commit === null) return { committed: false };
        // nothing awaited from here on: all of the page or none of it
        if (proc.epoch !== epoch) throw new FencedError(ref, epoch, proc.epoch);
        const cursor = { transactionId: commit.cursor.transactionId, sequence: commit.cursor.sequence };
        const byTenant = new Map<string, TenantRowWrite[]>();
        for (const write of commit.writes) {
          if (!locked.has(write.tenant)) throw new Error(`eventstore: a cross-tenant page wrote tenant ${write.tenant} without locking its row`);
          const list = byTenant.get(write.tenant);
          if (list) list.push(write);
          else byTenant.set(write.tenant, [write]);
        }
        for (const [tenant, model] of locked) {
          const writes = byTenant.get(tenant) ?? [];
          const revision = writes.length > 0 ? model.revision + 1 : model.revision;
          for (const write of writes) {
            const old = model.rows.get(write.key);
            model.rows.set(write.key, {
              key: write.key,
              state: write.state ? Uint8Array.from(write.state) : null,
              doc: write.doc,
              failed: write.failed,
              cursor,
              revision,
              prevDoc: old ? old.doc : null,
              prevRevision: old ? old.revision : null,
            });
          }
          // greatest: a tenant checkpoint never goes back (a per-tenant writer of the same version may be ahead)
          if (model.cursor === null || compareCursor(cursor, model.cursor) > 0) model.cursor = cursor;
          model.revision = revision;
        }
        if (proc.cursor === null || compareCursor(cursor, proc.cursor) > 0) proc.cursor = cursor;
        if (commit.writes.length > 0) proc.revision++;
        committed = true;
        return { committed: true };
      } finally {
        for (const release of releases.reverse()) release();
        if (committed) {
          for (const model of locked.values()) ring(model);
          ring(proc);
        }
      }
    },
    async activate(ref, epoch) {
      const proc = modelOf(processorRef(ref));
      const releases = [await acquire(proc)];
      const switched: MemoryModel[] = [];
      try {
        if (proc.status === "retired") throw new RetiredError(ref);
        if (proc.epoch !== epoch) throw new FencedError(ref, epoch, proc.epoch);
        if (proc.status !== "building") return;
        // every tenant that has any version of the name gets an active row of this one (decision 45 §7)
        const tenants = new Set<string>();
        for (const model of models.values()) if (model.ref.tenant !== "" && model.ref.name === ref.name && model.status !== undefined) tenants.add(model.ref.tenant);
        for (const tenant of [...tenants].sort(compareCodePoints)) {
          const model = modelOf({ tenant, name: ref.name, version: ref.version });
          releases.push(await acquire(model));
          if (model.status === undefined || model.status === "building") {
            model.status = "active";
            switched.push(model);
          }
        }
        proc.status = "active";
        switched.push(proc);
      } finally {
        for (const release of releases.reverse()) release();
        for (const model of switched) ring(model);
      }
    },
  };
  return {
    pushes: true,
    knobs,
    attach: () => () => undefined,
    async lock(ref, owner, fingerprint) {
      const model = modelOf(ref);
      if (model.owner !== undefined && model.owner !== owner) return null;
      if (model.status === "retired") throw new RetiredError(ref);
      created(model);
      model.owner = owner;
      model.epoch++;
      const replaced = model.fingerprint;
      model.fingerprint = fingerprint;
      return { epoch: model.epoch, status: model.status!, fingerprint: replaced };
    },
    async unlock(ref, owner) {
      const model = modelOf(ref);
      if (model.owner === owner) model.owner = undefined;
    },
    onLost: () => () => undefined,
    steal(ref) {
      modelOf(ref).owner = undefined;
    },
    retire(ref) {
      const model = modelOf(ref);
      return exclusive(model, async () => {
        model.status = "retired";
        model.rows.clear();
        ring(model);
      });
    },
    async head(ref) {
      const model = existing(ref);
      return model ? headOf(model) : null;
    },
    async activeSibling(ref) {
      return activeSibling(ref);
    },
    activate(ref, epoch) {
      const model = modelOf(ref);
      return exclusive(model, async () => {
        if (model.status === "retired") throw new RetiredError(ref);
        if (model.epoch !== epoch) throw new FencedError(ref, epoch, model.epoch);
        if (model.status !== "building") return;
        model.status = "active";
        ring(model);
      });
    },
    async snapshot(ref, onRows) {
      // nothing awaited: one snapshot, as one REPEATABLE READ transaction on Postgres
      const model = models.get(idOf(ref));
      if (!model || model.status === undefined) return null;
      if (model.status !== "retired") onRows([...model.rows.values()].map(copyRow));
      return { checkpoint: model.cursor ? { ...model.cursor } : null, status: model.status, fingerprint: model.fingerprint };
    },
    page(ref, epoch, fn) {
      const model = modelOf(ref);
      const run = async (): Promise<{ revision: number; committed: boolean }> => {
        // a retired version is final: checked before the epoch
        if (model.status === "retired") throw new RetiredError(ref);
        created(model);
        if (model.epoch !== epoch) throw new FencedError(ref, epoch, model.epoch);
        const commit = await fn({
          head: headOf(model),
          rows: async (keys) => {
            const out = new Map<string, StoredRow>();
            for (const key of keys) {
              const row = model.rows.get(key);
              if (row) out.set(key, copyRow(row));
            }
            return out;
          },
        });
        if (commit === null) return { revision: model.revision, committed: false };
        // nothing awaited from here on: all of the page or none of it
        if (model.epoch !== epoch) throw new FencedError(ref, epoch, model.epoch);
        const revision = commit.writes.length > 0 ? model.revision + 1 : model.revision;
        const cursor = { transactionId: commit.cursor.transactionId, sequence: commit.cursor.sequence };
        for (const write of commit.writes) {
          const old = model.rows.get(write.key);
          model.rows.set(write.key, {
            key: write.key,
            state: write.state ? Uint8Array.from(write.state) : null,
            doc: write.doc,
            failed: write.failed,
            cursor,
            revision,
            prevDoc: old ? old.doc : null,
            prevRevision: old ? old.revision : null,
          });
        }
        if (model.cursor === null || compareCursor(cursor, model.cursor) > 0) model.cursor = cursor;
        model.revision = revision;
        ring(model);
        return { revision, committed: true };
      };
      return exclusive(model, run);
    },
    async row(ref, key) {
      const row = models.get(idOf(ref))?.rows.get(key);
      return row ? copyRow(row) : null;
    },
    async latest(ref, key) {
      // nothing awaited: one snapshot, as one statement on Postgres
      const model = existing(ref);
      const row = model?.rows.get(key);
      return { row: row ? copyRow(row) : null, checkpoint: model?.cursor ? { ...model.cursor } : null, fingerprint: model?.fingerprint ?? null };
    },
    async list(ref, query) {
      const model = existing(ref);
      if (!model) return { rows: [], total: 0, failed: 0, revision: 0, fingerprint: null };
      const picked: { key: string; rank: number; value: unknown; row: MemoryRow }[] = [];
      let failed = 0;
      for (const row of model.rows.values()) {
        if (row.failed) {
          failed++;
          continue;
        }
        if (!docMatches(row.doc, query.where)) continue;
        const value = query.sort ? docValue(row.doc!, query.sort.by) : undefined;
        picked.push({ key: row.key, rank: query.sort ? rankOf(value) : 0, value, row });
      }
      const desc = query.sort?.dir === "desc" ? -1 : 1;
      picked.sort((a, b) => compareListed(a, b, desc));
      const rows = picked.slice(query.offset, query.offset + query.limit).map((p) => ({ key: p.key, state: Uint8Array.from(p.row.state!) }));
      return { rows, total: picked.length, failed, revision: model.revision, fingerprint: model.fingerprint };
    },
    async changes(ref, after, limit) {
      const model = models.get(idOf(ref));
      if (!model) return [];
      const out: MemoryRow[] = [];
      for (const row of model.rows.values()) {
        if (row.revision > after.revision || (row.revision === after.revision && after.key !== null && compareCodePoints(row.key, after.key) > 0)) out.push(row);
      }
      out.sort((a, b) => a.revision - b.revision || compareCodePoints(a.key, b.key));
      return out.slice(0, limit).map((row) => ({ ...copyRow(row), prevDoc: row.prevDoc, prevRevision: row.prevRevision }));
    },
    onChanged(ref, listener) {
      const model = modelOf(ref);
      model.listeners.add(listener);
      return () => model.listeners.delete(listener);
    },
    // the memory store has no rls: its root reads every tenant
    crossTenant: async () => cross,
  };
}
