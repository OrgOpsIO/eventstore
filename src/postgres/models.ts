/**
 * The persisted read models on Postgres (decisions 26–28, 30, 31): the two tables, the page
 * transaction, the list SQL, and the one lock connection per store that holds every writer lock.
 */
import { createHash } from "node:crypto";
import type pg from "pg";
import type { Pool, PoolClient } from "pg";
import { EventStoreError, TenantMismatchError, TransientError, UsageError } from "../errors.js";
import { lockKeyOf } from "../ids.js";
import {
  FencedError,
  MAX_WRITERS_PER_STORE,
  RetiredError,
  WRITER_TICK_MS,
  pairKey,
  type ChangedRow,
  type CrossTenantStore,
  type FailedMarker,
  type ListPage,
  type ListQuery,
  type ModelHead,
  type ModelRef,
  type ReadModelStore,
  type StoredRow,
  type VersionStatus,
  type WriterKnobs,
} from "../modelstore.js";

/** The rows a snapshot reads per statement, keyset on `key_hash`, all inside its one transaction (decision 43 §2.2). */
const SNAPSHOT_PAGE = 5_000;
import type { Cursor } from "../types.js";
import type { CommitListener } from "./live.js";
import { erasureLockArgs, readModelChannel, tableRef, type ReadModelTargets, type Target } from "./sql.js";

/** What the lock session needs of a connection: `pg.Client` and a checked-out `PoolClient` both fit. */
export interface LockConnection {
  query<T extends pg.QueryResultRow = pg.QueryResultRow>(sql: string, params?: unknown[]): Promise<pg.QueryResult<T>>;
  on(event: "error" | "end", listener: (error?: Error) => void): unknown;
}

export interface OpenedLockConnection {
  readonly connection: LockConnection;
  /** Close it for good: the server frees every lock of the session (never back into a pool). */
  release(): void;
}

export interface PostgresReadModelsOptions {
  readonly pool: Pool;
  readonly target: Target;
  readonly targets: ReadModelTargets;
  readonly lockSalt: string | undefined;
  readonly rls: boolean;
  /** Under `rls`: the role a cross-tenant processor connects as (decision 46). */
  readonly processorRole: string | undefined;
  /** Whether the store is connected as `processorRole` (read once). */
  readonly isProcessor: () => Promise<boolean>;
  readonly listener: CommitListener | undefined;
  readonly openLockConnection: () => Promise<OpenedLockConnection>;
  readonly ensureInstalled: () => Promise<void>;
  /** The store's error translation (transient codes, permissions, rls). */
  readonly translate: (err: unknown, inTenantSession: boolean) => unknown;
}

interface HeadRow {
  epoch: string;
  cursor_xid: string | null;
  cursor_seq: string | null;
  revision: string;
  status: VersionStatus;
  fingerprint: string | null;
}

interface StateRow {
  key: string;
  state: Buffer | null;
  doc: Record<string, unknown> | null;
  failed: FailedMarker | null;
  cursor_xid: string;
  cursor_seq: string;
  revision: string;
}

/**
 * The writer locks of one store in one process: ONE session outside the pool (decision 27 §1),
 * with TCP keepalives so the server frees the locks of a vanished host in ~25 s, a heartbeat every
 * tick, and at most `MAX_WRITERS_PER_STORE` locks held.
 */
class WriterLocks {
  private opened: OpenedLockConnection | undefined;
  private opening: Promise<LockConnection> | undefined;
  private generation = 0;
  private heartbeat: ReturnType<typeof setInterval> | undefined;
  private readonly holders = new Map<string, object>();
  private readonly owners = new Set<object>();
  private readonly lostListeners = new Set<() => void>();
  /** The last statement queued on each session: one runs at a time. Per session, so a lost one's queue never holds up the next. */
  private readonly tails = new WeakMap<LockConnection, Promise<unknown>>();

  constructor(
    private readonly open: () => Promise<OpenedLockConnection>,
    readonly knobs: WriterKnobs,
  ) {}

  /** One statement at a time on the lock session: handles start and stop concurrently, and pg must not queue for us. */
  private serial<T extends pg.QueryResultRow = pg.QueryResultRow>(connection: LockConnection, sql: string, params?: unknown[]): Promise<pg.QueryResult<T>> {
    const result = (this.tails.get(connection) ?? Promise.resolve()).then(() => connection.query<T>(sql, params));
    this.tails.set(connection, result.catch(() => undefined));
    return result;
  }

  attach(owner: object): () => void {
    this.owners.add(owner);
    return () => {
      this.owners.delete(owner);
      if (this.owners.size === 0) this.disconnect(); // the last handle of the store closes the lock session
    };
  }

  onLost(listener: () => void): () => void {
    this.lostListeners.add(listener);
    return () => this.lostListeners.delete(listener);
  }

  /** `pg_try_advisory_lock` on the lock session; `false` when another session or another handle of this process holds it, or at the cap. */
  async tryLock(key: string, owner: object): Promise<boolean> {
    const holder = this.holders.get(key);
    if (holder !== undefined) return holder === owner;
    if (this.holders.size >= MAX_WRITERS_PER_STORE) return false;
    this.holders.set(key, owner); // reserved: a second handle of this process must not take the same (re-entrant) session lock
    try {
      const connection = await this.connection();
      if (this.holders.get(key) !== owner) return false; // unlocked or dropped while the session was opening
      const result = await this.serial<{ ok: boolean }>(connection, "SELECT pg_try_advisory_lock($1::bigint) AS ok", [key]);
      const won = result.rows[0]?.ok === true;
      if (won && this.holders.get(key) === owner && this.opened?.connection === connection) return true;
      // given up meanwhile (stopped, or the session dropped): never keep a lock nobody holds
      if (won) await this.serial(connection, "SELECT pg_advisory_unlock($1::bigint)", [key]).catch(() => undefined);
      if (this.holders.get(key) === owner) this.holders.delete(key);
      return false;
    } catch (error) {
      if (this.holders.get(key) === owner) this.holders.delete(key);
      throw error;
    }
  }

  async unlock(key: string, owner: object): Promise<void> {
    if (this.holders.get(key) !== owner) return;
    this.holders.delete(key);
    const opened = this.opened;
    if (!opened) return;
    try {
      await this.serial(opened.connection, "SELECT pg_advisory_unlock($1::bigint)", [key]);
    } catch {
      // a lost session has released it already
    }
  }

  private connection(): Promise<LockConnection> {
    if (this.opened) return Promise.resolve(this.opened.connection);
    if (!this.opening) {
      this.opening = (async () => {
        const started = this.generation;
        const opened = await this.open();
        const generation = ++this.generation;
        const { connection } = opened;
        connection.on("error", () => this.lost(generation));
        connection.on("end", () => this.lost(generation));
        try {
          // dropped while connecting (the last handle detached, the store closed): never keep a session nobody closes
          if (started !== generation - 1 || this.owners.size === 0) throw new Error("eventstore/postgres: the writer lock session was closed while it opened");
          // context `user`: the server notices a vanished host after ~10 + 3 × 5 s instead of the OS default of hours
          await this.serial(connection, "SET tcp_keepalives_idle = 10");
          await this.serial(connection, "SET tcp_keepalives_interval = 5");
          await this.serial(connection, "SET tcp_keepalives_count = 3");
          if (this.generation !== generation) throw new Error("eventstore/postgres: the writer lock session was closed while it opened");
        } catch (error) {
          opened.release();
          throw error;
        }
        this.opened = opened;
        if (this.knobs.noticeLoss) {
          let beating = false;
          this.heartbeat = setInterval(() => {
            if (beating) return; // the last one is still queued or running: never pile them up behind a slow session
            beating = true;
            this.serial(connection, "SELECT 1")
              .catch(() => this.lost(generation))
              .finally(() => (beating = false));
          }, this.knobs.tickMs);
          this.heartbeat.unref?.();
        }
        return connection;
      })().finally(() => {
        this.opening = undefined;
      });
    }
    return this.opening;
  }

  /** The lock session is gone, and with it every lock it held: every writer of this store goes to standby. */
  private lost(generation: number): void {
    if (generation !== this.generation || !this.knobs.noticeLoss) return;
    this.drop();
    for (const listener of [...this.lostListeners]) {
      try {
        listener();
      } catch {
        // a listener never breaks the lock session
      }
    }
  }

  private drop(): void {
    this.generation++;
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = undefined;
    const opened = this.opened;
    this.opened = undefined;
    this.holders.clear();
    opened?.release();
  }

  disconnect(): void {
    this.drop();
  }
}

/** The primary-key column of a state row: SHA-256 of the key (the key itself can outgrow a B-tree entry). */
function keyHash(key: string): Buffer {
  return createHash("sha256").update(key).digest();
}

function toInt(value: string | number): number {
  const n = Number(value);
  if (!Number.isSafeInteger(n)) throw new EventStoreError(`eventstore/postgres: ${value} is not a safe integer`);
  return n;
}

function cursorOfRow(xid: string | null, seq: string | null): Cursor | null {
  return xid === null || seq === null ? null : { transactionId: xid, sequence: toInt(seq) };
}

function storedOf(row: StateRow): StoredRow {
  return { key: row.key, state: row.state, doc: row.doc, failed: row.failed, cursor: cursorOfRow(row.cursor_xid, row.cursor_seq)!, revision: toInt(row.revision) };
}

/** The ORDER BY of a list: the live model's ranks (missing last both ways), values within a rank, ties by key — all by code point. */
function orderSql(sort: ListQuery["sort"], field: string): string {
  if (!sort) return `key COLLATE "C"`;
  const dir = sort.dir === "desc" ? "DESC" : "ASC";
  const typ = `jsonb_typeof(doc -> ${field})`;
  const rank = `CASE ${typ} WHEN 'number' THEN 0 WHEN 'string' THEN 1 WHEN 'boolean' THEN 2 WHEN 'object' THEN 3 END`;
  return [
    `(${rank}) IS NULL`,
    `(${rank}) ${dir}`,
    `CASE WHEN ${typ} = 'number' THEN (doc ->> ${field})::numeric END ${dir}`,
    `CASE WHEN ${typ} = 'string' THEN doc ->> ${field} END COLLATE "C" ${dir}`,
    `CASE WHEN ${typ} = 'boolean' THEN (doc ->> ${field})::boolean END ${dir}`,
    `CASE WHEN ${typ} = 'object' THEN (doc -> ${field} ->> '$date')::numeric END ${dir}`,
    `key COLLATE "C"`,
  ].join(", ");
}

/** The capability of one Postgres store: one per tenant session, sharing the lock session and the knobs. */
export class PostgresReadModels {
  readonly knobs: WriterKnobs = { tickMs: WRITER_TICK_MS, noticeLoss: true };
  private readonly locks: WriterLocks;
  private readonly local = new Map<string, Set<() => void>>();
  private readonly m: string;
  private readonly s: string;
  private readonly channel: string;
  private cross: CrossTenantStore | undefined;

  constructor(private readonly options: PostgresReadModelsOptions) {
    this.locks = new WriterLocks(options.openLockConnection, this.knobs);
    this.m = tableRef(options.targets.model);
    this.s = tableRef(options.targets.state);
    this.channel = readModelChannel(options.target);
  }

  /**
   * The lock key of a model (decisions 27 §2, 35), also the notification payload: an HMAC with the salt, no names in
   * clear. It names the events table too — schema and table, as the rows do — so stores of other tables in the same
   * database never compete for it; every process on this table derives the same key.
   */
  private keyOf(tenant: string, ref: ModelRef): string {
    const { schemaName, table } = this.options.target;
    return lockKeyOf(`readmodel\u0000${schemaName}\u0000${table}\u0000${tenant}\u0000${ref.name}\u0000${ref.version}`, this.options.lockSalt).toString();
  }

  close(): void {
    this.locks.disconnect();
  }

  /**
   * The capability for the root store (`session` `null`: tenant `''`, no session variable) or for one
   * tenant session. Under `rls` the root refuses, and so does the empty tenant.
   */
  capability(session: string | null): ReadModelStore {
    const rls = this.options.rls;
    let refusal: Error | undefined;
    if (rls && session === null) {
      refusal = new UsageError(
        "eventstore/postgres: rls is enabled — persisted read models are read and written only in a tenant session: use es.forTenant(tenantId).persistedReadModel(def); the root store has none",
      );
    } else if (rls && session === "") {
      refusal = new UsageError("eventstore/postgres: rls is enabled — a persisted read model needs a tenant; the empty tenant id has none");
    }
    const tenantOf = (ref: ModelRef): string => {
      if (session === null) return ref.tenant ?? "";
      if (ref.tenant !== undefined && ref.tenant !== session) {
        throw new TenantMismatchError(`eventstore/postgres: a read model reference names tenant ${ref.tenant} but the session is bound to ${session}`);
      }
      return session;
    };
    const refuse = (): void => {
      if (refusal) throw refusal;
    };
    const run = <T>(fn: (runner: { query: PoolClient["query"] } | Pool) => Promise<T>): Promise<T> => this.run(session, fn);
    const pushes = this.options.listener !== undefined;
    // the model row comes into being `building` beside an active version of the name, else `active` (decision 38 §2)
    const create = `INSERT INTO ${this.m} (tenant, name, version, status) VALUES ($1, $2, $3,
  CASE WHEN EXISTS (SELECT 1 FROM ${this.m} WHERE tenant = $1 AND name = $2 AND version <> $3 AND status = 'active') THEN 'building' ELSE 'active' END)
ON CONFLICT DO NOTHING`;
    const lockRow = `SELECT epoch::text AS epoch, cursor_xid::text AS cursor_xid, cursor_seq::text AS cursor_seq, revision::text AS revision, status, fingerprint FROM ${this.m} WHERE tenant = $1 AND name = $2 AND version = $3 FOR UPDATE`;
    return {
      ...(refusal ? { refusal } : {}),
      pushes,
      knobs: this.knobs,
      attach: (owner) => this.locks.attach(owner),
      lock: async (ref, owner, fingerprint) => {
        refuse();
        const tenant = tenantOf(ref);
        const key = this.keyOf(tenant, ref);
        if (!(await this.locks.tryLock(key, owner))) return null;
        try {
          return await this.tx(session, async (c) => {
            const params = [tenant, ref.name, ref.version];
            await c.query(create, params);
            const old = (await c.query<HeadRow>(lockRow, params)).rows[0]!;
            if (old.status === "retired") throw new RetiredError(ref); // a tombstone is never claimed: no rebuild
            const claimed = await c.query<{ epoch: string }>(
              `UPDATE ${this.m} SET epoch = epoch + 1, fingerprint = $4, updated_at = now() WHERE tenant = $1 AND name = $2 AND version = $3 RETURNING epoch::text AS epoch`,
              [...params, fingerprint],
            );
            return { epoch: toInt(claimed.rows[0]!.epoch), status: old.status, fingerprint: old.fingerprint };
          });
        } catch (error) {
          await this.locks.unlock(key, owner);
          throw error;
        }
      },
      unlock: async (ref, owner) => {
        // never refused: the processor of an rls store takes its lock through crossTenant() and releases it here
        await this.locks.unlock(this.keyOf(tenantOf(ref), ref), owner);
      },
      onLost: (listener) => this.locks.onLost(listener),
      head: async (ref) => {
        refuse();
        const tenant = tenantOf(ref);
        return run((r) => this.head(r, tenant, ref));
      },
      activeSibling: async (ref) => {
        refuse();
        const tenant = tenantOf(ref);
        const found = await run((r) =>
          r.query<{ active: boolean }>(`SELECT EXISTS (SELECT 1 FROM ${this.m} WHERE tenant = $1 AND name = $2 AND version <> $3 AND status = 'active') AS active`, [tenant, ref.name, ref.version]),
        );
        return found.rows[0]?.active === true;
      },
      activate: async (ref, epoch) => {
        refuse();
        const tenant = tenantOf(ref);
        const key = this.keyOf(tenant, ref);
        const params = [tenant, ref.name, ref.version];
        const switched = await this.tx(session, async (c) => {
          const locked = (await c.query<HeadRow>(lockRow, params)).rows[0];
          if (locked?.status === "retired") throw new RetiredError(ref);
          const current = locked ? toInt(locked.epoch) : 0;
          if (current !== epoch) throw new FencedError(ref, epoch, current);
          if (locked!.status !== "building") return false;
          await c.query(`UPDATE ${this.m} SET status = 'active', updated_at = now() WHERE tenant = $1 AND name = $2 AND version = $3`, params);
          // the same channel and payload as a page: waiting reads in every process look again
          await c.query("SELECT pg_notify($1, $2)", [this.channel, key]);
          return true;
        });
        if (switched) this.ringLocal(key);
      },
      snapshot: async (ref, onRows) => {
        refuse();
        const tenant = tenantOf(ref);
        const params = [tenant, ref.name, ref.version];
        // one REPEATABLE READ snapshot: the checkpoint and every row of it, in pages, never two statements' views
        return this.tx(
          session,
          async (c) => {
            const model = (
              await c.query<HeadRow>(
                `SELECT epoch::text AS epoch, cursor_xid::text AS cursor_xid, cursor_seq::text AS cursor_seq, revision::text AS revision, status, fingerprint FROM ${this.m} WHERE tenant = $1 AND name = $2 AND version = $3`,
                params,
              )
            ).rows[0];
            if (!model) return null;
            if (model.status !== "retired") {
              let after: Buffer | null = null;
              for (;;) {
                const page: pg.QueryResult<StateRow & { key_hash: Buffer }> = await c.query<StateRow & { key_hash: Buffer }>(
                  `SELECT key_hash, key, state, doc, failed, cursor_xid::text AS cursor_xid, cursor_seq::text AS cursor_seq, revision::text AS revision FROM ${this.s}
WHERE tenant = $1 AND name = $2 AND version = $3 AND ($4::bytea IS NULL OR key_hash > $4::bytea) ORDER BY key_hash LIMIT ${SNAPSHOT_PAGE}`,
                  [...params, after],
                );
                onRows(page.rows.map(storedOf));
                if (page.rows.length < SNAPSHOT_PAGE) break;
                after = page.rows[page.rows.length - 1]!.key_hash;
              }
            }
            return { checkpoint: cursorOfRow(model.cursor_xid, model.cursor_seq), status: model.status, fingerprint: model.fingerprint };
          },
          "REPEATABLE READ READ ONLY",
        );
      },
      page: async (ref, epoch, fn) => {
        refuse();
        const tenant = tenantOf(ref);
        const key = this.keyOf(tenant, ref);
        const params = [tenant, ref.name, ref.version];
        const result = await this.tx(session, async (c) => {
          let locked = (await c.query<HeadRow>(lockRow, params)).rows[0];
          if (!locked) {
            await c.query(create, params);
            locked = (await c.query<HeadRow>(lockRow, params)).rows[0]!;
          }
          // a retired version is final: checked before the epoch, under the row lock the retire recipe takes too
          if (locked.status === "retired") throw new RetiredError(ref);
          const current = toInt(locked.epoch);
          if (current !== epoch) throw new FencedError(ref, epoch, current);
          const revision = toInt(locked.revision);
          const commit = await fn({
            head: { cursor: cursorOfRow(locked.cursor_xid, locked.cursor_seq), epoch: current, revision },
            rows: async (keys) => {
              const out = new Map<string, StoredRow>();
              if (keys.length === 0) return out;
              const found = await c.query<StateRow>(
                `SELECT key, state, doc, failed, cursor_xid::text AS cursor_xid, cursor_seq::text AS cursor_seq, revision::text AS revision FROM ${this.s}
WHERE tenant = $1 AND name = $2 AND version = $3 AND key_hash = ANY($4::bytea[])`,
                [...params, keys.map(keyHash)],
              );
              for (const row of found.rows) out.set(row.key, storedOf(row)); // a hash collision is a miss: its key differs
              return out;
            },
          });
          if (commit === null) return { revision, committed: false };
          const next = commit.writes.length > 0 ? revision + 1 : revision;
          const cursor = [commit.cursor.transactionId, String(commit.cursor.sequence)];
          if (commit.writes.length > 0) {
            await c.query(
              `INSERT INTO ${this.s} AS t (tenant, name, version, key_hash, key, state, doc, failed, cursor_xid, cursor_seq, revision)
SELECT $1, $2, $3, u.h, u.k, u.s, u.d, u.f, $4::xid8, $5::bigint, $6::bigint
FROM unnest($7::bytea[], $8::text[], $9::bytea[], $10::jsonb[], $11::jsonb[]) AS u(h, k, s, d, f)
ON CONFLICT (tenant, name, version, key_hash) DO UPDATE SET
  key = EXCLUDED.key, state = EXCLUDED.state, doc = EXCLUDED.doc, failed = EXCLUDED.failed,
  prev_doc = t.doc, prev_revision = t.revision,
  cursor_xid = EXCLUDED.cursor_xid, cursor_seq = EXCLUDED.cursor_seq, revision = EXCLUDED.revision`,
              [
                ...params,
                ...cursor,
                String(next),
                commit.writes.map((w) => keyHash(w.key)),
                commit.writes.map((w) => w.key),
                commit.writes.map((w) => (w.state ? Buffer.from(w.state.buffer, w.state.byteOffset, w.state.byteLength) : null)),
                commit.writes.map((w) => (w.doc ? JSON.stringify(w.doc) : null)),
                commit.writes.map((w) => (w.failed ? JSON.stringify(w.failed) : null)),
              ],
            );
          }
          await c.query(`UPDATE ${this.m} SET cursor_xid = $4::xid8, cursor_seq = $5::bigint, revision = $6::bigint, updated_at = now() WHERE tenant = $1 AND name = $2 AND version = $3`, [
            ...params,
            ...cursor,
            String(next),
          ]);
          // transactional: a rolled-back page rings nothing; the payload is the lock key, never a name or a tenant
          await c.query("SELECT pg_notify($1, $2)", [this.channel, key]);
          return { revision: next, committed: true };
        });
        if (result.committed) this.ringLocal(key);
        return result;
      },
      row: async (ref, key) => {
        refuse();
        const tenant = tenantOf(ref);
        const found = await run((r) =>
          r.query<StateRow>(
            `SELECT key, state, doc, failed, cursor_xid::text AS cursor_xid, cursor_seq::text AS cursor_seq, revision::text AS revision FROM ${this.s}
WHERE tenant = $1 AND name = $2 AND version = $3 AND key_hash = $4`,
            [tenant, ref.name, ref.version, keyHash(key)],
          ),
        );
        const row = found.rows[0];
        return row && row.key === key ? storedOf(row) : null;
      },
      latest: async (ref, key) => {
        refuse();
        const tenant = tenantOf(ref);
        // one statement, one snapshot: a page committing in between moves both or neither
        const found = await run((r) =>
          r.query<{ [K in keyof StateRow]: StateRow[K] | null } & { m_xid: string | null; m_seq: string | null; m_status: VersionStatus; m_fingerprint: string | null }>(
            `SELECT m.cursor_xid::text AS m_xid, m.cursor_seq::text AS m_seq, m.status AS m_status, m.fingerprint AS m_fingerprint,
  s.key, s.state, s.doc, s.failed, s.cursor_xid::text AS cursor_xid, s.cursor_seq::text AS cursor_seq, s.revision::text AS revision
FROM ${this.m} m LEFT JOIN ${this.s} s ON s.tenant = m.tenant AND s.name = m.name AND s.version = m.version AND s.key_hash = $4
WHERE m.tenant = $1 AND m.name = $2 AND m.version = $3`,
            [tenant, ref.name, ref.version, keyHash(key)],
          ),
        );
        const hit = found.rows[0];
        if (!hit) return { row: null, checkpoint: null, fingerprint: null };
        if (hit.m_status === "retired") throw new RetiredError(ref);
        const row = hit.key === key ? storedOf(hit as StateRow) : null; // a hash collision is a miss: its key differs
        return { row, checkpoint: cursorOfRow(hit.m_xid, hit.m_seq), fingerprint: hit.m_fingerprint };
      },
      list: async (ref, query) => {
        refuse();
        const tenant = tenantOf(ref);
        return run((r) => this.list(r, tenant, ref, query));
      },
      changes: async (ref, after, limit) => {
        refuse();
        const tenant = tenantOf(ref);
        const found = await run((r) =>
          r.query<StateRow & { prev_doc: Record<string, unknown> | null; prev_revision: string | null }>(
            `SELECT key, state, doc, prev_doc, prev_revision::text AS prev_revision, failed, cursor_xid::text AS cursor_xid, cursor_seq::text AS cursor_seq, revision::text AS revision FROM ${this.s}
WHERE tenant = $1 AND name = $2 AND version = $3 AND (revision > $4::bigint OR (revision = $4::bigint AND key COLLATE "C" > $5::text))
ORDER BY revision, key COLLATE "C" LIMIT $6`,
            [tenant, ref.name, ref.version, String(after.revision), after.key, limit],
          ),
        );
        return found.rows.map((row): ChangedRow => ({ ...storedOf(row), prevDoc: row.prev_doc, prevRevision: row.prev_revision === null ? null : toInt(row.prev_revision) }));
      },
      onChanged: (ref, listener) => {
        const key = this.keyOf(tenantOf(ref), ref);
        let set = this.local.get(key);
        if (!set) this.local.set(key, (set = new Set()));
        set.add(listener);
        // across processes: the model channel on the store's one LISTEN connection (live: true)
        const off = this.options.listener?.addChannel(this.channel, (payload) => {
          if (payload === null || payload === key) listener();
        });
        return () => {
          set.delete(listener);
          if (set.size === 0 && this.local.get(key) === set) this.local.delete(key);
          off?.();
        };
      },
      ...(session === null ? { crossTenant: () => this.crossTenant() } : {}),
    };
  }

  /** The cross-tenant backend, on the root only; under `rls` only for a store connected as the processor role (decision 46 §4). */
  private async crossTenant(): Promise<CrossTenantStore> {
    if (this.options.rls) {
      const role = this.options.processorRole;
      if (role === undefined) {
        throw new UsageError(
          "eventstore/postgres: rls is enabled — a cross-tenant processor reads and writes every tenant, which the application role may not: set postgres.processorRole and run the processor (es.persistedReadModel(def) on the root view) on an api connected as that role; tenant handles es.forTenant(t).persistedReadModel(def) read it",
        );
      }
      if (!(await this.options.isProcessor())) {
        throw new UsageError(
          `eventstore/postgres: rls is enabled — the cross-tenant processor runs only on a store connected as the processor role "${role}" (postgres.processorRole), and this store is connected as another role; open the model on es.forTenant(t) here to read it`,
        );
      }
    }
    return (this.cross ??= this.crossStore());
  }

  /** The processor row's SQL and the page across tenants (decisions 45 §5, 47). Every statement on the pool, no tenant session. */
  private crossStore(): CrossTenantStore {
    const m = this.m;
    const s = this.s;
    const events = tableRef(this.options.target);
    // the processor row comes into being `building` when another version of the name is active in ANY row (45 §5.2)
    const create = `INSERT INTO ${m} (tenant, name, version, status) VALUES ('', $1, $2,
  CASE WHEN EXISTS (SELECT 1 FROM ${m} WHERE name = $1 AND version <> $2 AND status = 'active') THEN 'building' ELSE 'active' END)
ON CONFLICT DO NOTHING`;
    const lockRow = `SELECT epoch::text AS epoch, cursor_xid::text AS cursor_xid, cursor_seq::text AS cursor_seq, revision::text AS revision, status, fingerprint FROM ${m} WHERE tenant = '' AND name = $1 AND version = $2 FOR UPDATE`;
    const processorRow = async (c: PoolClient, ref: ModelRef): Promise<HeadRow> => {
      const params = [ref.name, ref.version];
      let locked = (await c.query<HeadRow>(lockRow, params)).rows[0];
      if (!locked) {
        await c.query(create, params);
        locked = (await c.query<HeadRow>(lockRow, params)).rows[0]!;
      }
      return locked;
    };
    const processorKey = (ref: ModelRef) => this.keyOf("", ref);
    return {
      lock: async (ref, owner, fingerprint) => {
        const key = processorKey(ref);
        if (!(await this.locks.tryLock(key, owner))) return null;
        try {
          return await this.tx(null, async (c) => {
            const old = await processorRow(c, ref);
            if (old.status === "retired") throw new RetiredError(ref);
            const claimed = await c.query<{ epoch: string }>(
              `UPDATE ${m} SET epoch = epoch + 1, fingerprint = $3, updated_at = now() WHERE tenant = '' AND name = $1 AND version = $2 RETURNING epoch::text AS epoch`,
              [ref.name, ref.version, fingerprint],
            );
            return { epoch: toInt(claimed.rows[0]!.epoch), status: old.status, fingerprint: old.fingerprint };
          });
        } catch (error) {
          await this.locks.unlock(key, owner);
          throw error;
        }
      },
      head: async (ref) => {
        const found = await this.run(null, (r) =>
          r.query<HeadRow & { failed: string }>(
            `SELECT m.epoch::text AS epoch, m.cursor_xid::text AS cursor_xid, m.cursor_seq::text AS cursor_seq, m.revision::text AS revision, m.status, m.fingerprint,
  (SELECT count(*) FROM ${s} st WHERE st.tenant <> '' AND st.name = $1 AND st.version = $2 AND st.failed IS NOT NULL)::text AS failed
FROM ${m} m WHERE m.tenant = '' AND m.name = $1 AND m.version = $2`,
            [ref.name, ref.version],
          ),
        );
        const row = found.rows[0];
        if (!row) return null;
        if (row.status === "retired") throw new RetiredError(ref);
        return { cursor: cursorOfRow(row.cursor_xid, row.cursor_seq), epoch: toInt(row.epoch), revision: toInt(row.revision), failed: toInt(row.failed), status: row.status, fingerprint: row.fingerprint };
      },
      activeSibling: async (ref) => {
        const found = await this.run(null, (r) =>
          r.query<{ active: boolean }>(`SELECT EXISTS (SELECT 1 FROM ${m} WHERE name = $1 AND version <> $2 AND status = 'active') AS active`, [ref.name, ref.version]),
        );
        return found.rows[0]?.active === true;
      },
      page: async (ref, epoch, sequences, fn) => {
        const name = ref.name;
        const version = ref.version;
        const keys: string[] = [];
        const result = await this.tx(null, async (c) => {
          // the erasure fence (decision 47): shared lock first, then — in a later statement — every event of the part still there
          await c.query(`SELECT pg_advisory_xact_lock_shared(${erasureLockArgs(this.options.target)})`);
          const wanted = new Set(sequences).size;
          const present = await c.query<{ n: string }>(`SELECT count(*)::text AS n FROM ${events} WHERE sequence_number = ANY($1::bigint[])`, [sequences.map(String)]);
          if (toInt(present.rows[0]!.n) < wanted) {
            const stale = new TransientError("read_model_page_stale");
            stale.message = `eventstore/postgres: persisted read model "${name}" v${version}: ${wanted - toInt(present.rows[0]!.n)} of the page's ${wanted} events are gone (a tenant was erased) — the page is read again`;
            throw stale;
          }
          await this.knobs.inPage?.(sequences);
          const locked = await processorRow(c, ref);
          // a retired version is final: checked before the epoch, under the row lock the retire recipe takes too
          if (locked.status === "retired") throw new RetiredError(ref);
          const current = toInt(locked.epoch);
          if (current !== epoch) throw new FencedError(ref, epoch, current);
          const revision = toInt(locked.revision);
          const tenants = new Map<string, number>(); // tenant → its revision, for every tenant row locked
          const commit = await fn({
            head: { cursor: cursorOfRow(locked.cursor_xid, locked.cursor_seq), epoch: current, revision },
            tenants: async (list) => {
              const wantedTenants = [...new Set(list)].filter((t) => !tenants.has(t));
              const out = new Map<string, Cursor | null>();
              if (wantedTenants.length > 0) {
                if (wantedTenants.includes("")) throw new UsageError("eventstore/postgres: a cross-tenant page has no row for the empty tenant");
                // created with the processor row's status (building while it builds), then locked in one order (45 §5.5)
                await c.query(`INSERT INTO ${m} (tenant, name, version, status) SELECT u, $2, $3, $4 FROM unnest($1::text[]) u ON CONFLICT DO NOTHING`, [wantedTenants, name, version, locked.status]);
                const rows = await c.query<{ tenant: string; cursor_xid: string | null; cursor_seq: string | null; revision: string }>(
                  `SELECT tenant, cursor_xid::text AS cursor_xid, cursor_seq::text AS cursor_seq, revision::text AS revision FROM ${m}
WHERE tenant = ANY($1::text[]) AND name = $2 AND version = $3 ORDER BY tenant COLLATE "C" FOR UPDATE`,
                  [wantedTenants, name, version],
                );
                for (const row of rows.rows) {
                  tenants.set(row.tenant, toInt(row.revision));
                  out.set(row.tenant, cursorOfRow(row.cursor_xid, row.cursor_seq));
                }
              }
              return out;
            },
            rows: async (pairs) => {
              const out = new Map<string, StoredRow>();
              if (pairs.length === 0) return out;
              const found = await c.query<StateRow & { tenant: string }>(
                `SELECT tenant, key, state, doc, failed, cursor_xid::text AS cursor_xid, cursor_seq::text AS cursor_seq, revision::text AS revision FROM ${s}
WHERE (tenant, key_hash) IN (SELECT * FROM unnest($1::text[], $2::bytea[])) AND name = $3 AND version = $4`,
                [pairs.map((p) => p.tenant), pairs.map((p) => keyHash(p.key)), name, version],
              );
              for (const row of found.rows) out.set(pairKey(row.tenant, row.key), storedOf(row)); // a hash collision is a miss: its key differs
              return out;
            },
          });
          if (commit === null) return { committed: false };
          const cursor = [commit.cursor.transactionId, String(commit.cursor.sequence)];
          const written = new Set<string>();
          for (const write of commit.writes) {
            if (!tenants.has(write.tenant)) throw new EventStoreError(`eventstore/postgres: a cross-tenant page wrote tenant ${write.tenant} without locking its row`);
            written.add(write.tenant);
          }
          if (commit.writes.length > 0) {
            await c.query(
              `INSERT INTO ${s} AS t (tenant, name, version, key_hash, key, state, doc, failed, cursor_xid, cursor_seq, revision)
SELECT u.tn, $1, $2, u.h, u.k, u.s, u.d, u.f, $3::xid8, $4::bigint, u.r
FROM unnest($5::text[], $6::bytea[], $7::text[], $8::bytea[], $9::jsonb[], $10::jsonb[], $11::bigint[]) AS u(tn, h, k, s, d, f, r)
ON CONFLICT (tenant, name, version, key_hash) DO UPDATE SET
  key = EXCLUDED.key, state = EXCLUDED.state, doc = EXCLUDED.doc, failed = EXCLUDED.failed,
  prev_doc = t.doc, prev_revision = t.revision,
  cursor_xid = EXCLUDED.cursor_xid, cursor_seq = EXCLUDED.cursor_seq, revision = EXCLUDED.revision`,
              [
                name,
                version,
                ...cursor,
                commit.writes.map((w) => w.tenant),
                commit.writes.map((w) => keyHash(w.key)),
                commit.writes.map((w) => w.key),
                commit.writes.map((w) => (w.state ? Buffer.from(w.state.buffer, w.state.byteOffset, w.state.byteLength) : null)),
                commit.writes.map((w) => (w.doc ? JSON.stringify(w.doc) : null)),
                commit.writes.map((w) => (w.failed ? JSON.stringify(w.failed) : null)),
                commit.writes.map((w) => String(tenants.get(w.tenant)! + 1)),
              ],
            );
          }
          if (tenants.size > 0) {
            const list = [...tenants.keys()];
            // greatest of the pair: a tenant checkpoint never goes back (a per-tenant writer of this version may be ahead)
            await c.query(
              `UPDATE ${m} AS t SET
  cursor_xid = CASE WHEN t.cursor_xid IS NULL OR (t.cursor_xid, t.cursor_seq) < ($4::xid8, $5::bigint) THEN $4::xid8 ELSE t.cursor_xid END,
  cursor_seq = CASE WHEN t.cursor_xid IS NULL OR (t.cursor_xid, t.cursor_seq) < ($4::xid8, $5::bigint) THEN $5::bigint ELSE t.cursor_seq END,
  revision = t.revision + u.b, updated_at = now()
FROM unnest($1::text[], $6::int[]) AS u(tn, b) WHERE t.tenant = u.tn AND t.name = $2 AND t.version = $3`,
              [list, name, version, ...cursor, list.map((t) => (written.has(t) ? 1 : 0))],
            );
            for (const tenant of list) keys.push(this.keyOf(tenant, ref));
          }
          await c.query(
            `UPDATE ${m} SET cursor_xid = $3::xid8, cursor_seq = $4::bigint, revision = revision + $5, updated_at = now() WHERE tenant = '' AND name = $1 AND version = $2`,
            [name, version, ...cursor, commit.writes.length > 0 ? 1 : 0],
          );
          // one notification per tenant whose row moved: its readers' followers and waits look again, in every process
          keys.push(processorKey(ref));
          await c.query("SELECT pg_notify($1, k) FROM unnest($2::text[]) k", [this.channel, keys]);
          return { committed: true };
        });
        if (result.committed) for (const key of keys) this.ringLocal(key);
        return result;
      },
      activate: async (ref, epoch) => {
        const keys: string[] = [];
        await this.tx(null, async (c) => {
          // the erasure fence as in a page (decision 50): the tenant list below is read after an erasure commits, or deleted by it
          await c.query(`SELECT pg_advisory_xact_lock_shared(${erasureLockArgs(this.options.target)})`);
          const locked = (await c.query<HeadRow>(lockRow, [ref.name, ref.version])).rows[0];
          if (locked?.status === "retired") throw new RetiredError(ref);
          const current = locked ? toInt(locked.epoch) : 0;
          if (current !== epoch) throw new FencedError(ref, epoch, current);
          if (locked!.status !== "building") return;
          // every tenant that has any version of the name gets an active row of this one, also one without events in its source (45 §7)
          const switched = await c.query<{ tenant: string }>(
            `INSERT INTO ${m} AS t (tenant, name, version, status)
SELECT DISTINCT tenant, $1, $2::int, 'active' FROM ${m} WHERE name = $1 AND tenant <> ''
ON CONFLICT (tenant, name, version) DO UPDATE SET status = 'active', updated_at = now() WHERE t.status = 'building'
RETURNING tenant`,
            [ref.name, ref.version],
          );
          await c.query(`UPDATE ${m} SET status = 'active', updated_at = now() WHERE tenant = '' AND name = $1 AND version = $2`, [ref.name, ref.version]);
          for (const row of switched.rows) keys.push(this.keyOf(row.tenant, ref));
          keys.push(processorKey(ref));
          await c.query("SELECT pg_notify($1, k) FROM unnest($2::text[]) k", [this.channel, keys]);
        });
        for (const key of keys) this.ringLocal(key);
      },
    };
  }

  private ringLocal(key: string): void {
    for (const listener of [...(this.local.get(key) ?? [])]) {
      try {
        listener();
      } catch {
        // a listener never breaks a page
      }
    }
  }

  private async head(r: { query: PoolClient["query"] } | Pool, tenant: string, ref: ModelRef): Promise<ModelHead | null> {
    const found = await r.query<HeadRow & { failed: string }>(
      `SELECT m.epoch::text AS epoch, m.cursor_xid::text AS cursor_xid, m.cursor_seq::text AS cursor_seq, m.revision::text AS revision, m.status, m.fingerprint,
  (SELECT count(*) FROM ${this.s} s WHERE s.tenant = $1 AND s.name = $2 AND s.version = $3 AND s.failed IS NOT NULL)::text AS failed
FROM ${this.m} m WHERE m.tenant = $1 AND m.name = $2 AND m.version = $3`,
      [tenant, ref.name, ref.version],
    );
    const row = found.rows[0];
    if (!row) return null;
    if (row.status === "retired") throw new RetiredError(ref);
    return {
      cursor: cursorOfRow(row.cursor_xid, row.cursor_seq),
      epoch: toInt(row.epoch),
      revision: toInt(row.revision),
      failed: toInt(row.failed),
      status: row.status,
      fingerprint: row.fingerprint,
    };
  }

  /** One statement, one snapshot: the page, the matching total, the failed count and the revision (decision 30 §4). */
  private async list(r: { query: PoolClient["query"] } | Pool, tenant: string, ref: ModelRef, query: ListQuery): Promise<ListPage> {
    const params: unknown[] = [tenant, ref.name, ref.version];
    const base = `tenant = $1 AND name = $2 AND version = $3`;
    let match = `${base} AND failed IS NULL`;
    if (query.where.length > 0) {
      const where: Record<string, unknown> = {};
      for (const [field, value] of query.where) Object.defineProperty(where, field, { value, enumerable: true, writable: true, configurable: true });
      params.push(JSON.stringify(where));
      match += ` AND doc @> $${params.length}::jsonb`;
    }
    let field = "";
    if (query.sort) {
      params.push(query.sort.by);
      field = `$${params.length}::text`;
    }
    const order = orderSql(query.sort, field);
    params.push(query.offset, query.limit);
    const offset = `$${params.length - 1}`;
    const limit = `$${params.length}`;
    const found = await r.query<{ revision: string | null; status: VersionStatus | null; fingerprint: string | null; failed: string; total: string; key: string | null; state: Buffer | null }>(
      `SELECT c.revision::text AS revision, c.status, c.fingerprint, c.failed::text AS failed, c.total::text AS total, p.key, p.state
FROM (SELECT (SELECT revision FROM ${this.m} WHERE ${base}) AS revision,
             (SELECT status FROM ${this.m} WHERE ${base}) AS status,
             (SELECT fingerprint FROM ${this.m} WHERE ${base}) AS fingerprint,
             (SELECT count(*) FROM ${this.s} WHERE ${base} AND failed IS NOT NULL) AS failed,
             (SELECT count(*) FROM ${this.s} WHERE ${match}) AS total) c
LEFT JOIN LATERAL (SELECT key, state, row_number() OVER (ORDER BY ${order}) AS ord FROM ${this.s} WHERE ${match} ORDER BY ${order} OFFSET ${offset} LIMIT ${limit}) p ON true
ORDER BY p.ord`,
      params,
    );
    const first = found.rows[0];
    // a retired version never answers `total: 0` (decision 40 §2)
    if (first?.status === "retired") throw new RetiredError(ref);
    const rows: { key: string; state: Uint8Array }[] = [];
    for (const row of found.rows) if (row.key !== null && row.state !== null) rows.push({ key: row.key, state: row.state });
    return {
      rows,
      total: first ? toInt(first.total) : 0,
      failed: first ? toInt(first.failed) : 0,
      revision: first?.revision ? toInt(first.revision) : 0,
      fingerprint: first?.fingerprint ?? null,
    };
  }

  /** A single read: on the pool, or in a transaction with the tenant set when `rls` needs the session variable. */
  private async run<T>(session: string | null, fn: (runner: { query: PoolClient["query"] } | Pool) => Promise<T>): Promise<T> {
    if (!this.options.rls || session === null) {
      await this.options.ensureInstalled();
      try {
        return await fn(this.options.pool);
      } catch (error) {
        throw this.translate(error, false);
      }
    }
    return this.tx(session, fn);
  }

  /** One transaction (`READ COMMITTED` unless `mode` says otherwise), with the tenant set under `rls`; rolled back when anything throws. */
  private async tx<T>(session: string | null, fn: (client: PoolClient) => Promise<T>, mode?: "REPEATABLE READ READ ONLY"): Promise<T> {
    await this.options.ensureInstalled();
    const inTenantSession = session !== null && this.options.rls;
    let client: PoolClient;
    try {
      client = await this.options.pool.connect();
    } catch (error) {
      throw this.translate(error, inTenantSession);
    }
    try {
      await client.query(mode ? `BEGIN ISOLATION LEVEL ${mode}` : "BEGIN");
      try {
        if (inTenantSession) await client.query("SELECT set_config('app.current_tenant', $1, true)", [session]);
        const result = await fn(client);
        await client.query("COMMIT");
        return result;
      } catch (error) {
        await client.query("ROLLBACK").catch(() => undefined);
        throw error;
      }
    } catch (error) {
      throw this.translate(error, inTenantSession);
    } finally {
      client.release();
    }
  }

  /** Errors of the two tables in words that say what to do; anything not from the database (fenced, a fold) passes as it is. */
  private translate(err: unknown, inTenantSession: boolean): unknown {
    if (err instanceof EventStoreError || !(err && typeof err === "object" && "code" in err)) return err;
    const e = err as { code?: string; message?: string };
    const names = `${this.options.targets.model.schemaName}.${this.options.targets.model.table} and ${this.options.targets.state.table}`;
    if (e.code === "42P01") {
      return new EventStoreError(
        `eventstore/postgres: the read model tables ${names} do not exist — let the store install them (persistedModels: true with install: "auto"), or run printSchemaSql(schema, { persistedModels: true }) as the owner`,
        { cause: err },
      );
    }
    if (e.code === "42501" && !(this.options.rls && inTenantSession)) {
      return new EventStoreError(`eventstore/postgres: permission denied — the application role needs SELECT, INSERT, UPDATE on ${names}: ${e.message ?? ""}`, { cause: err });
    }
    return this.options.translate(err, inTenantSession);
  }
}
