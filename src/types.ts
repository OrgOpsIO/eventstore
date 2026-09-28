/**
 * Core contract of @orgops/eventstore.
 *
 * The vocabulary and every concept here are the joint work of Ralf Westphal and Rico
 * Fritzsche (event-orientation, Command Context Consistency, scoping events, the store
 * contract with its two sequence numbers). An event carries its own `<eventName>Id` and a
 * `scopes` object with back-links to the events it happened in relation to. The store is a
 * single append-only `events` table; everything else the SDK creates is an index or a
 * function on that table. This package is one implementation of their model, nothing more.
 */

/** Back-links to other events: `{ articleDraftedId: "…", workspaceProvisionedId: "…" }`. */
export type Scopes = Readonly<Record<string, string>>;

/** Envelope metadata — never part of the domain payload. */
export interface Metadata {
  actor?: string;
  correlationId?: string;
  causationId?: string;
  /** Unique per intent; a second append with the same key is rejected with `UniqueViolationError`. */
  idempotencyKey?: string;
  [key: string]: unknown;
}

/** An event that has not been recorded yet. */
export interface NewEvent<T extends string = string, D = Record<string, unknown>, Sc extends Scopes = Scopes> {
  readonly type: T;
  readonly data: D;
  /** Scope back-links. Keys declared in the registry are indexed and locked. */
  readonly scopes?: Sc;
  /** The event's own id (`<eventName>Id`). Registry creators generate a UUIDv7; stores generate one when still missing. */
  readonly id?: string;
  readonly metadata?: Metadata;
}

/** An event as returned by the store. */
export interface RecordedEvent<T extends string = string, D = Record<string, unknown>, Sc extends Scopes = Scopes> {
  readonly type: T;
  readonly data: D;
  /** The event's own id. A legacy row without one reads back as `~<sequence>`. */
  readonly id: string;
  readonly scopes: Sc;
  readonly metadata: Metadata;
  /** Global committed position. Safe integer. Defines order within a context. */
  readonly sequence: number;
  readonly recordedAt: Date;
  /**
   * The transaction that recorded the event (Postgres `xid8` as a decimal string; one per
   * append batch; the memory store uses the batch's first sequence). Together with
   * `sequence` it forms the gap-free cursor order `(transactionId, sequence)`.
   */
  readonly transactionId: string;
  /**
   * `true` when no transaction that started before this one is still in flight — i.e. no
   * event with a lower `(transactionId, sequence)` can still appear. Only settled events
   * may advance a durable cursor. A just-committed event is usually NOT yet settled from the
   * committing connection's point of view; it becomes settled once every older transaction ended.
   */
  readonly settled: boolean;
}

/**
 * One filter. A record matches when ALL present constraints match:
 * - `types`: the event type is one of these (OR). An empty list matches nothing.
 * - `scopes`: for every key, the event's value for that scope key is one of the given values.
 *   An event's value for key K is `scopes[K]`, or its own id when K is its id key, or the
 *   top-level string field K of its data (flat-id compatibility).
 * - `where`: the wire payload contains at least one of these objects (JSONB `@>`, OR).
 *   Predicates are sent as JSON: `undefined` keys vanish, `Date`s become ISO strings,
 *   `null` matches only a JSON `null`.
 * - `not`: records matching this filter (or ANY of these filters) are left out — of the rows
 *   and of the context version, so `appendIf` re-checks exactly what was read. A record that
 *   lacks a negated scope key is kept. Locks come from the positive part only: a negation
 *   never widens or narrows them, and a negated scope key need not be declared.
 */
export interface Filter {
  readonly types?: readonly string[];
  readonly scopes?: Readonly<Record<string, string | readonly string[]>>;
  readonly where?: readonly Readonly<Record<string, unknown>>[];
  readonly not?: NegatedFilter | readonly NegatedFilter[];
}

/** The part of a filter a `not` may name: one level, no nested `not`. It must constrain something. */
export type NegatedFilter = Omit<Filter, "not">;

/** One filter, or several filters combined with OR. */
export type Query = Filter | readonly Filter[];

/** A gap-free read cursor over settled events, ordered by `(transactionId, sequence)`. */
export interface Cursor {
  readonly transactionId: string;
  readonly sequence: number;
}

/** Read options. `after` and `cursor` narrow the records, never the context version. */
export interface QueryOptions {
  /**
   * Exclusive sequence cursor: return only records with `sequence > after`. A convenience for
   * one-off delta reads; it can miss an event that commits late with a lower sequence, so
   * durable consumers use `cursor` instead. Never narrows the context version.
   */
  readonly after?: number;
  /**
   * Inclusive upper bound on `sequence`: only records with `sequence <= until`. Unlike `after`
   * and `cursor` it narrows the context version too — it reads the store as it was at event
   * `until`, and that version is for reading only (see `EventStoreApi.asOf`). Records with a
   * lower sequence can still commit later until every transaction below `until` has ended;
   * combine with `settledOnly` for a stable answer.
   */
  readonly until?: number;
  /** Return only settled records (see `RecordedEvent.settled`). Orders by `(transactionId, sequence)`. */
  readonly settledOnly?: boolean;
  /**
   * Exclude settled records at or below this `(transactionId, sequence)` cursor. Unsettled
   * records are always returned so a decision never misses a visible fact; the caller folds
   * them without persisting them. Orders by `(transactionId, sequence)`.
   */
  readonly cursor?: Cursor | null;
  /** Safe non-negative integer. */
  readonly limit?: number;
  /** Ascending (default) or descending. */
  readonly order?: "asc" | "desc";
  /**
   * Top-level `data` keys to leave out of every returned record — a projection for reads that
   * do not need a large field (an article body, a document). The Postgres store strips them in
   * SQL, before the rows travel. Identity is never trimmed: `scopes` is rejected, and so is an
   * event's own id key. `where` and `contextVersion` still see the full record. For reads only:
   * a decision (`command()`, `context()`) needs complete facts and has no `omit`.
   */
  readonly omit?: readonly string[];
}

/**
 * Ties a query to the context version observed when it was read. Pass it to `appendIf`.
 * It is an unauthenticated capability: `appendIf` trusts both fields, so a handle must never
 * come from a client. To expose optimistic concurrency to a browser, send only the version
 * number and re-pair it with a server-built query.
 */
export interface ContextHandle {
  readonly query: Query;
  readonly version: number;
}

/** What a read returns: the records, their per-filter grouping, and the three numbers that must not be confused (see fields). */
export interface QueryResult<E extends RecordedEvent<string, unknown> = RecordedEvent> {
  /**
   * Matching records. In sequence order, unless `cursor` or `settledOnly` is set — then in
   * `(transactionId, sequence)` order, which is the only gap-free order across transactions.
   * Within one lockable context both orders agree.
   */
  readonly events: readonly E[];
  /** The same records grouped per filter, in the order of `events` (a record matching several filters appears in each). */
  readonly byFilter: readonly (readonly E[])[];
  /** Highest sequence among the RETURNED records; 0 when none. A read cursor, not the context. */
  readonly lastReturned: number;
  /**
   * Highest sequence among ALL records matching the query, ignoring `after`, `cursor`, `limit`.
   * This — and only this — is what `appendIf` guards. 0 when nothing matches.
   */
  readonly contextVersion: number;
  /** The query paired with `contextVersion`. Pass straight to `appendIf`. */
  readonly ctx: ContextHandle;
  /** Highest settled `(transactionId, sequence)` among returned records; use it as the next `cursor`. */
  readonly settledCursor: Cursor | null;
}

/** The consecutive sequence range one append batch received. */
export interface AppendResult {
  readonly first: number;
  readonly last: number;
  readonly count: number;
}

/** The version `appendIf` expected versus the one it found. */
export interface Conflict {
  readonly expected: number;
  readonly actual: number;
}

/** Result of `appendIf`: committed (`appended`) or refused (`conflict`). Naming rule: `*Outcome` is a discriminated union on `ok`, `*Result` plain data. */
export type AppendIfOutcome =
  | { readonly ok: true; readonly appended: AppendResult }
  | { readonly ok: false; readonly conflict: Conflict };

/** Count and stored size of one event type (see `StatisticsStore`). */
export interface TypeStatistics {
  readonly type: string;
  readonly count: number;
  /**
   * Bytes of the stored payloads. Postgres: the on-disk size (`pg_column_size`, compressed when
   * TOASTed) — no payload is fetched. Memory store: the JSON text length. The two differ.
   */
  readonly bytes: number;
  /** Highest sequence among the counted records. */
  readonly lastSequence: number;
}

/** Optional store capability: per-type count and size without fetching a payload. Narrowed by a query when given. */
export interface StatisticsStore {
  statistics(query?: Query): Promise<readonly TypeStatistics[]>;
}

/** The store contract. Both the memory store and the Postgres store implement exactly this. */
export interface EventStore {
  query(query: Query, options?: QueryOptions): Promise<QueryResult>;
  /** Unconditional append. One batch, one consecutive sequence range, all or nothing. */
  append(events: readonly NewEvent[]): Promise<AppendResult>;
  /**
   * Conditional append (CCC `append_if`): commits only if the context version of `ctx.query`
   * still equals `ctx.version`. The check and the commit are one atomic step.
   */
  appendIf(events: readonly NewEvent[], ctx: ContextHandle): Promise<AppendIfOutcome>;
  close(): Promise<void>;
}

/** What a store needs to know about declared events (derived from registries + config). */
export interface StoreSchema {
  /** Scope keys that are indexed and locked. */
  readonly scopeKeys: readonly string[];
  /** The scope key that identifies the tenant, if any. Locked shared by appends, exclusive only by tenant-wide guards. */
  readonly tenantScopeKey?: string;
  /**
   * Per-deployment secret mixed into every advisory-lock key. Without it the keys are plain
   * FNV-1a hashes of public strings, so any database role can compute and hold them.
   */
  readonly lockSalt?: string;
  /** Per event type: unique payload paths (e.g. `"email"`, `"scopes.magicLinkRequestedId"`). */
  readonly uniques: readonly { readonly type: string; readonly path: string }[];
  /** The id key of an event type (`<lowerFirst(type)>Id` unless declared otherwise). */
  idKeyOf(type: string): string;
  /** Upcast a stored wire payload of the given type to the current shape (identity by default). */
  upcast(type: string, payload: Record<string, unknown>): Record<string, unknown>;
  /**
   * Validate and normalise a new event: registry schema for declared types, and for every type
   * the envelope rules (no `scopes`/id-key collision in `data`, string scope values).
   * Throws `ValidationError`.
   */
  validate(event: NewEvent): NewEvent;
  /**
   * `strict`: a condition query must be lockable through declared scope keys. Otherwise the
   * store falls back to an exclusive global lock (correct, but serialises every append).
   */
  readonly strict: boolean;
}
