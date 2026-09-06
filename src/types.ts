/**
 * Core contract of @orgops/eventstore.
 *
 * Vocabulary follows Rico Fritzsche's Command Context Consistency (CCC) spec, Draft 0.1,
 * and Ralf Westphal's "Scoping Events": an event carries its own `<eventName>Id` and a
 * `scopes` object with back-links to the events it happened in relation to. The store is a
 * single append-only `events` table; everything else the SDK creates is an index or a
 * function on that table.
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
export interface NewEvent<T extends string = string, D = Record<string, unknown>> {
  readonly type: T;
  readonly data: D;
  /** Scope back-links. Keys declared in the registry are indexed and locked. */
  readonly scopes?: Scopes;
  /** The event's own id (`<eventName>Id`). Generated (UUIDv7) when omitted. */
  readonly id?: string;
  readonly metadata?: Metadata;
}

/** An event as returned by the store. */
export interface RecordedEvent<T extends string = string, D = Record<string, unknown>> {
  readonly type: T;
  readonly data: D;
  readonly id: string;
  readonly scopes: Scopes;
  readonly metadata: Metadata;
  /** Global committed position. Defines order. Safe integer. */
  readonly sequence: number;
  readonly recordedAt: Date;
  /**
   * The transaction that recorded the event (Postgres `xid8` as a decimal string; the
   * memory store uses the sequence). Together with `sequence` it forms a gap-free cursor.
   */
  readonly transactionId: string;
  /**
   * `true` when no transaction that started before this one is still in flight — i.e. no
   * event with a lower `(transactionId, sequence)` can still appear. Only settled events
   * may advance a durable cursor.
   */
  readonly settled: boolean;
}

/**
 * One filter. A record matches when ALL present constraints match:
 * - `types`: the event type is one of these (OR)
 * - `scopes`: for every key, the event's value for that scope key is one of the given values.
 *   An event's value for key K is `scopes[K]`, or its own id when K is its id key, or the
 *   top-level data field K (flat-id compatibility).
 * - `where`: the wire payload contains at least one of these objects (JSONB `@>`, OR)
 */
export interface Filter {
  readonly types?: readonly string[];
  readonly scopes?: Readonly<Record<string, string | readonly string[]>>;
  readonly where?: readonly Readonly<Record<string, unknown>>[];
}

/** One filter, or several filters combined with OR. */
export type Query = Filter | readonly Filter[];

/** A gap-free read cursor over settled events, ordered by `(transactionId, sequence)`. */
export interface Cursor {
  readonly transactionId: string;
  readonly sequence: number;
}

export interface QueryOptions {
  /** Exclusive sequence cursor: return only records with `sequence > after`. Does NOT narrow the context version. */
  readonly after?: number;
  /** Return only settled records. */
  readonly settledOnly?: boolean;
  /**
   * Exclude settled records at or below this cursor. Unsettled records are always returned so a
   * decision never misses a visible fact; the caller folds them without persisting them.
   */
  readonly cursor?: Cursor | null;
  readonly limit?: number;
  /** Ascending (default) or descending sequence. */
  readonly order?: "asc" | "desc";
}

export interface QueryResult<E extends RecordedEvent = RecordedEvent> {
  /** Matching records, ascending by sequence (unless `order: "desc"`). */
  readonly events: readonly E[];
  /** The same records grouped per filter (a record matching several filters appears in each). */
  readonly byFilter: readonly (readonly E[])[];
  /** Highest sequence among the RETURNED records; 0 when none. A read cursor, not the context. */
  readonly lastReturned: number;
  /**
   * Highest sequence among ALL records matching the query, ignoring `after`, `cursor`, `limit`.
   * This — and only this — is what `appendIf` guards. 0 when nothing matches.
   */
  readonly contextVersion: number;
  /** Highest settled `(transactionId, sequence)` among returned records; use it as the next `cursor`. */
  readonly settledCursor: Cursor | null;
}

/** Ties a query to the context version observed when it was read. Pass it to `appendIf`. */
export interface ContextHandle {
  readonly query: Query;
  readonly version: number;
}

export interface AppendResult {
  readonly first: number;
  readonly last: number;
  readonly count: number;
}

export interface Conflict {
  readonly expected: number;
  readonly actual: number;
}

export type AppendIfOutcome =
  | { readonly ok: true; readonly appended: AppendResult }
  | { readonly ok: false; readonly conflict: Conflict };

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
  /** Per event type: unique payload paths (e.g. `"email"`, `"scopes.magicLinkRequestedId"`). */
  readonly uniques: readonly { readonly type: string; readonly path: string }[];
  /** The id key of an event type (`<lowerFirst(type)>Id` unless declared otherwise). */
  idKeyOf(type: string): string;
  /** Upcast a stored wire payload of the given type to the current shape (identity by default). */
  upcast(type: string, payload: Record<string, unknown>): Record<string, unknown>;
  /** Validate (and normalise) event data against the registry; unknown types pass through. Throws `ValidationError`. */
  validate(event: NewEvent): NewEvent;
  /**
   * `strict`: a condition query must be lockable through declared scope keys. Otherwise the
   * store falls back to an exclusive global lock (correct, but serialises every append).
   */
  readonly strict: boolean;
}
