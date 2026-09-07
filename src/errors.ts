import type { Conflict } from "./types.js";

/** One validation problem: message, JSON path, optional code (Zod's or the SDK's). */
export interface ValidationIssue {
  readonly message: string;
  readonly path?: readonly (string | number)[];
  readonly code?: string;
}

/** Base class of every error the store raises; `httpStatus` suggests a status for delivery mechanisms. */
export class EventStoreError extends Error {
  override readonly name: string = "EventStoreError";
  /** Suggested HTTP status for delivery mechanisms. */
  readonly httpStatus: number = 500;
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
  }
}

/** `appendIf` found a changed context. Thrown by `es.appendIfOrThrow`; `appendIf` returns it as a value. */
export class ConflictError extends EventStoreError {
  override readonly name = "ConflictError";
  override readonly httpStatus = 409;
  constructor(
    readonly conflict: Conflict,
    options?: { cause?: unknown },
  ) {
    super(`eventstore: context changed — expected context version ${conflict.expected}, actual ${conflict.actual}`, options);
  }
}

/** A declared `unique` path or an `idempotencyKey` was violated. The message never echoes the value. */
export class UniqueViolationError extends EventStoreError {
  override readonly name = "UniqueViolationError";
  override readonly httpStatus = 409;
  constructor(
    readonly detail: { type?: string; path?: string; idempotencyKey?: boolean },
    options?: { cause?: unknown },
  ) {
    super(
      detail.idempotencyKey
        ? "eventstore: idempotency key already used"
        : `eventstore: unique violation on ${detail.type ?? "?"}.${detail.path ?? "?"}`,
      options,
    );
  }
}

/** Payload failed the registry's Zod schema or the envelope rules. */
export class ValidationError extends EventStoreError {
  override readonly name = "ValidationError";
  override readonly httpStatus = 400;
  constructor(
    readonly type: string,
    readonly issues: readonly ValidationIssue[],
    options?: { cause?: unknown },
  ) {
    super(`eventstore: ${type} is invalid: ${issues.map((i) => (i.path?.length ? `${i.path.join(".")}: ` : "") + i.message).join("; ")}`, options);
  }
}

/** Strict mode: a condition query cannot be locked through declared scope keys. */
export class UnindexableContextError extends EventStoreError {
  override readonly name = "UnindexableContextError";
  override readonly httpStatus = 400;
  constructor(reason: string, options?: { cause?: unknown }) {
    super(
      `eventstore (strict): the context query is not lockable/indexable — ${reason}. ` +
        `Add a declared scope key to every filter, declare the key in configure({ scopeKeys }), ` +
        `or set strict: false to accept a global lock.`,
      options,
    );
  }
}

/** The store could not complete the operation right now (deadlock victim, serialization failure, lock timeout). Safe to retry. */
export class TransientError extends EventStoreError {
  override readonly name = "TransientError";
  override readonly httpStatus = 503;
  constructor(
    readonly code: string,
    options?: { cause?: unknown },
  ) {
    super(`eventstore: transient failure (${code}), retry`, options);
  }
}

/** Row-level security refused the statement (`rls: true`): the row belongs to another tenant. */
export class PolicyViolationError extends EventStoreError {
  override readonly name = "PolicyViolationError";
  override readonly httpStatus = 403;
  constructor(message: string, options?: { cause?: unknown }) {
    super(`eventstore: ${message}`, options);
  }
}

/** A query or event names a tenant other than the one the view is bound to. Fail-closed. */
export class TenantMismatchError extends EventStoreError {
  override readonly name = "TenantMismatchError";
  override readonly httpStatus = 403;
}

/** A context exceeds `contextCache.maxEvents`; narrow the query, snapshot, or raise the limit. */
export class ContextTooLargeError extends EventStoreError {
  override readonly name = "ContextTooLargeError";
  constructor(
    readonly key: string,
    readonly maxEvents: number,
  ) {
    super(`eventstore: context ${key.slice(0, 200)} has more than ${maxEvents} events; narrow the query or raise contextCache.maxEvents`);
  }
}

/** Base for programming errors surfaced at request time: invalid query shapes, undeclared types. */
export class UsageError extends EventStoreError {
  override readonly name = "UsageError";
  override readonly httpStatus = 400;
}

/** The ambient `es` was used before `configure()`. */
export class NotConfiguredError extends EventStoreError {
  override readonly name = "NotConfiguredError";
  constructor() {
    super(
      "eventstore: not configured. Call configure({ connection, events }) once at startup " +
        "(e.g. a Nuxt server plugin) before using the ambient `es`, or use createEventStore() for an explicit instance.",
    );
  }
}
