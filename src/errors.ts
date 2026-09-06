import type { Conflict } from "./types.js";

export class EventStoreError extends Error {
  override readonly name: string = "EventStoreError";
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
  }
}

/** `appendIf` found a changed context. Thrown by `es.appendIfOrThrow`; `appendIf` returns it as a value. */
export class ConflictError extends EventStoreError {
  override readonly name = "ConflictError";
  readonly httpStatus = 409;
  constructor(readonly conflict: Conflict) {
    super(
      `eventstore: context changed — expected context version ${conflict.expected}, actual ${conflict.actual}`,
    );
  }
}

/** A declared `unique` path or an `idempotencyKey` was violated. */
export class UniqueViolationError extends EventStoreError {
  override readonly name = "UniqueViolationError";
  readonly httpStatus = 409;
  constructor(
    readonly detail: { type?: string; path?: string; idempotencyKey?: string },
    options?: { cause?: unknown },
  ) {
    super(
      detail.idempotencyKey !== undefined
        ? `eventstore: idempotency key already used: ${detail.idempotencyKey}`
        : `eventstore: unique violation on ${detail.type ?? "?"}.${detail.path ?? "?"}`,
      options,
    );
  }
}

/** Payload failed the registry's Zod schema. */
export class ValidationError extends EventStoreError {
  override readonly name = "ValidationError";
  readonly httpStatus = 400;
  constructor(
    readonly type: string,
    readonly issues: unknown,
  ) {
    super(`eventstore: payload of ${type} is invalid: ${JSON.stringify(issues)}`);
  }
}

/** Strict mode: a condition query cannot be locked through declared scope keys. */
export class UnindexableContextError extends EventStoreError {
  override readonly name = "UnindexableContextError";
  constructor(reason: string) {
    super(
      `eventstore (strict): the context query is not lockable/indexable — ${reason}. ` +
        `Add a declared scope key to every filter, declare the key in configure({ scopeKeys }), ` +
        `or set strict: false to accept a global lock.`,
    );
  }
}

export class NotConfiguredError extends EventStoreError {
  override readonly name = "NotConfiguredError";
  constructor() {
    super(
      "eventstore: not configured. Call configure({ connection, events }) once at startup " +
        "(e.g. a Nuxt server plugin) before using the ambient `es`, or use createEventStore() for an explicit instance.",
    );
  }
}
