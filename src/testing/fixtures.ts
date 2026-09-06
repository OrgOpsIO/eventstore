import { defaultIdKey, uuidv7, type Metadata, type RecordedEvent, type Scopes } from "../index.js";

let counter = 0;

export interface MakeEventExtra {
  readonly id?: string;
  readonly scopes?: Scopes;
  readonly metadata?: Metadata;
  readonly sequence?: number;
  readonly recordedAt?: Date;
  readonly transactionId?: string;
  readonly settled?: boolean;
}

/**
 * A typed `RecordedEvent` fixture for fold/decide tests — no store needed, no casts.
 * Sequences auto-increment per process unless given; `resetSequence()` restarts them.
 */
export function makeEvent<T extends string, D extends Record<string, unknown>>(
  type: T,
  data: D,
  extra: MakeEventExtra = {},
): RecordedEvent<T, D> {
  const sequence = extra.sequence ?? ++counter;
  if (extra.sequence !== undefined && extra.sequence > counter) counter = extra.sequence;
  return {
    type,
    data,
    id: extra.id ?? uuidv7(),
    scopes: { ...(extra.scopes ?? {}) },
    metadata: { ...(extra.metadata ?? {}) },
    sequence,
    recordedAt: extra.recordedAt ?? new Date("2026-01-01T00:00:00Z"),
    transactionId: extra.transactionId ?? String(sequence),
    settled: extra.settled ?? true,
  };
}

/** Restart the fixture sequence counter (e.g. in `beforeEach`). */
export function resetSequence(): void {
  counter = 0;
}

/** The own-id key of a fixture event type (`ArticleDrafted` → `articleDraftedId`). */
export const idKeyOf = defaultIdKey;
