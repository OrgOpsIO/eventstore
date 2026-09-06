import { randomBytes } from "node:crypto";

/**
 * UUIDv7: time-ordered, so `<eventName>Id` values cluster in B-tree indexes and sort by
 * creation. Falls back to `crypto.randomUUID()` style entropy for the random bits.
 */
export function uuidv7(now: number = Date.now()): string {
  const bytes = randomBytes(16);
  const ts = BigInt(now);
  bytes[0] = Number((ts >> 40n) & 0xffn);
  bytes[1] = Number((ts >> 32n) & 0xffn);
  bytes[2] = Number((ts >> 24n) & 0xffn);
  bytes[3] = Number((ts >> 16n) & 0xffn);
  bytes[4] = Number((ts >> 8n) & 0xffn);
  bytes[5] = Number(ts & 0xffn);
  bytes[6] = ((bytes[6] as number) & 0x0f) | 0x70; // version 7
  bytes[8] = ((bytes[8] as number) & 0x3f) | 0x80; // variant
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/** `ArticleDrafted` → `articleDraftedId`. */
export function defaultIdKey(type: string): string {
  return `${type.charAt(0).toLowerCase()}${type.slice(1)}Id`;
}

/**
 * FNV-1a 64-bit of a string, as a signed 64-bit BigInt — the advisory-lock key space of
 * Postgres. Deterministic across processes and languages.
 */
export function fnv1a64(input: string): bigint {
  let hash = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;
  const bytes = Buffer.from(input, "utf8");
  for (const b of bytes) {
    hash ^= BigInt(b);
    hash = (hash * prime) & 0xffffffffffffffffn;
  }
  return BigInt.asIntN(64, hash);
}
