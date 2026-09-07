import { mkdir, open, readFile, rename } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { dirname } from "node:path";
import type { Cursor } from "../types.js";

/**
 * Where a subscription remembers how far it got. Pluggable so the `events` table stays the
 * only table: the default lives in memory, `fileCursors` writes a JSON file, and a store-backed
 * implementation can be supplied by the application.
 */
export interface CursorStore {
  load(name: string): Promise<Cursor | null>;
  save(name: string, cursor: Cursor): Promise<void>;
  /** Forget a cursor so the next `subscribe` replays from `from`. */
  delete?(name: string): Promise<void>;
}

/** Default: cursors live only in this process. A restart replays from `from`. */
export function memoryCursors(): CursorStore {
  const cursors = new Map<string, Cursor>();
  return {
    async load(name) {
      return cursors.get(name) ?? null;
    },
    async save(name, cursor) {
      cursors.set(name, cursor);
    },
    async delete(name) {
      cursors.delete(name);
    },
  };
}

/**
 * Cursors in one JSON file (`{ [name]: { transactionId, sequence } }`). Written atomically
 * (temp file + rename). Good enough for single-instance deployments that must survive restarts
 * without a second database table.
 */
export function fileCursors(path: string): CursorStore {
  let chain: Promise<void> = Promise.resolve();
  const serialize = <T>(fn: () => Promise<T>): Promise<T> => {
    const next = chain.then(fn);
    chain = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  };
  const read = async (): Promise<Record<string, Cursor>> => {
    try {
      const text = await readFile(path, "utf8");
      const parsed = JSON.parse(text) as Record<string, Cursor>;
      // a prototype-free object: a subscription named "__proto__" must not re-parent the map
      return parsed && typeof parsed === "object" ? Object.assign(Object.create(null) as Record<string, Cursor>, parsed) : (Object.create(null) as Record<string, Cursor>);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") return Object.create(null) as Record<string, Cursor>;
      throw err;
    }
  };
  const write = async (all: Record<string, Cursor>): Promise<void> => {
    await mkdir(dirname(path), { recursive: true });
    // exclusive create with a random name (no symlink following, no predictable path), owner-only,
    // fsync'd before the atomic rename; the file is meant for ONE process — two instances sharing
    // it would clobber each other's cursors
    const tmp = `${path}.${randomBytes(6).toString("hex")}.tmp`;
    const handle = await open(tmp, "wx", 0o600);
    try {
      await handle.writeFile(JSON.stringify(all, null, 2), "utf8");
      await handle.sync();
    } finally {
      await handle.close();
    }
    await rename(tmp, path);
  };
  return {
    load(name) {
      return serialize(async () => {
        const all = await read();
        const cursor = all[name];
        return cursor && typeof cursor.sequence === "number" && typeof cursor.transactionId === "string" ? cursor : null;
      });
    },
    save(name, cursor) {
      return serialize(async () => {
        const all = await read();
        all[name] = { transactionId: cursor.transactionId, sequence: cursor.sequence };
        await write(all);
      });
    },
    delete(name) {
      return serialize(async () => {
        const all = await read();
        if (!(name in all)) return;
        delete all[name];
        await write(all);
      });
    },
  };
}
