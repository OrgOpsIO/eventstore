/**
 * The listening side of the commit doorbell: ONE connection per store (per process), outside
 * the pool, that LISTENs on the store's channel and fans every notification out to in-process
 * listeners. It carries no data: a listener answers it with a cursor read through its own view.
 *
 * Losing it costs latency, never events: the connection reconnects with back-off, and every
 * time LISTEN is (re)established it rings once with `null` ("unknown — read now"), which also
 * closes the window between a subscriber's first read and the LISTEN taking effect.
 */
import { quoteIdent } from "./sql.js";

/** What the listener needs of a connection: `pg.Client` and a checked-out `PoolClient` both fit. */
export interface ListenConnection {
  query(sql: string): Promise<unknown>;
  on(event: "notification", listener: (message: { channel: string; payload?: string }) => void): unknown;
  on(event: "error" | "end", listener: (error?: Error) => void): unknown;
}

export interface OpenedConnection {
  readonly connection: ListenConnection;
  /** Give the connection back for good (it holds LISTEN state: never back into a pool). */
  release(): void;
}

const MIN_BACKOFF_MS = 100;
const MAX_BACKOFF_MS = 5_000;

/** How the listener checks that something will ring at all. */
export interface CommitListenerOptions {
  /** Resolves `false` when the trigger is missing (`install: "none"` without the live DDL). */
  readonly verify?: () => Promise<boolean>;
  /** Told once when `verify` failed; the listener then rings on a timer, like polling. */
  readonly onMissing?: () => void;
  /** Ring interval without a trigger. Default 500 ms — the polling a subscription would do without a doorbell. */
  readonly fallbackMs?: number;
}

export class CommitListener {
  private readonly listeners = new Set<(hint: number | null) => void>();
  private opened: OpenedConnection | undefined;
  private opening = false;
  private closed = false;
  private backoffMs = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  /** Bumped per connection, so events of a dropped connection cannot act on its successor. */
  private generation = 0;
  private fallback: ReturnType<typeof setInterval> | undefined;
  private warned = false;

  constructor(
    private readonly open: () => Promise<OpenedConnection>,
    private readonly channel: string,
    private readonly options: CommitListenerOptions = {},
  ) {}

  add(listener: (hint: number | null) => void): () => void {
    if (this.closed) throw new Error("eventstore/postgres: the store is closed");
    this.listeners.add(listener);
    if (!this.opened && !this.opening && !this.timer) void this.connect();
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0) this.disconnect();
    };
  }

  /** Whether a LISTEN connection is currently established (tests, health checks). */
  get connected(): boolean {
    return this.opened !== undefined;
  }

  async close(): Promise<void> {
    this.closed = true;
    this.listeners.clear();
    this.disconnect();
  }

  private ring(hint: number | null): void {
    for (const listener of this.listeners) {
      try {
        listener(hint);
      } catch {
        // a listener must never break the doorbell
      }
    }
  }

  private async connect(): Promise<void> {
    if (this.closed || this.listeners.size === 0) return;
    this.opening = true;
    const generation = ++this.generation;
    let opened: OpenedConnection | undefined;
    let again = false;
    try {
      opened = await this.open();
      const { connection } = opened;
      connection.on("notification", (message) => {
        if (generation !== this.generation || message.channel !== this.channel) return;
        // the SDK's trigger sends an empty payload; a digit string (an older trigger) is only a hint
        const hint = message.payload ? Number(message.payload) : Number.NaN;
        this.ring(Number.isSafeInteger(hint) ? hint : null);
      });
      connection.on("error", () => this.lost(generation));
      connection.on("end", () => this.lost(generation));
      await connection.query(`LISTEN ${quoteIdent(this.channel)}`);
      if (this.closed || this.listeners.size === 0 || generation !== this.generation) {
        // dropped while connecting; a listener that arrived meanwhile still needs a connection
        opened.release();
        again = !this.closed && this.listeners.size > 0;
        return;
      }
      this.opened = opened;
      this.backoffMs = 0;
      this.ring(null); // whatever committed before LISTEN took effect: read now
      await this.checkTrigger(generation);
    } catch {
      opened?.release();
      this.retry();
    } finally {
      this.opening = false;
      if (again) void this.connect();
    }
  }

  /** Without the trigger nothing ever rings: fall back to ringing on a timer, so subscribers keep polling's pace. */
  private async checkTrigger(generation: number): Promise<void> {
    if (!this.options.verify || this.fallback) return;
    let present = true;
    try {
      present = await this.options.verify();
    } catch {
      return; // unknown: the next connect checks again
    }
    if (present || generation !== this.generation) return;
    if (!this.warned) {
      this.warned = true;
      this.options.onMissing?.();
    }
    this.fallback = setInterval(() => this.ring(null), this.options.fallbackMs ?? 500);
    this.fallback.unref?.();
  }

  private lost(generation: number): void {
    if (generation !== this.generation) return;
    const opened = this.opened;
    this.opened = undefined;
    this.generation++;
    opened?.release();
    this.retry();
  }

  private retry(): void {
    if (this.closed || this.listeners.size === 0 || this.timer) return;
    this.backoffMs = this.backoffMs === 0 ? MIN_BACKOFF_MS : Math.min(this.backoffMs * 2, MAX_BACKOFF_MS);
    this.timer = setTimeout(() => {
      this.timer = undefined;
      if (!this.opening) void this.connect();
    }, this.backoffMs);
  }

  private disconnect(): void {
    if (this.fallback) {
      clearInterval(this.fallback);
      this.fallback = undefined;
    }
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    this.generation++;
    const opened = this.opened;
    this.opened = undefined;
    opened?.release();
  }
}
