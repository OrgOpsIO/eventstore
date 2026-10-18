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
  /** Listeners of further channels on the same connection, by channel. */
  private readonly channels = new Map<string, Set<(payload: string | null) => void>>();
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
      if (!this.wanted) this.disconnect();
    };
  }

  /**
   * Listen on a second channel over the same connection (the persisted read models' channel):
   * the listener gets each notification's payload, and `null` whenever LISTEN is (re)established.
   */
  addChannel(channel: string, listener: (payload: string | null) => void): () => void {
    if (this.closed) throw new Error("eventstore/postgres: the store is closed");
    let set = this.channels.get(channel);
    const fresh = !set;
    if (!set) this.channels.set(channel, (set = new Set()));
    set.add(listener);
    const opened = this.opened;
    if (opened && fresh) {
      const generation = this.generation;
      opened.connection.query(`LISTEN ${quoteIdent(channel)}`).then(
        () => generation === this.generation && listener(null), // whatever committed before LISTEN took effect
        () => this.lost(generation),
      );
    } else if (!opened && !this.opening && !this.timer) {
      void this.connect();
    }
    return () => {
      set.delete(listener);
      if (set.size === 0 && this.channels.get(channel) === set) this.channels.delete(channel);
      if (!this.wanted) this.disconnect();
    };
  }

  /** Someone listens: on the doorbell or on another channel. */
  private get wanted(): boolean {
    return this.listeners.size > 0 || this.channels.size > 0;
  }

  /** Whether a LISTEN connection is currently established (tests, health checks). */
  get connected(): boolean {
    return this.opened !== undefined;
  }

  async close(): Promise<void> {
    this.closed = true;
    this.listeners.clear();
    this.channels.clear();
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

  private ringChannel(channel: string, payload: string | null): void {
    for (const listener of [...(this.channels.get(channel) ?? [])]) {
      try {
        listener(payload);
      } catch {
        // a listener must never break the doorbell
      }
    }
  }

  private async connect(): Promise<void> {
    if (this.closed || !this.wanted) return;
    this.opening = true;
    const generation = ++this.generation;
    let opened: OpenedConnection | undefined;
    let again = false;
    try {
      opened = await this.open();
      const { connection } = opened;
      connection.on("notification", (message) => {
        if (generation !== this.generation) return;
        if (message.channel !== this.channel) {
          if (this.channels.has(message.channel)) this.ringChannel(message.channel, message.payload ?? "");
          return;
        }
        // the SDK's trigger sends an empty payload; a digit string (an older trigger) is only a hint
        const hint = message.payload ? Number(message.payload) : Number.NaN;
        this.ring(Number.isSafeInteger(hint) ? hint : null);
      });
      connection.on("error", () => this.lost(generation));
      connection.on("end", () => this.lost(generation));
      await connection.query(`LISTEN ${quoteIdent(this.channel)}`);
      const channels = [...this.channels.keys()];
      for (const channel of channels) await connection.query(`LISTEN ${quoteIdent(channel)}`);
      if (this.closed || !this.wanted || generation !== this.generation) {
        // dropped while connecting; a listener that arrived meanwhile still needs a connection
        opened.release();
        again = !this.closed && this.wanted;
        return;
      }
      this.opened = opened;
      this.backoffMs = 0;
      // a channel added while LISTEN was being set up
      for (const channel of this.channels.keys()) if (!channels.includes(channel)) await connection.query(`LISTEN ${quoteIdent(channel)}`);
      this.ring(null); // whatever committed before LISTEN took effect: read now
      for (const channel of this.channels.keys()) this.ringChannel(channel, null);
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
    if (this.closed || !this.wanted || this.timer) return;
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
