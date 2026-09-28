/**
 * Adopting an existing events table: the installer takes a table in an older shape over in
 * place — columns renamed, missing ones added — instead of the store mapping foreign column
 * names forever. Every step is metadata-only in Postgres (no row is rewritten, `payload` is never
 * touched), and it runs inside the install transaction under the install lock: all or nothing,
 * one process at a time, idempotent on every later boot.
 *
 * `planAdoption` is pure: it turns what the catalogue says about the table into a plan. The
 * installer only executes a plan without problems.
 */
import { quoteIdent, quoteLiteral } from "./sql.js";

/** The columns the store works with, by their name in the adopted table. */
export interface AdoptColumns {
  /** The gap-free global position (BIGSERIAL / bigint identity). Becomes `sequence_number`. */
  readonly sequence?: string;
  /** The event type (text). Becomes `event_type`. */
  readonly type?: string;
  /** The JSONB payload in the *Scoping Events* convention. Becomes `payload`; never modified. */
  readonly payload?: string;
  /** When the row was recorded (timestamptz). Becomes `recorded_at`; added when missing. */
  readonly recordedAt?: string;
  /** Envelope metadata (jsonb). Becomes `metadata`; added when missing. */
  readonly metadata?: string;
  /** The recording transaction (xid8). Becomes `transaction_id`; added when missing. */
  readonly transactionId?: string;
}

/** `postgres: { adopt }` — take an existing table over at install. */
export interface AdoptOptions {
  /** Legacy column names; a column not named here is expected under the store's own name. */
  readonly columns?: AdoptColumns;
  /**
   * The `transaction_id` every legacy row gets: one low constant, so legacy rows sort before
   * every future one in the gap-free `(transactionId, sequence)` order. Default `"3"` (the lowest
   * normal transaction id). A durable subscriber starting from the beginning sees them all.
   */
  readonly legacyTransactionId?: string;
  /**
   * `"migrate"` (default): adopt at install. `"check"`: change nothing; a table that still needs
   * adopting makes the install fail with the plan, so a first run shows what would happen.
   */
  readonly mode?: "migrate" | "check";
  /**
   * Refuse to start when the table does not exist, instead of creating a new, empty one. Worth
   * setting on machines that must adopt: a mistyped `table` or `schemaName` would otherwise
   * create a fresh table that every later boot calls "current" — and never adopt the real one.
   * Default `false` (a missing table is created, with a warning).
   */
  readonly requireExisting?: boolean;
}

/** One column as the catalogue describes it. */
export interface CatalogColumn {
  readonly name: string;
  /** `format_type()` with the modifier: `bigint`, `character varying(64)`, `timestamp(3) with time zone`, … */
  readonly type: string;
  /** `format_type()` without the modifier: `character varying`, `timestamp with time zone`. Defaults to `type`. */
  readonly baseType?: string;
  readonly notNull: boolean;
  /** A default, an identity or a generated expression: an insert that omits it still works. */
  readonly filled: boolean;
  /** Whether the column's default draws from a sequence (serial) or it is an identity. */
  readonly sequenced: boolean;
}

/** What the planner needs to know about the table. */
export interface CatalogTable {
  readonly columns: readonly CatalogColumn[];
  /** Columns that carry a single-column unique index or constraint (no predicate). */
  readonly uniqueColumns: readonly string[];
  /** Columns that hold at least one NULL (only asked for nullable columns the plan cares about). */
  readonly columnsWithNulls: readonly string[];
  /** Indexes and triggers already on the table, for the notes. */
  readonly indexes: readonly string[];
  readonly triggers: readonly string[];
  /** Row-level security policies on the table (`permissive` ones are OR-ed: any of them opens rows). */
  readonly policies?: readonly { readonly name: string; readonly permissive: boolean }[];
  /** Roles other than the owner holding UPDATE, DELETE or TRUNCATE on an append-only table. */
  readonly writeGrants?: readonly string[];
}

/** What the store will add on top: its own policy is not foreign, and under `rls` a foreign permissive one is a hole. */
export interface AdoptContext {
  readonly rls?: boolean;
  readonly ownPolicy?: string;
}

export interface AdoptionPlan {
  /** `absent`: no table (the installer creates it); `current`: nothing to do; `legacy`: `statements` adopt it. */
  readonly state: "absent" | "current" | "legacy";
  /** In order; each is metadata-only unless a note says otherwise. */
  readonly statements: readonly string[];
  /** Any problem stops the adoption before anything runs. */
  readonly problems: readonly string[];
  /** What stays as it is and deserves a look (legacy indexes, triggers, extra columns). */
  readonly notes: readonly string[];
}

const TARGET = {
  sequence: "sequence_number",
  type: "event_type",
  payload: "payload",
  recordedAt: "recorded_at",
  metadata: "metadata",
  transactionId: "transaction_id",
} as const satisfies Record<keyof AdoptColumns, string>;

const TYPES: Record<keyof AdoptColumns, readonly string[]> = {
  sequence: ["bigint"],
  type: ["text", "character varying"],
  payload: ["jsonb"],
  recordedAt: ["timestamp with time zone"],
  metadata: ["jsonb"],
  transactionId: ["xid8"],
};

const HINTS: Partial<Record<keyof AdoptColumns, string>> = {
  sequence: "a sequence column of type integer needs ALTER COLUMN … TYPE bigint first (that rewrites the table — do it in a maintenance window)",
  payload: "a json column needs ALTER COLUMN … TYPE jsonb USING … first (that rewrites the table)",
  recordedAt: "a timestamp without time zone needs ALTER COLUMN … TYPE timestamptz USING … AT TIME ZONE '<zone>' first",
};

const MAX_XID8 = 2n ** 64n - 1n;

/**
 * Validates the legacy transaction id: a decimal xid8. Whether it is also BELOW every
 * transaction that can still commit — the point of it — only the database knows; the installer
 * checks that before it runs anything.
 */
export function legacyXid(options: AdoptOptions): string {
  const value = options.legacyTransactionId ?? "3";
  if (!/^\d{1,20}$/.test(value) || BigInt(value) > MAX_XID8) {
    throw new Error(`eventstore/postgres: adopt.legacyTransactionId must be a decimal transaction id, got "${value}"`);
  }
  return value;
}

/**
 * The plan for one table. `table` is `null` when the table does not exist. `tableRef` is the
 * quoted, schema-qualified name the statements use.
 */
export function planAdoption(table: CatalogTable | null, tableRef: string, options: AdoptOptions = {}, context: AdoptContext = {}): AdoptionPlan {
  if (table === null) return { state: "absent", statements: [], problems: [], notes: [] };
  const xid = legacyXid(options);
  const byName = new Map(table.columns.map((c) => [c.name, c] as const));
  const roles = Object.keys(TARGET) as (keyof AdoptColumns)[];
  const renames: { from: string; to: string }[] = [];
  const statements: string[] = [];
  const problems: string[] = [];
  const notes: string[] = [];
  const claimed = new Set<string>();

  // a legacy column named for two roles cannot become both
  const named = new Map<string, (keyof AdoptColumns)[]>();
  for (const role of roles) {
    const legacy = options.columns?.[role];
    if (legacy !== undefined) named.set(legacy, [...(named.get(legacy) ?? []), role]);
  }
  for (const [legacy, list] of named) if (list.length > 1) problems.push(`column "${legacy}" is named for ${list.join(" and ")} — one column can play one role`);
  // a target name that another role renames away is free: that column is not this role's
  const movedAway = new Set(roles.flatMap((role) => {
    const legacy = options.columns?.[role];
    return legacy !== undefined && legacy !== TARGET[role] && byName.has(legacy) ? [legacy] : [];
  }));

  for (const role of roles) {
    const target = TARGET[role];
    const legacy = options.columns?.[role] ?? target;
    const atTarget = movedAway.has(target) ? undefined : byName.get(target);
    const atLegacy = legacy === target ? undefined : byName.get(legacy);
    if (atTarget && atLegacy) {
      problems.push(`both "${legacy}" and "${target}" exist — the table looks half adopted; resolve it by hand`);
      continue;
    }
    const column = atTarget ?? atLegacy;
    if (!column) {
      if (options.columns?.[role] !== undefined && legacy !== target) {
        problems.push(`column "${legacy}" (named as the ${role} column) does not exist`);
        continue;
      }
      switch (role) {
        case "metadata":
          statements.push(`ALTER TABLE ${tableRef} ADD COLUMN metadata JSONB NOT NULL DEFAULT '{}'::jsonb`);
          break;
        case "transactionId":
          // a constant default is metadata-only; pg_current_xact_id() as the ADD default would rewrite every row
          statements.push(`ALTER TABLE ${tableRef} ADD COLUMN transaction_id XID8 NOT NULL DEFAULT ${quoteLiteral(xid)}::xid8`);
          statements.push(`ALTER TABLE ${tableRef} ALTER COLUMN transaction_id SET DEFAULT pg_current_xact_id()`);
          break;
        case "recordedAt":
          statements.push(`ALTER TABLE ${tableRef} ADD COLUMN recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()`);
          notes.push("recorded_at was missing: legacy rows carry the time of the adoption");
          break;
        default:
          problems.push(`no ${role} column: expected "${legacy}"${legacy === target ? "" : ` or "${target}"`} — name it in adopt.columns.${role}`);
      }
      continue;
    }
    claimed.add(column.name);
    const base = column.baseType ?? column.type;
    if (!TYPES[role].includes(base)) {
      problems.push(`"${column.name}" is ${column.type}, the ${role} column must be ${TYPES[role].join(" or ")}${HINTS[role] ? ` — ${HINTS[role]}` : ""}`);
      continue;
    }
    if (role === "type" && column.type !== base) notes.push(`"${column.name}" is ${column.type}: an event type longer than that is refused by the column (ALTER COLUMN event_type TYPE text lifts it without a rewrite)`);
    if (column.name !== target) renames.push({ from: column.name, to: target });
    if (role === "sequence") {
      if (!column.sequenced) problems.push(`"${column.name}" draws from no sequence (no serial default, no identity): the store inserts without it`);
      if (!table.uniqueColumns.includes(column.name)) {
        // UNIQUE, not PRIMARY KEY: the table may already have another primary key
        statements.push(`ALTER TABLE ${tableRef} ADD UNIQUE (sequence_number)`);
        notes.push("sequence_number had no unique index: adding one builds it (not metadata-only) and fails on duplicates");
      }
    }
    if (role === "transactionId" && !column.filled) statements.push(`ALTER TABLE ${tableRef} ALTER COLUMN transaction_id SET DEFAULT pg_current_xact_id()`);
    if (role === "metadata" && !column.filled) statements.push(`ALTER TABLE ${tableRef} ALTER COLUMN metadata SET DEFAULT '{}'::jsonb`);
    if (role === "recordedAt" && !column.filled) statements.push(`ALTER TABLE ${tableRef} ALTER COLUMN recorded_at SET DEFAULT now()`);
    if (!column.notNull) {
      // a row without a sequence is invisible to every cursor read; without the others it cannot be read at all
      if (table.columnsWithNulls.includes(column.name)) {
        problems.push(`"${column.name}" holds NULLs; the store needs a value in every row — fill them first`);
      } else {
        statements.push(`ALTER TABLE ${tableRef} ALTER COLUMN ${target} SET NOT NULL`);
        notes.push(`${target} becomes NOT NULL: Postgres checks every row for that (a scan, no rewrite)`);
      }
    }
  }

  for (const column of table.columns) {
    if (claimed.has(column.name) || ((Object.values(TARGET) as string[]).includes(column.name) && !movedAway.has(column.name))) continue;
    if (column.notNull && !column.filled) {
      problems.push(`"${column.name}" is NOT NULL without a default: every append of the store would fail — give it a default or drop NOT NULL`);
    } else {
      notes.push(`column "${column.name}" is kept as it is; the store neither reads nor writes it`);
    }
  }

  for (const policy of table.policies ?? []) {
    if (policy.name === context.ownPolicy) continue;
    if (context.rls && policy.permissive) {
      problems.push(`policy "${policy.name}" is permissive: under rls it is OR-ed with the store's tenant policy and opens rows of every tenant — drop it or make it RESTRICTIVE`);
    } else {
      notes.push(`policy "${policy.name}" is kept`);
    }
  }
  for (const grant of table.writeGrants ?? []) notes.push(`${grant} on an append-only table — revoke what no one needs`);

  // renames first, each after the one that frees its target name; a swap has no such order
  const ordered: string[] = [];
  const pending = [...renames];
  while (pending.length > 0) {
    const next = pending.findIndex((r) => !pending.some((o) => o !== r && o.from === r.to));
    if (next === -1) {
      problems.push(`columns ${pending.map((r) => `"${r.from}" → ${r.to}`).join(", ")} swap names — rename them by hand`);
      break;
    }
    const [r] = pending.splice(next, 1);
    ordered.push(`ALTER TABLE ${tableRef} RENAME COLUMN ${quoteIdent(r!.from)} TO ${r!.to}`);
  }
  const all = [...ordered, ...statements];

  if (all.length === 0) return { state: "current", statements: [], problems, notes: [] }; // an adopted table: nothing to say on every boot
  for (const index of table.indexes) notes.push(`index ${index} is kept — check whether the store's scope indexes make it redundant`);
  for (const trigger of table.triggers) notes.push(`trigger ${trigger} is kept and fires on the store's inserts too`);
  return { state: "legacy", statements: all, problems, notes };
}

/** The catalogue query behind `CatalogTable.columns`; `$1` = the quoted, qualified table name. */
export const ADOPT_COLUMNS_SQL = `SELECT a.attname AS name,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
       pg_catalog.format_type(a.atttypid, NULL) AS base_type,
       a.attnotnull AS not_null,
       (a.atthasdef OR a.attidentity <> '' OR a.attgenerated <> '') AS filled,
       (a.attidentity <> '' OR pg_catalog.pg_get_serial_sequence($1, a.attname) IS NOT NULL
         OR coalesce(pg_catalog.pg_get_expr(d.adbin, d.adrelid), '') LIKE 'nextval(%') AS sequenced
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`;

/** Single-column unique indexes (and so unique/primary-key constraints) without a predicate. */
export const ADOPT_UNIQUE_SQL = `SELECT a.attname AS name
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
WHERE i.indrelid = to_regclass($1) AND i.indisunique AND i.indisvalid AND i.indnkeyatts = 1 AND i.indpred IS NULL`;

export const ADOPT_INDEXES_SQL = `SELECT c.relname AS name FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE i.indrelid = to_regclass($1) ORDER BY 1`;

export const ADOPT_POLICIES_SQL = `SELECT polname AS name, polpermissive AS permissive FROM pg_catalog.pg_policy WHERE polrelid = to_regclass($1) ORDER BY 1`;

/** Roles other than the owner that may change or remove rows of the table. */
export const ADOPT_WRITE_GRANTS_SQL = `SELECT g.grantee || ' may ' || string_agg(g.privilege_type, '/' ORDER BY g.privilege_type) AS grant
FROM information_schema.role_table_grants g
JOIN pg_catalog.pg_class c ON c.oid = to_regclass($1)
WHERE g.table_schema = $2 AND g.table_name = $3 AND g.privilege_type IN ('UPDATE', 'DELETE', 'TRUNCATE')
  AND g.grantee <> pg_catalog.pg_get_userbyid(c.relowner)
GROUP BY g.grantee ORDER BY 1`;

export const ADOPT_TRIGGERS_SQL = `SELECT tgname AS name FROM pg_catalog.pg_trigger WHERE tgrelid = to_regclass($1) AND NOT tgisinternal ORDER BY 1`;

/**
 * The statements for a table in exactly the declared legacy shape, as text — for `install:
 * "none"` and DBAs. Unlike the installer this cannot look at the table: it assumes every named
 * legacy column exists, sequence/type/payload/recorded_at otherwise under the store's names, and
 * metadata/transaction_id missing. Prefer `store.adoptionPlan()`, which does look.
 */
export function adoptStatements(tableRef: string, options: AdoptOptions = {}): string[] {
  const columns: CatalogColumn[] = [];
  for (const role of Object.keys(TARGET) as (keyof AdoptColumns)[]) {
    // named: present under that name; unnamed: sequence, type, payload and recorded_at under the
    // store's own name (a legacy table has them), metadata and transaction_id missing
    const legacy = options.columns?.[role] ?? (role === "metadata" || role === "transactionId" ? undefined : TARGET[role]);
    if (legacy === undefined) continue;
    columns.push({ name: legacy, type: TYPES[role][0]!, notNull: true, filled: true, sequenced: role === "sequence" });
  }
  const sequence = options.columns?.sequence ?? TARGET.sequence;
  const plan = planAdoption({ columns, uniqueColumns: [sequence], columnsWithNulls: [], indexes: [], triggers: [] }, tableRef, options);
  return [...plan.statements];
}
