import { describe, expect, it } from "vitest";
import { adoptStatements, planAdoption, type CatalogColumn, type CatalogTable } from "../src/postgres/index.js";

const col = (name: string, type: string, extra: Partial<CatalogColumn> = {}): CatalogColumn => ({ name, type, notNull: true, filled: false, sequenced: false, ...extra });
const legacy = (columns: CatalogColumn[], extra: Partial<CatalogTable> = {}): CatalogTable => ({
  columns,
  uniqueColumns: ["id"],
  columnsWithNulls: [],
  indexes: [],
  triggers: [],
  ...extra,
});
const consumer = () =>
  legacy([
    col("id", "bigint", { filled: true, sequenced: true }),
    col("eventtype", "text"),
    col("payload", "jsonb"),
    col("recorded_at", "timestamp with time zone", { filled: true }),
  ]);
const T = '"public"."events"';
const columns = { sequence: "id", type: "eventtype" };

describe("planAdoption", () => {
  it("adopts the consumer's table with renames and metadata-only additions", () => {
    const plan = planAdoption(consumer(), T, { columns });
    expect(plan.problems).toEqual([]);
    expect(plan.state).toBe("legacy");
    expect(plan.statements).toEqual([
      `ALTER TABLE ${T} RENAME COLUMN "id" TO sequence_number`,
      `ALTER TABLE ${T} RENAME COLUMN "eventtype" TO event_type`,
      `ALTER TABLE ${T} ADD COLUMN metadata JSONB NOT NULL DEFAULT '{}'::jsonb`,
      `ALTER TABLE ${T} ADD COLUMN transaction_id XID8 NOT NULL DEFAULT '3'::xid8`,
      `ALTER TABLE ${T} ALTER COLUMN transaction_id SET DEFAULT pg_current_xact_id()`,
    ]);
  });

  it("does nothing, and says nothing, for a table already in the store's shape", () => {
    const current = legacy(
      [
        col("sequence_number", "bigint", { filled: true, sequenced: true }),
        col("event_type", "text"),
        col("payload", "jsonb"),
        col("metadata", "jsonb", { filled: true }),
        col("recorded_at", "timestamp with time zone", { filled: true }),
        col("transaction_id", "xid8", { filled: true }),
      ],
      { uniqueColumns: ["sequence_number"], indexes: ["events_pkey"] },
    );
    expect(planAdoption(current, T, { columns })).toEqual({ state: "current", statements: [], problems: [], notes: [] });
    expect(planAdoption(null, T, { columns }).state).toBe("absent");
  });

  it("refuses what it cannot adopt without rewriting or guessing", () => {
    const problems = (table: CatalogTable, opts = { columns }) => planAdoption(table, T, opts).problems.join(" | ");
    const withColumn = (i: number, c: CatalogColumn) => {
      const t = consumer();
      return { ...t, columns: t.columns.map((x, j) => (j === i ? c : x)) };
    };
    expect(problems(withColumn(0, col("id", "integer", { filled: true, sequenced: true })))).toMatch(/must be bigint.*rewrites/);
    expect(problems(withColumn(2, col("payload", "json")))).toMatch(/must be jsonb/);
    expect(problems(withColumn(0, col("id", "bigint")))).toMatch(/no sequence/);
    expect(problems({ ...consumer(), columns: [...consumer().columns, col("tenant", "text")] })).toMatch(/"tenant" is NOT NULL without a default/);
    expect(problems({ ...consumer(), columns: [...consumer().columns, col("sequence_number", "bigint")] })).toMatch(/half adopted/);
    expect(problems(consumer(), { columns: { sequence: "seq", type: "eventtype" } })).toMatch(/"seq".*does not exist/);
    const nullable = withColumn(3, col("recorded_at", "timestamp with time zone", { notNull: false, filled: true }));
    expect(problems({ ...nullable, columnsWithNulls: ["recorded_at"] })).toMatch(/holds NULLs/);
    expect(planAdoption(nullable, T, { columns }).statements).toContain(`ALTER TABLE ${T} ALTER COLUMN recorded_at SET NOT NULL`);
  });

  it("keeps extra columns, legacy indexes and triggers, and says so", () => {
    const table = { ...consumer(), columns: [...consumer().columns, col("note", "text", { notNull: false })], indexes: ["events_payload_gin"], triggers: ["audit"] };
    const plan = planAdoption(table, T, { columns });
    expect(plan.problems).toEqual([]);
    expect(plan.notes.join(" ")).toMatch(/"note" is kept.*events_payload_gin is kept.*audit is kept/);
  });

  it("adoptStatements gives the same statements as text for the declared shape", () => {
    expect(adoptStatements(T, { columns })).toEqual(planAdoption(consumer(), T, { columns }).statements);
    expect(() => adoptStatements(T, { columns, legacyTransactionId: "3; DROP TABLE x" })).toThrow(/decimal/);
  });

  it("renames first, in the order that frees each name; a legacy column named metadata can become the payload", () => {
    const table = legacy([
      col("id", "bigint", { filled: true, sequenced: true }),
      col("eventtype", "text"),
      col("metadata", "jsonb"), // the real payload, under the store's name for something else
    ]);
    const plan = planAdoption(table, T, { columns: { ...columns, payload: "metadata" } });
    expect(plan.problems).toEqual([]);
    const renameAt = plan.statements.indexOf(`ALTER TABLE ${T} RENAME COLUMN "metadata" TO payload`);
    const addAt = plan.statements.indexOf(`ALTER TABLE ${T} ADD COLUMN metadata JSONB NOT NULL DEFAULT '{}'::jsonb`);
    expect(renameAt).toBeGreaterThanOrEqual(0);
    expect(addAt).toBeGreaterThan(renameAt);
  });

  it("refuses one column named for two roles, and two columns that swap names", () => {
    const table = legacy([col("id", "bigint", { filled: true, sequenced: true }), col("eventtype", "text"), col("data", "jsonb")]);
    expect(planAdoption(table, T, { columns: { ...columns, payload: "data", metadata: "data" } }).problems.join(" ")).toMatch(/"data" is named for payload and metadata/);
    const swapped = legacy([
      col("id", "bigint", { filled: true, sequenced: true }),
      col("payload", "text"), // holds the type
      col("event_type", "jsonb"), // holds the payload
    ]);
    expect(planAdoption(swapped, T, { columns: { sequence: "id", type: "payload", payload: "event_type" } }).problems.join(" ")).toMatch(/swap names/);
  });

  it("a nullable sequence column: NULLs refuse the adoption, otherwise it becomes NOT NULL", () => {
    const table = legacy([col("id", "bigint", { notNull: false, filled: true, sequenced: true }), col("eventtype", "text"), col("payload", "jsonb")]);
    expect(planAdoption({ ...table, columnsWithNulls: ["id"] }, T, { columns }).problems.join(" ")).toMatch(/"id" holds NULLs/);
    expect(planAdoption(table, T, { columns }).statements).toContain(`ALTER TABLE ${T} ALTER COLUMN sequence_number SET NOT NULL`);
  });

  it("matches bounded types by their base type and says what the bound means", () => {
    const table = legacy([
      col("id", "bigint", { filled: true, sequenced: true }),
      col("eventtype", "character varying(64)", { baseType: "character varying" }),
      col("payload", "jsonb"),
      col("recorded_at", "timestamp(3) with time zone", { baseType: "timestamp with time zone", filled: true }),
    ]);
    const plan = planAdoption(table, T, { columns });
    expect(plan.problems).toEqual([]);
    expect(plan.notes.join(" ")).toMatch(/character varying\(64\).*longer than that is refused/);
  });

  it("under rls a foreign permissive policy is a hole; the store's own policy and restrictive ones are not", () => {
    const table = { ...consumer(), policies: [{ name: "reporting", permissive: true }, { name: "narrow", permissive: false }, { name: "es_events_tenant_isolation", permissive: true }], writeGrants: ["reporter may DELETE/UPDATE"] };
    const context = { rls: true, ownPolicy: "es_events_tenant_isolation" };
    const plan = planAdoption(table, T, { columns }, context);
    expect(plan.problems).toHaveLength(1);
    expect(plan.problems[0]).toMatch(/policy "reporting" is permissive/);
    expect(planAdoption(table, T, { columns }, { rls: false }).problems).toEqual([]);
    expect(planAdoption(table, T, { columns }, { rls: false }).notes.join(" ")).toMatch(/reporter may DELETE\/UPDATE/);
  });

  it("the legacy transaction id must be a decimal xid8 in range", () => {
    expect(() => planAdoption(consumer(), T, { columns, legacyTransactionId: "18446744073709551616" })).toThrow(/decimal transaction id/);
    expect(() => planAdoption(consumer(), T, { columns, legacyTransactionId: "18446744073709551615" })).not.toThrow();
  });
});

