import { describe, expectTypeOf, it } from "vitest";
import { z } from "zod";
import {
  MemoryStore,
  buildSchema,
  createEventStore,
  defineEvents,
  type Fold,
  type NewEventOf,
  type QueryResult,
  type RecordedEvent,
  type RecordedEventOf,
  type RegistryLike,
} from "../src/index.js";

const articles = defineEvents({
  ArticleDrafted: {
    data: z.object({ title: z.string(), slug: z.string() }),
    scopes: ["workspaceProvisionedId"],
    optionalScopes: ["categoryDefinedId"],
  },
  ArticleArchived: {
    data: z.object({ reason: z.string().optional() }),
    scopes: ["articleDraftedId"],
  },
  Pinged: { data: z.object({ n: z.number() }) },
});

type Defs = typeof articles.$defs;

describe("types", () => {
  it("creators infer data, require declared scopes, reject unknown scope keys", () => {
    const e = articles.ArticleDrafted({ title: "t", slug: "s" }, { workspaceProvisionedId: "w" });
    expectTypeOf(e.type).toEqualTypeOf<"ArticleDrafted">();
    expectTypeOf(e.data).toEqualTypeOf<{ title: string; slug: string }>();
    expectTypeOf(e.id).toEqualTypeOf<string>();
    expectTypeOf(e.scopes.workspaceProvisionedId).toEqualTypeOf<string>();
    expectTypeOf(e.scopes.categoryDefinedId).toEqualTypeOf<string | undefined>();

    // compile-time only: these would (rightly) throw at runtime
    const rejectedByTypes = () => {
      // @ts-expect-error unknown data field
      articles.ArticleDrafted({ title: "t", slug: "s", nope: 1 }, { workspaceProvisionedId: "w" });
      // @ts-expect-error required scopes argument missing
      articles.ArticleDrafted({ title: "t", slug: "s" });
      // @ts-expect-error wrong key, required key absent
      articles.ArticleDrafted({ title: "t", slug: "s" }, { bogusId: "x" });
      // @ts-expect-error unknown key alongside the required one
      articles.ArticleDrafted({ title: "t", slug: "s" }, { workspaceProvisionedId: "w", bogusId: "x" });
      // @ts-expect-error a scope-less event has no scope keys
      articles.Pinged({ n: 1 }, { anyId: "x" });
    };
    void rejectedByTypes;

    // optional scope accepted; scope-less events take an optional second argument
    articles.ArticleDrafted({ title: "t", slug: "s" }, { workspaceProvisionedId: "w", categoryDefinedId: "c" });
    articles.Pinged({ n: 1 });
    articles.Pinged({ n: 1 }, {});
  });

  it("fold handlers get typed data and typed scopes; the registry fold is incremental", () => {
    const fold = articles.$fold<Map<string, string>>({
      ArticleDrafted: (data, state, event) => {
        expectTypeOf(data).toEqualTypeOf<{ title: string; slug: string }>();
        expectTypeOf(event.scopes.workspaceProvisionedId).toEqualTypeOf<string>();
        expectTypeOf(event.id).toEqualTypeOf<string>();
        return new Map(state).set(event.id, data.title);
      },
      ArticleArchived: (_data, state, event) => {
        expectTypeOf(event.scopes.articleDraftedId).toEqualTypeOf<string>();
        return state;
      },
      // @ts-expect-error unknown handler key
      Nope: (_d: unknown, s: Map<string, string>) => s,
    });
    expectTypeOf(fold).toMatchTypeOf<Fold<Map<string, string>>>();

    const oneShot = articles.$foldAll(0, { Pinged: (d, s) => s + d.n });
    expectTypeOf(oneShot).parameter(0).toEqualTypeOf<readonly RecordedEvent[]>();
    // a one-shot fold must not slot into an incremental fold
    // @ts-expect-error `$foldAll` returns (events) => S, not Fold<S>
    const wrong: Fold<number> = oneShot;
    void wrong;
  });

  it("RecordedEventOf narrows on type; $parse and $is narrow", () => {
    type Ev = RecordedEventOf<Defs>;
    const narrow = (e: Ev) => {
      if (e.type === "ArticleDrafted") {
        expectTypeOf(e.data).toEqualTypeOf<{ title: string; slug: string }>();
        expectTypeOf(e.scopes.workspaceProvisionedId).toEqualTypeOf<string>();
      } else if (e.type === "ArticleArchived") {
        expectTypeOf(e.data).toEqualTypeOf<{ reason?: string | undefined }>();
      } else {
        expectTypeOf(e.type).toEqualTypeOf<"Pinged">();
      }
    };
    void narrow;
    const narrowing = (any: RecordedEvent) => {
      if (articles.$is(any)) expectTypeOf(any).toEqualTypeOf<Ev>();
      expectTypeOf(articles.$parse(any)).toEqualTypeOf<Ev>();
    };
    void narrowing;
    type New = NewEventOf<Defs, "ArticleArchived">;
    expectTypeOf<New["scopes"]["articleDraftedId"]>().toEqualTypeOf<string>();
  });

  it("read with omit trims the data type per event; without omit the union is unchanged", async () => {
    const api = createEventStore({ events: [articles], store: new MemoryStore({ schema: buildSchema([articles]) }) });
    await api.append([articles.ArticleDrafted({ title: "t", slug: "s" }, { workspaceProvisionedId: "w" })]);
    const trimmed = await api.read(articles, undefined, { omit: ["slug"] });
    const e = trimmed.events[0]!;
    if (e.type === "ArticleDrafted") {
      expectTypeOf(e.data).toEqualTypeOf<{ title: string }>();
      expectTypeOf(e.scopes.workspaceProvisionedId).toEqualTypeOf<string>();
    }
    if (e.type === "Pinged") expectTypeOf(e.data).toEqualTypeOf<{ n: number }>();
    const full = await api.read(articles);
    expectTypeOf(full.events[0]!).toEqualTypeOf<RecordedEventOf<Defs>>();
    expectTypeOf(articles.$parse(full.events[0]!, ["title"])).toEqualTypeOf<RecordedEventOf<Defs, keyof Defs, "title">>();
  });

  it("query results carry a ctx handle that appendIf accepts; registries are RegistryLike", async () => {
    const store = new MemoryStore({ schema: buildSchema([articles], { strict: false }) });
    const read: QueryResult = await store.query(articles.$filter());
    expectTypeOf(read.ctx).toEqualTypeOf<Parameters<typeof store.appendIf>[1]>();
    expectTypeOf(read.contextVersion).toEqualTypeOf<number>();
    expectTypeOf(read.lastReturned).toEqualTypeOf<number>();
    const like: RegistryLike = articles;
    void like;
    const registries: readonly RegistryLike[] = [articles];
    void registries;
  });
});
