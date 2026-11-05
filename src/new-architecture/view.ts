import z from "zod";
import { Table } from "./table";
import { ZodSchema } from "./zod-schema";
import { RootDatabase } from "lmdb";

export class View<
  TABLES extends readonly [Table.AnyTable, ...Table.AnyTable[]],
  OUTPUT extends Record<string, any>,
> {
  constructor(init: {
    name: string;
    env: RootDatabase;
    tables: TABLES;
    transform: (source: View.Select<TABLES>) => OUTPUT; // executed only on insert/update
  }) {
    // here we listen to tables provided
    // if insert/update execute transform (create db if not exist)
    // else delete
    // and update the view
  }
  get() {
    // we return a range wrapped by a Source stream
  }
}

export namespace View {
  export type Select<
    TABLES extends readonly [Table.AnyTable, ...Table.AnyTable[]],
  > = {
    [K in keyof TABLES]: {
      name: Table.InferName<TABLES[K]>;
      row: Table.InferRow<TABLES[K]>;
    };
  }[number];
}

/// a simple DX test
const users = new Table({
  name: "users",
  env: null as any,
  schema: new ZodSchema({ id: z.number(), name: z.string(), age: z.number() }),
});
const requests = new Table({
  name: "requests",
  env: null as any,
  schema: new ZodSchema({
    id: z.number(),
    category: z.union([z.literal("ride"), z.literal("delivery")]),
    description: z.string().optional(),
    location: z.tuple([z.number(), z.number()]),
  }),
});

const userNames = new View({
  name: "userNames",
  env: null as any,
  tables: [users, requests],
  transform(source) {
    if (source.name === "requests") {
      source.row.category;
    }
    return { kk: "kechmahaja", bb: 45 };
  },
});
