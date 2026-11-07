import z from "zod";
import { Index } from ".";
import { SchemaProvider } from "./schema-provider";
import { Table } from "./table";
import { ZodSchema } from "./zod-schema";

export type IndexDefinition<
  TABLE extends Table.AnyTable,
  KEY extends (keyof Table.InferRow<TABLE>)[],
> = Omit<Index.Init<TABLE, KEY>, "env" | "name" | "table">;

export type Indexes<
  TABLE extends Table.AnyTable,
  KEY extends (keyof Table.InferRow<TABLE>)[],
> = {
  [K in string]: IndexDefinition<TABLE, KEY>;
};

export type TableDefinition<
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  INDEXES extends Indexes<
    Table<SCHEMA>,
    (keyof Table.InferRow<Table<SCHEMA>>)[]
  >,
> = {
  schema: SCHEMA;
  indexes: INDEXES;
};

export function defineIndex<
  TABLE extends Table.AnyTable,
  const KEY extends [
    keyof Table.InferRow<TABLE>,
    ...(keyof Table.InferRow<TABLE>)[],
  ],
>(def: IndexDefinition<TABLE, KEY>) {
  return def;
}
export function defineTable<
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  INDEXES extends Indexes<
    Table<SCHEMA>,
    (keyof Table.InferRow<Table<SCHEMA>>)[]
  >,
>(def: TableDefinition<SCHEMA, INDEXES>) {
  return def;
}
export function create<DEF extends Record<string, TableDefinition<any, any>>>(
  def: DEF,
) {
  return def;
}

const v = create({
  users: defineTable({
    schema: new ZodSchema({
      id: z.number(),
      name: z.string(),
      age: z.number(),
    }),
    indexes: {
      byName: defineIndex({ key: ["name"] }),
    },
  }),
});
