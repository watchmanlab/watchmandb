import z from "zod";
import { Index } from ".";
import { SchemaProvider } from "./schema-provider";
import { Table } from "./table";
import { ZodSchema } from "./zod-schema";

export type IndexDefinition<
  TABLE extends Table.AnyTable,
  KEY extends (keyof Table.InferRow<TABLE>)[],
> = Omit<Index.Init<TABLE, KEY>, "env" | "name" | "table">;

export type TableDefinition<SCHEMA extends SchemaProvider.AnySchemaProvider> = {
  schema: SCHEMA;
  indexes: {
    [K: string]: IndexDefinition<
      Table<SCHEMA>,
      (keyof Table.InferRow<Table<SCHEMA>>)[]
    >;
  };
};
export function defineIndex<
  TABLE extends Table.AnyTable,
  KEY extends (keyof Table.InferRow<TABLE>)[],
>(def: IndexDefinition<TABLE, KEY>) {
  return def;
}
export function defineTable<SCHEMA extends SchemaProvider.AnySchemaProvider>(
  def: TableDefinition<SCHEMA>,
) {
  return def;
}
export function create<DEF extends Record<string, TableDefinition<any>>>(
  def: DEF,
) {
  return def;
}

const v = create({
  users: {
    schema: new ZodSchema({
      id: z.number(),
      name: z.string(),
      age: z.number(),
    }),
    indexes: {
      byName: {
        key: ["ddd"],
        unique: false,
      },
    },
  },
});
