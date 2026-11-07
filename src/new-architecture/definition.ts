import z from "zod";
import { Index } from ".";
import { SchemaProvider } from "./schema-provider";

import { ZodSchema } from "./zod-schema";

export type IndexDefinition<
  ROW extends Record<string, any>,
  out KEY extends [keyof ROW, ...(keyof ROW)[]],
> = Omit<Index.Init<ROW, KEY>, "env" | "name" | "table">;

export type IndexesDefinition<SCHEMA extends SchemaProvider.AnySchemaProvider> =
  Record<
    string,
    IndexDefinition<
      SchemaProvider.InferOutput<SCHEMA>,
      [
        keyof SchemaProvider.InferOutput<SCHEMA>,
        ...(keyof SchemaProvider.InferOutput<SCHEMA>)[],
      ]
    >
  >;

export type TableDefinition<
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  out INDEXES extends IndexesDefinition<SCHEMA>,
> = {
  schema: SCHEMA;
  indexes: INDEXES;
};

export function defineIndex<
  ROW extends Record<string, any>,
  KEY extends [keyof ROW, ...(keyof ROW)[]],
>(def: IndexDefinition<ROW, KEY>): IndexDefinition<ROW, KEY> {
  return def;
}
export function defineTable<
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  INDEXES extends IndexesDefinition<SCHEMA>,
>(schema: SCHEMA, indexes?: INDEXES) {
  return {
    schema,
    indexes,
  };
}

export function create<DEF extends Record<string, TableDefinition<any, any>>>(
  def: DEF,
) {
  return def;
}

const db = create({
  users: defineTable(
    new ZodSchema({
      id: z.number(),
      name: z.string(),
      age: z.number(),
    }),
    {
      byName: defineIndex({ key: ["name", "age"] }),
    },
  ),
});
