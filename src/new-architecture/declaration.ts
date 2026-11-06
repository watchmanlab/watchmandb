import z from "zod";
import { SchemaProvider } from "./schema-provider";
import { ZodSchema } from "./zod-schema";

//experimental
export type QueryDeclaration<SHAPE extends Record<string, any>> = {
  [K in "eq" | "lt" | "lte" | "gt" | "gte" | "or"]?: keyof SHAPE;
};

export function declareTable<SP extends SchemaProvider<any, any>>(
  schemaProvider: SP,
  queries?: Record<
    string,
    QueryDeclaration<Omit<SchemaProvider.InferOutput<SP>, "id">>
  >,
) {}

const user = declareTable(
  new ZodSchema({
    id: z.number(),
    name: z.string(),
    age: z.number(),
    count1: z.number(),
    count2: z.number(),
    count3: z.number(),
  }),
  {
    byName: {
      eq: "name",
      gte: "count1",
      lte: "count3",
    },
  },
);
