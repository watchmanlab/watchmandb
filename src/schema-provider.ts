import { Table } from "./table";

export interface SchemaProvider<SCHEMA, OUT extends Table.Row> {
  schema: SCHEMA;
  parse(data: unknown): Readonly<OUT>;
}

export namespace SchemaProvider {
  export type AnySchemaProvider = SchemaProvider<any, any>;
  export type InferOutput<SP extends SchemaProvider<any, any>> =
    SP extends SchemaProvider<any, infer O> ? O : never;
}
