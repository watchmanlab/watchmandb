export interface SchemaProvider<SCHEMA, OUT> {
  schema: SCHEMA;
  parse(data: unknown): Readonly<OUT>;
}

export namespace SchemaProvider {
  export type InferOutput<SCHEMA extends SchemaProvider<any, any>> =
    SCHEMA extends SchemaProvider<any, infer O> ? O : never;
}
