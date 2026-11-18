export interface SchemaProvider<SCHEMA, INPUT, OUPUT> {
  schema: SCHEMA;
  parse(data: unknown): Readonly<OUPUT>;
}

export namespace SchemaProvider {
  export type AnySchemaProvider = SchemaProvider<any, any, any>;
  export type InferOutput<SCHEMA extends AnySchemaProvider> =
    SCHEMA extends SchemaProvider<any, any, infer OUTPUT> ? OUTPUT : never;
  export type InferInput<SCHEMA extends AnySchemaProvider> =
    SCHEMA extends SchemaProvider<any, infer INPUT, any> ? INPUT : never;
}
