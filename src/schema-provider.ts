import { Result } from "./utils";

export interface SchemaProvider<SCHEMA, INPUT, OUPUT, ERROR> {
  schema: SCHEMA;
  parse(data: unknown): Result<Readonly<OUPUT>, ERROR>;
}

export namespace SchemaProvider {
  export type AnySchemaProvider = SchemaProvider<any, any, any, any>;
  export type InferOutput<SCHEMA extends AnySchemaProvider> =
    SCHEMA extends SchemaProvider<any, any, infer OUTPUT, any> ? OUTPUT : never;
  export type InferInput<SCHEMA extends AnySchemaProvider> =
    SCHEMA extends SchemaProvider<any, infer INPUT, any, any> ? INPUT : never;
  export type InferError<SCHEMA extends AnySchemaProvider> =
    SCHEMA extends SchemaProvider<any, any, any, infer ERROR> ? ERROR : never;
}
