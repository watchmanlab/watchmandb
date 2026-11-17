import Type from "typebox";
import Schema from "typebox/schema";
import type { SchemaProvider } from "./schema-provider.js";

export class TypeBoxSchema<
  SCHEMA extends Type.TSchema,
> implements SchemaProvider<SCHEMA, Type.Static<SCHEMA>> {
  schema: SCHEMA;
  private validator: Schema.Validator;
  constructor(schema: SCHEMA) {
    this.schema = schema;

    this.validator = Schema.Compile(this.schema);
  }
  parse(data: unknown): Readonly<Type.Static<SCHEMA>> {
    return this.validator.Parse(data) as never;
  }
}

export function typeBox<SCHEMA extends Type.TSchema>(
  schema: SCHEMA,
): SchemaProvider<SCHEMA, Readonly<Type.Static<SCHEMA>>> {
  return new TypeBoxSchema(schema);
}
