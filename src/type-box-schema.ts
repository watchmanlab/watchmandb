import Type from "typebox";
import Schema, { ParseError } from "typebox/schema";
import type { SchemaProvider } from "./schema-provider.js";
import { fail, Result, success } from "./utils.js";

export class TypeBoxSchema<
  SCHEMA extends Type.TSchema,
> implements SchemaProvider<
  SCHEMA,
  Readonly<Type.StaticEncode<SCHEMA>>,
  Readonly<Type.StaticDecode<SCHEMA>>,
  ParseError
> {
  schema: SCHEMA;
  private validator: Schema.Validator<SCHEMA>;
  constructor(schema: SCHEMA) {
    this.schema = schema;

    this.validator = Schema.Compile(this.schema);
  }
  parse(
    data: unknown,
  ): Result<Readonly<Type.StaticDecode<SCHEMA>>, ParseError> {
    try {
      return success(Object.freeze(this.validator.Parse(data)) as never);
    } catch (error: any) {
      return fail(error);
    }
  }
}

export function typeBox<SCHEMA extends Type.TSchema>(
  schema: SCHEMA,
): SchemaProvider<
  SCHEMA,
  Readonly<Type.StaticEncode<SCHEMA>>,
  Readonly<Type.StaticDecode<SCHEMA>>,
  unknown
> {
  return new TypeBoxSchema(schema);
}
