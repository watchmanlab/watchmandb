import Type from "typebox";
import Schema from "typebox/schema";
import type { SchemaProvider } from "./schema-provider.js";
import { Key } from "lmdb";

export class TypBoxSchema<
  SHAPE extends {},
  OUT extends Type.Static<Type.TObject<SHAPE>>,
> implements SchemaProvider<Type.TObject<SHAPE>, OUT> {
  schema: Type.TObject<SHAPE>;
  private validator: Schema.Validator;
  constructor(shape: SHAPE, options?: Type.TObjectOptions) {
    this.schema = Type.Object(shape, options);

    this.validator = Schema.Compile(this.schema);
  }
  parse(data: unknown): Readonly<OUT> {
    return this.validator.Parse(data) as never;
  }
}
