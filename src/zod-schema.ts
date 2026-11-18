import type { SchemaProvider } from "./schema-provider";
import { z } from "zod";

export class ZodSchema<SCHEMA extends z.ZodType> implements SchemaProvider<
  SCHEMA,
  Readonly<z.input<SCHEMA>>,
  Readonly<z.output<SCHEMA>>
> {
  schema: SCHEMA;
  constructor(schema: SCHEMA) {
    this.schema = schema;
  }
  parse(data: unknown): Readonly<z.output<SCHEMA>> {
    return Object.freeze(this.schema.parse(data));
  }
}

export function zodSchema<SCHEMA extends z.ZodType>(
  schema: SCHEMA,
): SchemaProvider<
  SCHEMA,
  Readonly<z.input<SCHEMA>>,
  Readonly<z.output<SCHEMA>>
> {
  return new ZodSchema(schema);
}
