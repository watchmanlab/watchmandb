import type { SchemaProvider } from "./schema-provider";
import { z } from "zod";
import { fail, Result, success } from "./utils";

export class ZodSchema<SCHEMA extends z.ZodType> implements SchemaProvider<
  SCHEMA,
  Readonly<z.input<SCHEMA>>,
  Readonly<z.output<SCHEMA>>,
  z.ZodError<z.core.output<SCHEMA>>
> {
  schema: SCHEMA;
  constructor(schema: SCHEMA) {
    this.schema = schema;
  }
  parse(
    data: unknown,
  ): Result<z.output<SCHEMA>, z.ZodError<z.core.output<SCHEMA>>> {
    const result = this.schema.safeParse(data);
    if (result.success) {
      return success(result.data);
    } else {
      return fail(result.error);
    }
  }
}

export function zodSchema<SCHEMA extends z.ZodType>(
  schema: SCHEMA,
): SchemaProvider<
  SCHEMA,
  Readonly<z.input<SCHEMA>>,
  Readonly<z.output<SCHEMA>>,
  z.ZodError<z.core.output<SCHEMA>>
> {
  return new ZodSchema(schema);
}
