import { Key } from "lmdb";
import type { SchemaProvider } from "./schema-provider.js";
import { z } from "zod";

export class ZodSchema<
  SHAPE extends {
    readonly id: z.ZodString | z.ZodNumber;
  },
  OUT extends z.output<z.ZodObject<SHAPE>> extends { id: Key }
    ? z.output<z.ZodObject<SHAPE>>
    : z.output<z.ZodObject<SHAPE>> & { id: Key },
> implements SchemaProvider<z.ZodObject<SHAPE>, OUT> {
  schema: z.ZodObject<SHAPE, z.core.$strip>;
  constructor(shape: SHAPE) {
    this.schema = z.object(shape);
  }
  parse(data: unknown): Readonly<OUT> {
    return Object.freeze(this.schema.parse(data)) as never;
  }
  looseObject(): this {
    this.schema = z.looseObject(this.schema.shape);
    return this;
  }
}
