import { open, RootDatabase, RootDatabaseOptions } from "lmdb";
import { Table } from "./table";
import { Index } from "./table-index";
import { Stream } from "@watchmanlab/stream";
import z from "zod";

export function createDb<CONFIG extends Config>(
  config: CONFIG,
  options?: RootDatabaseOptions,
) {
  const { path, tables } = config;
  const env = open(path, { ...options, maxDbs: 10 });
}

const userSchema = z.object({
  id: z.number(),
  name: z.string(),
  age: z.number(),
});

const stream = new Stream<z.output<typeof userSchema>>();
