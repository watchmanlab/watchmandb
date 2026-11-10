import { open, RootDatabase, RootDatabaseOptions } from "lmdb";
import { Table } from "./table";

import { Index } from "./table-index";

export type Config = {
  path: string;
  tables: {
    [K: string]: Table.AnyDefinition;
  };
};

export function createDb<CONFIG extends Config>(
  config: CONFIG,
  options?: RootDatabaseOptions,
) {
  const { path, tables } = config;
  const env = open(path, { ...options, maxDbs: 10 });
}
