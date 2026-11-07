import { open, RootDatabase, RootDatabaseOptions } from "lmdb";
import { Table } from "./table";
import { Index } from ".";
import { View } from "./view";

export type Definition<
  T extends Table.AnyTable | Index.AnyIndex | View.AnyView,
> = T extends Table.AnyTable
  ? { sourceName: never; init: (env: RootDatabase) => T }
  : {
      sourceName: string;
      init: (env: RootDatabase, Table: Table.AnyTable) => T;
    };

export function createDb<ENT extends [Definition<any>, ...Definition<any>[]]>(
  path: string,
  entries: ENT,
  options?: RootDatabaseOptions,
) {
  const env = open(path, { ...options, maxDbs: entries.length });
  const tables = new Map(
    entries
      .filter((d) => !d.sourceName)
      .map((d) => {
        const table = d.init(env, null as never);
        return [table.name, table];
      }),
  );
  const others = entries
    .filter((d) => d.sourceName)
    .map((d) => d.init(env, tables.get(d.sourceName)));
}
