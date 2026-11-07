import { open, RootDatabase, RootDatabaseOptions } from "lmdb";
import { Table } from "./table";

export class WatchmanDB<
  TABLES extends readonly [Table.AnyTable, ...Table.AnyTable[]],
> {
  private env: RootDatabase;
  constructor(path: string, tables: TABLES, options?: RootDatabaseOptions) {
    this.env = open(path, options ?? {});
  }
}
