import { open, RootDatabase, RootDatabaseOptions } from "lmdb";

export class WatchmanDB<
  ENTRIES extends readonly [
    (env: RootDatabase) => any,
    ...((env: RootDatabase) => any)[],
  ],
> {
  private env: RootDatabase;
  constructor(path: string, entries: ENTRIES, options?: RootDatabaseOptions) {
    this.env = open(path, options ?? {});
  }
}

export function createDb(
  path: string,
  entries: "",
  options?: RootDatabaseOptions,
) {
  const env = open(path, options ?? {});
}
