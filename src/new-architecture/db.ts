import { open, RootDatabase, RootDatabaseOptions } from "lmdb";

export class WatchmanDB<
  ENTRIES extends readonly [
    (env: RootDatabase) => void,
    ...((env: RootDatabase) => void)[],
  ],
> {
  private env: RootDatabase;
  constructor(path: string, entries: ENTRIES, options?: RootDatabaseOptions) {
    this.env = open(path, options ?? {});
  }
}
