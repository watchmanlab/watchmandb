import { listen } from "@watchmanlab/stream";
import { Table } from "./table";
import { Key, RootDatabase } from "lmdb";
import { isDeepEqual } from "./utils";

export class View<
  SOURCE extends Table.AnyTable,
  KEY extends Key,
  VALUE = Table.InferValue<SOURCE>,
  TX extends boolean = false,
> extends Table<never, KEY, VALUE, TX> {
  constructor(private init: View.Init<SOURCE, KEY, VALUE>) {
    const { name, env, table } = init;
    super({ env, name: `view:${table.name}:${name}` });

    table.$beforeInsert.pipe(
      listen(({ key, curr }) => this._beforeInsert(key, curr)),
    );
    table.$beforeUpdate.pipe(
      listen(({ key, curr, prev }) => this._beforeUpdate(key, curr, prev)),
    );
    table.$beforeDelete.pipe(
      listen(({ key, prev }) => this._beforeDelete(key, prev)),
    );
  }

  private _beforeInsert(
    key: Table.InferKey<SOURCE>,
    curr: Readonly<Table.InferValue<SOURCE>>,
  ) {
    if (this.init.filter && this.init.filter(curr) === false) return;

    const newKey = this.init.keySelector?.(curr) ?? key;
    this.insert(newKey, this.init.map?.(curr) ?? curr);
  }
  private _beforeUpdate(
    key: Table.InferKey<SOURCE>,
    curr: Readonly<Table.InferValue<SOURCE>>,
    prev: Readonly<Table.InferValue<SOURCE>>,
  ) {
    if (this.init.filter && this.init.filter(curr) === false) return;

    const oldKey = this.init.keySelector?.(prev) ?? key;
    const newKey = this.init.keySelector?.(curr) ?? key;

    if (isDeepEqual(oldKey, newKey)) return;

    this.delete(oldKey);
    this.insert(newKey, this.init.map?.(curr) ?? curr);
  }
  private _beforeDelete(
    key: Table.InferKey<SOURCE>,
    prev: Readonly<Table.InferValue<SOURCE>>,
  ) {
    this.delete(this.init.keySelector?.(prev) ?? key);
  }
}

export function createView<
  SOURCE extends Table.AnyTable,
  KEY extends Key,
  VALUE,
>(init: Omit<View.Init<SOURCE, KEY, VALUE>, "env" | "table">) {
  return (env: RootDatabase, table: SOURCE) =>
    new View({ ...init, env, table } as View.Init<SOURCE, KEY, VALUE>);
}

export namespace View {
  export type Init<SOURCE extends Table.AnyTable, KEY extends Key, VALUE> = {
    name: string;
    env: RootDatabase;
    table: SOURCE;
    keySelector?: (row: Readonly<Table.InferValue<SOURCE>>) => KEY;
  } & (
    | {
        map: (row: Readonly<Table.InferValue<SOURCE>>) => VALUE;
        filter?: (row: Readonly<Table.InferValue<SOURCE>>) => boolean;
      }
    | {
        map?: (row: Readonly<Table.InferValue<SOURCE>>) => VALUE;
        filter: (row: Readonly<Table.InferValue<SOURCE>>) => boolean;
      }
  );
}
