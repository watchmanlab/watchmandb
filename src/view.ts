import { listen } from "@watchmanlab/stream";
import { Table } from "./table";
import { Key, RootDatabase } from "lmdb";
import { isDeepEqual } from "./utils";

export class View<
  SOURCE extends Table.AnyTable,
  KEY extends Key,
  VALUE,
  TX extends boolean = false,
> extends Table<KEY, VALUE, TX> {
  constructor(private init: View.Init<SOURCE, KEY, VALUE>) {
    const { name, env, table } = init;
    super({ env, name: `${table.name}:${name}:view` });

    table.$beforeInsert.pipe(listen(({ curr }) => this._beforeInsert(curr)));
    table.$beforeUpdate.pipe(
      listen(({ curr, prev }) => this._beforeUpdate(curr, prev)),
    );
    table.$beforeDelete.pipe(listen(({ prev }) => this._beforeDelete(prev)));
  }

  private _beforeInsert(curr: Readonly<Table.InferValue<SOURCE>>) {
    if (this.init.filter && this.init.filter(curr) === false) return;

    const newKey = this.init.keySelector(curr);
    this.insert(newKey, this.init.map(curr));
  }
  private _beforeUpdate(
    curr: Readonly<Table.InferValue<SOURCE>>,
    prev: Readonly<Table.InferValue<SOURCE>>,
  ) {
    if (this.init.filter && this.init.filter(curr) === false) return;

    const oldKey = this.init.keySelector(prev);
    const newKey = this.init.keySelector(curr);

    if (isDeepEqual(oldKey, newKey)) return;

    this.delete(oldKey);
    this.insert(newKey, this.init.map(curr));
  }
  private _beforeDelete(prev: Readonly<Table.InferValue<SOURCE>>) {
    this.delete(this.init.keySelector(prev));
  }
}

export namespace View {
  export type Init<SOURCE extends Table.AnyTable, KEY extends Key, VALUE> = {
    name: string;
    env: RootDatabase;
    table: SOURCE;
    keySelector: (row: Readonly<Table.InferValue<SOURCE>>) => KEY;
    map: (row: Readonly<Table.InferValue<SOURCE>>) => VALUE;
    filter?: (row: Readonly<Table.InferValue<SOURCE>>) => boolean;
  };
}
