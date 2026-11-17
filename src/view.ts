import { fromIterable, listen, map, Source } from "@watchmanlab/stream";
import { Table } from "./table";
import { Database, Key, RootDatabase } from "lmdb";
import { isDeepEqual } from "./utils";

export class View<
  TABLE extends Table.AnyTable,
  ID extends Key = Table.InferId<TABLE>,
  ROW = Table.InferRow<TABLE>,
> {
  private _name: string;
  private _db: Database;
  private _idSelector?: (row: Readonly<Table.InferRow<TABLE>>) => ID;
  private _map?: (row: Readonly<Table.InferRow<TABLE>>) => ROW;
  private _filter?: (row: Readonly<Table.InferRow<TABLE>>) => boolean;
  constructor(init: View.Init<TABLE, ID, ROW>) {
    const { name, env, table, idSelector, map, filter } = init;
    this._name = `view:${table.name}:${name}`;
    this._db = env.openDB({ name: this._name });
    this._idSelector = idSelector;
    this._map = map;
    this._filter = filter;

    table.$beforeInsert.pipe(
      listen(({ id, curr }) => this._beforeInsert(id, curr)),
    );
    table.$beforeUpdate.pipe(
      listen(({ id, curr, prev }) => this._beforeUpdate(id, curr, prev)),
    );
    table.$beforeDelete.pipe(
      listen(({ id, prev }) => this._beforeDelete(id, prev)),
    );
  }
  get(id: ID) {
    return this._db.get(id);
  }
  all(): Source<ROW> {
    return fromIterable(this._db.getRange()).pipe(map((e) => e.value));
  }

  private _beforeInsert(
    key: Table.InferId<TABLE>,
    curr: Readonly<Table.InferRow<TABLE>>,
  ) {
    if (this._filter?.(curr) === false) return;

    const newKey = this._idSelector?.(curr) ?? key;
    this._db.put(newKey, this._map?.(curr) ?? curr);
  }
  private _beforeUpdate(
    key: Table.InferId<TABLE>,
    curr: Readonly<Table.InferRow<TABLE>>,
    prev: Readonly<Table.InferRow<TABLE>>,
  ) {
    if (this._filter?.(curr) === false) return;

    const oldKey = this._idSelector?.(prev) ?? key;
    const newKey = this._idSelector?.(curr) ?? key;

    if (isDeepEqual(oldKey, newKey)) return;

    this._db.remove(oldKey);
    this._db.put(newKey, this._map?.(curr) ?? curr);
  }
  private _beforeDelete(
    key: Table.InferId<TABLE>,
    prev: Readonly<Table.InferRow<TABLE>>,
  ) {
    this._db.remove(this._idSelector?.(prev) ?? key);
  }
}

// export function defineView<
//   TABLE_DEF extends Table.Definition<any>,
//   ID extends Key,
//   ROW,
// >(def: View.Definition<TABLE_DEF, ID, ROW>) {
//   return def;
// }

export namespace View {
  export type Init<TABLE extends Table.AnyTable, ID extends Key, ROW> = {
    name: string;
    env: RootDatabase;
    table: TABLE;
    idSelector?: (row: Readonly<Table.InferRow<TABLE>>) => ID;
  } & (
    | {
        map: (row: Readonly<Table.InferRow<TABLE>>) => ROW;
        filter?: (row: Readonly<Table.InferRow<TABLE>>) => boolean;
      }
    | {
        map?: (row: Readonly<Table.InferRow<TABLE>>) => ROW;
        filter: (row: Readonly<Table.InferRow<TABLE>>) => boolean;
      }
  );
  export type Definition<
    TABLE_DEF extends Table.AnyDefinition,
    ID extends Key,
    ROW,
  > = Omit<Init<Table.AnyTable, ID, ROW>, "env" | "table"> & {
    table: TABLE_DEF;
  };
}
