import { Database, Key, RootDatabase } from "lmdb";
import { Table } from "./table";
import { fromIterable, listen } from "@watchmanlab/stream";

export class Aggregate<
  TABLE extends Table.AnyTable,
  KEY extends Key,
  VALUE,
  ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
> {
  private _name: string;
  private _db: Database;
  private _table: TABLE;
  constructor(config: {
    env: RootDatabase;
    table: TABLE;
    name: string;
    seed: VALUE;
    groupBy: (row: ROW) => KEY;
    reducer: (acc: VALUE, curr?: ROW, prev?: ROW) => VALUE;
  }) {
    const { env, groupBy, name, reducer, seed, table } = config;
    this._table = table;
    this._name = `${table.name}:${name}`;
    this._db = env.openDB({ name: this._name });

    this._table.$beforeInsert.pipe(
      listen((curr) => {
        const key = groupBy(curr);
        const acc = this._db.get(key) ?? seed;
        this._db.put(groupBy(curr), reducer(acc, curr));
      }),
    );
    this._table.$beforeUpdate.pipe(
      listen(({ curr, prev }) => {
        const key = groupBy(prev);
        const acc = this._db.get(key);
        this._db.put(key, reducer(acc, curr, prev));
      }),
    );
    this._table.$beforeDelete.pipe(
      listen((prev) => {
        const key = groupBy(prev);
        const acc = this._db.get(key);
        this._db.put(key, reducer(acc, undefined, prev));
      }),
    );
  }

  all() {
    return fromIterable(this._db.getRange());
  }
}
