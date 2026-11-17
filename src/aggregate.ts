import { Database, Key, RootDatabase } from "lmdb";
import { listen } from "@watchmanlab/stream";
import { isDeepEqual } from "./utils";
import { Table } from "./table";

export class Aggregate<TABLE extends Table.AnyTable, KEY extends Key, ACC> {
  private _name: string;
  private _db: Database;
  private _seed: Readonly<ACC>;
  private _groupBy: (row: Readonly<Readonly<Table.InferRow<TABLE>>>) => KEY;
  private _reducer: (
    acc: Readonly<ACC>,
    curr?: Readonly<Readonly<Table.InferRow<TABLE>>>,
    prev?: Readonly<Readonly<Table.InferRow<TABLE>>>,
  ) => Readonly<ACC>;
  constructor(init: Aggregate.Init<TABLE, KEY, ACC>) {
    const { env, name, table, seed, groupBy, reducer } = init;
    this._name = `agg:${table.name}:${name}`;
    this._db = env.openDB({ name: this._name });
    this._seed = Object.freeze(init.seed);
    this._groupBy = groupBy;
    this._reducer = reducer;

    table.$beforeInsert.pipe(listen(({ curr }) => this._beforeInsert(curr)));
    table.$beforeUpdate.pipe(
      listen(({ curr, prev }) => this._beforeUpdate(curr, prev)),
    );
    table.$beforeDelete.pipe(listen(({ prev }) => this._beforeDelete(prev)));
  }

  private _beforeInsert(curr: Readonly<Table.InferRow<TABLE>>) {
    const currKey = this._groupBy(curr);
    const prevAcc = this._db.get(currKey);
    const currAcc = this._reducer(prevAcc ?? this._seed, curr, undefined);

    this._commit(currKey, currAcc, prevAcc);
  }

  private _beforeUpdate(
    curr: Readonly<Table.InferRow<TABLE>>,
    prev: Readonly<Table.InferRow<TABLE>>,
  ) {
    const prevKey = this._groupBy(prev);
    const currKey = this._groupBy(curr);

    if (!isDeepEqual(prevKey, currKey)) {
      this._beforeDelete(prev);
      this._beforeInsert(curr);
      return;
    }

    const prevAcc = this._db.get(currKey);
    const currAcc = this._reducer(prevAcc ?? this._seed, curr, prev);

    this._commit(currKey, currAcc, prevAcc);
  }

  private _beforeDelete(prev: Readonly<Table.InferRow<TABLE>>) {
    const key = this._groupBy(prev);
    const prevAcc = this._db.get(key);
    if (!prevAcc) return;

    const currAcc = this._reducer(prevAcc, undefined, prev);

    this._commit(key, currAcc, prevAcc);
  }

  private _commit(key: KEY, currAcc: ACC, prevAcc?: ACC) {
    if (isDeepEqual(currAcc, this._seed)) {
      if (!prevAcc) return;
      this._db.remove(key);
      return;
    }

    this._db.put(key, currAcc);
  }
}

export namespace Aggregate {
  export type Init<TABLE extends Table.AnyTable, KEY extends Key, ACC> = {
    name: string;
    env: RootDatabase;
    table: TABLE;
    seed: Readonly<ACC>;
    groupBy: (row: Readonly<Readonly<Table.InferRow<TABLE>>>) => KEY;
    reducer: (
      acc: Readonly<ACC>,
      curr?: Readonly<Readonly<Table.InferRow<TABLE>>>,
      prev?: Readonly<Readonly<Table.InferRow<TABLE>>>,
    ) => Readonly<ACC>;
  };
}
