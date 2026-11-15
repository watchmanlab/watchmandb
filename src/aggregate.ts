import { Key, RootDatabase } from "lmdb";
import { listen } from "@watchmanlab/stream";

import { isDeepEqual } from "./utils";
import { Table } from "./table";

export class Aggregate<
  TABLE extends Table.AnyTable,
  ROW extends Record<string, any>,
  KEY extends Key,
  SOURCE_ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
> {
  private _table: Table<ROW, KEY>;
  constructor(private init: Aggregate.Init<TABLE, ROW, KEY, SOURCE_ROW>) {
    const { env, name, table } = init;

    this._table = new Table({ env, name: `${table.name}:${name}` });

    this.init.seed = Object.freeze(init.seed);

    table.$beforeInsert.pipe(listen(({ curr }) => this._insert(curr)));
    table.$beforeUpdate.pipe(
      listen(({ curr, prev }) => this._update(curr, prev)),
    );
    table.$beforeDelete.pipe(listen(({ prev }) => this._delete(prev)));
  }

  private _insert(curr: SOURCE_ROW) {
    const currKey = this.init.groupBy(curr);
    const prevAcc = this._table.get(currKey);
    const currAcc = this.init.reducer(
      prevAcc ?? this.init.seed,
      curr,
      undefined,
    );

    this._commit(currKey, currAcc, prevAcc);
  }

  private _update(curr: SOURCE_ROW, prev: SOURCE_ROW) {
    const prevKey = this.init.groupBy(prev);
    const currKey = this.init.groupBy(curr);

    if (!isDeepEqual(prevKey, currKey)) {
      this._delete(prev);
      this._insert(curr);
      return;
    }

    const prevAcc = this._table.get(currKey);
    const currAcc = this.init.reducer(prevAcc ?? this.init.seed, curr, prev);

    this._commit(currKey, currAcc, prevAcc);
  }

  private _delete(prev: SOURCE_ROW) {
    const key = this.init.groupBy(prev);
    const prevAcc = this._table.get(key);
    if (!prevAcc) return;

    const currAcc = this.init.reducer(prevAcc, undefined, prev);

    this._commit(key, currAcc, prevAcc);
  }

  private _commit(key: KEY, currAcc: ROW, prevAcc?: ROW) {
    if (isDeepEqual(currAcc, this.init.seed)) {
      if (!prevAcc) return;

      this._table.delete(key);
      return;
    }

    if (!prevAcc) {
      this._table.insert(key, currAcc);
    } else {
      this._table.update(key, currAcc);
    }
  }
  get table(): Table<ROW, KEY, false> {
    return this._table;
  }
  get(key: KEY): Readonly<ROW> {
    return this._table.get(key) ?? this.init.seed;
  }
}

export namespace Aggregate {
  export type Init<
    TABLE extends Table.AnyTable,
    ROW extends Record<string, any>,
    KEY extends Key,
    SOURCE_ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
  > = {
    env: RootDatabase;
    table: TABLE;
    name: string;
    seed: Readonly<ROW>;
    groupBy: (row: Readonly<SOURCE_ROW>) => KEY;
    reducer: (
      acc: Readonly<ROW>,
      curr?: Readonly<SOURCE_ROW>,
      prev?: Readonly<SOURCE_ROW>,
    ) => Readonly<ROW>;
  };
}
