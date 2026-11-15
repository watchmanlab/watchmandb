import { Key, RootDatabase } from "lmdb";
import { listen } from "@watchmanlab/stream";

import { isDeepEqual } from "./utils";
import { Table } from "./table";

export class Aggregate<
  TABLE extends Table.AnyTable,
  KEY extends Key,
  VALUE,
  SOURCE_VALUE extends Table.InferValue<TABLE> = Table.InferValue<TABLE>,
> {
  private _table: Table<KEY, VALUE>;
  constructor(private init: Aggregate.Init<TABLE, KEY, VALUE, SOURCE_VALUE>) {
    const { env, name, table } = init;

    this._table = new Table({ env, name: `${table.name}:${name}` });

    this.init.seed = Object.freeze(init.seed);

    table.$beforeInsert.pipe(listen(({ curr }) => this._insert(curr)));
    table.$beforeUpdate.pipe(
      listen(({ curr, prev }) => this._update(curr, prev)),
    );
    table.$beforeDelete.pipe(listen(({ prev }) => this._delete(prev)));
  }

  private _insert(curr: SOURCE_VALUE) {
    const currKey = this.init.groupBy(curr);
    const prevAcc = this._table.get(currKey);
    const currAcc = this.init.reducer(
      prevAcc ?? this.init.seed,
      curr,
      undefined,
    );

    this._commit(currKey, currAcc, prevAcc);
  }

  private _update(curr: SOURCE_VALUE, prev: SOURCE_VALUE) {
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

  private _delete(prev: SOURCE_VALUE) {
    const key = this.init.groupBy(prev);
    const prevAcc = this._table.get(key);
    if (!prevAcc) return;

    const currAcc = this.init.reducer(prevAcc, undefined, prev);

    this._commit(key, currAcc, prevAcc);
  }

  private _commit(key: KEY, currAcc: VALUE, prevAcc?: VALUE) {
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
  get table(): Table<KEY, VALUE, false> {
    return this._table;
  }
  get(key: KEY): Readonly<VALUE> {
    return this._table.get(key) ?? this.init.seed;
  }
}

export namespace Aggregate {
  export type Init<
    TABLE extends Table.AnyTable,
    KEY extends Key,
    VALUE,
    SOURCE_VALUE extends Table.InferValue<TABLE> = Table.InferValue<TABLE>,
  > = {
    env: RootDatabase;
    table: TABLE;
    name: string;
    seed: Readonly<VALUE>;
    groupBy: (row: Readonly<SOURCE_VALUE>) => KEY;
    reducer: (
      acc: Readonly<VALUE>,
      curr?: Readonly<SOURCE_VALUE>,
      prev?: Readonly<SOURCE_VALUE>,
    ) => Readonly<VALUE>;
  };
}
