import { Key, RootDatabase } from "lmdb";
import { listen } from "@watchmanlab/stream";
import { isDeepEqual } from "./utils";
import { Table } from "./table";

export class Aggregate<
  TABLE extends Table.AnyTable,
  KEY extends Key,
  ACC,
  TX extends boolean = false,
> extends Table<never, KEY, ACC, TX> {
  constructor(private init: Aggregate.Init<TABLE, KEY, ACC>) {
    const { env, name, table } = init;
    super({ env, name: `agg:${table.name}:${name}` });

    this.init.seed = Object.freeze(init.seed);

    table.$beforeInsert.pipe(listen(({ curr }) => this._beforeInsert(curr)));
    table.$beforeUpdate.pipe(
      listen(({ curr, prev }) => this._beforeUpdate(curr, prev)),
    );
    table.$beforeDelete.pipe(listen(({ prev }) => this._beforeDelete(prev)));
  }

  private _beforeInsert(curr: Readonly<Table.InferValue<TABLE>>) {
    const currKey = this.init.groupBy(curr);
    const prevAcc = this.get(currKey);
    const currAcc = this.init.reducer(
      prevAcc ?? this.init.seed,
      curr,
      undefined,
    );

    this._commit(currKey, currAcc, prevAcc);
  }

  private _beforeUpdate(
    curr: Readonly<Table.InferValue<TABLE>>,
    prev: Readonly<Table.InferValue<TABLE>>,
  ) {
    const prevKey = this.init.groupBy(prev);
    const currKey = this.init.groupBy(curr);

    if (!isDeepEqual(prevKey, currKey)) {
      this._beforeDelete(prev);
      this._beforeInsert(curr);
      return;
    }

    const prevAcc = this.get(currKey);
    const currAcc = this.init.reducer(prevAcc ?? this.init.seed, curr, prev);

    this._commit(currKey, currAcc, prevAcc);
  }

  private _beforeDelete(prev: Readonly<Table.InferValue<TABLE>>) {
    const key = this.init.groupBy(prev);
    const prevAcc = this.get(key);
    if (!prevAcc) return;

    const currAcc = this.init.reducer(prevAcc, undefined, prev);

    this._commit(key, currAcc, prevAcc);
  }

  private _commit(key: KEY, currAcc: ACC, prevAcc?: ACC) {
    if (isDeepEqual(currAcc, this.init.seed)) {
      if (!prevAcc) return;
      this.delete(key);
      return;
    }

    if (!prevAcc) {
      this.insert(key, currAcc);
    } else {
      this.update(key, currAcc);
    }
  }
}

export namespace Aggregate {
  export type Init<TABLE extends Table.AnyTable, KEY extends Key, ACC> = {
    env: RootDatabase;
    table: TABLE;
    name: string;
    seed: Readonly<ACC>;
    groupBy: (row: Readonly<Readonly<Table.InferValue<TABLE>>>) => KEY;
    reducer: (
      acc: Readonly<ACC>,
      curr?: Readonly<Readonly<Table.InferValue<TABLE>>>,
      prev?: Readonly<Readonly<Table.InferValue<TABLE>>>,
    ) => Readonly<ACC>;
  };
}
