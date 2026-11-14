import { Key, RootDatabase } from "lmdb";
import { Table } from "./table";
import { listen } from "@watchmanlab/stream";
import { SourceTable } from "./source-table";

export class Aggregate<
  TABLE extends Table.AnyTable,
  VALUE extends Record<string, any>,
  KEY extends Key,
  ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
> extends SourceTable<VALUE, KEY> {
  constructor(private init: Aggregate.Init<TABLE, VALUE, KEY, ROW>) {
    const { env, name, table } = init;
    super({ env, name: `${table.name}:${name}` });

    table.$beforeInsert.pipe(
      listen((curr) => {
        const key = this.init.groupBy(curr);
        const prevAcc = this._db.get(key);
        const currAcc = this.init.reducer(prevAcc ?? this.init.seed, curr);
        if (!prevAcc) {
          this._insert(key, currAcc);
        } else {
          this._update(key, currAcc, prevAcc);
        }
      }),
    );
    table.$beforeUpdate.pipe(
      listen(({ curr, prev }) => {
        this._delete(prev);

        const currKey = this.init.groupBy(curr);
        const prevAcc = this._db.get(currKey);
        const currAcc = this.init.reducer(
          prevAcc ?? this.init.seed,
          curr,
          prev,
        );
        if (!prevAcc) {
          this._insert(currKey, currAcc);
        } else {
          this._update(currKey, currAcc, prevAcc);
        }
      }),
    );
    table.$beforeDelete.pipe(listen((prev) => this._delete(prev)));
  }

  private _insert(key: KEY, acc: VALUE) {
    this._$beforeInsert?.push(acc);

    this._db.put(key, acc).then((r) => {
      if (!r) return;
      this._$afterInsert?.push(acc);
    });
  }

  private _update(key: KEY, currAcc: VALUE, prevAcc: VALUE) {
    this._$beforeUpdate?.push({ curr: currAcc, prev: prevAcc });

    this._db.put(key, currAcc).then((r) => {
      if (!r) return;
      this._$afterUpdate?.push({ curr: currAcc, prev: prevAcc });
    });
  }
  private _delete(prev: ROW) {
    const key = this.init.groupBy(prev);
    const prevAcc = this._db.get(key);
    if (!prevAcc) return;

    const currAcc = this.init.reducer(prevAcc, undefined, prev);

    this._$beforeDelete?.push(currAcc);

    this._db.put(key, currAcc).then((r) => {
      if (!r) return;
      this._$afterDelete?.push(currAcc);
    });
  }
}

export namespace Aggregate {
  export type Init<
    TABLE extends Table.AnyTable,
    VALUE extends Record<string, any>,
    KEY extends Key,
    ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
  > = {
    env: RootDatabase;
    table: TABLE;
    name: string;
    seed: VALUE;
    groupBy: (row: ROW) => KEY;
    reducer: (acc: VALUE, curr?: ROW, prev?: ROW) => VALUE;
  };
}
