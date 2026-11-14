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

    table.$beforeInsert.pipe(listen((curr) => this._upsert(curr)));
    table.$beforeUpdate.pipe(
      listen(({ curr, prev }) => this._upsert(curr, prev)),
    );
    table.$beforeDelete.pipe(listen((prev) => this._delete(prev)));
  }

  private _upsert(curr: ROW, prev?: ROW) {
    const key = this.init.groupBy(curr);
    const prevAcc = this._db.get(key);
    const currAcc = this.init.reducer(prevAcc ?? this.init.seed, curr, prev);

    if (prevAcc) {
      this._$beforeUpdate?.push({ curr: currAcc, prev: prevAcc });

      this._db.put(key, currAcc).then((r) => {
        if (!r) return;
        this._$afterUpdate?.push({ curr: currAcc, prev: prevAcc });
      });
    } else {
      this._$beforeInsert?.push(currAcc);

      this._db.put(key, currAcc).then((r) => {
        if (!r) return;
        this._$afterInsert?.push(currAcc);
      });
    }
  }
  private _delete(prev: ROW) {
    const key = this.init.groupBy(prev);
    const prevAcc = this._db.get(key);
    if (!prevAcc) return;

    this._$beforeDelete?.push(prevAcc);

    this._db.put(key, this.init.reducer(prevAcc, undefined, prev)).then((r) => {
      if (!r) return;
      this._$afterDelete?.push(prevAcc);
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
