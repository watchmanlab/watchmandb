import { Key, RootDatabase } from "lmdb";
import { listen } from "@watchmanlab/stream";
import { SourceTable } from "./source-table";
import { isDeepEqual } from "./utils";

export class Aggregate<
  TABLE extends SourceTable.AnySourceTable,
  VALUE extends Record<string, any>,
  KEY extends Key,
  ROW extends SourceTable.InferRow<TABLE> = SourceTable.InferRow<TABLE>,
> extends SourceTable<VALUE, KEY> {
  constructor(private init: Aggregate.Init<TABLE, VALUE, KEY, ROW>) {
    const { env, name, table } = init;

    super({ env, name: `${table.name}:${name}` });

    this.init.seed = Object.freeze(init.seed);

    table.$beforeInsert.pipe(listen((curr) => this._insert(curr)));
    table.$beforeUpdate.pipe(
      listen(({ curr, prev }) => this._update(curr, prev)),
    );
    table.$beforeDelete.pipe(listen((prev) => this._delete(prev)));
  }

  protected _insert(curr: ROW) {
    const currKey = this.init.groupBy(curr);
    const prevAcc = this._db.get(currKey);
    const currAcc = this.init.reducer(
      prevAcc ?? this.init.seed,
      curr,
      undefined,
    );

    this._commit(currKey, currAcc, prevAcc);
  }

  protected _update(curr: ROW, prev: ROW) {
    const prevKey = this.init.groupBy(prev);
    const currKey = this.init.groupBy(curr);

    if (!isDeepEqual(prevKey, currKey)) {
      this._delete(prev);
      this._insert(curr);
      return;
    }

    const prevAcc = this._db.get(currKey);
    const currAcc = this.init.reducer(prevAcc ?? this.init.seed, curr, prev);

    this._commit(currKey, currAcc, prevAcc);
  }

  protected _delete(prev: ROW) {
    const key = this.init.groupBy(prev);
    const prevAcc = this._db.get(key);
    if (!prevAcc) return;

    const currAcc = this.init.reducer(prevAcc, undefined, prev);

    this._commit(key, currAcc, prevAcc);
  }

  private _commit(key: KEY, currAcc: VALUE, prevAcc?: VALUE) {
    if (isDeepEqual(currAcc, this.init.seed)) {
      if (!prevAcc) return;

      this._$beforeDelete?.push(prevAcc);

      this._db.remove(key).then((r) => {
        if (!r) return;
        this._$afterDelete?.push(prevAcc);
      });
      return;
    }

    if (!prevAcc) {
      this._$beforeInsert?.push(currAcc);

      this._db.put(key, currAcc).then((r) => {
        if (!r) return;
        this._$afterInsert?.push(currAcc);
      });
    } else {
      this._$beforeUpdate?.push({ curr: currAcc, prev: prevAcc });

      this._db.put(key, currAcc).then((r) => {
        if (!r) return;
        this._$afterUpdate?.push({ curr: currAcc, prev: prevAcc });
      });
    }
  }
  override get(key: KEY): Readonly<VALUE> {
    return this._db.get(key) ?? this.init.seed;
  }
}

export namespace Aggregate {
  export type Init<
    TABLE extends SourceTable.AnySourceTable,
    VALUE extends Record<string, any>,
    KEY extends Key,
    ROW extends SourceTable.InferRow<TABLE> = SourceTable.InferRow<TABLE>,
  > = {
    env: RootDatabase;
    table: TABLE;
    name: string;
    seed: Readonly<VALUE>;
    groupBy: (row: Readonly<ROW>) => KEY;
    reducer: (
      acc: Readonly<VALUE>,
      curr?: Readonly<ROW>,
      prev?: Readonly<ROW>,
    ) => Readonly<VALUE>;
  };
}
