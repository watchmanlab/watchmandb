import {
  flat$,
  fromIterable,
  listen,
  map,
  of,
  Source,
  Stream,
} from "@watchmanlab/stream";
import { Table } from "./table";
import { Database, Key, RootDatabase } from "lmdb";
import { isDeepEqual } from "./utils";

export class Index<
  SOURCE extends Table.AnyTable,
  KEY extends keyof Table.InferRow<SOURCE>,
  SOURCE_ROW extends Table.InferRow<SOURCE> = Table.InferRow<SOURCE>,
  SOURCE_KEY extends Table.InferKey<SOURCE> = Table.InferKey<SOURCE>,
> {
  private _table: Table<{}, [KEY, Table.InferKey<SOURCE>]>;

  constructor(private init: Index.Init<SOURCE, KEY>) {
    const { name, env, table } = init;

    this._table = new Table({ env, name: `${table.name}:${name}` });

    this._init();
    table.$beforeInsert.pipe(
      listen(({ key, curr }) => this._insert(key, curr)),
    );
    table.$beforeUpdate.pipe(
      listen(({ key, curr, prev }) => this._update(key, curr, prev)),
    );
    table.$beforeDelete.pipe(
      listen(({ key, prev }) => this._delete(key, prev)),
    );
  }

  private _init() {
    // const { _table } = this;
    // if (!_table.length || !this._db.getRange()[Symbol.iterator]().next().done)
    //   return;
    // this._db.transaction(() =>
    //   _table.all().pipe(listen((row) => this._insert(row))),
    // );
  }
  private _makeKey(
    key: SOURCE_KEY,
    row: SOURCE_ROW,
  ): [KEY, Table.InferKey<SOURCE>] {
    return [this.init.keySelector(row), key];
  }
  private _insert(key: SOURCE_KEY, curr: SOURCE_ROW) {
    const newKey = this._makeKey(key, curr);

    if (this.init.unique) {
      const [colVal] = newKey;

      this.equal(colVal as KEY)
        .consume((self) => {
          self.terminate("abort");
          throw new Error(
            `duplicate key ${String(colVal)} at ${this._table.name}`,
          );
        })
        .next();
    }

    this._table.insert(newKey, {});
  }
  private _update(key: SOURCE_KEY, curr: SOURCE_ROW, prev: SOURCE_ROW) {
    const oldKey = this._makeKey(key, prev);
    const newKey = this._makeKey(key, curr);

    if (isDeepEqual(oldKey, newKey)) return;

    if (this.init.unique) {
      const [colVal, rowId] = newKey;

      this.equal(colVal as KEY)
        .consume((self, prevId) => {
          if (prevId !== rowId) {
            self.terminate("abort");
            throw new Error(
              `duplicate key ${String(colVal)} at ${this._table.name}`,
            );
          }
          self.next();
        })
        .next();
    }

    this._table.delete(oldKey);
    this._table.insert(newKey, {});
  }
  private _delete(key: SOURCE_KEY, prev: SOURCE_ROW) {
    this._table.delete(this._makeKey(key, prev));
  }

  equal(key: SOURCE_ROW[KEY]): Source<SOURCE_KEY> {
    return fromIterable(
      this._table["_db"].getKeys({ start: [key], end: [key, "\xff"] }),
    ).pipe(map((key) => key[1]));
  }
  notEqual(key: SOURCE_ROW[KEY]) {
    const left = this.lessThan(key);
    const right = this.greaterThan(key);

    return of(left, right).pipe(flat$());
  }
  in(keys: [SOURCE_ROW[KEY], SOURCE_ROW[KEY], ...SOURCE_ROW[KEY][]]) {
    return fromIterable(keys)
      .pipe(map((key) => this.equal(key)))
      .pipe(flat$());
  }
  startWith(prefix: SOURCE_ROW[KEY]): Source<SOURCE_KEY> {
    const endPrefix =
      prefix.slice(0, -1) +
      String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1);

    return fromIterable(
      this._table["_db"].getKeys({ start: prefix, end: endPrefix }),
    ).pipe(map((key) => key[1]));
  }
  lessThan(key: SOURCE_ROW[KEY]): Source<SOURCE_KEY> {
    return fromIterable(this._table["_db"].getKeys({ end: key })).pipe(
      map((key) => key[1]),
    );
  }
  lessThanOrEqual(key: SOURCE_ROW[KEY]): Source<SOURCE_KEY> {
    return fromIterable(
      this._table["_db"].getKeys({ end: [key, "\xff"] }),
    ).pipe(map((key) => key[1]));
  }
  greaterThan(key: SOURCE_ROW[KEY]): Source<SOURCE_KEY> {
    return fromIterable(
      this._table["_db"].getKeys({ start: [key, "\xff"] }),
    ).pipe(map((key) => key[1]));
  }
  greaterThanOrEqual(key: SOURCE_ROW[KEY]): Source<SOURCE_KEY> {
    return fromIterable(this._table["_db"].getKeys({ start: key })).pipe(
      map((key) => key[1]),
    );
  }
  between(start: SOURCE_ROW[KEY], end: SOURCE_ROW[KEY]): Source<SOURCE_KEY> {
    return fromIterable(
      this._table["_db"].getKeys({ start: [start], end: [end, "\xff"] }),
    ).pipe(map((key) => key[1]));
  }
}

export namespace Index {
  export type AnyIndex = Index<any, any, any>;
  export type InferKey<T extends AnyIndex> =
    T extends Index<any, infer K, any> ? K : never;
  export type Init<
    SOURCE extends Table.AnyTable,
    KEY extends keyof Table.InferRow<SOURCE>,
  > = {
    name: string;
    env: RootDatabase;
    table: SOURCE;
    keySelector: (curr: Table.InferRow<SOURCE>) => KEY;
    unique?: boolean;
  };
}
