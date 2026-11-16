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
import { Key, RootDatabase } from "lmdb";
import { isDeepEqual } from "./utils";

export class Index<
  SOURCE extends Table.AnyTable,
  KEY extends Key,
  VALUE = KEY,
  SOURCE_KEY extends Table.InferKey<SOURCE> = Table.InferKey<SOURCE>,
  SOURCE_VALUE extends Table.InferValue<SOURCE> = Table.InferValue<SOURCE>,
> {
  private _table: Table<[KEY, Table.InferKey<SOURCE>], VALUE>;

  constructor(private init: Index.Init<SOURCE, KEY, VALUE>) {
    const { name, env, table } = init;

    this._table = new Table({ env, name: `${table.name}:${name}:index` });

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
  get table(): Table<[KEY, Table.InferKey<SOURCE>], VALUE, false> {
    return this._table;
  }

  private _makeKey(
    key: SOURCE_KEY,
    value: SOURCE_VALUE,
  ): [KEY, Table.InferKey<SOURCE>] {
    return [this.init.keySelector(value), key];
  }
  private _insert(key: SOURCE_KEY, curr: SOURCE_VALUE) {
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

    this._table.insert(
      newKey,
      this.init.valueSelector?.(curr) ?? (null as VALUE),
    );
  }
  private _update(key: SOURCE_KEY, curr: SOURCE_VALUE, prev: SOURCE_VALUE) {
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
    this._table.insert(
      newKey,
      this.init.valueSelector?.(curr) ?? (null as VALUE),
    );
  }
  private _delete(key: SOURCE_KEY, prev: SOURCE_VALUE) {
    this._table.delete(this._makeKey(key, prev));
  }

  equal(key: KEY): Source<VALUE> {
    return fromIterable(
      this._table["_db"].getRange({ start: [key], end: [key, "\xff"] }),
    ).pipe(map(({ key, value }) => value ?? key[1]));
  }
  notEqual(key: KEY): Source<VALUE> {
    const left = this.lessThan(key);
    const right = this.greaterThan(key);

    return of(left, right).pipe(flat$()) as never;
  }
  in(keys: [KEY, KEY, ...KEY[]]): Source<VALUE> {
    return fromIterable(keys)
      .pipe(map((key) => this.equal(key)))
      .pipe(flat$());
  }
  startWith(prefix: string): Source<VALUE> {
    const endPrefix =
      prefix.slice(0, -1) +
      String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1);

    return fromIterable(
      this._table["_db"].getRange({ start: prefix, end: endPrefix }),
    ).pipe(map(({ key, value }) => value ?? key[1]));
  }
  lessThan(key: KEY): Source<VALUE> {
    return fromIterable(this._table["_db"].getRange({ end: key })).pipe(
      map(({ key, value }) => value ?? key[1]),
    );
  }
  lessThanOrEqual(key: KEY): Source<VALUE> {
    return fromIterable(
      this._table["_db"].getRange({ end: [key, "\xff"] }),
    ).pipe(map(({ key, value }) => value ?? key[1]));
  }
  greaterThan(key: KEY): Source<VALUE> {
    return fromIterable(
      this._table["_db"].getRange({ start: [key, "\xff"] }),
    ).pipe(map(({ key, value }) => value ?? key[1]));
  }
  greaterThanOrEqual(key: KEY): Source<VALUE> {
    return fromIterable(this._table["_db"].getRange({ start: key })).pipe(
      map(({ key, value }) => value ?? key[1]),
    );
  }
  between(start: KEY, end: KEY): Source<VALUE> {
    return fromIterable(
      this._table["_db"].getRange({ start: [start], end: [end, "\xff"] }),
    ).pipe(map(({ key, value }) => value ?? key[1]));
  }
}

export namespace Index {
  export type AnyIndex = Index<any, any, any>;
  export type InferKey<T extends AnyIndex> =
    T extends Index<any, infer K, any> ? K : never;
  export type Init<SOURCE extends Table.AnyTable, KEY extends Key, VALUE> = {
    name: string;
    env: RootDatabase;
    table: SOURCE;
    keySelector: (row: Table.InferValue<SOURCE>) => KEY;
    valueSelector?: (row: Table.InferValue<SOURCE>) => VALUE;
    unique?: boolean;
  };
}
