import {
  flat$,
  fromIterable,
  listen,
  map,
  of,
  Source,
} from "@watchmanlab/stream";
import { Table } from "./table";
import { Key, RootDatabase } from "lmdb";
import { isDeepEqual } from "./utils";

export class Index<
  SOURCE extends Table.AnyTable,
  KEY extends Key,
  VALUE = KEY,
  TX extends boolean = false,
> extends Table<[KEY, Table.InferKey<SOURCE>], VALUE> {
  constructor(private init: Index.Init<SOURCE, KEY, VALUE>) {
    const { name, env, table } = init;
    super({ env, name: `index:${table.name}:${name}` });

    table.$beforeInsert.pipe(
      listen(({ key, curr }) => this._beforeInsert(key, curr)),
    );
    table.$beforeUpdate.pipe(
      listen(({ key, curr, prev }) => this._beforeUpdate(key, curr, prev)),
    );
    table.$beforeDelete.pipe(
      listen(({ key, prev }) => this._beforeDelete(key, prev)),
    );
  }

  private _makeKey(
    key: Table.InferKey<SOURCE>,
    value: Readonly<Table.InferValue<SOURCE>>,
  ): [KEY, Table.InferKey<SOURCE>] {
    return [this.init.keySelector(value), key];
  }
  private _beforeInsert(
    key: Table.InferKey<SOURCE>,
    curr: Readonly<Table.InferValue<SOURCE>>,
  ) {
    const newKey = this._makeKey(key, curr);

    if (this.init.unique) {
      const [colVal] = newKey;

      this.eq(colVal as KEY)
        .consume((self) => {
          self.terminate("abort");
          throw new Error(`duplicate key ${String(colVal)} at ${this._name}`);
        })
        .next();
    }

    this.insert(newKey, this.init.includes?.(curr) ?? (null as VALUE));
  }
  private _beforeUpdate(
    key: Table.InferKey<SOURCE>,
    curr: Readonly<Table.InferValue<SOURCE>>,
    prev: Readonly<Table.InferValue<SOURCE>>,
  ) {
    const oldKey = this._makeKey(key, prev);
    const newKey = this._makeKey(key, curr);

    if (isDeepEqual(oldKey, newKey)) return;

    if (this.init.unique) {
      const [colVal, rowId] = newKey;

      this.eq(colVal as KEY)
        .consume((self, prevId) => {
          if (prevId !== rowId) {
            self.terminate("abort");
            throw new Error(`duplicate key ${String(colVal)} at ${this._name}`);
          }
          self.next();
        })
        .next();
    }

    this.delete(oldKey);
    this.insert(newKey, this.init.includes?.(curr) ?? (null as VALUE));
  }
  private _beforeDelete(
    key: Table.InferKey<SOURCE>,
    prev: Readonly<Table.InferValue<SOURCE>>,
  ) {
    this.delete(this._makeKey(key, prev));
  }

  eq(key: KEY): Source<VALUE> {
    return fromIterable(
      this._db.getRange({ start: [key], end: [key, "\xff"] }),
    ).pipe(map(({ key, value }) => value ?? key[1]));
  }
  neq(key: KEY): Source<VALUE> {
    const left = this.lt(key);
    const right = this.gt(key);

    return of(left, right).pipe(flat$()) as never;
  }
  in(keys: [KEY, KEY, ...KEY[]]): Source<VALUE> {
    return fromIterable(keys)
      .pipe(map((key) => this.eq(key)))
      .pipe(flat$());
  }
  startWith(prefix: string): Source<VALUE> {
    const endPrefix =
      prefix.slice(0, -1) +
      String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1);

    return fromIterable(
      this._db.getRange({ start: prefix, end: endPrefix }),
    ).pipe(map(({ key, value }) => value ?? key[1]));
  }
  lt(key: KEY): Source<VALUE> {
    return fromIterable(this._db.getRange({ end: key })).pipe(
      map(({ key, value }) => value ?? key[1]),
    );
  }
  lte(key: KEY): Source<VALUE> {
    return fromIterable(this._db.getRange({ end: [key, "\xff"] })).pipe(
      map(({ key, value }) => value ?? key[1]),
    );
  }
  gt(key: KEY): Source<VALUE> {
    return fromIterable(this._db.getRange({ start: [key, "\xff"] })).pipe(
      map(({ key, value }) => value ?? key[1]),
    );
  }
  gte(key: KEY): Source<VALUE> {
    return fromIterable(this._db.getRange({ start: key })).pipe(
      map(({ key, value }) => value ?? key[1]),
    );
  }
  between(start: KEY, end: KEY): Source<VALUE> {
    return fromIterable(
      this._db.getRange({ start: [start], end: [end, "\xff"] }),
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
    keySelector: (row: Readonly<Table.InferValue<SOURCE>>) => KEY;
    includes?: (row: Readonly<Table.InferValue<SOURCE>>) => VALUE;
    unique?: boolean;
  };
}
