import {
  flat$,
  fromIterable,
  listen,
  map,
  of,
  Source,
} from "@watchmanlab/stream";
import { Table } from "./table";
import { Database, Key, RootDatabase } from "lmdb";
import { isDeepEqual } from "./utils";

export class Index<TABLE extends Table.AnyTable, KEY extends Key, VALUE = KEY> {
  private _name: string;
  private _db: Database<any, [KEY, Table.InferId<TABLE>]>;
  constructor(private init: Index.Init<TABLE, KEY, VALUE>) {
    const { name, env, table } = init;
    this._name = `index:${table.name}:${name}`;
    this._db = env.openDB({ name: this._name });

    table.$beforeUpsert.pipe(
      listen(({ id, row }) => this._beforeInsert(id, row)),
    );
    table.$beforeUpdate.pipe(
      listen(({ id, row, row }) => this._beforeUpdate(id, row, row)),
    );
    table.$beforeDelete.pipe(
      listen(({ id, row }) => this._beforeDelete(id, row)),
    );
  }
  get name(): string {
    return this._name;
  }
  private _makeKey(
    key: Table.InferId<TABLE>,
    value: Readonly<Table.InferOutput<TABLE>>,
  ): [KEY, Table.InferId<TABLE>] {
    return [this.init.keySelector(value), key];
  }
  private _beforeInsert(
    key: Table.InferId<TABLE>,
    row: Readonly<Table.InferOutput<TABLE>>,
  ) {
    const newKey = this._makeKey(key, row);

    if (this.init.unique) {
      const [colVal] = newKey;

      this.eq(colVal as KEY)
        .consume((self) => {
          self.terminate("abort");
          throw new Error(`duplicate key ${String(colVal)} at ${this._name}`);
        })
        .next();
    }

    this._db.put(newKey, this.init.includes?.(row) ?? (null as VALUE));
  }
  private _beforeUpdate(
    key: Table.InferId<TABLE>,
    row: Readonly<Table.InferOutput<TABLE>>,
    row: Readonly<Table.InferOutput<TABLE>>,
  ) {
    const oldKey = this._makeKey(key, row);
    const newKey = this._makeKey(key, row);

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

    this._db.remove(oldKey);
    this._db.put(newKey, this.init.includes?.(row) ?? (null as VALUE));
  }
  private _beforeDelete(
    key: Table.InferId<TABLE>,
    row: Readonly<Table.InferOutput<TABLE>>,
  ) {
    this._db.remove(this._makeKey(key, row));
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
  export type InferId<T extends AnyIndex> =
    T extends Index<any, infer K, any> ? K : never;
  export type Init<TABLE extends Table.AnyTable, KEY extends Key, VALUE> = {
    name: string;
    env: RootDatabase;
    table: TABLE;
    keySelector: (row: Readonly<Table.InferOutput<TABLE>>) => KEY;
    includes?: (row: Readonly<Table.InferOutput<TABLE>>) => VALUE;
    unique?: boolean;
  };
}
