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
  TABLE extends Table.AnyTable,
  COLUMN extends keyof Table.InferRow<TABLE>,
  ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
> {
  private _name: string;
  private _db: Database<any, [COLUMN, Table.InferPrimaryKey<TABLE>]>;
  private _table: TABLE;
  private _key: COLUMN;
  private _unique?: boolean;

  constructor(init: Index.Init<TABLE, COLUMN>) {
    const { name, env, table, key, unique } = init;

    this._name = `${table.name}:${name}`;
    this._db = env.openDB({ name: this._name });
    this._table = table;
    this._key = key;
    this._unique = unique;
    this._init();
    table.$beforeInsert.pipe(listen((newRow) => this._insert(newRow)));
    table.$beforeUpdate.pipe(
      listen(({ curr, prev }) => this._update(curr, prev)),
    );
    table.$beforeDelete.pipe(listen((oldRow) => this._delete(oldRow)));
  }
  get name(): string {
    return this._name;
  }
  private _init() {
    const { _table } = this;
    if (!_table.length || !this._db.getRange()[Symbol.iterator]().next().done)
      return;
    this._db.transaction(() =>
      _table.all().pipe(listen((row) => this._insert(row))),
    );
  }
  private _makeKey(row: ROW): [COLUMN, Table.InferPrimaryKey<TABLE>] {
    return [
      row[this._key],
      this._table.primaryKey.map((k) => row[k]) as Table.InferPrimaryKey<TABLE>,
    ];
  }
  private _insert(newRow: ROW) {
    const newKey = this._makeKey(newRow);

    if (this._unique) {
      const [colVal] = newKey;

      this.equal(colVal as COLUMN)
        .consume((self) => {
          self.terminate("abort");
          throw new Error(`duplicate key ${String(colVal)} at ${this._name}`);
        })
        .next();
    }

    this._db.put(newKey, null);
  }
  private _update(newRow: ROW, oldRow: ROW) {
    const oldKey = this._makeKey(oldRow);
    const newKey = this._makeKey(newRow);

    if (isDeepEqual(oldKey, newKey)) return;

    if (this._unique) {
      const [colVal, rowId] = newKey;

      this.equal(colVal as COLUMN)
        .consume((self, oldRowId) => {
          if (oldRowId !== rowId) {
            self.terminate("abort");
            throw new Error(`duplicate key ${String(colVal)} at ${this._name}`);
          }
          self.next();
        })
        .next();
    }

    this._db.remove(oldKey);
    this._db.put(newKey, null);
  }
  private _delete(oldRow: ROW) {
    this._db.remove(this._makeKey(oldRow));
  }

  equal(key: ROW[COLUMN]): Source<Table.InferPrimaryKey<TABLE>> {
    return fromIterable(
      this._db.getKeys({ start: [key], end: [key, "\xff"] }),
    ).pipe(map((key) => key[1]));
  }
  notEqual(key: ROW[COLUMN]) {
    const left = this.lessThan(key);
    const right = this.greaterThan(key);

    return of(left, right).pipe(flat$());
  }
  in(keys: [ROW[COLUMN], ROW[COLUMN], ...ROW[COLUMN][]]) {
    return fromIterable(keys)
      .pipe(map((key) => this.equal(key)))
      .pipe(flat$());
  }
  startWith(prefix: ROW[COLUMN]): Source<Table.InferPrimaryKey<TABLE>> {
    const endPrefix =
      prefix.slice(0, -1) +
      String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1);

    return fromIterable(
      this._db.getKeys({ start: prefix, end: endPrefix }),
    ).pipe(map((key) => key[1]));
  }
  lessThan(key: ROW[COLUMN]): Source<Table.InferPrimaryKey<TABLE>> {
    return fromIterable(this._db.getKeys({ end: key })).pipe(
      map((key) => key[1]),
    );
  }
  lessThanOrEqual(key: ROW[COLUMN]): Source<Table.InferPrimaryKey<TABLE>> {
    return fromIterable(this._db.getKeys({ end: [key, "\xff"] })).pipe(
      map((key) => key[1]),
    );
  }
  greaterThan(key: ROW[COLUMN]): Source<Table.InferPrimaryKey<TABLE>> {
    return fromIterable(this._db.getKeys({ start: [key, "\xff"] })).pipe(
      map((key) => key[1]),
    );
  }
  greaterThanOrEqual(key: ROW[COLUMN]): Source<Table.InferPrimaryKey<TABLE>> {
    return fromIterable(this._db.getKeys({ start: key })).pipe(
      map((key) => key[1]),
    );
  }
  between(
    start: ROW[COLUMN],
    end: ROW[COLUMN],
  ): Source<Table.InferPrimaryKey<TABLE>> {
    return fromIterable(
      this._db.getKeys({ start: [start], end: [end, "\xff"] }),
    ).pipe(map((key) => key[1]));
  }
}

export function defineIndex<
  TABLE_DEF extends Table.AnyDefinition,
  COLUMN extends keyof Table.InferRow<Table.InferTable<TABLE_DEF>>,
>(
  config: Index.Definition<TABLE_DEF, COLUMN>,
): Index.Definition<TABLE_DEF, COLUMN> {
  return config;
}

export namespace Index {
  export type AnyIndex = Index<any, any, any>;
  export type InferColumn<T extends AnyIndex> =
    T extends Index<any, infer K, any> ? K : never;
  export type Init<
    TABLE extends Table.AnyTable,
    COLUMN extends keyof Table.InferRow<TABLE>,
  > = {
    name: string;
    env: RootDatabase;
    table: TABLE;
    key: COLUMN;
    unique?: boolean;
  };
  export type Definition<
    TABLE_DEF extends Table.AnyDefinition,
    COLUMN extends keyof Table.InferRow<Table.InferTable<TABLE_DEF>>,
  > = Omit<Init<Table.InferTable<TABLE_DEF>, COLUMN>, "env" | "table"> & {
    tableDefinition: TABLE_DEF;
  };
}
