import {
  filter,
  flat,
  flat$,
  fromFunction,
  fromIterable,
  listen,
  map,
  merge,
  of,
  scope,
  Signal,
  Source,
  tap,
} from "@watchmanlab/stream";
import { Table } from "./table";
import { Database, Key, RootDatabase } from "lmdb";
import { isDeepEqual } from "./utils";

export class View<
  TABLE extends Table.AnyTable,
  KEY extends keyof Table.InferRow<TABLE>,
  ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
> {
  private _name: string;
  private _db: Database;
  private _table: TABLE;
  private _key: KEY;
  private _unique?: boolean;
  constructor(init: View.Init<TABLE, KEY>) {
    const { name, env, table, key, unique } = init;

    this._name = `${table.name}:${name}`;
    this._db = env.openDB({ name: this._name });
    this._table = table;
    this._key = key;
    this._unique = unique;
    this._init();
    table.$beforeInsert.pipe(listen((newRow) => this._insert(newRow)));
    table.$beforeUpdate.pipe(
      listen(({ newRow, oldRow }) => this._update(newRow, oldRow)),
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

  private _makeKey(row: ROW): [KEY, Table.InferPrimaryKey<TABLE>] {
    return [
      row[this._key],
      this._table.primaryKey.map((k) => row[k]) as Table.InferPrimaryKey<TABLE>,
    ];
  }
  private _insert(newRow: ROW) {
    const { _unique, _name } = this;
    const newKey = this._makeKey(newRow);

    if (_unique) {
      const rawKey = newKey.toSpliced(-1)[0]!;

      this.equal(rawKey as KEY)
        .consume((self) => {
          self.terminate("abort");
          throw new Error(`duplicate key ${String(rawKey)} at ${_name}`);
        })
        .next();
    }

    this._db.put(newKey, newRow);
  }
  private _update(newRow: ROW, oldRow: ROW) {
    const { _unique, _name } = this;

    const oldKey = this._makeKey(oldRow);
    const newKey = this._makeKey(newRow);

    if (isDeepEqual(oldKey, newKey)) return;

    if (_unique) {
      const rawKey = newKey.toSpliced(-1)[0]!;

      this.equal(rawKey as KEY)
        .consume((self, { key }) => {
          if ((key as Array<any>).pop() !== newKey[newKey.length - 1]) {
            self.terminate("abort");
            throw new Error(`duplicate key ${String(rawKey)} at ${_name}`);
          }
          self.next();
        })
        .next();
    }

    this._db.remove(oldKey);
    this._db.put(newKey, newRow);
  }
  private _delete(oldRow: ROW) {
    this._db.remove(this._makeKey(oldRow));
  }
  equal(key: ROW[KEY]): Source<ROW> {
    return fromIterable(
      this._db.getRange({ start: [key], end: [key, "\xff"] }),
    ).pipe(map((e) => e.value));
  }
  notEqual(key: ROW[KEY]): Source<ROW> {
    const left = this.lessThan(key);
    const right = this.greaterThan(key);

    return of(left, right).pipe(flat$());
  }
  in(keys: [ROW[KEY], ROW[KEY], ...ROW[KEY][]]): Source<ROW> {
    return fromIterable(keys)
      .pipe(map((key) => this.equal(key)))
      .pipe(flat$());
  }
  startWith(prefix: ROW[KEY]): Source<ROW> {
    const endPrefix =
      prefix.slice(0, -1) +
      String.fromCharCode(prefix.charCodeAt(prefix.length - 1) + 1);

    return fromIterable(
      this._db.getRange({ start: prefix, end: endPrefix }),
    ).pipe(map((e) => e.value));
  }

  lessThan(key: ROW[KEY]): Source<ROW> {
    return fromIterable(this._db.getRange({ end: key })).pipe(
      map((e) => e.value),
    );
  }
  lessThanOrEqual(key: ROW[KEY]): Source<ROW> {
    return fromIterable(this._db.getRange({ end: [key, "\xff"] })).pipe(
      map((e) => e.value),
    );
  }
  greaterThan(key: ROW[KEY]): Source<ROW> {
    return fromIterable(this._db.getRange({ start: [key, "\xff"] })).pipe(
      map((e) => e.value),
    );
  }
  greaterThanOrEqual(key: ROW[KEY]): Source<ROW> {
    return fromIterable(this._db.getRange({ start: key })).pipe(
      map((e) => e.value),
    );
  }
  between(start: ROW[KEY], end: ROW[KEY]): Source<ROW> {
    return fromIterable(
      this._db.getRange({ start: [start], end: [end, "\xff"] }),
    ).pipe(map((e) => e.value));
  }
  intersect() {}
}

export function defineView<
  TABLE_DEF extends Table.AnyDefinition,
  KEY extends keyof Table.InferRow<Table.InferTable<TABLE_DEF>>,
>(config: View.Definition<TABLE_DEF, KEY>): View.Definition<TABLE_DEF, KEY> {
  return config;
}

export namespace View {
  export type Value<ID extends Key, VALUE> = { id: ID; map: VALUE };
  export type AnyView = View<any, any, any>;
  export type StringOnly = string & Symbol;
  export type Init<
    TABLE extends Table.AnyTable,
    KEY extends keyof Table.InferRow<TABLE>,
  > = {
    name: string;
    env: RootDatabase;
    table: TABLE;
    key: KEY;
    unique?: boolean;
  };
  export type Definition<
    TABLE_DEF extends Table.AnyDefinition,
    KEY extends keyof Table.InferRow<Table.InferTable<TABLE_DEF>>,
  > = Omit<Init<Table.InferTable<TABLE_DEF>, KEY>, "env" | "table"> & {
    tableDefinition: TABLE_DEF;
  };
}
