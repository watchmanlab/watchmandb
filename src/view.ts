import { fromIterable, listen, map, Source } from "@watchmanlab/stream";
import { Table } from "./table";
import { Database, Key, RootDatabase } from "lmdb";
import { isDeepEqual } from "./utils";
import { Queryable } from "./queryable";

export class View<
  NAME extends string,
  TABLE extends Table.AnyTable,
  const KEY extends [
    keyof Table.InferRow<TABLE>,
    ...(keyof Table.InferRow<TABLE>)[],
  ],
  const VALUE,
  ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
> {
  private _name: NAME;
  protected _db: Database<VALUE, Key[]>;

  constructor(protected init: View.Init<NAME, TABLE, KEY, VALUE, ROW>) {
    const { name, env, table, onConflict, map } = init;
    this._name = name;
    this._db = env.openDB({ name: `view:${name}` });

    table.$beforeInsert.pipe(
      listen((newRow) => {
        const key = this._makeKey(newRow);

        if (onConflict) {
          const rawKeys = key.toSpliced(-1);
          this.eq(...(rawKeys as never)).pipe(
            listen((includes) => {
              onConflict(rawKeys as never, newRow, includes);
            }),
          );
        }

        this._db.put(key, map(newRow));
      }),
    );
    table.$beforeUpdate.pipe(
      listen(({ newRow, oldRow }) => {
        const oldKeys = this._makeKey(oldRow);
        const newKeys = this._makeKey(newRow);

        if (isDeepEqual(oldKeys, newKeys)) return;

        if (onConflict) {
          const rawKeys = newKeys.toSpliced(-1);

          fromIterable(
            this._db.getRange({
              start: rawKeys as never,
              end: [...rawKeys, "\xff"],
            }),
          ).pipe(
            listen(({ key, value }) => {
              if ((key as Array<any>).pop() === newRow.id) {
                onConflict(rawKeys as never, newRow, value);
              }
            }),
          );
        }

        this._db.remove(oldKeys);
        this._db.put(newKeys, map(newRow));
      }),
    );
    table.$beforeDelete.pipe(
      listen((oldRow) => {
        const keys = this._makeKey(oldRow);
        if (keys) this._db.remove(keys);
      }),
    );
  }
  get name(): NAME {
    return this._name;
  }
  private _makeKey(row: ROW): [...KEY, ROW["id"]] {
    return [...this.init.key.map((k) => row[k]), row.id] as [...KEY, ROW["id"]];
  }
  has(...keys: [...{ [k in keyof KEY]?: ROW[KEY[k]] }]): boolean {
    for (const v of this.eq(...keys)) return true;
    return false;
  }
  get(...keys: [...{ [k in keyof KEY]?: ROW[KEY[k]] }]): VALUE | undefined {
    for (const v of this.eq(...keys)) return v;
    return;
  }
  eq(...keys: [...{ [k in keyof KEY]?: ROW[KEY[k]] }]): Source<VALUE> {
    return fromIterable(
      this._db.getRange({
        start: keys as never,
        end: [...keys, "\xff"] as never,
      }),
    ).pipe(map((e) => e.value));
  }
  lt(...keys: [...{ [k in keyof KEY]?: ROW[KEY[k]] }]): Source<VALUE> {
    return fromIterable(this._db.getRange({ end: keys as never })).pipe(
      map((e) => e.value),
    );
  }
  lte(...keys: [...{ [k in keyof KEY]?: ROW[KEY[k]] }]): Source<VALUE> {
    return fromIterable(
      this._db.getRange({ end: [...keys, "\xff"] as never }),
    ).pipe(map((e) => e.value));
  }
  gt(...keys: [...{ [k in keyof KEY]?: ROW[KEY[k]] }]): Source<VALUE> {
    return fromIterable(
      this._db.getRange({ start: [...keys, "\xff"] as never }),
    ).pipe(map((e) => e.value));
  }
  gte(...keys: [...{ [k in keyof KEY]?: ROW[KEY[k]] }]): Source<VALUE> {
    return fromIterable(this._db.getRange({ start: keys as never })).pipe(
      map((e) => e.value),
    );
  }
  between(
    start: [...{ [k in keyof KEY]?: ROW[KEY[k]] }],
    end: [...{ [k in keyof KEY]?: ROW[KEY[k]] }],
  ): Source<VALUE> {
    return fromIterable(
      this._db.getRange({
        start: start as never,
        end: [...end, "\xff"] as never,
      }),
    ).pipe(map((e) => e.value));
  }
}

export namespace View {
  export type Value<ID extends Key, VALUE> = { id: ID; map: VALUE };
  export type AnyView = View<any, any, any, any, any>;
  export type Init<
    NAME extends string,
    TABLE extends Table.AnyTable,
    KEY extends [Key, ...Key[]],
    VALUE,
    ROW extends Table.InferRow<TABLE>,
  > = {
    name: NAME;
    env: RootDatabase;
    table: TABLE;
    key: KEY;
    map: (row: ROW) => VALUE;
    onConflict?: (key: KEY, row: ROW, includes: VALUE) => VALUE;
  };
}
