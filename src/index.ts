import { Queryable } from "./queryable";
import { Table } from "./table";
import { View } from "./view";
import { map, Source } from "@watchmanlab/stream";

export class Index<
  NAME extends string,
  TABLE extends Table.AnyTable,
  const KEY extends [
    keyof Table.InferRow<TABLE>,
    ...(keyof Table.InferRow<TABLE>)[],
  ],
  ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
> {
  private _name: NAME;
  private _view: View<NAME, TABLE, KEY, ROW["id"], ROW>;
  constructor(private init: Index.Init<NAME, TABLE, KEY, ROW>) {
    const { key, name, unique, ...rest } = init;
    this._name = name;
    this._view = new View({
      ...rest,
      name,
      keySelector(row) {
        return key.map((k) => row[k]) as never;
      },
      map(row) {
        return row.id as ROW["id"];
      },
      ...(unique
        ? {
            onConflict(key) {
              throw new Error(`conflict on key ${key}`);
            },
          }
        : {}),
    });
  }
  get name(): NAME {
    return this._name;
  }
  has(...keys: Partial<KEY>): boolean {
    return this._view.has(...keys);
  }
  get(...keys: Partial<KEY>): ROW | undefined {
    return this._view.get(...keys);
  }
  eq(...keys: Partial<KEY>): Source<ROW> {
    return this._view.eq(...keys).pipe(map((id) => this.init.table.get(id)));
  }
  lt(...keys: Partial<KEY>): Source<ROW> {
    return this._view.lt(...keys).pipe(map((id) => this.init.table.get(id)));
  }
  lte(...keys: Partial<KEY>): Source<ROW> {
    return this._view.lte(...keys).pipe(map((id) => this.init.table.get(id)));
  }
  gt(...keys: Partial<KEY>): Source<ROW> {
    return this._view.gt(...keys).pipe(map((id) => this.init.table.get(id)));
  }
  gte(...keys: Partial<KEY>): Source<ROW> {
    return this._view.gte(...keys).pipe(map((id) => this.init.table.get(id)));
  }
  between(start: Partial<KEY>, end: Partial<KEY>): Source<ROW> {
    return this._view
      .between(start, end)
      .pipe(map((id) => this.init.table.get(id)));
  }
}

export namespace Index {
  export type AnyIndex = Index<any, any, any, any>;
  export type Init<
    NAME extends string,
    TABLE extends Table.AnyTable,
    KEY extends [
      keyof Table.InferRow<TABLE>,
      ...(keyof Table.InferRow<TABLE>)[],
    ],
    ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
  > = Omit<
    View.Init<
      NAME,
      TABLE,
      [...{ [K in keyof KEY]: ROW[KEY[K]] }],
      ROW["id"],
      ROW
    >,
    "map" | "keySelector" | "onConflict"
  > & { key: KEY; unique?: boolean };
}
