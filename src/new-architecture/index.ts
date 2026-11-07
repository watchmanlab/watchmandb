import { keyof } from "zod";
import { Queryable } from "./queryable";
import { Table } from "./table";
import { View } from "./view";
import { map, Source } from "@watchmanlab/stream";
import { RootDatabase } from "lmdb";

export class Index<
  NAME extends string,
  TABLE extends Table.AnyTable,
  const KEY extends [
    keyof Table.InferRow<TABLE>,
    ...(keyof Table.InferRow<TABLE>)[],
  ],
  ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
> implements Queryable<KEY, ROW> {
  private _name: NAME;
  private _view: View<NAME, TABLE, KEY, ROW["id"], ROW>;
  constructor(private init: Index.Init<NAME, TABLE, KEY, ROW>) {
    const { key, name, ...rest } = init;
    this._name = name;
    this._view = new View({
      ...rest,
      name,
      keySelector(row) {
        return key.map((k) => row[k]) as never;
      },
      includes(row) {
        return row.id as ROW["id"];
      },
    });
  }
  get name(): NAME {
    return this._name;
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

export function createIndex<
  NAME extends string,
  TABLE extends Table.AnyTable,
  const KEY extends [
    keyof Table.InferRow<TABLE>,
    ...(keyof Table.InferRow<TABLE>)[],
  ],
  ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
>(init: Omit<Index.Init<NAME, TABLE, KEY, ROW>, "env">) {
  return (env: RootDatabase) => new Index({ ...init, env });
}

export namespace Index {
  export type Init<
    NAME extends string,
    TABLE extends Table.AnyTable,
    KEY extends [
      keyof Table.InferRow<TABLE>,
      ...(keyof Table.InferRow<TABLE>)[],
    ],
    ROW extends Table.InferRow<TABLE>,
  > = Omit<
    View.Init<
      NAME,
      TABLE,
      [...{ [K in keyof KEY]: ROW[KEY[K]] }],
      ROW["id"],
      ROW
    >,
    "includes" | "keySelector"
  > & { key: KEY };
}
