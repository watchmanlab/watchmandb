import { keyof } from "zod";
import { Queryable } from "./queryable";
import { Table } from "./table";
import { View } from "./view";
import { map, Source } from "@watchmanlab/stream";

export class Index<
  ROW extends Record<string, any>,
  const KEY extends [keyof ROW, ...(keyof ROW)[]],
> implements Queryable<KEY, ROW> {
  private _view: View<ROW, KEY, ROW["id"]>;
  constructor(private init: Index.Init<ROW, KEY>) {
    const { key, ...rest } = init;

    this._view = new View({
      ...rest,
      keySelector(row) {
        return key.map((k) => row[k]) as never;
      },
      includes(row) {
        return row.id as ROW["id"];
      },
    });
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
  export type Init<
    ROW extends Record<string, any>,
    KEY extends [keyof ROW, ...(keyof ROW)[]],
  > = Omit<
    View.Init<ROW, [...{ [K in keyof KEY]: ROW[KEY[K]] }], ROW["id"]>,
    "includes" | "keySelector"
  > & { key: KEY };
}
