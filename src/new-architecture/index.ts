import { Key } from "lmdb";
import { Table } from "./table";
import { View } from "./view";
import { map, Source } from "@watchmanlab/stream";

export class Index<
  TABLE extends Table.AnyTable,
  const KEY extends Key[],
  ROW extends Readonly<Table.InferRow<TABLE>> = Readonly<Table.InferRow<TABLE>>,
> extends View<TABLE, KEY, ROW["id"], ROW> {
  constructor(init: Index.Init<TABLE, KEY, ROW>) {
    super({
      ...init,
      includes(row) {
        return row.id as ROW["id"];
      },
    });
  }

  override eq(...keys: Partial<KEY>): Source<ROW> {
    return super.eq(...keys).pipe(map((id) => this.init.table.get(id)));
  }
  override lt(...keys: Partial<KEY>): Source<ROW> {
    return super.lt(...keys).pipe(map((id) => this.init.table.get(id)));
  }
  override lte(...keys: Partial<KEY>): Source<ROW> {
    return super.lte(...keys).pipe(map((id) => this.init.table.get(id)));
  }
  override gt(...keys: Partial<KEY>): Source<ROW> {
    return super.gt(...keys).pipe(map((id) => this.init.table.get(id)));
  }
  override gte(...keys: Partial<KEY>): Source<ROW> {
    return super.gte(...keys).pipe(map((id) => this.init.table.get(id)));
  }
  override between(start: Partial<KEY>, end: Partial<KEY>): Source<ROW> {
    return super.between(start, end).pipe(map((id) => this.init.table.get(id)));
  }
}

export namespace Index {
  export type Init<
    TABLE extends Table.AnyTable,
    KEY extends readonly Key[],
    ROW extends Readonly<Table.InferRow<TABLE>> = Readonly<
      Table.InferRow<TABLE>
    >,
  > = Omit<View.Init<TABLE, KEY, ROW["id"], ROW>, "includes">;
}
