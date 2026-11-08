import { RootDatabase } from "lmdb";
import { Table } from "./table";

export class Query<
  TABLE extends Table.AnyTable,
  ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
> {
  constructor(env: RootDatabase, table: TABLE) {}

  where(column: keyof ROW) {}
}
