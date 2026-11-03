import type { Database, Key, RootDatabase } from "lmdb";
import type { Table } from "./table";
import {
  filter,
  fromIterable,
  listen,
  map,
  Source,
  tap,
} from "@watchmanlab/stream";
import { isDeepEqual } from "./utils";
export type IndexMatchResult =
  | { type: "FULL_COVER" }
  | { type: "RESIDUAL_FILTER"; residualKeys: string[] }
  | { type: "NO_COVER" };
export class Index {
  private _name: string;
  private _db: Database;
  private _table: Table<any, any, any, any>;
  private _columns: string[];

  constructor(
    env: RootDatabase,
    table: Table<any, any, any, any>,
    columns: string[],
  ) {
    this._name = ["index", ...columns, table.name].join(":");
    this._db = env.openDB({ name: this._name });
    this._table = table;
    this._columns = columns;

    // build the index if new and its table has rows
    if (
      this._db.getKeys()[Symbol.iterator]().next().done &&
      !this._table.find()[Symbol.iterator]().next().done
    ) {
      this._db.transaction(() =>
        this._table.find().pipe(listen((row) => this.insert(row))),
      );
    }
  }
  get columns(): string[] {
    return this._columns;
  }

  private _makeIndexKey(obj: Table.Row | Record<string, any>): Key[] {
    const values = this._columns.map((col) => obj[col]);
    if (obj?.id) values.push(obj.id);
    return values;
  }
  find(
    query: Record<string, any>,
    queryLen: number,
  ): { source: Source<any>; strategy: Table.IndexStrategy } | undefined {
    delete query.id;

    const residualKeys: string[] = [];

    for (let i = 0; i < Math.min(queryLen, this._columns.length); i++) {
      const indexKey = this._columns[i]!;
      if (!(indexKey in query)) {
        residualKeys.push(indexKey);
      }
    }

    // NO COVER
    if (residualKeys.length >= queryLen) return;

    const start = this._makeIndexKey(query);
    const end = [...start, "\xff"];

    let source = fromIterable(
      this._db.getRange({ start, end, inclusiveEnd: true }),
    ).pipe(map((e) => this._table.get(e.value)!));

    if (residualKeys.length && residualKeys.length < queryLen) {
      return {
        source: source.pipe(
          filter((row) =>
            residualKeys.every((key) => isDeepEqual(row[key], query[key])),
          ),
        ),
        strategy: "RESIDUAL_FILTER",
      };
    }

    return { source, strategy: "FULL_COVER" };
  }

  insert(row: Table.Row): void {
    const indexKey = this._makeIndexKey(row);
    this._db.put(indexKey, row.id);
  }
  update(newRow: Table.Row, oldRow: Table.Row): void {
    const oldIndexKey = this._makeIndexKey(oldRow);
    const newIndexKey = this._makeIndexKey(newRow);

    if (!isDeepEqual(oldIndexKey, newIndexKey)) {
      this._db.remove(oldIndexKey);
      this._db.put(newIndexKey, newRow.id);
    }
  }

  delete(row: Table.Row): void {
    const indexKey = this._makeIndexKey(row);
    this._db.remove(indexKey);
  }
  clear(): Promise<void> {
    return this._db.clearAsync();
  }
}
