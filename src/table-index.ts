import type { Database, RootDatabase } from "lmdb";
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
  | { type: "FULL_COVER" } // 0% row validation needed
  | { type: "RESIDUAL_FILTER"; indexKeysCount: number } // Use index first, then filter rows
  | { type: "NO_COVER" }; // Cannot use this index
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

  private _matchQuery(
    query: Record<string, any>,
    queryLen: number,
  ): IndexMatchResult {
    const indexLen = this._columns.length;

    let matchCount = 0;

    for (let i = 0; i < Math.min(queryLen, indexLen); i++) {
      if (this._columns[i]! in query) {
        matchCount++;
      } else {
        break;
      }
    }

    if (matchCount === queryLen) {
      return { type: "FULL_COVER" };
    }

    if (matchCount === indexLen) {
      return { type: "RESIDUAL_FILTER", indexKeysCount: indexLen };
    }

    return { type: "NO_COVER" };
  }
  private _makeIndexKey(row: Table.Row | Record<string, any>): string {
    const values = this._columns.map((col) => row[col]);
    if (row?.id) values.push(row.id);
    return values.join("\x00");
  }
  find(
    query: Record<string, any>,
    queryLen: number,
  ): { source: Source<any>; strategy: Table.IndexStrategy } | undefined {
    delete query.id;

    const match = this._matchQuery(query, queryLen);

    if (match.type === "NO_COVER") return;

    const searchValues = this._makeIndexKey(query) + "\x00";

    let source = fromIterable(
      this._db.getRange({
        start: searchValues,
        end: searchValues + "\xff",
        inclusiveEnd: true,
      }),
    ).pipe(map((e) => this._table.get(e.value)!));

    if (match.type === "RESIDUAL_FILTER") {
      const residualKeys = Object.keys(query).filter(
        (k) => !this._columns.includes(k),
      );

      return {
        source: source.pipe(
          filter((row) =>
            residualKeys.every((key) => isDeepEqual(row[key], query[key])),
          ),
        ),
        strategy: "NRESIDUAL_FILTER",
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

    if (oldIndexKey !== newIndexKey) {
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
