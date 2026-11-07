import { listen } from "@watchmanlab/stream";
import { isDeepEqual } from "./utils";
import { Database, Key, RootDatabase } from "lmdb";
import { Table } from "./table";

export class Aggregate<
  TABLE extends Table.AnyTable,
  KEYS extends Key[],
  VALUE,
  ROW extends Readonly<Table.InferRow<TABLE>> = Readonly<Table.InferRow<TABLE>>,
> {
  private _db: Database;

  constructor(init: {
    name: string;
    env: RootDatabase;
    table: TABLE;
    keys: (row: ROW) => KEYS;
    seed: () => VALUE;
    reduce: (current: VALUE, newRow: ROW | null, oldRow: ROW | null) => VALUE;
  }) {
    const { name, env, table, keys, seed, reduce } = init;
    this._db = env.openDB({ name: `agg:${name}` });

    table.$beforeInsert.pipe(
      listen((newRow) => {
        const key = keys(newRow);
        const currentAccumulator = this._db.get(key) ?? seed();

        const nextAccumulator = reduce(currentAccumulator, newRow, null);
        this._db.put(key, nextAccumulator);
      }),
    );

    table.$beforeUpdate.pipe(
      listen(({ newRow, oldRow }) => {
        const oldKey = keys(oldRow);
        const newKey = keys(newRow);

        // Scenario 1: The record stayed in the same keys block
        if (isDeepEqual(oldKey, newKey)) {
          const current = this._db.get(newKey) ?? seed();
          this._db.put(newKey, reduce(current, newRow, oldRow));
          return;
        }

        // Scenario 2: The record shifted groups! (e.g., user changed departments)
        // Subtract from the old keys accumulator
        const currentOld = this._db.get(oldKey) ?? seed();
        this._db.put(oldKey, reduce(currentOld, null, oldRow));

        // Add to the new keys accumulator
        const currentNew = this._db.get(newKey) ?? seed();
        this._db.put(newKey, reduce(currentNew, newRow, null));
      }),
    );

    table.$beforeDelete.pipe(
      listen((oldRow) => {
        const key = keys(oldRow);
        const current = this._db.get(key) ?? seed();
        this._db.put(key, reduce(current, null, oldRow));
      }),
    );
  }

  // Pure, instant O(1) point lookup to get the pre-calculated aggregate total!
  get(key: KEYS): VALUE | null {
    return this._db.get(key) ?? null;
  }
}
