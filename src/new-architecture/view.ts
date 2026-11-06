import { fromIterable, listen, map, Source } from "@watchmanlab/stream";
import { Table } from "./table";
import { Database, Key, RootDatabase } from "lmdb";
import { isDeepEqual } from "./utils";
import { ZodSchema } from "./zod-schema";
import z, { includes } from "zod";

export class View<
  TABLE extends Table.AnyTable,
  const KEYS extends readonly Key[],
  const INCLUDES,
  ROW extends Readonly<Table.InferRow<TABLE>> = Readonly<Table.InferRow<TABLE>>,
> {
  private _db: Database;

  constructor(private init: View.Init<TABLE, KEYS, INCLUDES, ROW>) {
    const { name, env, table, unique } = init;
    this._db = env.openDB({ name: `view:${name}` });

    table.$beforeInsert.pipe(
      listen((newRow) => {
        const keys = this._makeKey(newRow);
        if (keys) {
          if (unique) {
            const rawKeys = keys.toSpliced(-1);
            this.eq(...(rawKeys as never)).pipe(
              listen(() => {
                throw new Error(`duplicate keys: ${rawKeys}`);
              }),
            );
          }

          this._db.put(keys, this._makeIncludes(newRow));
        }
      }),
    );
    table.$beforeUpdate.pipe(
      listen(({ newRow, oldRow }) => {
        const oldKeys = this._makeKey(oldRow);
        const newKeys = this._makeKey(newRow);

        if (isDeepEqual(oldKeys, newKeys)) return;

        if (unique && newKeys) {
          const rawKeys = newKeys.toSpliced(-1);
          this.eq(...(rawKeys as never)).pipe(
            listen((match) => {
              if (match.id !== newRow.id) {
                throw new Error(`duplicate keys: ${rawKeys}`);
              }
            }),
          );
        }

        if (oldKeys) this._db.remove(oldKeys);
        if (newKeys) this._db.put(newKeys, this._makeIncludes(newRow));
      }),
    );
    table.$beforeDelete.pipe(
      listen((oldRow) => {
        const keys = this._makeKey(oldRow);
        if (keys) this._db.remove(keys);
      }),
    );
  }

  private _makeKey(row: ROW): null | any[] {
    const keys = this.init.keys(row);
    if (!keys) return null;
    return [...keys, row.id];
  }
  private _makeIncludes(row: ROW) {
    return { id: row.id, includes: this.init.includes?.(row) };
  }

  eq(...keys: Partial<KEYS>): Source<View.Value<ROW["id"], INCLUDES>> {
    return fromIterable(
      this._db.getRange({
        start: keys as never,
        end: [...keys, "\xff"] as never,
      }),
    ).pipe(map((e) => e.value));
  }
  lt(...keys: Partial<KEYS>): Source<View.Value<ROW["id"], INCLUDES>> {
    return fromIterable(this._db.getRange({ end: keys as never })).pipe(
      map((e) => e.value),
    );
  }
  lte(...keys: Partial<KEYS>): Source<View.Value<ROW["id"], INCLUDES>> {
    return fromIterable(
      this._db.getRange({ end: [...keys, "\xff"] as never }),
    ).pipe(map((e) => e.value));
  }
  gt(...keys: Partial<KEYS>): Source<View.Value<ROW["id"], INCLUDES>> {
    return fromIterable(
      this._db.getRange({ start: [...keys, "\xff"] as never }),
    ).pipe(map((e) => e.value));
  }
  gte(...keys: Partial<KEYS>): Source<View.Value<ROW["id"], INCLUDES>> {
    return fromIterable(this._db.getRange({ start: keys as never })).pipe(
      map((e) => e.value),
    );
  }
  between(
    start: Partial<KEYS>,
    end: Partial<KEYS>,
  ): Source<View.Value<ROW["id"], INCLUDES>> {
    return fromIterable(
      this._db.getRange({
        start: start as never,
        end: [...end, "\xff"] as never,
      }),
    ).pipe(map((e) => e.value));
  }
}

export namespace View {
  export type Value<ID extends Key, INCLUDES> = { id: ID; includes: INCLUDES };
  export type Init<
    TABLE extends Table.AnyTable,
    KEYS extends readonly Key[],
    INCLUDES,
    ROW extends Readonly<Table.InferRow<TABLE>> = Readonly<
      Table.InferRow<TABLE>
    >,
  > = {
    name: string;
    env: RootDatabase;
    table: TABLE;
    keys: (row: ROW) => KEYS | null | undefined;
    includes?: (row: ROW) => INCLUDES;
    unique?: boolean;
  };
}

const users = new Table({
  name: "users",
  env: null as never,
  schema: new ZodSchema({
    id: z.number(),
    name: z.string(),
    age: z.number(),
  }),
});

const view = new View({
  name: "myView",
  env: null as never,
  table: users,
  keys(row) {
    return [row.age, row.name] as [age: number, name: string];
  },
  includes(row) {
    return { id: "" };
  },
});
//get(age: number, name: string): void
view.eq(33, "sof");
view.lt(44);
view.between([33], [44]);
