import { fromIterable, listen, map, Source } from "@watchmanlab/stream";
import { Table } from "./table";
import { Database, Key, RootDatabase } from "lmdb";
import { isDeepEqual } from "./utils";
import { ZodSchema } from "./zod-schema";
import z from "zod";
import { Queryable } from "./queryable";

export class View<
  ROW extends Record<string, any>,
  const KEY extends [Key, ...Key[]],
  const INCLUDES,
> implements Queryable<KEY, INCLUDES> {
  protected _db: Database<INCLUDES, [...KEY, ROW["id"]]>;

  constructor(protected init: View.Init<ROW, KEY, INCLUDES>) {
    const { name, env, table, unique, includes } = init;
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

          this._db.put(keys, includes(newRow));
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
          fromIterable(
            this._db.getRange({
              start: rawKeys as never,
              end: [...rawKeys, "\xff"] as never,
            }),
          ).pipe(
            listen(({ key }) => {
              if ((key as Array<any>).pop() !== newRow.id) {
                throw new Error(`duplicate keys: ${rawKeys}`);
              }
            }),
          );
        }

        if (oldKeys) this._db.remove(oldKeys);
        if (newKeys) this._db.put(newKeys, includes(newRow));
      }),
    );
    table.$beforeDelete.pipe(
      listen((oldRow) => {
        const keys = this._makeKey(oldRow);
        if (keys) this._db.remove(keys);
      }),
    );
  }

  private _makeKey(row: ROW): null | [...KEY, ROW["id"]] {
    const keys = this.init.keySelector(row);
    if (!keys) return null;
    return [...keys, row.id] as [...KEY, ROW["id"]];
  }

  eq(...keys: Partial<KEY>): Source<INCLUDES> {
    return fromIterable(
      this._db.getRange({
        start: keys as never,
        end: [...keys, "\xff"] as never,
      }),
    ).pipe(map((e) => e.value));
  }
  lt(...keys: Partial<KEY>): Source<INCLUDES> {
    return fromIterable(this._db.getRange({ end: keys as never })).pipe(
      map((e) => e.value),
    );
  }
  lte(...keys: Partial<KEY>): Source<INCLUDES> {
    return fromIterable(
      this._db.getRange({ end: [...keys, "\xff"] as never }),
    ).pipe(map((e) => e.value));
  }
  gt(...keys: Partial<KEY>): Source<INCLUDES> {
    return fromIterable(
      this._db.getRange({ start: [...keys, "\xff"] as never }),
    ).pipe(map((e) => e.value));
  }
  gte(...keys: Partial<KEY>): Source<INCLUDES> {
    return fromIterable(this._db.getRange({ start: keys as never })).pipe(
      map((e) => e.value),
    );
  }
  between(start: Partial<KEY>, end: Partial<KEY>): Source<INCLUDES> {
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
    ROW extends Record<string, any>,
    KEY extends [Key, ...Key[]],
    INCLUDES,
  > = {
    name: string;
    env: RootDatabase;
    table: Table.AnyTable;
    keySelector: (row: ROW) => KEY | null | undefined;
    includes: (row: ROW) => INCLUDES;
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
  keySelector(row) {
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
