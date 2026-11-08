import { Key, RootDatabase } from "lmdb";
import { Table } from "./table";
import { View } from "./view";

class Index<
  NAME extends string,
  TABLE extends Table.AnyTable,
  const KEY extends [
    keyof Table.InferRow<TABLE>,
    ...(keyof Table.InferRow<TABLE>)[],
  ],
  ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
> extends View<
  NAME,
  TABLE,
  [...{ [k in keyof KEY]: ROW[KEY[k]] }],
  ROW["id"],
  ROW
> {
  constructor(init: Index.Init<NAME, TABLE, KEY, ROW>) {
    const { key, unique, ...rest } = init;
    super({
      ...rest,

      keySelector(row): [...{ [k in keyof KEY]: ROW[KEY[k]] }] {
        return key.map((k) => row[k]) as [...{ [k in keyof KEY]: ROW[KEY[k]] }];
      },
      map(row): ROW["id"] {
        return row.id;
      },
      ...(unique
        ? {
            onConflict(key): void {
              throw new Error(`conflict on key ${key}`);
            },
          }
        : {}),
    });
  }
}

export function defineIndex<
  NAME extends string,
  TABLE_DEF extends Table.AnyDefinition,
  KEY extends [
    keyof Table.InferRow<Table.InferTable<TABLE_DEF>>,
    ...(keyof Table.InferRow<Table.InferTable<TABLE_DEF>>)[],
  ],
>(config: {
  name: NAME;
  tableDefinition: TABLE_DEF;
  key: KEY;
  unique?: boolean;
}): Index.Definition<NAME, TABLE_DEF, KEY> {
  const { name, tableDefinition, key, unique } = config;
  return {
    name,
    tableDefinition,
    init: (env: RootDatabase, table: Table.InferTable<TABLE_DEF>) =>
      new Index({ name, env, table, key, unique }),
  };
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

  export interface Definition<
    NAME extends string,
    TABLE_DEF extends Table.AnyDefinition,
    KEY extends [
      keyof Table.InferRow<Table.InferTable<TABLE_DEF>>,
      ...(keyof Table.InferRow<Table.InferTable<TABLE_DEF>>)[],
    ],
  > {
    name: NAME;
    tableDefinition: TABLE_DEF;
    init: (
      env: RootDatabase,
      table: Table.InferTable<TABLE_DEF>,
    ) => Index<NAME, Table.InferTable<TABLE_DEF>, KEY>;
  }
}
