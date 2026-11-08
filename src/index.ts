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
> {
  private _name: NAME;
  private _view: View<NAME, TABLE, KEY, ROW["id"], ROW>;
  constructor(init: Index.Init<NAME, TABLE, KEY, ROW>) {
    const { name, key, unique, ...rest } = init;
    this._name = name;
    this._view = new View({
      ...rest,
      name,
      keySelector(row): KEY {
        return key;
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
  get name(): NAME {
    return this._name;
  }
  get view(): View<NAME, TABLE, KEY, ROW["id"], ROW> {
    return this._view;
  }
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
}
