import {
  fromIterable,
  listen,
  map,
  Source,
  Stream,
  Terminate,
  terminate,
  TerminateReason,
} from "@watchmanlab/stream";
import type { Database, Key, RootDatabase } from "lmdb";
import type { SchemaProvider } from "./schema-provider";

export class Table<
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  PRIMARY_KEY extends [
    keyof SchemaProvider.InferOutput<SCHEMA>,
    ...(keyof SchemaProvider.InferOutput<SCHEMA>)[],
  ],
  TX extends boolean = false,
  ROW extends SchemaProvider.InferOutput<SCHEMA> =
    SchemaProvider.InferOutput<SCHEMA>,
> implements Iterable<ROW> {
  private _name: string;
  private _db: Database<Readonly<ROW>>;
  private _schema: SCHEMA;
  private _primaryKey: PRIMARY_KEY;
  private _length: number;
  private _$beforeInsert?: Stream<Readonly<ROW>>;
  private _$afterInsert?: Stream<Readonly<ROW>>;
  private _$beforeUpdate?: Stream<{
    newRow: Readonly<ROW>;
    oldRow: Readonly<ROW>;
  }>;
  private _$afterUpdate?: Stream<{
    newRow: Readonly<ROW>;
    oldRow: Readonly<ROW>;
  }>;
  private _$beforeDelete?: Stream<Readonly<ROW>>;
  private _$afterDelete?: Stream<Readonly<ROW>>;

  constructor(init: Table.Init<SCHEMA, PRIMARY_KEY>) {
    const { name, env, schema, primaryKey } = init;
    this._name = name;
    this._db = env.openDB({
      name: this._name,
      sharedStructuresKey: Symbol.for(this._name),
    });
    this._schema = schema;
    this._primaryKey = primaryKey;
    this._length = 0;
  }
  [Symbol.iterator]() {
    return this.all()[Symbol.iterator]();
  }
  get name(): string {
    return this._name;
  }
  get primaryKey(): PRIMARY_KEY {
    return this._primaryKey;
  }
  get $beforeInsert(): Source<Readonly<ROW>> {
    return Source.from((this._$beforeInsert ??= new Stream()));
  }
  get $afterInsert(): Source<Readonly<ROW>> {
    return Source.from((this._$afterInsert ??= new Stream()));
  }
  get $beforeUpdate(): Source<{
    newRow: Readonly<ROW>;
    oldRow: Readonly<ROW>;
  }> {
    return Source.from((this._$beforeUpdate ??= new Stream()));
  }
  get $afterUpdate(): Source<{ newRow: Readonly<ROW>; oldRow: Readonly<ROW> }> {
    return Source.from((this._$afterUpdate ??= new Stream()));
  }
  get $beforeDelete(): Source<Readonly<ROW>> {
    return Source.from((this._$beforeDelete ??= new Stream()));
  }
  get $afterDelete(): Source<Readonly<ROW>> {
    return Source.from((this._$afterDelete ??= new Stream()));
  }
  query() {
    return {
      execute: () => {},
    };
  }
  get(primaryKey: Table.InferColumnsValue<PRIMARY_KEY, ROW>) {
    return this._db.get(Array.isArray(primaryKey) ? primaryKey : [primaryKey]);
  }
  all(): Source<ROW> {
    return fromIterable(this._db.getRange()).pipe(map((e) => e.value));
  }
  get length() {
    return this._length;
  }
  insert(row: ROW): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._insert(row) as never;
    } else {
      return this._db.transaction(() => this._insert(row)) as never;
    }
  }
  update(
    row: Partial<Omit<ROW, PRIMARY_KEY[number]>> & {
      [K in PRIMARY_KEY[number]]: ROW[K];
    },
  ): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._update(row) as never;
    } else {
      return this._db.transaction(() => this._update(row)) as never;
    }
  }
  upsert(row: ROW): TX extends true ? boolean : Promise<boolean> {
    if (this._db.doesExist(row.id)) {
      return this.update(row) as never;
    } else {
      return this.insert(row) as never;
    }
  }
  delete(
    primaryKey: PRIMARY_KEY,
  ): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._delete(primaryKey) as never;
    } else {
      return this._db.transaction(() => this._delete(primaryKey)) as never;
    }
  }
  private _isInsideTransaction(): boolean {
    try {
      return this._db.getWriteTxnId() > 0;
    } catch (error) {
      return false;
    }
  }
  private _makeKey(row: ROW) {
    return this._primaryKey.map((k) => row[k]);
  }
  private _insert(row: ROW): boolean {
    const parsed = this._schema.parse(row);

    const key = this._makeKey(parsed);

    if (this._db.doesExist(key)) return false;

    this._$beforeInsert?.push(parsed);

    this._db.put(key, parsed).then((r) => {
      if (!r) return;
      this._length++;
      this._$afterInsert?.push(parsed);
    });

    return true;
  }
  private _update(
    row: Partial<Omit<ROW, PRIMARY_KEY[number]>> & {
      [K in PRIMARY_KEY[number]]: ROW[K];
    },
  ): boolean {
    const key = this._makeKey(row);

    const oldRow = this._db.get(key);

    if (!oldRow) return false;

    const parsed = this._schema.parse({ ...oldRow, ...row });

    this._$beforeUpdate?.push({ newRow: parsed, oldRow });

    this._db.put(key, parsed).then((r) => {
      if (r) this._$afterUpdate?.push({ newRow: parsed, oldRow });
    });

    return true;
  }
  private _delete(primaryKey: PRIMARY_KEY): boolean {
    const oldRow = this._db.get(primaryKey);
    if (!oldRow) return false;

    this._$beforeDelete?.push(oldRow);

    this._db.remove(oldRow.id).then((r) => {
      if (!r) return;
      this._length--;
      this._$afterDelete?.push(oldRow);
    });
    return true;
  }
  clear(): Terminate<Source<TerminateReason>> {
    return fromIterable(this._db.getKeys())
      .pipe(listen((key: PRIMARY_KEY) => this.delete(key)))
      .pipe(terminate());
  }
}

export function defineTable<
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  PRIMARY_KEY extends [
    keyof SchemaProvider.InferOutput<SCHEMA>,
    ...(keyof SchemaProvider.InferOutput<SCHEMA>)[],
  ],
>(
  config: Table.Definition<SCHEMA, PRIMARY_KEY>,
): Table.Definition<SCHEMA, PRIMARY_KEY> {
  return config;
}

export namespace Table {
  export type AnyTable = Table<any, [any, ...any[]], any, any>;
  export type AnyDefinition = Definition<any, [any, ...any[]]>;
  export type Row = Record<string, any> & { id: Key };
  export type InferRow<TABLE extends AnyTable> =
    TABLE extends Table<any, any, any, infer R> ? R : never;
  export type InferPrimaryKey<TABLE extends AnyTable> =
    TABLE extends Table<any, infer P, any, any> ? P : never;
  export type InferTable<DEF extends AnyDefinition> =
    DEF extends Definition<infer SCHEMA, infer PRIMARY_KEY>
      ? Table<SCHEMA, PRIMARY_KEY>
      : never;
  export type InferColumnsValue<COLS extends any[], ROW> = {
    [K in keyof COLS]: COLS[K] extends keyof ROW ? ROW[COLS[K]] : never;
  };

  export type Init<
    SCHEMA extends SchemaProvider.AnySchemaProvider,
    PRIMARY_KEY extends [
      keyof SchemaProvider.InferOutput<SCHEMA>,
      ...(keyof SchemaProvider.InferOutput<SCHEMA>)[],
    ],
  > = {
    name: string;
    env: RootDatabase;
    schema: SCHEMA;
    primaryKey: PRIMARY_KEY;
  };

  export type Definition<
    SCHEMA extends SchemaProvider.AnySchemaProvider,
    PRIMARY_KEY extends [
      keyof SchemaProvider.InferOutput<SCHEMA>,
      ...(keyof SchemaProvider.InferOutput<SCHEMA>)[],
    ],
  > = Omit<Init<SCHEMA, PRIMARY_KEY>, "env" | "name">;
}
