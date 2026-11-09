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
  NAME extends string,
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  PK extends [
    keyof SchemaProvider.InferOutput<SCHEMA>,
    ...(keyof SchemaProvider.InferOutput<SCHEMA>)[],
  ],
  TX extends boolean = false,
  ROW extends SchemaProvider.InferOutput<SCHEMA> =
    SchemaProvider.InferOutput<SCHEMA>,
> {
  private _name: NAME;
  private _db: Database<Readonly<ROW>>;
  private _schema: SCHEMA;
  private _pk: PK;
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

  constructor(init: Table.Init<NAME, SCHEMA, PK>) {
    this._name = init.name;
    this._db = init.env.openDB({
      name: this._name,
      sharedStructuresKey: Symbol.for(this._name),
    });
    this._schema = init.schema;
    this._pk = init.pk;
    this._length = 0;
  }
  get name(): NAME {
    return this._name;
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
  get(
    pk: Table.InferColumnsValue<PK, ROW>["length"] extends 1
      ? Table.InferColumnsValue<PK, ROW>[0]
      : Table.InferColumnsValue<PK, ROW>,
  ) {
    return this._db.get(Array.isArray(pk) ? pk : [pk]);
  }
  find() {
    //
  }
  all(limit?: number): Source<ROW> {
    return fromIterable(this._db.getRange({ limit })).pipe(map((e) => e.value));
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
    row: Partial<Omit<ROW, PK[number]>> & { [K in PK[number]]: ROW[K] },
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
  delete(pk: PK): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._delete(pk) as never;
    } else {
      return this._db.transaction(() => this._delete(pk)) as never;
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
    return this._pk.map((k) => row[k]);
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
    row: Partial<Omit<ROW, PK[number]>> & { [K in PK[number]]: ROW[K] },
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
  private _delete(pk: PK): boolean {
    const oldRow = this._db.get(pk);
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
      .pipe(listen((key: PK) => this.delete(key)))
      .pipe(terminate());
  }
}

export function defineTable<
  NAME extends string,
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  PK extends [
    keyof SchemaProvider.InferOutput<SCHEMA>,
    ...(keyof SchemaProvider.InferOutput<SCHEMA>)[],
  ],
>(
  config: Table.Definition<NAME, SCHEMA, PK>,
): Table.Definition<NAME, SCHEMA, PK> {
  return config;
}

export namespace Table {
  export type AnyTable = Table<any, any, any, any, any>;
  export type AnyDefinition = Definition<any, any, any>;
  export type Row = Record<string, any> & { id: Key };
  export type InferRow<TABLE extends AnyTable> =
    TABLE extends Table<any, any, any, any, infer R> ? R : never;
  export type InferName<TABLE extends AnyTable> =
    TABLE extends Table<infer N, any, any, any, any> ? N : never;
  export type InferPrimaryID<TABLE extends AnyTable> =
    TABLE extends Table<any, any, infer P, any, any> ? P : never;
  export type InferTable<DEF extends Definition<any, any, any>> =
    DEF extends Definition<infer NAME, infer SCHEMA, infer PK>
      ? Table<NAME, SCHEMA, PK>
      : never;
  export type InferColumnsValue<COLS extends any[], ROW> = {
    [K in keyof COLS]: COLS[K] extends keyof ROW ? ROW[COLS[K]] : never;
  };

  export type Init<
    NAME extends string,
    SCHEMA extends SchemaProvider.AnySchemaProvider,
    PK extends [
      keyof SchemaProvider.InferOutput<SCHEMA>,
      ...(keyof SchemaProvider.InferOutput<SCHEMA>)[],
    ],
  > = {
    name: NAME;
    env: RootDatabase;
    schema: SCHEMA;
    pk: PK;
  };

  export type Definition<
    NAME extends string,
    SCHEMA extends SchemaProvider.AnySchemaProvider,
    PK extends [
      keyof SchemaProvider.InferOutput<SCHEMA>,
      ...(keyof SchemaProvider.InferOutput<SCHEMA>)[],
    ],
  > = Omit<Init<NAME, SCHEMA, PK>, "env">;
}
