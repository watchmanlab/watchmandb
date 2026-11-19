import { fromIterable, map, Source, Stream } from "@watchmanlab/stream";
import type { Database, Key, RootDatabase } from "lmdb";
import { SchemaProvider } from "./schema-provider";
import { fail, Result, success } from "./utils";

export class Table<
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  ID extends Key,
  INPUT extends SchemaProvider.InferInput<SCHEMA> =
    SchemaProvider.InferInput<SCHEMA>,
  OUTPUT extends SchemaProvider.InferOutput<SCHEMA> =
    SchemaProvider.InferOutput<SCHEMA>,
  ERROR extends SchemaProvider.InferError<SCHEMA> =
    SchemaProvider.InferError<SCHEMA>,
> implements Iterable<{ id: ID; row: OUTPUT }> {
  protected _name: string;
  protected _env: RootDatabase;
  protected _schema: SCHEMA;
  protected _idSelector: (row: OUTPUT) => ID;
  protected _db: Database<OUTPUT, ID>;
  protected _$beforeUpsert?: Stream<{ id: ID; row: OUTPUT }>;
  protected _$afterUpsert?: Stream<{ id: ID; row: OUTPUT }>;

  protected _$beforeDelete?: Stream<{ id: ID; row: OUTPUT }>;
  protected _$afterDelete?: Stream<{ id: ID; row: OUTPUT }>;
  constructor(init: Table.Init<SCHEMA, ID, OUTPUT>) {
    const { name, env, schema, id } = init;
    this._name = name;
    this._env = env;
    this._schema = schema;
    this._idSelector = id;
    this._db = env.openDB({
      name: `table:${this._name}`,
      sharedStructuresKey: Symbol.for(this._name),
    });
  }
  [Symbol.iterator]() {
    return this.all()[Symbol.iterator]();
  }
  get name(): string {
    return this._name;
  }
  get schema(): SCHEMA | undefined {
    return this._schema;
  }
  get $beforeUpsert(): Source<{ id: ID; row: OUTPUT }> {
    return Source.from((this._$beforeUpsert ??= new Stream()));
  }
  get $afterUpsert(): Source<{ id: ID; row: OUTPUT }> {
    return Source.from((this._$afterUpsert ??= new Stream()));
  }
  get $beforeDelete(): Source<{ id: ID; row: OUTPUT }> {
    return Source.from((this._$beforeDelete ??= new Stream()));
  }
  get $afterDelete(): Source<{ id: ID; row: OUTPUT }> {
    return Source.from((this._$afterDelete ??= new Stream()));
  }
  has(id: ID): boolean {
    return this._db.doesExist(id);
  }
  get(id: ID): OUTPUT | undefined {
    return this._db.get(id);
  }
  all(): Source<{ id: ID; row: OUTPUT }> {
    return fromIterable(this._db.getRange()).pipe(
      map((e) => ({ id: e.key, row: e.value })),
    );
  }
  private inTxn = false;
  upsert(row: INPUT): Result<ID, ERROR> {
    const result = this._schema.parse(row);
    if (!result.ok) return fail(result.error);

    const id = this._idSelector(result.data);

    if (this.inTxn) {
      this._upsert(id, result.data);
    } else {
      try {
        this._db.getWriteTxnId();
        this.inTxn = true;
        this._db.flushed.then().finally(() => (this.inTxn = false));
      } catch (e) {
        this.inTxn = false;
      }
      if (this.inTxn) {
        this._upsert(id, result.data);
      } else {
        this._db.transaction(() => {
          this._upsert(id, result.data);
        });
      }
    }

    return success(id);
  }
  private _upsert(id: ID, row: OUTPUT) {
    this._$beforeUpsert?.push({ id, row: row });
    this._db.put(id, row).then((r) => {
      if (r) this._$afterUpsert?.push({ id, row: row });
    });
  }
  delete(id: ID): void {
    this._db.transaction(() => {
      const row = this._db.get(id);
      if (!row) return;

      this._$beforeDelete?.push({ id, row });

      this._db.remove(id).then((r) => {
        if (!r) return;
        this._$afterDelete?.push({ id, row });
      });
    });
  }

  drop(): Promise<void> {
    return this._db.drop();
  }
}

export function defineTable<
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  ID extends Key,
  OUTPUT extends SchemaProvider.InferOutput<SCHEMA> =
    SchemaProvider.InferOutput<SCHEMA>,
>(
  def: Table.Definition<SCHEMA, ID, OUTPUT>,
): Table.Definition<SCHEMA, ID, OUTPUT> {
  return def;
}

export function createTable() {}

export namespace Table {
  export type AnyTable = Table<
    SchemaProvider.AnySchemaProvider,
    any,
    any,
    any,
    any
  >;
  export type AnyDefinition = Definition<
    SchemaProvider.AnySchemaProvider,
    any,
    any
  >;
  export type InferId<TABLE extends AnyTable> =
    TABLE extends Table<any, infer ID, any, any, any> ? ID : never;
  export type InferInput<TABLE extends AnyTable> =
    TABLE extends Table<any, any, infer INPUT, any, any> ? INPUT : never;
  export type InferOutput<TABLE extends AnyTable> =
    TABLE extends Table<any, any, any, infer OUTPUT, any> ? OUTPUT : never;
  export type InferError<TABLE extends AnyTable> =
    TABLE extends Table<any, any, any, any, infer ERROR> ? ERROR : never;
  export type Init<
    SCHEMA extends SchemaProvider.AnySchemaProvider,
    ID extends Key,
    OUTPUT extends SchemaProvider.InferOutput<SCHEMA>,
  > = {
    name: string;
    env: RootDatabase;
    id: (row: OUTPUT) => ID;
    schema: SCHEMA;
  };

  export type Definition<
    SCHEMA extends SchemaProvider.AnySchemaProvider,
    ID extends Key,
    OUTPUT extends SchemaProvider.InferOutput<SCHEMA>,
  > = Omit<Init<SCHEMA, ID, OUTPUT>, "env" | "name">;
}
