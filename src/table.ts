import { fromIterable, map, Source, Stream } from "@watchmanlab/stream";
import type { Database, Key, RootDatabase } from "lmdb";
import { SchemaProvider } from "./schema-provider";

export class Table<
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  ID extends Key,
  INPUT extends SchemaProvider.InferInput<SCHEMA> =
    SchemaProvider.InferInput<SCHEMA>,
  OUTPUT extends SchemaProvider.InferOutput<SCHEMA> =
    SchemaProvider.InferOutput<SCHEMA>,
> implements Iterable<{ id: ID; row: OUTPUT }> {
  protected _name: string;
  protected _env: RootDatabase;
  protected _schema: SCHEMA;
  protected _idSelector: (row: OUTPUT) => ID;
  protected _db: Database<OUTPUT, ID>;
  protected _$beforeInsert?: Stream<{ id: ID; curr: OUTPUT }>;
  protected _$afterInsert?: Stream<{ id: ID; curr: OUTPUT }>;
  protected _$beforeUpdate?: Stream<{
    id: ID;
    curr: OUTPUT;
    prev: OUTPUT;
  }>;
  protected _$afterUpdate?: Stream<{
    id: ID;
    curr: OUTPUT;
    prev: OUTPUT;
  }>;
  protected _$beforeDelete?: Stream<{ id: ID; prev: OUTPUT }>;
  protected _$afterDelete?: Stream<{ id: ID; prev: OUTPUT }>;
  constructor(init: Table.Init<SCHEMA, ID, OUTPUT>) {
    const { name, env, schema, idSelector } = init;
    this._name = name;
    this._env = env;
    this._schema = schema;
    this._idSelector = idSelector;
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
  get $beforeInsert(): Source<{ id: ID; curr: OUTPUT }> {
    return Source.from((this._$beforeInsert ??= new Stream()));
  }
  get $afterInsert(): Source<{ id: ID; curr: OUTPUT }> {
    return Source.from((this._$afterInsert ??= new Stream()));
  }
  get $beforeUpdate(): Source<{
    id: ID;
    curr: OUTPUT;
    prev: OUTPUT;
  }> {
    return Source.from((this._$beforeUpdate ??= new Stream()));
  }
  get $afterUpdate(): Source<{
    id: ID;
    curr: OUTPUT;
    prev: OUTPUT;
  }> {
    return Source.from((this._$afterUpdate ??= new Stream()));
  }
  get $beforeDelete(): Source<{ id: ID; prev: OUTPUT }> {
    return Source.from((this._$beforeDelete ??= new Stream()));
  }
  get $afterDelete(): Source<{ id: ID; prev: OUTPUT }> {
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
  insert(row: INPUT): void {
    const parsed = this._schema.parse(row);
    const id = this._idSelector(parsed);
    if (this._isInsideTransaction()) {
      this._insert(id, parsed);
    } else {
      this._db.transaction(() => this._insert(id, parsed));
    }
  }
  update(row: OUTPUT): void {
    const parsed = this._schema.parse(row);

    const id = this._idSelector(parsed);

    if (this._isInsideTransaction()) {
      this._update(id, parsed);
    } else {
      this._db.transaction(() => this._update(id, parsed));
    }
  }
  upsert(row: INPUT | OUTPUT): void {
    const parsed = this._schema.parse(row);

    const id = this._idSelector(parsed);

    if (this._db.doesExist(id)) {
      this.update(parsed);
    } else {
      this.insert(parsed);
    }
  }
  delete(id: ID): void {
    if (this._isInsideTransaction()) {
      this._delete(id);
    } else {
      this._db.transaction(() => this._delete(id));
    }
  }
  protected _isInsideTransaction(): boolean {
    try {
      return this._db.getWriteTxnId() > 0;
    } catch (error) {
      return false;
    }
  }
  protected _insert(id: ID, curr: OUTPUT): void {
    if (this._db.doesExist(id)) return;

    this._$beforeInsert?.push({ id, curr });

    this._db.put(id, curr).then((r) => {
      if (!r) return;
      this._$afterInsert?.push({ id, curr });
    });

    return;
  }
  protected _update(id: ID, curr: OUTPUT): void {
    const prev = this._db.get(id);

    if (!prev) return;

    this._$beforeUpdate?.push({ id, curr, prev });

    this._db.put(id, curr).then((r) => {
      if (r) this._$afterUpdate?.push({ id, curr, prev });
    });

    return;
  }
  protected _delete(id: ID): void {
    const prev = this._db.get(id);
    if (!prev) return;

    this._$beforeDelete?.push({ id, prev });

    this._db.remove(id).then((r) => {
      if (!r) return;
      this._$afterDelete?.push({ id, prev });
    });
    return;
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
  export type AnyTable = Table<SchemaProvider.AnySchemaProvider, any, any, any>;
  export type AnyDefinition = Definition<
    SchemaProvider.AnySchemaProvider,
    any,
    any
  >;
  export type InferInput<TABLE extends AnyTable> =
    TABLE extends Table<any, any, infer INPUT, any> ? INPUT : never;
  export type InferOutput<TABLE extends AnyTable> =
    TABLE extends Table<any, any, any, infer OUTPUT> ? OUTPUT : never;
  export type InferId<TABLE extends AnyTable> =
    TABLE extends Table<any, infer ID, any, any> ? ID : never;

  export type Init<
    SCHEMA extends SchemaProvider.AnySchemaProvider,
    ID extends Key,
    OUTPUT extends SchemaProvider.InferOutput<SCHEMA>,
  > = {
    name: string;
    env: RootDatabase;
    idSelector: (row: OUTPUT) => ID;
    schema: SCHEMA;
  };

  export type Definition<
    SCHEMA extends SchemaProvider.AnySchemaProvider,
    ID extends Key,
    OUTPUT extends SchemaProvider.InferOutput<SCHEMA>,
  > = Omit<Init<SCHEMA, ID, OUTPUT>, "env" | "name">;
}
