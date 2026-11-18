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
import { SchemaProvider } from "./schema-provider";

export class Table<
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  ID extends Key,
  ROW = SchemaProvider.InferOutput<SCHEMA>,
  TX extends boolean = false,
> implements Iterable<ROW> {
  protected _name: string;
  protected _env: RootDatabase;
  protected _schema: SCHEMA;
  protected _idSelector: (row: ROW) => ID;
  protected _db: Database<Readonly<ROW>, ID>;
  protected _$beforeInsert?: Stream<{ id: ID; curr: Readonly<ROW> }>;
  protected _$afterInsert?: Stream<{ id: ID; curr: Readonly<ROW> }>;
  protected _$beforeUpdate?: Stream<{
    id: ID;
    curr: Readonly<ROW>;
    prev: Readonly<ROW>;
  }>;
  protected _$afterUpdate?: Stream<{
    id: ID;
    curr: Readonly<ROW>;
    prev: Readonly<ROW>;
  }>;
  protected _$beforeDelete?: Stream<{ id: ID; prev: Readonly<ROW> }>;
  protected _$afterDelete?: Stream<{ id: ID; prev: Readonly<ROW> }>;
  constructor(init: Table.Init<SCHEMA, ID, ROW>) {
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
  get $beforeInsert(): Source<{ id: ID; curr: Readonly<ROW> }> {
    return Source.from((this._$beforeInsert ??= new Stream()));
  }
  get $afterInsert(): Source<{ id: ID; curr: Readonly<ROW> }> {
    return Source.from((this._$afterInsert ??= new Stream()));
  }
  get $beforeUpdate(): Source<{
    id: ID;
    curr: Readonly<ROW>;
    prev: Readonly<ROW>;
  }> {
    return Source.from((this._$beforeUpdate ??= new Stream()));
  }
  get $afterUpdate(): Source<{
    id: ID;
    curr: Readonly<ROW>;
    prev: Readonly<ROW>;
  }> {
    return Source.from((this._$afterUpdate ??= new Stream()));
  }
  get $beforeDelete(): Source<{ id: ID; prev: Readonly<ROW> }> {
    return Source.from((this._$beforeDelete ??= new Stream()));
  }
  get $afterDelete(): Source<{ id: ID; prev: Readonly<ROW> }> {
    return Source.from((this._$afterDelete ??= new Stream()));
  }
  has(id: ID): boolean {
    return this._db.doesExist(id);
  }
  get(id: ID): Readonly<ROW> | undefined {
    return this._db.get(id);
  }
  all(): Source<ROW> {
    return fromIterable(this._db.getRange()).pipe(map((e) => e.value));
  }
  insert(row: ROW): TX extends true ? boolean : Promise<boolean> {
    const id = this._idSelector(row);

    if (this._isInsideTransaction()) {
      return this._insert(id, row) as never;
    } else {
      return this._db.transaction(() => this._insert(id, row)) as never;
    }
  }
  update(row: ROW): TX extends true ? boolean : Promise<boolean> {
    const id = this._idSelector(row);

    if (this._isInsideTransaction()) {
      return this._update(id, row) as never;
    } else {
      return this._db.transaction(() => this._update(id, row)) as never;
    }
  }
  upsert(row: ROW): TX extends true ? boolean : Promise<boolean> {
    const id = this._idSelector(row);

    if (this._db.doesExist(id)) {
      return this.update(row) as never;
    } else {
      return this.insert(row) as never;
    }
  }
  delete(id: ID): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._delete(id) as never;
    } else {
      return this._db.transaction(() => this._delete(id)) as never;
    }
  }
  protected _isInsideTransaction(): boolean {
    try {
      return this._db.getWriteTxnId() > 0;
    } catch (error) {
      return false;
    }
  }
  protected _insert(id: ID, curr: ROW): boolean {
    this._schema.parse(curr);

    if (this._db.doesExist(id)) return false;

    this._$beforeInsert?.push({ id, curr });

    this._db.put(id, curr).then((r) => {
      if (!r) return;
      this._$afterInsert?.push({ id, curr });
    });

    return true;
  }
  protected _update(id: ID, curr: ROW): boolean {
    this._schema.parse(curr);

    const prev = this._db.get(id);

    if (!prev) return false;

    this._$beforeUpdate?.push({ id, curr, prev });

    this._db.put(id, curr).then((r) => {
      if (r) this._$afterUpdate?.push({ id, curr, prev });
    });

    return true;
  }
  protected _delete(id: ID): boolean {
    const prev = this._db.get(id);
    if (!prev) return false;

    this._$beforeDelete?.push({ id, prev });

    this._db.remove(id).then((r) => {
      if (!r) return;
      this._$afterDelete?.push({ id, prev });
    });
    return true;
  }
  drop(): Promise<void> {
    return this._db.drop();
  }
}

export function defineTable<
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  ID extends Key,
  ROW = SchemaProvider.InferOutput<SCHEMA>,
>(def: Table.Definition<SCHEMA, ID, ROW>): Table.Definition<SCHEMA, ID, ROW> {
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
  export type InferRow<TABLE extends AnyTable> =
    TABLE extends Table<any, any, infer ROW, any> ? ROW : never;
  export type InferId<TABLE extends AnyTable> =
    TABLE extends Table<any, infer ID, any, any> ? ID : never;

  export type Init<
    SCHEMA extends SchemaProvider.AnySchemaProvider,
    ID extends Key,
    ROW = SchemaProvider.InferOutput<SCHEMA>,
  > = {
    name: string;
    env: RootDatabase;
    idSelector: (row: ROW) => ID;
    schema: SCHEMA;
  };

  export type Definition<
    SCHEMA extends SchemaProvider.AnySchemaProvider,
    ID extends Key,
    ROW = SchemaProvider.InferOutput<SCHEMA>,
  > = Omit<Init<SCHEMA, ID, ROW>, "env" | "name">;
}
