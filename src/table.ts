import {
  batch,
  filter,
  fromIterable,
  last,
  listen,
  map,
  Signal,
  Source,
  Stream,
  take,
  takeUntil,
  tap,
  tapTerminate,
  terminate,
  toConsole,
} from "@watchmanlab/stream";
import {
  getLastVersion,
  type Database,
  type Key,
  type RootDatabase,
} from "lmdb";
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
  protected _$beforeInsert?: Stream<{
    id: ID;
    row: OUTPUT;
    commit: () => void;
    rollback: (error: unknown) => void;
  }>;
  protected _$afterInsert?: Stream<{ id: ID; row: OUTPUT }>;

  protected _$beforeDelete?: Stream<{
    id: ID;
    row: OUTPUT;
    commit: () => void;
    rollback: (error: unknown) => void;
  }>;
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
  get $beforeInsert(): Source<{
    id: ID;
    row: OUTPUT;
    commit: () => void;
    rollback: (error: unknown) => void;
  }> {
    return Source.from((this._$beforeInsert ??= new Stream()));
  }
  get $afterInsert(): Source<{ id: ID; row: OUTPUT }> {
    return Source.from((this._$afterInsert ??= new Stream()));
  }
  get $beforeDelete(): Source<{
    id: ID;
    row: OUTPUT;
    commit: () => void;
    rollback: (error: unknown) => void;
  }> {
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

  async insert(
    row: INPUT,
  ): Promise<
    Result<
      ID,
      | { code: "validation"; detail: ERROR }
      | { code: "exist" }
      | { code: "rollback"; detail: unknown }
    >
  > {
    const result = this._schema.parse(row);

    if (!result.ok) return fail({ code: "validation", detail: result.error });

    const id = this._idSelector(result.data);

    if (this._db.doesExist(id)) return fail({ code: "exist" });

    if (this._$beforeInsert) {
      const consumersCount = this._$beforeInsert.consumersCount;
      const commit = new Stream<void>();
      const rollback = new Signal<unknown>();

      commit
        .pipe(takeUntil(rollback))
        .pipe(batch(consumersCount))
        .pipe(filter((b) => b.length === consumersCount))
        .pipe(listen(() => this._db.put(id, row)));

      this._$beforeInsert.push({
        id,
        row,
        commit: () => commit.push(),
        rollback: (error) => rollback.push(error),
      });
    } else {
      this._db.put(id, row);
    }

    this._$afterInsert?.push({ id, row: row });

    return success(id);
  }
  async update(row: INPUT): Promise<Result<ID, ERROR | "not-exist">> {
    const result = this._schema.parse(row);
    if (!result.ok) return fail(result.error);
    const id = this._idSelector(result.data);

    if (!this._db.doesExist(id)) return fail("not-exist");

    this._db.batch(() => {
      this._$beforeInsert?.push({ id, row: row });
      this._db.put(id, row).then((r) => {
        if (r) this._$afterInsert?.push({ id, row: row });
      });
    });

    return success(id);
  }
  upsert(row: INPUT): Result<ID, ERROR> {
    const result = this._schema.parse(row);
    if (!result.ok) return fail(result.error);
    const id = this._idSelector(result.data);
    this._db.batch(() => {
      this._$beforeInsert?.push({ id, row: row });

      this._db.put(id, row).then((r) => {
        if (r) this._$afterInsert?.push({ id, row: row });
        return r;
      });
    });

    return success(id);
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
