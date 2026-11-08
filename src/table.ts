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
import type { Database, GetOptions, Key, RootDatabase } from "lmdb";
import type { SchemaProvider } from "./schema-provider";

export class Table<
  NAME extends string,
  SCHEMA extends SchemaProvider.AnySchemaProvider,
  TX extends boolean = false,
  ROW extends SchemaProvider.InferOutput<SCHEMA> =
    SchemaProvider.InferOutput<SCHEMA>,
> {
  private _name: NAME;
  private _db: Database<Readonly<ROW>>;
  private _schema: SCHEMA;
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

  constructor(init: Table.Init<NAME, SCHEMA>) {
    this._name = init.name;
    this._db = init.env.openDB({
      name: this._name,
      sharedStructuresKey: Symbol.for(this._name),
    });
    this._schema = init.schema;
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
  get(id: ROW["id"]): ROW | undefined {
    return this._db.get(id);
  }
  all(limit?: number): Source<ROW> {
    return fromIterable(this._db.getRange({ limit })).pipe(map((e) => e.value));
  }
  get isEmpty(): boolean {
    return this._db.getRange()[Symbol.iterator]().next().done == true;
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
    row: Partial<Omit<ROW, "id">> & Pick<ROW, "id">,
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
  delete(id: ROW["id"]): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._delete(id) as never;
    } else {
      return this._db.transaction(() => this._delete(id)) as never;
    }
  }
  private _isInsideTransaction(): boolean {
    try {
      return this._db.getWriteTxnId() > 0;
    } catch (error) {
      return false;
    }
  }
  private _insert(row: ROW): boolean {
    if (this._db.doesExist(row.id)) return false;

    const parsed = this._schema.parse(row);

    this._$beforeInsert?.push(parsed);

    this._db.put(parsed.id, parsed).then((r) => {
      if (!r) return;
      this._length++;
      this._$afterInsert?.push(parsed);
    });

    return true;
  }
  private _update(row: Partial<Omit<ROW, "id">> & Pick<ROW, "id">): boolean {
    const oldRow = this._db.get(row.id);
    if (!oldRow) return false;

    const parsed = this._schema.parse({ ...oldRow, ...row });

    this._$beforeUpdate?.push({ newRow: parsed, oldRow });

    this._db.put(parsed.id, parsed).then((r) => {
      if (r) this._$afterUpdate?.push({ newRow: parsed, oldRow });
    });

    return true;
  }
  private _delete(id: ROW["id"]): boolean {
    const oldRow = this._db.get(id);
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
      .pipe(listen((key) => this.delete(key)))
      .pipe(terminate());
  }
}

export function defineTable<
  NAME extends string,
  SCHEMA extends SchemaProvider.AnySchemaProvider,
>(config: { name: NAME; schema: SCHEMA }): Table.Definition<NAME, SCHEMA> {
  const { name, schema } = config;
  return {
    name,
    schema,
    init: (env: RootDatabase) => new Table({ env, name, schema }),
  };
}

export namespace Table {
  export type AnyTable = Table<any, any, any, any>;
  export type AnyDefinition = Definition<any, any>;
  export type Row = Record<string, any> & { id: Key };
  export type InferRow<TABLE extends AnyTable> =
    TABLE extends Table<any, any, any, infer R> ? R : never;
  export type InferName<TABLE extends AnyTable> =
    TABLE extends Table<infer N, any, any, any> ? N : never;
  export type InferTable<DEF extends Definition<any, any>> =
    DEF extends Definition<infer NAME, infer SCHEMA>
      ? Table<NAME, SCHEMA>
      : never;

  export type Init<
    NAME extends string,
    SCHEMA extends SchemaProvider.AnySchemaProvider,
  > = {
    name: NAME;
    env: RootDatabase;
    schema: SCHEMA;
  };

  export interface Definition<
    NAME extends string,
    SCHEMA extends SchemaProvider.AnySchemaProvider,
  > {
    name: NAME;
    schema: SCHEMA;
    init(env: RootDatabase): Table<NAME, SCHEMA>;
  }
}
