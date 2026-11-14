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

export class Table<
  ROW extends Record<string, any>,
  KEY extends Key,
  TX extends boolean = false,
> implements Iterable<ROW> {
  private _name: string;
  private _db: Database<Readonly<ROW>>;
  private _keySelector: (value: ROW) => KEY;
  private _length: number;
  private _$beforeInsert?: Stream<Readonly<ROW>>;
  private _$afterInsert?: Stream<Readonly<ROW>>;
  private _$beforeUpdate?: Stream<{
    curr: Readonly<ROW>;
    prev: Readonly<ROW>;
  }>;
  private _$afterUpdate?: Stream<{
    curr: Readonly<ROW>;
    prev: Readonly<ROW>;
  }>;
  private _$beforeDelete?: Stream<Readonly<ROW>>;
  private _$afterDelete?: Stream<Readonly<ROW>>;

  constructor(init: Table.Init<ROW, KEY>) {
    const { name, env, keySelector } = init;
    this._name = name;
    this._db = env.openDB({
      name: this._name,
      sharedStructuresKey: Symbol.for(this._name),
    });
    this._keySelector = keySelector;
    this._length = 0;
  }
  [Symbol.iterator]() {
    return this.all()[Symbol.iterator]();
  }
  get name(): string {
    return this._name;
  }

  get $beforeInsert(): Source<Readonly<ROW>> {
    return Source.from((this._$beforeInsert ??= new Stream()));
  }
  get $afterInsert(): Source<Readonly<ROW>> {
    return Source.from((this._$afterInsert ??= new Stream()));
  }
  get $beforeUpdate(): Source<{
    curr: Readonly<ROW>;
    prev: Readonly<ROW>;
  }> {
    return Source.from((this._$beforeUpdate ??= new Stream()));
  }
  get $afterUpdate(): Source<{ curr: Readonly<ROW>; prev: Readonly<ROW> }> {
    return Source.from((this._$afterUpdate ??= new Stream()));
  }
  get $beforeDelete(): Source<Readonly<ROW>> {
    return Source.from((this._$beforeDelete ??= new Stream()));
  }
  get $afterDelete(): Source<Readonly<ROW>> {
    return Source.from((this._$afterDelete ??= new Stream()));
  }

  get(primaryKey: KEY) {
    return this._db.get(primaryKey);
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
  update(row: ROW): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._update(row) as never;
    } else {
      return this._db.transaction(() => this._update(row)) as never;
    }
  }
  upsert(row: ROW): TX extends true ? boolean : Promise<boolean> {
    if (this._db.doesExist(this._keySelector(row))) {
      return this.update(row) as never;
    } else {
      return this.insert(row) as never;
    }
  }
  delete(primaryKey: KEY): TX extends true ? boolean : Promise<boolean> {
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

  private _insert(row: ROW): boolean {
    const key = this._keySelector(row);

    if (this._db.doesExist(key)) return false;

    this._$beforeInsert?.push(row);

    this._db.put(key, row).then((r) => {
      if (!r) return;
      this._length++;
      this._$afterInsert?.push(row);
    });

    return true;
  }
  private _update(row: ROW): boolean {
    const key = this._keySelector(row);

    const prev = this._db.get(key);

    if (!prev) return false;

    this._$beforeUpdate?.push({ curr: row, prev });

    this._db.put(key, row).then((r) => {
      if (r) this._$afterUpdate?.push({ curr: row, prev });
    });

    return true;
  }
  private _delete(primaryKey: KEY): boolean {
    const prev = this._db.get(primaryKey);
    if (!prev) return false;

    this._$beforeDelete?.push(prev);

    this._db.remove(primaryKey).then((r) => {
      if (!r) return;
      this._length--;
      this._$afterDelete?.push(prev);
    });
    return true;
  }
  clear(): Terminate<Source<TerminateReason>> {
    return fromIterable(this._db.getKeys())
      .pipe(listen((key: KEY) => this.delete(key)))
      .pipe(terminate());
  }
}

export function defineTable<ROW extends Record<string, any>, KEY extends Key>(
  config: Table.Definition<ROW, KEY>,
): Table.Definition<ROW, KEY> {
  return config;
}

export namespace Table {
  export type AnyTable = Table<any, any, any>;
  export type AnyDefinition = Definition<any, any>;

  export type InferRow<TABLE extends AnyTable> =
    TABLE extends Table<infer R, any, any> ? R : never;
  export type InferPrimaryKey<TABLE extends AnyTable> =
    TABLE extends Table<any, infer P, any> ? P : never;
  export type InferTable<DEF extends AnyDefinition> =
    DEF extends Definition<infer ROW, infer KEY> ? Table<ROW, KEY> : never;

  export type Init<ROW extends Record<string, any>, KEY extends Key> = {
    name: string;
    env: RootDatabase;
    keySelector: (value: ROW) => KEY;
  };

  export type Definition<
    ROW extends Record<string, any>,
    KEY extends Key,
  > = Omit<Init<ROW, KEY>, "env" | "name">;
}
