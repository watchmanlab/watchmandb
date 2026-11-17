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
  KEY extends Key,
  VALUE,
  TX extends boolean = false,
> implements Iterable<VALUE> {
  protected _name: string;
  protected _env: RootDatabase;
  protected _db: Database<Readonly<VALUE>, KEY>;
  // protected _events?: Stream<{type:"";  key: KEY; curr: Readonly<VALUE> }>
  protected _$beforeInsert?: Stream<{ key: KEY; curr: Readonly<VALUE> }>;
  protected _$afterInsert?: Stream<{ key: KEY; curr: Readonly<VALUE> }>;
  protected _$beforeUpdate?: Stream<{
    key: KEY;
    curr: Readonly<VALUE>;
    prev: Readonly<VALUE>;
  }>;
  protected _$afterUpdate?: Stream<{
    key: KEY;
    curr: Readonly<VALUE>;
    prev: Readonly<VALUE>;
  }>;
  protected _$beforeDelete?: Stream<{ key: KEY; prev: Readonly<VALUE> }>;
  protected _$afterDelete?: Stream<{ key: KEY; prev: Readonly<VALUE> }>;
  constructor(init: Table.Init) {
    const { name, env } = init;
    this._name = name;
    this._env = env;
    this._db = env.openDB({
      name: this._name,
      sharedStructuresKey: Symbol.for(this._name),
    });
  }

  [Symbol.iterator]() {
    return this.all()[Symbol.iterator]();
  }
  get name(): string {
    return this._name;
  }

  get $beforeInsert(): Source<{ key: KEY; curr: Readonly<VALUE> }> {
    return Source.from((this._$beforeInsert ??= new Stream()));
  }
  get $afterInsert(): Source<{ key: KEY; curr: Readonly<VALUE> }> {
    return Source.from((this._$afterInsert ??= new Stream()));
  }
  get $beforeUpdate(): Source<{
    key: KEY;
    curr: Readonly<VALUE>;
    prev: Readonly<VALUE>;
  }> {
    return Source.from((this._$beforeUpdate ??= new Stream()));
  }
  get $afterUpdate(): Source<{
    key: KEY;
    curr: Readonly<VALUE>;
    prev: Readonly<VALUE>;
  }> {
    return Source.from((this._$afterUpdate ??= new Stream()));
  }
  get $beforeDelete(): Source<{ key: KEY; prev: Readonly<VALUE> }> {
    return Source.from((this._$beforeDelete ??= new Stream()));
  }
  get $afterDelete(): Source<{ key: KEY; prev: Readonly<VALUE> }> {
    return Source.from((this._$afterDelete ??= new Stream()));
  }

  get(key: KEY) {
    return this._db.get(key);
  }
  all(): Source<VALUE> {
    return fromIterable(this._db.getRange()).pipe(map((e) => e.value));
  }
  insert(key: KEY, value: VALUE): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._insert(key, value) as never;
    } else {
      return this._db.transaction(() => this._insert(key, value)) as never;
    }
  }
  update(key: KEY, value: VALUE): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._update(key, value) as never;
    } else {
      return this._db.transaction(() => this._update(key, value)) as never;
    }
  }
  upsert(key: KEY, value: VALUE): TX extends true ? boolean : Promise<boolean> {
    if (this._db.doesExist(key)) {
      return this.update(key, value) as never;
    } else {
      return this.insert(key, value) as never;
    }
  }
  delete(key: KEY): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._delete(key) as never;
    } else {
      return this._db.transaction(() => this._delete(key)) as never;
    }
  }
  protected _isInsideTransaction(): boolean {
    try {
      return this._db.getWriteTxnId() > 0;
    } catch (error) {
      return false;
    }
  }
  protected _insert(key: KEY, curr: VALUE): boolean {
    if (this._db.doesExist(key)) return false;

    this._$beforeInsert?.push({ key, curr });

    this._db.put(key, curr).then((r) => {
      if (!r) return;
      this._$afterInsert?.push({ key, curr });
    });

    return true;
  }
  protected _update(key: KEY, curr: VALUE): boolean {
    const prev = this._db.get(key);

    if (!prev) return false;

    this._$beforeUpdate?.push({ key, curr, prev });

    this._db.put(key, curr).then((r) => {
      if (r) this._$afterUpdate?.push({ key, curr, prev });
    });

    return true;
  }
  protected _delete(key: KEY): boolean {
    const prev = this._db.get(key);
    if (!prev) return false;

    this._$beforeDelete?.push({ key, prev });

    this._db.remove(key).then((r) => {
      if (!r) return;
      this._$afterDelete?.push({ key, prev });
    });
    return true;
  }
  clear(): Terminate<Source<TerminateReason>> {
    return fromIterable(this._db.getKeys())
      .pipe(listen((key: KEY) => this.delete(key)))
      .pipe(terminate());
  }
}

export namespace Table {
  export type AnyTable = Table<any, any, any>;
  export type InferValue<TABLE extends AnyTable> =
    TABLE extends Table<any, infer V, any> ? V : never;
  export type InferKey<TABLE extends AnyTable> =
    TABLE extends Table<infer K, any, any> ? K : never;

  export type Init = {
    name: string;
    env: RootDatabase;
  };
}
