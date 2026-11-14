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
import { SystemTable } from "./system-table";

export abstract class SourceTable<
  ROW extends Record<string, any>,
  KEY extends Key,
> implements Iterable<ROW> {
  protected _name: string;
  protected _env: RootDatabase;
  protected _db: Database<Readonly<ROW>>;

  protected _$beforeInsert?: Stream<Readonly<ROW>>;
  protected _$afterInsert?: Stream<Readonly<ROW>>;
  protected _$beforeUpdate?: Stream<{
    curr: Readonly<ROW>;
    prev: Readonly<ROW>;
  }>;
  protected _$afterUpdate?: Stream<{
    curr: Readonly<ROW>;
    prev: Readonly<ROW>;
  }>;
  protected _$beforeDelete?: Stream<Readonly<ROW>>;
  protected _$afterDelete?: Stream<Readonly<ROW>>;

  constructor(init: SourceTable.Init) {
    const { name, env } = init;
    this._name = name;
    this._env = env;
    this._db = env.openDB({
      name: this._name,
      sharedStructuresKey: Symbol.for(this._name),
    });

    SystemTable.getInstance(env).register(this);
  }
  [Symbol.iterator]() {
    return this.all()[Symbol.iterator]();
  }
  get name(): string {
    return this._name;
  }
  get stats(): SystemTable.Stats {
    return SystemTable.getInstance(this._env).getStats(this);
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

  get(key: KEY) {
    return this._db.get(key);
  }
  all(): Source<ROW> {
    return fromIterable(this._db.getRange()).pipe(map((e) => e.value));
  }
}

export namespace SourceTable {
  export type AnySourceTable = SourceTable<any, any>;

  export type InferRow<TABLE extends AnySourceTable> =
    TABLE extends SourceTable<infer R, any> ? R : never;
  export type InferPrimaryKey<TABLE extends AnySourceTable> =
    TABLE extends SourceTable<any, infer P> ? P : never;

  export type Init = {
    name: string;
    env: RootDatabase;
  };
}
