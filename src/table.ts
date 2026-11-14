import {
  fromIterable,
  listen,
  Source,
  Terminate,
  terminate,
  TerminateReason,
} from "@watchmanlab/stream";
import type { Key } from "lmdb";
import { SourceTable } from "./source-table";

export class Table<
  ROW extends Record<string, any>,
  KEY extends Key,
  TX extends boolean = false,
> extends SourceTable<ROW, KEY> {
  constructor(private init: Table.Init<ROW, KEY>) {
    super(init);
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
    if (this._db.doesExist(this.init.keySelector(row))) {
      return this.update(row) as never;
    } else {
      return this.insert(row) as never;
    }
  }
  delete(key: KEY): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._delete(key) as never;
    } else {
      return this._db.transaction(() => this._delete(key)) as never;
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
    const key = this.init.keySelector(row);

    if (this._db.doesExist(key)) return false;

    this._$beforeInsert?.push(row);

    this._db.put(key, row).then((r) => {
      if (!r) return;
      this._$afterInsert?.push(row);
    });

    return true;
  }
  private _update(row: ROW): boolean {
    const key = this.init.keySelector(row);

    const prev = this._db.get(key);

    if (!prev) return false;

    this._$beforeUpdate?.push({ curr: row, prev });

    this._db.put(key, row).then((r) => {
      if (r) this._$afterUpdate?.push({ curr: row, prev });
    });

    return true;
  }
  private _delete(key: KEY): boolean {
    const prev = this._db.get(key);
    if (!prev) return false;

    this._$beforeDelete?.push(prev);

    this._db.remove(key).then((r) => {
      if (!r) return;
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

  export type Init<
    ROW extends Record<string, any>,
    KEY extends Key,
  > = SourceTable.Init & {
    keySelector: (value: ROW) => KEY;
  };

  export type Definition<
    ROW extends Record<string, any>,
    KEY extends Key,
  > = Omit<Init<ROW, KEY>, "env" | "name">;
}
