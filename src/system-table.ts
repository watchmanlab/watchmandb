import { Database, RootDatabase } from "lmdb";

import { listen } from "@watchmanlab/stream";
import { Table } from "./table";

export class SystemTable {
  private _db: Database<SystemTable.Stats, string>;
  private constructor(env: RootDatabase) {
    this._db = env.openDB({ name: "__system" });
  }
  register(table: Table.AnyTable) {
    table.$beforeInsert.pipe(
      listen(() => {
        const stats = this._db.get(table.name) ?? { size: 0 };
        this._db.put(table.name, { size: stats.size + 1 });
      }),
    );
    table.$beforeDelete.pipe(
      listen(() => {
        const stats = this._db.get(table.name) ?? { size: 1 };
        this._db.put(table.name, { size: stats.size - 1 });
      }),
    );
  }
  getStats(table: Table.AnyTable): SystemTable.Stats {
    return this._db.get(table.name) ?? { size: 0 };
  }
  private static instances: Map<RootDatabase, SystemTable>;
  static getInstance(env: RootDatabase) {
    let instance = SystemTable.instances.get(env);

    if (!instance) {
      instance = new SystemTable(env);
      SystemTable.instances.set(env, instance);
    }
    return instance;
  }
}

export namespace SystemTable {
  export type Stats = { size: number };
}
