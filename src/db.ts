import { Consumable, Consumer, Source, Stream } from "@watchmanlab/stream";
import { Database, Key, RootDatabase } from "lmdb";

export class Write<INPUT extends Consumable<Command>> extends Source<
  Promise<boolean>
> {
  private static dbs = new Map<string, Database>();
  constructor(
    readonly $input: INPUT,
    private env: RootDatabase,
  ) {
    super();
  }
  override consume(
    handler: Consumer.Handler<Promise<boolean>>,
    options?: Consumer.Options<Promise<boolean>> | undefined,
  ): Consumer<Promise<boolean>> {
    const { next, terminate, ...rest } = options ?? {};

    const output$ = new Consumer(handler, {
      ...rest,
      next: (c) => {
        input$.next();
        next?.(c);
      },
      terminate: (c, r) => {
        input$.terminate(r);
        terminate?.(c, r);
      },
    });

    const input$ = this.$input.consume(
      (_, cmd) => {
        const db =
          Write.dbs.get(cmd.table) ??
          Write.dbs
            .set(cmd.table, this.env.openDB({ name: cmd.table }))
            .get(cmd.table)!;

        switch (cmd.type) {
          case "insert":
            db.doesExist(cmd.id)
              ? output$.push(Promise.resolve(false))
              : output$.push(db.put(cmd.id, cmd.payload));
            break;
          case "update":
            db.doesExist(cmd.id)
              ? output$.push(db.put(cmd.id, cmd.payload))
              : output$.push(Promise.resolve(false));
            break;
          case "upsert":
            output$.push(db.put(cmd.id, cmd.payload));
            break;
          case "delete":
            output$.push(db.remove(cmd.id));
            break;
        }
      },
      { terminate: (_, r) => output$.terminate(r) },
    );

    return output$;
  }
}

export function write<INPUT extends Consumable<Command>>(env: RootDatabase) {
  return ($input: INPUT) => new Write($input, env);
}
export function createWriter() {
  return new Stream<Command>();
}
export type Command =
  | { type: "insert"; table: string; id: Key; payload: any }
  | { type: "update"; table: string; id: Key; payload: any }
  | { type: "upsert"; table: string; id: Key; payload: any }
  | { type: "delete"; table: string; id: Key };
