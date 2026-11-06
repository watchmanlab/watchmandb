import z from "zod";
import { Table } from "./table";
import { ZodSchema } from "./zod-schema";

export function from<TABLE extends Table.AnyTable>(table: TABLE) {
  let selected: (keyof Table.InferRow<TABLE>)[];

  // let key: Key[] = [];

  return { select };
  function as(name: string) {}

  function operators<
    COLS extends (keyof Table.InferRow<TABLE>)[],
    C extends COLS[number],
  >(column: C) {
    return {
      eq() {
        return {
          ...logics<COLS>(),
          as,
        };
      },
      lt() {
        return {
          ...logics<COLS>(),
          as,
        };
      },
      lte() {
        return {
          ...logics<COLS>(),
          as,
        };
      },
      gt() {
        return {
          ...logics<COLS>(),
          as,
        };
      },
      gte() {
        return {
          ...logics<COLS>(),
          as,
        };
      },
    };
  }
  function logics<COLS extends (keyof Table.InferRow<TABLE>)[]>() {
    return {
      and<C extends COLS[number]>(column: C) {
        return operators<COLS, C>(column);
      },
      or<C extends COLS[number]>(column: C) {
        return operators<COLS, C>(column);
      },
    };
  }
  function select<COLS extends (keyof Table.InferRow<TABLE>)[]>(
    ...columns: COLS
  ) {
    selected = columns;

    return {
      where: <C extends COLS[number]>(column: C) => operators<COLS, C>(column),
      as,
    };
  }
}

/// a simple DX test
const users = new Table({
  name: "users",
  env: null as any,
  schema: new ZodSchema({ id: z.number(), name: z.string(), age: z.number() }),
});
const requests = new Table({
  name: "requests",
  env: null as any,
  schema: new ZodSchema({
    id: z.number(),
    category: z.union([z.literal("ride"), z.literal("delivery")]),
    description: z.string().optional(),
    location: z.tuple([z.number(), z.number()]),
  }),
});

const q = from(users)
  .select("age", "name")
  .where("name")
  .eq()
  .and("age")
  .gt()
  .as("blablabla");
