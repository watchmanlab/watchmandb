import z from "zod";
import { create } from "./src/db";
import { ZodSchema } from "./src/zod-schema";
import { TypBoxSchema } from "./src";
import Type from "typebox";
import {} from "bun";
import {
  filter,
  flat$,
  fromRange,
  last,
  listen,
  map,
  of,
  resolve,
  scanArray,
  terminate,
  toConsole,
} from "@watchmanlab/stream";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}
//
const db = create({
  path: "test.mdb",
  tables: {
    users: new TypBoxSchema({
      id: Type.Number(),
      name: Type.String(),
      age: Type.Number(),
      locations: Type.Optional(Type.Tuple([Type.Number(), Type.Number()])),
    }),
    applies: new ZodSchema({
      id: z.number(),
      userId: z.number(),
      price: z.number(),
    }),
  },
  indexes: {
    users: [["name", "age"], ["age"], ["locations"]],
    applies: [["price"], ["userId", "price"]],
  },
});

const { users, applies } = db.tables;

fromRange(1, 101)
  .pipe(
    map((i) =>
      users.insert({ id: i, name: i + "sof", age: i, locations: [i, i + 1] }),
    ),
  )
  .pipe(resolve(100))
  .pipe(terminate())
  .pipe(
    listen(() => {
      const source = users.find({
        // age: 10,
        // name: "10sof",
        locations: [10, 11],
      });

      for (const user of source) {
        console.log(user);
      }
    }),
  );
