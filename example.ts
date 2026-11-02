import z from "zod";
import { create } from "./src/db";
import { ZodSchema } from "./src/zod-schema";
import { TypBoxSchema } from ".";
import Type from "typebox";
import {
  fromRange,
  listen,
  map,
  resolve,
  terminate,
} from "@watchmanlab/stream";
import {} from "bun";

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
    users: [["name", "locations"]],
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
    listen((s) => {
      users.find({
        // age: 10,
        name: "3sof",
        locations: [3, 4],
      });
      console.time("");
      const source = users.find({
        // age: 10,
        name: "3sof",
        locations: [3, 4],
      });

      for (const user of source) {
        console.log(user);
      }
      console.timeEnd("");
    }),
  );
