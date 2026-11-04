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
    }),
    applies: new ZodSchema({
      id: z.number(),
      userId: z.number(),
      price: z.number(),
    }),
  },
  indexes: {
    users: [["age"], ["name"]],
  },
});

const { users, applies } = db.tables;

fromRange(1, 100)
  .pipe(
    map((i) =>
      users.insert({
        id: i,
        name: i + "sof",
        age: i,
        // locations: [i, i + 1],
        // contacts: [{ number: i + 1234 }],
      }),
    ),
  )
  .pipe(resolve(100))
  .pipe(terminate())
  .pipe(
    listen(() => {
      console.time("");

      // const source = users.find({ age: 20 });

      // for (const user of source) {
      //   console.log(user);
      // }
      console.timeEnd("");
    }),
  );

//just an experiment with range queries
await db.env.transaction(() => {
  db.env.put([10, "foo", 1], "ok sof");
  db.env.put([20, "zar", 2], "ok sof");
  db.env.put([20, "baz", 3], "ok sof");
  db.env.put([30, "zip", 4], "ok mol");
});

const iter = db.env.getRange({
  start: [20],
  end: ["\x00"],
  // start: undefined,
  // end: [20, "\xff"],
  // inclusiveEnd: true,
});
for (const v of iter) {
  console.log(v.key);
}
