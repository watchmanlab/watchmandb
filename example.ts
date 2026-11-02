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
      contacts: Type.Array(Type.Object({ number: Type.Number() })),
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

fromRange(1, 101)
  .pipe(
    map((i) =>
      users.insert({
        id: i,
        name: i + "sof",
        age: i,
        locations: [i, i + 1],
        contacts: [{ number: i + 1234 }],
      }),
    ),
  )
  .pipe(resolve(100))
  .pipe(terminate())
  .pipe(
    listen(() => {
      console.time("");

      const source = users.find(
        { contacts: [{ number: 1235 }] },
        { forceFullScan: true },
      );

      for (const user of source) {
        console.log(user);
      }
      console.timeEnd("");
    }),
  );
