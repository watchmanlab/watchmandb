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
import { open } from "lmdb";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}
//
// const dbb = create({
//   path: "test.mdb",
//   tables: {
//     users: new TypBoxSchema({
//       id: Type.Number(),
//       name: Type.String(),
//       age: Type.Number(),
//     }),
//     applies: new ZodSchema({
//       id: z.number(),
//       userId: z.number(),
//       price: z.number(),
//     }),
//   },
//   indexes: {
//     users: [["age"], ["name"]],
//   },
// });

// const { users, applies } = db.tables;

// fromRange(1, 100)
//   .pipe(
//     map((i) =>
//       users.insert({
//         id: i,
//         name: i + "sof",
//         age: i,
//         // locations: [i, i + 1],
//         // contacts: [{ number: i + 1234 }],
//       }),
//     ),
//   )
//   .pipe(resolve(100))
//   .pipe(terminate())
//   .pipe(
//     listen(() => {
//       console.time("");

//       // const source = users.find({ age: 20 });

//       // for (const user of source) {
//       //   console.log(user);
//       // }
//       console.timeEnd("");
//     }),
//   );
const db = open("test.mdb", {}).openDB({ name: "a" });
//just an experiment with range queries
await db.transaction(() => {
  db.put(["a", "foo", 1], "ok sof");
  db.put(["b", "zar", 2], "ok zof");
  db.put(["c", "baz", 3], "ok bof");
  db.put(["d", "zip", 4], "ok mol");
  db.put(["e", "pip", 5], "ok pol");
});

await Bun.sleep(0);

const iter = db.getRange({
  end: ["c"],
});

for (const v of iter) {
  console.log(v.key);
}
