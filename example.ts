import z from "zod";
import { defineTable, Table } from "./src/table";
import { ZodSchema } from "./src/zod-schema";
import {} from "bun";
import { open } from "lmdb";
import { defineView } from "./src/view";
import { defineIndex } from "./src/index";
import { map } from "@watchmanlab/stream";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}

const usersDef = defineTable({
  name: "users",
  schema: new ZodSchema({
    email: z.string(),
    name: z.string(),
    age: z.number(),
  }),
  pk: ["email"],
});

// const byNameDef = defineIndex({
//   name: "byName",
//   tableDefinition: usersDef,
//   key: ["name"],
// });

const env = open("test.mdb", {});
const users = new Table({ ...usersDef, env });
// const byName = byNameDef.init(env, users);

await users.insert({ email: "bbb@g", name: "ambic", age: 42 });
await users.insert({ email: "aaa@g", name: "sofiane", age: 42 });
await users.insert({ email: "ddd@g", name: "soffinal", age: 40 });
await users.insert({ email: "eee@g", name: "zrezem", age: 22 });
await users.insert({ email: "vvv@g", name: "amb", age: 22 });

console.time("");

console.log(users.get(""));

console.timeEnd("");
