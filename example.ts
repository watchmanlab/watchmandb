import z from "zod";
import { defineTable, Table } from "./src/table";
import { ZodSchema } from "./src/zod-schema";
import {} from "bun";
import { open } from "lmdb";
import { defineView, View } from "./src/view";
import { map } from "@watchmanlab/stream";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}

const usersDef = defineTable({
  schema: new ZodSchema({
    email: z.string(),
    name: z.string(),
    age: z.number(),
  }),
  primaryKey: ["email"],
});

// const byNameDef = defineView({
//   name: "byName",
//   tableDefinition: usersDef,
//   key: "name",
// });

const env = open("test.mdb", {});
const users = new Table({ ...usersDef, env, name: "users" });
const byName = new View({ env, table: users, name: "byName", key: "age" });

await env.transaction(() => {
  users.insert({ email: "bbb@g", name: "ambic", age: 42 });
  users.insert({ email: "aaa@g", name: "sofiane", age: 42 });
  users.insert({ email: "ddd@g", name: "soffinal", age: 40 });
  users.insert({ email: "eee@g", name: "zrezem", age: 22 });
  users.insert({ email: "vvv@g", name: "amb", age: 22 });
});

for (const v of byName.in([22, 40])) {
  // console.log(v);
}

console.time("");

for (const v of byName.in([22, 40])) {
  console.log(v);
}

console.timeEnd("");
