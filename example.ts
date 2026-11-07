import z from "zod";
import { defineTable, Table } from "./src/table";
import { ZodSchema } from "./src/zod-schema";
import {} from "bun";
import { open } from "lmdb";
import { Index } from "./src/index";
import { defineView, View } from "./src/view";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}

const usersDef = defineTable({
  name: "users",
  schema: new ZodSchema({ id: z.number(), name: z.string(), age: z.number() }),
});

const byNameDef = defineView({
  name: "byName",
  tableDefinition: usersDef,
  keySelector(row) {
    return [row.name, row.age] as [name: string, age: number];
  },
  map: (row) => row,
});

const env = open("test.mdb", {});
const users = usersDef.init(env);
const byName = byNameDef.init(env, users);

await users.insert({ id: 0, name: "ambic", age: 42 });
await users.insert({ id: 1, name: "sofiane", age: 42 });
await users.insert({ id: 2, name: "soffinal", age: 40 });
await users.insert({ id: 3, name: "zrezem", age: 22 });
await users.insert({ id: 4, name: "amb", age: 22 });

console.time("");
for (const v of byName.startWith("amb")) {
  console.log(v);
}
console.timeEnd("");
