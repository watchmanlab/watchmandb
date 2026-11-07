import z from "zod";
import { Table } from "./src/table";
import { ZodSchema } from "./src/zod-schema";
import {} from "bun";
import { open } from "lmdb";
import { Index } from "./src/index";
import { View } from "./src/view";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}

const env = open("test.mdb", {});

const users = new Table({
  name: "users",
  env,
  schema: new ZodSchema({ id: z.number(), name: z.string(), age: z.number() }),
});

const byName = new View({
  name: "byName",
  env,
  table: users,
  keySelector(row) {
    return ["name", "age"];
  },
  map: (row) => row,
});

await users.insert({ id: 1, name: "sofiane", age: 42 });
await users.insert({ id: 2, name: "soffinal", age: 40 });
await users.insert({ id: 3, name: "zrezem", age: 22 });
await users.insert({ id: 4, name: "amb", age: 22 });

for (const v of byName.all()) {
  console.log(v);
}
