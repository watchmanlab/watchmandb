import z from "zod";
import { Table } from "./src/table";
import { ZodSchema } from "./src/zod-schema";
import {} from "bun";
import { open } from "lmdb";
import { Index } from "./src/index";

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

const byName = new Index({ name: "byName", env, table: users, key: ["name"] });

await users.insert({ id: 1, name: "sofiane", age: 42 });
await users.insert({ id: 2, name: "soffinal", age: 40 });

for (const v of byName.gt("soffinal")) {
  console.log(v);
}
