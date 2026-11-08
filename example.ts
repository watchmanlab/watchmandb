import z from "zod";
import { defineTable } from "./src/table";
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
  schema: new ZodSchema({ id: z.number(), name: z.string(), age: z.number() }),
});

const byNameDef = defineIndex({
  name: "byName",
  tableDefinition: usersDef,
  key: ["name"],
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
for (const v of byName.view.startWith("amb").pipe(map((id) => users.get(id)))) {
  console.log(v);
}
console.timeEnd("");
