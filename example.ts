import z from "zod";
import { defineTable, Table } from "./src/table";
import { ZodSchema } from "./src/zod-schema";
import {} from "bun";
import { open } from "lmdb";
import { Index } from "./src/table-index";
import { Aggregate } from "./src/aggregate";
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

const env = open("test.mdb", {});
const users = new Table({ ...usersDef, env, name: "users" });
const byAge = new Index({ env, table: users, name: "byAge", key: "age" });
const maxAgeAgg = new Aggregate({
  env,
  table: users,
  name: "count",
  seed: { maxAge: 0 },
  groupBy(row) {
    return row.name;
  },
  reducer(acc, curr, prev) {
    return {
      maxAge: Math.max(acc.maxAge, curr?.age ?? 0),
    };
  },
});

await env.transaction(() => {
  users.insert({ email: "bbb@g", name: "ambic", age: 42 });
  users.insert({ email: "aaa@g", name: "sofiane", age: 53 });
  users.insert({ email: "ddd@g", name: "sofiane", age: 40 });
  users.insert({ email: "eee@g", name: "zrezem", age: 22 });
  users.insert({ email: "vvv@g", name: "ambic", age: 22 });
});

console.time("");

for (const v of maxAgeAgg.all()) {
  console.log(v);
}

console.timeEnd("");
