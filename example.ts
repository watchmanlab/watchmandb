import { Table } from "./src/table";
import {} from "bun";
import { open } from "lmdb";
import { Index } from "./src/table-index";
import { Aggregate } from "./src/aggregate";
import { View } from "./src/view";
import { zodSchema } from "./src/zod-schema";
import z from "zod";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}

const env = open("test.mdb", {});
const users = new Table({
  env,
  name: "users",
  idSelector: (row) => row.email,
  schema: zodSchema(
    z.object({ email: z.string(), name: z.string(), age: z.number() }),
  ),
});

const olders = new View({
  env,
  table: users,
  name: "olders",
  // map(row) {
  //   return row;
  // },
  filter: (v) => v.age <= 3,
});

const byAge = new Index({
  env,
  table: users,
  name: "byAge",
  keySelector: (curr) => curr.age,
  includes: (curr) => curr,
});

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

console.time("write");
await env.transaction(() => {
  users.insert({ email: "bbb@g", name: "ambic", age: 42 });
  users.insert({ email: "aaa@g", name: "sofiane", age: 53 });
  users.insert({ email: "ddd@g", name: "sofiane", age: 40 });
  users.insert({ email: "eee@g", name: "zrezem", age: 22 });
  users.insert({ email: "vvv@g", name: "ambic", age: 22 });
  for (let i = 1; i <= 100_000; i++) {
    users.insert({ email: `${i}@g`, name: `${i}name`, age: i });
  }
});
console.timeEnd("write");

for (const v of olders.all()) {
}

console.time("read");

for (const v of olders.all()) {
  console.log(v);
}

console.timeEnd("read");
