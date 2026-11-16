import { Table } from "./src/table";
import {} from "bun";
import { open } from "lmdb";
import { Index } from "./src/table-index";
import { Aggregate } from "./src/aggregate";
import { View } from "./src/view";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}

const env = open("test.mdb", {});
const users = new Table<string, { email: string; name: string; age: number }>({
  env,
  name: "users",
});

const olders = new View({
  env,
  table: users,
  name: "olders",
  keySelector: (v) => v.email,
  map: (v) => v,
  filter: (v) => v.age <= 3,
});
const byAge = new Index({
  env,
  table: users,
  name: "byAge",
  keySelector: (curr) => curr.age,
  valueSelector: (curr) => curr,
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
  users.insert("bbb@g", { email: "bbb@g", name: "ambic", age: 42 });
  users.insert("aaa@g", { email: "aaa@g", name: "sofiane", age: 53 });
  users.insert("ddd@g", { email: "ddd@g", name: "sofiane", age: 40 });
  users.insert("eee@g", { email: "eee@g", name: "zrezem", age: 22 });
  users.insert("vvv@g", { email: "vvv@g", name: "ambic", age: 22 });
  for (let i = 1; i <= 100_000; i++) {
    users.insert(`${i}@g`, { email: `${i}@g`, name: `${i}name`, age: i });
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
