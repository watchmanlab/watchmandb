import { Table } from "./src/table";
import {} from "bun";
import { open } from "lmdb";
import { Index } from "./src/index";
import { zodSchema } from "./src/zod-schema";
import z from "zod";
import { filter, listen } from "@watchmanlab/stream";

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
    z.object({
      id: z.uuidv7().optional(),
      email: z.string(),
      name: z.string(),
      age: z.number(),
    }),
  ),
});

const admins = new Table({
  env,
  name: "admins",
  schema: zodSchema(z.object({ name: z.string() })),
  idSelector: (row) => row.name,
});

users.$beforeInsert.pipe(filter((e) => e.curr.age <= 3)).pipe(
  listen((r) => {
    admins.insert({ name: r.curr.name });
  }),
);
users.$beforeUpdate.pipe(
  listen(({ id, curr, prev }) => {
    admins.delete(prev.name);
    admins.insert({ name: curr.name });
  }),
);
users.$beforeDelete.pipe(
  listen((r) => {
    admins.delete(r.prev.name);
  }),
);

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

for (const v of admins.all()) {
}

console.time("read");

for (const v of admins.all()) {
  console.log(v);
}

console.timeEnd("read");
