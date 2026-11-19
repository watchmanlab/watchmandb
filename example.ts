import { Table } from "./src/table";
import { randomUUIDv7 } from "bun";
import { open } from "lmdb";
import { Index } from "./src/index";
import { zodSchema } from "./src/zod-schema";
import z from "zod";
import { filter, listen } from "@watchmanlab/stream";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}

let count = 0;
const env = open("test.mdb", {});
const users = new Table({
  env,
  name: "users",
  schema: zodSchema(
    z.object({
      id: z.number().default(() => count++),
      email: z.string(),
      name: z.string(),
      age: z.number(),
    }),
  ),
  id: (row) => row.id,
});

const admins = new Table({
  env,
  name: "admins",
  schema: zodSchema(z.object({ id: z.number(), name: z.string() })),
  id: (row) => row.id,
});

users.$beforeUpsert.pipe(filter((e) => e.row.age <= 3)).pipe(
  listen((r) => {
    admins.upsert({ id: r.id, name: r.row.name });
  }),
);
users.$beforeDelete.pipe(
  listen((r) => {
    admins.delete(r.id);
  }),
);

console.time("write");
await env.transaction(() => {
  const result = users.upsert({ email: "bbb@g", name: "ambic", age: 42 });
  if (result.ok) {
    console.log("id", "\n", result.data, "\n", users.get(result.data), "\n");
  }

  users.upsert({ email: "aaa@g", name: "sofiane", age: 53 });
  users.upsert({ email: "ddd@g", name: "sofiane", age: 40 });
  users.upsert({ email: "eee@g", name: "zrezem", age: 22 });
  users.upsert({ email: "vvv@g", name: "ambic", age: 22 });
  for (let i = 1; i <= 100_000; i++) {
    users.upsert({ email: `${i}@g`, name: `${i}name`, age: i });
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
