import { Table } from "./src/table";
import { randomUUIDv7 } from "bun";
import { open } from "lmdb";
import { Index } from "./src/index";
import { zodSchema } from "./src/zod-schema";
import z from "zod";
import { filter, listen, tapInput } from "@watchmanlab/stream";

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

users.$beforeInsert
  .pipe(filter((e) => e.row.age <= 3))
  .pipe(tapInput((f) => f.$complements.pipe(listen((e) => e.commit()))))
  .pipe(
    listen((r) => {
      admins.insert({ id: r.id, name: r.row.name });
      r.commit();
    }),
  );
users.$beforeDelete.pipe(
  listen((r) => {
    admins.delete(r.id);
    r.commit();
  }),
);

console.time("write");
await env.transaction(async () => {
  const result = await users.insert({ email: "bbb@g", name: "ambic", age: 42 });

  if (result.ok) {
    console.log("id", "\n", result.data, "\n", users.get(result.data), "\n");
  }

  await users.insert({ email: "aaa@g", name: "sofiane", age: 53 });
  await users.insert({ email: "ddd@g", name: "sofiane", age: 40 });
  await users.insert({ email: "eee@g", name: "zrezem", age: 22 });
  await users.insert({ email: "vvv@g", name: "ambic", age: 22 });
  for (let i = 1; i <= 100_000; i++) {
    await users.insert({ email: `${i}@g`, name: `${i}name`, age: i });
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
