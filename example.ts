import z from "zod";
import { create } from "./src/db";
import { ZodSchema } from "./src/zod-schema";
import { TypBoxSchema } from ".";
import Type from "typebox";
import {
  fromRange,
  listen,
  map,
  resolve,
  terminate,
} from "@watchmanlab/stream";
import {} from "bun";
import { open } from "lmdb";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}

const db = open("test.mdb", {}).openDB({ name: "a" });
//just an experiment with range queries
await db.transaction(() => {
  db.put(["sofiane", "foo", 1], "ok sof");
  db.put(["soffinal", "bar", 2], "ok bar");
  db.put(["sof", "baz", 3], "ok baz");
});

await Bun.sleep(0);

const key = ["soffinal"];

const iter = db.getRange({
  start: key,
});

for (const v of iter) {
  console.log(v.key);
}
