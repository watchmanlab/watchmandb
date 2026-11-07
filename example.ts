import z from "zod";

import {} from "bun";
import { open } from "lmdb";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}

const env = open("test.mdb", {});
