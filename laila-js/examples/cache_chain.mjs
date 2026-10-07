/**
 * Stack a fast local cache in front of slower storage with one operator.
 *
 * JS counterpart of the README "Quick example"; uses local HDF5 + SQLite pools
 * (under a temporary directory) instead of S3 so it runs anywhere. Reads
 * cascade through the chain until they find the data, caching a copy in every
 * tier on the way back up.
 *
 * This example uses laila's *blocking* style (``future.wait()``, synchronous
 * pool access), which pumps the event loop and therefore must run from a
 * macrotask -- hence ``setImmediate(main)`` (the top level of an ES module is a
 * microtask). See ``data_types.mjs`` for the ``await`` style.
 *
 *     node examples/cache_chain.mjs
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import laila from "../src/index.js";
import { HDF5Pool, SQLitePool } from "../src/data/index.js";

function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "laila-cache-chain-"));
  laila.set_default_directory(root);

  const hdf5_pool = new HDF5Pool({ nickname: "cache_hdf5" });
  const origin = new SQLitePool({ nickname: "origin_sqlite" });
  laila.memory.extend(hdf5_pool, { pool_nickname: "cache_hdf5" });
  laila.memory.extend(origin, { pool_nickname: "origin_sqlite" });

  // memory <- HDF5 <- SQLite   (Python: ``laila.alpha_pool << hdf5_pool << origin``)
  laila.alpha_pool.__lshift__(hdf5_pool).__lshift__(origin);

  const entry = laila.constant({ msg: "hello from the origin" }, { nickname: "proxy_demo" });
  laila.memorize(entry, { pool_nickname: "origin_sqlite" }).wait();

  console.log("alpha cached before read :", laila.alpha_pool.exists(entry.global_id)); // false
  laila.alpha_pool.__getitem__(entry.global_id); // cascades down to SQLite, caches on the way up
  console.log("alpha cached after read  :", laila.alpha_pool.exists(entry.global_id)); // true
  console.log("hdf5 tier                :", hdf5_pool.exists(entry.global_id)); // true
  console.log("origin                   :", origin.exists(entry.global_id)); // true

  console.log("remembered data          :", laila.remember(entry.global_id).wait().data);

  laila.terminate();
  fs.rmSync(root, { recursive: true, force: true });
}

setImmediate(main);
