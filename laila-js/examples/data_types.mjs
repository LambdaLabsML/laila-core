/**
 * laila is type-free: whatever you memorize is exactly what you get back.
 *
 * JS counterpart of ``examples/simple/data_types.ipynb``. Runs against the
 * default in-memory pool of the default policy; nothing touches the disk.
 *
 *     node examples/data_types.mjs
 */
import laila from "../src/index.js";
import { NDArray } from "../src/_compat/ndarray.js";

const samples = {
  none: null,
  bool: true,
  int: 42,
  float: 3.5,
  str: "héllo wörld",
  bytes: new Uint8Array([0, 1, 2, 255]),
  list: [1, "two", 3.0, null],
  dict: { key: [1, 2, 3], nested: { ok: true } },
  ndarray: NDArray.array(
    [
      [1, 2, 3],
      [4, 5, 6],
    ],
    "<f4",
  ),
};

for (const [name, data] of Object.entries(samples)) {
  const entry = laila.constant(data, { nickname: `data-types-${name}` });
  await laila.memorize(entry); // every operation returns an awaitable future
  const back = (await laila.remember(entry.global_id)).data;
  const shown = back instanceof NDArray ? `NDArray(dtype=${back.dtype}, shape=[${back.shape}])` : JSON.stringify(back, (_k, v) => (v instanceof Uint8Array ? [...v] : v));
  console.log(`${name.padEnd(8)} -> ${shown}`);
}

laila.terminate();
