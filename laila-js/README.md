# laila (JavaScript)

**Lambda's Interdisciplinary Large Atlas** -- an exact, 1:1 JavaScript port of
the Python [`laila`](../) package (`laila-core`) for Node.js >= 22.12.

```bash
npm install laila-core
```

The port mirrors the Python package *module by module*: `laila/entry/entry.py`
is `src/entry/entry.js`, class names, method names, field names, default
values, error types and messages, log records, on-disk layouts and wire bytes
are identical. Anything serialized by one side is readable by the other
(pickle, msgpack, `.npy`, Fernet, SQLite / DuckDB / HDF5 pool layouts, the
JSON-RPC peer protocol). The only differences are the mechanical ones the
language forces:

| Python                                   | JavaScript                                              |
| ---------------------------------------- | ------------------------------------------------------- |
| `laila.constant(data=x, nickname="n")`   | `laila.constant(x, { nickname: "n" })`                  |
| `f(a, b, key=v)` (keyword arguments)     | `f(a, b, { key: v })` (trailing keyword block)          |
| `pool[gid]`, `del pool[gid]`, `gid in pool` | `pool.__getitem__(gid)`, `pool.__delitem__(gid)`, `pool.__contains__(gid)` |
| `cache << origin`, `cache <= origin`     | `cache.__lshift__(origin)`, `cache.__le__(origin)`      |
| `with pool.atomic(): ...`                | `with_(pool.atomic(), () => { ... })`                   |
| `await future` / `future.wait()`         | `await future` / `future.wait()`                        |
| `numpy.ndarray`                          | `NDArray` (`laila-core/ndarray`), byte-identical `.npy`  |
| `torch.Tensor`                           | not available (raises `NotImplementedError`)            |

## laila is type-free

Whatever you memorize is exactly what you get back:

```js
import laila from "laila-core";

const dict_entry = laila.constant({ key: [1, 2, 3] });
laila.memorize(dict_entry).wait();           // memorize a dict
laila.remember(dict_entry.global_id).wait().data;   // -> { key: [1, 2, 3] }

const bytes_entry = laila.constant(new Uint8Array([1, 2, 3]));
await laila.memorize(bytes_entry);           // every call returns a future
(await laila.remember(bytes_entry.global_id)).data; // -> Uint8Array
```

## laila has a uniform API

`memorize`, `remember` and `forget` work across every storage backend:

```js
import laila from "laila-core";
import { S3Pool, HDF5Pool, CloudflarePool } from "laila-core/data";

laila.memory.extend(new S3Pool({ bucket_name: "b", access_key_id: "...", secret_access_key: "...", region_name: "us-east-1" }), { pool_nickname: "s3" });
laila.memory.extend(new HDF5Pool(), { pool_nickname: "hdf5" });
laila.memory.extend(new CloudflarePool({ account_id: "...", bucket_name: "b", access_key_id: "...", secret_access_key: "..." }), { pool_nickname: "cloudflare" });

const entry = laila.constant({ weights: [0.1, 0.2] });
await laila.memorize(entry, { pool_nickname: "s3" });
await laila.remember(entry.global_id, { pool_nickname: "hdf5" });
await laila.forget(entry.global_id, { pool_nickname: "cloudflare" });
```

Backends (`laila-core/data`): the in-memory `DefaultPool`, `FilesystemPool`,
`SQLitePool` (`node:sqlite`), `DuckDBPool`, `HDF5Pool` (`h5wasm`),
`RedisPool`, `PostgresPool`, `MongoPool`, `S3Pool`, `CloudflarePool`,
`BackblazePool`, `GCSPool`, `AzurePool`, `HuggingFacePool`, plus
`MultiBuffer`. Cloud / database clients are optional peer dependencies and are
loaded lazily on first use.

## laila has async operations

Every operation returns a future you can block on or `await`:

```js
const future = laila.memorize(entry);
laila.wait(future);   // blocking (pumps the event loop, see below)
await future;         // or async
```

## Quick example: a cache chain

```js
import laila from "laila-core";
import { HDF5Pool, SQLitePool } from "laila-core/data";

const hdf5_pool = new HDF5Pool({ nickname: "cache_hdf5" });
const origin = new SQLitePool({ nickname: "origin" });
laila.memory.extend(hdf5_pool, { pool_nickname: "cache_hdf5" });
laila.memory.extend(origin, { pool_nickname: "origin" });

// memory <- HDF5 <- SQLite  (same as ``laila.alpha_pool << hdf5_pool << origin``)
laila.alpha_pool.__lshift__(hdf5_pool).__lshift__(origin);

const entry = laila.constant({ msg: "hello" }, { nickname: "proxy_demo" });
await laila.memorize(entry, { pool_nickname: "origin" });

laila.alpha_pool.exists(entry.global_id);   // false -- not cached yet
laila.alpha_pool.__getitem__(entry.global_id);
laila.alpha_pool.exists(entry.global_id);   // true
hdf5_pool.exists(entry.global_id);          // true
```

## Peers

Policies talk to each other over 60+ transports (TCP/WebSocket, UDP, Unix
sockets, serial/UART, MQTT/AMQP/ZeroMQ/XMPP brokers, Modbus/EtherNet-IP,
BLE/LoRa/Zigbee radios, I2C/SPI/CAN buses, ...). The wire protocol is JSON-RPC
and is identical to Python's, so a Node policy and a Python policy peer
transparently (`tests/interop/` runs both directions over TCP and WebSocket):

```js
import laila from "laila-core";

laila.communication.add_connection(new laila.DefaultTCPIPProtocol({ host: "0.0.0.0", port: 8765, peer_secret_key: "s3cret" }));
// on the other machine (Python or JS):
const peer_id = laila.add_peer("tcp://10.0.0.5:8765", "s3cret");
const remote = laila.request(peer_id);
await remote.remember(entry.global_id);
```

## Configuration (`laila.args`)

`laila.read_args(path)` loads TOML / JSON / `.env` / XML / CLI arguments into
`laila.args` (a `DotMap`), and `laila.args.environment` mirrors the active
policy's full CLI-capable configuration; assigning a populated environment
rebuilds the policies exactly like the Python `_load_environment`.

## Blocking waits and the loop pump

Python laila blocks threads (`Future.wait()`, `Thread.join()`, `pool.keys()`
on a remote pool...). Node has one thread, so blocking calls *pump* the libuv
loop (nested `uv_run`) through the small N-API addon in `native/loop_pump.c`
(prebuilt for linux-x64; `npm run build:native` rebuilds it). The pump cannot
run from inside a microtask (a promise callback, or the top level of an ES
module); call blocking APIs from a macrotask (`setImmediate`, a timer, an I/O
callback, the top level of a CommonJS script) or use the `await` form. See
`examples/` for both styles. Without the addon (`LAILA_DISABLE_PUMP=1`)
every blocking entry point raises `BlockingNotPossibleError` and only the
`async` surface is usable.

"Threads" (`threading.Thread`, executor bodies, taskforce workers) are
cooperative frames on that one loop. A blocked frame lets the others run
underneath it, so threads interleave as in Python, with one structural
limit: a frame can only resume once every frame that started *while it was
blocked* has returned. The runtime schedules around this (a waiter holding a
lock does not start contenders for it; slot hand-offs go to the frame that
can actually take them), but a test that expects a *suspended* thread to
keep holding a lock while the main thread times out on it cannot be
reproduced.

## Development

```bash
npm install                       # also builds/loads the native pump
npm test                          # unit + deep_eval + vectors + interop
npm run test:unit
npm run test:vectors              # byte-identity against Python-generated fixtures
npm run vectors                   # regenerate fixtures with the Python package
npm run test:interop              # live Python <-> JS peers (needs python3 + laila)
```

`tests/unit`, `tests/deep_eval` port the Python test tree one file to one file
(`test_x.py` -> `x.test.js`, same test names); `tests/vectors` checks pickle /
msgpack / npy / Fernet / UUID / recovery-code bytes against the Python
originals; `tests/interop` spawns `py_peer.py` as a server and as a client.

## License

MIT, same as the Python package.
