# laila-C

A generic, platform-independent C/C++17 port of [laila](../). laila-C mirrors
laila's public API 1:1 (only mechanical differences: `.` -> `->`,
`Type.classmethod` -> `Type::classmethod`, Python kwargs -> matching `*Opts`
structs) and is byte-compatible with laila's serialization. It targets anything
from an AWS host down to a single-core MCU via a Hardware Abstraction Layer
(HAL); the (LLM) translation step picks a platform backend and raises
`Status::Unsupported` (`LAILA_UNSUPPORTED`) when a target cannot honor a
capability.

## Layout

- `include/laila/` - public headers (the API surface).
- `src/` - generic, platform-free core implementation.
- `hal/include/laila/hal/` - HAL interfaces (clock, mutex, executor, storage,
  transport, random). Zero platform includes.
- `platform/<target>/` - concrete HAL backends:
  - `posix` - reference backend (host/AWS); cooperative or `-DLAILA_POSIX_THREADED=ON` worker pool.
  - `baremetal_singlecore` - OS-free single-core backend; also host-buildable and used to test the cooperative path.
  - `esp32` (ESP-IDF), `rp2040` (pico-sdk), `stm32` (CMSIS/HAL + FreeRTOS) - compile within their SDK toolchains.
- `tests/` - Tier-1 host unit tests.
- `examples/` - runnable examples (mirrors laila's README).

## Build & test (host / POSIX)

```bash
cmake -S . -B build
cmake --build build -j
cd build && ctest --output-on-failure   # per-suite tests, labeled by platform
./tests/laila_tests                     # full run (12k+ assertions, 16 suites)
./tests/laila_tests --suite value       # one suite
./tests/laila_tests --list              # list all cases
./examples/example_data_types
```

The suite holds 12,000+ diverse assertions across 16 suites (json, identity,
value, transform, constitution, entry, pool, future, policy, peers, comm,
xlang, compdata, interop, platform, stress). The full run enforces a
>=1000-assertion floor.

## compdata: byte-compatible auto-serialization (mirrors Python laila)

Every `Entry` payload is auto-serialized by type, exactly like Python's
`ComputationalData`: `dict`/`list` -> **msgpack**, `numpy` arrays -> **`.npy`**,
and everything else (`None`/`bool`/`int`/`float`/`str`/`bytes`) -> **pickle**.
The inverse "recovery codes" are the same executable Python `def backward(inp)`
strings laila emits; laila-C recognizes them by signature and runs the native
C++ inverse (`src/cd_msgpack.cpp`, `src/cd_pickle.cpp` -- a CPython-pickle
reader/writer, `src/cd_npy.cpp`). The cross-the-wire form is
`serialize(transformation_base64)` = `base64(serializer bytes)` + the recovery
codes. `torch.Tensor`, Fernet-encrypted payloads, and arbitrary pickled objects
raise `LAILA_UNSUPPORTED`. The `compdata` suite verifies parity against real
Python vectors (byte-identical msgpack/npy; pickle value-equal + cross-loadable
both directions).

### Cross-language parity (`xlang`)

The `xlang` suite verifies laila-C against **real fixtures produced by the
Python `laila` library** (`tests/vectors/`): byte-identical UUID5/nickname
`global_id`s and reading laila's actual `Entry.as_dict` output. Regenerate the
vectors whenever laila changes:

```bash
PYTHONPATH=/path/to/laila/parent python3 tests/gen_vectors.py
```

laila-C's default nickname namespace is aligned to laila's universal namespace,
and `set_active_namespace(key)` matches laila's `uuid5(NAMESPACE_DNS, key)`.

## Communication / connection types

`central.communication` is protocol-pluggable. `include/laila/communication.hpp`
defines a `CommProtocol` base (identity + `connect`/`send`/`recv`/`close` over a
HAL `Connection`) and named protocol classes for many link types:

- IP: `TCPIPProtocol`, `UDPProtocol`, `TLSProtocol`, `EthernetProtocol`
- application: `WebSocketProtocol`, `HTTPProtocol`, `MQTTProtocol`, `CoAPProtocol`, `AMQPProtocol`, `GRPCProtocol`
- long-range radio: `LoRaProtocol`, `LoRaWANProtocol`
- wireless: `BLEProtocol`, `BluetoothClassicProtocol`, `ZigbeeProtocol`, `ThreadProtocol`, `NFCProtocol`, `WiFiDirectProtocol`, `CellularProtocol`
- wired/bus: `SerialProtocol`, `I2CProtocol`, `SPIProtocol`, `CANProtocol`, `RS485Protocol`
- `LoopbackProtocol` (in-process; always available)

Open one directly, by `ConnectionType`, or by URI scheme:

```cpp
auto c = laila->communication->connect_uri("tcp://10.0.0.5:9000", "psk");
c->send_text("hello");
// or: laila->communication->connect(ConnectionType::LoRa, cfg);
```

A connection type with no HAL backend on the active target raises
`LAILA_UNSUPPORTED` on `connect()`. The POSIX backend implements live TCP/UDP
sockets (the `comm` suite includes a real localhost TCP echo); MCU backends are
where LoRa/BLE/Serial/I2C/SPI/CAN transports get wired in.

laila-C implements laila's carrier wire protocol: `TCPIPProtocol` is the
**stream carrier** (4-byte length-prefixed JSON-RPC + `peer.connect`/`rpc.call`/
`__comm_ping__`), byte-compatible with Python's `tcp://` transport;
`WebSocketProtocol` is a real RFC6455 client for `ws://`. The inbound listener
auto-detects, per connection, a WebSocket upgrade vs. a length-prefixed stream
frame, so one listener serves both. Datagram (`udp://`), broker, register, and
p2p carriers are scaffolded and raise `LAILA_UNSUPPORTED` until backed.

## Python <-> laila-C interop

Python `laila` peers over its carrier transports (`tcp://`, `ws://`, ...) using
`peer.connect` + `rpc.call`, with cross-policy memory served by `_remote_*` /
`_relay_*` methods and entries shipped as `serialize(transformation_base64)`
blobs. laila-C speaks the identical protocol, so an **unmodified** Python `laila`
policy can `add_peer("tcp://<host-or-device>:<port>", secret)` into a laila-C
policy (host or emulated ESP32) and exchange `memorize` / `remember` / `forget`
in both directions -- for every compdata type (pickle scalars, msgpack
dict/list, bytes, numpy). Verbs take laila's `src/dst/relay` options
(`dst_policy=` 2-party, `src_policy=` 3-party relay; `policy_id`/`pool_nickname`
are back-compat aliases).

```bash
# Build the host harness and run the Python<->C matrix over tcp:// and ws://,
# both directions, all compdata types:
bash tests/interop/run_interop.sh
# Also drive an emulated ESP32 device over the tcp:// stream carrier:
bash emulator/run_qemu_test.sh   # QEMU_NET_TEST (C client) + QEMU_PY_INTEROP (Python)
```

The C-only `interop` unit suite (in the main test binary) covers
`memorize`/`remember`/`forget` in-process and over a real TCP stream socket,
bi- and tri-directionally; the `tests/interop/` harness adds the cross-language
(Python) legs.

## Peers and `laila.request`

A policy can peer with another policy — in-process (by `global_id`) or over a
network link — and call methods on it via a `RemotePolicyProxy`:

```cpp
auto peer  = laila->add_peer("tcp://192.168.1.50:8770", "s3cr3t");
// or by host/port (sugar): laila->add_tcpip_peer("192.168.1.50", 8770, "s3cr3t");
auto value = laila->request(peer, "central.memory.remember", {gid})->data();
// or the convenience verb:
auto value = laila->peer(peer)->remember(gid)->data();
```

### Pulling from a peer by `policy_id` (mirrors laila's top-level verb)

Instead of changing the active policy, route a `remember` to a peer by setting
`policy_id` (the peer's id), optionally choosing a `pool_nickname` on the peer
and a `persist` cache-back policy — the active policy stays local:

```cpp
auto peer = laila->add_tcpip_peer("192.168.1.50", 8770, "s3cr3t");
RememberOpts o;
o.policy_id = peer;             // route via central.communication (a RemoteFuture)
o.pool_nickname = "remote-store";  // the pool *on the peer*
o.persist = false;              // one-shot read; no cache-back into the peer's alpha pool
auto entry = laila->remember(gid, o)->data();
```

### Serving peers (so another node can pull from THIS policy)

A policy can act as an inbound RPC server. `add_connection` opens a listener when
the platform can serve the link type; pump it with `poll()` from your run loop
(cooperative/single-core) or a background thread:

```cpp
laila->add_connection(std::make_shared<TCPIPProtocol>("0.0.0.0", 8770, "s3cr3t"));
uint16_t port = laila->listen_port();   // resolves an OS-assigned port if you bound :0
for (;;) { laila->poll(50); /* ... */ } // answers peers' central.memory.{remember,...}
```

The inbound dispatcher only ever serves THIS policy's own `central.memory`
(golden-rule compliant). The POSIX and ESP32 (lwIP) backends implement both the
outbound socket transport and the inbound listener; `examples/esp32_peer_request/`
shows the ESP32 side. Every call returns a `Future` (a `RemoteFuture`).

## Translation: Python → laila-C → targeted C (requires a Claude API token)

Producing platform-targeted C/C++ from a Python `laila` program is an
*intelligent* step performed by Claude, so it **requires a Claude API token**
(the compiled device binary does not). See `docs/TRANSLATION.md`.

```bash
pip install anthropic
export LAILA_CLAUDE_TOKEN=sk-ant-...
python tools/laila_translate.py app.py --target esp32 --stage full -o out/
```

Without a token the translator refuses to run.

### Run across platform backends

```bash
./tests/run_all_platforms.sh
```

Builds and runs the suite on `posix` (cooperative), `posix` (threaded), and
`baremetal_singlecore`, and prints a pass/fail matrix. MCU backends
(`esp32`/`rp2040`/`stm32`) compile within their SDK toolchains (CI).

## Build options

- `-DLAILA_PLATFORM=posix|baremetal_singlecore|esp32|rp2040|stm32` - select the HAL backend.
- `-DLAILA_SINGLE_CORE=ON` - cooperative (single-core) executor strategy.
- `-DLAILA_POSIX_THREADED=ON` - use a real worker-thread pool on POSIX.
- `-DLAILA_NO_EXCEPTIONS=ON` - build without exceptions/RTTI; APIs use status
  codes (`Status` / `LAILA_UNSUPPORTED`) instead of throwing.

Verified locally: `posix` (cooperative and threaded) and `baremetal_singlecore`
each pass the host suite (48/48); the core also compiles under
`-DLAILA_NO_EXCEPTIONS=ON -DLAILA_SINGLE_CORE=ON`.

## Usage (compare to laila Python)

```cpp
#include "laila/laila.hpp"
using namespace laila_c;

// Python: dict_entry = laila.constant(data={"key": [1, 2, 3]})
auto e = laila->constant(LailaValue::from_json(/* {"key":[1,2,3]} */));
laila->memorize(e)->wait();                       // laila.memorize(dict_entry)
auto data = laila->remember(e->global_id())->data();  // type-free, like laila
```

The three verbs (`memorize` / `remember` / `forget`), `build`, `Future`s,
`Entry`, `Pool` proxy chaining (`cache << origin`), constitutions, and the
policy/central structure all match laila. See `../vault/agent/*.md` for the
golden rules this port preserves.
