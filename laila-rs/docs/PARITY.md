# Python ↔ Rust parity: the data layer

`PORT_RULES.md` says how Python *syntax* maps onto Rust. This file says
where the Rust port of `laila.data` *behaves* differently from the Python
package, backend by backend, and what has been verified against a live
service. Anything not listed here is meant to be indistinguishable from
Python: same constructor fields and defaults, same pydantic validation
texts, same on-disk / on-wire layout, same error classes and messages,
same `laila.args` resolution.

The rule for every backend: **Python drives a vendor SDK; the port speaks
the SDK's wire protocol directly.** The SDK is not a dependency the port
can take, so the port re-implements the slice of it that laila uses
(authentication, request signing, the five object operations, retries)
and nothing else. The data written by one implementation is read by the
other (the Python ↔ Rust fixtures under `tests/fixtures` cover the local
backends; for the services the test suites write and read the same
bucket / database the Python suites use).

## Backends

| Backend | Python | Rust (`laila-core` feature) | Wire protocol in the port |
|---|---|---|---|
| in-memory | `_LAILA_IDENTIFIABLE_POOL` | `DefaultPool` (always) | — |
| SQLite | `sqlite3` | `SQLitePool` (`sqlite`, default) | system `libsqlite3` via `rusqlite` |
| DuckDB | `duckdb` 1.5.x wheel | `DuckDBPool` (`duckdb`) | system `libduckdb` 1.5.5 via the `duckdb` crate |
| HDF5 | `h5py` | `HDF5Pool` (`hdf5`) | system `libhdf5` via the `hdf5` crate |
| Filesystem | loop-mounted ext4 image | `FilesystemPool` (`std`) | `mkfs.ext4` / `mount` subprocesses, as Python |
| AWS S3 | `boto3` + `aioboto3` | `S3Pool` (`s3`) | S3 REST, SigV4 (`BotoPool` chassis) |
| Cloudflare R2 | `boto3` + `aioboto3` | `CloudflarePool` (`cloudflare`) | same chassis, `https://{account_id}.r2.cloudflarestorage.com`, region `auto` |
| Backblaze B2 | `boto3` + `aioboto3` | `BackblazePool` (`backblaze`) | same chassis, `endpoint_url` field |
| Google Cloud Storage | `google-cloud-storage` | `GCSPool` (`gcs`) | Cloud Storage JSON API (`storage/v1`) |
| Azure Blob Storage | `azure-storage-blob` | `AzurePool` (`azure`) | Blob service REST API (`x-ms-version: 2023-11-03`) |
| Hugging Face Hub | `huggingface_hub` | `HuggingFacePool` (`huggingface`) | Hub HTTP API: `resolve/`, `api/.../tree/`, `preupload/`, git-LFS batch, `commit/` |
| Redis | `redis` client, managed `redis-server` | `RedisPool` (`redis`) | RESP over the pool's UNIX socket (`redis` crate) |
| PostgreSQL | `psycopg`, optional managed `initdb`/`postgres` | `PostgresPool` (`postgres`) | PostgreSQL wire protocol (`postgres` crate) |
| MongoDB | `pymongo`, optional managed `mongod` | `MongoPool` (`mongo`) | MongoDB wire protocol (`mongodb` crate, sync API) |

`pools` enables every backend; mirrors
`pip install laila-core[duckdb,hdf5,s3,cloudflare,backblaze,gcs,azure,huggingface,redis,postgres,mongo]`.
As in Python, where each `*Pool` import sits in a `try/except
ModuleNotFoundError`, a backend whose feature is off is simply absent
from `laila::data`.

## Differences that apply to every HTTP backend

**No second, asynchronous client.** Python's `BotoPool` has two code
paths: `async_default=True` (the default) routes `_read_async` /
`_write_async` / `_delete_async` / `_exists_async` through an `aioboto3`
client cached per event loop; `async_default=False` runs the sync `boto3`
call inline on the calling loop. The port keeps the `async_default` and
`max_pool_connections` fields (constructor and validation parity: both
are accepted, `max_pool_connections` must be ≥ 1) and *always* takes the
inline path: a pool's `_*_async` hooks are its sync hooks awaited on the
caller's task. Observable consequence: a Rust task doing a remote read
holds its executor thread for the duration of the request; Python with
`aioboto3` would yield. Correctness, ordering and the data on the wire
are the same. The other SDKs (`google-cloud-storage`, `azure-storage-blob`,
`huggingface_hub`, `redis`, `psycopg`, `pymongo`) are used synchronously
by Python too, so for them there is nothing to diverge from.

**One HTTP client.** The SDKs bring their own HTTP stacks (`urllib3` +
`aiohttp` for boto, `requests`, `aiohttp`, …). The port uses one blocking
`ureq` agent over `rustls` with a 120 s global timeout, redirects
disabled (object stores sign the exact URL) and HTTP status never raised
as a transport error. TLS trust is `rustls`' bundled roots rather than
the system / `certifi` store: a corporate root CA installed system-wide
is not picked up, and `REQUESTS_CA_BUNDLE` / `SSL_CERT_FILE` are ignored.

**Retries.** `BotoPool` configures botocore's
`retries={"max_attempts": 3, "mode": "standard"}`. The port makes the
same three attempts, retrying transport failures, HTTP 5xx and 429, with
a fixed 200 ms / 400 ms back-off instead of botocore's jittered
exponential back-off. The same policy is applied to the other three HTTP
backends (their SDKs retry comparable conditions with their own timers).

**Credential chains are the common subset.** Each port implements the
providers a developer machine or CI runner actually hits; the long tail
of SDK providers is not there:

- boto chassis: explicit fields → `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` / `AWS_DEFAULT_REGION` →
  `~/.aws/credentials` + `~/.aws/config` under `AWS_PROFILE`. Not
  implemented: SSO, `credential_process`, assume-role profiles, the EC2 /
  ECS metadata endpoints, web-identity tokens. A missing credential
  raises the same `botocore.exceptions.NoCredentialsError` text.
- GCS: explicit service-account info (RS256 JWT → OAuth2 token) →
  `GOOGLE_APPLICATION_CREDENTIALS` → `~/.config/gcloud/application_default_credentials.json`
  (user refresh token) → the GCE metadata server; anonymous when
  `STORAGE_EMULATOR_HOST` is set. Not implemented: external-account /
  workload-identity federation, impersonation.
- Azure: a connection string (`AccountName` + `AccountKey` SharedKey,
  `SharedAccessSignature`, or an anonymous `BlobEndpoint` such as
  Azurite's). Not implemented: `DefaultAzureCredential` (AAD / managed
  identity).
- Hugging Face: explicit token → `HF_TOKEN` → `HF_TOKEN_PATH` / the
  cached login under `~/.cache/huggingface`; `HF_ENDPOINT` honoured.

**Error texts.** The SDKs' exception classes do not exist in Rust; the
port raises `LailaError` with the message Python would have printed,
including the class name (`botocore.exceptions.ClientError: An error
occurred (NoSuchKey) when calling the GetObject operation: ...`,
`google.api_core.exceptions.NotFound: 404 ...`,
`huggingface_hub.utils.EntryNotFoundError`, …). Tests that match on the
text pass unchanged; code that catches by *type* in Python catches
`LailaError` here.

## Differences in the managed-server backends

`RedisPool`, `PostgresPool` and `MongoPool` launch and own a server
process in Python (`subprocess.Popen` + `atexit.register(self.close)`),
and the port does exactly that through its `subprocess` / `atexit` shims:
same binaries, same command lines, same socket / data directories under
the pool's directory, same readiness polling, same teardown (`SHUTDOWN SAVE` for Redis,
`terminate()` then `kill()` for `postgres`, the `shutdown` command then
`terminate()` for `mongod`). Two things differ:

- The server-process handle is a Rust enum (`PgProc`, `MongoProc`) that
  is either a real child process or a `unittest.mock` stand-in. This is
  what lets the ported tests `patch.object(PostgresPool, "_start_local_server")`
  exactly as the Python tests do; it has no effect outside tests.
- A pool constructed bare (`Cls.__new__(Cls)`, as the Python tests do to
  reach `_initdb_if_needed` without connecting) has its field defaults in
  place and no connection or server, as in Python; `Pool::__new__` is the
  typed spelling of that and exists for the tests' sake only.

## The local backends (for completeness)

SQLite, DuckDB and HDF5 link the same system libraries the Python wheels
embed; the files are byte-comparable and the `tests/fixtures` round-trips
(a Python-written file read by Rust and vice versa) are part of the
suite. `FilesystemPool` shells out to the same tools. `DefaultPool` and
`MultiBuffer` are pure data structures with no divergence.

## Verification status

Every suite below is the py2rs port of its Python twin
(`tests/functional/pools/<backend>/unit_tests/test_*.py`) and is
regenerated and byte-diffed in CI. The gates match the Python ones:
secrets file present → run live, otherwise skip; server binary on `PATH`
→ run live, otherwise skip.

| Suite | Tests | Gate | Run live |
|---|---|---|---|
| `s3` | 22 | `~/.laila/secrets/s3_test.toml` (`AWS_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`; bucket `laila-test-1`) | not yet (no AWS credentials on the port machine; the request path is exercised to the first signed `PutObject`) |
| `cloudflare` | 21 | `~/dev_secrets.toml` with `CLOUDFLARE_*` | **yes** — 21/21 against R2 |
| `backblaze` | 21 | `~/dev_secrets.toml` with `BACKBLAZE_*` | not yet |
| `gcs` | 21 | `~/dev_secrets.toml` with `GCS_*` | not yet |
| `azure` | 21 | `~/dev_secrets.toml` with `AZURE_*` | not yet |
| `huggingface` | 20 | `~/dev_secrets.toml` with `HF_REPO_ID` / `HF_TOKEN` | not yet |
| `redispool` | 25 | `redis-server` on `PATH` | **yes** — 25/25 with a managed `redis-server` |
| `postgres` | 27 | `initdb` / `postgres` on `PATH` (the bootstrap tests mock the server) | **yes** — 27/27 with a managed cluster |
| `mongo` | 25 | `mongod` on `PATH` | **yes** — 25/25 with a managed `mongod` |

The Backblaze, GCS, Azure and Hugging Face ports are therefore verified
for constructor / validation parity, key encoding and the request shape
(the shared SigV4 signer is checked against the AWS reference vector, the
SharedKey / JWT / commit-payload builders follow the SDKs' canonical
forms), but have not yet been run against the real service.
Dropping the matching keys into `~/dev_secrets.toml` and running
`cargo test -p laila-tests --test functional_pools_<backend>_unit_tests`
is all that is needed; the
suites are written to the same buckets / repos as the Python ones.

The two Phase 0 spikes (`laila/tests/spike_readme.rs`, the README's
HDF5 ← S3 chain, and `spike_tier1.rs::pool_chain_shl_per_type`) use the
real `S3Pool` and are gated on the same `s3_test.toml` as the `s3` suite.

## Running the live suites

```sh
cd laila-rs
# object stores: credentials as the Python suites expect them
cargo test -p laila-tests --test functional_pools_cloudflare_unit_tests
# managed servers: the binaries on PATH (apt install redis-server postgresql; mongod from MongoDB's repo)
cargo test -p laila-tests --test functional_pools_redispool_unit_tests
cargo test -p laila-tests --test functional_pools_postgres_unit_tests
cargo test -p laila-tests --test functional_pools_mongo_unit_tests
```

CI (`.github/workflows/laila-rs.yml`) builds every backend (`pools`),
installs `redis-server` so the Redis suite runs live, and skips the
suites whose credentials or binaries it does not have — the same subset
the Python CI runs.

# Python ↔ Rust parity: communication

`laila.policy.central.communication` is the peer-to-peer layer: a
`Communication` hub on every `Policy`, one or more transport protocols
under it, a JSON-RPC envelope (`wire.py`) with 4-byte length framing and
a reserved stream-lane frame format, a connection state machine with
handshake / liveness / reconnect, and the user-facing `Channel` /
`Relay` / `RemoteProxy` objects. Everything on the wire — the envelope,
the framing, the `peer.connect` handshake fields, the stream header
`>3sBHH` / `LDG` datagram header, the error codes — is byte-identical
between the two implementations: a Rust policy and a Python policy peer
with each other (see *Interop* below).

The same rule as the data layer applies: **Python drives a third-party
transport library; the port speaks the wire directly or stops at the
driver check.** Python's transports fall in three groups, and the port
mirrors the grouping:

1. **Socket transports** (`loopback`, `tcp`, `tls`, `unix`, `udp`,
   `tcpip`, and the thin aliases that ride on them) use only the standard
   library in Python; the port implements them on `std::net` /
   `std::os::unix::net` / `rustls` / an in-tree RFC 6455 WebSocket.
   These are complete and interop-tested.
2. **Serial-style transports** (`uart`, `rs232`, `rs485`, `usb`, `lin`,
   `rfid`, `satellite`, `sigfox`) use `pyserial` in Python; the port opens
   the configured `port` as a raw tty (`Wire::Serial`) with no driver.
   The carrier (priority RPC queue, chunked stream queue, 4 KiB chunks)
   is complete; `uart` is exercised end-to-end over a pty pair in the
   streaming suite, the aliases through an in-process `Duplex`
   (`p2p::set_link`). None has been run on a physical serial device.
3. **Driver-backed transports** (everything else) `_require_drivers()` a
   third-party package in Python (`paho-mqtt`, `pymodbus`, `zigpy`, …)
   and raise the `pip install laila-core[<extra>]` hint when it is
   missing. The Rust build ships no host-side drivers, so these
   transports stop at the same check with the same message
   (`Wire::Driver`); the carrier above them (broker inbox routing,
   register mailbox polling, datagram fragmentation / ack / retry) is
   complete and exercised through an in-process bus (`broker::set_bus`,
   `register::set_bus`) in the suites. On a Python host with the extra
   installed the Python transport works and the Rust one does not; that
   is the one asymmetry in this layer.

## Carriers

| Python carrier | Rust `CarrierKind` | Rust shape |
|---|---|---|
| `_LAILA_IDENTIFIABLE_COMM_PROTOCOL` (abstract) | `Base` | every verb raises `NotImplementedError`, as Python |
| `loopback` | `Loopback` | in-process zero-copy dispatch |
| `_StreamRPCProtocol` | `Stream` | accept thread per listener; per link a reader thread and a writer thread draining a two-tier queue (RPC frames first, then one chunk of the next stream message); bounded chunk queue drops oldest (`tx_dropped`) |
| `_DatagramRPCProtocol` | `Datagram` | receive thread + retransmit thread per endpoint; `LDG` fragment header, 16-byte message id, ack per fragment, 4096-entry de-dup window |
| `_LazyDatagramRPCProtocol` | `LazyDatagram` | `Datagram` behind `_require_drivers()`; stops at the driver check |
| `_P2PStreamRPCProtocol` | `P2P` | one `Link` on a raw tty (`Wire::Serial`) or a harness `Duplex`; `stream_chunk_bytes = 4096` |
| `_BrokerRPCProtocol` | `Broker` | `BrokerBus` trait: inbox topic `laila/inbox/<global_id>`, `reply_to` correlation |
| `_RegisterRPCProtocol` | `Register` | `RegisterBus` trait: mailbox write + poll loop |
| `tcpip` (`websockets`) | `WebSocket` | RFC 6455 in-tree: text frames, client masking, ping/pong, symmetric `MAX_FRAME_BYTES` |

Python runs every wire-backed transport on a private asyncio loop
thread (`start_loop_thread` / `stop_loop_thread`). The Rust carriers do
their socket I/O on plain blocking threads; the loop-thread helpers exist
on the `asyncio` shim for API parity and the teardown tests, and a
transport subclass written against the Python contract (`_async_start`
/ `_async_stop`) still works.

## Protocols

59 protocol classes, one per Python module, same `protocol_name`, same
config fields and defaults (the per-class overrides such as the P2P
`stream_chunk_bytes` win over the carrier defaults as in Python), same
URI scheme. *Status* is what `start()` does on a host without the Python
driver installed.

| Scheme | Python module | Python driver | Carrier | Rust wire | Status |
|---|---|---|---|---|---|
| `loopback` | `local/loopback` | — | `Loopback` | `Loopback` | **complete**, suites |
| `tcpip` | `tcpip` | `websockets` | `WebSocket` | `WebSocket` | **complete**, suites + interop |
| `tcp` | `ip_app/tcp` | — | `Stream` | `Tcp` | **complete**, suites + interop |
| `tls` | `ip_app/tls` | `ssl` | `Stream` | `Tls` (`rustls`, feature `tls`) | **complete**, suites |
| `unix` | `local/unixsocket` | — | `Stream` | `Unix` | **complete**, suites |
| `udp` | `ip_app/udp` | — | `Datagram` | `Udp` | **complete**, suites |
| `ethernet` | `ip_app/ethernet` | — | `Stream` | `Tcp` | complete (TCP alias, as Python) |
| `wifi` | `short_range/wifi` | — | `Stream` | `Tcp` | complete (TCP alias) |
| `wifi-direct` | `short_range/wifi_direct` | — | `Stream` | `Tcp` | complete (TCP alias) |
| `matter` | `short_range/matter` | — | `Stream` | `Tcp` | complete (TCP alias) |
| `cellular` | `cellular/cellular` | — | `Stream` | `Tcp` | complete (TCP alias) |
| `gsm` | `cellular/gsm_gprs` | — | `Stream` | `Tcp` | complete (TCP alias) |
| `lte` | `cellular/lte` | — | `Stream` | `Tcp` | complete (TCP alias) |
| `nr5g` | `cellular/nr5g` | — | `Stream` | `Tcp` | complete (TCP alias) |
| `ltem` | `lpwan/ltem` | — | `Stream` | `Tcp` | complete (TCP alias) |
| `nbiot` | `lpwan/nbiot` | — | `Stream` | `Tcp` | complete (TCP alias) |
| `6lowpan` | `short_range/sixlowpan` | — | `Datagram` | `Udp` | complete (UDP alias) |
| `uart` | `wired/uart` | `pyserial` | `P2P` | `Serial` | **complete**, suites over a pty pair (saturation, lossy link, teardown) |
| `rs232` | `wired/rs232` | `pyserial` | `P2P` | `Serial` | raw tty (same path as `uart`); suites via `Duplex` |
| `rs485` | `wired/rs485` | `pyserial` | `P2P` | `Serial` | raw tty (same path as `uart`); suites via `Duplex` |
| `usb` | `wired/usb` | `pyserial` (CDC) | `P2P` | `Serial` | raw tty (same path as `uart`); suites via `Duplex` |
| `lin` | `wired/lin` | `pyserial` | `P2P` | `Serial` | raw tty (same path as `uart`); suites via `Duplex` |
| `rfid` | `short_range/rfid` | `pyserial` | `P2P` | `Serial` | raw tty (same path as `uart`); suites via `Duplex` |
| `satellite` | `lpwan/satellite` | `pyserial` | `P2P` | `Serial` | raw tty (same path as `uart`); suites via `Duplex` |
| `sigfox` | `lpwan/sigfox` | `pyserial` | `P2P` | `Serial` | raw tty (same path as `uart`); suites via `Duplex` |
| `can` | `wired/can` | `python-can` + `can-isotp` | `P2P` | `Driver` | driver check |
| `grpc` | `ip_app/grpc` | `grpcio` | `P2P` | `Driver` | driver check |
| `http2` | `ip_app/http2` | `h2` | `P2P` | `Driver` | driver check |
| `opcua` | `ip_app/opcua` | `asyncua` | `P2P` | `Driver` | driver check |
| `mqtt` | `ip_app/mqtt` | `paho-mqtt` | `Broker` | `Driver` | driver check; suites via `BrokerBus` |
| `amqp` | `ip_app/amqp` | `aio-pika` | `Broker` | `Driver` | driver check; suites via `BrokerBus` |
| `zeromq` | `ip_app/zeromq` | `pyzmq` | `Broker` | `Driver` | driver check; suites via `BrokerBus` |
| `xmpp` | `ip_app/xmpp` | `slixmpp` | `Broker` | `Driver` | driver check; suites via `BrokerBus` |
| `dds` | `ip_app/dds` | `cyclonedds` | `Broker` | `Driver` | driver check; suites via `BrokerBus` |
| `modbus-tcp` | `ip_app/modbus_tcp` | `pymodbus` | `Register` | `Driver` | driver check; suites via `RegisterBus` |
| `modbus-rtu` | `wired/modbus_rtu` | `pymodbus` | `Register` | `Driver` | driver check; suites via `RegisterBus` |
| `i2c` | `wired/i2c` | `smbus2` | `Register` | `Driver` | driver check; suites via `RegisterBus` |
| `spi` | `wired/spi` | `spidev` | `Register` | `Driver` | driver check; suites via `RegisterBus` |
| `ethernet-ip` | `wired/enip` | `pycomm3` | `Register` | `Driver` | driver check; suites via `RegisterBus` |
| `coap` | `ip_app/coap` | `aiocoap` | `LazyDatagram` | `Driver` | driver check |
| `dtls` | `ip_app/dtls` | (firmware / OS) | `LazyDatagram` | `Driver` | hardware note |
| `http3` | `ip_app/http3` | `aioquic` | `LazyDatagram` | `Driver` | driver check |
| `ant` | `short_range/ant` | `openant` | `LazyDatagram` | `Driver` | driver check |
| `esp-now` | `short_range/espnow` | (firmware) | `LazyDatagram` | `Driver` | hardware note |
| `irda` | `short_range/irda` | (firmware) | `LazyDatagram` | `Driver` | hardware note |
| `nfc` | `short_range/nfc` | `nfcpy` | `LazyDatagram` | `Driver` | driver check |
| `thread` | `short_range/thread` | `spinel` | `LazyDatagram` | `Driver` | driver check |
| `uwb` | `short_range/uwb` | (firmware) | `LazyDatagram` | `Driver` | hardware note |
| `zigbee` | `short_range/zigbee` | `zigpy` | `LazyDatagram` | `Driver` | driver check |
| `zwave` | `short_range/zwave` | `zwave_js_server` | `LazyDatagram` | `Driver` | driver check |
| `lorawan` | `lpwan/lorawan` | (gateway) | `LazyDatagram` | `Driver` | hardware note |
| `ethercat` | `wired/ethercat` | `pysoem` | `LazyDatagram` | `Driver` | driver check |
| `profinet` | `wired/profinet` | `pnet` | `LazyDatagram` | `Driver` | driver check |
| `i2s` | `wired/i2s` | (firmware) | `LazyDatagram` | `Driver` | hardware note |
| `1-wire` | `wired/one_wire` | (kernel `w1`) | `LazyDatagram` | `Driver` | hardware note |
| `sdio` | `wired/sdio` | (firmware) | `LazyDatagram` | `Driver` | hardware note |
| `lora` | `lpwan/lora` | — | `Base` | `None` | `NotImplementedError`, as Python |
| `bluetooth` | `short_range/bluetooth` | — | `Base` | `None` | `NotImplementedError`, as Python |

"driver check" = `start()` raises the `laila-core[<extra>]` install hint
with the Python text; "hardware note" = `start()` raises the runtime
error naming the firmware / hardware requirement, as the Python class
does when it has no host-side driver to import.

## Differences in the communication layer

**Threads, not an event loop.** Python moves bytes on a private asyncio
loop thread per transport and dispatches RPC handlers off that loop; the
port moves bytes on blocking reader / writer threads and keeps the same
off-thread handler dispatch, so ordering guarantees (RPC before the next
stream chunk, pings never behind a bulk frame) are the same. Only
timing differs.

**`tls` trusts `rustls`' bundled roots**, as the HTTP backends do;
Python's `ssl` uses the system store. Self-signed test certificates are
loaded from the same `certfile` / `keyfile` / `cafile` fields.

**Liveness and reconnect** (`ping_interval`, `liveness_timeout`,
`reconnect_backoff`, the silent-peer drop, stream-in during a liveness
drop) are ported timer for timer and are covered by the streaming suite.

**Mocking surface.** The Python suites monkeypatch transport internals
(`patch.object(type(proto), "_execute_rpc", spy)`,
`proto._build_response = spy`, `object.__setattr__(proto,
"_handle_request_frame", …)`). The port honours these through the
`mock` shim's class and instance hooks; this exists for the suites and
has no runtime cost outside them.

## Differences in the command layer and the runtime helpers

**`laila.guarantee_async` cancels the body through the suspension
points.** Python's watcher task calls `parent_task.cancel()` and the body
sees `CancelledError` at its next `await`, after which `__aexit__`
re-raises the in-scope future's error. The port has no task objects: a
watcher thread cancels a per-task token (`asyncio::CANCEL_SCOPE`), and
the body's next `asyncio.sleep` / `to_thread` / one-shot resolves to the
future's exception *directly*. From outside both read the same -- a hung
body ends the moment an in-scope future fails and the future's error is
what `asyncio.run` raises -- but a body that catches `CancelledError`
itself would not see one in Rust.

**`threading.main_thread()` is the process' main thread**, also when the
first call comes from a worker (the libtest harness runs tests on named
threads). A native thread laila spawned -- a taskforce loop or
dispatcher, an executor worker -- is reported by `current_thread()` as
Python's `_DummyThread` would be, under its OS name
(`AsyncTF-<tag>-Loop-<i>`).

**The logger singleton is constructed on the first structured hook**
(`record_memorize` / `record_remember` / ...), as Python's `get_logger()`
does; that construction writes the `laila.args.environment.logger`
mirror entry, which is why `laila.memorize` after `ArgReader().clear()`
recreates `laila.args.environment`.

**Every thread laila starts is listed by `threading.enumerate()` /
`active_count()` from the moment the spawning call returns** (a taskforce
dispatcher, loop and sync worker, a transport's accept / receive / poll
thread), as Python's `Thread.start()` guarantees for its `Thread`s; the
Rust-only helper threads (the guarantee watcher, a subprocess pipe
drainer) have no Python twin and are not listed.

**An f-string field with a format spec (`f"{name:>12}"`, `f"{x:,.2f}"`)
is formatted by the runtime's format-spec mini-language**
(`strfmt::fstr_spec`), not by Rust's `format!` flags, so fill, align,
sign, grouping, precision and the type codes read as in Python; a
malformed spec literal is a `ValueError` at the call, as an uncaught one
would be.

## Verification status (whole package)

`cargo test --workspace` runs 3 831 tests: the Tier 1 spikes, every
functional suite under `../tests/functional` and the ten deep-evaluation
suites under `../tests/deep_eval` (`cargo test -p laila-tests --test
deep_eval`: 1 361 tests, 33 skipped with the reason recorded in
`tests/overrides/tests/deep_eval/*.yaml` -- `exec` of hand-written Python
source, test-local subclasses of typed structs, class identity of the
registries, ndarray unicode / object dtypes). Every generated file is
byte-exact against `py2rs` (`--check` in CI).

The surface itself is checked by the Mirror Law gate
(`tools/conformance/check.py`, Layer A, zero violations in CI): every
Python module, class, public method, pydantic field, module function and
constant has a same-named Rust twin in the twin file, and every
`#[pysig]` matches the Python `def` parameter for parameter (names,
order, `*` / `**`, defaults). The documented deviations
(`tools/conformance/allow.txt`):

| Python | Rust | Why |
|---|---|---|
| `_DatagramEndpoint.connection_made` / `datagram_received` / `error_received` | `DatagramLink::open` / `recv_from` / `close` | `asyncio.DatagramProtocol` callbacks; the Rust link is pull-based on the carrier thread |
| `_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT.__getattribute__` | the gate in the typed accessors (`UNGATED_NAMES`) | Rust has no attribute interception |
| -- | `Identity::from_args` (`#[pysig("Identity(..)")]`) | the typed identity state's constructor; binds the `_LAILA_IDENTIFIABLE_OBJECT` kwargs plus the port-internal `global_id=` / `default_scopes=` |

Closed-set class families are one handle dispatching on a kind
(`Future` for `_LAILA_IDENTIFIABLE_FUTURE` / `Future` /
`ConcurrentPackageFuture` / `ProcessPackageFuture` / `ComplexFuture` /
`GroupFuture` / `RemoteFuture`; `Policy` for `_LAILA_IDENTIFIABLE_POLICY`;
`Memory` for `_LAILA_IDENTIFIABLE_CENTRAL_MEMORY`), so a member's methods
live in the host file (`HOSTED_IN` in `check.py`). `PythonProcessPoolTaskForce`
and `ProcessPackageFuture` exist as classes (the environment and
`laila.TaskForce` namespaces carry them) but cannot be constructed: there
is no interpreter to fork a worker from. Private (leading-underscore)
members are compared too and listed by `--internals`; 296 differ in name
or shape and are advisory.

## Verification status

| Suite | Tests | Covers |
|---|---|---|
| `functional_policy_communication_unit_tests` | 238 | RPC envelope and framing, hub and peer routing, tier-1 transports (`loopback` / `tcpip` / `tcp` / `tls` / `unix` / `udp`), transport conformance and environment resolution for every scheme, carriers on an in-process bus / link (broker, register, P2P, driver-gated), liveness, back-pressure, graceful teardown, remote-future materialisation, memory round-trips |
| `functional_policy_communication_streaming_unit_tests` | 64 | `Channel` / `Relay`, stream lanes and `StreamEntry`, liveness while streaming, peer registry, HEVC-sized streams, `uart` over a pty pair (saturated and lossy links), UART teardown |
| `interop_py_peer` | 18 | Rust client ↔ CPython server and CPython client ↔ Rust server over `tcpip` and `tcp`: handshake, mixed-type RPC echo, `remember` / `memorize` / `forget` across the pools (including a NumPy array rebuilt as `NDArray`), a 256 KB payload both ways, stream lanes, remote errors, `remove_peer` and re-peering |

The interop suite drives `laila-js/tests/interop/py_peer.py` (the same
peer the JavaScript port is tested against) with the Python package from
the repository root; it is skipped when that interpreter is not
available. Run it with:

```sh
cd laila-rs
cargo test -p laila-tests --test interop_py_peer
```
