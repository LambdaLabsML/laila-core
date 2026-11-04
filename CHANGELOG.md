# Changelog

All notable changes to **laila-core** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `laila-rs/`: a Rust port of the whole package (`laila-core` on crates.io
  naming, lib name `laila`), written so that laila code reads the same in
  both languages. Three layers: a typed Tier 1 API (`Arg` / `Args` /
  `#[pysig]` give every function Python's `f(a, k=v)` calling convention
  and pydantic's validation texts), the `laila::python!` rewriter (a
  Python-shaped Rust dialect: `with`, `try`/`except`/`finally`, `lambda`,
  comprehensions, slices, f-strings, `async with`), and `py2rs`
  (`laila-rs/tools/py2rs`), which transpiles the Python test tree into
  that dialect so the suites are shared, not rewritten. The core (`basics`,
  `atomic`, `Entry` with compdata / transformations / constitutions, the
  pool base, `MultiBuffer`, `PoolIndex`, the `Record` envelope, the
  codecs) builds `no_std + alloc` for bare-metal targets (CI:
  `thumbv7em-none-eabihf`); the `std` feature adds the pools (local
  backends link the system `libsqlite3` / `libduckdb` / `libhdf5`, so a
  pool file written by either implementation opens in the other; the
  cloud / database backends speak the vendors' wire protocols directly),
  central command (thread / async / process taskforces, parking,
  `laila.guarantee`), central communication (RPC, hub / peer routing, the
  tier-1 transports, carriers, stream lanes -- byte-compatible with the
  Python wire format, verified live against the CPython peer) and the
  logger. Every functional suite and the ten `tests/deep_eval` suites run
  green from their transpiled twins (3 831 tests; the 33 deep-eval cases a
  typed language cannot express are skipped with the reason recorded in
  `laila-rs/tests/overrides/`), the generated files are byte-exact against
  `py2rs` in CI, and `laila-rs/docs/PARITY.md` lists the behavioural
  differences that remain. `laila-rs/examples/` holds four programs that
  are the same script in both languages (`data_types`, `cache_chain`,
  `peers_tcp`, `async_futures`: `python3 examples/x.py` and `cargo run
  --example x` print identical lines; `py2rs --script` generates the
  `.rs` from the `.py`, CI diffs and runs them). The Mirror Law checker
  (`laila-rs/tools/conformance/check.py`) diffs the Python AST against the
  crate -- modules, classes, public methods / fields / functions /
  constants and every `#[pysig]` signature -- and runs at zero violations
  in CI (five documented deviations in `allow.txt`). The rewriter and the
  transpiler carry rule tests: one compile-pass case per expression-layer
  rule of `docs/PORT_RULES.md` (`laila/tests/macro_rules.rs`), the
  macro's diagnostics as `trybuild` compile-fail snapshots
  (`laila/tests/ui/`), and one golden fixture per py2rs statement-layer
  rule (`tools/py2rs/tests/golden/`). Workflow:
  `.github/workflows/laila-rs.yml`.
- `laila-go/`: a Go port of the whole package, file-for-file. Every Python
  module `a/b/c.py` is `a.b.c.go` in one flat package `laila` (the Python
  package is one import cycle); every class, method, field, function and
  module-level name keeps its Python name under one mechanical rule
  (`memorize` -> `Memorize`, `_read` -> `Read_`, `__getitem__` -> `Getitem`,
  `Entry.constant` -> `Entry_constant`), exceptions become `(T, error)`
  with `errors.Is(err, py.KeyError)`-style typed values, and the Python
  test tree is ported 1:1 under `laila-go/tests`. It ships:
  - `py/`: a Python-semantics compatibility layer (dict ordering and
    hashing, pickle / msgpack / `.npy` codecs, `asyncio` streams with the
    same flow control, pydantic-style models, logging, `uuid`, `struct`,
    ...) verified against CPython through committed vectors.
  - all storage backends; the third-party SDKs live in `drivers/<extra>`
    modules so `import _ ".../drivers/s3"` is `pip install laila-core[s3]`,
    with the same `ImportError` text without it.
  - every carrier and transport protocol, peering, RPC, relays, channels
    and streams, byte-compatible with the Python wire format: a live
    Python <-> Go interop suite (`laila-go/tests/interop`, both directions,
    TCP + WebSocket) and a Go driver for the shared N-language matrix
    (`tests/interop/langs/go.py`) run in CI.
  - facade alias packages generated from each `__init__.py`
    (`tools/facades/gen_facades.py`), so `from laila.entry import Entry`
    is `import ".../laila-go/entry"`; `examples/` twins of the notebooks;
    a mirror-law checker (`tools/conformance/check.py` + `allow.txt`, the
    complete list of documented deviations) and the `laila-go` workflow.
- `tests/interop/`: an N-language interop matrix. Every laila implementation
  (Python, `laila-js`, host `laila-c`, `laila-c` on an emulated ESP32) is
  driven through a shared peer-harness CLI contract; pytest derives the
  client x server x carrier matrix, typed-payload parity (pickle / msgpack /
  `.npy`), and 3-party relays across three languages from a registry, and a
  guard test fails when a `laila-*` tree has no driver. `make test-interop`;
  CI runs it on every PR, and `laila-c/emulator/run_qemu_test.sh` runs the
  device cells.
- `tests/interop/test_scenarios.py`: seeded, randomized interop scenarios on
  top of the fixed matrix (unicode strings, nested JSON compared structurally,
  1 B - 4 MB byte streams verified by FNV-1a 64, pool routing, forget
  semantics, concurrent bursts, random relays, error paths, server churn).
  `LAILA_INTEROP_SCENARIOS` / `LAILA_INTEROP_SEED` size and seed the run (CI
  runs 25); every failure prints the command that regenerates that one
  scenario. The harness contract gained `memorize_json` / `remember_json`,
  `memorize_bytes` / `remember_hash`, and an optional pool on `memorize*` /
  `forget`, implemented by all three harnesses.
- `laila.addons.torch.Module` / `Optimizer`: `torch.nn.Module` /
  `torch.optim.Optimizer` wrappers with the same call API (DDP-style
  `.module`, forwarded `param_groups` / `step` / `zero_grad`, LR schedulers
  accept the wrapper) that `laila.memorize` / `laila.remember` /
  `laila.forget` take directly. Each call turns the object into a
  `ModuleManifest` / `OptimizerManifest` -- `Manifest` subclasses under their
  own scopes (`"MANIFEST:TORCH_MODULE:<nickname>"` /
  `"MANIFEST:TORCH_OPTIMIZER:<nickname>"`) that store one constant entry per
  tensor (detached and copied to the CPU before upload, deterministic
  per-leaf nicknames so re-memorizing overwrites in place), a pickled
  weight-free architecture *skeleton* (the module deep-copied onto the
  `meta` device, a few KB; non-persistent buffers such as RoPE tables ride
  along as real CPU copies, `parametrize` parametrizations are detached and
  re-registered on rebuild, `cloudpickle` when a class lives in `__main__`
  or is otherwise not importable by name) plus a `meta` entry, and implement their own `remember`: fetch the
  stored blueprint, realize it and `load_state_dict` into the bound object
  on the taskforce, resolving to that object. No optimizer priming is
  needed on restore; an optimizer without parameters raises `RuntimeError`
  before any I/O and a parameter-count mismatch raises `ValueError` before
  anything is loaded. `manifest.rebuild(device=)` reconstructs the object
  from the checkpoint alone: the model from its skeleton (weight ties and
  LSTM / GRU flat weights preserved, `train` / `eval` mode restored), the
  optimizer from `meta["param_refs"]` (`[model gid, parameter name]` per
  parameter) bound to the live parameters of the rebuilt model.
  New `laila.ManifestBacked` ABC: the top-level shims dispatch any bare
  instance like a bare `Manifest`. Tests cover MLP / CNN (BatchNorm,
  Dropout) / LSTM+GRU / Transformer (tied weights) / buffer and dtype edge
  cases across SGD, Adam, AdamW, RMSprop and Adagrad with two param groups,
  plus a realistic zoo (`test_realistic.py`): a mixture-of-experts decoder
  LM (RoPE, RMSNorm, GQA, top-k SwiGLU experts, tied head) in float32 and
  bfloat16, LoRA over frozen dense / MoE bases with adapter-only
  optimizers, a ViT with `weight_norm` parametrization, hook-based
  `spectral_norm` / `weight_norm`, Hugging Face Llama (gradient
  checkpointing, bf16) / Mixtral / `peft` LoRA with decay / no-decay
  groups (skipped without `transformers` / `peft`), and a `__main__`-defined
  model rebuilt in another process.
- `laila.ComplexManifest` / `laila.complex_manifest(nickname=...)` (scope
  `MANIFEST:COMPLEX`): a manifest whose members may be manifests,
  manifest-backed objects, entries or gids -- the "everything" of a
  training run (`everything["model"] = model`, `["optimizer"]`,
  `["dataset"] = ds.manifest`, `["config"] = laila.constant(...)`).
  `memorize` is recursive (each member runs its own `memorize` into the same
  pool, with the siblings as `context`), the blueprint is `{key: member
  gid}`. On `remember` the read path performs *recursive blueprint
  resolution*: every member gid under the `MANIFEST` scope is fetched and
  hydrated into its typed class (`ModuleManifest`, `OptimizerManifest`,
  nested `ComplexManifest`, plain `Manifest`), leaves stay gids; the
  trigger is the `COMPLEX` scope on the container, so a plain dataset
  manifest is never walked. `everything.rebuild(device=)` returns `{key:
  live object}` in insertion order with one shared context (the optimizer
  finds the model rebuilt before it) and a round trip gives back the exact
  assigned types and values. `forget` deletes the container and the members
  it owns (typed checkpoints, nested complex manifests, plain entries) and
  drops a plain manifest member as a record only. Importing
  `laila.addons.torch` registers adapters so bare `torch.nn.Module` /
  `torch.optim.Optimizer` objects can be assigned directly and come back
  bare. A plain `Manifest` now raises `TypeError` on a manifest member and
  points at `laila.complex_manifest`. Constitution builders are looked up by
  joined scope with longest-prefix fallback
  (`entry/constitution/build_maps.py:lookup_builder`), so any gid read
  under `MANIFEST:<SUBSCOPE>` comes back as the registered subclass.
- `addons/skills/model_checkpoint` (`.cursor/skills/model-checkpoint`): agent
  skill for checkpointing and resuming a torch model + optimizer with the
  wrappers above and bundling a run under `"everything"`. Tutorial 04 and
  the MNIST / CIFAR / CNN-on-S3 examples checkpoint and resume through it.
- `torch-skills` install extra (`pip install "laila-core[torch-skills]"`):
  pulls in what `laila.addons.torch` needs (torch + pillow, same as
  `torch`); the wrappers and the `data-loading` / `model-checkpoint` skills
  are packaged in every wheel under `laila/addons/`. New
  `laila.addons.skills` module (`available()` / `install()`) and
  `laila-skills` console script (`laila-skills list`, `laila-skills install
  [names] [--user | --dest DIR] [--link] [--force]`) copy the bundled skills
  from site-packages into `./.cursor/skills/<name>/` (or `~/.cursor/skills/`)
  so agents pick them up.
- Docs: the **Examples** tab is nested into two groups. *Simple Data
  Loading* holds the existing R2 dataset-creation and data-loader pages
  (`docs/examples/simple_data_loading/`); *Torch Wrappers*
  (`docs/examples/torch_wrappers/`, notebooks in `examples/torch_wrappers/`)
  adds Example 3 -- checkpoint / restore / `rebuild()` a CNN and its
  `AdamW` through `Module` / `Optimizer` on a local HDF5 pool -- and
  Example 4 -- a training loop over `Dataset` / `DataLoader` with the
  model, optimizer, dataset manifest, config and RNG state under
  `laila.complex_manifest(nickname="everything")`, resumed from that
  nickname alone. Both run on local pools with no credentials.
  `docs/gen_example_nav.py` builds the nested sidebar from a `GROUPS`
  table and the notebook-download hook now resolves `notebooks/` from
  nested pages.

### Changed

- Dependency floors: `grpcio` is now `>=1.83` in the `grpc`, `gcs`, `all`
  and `transports-all` extras (it was `>=1.60`, and `gcs` / `all` did not
  pin it at all). `google-api-core` and `google-auth` emit a `FutureWarning`
  on import for any older grpcio (no Post-Quantum Cryptography support) and
  will hard-require `>=1.83` from April 2027, so an outdated grpcio already
  present in the environment is now upgraded by `pip install laila-core[gcs]`.
  A new `notebook` extra (`ipywidgets`, also included in `all`) silences the
  `tqdm` "IProgress not found" warning that `huggingface_hub` triggers when
  imported inside a Jupyter kernel.
- `GCSPool` and `HuggingFacePool` now import their SDKs (`google-cloud-storage`,
  `huggingface_hub`) on first use instead of at module load. `laila.data`
  imports every backend eagerly, so previously a plain `import laila` paid
  both SDKs' import cost and surfaced their import-time warnings (grpcio
  PQC `FutureWarning`s, tqdm `IProgress` warning) even in sessions that only
  use local pools. The `ImportError` raised on first use when the SDK is
  missing is unchanged.
- laila-C `Future::wait()` now has Python parity: `RemoteFuture::wait()` and
  `GroupFuture::wait()` rethrow the recorded exception (and again on every
  later `wait()`), instead of silently setting `ERROR` and returning. Code
  that treated a returning `wait()` as success is now correct by
  construction; code that wants the status without an exception uses
  `error()` / `exception()` / the new `exception_message()`. A `GroupFuture`
  now also carries its last child's result on success.

### Fixed

- laila-C on ESP32 (ESP-IDF v5.3.1) now builds and serves peers on-target, so
  the `laila-c/emulator` QEMU interop cells run green:
  - `identity.cpp` sized the ISO-8601 timestamp buffer so the Xtensa GCC
    `-Wformat-truncation` (an error under ESP-IDF's `-Werror`) no longer fails
    the build.
  - The inbound session threads were created with the 3 KiB default pthread
    stack and overflowed on the first request (device reboot). The esp32 HAL
    now raises the pthread stack (12 KiB) for the serving threads via
    `esp_pthread_set_cfg` on `listen()`.
  - A session thread can no longer take the whole device down: an exception
    escaping `serve_connection` (e.g. `std::bad_alloc` on an oversized frame)
    is caught per-session, and a failed `std::thread` creation under memory
    pressure falls back to serving that one connection inline.
  - The emulator models a WROVER-class part (4 MB PSRAM, which `idf.py qemu`
    already provides) so the device can absorb a full interop run; the device
    payload cap in the interop suite is set to the measured on-target ceiling
    (16 KiB), and the ESP32 inbound-session cap is 4 so a burst of peers is
    handshaken promptly instead of timing out in the backlog.
- Fixes from the 1000-scenario interop soak (45 failing scenarios -> 0):
  - laila-js: `StreamReader.readexactly()` deadlocked on any `tcp://` frame
    larger than about 128 KiB. Flow control paused the socket once the
    buffer passed `2 * limit` and nothing resumed it until the whole frame
    was buffered, so the frame never completed. `_wait_for_data()` now
    resumes a paused transport, as CPython's `asyncio.streams` does
    (38 of the 45 failures; the new `test_large_payload` matrix cell and a
    256 KB `tcp://` round-trip in the JS pairwise suite guard it).
  - laila-C WebSocket server sessions never completed the RFC 6455 closing
    handshake: a peer Close was echoed but the TCP connection stayed open,
    so Python `websockets` clients waited out their 5 s close timeout on
    every `ws://` call to a C policy. `WsSession` now closes TCP after the
    handshake (both directions, idempotent), and `recv_text()` can no longer
    stretch a timeout to 2x on a partial frame.
  - laila-C served inbound peers one at a time on the listener thread, so a
    second peer's handshake blocked until the first hung up (burst
    handshakes timed out). Each accepted connection is now served on its own
    session thread (capped by `ConnectionConfig::max_inbound_sessions`,
    default 8 on hosts / 4 on ESP32; beyond the cap connections wait in the
    kernel backlog); RPC execution stays serialized through a per-policy
    dispatch mutex. See the concurrency section of `laila-c/README.md`.
  - laila-C clients read the first frame on the link as the RPC reply, so a
    server-side `__comm_ping__` (Python probes liveness before admitting a
    call) was mistaken for the response (`peer returned no entry`). Replies
    are now matched by JSON-RPC `id`; inbound `rpc.call` frames that arrive
    mid-call are dispatched (ping -> `"pong"`), `peer.disconnect` fails the
    call cleanly, and garbled frames surface as `Status::Error`.
- laila-C emitted a stale msgpack recovery snippet (missing
  `"strict_map_key": False`), so dict / list entries served by a laila-C
  policy were rejected by Python and JS peers as untrusted constitutions.
- laila-C 3-party relays sent `dst_pool: ""` / `src_pool: ""` when no pool
  was given; Python resolves `""` as a pool *name*, so relays orchestrated
  from C against Python or JS peers failed. Unset pools are now omitted,
  matching the 2-party path.
- Central memory is now *owner-bound*: `central.memory` keeps a weak
  back-reference to its policy and every memorize/remember/forget submits
  to the owner's command. `laila.memorize/remember/forget(..., dst_policy=B)`
  and the 3-party relay no longer transiently `activate_policy(B)`; the
  active policy is never mutated by a routed op, other threads never observe
  a switch, no environment-mirror refresh is triggered per op, and peer-side
  memory ops no longer compete for the caller's taskforce slots (fan-out
  beyond the slot cap used to deadlock until `rpc_timeout`). Record
  provenance (`recorder`) is unchanged: the owning policy.
- `HDF5Pool` now flushes after every write/delete, so a process that exits
  without `close()` (crash, `os._exit`, SIGKILL) no longer loses acknowledged
  writes or leaves an unreadable file.
- `close()` on `SQLitePool`, `DuckDBPool` and `HDF5Pool` takes the pool lock;
  closing while other threads write no longer crashes the interpreter or
  surfaces backend-specific errors -- later operations raise the documented
  `RuntimeError("... is closed")`.
- Records are validated on read: `Entry.from_dict` rejects malformed
  `_uuid` / `_evolution` fields and central memory raises
  `KeyError("record identity mismatch ...")` when a record's identity does
  not match the key it was read under.
- **Security:** constitutions rebuilt from a pool or a peer pass a trust
  gate before any code runs (`entry/constitution/trust.py`). Simple
  constitutions must match the recovery-code template of a loaded
  transformation (any `backward_kwargs`, any Fernet fingerprint -- records
  from other hosts and from the JS/C ports still load) or be generated /
  HMAC-signed locally; complex constitutions carry an HMAC-SHA256 keyed by a
  per-user secret in `~/.laila/constitution.key` (auto-created, `0600`,
  overridable with `LAILA_CONSTITUTION_KEY_FILE`). Tampered or foreign code
  raises `ValueError("untrusted constitution ...")`.
- Payload fidelity: msgpack now packs in strict mode, so tuples (nested or
  as dict keys), `OrderedDict` and other `dict`/`list`/scalar subclasses fall
  back to pickle instead of being silently coerced -- `remember` returns the
  exact types that were memorized, and dicts with tuple keys are no longer
  unrecoverable. `CD_dict` / `CD_list` keep the caller's object rather than a
  coerced copy. Pass `forward_kwargs={"strict_types": False}` to a
  `MsgpackSerializer` to opt back into coercion.
- `transformation_encryption` preset is now `Base64 -> Fernet` (Fernet is
  text-in); it previously could not store any entry. A hand-built chain that
  starts with `FernetEncryption()` fails with a clear `TypeError`.
- `FernetEncryption()` without an explicit key follows `laila.encryption_key`
  live: rotating the key mid-run is honoured by pools built earlier, and the
  recovery code's key fingerprint is re-rendered.
- Reads no longer mutate the stored record dict in in-memory pools; every
  `remember` rebuilds from the stored bytes (previously the second read
  returned the same already-built entry object and bypassed the
  constitution, e.g. decryption under the current key).
- `laila.build` is idempotent on READY entries (no more
  `RuntimeError("Entry is already built")`) and coalesces concurrent builds
  of the same entry object, so a constitution body runs exactly once.
- `Manifest.realized` / `async_realized` and complex-constitution manifest
  resolution look in the pool the manifest was read from / written to
  first, then the alpha pool, then every other registered pool: manifests
  memorized into a named (persistent) pool can now be realized and built
  over.
- Re-assigning `manifest.data` invalidates the SQL index (as documented for
  `extend` / `+=`).
- In-memory pools store a detached snapshot of the entry whose payload is
  deep-copied at `memorize` time (non-copyable payloads keep the reference);
  mutating the original object afterwards no longer changes what `remember`
  returns.
- Pool index: shard ids are derived from a persisted *index identity*
  (a hidden `LAILA:POOL_INDEX:...` record written on first use and adopted
  when the same file is reopened) instead of the transient pool uuid, so a
  fresh `SQLitePool(file_path=...)` / `DuckDBPool` / `HDF5Pool` instance
  finds its shards again. Shards live under constant keys and are rewritten
  in place, so memorize cost no longer grows with pool size; legacy
  evolvable shards are purged on first use. Plain (non-`LAILA:`) keys are
  not indexed.
- Lineage resolution (`remember("ENTRY:x")`, `@evolution=-k`,
  `@creation_timestamp=...`) ranks candidates across the whole proxy chain,
  so an origin holding newer evolutions than its cache wins; index
  self-repair is no longer capped at 64 stale hits and falls back to a key
  scan when the index is entirely stale.
- Proxy wiring rejects cycles: `a.proxy = a`, `a << b; b << a`, or a longer
  loop raise `ValueError` instead of recursing until `RecursionError` on the
  first miss.
- `DuckDBPool` reclaims space under write/delete churn: deletes are
  accounted and, past a threshold, the pool checkpoints and -- when the
  deleted volume is comparable to the live volume -- rewrites the table
  with its live rows (DuckDB never frees deleted rows that share a row
  group with a long-lived row). The `.duckdb` file and RSS now plateau
  instead of growing per cycle. `checkpoint(compact=...)` is public and a
  `memory_limit` field exposes DuckDB's setting (off by default: DuckDB
  treats it as a fatal, connection-invalidating limit).
- Persistent pools reject `transformations=None` at construction (it made
  every `memorize` fail with an opaque JSON error).
- Taskforce `shutdown(wait=True)` (the default, also used by
  `laila.terminate`) now drains: queued-but-undispatched tasks are run to
  completion before the threads are joined, matching the stdlib
  `ThreadPoolExecutor.shutdown(wait=True)` contract. Previously they were
  neither run nor cancelled and their futures stayed `NOT_STARTED` forever.
  `cancel_pending=True` still cancels them. Calling `shutdown(wait=True)`
  from a task on that same taskforce cannot drain (it would wait for the
  caller) and falls back to cancelling with a warning.
- `laila.command.submit(..., taskforce_id=<process pool>)` and
  `laila.build(entry, taskforce_id=<process pool>)` work: `Command.submit`
  no longer wraps callables in a local coroutine closure for
  `PythonProcessPoolTaskForce` (submit top-level picklable callables or
  `functools.partial` of one), and `build` resolves/realizes the manifest on
  the alpha taskforce then ships only the constitution body plus the
  realized entries to a worker, where `manifest.realized` returns the
  pre-built entries. Bodies that need a live policy inside the worker are
  not supported there.
- A dead process-pool worker (`BrokenProcessPool`) no longer kills the
  dispatcher thread: the task being placed is retried on a rebuilt pool
  and the taskforce keeps running; only if the rebuild fails does it flip
  to `TaskForceStatus.CRASHED`, fail the queued futures and reject new
  submissions.
- `GroupFuture.wait()/result/data/status` resolve children through the
  owning policy's `future_bank` first and then every other local policy's,
  so a group handle keeps working after `laila.activate_policy(other)`.
- `submit(wait=True)` releases the futures it consumed from the
  `future_bank` (there is no handle to release them through); it used to
  leak one future per task plus the group for every call.
- `PythonAsyncThreadPoolTaskForce` recycles its sync executor after one
  second of complete idleness, so the threads spawned by a burst of sync
  bodies exit instead of lingering for the life of the policy.
- Morph mode (`laila.activate_policy(proxy)`) works with any number of local
  policies in the process: futures created for a remote proxy register on the
  local policy that owns the link to it. Futures stamped with a local
  `policy_id` (owner-bound memory, taskforces) now register in *that*
  policy's `future_bank` -- whether or not it was ever activated -- instead of
  the active policy's; an in-process peer serving a morphed caller no longer
  answers `RemoteFuture.wait()` with `KeyError: Future ... not in bank`.
- In morph mode `laila.memorize(entry)` ships a READY `Entry` as its
  canonical serialized envelope and the peer rebuilds a real `Entry` before
  invoking the target method (it used to receive a plain dict and fail with
  `'dict' object has no attribute 'serialize'`). Any `Entry` in RPC
  `args`/`kwargs` (or a top-level list of them) gets the same treatment.
- `communication.stop()` and peer loss (liveness timeout, EOF,
  `remove_peer`) fail the outstanding outbound RPCs on every transport with
  `ConnectionError`; threads blocked in `send_rpc` / `RemoteFuture.wait()`
  wake immediately instead of after `rpc_timeout` (60 s).
- `laila.memorize/remember/forget(e1, e2, ..., dst_policy=peer)` raises
  `TypeError` like the local verbs (one positional, or a list) instead of
  silently storing only the first entry; `persist=` is forwarded to the
  peer so `remember(dst_policy=peer, persist=True)` caches into the peer's
  alpha pool as documented.
- WebSocket transport: server and client accept frames up to 64 MiB
  (`connection.MAX_FRAME_BYTES`; the `websockets` default was 1 MiB). An
  oversized outbound RPC is refused up front with a descriptive
  `ConnectionError` and the link stays up (it used to be closed with 1009,
  the RPC timed out and the peer vanished from the registry); a failed send
  fails its RPC at once. Inbound `rpc.call`s run off the receive loop so a
  blocking policy method cannot stall pings or other replies.
- UDP / datagram carrier: a message whose fragments exhaust their
  retransmits (or whose send fails) fails its pending RPC with
  `ConnectionError` instead of leaving the caller to `rpc_timeout`.
- `_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT` is usable again (it used to
  recurse infinitely on construction). The secret gate now applies only to
  public callables defined by a subclass and only while the global lock is
  held; a failed check raises `PermissionError` (it used to *return* it).
  The holder passes with `secret=` or implicitly inside its own
  `atomic(scope="global")` block, and `lock_global`/`unlock_global` are
  reference-counted for nested holds by the same policy.
- `AtomicDict`: keyed `pop`/`del` are O(1) (was O(n) through the order
  list); `keys()`/`values()`/`items()`/iteration read insertion order from
  `data`, so direct edits to `.data` are visible without `reindex()`.
  Positional accessors (`item_at`, `key_at`, `value_at`, `trim`) keep
  working off a lazily rebuilt index.
- `AtomicStr.append` is amortised linear when building a long string
  (in-place concatenation), not quadratic.
- `laila.args.environment` is mirrored lazily: constructing CLI-capable
  objects (pools, taskforces, protocols) marks the owning policy dirty and
  the dump happens on the next read of `environment`. Adding N pools is
  O(N) instead of O(N^2); `terminate()` drops pending updates.
- `laila.read_args` / `ArgReader` populate the nested `laila.args` tree
  (`args.policy.central.memory.pools[<gid>]...`) in addition to the
  flattened `a_b` keys, so a JSON/TOML/XML file drives CLI-capable
  defaults as documented. A file literally named `.env` is loaded as
  dotenv, and `read_args("terminal")` accepts `--key value`, `--key=value`
  and bare `--flag` (-> `True`) with the dashes stripped from key names.
- `terminate()` also clears the construction-time policy registry used by
  future-bank lookups, so handles of a terminated policy are no longer
  resolvable through `laila.runtime.*`.

### Added

- **laila-js** (`laila-js/`): an exact, 1:1 JavaScript port of the package
  for Node.js >= 22.12 (ESM + JSDoc), published as `laila-core` on npm. Same
  module tree, class / method / field names, defaults, error types and
  messages, log records, on-disk pool layouts and wire protocol; Python
  keyword arguments become a trailing object. Byte-compatible codecs
  (CPython pickle protocol 5 reader/writer, msgpack, `.npy`, Fernet, the
  recovery-code emitter and recogniser) verified against fixtures generated
  by the Python package (`laila-js/tests/vectors`). A small N-API addon
  (`native/loop_pump.c`) pumps the libuv loop so blocking APIs
  (`Future.wait()`, `Thread.join()`, synchronous pool access) behave like
  their Python counterparts; the `async` surface works without it. All 14
  pool backends, the 60+ communication protocols (TCP/WebSocket, UDP, Unix
  sockets, serial, broker, register-bus, p2p and loopback carriers), the
  process-pool and async taskforces, manifests and the SQL index are
  ported. The Python test tree is ported one file to one file under
  `laila-js/tests/{unit,deep_eval}`, and `laila-js/tests/interop` runs live
  Python <-> JS peers in both directions over TCP and WebSocket.
  A dedicated `laila-js` GitHub Actions workflow runs it all.

## [1.0.13]

### Changed

- Graceful teardown for **every** transport, not just serial. A new
  `peer.disconnect` JSON-RPC *notification* (no `id`, no reply) is sent by
  `disconnect()` / `remove_peer()` and by `stop()` to each peer right before
  the handle closes, so the remote unregisters us at once instead of on the
  liveness timeout. Matters most for carriers with no EOF to notice:
  datagram (UDP/CoAP/radios), broker (MQTT/AMQP/...), register buses,
  point-to-point serial, and loopback (where the remote's pings kept
  succeeding forever). Stream and WebSocket receive loops also end on it.
  Receivers only honour the notification from the address/connection the
  peer is registered on; older peers answer it with a harmless
  `method not found` error.
- Shared loop-thread lifecycle (`protocols/_carriers/loopthread.py`) used by
  the stream, datagram, p2p, broker and register carriers and by the
  WebSocket `tcpip` protocol. `stop()` now cancels *and awaits* every task,
  stops the loop, joins the thread and **closes the loop**; a `start()` whose
  boot raises closes its loop too. Previously every lifecycle leaked the
  loop's selector fd + self-pipe pair (3 fds) and `tcpip` left cancelled
  tasks pending. After `stop()` the carrier's `_event_loop` is `None` and any
  send raises `ConnectionError` (never `AttributeError` / asyncio
  `RuntimeError`).
- `communication.stop()` is best-effort per protocol (one failing `stop()` no
  longer prevents the others from closing) and unregisters remaining peers
  from `laila._remote_policies`.

### Fixed

- WebSocket (`tcpip`): `ping()` was the base-class `return False` while
  `supports_ping` was `True`, so **every ws peer was dropped by the first
  liveness sweep** (default 15 s). It now round-trips `__comm_ping__`
  (answered before the policy, like the carriers). `remove_peer()` on a ws
  peer was a no-op (`disconnect()` not implemented) -- it now closes the
  socket; `send_rpc` raises `TimeoutError` after `rpc_timeout` instead of
  silently returning `None`.
- UART/serial: close the asyncio read transport before the serial fd; silence
  "Fatal read error on pipe transport" (`OSError: [Errno 9] Bad file
  descriptor`) on shutdown. `_open_stream` now keeps the `connect_read_pipe`
  transport and `_close_stream` unregisters it from the selector before
  pyserial closes the fd. Same fix applied to the CAN (ISO-TP) transport,
  which used the same pattern. The point-to-point carrier's `stop()` now
  awaits the cancelled receive task before closing any fd, so `read_frame()`
  can no longer wake on a closed descriptor. Hardware-free regression tests
  on an `os.openpty()` pair cover `communication.stop()`, `laila.terminate()`,
  `remove_connection()`, stop/start/re-peer, teardown mid-frame and teardown
  under an active stream lane.

### Added

- `tests/.../test_graceful_teardown.py`: teardown conformance across
  loopback, raw TCP, Unix socket, UDP, WebSocket, UART (pty) and the
  in-memory broker / register / datagram / p2p carrier fakes -- clean stop
  (no asyncio errors, loop closed, zero fd leak per lifecycle, registries
  empty), remote notices `remove_peer()` and one-sided `stop()`, sends after
  stop raise `ConnectionError`, healthy peers survive liveness sweeps,
  `laila.terminate()` reports no errors.

## [1.0.11]

### Changed

- Global ID grammar is now `LAILA:<scope>:...:<uuid>[@k=v,...]`. The
  `GLOBAL_ID:` frame segment is gone; `evolution` is the only attribute that
  is part of identity (emitted by `.global_id`), everything else after `@`
  (e.g. `creation_timestamp`) is a search argument for `remember`.
- Scope-less references (`"run-3"`, `"my_entry"`) default to the `ENTRY`
  scope everywhere.
- `remember` on a reference without an evolution resolves to the highest
  stored evolution. `memorize` on a variable only bumps the evolution when
  the payload changed since the last memorize; otherwise the write is
  idempotent.
- Renamed `heartbeat_timestamp` -> `creation_timestamp` and
  `dirty` -> `locally_modified` (Python, laila-C, wire format, docs).
- `vault/agent/` moved to `agentic/internal/`; new empty `agentic/embedded/`
  reserved for embedded-systems documentation. Packaging/lint excludes
  updated accordingly.

### Added

- Per-pool index (`POOL_INDEX` scope, `data/schema/pool_index.py`) with
  per-base shards, write-through persistence, and self-healing validation.
  Enables `@evolution=-1` / `-k` and `@creation_timestamp=<iso>` lookups
  without scanning every key. Index keys are hidden from `keys()` unless
  `include_index=True`; pools expose `index_enabled` and `index_pool`.
- laila-C parity for all of the above (identity, entry, `PoolIndex`,
  resolver), plus regenerated interop vectors.
- Tutorial `01a_variables_and_evolution`; tutorials 01, 15, 23 updated.

### Fixed

- `laila-c/tools/test_translate.py` `gate-bad/DefaultPolicy` asserted that a
  legal C++17 by-value construction must fail to compile; the gate now checks
  the actual naive translation (`activate_policy` on a by-value policy).

The entries below accumulated under *Unreleased* since 1.0.6 and shipped in
the 1.0.7 - 1.0.11 patch series.

### Added

- Consolidated release pipeline: tags pushed to the private `LambdaLabsML/laila`
  repo now drive a single workflow that runs tests, builds, publishes to PyPI
  via OIDC trusted publishing, and mirrors the tagged tree to the public
  `LambdaLabsML/laila-core` repo.
- Pull-request CI (`.github/workflows/ci.yml`) running `ruff`, `mypy`, and
  `pytest` across Python 3.11 / 3.12.
- `ruff`, `mypy`, `pytest`, and `coverage` configuration in `pyproject.toml`.
- `.pre-commit-config.yaml` integrating `ruff`, `nbstripout`, and `gitleaks`
  on top of the existing project-local secret scanner.
- `CHANGELOG.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`,
  `CITATION.cff`, issue and PR templates, and a Dependabot config.
- `make help`, `make lint`, `make fmt`, `make typecheck`, `make build`,
  `make cov`, `make docs`, `make release-dryrun` targets.
- Distribution-contents audit in CI, the release pipeline, and
  `make release-dryrun`. Both the wheel and the sdist are now inspected and
  rejected if they contain `vault/`, `_dev/`, `tests/`, `hooks/`, `examples/`,
  `docs/`, `site/`, `.github/`, `.venv/`, `CLAUDE.md`, `conftest.py`, or
  `Makefile`. Listings are normalized (the wheel's `laila/` package prefix
  and the sdist's `<name>-<version>/` top-level dir are stripped) so the
  anchored regex correctly catches forbidden paths regardless of where they
  appear in the archive. Positive controls verify exactly one wheel and one
  sdist are present and that each contains its expected sentinel file
  (`laila/__init__.py`, `pyproject.toml`).
- `laila.__version__` resolved from installed package metadata.

### Changed

- All direct and optional dependencies now carry explicit lower and upper
  version bounds. `pydantic` is pinned to `>=2.5,<3` (v2 API is required).
- Developer tooling is installed via the standard `pre-commit` framework;
  the test suite no longer mutates `.git/hooks` or `git config`.
- Consolidated `publish-to-pypi.yml` and `sync-to-public.yml` into a single
  `release.yml` workflow. The merged workflow detects its mode at runtime:
  a `v*` tag push (or `workflow_dispatch` with the `tag` input) runs the
  full verify -> test -> build -> audit -> PyPI publish -> mirror -> tag ->
  GitHub Release pipeline; a plain push to `main` (with `paths-ignore`)
  only runs the mirror step. A single `concurrency: { group: release }`
  serializes every push to `laila-core`, eliminating the non-fast-forward
  race that two parallel sync jobs could otherwise hit.
- Bumped the `twine` pin in the `dev` extras from `<6` to `<7`. Setuptools
  now writes `Metadata-Version: 2.4`, which twine 5.x rejects in
  `--strict` mode; twine 6.x supports it. CI installs `twine` unpinned,
  so this only affected local `make release-dryrun`.
- Ruff baseline normalized: ran `ruff format` and `ruff check --fix`
  across the codebase (252 files reformatted, 977 lint issues auto-fixed).
  The `PL` (pylint) and `SIM` (flake8-simplify) rule sets are temporarily
  disabled rather than ignoring 30+ individual codes -- they conflict
  with several intentional patterns (lazy/circular imports, best-effort
  `try/except/pass` cleanup, lambdas in dispatch tables). The remaining
  stylistic noise (E731 lambdas, F405 star-re-exports, B007 unused loop
  vars, etc.) is enumerated in `[tool.ruff.lint].ignore` with comments
  explaining each. Tighten per-rule as the codebase is normalized.

### Fixed

- `entry/__init__.py` imported `EntryIdentityView` from `.entry`, but the
  class actually lives in `.entry_metadata` (the move was never reflected
  in the public re-export). This broke `import laila` at the very first
  CI test collection and was the root cause of every CI test job failure.
  Corrected to `from .entry_metadata import EntryIdentityView`.
- `__init__.py:terminate` reused the name `policy` (also a module-level
  import) as a loop variable, shadowing the import inside the function.
  Renamed the loop variable to `pol`.
- `policy/central/memory/schema/base.py:borrow` used a mutable default
  argument (`keys=[]`). Switched to `keys=None` with in-body normalization.
- `tests/functional/logger/unit_tests/test_logger.py::TestHDF5PoolSink`
  now skips cleanly when `h5py` is not installed instead of erroring out
  on the bare `from laila.data.hdf5.hdf5 import HDF5Pool` in `setUp`.

### Removed

- Tag/publish automation from the public `laila-core` mirror. PyPI is now
  published exclusively from the private repo.
- Root-level `conftest.py`. With `[tool.setuptools] package-dir = { laila = "." }`,
  any `.py` file at the repo root is implicitly part of the `laila` package
  and ships in the wheel as `laila/<file>.py`. The file was already empty
  (a docstring noting that pre-commit handles hook installation), so it was
  deleted outright. No `tests/conftest.py` is required; pytest works without
  one. The distribution audit retains a `conftest.py` entry in its forbidden
  list as defense-in-depth against any future regression.

## [1.0.6]

Initial public history baseline. Earlier versions tracked privately.

[Unreleased]: https://github.com/LambdaLabsML/laila-core/compare/v1.0.13...HEAD
[1.0.13]: https://github.com/LambdaLabsML/laila-core/compare/v1.0.12...v1.0.13
[1.0.11]: https://github.com/LambdaLabsML/laila-core/compare/v1.0.6...v1.0.11
[1.0.6]: https://github.com/LambdaLabsML/laila-core/releases/tag/v1.0.6
