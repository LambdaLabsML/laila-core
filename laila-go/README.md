# laila-go

Go port of the `laila` Python package. A verbatim, file-for-file translation:
every Python module `a/b/c.py` is `a.b.c.go` in the single flat package
`laila`, every class and function keeps its Python name under one mechanical
rule, and the Python test tree is ported 1:1 as the acceptance gate.

Status: Phases 0-8 done (compat layer, foundations, entry, command, memory
and root verbs, all storage backends + driver modules, communication with
every carrier / protocol, the live Python <-> Go interop harness, facade
packages, examples, the conformance checker, CI, and the `tests/workloads`
soak suite w01-w24 under `//go:build workload`). The deviation table below
is complete for everything shipped.

## Layout

```
laila-go/            module github.com/amirzadehlambda/laila/laila-go  (package laila, flat)
  py/                Python-semantics compatibility layer (no laila knowledge)
  drivers/<extra>/   one Go module per pyproject extra (s3, postgres, ...): `import _` = `pip install laila-core[extra]`
  tests/             separate module holding the 1:1 port of ../tests
  entry/ data/ ...   facade alias packages reproducing the Python import paths (generated)
  examples/          runnable twins of the Python notebooks (data_types, local_pools, peers_tcp)
  tools/conformance  mirror-law checker: Python surface == Go surface, with allow.txt
  tools/facades      generator for the facade packages (`--check` in CI)
```

## Import paths

The Python package is one import cycle, so the Go port is one flat package;
the Python *package* paths are reproduced as alias packages generated from
each `__init__.py` (`tools/facades/gen_facades.py`):

| Python | Go |
|---|---|
| `import laila; laila.memorize(e)` | `import laila "github.com/amirzadehlambda/laila/laila-go"; laila.Memorize(e)` |
| `from laila.entry import Entry` | `import ".../laila-go/entry"; entry.Entry` |
| `from laila.data import SQLitePool` | `import ".../laila-go/data"; data.New_SQLitePool(py.Kw{...})` |
| `from laila.policy.central.communication import Channel` | `import ".../laila-go/policy/central/communication"; communication.Channel` |
| `from laila.runtime import wait` | `import laila_runtime ".../laila-go/runtime"; laila_runtime.Wait(f)` |
| `from laila.entry.compdata.taxonomy.cd_numpy import CD_numpyarray` (a *module* path) | `laila.CD_numpyarray` -- modules are files of the flat package, only packages get a facade |

Every name an `__init__.py` binds is aliased: `type X = laila.X` for classes
(plus `New_X`, `X_class`, and the `X_<member>` classmethods / enum members),
`var f = laila.F` for functions, `const` / `var` for module-level values.
Three kinds of names are deliberately *not* aliased and are listed in each
facade's header comment: generic functions (Go cannot alias them without
instantiation), package vars that are patch points or mutable state (a copy
would silently diverge -- `laila.PostgresPool_start_local_server_ = f` is the
only place that patches), and symbols behind a build tag (`DuckDBPool`,
`HDF5Pool` under `cgo`).

## Examples

```
go run ./examples/data_types     # examples/simple/data_types.ipynb: every Python type through a pool
go run ./examples/local_pools    # tutorials/01_basics/02_local_pools.ipynb: SQLite + in-memory pools
go run ./examples/peers_tcp      # examples/communication/localhost/peer_active_policy.ipynb: two peered policies
```

Each file's header is a Python -> Go table for the calls it makes.

## Conformance

`python3 laila-go/tools/conformance/check.py` walks the Python tree with
`ast` and the Go tree with a declaration scanner and reports every Python
module, class, method, field, function and module-level name that has no Go
twin under the naming rule, plus every exported Go name with no Python
origin. `tools/conformance/allow.txt` is the complete list of documented
deviations (one line, one reason each); a line that no longer waives
anything fails the run as `STALE`, so the file cannot drift. CI runs the
checker and `gen_facades.py --check` on every change.

## Naming rule (Python -> Go)

| Python | Go | Why |
|---|---|---|
| `memorize`, `global_id`, `add_peer` | `Memorize`, `Global_id`, `Add_peer` | Go exports only capitalised identifiers; snake_case is kept |
| `_read`, `_remote_memorize`, `_LAILA_IDENTIFIABLE_POLICY` | `Read_`, `Remote_memorize_`, `LAILA_IDENTIFIABLE_POLICY_` | leading underscores move to the end so hooks, tests and RPC dispatch can reach them |
| `__getitem__`, `__lshift__`, `__str__`, `__repr__` | `Getitem`, `Lshift`, `String`, `Repr` | dunders drop both underscores |
| `Entry.constant(...)`, `FutureStatus.FINISHED` | `Entry_constant(...)`, `FutureStatus_FINISHED` | classmethods / enum members |
| `S3Pool(bucket_name="b")` | `New_S3Pool(laila.Kw{"bucket_name": "b"})` | a type and a function cannot share a name |
| `entry.data` / `laila.active_policy = p` | `entry.Data()` / `laila.Set_active_policy(p)` | properties |
| `with d.atomic(): ...` | `v, release, err := d.Atomic(); defer release()` | context managers |
| raises `KeyError` | returns `(T, error)` with `errors.Is(err, py.KeyError)` | exceptions |

## Deviations

Filled in per phase. Everything not listed here behaves exactly as the Python
package does, including error messages and serialized bytes.

### Phase 5: storage backends

| Python | Go | Why |
|---|---|---|
| `pip install laila-core[s3]` (and `postgres`, `mongo`, `redis`, `duckdb`, `hdf5`, `gcs`, `azure`, `huggingface`) | `import _ "github.com/amirzadehlambda/laila/laila-go/drivers/s3"` | third-party SDKs live in their own modules under `drivers/`; without the blank import the pool raises the same `ImportError` / `ModuleNotFoundError` text Python does |
| `if self._client is None: self._client = boto3.client(...)` | same, under a per-pool `Client_mu_` mutex | the GIL makes Python's unsynchronised lazy init memory-safe (at worst two clients); in Go it is a data race |
| `_aio_clients` dict keyed by event loop | one shared async client | Go has no event loops; the AWS SDK client is already goroutine-safe |
| `DuckDBPool`, `HDF5Pool` | built only with cgo (`//go:build cgo`); HDF5 needs `pkg-config hdf5` | both backends are C libraries; the datasets written are byte-compatible with `duckdb` / `h5py` |
| `unittest.mock.patch.object(PostgresPool, "_start_local_server")` | assign `laila.PostgresPool_start_local_server_ = func(...)` | the bootstrap steps are package-level function variables so tests can patch them like Python does |

### Phase 6: communication

| Python | Go | Why |
|---|---|---|
| `class _P2PStreamRPCProtocol(_CarrierRPCProtocol): stream_chunk_bytes: int = Field(default=4096)` | `P2PStreamRPCProtocol_class.Set_field_default("stream_chunk_bytes", 4096)` | a subclass changing an inherited field's *default* keeps the single storage slot in the base struct; redeclaring the Go field would shadow the slot the base-class methods read |
| `ch.closed`, `ch.rx_seq`, `ch.dropped`, ... (plain attributes read from any thread) | `ch.Closed()`, `ch.Rx_seq()`, `ch.Dropped()`, ... | the carrier thread updates them while user threads read; Go needs the mutex the GIL provided |
| `proto._event_loop` read from executor workers while `stop()` sets it to `None` | `proto.Loop()` (mutex-guarded; `_event_loop` stays for reflection) | same reason |
| `asyncio.StreamReader(limit=n)` | `py.Stream_pair(r, w, nil, n)` / `reader.Set_limit(n)` | flow-control high-water mark (pause at `2*limit`, resume at `limit`) |
| `_hevc.py`, `_helpers.py` test modules | `hevc.go`, `helpers.go` (non-`_test` files in the test package) | Go test helpers shared across files must be regular package files |
| `object.__setattr__(policy, "echo", fn)` (an ad-hoc RPC target) | `policy.Private_set("echo", fn)` | pydantic models have no instance `__dict__` in Go; dynamic attributes live in the private slot table that `py.Getattr` (and therefore `_execute_rpc`) consults |
| `arr.tolist()` | `arr.Tolist()` (nested `*py.List`); `arr.Item(i)` is `arr.flat[i]` | numpy scalar decoding for the plain numeric dtypes (`b1`, `i1..i8`, `u1..u8`, `f4`, `f8`, both byte orders) |

### Phase 7: facades, tooling, flat-package names

| Python | Go | Why |
|---|---|---|
| `laila/policy/central/communication/protocols/_carriers/` | `policy/central/communication/protocols/carriers_/` (package `carriers_`) | `go build ./...` ignores `_`-prefixed directories; the naming rule (underscore to the end) applies to the path too |
| `from laila.runtime import wait` | `import laila_runtime ".../laila-go/runtime"` | the package is named `runtime` like Python; alias it on import to keep Go's `runtime` reachable |
| `laila.relay(channel)` | `laila.Get_relay(channel)` | `relay` the function and `Relay` the class collide in one flat package |
| `_INSTALL_HINT`, `_NOT_IMPLEMENTED`, `log` (one per module) | `INSTALL_HINT_amqp_`, `NOT_IMPLEMENTED_lora_`, the package logger | module-private names that repeat across modules carry a `_<module>` suffix in the flat package |
| `_REGISTRY` / `Constitution._from_dict` (classmethod overridden per subclass) | `Constitution_REGISTRY_` / `Constitution_from_dict_hooks_` | Go has no classmethod override; each kind registers its hook |
| `laila._LailaModule` (module class swap for `laila.active_policy = p`) | `laila.Set_active_policy(p)`, `laila.Set_logger(l)`, `laila.Set_encryption_key(k)` | module-level property setters |
| `CLIExempt(default=...)` | struct tag `py:"name,cli_exempt"` | field metadata lives in tags |
| `LailaJSONEncoder.default`: pydantic models serialise via `model_dump()` | `Laila_json_default`: any object bound to a `py.Class` goes through `py.Model_dump` | same wire shape for pools / protocols returned from RPC (`resource`, `batch_accelerated`, ... -- private attrs and ClassVars excluded) |
| `pickle.dumps(model)` (`copyreg.__newobj__` + `BaseModel.__getstate__`, `Enum.__reduce_ex__`) | the same opcodes and state layout for every class-bound object and registered enum (`py.Register_enum`) | blobs cross the wire in both directions; the class global is `laila.<flat file>.<Name>`, derived by `py.NewClass` from the declaring file (`atomic.types.atomic_dict.go` -> `laila.atomic.types.atomic_dict.AtomicDict`), and locks are split out / recreated exactly as `_LAILA_LOCALLY_ATOMIC_OBJECT.__getstate__` / `__setstate__` do |

### Phase 8: workload / soak suite

| Python | Go | Why |
|---|---|---|
| `pytest tests/workloads -m workload` | `cd laila-go/tests && go test -tags workload -timeout 0 ./workloads/` | opt-in build tag instead of a pytest marker; `LAILA_WORKLOAD_SCALE`, `LAILA_WORKLOAD_TIMEOUT`, `LAILA_WORKLOAD_TMP` are honoured as in `conftest.py` |
| `subprocess.run([sys.executable, "-c", code])`, `ChildPeer(extra=code)` | `Run_script(name, args)` / `Child_extras[name]`: the test binary re-executes itself with `LAILA_WORKLOAD_SCRIPT` / `LAILA_WORKLOAD_CHILD` set | no `exec` of source strings in Go; each code string is a named func registered from `init()` |
| `pytest.xfail(reason)` (imperative, w17) | `t.Skipf("xfail: ...")` | Go has no expected-failure outcome |
| `threading.active_count()` | `len(py.Enumerate())` (thread probes) / `runtime.NumGoroutine()` (`ResourceProbe.Threads`) | goroutines are not threads; each probe uses the nearest twin |
| `gc.collect()`, `weakref.ref(e)`, `gc.get_objects()` size scans (w24) | `runtime.GC()` + `debug.FreeOSMemory()`, `py.WeakRef_self(e)`, a `runtime.MemStats.HeapAlloc` bound | the Go heap cannot be enumerated; the live-heap bound expresses the same "no 64 MB object survives" invariant |
| `random.Random(seed)`, `np.random.default_rng(seed)` | `math/rand` with the same seed | different sequences, same determinism; no assertion compares the draws across languages |
| `d.atomic(hint=5)` raises `TypeError` (w14) | `py.Call(py.Getattr(d, "atomic"), nil, py.Kw{"hint": 5})` | a non-`str` hint is a compile-time error on `Atomic(hint ...string)`; dynamic dispatch is the only runtime path |
| `seen[i] += 1` from several threads, `if self._client is None` (GIL-atomic) | guarded by a mutex | data races in Go |
| a thread whose body raises dies with a traceback (w23) | goroutine body wrapped in `recover()` | same outcome: the role stops, the test judges `stats` |
| tight `ch.send()` flood (w19 `slow_consumer_keeps_rpc_alive`) | same loop plus an RSS cap that stops the flood and fails the test | without it an unbounded backlog would take the whole test binary down (OOM) instead of failing one test |

The Python suite documents *suspected defects* as `xfail(strict=True)`; the
Go port runs the same bodies through `Xfail_strict`, so a fix on either
side shows up as an XPASS.

## Interop with the Python package

`tests/interop` is the live wire-compatibility gate: the Go port talks to a
CPython `laila` process in both directions (Go client -> Python server and
Python client -> Go server) over the WebSocket (`tcpip`) and raw TCP
transports, exchanging RPCs, `remember` / `memorize` / `forget` with
`policy_id=` (CPython pickle blobs rebuilt in Go and vice versa, including
numpy arrays and a 256 KiB bytes payload) and, on TCP, stream lanes.

```
cd laila-go/tests && go test ./interop/            # ~10 s with a CPython laila
LAILA_PYTHON=/path/to/python3 LAILA_PYTHONPATH=/path/containing/laila go test ./interop/
```

The suite needs a CPython with `laila`, `websockets` and `numpy` importable.
`LAILA_PYTHON` (default: `python3` on `PATH`) and `LAILA_PYTHONPATH`
(default: the parent of the repository root, i.e. the directory that holds
the `laila/` package) select it; when unavailable the suite is *skipped*,
not failed. It is also skipped under `go test -short`.
