# laila (Rust)

**Lambda's Interdisciplinary Large Atlas** -- a 1:1 Rust port of the Python
[`laila`](../) package, written so that laila code reads the same in both
languages.

```toml
[dependencies]
laila = { package = "laila-core", version = "0.1" }
```

> Status: the port is complete. The object model (`Arg`/`Args`/`#[pysig]`),
> the `laila` facade, the `laila::python!` rewriter and the `py2rs`
> transpiler; `basics` (identity, `laila.args`, the CLI-capable models and
> the `laila.args.environment` mirror), `laila.atomic`, `Entry` 1:1
> (compdata taxonomy, transformations, simple / complex constitutions with
> the trust gate, the `Manifest` subclass and its SQL index, `laila.build`);
> the data layer 1:1 (the virtual data container, the pool base class, the
> per-pool `PoolIndex`, `PoolWrapper`, `MultiBuffer`, the `Record` envelope,
> the pool router, the local backends -- `FilesystemPool`, `SQLitePool`,
> `DuckDBPool`, `HDF5Pool` link the system libraries Python uses, so a pool
> file written by either implementation opens in the other -- and the
> cloud / database backends speaking the vendors' wire protocols); central
> command (futures, group / remote / identity futures, the thread, async
> and process taskforces, parking, `laila.guarantee`); central
> communication (the RPC envelope, hub / peer routing, the tier-1
> transports, carriers, stream lanes); and the logger. Every functional
> suite under `../tests/functional` and the ten deep-evaluation suites
> under `../tests/deep_eval` run green from their transpiled Rust twins
> (3 831 tests; the 33 deep-eval cases a typed language cannot express --
> `exec` of Python source, test-local subclasses of typed structs, class
> identity of registries -- are skipped with the reason recorded in
> `tests/overrides/`). `docs/PARITY.md` lists the behavioural differences
> that remain, backend by backend.

## The same call, in both languages

```python
import laila
from laila.data import HDF5Pool
from laila.policy.central.memory.record.record import Record

hdf5_pool = HDF5Pool(nickname="cache_hdf5")
laila.memory.extend(hdf5_pool, pool_nickname="cache_hdf5")
laila.alpha_pool << hdf5_pool

entry = laila.constant(data={"msg": "hello"}, nickname="demo")
laila.wait(laila.memorize(entry, pool_nickname="cache_hdf5"))
blob = laila.alpha_pool[entry.global_id]
assert Record._build_sync(blob)["entry"].data["msg"] == "hello"
```

```rust
use laila::prelude::*;
use laila::data::HDF5Pool;
use laila::policy::central::memory::record::Record;

laila::python! {
    fn main() {
        let hdf5_pool = HDF5Pool(nickname="cache_hdf5");
        laila.memory.extend(hdf5_pool, pool_nickname="cache_hdf5");
        laila.alpha_pool << hdf5_pool;

        let entry = laila.constant(data={"msg": "hello"}, nickname="demo");
        laila.wait(laila.memorize(entry, pool_nickname="cache_hdf5"));
        let blob = laila.alpha_pool[entry.global_id];
        assert(Record._build_sync(blob)["entry"].data["msg"] == "hello");
    }
}
```

The differences are exactly the ones Rust's grammar forces: `fn` and braces
instead of `def` and indentation, `let` on first assignment, `;` at the end
of statements, `use` instead of `import`. Everything *between* those -- the
call spelling, keyword arguments, dict literals, indexing, `in`, `is None`,
f-strings, comprehensions, `with`, `try`/`except`, error classes and
messages -- is Python, rewritten at compile time by `laila::python!`.

## Three layers, pick the one you want

| | Write | Get |
|---|---|---|
| **Tier 1** -- typed Rust | `laila.constant(args!(data = py!([1, 2]), nickname = "n"))?` | A plain crate: `Arg`, `Args`, `Result<T, LailaError>`, no macros required. |
| **Tier 2** -- `laila::python! { .. }` | `laila.constant(data=[1, 2], nickname="n")` | Python expression syntax inside Rust functions, items and `impl` blocks. |
| **py2rs** -- transpiler | the `.py` file | A deterministic `.rs` twin of the statement skeleton; CI regenerates and diffs. |

`laila::python!` lowers to Tier 1, and `py2rs` emits Tier 2, so all three
share one implementation. A Rust user never has to touch the macro; a port
reviewer never has to read anything but Python.

### Tier 1 in a sentence

`Arg` is a Python value (`None`, `bool`, `int`, `float`, `str`, `bytes`,
`list`, `tuple`, `dict`, `Entry`, pools, futures, callables, exceptions) with
Python equality, truthiness, arithmetic, indexing, and the list/dict/str
methods, each raising the CPython error text. `Args` is `*args, **kwargs`;
`#[pysig("f(a, b=1, *, c=None)")]` binds it with CPython's `TypeError`
messages. `LailaError` carries the Python exception class and message, so
`str(err)` matches across languages.

### `laila::python!` in a sentence

A function-like proc-macro that parses Python-shaped expressions in Rust item
syntax and emits Tier 1 calls, inserting `?` wherever Python could raise.
It understands `#![unittest]` (generates `#[test]` wrappers for
`unittest.TestCase` classes), `py_class!` dynamic structs, `with`,
`try`/`except`/`finally`, `lambda`, comprehensions, slices, f-strings,
chained comparisons, `and`/`or` returning operands, and `pytest.raises` /
`self.assertRaises`.

### `py2rs` in a sentence

```bash
python3 tools/py2rs/py2rs.py ../tests/functional/entry/unit_tests/test_entry.py \
    -o tests/functional/entry/unit_tests/test_entry.rs --root ..
python3 tools/py2rs/py2rs.py ../tests/functional -o tests/functional --root .. --check   # CI
cargo test -p laila-tests
```

Rewrites `def`/`class`/blocks/assignments/`try`/`with`/decorators, leaves
every expression verbatim, and never needs hand edits: per-file knobs live in
`tests/overrides/<path>.yaml` (`skip`, `skip_tests`, `use`).

## Layout

```
laila-rs/
  laila/           laila-core: the library (lib name `laila`), no_std + alloc core, `std` feature on by default
    src/python/    Arg, Args, builtins, ops, methods, errors, unittest, stdlib shims
    src/basics/    _LAILA_OBJECT, _LAILA_IDENTIFIABLE_OBJECT (Identity), CLI-capable classes, laila.args
    src/atomic/    the locally / globally atomic mixins, Atomic* types
    src/entry/     Entry, EntryState, compdata (taxonomy + transformations), constitution (simple / complex, trust)
    src/policy/    the policy skeleton (identity + central memory), central.memory (Record, PoolRouter, memorize/remember/forget), command.Future, memory.schema.Manifest
    src/data/      schema (data container, pool base: PoolBackend / PoolObject / Pool, PoolIndex, PoolWrapper), multibuffer, filesystem / sqlite / duckdb / hdf5 pools, s3 scaffold
    src/logger/    laila.logger: the Logger singleton (stdlib `logging` sink + pool sink), build_record, get_logger / enable_logging
    src/utils/     laila.args loader, @synchronized
    src/_compat/   uuid v4/v5, json, pydantic, DotMap, datetime, ndarray, numpy.random (SeedSequence / PCG64)
    src/_codecs/   pickle, msgpack, npy, Fernet, base64, zlib, recovery-code templates
  laila-macros/    args!, py!, #[pysig], laila::python!
  tests/           laila-tests: transpiled twins of ../tests/functional/** (+ hand-written conftest twins: builders.rs, compdata_roundtrip.rs)
    interop/       py_peer.rs: Rust <-> CPython pairwise suite; rs_harness/: the Rust peer of the N-language matrix in ../../tests/interop (`cargo build -p laila-interop-harness`)
  examples/        runnable programs: <name>.py (the original, runs under CPython) + <name>.rs (py2rs --script), `cargo run --example <name>`
  tools/py2rs/     the transpiler
  docs/PORT_RULES.md   numbered P-rules both the macro and py2rs implement
```

## Host and embedded

`laila-core` builds `no_std` with `alloc` (`default-features = false`): the
object model, `Entry`, identity, serialization, the in-memory pool with its
index, `MultiBuffer`, the pool router and central memory are available on
embedded targets. The `std` feature adds the filesystem pool, threads, and
the tokio transports; without it the `threading` shim keeps `Lock` /
`RLock` / `Event` / `Semaphore` / `Condition` with single-context
semantics (a wait that would block returns at once, as a timeout) and the
background pipelines (`remember(persist=True)`, pool duplication) run
inline on the caller; the storage backends mirror the Python extras as
features that link the system library (`sqlite` -- on by default, like
CPython's `sqlite3`; `duckdb` and `hdf5` -- `pip install
laila-core[duckdb,hdf5]`; `pools` for all three). `DuckDBPool` needs
`libduckdb.so` 1.5.5 (the engine of the pinned Python wheel; DuckDB files are
only portable within a storage version) at `DUCKDB_LIB_DIR` (`.cargo/config.toml`
points at `/usr/local/lib`), `HDF5Pool` the `libhdf5-dev` package
(`pkg-config hdf5`). The macro crate is host-only (it runs at compile
time).

## Development

```bash
cargo test --workspace                 # Tier 1 spikes + transpiled suites (functional + deep_eval)
cargo clippy --workspace --all-targets # zero warnings
cargo fmt --all -- --check             # generated files are #[rustfmt::skip]
cargo build -p laila-core --no-default-features --target thumbv7em-none-eabihf
python3 tools/py2rs/py2rs.py ../tests/functional/entry/unit_tests/test_entry.py \
    -o tests/functional/entry/unit_tests/test_entry.rs --root .. --check
python3 tools/py2rs/py2rs.py ../tests/deep_eval -o tests/deep_eval --root .. --check
python3 -m unittest tools/py2rs/tests/test_golden.py   # one golden per py2rs rule
cargo test -p laila-core --test macro_rules --test macro_ui  # one case per laila::python! rule; compile-fail diagnostics
python3 tools/conformance/check.py                     # Mirror Law, Layer A
```

`cargo test -p laila-tests --test deep_eval` runs the deep-evaluation
suites alone (about 13 s).

`python3 tools/conformance/check.py` is the Mirror Law gate (Layer A):
one Python module is one Rust file (`a/b/c.py` ↔ `src/a/b/c.rs`,
`__init__.py` ↔ `mod.rs`), one class is one struct / trait / descriptor,
one public method, field, function or constant is one same-named item,
and every `#[pysig("name(params)")]` is the Python signature verbatim.
It diffs the Python AST against the crate and exits non-zero on any
undocumented difference; `allow.txt` holds the five language-forced
deviations with their reasons, and `--internals` lists the
leading-underscore implementation names whose decomposition differs
(asyncio callbacks that became threads, `PrivateAttr` state that became
struct fields) -- advisory, not failures. Layer B is the `py2rs --check`
regenerate-and-diff of the suites and examples above.

The programs in `examples/` are the same script in both languages
(`data_types`: every data type round-tripped through a pool;
`cache_chain`: an in-memory pool fronting SQLite with `<<`; `peers_tcp`:
two policies peered over loopback TCP, an RPC through the peer proxy;
`async_futures`: `.wait()`, `with laila.guarantee`, `await` inside a
submitted coroutine). `python3 examples/data_types.py` and `cargo run
--example data_types` print the same lines; the `.rs` is regenerated from
the `.py` with `py2rs --script` and diffed in CI.

The test profile keeps line tables only (`debug = "line-tables-only"` in
the workspace `Cargo.toml`): the ~130 macro-expanded test binaries are 30 MB
each instead of 500 MB. `cargo test --config profile.test.debug=2` builds
full debuginfo for a debugger session.

Cross-implementation vectors (`laila-c/tests/vectors/*.json`, shared with
`laila-c` and `laila-js`) pin byte identity against CPython: codecs,
compdata, constitutions, `Entry.as_dict` / `global_id` (`vectors/entries.rs`),
nickname UUID-5. Pool files written by the Python backends
(`tests/fixtures/pools/*/python_written.*`, regenerated by the
`gen_fixture.py` beside each) are opened by the Rust pools and the Rust-written
files carry the identical layout (`vectors/pools_{sqlite,duckdb,hdf5}.rs`;
`gen_fixture.py --check <path>` reads a Rust-written file from Python).

Binding cost (release, 1 M calls): a typed four-`Arg` call is 74 ns; the
same call through `args!` + `#[pysig]` is 355 ns; a full
`laila.constant(data=i, nickname="bench")` is 3.4 µs, dominated by the
nickname UUID-5 and the entry allocation. Numbers and method in
`docs/PORT_RULES.md`.

## License

MIT, as the Python package.
