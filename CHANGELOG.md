# Changelog

All notable changes to **laila-core** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
