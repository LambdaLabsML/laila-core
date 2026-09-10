# LAILA Tutorials

A progressive series of hands-on Jupyter notebooks that walk you through LAILA's core concepts, from creating your first entry to peer-to-peer communication, transport selection, SQL-indexed manifests, custom serializers, custom storage backends, and ring buffers for fast producers.

Each tutorial builds on the one before it. If you are new to LAILA, start at the top.

## Basics — `01_basics/`

### 1. Entries and Identity — `01_entries_and_identity.ipynb`

Learn the most fundamental building block in LAILA: the **Entry**. This tutorial covers how to wrap arbitrary data (dicts, numpy arrays, torch tensors) using `laila.constant` and `laila.variable`, and explains the identity system — `global_id`, `uuid`, `scopes`, `evolution`, and `state`. You will also see how **nicknames** give entries deterministic, human-readable identities so the same name always maps to the same storage key.

**No credentials or external services required.**

### 1a. Variables and Evolution — `01a_variables_and_evolution.ipynb`

Dig into the `evolution` counter that lets a single nickname address an ordered sequence of versions of the same logical entry. Memorize successive evolutions, recall a specific one with `remember(nickname=..., evolution=N)`, and see why `Entry.evolve(new_data=...)` returns a new entry instead of mutating in place. Contrasts variables with immutable constants.

**No credentials or external services required.**

### 2. Local Pools — `02_local_pools.ipynb`

Store and recall an entry across three local storage backends — **FilesystemPool**, **RedisPool**, and **HDF5Pool** — using a single for-loop. The point of this tutorial is that the `memorize` / `remember` / `forget` API is identical regardless of which backend you use. Only the pool constructor changes; everything else stays the same.

**Requires:** `pip install "laila-core[redis,hdf5]"`

### 3. S3 with NumPy and PyTorch Tensors — `03_remote_pools.ipynb`

Move to cloud storage. Create an S3 pool, store a numpy matrix and a PyTorch tensor, delete the local references, then recall both objects purely by their `global_id`. This tutorial also introduces future inspection — checking status, reading exceptions, and using the future bank.

**Requires:** `pip install "laila-core[s3,torch]"` and a `secrets.toml` with AWS credentials.

### 4. Model Checkpoint and Reload — `04_manifest_model_checkpoint.ipynb`

Define a small CNN and an Adam optimizer, run a few training steps, then dump every weight tensor and the full optimizer state to S3 as individual entries. A single **manifest** entry ties them all together by mapping parameter names to `global_id` strings. After destroying all local state, you rebuild the entire model and optimizer from that one manifest — using `manifest.realized` to fetch every leaf in one call. Demonstrates how LAILA can serve as a checkpoint system backed by any storage pool.

**Requires:** `pip install "laila-core[s3,torch]"` and a `secrets.toml` with AWS credentials.

### 5. Sentiment Analysis Dataset with a Manifest — `05_manifest_sentiment_dataset.ipynb`

Build a small sentiment-analysis dataset as LAILA entries, organise them under a **Manifest**, push everything to S3, then remember and inspect the dataset from scratch using only the manifest nickname. Demonstrates how `laila.manifest` batches `memorize` / `remember` / `forget` over a nested blueprint and how `manifest.realized` materialises every leaf entry in a single synchronous call.

**Requires:** `pip install "laila-core[s3]"` and a `secrets.toml` with AWS credentials.

### 6. Pool Proxies — Tiered Caching Across Backends — `06_pool_proxies.ipynb`

Wire a multi-tier cache chain with a single `<<` expression: `laila.alpha_pool << hdf5_pool << s3_pool`. When you read a key from the top-level pool, the request cascades down the chain until it finds the data, then caches a copy in every intermediate layer on the way back up. Writes, deletes, and existence checks stay local — only reads propagate. Demonstrates the `<<` / `>>` operators, `proxy_to` property, detaching and reattaching proxies at runtime, and the performance benefit of cache hits.

**Requires:** `pip install "laila-core[s3,hdf5]"` and a `secrets.toml` with AWS credentials.

### 7. Async Operations with Futures — `07_async_futures.ipynb`

Use Python's `async` / `await` with LAILA's futures to read numeric entries from S3, double each value, and write the results back without blocking the event loop. Covers three async patterns: awaiting a `GroupFuture` directly, awaiting individual futures via the future bank, and using `async with laila.guarantee_async:` to automatically track and await every future in scope.

**Requires:** `pip install "laila-core[s3]"` and a `secrets.toml` with AWS credentials.

### 7a. Observability with `laila.logger` — `07a_logger.ipynb`

Turn on structured logging with `enable_logging(level=..., display=..., pool_nickname=...)`. Stream records to stderr, persist them into a pool, swap in a custom `Logger` instance, and see how `set_default_directory` controls the on-disk layout that custom handlers can target. A short tutorial that's worth doing before tackling the Intermediate track — you'll appreciate the traces.

**No credentials or external services required.**

## Intermediate — `02_intermediate/`

### 8a. Saving the Environment to S3 — `08a_environment_to_s3.ipynb`

Set up LAILA with an S3 pool and an HDF5 pool, inspect `laila.args.environment`, wrap the full configuration dict (every policy plus the active `global_id`) as a **Manifest**, and upload it to S3. Demonstrates how `laila.args.environment` captures every CLI-eligible field in each live policy — pool settings, routing, command parameters, communication protocols — as a JSON-serialisable dict, and how storing it as a manifest makes the setup recoverable from anywhere.

**Requires:** `pip install "laila-core[s3,hdf5]"` and a `secrets.toml` with AWS credentials.

### 8b. Recovering a Policy from a Manifest — `08b_policy_from_manifest.ipynb`

Starting with nothing but S3 credentials, bootstrap a minimal policy, remember the environment manifest stored in **8a**, then replay the snapshot with a single assignment: `laila.args.environment = recovered_env`. The hook tears down the bootstrap policy via `laila.terminate(...)`, rebuilds every policy listed in the snapshot — pools, taskforces, and protocols included — and activates the one identified by `active_gid`.

**Requires:** `pip install "laila-core[s3]"`, a `secrets.toml` with AWS credentials, and a completed run of **8a**.

### 9. Peer-to-Peer Communication on Localhost — `09_peer_request_entry.ipynb`

Launch a second policy in a subprocess, peer with it over WebSocket via `laila.add_peer("ws://...", secret)`, and read, write, and delete entries on the peer with `dst_policy=` / `policy=` while the active policy stays local. Shows that peering is symmetric (the subprocess reads back from the notebook), introduces the `RemotePolicyProxy`, and covers **morph mode** (`laila.active_policy = laila.peers[gid]`, `RemoteFuture`) as the secondary path.

**No credentials or external services required.**

### 9a. The Transport Zoo — `09a_transport_zoo.ipynb`

Run the same peer flow over **loopback**, **raw TCP**, **UDP**, a **Unix socket**, and **TLS** (self-signed cert, optional) using `Default{Loopback,TCP,UDP,UnixSocket,TLS}Protocol`, `add_connection`, and URI-driven `add_peer`; switch the wire codec to msgpack. Hold two transports to one peer and pin a channel with `comm=`, `laila.request(gid, comm_protocol=...)`, or `proxy.via(...)`. Tours the liveness loop (`liveness_interval`, `ping`, `remove_peer`), the backpressure knobs (`max_inflight_rpcs`, `rpc_backoff_*`, `BackpressureError`), and how transports appear in `laila.args.environment`.

**No credentials or external services required.**

### 9b. Peer Routing and 3-Party Relays — `09b_peer_routing_and_relay.ipynb`

Address a peer by `global_id`, by its proxy object, or by **nickname**; understand `persist=` on peer reads and the string-only rule for peer-side pools. Then orchestrate a transfer between two *other* policies with `src_policy=` / `dst_policy=`: a **push relay** (`memorize`) and a **pull relay** (`remember`), with the topology rule (orchestrator peered to the source, source peered to the destination) enforced by `ConnectionError`.

**No credentials or external services required.**

### 10. Accessing S3 Through a Remote Peer — `10_peer_remote_s3.ipynb`

Peer with a subprocess that holds AWS credentials and an S3 pool, then memorize / remember / forget on S3 through it with `dst_policy=node_b, dst_pool="s3"` — all without local S3 access and without changing the active policy. A short morph-mode section shows the same thing with implicit routing. Demonstrates how peering turns a remote node's storage into a transparent backend for the local process.

**Requires:** `pip install "laila-core[s3]"` and a `secrets.toml` with AWS credentials.

### 11. Lazy Entries with Constitutions — `11_constitutions_and_build.ipynb`

A LAILA entry can carry a **constitution** instead of a payload — a recipe for computing the payload at build time. This tutorial walks through both `SimpleConstitution` (an inverse transform chain) and `ComplexConstitution` (a function bound to a `Manifest` of inputs), shows that `entry.data` raises until `laila.build(entry)` completes, and demonstrates the same future awaitable both synchronously and asynchronously.

**No credentials or external services required.**

### 12. Routing CPU work to a Process Pool — `12_process_pool_taskforce.ipynb`

The default taskforce is async-thread-pool-backed — great for I/O, GIL-limited for CPU work. Register a `PythonProcessPoolTaskForce` (requires `num_workers >= 4`) with `laila.command.add_taskforce(tf)` and route a CPU-bound function to it via `taskforce_id`. Compare wall-clock times and tear both pools down with `laila.terminate(wait=True)`.

**No credentials or external services required.**

### 13. Configuring LAILA from a TOML file — `13_cli_and_toml_config.ipynb`

`laila.read_args(source)` populates `laila.args` from TOML, JSON, `.env`, XML, or terminal-style `key=value` arguments. Every CLI-capable class then walks `laila.args` to find defaults during validation. This tutorial covers the file-loading surface and the four-tier resolution order, with explicit gotchas about `from_terminal`'s parsing rules.

**No credentials or external services required.**

### 14. Multi-pool Routing — `14_multi_pool_routing.ipynb`

Register three pools (`hot` / `warm` / `cold`) under separate nicknames and use `dst_pool=` on each `memorize` / `remember` call (and `pool=` on `forget`) to direct it to a specific destination — by nickname, by gid, or by passing the pool object itself, including a standalone pool that was never registered. Builds a `Manifest` whose leaves span all three pools and verifies the router resolves each leaf correctly at realization time. Notes the `pool_nickname=` / `pool_id=` back-compat aliases.

**Requires:** `pip install "laila-core[hdf5]"`

### 14a. Querying a Manifest with SQL — `14a_manifest_sql_index.ipynb`

Turn a manifest whose top-level values are metadata dicts into a small catalog and filter it with `Manifest.sql("SELECT ... FROM ... WHERE ...")`. Covers the supported grammar and what is rejected (`ORDER BY`, `LIMIT`, unquoted strings), explicit index control with `build_index(on=, composite=)`, automatic invalidation on `extend` / `+=`, `clear_index()`, reusable on-disk indexes via `persist=`, and flattening list-valued metadata through a `_sql_rows` override. The index is LAILA's sanctioned *non-memorizing algorithmic helper* — a local sqlite file under `<laila_root>/indices/` that never enters memory or the wire.

**No credentials or external services required.**

### 15. Migrating Entries Between Pools — `15_pool_migration.ipynb`

`forget` is pool-local — moving entries between backends is a read-write-forget loop. This tutorial sets up a `warm` filesystem pool and a `cold` HDF5 pool, populates `warm`, then migrates the lot to `cold` with `persist=False` reads so the alpha pool doesn't re-cache mid-migration. Verifies emptiness on the source and completeness on the destination.

**Requires:** `pip install "laila-core[hdf5]"`

### 16. Working with the Future Bank — `16_future_bank_deep_dive.ipynb`

Every async operation registers a future in `policy.future_bank` keyed by gid. Submit a batch, inspect children of the resulting `GroupFuture`, look up a future by gid string via `laila.runtime.status / result`, and recover a captured exception from a deliberately failing build. Closes with `terminate(cancel_pending=True)` to discard queued work.

**No credentials or external services required.**

### 17. Three-Node Mesh on Localhost — `17_three_node_mesh.ipynb`

Three policies in one process, each on its own raw-TCP listener, fully pair-peered with `add_peer("tcp://...")`. Only the orchestrating node is activated, so cross-node reads and writes with `dst_policy=` really cross the wire. Seed one entry per node, fetch any entry from any node, compare `laila.peers` with `laila.universe`, then drop one link with `remove_peer` and take a node offline with `communication.stop()` while the rest of the mesh stays healthy. Extends Tutorial 9 (two-node) to an N-node mesh pattern.

**No credentials or external services required.**

## Advanced — `03_advanced/`

### 18. End-to-End Encrypted Entries — `18_encryption.ipynb`

Drop a `FernetEncryption` step into any pool's transformation sequence and get at-rest encryption for free. The `transformation_base64_compression_encryption(key)` preset compresses → encrypts → base64-encodes; the demo verifies the on-disk bytes contain no plaintext yet `remember` returns the original payload. Covers key rotation, TTLs, and layering with compression.

**Requires:** `pip install "laila-core[crypto]"`

### 19. Object Stores Beyond AWS — `19_object_stores_beyond_aws.ipynb`

Same `memorize` / `remember` API, four more constructors: `GCSPool`, `AzurePool`, `CloudflarePool` (R2), `BackblazePool` (B2). One round-trip per provider, all driven from a single `secrets.toml`. Each cell wraps its provider in try/except so missing credentials skip cleanly.

**Requires:** `pip install "laila-core[gcs,azure,cloudflare,backblaze]"` and a `secrets.toml` with credentials for whichever providers you want to exercise.

### 20. Structured Backends — DuckDB, SQLite, Postgres, Mongo — `20_structured_backends.ipynb`

Four pool backends for the case where you want LAILA entries alongside structured data. DuckDB and SQLite are file-based; Postgres and Mongo can self-host a managed local server or attach to an existing cluster. Round-trips both a numpy array and a dict through each to show the API parity.

**Requires:** `pip install "laila-core[duckdb,postgres,mongo]"` (SQLite is stdlib, no extra).

### 21. Publishing to the Hugging Face Hub — `21_huggingface_publishing.ipynb`

`HuggingFacePool` turns the HF Hub into a LAILA pool. Build a tiny CNN, capture every parameter as an entry, tie them together in a manifest, and memorize the lot — the artefacts appear as a real HF repo. Restore from scratch using just the manifest nickname.

**Requires:** `pip install "laila-core[huggingface,torch]"` and an HF token with write access to a repo you control.

### 22. Writing a Custom Serializer — `22_custom_serializer.ipynb`

Subclass `_data_transformation` for a hand-written msgpack serializer over a custom dataclass, then `register_cdtype(MyType)` a `ComputationalData` wrapper so LAILA picks it up automatically. Round-trip the new type through any existing pool to confirm the registry works end-to-end.

**No credentials or external services required.**

### 23. Building a Custom Pool Backend — `23_custom_pool_backend.ipynb`

Subclass `_LAILA_IDENTIFIABLE_POOL`, override the six sync hooks (`_read`, `_write`, `_delete`, `_exists`, `_keys`, `_empty`), and optionally override the async variants for true non-blocking I/O. The toy `DictPool` shown here is what a real backend looks like before plumbing in a database client or network handle.

**No credentials or external services required.**

### 24. MultiBuffer — A Ring of Records for Fast Producers — `24_multibuffer.ipynb`

Where a pool is a map keyed by `global_id`, `MultiBuffer` is a fixed ring of integer-indexed slots with independent read and write heads — the container a microcontroller puts in front of a camera or sensor that outruns persistence. Write `Entry`, `Record`, or raw payloads and read `Entry` objects back; watch the heads wrap and the producer lap the consumer; use `mapped=True` over externally owned memory (DMA-style `bytearray` slots) where `write()` only advances the head; then run the camera loop that `laila.memorize`s every frame. Explains why it is a `DataContainer` but not a pool.

**No credentials or external services required.**

## Getting started

```bash
pip install "laila-core[all]"
cd tutorials/
jupyter notebook
```

Open the first notebook and work through them in order. The Advanced track (18+) assumes you've completed at least the Manifest tutorials (5, 8a, 8b, 14a).
