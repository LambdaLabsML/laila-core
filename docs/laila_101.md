# LAILA 101

This page introduces the core concepts behind LAILA. Every piece of the system is built around one idea: you should be able to store, retrieve, and orchestrate any Python object across any backend without writing glue code.

---

## Entry

An **Entry** is LAILA's universal container. It wraps any Python object — a dict, a NumPy array, a PyTorch tensor, a string, model weights — and gives it a deterministic `global_id` that acts as its permanent address.

There are two kinds of entries:

- **`laila.constant(data=...)`** — immutable. Once created, its data and identity are fixed.
- **`laila.variable(data=...)`** — mutable. You can call `entry.evolve(new_data)` to bump its version while keeping the same identity lineage.

```python
import laila

c = laila.constant(data={"model": "v1", "accuracy": 0.93})
print(c.global_id)  # deterministic, unique identifier

v = laila.variable(data=[1, 2, 3])
v = v.evolve([1, 2, 3, 4])  # new entry, same uuid, evolution += 1
```

Every entry, regardless of what it holds, passes through the same `memorize` / `remember` / `forget` interface.

---

## Type-aware serialization

LAILA is **type-free**. When you memorize an entry, LAILA inspects the payload's Python type and dispatches to a specialized serializer — NumPy arrays use `NumpySerializer`, PyTorch tensors use `TorchSerializer`, and everything else falls back to `PickleSerializer`. When you remember the entry, the exact same type comes back.

```python
import torch, numpy as np

laila.memorize(laila.constant(data=np.zeros((3, 3))))       # ndarray in
laila.memorize(laila.constant(data=torch.randn(128, 64)))   # Tensor in
laila.memorize(laila.constant(data={"key": "value"}))       # dict in
```

Each pool can also attach its own **transformation sequence** — base64 encoding, zlib compression, encryption — which wraps the serialized bytes on write and unwraps them on read. The serialized entry stores a **`Constitution`** describing how to rebuild it: a `SimpleConstitution` for ordinary data (an ordered list of inverse-transform code strings) or a `ComplexConstitution` for entries built from a `Manifest` of other entries (a single `f(manifest) -> payload` function). `entry.build()` runs the constitution back-to-back to materialize the payload — the caller never has to think about serialization formats.

---

## Policy

A **Policy** is the top-level object that ties everything together. When you call `laila.memorize(...)` or `laila.remember(...)`, you are talking to the **active policy** — a singleton that LAILA creates automatically on first use.

The policy has four subsystems under `policy.central`:

| Subsystem | Role |
|-----------|------|
| **Command** | Task execution — thread pools, process pools, futures |
| **Communication** | Peer-to-peer networking between policies |
| **Control** | Orchestration logic *(not yet included in the current release)* |
| **Memory** | Storage routing — pools, pool router, memorize/remember/forget |

```
Policy
└── central
    ├── command         ← runs tasks in parallel
    ├── communication   ← connects to remote policies
    ├── control         ← (coming soon)
    └── memory          ← reads and writes to storage backends
```

You rarely need to interact with the policy directly. The top-level `laila.*` functions are shortcuts:

- `laila.memory` → `policy.central.memory`
- `laila.command` → `policy.central.command`
- `laila.communication` → `policy.central.communication`

---

## Command

**Command** is the execution engine. It manages one or more **taskforces** — worker pools that run tasks in parallel and return **futures**.

When you call `laila.memorize(entry)`, behind the scenes command submits serialize-and-write jobs to its taskforce. The result is a future you can wait on:

```python
future = laila.memorize(entry)
laila.wait(future)    # block until done
await future          # or await in async code
```

A taskforce is either thread-based (`PythonThreadPoolTaskForce`) or process-based (`PythonProcessPoolTaskForce`). The default is a thread pool with `max(1, cpu_count // 2)` workers.

Futures track status (`NOT_STARTED`, `RUNNING`, `FINISHED`, `CANCELLED`, `ERROR`) and carry the result payload. A `GroupFuture` aggregates multiple futures from batch operations and reports percentage completion.

---

## Communication

**Communication** enables peer-to-peer connections between policies running on different machines (or the same machine). Each policy can host a WebSocket server and connect to other policies as peers.

```python
# On machine A
laila.communication.start()

# On machine B — connect to A
remote_id = laila.add_peer("ws://machine-a:port", secret="shared-key")

# Now call remote methods as if they were local
laila.peers[remote_id].central.memory.memorize(entry)
```

Under the hood, communication uses JSON-RPC over WebSockets. A `RemotePolicyProxy` lets you chain attribute access (`peer.central.memory.memorize(...)`) — the dotted path is sent as an RPC call and executed on the remote policy.

Each transport is a pluggable **protocol** (currently TCP/IP via WebSockets). The architecture supports adding protocols for shared memory, InfiniBand, or other transports.

---

## Control

**Control** is the orchestration layer for defining higher-level workflows — training loops, simulation pipelines, reactive data flows. It is **not yet included** in the current beta release and will be added in a future version.

---

## Memory

**Memory** is the storage subsystem. It owns the **pool router** and implements the three core verbs:

- **`memorize(entries)`** — serialize each entry, route to the target pool, write.
- **`remember(entry_ids)`** — route to the target pool, read, deserialize.
- **`forget(entry_ids)`** — route to the target pool, delete.

Memory delegates the actual I/O to Command's taskforce, so operations run in parallel and return futures.

```python
entry = laila.constant(data=my_data)

future = laila.memorize(entry, pool_nickname="s3")
laila.wait(future)

result = laila.remember(entry.global_id, pool_nickname="s3")
laila.wait(result)
print(result.data)  # original object, same type
```

---

## Pools

A **Pool** is a storage backend. Every pool implements the same key-value interface — `_read`, `_write`, `_delete`, `_exists`, `_keys`, `_empty` — so the rest of LAILA never has to know what lives behind it.

LAILA ships with pools for:

| Pool | Backend |
|------|---------|
| `S3Pool` | Amazon S3 |
| `CloudflarePool` | Cloudflare R2 |
| `BackblazePool` | Backblaze B2 |
| `GCSPool` | Google Cloud Storage |
| `AzurePool` | Azure Blob Storage |
| `RedisPool` | Redis |
| `HDF5Pool` | HDF5 files |
| `FilesystemPool` | Local filesystem |
| `DuckDBPool` | DuckDB |
| `PostgresPool` | PostgreSQL |
| `MongoPool` | MongoDB |
| `HuggingFacePool` | Hugging Face Hub |
| `SQLitePool` | SQLite |

Pools also support **proxy chaining** for tiered caching. The `<<` operator wires a read-through cache:

```python
laila.alpha_pool << hdf5_pool << s3_pool
```

A read on `alpha_pool` cascades through HDF5 to S3 if the key is not found locally, caching the result in every tier on the way back.

---

## Pool router

The **pool router** sits between Memory and the pools. It decides *which pool* handles a given `memorize` / `remember` / `forget` call.

Every pool is registered with a **nickname** and an optional **affinity** (priority). When you call:

```python
laila.memorize(entry, pool_nickname="s3")
```

the router resolves `"s3"` to the corresponding pool and dispatches the operation. If no nickname is given, the router falls back to the **alpha pool** — a default in-memory pool that LAILA creates at startup.

```python
laila.memory.extend(s3_pool, pool_nickname="s3")
laila.memory.extend(hdf5_pool, pool_nickname="hdf5")
```

The router maintains:

- **`pools`** — a map of `global_id` → pool instance.
- **`pools_nicknames`** — a map of nickname → `global_id`.
- **`pools_pq`** — a priority queue ordered by affinity, used when routing without an explicit target.

The **alpha pool** (`laila.alpha_pool`) is the default destination. It starts as a simple in-memory pool, but you can replace it or wire it into a proxy chain for tiered caching.
