---
hide:
  - toc
---

# LAILA

**Lambda's Interdisciplinary Large Atlas** — a Python platform for unified data
workflows. LAILA wraps heterogeneous storage backends (S3, GCS, Redis, HDF5,
filesystem, and more) behind one consistent API so that *memorizing* data,
*recalling* it, and *managing* it feels the same regardless of where things
live. Compute workflows will be added in later releases.

!!! abstract "At a glance"
    - **One API, many backends** — the same three verbs against any pool.
    - **Type-aware** — NumPy, PyTorch, dicts, model weights all round-trip with
      no manual serialization code.
    - **Async by default** — every operation returns a future you can await,
      poll, or batch.
    - **Deterministic identity** — every entry has a stable `global_id`.

---

## Installation

=== "Core"

    ```bash
    pip install laila-core
    ```

=== "S3 / R2 / B2"

    ```bash
    pip install "laila-core[s3]"
    ```

=== "Redis"

    ```bash
    pip install "laila-core[redis]"
    ```

=== "HDF5"

    ```bash
    pip install "laila-core[hdf5]"
    ```

=== "PyTorch"

    ```bash
    pip install "laila-core[torch]"
    ```

=== "Everything"

    ```bash
    pip install "laila-core[all]"
    ```

!!! tip "Pick what you need"
    Each extras group only pulls in the dependencies for that backend. Mix and
    match — `pip install "laila-core[s3,redis,torch]"` works fine.

---

## Quick example

The shortest path from "I have a NumPy array" to "it lives in S3 and I can get
it back from anywhere with just an id":

```python
import numpy as np
import laila
from laila.pool import S3Pool

# 1. Create a pool (any backend — S3, Redis, HDF5, filesystem, …)
pool = S3Pool(
    bucket_name="your-bucket",
    access_key_id="YOUR_ACCESS_KEY_ID",
    secret_access_key="YOUR_SECRET_ACCESS_KEY",
    region_name="us-east-1",
    nickname="my_pool",
)

# 2. Register the pool with LAILA's memory system
laila.memory.extend(pool, pool_nickname="my_pool")

# 3. Wrap your data in an Entry — a universal container with a unique global_id
entry = laila.constant(data=np.random.randn(10, 10), nickname="my_matrix")
entry_id = entry.global_id  # save the id before we lose the local reference

# 4. Memorize (write) to S3 — returns a future you can wait on
future_memorize = laila.memorize(entry, pool_nickname="my_pool")
laila.wait(future_memorize)

# 5. Destroy local state — the only way to get the data back is through LAILA
del entry

# 6. Remember (read) using just the global_id — reconstructs the entry
future_remember = laila.remember(entry_id, pool_nickname="my_pool")
laila.wait(future_remember)

# .data unwraps the entry and returns your original object,
# preserving the exact Python type that was memorized.
print(type(future_remember.data))  # <class 'numpy.ndarray'>
print(future_remember.data)        # your numpy array, intact
```

!!! note "What just happened?"
    LAILA detected the payload was a NumPy array, dispatched to its
    `NumpySerializer`, wrote the bytes to S3 under a deterministic key derived
    from `global_id`, and recorded a `Constitution` describing how to rebuild
    the entry. On the way back, it ran the constitution in reverse — the
    caller never had to think about serialization formats.

---

## Core concepts

`Entry`
:   An immutable (`constant`) or versioned (`variable`) container for any
    Python object. Each entry has a deterministic `global_id` that acts as
    its permanent address.

`Pool`
:   A storage backend. LAILA ships adapters for S3, GCS, Azure, Redis, HDF5,
    the local filesystem, DuckDB, Postgres, MongoDB, the Hugging Face Hub,
    and SQLite.

`memorize / remember / forget`
:   The three core verbs — write, read, and delete entries against any
    registered pool. Every backend speaks the same vocabulary.

`Future`
:   Async operations return futures. Use `laila.status(future)`,
    `laila.wait(future)`, `.data` to unwrap the payload, or `.result` /
    `.exception` directly.

---

## Where to next

!!! example "Tutorials"
    Walk through LAILA from the ground up — entries, local pools, remote
    pools, manifests, async futures, and policy management.
    [Start with tutorial 01 →](tutorials/01_entries_and_identity.md)

!!! info "LAILA 101"
    A single-page conceptual tour of the platform's design — what an entry
    is, how serialization is dispatched, and how policies tie it all
    together. [Read LAILA 101 →](laila_101.md)

!!! quote "API Reference"
    Auto-generated reference for every public symbol, including type
    signatures and source links. [Browse the API →](reference/laila/index.md)
