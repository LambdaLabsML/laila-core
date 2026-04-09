# LAILA

**Lambda's Interdisciplinary Large Atlas**

LAILA is a Python platform for unified data workflows. It wraps heterogeneous storage backends (S3, GCS, Redis, HDF5, filesystem, and more) behind one consistent API so that memorizing data, recalling it, and managing it feels the same regardless of where things live. Compute workflows will be added in later releases.

## Installation

```bash
pip install laila-core
```

Install only the backends you need:

```bash
pip install "laila-core[s3]"        # S3 / Cloudflare R2 / Backblaze B2
pip install "laila-core[redis]"     # Redis
pip install "laila-core[hdf5]"      # HDF5
pip install "laila-core[torch]"     # PyTorch tensor support
pip install "laila-core[all]"       # everything
```

## Quick example

```python
import numpy as np
import laila
from laila.pool import S3Pool

# Create a pool (any backend — S3, Redis, HDF5, filesystem, …)
pool = S3Pool(
    bucket_name="your-bucket",
    access_key_id="YOUR_ACCESS_KEY_ID",
    secret_access_key="YOUR_SECRET_ACCESS_KEY",
    region_name="us-east-1",
    nickname="my_pool",
)

# Register the pool with LAILA's memory system
laila.memory.extend(pool, pool_nickname="my_pool")

# Wrap your data in an Entry — a universal container with a unique global_id
entry = laila.constant(data=np.random.randn(10, 10), nickname="my_matrix")
entry_id = entry.global_id  # save the id before we lose the local reference

# Memorize (write) to S3 — returns a future you can wait on
future_memorize = laila.memorize(entry, pool_nickname="my_pool")
laila.wait(future_memorize)

# Destroy local state — the only way to get the data back is through LAILA
del entry

# Remember (read) using just the global_id — reconstructs the entry from storage
future_remember = laila.remember(entry_id, pool_nickname="my_pool")
laila.wait(future_remember)

# .data unwraps the entry and returns your original object
# Same type that was memorized is remembered — no type casting or
# serialization code needed. LAILA takes care of that.
print(type(future_remember.data))  # <class 'numpy.ndarray'>
print(future_remember.data)        # your numpy array, intact
```

## Core concepts

- **Entry** — an immutable (`constant`) or versioned (`variable`) container for any Python object. Each entry has a deterministic `global_id`.
- **Pool** — a storage backend (S3, GCS, Azure, Redis, HDF5, filesystem, DuckDB, Postgres, MongoDB, Hugging Face Hub, SQLite).
- **memorize / remember / forget** — the three core verbs: write, read, and delete entries from any registered pool.
- **Future** — async operations return futures. Use `laila.status(future)`, `laila.wait(future)`, `.data` to unwrap the result payload, or `.result` / `.exception` directly.

## Next steps

Start with the [tutorials](tutorials/01_entries_and_identity.md) to learn LAILA from the ground up, or jump to the [API Reference](reference/laila/index.md) for full details.
