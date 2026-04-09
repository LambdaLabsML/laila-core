# Tutorial 2: Local Pools — Filesystem, Redis, HDF5

In [Tutorial 1](01_entries_and_identity.md) you created entries. Now you will store them in actual storage backends. LAILA provides a uniform `memorize` / `remember` / `forget` interface that works identically across every pool. This tutorial demonstrates that by writing the same entry to three local backends in a single loop.

## Prerequisites

Install the required extras:

```bash
pip install "laila-core[redis,hdf5]"
```

The filesystem pool has no extra dependencies.

## Setup

```python
import numpy as np
import laila
from laila.pool import FilesystemPool, RedisPool, HDF5Pool
```

## Creating the pools

Each pool type manages its own storage location automatically. No paths or servers to configure manually — LAILA handles defaults:

```python
pools = [
    ("fs",    FilesystemPool(nickname="tutorial_fs")),
    ("redis", RedisPool(nickname="tutorial_redis")),
    ("hdf5",  HDF5Pool(nickname="tutorial_hdf5")),
]
```

- **FilesystemPool** creates a local disk image under LAILA's default directory.
- **RedisPool** starts a private `redis-server` on a UNIX socket — no system-level Redis needed.
- **HDF5Pool** creates a local `.h5py` file.

## Register, memorize, remember, verify — in a loop

The key point: the code inside the loop is identical for all three backends.

```python
entry = laila.constant(data=np.random.randn(10, 10), nickname="tutorial_matrix")

for nick, pool in pools:
    # Register the pool
    laila.memory.extend(pool, pool_nickname=nick)

    # Memorize (write)
    future = laila.memorize(entry, pool_nickname=nick)
    laila.wait(future)

    print(f"[{nick}] memorized — status: {laila.status(future)}")

    # Remember (read)
    recall_future = laila.remember(entry.global_id, pool_nickname=nick)
    laila.wait(recall_future)
    recalled_data = recall_future.data

    print(f"[{nick}] remembered — data shape: {recalled_data.shape}")

    # Verify round-trip
    assert np.array_equal(entry.data, recalled_data), f"Data mismatch on {nick}!"
    print(f"[{nick}] verified ✓")

    # Clean up
    forget_future = laila.forget(entry.global_id, pool_nickname=nick)
    laila.wait(forget_future)
    print(f"[{nick}] cleaned up\n")
```

Expected output:

```
[fs] memorized — status: FutureStatus.FINISHED
[fs] remembered — data shape: (10, 10)
[fs] verified ✓
[fs] cleaned up

[redis] memorized — status: FutureStatus.FINISHED
[redis] remembered — data shape: (10, 10)
[redis] verified ✓
[redis] cleaned up

[hdf5] memorized — status: FutureStatus.FINISHED
[hdf5] remembered — data shape: (10, 10)
[hdf5] verified ✓
[hdf5] cleaned up
```

## What just happened

1. **`extend`** registered each backend under a nickname so `memorize` / `remember` know where to route.
2. **`memorize`** serialized the entry, applied the pool's transformation sequence (base64 by default), and wrote it to the backend. It returned a future.
3. **`wait`** blocked until the write completed.
4. **`remember`** fetched the blob by `global_id`, reversed the transformations, and reconstructed the entry.
5. **`forget`** deleted the stored blob.

The same four steps work for every pool LAILA supports — S3, GCS, Azure, Postgres, MongoDB, and more. Only the pool constructor changes.

## Summary

- All pools share the same `memorize` / `remember` / `forget` API.
- `FilesystemPool`, `RedisPool`, and `HDF5Pool` require zero external infrastructure.
- The pool nickname is how you route data to a specific backend.
- Round-trip integrity is guaranteed: what you memorize is exactly what you remember.

Next: [Tutorial 3 — S3 with Tensors](03_s3_tensors.md), where you work with cloud storage and both numpy and PyTorch tensors.
