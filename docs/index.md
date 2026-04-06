# LAILA

**Lambda's Interdisciplinary Large Atlas**

LAILA is a Python platform for unifying training, simulation, and data management into a single computational workflow. It wraps heterogeneous storage backends (S3, GCS, Redis, HDF5, filesystem, and more) behind one consistent API so that memorizing data, recalling it, and orchestrating compute feels the same regardless of where things live.

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

pool = S3Pool(
    bucket_name="your-bucket",
    access_key_id="YOUR_ACCESS_KEY_ID",
    secret_access_key="YOUR_SECRET_ACCESS_KEY",
    region_name="us-east-1",
    nickname="my_pool",
)

laila.memory.add_pool(pool, pool_nickname="my_pool")

entry = laila.constant(data=np.random.randn(10, 10), nickname="my_matrix")

future = laila.memorize(entry, pool_nickname="my_pool")
laila.wait(future)

recalled = laila.remember(entry.global_id, pool_nickname="my_pool")
laila.wait(recalled)
print(recalled.result.data)
```

## Core concepts

- **Entry** — an immutable (`constant`) or versioned (`variable`) container for any Python object. Each entry has a deterministic `global_id`.
- **Pool** — a storage backend (S3, GCS, Azure, Redis, HDF5, filesystem, DuckDB, Postgres, MongoDB, Hugging Face Hub, SQLite).
- **memorize / remember / forget** — the three core verbs: write, read, and delete entries from any registered pool.
- **Future** — async operations return futures. Use `laila.status(future)`, `laila.wait(future)`, or access `.result` / `.exception` directly.

## Next steps

Start with the [tutorials](tutorials/01_entries_and_identity.md) to learn LAILA from the ground up, or jump to the [API Reference](reference/laila/index.md) for full details.
