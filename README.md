# LAILA

**Lambda's Interdisciplinary Large Atlas**

LAILA is a Python platform for unifying training, simulation, and data management into a single computational workflow. It wraps heterogeneous storage backends (S3, GCS, Redis, HDF5, filesystem, and more) behind one consistent API so that memorizing data, recalling it, and orchestrating compute feels the same regardless of where things live.

```bash
pip install laila-core
```

## Quick example

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

# 3. Wrap your data in an Entry
entry = laila.constant(data=np.random.randn(10, 10), nickname="my_matrix")

# 4. Memorize (write) — returns a future you can wait on
future = laila.memorize(entry, pool_nickname="my_pool")
laila.wait(future)

# 5. Remember (read) — reconstruct the entry from storage
recalled = laila.remember(entry.global_id, pool_nickname="my_pool")
laila.wait(recalled)
print(recalled.result.data)  # your numpy array, intact
```

## Core concepts

| Concept | What it is |
|---------|------------|
| **Entry** | An immutable (`constant`) or versioned (`variable`) container for any Python object — tensors, dicts, strings, model weights. Each entry has a deterministic `global_id`. |
| **Pool** | A storage backend. LAILA ships with pools for S3, GCS, Azure Blob, Cloudflare R2, Redis, HDF5, filesystem, DuckDB, Postgres, MongoDB, Hugging Face Hub, and SQLite. |
| **memorize / remember / forget** | The three core verbs. Write, read, and delete entries from any registered pool using the same interface. |
| **Future** | Async operations return futures. Use `laila.status(future)`, `laila.wait(future)`, or access `.result` / `.exception` directly. |

## Installation extras

Install only the backends you need:

```bash
pip install "laila-core[s3]"        # S3 / Cloudflare R2 / Backblaze B2
pip install "laila-core[redis]"     # Redis
pip install "laila-core[hdf5]"      # HDF5
pip install "laila-core[torch]"     # PyTorch tensor support
pip install "laila-core[all]"       # everything
```

## Vision

LAILA is intended to serve as an interdisciplinary platform for teams that need to move fluidly between data creation, data storage, model training, and large-scale execution. Rather than treating infrastructure boundaries as the primary abstraction, LAILA focuses on ergonomic syntax and reusable interfaces that let users reason about workflows at a higher level.

This approach makes it easier to:

- organize and manage data across multiple storage systems
- connect compute and memory workflows with less boilerplate
- build distributed pipelines that remain readable and maintainable
- reduce the operational friction between experimentation and production-scale execution

## Current release

LAILA is currently in **beta 1.0**.

The current release includes the **command and memory module** as the first public component of the broader platform. Interfaces may continue to evolve as the platform expands and real-world usage informs the next stage of development.

## Learn more

- **[Tutorials](docs/tutorials/)** — progressive walkthroughs from basic entries to full model checkpointing
- **[API Reference](docs/reference/)** — auto-generated from docstrings
- **[Examples](examples/)** — end-to-end notebooks covering datasets, multipool setups, and more

## Credits

- Creator: Amir Zadeh
- Tutorials and Documentation: Jessica Nicholson
- Acknowledgements: Jason Zhang, Xuweiyi Chen, Connor Alvarez
