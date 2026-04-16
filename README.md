# LAILA

**Lambda's Interdisciplinary Large Atlas**

```bash
pip install laila-core
```

LAILA is a Python platform for unifying training, simulation, and data management into a single computational workflow. It wraps heterogeneous storage backends (S3, GCS, Redis, HDF5, filesystem, and more) behind one consistent API so that memorizing data, recalling it, and orchestrating compute feels the same regardless of where things live.


## LAILA is type-free

LAILA is **type-free** — whatever type you memorize is exactly the type you get back. No serialization boilerplate, no type casting, one interface for everything:

```python
import torch, laila

dict_entry = laila.constant(data={"key": [1, 2, 3]})
laila.memorize(dict_entry)                          # memorize a dict
laila.remember(dict_entry.global_id).data            # returns a dict

tensor_entry = laila.constant(data=torch.randn(128, 64))
laila.memorize(tensor_entry)                         # memorize a tensor
laila.remember(tensor_entry.global_id).data           # returns a tensor
```




## LAILA has a uniform API
The same three verbs — `memorize`, `remember`, and `forget` — work across every storage backend. S3, HDF5, Cloudflare R2, Redis, GCS, filesystem — swap the pool, keep the code:

```python
from laila.pool import S3Pool, HDF5Pool, CloudflarePool

s3_pool = S3Pool(...)
hdf5_pool = HDF5Pool(...)
cf_pool = CloudflarePool(...)

laila.memory.extend(s3_pool, pool_nickname="s3")
laila.memory.extend(hdf5_pool, pool_nickname="hdf5")
laila.memory.extend(cf_pool, pool_nickname="cloudflare")

entry = laila.constant(data=torch.randn(128, 64))

laila.memorize(entry, pool_nickname="s3")          # write to S3
laila.memorize(entry, pool_nickname="hdf5")        # write to HDF5
laila.memorize(entry, pool_nickname="cloudflare")  # write to Cloudflare R2

laila.remember(entry.global_id, pool_nickname="s3")          # read from S3
laila.remember(entry.global_id, pool_nickname="hdf5")        # read from HDF5
laila.remember(entry.global_id, pool_nickname="cloudflare")  # read from Cloudflare R2

laila.forget(entry.global_id, pool_nickname="s3")          # delete from S3
laila.forget(entry.global_id, pool_nickname="hdf5")        # delete from HDF5
laila.forget(entry.global_id, pool_nickname="cloudflare")  # delete from Cloudflare R2
```

## LAILA has async operations

Every operation returns a **future** you can wait on synchronously or `await` asynchronously:

```python
future = laila.memorize(entry)
laila.wait(future)    # blocking
await future          # or async
```

## Quick example

Stack a fast local cache in front of remote storage with a single operator.
Reads cascade through the chain until they find the data, caching a copy
in every tier on the way back up.

```python
import laila
from laila.pool import S3Pool, HDF5Pool

# Create an HDF5 pool (local disk) and an S3 pool (remote)
hdf5_pool = HDF5Pool(nickname="cache_hdf5")
s3_pool = S3Pool(
    bucket_name="your-bucket",
    access_key_id="YOUR_ACCESS_KEY_ID",
    secret_access_key="YOUR_SECRET_ACCESS_KEY",
    region_name="us-east-1",
    nickname="origin_s3",
)

# Register both pools with LAILA's memory system
laila.memory.extend(hdf5_pool, pool_nickname="cache_hdf5")
laila.memory.extend(s3_pool, pool_nickname="origin_s3")

# Wire a three-tier proxy chain: memory → HDF5 → S3
laila.alpha_pool << hdf5_pool << s3_pool

# Store an entry directly in S3
entry = laila.constant(data={"msg": "hello from S3"}, nickname="proxy_demo")
future = laila.memorize(entry, pool_nickname="origin_s3")
laila.wait(future)

print(laila.alpha_pool.exists(entry.global_id))  # False — not cached yet

# Read through the alpha pool — cascades to S3, caches on the way back
blob = laila.alpha_pool[entry.global_id]

print(laila.alpha_pool.exists(entry.global_id))  # True  — cached in memory
print(hdf5_pool.exists(entry.global_id))          # True  — cached on disk
print(s3_pool.exists(entry.global_id))            # True  — the origin
```

## Core concepts

| Concept | What it is |
|---------|------------|
| **Entry** | An immutable (`constant`) or versioned (`variable`) container for any Python object — tensors, dicts, strings, model weights. Each entry has a deterministic `global_id`. |
| **Pool** | A storage backend. LAILA ships with pools for S3, GCS, Azure Blob, Cloudflare R2, Redis, HDF5, filesystem, DuckDB, Postgres, MongoDB, Hugging Face Hub, and SQLite. |
| **memorize / remember / forget** | The three core verbs. Write, read, and delete entries from any registered pool using the same interface. |
| **Future** | Async operations return futures. Use `laila.status(future)`, `laila.wait(future)`, `.data` to unwrap the result payload, or `.result` / `.exception` directly. |

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
