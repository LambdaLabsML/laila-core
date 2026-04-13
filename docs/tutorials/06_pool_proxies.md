# Tutorial 6: Pool Proxies — Tiered Caching Across Backends

Set up a multi-tier cache chain so that a fast local pool automatically pulls entries from a slower remote pool on demand. When you read a key from the top-level pool, the request cascades down the chain until it finds the data, then caches a copy in every intermediate layer on the way back up.

This tutorial wires three tiers:

1. **alpha pool** (in-memory) — fastest, used by default
2. **HDF5 pool** — fast local disk
3. **S3 pool** — durable remote storage

A single `<<` expression connects them:

```python
laila.alpha_pool << hdf5_pool << s3_pool
```

## Prerequisites

```bash
pip install "laila-core[s3,hdf5]"
```

You will need an AWS S3 bucket and credentials. Store them in a `secrets.toml` as described in [Tutorial 3](03_remote_pools.md).

## Setup

```python
import laila
from laila.pool import S3Pool, HDF5Pool

laila.read_args("./secrets.toml")
```

## Create the pools

The alpha pool already exists — LAILA creates a default in-memory pool at startup. Create an HDF5 pool and an S3 pool, then register both:

```python
hdf5_pool = HDF5Pool(nickname="proxy_hdf5")

s3_pool = S3Pool(
    bucket_name=laila.args.AWS_BUCKET_NAME,
    access_key_id=laila.args.AWS_ACCESS_KEY_ID,
    secret_access_key=laila.args.AWS_SECRET_ACCESS_KEY,
    region_name=laila.args.AWS_REGION,
    nickname="proxy_s3",
)

laila.memory.extend(hdf5_pool, pool_nickname="proxy_hdf5")
laila.memory.extend(s3_pool, pool_nickname="proxy_s3")
```

## Wire the proxy chain

The `<<` operator reads as "is a cache for". After this line:

- `alpha_pool.proxy_to` → `hdf5_pool`
- `hdf5_pool.proxy_to` → `s3_pool`
- `s3_pool.proxy_to` → `None` (the origin — no further fallback)

```python
laila.alpha_pool << hdf5_pool << s3_pool
```

Reads cascade right: alpha → HDF5 → S3. Writes, deletes, and existence checks are **local only** — they never propagate.

## Step 1: Store an entry directly in S3

Write to S3 only. At this point the alpha pool and HDF5 pool know nothing about this entry:

```python
entry = laila.constant(data={"message": "hello from S3"}, nickname="proxy_demo")

future = laila.memorize(entry, pool_nickname="proxy_s3")
laila.wait(future)

print(f"alpha_pool has it? {laila.alpha_pool.exists(entry.global_id)}")  # False
print(f"hdf5_pool has it?  {hdf5_pool.exists(entry.global_id)}")         # False
print(f"s3_pool has it?    {s3_pool.exists(entry.global_id)}")           # True
```

## Step 2: Read through the alpha pool — cascading lookup

When you ask the alpha pool for this key:

1. Alpha misses → asks HDF5 (its `proxy_to`)
2. HDF5 misses → asks S3 (its `proxy_to`)
3. S3 hits → returns the blob
4. HDF5 caches the blob locally
5. Alpha caches the blob in memory

After this single read, all three tiers hold the entry:

```python
blob = laila.alpha_pool[entry.global_id]

print(f"alpha_pool has it? {laila.alpha_pool.exists(entry.global_id)}")  # True
print(f"hdf5_pool has it?  {hdf5_pool.exists(entry.global_id)}")         # True
print(f"s3_pool has it?    {s3_pool.exists(entry.global_id)}")           # True
```

## Step 3: Second read is a local hit

Now the alpha pool already has the blob cached. The read completes instantly without touching HDF5 or S3:

```python
import time

start = time.perf_counter()
blob_again = laila.alpha_pool[entry.global_id]
elapsed_us = (time.perf_counter() - start) * 1_000_000

print(f"Elapsed: {elapsed_us:.0f} µs (local memory hit)")
```

## Step 4: Writes are local only

Writing to the alpha pool does **not** propagate down to HDF5 or S3. The local pool owns its own writes, and the origin is unaffected:

```python
local_entry = laila.constant(data={"scope": "alpha only"}, nickname="local_only")
laila.alpha_pool[local_entry.global_id] = "local_blob"

print(f"alpha_pool has it? {laila.alpha_pool.exists(local_entry.global_id)}")  # True
print(f"hdf5_pool has it?  {hdf5_pool.exists(local_entry.global_id)}")         # False
print(f"s3_pool has it?    {s3_pool.exists(local_entry.global_id)}")           # False
```

## Step 5: Deletes are local only

Deleting from the alpha pool removes the cached copy but leaves HDF5 and S3 intact. A subsequent read will cascade again:

```python
del laila.alpha_pool[entry.global_id]

print(f"alpha_pool has it? {laila.alpha_pool.exists(entry.global_id)}")  # False
print(f"hdf5_pool has it?  {hdf5_pool.exists(entry.global_id)}")         # True
print(f"s3_pool has it?    {s3_pool.exists(entry.global_id)}")           # True

blob_refetched = laila.alpha_pool[entry.global_id]  # cascades again
print(f"alpha_pool has it? {laila.alpha_pool.exists(entry.global_id)}")  # True
```

## Step 6: Detach and reattach proxies

The proxy chain is fully dynamic. Set `proxy_to = None` to detach, or reassign at any time:

```python
laila.alpha_pool.proxy_to = None
del laila.alpha_pool[entry.global_id]

print(f"alpha_pool[key]: {laila.alpha_pool[entry.global_id]}")  # None (no cascade)

laila.alpha_pool << hdf5_pool << s3_pool
print(f"alpha_pool[key]: {laila.alpha_pool[entry.global_id]}")  # blob (cascade restored)
```

## Alternative syntax: `>>`

The `>>` operator reads in the opposite direction. These two lines are equivalent:

```python
alpha_pool << hdf5_pool << s3_pool
s3_pool >> hdf5_pool >> alpha_pool
```

You can also assign the property directly:

```python
alpha_pool.proxy_to = hdf5_pool
hdf5_pool.proxy_to = s3_pool
```

## Clean up

```python
forget_future = laila.forget(entry.global_id, pool_nickname="proxy_s3")
laila.wait(forget_future)

hdf5_pool.empty()
laila.alpha_pool.proxy_to = None
```

## Summary

- **`cache << origin`** (or **`origin >> cache`**) wires a proxy chain — reads cascade toward the origin and cache on the way back.
- **Writes**, **deletes**, **`exists`**, and **`empty`** are all local only — they never propagate through `proxy_to`.
- The chain can have any number of tiers: `mem << hdf5 << s3`, or `mem << redis << gcs << azure`.
- **`pool.proxy_to = None`** detaches the link; reassign at any time to rewire.
- Proxy fields are `PrivateAttr`, so they are invisible to the CLI and serialization — each pool in the chain remains independently configurable.

**Use case:** keep a fast local cache in front of slow remote storage. The first read pays the network cost; every subsequent read hits local memory or disk.

Next: [Tutorial 7 — Async Futures](07_async_futures.md), where you use `async` / `await` to process entries without blocking the event loop.
