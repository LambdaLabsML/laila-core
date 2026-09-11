# Example 2: Data Loader — Prefetching Through memory << hdd << cloudflare

Stream the `my_dataset` images from [Example 1](01_dataset_creation.md) into a training loop as torch tensors without ever waiting on the network. The loader sits on top of a three-tier proxy chain

```python
laila.alpha_pool << hdd << r2
```

and keeps a **lookahead of 4 batches** in flight: while the model consumes batch *i*, batches *i+1 … i+4* are already being pulled from R2 into the local HDF5 cache and decoded into tensors on a background taskforce. By the time `next()` reaches them, they are ready.

Each sample goes through the same async pipeline: `laila.remember(gid)` (which cascades memory ← hdd ← cloudflare), then a cast from image bytes to a `torch.Tensor`. After a batch has been through the model, its entries are always forgotten from the in-memory alpha pool and, optionally, from the HDD cache too.

## Prerequisites

```bash
pip install "laila-core[cloudflare,hdf5,torch]" pillow
```

Run [Example 1](01_dataset_creation.md) first so the `my_dataset` manifest exists in your R2 bucket, and reuse the same `secrets.toml`.

## Setup

```python
import asyncio
import collections
import functools
import io
import math
import time

import numpy as np
import torch
from PIL import Image

import laila
from laila.data import CloudflarePool, HDF5Pool
from laila.policy.central.memory.schema import Manifest

laila.read_args("./secrets.toml")
```

## Create the pools and wire the chain

Three tiers: the **alpha pool** (in-memory, created by LAILA at startup), an **HDF5 pool** standing in for the local HDD, and the **R2 pool** holding the dataset.

```python
hdd = HDF5Pool(nickname="hdd")

r2 = CloudflarePool(
    account_id=laila.args.R2_ACCOUNT_ID,
    access_key_id=laila.args.R2_ACCESS_KEY_ID,
    secret_access_key=laila.args.R2_SECRET_ACCESS_KEY,
    bucket_name=laila.args.R2_BUCKET,
    nickname="r2",
)

laila.memory.extend(hdd, pool_nickname="hdd")
laila.memory.extend(r2, pool_nickname="r2")

laila.alpha_pool << hdd << r2

print(f"alpha -> {type(laila.alpha_pool.proxy_to).__name__}")
print(f"hdd   -> {type(hdd.proxy_to).__name__}")
print(f"r2    -> {r2.proxy_to}")
```

Expected output:

```
alpha -> HDF5Pool
hdd   -> CloudflarePool
r2    -> None
```

`<<` reads as "is a cache for". A `laila.remember(gid)` routed to the alpha pool (the default) now does the following on a cold key:

1. alpha misses → asks `hdd`
2. `hdd` misses → asks `r2`
3. `r2` hits → returns the blob
4. `hdd` writes the blob to disk
5. alpha keeps the blob in memory and rebuilds the entry

On the second epoch step 2 becomes a disk hit and R2 is never contacted. Writes and deletes stay local to the pool they are issued on, which is what lets the loader evict from memory without touching the disk cache.

## Load the manifest

Rebuild the manifest identity from its nickname and fetch the blueprint from R2. The flat list of `global_id` strings is the dataset index:

```python
cold = Manifest(nickname="my_dataset")
ref = laila.remember(cold.global_id, dst_pool="r2", persist=False)
manifest = Manifest(data=ref.wait().data, nickname="my_dataset")
ref.release()

gids = list(manifest)
print(f"{len(gids)} images in my_dataset")
```

Expected output:

```
64 images in my_dataset
```

## The transform: bytes to tensor

Decode PNG or JPEG bytes with Pillow, move channels first, and scale to `[0, 1]`:

```python
def bytes_to_tensor(image_bytes: bytes) -> torch.Tensor:
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    array = np.array(image, dtype=np.uint8)                # H x W x C (writable copy)
    return torch.from_numpy(array).permute(2, 0, 1).float() / 255.0
```

## The data loader

`LailaDataLoader` is a plain Python iterator. The important pieces:

- **`_prepare_sample`** is the per-datapoint pipeline: `await laila.remember(gid)` through the proxy chain, then `transform`.
- **`_prepare_batch`** runs every sample of a batch concurrently with `asyncio.gather` and stacks the tensors.
- **`_schedule_next`** hands `_prepare_batch` to LAILA's taskforce with `laila.command.submit`, which returns a future immediately. That future is the "pre-ask".
- **`__iter__`** schedules the first `lookahead` batches; **`__next__`** waits on the oldest future, immediately schedules one more to keep the window full, forgets the finished batch, and returns the tensor.

```python
class LailaDataLoader:
    """Iterate a list of global_ids as stacked torch tensors, prefetching
    ``lookahead`` batches through ``alpha << hdd << r2``."""

    def __init__(
        self,
        gids,
        batch_size,
        *,
        lookahead=4,
        transform=bytes_to_tensor,
        forget_from_hdd=False,
        hdd_pool="hdd",
    ):
        self.gids = list(gids)
        self.batch_size = batch_size
        self.lookahead = lookahead
        self.transform = transform
        self.forget_from_hdd = forget_from_hdd
        self.hdd_pool = hdd_pool

        self._pending = collections.deque()   # (batch_gids, future), oldest first
        self._next_to_schedule = 0
        self.last_wait_s = 0.0                # time spent blocked in the last __next__

    def __len__(self):
        return math.ceil(len(self.gids) / self.batch_size)

    def _batch_gids(self, idx):
        return self.gids[idx * self.batch_size : (idx + 1) * self.batch_size]

    # -- async pipeline, runs on LAILA's taskforce -------------------------
    async def _prepare_sample(self, gid):
        ref = laila.remember(gid)             # alpha << hdd << r2 cascade
        entry = await ref
        ref.release()
        return self.transform(entry.data)     # bytes -> torch.Tensor

    async def _prepare_batch(self, batch_gids):
        tensors = await asyncio.gather(*(self._prepare_sample(g) for g in batch_gids))
        return torch.stack(tensors)

    # -- scheduling ---------------------------------------------------------
    def _schedule_next(self):
        if self._next_to_schedule >= len(self):
            return
        batch_gids = self._batch_gids(self._next_to_schedule)
        future = laila.command.submit([functools.partial(self._prepare_batch, batch_gids)])
        self._pending.append((batch_gids, future))
        self._next_to_schedule += 1

    def _forget(self, batch_gids):
        futures = [laila.forget(batch_gids)]                       # alpha pool: always
        if self.forget_from_hdd:
            futures.append(laila.forget(batch_gids, pool=self.hdd_pool))
        for f in futures:
            f.wait()
            f.release()

    # -- iterator protocol --------------------------------------------------
    def __iter__(self):
        self._pending.clear()
        self._next_to_schedule = 0
        for _ in range(min(self.lookahead, len(self))):
            self._schedule_next()
        return self

    def __next__(self):
        if not self._pending:
            raise StopIteration
        batch_gids, future = self._pending.popleft()

        start = time.perf_counter()
        future.wait()                         # returns at once if the lookahead kept up
        self.last_wait_s = time.perf_counter() - start
        batch = future.data                   # the stacked tensor returned by _prepare_batch
        future.release()

        self._schedule_next()                 # keep `lookahead` batches in flight
        self._forget(batch_gids)              # batch is done: free memory (and optionally disk)
        return batch
```

Nothing in `__next__` talks to R2 directly. The only place the network is touched is inside `_prepare_sample`, and that runs on the taskforce up to four batches before the loop asks for it.

## Epoch 1: cold start, R2 → HDD → memory

```python
loader = LailaDataLoader(gids, batch_size=8, lookahead=4)

for step, batch in enumerate(loader):
    loss = batch.mean()                       # stand-in for a model step
    print(
        f"step {step}: batch {tuple(batch.shape)} {batch.dtype} "
        f"waited {loader.last_wait_s * 1000:6.1f} ms  loss={loss:.3f}"
    )

first = gids[0]
print(f"\nalpha has image 0? {laila.alpha_pool.exists(first)}")
print(f"hdd   has image 0? {hdd.exists(first)}")
```

Expected output (timings depend on your connection):

```
step 0: batch (8, 3, 32, 32) torch.float32 waited  346.3 ms  loss=0.500
step 1: batch (8, 3, 32, 32) torch.float32 waited    6.2 ms  loss=0.498
step 2: batch (8, 3, 32, 32) torch.float32 waited    0.3 ms  loss=0.500
step 3: batch (8, 3, 32, 32) torch.float32 waited    0.2 ms  loss=0.497
step 4: batch (8, 3, 32, 32) torch.float32 waited  306.2 ms  loss=0.499
step 5: batch (8, 3, 32, 32) torch.float32 waited    0.2 ms  loss=0.498
step 6: batch (8, 3, 32, 32) torch.float32 waited    0.2 ms  loss=0.496
step 7: batch (8, 3, 32, 32) torch.float32 waited    0.2 ms  loss=0.499

alpha has image 0? False
hdd   has image 0? True
```

Step 0 pays for the first R2 round-trip. The first four batches were requested at the same time, so batches 1–3 arrive together with batch 0 and cost nothing. Step 4 shows a second round-trip only because the stand-in "model step" here takes microseconds: batch 4 was scheduled the instant batch 0 was handed out, and the loop reached it before Cloudflare could answer. With a real model, four steps of forward/backward pass are far longer than one fetch, and every `next()` after the first finds its tensor waiting. Increase `lookahead` if your steps are shorter than your network latency.

After the epoch the alpha pool is empty (each batch was forgotten from memory once consumed) while the HDF5 cache still holds every image.

## Epoch 2: disk hits

Run the same loader again. The chain now stops at `hdd`; R2 is never contacted:

```python
for step, batch in enumerate(loader):
    print(f"step {step}: waited {loader.last_wait_s * 1000:6.1f} ms")
```

Expected output:

```
step 0: waited   70.0 ms
step 1: waited   22.9 ms
step 2: waited    0.2 ms
...
```

Step 0 still shows the decode cost of the first batch, but it is disk latency rather than a round-trip to Cloudflare.

## Optional: free disk after each batch

If the dataset is larger than your disk, set `forget_from_hdd=True`. Every batch is then forgotten from **both** the alpha pool and the HDD cache once the model has seen it, so at most `lookahead + 1` batches are ever on disk:

```python
lean_loader = LailaDataLoader(gids, batch_size=8, lookahead=4, forget_from_hdd=True)

for step, batch in enumerate(lean_loader):
    pass

print(f"alpha has image 0? {laila.alpha_pool.exists(first)}")
print(f"hdd   has image 0? {hdd.exists(first)}")
print(f"r2    has image 0? {r2.exists(first)}")
```

Expected output:

```
alpha has image 0? False
hdd   has image 0? False
r2    has image 0? True
```

The origin is untouched: `forget` is pool-local, so evicting from the caches never deletes from R2.

## Clean up

Delete the dataset and manifest from R2, drop the local caches, and detach the chain:

```python
with laila.guarantee:
    manifest.forget(pool_nickname="r2")

hdd.empty()
laila.alpha_pool.proxy_to = None

print(f"manifest still in R2? {r2.exists(manifest.global_id)}")
```

Expected output:

```
manifest still in R2? False
```

## What just happened

1. **`laila.alpha_pool << hdd << r2`** turned the default pool into the front of a read-through cache: memory ← disk ← cloud.
2. **`laila.command.submit`** put `_prepare_batch` on a LAILA taskforce and returned a future immediately. The loader keeps four of these futures queued, so the network work for batch *i+4* starts as soon as batch *i* is handed to the training loop.
3. Inside the taskforce, **`await laila.remember(gid)`** walked the chain, cached the blob in `hdd` and alpha, and produced the entry; **`bytes_to_tensor`** decoded the PNG/JPEG bytes into a `torch.Tensor`. `asyncio.gather` did this for all samples of a batch concurrently.
4. **`__next__`** only ever waited on an already-running future, then topped the window back up to four.
5. **`laila.forget(batch_gids)`** removed the consumed batch from the alpha pool; with `forget_from_hdd=True` it also removed it from the HDF5 cache. R2 was never modified until the explicit clean-up.

## Summary

- Prefetching is a queue of futures: schedule `lookahead` batches up front, then schedule one more every time one is consumed.
- `laila.remember` through a proxy chain is the whole caching strategy; the loader has no R2-specific code.
- Per-sample async transforms (`remember` → `bytes_to_tensor`) compose with `asyncio.gather` and run on LAILA's taskforce, off the training thread.
- Always `forget` from the alpha pool after a batch; `forget` from the HDD tier only when disk is scarce, since keeping it makes the next epoch free of network I/O.
- `forget` is pool-local, so cache eviction never touches the origin bucket.

Back to [Example 1 — Dataset Creation](01_dataset_creation.md) or the [Tutorials](../tutorials/06_pool_proxies.md) for more on proxy chains.
