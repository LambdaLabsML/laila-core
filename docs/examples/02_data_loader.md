# Example 2: Data Loader — Prefetching Through memory << hdd << cloudflare

Stream the `my_dataset` images from [Example 1](01_dataset_creation.md) into a training loop as torch tensors without ever waiting on the network. The loader sits on top of a three-tier proxy chain

```python
laila.alpha_pool << hdd << r2
```

and keeps a **lookahead of 4 batches** in flight: while the model consumes batch *i*, batches *i+1 … i+4* are already being pulled from R2 into the local HDF5 cache and decoded into tensors on a background taskforce. By the time `next()` reaches them, they are ready.

A batch is a **sub-manifest**: `manifest.sub_manifest([...])` slices the dataset manifest down to the keys of one batch, and `await sub.async_realized` remembers every entry in it through the proxy chain (memory ← hdd ← cloudflare). Each realized entry is then cast from image bytes to a `torch.Tensor`. After a batch has been through the model, its entries are always forgotten from the in-memory alpha pool and, optionally, from the HDD cache too.

## Prerequisites

```bash
pip install "laila-core[cloudflare,hdf5,torch]" pillow
```

Run [Example 1](01_dataset_creation.md) first so the `my_dataset` manifest exists in your R2 bucket, and reuse the same `secrets.toml`.

## Setup

```python
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

`<<` reads as "is a cache for". Any read routed to the alpha pool (the default) now does the following on a cold key:

1. alpha misses → asks `hdd`
2. `hdd` misses → asks `r2`
3. `r2` hits → returns the blob
4. `hdd` writes the blob to disk
5. alpha keeps the blob in memory and rebuilds the entry

On the second epoch step 2 becomes a disk hit and R2 is never contacted. Writes and deletes stay local to the pool they are issued on, which is what lets the loader evict from memory without touching the disk cache.

## Load the manifest

Rebuild the manifest identity from its nickname and fetch the blueprint from R2. Its top-level keys (`image_0000` … `image_0063`) are the dataset index:

```python
cold = Manifest(nickname="my_dataset")
ref = laila.remember(cold.global_id, dst_pool="r2", persist=False)
manifest = Manifest(data=ref.wait().data, nickname="my_dataset")
ref.release()

keys = list(manifest.keys())
print(f"{len(keys)} images in my_dataset: {keys[0]} ... {keys[-1]}")
```

Expected output:

```
64 images in my_dataset: image_0000 ... image_0063
```

## Slicing a batch with `sub_manifest`

`sub_manifest` returns a new `Manifest` holding only the requested top-level keys. It is a pure blueprint operation, nothing is fetched yet:

```python
sub = manifest.sub_manifest(keys[:8])

print(f"sub-manifest keys: {list(sub.keys())}")
print(f"first global_id:   {sub['image_0000']}")
print(f"alpha has it?      {laila.alpha_pool.exists(sub['image_0000'])}")
```

Expected output:

```
sub-manifest keys: ['image_0000', 'image_0001', 'image_0002', 'image_0003', 'image_0004', 'image_0005', 'image_0006', 'image_0007']
first global_id:   LAILA:ENTRY:GLOBAL_ID:3b9c...
alpha has it?      False
```

`await sub.async_realized` is the fetch: it remembers every entry in the sub-manifest through the default pool, i.e. the front of the proxy chain, and returns a dict `{key: Entry}` mirroring the blueprint.

## The transform: bytes to tensor

Decode PNG or JPEG bytes with Pillow, move channels first, and scale to `[0, 1]`:

```python
def bytes_to_tensor(image_bytes: bytes) -> torch.Tensor:
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    array = np.array(image, dtype=np.uint8)                # H x W x C (writable copy)
    return torch.from_numpy(array).permute(2, 0, 1).float() / 255.0
```

## The data loader

`LailaDataLoader` is a plain Python iterator over a `Manifest`. The important pieces:

- **`_prepare_batch`** is the per-batch pipeline: slice a sub-manifest, `await sub.async_realized` to pull every entry through the proxy chain, then `transform` each realized entry in key order and stack.
- **`_schedule_next`** hands `_prepare_batch` to LAILA's taskforce with `laila.command.submit`, which returns a future immediately. That future is the "pre-ask".
- **`__iter__`** schedules the first `lookahead` batches; **`__next__`** waits on the oldest future, immediately schedules one more to keep the window full, forgets the finished batch, and returns the tensor.

```python
class LailaDataLoader:
    """Iterate a Manifest as stacked torch tensors, prefetching ``lookahead``
    batches through ``alpha << hdd << r2`` one sub-manifest at a time."""

    def __init__(
        self,
        manifest,
        batch_size,
        *,
        lookahead=4,
        transform=bytes_to_tensor,
        forget_from_hdd=False,
        hdd_pool="hdd",
    ):
        self.manifest = manifest
        self.keys = list(manifest.keys())
        self.batch_size = batch_size
        self.lookahead = lookahead
        self.transform = transform
        self.forget_from_hdd = forget_from_hdd
        self.hdd_pool = hdd_pool

        self._pending = collections.deque()   # (sub_manifest, future), oldest first
        self._next_to_schedule = 0
        self.last_wait_s = 0.0                # time spent blocked in the last __next__

    def __len__(self):
        return math.ceil(len(self.keys) / self.batch_size)

    def _batch_manifest(self, idx):
        batch_keys = self.keys[idx * self.batch_size : (idx + 1) * self.batch_size]
        return self.manifest.sub_manifest(batch_keys)

    # -- async pipeline, runs on LAILA's taskforce -------------------------
    async def _prepare_batch(self, sub):
        realized = await sub.async_realized   # {key: Entry}, via alpha << hdd << r2
        tensors = [self.transform(realized[k].data) for k in sub.keys()]
        return torch.stack(tensors)

    # -- scheduling ---------------------------------------------------------
    def _schedule_next(self):
        if self._next_to_schedule >= len(self):
            return
        sub = self._batch_manifest(self._next_to_schedule)
        future = laila.command.submit([functools.partial(self._prepare_batch, sub)])
        self._pending.append((sub, future))
        self._next_to_schedule += 1

    def _forget(self, sub):
        gids = list(sub)                                           # leaf global_ids of the batch
        futures = [laila.forget(gids)]                             # alpha pool: always
        if self.forget_from_hdd:
            futures.append(laila.forget(gids, pool=self.hdd_pool))
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
        sub, future = self._pending.popleft()

        start = time.perf_counter()
        future.wait()                         # returns at once if the lookahead kept up
        self.last_wait_s = time.perf_counter() - start
        batch = future.data                   # the stacked tensor returned by _prepare_batch
        future.release()

        self._schedule_next()                 # keep `lookahead` batches in flight
        self._forget(sub)                     # batch is done: free memory (and optionally disk)
        return batch
```

Nothing in `__next__` talks to R2 directly. The only place the network is touched is `await sub.async_realized` inside `_prepare_batch`, and that runs on the taskforce up to four batches before the loop asks for it. Because `async_realized` is awaited from a taskforce coroutine, it reads the pool directly with no per-entry futures: one sub-manifest, one concurrent read of its entries.

## Epoch 1: cold start, R2 → HDD → memory

```python
loader = LailaDataLoader(manifest, batch_size=8, lookahead=4)

for step, batch in enumerate(loader):
    loss = batch.mean()                       # stand-in for a model step
    print(
        f"step {step}: batch {tuple(batch.shape)} {batch.dtype} "
        f"waited {loader.last_wait_s * 1000:6.1f} ms  loss={loss:.3f}"
    )

first = manifest["image_0000"]
print(f"\nalpha has image_0000? {laila.alpha_pool.exists(first)}")
print(f"hdd   has image_0000? {hdd.exists(first)}")
```

Expected output (timings depend on your connection):

```
step 0: batch (8, 3, 32, 32) torch.float32 waited  353.8 ms  loss=0.500
step 1: batch (8, 3, 32, 32) torch.float32 waited    0.2 ms  loss=0.498
step 2: batch (8, 3, 32, 32) torch.float32 waited    0.2 ms  loss=0.500
step 3: batch (8, 3, 32, 32) torch.float32 waited    1.5 ms  loss=0.497
step 4: batch (8, 3, 32, 32) torch.float32 waited  333.0 ms  loss=0.499
step 5: batch (8, 3, 32, 32) torch.float32 waited    0.2 ms  loss=0.498
step 6: batch (8, 3, 32, 32) torch.float32 waited    0.2 ms  loss=0.496
step 7: batch (8, 3, 32, 32) torch.float32 waited    0.2 ms  loss=0.499

alpha has image_0000? False
hdd   has image_0000? True
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
step 0: waited   50.1 ms
step 1: waited    0.2 ms
step 2: waited   27.0 ms
step 3: waited    0.2 ms
...
```

The remaining waits are HDF5 reads plus PNG/JPEG decoding, tens of milliseconds rather than a round-trip to Cloudflare.

## Optional: free disk after each batch

If the dataset is larger than your disk, set `forget_from_hdd=True`. Every batch is then forgotten from **both** the alpha pool and the HDD cache once the model has seen it, so at most `lookahead + 1` batches are ever on disk:

```python
lean_loader = LailaDataLoader(manifest, batch_size=8, lookahead=4, forget_from_hdd=True)

for step, batch in enumerate(lean_loader):
    pass

print(f"alpha has image_0000? {laila.alpha_pool.exists(first)}")
print(f"hdd   has image_0000? {hdd.exists(first)}")
print(f"r2    has image_0000? {r2.exists(first)}")
```

Expected output:

```
alpha has image_0000? False
hdd   has image_0000? False
r2    has image_0000? True
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
2. **`manifest.sub_manifest(batch_keys)`** sliced the dataset manifest into one small manifest per batch. That is a blueprint operation only; no I/O happens until the sub-manifest is realized.
3. **`laila.command.submit`** put `_prepare_batch` on a LAILA taskforce and returned a future immediately. The loader keeps four of these futures queued, so the network work for batch *i+4* starts as soon as batch *i* is handed to the training loop.
4. Inside the taskforce, **`await sub.async_realized`** remembered every entry of the sub-manifest through the chain, caching the blobs in `hdd` and alpha; **`bytes_to_tensor`** decoded each realized entry's PNG/JPEG bytes into a `torch.Tensor`.
5. **`__next__`** only ever waited on an already-running future, then topped the window back up to four.
6. **`laila.forget(list(sub))`** removed the consumed batch from the alpha pool; with `forget_from_hdd=True` it also removed it from the HDF5 cache. R2 was never modified until the explicit clean-up.

## Summary

- Prefetching is a queue of futures: schedule `lookahead` batches up front, then schedule one more every time one is consumed.
- A batch is a `sub_manifest`; `await sub.async_realized` fetches it through the proxy chain in one concurrent read, and `list(sub)` gives the gids to forget afterwards.
- `laila.remember` through a proxy chain is the whole caching strategy; the loader has no R2-specific code.
- Always `forget` from the alpha pool after a batch; `forget` from the HDD tier only when disk is scarce, since keeping it makes the next epoch free of network I/O.
- `forget` is pool-local, so cache eviction never touches the origin bucket.

Back to [Example 1 — Dataset Creation](01_dataset_creation.md) or the [Tutorials](../tutorials/06_pool_proxies.md) for more on proxy chains.
