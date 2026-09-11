# Example 2: Data Loader — Prefetching Through memory << hdd << cloudflare

Stream the `my_dataset` images from [Example 1](01_dataset_creation.md) into a training loop as torch tensors without ever waiting on the network. The loader is a `torch.utils.data.DataLoader` subclass that sits on top of a three-tier proxy chain

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
import time

import numpy as np
import torch
from PIL import Image

import laila
from laila.data import CloudflarePool, HDF5Pool

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

`remember` the manifest from R2 by its nickname shorthand (`"MANIFEST:my_dataset"` expands to the full `LAILA:MANIFEST:GLOBAL_ID:<uuid5>` id); the result is the `Manifest` itself. Its top-level keys (`image_0000` … `image_0063`) are the dataset index:

```python
ref = laila.remember("MANIFEST:my_dataset", dst_pool="r2", persist=False)
manifest = ref.wait()   # remember returns the Manifest itself
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

Decode PNG bytes with Pillow, move channels first, and scale to `[0, 1]`:

```python
def bytes_to_tensor(image_bytes: bytes) -> torch.Tensor:
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    array = np.array(image, dtype=np.uint8)                # H x W x C (writable copy)
    return torch.from_numpy(array).permute(2, 0, 1).float() / 255.0
```

## The data loader

`LailaDataLoader` is a `torch.utils.data.DataLoader`. It keeps everything torch gives you for free (`batch_size`, `shuffle`, `drop_last`, `sampler` / `batch_sampler`, `collate_fn`, `len(loader)`) and replaces only the part that fetches data: instead of worker processes calling `dataset[i]` one item at a time, whole batches are pulled as sub-manifests through the proxy chain on LAILA's taskforce, `lookahead` batches ahead of the training loop.

Two small classes:

- **`ManifestDataset`** is a map-style `Dataset` over the manifest's top-level keys, so torch's samplers can address items by position. The loader never calls `__getitem__` itself; it exists so the dataset is a legitimate `Dataset` and so `shuffle=True` etc. work.
- **`LailaDataLoader`** subclasses `DataLoader`, forces `num_workers=0` (prefetching is LAILA's job, not multiprocessing's), and overrides `__iter__` to return a `_LailaLoaderIter`.

Inside the iterator:

- **`_prepare_batch`** is the per-batch pipeline: `await sub.async_realized` pulls every entry of the sub-manifest through the proxy chain, then each realized entry is `transform`ed in key order and handed to the loader's `collate_fn` (the torch default stacks tensors).
- **`_schedule_next`** takes the next index batch from torch's `batch_sampler`, slices the matching sub-manifest, and hands `_prepare_batch` to LAILA's taskforce with `laila.command.submit`, which returns a future immediately. That future is the "pre-ask".
- **`__init__`** schedules the first `lookahead` batches; **`__next__`** waits on the oldest future, immediately schedules one more to keep the window full, forgets the finished batch, and returns the tensor.

```python
class ManifestDataset(torch.utils.data.Dataset):
    """Map-style view of a Manifest: position -> top-level key -> global_id."""

    def __init__(self, manifest):
        self.manifest = manifest
        self.keys = list(manifest.keys())

    def __len__(self):
        return len(self.keys)

    def __getitem__(self, idx):
        return self.manifest[self.keys[idx]]          # the global_id; batches are fetched by the loader

    def sub_manifest(self, indices):
        return self.manifest.sub_manifest([self.keys[i] for i in indices])


class _LailaLoaderIter:
    """One epoch: keeps ``lookahead`` sub-manifests in flight on LAILA's taskforce."""

    def __init__(self, loader):
        self.loader = loader
        self._batches = iter(loader.batch_sampler)    # index batches: shuffle / drop_last already applied
        self._pending = collections.deque()           # (sub_manifest, future), oldest first
        for _ in range(loader.lookahead):
            self._schedule_next()

    def __iter__(self):
        return self

    # -- async pipeline, runs on LAILA's taskforce -------------------------
    async def _prepare_batch(self, sub):
        realized = await sub.async_realized           # {key: Entry}, via alpha << hdd << r2
        tensors = [self.loader.transform(realized[k].data) for k in sub.keys()]
        return self.loader.collate_fn(tensors)        # default_collate -> stacked tensor

    # -- scheduling ---------------------------------------------------------
    def _schedule_next(self):
        indices = next(self._batches, None)
        if indices is None:
            return
        sub = self.loader.dataset.sub_manifest(indices)
        future = laila.command.submit([functools.partial(self._prepare_batch, sub)])
        self._pending.append((sub, future))

    def _forget(self, sub):
        gids = list(sub)                                           # leaf global_ids of the batch
        futures = [laila.forget(gids)]                             # alpha pool: always
        if self.loader.forget_from_hdd:
            futures.append(laila.forget(gids, pool=self.loader.hdd_pool))
        for f in futures:
            f.wait()
            f.release()

    def __next__(self):
        if not self._pending:
            raise StopIteration
        sub, future = self._pending.popleft()

        start = time.perf_counter()
        future.wait()                         # returns at once if the lookahead kept up
        self.loader.last_wait_s = time.perf_counter() - start
        batch = future.data                   # the collated tensor returned by _prepare_batch
        future.release()

        self._schedule_next()                 # keep `lookahead` batches in flight
        self._forget(sub)                     # batch is done: free memory (and optionally disk)
        return batch


class LailaDataLoader(torch.utils.data.DataLoader):
    """torch DataLoader whose batches are sub-manifests prefetched ``lookahead``
    steps ahead through ``alpha << hdd << r2``."""

    def __init__(
        self,
        manifest,
        batch_size=1,
        *,
        lookahead=4,
        transform=bytes_to_tensor,
        forget_from_hdd=False,
        hdd_pool="hdd",
        **dataloader_kwargs,                  # shuffle, drop_last, sampler, collate_fn, generator, ...
    ):
        if dataloader_kwargs.get("num_workers", 0):
            raise ValueError("LailaDataLoader prefetches on LAILA's taskforce; use lookahead, not num_workers")
        dataset = manifest if isinstance(manifest, ManifestDataset) else ManifestDataset(manifest)
        super().__init__(dataset, batch_size=batch_size, num_workers=0, **dataloader_kwargs)

        self.lookahead = lookahead
        self.transform = transform
        self.forget_from_hdd = forget_from_hdd
        self.hdd_pool = hdd_pool
        self.last_wait_s = 0.0                # time spent blocked in the last __next__

    def __iter__(self):
        return _LailaLoaderIter(self)
```

Nothing in `__next__` talks to R2 directly. The only place the network is touched is `await sub.async_realized` inside `_prepare_batch`, and that runs on the taskforce up to four batches before the loop asks for it. Because `async_realized` is awaited from a taskforce coroutine, it reads the pool directly with no per-entry futures: one sub-manifest, one concurrent read of its entries.

Because the batches come from torch's own `batch_sampler`, `LailaDataLoader(manifest, batch_size=8, shuffle=True, drop_last=True)` works exactly as it would on a stock `DataLoader`; only the fetch path is different.

## Epoch 1: cold start, R2 → HDD → memory

The "model step" below is a `time.sleep(0.5)`. Any real forward/backward pass plays the same role: it is the time during which the next four batches are being fetched in the background.

```python
MODEL_STEP_S = 0.5                            # stand-in for forward + backward

loader = LailaDataLoader(manifest, batch_size=8, lookahead=4)

for step, batch in enumerate(loader):
    loss = batch.mean()
    time.sleep(MODEL_STEP_S)                  # the model is busy; the loader keeps fetching
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
step 0: batch (8, 3, 32, 32) torch.float32 waited  323.9 ms  loss=0.500
step 1: batch (8, 3, 32, 32) torch.float32 waited    0.3 ms  loss=0.498
step 2: batch (8, 3, 32, 32) torch.float32 waited    0.3 ms  loss=0.500
step 3: batch (8, 3, 32, 32) torch.float32 waited    0.3 ms  loss=0.498
step 4: batch (8, 3, 32, 32) torch.float32 waited    0.4 ms  loss=0.499
step 5: batch (8, 3, 32, 32) torch.float32 waited    0.3 ms  loss=0.498
step 6: batch (8, 3, 32, 32) torch.float32 waited    0.3 ms  loss=0.496
step 7: batch (8, 3, 32, 32) torch.float32 waited    0.3 ms  loss=0.499

alpha has image_0000? False
hdd   has image_0000? True
```

Step 0 pays for the first R2 round-trip: nothing has been requested before the loop starts. From then on the window slides: the instant batch *i* is handed out, batch *i+4* is submitted, and it has four model steps (2 s here) to arrive before the loop needs it. One R2 round-trip is far shorter than that, so every `next()` after the first finds its tensor already decoded and waits ~0 ms. Drop the sleep and you will see a wait every fourth step instead, because the loop then drains the whole window faster than one round-trip. Increase `lookahead` if your steps are shorter than your network latency.

After the epoch the alpha pool is empty (each batch was forgotten from memory once consumed) while the HDF5 cache still holds every image.

## Epoch 2: disk hits

Run the same loader again. The chain now stops at `hdd`; R2 is never contacted:

```python
for step, batch in enumerate(loader):
    time.sleep(MODEL_STEP_S)
    print(f"step {step}: waited {loader.last_wait_s * 1000:6.1f} ms")
```

Expected output:

```
step 0: waited   65.2 ms
step 1: waited    0.3 ms
step 2: waited    0.3 ms
step 3: waited    0.3 ms
...
```

Step 0 is now an HDF5 read plus PNG decoding, tens of milliseconds rather than a round-trip to Cloudflare, and every later step is hidden behind the model exactly as before.

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
    laila.forget(manifest, pool="r2")   # every referenced image + the manifest itself

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
2. **`LailaDataLoader(torch.utils.data.DataLoader)`** kept torch's batching machinery (`batch_sampler`, `collate_fn`, `shuffle`, `drop_last`) and swapped only the fetch path; **`manifest.sub_manifest(batch_keys)`** sliced the dataset manifest into one small manifest per index batch. That is a blueprint operation only; no I/O happens until the sub-manifest is realized.
3. **`laila.command.submit`** put `_prepare_batch` on a LAILA taskforce and returned a future immediately. The loader keeps four of these futures queued, so the network work for batch *i+4* starts as soon as batch *i* is handed to the training loop.
4. Inside the taskforce, **`await sub.async_realized`** remembered every entry of the sub-manifest through the chain, caching the blobs in `hdd` and alpha; **`bytes_to_tensor`** decoded each realized entry's PNG bytes into a `torch.Tensor`.
5. **`__next__`** only ever waited on an already-running future, then topped the window back up to four.
6. **`laila.forget(list(sub))`** removed the consumed batch from the alpha pool; with `forget_from_hdd=True` it also removed it from the HDF5 cache. R2 was never modified until the explicit clean-up.

## Summary

- The loader is a real `torch.utils.data.DataLoader`; only `__iter__` is overridden, so it drops into any training loop that expects one.
- Prefetching is a queue of futures: schedule `lookahead` batches up front, then schedule one more every time one is consumed.
- A batch is a `sub_manifest`; `await sub.async_realized` fetches it through the proxy chain in one concurrent read, and `list(sub)` gives the gids to forget afterwards.
- `laila.remember` through a proxy chain is the whole caching strategy; the loader has no R2-specific code.
- Always `forget` from the alpha pool after a batch; `forget` from the HDD tier only when disk is scarce, since keeping it makes the next epoch free of network I/O.
- `forget` is pool-local, so cache eviction never touches the origin bucket.

Back to [Example 1 — Dataset Creation](01_dataset_creation.md) or the [Tutorials](../tutorials/06_pool_proxies.md) for more on proxy chains.
