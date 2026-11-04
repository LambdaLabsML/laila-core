# Example 2: Data Loader — Prefetching Through memory << hdd << cloudflare

Stream the `my_dataset` datapoints from [Example 1](01_dataset_creation.md) into a training loop as batches of torch tensors and captions without ever waiting on the network, using the add-on loader:

```python
from laila.addons.torch import Dataset, DataLoader, default_tiers, transforms
```

`default_tiers` wires a three-tier read-through cache, `laila.alpha_pool << hdd << r2`. `DataLoader` is a `torch.utils.data.DataLoader` that keeps a **lookahead of 4 batches** in flight: while the model consumes batch *i*, batches *i+1 … i+4* are already being pulled from R2 into the local HDF5 cache and decoded into tensors on a background taskforce. By the time `next()` reaches them, they are ready.

Under the hood a batch is a **sub-manifest**: the loader slices the dataset manifest down to the keys of one batch and realizes it through the proxy chain (memory ← hdd ← cloudflare). Each realized modality is passed through its transform (PNG bytes → `torch.Tensor`; captions stay strings) and collated. After a batch has been through the model, its entries are forgotten from the in-memory alpha pool and, optionally, from the HDD cache too.

## Prerequisites

```bash
pip install "laila-core[cloudflare,hdf5,torch]" pillow
```

Run [Example 1](01_dataset_creation.md) first so the `my_dataset` manifest exists in your R2 bucket, and reuse the same `secrets.toml`.

## Setup

```python
import time

import laila
from laila.data import CloudflarePool
from laila.addons.torch import Dataset, DataLoader, default_tiers, transforms

laila.read_args("./secrets.toml")
# secrets.toml example:
#
#   R2_ACCOUNT_ID = "0123456789abcdef0123456789abcdef"
#   R2_ACCESS_KEY_ID = "..."
#   R2_SECRET_ACCESS_KEY = "..."
#   R2_BUCKET = "my-r2-bucket"
```

## Create the R2 pool and wire the chain

Three tiers: the **alpha pool** (in-memory, created by LAILA at startup), an **HDF5 pool** standing in for the local HDD, and the **R2 pool** holding the dataset. `default_tiers` registers the pools and chains them; with `hdd=None` it creates the `HDF5Pool(nickname="hdd")` itself.

```python
r2 = CloudflarePool(
    account_id=laila.args.R2_ACCOUNT_ID,
    access_key_id=laila.args.R2_ACCESS_KEY_ID,
    secret_access_key=laila.args.R2_SECRET_ACCESS_KEY,
    bucket_name=laila.args.R2_BUCKET,
    nickname="r2",
)

hdd, r2 = default_tiers(r2, cloud_nickname="r2")   # laila.alpha_pool << hdd << r2

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

## The dataset

`Dataset` is a map-style `torch.utils.data.Dataset` over the manifest. Hand it the nickname shorthand (`"MANIFEST:my_dataset"` expands to the full `LAILA:MANIFEST:<uuid5>` id) and the pool to remember the manifest from; the datapoints (`image_0000` … `image_0063`) become positions, and each inner key (`image`, `caption`) becomes a field. `transforms` maps a modality to the function applied to its payload: `transforms.image_bytes_to_tensor` decodes PNG bytes with Pillow, moves channels first and scales to `[0, 1]`. The caption has no transform and stays a string.

```python
dataset = Dataset(
    "MANIFEST:my_dataset",
    pool="r2",
    transforms={"image": transforms.image_bytes_to_tensor},
)

print(f"{len(dataset)} datapoints, modalities {dataset.modalities}")
print(f"image_0000 -> {dataset.manifest['image_0000']}")
```

Expected output:

```
64 datapoints, modalities ['image', 'caption']
image_0000 -> {'image': 'LAILA:ENTRY:3b9c...', 'caption': 'LAILA:ENTRY:a41e...'}
```

Indexing the dataset realizes one datapoint through the chain and returns the transformed dict. It is the slow path (one round-trip per item) but it makes the dataset a legitimate `torch.utils.data.Dataset`, so stock samplers and even a stock `DataLoader(num_workers=0)` work on it:

```python
item = dataset[0]
first = dataset.manifest["image_0000"]["image"]

print(f"image:   {tuple(item['image'].shape)} {item['image'].dtype}")
print(f"caption: {item['caption']!r}")
print(f"alpha has it? {laila.alpha_pool.exists(first)}   hdd has it? {hdd.exists(first)}")
```

Expected output:

```
image:   (3, 32, 32) torch.float32
caption: 'random noise image 0, mostly green'
alpha has it? False   hdd has it? True
```

The read went R2 → hdd → alpha; the datapoint was then forgotten from alpha again (RAM is a cache tier here, so `evict="auto"` frees it), while the HDF5 tier keeps the blob.

## The data loader

`DataLoader` is a `torch.utils.data.DataLoader`. It keeps everything torch gives you for free (`batch_size`, `shuffle`, `drop_last`, `sampler` / `batch_sampler`, `collate_fn`, `generator`, `len(loader)`; with the same `generator` seed it yields the same batches in the same order as the stock loader) and replaces only the part that fetches data: instead of worker processes calling `dataset[i]` one item at a time, whole batches are pulled as sub-manifests through the proxy chain on LAILA's taskforce, `lookahead` batches ahead of the training loop. `num_workers` must stay `0`; `lookahead` is the knob.

Per batch, on the taskforce:

1. `manifest.sub_manifest(batch_keys)` slices the manifest (a blueprint operation, no I/O);
2. `await sub.async_realized` remembers every entry in the batch through `alpha << hdd << r2` in one concurrent read;
3. the transforms and `collate_fn` run on the taskforce's executor: `default_collate` stacks the image tensors and leaves the captions as a list of strings.

`__next__` only ever waits on an already-running future, then tops the window back up and forgets the consumed batch from the alpha pool.

```python
loader = DataLoader(dataset, batch_size=8, lookahead=4)
print(f"{len(loader)} batches per epoch")
```

Expected output:

```
8 batches per epoch
```

## Epoch 1: cold start, R2 → HDD → memory

The "model step" below is a `time.sleep(0.5)`. Any real forward/backward pass plays the same role: it is the time during which the next four batches are being fetched in the background.

```python
MODEL_STEP_S = 0.5                            # stand-in for forward + backward

for step, batch in enumerate(loader):
    images, captions = batch["image"], batch["caption"]
    loss = images.mean()
    time.sleep(MODEL_STEP_S)                  # the model is busy; the loader keeps fetching
    print(
        f"step {step}: images {tuple(images.shape)} {images.dtype}  "
        f"captions[0]={captions[0]!r:38}  "
        f"waited {loader.last_wait_s * 1000:6.1f} ms  loss={loss:.3f}"
    )

print(f"\nalpha has image_0000? {laila.alpha_pool.exists(first)}")
print(f"hdd   has image_0000? {hdd.exists(first)}")
```

Expected output (timings depend on your connection):

```
step 0: images (8, 3, 32, 32) torch.float32  captions[0]='random noise image 0, mostly green'   waited  323.9 ms  loss=0.500
step 1: images (8, 3, 32, 32) torch.float32  captions[0]='random noise image 8, mostly green'   waited    0.3 ms  loss=0.498
step 2: images (8, 3, 32, 32) torch.float32  captions[0]='random noise image 16, mostly blue'   waited    0.3 ms  loss=0.500
step 3: images (8, 3, 32, 32) torch.float32  captions[0]='random noise image 24, mostly red'    waited    0.3 ms  loss=0.498
step 4: images (8, 3, 32, 32) torch.float32  captions[0]='random noise image 32, mostly green'  waited    0.4 ms  loss=0.499
step 5: images (8, 3, 32, 32) torch.float32  captions[0]='random noise image 40, mostly red'    waited    0.3 ms  loss=0.498
step 6: images (8, 3, 32, 32) torch.float32  captions[0]='random noise image 48, mostly blue'   waited    0.3 ms  loss=0.496
step 7: images (8, 3, 32, 32) torch.float32  captions[0]='random noise image 56, mostly blue'   waited    0.3 ms  loss=0.499

alpha has image_0000? False
hdd   has image_0000? True
```

Step 0 pays for the first R2 round-trip: nothing has been requested before the loop starts. From then on the window slides: the instant batch *i* is handed out, batch *i+4* is submitted, and it has four model steps (2 s here) to arrive before the loop needs it. One R2 round-trip is far shorter than that, so every `next()` after the first finds its batch already decoded and waits ~0 ms. Drop the sleep and you will see a wait every fourth step instead, because the loop then drains the whole window faster than one round-trip. Increase `lookahead` if your steps are shorter than your network latency.

After the epoch the alpha pool is empty (each batch was forgotten from memory once consumed) while the HDF5 cache still holds every entry.

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

## Shuffling, like any DataLoader

Every torch argument keeps its meaning. A shuffled, seeded epoch with `drop_last`:

```python
import torch

shuffled = DataLoader(
    dataset, batch_size=12, shuffle=True, drop_last=True,
    generator=torch.Generator().manual_seed(0), lookahead=4,
)
print(f"{len(shuffled)} batches of 12 (drop_last)")
for batch in shuffled:
    print([c.split(",")[0].rsplit(" ", 1)[-1] for c in batch["caption"]])   # the image numbers
```

Expected output:

```
5 batches of 12 (drop_last)
['53', '52', '29', '32', '11', '1', '21', '49', '45', '35', '16', '19']
['22', '54', '20', '59', '42', '34', '30', '48', '39', '55', '13', '56']
['0', '27', '15', '63', '2', '36', '33', '17', '51', '9', '23', '26']
['3', '62', '50', '57', '31', '40', '28', '12', '47', '41', '60', '14']
['4', '25', '24', '44', '7', '37', '10', '18', '46', '6', '43', '8']
```

That is exactly the order a stock `torch.utils.data.DataLoader` with the same seed would produce.

## Optional: free disk after each batch

If the dataset is larger than your disk, set `forget_from_hdd=True`. Every batch is then forgotten from **both** the alpha pool and the HDD cache once the model has seen it, so at most `lookahead + 1` batches are ever on disk:

```python
lean_loader = DataLoader(dataset, batch_size=8, lookahead=4, forget_from_hdd=True)

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
manifest = dataset.manifest

with laila.guarantee:
    laila.forget(manifest, pool="r2")   # every referenced image + caption + the manifest itself

hdd.empty()
laila.alpha_pool.proxy_to = None

print(f"manifest still in R2? {r2.exists(manifest.global_id)}")
```

Expected output:

```
manifest still in R2? False
```

## What just happened

1. **`default_tiers(r2)`** turned the default pool into the front of a read-through cache: memory ← disk ← cloud (`laila.alpha_pool << hdd << r2`).
2. **`Dataset("MANIFEST:my_dataset", pool="r2", transforms=...)`** remembered the manifest and exposed its `datapoint -> {modality: global_id}` blueprint as a map-style torch dataset, with one transform per modality.
3. **`DataLoader(dataset, batch_size=8, lookahead=4)`** kept torch's batching machinery (`batch_sampler`, `collate_fn`, `shuffle`, `drop_last`, `generator`) and swapped only the fetch path: each index batch became a **`sub_manifest`**, realized on a LAILA taskforce with **`await sub.async_realized`** through the chain, decoded by the transforms and collated into `{"image": Tensor, "caption": [str]}`.
4. The loader keeps four of these futures queued, so the network work for batch *i+4* starts as soon as batch *i* is handed to the training loop; `__next__` only ever waited on an already-running future.
5. Consumed batches were **forgotten from the alpha pool** (RAM is a cache tier here); with `forget_from_hdd=True` also from the HDF5 cache. R2 was never modified until the explicit clean-up.

## Summary

- `laila.addons.torch.DataLoader` is a real `torch.utils.data.DataLoader`; it drops into any training loop that expects one, and `lookahead` replaces `num_workers`.
- `Dataset` wraps a manifest whose top-level keys are datapoints and whose inner keys are modalities; `transforms={modality: fn}` decodes each modality at load time, so the stored data can stay as cheap as the encoded file.
- `default_tiers` is the whole caching strategy; the loader has no R2-specific code.
- Consumed batches are forgotten from the alpha pool automatically; forget from the HDD tier only when disk is scarce, since keeping it makes the next epoch free of network I/O.
- `forget` is pool-local, so cache eviction never touches the origin bucket.

The hand-rolled version of this loader (sub-manifests, `laila.command.submit`, a deque of futures) is what `laila.addons.torch` packages; read `addons/torch/dataloader.py` to see the mechanism. Back to [Example 1 — Dataset Creation](01_dataset_creation.md) or the [Tutorials](../../tutorials/06_pool_proxies.md) for more on proxy chains. The [Torch Wrappers](../torch_wrappers/03_checkpoint_module_optimizer.md) examples take the same add-on to the model side: checkpointing `Module` / `Optimizer` and resuming a run from one nickname.
