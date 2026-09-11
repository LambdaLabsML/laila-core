# Example 1: Dataset Creation — Random Images to Cloudflare R2

Build an image dataset from scratch and push it to Cloudflare R2. Every image is encoded as PNG bytes and stored as one LAILA entry, so what lives in the bucket is exactly the file you would get from `PIL.Image.save`. A single **Manifest** named `my_dataset` records the `global_id` of every image under its own key (`image_0000`, `image_0001`, ...), which is all a consumer needs to find and slice the dataset later.

[Example 2](02_data_loader.md) reads this dataset back through a `memory << hdd << cloudflare` cache chain with a prefetching data loader.

## Prerequisites

```bash
pip install "laila-core[cloudflare,hdf5,torch]" pillow
```

`cloudflare` pulls in the S3-compatible client used by `CloudflarePool`. `hdf5` and `torch` are only needed for Example 2 but installing them now saves a second install.

## Setting up Cloudflare R2

R2 is Cloudflare's S3-compatible object store. You need three things: an account id, a bucket, and an API token with read/write access to that bucket.

1. Sign in to the [Cloudflare dashboard](https://dash.cloudflare.com/) (create an account if you do not have one) and open **R2 Object Storage** in the left sidebar. The first time you open it you will be asked to enable R2 and add a payment method; the free tier is large enough for this example.
2. Click **Create bucket**, choose a name (for example `laila-examples`), keep the default location, and create it. This is your `R2_BUCKET`.
3. Copy the **Account ID**. It is shown on the R2 overview page (and in the dashboard URL as `dash.cloudflare.com/<account-id>/r2`). This is your `R2_ACCOUNT_ID`.
4. Back on the R2 overview page, open **Manage R2 API Tokens** and click **Create API token**. Give it a name, choose the **Object Read & Write** permission, and under *Specify bucket(s)* select only the bucket you just created. Create the token.
5. Cloudflare shows the **Access Key ID** and **Secret Access Key** exactly once. Copy both into your `secrets.toml` now; these are `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`.

Write the four values to a `secrets.toml` next to the notebook (the same keys used in [Tutorial 19](../tutorials/19_object_stores_beyond_aws.md)):

```toml
R2_ACCOUNT_ID = "0123456789abcdef0123456789abcdef"
R2_ACCESS_KEY_ID = "..."
R2_SECRET_ACCESS_KEY = "..."
R2_BUCKET = "laila-examples"
```

Keep `secrets.toml` out of version control.

## Setup

```python
import io

import numpy as np
from PIL import Image

import laila
from laila.data import CloudflarePool
from laila.policy.central.memory.schema import Manifest

laila.read_args("./secrets.toml")
```

## Create and register the R2 pool

`CloudflarePool` fills in the R2 endpoint from your account id. Register it under the nickname `r2` so `memorize` / `remember` calls can route to it by name:

```python
r2 = CloudflarePool(
    account_id=laila.args.R2_ACCOUNT_ID,
    access_key_id=laila.args.R2_ACCESS_KEY_ID,
    secret_access_key=laila.args.R2_SECRET_ACCESS_KEY,
    bucket_name=laila.args.R2_BUCKET,
    nickname="r2",
)
laila.memory.extend(r2, pool_nickname="r2")
```

## Generate and upload images as they are created

Draw `uint8` noise and encode it as PNG **in memory**: `encode_png` writes into a `BytesIO` buffer, never to disk, and returns the file bytes. Each iteration wraps those bytes in a `laila.constant` and calls `laila.memorize(..., dst_pool="r2")` right away. `memorize` returns a future immediately, so the upload of image *i* is already in flight while image *i+1* is being generated. Nothing is kept locally except the entry's `global_id`, which goes into a plain dict, the **blueprint**, under the key `image_0000`, `image_0001`, ...

The loop sits inside `with laila.guarantee:` so the block only exits once every upload has finished.

```python
N_IMAGES = 64
HEIGHT = WIDTH = 32


def encode_png(pixels: np.ndarray) -> bytes:
    """Encode an H x W x 3 uint8 array as PNG bytes, entirely in memory."""
    buffer = io.BytesIO()
    Image.fromarray(pixels, mode="RGB").save(buffer, format="PNG")  # into the buffer, not a file
    return buffer.getvalue()


rng = np.random.default_rng(seed=0)
blueprint = {}
total_bytes = 0

with laila.guarantee:  # every future created inside is awaited when the block exits
    for i in range(N_IMAGES):
        pixels = rng.integers(0, 256, size=(HEIGHT, WIDTH, 3), dtype=np.uint8)
        png = encode_png(pixels)
        entry = laila.constant(data=png)

        laila.memorize(entry, dst_pool="r2")          # upload starts now, in the background
        blueprint[f"image_{i:04d}"] = entry.global_id  # keep only the id

        total_bytes += len(png)

print(f"Uploaded {len(blueprint)} PNG images ({total_bytes:,} bytes) to R2")
print(f"image_0000 -> {blueprint['image_0000']}")
print(f"image_0000 in R2? {r2.exists(blueprint['image_0000'])}")
print(f"first bytes: {png[:4]!r}")
```

Expected output:

```
Uploaded 64 PNG images (203,008 bytes) to R2
image_0000 -> LAILA:ENTRY:GLOBAL_ID:3b9c...
image_0000 in R2? True
first bytes: b'\x89PNG'
```

The byte count varies with the random seed, but the `\x89PNG` magic number shows that each entry holds a real PNG file.

## Build the manifest from the blueprint

A `Manifest` can be constructed directly from a blueprint: a dict whose leaves are `global_id` strings. Because the images are already in R2, there is nothing pending to upload; the manifest is only an index. Give it the nickname `my_dataset` so anyone can rebuild its identity later without knowing the UUID.

Each image has its **own top-level key** (`image_0000` ... `image_0063`) rather than all of them sitting in one list. Top-level keys are what `manifest.sub_manifest([...])` slices on, and that is how the data loader in Example 2 fetches one batch at a time.

```python
manifest = Manifest(data=blueprint, nickname="my_dataset")

print(f"Manifest global_id: {manifest.global_id}")
print(f"Images in manifest:  {len(manifest)}")
print(f"image_0000 ->        {manifest['image_0000']}")
```

Expected output:

```
Manifest global_id: LAILA:MANIFEST:GLOBAL_ID:6f1d...
Images in manifest:  64
image_0000 ->        LAILA:ENTRY:GLOBAL_ID:3b9c...
```

## Store the manifest in R2

A `Manifest` is itself an `Entry` whose payload is the blueprint, so `laila.memorize` stores it like any other entry. This is the only remaining upload; the images are already there:

```python
with laila.guarantee:
    laila.memorize(manifest, dst_pool="r2")

print(f"Manifest stored in R2? {r2.exists(manifest.global_id)}")
```

Expected output:

```
Manifest stored in R2? True
```

## Verify from a cold start

Pretend this is a fresh process that knows nothing but the name `my_dataset`. `laila.remember` accepts the shorthand `"MANIFEST:my_dataset"`: it expands to the full id `LAILA:MANIFEST:GLOBAL_ID:<uuid5(my_dataset)>` (the `LAILA` prefix and `GLOBAL_ID` postfix are the defaults of the `prefix_scopes` / `postfix_scopes` arguments), and the read path rebuilds a `Manifest` rather than a plain `Entry` because the stored id carries the `MANIFEST` scope. Then pull one image and decode it:

```python
del blueprint, manifest

ref = laila.remember("MANIFEST:my_dataset", dst_pool="r2")   # nickname shorthand -> full manifest id
manifest = ref.wait()   # a Manifest, rebuilt by its MANIFEST scope
ref.release()

first_gid = manifest["image_0000"]
image_ref = laila.remember(first_gid, dst_pool="r2")
image_bytes = image_ref.wait().data
image_ref.release()

image = Image.open(io.BytesIO(image_bytes))
print(f"Recovered {len(manifest)} images from the manifest")
print(f"First image: {image.format} {image.size} {image.mode}")
```

Expected output:

```
Recovered 64 images from the manifest
First image: PNG (32, 32) RGB
```

Leave the dataset in the bucket. Example 2 consumes it and takes care of cleaning up.

## What just happened

1. **`CloudflarePool`** wrapped an R2 bucket behind the same `memorize` / `remember` / `forget` API as every other LAILA pool.
2. Each random image was PNG-encoded in memory, wrapped as a `laila.constant` whose payload is the raw PNG **bytes**, and handed to **`laila.memorize(entry, dst_pool="r2")`** in the same loop iteration, so uploads overlapped with generation. Only the `global_id` was kept, in the `blueprint` dict.
3. **`Manifest(data=blueprint, nickname="my_dataset")`** built the index from `global_id` strings (one top-level key per image) with nothing left to upload.
4. **`laila.memorize(manifest, dst_pool="r2")`** stored the manifest itself; the images were already in the bucket.
5. The dataset was recovered from nothing but the nickname: `laila.remember("MANIFEST:my_dataset")` expands the shorthand to the manifest's id and returns the `Manifest` directly, and each leaf `global_id` fetches an image.

## Summary

- Entries hold **bytes**; encode images with Pillow into a `BytesIO` and store the buffer contents. Nothing touches the local disk.
- A `Manifest` is the dataset index: one `global_id` per top-level key under a stable nickname, sliceable with `sub_manifest`.
- Memorize each image the moment it is created; `laila.memorize` returns at once, and `with laila.guarantee:` around the loop waits for all uploads at the end.
- A manifest built from a blueprint is just an index; `laila.memorize(manifest, dst_pool=...)` stores it as one more entry.
- Consumers only need the nickname `my_dataset` and access to the same bucket.

Next: [Example 2 — Data Loader](02_data_loader.md), where a prefetching loader streams this dataset through `memory << hdd << cloudflare` and yields torch tensors.
