# Example 1: Dataset Creation — Random Images to Cloudflare R2

Build an image dataset from scratch and push it to Cloudflare R2. Every image is encoded as PNG or JPEG bytes and stored as one LAILA entry, so what lives in the bucket is exactly the file you would get from `PIL.Image.save`. A single **Manifest** named `my_dataset` records the `global_id` of every image, which is all a consumer needs to find the dataset later.

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

## Generate random images

Draw `uint8` noise, turn it into a `PIL.Image`, and encode it into an in-memory buffer. Even-numbered images are saved as PNG, odd-numbered ones as JPEG, so the dataset mixes both formats. The bytes in the buffer are what gets stored; the entry's `data` is the file contents, nothing more.

```python
N_IMAGES = 64
HEIGHT = WIDTH = 32

rng = np.random.default_rng(seed=0)
entries = []

for i in range(N_IMAGES):
    pixels = rng.integers(0, 256, size=(HEIGHT, WIDTH, 3), dtype=np.uint8)
    image = Image.fromarray(pixels, mode="RGB")

    buffer = io.BytesIO()
    if i % 2 == 0:
        image.save(buffer, format="PNG")
    else:
        image.save(buffer, format="JPEG", quality=90)

    entries.append(laila.constant(data=buffer.getvalue()))

print(f"Created {len(entries)} image entries")
print(f"  image 0 (PNG):  {len(entries[0].data):,} bytes, starts with {entries[0].data[:4]!r}")
print(f"  image 1 (JPEG): {len(entries[1].data):,} bytes, starts with {entries[1].data[:4]!r}")
```

Expected output:

```
Created 64 image entries
  image 0 (PNG):  3,209 bytes, starts with b'\x89PNG'
  image 1 (JPEG): 2,301 bytes, starts with b'\xff\xd8\xff\xe0'
```

The byte counts vary with the random seed, but the magic numbers show that each entry holds a real PNG or JPEG file.

## Build the manifest

A `Manifest` wraps a nested dict of entries and extracts a **blueprint**: the same structure with `global_id` strings in place of the entries. Give it the nickname `my_dataset` so anyone can rebuild its identity later without knowing the UUID.

```python
manifest = Manifest(data={"images": entries}, nickname="my_dataset")

print(f"Manifest global_id: {manifest.global_id}")
print(f"Images in manifest:  {sum(1 for _ in manifest)}")
print(f"First global_id:     {manifest.blueprint['images'][0]}")
```

Expected output:

```
Manifest global_id: LAILA:MANIFEST:GLOBAL_ID:6f1d...
Images in manifest:  64
First global_id:     LAILA:ENTRY:GLOBAL_ID:3b9c...
```

## Push everything to R2

`manifest.memorize()` uploads all 64 image entries **and** the manifest blueprint itself in one call. Wrap it in `laila.guarantee` to block until every write has finished:

```python
with laila.guarantee:
    manifest.memorize(pool_nickname="r2")

print(f"Manifest stored in R2? {r2.exists(manifest.global_id)}")
print(f"First image in R2?     {r2.exists(manifest.blueprint['images'][0])}")
```

Expected output:

```
Manifest stored in R2? True
First image in R2?     True
```

## Verify from a cold start

Pretend this is a fresh process that knows nothing but the name `my_dataset`. Rebuild the manifest identity from the nickname, remember the blueprint from R2, then pull one image and decode it:

```python
del entries, manifest

cold = Manifest(nickname="my_dataset")
ref = laila.remember(cold.global_id, dst_pool="r2")
manifest = Manifest(data=ref.wait().data, nickname="my_dataset")
ref.release()

first_gid = manifest.blueprint["images"][0]
image_ref = laila.remember(first_gid, dst_pool="r2")
image_bytes = image_ref.wait().data
image_ref.release()

image = Image.open(io.BytesIO(image_bytes))
print(f"Recovered {sum(1 for _ in manifest)} global_ids from the manifest")
print(f"First image: {image.format} {image.size} {image.mode}")
```

Expected output:

```
Recovered 64 global_ids from the manifest
First image: PNG (32, 32) RGB
```

Leave the dataset in the bucket. Example 2 consumes it and takes care of cleaning up.

## What just happened

1. **`CloudflarePool`** wrapped an R2 bucket behind the same `memorize` / `remember` / `forget` API as every other LAILA pool.
2. Each random image was encoded with Pillow and wrapped as a `laila.constant` whose payload is the raw PNG or JPEG **bytes**.
3. **`Manifest(data={"images": entries}, nickname="my_dataset")`** extracted the blueprint of `global_id` strings and stashed the entries for upload.
4. **`manifest.memorize(pool_nickname="r2")`** pushed all images plus the blueprint to R2 in a single call.
5. The dataset was recovered from nothing but the nickname: `Manifest(nickname="my_dataset")` gives the manifest's `global_id`, `remember` fetches the blueprint, and each leaf `global_id` fetches an image.

## Summary

- Entries hold **bytes**; encode images with Pillow and store `buffer.getvalue()`.
- A `Manifest` is the dataset index: a list of `global_id` strings under a stable nickname.
- `manifest.memorize()` uploads entries and blueprint together; `laila.guarantee` waits for all of them.
- Consumers only need the nickname `my_dataset` and access to the same bucket.

Next: [Example 2 — Data Loader](02_data_loader.md), where a prefetching loader streams this dataset through `memory << hdd << cloudflare` and yields torch tensors.
