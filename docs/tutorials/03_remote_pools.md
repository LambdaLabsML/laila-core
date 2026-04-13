# Tutorial 3: S3 with NumPy and PyTorch Tensors

This tutorial shows how to store and recall numerical data — numpy arrays and PyTorch tensors — on AWS S3. The workflow is the same as [Tutorial 2](02_local_pools.md), but the pool talks to a remote bucket instead of local storage.

## Prerequisites

```bash
pip install "laila-core[s3,torch]"
```

You will need an AWS S3 bucket and credentials (access key ID, secret access key, region).

## Loading credentials

Store your credentials in a TOML file (e.g. `secrets.toml`):

```toml
AWS_BUCKET_NAME = "your-bucket"
AWS_ACCESS_KEY_ID = "AKIA..."
AWS_SECRET_ACCESS_KEY = "wJa..."
AWS_REGION = "us-east-1"
```

Then load them into LAILA's arg system:

```python
import numpy as np
import torch
import laila
from laila.pool import S3Pool

laila.read_args("./secrets.toml")
```

## Creating and registering the S3 pool

```python
s3_pool = S3Pool(
    bucket_name=laila.args.AWS_BUCKET_NAME,
    access_key_id=laila.args.AWS_ACCESS_KEY_ID,
    secret_access_key=laila.args.AWS_SECRET_ACCESS_KEY,
    region_name=laila.args.AWS_REGION,
    nickname="s3",
)

laila.memory.extend(s3_pool, pool_nickname="s3")
```

## Storing a numpy array

```python
matrix = np.random.randn(100, 100)
np_entry = laila.constant(data=matrix, nickname="np_matrix")

future = laila.memorize(np_entry, pool_nickname="s3")
laila.wait(future)

print(f"Status: {laila.status(future)}")
print(f"Stored global_id: {np_entry.global_id}")
```

## Storing a PyTorch tensor

```python
image_tensor = torch.randn(3, 224, 224)
torch_entry = laila.constant(data=image_tensor, nickname="torch_img")

future = laila.memorize(torch_entry, pool_nickname="s3")
laila.wait(future)

print(f"Status: {laila.status(future)}")
print(f"Stored global_id: {torch_entry.global_id}")
```

## Recalling from S3

Now clear the local references and recall purely from the stored `global_id`:

```python
np_gid = np_entry.global_id
torch_gid = torch_entry.global_id

del matrix, np_entry, image_tensor, torch_entry

# Recall numpy array
np_future = laila.remember(np_gid, pool_nickname="s3")
laila.wait(np_future)
recalled_np = np_future.data

print(f"NumPy shape: {recalled_np.shape}")   # (100, 100)
print(f"NumPy dtype: {recalled_np.dtype}")    # float64

# Recall torch tensor
torch_future = laila.remember(torch_gid, pool_nickname="s3")
laila.wait(torch_future)
recalled_torch = torch_future.data

print(f"Torch shape: {recalled_torch.shape}") # torch.Size([3, 224, 224])
print(f"Torch dtype: {recalled_torch.dtype}")  # torch.float32
```

## Inspecting futures

LAILA provides several ways to inspect what happened during an async operation:

```python
# Simple status check
print(laila.status(np_future))
# FutureStatus.FINISHED

# Detailed breakdown (useful for GroupFutures with multiple children)
print(np_future.what)
```

If something goes wrong, check the exception:

```python
future_obj = laila.active_policy.future_bank[np_future.global_id]
print(future_obj.exception)  # None if everything succeeded
```

## Clean up

```python
laila.forget(np_gid, pool_nickname="s3")
laila.forget(torch_gid, pool_nickname="s3")
```

## Summary

- S3Pool takes bucket name and AWS credentials. The API is identical to local pools.
- numpy arrays and PyTorch tensors are serialized and deserialized automatically — types and shapes are preserved.
- `laila.status(future)` gives you a quick status check; `.what` and `.exception` provide deeper introspection.
- Nicknames give entries stable, human-readable identities backed by deterministic UUIDs.

Next: [Tutorial 4 — Model Checkpoint and Reload](04_model_checkpoint.md), where you dump an entire model and optimizer to S3 and reload from a single manifest.
