# Tutorial 4: Model Checkpoint and Reload from a Single Manifest

This is the most advanced tutorial. You will save a PyTorch model's weights **and** optimizer state to S3 as individual entries, create a single **manifest** entry that references all of them, then destroy every local reference and rebuild the entire training state from that one manifest ID.

## Prerequisites

```bash
pip install "laila-core[s3,torch]"
```

## Setup

```python
import torch
import torch.nn as nn
import laila
from laila.pool import S3Pool

laila.read_args("./secrets.toml")

s3_pool = S3Pool(
    bucket_name=laila.args.AWS_BUCKET_NAME,
    access_key_id=laila.args.AWS_ACCESS_KEY_ID,
    secret_access_key=laila.args.AWS_SECRET_ACCESS_KEY,
    region_name=laila.args.AWS_REGION,
    nickname="ckpt_pool",
)
laila.memory.add_pool(s3_pool, pool_nickname="ckpt")
```

## Step 1: Define a model and optimizer

```python
class SimpleCNN(nn.Module):
    def __init__(self):
        super().__init__()
        self.features = nn.Sequential(
            nn.Conv2d(3, 16, 3, padding=1),
            nn.ReLU(),
            nn.Conv2d(16, 32, 3, padding=1),
            nn.ReLU(),
            nn.AdaptiveAvgPool2d(1),
        )
        self.classifier = nn.Sequential(
            nn.Flatten(),
            nn.Linear(32, 10),
        )

    def forward(self, x):
        return self.classifier(self.features(x))


model = SimpleCNN()
optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
```

Run a few dummy training steps so the optimizer accumulates state:

```python
for _ in range(3):
    x = torch.randn(4, 3, 32, 32)
    loss = model(x).sum()
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()

model.eval()
```

## Step 2: Wrap every parameter as a LAILA entry

Each tensor in the model's `state_dict` and the optimizer's `state_dict` becomes its own constant entry with a descriptive nickname:

```python
weight_entries = {}
for name, tensor in model.state_dict().items():
    entry = laila.constant(data=tensor.detach().cpu(), nickname=f"model.{name}")
    weight_entries[name] = entry

optim_entry = laila.constant(
    data=optimizer.state_dict(),
    nickname="optimizer_state",
)
```

## Step 3: Build the manifest

The manifest is a single entry whose data is a dictionary mapping human-readable names to `global_id` strings. This is the only ID you need to reconstruct everything.

```python
manifest_data = {
    "model_class": "SimpleCNN",
    "model_params": {
        name: entry.global_id for name, entry in weight_entries.items()
    },
    "optimizer": optim_entry.global_id,
}

manifest = laila.constant(data=manifest_data, nickname="my_checkpoint")
print(f"Manifest global_id: {manifest.global_id}")
```

## Step 4: Memorize everything to S3

Use `laila.guarantee` to ensure all futures are waited on before exiting the block:

```python
all_entries = list(weight_entries.values()) + [optim_entry]

with laila.guarantee:
    laila.memorize(all_entries, pool_nickname="ckpt")

with laila.guarantee:
    laila.memorize(manifest, pool_nickname="ckpt")

print(f"Uploaded {len(all_entries)} parameter entries + 1 manifest")
```

## Step 5: Nuke all local state

Destroy every local reference. After this, the only way to get the model back is through LAILA.

```python
manifest_nickname = "my_checkpoint"

del model, optimizer
del weight_entries, optim_entry, manifest, manifest_data, all_entries
```

## Step 6: Reload from the manifest alone

Recall the manifest by its nickname, then use the IDs inside to fetch all parameters:

```python
# Recall the manifest
manifest_future = laila.remember(
    nickname=manifest_nickname,
    pool_nickname="ckpt",
)
laila.wait(manifest_future)
manifest_data = manifest_future.result.data

print(f"Model class: {manifest_data['model_class']}")
print(f"Parameters: {list(manifest_data['model_params'].keys())}")
```

Now fetch every model parameter:

```python
param_ids = list(manifest_data["model_params"].values())

with laila.guarantee:
    param_future = laila.remember(param_ids, pool_nickname="ckpt")

laila.wait(param_future)
```

Reconstruct the `state_dict` and load it into a fresh model:

```python
recalled_state_dict = {}
for name, gid in manifest_data["model_params"].items():
    future = laila.remember(gid, pool_nickname="ckpt")
    laila.wait(future)
    recalled_state_dict[name] = future.result.data

model = SimpleCNN()
model.load_state_dict(recalled_state_dict)
model.eval()

print("Model reconstructed from S3 ✓")
```

Restore the optimizer:

```python
optim_future = laila.remember(manifest_data["optimizer"], pool_nickname="ckpt")
laila.wait(optim_future)

optimizer = torch.optim.Adam(model.parameters())
optimizer.load_state_dict(optim_future.result.data)

print("Optimizer reconstructed from S3 ✓")
```

## Step 7: Verify

Run a deterministic forward pass to confirm the model produces output:

```python
test_input = torch.randn(1, 3, 32, 32)
output = model(test_input)
print(f"Output shape: {output.shape}")  # torch.Size([1, 10])
print(f"Output: {output}")
```

## Clean up

```python
all_ids = param_ids + [manifest_data["optimizer"]]
with laila.guarantee:
    laila.forget(all_ids, pool_nickname="ckpt")

laila.forget(
    nickname=manifest_nickname,
    pool_nickname="ckpt",
)
print("All entries cleaned up from S3")
```

## Summary

- Each model parameter and the optimizer state become individual LAILA entries.
- A **manifest** entry maps human-readable names to `global_id` strings, serving as the single point of entry for reconstruction.
- After destroying all local state, you only need the manifest's nickname to recall everything from S3 and rebuild the model + optimizer.
- `laila.guarantee` ensures all async writes complete before continuing.
- This pattern scales to any checkpoint size — add learning rate schedulers, training metadata, or dataset fingerprints to the manifest as needed.
