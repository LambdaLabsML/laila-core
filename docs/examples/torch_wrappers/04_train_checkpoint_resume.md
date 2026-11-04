# Example 4: Train, Checkpoint, Resume — Everything Under One Nickname

Put the whole of a training run — the model, its optimizer, the dataset manifest it trains on, the config and the RNG state — under one **`ComplexManifest`** named `everything`, checkpoint it at the end of every epoch with a single call, lose the process, and pick the run back up from nothing but that nickname. All four add-on wrappers take part:

```python
from laila.addons.torch import Dataset, DataLoader, Module, Optimizer, default_tiers, transforms
```

The data side is [Example 2](../simple_data_loading/02_data_loader.md) in miniature: a `datapoint -> {modality: gid}` manifest streamed through a `RAM << HDF5 << origin` cache by the prefetching `DataLoader`. The model side is [Example 3](03_checkpoint_module_optimizer.md): `Module` / `Optimizer` snapshot themselves as typed manifests. `laila.complex_manifest` is what ties them together: memorizing it is recursive (each member memorizes itself), remembering it resolves every manifest-scoped member back into its typed class, and `rebuild()` hands back live objects with the optimizer bound to the rebuilt model.

Everything runs on local pools, so there is nothing to configure. The origin pool is a `SQLitePool` standing in for the cloud; replace it with an `S3Pool` / `CloudflarePool` and the rest of the example is unchanged.

## Prerequisites

```bash
pip install "laila-core[hdf5,torch]" pillow
```

## Setup

```python
import io
import time

import numpy as np
import torch
import torch.nn as nn
from PIL import Image

import laila
from laila.data import SQLitePool
from laila.addons.torch import Dataset, DataLoader, Module, Optimizer, default_tiers, transforms

torch.manual_seed(0)
rng = np.random.default_rng(0)
```

## Wire the pools

`default_tiers` registers the origin, creates an `HDF5Pool(nickname="hdd")` and chains `laila.alpha_pool << hdd << origin`. Reads routed at the alpha pool (the loader's path) walk the chain on a miss and cache the blob in every tier in front; writes and deletes stay local to the pool they are issued on.

```python
origin = SQLitePool(nickname="origin")
hdd, origin = default_tiers(origin, cloud_nickname="origin")

print(f"alpha -> {type(laila.alpha_pool.proxy_to).__name__}   hdd -> {type(hdd.proxy_to).__name__}   origin -> {origin.proxy_to}")
```

Expected output:

```
alpha -> HDF5Pool   hdd -> SQLitePool   origin -> None
```

## Build a small dataset

Sixty-four 32×32 PNGs in four classes (class *k* is "mostly" one colour channel, so a tiny CNN can actually learn it) and an integer label each. The ingestion loop is the one from the data-loading skill: build one datapoint, memorize its entries right away, keep only the gids. The manifest, an index of gids, is the only thing memorized after the loop.

```python
N, CLASSES = 64, 4


def make_png(label: int) -> bytes:
    base = np.full((32, 32, 3), 40, dtype=np.uint8)
    base[..., label % 3] = 200
    noise = rng.integers(0, 60, size=(32, 32, 3), dtype=np.uint8)
    buf = io.BytesIO()
    Image.fromarray(np.clip(base + noise, 0, 255).astype(np.uint8)).save(buf, format="PNG")
    return buf.getvalue()


blueprint = {}
with laila.guarantee:
    for i in range(N):
        label = i % CLASSES
        image = laila.constant(data=make_png(label))
        target = laila.constant(data=label)
        laila.memorize([image, target], dst_pool="origin")
        blueprint[f"img_{i:04d}"] = {"image": image.global_id, "label": target.global_id}

manifest = laila.manifest(data=blueprint, nickname="tiny_images")
with laila.guarantee:
    laila.memorize(manifest, dst_pool="origin")

print(f"dataset -> {manifest.global_id}, floating leaves: {manifest.floating_leaves()}")
print(f"img_0000 -> {manifest['img_0000']}")
```

Expected output:

```
dataset -> LAILA:MANIFEST:6cf639fb-3abe-51a0-924a-78618e37b887, floating leaves: []
img_0000 -> {'image': 'LAILA:ENTRY:650d8fe8-...', 'label': 'LAILA:ENTRY:d6e8e741-...'}
```

`Dataset` turns the manifest into a map-style `torch.utils.data.Dataset`: datapoints become positions, inner keys become fields, and `transforms` decodes the PNG bytes into a `3×32×32` float tensor at load time. The label has no transform and stays an `int`, which `default_collate` stacks into a `LongTensor`.

```python
dataset = Dataset("MANIFEST:tiny_images", pool="origin", transforms={"image": transforms.image_bytes_to_tensor})
item = dataset[0]

print(f"{len(dataset)} datapoints, modalities {dataset.modalities}")
print(f"dataset[0]: image {tuple(item['image'].shape)} {item['image'].dtype}, label {item['label']}")
```

Expected output:

```
64 datapoints, modalities ['image', 'label']
dataset[0]: image (3, 32, 32) torch.float32, label 0
```

## Model, optimizer, and `everything`

Wrap the model and optimizer as in Example 3, then put them under a complex manifest together with the dataset manifest. `everything[key] = value` accepts the wrappers (or bare `nn.Module` / `torch.optim.Optimizer` objects, which are adapted for you and come back bare), plain manifests and entries. The blueprint only ever holds gids; the live objects stay attached as *members*. The dataset goes in as a **plain `Manifest` member**: it is referenced by gid, never copied, and `forget`ting `everything` never touches its image entries.

```python
class TinyCNN(nn.Module):
    def __init__(self, classes):
        super().__init__()
        self.features = nn.Sequential(
            nn.Conv2d(3, 8, 3, padding=1), nn.ReLU(), nn.AdaptiveAvgPool2d(1), nn.Flatten()
        )
        self.head = nn.Linear(8, classes)

    def forward(self, x):
        return self.head(self.features(x))


model = Module(TinyCNN(CLASSES), nickname="tiny_cnn")
optimizer = Optimizer(torch.optim.SGD(model.parameters(), lr=0.05, momentum=0.9), nickname="tiny_cnn_sgd")
criterion = nn.CrossEntropyLoss()

everything = laila.complex_manifest(nickname="everything")
everything["model"] = model
everything["optimizer"] = optimizer
everything["dataset"] = dataset.manifest

print(f"everything -> {everything.global_id}")
```

Expected output:

```
everything -> LAILA:MANIFEST:COMPLEX:de2c4fc8-c6b7-5041-b071-fa0c184ecc46
```

No `model=` on the optimizer this time: inside a complex manifest the optimizer sees its sibling model at memorize time and records which parameter belongs to which model.

## The training loop and the checkpoint

`train_epoch` is an ordinary PyTorch loop over the add-on `DataLoader` (batches of 8, four in flight; `last_wait_s` tells you how long `next()` actually blocked). `checkpoint` is the whole persistence story: refresh the two plain members that change — the config carries the epoch counter, `rng` the torch RNG state — and memorize `everything` once. The complex manifest asks each member to memorize itself into the same pool and stores its own blueprint last; the `guarantee` block waits for all of it. Fixed nicknames on the config and RNG entries, like fixed nicknames on the wrappers, mean every checkpoint overwrites the previous one in place.

```python
def train_epoch(epoch, model, optimizer, loader):
    model.train()
    total, correct, running = 0, 0, 0.0
    for batch in loader:
        x, y = batch["image"], batch["label"]
        optimizer.zero_grad()
        logits = model(x)
        loss = criterion(logits, y)
        loss.backward()
        optimizer.step()
        running += loss.item() * len(y)
        correct += (logits.argmax(1) == y).sum().item()
        total += len(y)
    print(f"epoch {epoch}: loss={running / total:.4f}  acc={correct / total:.2f}  last wait {loader.last_wait_s * 1000:.1f} ms")


def checkpoint(epoch):
    everything["config"] = laila.constant(
        data={"epoch": epoch, "lr": 0.05, "batch_size": 8, "seed": 0}, nickname="everything/config"
    )
    everything["rng"] = laila.constant(data=torch.get_rng_state(), nickname="everything/rng")
    t0 = time.perf_counter()
    with laila.guarantee:
        laila.memorize(everything, dst_pool="origin")
    print(f"   checkpointed epoch {epoch} ({(time.perf_counter() - t0) * 1000:.0f} ms)")
```

Three epochs, a checkpoint after each:

```python
loader = DataLoader(dataset, batch_size=8, shuffle=True, generator=torch.Generator().manual_seed(0), lookahead=4)

for epoch in range(1, 4):
    train_epoch(epoch, model, optimizer, loader)
    checkpoint(epoch)

for key in everything.keys():
    print(f"  {key:9s} -> {everything.blueprint[key]}")
```

Expected output:

```
epoch 1: loss=1.3299  acc=0.27  last wait 0.2 ms
   checkpointed epoch 1 (50 ms)
epoch 2: loss=1.0728  acc=0.67  last wait 0.2 ms
   checkpointed epoch 2 (51 ms)
epoch 3: loss=0.5965  acc=0.75  last wait 7.9 ms
   checkpointed epoch 3 (104 ms)
  model     -> LAILA:MANIFEST:TORCH_MODULE:abaa25c1-3b9e-5535-aa58-9af26e338603
  optimizer -> LAILA:MANIFEST:TORCH_OPTIMIZER:da8260c7-336a-56df-9e99-a9116c51b860
  dataset   -> LAILA:MANIFEST:6cf639fb-3abe-51a0-924a-78618e37b887
  config    -> LAILA:ENTRY:22ade998-4233-592c-8bf3-b2e1f01d3e3b
  rng       -> LAILA:ENTRY:4943ed20-6163-5606-b4d3-71bb318f3dcc
```

The first epoch is the only one that reads from the origin: from then on the HDF5 tier serves every batch and the loader's wait stays near zero. The blueprint of `everything` is five gids, three of them manifests.

## Lose the process

Keep a copy of the weights and one momentum buffer for the check below, then delete every live object and empty the alpha pool. What follows is exactly what a fresh process would run: it needs the same pool wiring (pools with the same nickname open the same files) and the nickname `everything`, nothing else.

```python
reference = {k: v.clone() for k, v in model.state_dict().items()}
reference_momentum = optimizer.state[next(model.parameters())]["momentum_buffer"].clone()

del model, optimizer, dataset, loader, everything, manifest
laila.alpha_pool.empty()
```

## Resume from the nickname

`laila.remember("MANIFEST:COMPLEX:everything")` fetches the blueprint and, because the scope says `COMPLEX`, walks it on arrival: every manifest-scoped leaf is fetched from the same pool and cast into its typed manifest (**recursive blueprint resolution**). The config and RNG leaves are plain entries and stay gids until `rebuild()`, which turns every member into the live object it was made from: a `Module` over a rebuilt `TinyCNN` with the weights loaded, an `Optimizer` constructed over *that* model's parameters with its momentum restored, the dataset manifest untouched, and the two entries fetched.

```python
everything = laila.remember("MANIFEST:COMPLEX:everything", dst_pool="origin").wait()
print({k: type(v).__name__ for k, v in everything.members.items()})
print(f"config leaf is still a gid: {everything['config']}")

live = everything.rebuild()
print({k: type(v).__name__ for k, v in live.items()})

model, optimizer = live["model"], live["optimizer"]
start_epoch = live["config"].data["epoch"] + 1
torch.set_rng_state(live["rng"].data)

assert all(torch.equal(reference[k], v) for k, v in model.state_dict().items())
assert torch.equal(optimizer.state[next(model.parameters())]["momentum_buffer"], reference_momentum)
print(f"weights + momentum restored; resuming at epoch {start_epoch}")
```

Expected output:

```
{'model': 'ModuleManifest', 'optimizer': 'OptimizerManifest', 'dataset': 'Manifest'}
config leaf is still a gid: LAILA:ENTRY:22ade998-4233-592c-8bf3-b2e1f01d3e3b
{'model': 'Module', 'optimizer': 'Optimizer', 'dataset': 'Manifest', 'config': 'Entry', 'rng': 'Entry'}
weights + momentum restored; resuming at epoch 4
```

The dataset comes back as the same `Manifest`, so the loader is one line away. Two more epochs, each checkpointed under the same nickname (the members went live, so `everything["model"] is model` and a later `memorize` re-snapshots them):

```python
dataset = Dataset(live["dataset"], transforms={"image": transforms.image_bytes_to_tensor})
loader = DataLoader(
    dataset, batch_size=live["config"].data["batch_size"], shuffle=True,
    generator=torch.Generator().manual_seed(live["config"].data["seed"] + start_epoch), lookahead=4,
)

for epoch in range(start_epoch, start_epoch + 2):
    train_epoch(epoch, model, optimizer, loader)
    checkpoint(epoch)
```

Expected output:

```
epoch 4: loss=0.3960  acc=0.75  last wait 0.5 ms
   checkpointed epoch 4 (49 ms)
epoch 5: loss=0.3938  acc=0.72  last wait 0.2 ms
   checkpointed epoch 5 (55 ms)
```

The loss carries on from where epoch 3 left it, which it would not if the momentum buffers or the weights had come back fresh. Note that `TinyCNN` must be importable where `rebuild()` runs; a class defined in a notebook cell or a script's `__main__` is pickled by value when `cloudpickle` is installed, so a genuinely new process can still rebuild it.

## Clean up

`forget` is owner-aware. Forgetting `everything` deletes what it owns — the model and optimizer checkpoints with every tensor, the config and RNG entries, the complex manifest's own record — and drops the dataset manifest's record, but the dataset's image and label entries are shared and are never touched. They go only when the dataset itself is forgotten.

```python
first_image = dataset.manifest["img_0000"]["image"]

with laila.guarantee:
    laila.forget(everything, pool="origin")
print(f"checkpoint in origin? {origin.exists(everything.global_id)}   model weights? {origin.exists(everything.blueprint['model'])}")
print(f"img_0000 image in origin? {origin.exists(first_image)}")

with laila.guarantee:
    laila.forget(dataset.manifest, pool="origin")
hdd.empty()
laila.alpha_pool.proxy_to = None
print(f"img_0000 image in origin? {origin.exists(first_image)}")
```

Expected output:

```
checkpoint in origin? False   model weights? False
img_0000 image in origin? True
img_0000 image in origin? False
```

## What just happened

1. **`default_tiers(origin)`** wired `laila.alpha_pool << hdd << origin`; the dataset was memorized one datapoint at a time into the origin and indexed by a `Manifest` named `tiny_images`.
2. **`Dataset` / `DataLoader`** streamed it as `{"image": Tensor[8,3,32,32], "label": LongTensor[8]}` batches, four in flight, through the cache chain.
3. **`laila.complex_manifest(nickname="everything")`** held the `Module`, the `Optimizer`, the dataset manifest, the config and the RNG state as members; **one `laila.memorize(everything)` per epoch** snapshotted all of them, overwriting the previous checkpoint in place.
4. **`laila.remember("MANIFEST:COMPLEX:everything")`** resolved the members into their typed manifests and **`rebuild()`** returned the live model, the optimizer bound to it, the dataset manifest and the entries; the loop resumed at epoch 4 with the loss continuing where it stopped.
5. **`laila.forget(everything)`** removed the checkpoint and left the dataset's entries alone.

## Summary

- One nickname is enough to resume a run: model, optimizer, dataset, config and RNG travel together under a `ComplexManifest`, and `rebuild()` returns them as live objects.
- Memorizing a complex manifest is recursive and in place; put the checkpoint call at the end of the epoch (or every *N* steps) and keep the member nicknames fixed.
- A dataset manifest is a plain member: referenced, never copied, never deleted by the checkpoint's `forget`.
- Inside `everything` the optimizer finds its model by itself; standalone (Example 3) it needs `Optimizer(..., model=model)` to be rebuildable.
- Anything else that is state can be a member too: a scheduler's `state_dict`, metrics, a tokenizer config, a second model.

Back to [Example 3 — Checkpoint and Restore](03_checkpoint_module_optimizer.md), or to [Tutorial 4](../../tutorials/04_model_checkpoint.md) for the same `everything` pattern against S3.
