# Tutorial 21: Publishing to the Hugging Face Hub

`HuggingFacePool` turns the HF Hub into a LAILA pool. Memorize a model manifest into it, and the artifacts appear as a real HF repo you can browse, share, or load with `huggingface_hub` clients.

## Prerequisites

```bash
pip install "laila-core[huggingface,torch]"
```

You need an HF token with **write** access to a repo you control. Put it in `secrets.toml`:

```toml
HF_TOKEN = "hf_..."
HF_REPO_ID = "your-username/laila-demo"
```

```python
import laila
laila.read_args("./secrets.toml")
```

## Build the pool

`repo_id` is `<user>/<repo>`. `repo_type` defaults to `"model"`; pass `"dataset"` if you're storing datasets. The repo is auto-created on first write:

```python
from laila.pool import HuggingFacePool

hf = HuggingFacePool(
    repo_id=laila.args.HF_REPO_ID,
    token=laila.args.HF_TOKEN,
    repo_type="model",
    path_prefix="laila_pool",
    nickname="hf",
)
laila.memory.extend(hf, pool_nickname="hf")
```

## A small model to publish

A two-layer CNN keeps the demo small. We capture every parameter as its own entry, then tie them together with a manifest:

```python
import torch
import torch.nn as nn
from laila.policy.central.memory.schema.manifest import Manifest

class TinyCNN(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv = nn.Conv2d(3, 8, kernel_size=3)
        self.fc = nn.Linear(8 * 6 * 6, 10)

    def forward(self, x):
        return self.fc(self.conv(x).flatten(1))

model = TinyCNN()
weights = {name: laila.constant(data=p.detach().clone(), nickname=f"tinycnn.{name}")
           for name, p in model.named_parameters()}
```

## Memorize the manifest

```python
manifest = Manifest(data=weights, nickname="tinycnn_manifest")
laila.memorize(list(weights.values()), pool_nickname="hf").wait()
manifest.memorize(pool_nickname="hf").wait()
```

Visit `https://huggingface.co/<your-repo-id>` to see the artefacts appear.

## Restore from scratch

After clearing local state, the manifest nickname is enough to rebuild the model. `manifest.realized` fetches every leaf in one batch:

```python
recovered = laila.remember(
    nickname="tinycnn_manifest", pool_nickname="hf", persist=False,
).wait()
restored_params = recovered.realized

new_model = TinyCNN()
new_model.load_state_dict({name: e.data for name, e in restored_params.items()})
```

## Datasets, too

Set `repo_type="dataset"` and `path_prefix=""` to publish a dataset manifest the same way. The `path_prefix` field separates LAILA-managed files from anything you upload through the regular `huggingface_hub` API in the same repo.

## Summary

- A single `HuggingFacePool` turns an HF Hub repo into a fully addressable LAILA pool.
- Memorize a manifest, share the repo URL, and any consumer with the nickname can rebuild your artifacts.
- `repo_type` and `path_prefix` let one repo host both LAILA artefacts and conventional HF content.

Next: [Tutorial 22 — Writing a Custom Serializer](22_custom_serializer.md).
