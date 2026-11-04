# Add-ons

Optional layers on top of the core `laila` API. They only use the public surface (`memorize` / `remember` / `forget`, `Manifest`, the `<<` proxy operator, `laila.command.submit`) and never touch pool internals.

| Add-on | Import | What it does |
|---|---|---|
| `cache` | `from laila.addons import default_tiers` | Wires `laila.alpha_pool << hdd << cloud` (RAM << HDF5 << cloud read-through cache) in one call. |
| `torch` | `from laila.addons.torch import Dataset, DataLoader` | `torch.utils.data` pair over a manifest keyed `datapoint -> {modality: gid}`; the loader is a stock `DataLoader` with `lookahead` prefetching on LAILA's taskforce. Requires `pip install "laila-core[torch]"`. |
| `torch` | `from laila.addons.torch import Module, Optimizer` | `torch.nn.Module` / `torch.optim.Optimizer` wrappers with the same call API; `laila.memorize(obj)` / `laila.remember(obj)` / `laila.forget(obj)` turn them into a `ModuleManifest` (`MANIFEST:TORCH_MODULE:<nickname>`) / `OptimizerManifest` (`MANIFEST:TORCH_OPTIMIZER:<nickname>`) -- one detached CPU entry per tensor, plus a weight-free architecture skeleton -- and run it. No optimizer priming on restore; `manifest.rebuild()` reconstructs the object from scratch. |
| `torch` + core | `laila.complex_manifest(nickname="everything")` | `everything["model"] = model; everything["optimizer"] = optimizer; everything["dataset"] = ds.manifest; ...` -- one `ComplexManifest` (`MANIFEST:COMPLEX`) for the whole training state. `laila.memorize(everything)` is recursive; `laila.remember("MANIFEST:COMPLEX:everything")` resolves the nested manifests back into their typed classes on arrival and `everything.rebuild()` hands back the live `Module` / `Optimizer`. Importing `laila.addons.torch` lets bare `nn.Module` / `torch.optim.Optimizer` objects be assigned directly. |
| `skills/data_loading` | `.cursor/skills/data-loading` | Agent skill: point it at a dataset folder and it infers the datapoint mapping, builds the manifest with minimal storage, sets up the cache and hands back a loader. |
| `skills/model_checkpoint` | `.cursor/skills/model-checkpoint` | Agent skill: checkpoint and resume a torch model + optimizer through `Module` / `Optimizer` (nicknames, pools, restore order, deletion). |
| `skills` | `laila-skills install` / `from laila.addons.skills import install` | Copies the bundled skills out of the installed package into a skills folder (`./.cursor/skills/` by default, `--user` for `~/.cursor/skills/`, `--dest` for anywhere else; `--link` symlinks, `--force` replaces). |

Install everything the torch add-on and its skills need with `pip install "laila-core[torch-skills]"`, then `laila-skills install` in the project that should see the skills.

```python
import laila
from laila.data import S3Pool
from laila.addons.torch import Dataset, DataLoader, default_tiers, transforms

cloud = S3Pool(bucket_name="my-bucket", nickname="cloud")
default_tiers(cloud)  # alpha << HDF5Pool("hdd") << cloud

ds = Dataset(
    "MANIFEST:vqa_train", pool="cloud", transforms={"image": transforms.image_bytes_to_tensor}
)
for batch in DataLoader(ds, batch_size=32, shuffle=True, lookahead=4):
    ...  # {"image": Tensor, "question": [str], "answer": [str]}
```

Checkpointing a model and its optimizer:

```python
from laila.addons.torch import Module, Optimizer

model = Module(SimpleCNN().cuda(), nickname="cnn")  # same API as the wrapped model
optimizer = Optimizer(torch.optim.AdamW(model.parameters()), nickname="cnn_adamw")

with laila.guarantee:  # one entry per tensor, detached and copied to CPU first
    laila.memorize(model, dst_pool="cloud")
    laila.memorize(optimizer, dst_pool="cloud")

# later / elsewhere: same architecture, same optimizer construction, weights come from the pool
model = Module(SimpleCNN().cuda(), nickname="cnn")
optimizer = Optimizer(torch.optim.AdamW(model.parameters()), nickname="cnn_adamw")
with laila.guarantee:  # loads in place; no backward / priming step needed
    laila.remember(model, dst_pool="cloud")
    laila.remember(optimizer, dst_pool="cloud")
```

Everything under one nickname -- model, optimizer, dataset, config -- rebuilt elsewhere with nothing but the name:

```python
everything = laila.complex_manifest(nickname="everything")
everything["model"] = model  # Module (or a bare nn.Module)
everything["optimizer"] = optimizer  # Optimizer (or a bare torch.optim.Optimizer)
everything["dataset"] = ds.manifest  # plain manifest: referenced, never copied or deleted
everything["config"] = laila.constant(data={"lr": 1e-3, "epochs": 10})
with laila.guarantee:
    laila.memorize(everything, dst_pool="cloud")  # recursive: each member memorizes itself

# later / elsewhere
everything = laila.remember("MANIFEST:COMPLEX:everything", dst_pool="cloud").wait()
everything["model"]  # ModuleManifest  (recursive blueprint resolution)
live = everything.rebuild(
    device="cuda"
)  # {"model": Module, "optimizer": Optimizer, "dataset": Manifest, "config": Entry}
```

Tests: `python -m pytest tests/functional/addons -m "not cloud and not slow"`.
