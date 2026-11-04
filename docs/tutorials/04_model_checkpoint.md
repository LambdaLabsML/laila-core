# Tutorial 4: Model Checkpoint and Reload with an "everything" Manifest

This is the most advanced tutorial. You will save a PyTorch model **and** its optimizer to S3, together with the training config, under one **`ComplexManifest`** named `everything`; destroy every local reference; and get the exact same objects back from that single nickname.

Two pieces do the work:

- **`laila.addons.torch.Module` / `Optimizer`** wrap the torch objects without changing how you use them. Memorizing them snapshots every tensor as a detached CPU entry; the model additionally stores a weight-free *architecture skeleton* and the optimizer a reference to the model (and parameter name) each of its parameters belongs to.
- **`laila.complex_manifest`** (scope `MANIFEST:COMPLEX`) holds them as members. Memorizing it is recursive; remembering it triggers **recursive blueprint resolution** (every manifest-scoped leaf comes back as its typed manifest) and `rebuild()` hands back the live `Module` and `Optimizer`.

## Prerequisites

```bash
pip install "laila-core[s3,torch]"
```

## Setup

```python
import torch
import torch.nn as nn
import laila
from laila.data import S3Pool
from laila.addons.torch import Module, Optimizer

laila.read_args("./secrets.toml")
# secrets.toml example:
#
#   AWS_BUCKET_NAME = "my-bucket"
#   AWS_ACCESS_KEY_ID = "AKIA..."
#   AWS_SECRET_ACCESS_KEY = "..."
#   AWS_REGION = "us-east-1"

s3_pool = S3Pool(
    bucket_name=laila.args.AWS_BUCKET_NAME,
    access_key_id=laila.args.AWS_ACCESS_KEY_ID,
    secret_access_key=laila.args.AWS_SECRET_ACCESS_KEY,
    region_name=laila.args.AWS_REGION,
    nickname="ckpt_pool",
)
laila.memory.extend(s3_pool, pool_nickname="ckpt")
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

## Step 2: Wrap the model and the optimizer

`Module` and `Optimizer` behave exactly like the objects they wrap (`forward`, `parameters()`, `step()`, `state_dict()`, LR schedulers, ...) and are addressable by nickname: the model's checkpoint lives under `MANIFEST:TORCH_MODULE:<nickname>`, the optimizer's under `MANIFEST:TORCH_OPTIMIZER:<nickname>`.

```python
model = Module(model, nickname="my_cnn")
optimizer = Optimizer(optimizer, nickname="my_cnn_adam")

print(model.global_id)      # LAILA:MANIFEST:TORCH_MODULE:<uuid5("my_cnn")>
print(optimizer.global_id)  # LAILA:MANIFEST:TORCH_OPTIMIZER:<uuid5("my_cnn_adam")>
```

## Step 3: Put everything under one complex manifest

`everything[key] = value` accepts manifests, manifest-backed objects (the wrappers -- or bare `nn.Module` / `torch.optim.Optimizer` objects, which are wrapped for you and come back bare), entries and plain blueprint fragments. The blueprint only ever holds gids; the live objects stay attached as *members*.

```python
everything = laila.complex_manifest(nickname="my_checkpoint")
everything["model"] = model
everything["optimizer"] = optimizer
everything["config"] = laila.constant(
    data={"lr": 1e-3, "steps": 3, "arch": "SimpleCNN"}, nickname="my_checkpoint/config"
)

print(everything.global_id)   # LAILA:MANIFEST:COMPLEX:<uuid5("my_checkpoint")>
print(everything.blueprint)   # {"model": "LAILA:MANIFEST:TORCH_MODULE:...", "optimizer": "...", "config": "LAILA:ENTRY:..."}
```

A plain `Manifest` (a dataset, say) can be a member too -- `everything["dataset"] = ds.manifest`. It is referenced, never copied: only its one record is fetched on the way back, and forgetting `everything` never touches its entries.

## Step 4: Memorize everything to S3

One call. The complex manifest asks each member to memorize itself into the same pool (the optimizer sees its sibling model and records which parameter belongs to which model), batches the plain entries, and finally stores its own blueprint. The returned `GroupFuture` covers every write; `laila.guarantee` blocks until all of them are done:

```python
with laila.guarantee:
    laila.memorize(everything, dst_pool="ckpt")
```

Every leaf has a deterministic nickname (`my_cnn/state_dict/features.0.weight`, `my_cnn/architecture`, `my_cnn_adam/state/0/exp_avg`, ...), so memorizing again after more training overwrites the same keys in place.

## Step 5: Nuke all local state

Keep a copy of the weights for the final check, then delete every local object. After this, the only way to get the model back is through LAILA.

```python
reference_state = {name: tensor.clone() for name, tensor in model.state_dict().items()}
reference_optimizer = optimizer.state_dict()

del model, optimizer, everything
```

## Step 6: Reload from the nickname alone

`laila.remember("MANIFEST:COMPLEX:my_checkpoint")` fetches the blueprint. Because the scope says `COMPLEX`, LAILA walks it on arrival and casts every manifest-scoped leaf into its typed manifest -- **recursive blueprint resolution**. The `config` leaf is a plain entry and stays a gid; a plain manifest would never be walked.

```python
with laila.guarantee:
    ref = laila.remember("MANIFEST:COMPLEX:my_checkpoint", dst_pool="ckpt")
everything = ref.result

type(everything)              # ComplexManifest
type(everything["model"])     # ModuleManifest
type(everything["optimizer"]) # OptimizerManifest
everything["config"]          # "LAILA:ENTRY:..."  (plain leaf: still a gid)
```

`rebuild()` then unpickles the architecture skeleton, materializes it (on the CPU by default -- pass `device=`), loads the weights, constructs the optimizer over the rebuilt model's parameters and loads its state:

```python
live = everything.rebuild()
model, optimizer, config = live["model"], live["optimizer"], live["config"].data

type(model)             # laila.addons.torch.Module  (over SimpleCNN)
type(optimizer)         # laila.addons.torch.Optimizer  (over Adam)
everything["model"] is model   # True: the members went live
```

Pass `resolve=False` to `laila.remember` to get the bare blueprint instead, and call `everything.resolve()` later.

## Step 7: Verify

Same types, same values: the weights match the copy taken before the nuke, the optimizer state matches, and the optimizer drives the rebuilt model's parameters.

```python
assert all(torch.equal(reference_state[n], t) for n, t in model.state_dict().items())
live_ids = {id(p) for p in model.parameters()}
assert all(id(p) in live_ids for g in optimizer.param_groups for p in g["params"])
assert optimizer.state_dict()["param_groups"] == reference_optimizer["param_groups"]

output = model(torch.randn(1, 3, 32, 32))
print(output.shape)  # torch.Size([1, 10])
```

Training can simply continue -- `optimizer.step()` updates the rebuilt model -- and `laila.memorize(everything, dst_pool="ckpt")` re-snapshots the live members.

## Clean up

Forgetting `everything` removes what it owns: the model and optimizer checkpoints (every tensor and their blueprints), the config entry and the complex manifest itself. A plain `Manifest` member would only be dropped as a record -- its entries are shared, never deleted here.

```python
with laila.guarantee:
    laila.forget(everything, pool="ckpt")
```

## Summary

- **`Module` / `Optimizer`** (`laila.addons.torch`) wrap the torch objects without changing how you use them; each memorizes as a typed manifest (`MANIFEST:TORCH_MODULE:<nickname>` / `MANIFEST:TORCH_OPTIMIZER:<nickname>`) with one detached CPU entry per tensor. No optimizer priming is needed on restore.
- **`laila.complex_manifest`** (scope `MANIFEST:COMPLEX`) holds manifests, manifest-backed objects and entries as members; `laila.memorize(everything)` is recursive.
- **Recursive blueprint resolution**: when a complex manifest is remembered, every manifest-scoped leaf is fetched from the same pool and cast into its typed manifest. Plain manifests are never scanned -- the `COMPLEX` scope is what opts a manifest in.
- **`everything.rebuild()`** returns `{key: live object}` -- the model rebuilt from its architecture skeleton and weights, the optimizer bound to it with its state loaded, entries and dataset manifests as they are. A round trip gives back the same types and values.
- **`laila.forget(everything)`** is owner-aware: checkpoints are deleted in full, plain manifest members only as records.
- This pattern scales to any checkpoint: add LR schedulers' `state_dict`, RNG states (`torch.get_rng_state()`), metrics or a dataset manifest as further members.
