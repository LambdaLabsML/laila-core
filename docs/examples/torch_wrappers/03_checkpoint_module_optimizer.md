# Example 3: Checkpoint and Restore — `Module` and `Optimizer` Wrappers

Checkpoint a PyTorch model and its optimizer through LAILA without writing a single `state_dict` to disk yourself, using the two wrappers from the add-on:

```python
from laila.addons.torch import Module, Optimizer
```

`Module` is a `torch.nn.Module` and `Optimizer` is a `torch.optim.Optimizer`; they wrap the objects you already built and change nothing about how you use them (`forward`, `parameters()`, `step()`, LR schedulers, `state_dict()`). What they add is a **nickname** and the three memory verbs: `laila.memorize(model)` snapshots every tensor as a detached CPU entry under `MANIFEST:TORCH_MODULE:<nickname>`, `laila.remember(model)` loads the stored tensors back into the live model in place, `laila.forget(model)` deletes the checkpoint. The optimizer is the same under `MANIFEST:TORCH_OPTIMIZER:<nickname>`, with no priming step on restore.

This example checkpoints inside a training loop, throws the objects away, restores them into freshly constructed ones, keeps training, rebuilds a second copy from nothing but the nickname and finally deletes the checkpoint. It runs against a local HDF5 pool so there is nothing to configure; swap the pool for any other (an `S3Pool` as in [Tutorial 4](../../tutorials/04_model_checkpoint.md)) and nothing else changes.

## Prerequisites

```bash
pip install "laila-core[hdf5,torch]"
```

## Setup

```python
import torch
import torch.nn as nn

import laila
from laila.data import HDF5Pool
from laila.addons.torch import Module, Optimizer

laila.memory.extend(HDF5Pool(nickname="ckpt_hdf5"), pool_nickname="ckpt")
torch.manual_seed(0)
```

A small CNN with BatchNorm, so the checkpoint has buffers (`running_mean`, `running_var`, `num_batches_tracked`) as well as parameters, and a deterministic stand-in for a data loader:

```python
class SimpleCNN(nn.Module):
    def __init__(self):
        super().__init__()
        self.features = nn.Sequential(
            nn.Conv2d(3, 16, 3, padding=1), nn.BatchNorm2d(16), nn.ReLU(),
            nn.Conv2d(16, 32, 3, padding=1), nn.BatchNorm2d(32), nn.ReLU(),
            nn.AdaptiveAvgPool2d(1),
        )
        self.classifier = nn.Sequential(nn.Flatten(), nn.Linear(32, 10))

    def forward(self, x):
        return self.classifier(self.features(x))


def make_batch(step):
    g = torch.Generator().manual_seed(step)
    return torch.randn(8, 3, 32, 32, generator=g), torch.randint(0, 10, (8,), generator=g)
```

## Wrap the model and the optimizer

Build the model and optimizer exactly as you normally would, then wrap them. The scheduler receives the wrapper and is none the wiser, because the wrapper *is* a `torch.optim.Optimizer`. `model=model` on the optimizer records, per parameter, which model it belongs to; that is what makes a from-scratch `rebuild()` of the optimizer possible at the end of this example (inside an `everything` manifest, as in [Example 4](04_train_checkpoint_resume.md), the sibling model is found automatically).

```python
model = Module(SimpleCNN(), nickname="cnn")
optimizer = Optimizer(
    torch.optim.AdamW(model.parameters(), lr=1e-3), nickname="cnn_adamw", model=model
)
scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=4, gamma=0.5)
criterion = nn.CrossEntropyLoss()

print(f"model      -> {model.global_id}")
print(f"optimizer  -> {optimizer.global_id}")
print(f"nn.Module? {isinstance(model, nn.Module)}   torch.optim.Optimizer? {isinstance(optimizer, torch.optim.Optimizer)}")
print(f"state_dict keys: {list(model.state_dict())[:3]} ... ({len(model.state_dict())} tensors)")
```

Expected output:

```
model      -> LAILA:MANIFEST:TORCH_MODULE:0e901461-7ba8-58d8-87fd-3764a13cf02a
optimizer  -> LAILA:MANIFEST:TORCH_OPTIMIZER:c7b19b5d-e8a1-5014-be97-dfa2c6c54076
nn.Module? True   torch.optim.Optimizer? True
state_dict keys: ['features.0.weight', 'features.0.bias', 'features.1.weight'] ... (16 tensors)
```

The ids are `uuid5` of the nickname, so they are the same on every machine and every run: `"cnn"` is the whole address of this checkpoint. `state_dict()` keys are the bare model's (no `module.` prefix), so a checkpoint written through the wrapper loads into the unwrapped model and vice versa.

## Train, checkpointing every few steps

The training step is untouched. Every four steps the model and optimizer are memorized to the `ckpt` pool; `with laila.guarantee:` makes the step wait for the writes, and because the nickname is the address, each checkpoint **overwrites the previous one in place** (deterministic leaf nicknames: `cnn/state_dict/features.0.weight`, `cnn_adamw/state/0/exp_avg`, ...). Every tensor is `detach()`ed and copied to the CPU before the upload starts, so a GPU model is never pinned by a checkpoint; drop the `guarantee` block to overlap the upload with the next steps.

```python
CHECKPOINT_EVERY = 4

model.train()
for step in range(1, 9):
    x, y = make_batch(step)
    optimizer.zero_grad()
    loss = criterion(model(x), y)
    loss.backward()
    optimizer.step()
    scheduler.step()
    line = f"step {step}: loss={loss.item():.4f}  lr={scheduler.get_last_lr()[0]:.2e}"
    if step % CHECKPOINT_EVERY == 0:
        with laila.guarantee:
            laila.memorize(model, dst_pool="ckpt")
            laila.memorize(optimizer, dst_pool="ckpt")
        line += "  [checkpointed]"
    print(line)
```

Expected output:

```
step 1: loss=2.2669  lr=1.00e-03
step 2: loss=2.3998  lr=1.00e-03
step 3: loss=2.3807  lr=1.00e-03
step 4: loss=2.1856  lr=5.00e-04  [checkpointed]
step 5: loss=2.2514  lr=5.00e-04
step 6: loss=2.3585  lr=5.00e-04
step 7: loss=2.3605  lr=5.00e-04
step 8: loss=2.3078  lr=2.50e-04  [checkpointed]
```

## What is in the pool

`laila.remember("MANIFEST:TORCH_MODULE:cnn")` with the nickname shorthand returns the typed `ModuleManifest` (the scope in the id selects the class) whose blueprint is the checkpoint's table of contents: one entry per `state_dict()` tensor, parameters and buffers alike, plus an `architecture` entry (a pickled, weight-free skeleton of the module a few KB in size) and a `meta` entry. The optimizer's blueprint holds one entry per state tensor per parameter, the `param_groups` (hyper-parameters, including the LR the scheduler had set) and its own `meta`.

```python
ckpt = laila.remember("MANIFEST:TORCH_MODULE:cnn", dst_pool="ckpt", persist=False).wait()
sd = ckpt.data["state_dict"]
print(type(ckpt).__name__, list(ckpt.data))
print(f"{len(sd)} tensor entries, e.g. features.0.weight -> {sd['features.0.weight']}")
print(f"architecture -> {ckpt.data['architecture']}")
print("features.1.running_mean in state_dict?", "features.1.running_mean" in sd)

opt_ckpt = laila.remember("MANIFEST:TORCH_OPTIMIZER:cnn_adamw", dst_pool="ckpt", persist=False).wait()
print(type(opt_ckpt).__name__, list(opt_ckpt.data))
print(f"state keys per param: {list(opt_ckpt.data['state']['0'])}")
```

Expected output:

```
ModuleManifest ['state_dict', 'architecture', 'meta']
16 tensor entries, e.g. features.0.weight -> LAILA:ENTRY:9720ef36-9d6e-5af6-9959-bd30d61d7174
architecture -> LAILA:ENTRY:cab616b0-441a-5277-8af2-16496826d5d4
features.1.running_mean in state_dict? True
OptimizerManifest ['state', 'param_groups', 'meta']
state keys per param: ['step', 'exp_avg', 'exp_avg_sq']
```

`persist=False` keeps the blueprint out of the alpha pool, which is what you want for a quick inspection.

## Throw everything away

Keep a copy of the weights, one optimizer moment and the current LR for the check below, then delete the live objects.

```python
reference_state = {k: v.clone() for k, v in model.state_dict().items()}
reference_exp_avg = optimizer.state[next(model.parameters())]["exp_avg"].clone()
reference_lr = optimizer.param_groups[0]["lr"]

del model, optimizer, scheduler
```

## Restore into fresh objects

Same architecture, same optimizer construction, same nicknames, then `laila.remember` both. Order does not matter and **no priming step** is needed: the optimizer is restored onto a freshly built `AdamW` whose `state` is empty, with no backward pass and no dummy `step()`. An optimizer without parameters raises before any I/O, and a parameter-count mismatch raises before anything is loaded. Tensors are copied onto whatever device the live model is on; optimizer state follows each parameter's device.

```python
model = Module(SimpleCNN(), nickname="cnn")
optimizer = Optimizer(
    torch.optim.AdamW(model.parameters(), lr=1e-3), nickname="cnn_adamw", model=model
)
scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=4, gamma=0.5)

w = "features.0.weight"
print(f"before remember: weights equal? {torch.equal(reference_state[w], model.state_dict()[w])}, "
      f"optimizer state entries = {len(optimizer.state)}")

with laila.guarantee:
    laila.remember(model, dst_pool="ckpt")
    laila.remember(optimizer, dst_pool="ckpt")

assert all(torch.equal(reference_state[k], v) for k, v in model.state_dict().items())
assert torch.equal(optimizer.state[next(model.parameters())]["exp_avg"], reference_exp_avg)
print(f"after remember:  weights equal, exp_avg equal, lr={optimizer.param_groups[0]['lr']:.2e} (was {reference_lr:.2e})")
print(f"model.training = {model.training}")
```

Expected output:

```
before remember: weights equal? False, optimizer state entries = 0
after remember:  weights equal, exp_avg equal, lr=2.50e-04 (was 2.50e-04)
model.training = True
```

The LR came back as `2.5e-4` because `param_groups` is part of the optimizer checkpoint; re-creating the `StepLR` over the restored optimizer is all the scheduler needs (add `scheduler.state_dict()` as a `laila.constant` next to the checkpoint if you also want its step counter back). `train()` / `eval()` mode is restored too.

## Keep training

Nothing to re-wire: the scheduler drives the restored optimizer, which drives the restored model.

```python
for step in range(9, 11):
    x, y = make_batch(step)
    optimizer.zero_grad()
    loss = criterion(model(x), y)
    loss.backward()
    optimizer.step()
    scheduler.step()
    print(f"step {step}: loss={loss.item():.4f}  lr={scheduler.get_last_lr()[0]:.2e}")
```

Expected output:

```
step 9: loss=2.2328  lr=2.50e-04
step 10: loss=2.3272  lr=2.50e-04
```

## Rebuild from the nickname alone

`remember` needs a model to load *into*. When there is none, `ModuleManifest.rebuild()` unpickles the architecture skeleton, materializes it (on the CPU unless you pass `device=`) and loads the weights: a second, independent `Module` over a brand-new `SimpleCNN`, with no class instantiation on your side. `OptimizerManifest.rebuild()` constructs the optimizer over that model's parameters (hand it the rebuilt model through `context`; with no context it rebuilds the model from the gid recorded at memorize time) and loads the state.

```python
rebuilt = ckpt.rebuild()
print(f"rebuilt: {type(rebuilt).__name__} over {type(rebuilt.module).__name__}, training={rebuilt.training}")
assert all(torch.equal(reference_state[k], v) for k, v in rebuilt.state_dict().items())

rebuilt_opt = opt_ckpt.rebuild(context={ckpt.global_id: rebuilt})
print(f"rebuilt optimizer: {type(rebuilt_opt.optimizer).__name__}, state entries={len(rebuilt_opt.state)}, "
      f"lr={rebuilt_opt.param_groups[0]['lr']:.2e}")
print(f"bound to the rebuilt model: {rebuilt_opt.param_groups[0]['params'][0] is next(rebuilt.parameters())}")
```

Expected output:

```
rebuilt: Module over SimpleCNN, training=True
rebuilt optimizer: AdamW, state entries=10, lr=2.50e-04
bound to the rebuilt model: True
```

The model class has to be importable where `rebuild()` runs (`from models import SimpleCNN` in a training script). A class defined in a notebook cell or a script's `__main__` is pickled *by value* instead when `cloudpickle` is installed, so a fresh process that has never seen the class can still rebuild it.

## Clean up

`forget` deletes the manifest and every tensor entry it owns.

```python
with laila.guarantee:
    laila.forget(model, pool="ckpt")
    laila.forget(optimizer, pool="ckpt")

ckpt_pool = laila.memory.pool_router.pools[laila.memory.pool_router.pools_nicknames["ckpt"]]
print(f"checkpoint still there? {ckpt_pool.exists(model.global_id)}   weight entry? {ckpt_pool.exists(sd['features.0.weight'])}")
```

Expected output:

```
checkpoint still there? False   weight entry? False
```

## What just happened

1. **`Module(model, nickname="cnn")` / `Optimizer(opt, nickname="cnn_adamw", model=model)`** gave the objects a stable address (`MANIFEST:TORCH_MODULE:<uuid5("cnn")>`) and left their torch behaviour alone: the loop, the loss and the scheduler never saw the difference.
2. **`laila.memorize(model)`** snapshotted the `state_dict()` — parameters and BatchNorm buffers — as one detached CPU entry per tensor plus a weight-free architecture skeleton; **`laila.memorize(optimizer)`** did the same for `exp_avg` / `exp_avg_sq` / `step` and the `param_groups`. Same nickname, same keys: the second checkpoint overwrote the first in place.
3. **`laila.remember(model)` / `laila.remember(optimizer)`** loaded the stored tensors into freshly built objects, in place and with no priming step, so training resumed on the next line.
4. **`ModuleManifest.rebuild()` / `OptimizerManifest.rebuild()`** built a second copy from the pool alone, the optimizer bound to the rebuilt model's parameters.
5. **`laila.forget`** removed the manifests and every tensor entry.

## Summary

- `laila.addons.torch.Module` and `Optimizer` are drop-in subclasses of the torch types; wrap once, train as before.
- One nickname per object, unique and stable: it is the checkpoint's address, and memorizing again overwrites in place. Suffix it (`f"cnn_step{step}"`) only when you want to keep several checkpoints.
- Restore is `laila.remember(wrapper)` into an object of the same construction: no `state_dict` plumbing, no optimizer priming. `strict=False` on `Module(...)` loads a partial checkpoint.
- `rebuild()` is for when there is no live object to load into; pass `device=` to land the model on a GPU.
- Any pool works (`dst_pool=` per call, or `pool=` on the wrapper). Checkpoints written to the alpha pool only live in RAM.

Next: [Example 4 — Train, Checkpoint, Resume](04_train_checkpoint_resume.md) puts the model, optimizer, dataset manifest and config under one `everything` manifest and resumes a real training loop from that single nickname.
