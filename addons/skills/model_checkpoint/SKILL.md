---
name: model-checkpoint
description: Checkpoint, restore and resume a PyTorch model and its optimizer through LAILA using laila.addons.torch.Module / Optimizer, which turn the objects into a ModuleManifest / OptimizerManifest (one detached CPU entry per tensor plus an architecture skeleton) on laila.memorize / remember / forget, and bundle model + optimizer + dataset + config under one laila.complex_manifest(nickname="everything") that rebuilds the whole training state elsewhere from its nickname alone. Use when the user asks to save, checkpoint, persist, restore, resume, reload, rebuild or delete a torch nn.Module, optimizer state, training state or model weights with laila, or wants a model / training run to be addressable by nickname across processes / machines.
---

# Model checkpoint

Takes a `torch.nn.Module` (and optionally its `torch.optim.Optimizer`, dataset manifest and config) and produces: (1) wrapped objects with the same call API that `laila.memorize` / `laila.remember` / `laila.forget` accept directly, (2) a checkpoint stored on a pool as a `ModuleManifest` keyed `"MANIFEST:TORCH_MODULE:<nickname>"` / `OptimizerManifest` keyed `"MANIFEST:TORCH_OPTIMIZER:<nickname>"`, (3) optionally one `ComplexManifest` keyed `"MANIFEST:COMPLEX:everything"` that holds all of them, (4) the restore snippet that resumes training in a fresh process -- either into a live model (`remember`) or from nothing but the nickname (`rebuild`).

```python
from laila.addons.torch import Module, Optimizer

model = Module(Net(), nickname="net")  # torch.nn.Module, same API
optimizer = Optimizer(
    torch.optim.AdamW(model.parameters()), nickname="net_adamw"
)  # torch.optim.Optimizer, same API
```

Golden rules:

- Only use `Module`, `Optimizer`, `laila.complex_manifest`, `laila.memorize` / `remember` / `forget`, `Manifest.*` and the `<<` proxy operator. Never serialize `state_dict()` by hand into one blob and never touch pool internals (`agentic/internal/memory.md`).
- **Wrap, do not rewrite.** The user's model and optimizer stay exactly as constructed; `Module(model)` / `Optimizer(opt)` only add a nickname and the three memory operations. Training code keeps calling `model(x)`, `optimizer.step()`, schedulers keep receiving the wrapper.
- **One nickname per object, unique and stable.** The nickname is the address (`"MANIFEST:TORCH_MODULE:<nickname>"` / `"MANIFEST:TORCH_OPTIMIZER:<nickname>"` / `"MANIFEST:COMPLEX:<nickname>"`) and every tensor gets a deterministic sub-nickname, so memorizing the same nickname again overwrites the previous checkpoint in place. Use distinct nicknames (`"net"`, `"net_adamw"`) for the model and its optimizer and suffix them (`f"net_step{step}"`) only when the user wants to keep several checkpoints.
- **Never prime the optimizer.** Restoring is `laila.remember(optimizer)` on an optimizer constructed over the model's parameters. No backward pass, no dummy step. An optimizer without parameters raises `RuntimeError` before any I/O; a parameter-count mismatch raises `ValueError` before anything is loaded.
- **One "everything" per training run.** When the user wants the whole run reproducible elsewhere (model + optimizer + dataset + config), put the members under `laila.complex_manifest(nickname="everything")` (Step 5) instead of memorizing them one by one; the nickname `"everything"` is the convention unless the user names it. Plain manifests (datasets) are referenced, never copied -- and never deleted by `forget`.
- Explicit user instructions override every default below.

## Step 1 - Wrap

```python
import laila
from laila.addons.torch import Module, Optimizer

model = Module(build_model().to(device), nickname="resnet18_cifar")
optimizer = Optimizer(
    torch.optim.SGD(model.parameters(), lr=0.1, momentum=0.9), nickname="resnet18_cifar_sgd"
)
scheduler = torch.optim.lr_scheduler.StepLR(
    optimizer, step_size=30
)  # the wrapper is a torch.optim.Optimizer
```

`Module` keeps the model as `.module` (DDP layout) and forwards `forward`, `parameters()`, `to()`, `train()` / `eval()`, hooks and any custom attribute or method; `state_dict()` keys are the bare model's (no `module.` prefix). `Optimizer` keeps the optimizer as `.optimizer` and forwards `param_groups` / `state` / `defaults` (same objects), `step`, `zero_grad`, `add_param_group`, `state_dict`, `load_state_dict`. The wrapped model may live on any device.

## Step 2 - Memorize

```python
with laila.guarantee:  # both uploads finished when the block exits
    laila.memorize(model, dst_pool="ckpt")
    laila.memorize(optimizer, dst_pool="ckpt")
```

What is written (see [reference.md](reference.md#blueprints)): one constant entry per `state_dict()` tensor (parameters **and** buffers: BatchNorm running stats, counters), one `architecture` entry (a pickled weight-free *skeleton* of the module on the `meta` device, a few KB), one per optimizer state tensor, one `param_groups` entry, one `meta` entry, and the manifest. Every tensor is `detach()`ed and copied to the CPU before the asynchronous upload starts, so the training loop can continue immediately and GPU memory is never pinned by the checkpoint. Non-persistent buffers (RoPE `inv_freq` tables, causal masks) are not `state_dict()` leaves; they travel inside the skeleton so a rebuilt model forwards bit-identically.

Checkpoint inside the training loop the same way, e.g. every `N` steps; the `with laila.guarantee:` block makes the step wait for the upload, or drop the block and let it overlap with the next steps (the snapshot is already a copy).

## Step 3 - Restore

Same architecture, same optimizer construction, then remember:

```python
model = Module(build_model().to(device), nickname="resnet18_cifar")
optimizer = Optimizer(
    torch.optim.SGD(model.parameters(), lr=0.1, momentum=0.9), nickname="resnet18_cifar_sgd"
)

with laila.guarantee:  # loads in place; order does not matter
    laila.remember(model, dst_pool="ckpt")
    laila.remember(optimizer, dst_pool="ckpt")
```

`laila.remember(wrapper)` returns a `Future` (runs on LAILA's taskforce) whose `.data` is the wrapper once `load_state_dict` has run; `future.wait()` is the explicit form. Tensors are copied onto whatever device the live model is on; optimizer state follows each parameter's device. Pass `strict=False` to `Module(...)` to load a partial checkpoint. `persist=False` on `laila.remember` skips the alpha-pool cache-back when RAM is tight.

## Step 4 - Delete / inspect

```python
with laila.guarantee:
    laila.forget(model, pool="ckpt")  # manifest + every tensor entry
    laila.forget(optimizer, pool="ckpt")

ckpt = laila.remember("MANIFEST:TORCH_MODULE:resnet18_cifar", dst_pool="ckpt", persist=False).wait()
ckpt.data  # blueprint: gids keyed like state_dict() + "architecture" + "meta"
model = ckpt.rebuild(device="cuda")  # Module built from the stored skeleton, weights loaded
```

A direct `laila.remember("MANIFEST:TORCH_MODULE:<nickname>")` returns a typed `ModuleManifest` (the scope picks the class); `.rebuild(device=)` reconstructs the model with no class instantiation on the caller's side (the class must be importable). `ModuleManifest.bound(raw_model, nickname).remember(pool=...)` loads into a bare, unwrapped model.

## Step 5 - Everything under one nickname

When the run must be reproducible from a different process / machine with nothing but a name:

```python
everything = laila.complex_manifest(nickname="everything")
everything["model"] = model  # Module (a bare nn.Module is adapted automatically)
everything["optimizer"] = optimizer  # Optimizer (a bare torch.optim.Optimizer too)
everything["dataset"] = train_ds.manifest  # plain Manifest: referenced by gid, not copied
everything["config"] = laila.constant(data={"lr": 0.1, "epochs": 90, "step": step})
with laila.guarantee:
    laila.memorize(everything, dst_pool="ckpt")  # recursive: each member memorizes itself
```

`ComplexManifest` (scope `MANIFEST:COMPLEX`) tells the read path that manifests live underneath, so only complex manifests are walked -- a dataset manifest with a million leaves is never scanned. Memorizing is recursive and the optimizer records, per parameter, which sibling model it belongs to (`meta["param_refs"]`), which is what makes a from-scratch rebuild possible.

Resume elsewhere:

```python
everything = laila.remember("MANIFEST:COMPLEX:everything", dst_pool="ckpt").wait()
everything["model"]  # ModuleManifest   (recursive blueprint resolution)
everything["optimizer"]  # OptimizerManifest
everything["dataset"]  # the dataset Manifest, untouched
everything["config"]  # still a gid: plain entries are fetched by rebuild()

live = everything.rebuild(
    device="cuda"
)  # {"model": Module, "optimizer": Optimizer, "dataset": Manifest, "config": Entry}
model, optimizer = live["model"], live["optimizer"]
optimizer.param_groups[0]["params"][0] is next(model.parameters())  # bound to the rebuilt model
step = live["config"].data["step"]
```

A round trip returns the same types and values: `Module` in -> `Module` out (bare `nn.Module` in -> bare out), `train()`/`eval()` mode preserved, optimizer state bound to the rebuilt parameters, dataset manifest identical. `laila.forget(everything, pool="ckpt")` deletes the complex manifest and the members it owns (model and optimizer with every tensor, plain entries); a plain manifest member (the dataset) is dropped as a record only, its data entries are never touched.

## Pools

Any pool works (`laila.memory.extend(S3Pool(...), pool_nickname="ckpt")`); with the `RAM << HDF5 << cloud` chain from `laila.addons.default_tiers`, remember at the alpha pool (`dst_pool=None`) and the read walks the chain. Checkpoints written to the alpha pool only live in RAM.

## Checklist before reporting back

- [ ] Model and optimizer wrapped with distinct, stable nicknames; training code unchanged otherwise
- [ ] `laila.memorize(model)` and `laila.memorize(optimizer)` inside `with laila.guarantee:` (or deliberately overlapped)
- [ ] Restore snippet constructs the same architecture / optimizer, then `laila.remember(...)` both; no priming step -- or, for a from-scratch resume, `laila.remember("MANIFEST:COMPLEX:everything").wait().rebuild(device=...)`
- [ ] Whole-run reproducibility requested: members under `laila.complex_manifest(nickname="everything")`, memorized once, dataset as `.manifest`
- [ ] Pool named explicitly (`dst_pool=`) or wrapper built with `pool=`
- [ ] `forget` snippet given when the user asked for cleanup / rotation

Blueprint shapes, device and dtype semantics, error cases and caveats: [reference.md](reference.md).
