# Model checkpoint reference

What `laila.addons.torch.Module` / `Optimizer` store, how the round trip behaves on devices and dtypes, the error cases, and the caveats. Everything here uses only the public `laila.*` surface.

## How dispatch works

`laila.memorize(obj)` / `laila.remember(obj)` / `laila.forget(obj)` look at a single bare argument: a `Manifest`, or any `laila.ManifestBacked` object, is dispatched to its own `memorize(pool=...)` / `remember(pool=..., persist=...)` / `forget(pool=...)`. `Module` and `Optimizer` are `ManifestBacked`; each call builds the matching `Manifest` subclass and runs it:

| Wrapper | Manifest subclass | `memorize` | `remember` | `forget` |
|---|---|---|---|---|
| `Module(nn.Module)` | `ModuleManifest.snapshot(model, nickname)` | inherited `Manifest.memorize` | fetch blueprint by gid, realize, `model.load_state_dict(..., strict=)` | inherited `Manifest.forget` |
| `Optimizer(torch.optim.Optimizer)` | `OptimizerManifest.snapshot(opt, nickname)` | inherited `Manifest.memorize` | fetch blueprint by gid, realize, `opt.load_state_dict(...)` | inherited `Manifest.forget` |

Each subclass has its own scope: `ModuleManifest` is `MANIFEST:TORCH_MODULE`, `OptimizerManifest` is `MANIFEST:TORCH_OPTIMIZER`, `ComplexManifest` is `MANIFEST:COMPLEX`, so the checkpoint's `global_id` is `LAILA:MANIFEST:TORCH_MODULE:<uuid5(nickname)>` and `wrapper.global_id == laila.resolve_global_id("MANIFEST:TORCH_MODULE:<nickname>")`. The scope is what types a read: `laila.remember("MANIFEST:TORCH_MODULE:<nickname>")` hands back a `ModuleManifest`, not a plain `Manifest` (the builder lookup matches the longest known scope prefix and falls back to `MANIFEST`).

## Blueprints

`ModuleManifest`:

```python
{
    "state_dict": {"conv1.weight": gid, "bn1.running_mean": gid, "bn1.num_batches_tracked": gid, ...},
    "architecture": gid,   # pickled skeleton: the module with every parameter / buffer on the "meta" device
    "meta": gid,   # {"kind": "module", "class": "ResNet", "module": "torchvision.models.resnet",
                   #  "pickler": "pickle" | "cloudpickle", "keys": [...], "device": "cuda:0",
                   #  "training": True, "nickname": ..., "wrapped": True, "torch": "2.x", "laila": "..."}
}
```

`OptimizerManifest`:

```python
{
    "state": {"0": {"step": gid, "exp_avg": gid, "exp_avg_sq": gid}, "1": {...}, ...},  # per parameter index
    "param_groups": gid,   # the list from state_dict()["param_groups"]: lr, betas, ..., "params": [indices]
    "meta": gid,           # {"kind": "optimizer", "class": "AdamW", "n_params": [per-group counts],
                           #  "defaults": {...}, "param_refs": [[[model gid, "conv1.weight"], ...], ...], ...}
}
```

`ComplexManifest` (`laila.complex_manifest(nickname=...)`): `{key: member gid}` for every top-level member -- a `ModuleManifest` gid, an `OptimizerManifest` gid, a dataset `Manifest` gid, a plain entry gid or a nested `ComplexManifest` gid. Nested dict / list fragments behave like a plain manifest.

Every leaf is a `laila.constant` with nickname `"<nickname>/state_dict/<key>"`, `"<nickname>/architecture"`, `"<nickname>/state/<idx>/<name>"`, `"<nickname>/param_groups"` or `"<nickname>/meta"`. The gids are therefore deterministic for a given nickname: memorizing the same wrapper again overwrites the same entries, the blueprint never changes and no blobs are orphaned. A parameter that never received a gradient has no optimizer state and no `state` key (as in torch).

## Skeleton and rebuild

`ModuleManifest.snapshot` deep-copies the module onto the `meta` device (`laila.addons.torch.skeleton`): same class, same submodule tree, same hooks and attributes, weight ties and flat-weight pointers (LSTM / GRU) preserved, parameters and persistent buffers without storage -- the pickle is a few KB regardless of model size. Non-persistent buffers (RoPE `inv_freq` / `cos` / `sin` tables, causal masks, anything `register_buffer(..., persistent=False)`) are not in a `state_dict`, so the skeleton carries them as detached CPU copies and `materialize` moves them to the target device intact. `torch.nn.utils.parametrize` parametrizations (`weight_norm`, `spectral_norm`, `orthogonal`, custom) cannot be pickled by torch, so they are detached from the skeleton, pickled alongside it and re-registered after `materialize`, before the `parametrizations.<name>.original*` leaves are loaded; the old hook-based `torch.nn.utils.weight_norm` / `spectral_norm` pickle as-is. `pickle` by reference is the default; `cloudpickle` (by value) is used when a class in the module tree lives in `__main__` (script / notebook cell -- `pickle` would accept it but another process could not resolve it) or when `pickle` fails; `meta["pickler"]` records which one. `rebuild(device=)` unpickles, `materialize`s every tensor on *device* (ties kept, dtype kept: a `bfloat16` model comes back `bfloat16`) and `load_state_dict`s the leaves. The class must be importable (or cloudpickle-able) wherever the rebuild happens; otherwise `RuntimeError`.

Verified end to end (bit-identical eval logits, optimizer bound to the rebuilt parameters, resumed training equal to torch's own `load_state_dict` resume) in `tests/functional/addons/torch/unit_tests/test_realistic.py`: a mixture-of-experts decoder LM (RoPE, RMSNorm, grouped-query attention, top-2 routed SwiGLU experts, tied head) in `float32` and `bfloat16`; hand-rolled LoRA over a frozen dense and MoE base with the optimizer covering the adapter weights only (`requires_grad` flags round-trip, base weights stay frozen through resumed steps); a ViT with `nn.MultiheadAttention`, stochastic depth and a `weight_norm` parametrization; hook-based `spectral_norm` / `weight_norm`; Hugging Face `LlamaForCausalLM` (tied embeddings, gradient checkpointing, `bfloat16`), `MixtralForCausalLM` (sparse MoE) and a `peft` LoRA model, all with HF-Trainer-style decay / no-decay parameter groups; a class defined in a script's `__main__` memorized there and rebuilt in a different process.

`OptimizerManifest.snapshot(..., context=siblings)` records `meta["param_refs"]`: per group, `[ModuleManifest gid, parameter name]` for every parameter it drives, resolved against the wrapper's explicit `model=` or the sibling `Module`s of the complex manifest. `rebuild(context={gid: live model})` picks the live parameters from the rebuilt model(s) -- a missing model is rebuilt from its own gid -- constructs `cls(groups, **defaults)` and loads the state. A checkpoint memorized without any known model has `None` refs and cannot be rebuilt from scratch (`RuntimeError`); it still `remember`s into a live optimizer.

`ComplexManifest.rebuild(device=)` resolves the typed members first (recursive blueprint resolution), then walks the blueprint in insertion order sharing one `context`, so the optimizer always sees the model rebuilt before it. Plain manifests are returned as is, plain entries are fetched, nested complex manifests are rebuilt in place. Afterwards `everything[key]` is the live object and `everything.rebuilt` is the same dict.

## Adapters and `bare`

`import laila.addons.torch` registers adapters so `everything["model"] = nn_module` / `everything["optimizer"] = torch_optimizer` wrap the bare objects into `Module(..., bare=True)` / `Optimizer(..., bare=True)` with nickname `"<container nickname>/<key>"`; `meta["wrapped"]` is `False` and `rebuild` hands the bare torch object back, so the round trip preserves the assigned type exactly. Explicit `Module(..., bare=True)` does the same outside a complex manifest.

## Device, dtype and aliasing

- Snapshot: `tensor.detach().to("cpu", copy=True)` for every tensor, including those already on the CPU, so the asynchronous upload never reads a parameter the training loop keeps mutating. Cost: one CPU copy of the checkpoint, freed when the upload completes.
- Restore: `load_state_dict` copies into the existing tensors, so the model keeps its device and dtype; optimizer state is cast by torch to each parameter's device (`step` counters stay CPU). Mixed dtypes (`float16`, `bfloat16`, `int64` buffers) round-trip unchanged.
- Tied weights (two `state_dict` keys sharing one tensor) are stored twice and loaded into the one shared parameter. Non-contiguous parameters are stored as-is and copied back in place.
- `state_dict()` on the wrapper is the inner model's; wrapping / unwrapping never changes keys.

## Error cases

| Situation | Raised by | Exception |
|---|---|---|
| `Module(x)` / `Optimizer(x)` where `x` is a class, a generator or anything but a constructed instance | constructor | `TypeError` |
| `ModuleManifest.bound(None, ...)` / `OptimizerManifest.bound(None, ...)` then `remember` on an empty manifest | `remember` | `RuntimeError` |
| optimizer with no `param_groups` / no parameters (never constructed over `model.parameters()`) | `memorize` / `remember`, before any I/O | `RuntimeError("optimizer is not initialized ...")` |
| stored `param_groups` sizes differ from the live optimizer | `remember`, before `load_state_dict` | `ValueError` naming both size lists |
| checkpoint keys do not match the model (`strict=True`) | `remember` | torch's `RuntimeError` from `load_state_dict` |
| nickname not in the pool | `remember` / `forget` | `KeyError` |
| `rebuild` on a manifest with no blueprint (never remembered) | `rebuild` | `RuntimeError` |
| architecture pickle cannot be loaded (class not importable here) | `ModuleManifest.rebuild` | `RuntimeError` naming the class |
| optimizer memorized without a known model (`param_refs` contain `None`) | `OptimizerManifest.rebuild` | `RuntimeError` -- remember into a live optimizer instead |
| `everything[key] = obj` with an unsupported type (not `Manifest` / `ManifestBacked` / `Entry` / gid / adapted type) | `__setitem__` | `TypeError` |

Errors raised inside `remember` surface from `future.wait()` (or from the `with laila.guarantee:` block).

## Caveats

- Dispatch applies to local pools only: with a peer `dst_policy` / `src_policy` the wrapper is not a transferable entry. To ship a checkpoint to a peer, memorize locally and relay the manifest's gids.
- `laila.memorize([model, optimizer])` (a list) does **not** dispatch; call `laila.memorize` once per wrapper.
- The optimizer must be constructed over the same parameters, in the same order, as when it was memorized (`param_groups` are matched by index, as in torch). Loading the model weights first is not required; both orders work.
- `remember` always loads what is in the pool. A manifest built with `.snapshot(...)` whose `.remember()` is called reloads the stored state, not the snapshot.
- A plain `laila.manifest(...)` refuses a `Manifest` / `ManifestBacked` member (`TypeError`); nesting needs `laila.complex_manifest(...)`. Only complex manifests are walked on read, which is what keeps a dataset manifest read O(1).
- `remember("MANIFEST:COMPLEX:...")` fetches the member *manifests* (small records) on arrival, never their leaves; tensors move only on `rebuild` / `remember`. Plain entries under a complex manifest stay gids until `rebuild`.
- The optimizer must be memorized next to its model (same complex manifest, or `Optimizer(..., model=model)`) for a from-scratch `rebuild`; without a known model it can still be `remember`ed into a live optimizer.
- Large non-persistent buffers are stored in full inside the architecture entry (they have nowhere else to go); a multi-megabyte precomputed table should be a persistent buffer or recomputed in `__init__`.
- `torch.compile`d models: wrap and memorize the eager model (`Module(model)` then `torch.compile(wrapper)`), or pass `compiled._orig_mod`. Checkpointing the `OptimizedModule` itself pickles but stores `_orig_mod.`-prefixed keys and recompiles on rebuild.
- LoRA checkpoints hold the full model (frozen base included); adapter-only checkpoints are not a separate mode.
- Nicknames are hashed under the active namespace (`laila.get_active_namespace()`); two processes must share the namespace to resolve the same nickname.
