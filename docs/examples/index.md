# Examples

End-to-end walkthroughs that combine several LAILA features into one workflow. Where the [Tutorials](../tutorials/01_entries_and_identity.md) each introduce a single concept, an example builds something you would run for real. Every page has a matching Jupyter notebook you can download from the button under its title.

## Simple Data Loading

Two examples that together form a complete dataset workflow on Cloudflare R2:

1. [Dataset Creation — Random Images to Cloudflare R2](simple_data_loading/01_dataset_creation.md)
   Set up an R2 bucket and API token, generate random PNG images, upload each image (raw file bytes) and its caption to R2 inside the generation loop, and index them under a `Manifest` named `my_dataset` as `datapoint -> {"image": gid, "caption": gid}`.

2. [Data Loader — Prefetching Through memory << hdd << cloudflare](simple_data_loading/02_data_loader.md)
   Wire `laila.alpha_pool << hdd << r2` with `laila.addons.torch.default_tiers`, then stream the dataset with `laila.addons.torch.Dataset` / `DataLoader`, a `torch.utils.data.DataLoader` that keeps four batches in flight, realizes each batch as a `sub_manifest` through the chain with per-modality transforms on a LAILA taskforce, and `forget`s each batch from memory (and optionally disk) once the model has consumed it.

**Requires:** `pip install "laila-core[cloudflare,hdf5,torch]" pillow` and a `secrets.toml` with Cloudflare R2 credentials. Example 1 walks through obtaining them.

## Torch Wrappers

The model side of `laila.addons.torch`: `Module` and `Optimizer` wrap a `torch.nn.Module` / `torch.optim.Optimizer` without changing how you use them and make `laila.memorize` / `remember` / `forget` work on them directly. Both examples run on local pools, so there is nothing to configure.

3. [Checkpoint and Restore — `Module` and `Optimizer` Wrappers](torch_wrappers/03_checkpoint_module_optimizer.md)
   Checkpoint a CNN and its `AdamW` every few steps inside a training loop (one detached CPU entry per tensor, parameters and BatchNorm buffers alike, plus a weight-free architecture skeleton), inspect what landed in the pool, restore into freshly built objects with no optimizer priming, keep training, `rebuild()` a second copy from the nickname alone, and delete the checkpoint.

4. [Train, Checkpoint, Resume — Everything Under One Nickname](torch_wrappers/04_train_checkpoint_resume.md)
   Build a small image dataset, stream it with `Dataset` / `DataLoader` through `RAM << HDF5 << origin`, and put the model, optimizer, dataset manifest, config and RNG state under `laila.complex_manifest(nickname="everything")`. One `laila.memorize(everything)` per epoch; after losing the process, `laila.remember("MANIFEST:COMPLEX:everything").rebuild()` returns the live objects and the loop picks up at the next epoch.

**Requires:** `pip install "laila-core[hdf5,torch]" pillow`.
