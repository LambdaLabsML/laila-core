---
name: data-loading
description: Turn a dataset folder (images, videos, audio, QA / caption annotations) into a LAILA Manifest keyed datapoint -> modality, store it cheaply (raw file bytes, deduplicated shared assets) on a cloud pool, set up the default RAM << HDF5 << cloud cache, and stream it with laila.addons.torch.Dataset / DataLoader. Use when the user points at a dataset folder, asks to load / ingest / memorize a dataset into laila, build a manifest from files, or wants a torch DataLoader over laila entries.
---

# Data loading

Takes a folder the user points at and produces: (1) a written understanding of how its datapoints map to each other, (2) a `Manifest` with the key structure below, memorized on a cloud pool with minimal storage, (3) the default `RAM << HDF5 << cloud` cache, (4) a `laila.addons.torch.DataLoader` ready for training.

```python
dataset["datapoint_id"] = {"modality_1": data, "modality_2": data}
```

Golden rules:

- Only use `laila.constant`, `laila.manifest`, `laila.memorize` / `remember` / `forget`, `Manifest.*`, the `<<` proxy operator and `laila.addons.*`. Never touch pool internals (`agentic/internal/memory.md`).
- **One datapoint at a time.** Build one datapoint, memorize its entries to the cloud pool in the same loop iteration, keep only the gids, move on. Never accumulate `Entry` objects and upload them all at the end.
- **Never combine datapoints.** Every datapoint has its own top-level key, and the value is a dict of its modalities only. No entry ever holds more than one datapoint (no stacked arrays, no lists of rows, no "chunk" / "shard" entries), and no top-level key ever maps to a list of datapoints.
- Explicit user instructions override every default below.

## Step 1 - Understand the structure

Run the scanner (stdlib only, safe on large folders):

```bash
python addons/skills/data_loading/scripts/scan_dataset.py <folder> --json /tmp/scan.json
```

It prints the extension histogram, assets per modality, every annotation file with its fields, **join candidates** (`image_id -> image files, hit 100%, 1:3 rows per asset`) and **segment candidates** (`start / end`). Then open two or three annotation rows yourself and decide:

| Question | Answer determines |
|---|---|
| What is the finest-grained training unit? (a QA pair, a caption, a clip) | the top-level `datapoint_id` |
| Which assets are referenced by more than one unit? (1:N joins) | shared assets, memorized once |
| Does an annotation reference a *part* of an asset? (`start`/`end`, frames, bbox) | an extra modality (`segment`, `bbox`) next to the asset |
| Are there splits (train/val/test folders or a `split` column)? | one manifest per split |

Report the mapping to the user as a short table (datapoint unit, modalities with their source and type, shared assets, segments, splits) before building anything, unless they already told you to just do it.

## Step 2 - Build the manifest

- One top-level key per datapoint: `f"{unit}_{i:06d}"` or the dataset's own id. The key maps to a dict of modalities and nothing else.
- One inner key per modality. Names are plain nouns: `image`, `video`, `audio`, `segment`, `question`, `answer`, `caption`, `label`, `bbox`, `meta`.
- One entry holds one modality of one datapoint. Do not pack several datapoints into one array / list / file entry to "save calls"; `laila.memorize` is asynchronous, so per-datapoint uploads already overlap.
- Shared assets: memorize **once**, keep a `path -> global_id` dict, reuse the gid in every datapoint. The manifest leaf is a string, so duplicates cost nothing. Sharing an asset between datapoints is the only form of cross-datapoint reuse.
- Segments are their own small entry: `laila.constant(data={"start": 12.0, "end": 14.5})` under `segment`; the asset they index stays whole under `video`.
- A modality may be a `list` of gids (`"questions": [gid, gid]`) only if the user explicitly wants the asset-centric shape; default is one datapoint per QA pair. A list is never a way to put several datapoints under one key.
- Nickname: `f"{dataset}_{split}"`. Keep manifests pinned (`manifest.floating_leaves() == []`).

Ingestion loop, the same for every dataset size: build one datapoint, memorize its entries right away, keep only gids in the `blueprint`. The manifest, an index of gids, is the only thing memorized after the loop.

```python
blueprint: dict[str, dict[str, str]] = {}

with laila.guarantee:  # waits for every upload started inside
    for i, row in enumerate(iter_rows()):
        image = laila.constant(data=read_bytes(row["path"]))
        caption = laila.constant(data=row["caption"])
        laila.memorize([image, caption], dst_pool="cloud")  # this datapoint, now
        blueprint[f"caption_{i:06d}"] = {"image": image.global_id, "caption": caption.global_id}

manifest = laila.manifest(
    data=blueprint, nickname="captions_train"
)  # strings only: nothing pending
with laila.guarantee:
    laila.memorize([manifest], dst_pool="cloud")
```

Shared assets and the full pattern: [reference.md](reference.md#per-datapoint-ingestion). Verify with `laila.remember("MANIFEST:captions_train", dst_pool="cloud", persist=False).wait()` and spot-check one datapoint through `laila.addons.torch.Dataset`.

## Step 3 - Minimise storage (defaults)

- Store the **encoded file bytes** as they are on disk: `open(path, "rb").read()` for `.mp4 .jpg .png .wav .parquet`. Never decoded frames, never resized copies, never one entry per video frame.
- Never duplicate an asset across datapoints (Step 2).
- Keep an annotation row as one `str` / `dict` entry, not one entry per field, unless fields are separate modalities (question vs answer).
- Pool transformations: default `transformation_base64` for media pools. `transformation_base64_compression` only for pools holding text / JSON; it is wasted CPU on already-compressed media. Persistent pools store base64 JSON, so stored size is about 1.37x the raw bytes regardless.
- When annotations are large relative to media, use two cloud pools (media uncompressed, annotations compressed) and reference both from one manifest.
- Overrides: if the user says "store as tensors", "pre-resize to 224", "sample 1 fps", do exactly that and note the storage cost in the summary.

## Step 4 - Default caching: RAM << HDF5 << cloud

```python
from laila.addons.torch import default_tiers

laila.set_default_directory("/data/laila")  # optional: where the HDF5 file lives
hdd, cloud = default_tiers(
    cloud_pool, cloud_nickname="cloud"
)  # hdd=None -> HDF5Pool(nickname="hdd")
```

`default_tiers` registers both pools and wires `laila.alpha_pool << hdd << cloud`: a read at the alpha pool that misses walks to HDF5, then to the cloud, and writes the blob back into each tier in front. Writes and `forget` are pool-local: the loader evicts from RAM only; the HDF5 tier keeps every entry until `forget_from_hdd=True` or `hdd.empty()`; the cloud is never modified. It is idempotent; pass `force=True` to re-wire an alpha pool already proxied elsewhere.

## Step 5 - Load

```python
from laila.addons.torch import Dataset, DataLoader, transforms

ds = Dataset(
    "MANIFEST:vqa_train", pool="cloud", transforms={"image": transforms.image_bytes_to_tensor}
)
loader = DataLoader(ds, batch_size=32, shuffle=True, drop_last=True, lookahead=4)

for batch in loader:  # {"image": Tensor[32,3,H,W], "question": [str]*32, "answer": [str]*32}
    ...
print(f"last wait {loader.last_wait_s * 1000:.1f} ms")
```

`DataLoader` is a `torch.utils.data.DataLoader` (same arguments, same batch order for the same `generator`), with `lookahead` batches prefetched on LAILA's taskforce instead of `num_workers`. Consumed batches are forgotten from RAM (`evict="auto"`: only when the alpha pool fronts a chain). Raise `lookahead` when steps are shorter than one cloud round-trip; set `forget_from_hdd=True` when the dataset exceeds the disk.

## Checklist before reporting back

- [ ] Mapping table shown to the user (unit, modalities, shared assets, segments, splits)
- [ ] Every top-level key is one datapoint mapping to `{modality: gid}`; no entry or key holds more than one datapoint
- [ ] Entries memorized inside the loop, one datapoint per iteration; only the manifest memorized after it
- [ ] Shared assets memorized once (`len(set(manifest)) < sum(1 for _ in manifest)` when 1:N)
- [ ] Media stored as raw file bytes unless overridden
- [ ] `manifest.floating_leaves() == []`
- [ ] `default_tiers` wired, one datapoint spot-checked through `Dataset`
- [ ] Loader snippet with the right `transforms` per modality

Worked blueprints, per-datapoint ingestion and the storage model: [reference.md](reference.md).
