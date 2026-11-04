# Data loading reference

Worked blueprints, per-datapoint ingestion, the storage model behind the defaults, and the loader API. Everything here uses only the public `laila.*` surface.

## Per-datapoint ingestion

The one ingestion pattern, for any dataset size. Each loop iteration builds **one** datapoint, memorizes its entries to the cloud pool immediately, and records only the gids under that datapoint's own key. `laila.memorize` returns a future at once, so the upload of datapoint *i* runs while datapoint *i+1* is being read; `with laila.guarantee:` around the loop waits for all of them at the end. RAM holds one datapoint's payload at a time plus the blueprint of strings.

```python
blueprint: dict[str, dict[str, str]] = {}  # datapoint key -> {modality: gid}
asset_gid: dict[str, str] = {}  # path -> gid, for assets shared by several datapoints


def memorized(entry: laila.Entry) -> str:
    """Start uploading one entry now; return its gid."""
    laila.memorize(entry, dst_pool="cloud")
    return entry.global_id


with laila.guarantee:  # every upload started inside has finished when the block exits
    for i, row in enumerate(iter_rows()):
        path = row["image_id"]
        if path not in asset_gid:  # shared asset: uploaded the first time it is seen
            asset_gid[path] = memorized(laila.constant(data=read_bytes(path)))
        blueprint[f"qa_{i:06d}"] = {  # this datapoint's key, its modalities only
            "image": asset_gid[path],
            "question": memorized(laila.constant(data=row["question"])),
            "answer": memorized(laila.constant(data=row["answer"])),
        }

manifest = laila.manifest(data=blueprint, nickname="vqa_train")  # strings only: nothing pending
with laila.guarantee:
    laila.memorize([manifest], dst_pool="cloud")  # the index is the only upload after the loop
```

Note the list in `laila.memorize([manifest], ...)`: a bare manifest dispatches to `manifest.memorize()`, which is also fine here (no pending entries) but the list form makes the intent explicit.

What this pattern rules out:

- Collecting `Entry` objects in a dict and calling `manifest.memorize()` once at the end (one big upload, whole dataset in RAM).
- Buffering entries into chunks of N and flushing: there is nothing to gain, the per-entry futures already overlap.
- Stacking several datapoints into one entry (`np.stack(images)`, a list of rows, a parquet shard as a datapoint). One entry = one modality of one datapoint; the only entry referenced by several datapoints is a genuinely shared asset.
- A top-level key whose value is a list of datapoints. The value of a datapoint key is always the dict of its modalities.

## Blueprints

A manifest leaf is a `global_id` string (or a list of them). Each example is the body of the ingestion loop above: what goes under one datapoint key.

### Image + caption (1:1)

`captions.csv: file, caption` with one row per image.

```python
with laila.guarantee:
    for i, row in enumerate(rows):
        blueprint[f"caption_{i:06d}"] = {
            "image": memorized(laila.constant(data=read_bytes(root / "images" / row["file"]))),
            "caption": memorized(laila.constant(data=row["caption"])),
        }
```

### Image -> multiple QA (1:N, shared asset)

`qa.json: [{"image_id": "img_0000", "question": ..., "answer": ...}, ...]`, three rows per image. Each QA pair is its own datapoint; the image is memorized the first time it is seen and its gid reused:

```python
with laila.guarantee:
    for i, row in enumerate(qa):
        image_id = row["image_id"]
        if image_id not in asset_gid:
            asset_gid[image_id] = memorized(
                laila.constant(data=read_bytes(root / "images" / f"{image_id}.jpg"))
            )
        blueprint[f"qa_{i:06d}"] = {
            "image": asset_gid[image_id],  # same gid in every QA of this image
            "question": memorized(laila.constant(data=row["question"])),
            "answer": memorized(laila.constant(data=row["answer"])),
        }
```

Resulting blueprint (abridged):

```python
{"qa_000000": {"image": "LAILA:ENTRY:3b9c...", "question": "LAILA:ENTRY:...", "answer": "LAILA:ENTRY:..."},
 "qa_000001": {"image": "LAILA:ENTRY:3b9c...", ...},     # same image gid
 "qa_000002": {"image": "LAILA:ENTRY:3b9c...", ...}}
```

`sum(1 for _ in manifest)` counts leaves (3 per datapoint); `len(set(manifest))` counts unique entries.

Asset-centric alternative, only on explicit request: `{"image": gid, "questions": [gid, gid, gid], "answers": [gid, gid, gid]}`, where the datapoint is the image. `Dataset` transforms list leaves element-wise and `default_collate` turns them into lists of lists. Even then each question and answer is its own entry; the list holds gids, never several rows in one entry.

### Video + timestamped questions (segment modality)

`video_qa.jsonl: {"video": "videos/vid_0.mp4", "start": 1.5, "end": 3.5, "question": ..., "answer": ...}`. The video is stored whole (raw `.mp4` bytes) and shared; the time range becomes its own small entry in the datapoint:

```python
with laila.guarantee:
    for i, row in enumerate(iter_jsonl(root / "video_qa.jsonl")):
        if row["video"] not in asset_gid:
            asset_gid[row["video"]] = memorized(
                laila.constant(data=read_bytes(root / row["video"]))
            )
        blueprint[f"vqa_{i:06d}"] = {
            "video": asset_gid[row["video"]],  # shared raw mp4 bytes
            "segment": memorized(laila.constant(data={"start": row["start"], "end": row["end"]})),
            "question": memorized(laila.constant(data=row["question"])),
            "answer": memorized(laila.constant(data=row["answer"])),
        }
```

At load time a `transforms={"video": decode_mp4_bytes}` callable receives only the bytes; to decode just the segment, combine the two modalities in a custom `collate_fn` (it sees the whole datapoint dict) or decode lazily in the training step. The same pattern covers `bbox` (region of an image), `frame_range`, `channel` (one channel of a multichannel recording).

### Splits

One manifest per split, nicknamed `f"{dataset}_{split}"`, each built with its own loop and blueprint. Loaders take a split by nickname: `Dataset("MANIFEST:vqa_val", pool="cloud")`.

## Storage model

- Entries are serialized by payload type: `dict`/`list` -> msgpack, `np.ndarray` -> npy, `torch.Tensor` -> `torch.save`, everything else (including `bytes` and `str`) -> pickle.
- Every persistent pool (HDF5, S3, R2, GCS, Azure, SQLite, ...) stores a JSON string whose payload went through the pool's `transformations`: `transformation_base64` (default) or `transformation_base64_compression` (Base64 + zlib). Base64 costs about 33%, so raw media stored as bytes occupies about 1.37x its file size; there is no raw-binary pool path.
- zlib on already-compressed media (`jpg`, `png`, `mp4`, `mp3`) saves nothing and costs CPU on every read: keep the default for media pools; use compression for text / JSON pools.
- Decoded frames are 10-100x larger than the encoded file. A 10 s 720p clip is ~1 MB as `.mp4` and ~800 MB as uint8 frames. Store the file, decode in the loader.
- The in-memory alpha pool stores live `Entry` objects (no serialization). Nothing evicts from any pool on its own; the loader forgets consumed batches from RAM.

## Cache chain

```python
from laila.addons.torch import default_tiers

hdd, cloud = default_tiers(
    cloud_pool, hdd=None, hdd_nickname="hdd", cloud_nickname="cloud", force=False
)
```

- `laila.alpha_pool << hdd << cloud`: reads routed at the alpha pool (the default) walk the chain on a miss and write back into every tier in front.
- `hdd=None` creates `HDF5Pool(nickname="hdd")` under `<laila root>/pools/<uuid>/pool.h5py`; call `laila.set_default_directory(...)` first to relocate. Any pool works as the disk tier (`SQLitePool`, `DuckDBPool`).
- Writes, deletes and `exists` are pool-local: `laila.forget(gids)` evicts from RAM only; `laila.forget(gids, pool="hdd")` from disk; the cloud origin is never modified by the loader.
- `Manifest.realized` on a manifest remembered from `cloud` reads from `cloud` first (it remembers its source pool). `Dataset` / `DataLoader` always go through `manifest.sub_manifest(keys)`, which has no source hint, so every read enters at the alpha pool and uses the chain.
- To check the chain: after one batch, `hdd.exists(gid)` is `True` and `laila.alpha_pool.exists(gid)` is `False`.

## Loader API

```python
Dataset(manifest, *, transforms=None, pool=None, evict="auto")
DataLoader(dataset, batch_size=1, shuffle=None, sampler=None, batch_sampler=None, num_workers=0,
           collate_fn=None, pin_memory=False, drop_last=False, timeout=0, worker_init_fn=None,
           multiprocessing_context=None, generator=None, *, prefetch_factor=None,
           persistent_workers=False, lookahead=4, evict="auto", forget_from_hdd=False, hdd_pool="hdd")
```

- `Dataset[i]` -> `{modality: transformed payload}` for one datapoint (fresh `sub_manifest`, so it uses the chain). Works with a stock `torch.utils.data.DataLoader(num_workers=0)`; worker processes are rejected (no LAILA policy there).
- `DataLoader` inherits sampling, batching and collation from torch; its iterator derives from torch's `_BaseDataLoaderIter`, so the same `generator` seed gives the same batch order as the stock loader. `num_workers > 0` raises; use `lookahead`.
- Each batch is one `sub_manifest` realized with `await sub.async_realized` on LAILA's taskforce; decoding and `collate_fn` run on the taskforce's sync executor so I/O keeps flowing.
- `evict="auto"` forgets consumed entries from RAM only when the alpha pool fronts a chain (otherwise RAM is the only copy). An in-flight reference count keeps assets shared by pending batches alive.
- `loader.last_wait_s` is the time the last `next()` blocked: ~0 means the lookahead is hiding the I/O; a wait every `lookahead` steps means steps are shorter than one round-trip, raise `lookahead`.
- Breaking out of the loop early is safe: the iterator drains and releases in-flight batches and forgets their entries.

Transforms shipped in `laila.addons.torch.transforms`: `image_bytes_to_tensor` (Pillow decode -> `C x H x W` float), `image_array_to_tensor` (`H x W x C` uint8 array), `numpy_to_tensor`, `identity`.
