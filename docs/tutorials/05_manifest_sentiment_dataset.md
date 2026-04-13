# Tutorial 5: Sentiment Analysis Dataset with a Manifest

Build a small sentiment-analysis dataset as LAILA entries, organise them under a **Manifest**, push everything to S3, then remember and inspect the dataset from scratch using only the manifest nickname. Each datapoint is stored as two entries (`text_en_base` for the sentence, `label_en_base` for the sentiment) grouped under a nested blueprint key `datapoint_0` … `datapoint_24`.

## Prerequisites

```bash
pip install "laila-core[s3]"
```

You will need an AWS S3 bucket and credentials. Store them in a `secrets.toml` as described in [Tutorial 3](03_remote_pools.md).

## Setup

```python
import laila
from laila.pool import S3Pool
from laila.policy.central.memory.schema import Manifest

laila.read_args("./secrets.toml")

s3_pool = S3Pool(
    bucket_name=laila.args.AWS_BUCKET_NAME,
    access_key_id=laila.args.AWS_ACCESS_KEY_ID,
    secret_access_key=laila.args.AWS_SECRET_ACCESS_KEY,
    region_name=laila.args.AWS_REGION,
    nickname="sentiment_pool",
)
laila.memory.extend(s3_pool, pool_nickname="sentiment_pool")
```

## Step 1: Define the raw dataset

25 short sentences with `positive` or `negative` sentiment labels:

```python
RAW_DATASET = [
    ("The sunrise over the lake was absolutely breathtaking.", "positive"),
    ("I can't believe how rude the cashier was today.", "negative"),
    ("This homemade pasta is the best I have ever tasted.", "positive"),
    ("The flight was delayed by six hours with no explanation.", "negative"),
    # ... 21 more datapoints
]
```

## Step 2: Wrap each datapoint as LAILA entries

For every sentence, create two constant entries — one for the text and one for the label — then group them in a dict:

```python
dataset_entries = {}

for idx, (text, label) in enumerate(RAW_DATASET):
    text_entry = laila.constant(data=text)
    label_entry = laila.constant(data=label)
    dataset_entries[f"datapoint_{idx}"] = {
        "text_en_base": text_entry,
        "label_en_base": label_entry,
    }
```

This gives you a nested dict: 25 top-level keys, each mapping to two entry objects.

## Step 3: Build the Manifest

The `Manifest` accepts the nested dict of Entry objects. It automatically extracts a **blueprint** — the same nested structure but with `global_id` strings instead of Entry objects — and stashes the entries for upload.

```python
manifest = Manifest(
    data=dataset_entries,
    nickname="my_sentiment_dataset_v1",
)

print(f"Manifest global_id: {manifest.global_id}")
print(f"Top-level keys:     {len(manifest.keys())}")
print(f"Total leaf entries:  {sum(1 for _ in manifest)}")
```

The blueprint preview for a single datapoint looks like:

```python
manifest.blueprint["datapoint_0"]
# {'text_en_base': 'LAILA:ENTRY:GLOBAL_ID:...', 'label_en_base': 'LAILA:ENTRY:GLOBAL_ID:...'}
```

## Step 4: Memorize everything to S3

`manifest.memorize()` uploads all 50 leaf entries **plus** the manifest's own blueprint (as a MANIFEST-scoped entry) in one call. Wrap it in `laila.guarantee` to block until all writes finish:

```python
with laila.guarantee:
    manifest.memorize(pool_nickname="sentiment_pool")
```

## Step 5: Destroy all local state

After this the only way to recover the dataset is through LAILA:

```python
manifest_nickname = "my_sentiment_dataset_v1"
del dataset_entries, manifest
```

## Step 6: Remember the dataset from S3

Rebuild the manifest's identity from its nickname, recall the blueprint from S3, then reconstruct the manifest and fetch every referenced entry:

```python
manifest = Manifest(nickname=manifest_nickname)

with laila.guarantee:
    ref = laila.remember(manifest.global_id, pool_nickname="sentiment_pool")
blueprint = ref.data[0]

manifest = Manifest(data=blueprint, nickname=manifest_nickname)
```

Now remember all leaf entries:

```python
with laila.guarantee:
    remember_future = manifest.remember(pool_nickname="sentiment_pool")

remembered_data = remember_future.data
data_map = dict(zip(list(manifest), remembered_data))
```

## Step 7: Inspect the remembered dataset

Walk the blueprint and print each datapoint:

```python
for i in range(len(manifest.keys())):
    dp = manifest.blueprint[f"datapoint_{i}"]
    text  = data_map[dp["text_en_base"]]
    label = data_map[dp["label_en_base"]]
    print(f"{i:<4} {label:<10} {text[:60]}")
```

## Clean up

```python
with laila.guarantee:
    manifest.forget(pool_nickname="sentiment_pool")
```

## Summary

- Each datapoint is two entries: `text_en_base` (the sentence) and `label_en_base` (the sentiment).
- The manifest blueprint mirrors the dataset structure: `datapoint_0` … `datapoint_24`, each a nested dict with those two keys.
- **`manifest.memorize()`** pushes all 50 entries + the manifest itself to S3 in one call.
- The manifest can be fully reconstructed from just its **nickname** — remember the blueprint, then `remember()` every referenced entry.
- **`manifest.forget()`** deletes everything (entries + manifest) in one call.
- This pattern generalises to any tabular or structured dataset — add columns, languages, or modalities as additional keys per datapoint.

Next: [Tutorial 6 — Pool Proxies](06_pool_proxies.md), where you wire a multi-tier cache chain across backends with a single `<<` expression.
