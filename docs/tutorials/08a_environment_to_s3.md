# Tutorial 8a: Saving the Environment to S3

`laila.args.environment.policies[<global_id>]` is a live mirror of every policy's configuration — pools, routing, command, communication — as a JSON-serialisable dict, updated automatically whenever a CLI-capable instance is constructed. By wrapping that dict in a **Manifest** and uploading it to S3, the entire setup becomes recoverable from anywhere with bucket access.

This tutorial covers:

- Inspecting what `laila.args.environment` captures (and what it excludes)
- Wrapping the environment dict in a Manifest
- Persisting the manifest to S3
- Verifying the round-trip

The manifest is **not cleaned up** at the end — [Tutorial 8b](08b_policy_from_manifest.md) picks up where this one leaves off.

## Prerequisites

```bash
pip install "laila-core[s3,hdf5]"
```

You will need an AWS S3 bucket and credentials. Store them in a `secrets.toml` as described in [Tutorial 3](03_remote_pools.md).

## Setup

```python
import json
import laila
from laila.pool import S3Pool, HDF5Pool
from laila.policy.central.memory.schema import Manifest

laila.read_args("./secrets.toml")
```

## Step 1: Set up multiple pools

Register an S3 pool and an HDF5 pool so the environment has something interesting to capture:

```python
s3_pool = S3Pool(
    bucket_name=laila.args.AWS_BUCKET_NAME,
    access_key_id=laila.args.AWS_ACCESS_KEY_ID,
    secret_access_key=laila.args.AWS_SECRET_ACCESS_KEY,
    region_name=laila.args.AWS_REGION,
    nickname="s3",
)
laila.memory.extend(s3_pool, pool_nickname="s3")

hdf5_pool = HDF5Pool(nickname="local_hdf5")
laila.memory.extend(hdf5_pool, pool_nickname="local_hdf5")

print(f"Registered pools: {list(laila.active_policy.central.memory.pool_router.pools.keys())}")
```

## Step 2: Inspect `laila.args.environment`

The mirror is updated live as CLI-capable instances are constructed and collects every CLI-eligible field: pool configurations, routing settings, command parameters, and communication connections. Runtime-only fields marked `CLIExempt` (like `resource` and `transformations`) are excluded — only the settings needed to **reconstruct** the policy appear in the dict.

```python
env = laila.args.environment.policies[laila.active_policy.global_id].toDict()

print(json.dumps(env, indent=2, default=str))
```

Drill into the pools section to see what was captured:

```python
pools_section = env["central"]["memory"]["pool_router"]["pools"]
print(f"Pools captured: {len(pools_section)}")
for pool_id, pool_cfg in pools_section.items():
    print(f"  {pool_id}")
    for k, v in pool_cfg.items():
        print(f"    {k}: {v}")
```

## Step 3: Wrap the environment in a Manifest

Turn the dict into an immutable Entry, then wrap it in a Manifest under the key `"config"`. The manifest's blueprint maps that key to the entry's `global_id`.

```python
env_entry = laila.constant(data=env, nickname="my_environment_v1")

manifest = Manifest(
    data={"config": env_entry},
    nickname="env_manifest_v1",
)

print(f"Entry global_id:    {env_entry.global_id}")
print(f"Manifest global_id: {manifest.global_id}")
print(f"Blueprint:          {manifest.blueprint}")
print(f"Leaf entries:       {sum(1 for _ in manifest)}")
```

## Step 4: Memorize to S3

`manifest.memorize()` uploads the leaf entry **and** the manifest's own blueprint in one call. `laila.guarantee` blocks until every write finishes:

```python
with laila.guarantee:
    manifest.memorize(pool_nickname="s3")

print("Environment manifest uploaded to S3")
```

## Step 5: Verify the round-trip

Destroy local references, then recover everything from S3 using only the manifest nickname. The recovered dict should match the original exactly.

```python
original_env = env
del env, env_entry, manifest
```

Reconstruct the manifest identity from its nickname, remember the blueprint, then remember the leaf entry:

```python
manifest = Manifest(nickname="env_manifest_v1")

with laila.guarantee:
    ref = laila.remember(manifest.global_id, pool_nickname="s3")

blueprint = ref.data[0]
manifest = Manifest(data=blueprint, nickname="env_manifest_v1")

print(f"Blueprint recovered: {manifest.blueprint}")
```

Now fetch the config entry and verify:

```python
with laila.guarantee:
    config_future = manifest.remember(pool_nickname="s3")

remembered_data = config_future.data
data_map = dict(zip(list(manifest), remembered_data))

recovered_env = data_map[manifest["config"]]

assert recovered_env == original_env, "Round-trip mismatch!"
print("Round-trip verified — recovered environment matches the original")
```

## Summary

- `laila.args.environment.policies[<global_id>]` is a live, JSON-serialisable mirror of every CLI-capable policy's configuration.
- Runtime-only fields (`resource`, `transformations`, etc.) are excluded — only reconstructable settings are captured.
- Wrapping the dict in `laila.constant` + a `Manifest` turns it into a first-class LAILA artefact that can be memorised, remembered, and forgotten like any other entry.
- The manifest nickname (`"env_manifest_v1"`) is all you need to recover the environment later.

Next: [Tutorial 8b — Recovering a Policy from a Manifest](08b_policy_from_manifest.md), where you rebuild a complete policy from the manifest stored here.
