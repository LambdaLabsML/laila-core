# Tutorial 8b: Recovering a Policy from a Manifest

A fresh process has nothing but AWS credentials. In this tutorial you will bootstrap a minimal policy with just an S3 pool, recover the full environment manifest stored in [Tutorial 8a](08a_environment_to_s3.md), rebuild a fully-configured policy from it, swap it in as the active policy, and shut down the bootstrap policy.

## Prerequisites

```bash
pip install "laila-core[s3]"
```

You will need a `secrets.toml` with AWS credentials **and** a completed run of [Tutorial 8a](08a_environment_to_s3.md) (the manifest must be in S3).

## Setup

```python
import json
import laila
from laila.pool import S3Pool
from laila.policy.central.memory.schema import Manifest
from laila.macros.defaults import DefaultPolicy
from dotmap import DotMap

laila.read_args("./secrets.toml")
```

## Step 1: Bootstrap a minimal policy

This is the "old" policy. It has a single S3 pool — just enough to reach the manifest stored in Tutorial 8a. No HDF5, no extra configuration.

```python
bootstrap_pool = S3Pool(
    bucket_name=laila.args.AWS_BUCKET_NAME,
    access_key_id=laila.args.AWS_ACCESS_KEY_ID,
    secret_access_key=laila.args.AWS_SECRET_ACCESS_KEY,
    region_name=laila.args.AWS_REGION,
    nickname="s3",
)
laila.memory.extend(bootstrap_pool, pool_nickname="s3")

old_policy = laila.active_policy
print(f"Bootstrap policy: {old_policy.global_id}")
print(f"Pools: {list(old_policy.central.memory.pool_router.pools.keys())}")
```

## Step 2: Remember the environment manifest

Reconstruct the manifest identity from its nickname, remember the blueprint, then remember the leaf entry that holds the environment dict.

```python
manifest = Manifest(nickname="env_manifest_v1")

with laila.guarantee:
    ref = laila.remember(manifest.global_id, pool_nickname="s3")

blueprint = ref.data[0]
manifest = Manifest(data=blueprint, nickname="env_manifest_v1")

print(f"Blueprint: {manifest.blueprint}")
```

```python
with laila.guarantee:
    config_future = manifest.remember(pool_nickname="s3")

remembered_data = config_future.data
data_map = dict(zip(list(manifest), remembered_data))

recovered_env = data_map[manifest["config"]]

print("Recovered environment:")
print(json.dumps(recovered_env, indent=2, default=str))
```

## Step 3: Build a new policy from the environment

The recovered dict is the *contents* of a single policy (`laila.args.environment.policies[<gid>]`). Wrapping it under a top-level `"policy"` key produces the shape that `laila.args` expects for field resolution. Assigning it as a `DotMap` to `laila.args`, resetting `_active_policy_gid`, and constructing a fresh `DefaultPolicy` picks up the full configuration — including pools that the bootstrap policy never had.

```python
laila.args = DotMap({"policy": recovered_env})
laila._active_policy_gid = None

new_policy = DefaultPolicy()
laila.active_policy = new_policy
laila._local_policies[new_policy.global_id] = new_policy

print(f"New policy: {new_policy.global_id}")
print(f"Pools on new policy: {list(new_policy.central.memory.pool_router.pools.keys())}")
```

## Step 4: Verify the new policy

The new policy should carry the full multi-pool setup from Tutorial 8a, not just the single S3 pool the bootstrap policy had:

```python
new_env = laila.args.environment.policies[laila.active_policy.global_id].toDict()

new_pools = new_env["central"]["memory"]["pool_router"]["pools"]
old_pools = recovered_env["central"]["memory"]["pool_router"]["pools"]

print(f"Recovered environment had {len(old_pools)} pool(s)")
print(f"New policy has {len(new_pools)} pool(s)")

for pool_id, pool_cfg in new_pools.items():
    print(f"\n  Pool: {pool_id}")
    for k, v in pool_cfg.items():
        print(f"    {k}: {v}")
```

## Step 5: Shut down the old policy

The bootstrap policy has served its purpose. `shutdown` stops its thread pools, and removing it from `_local_policies` drops the last reference.

```python
old_policy.central.command.shutdown(wait=True)
laila._local_policies.pop(old_policy.global_id, None)

print(f"Old policy {old_policy.global_id} shut down and removed")
print(f"Active policy is now: {laila.active_policy.global_id}")
```

## Clean up

Remove the environment manifest and its leaf entry from S3:

```python
s3_pool = S3Pool(
    bucket_name=laila.args.AWS_BUCKET_NAME,
    access_key_id=laila.args.AWS_ACCESS_KEY_ID,
    secret_access_key=laila.args.AWS_SECRET_ACCESS_KEY,
    region_name=laila.args.AWS_REGION,
    nickname="s3",
)
laila.memory.extend(s3_pool, pool_nickname="s3")

with laila.guarantee:
    manifest.forget(pool_nickname="s3")

print("Manifest and leaf entries cleaned up from S3")
```

## Summary

- Only S3 credentials were needed to recover the full policy configuration.
- `laila.remember` fetched the manifest blueprint, then the environment entry.
- Assigning the recovered dict to `laila.args` as a `DotMap` and constructing a `DefaultPolicy` rebuilt the complete setup — including pools the bootstrap policy never registered.
- The old bootstrap policy was shut down cleanly with `shutdown(wait=True)`.
- This pattern enables portable policy snapshots, migration between environments, and disaster recovery.

Next: [Tutorial 9 — Peer-to-Peer Communication](09_peer_request_entry.md), where two policies on localhost transparently access each other's memory over TCP.
