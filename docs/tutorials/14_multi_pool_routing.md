# Tutorial 14: Multi-pool Routing

Tutorial 2 introduced one pool at a time, and [Tutorial 6](06_pool_proxies.md) used the `<<` operator to chain a single cache hierarchy. Real systems often have several independent pools — one for hot data, one for warm data, one as the source of truth — and route writes accordingly.

## Prerequisites

```bash
pip install "laila-core[hdf5]"
```

## Register three pools

`laila.memory.extend(pool, pool_nickname="...")` registers a pool under a friendly name. The router supports any number of pools simultaneously:

```python
import laila
from laila.pool import FilesystemPool, HDF5Pool

hot_pool  = FilesystemPool(nickname="hot")
warm_pool = HDF5Pool(nickname="warm")
cold_pool = FilesystemPool(nickname="cold")

for p in (hot_pool, warm_pool, cold_pool):
    laila.memory.extend(p, pool_nickname=p.nickname)
```

## Memorize one entry per pool

Pass `pool_nickname=` to direct each write to a specific destination:

```python
hot_entry  = laila.constant(data={"role": "hot"},  nickname="entry_hot")
warm_entry = laila.constant(data=np.arange(8),     nickname="entry_warm")
cold_entry = laila.constant(data="archived",       nickname="entry_cold")

laila.memorize(hot_entry,  pool_nickname="hot").wait()
laila.memorize(warm_entry, pool_nickname="warm").wait()
laila.memorize(cold_entry, pool_nickname="cold").wait()
```

## Recall each from the correct pool

`remember` takes the same nickname/pool routing kwargs as `memorize`. Asking the wrong pool simply fails to find the gid:

```python
for nick, pool in [("entry_hot", "hot"), ("entry_warm", "warm"), ("entry_cold", "cold")]:
    e = laila.remember(nickname=nick, pool_nickname=pool, persist=False).wait()
    print(nick, "->", e.data)
```

## A manifest with leaves in multiple pools

A `Manifest` stores `global_id` strings; it does not care *which* pool each leaf lives in. At realization time the manifest fans out reads through the active policy's memory and the router picks the right pool for each leaf — provided the manifest itself is reachable too:

```python
from laila.policy.central.memory.schema.manifest import Manifest

multi = Manifest(data={
    "hot":  hot_entry,
    "warm": warm_entry,
    "cold": cold_entry,
}, nickname="multi_pool_manifest")
multi.memorize(pool_nickname="hot").wait()
```

Each leaf is keyed by its gid, so the router can read each from whichever pool it lives in. We register the manifest itself in the `hot` pool so callers know where to find the index.

## `pool_id` for direct routing

If you already have a pool handle, pass `pool_id=pool.global_id` to skip the nickname lookup. `pool_id` takes precedence over `pool_nickname`:

```python
direct = laila.constant(data="direct write", nickname="direct_entry")
laila.memorize(direct, pool_id=hot_pool.global_id).wait()
```

## Choosing nicknames vs ids

- **Nicknames** are stable across processes — register the same nickname in every node and routing code does not need to know the gid.
- **gids** are unique to a single pool instance — useful inside one process where you already have the handle.

## Summary

- `laila.memory.extend(pool, pool_nickname=...)` registers a pool under a friendly name.
- `memorize` / `remember` / `forget` accept either `pool_id` or `pool_nickname`; `pool_id` wins.
- Manifests can span pools transparently because each leaf is addressed by its gid.

Next: [Tutorial 15 — Migrating Entries Between Pools](15_pool_migration.md).
