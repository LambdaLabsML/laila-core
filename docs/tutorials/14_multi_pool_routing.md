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
from laila.data import FilesystemPool, HDF5Pool

hot_pool  = FilesystemPool(nickname="hot")
warm_pool = HDF5Pool(nickname="warm")
cold_pool = FilesystemPool(nickname="cold")

for p in (hot_pool, warm_pool, cold_pool):
    laila.memory.extend(p, pool_nickname=p.nickname)
```

## Memorize one entry per pool

Pass `dst_pool=` to direct each write to a specific destination. `dst_pool` accepts a registered nickname, a pool `global_id`, or a live pool object:

```python
hot_entry  = laila.constant(data={"role": "hot"},  nickname="entry_hot")
warm_entry = laila.constant(data=np.arange(8),     nickname="entry_warm")
cold_entry = laila.constant(data="archived",       nickname="entry_cold")

laila.memorize(hot_entry,  dst_pool="hot").wait()
laila.memorize(warm_entry, dst_pool="warm").wait()
laila.memorize(cold_entry, dst_pool="cold").wait()
```

!!! note "Back-compat aliases"
    The older `pool_nickname=` / `pool_id=` keywords still work and are folded into `dst_pool` inside `laila.memorize` / `laila.remember`. `laila.forget` uses `pool=` (with the same aliases).

## Recall each from the correct pool

`remember` takes the same `dst_pool=` routing kwarg as `memorize`. Asking the wrong pool simply fails to find the gid:

```python
for nick, pool in [("entry_hot", "hot"), ("entry_warm", "warm"), ("entry_cold", "cold")]:
    e = laila.remember(nickname=nick, dst_pool=pool, persist=False).wait()
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

## Routing by gid or by pool object

If you already have a pool handle, pass `dst_pool=pool.global_id` (or the pool object itself) to skip the nickname lookup. Resolution order inside the router is: pool object > gid > nickname > alpha pool:

```python
direct = laila.constant(data="direct write", nickname="direct_entry")
laila.memorize(direct, dst_pool=hot_pool.global_id).wait()
```

## Standalone pools: no registration required

`dst_pool=` also accepts a pool object that was **never** registered with the router. This is handy for scratch pools or one-off exports. Standalone pool objects are local-only: to target a pool on a peer you must name it with a gid/nickname string that exists on that peer.

```python
from laila.macros.defaults import DefaultPool

scratch = DefaultPool()   # not passed to laila.memory.extend
scratch_entry = laila.constant(data={"scratch": True}, nickname="scratch_entry")

fut = laila.memorize(scratch_entry, dst_pool=scratch)
if fut is not None:
    fut.wait()
print("in scratch pool:", scratch_entry.global_id in scratch)
print("in router:", scratch.global_id in laila.memory.pool_router.pools)

back = laila.remember(scratch_entry.global_id, dst_pool=scratch, persist=False)
print("recovered:", back.data)
```

## Choosing nicknames vs ids

- **Nicknames** are stable across processes — register the same nickname in every node and routing code does not need to know the gid.
- **gids** are unique to a single pool instance — useful inside one process where you already have the handle.

## Summary

- `laila.memory.extend(pool, pool_nickname=...)` registers a pool under a friendly name.
- `memorize` / `remember` take `dst_pool=`, `forget` takes `pool=`; each accepts a nickname, a gid, or a pool object (`pool_nickname=` / `pool_id=` remain as aliases).
- A pool object does not need to be registered: `dst_pool=<pool>` works for standalone pools, but only for local operations.
- Manifests can span pools transparently because each leaf is addressed by its gid.

Next: [Tutorial 15 — Migrating Entries Between Pools](15_pool_migration.md).
