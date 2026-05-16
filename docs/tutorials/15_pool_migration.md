# Tutorial 15: Migrating Entries Between Pools

`laila.forget` is **pool-local** — it only removes a blob from one pool. To move entries from one backend to another you read, write to the destination, then forget on the source.

## Prerequisites

```bash
pip install "laila-core[hdf5]"
```

## Setup — source and destination

```python
import laila
from laila.pool import FilesystemPool, HDF5Pool

warm = FilesystemPool(nickname="warm")
cold = HDF5Pool(nickname="cold")
laila.memory.extend(warm, pool_nickname="warm")
laila.memory.extend(cold, pool_nickname="cold")
```

## Populate the source

We seed five entries in `warm`. Their `global_id` values are what the migration loop will iterate over:

```python
import numpy as np

entries = [
    laila.constant(data=np.random.randn(4, 4), nickname=f"mig_{i}")
    for i in range(5)
]
for e in entries:
    laila.memorize(e, pool_nickname="warm").wait()

gids = [e.global_id for e in entries]
```

## Migration loop

Three steps per entry:

1. `remember(gid, pool_nickname="warm", persist=False)` — fetch without re-caching into the alpha pool.
2. `memorize(entry, pool_nickname="cold")` — write to the destination.
3. `forget(gid, pool_nickname="warm")` — drop the source copy.

The `persist=False` is critical. With the default `persist=True` the read would also cache into the alpha pool, defeating the purpose of the move:

```python
for gid in gids:
    entry = laila.remember(gid, pool_nickname="warm", persist=False).wait()
    if isinstance(entry, list):
        entry = entry[0]
    laila.memorize(entry, pool_nickname="cold").wait()
    laila.forget(gid, pool_nickname="warm").wait()
```

## Verify

Direct pool introspection: `warm._keys()` should be empty, and `cold._keys()` should contain all five gids:

```python
warm_keys = list(warm._keys())
cold_keys = list(cold._keys())
print(len(warm_keys), len(cold_keys))
# 0 5
```

## Sharp edges

| Concern | Mitigation |
|---|---|
| `forget` only touches one pool | Iterate every pool that may hold a copy. |
| `persist=True` would re-cache during migration | Pass `persist=False` on the read step. |
| Concurrent producers writing to `warm` mid-migration | Lock or pause writes outside LAILA — there is no built-in lease. |

## Summary

- Migration = read with `persist=False`, write to destination, forget on source.
- `forget` is pool-local: wipe every pool you want clean.
- Gids and payloads survive intact, so callers using nicknames see no break.

Next: [Tutorial 16 — Working with the Future Bank](16_future_bank_deep_dive.md).
