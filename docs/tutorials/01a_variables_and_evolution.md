# Tutorial 1a: Variables and Evolution

Tutorial 1 introduced `laila.constant` and `laila.variable`. This tutorial digs into the second one — the `evolution` counter that lets a single nickname address an ordered sequence of versions of the same logical entry.

## Prerequisites

```bash
pip install laila-core
```

No credentials or external services required.

## Setup

```python
import laila
from laila.macros.defaults import DefaultPool

laila.memory.extend(DefaultPool(), pool_nickname="evo")
```

## Constant vs. variable global IDs

A `global_id` is `LAILA:<scope>:<uuid>[@key=value,...]`. Everything after `@` is a list of *attributes*; `evolution` is the only one that is part of an entry's identity.

A `constant` has `evolution = None` and no `@` suffix. A `variable` starts at `evolution = 0` (or whatever you pass) and carries `@evolution=N`:

```python
c = laila.constant(data="immutable", nickname="model.config")
v = laila.variable(data=[0.1, 0.2, 0.3], nickname="model.weights")

print(c.global_id)
# LAILA:ENTRY:...               (no attributes)

print(v.global_id)
# LAILA:ENTRY:...@evolution=0   (attribute `evolution=0`)
```

The `@evolution=0` is what makes evolutions addressable: two entries with the same nickname but different evolutions are two **different keys** in any pool.

## Memorize and recall a specific evolution

References you pass to `laila.remember` use the same grammar. A reference without a scope is an `ENTRY`, and `@evolution=N` selects one version. The keyword form `remember(nickname=..., evolution=N)` builds the same reference for you:

```python
laila.memorize(v, dst_pool="evo").wait()

by_string = laila.remember("ENTRY:model.weights@evolution=0", dst_pool="evo").wait()
by_kwargs = laila.remember(nickname="model.weights", evolution=0, dst_pool="evo").wait()
print(by_string.data)
# [0.1, 0.2, 0.3]
```

## Evolving by memorizing

You do not have to manage the counter yourself. A variable remembers whether its payload was **re-assigned** since it was last memorized (`entry.locally_modified`). When you memorize it again:

- payload untouched → the same evolution is re-written under the same key (idempotent);
- payload re-assigned → the entry becomes `evolution + 1` **in place** (same Python object, new `global_id`, fresh `creation_timestamp`) and lands under a new key. The previous evolution stays on disk.

Only *assigning* `entry.data = ...` flips the flag; mutating the payload object in place (`entry.data.append(...)`) is not observed, so re-assign when you want a new version.

```python
import time

for step in range(1, 5):
    time.sleep(0.002)  # creation timestamps have millisecond precision
    v.data = [round(x + 0.1 * step, 2) for x in [0.1, 0.2, 0.3]]
    laila.memorize(v, dst_pool="evo").wait()
    print(v.global_id)
# LAILA:ENTRY:...@evolution=1
# LAILA:ENTRY:...@evolution=2
# LAILA:ENTRY:...@evolution=3
# LAILA:ENTRY:...@evolution=4

laila.memorize(v, dst_pool="evo").wait()   # payload untouched: still @evolution=4
```

## Which evolution do I get back?

All five evolutions live in the pool as distinct keys.

- A reference **without** an evolution returns the **latest** one stored in the routed pool.
- `@evolution=-1` means the same thing explicitly; `-2` is the one before, and so on (negative values are only valid in references, never in an identity).
- `@evolution=N` with `N >= 0` is an exact version.

```python
latest = laila.remember("model.weights", dst_pool="evo").wait()
print(latest.evolution, latest.data)
# 4 [0.5, 0.6, 0.7]

previous = laila.remember("model.weights@evolution=-2", dst_pool="evo").wait()
print(previous.evolution, previous.data)
# 3 [0.4, 0.5, 0.6]

for i in range(5):
    e = laila.remember(f"model.weights@evolution={i}", dst_pool="evo").wait()
    print(f"evolution {i}: {e.data}")
```

## Recall by creation timestamp

Every laila object carries a `creation_timestamp` (ISO-8601 UTC, millisecond precision) stamped when it was created; a memorize that advances the evolution re-stamps it, so each stored version has its own. A reference may ask for the version created at an exact stamp with `@creation_timestamp=<iso>` (the string must match byte-for-byte; take it from an entry you already have):

```python
first = laila.remember("model.weights@evolution=0", dst_pool="evo").wait()
stamp = first.creation_timestamp          # e.g. 2026-09-29T22:09:36.104+00:00

same = laila.remember(f"model.weights@creation_timestamp={stamp}", dst_pool="evo").wait()
print(same.evolution, same.data)
# 0 [0.1, 0.2, 0.3]
```

## Explicit evolution with `evolve`

`Entry.evolve(data=...)` is the explicit alternative: it returns a **new** entry that shares the variable's UUID but has `evolution + 1`, leaving the original untouched. Use it when you want to branch a new version as a separate object rather than advancing the one you hold.

```python
branch = latest.evolve(data=[9.9, 9.9, 9.9])
print(latest.global_id)   # ...@evolution=4
print(branch.global_id)   # ...@evolution=5  (a different object)
```

## Under the hood: the pool index

How does `remember("model.weights")` find the latest evolution without listing every key? Each pool maintains a small **index shard** per entry (`LAILA:POOL_INDEX:...`) that records the stored evolutions and their creation timestamps. Shards are updated on every write and delete and stored in the pool itself (or in a pool you nominate with `index_pool=`). They are bookkeeping, so `pool.keys()` hides them unless you ask for them:

```python
pool = laila.memory.pool_router.pools[laila.memory.pool_router.pools_nicknames["evo"]]

print(len(pool.keys()))                            # 5  entries
print(len(list(pool.keys(include_index=True))))    # 6  entries + 1 index shard

base = v.global_id.partition("@")[0]
print(pool.index.candidates(base))   # the five evolution keys
print(pool.index.latest(base))       # ...@evolution=4
```

## Constants cannot evolve

`evolve` is defined on variables only, and `laila.memorize` never advances a constant — re-memorizing a constant with new data simply overwrites the same key. Calling `evolve` on a constant raises:

```python
try:
    c.evolve(data="oops")
except (RuntimeError, AttributeError) as e:
    print("caught:", type(e).__name__)
```

## When to use which

| Use a `constant` for | Use a `variable` for |
|---|---|
| Configs, hyperparameters, dataset splits | Model weights, training metrics |
| Reference data that should never change | Anything you'll re-write under the same logical name |
| Source-of-truth content addressed by nickname | Ordered history you want to walk by version |

A practical rule: if you ever want to look at "the entry I produced yesterday", reach for a variable so yesterday's evolution is still on disk after today's update.

## Summary

- A variable's `global_id` carries `@evolution=N`; a constant's has no `@` suffix.
- `laila.memorize` advances a variable's evolution in place when its payload was re-assigned, and is idempotent otherwise.
- `remember("name")` returns the latest evolution; `@evolution=N` is exact, `@evolution=-k` counts from the end, `@creation_timestamp=<iso>` picks by stamp.
- `entry.evolve(data=...)` returns a new entry with `evolution + 1` — assign it back.
- Pools keep a per-entry index shard so these lookups do not scan; `keys()` hides the shards.
- Constants are immutable; `evolve` raises.

Next: [Tutorial 2 — Local Pools](02_local_pools.md).
