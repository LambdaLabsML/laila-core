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

A `constant` has `evolution = None` and no suffix on its `global_id`. A `variable` starts at `evolution = 0` (or whatever you pass) and carries a `-N` suffix derived from the counter:

```python
c = laila.constant(data="immutable", nickname="model.config")
v = laila.variable(data=[0.1, 0.2, 0.3], nickname="model.weights")

print(c.global_id)
# LAILA:ENTRY:GLOBAL_ID:...        (no suffix)

print(v.global_id)
# LAILA:ENTRY:GLOBAL_ID:...-0      (suffix `-0`)
```

The trailing `-0` is what makes evolutions addressable: two entries with the same nickname but different evolutions are two **different keys** in any pool.

## Recall a specific evolution

`laila.remember(nickname=..., evolution=N)` derives the same `global_id` you saw above. Pass the evolution explicitly to round-trip a single version:

```python
laila.memorize(v, pool_nickname="evo").wait()
recovered = laila.remember(nickname="model.weights", evolution=0, pool_nickname="evo").wait()
print(recovered.data)
# [0.1, 0.2, 0.3]
```

## Evolving — bumping the version

`Entry.evolve(data=...)` returns a **new** entry that shares the variable's UUID but has `evolution + 1`. The original entry is **not** mutated — you must rebind the result to capture the new version:

```python
history = [v]
for step in range(1, 5):
    next_v = history[-1].evolve(data=[x + 0.1 for x in history[-1].data])
    laila.memorize(next_v, pool_nickname="evo").wait()
    history.append(next_v)
```

All five evolutions now live in the pool as distinct keys. Recall any of them by passing `evolution=N`:

```python
for i in range(5):
    e = laila.remember(nickname="model.weights", evolution=i, pool_nickname="evo").wait()
    print(f"evolution {i}: {e.data}")
```

## Constants cannot evolve

`evolve` is defined on variables only. Calling it on a constant raises:

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

- A variable's `global_id` ends in `-N` where `N` is its evolution counter.
- `remember(nickname=..., evolution=N)` selects a specific version.
- `entry.evolve(data=...)` returns a new entry with `evolution + 1` — assign it back.
- Constants are immutable; `evolve` raises.

Next: [Tutorial 2 — Local Pools](02_local_pools.md).
