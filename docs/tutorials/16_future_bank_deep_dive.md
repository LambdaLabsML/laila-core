# Tutorial 16: Working with the Future Bank

Every async operation in LAILA returns a future. Every future is registered in the active policy's `future_bank` keyed by its `global_id`. This tutorial shows how to inspect, wait on, and recover errors from futures.

## Prerequisites

```bash
pip install laila-core
```

## Submit a batch and inspect the bank

`memorize` on a list returns a `GroupFuture`. Both the group and each child end up in `future_bank`:

```python
import laila
from laila.macros.defaults import DefaultPool

laila.memory.extend(DefaultPool(), pool_nickname="bank")

entries = [laila.constant(data=i, nickname=f"bank_{i}") for i in range(5)]
group = laila.memorize(entries, pool_nickname="bank")

bank = laila.active_policy.future_bank
print(len(bank), "futures registered")
```

## Looking up a future by gid

`laila.runtime` is the canonical way to inspect futures. It accepts a future object, an identity object, or just the `global_id` string:

```python
group.wait()
print(laila.runtime.status(group))
print(laila.runtime.result(group))
```

## Iterating a GroupFuture's children

A `GroupFuture` carries the `global_id` of each child future on its `future_ids` attribute. Resolve each one through the policy's `future_bank`:

```python
for fid in group.future_ids:
    child = bank[fid]
    print(fid[:30], "->", laila.runtime.status(child))
```

## Recovering from a failed build

Build a `ComplexConstitution` whose code raises. The future captures the exception — `laila.runtime.status` returns `FAILED` and `.exception` yields the original error:

```python
from laila.entry import Entry
from laila.policy.central.memory.schema.manifest import Manifest

a = laila.constant(data=10)
m = Manifest(data={"a": a}); m.memorize().wait()

BAD_SRC = (
    "def broken(manifest):\n"
    "    raise ValueError('intentional')\n"
)

bad = Entry.variable(constitution=BAD_SRC, manifest=m)
fut = laila.build(bad)
try:
    fut.wait()
except Exception as e:
    print(type(e).__name__, "-", e)

print(laila.runtime.status(fut))
print(repr(fut.exception))
```

## Cancelling pending work at shutdown

`laila.terminate(cancel_pending=True)` drops queued-but-unstarted tasks. Already-running tasks still finish (or fail) — only the unstarted backlog is discarded:

```python
laila.terminate(wait=True, cancel_pending=True)
```

## Summary

- `laila.active_policy.future_bank` is a live dict of every future the policy owns.
- `laila.runtime.status / wait / result` accept futures, identity objects, or gid strings.
- A `GroupFuture`'s `future_ids` list lets you look up children in `future_bank` for individual inspection.
- Failed futures keep their exception around — read `.exception` instead of expecting `wait()` to swallow it.
- `terminate(cancel_pending=True)` drops queued submissions on the way down.

Next: [Tutorial 17 — Three-Node Mesh on Localhost](17_three_node_mesh.md).
