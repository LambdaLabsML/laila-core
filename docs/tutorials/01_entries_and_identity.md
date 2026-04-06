# Tutorial 1: Entries and Identity

This tutorial introduces the most fundamental concept in LAILA: the **Entry**. You will learn how to create entries, inspect their identity attributes, and understand how LAILA's naming system works.

## Creating a constant entry

The simplest way to wrap data in LAILA is with `laila.constant`. A constant is an immutable entry — once created, its data and identity are fixed.

```python
import laila

entry = laila.constant(data={"message": "hello from laila"})
```

That's it. You now have a LAILA entry containing a dictionary.

## The global ID

Every entry has a `global_id` — a fully-qualified string that uniquely identifies it:

```python
print(entry.global_id)
# LAILA:ENTRY:GLOBAL_ID:a3f1c8e2-7b4d-4e9a-8f1c-2d3e4f5a6b7c
```

The format is `LAILA:<scope>:GLOBAL_ID:<uuid>`. This ID is deterministic for a given UUID and scope, and it is the key you use to recall the entry from storage later.

## Inspecting entry attributes

An entry carries several identity and state attributes:

```python
print(entry.uuid)
# a3f1c8e2-7b4d-4e9a-8f1c-2d3e4f5a6b7c

print(entry.scopes)
# ['ENTRY']

print(entry.evolution)
# None  — constants have no evolution counter

print(entry.state)
# EntryState.READY

print(entry.data)
# {'message': 'hello from laila'}
```

| Attribute | Meaning |
|-----------|---------|
| `uuid` | The underlying UUID for this entry. |
| `scopes` | A list of scope strings. Entries default to `['ENTRY']`. |
| `evolution` | Version counter. `None` for constants, starts at `0` for variables. |
| `state` | Lifecycle state — `STAGED` (pre-init) or `READY` (data loaded). |
| `data` | The actual payload you stored. |
| `global_id` | Fully-qualified identity string built from uuid, scopes, and evolution. |

## Constants vs. variables

A **constant** is immutable — it has no evolution counter:

```python
c = laila.constant(data=42)
print(c.evolution)  # None
```

A **variable** starts at evolution `0` and can be evolved:

```python
v = laila.variable(data=42)
print(v.evolution)  # 0
print(v.global_id)
# LAILA:ENTRY:GLOBAL_ID:...-0   (note the trailing -0)
```

The evolution suffix on the `global_id` lets LAILA track different versions of the same logical entry.

## Deterministic naming with nicknames

If you pass a `nickname`, LAILA derives the UUID deterministically from that string. This means the same nickname always produces the same `global_id`:

```python
a = laila.constant(data="first", nickname="my_entry")
b = laila.constant(data="second", nickname="my_entry")

print(a.global_id == b.global_id)  # True — same nickname, same identity
print(a.data)   # "first"
print(b.data)   # "second"
```

The two entries share the same identity (same `global_id`) but carry different data. When you memorize `b` to a pool, it overwrites `a` at that location because they have the same key. This is useful for giving stable, human-readable names to entries you want to recall later by nickname rather than by raw UUID.

## Wrapping different data types

`laila.constant` accepts any Python object that LAILA's serialization layer can handle — scalars, dicts, lists, numpy arrays, and torch tensors:

```python
import numpy as np

scalar_entry = laila.constant(data=3.14)
dict_entry = laila.constant(data={"key": "value"})
array_entry = laila.constant(data=np.zeros((5, 5)))
```

If you have PyTorch installed:

```python
import torch

tensor_entry = laila.constant(data=torch.randn(3, 224, 224))
```

## Summary

- `laila.constant(data=...)` creates an immutable entry.
- `laila.variable(data=...)` creates a versioned entry (evolution starts at 0).
- Every entry has a `global_id` — the key for storage and retrieval.
- Passing `nickname=` derives a deterministic UUID so the same name always maps to the same identity.
- The `data` property gives you back the original payload.

Next: [Tutorial 2 — Local Pools](02_local_pools.md), where you store and recall entries across Filesystem, Redis, and HDF5 backends.
