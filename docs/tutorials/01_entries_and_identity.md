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
# LAILA:ENTRY:a3f1c8e2-7b4d-4e9a-8f1c-2d3e4f5a6b7c
```

The format is `LAILA:<scope>:<uuid>[@key=value,...]`: a fixed `LAILA` prefix, one or more scope segments, the UUID, and an optional list of *attributes* after `@`. The only attribute that is part of an entry's identity is `evolution` (`...@evolution=0` for a fresh variable); anything else after `@` is a search argument understood by `laila.remember` (see [Tutorial 1a](01a_variables_and_evolution.md)). This ID is deterministic for a given UUID and scope, and it is the key you use to recall the entry from storage later.

## Inspecting entry attributes

An entry carries several identity and state attributes:

```python
print(entry.uuid)
# a3f1c8e2-7b4d-4e9a-8f1c-2d3e4f5a6b7c

print(entry.scopes)
# ['ENTRY']

print(entry.evolution)
# None  — constants have no evolution counter

print(entry.creation_timestamp)
# 2026-09-29T22:11:12.731+00:00

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
| `creation_timestamp` | ISO-8601 UTC stamp (millisecond precision) taken when the entry was created. Preserved across a pool round-trip; re-stamped when a new evolution is born. |
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
# LAILA:ENTRY:...@evolution=0   (note the trailing @evolution=0)
```

The evolution suffix on the `global_id` lets LAILA track different versions of the same logical entry.

## Deterministic naming with nicknames

If you pass a `nickname`, LAILA derives the UUID deterministically from that string. The same nickname always generates the same UUID every time:

```python
a = laila.constant(data="first", nickname="my_entry")
a_again = laila.constant(data="first", nickname="my_entry")

print(a.uuid == a_again.uuid)  # True — same nickname, same UUID every time
```

String references work the same way: `laila.remember("my_entry")` hashes the nickname exactly as the constructor did. A reference without a scope is an `ENTRY`; `"ENTRY:my_entry"` is the explicit spelling, and `"MANIFEST:my_dataset"` addresses another scope.

```python
print(laila.resolve_global_id("my_entry") == a.global_id)  # True
```

**Important:** because the UUID is derived solely from the nickname, creating a second entry with different data but the same nickname would clash with the first — avoid this:

```python
# Bad — clashes with a because the nickname is the same:
# b = laila.constant(data="second", nickname="my_entry")
```

This makes nicknames useful for giving stable, human-readable names to entries you want to recall later rather than by raw UUID.

## Wrapping different data types

`laila.constant` and `laila.variable` accept any Python object that LAILA's serialization layer can handle — scalars, dicts, lists, numpy arrays, and torch tensors.

Whatever data format you put in is exactly what you get back. A `dict` comes back as a `dict`, a numpy array comes back as a numpy array, and a torch tensor comes back as a torch tensor.

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
- Every entry has a `global_id` — `LAILA:<scope>:<uuid>[@evolution=<n>]` — the key for storage and retrieval, and a `creation_timestamp`.
- Passing `nickname=` derives a deterministic UUID so the same name always maps to the same identity; `laila.remember("name")` resolves the same way (scope-less references are entries).
- The `data` property gives you back the original payload.

Next: [Tutorial 1a — Variables and Evolution](01a_variables_and_evolution.md), then [Tutorial 2 — Local Pools](02_local_pools.md), where you store and recall entries across Filesystem, Redis, and HDF5 backends.
