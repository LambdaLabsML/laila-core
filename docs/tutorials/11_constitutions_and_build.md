# Tutorial 11: Lazy Entries with Constitutions

A LAILA entry can carry a **constitution** instead of a concrete payload. The constitution describes *how* to compute the payload, and the entry starts in `STAGED` state. Calling `laila.build(entry)` runs the recipe on a taskforce and flips the entry to `READY`.

LAILA ships two constitution flavours:

- **`SimpleConstitution`** — an ordered chain of single-callable code strings; primarily the inverse of a serializer pipeline.
- **`ComplexConstitution`** — a single Python source string defining `f(manifest) -> payload`, bound to a `Manifest` of input entries.

## Prerequisites

```bash
pip install laila-core
```

## A complex constitution

The recipe is a Python source string that defines exactly one callable taking a `Manifest` and returning the payload. The manifest's leaves are themselves entries — at build time, LAILA resolves every leaf and hands the materialized manifest to your function:

```python
import laila
from laila.entry import Entry
from laila.policy.central.memory.schema.manifest import Manifest
from laila.macros.defaults import DefaultPool

laila.memory.extend(DefaultPool(), pool_nickname="constitutions")

a = laila.constant(data=10)
b = laila.constant(data=32)
manifest = Manifest(data={"a": a, "b": b})
manifest.memorize().wait()

CONSTITUTION_SRC = (
    "def sum_entries(manifest):\n"
    "    d = manifest.realized\n"
    "    return d['a'].data + d['b'].data\n"
)

entry = Entry.variable(constitution=CONSTITUTION_SRC, manifest=manifest)
print(entry.state)
# EntryState.STAGED
```

## `entry.data` raises until built

Reading `.data` on a staged entry is an explicit error — there is no implicit lazy build, you must call `laila.build` yourself:

```python
from laila.entry.exceptions import EntryNotBuiltError

try:
    entry.data
except EntryNotBuiltError as e:
    print("caught EntryNotBuiltError")
```

## `laila.build` materializes the entry

`laila.build(entry)` returns a future. After it resolves, the entry's `state` is `READY`, the constitution is cleared, and `entry.data` is the computed payload:

```python
future = laila.build(entry)
future.wait()

print(entry.state)        # EntryState.READY
print(entry.constitution) # None
print(entry.data)         # 42
```

The future is awaitable, too. From an `async def`:

```python
async def build_async():
    e2 = Entry.variable(constitution=CONSTITUTION_SRC, manifest=manifest)
    await laila.build(e2)
    return e2.data
```

## SimpleConstitution — inverse serialization chains

A `SimpleConstitution` carries an ordered list of source strings. `build(payload_input)` threads the input through each callable left-to-right. This is the same machinery LAILA uses internally to invert a pool's transformation sequence on read:

```python
import base64, zlib, msgpack
from laila.entry.constitution import SimpleConstitution

decode_chain = SimpleConstitution(codes=[
    "def b64(x): import base64; return base64.b64decode(x)",
    "def unz(x): import zlib; return zlib.decompress(x)",
    "def unp(x): import msgpack; return msgpack.unpackb(x)",
])

original = {"hello": "world", "n": 42}
encoded = base64.b64encode(zlib.compress(msgpack.packb(original)))
print(decode_chain.build(payload_input=encoded))
# {'hello': 'world', 'n': 42}
```

## Persisting a constitution-driven entry

A `STAGED` entry cannot be memorized — build it first, then memorize the materialized result. The recovered entry is a plain `READY` entry with the concrete payload.

## When to reach for constitutions

| Pattern | Use a constitution? |
|---|---|
| Caching the output of an expensive function | Yes — the recipe is portable; recompute or reload as needed. |
| Composing a derived dataset from inputs in different pools | Yes — the manifest names the inputs by gid, so the build runs anywhere. |
| Plain data with no derivation | No — use `laila.constant` / `laila.variable` directly. |

## Summary

- `Entry.variable(constitution=src, manifest=m)` produces a `STAGED` entry.
- `laila.build(entry)` runs the recipe; both `.wait()` (sync) and `await` (async) work.
- Once built, the constitution is cleared and the entry is a plain `READY` entry.
- `SimpleConstitution` is for ordered transform chains; `ComplexConstitution` is for arbitrary derivations driven by a manifest.

Next: [Tutorial 12 — Routing CPU work to a Process Pool](12_process_pool_taskforce.md).
