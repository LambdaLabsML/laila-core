# Tutorial 23: Building a Custom Pool Backend

LAILA's pool base class (`_LAILA_IDENTIFIABLE_POOL`) defines a small set of hooks. Override them, register the new class with a nickname, and `memorize` / `remember` / `forget` against it work just like every other backend.

## Prerequisites

```bash
pip install laila-core
```

## The contract

Synchronous hooks every backend should override:

| Hook | Purpose |
|---|---|
| `_read(self, key)` | Return the stored blob for `key`, or `None`. |
| `_write(self, key, value)` | Persist `value` under `key`. |
| `_delete(self, key)` | Remove `key`. Missing keys silently no-op. |
| `_exists(self, key)` | Return `True` iff `key` is stored locally. |
| `_keys(self, as_generator=False)` | Enumerate keys. |
| `_empty(self)` | Wipe local storage. |

Optional async hooks (default to wrapping the sync versions): `_read_async`, `_write_async`, `_delete_async`, `_exists_async`.

## A toy dict-backed pool

```python
import asyncio
from typing import Optional, Any, Iterable

from pydantic import PrivateAttr

import laila
from laila.pool.schema.base import _LAILA_IDENTIFIABLE_POOL

class DictPool(_LAILA_IDENTIFIABLE_POOL):
    """A toy pool backed by a private dict."""

    _store: dict = PrivateAttr(default_factory=dict)

    def _read(self, key: str) -> Optional[Any]:
        with self.atomic():
            return self._store.get(key)

    def _write(self, key: str, value: Any) -> None:
        with self.atomic():
            self._store[key] = value

    def _delete(self, key: str) -> None:
        with self.atomic():
            self._store.pop(key, None)

    def _exists(self, key: str) -> bool:
        with self.atomic():
            return key in self._store

    def _keys(self, as_generator: bool = False) -> Iterable[str]:
        with self.atomic():
            return list(self._store.keys())

    def _empty(self) -> None:
        with self.atomic():
            self._store.clear()

    async def _write_async(self, key: str, value: Any) -> None:
        await asyncio.sleep(0)
        self._write(key, value)
```

## Register and round-trip

```python
import numpy as np
import torch

pool = DictPool(nickname="dict_pool")
laila.memory.extend(pool, pool_nickname="dict_pool")

entries = [
    laila.constant(data=np.arange(8), nickname="cp_array"),
    laila.constant(data=torch.randn(2, 3), nickname="cp_tensor"),
    laila.constant(data={"label": "demo", "score": 0.95}, nickname="cp_dict"),
]
for e in entries:
    laila.memorize(e, pool_nickname="dict_pool").wait()
```

Recall and confirm types are preserved:

```python
for nick in ["cp_array", "cp_tensor", "cp_dict"]:
    r = laila.remember(nickname=nick, pool_nickname="dict_pool", persist=False).wait()
    print(nick, "->", type(r.data).__name__)
```

## Async paths

Default `_read_async` / `_write_async` / `_delete_async` just call the sync hook on the calling thread. That's correct but blocks the loop for the duration of the call. The override above uses `asyncio.sleep(0)` as a placeholder — replace it with a real `await` against an async client (`aioboto3`, `asyncpg`, `motor`, etc.) for true non-blocking I/O.

## Packaging as a separate distribution

A custom pool can ship in its own pip package — there is no plug-in registry. Users `pip install my-pool-pkg`, `from my_pool import MyPool`, and pass it to `laila.memory.extend(...)`. The `transformations` field is inherited automatically, so callers can wrap any custom backend in encryption or compression without touching your class.

## Summary

- Six sync hooks (`_read`, `_write`, `_delete`, `_exists`, `_keys`, `_empty`) cover the local API.
- Four async hooks (`_read_async`, `_write_async`, `_delete_async`, `_exists_async`) default to the sync versions; override them for genuine non-blocking I/O.
- Custom pools work with the routing, manifest, and proxy machinery out of the box.

You've reached the end of the Advanced track. Head back to [Tutorial 1 — Entries and Identity](01_entries_and_identity.md) for a refresher or browse the [API reference](../reference/laila/index.md) for the full surface area.
