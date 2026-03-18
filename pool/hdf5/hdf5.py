from __future__ import annotations

from typing import Optional, Any, Iterable, Iterator
from pydantic import Field, PrivateAttr
from urllib.parse import quote, unquote
import os
import json
import h5py

from ..schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry.compdata.transformation import TransformationSequence
from ...entry import transformation_base64


class HDF5Pool(_LAILA_IDENTIFIABLE_POOL):
    # Config / metadata
    file_path: Optional[str] = Field(default=None)
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)

    _file: Any = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

    def _resolve_memory_global_id(self) -> str:
        from ... import active_policy
        return active_policy.central.memory.global_id

    def model_post_init(self, __context: Any) -> None:
        if self.file_path is None:
            memory_global_id = self._resolve_memory_global_id()
            base_dir = os.path.expanduser("~/.laila/pools")
            pool_dir = os.path.join(base_dir, memory_global_id)
            os.makedirs(pool_dir, exist_ok=True)
            self.file_path = os.path.join(pool_dir, f"{self.pool_id}.h5py")
        else:
            os.makedirs(os.path.dirname(os.path.expanduser(self.file_path)), exist_ok=True)
            self.file_path = os.path.expanduser(self.file_path)

        # Open file and keep it open for the lifetime of the pool.
        self._file = h5py.File(self.file_path, "a")

    def _root(self):
        if self._file is None:
            raise RuntimeError("HDF5Pool is closed.")
        return self._file

    # ---------------- internal helpers ----------------
    def _storage_key(self, key: str) -> str:
        # Avoid "/" collisions in HDF5 path semantics.
        return quote(key, safe="")

    def _logical_key(self, key: str) -> str:
        return unquote(key)

    def _read_raw(self, key: str) -> Optional[str]:
        skey = self._storage_key(key)
        root = self._root()
        if skey not in root:
            return None
        raw = root[skey][()]
        if isinstance(raw, bytes):
            return raw.decode("utf-8")
        return str(raw)

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    # ---------------- mapping API ----------------
    def __getitem__(self, key: str) -> Optional[Any]:
        with self.atomic():
            raw = self._read_raw(key)
        return json.loads(raw) if raw is not None else None

    def __setitem__(self, key: str, entry: Any) -> None:
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("HDF5Pool expects a serialized JSON string.")

        skey = self._storage_key(key)
        with self.atomic():
            root = self._root()
            if skey in root:
                del root[skey]
            dt = h5py.string_dtype(encoding="utf-8")
            root.create_dataset(skey, data=value, dtype=dt)

    def __delitem__(self, key: str) -> None:
        skey = self._storage_key(key)
        with self.atomic():
            root = self._root()
            if skey in root:
                del root[skey]

    def empty(self) -> None:
        """Remove all entries from the pool."""
        if self._file is None:
            raise RuntimeError("HDF5Pool is closed.")
        with self.atomic():
            for name in list(self._file.keys()):
                del self._file[name]

    def exists(self, key: str) -> bool:
        skey = self._storage_key(key)
        with self.atomic():
            return skey in self._root()

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def keys(self, as_generator: bool = False) -> Iterable[str]:
        if not as_generator:
            with self.atomic():
                skeys = list(self._root().keys())
                return [self._logical_key(k) for k in skeys]

        def _gen() -> Iterator[str]:
            with self.atomic():
                for k in self._root().keys():
                    yield self._logical_key(k)

        return _gen()
