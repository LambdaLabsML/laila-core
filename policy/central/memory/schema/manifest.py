"""Manifest — a structured map of user-defined keys to ``global_id`` references.

A Manifest wraps a nested dictionary whose leaves are ``global_id`` strings
(or lists thereof).  It provides ``memorize``, ``remember``, and ``forget``
operations that batch-process the referenced entries through the active
policy's memory layer.
"""

from __future__ import annotations

import copy
from typing import Any, Iterator, Optional

from pydantic import PrivateAttr

from .....atomic.definitions.locally_atomic_identifiable_object import (
    _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT,
)
from .....basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .....macros.strings import _MANIFEST_SCOPE, _ENTRY_SCOPE


class Manifest(_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT):
    """Identifiable, thread-safe wrapper around a nested dict of ``global_id`` references.

    A manifest maps user-defined string keys to ``global_id`` strings, lists
    of ``global_id`` strings, or recursively nested dicts following the same
    rules.  It can be constructed from raw ID strings, from ``Entry`` objects,
    or by recalling a previously stored manifest from a pool.

    Parameters
    ----------
    data : dict, optional
        A nested dict whose leaves are ``global_id`` strings, lists of
        ``global_id`` strings, or ``Entry`` instances.
    global_id : str, optional
        Reconstruct the manifest by recalling its blueprint from a pool.
    pool_nickname : str, optional
        Pool alias used when constructing from *global_id*.
    pool_id : str, optional
        Explicit pool id used when constructing from *global_id*.
    uuid : str, optional
        Explicit UUID for the manifest's own identity.
    nickname : str, optional
        Human-readable name converted to a deterministic UUID.
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: [_MANIFEST_SCOPE])
    _blueprint: Optional[dict] = PrivateAttr(default=None)
    _resolved: Optional[dict] = PrivateAttr(default=None)
    _pool_nickname: Optional[str] = PrivateAttr(default=None)
    _pool_id: Optional[str] = PrivateAttr(default=None)

    def __init__(self, **data: Any):
        global_id = data.pop("global_id", None)
        pool_nickname = data.pop("pool_nickname", None)
        pool_id = data.pop("pool_id", None)
        raw_data = data.pop("data", None)
        nickname = data.pop("nickname", None)
        uuid_val = data.pop("uuid", None)

        identity_kwargs: dict[str, Any] = {}
        if nickname is not None:
            identity_kwargs["nickname"] = nickname
        elif uuid_val is not None:
            identity_kwargs["uuid"] = uuid_val

        if global_id is not None:
            identity_kwargs = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)

        _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT.__init__(self, **identity_kwargs)

        self._pool_nickname = pool_nickname
        self._pool_id = pool_id

        if global_id is not None and raw_data is None:
            self._recall_own_blueprint(pool_nickname=pool_nickname, pool_id=pool_id)
        elif raw_data is not None:
            self._ingest(raw_data)

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    def _recall_own_blueprint(
        self,
        *,
        pool_nickname: Optional[str] = None,
        pool_id: Optional[str] = None,
    ) -> None:
        """Recall the manifest's own blueprint dict from a pool."""
        import laila

        manifest_entry_gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id(
            uuid=self._uuid,
            scopes=[_ENTRY_SCOPE],
        )

        kwargs: dict[str, Any] = {}
        if pool_nickname is not None:
            kwargs["pool_nickname"] = pool_nickname
        if pool_id is not None:
            kwargs["pool_id"] = pool_id

        with laila.guarantee:
            ref = laila.remember(manifest_entry_gid, **kwargs)
        recalled = ref.wait()

        if isinstance(recalled, list):
            recalled = recalled[0]

        self._blueprint = recalled.data
        Manifest._validate_blueprint(self._blueprint)

    def _ingest(self, data: dict) -> None:
        """Detect whether *data* contains Entry objects or raw global_id strings."""
        from .....entry import Entry

        has_entries = False
        has_strings = False

        def _check(val: Any) -> None:
            nonlocal has_entries, has_strings
            if isinstance(val, Entry):
                has_entries = True
            elif isinstance(val, str):
                has_strings = True
            elif isinstance(val, list):
                for item in val:
                    _check(item)
            elif isinstance(val, dict):
                for v in val.values():
                    _check(v)

        for v in data.values():
            _check(v)

        if has_entries and has_strings:
            raise ValueError(
                "Manifest data must contain either all Entry objects or all "
                "global_id strings, not a mix of both."
            )

        if has_entries:
            self._resolved = copy.deepcopy(data)
            self._blueprint = Manifest._extract_blueprint(data)
        else:
            Manifest._validate_blueprint(data)
            self._blueprint = copy.deepcopy(data)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def blueprint(self) -> Optional[dict]:
        """The nested dict with ``global_id`` strings as leaf values."""
        return self._blueprint

    @property
    def resolved(self) -> Optional[dict]:
        """The nested dict with ``Entry`` objects as leaf values (after ``remember``)."""
        return self._resolved

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def memorize(
        self,
        *,
        pool_nickname: Optional[str] = None,
        pool_id: Optional[str] = None,
        batch_size: int = 128,
        memorize_self: bool = True,
    ) -> None:
        """Upload all referenced entries and store the manifest's own blueprint.

        Walks ``_resolved`` to collect all ``Entry`` objects, uploads them in
        batches via the active policy's memory, then builds ``_blueprint``
        from the resulting ``global_id`` strings.  When *memorize_self* is
        ``True``, also stores the blueprint dict itself as an entry so the
        manifest can be reconstructed from its ``global_id`` alone.

        Parameters
        ----------
        pool_nickname : str, optional
            Pool alias to route entries to.
        pool_id : str, optional
            Explicit pool ``global_id``.
        batch_size : int
            Maximum entries per upload batch.
        memorize_self : bool
            If ``True``, persist the blueprint dict as its own entry.
        """
        import laila
        from .....entry import Entry

        if self._resolved is None:
            raise RuntimeError(
                "Nothing to memorize — the manifest has no resolved entries. "
                "Construct with Entry objects or call remember() first."
            )

        all_entries = list(self._iter_resolved_entries())

        pool_kwargs = self._pool_kwargs(pool_nickname, pool_id)

        for i in range(0, len(all_entries), batch_size):
            batch = all_entries[i : i + batch_size]
            with laila.guarantee:
                laila.memorize(batch, **pool_kwargs)

        self._blueprint = Manifest._extract_blueprint(self._resolved)

        if memorize_self:
            self_entry = Entry.constant(
                data=copy.deepcopy(self._blueprint),
                uuid=self._uuid,
            )
            with laila.guarantee:
                laila.memorize(self_entry, **pool_kwargs)


    def remember(
        self,
        *,
        pool_nickname: Optional[str] = None,
        pool_id: Optional[str] = None,
        batch_size: int = 128,
    ) -> None:
        """Resolve all ``global_id`` strings in the blueprint to ``Entry`` objects.

        Collects every leaf ``global_id`` from ``_blueprint``, fetches them
        from the pool in batches, and populates ``_resolved`` with the
        recovered ``Entry`` instances in the same nested structure.

        Parameters
        ----------
        pool_nickname : str, optional
            Pool alias to read from.
        pool_id : str, optional
            Explicit pool ``global_id``.
        batch_size : int
            Maximum entries per recall batch.
        """
        import laila

        if self._blueprint is None:
            raise RuntimeError("No blueprint to resolve — manifest is empty.")

        all_gids = list(self)
        pool_kwargs = self._pool_kwargs(pool_nickname, pool_id)

        resolved_map: dict[str, Any] = {}

        for i in range(0, len(all_gids), batch_size):
            batch = all_gids[i : i + batch_size]
            with laila.guarantee:
                ref = laila.remember(batch, **pool_kwargs)
            if ref is not None:
                results = ref.wait()
                if not isinstance(results, list):
                    results = [results]
                for gid, entry in zip(batch, results):
                    resolved_map[gid] = entry

        self._resolved = Manifest._rebuild_with_entries(
            self._blueprint, resolved_map
        )

    def forget(
        self,
        *,
        pool_nickname: Optional[str] = None,
        pool_id: Optional[str] = None,
        batch_size: int = 128,
        forget_self: bool = True,
    ) -> None:
        """Delete all referenced entries (and optionally the manifest itself) from the pool.

        Parameters
        ----------
        pool_nickname : str, optional
            Pool alias to delete from.
        pool_id : str, optional
            Explicit pool ``global_id``.
        batch_size : int
            Maximum entries per deletion batch.
        forget_self : bool
            If ``True``, also delete the manifest's own stored blueprint entry.
        """
        import laila

        if self._blueprint is None:
            raise RuntimeError("No blueprint — nothing to forget.")

        all_gids = list(self)
        pool_kwargs = self._pool_kwargs(pool_nickname, pool_id)

        for i in range(0, len(all_gids), batch_size):
            batch = all_gids[i : i + batch_size]
            with laila.guarantee:
                laila.forget(batch, **pool_kwargs)

        if forget_self:
            self_entry_gid = _LAILA_IDENTIFIABLE_OBJECT.to_global_id(
                uuid=self._uuid,
                scopes=[_ENTRY_SCOPE],
            )
            with laila.guarantee:
                laila.forget(self_entry_gid, **pool_kwargs)

    # ------------------------------------------------------------------
    # Mapping-like API  (top-level blueprint keys for dict(manifest))
    # ------------------------------------------------------------------

    def keys(self):
        """Top-level blueprint keys."""
        if self._blueprint is None:
            return {}.keys()
        return self._blueprint.keys()

    def values(self):
        """Top-level blueprint values."""
        if self._blueprint is None:
            return {}.values()
        return self._blueprint.values()

    def items(self):
        """Top-level blueprint items."""
        if self._blueprint is None:
            return {}.items()
        return self._blueprint.items()

    def __getitem__(self, key: str) -> Any:
        """Look up a top-level key in the blueprint."""
        if self._blueprint is None:
            raise KeyError(key)
        return self._blueprint[key]

    def __len__(self) -> int:
        """Number of top-level keys in the blueprint."""
        if self._blueprint is None:
            return 0
        return len(self._blueprint)

    # ------------------------------------------------------------------
    # Iteration / containment  (flattened global_id leaves)
    # ------------------------------------------------------------------

    def __iter__(self) -> Iterator[str]:
        """Yield every ``global_id`` string via a depth-first, insertion-order walk."""
        if self._blueprint is None:
            return
        yield from Manifest._iter_global_ids(self._blueprint)

    def __contains__(self, global_id: object) -> bool:
        """Return ``True`` if *global_id* appears anywhere in the blueprint leaves."""
        if not isinstance(global_id, str):
            return False
        for gid in self:
            if gid == global_id:
                return True
        return False

    # ------------------------------------------------------------------
    # String representation
    # ------------------------------------------------------------------

    def __str__(self) -> str:
        return self.global_id

    def __repr__(self) -> str:
        n = sum(1 for _ in self)
        return f"Manifest({self.global_id}, entries={n})"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _pool_kwargs(
        self,
        pool_nickname: Optional[str] = None,
        pool_id: Optional[str] = None,
    ) -> dict[str, str]:
        """Build keyword arguments for pool routing."""
        pn = pool_nickname or self._pool_nickname
        pid = pool_id or self._pool_id
        kwargs: dict[str, str] = {}
        if pn is not None:
            kwargs["pool_nickname"] = pn
        if pid is not None:
            kwargs["pool_id"] = pid
        return kwargs

    def _iter_resolved_entries(self) -> Iterator:
        """Yield every ``Entry`` object from ``_resolved``."""
        from .....entry import Entry

        def _walk(node: Any) -> Iterator:
            if isinstance(node, Entry):
                yield node
            elif isinstance(node, list):
                for item in node:
                    yield from _walk(item)
            elif isinstance(node, dict):
                for v in node.values():
                    yield from _walk(v)

        if self._resolved is not None:
            yield from _walk(self._resolved)

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_blueprint(data: Any) -> None:
        """Recursively validate that *data* follows the blueprint schema.

        Raises ``ValueError`` if any key is not a string or any leaf is not
        a ``global_id`` string (or list of them).
        """
        if not isinstance(data, dict):
            raise ValueError("Blueprint must be a dict.")
        for key, val in data.items():
            if not isinstance(key, str):
                raise ValueError(f"Blueprint keys must be strings, got {type(key)}")
            if isinstance(val, str):
                continue
            elif isinstance(val, list):
                for item in val:
                    if not isinstance(item, str):
                        raise ValueError(
                            f"List values in blueprint must be strings, got {type(item)}"
                        )
            elif isinstance(val, dict):
                Manifest._validate_blueprint(val)
            else:
                raise ValueError(
                    f"Blueprint values must be str, list[str], or dict — got {type(val)}"
                )

    @staticmethod
    def _extract_blueprint(data: dict) -> dict:
        """Convert a dict of ``Entry`` objects to a blueprint of ``global_id`` strings."""
        from .....entry import Entry

        result: dict[str, Any] = {}
        for key, val in data.items():
            if isinstance(val, Entry):
                result[key] = val.global_id
            elif isinstance(val, str):
                result[key] = val
            elif isinstance(val, list):
                result[key] = [
                    item.global_id if isinstance(item, Entry) else item
                    for item in val
                ]
            elif isinstance(val, dict):
                result[key] = Manifest._extract_blueprint(val)
            else:
                raise ValueError(f"Unexpected value type in manifest data: {type(val)}")
        return result

    @staticmethod
    def _iter_global_ids(blueprint: dict) -> Iterator[str]:
        """Depth-first, insertion-order walk yielding every leaf ``global_id``."""
        for val in blueprint.values():
            if isinstance(val, str):
                yield val
            elif isinstance(val, list):
                yield from val
            elif isinstance(val, dict):
                yield from Manifest._iter_global_ids(val)

    @staticmethod
    def _rebuild_with_entries(
        blueprint: dict,
        resolved_map: dict[str, Any],
    ) -> dict:
        """Reconstruct the nested structure replacing ``global_id`` strings with entries."""
        result: dict[str, Any] = {}
        for key, val in blueprint.items():
            if isinstance(val, str):
                result[key] = resolved_map[val]
            elif isinstance(val, list):
                result[key] = [resolved_map[gid] for gid in val]
            elif isinstance(val, dict):
                result[key] = Manifest._rebuild_with_entries(val, resolved_map)
        return result
