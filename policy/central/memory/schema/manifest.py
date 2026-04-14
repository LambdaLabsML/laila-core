"""Manifest — a structured map of user-defined keys to ``global_id`` references.

A Manifest is a special type of Entry whose payload is a blueprint: a nested
dictionary whose leaves are ``global_id`` strings (or lists thereof).  It
provides ``memorize``, ``remember``, and ``forget`` operations that
batch-process the referenced entries through the active policy's memory layer,
each returning a ``GroupFuture``.
"""

from __future__ import annotations

import copy
from typing import Any, Iterator, Optional

from pydantic import PrivateAttr

from .....entry import Entry
from .....entry.entry_state import EntryState
from .....entry.compdata import ComputationalData
from .....macros.strings import _MANIFEST_SCOPE


class Manifest(Entry):
    """Entry subclass wrapping a nested dict of ``global_id`` references.

    A manifest maps user-defined string keys to ``global_id`` strings, lists
    of ``global_id`` strings, or recursively nested dicts following the same
    rules.  It can be constructed from raw ID strings or from ``Entry``
    objects.

    The manifest's ``.data`` IS the blueprint dict.  It carries scope
    ``MANIFEST`` and evolution ``None`` (constant).

    Parameters
    ----------
    data : dict, optional
        A nested dict whose leaves are ``global_id`` strings, lists of
        ``global_id`` strings, or ``Entry`` instances.  Entry instances are
        converted to a blueprint of ``global_id`` strings and stashed for
        a subsequent ``memorize()`` call.
    uuid : str, optional
        Explicit UUID for the manifest's own identity.
    nickname : str, optional
        Human-readable name converted to a deterministic UUID.
    global_id : str, optional
        Composite identifier used to set identity.
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: [_MANIFEST_SCOPE])
    _pending_entries: Optional[list] = PrivateAttr(default=None)

    def __init__(self, **data: Any):
        raw_data = data.pop("data", None)

        blueprint = None
        pending = None

        if raw_data is not None:
            has_entries, has_strings = Manifest._classify_data(raw_data)

            if has_entries and has_strings:
                raise ValueError(
                    "Manifest data must contain either all Entry objects or all "
                    "global_id strings, not a mix of both."
                )

            if has_entries:
                blueprint = Manifest._extract_blueprint(raw_data)
                pending = list(Manifest._iter_entries(raw_data))
            else:
                Manifest._validate_blueprint(raw_data)
                blueprint = copy.deepcopy(raw_data)

        entry_kwargs = dict(data)
        entry_kwargs["evolution"] = None
        if blueprint is not None:
            entry_kwargs["data"] = blueprint
            entry_kwargs["state"] = EntryState.READY

        Entry.__init__(self, **entry_kwargs)
        self._pending_entries = pending

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def blueprint(self) -> Optional[dict]:
        """The nested dict with ``global_id`` strings as leaf values."""
        return self.data

    @property
    def resolved(self) -> dict:
        """Synchronously fetch all referenced entries through central memory.

        Walks the blueprint, recalls each entry via the memory layer's
        ``remember`` path (routing, deserialization, etc.), and returns a
        nested dict of ``Entry`` objects mirroring the blueprint structure.
        No caching — each access re-fetches.

        Raises
        ------
        RuntimeError
            If the manifest has no blueprint.
        KeyError
            If any referenced entry is missing from the routed pool.
        """
        import laila

        if self.data is None:
            raise RuntimeError("No blueprint to resolve — manifest is empty.")

        all_gids = list(self)
        if not all_gids:
            return Manifest._rebuild_with_entries(self.data, {})

        memory = laila.get_active_policy().central.memory
        ref = memory.remember(entry_ids=all_gids)
        results = ref.wait(None)
        if not isinstance(results, list):
            results = [results]

        resolved_map = dict(zip(all_gids, results))
        return Manifest._rebuild_with_entries(self.data, resolved_map)

    # ------------------------------------------------------------------
    # Core operations (all return GroupFuture)
    # ------------------------------------------------------------------

    def memorize(
        self,
        *,
        pool_nickname: Optional[str] = None,
        pool_id: Optional[str] = None,
        batch_size: int = 128,
    ):
        """Upload all referenced entries and store the manifest's blueprint.

        Collects any pending ``Entry`` objects provided at construction time,
        uploads them in batches, then stores the manifest itself (whose
        payload is the blueprint dict).

        Parameters
        ----------
        pool_nickname : str, optional
            Pool alias to route entries to.  Defaults to the alpha pool.
        pool_id : str, optional
            Explicit pool ``global_id``.  Defaults to the alpha pool.
        batch_size : int
            Maximum entries per upload batch.

        Returns
        -------
        GroupFuture
            Tracks all writes (leaf entries + manifest itself).
        """
        import laila
        from ...command.schema.future.future.group_future import GroupFuture

        if self.data is None:
            raise RuntimeError("Nothing to memorize — manifest has no blueprint.")

        pool_kwargs = Manifest._build_pool_kwargs(pool_nickname, pool_id)
        all_future_ids: list[str] = []
        policy = laila.get_active_policy()

        if self._pending_entries:
            for i in range(0, len(self._pending_entries), batch_size):
                batch = self._pending_entries[i : i + batch_size]
                ref = laila.memorize(batch, **pool_kwargs)
                all_future_ids.extend(Manifest._collect_future_ids(ref))
            self._pending_entries = None

        self_ref = laila.memorize(self, **pool_kwargs)
        all_future_ids.extend(Manifest._collect_future_ids(self_ref))

        return GroupFuture(
            taskforce_id=policy.central.command.alpha_taskforce,
            policy_id=policy.global_id,
            future_ids=all_future_ids,
        )

    def remember(
        self,
        *,
        pool_nickname: Optional[str] = None,
        pool_id: Optional[str] = None,
        batch_size: int = 128,
    ):
        """Recall all referenced entries from the pool.

        Collects every leaf ``global_id`` from the blueprint, fetches them
        in batches via ``laila.remember()``.

        Parameters
        ----------
        pool_nickname : str, optional
            Pool alias to read from.  Defaults to the alpha pool.
        pool_id : str, optional
            Explicit pool ``global_id``.  Defaults to the alpha pool.
        batch_size : int
            Maximum entries per recall batch.

        Returns
        -------
        GroupFuture
            Child futures resolve to the recalled ``Entry`` objects.
        """
        import laila
        from ...command.schema.future.future.group_future import GroupFuture

        if self.data is None:
            raise RuntimeError("No blueprint to resolve — manifest is empty.")

        pool_kwargs = Manifest._build_pool_kwargs(pool_nickname, pool_id)
        all_gids = list(self)
        all_future_ids: list[str] = []
        policy = laila.get_active_policy()

        for i in range(0, len(all_gids), batch_size):
            batch = all_gids[i : i + batch_size]
            ref = laila.remember(batch, **pool_kwargs)
            all_future_ids.extend(Manifest._collect_future_ids(ref))

        return GroupFuture(
            taskforce_id=policy.central.command.alpha_taskforce,
            policy_id=policy.global_id,
            future_ids=all_future_ids,
        )

    def forget(
        self,
        *,
        pool_nickname: Optional[str] = None,
        pool_id: Optional[str] = None,
        batch_size: int = 128,
    ):
        """Delete all referenced entries and the manifest itself from the pool.

        Collects every leaf ``global_id`` plus the manifest's own
        ``global_id``, deletes them in batches via ``laila.forget()``.

        Parameters
        ----------
        pool_nickname : str, optional
            Pool alias to delete from.  Defaults to the alpha pool.
        pool_id : str, optional
            Explicit pool ``global_id``.  Defaults to the alpha pool.
        batch_size : int
            Maximum entries per deletion batch.

        Returns
        -------
        GroupFuture
            Tracks all deletions (leaf entries + manifest itself).
        """
        import laila
        from ...command.schema.future.future.group_future import GroupFuture

        if self.data is None:
            raise RuntimeError("No blueprint — nothing to forget.")

        pool_kwargs = Manifest._build_pool_kwargs(pool_nickname, pool_id)
        all_gids = list(self)
        all_future_ids: list[str] = []
        policy = laila.get_active_policy()

        for i in range(0, len(all_gids), batch_size):
            batch = all_gids[i : i + batch_size]
            ref = laila.forget(batch, **pool_kwargs)
            all_future_ids.extend(Manifest._collect_future_ids(ref))

        self_ref = laila.forget(self.global_id, **pool_kwargs)
        all_future_ids.extend(Manifest._collect_future_ids(self_ref))

        return GroupFuture(
            taskforce_id=policy.central.command.alpha_taskforce,
            policy_id=policy.global_id,
            future_ids=all_future_ids,
        )

    # ------------------------------------------------------------------
    # Serialization override
    # ------------------------------------------------------------------

    def serialize(
        self,
        transformations=None,
        *,
        exclude_private=None,
    ):
        """Serialize the manifest, excluding transient ``_pending_entries``."""
        if exclude_private is None:
            exclude_private = {"_local_lock", "_payload", "_state", "_pending_entries"}
        return Entry.serialize(
            self, transformations, exclude_private=exclude_private
        )

    @classmethod
    def recover(cls, in_dict: dict, notify_on_creation=False):
        """Reconstruct a Manifest from a serialised dict or JSON string.

        Parameters
        ----------
        in_dict : dict or str or Manifest
            Serialised representation produced by ``serialize()``, a JSON
            string thereof, or an existing Manifest (returned as-is).
        notify_on_creation : bool, optional
            If ``True``, notify the active policy after construction.

        Returns
        -------
        Manifest
            The recovered Manifest instance.
        """
        import json

        if isinstance(in_dict, Manifest):
            return in_dict

        if isinstance(in_dict, str):
            try:
                in_dict = json.loads(in_dict)
            except Exception as e:
                raise ValueError("Invalid JSON string") from e

        if isinstance(in_dict, dict):
            payload = ComputationalData.recover(
                payload_blob=in_dict["transformed_payload"],
                recovery_sequence=in_dict["recovery_sequence"],
            )
            blueprint = payload.data if payload is not None else None
            return Manifest(
                data=blueprint,
                uuid=in_dict["_uuid"],
            )

        raise RuntimeError("Invalid input for manifest recovery.")

    # ------------------------------------------------------------------
    # Mapping-like API  (top-level blueprint keys for dict(manifest))
    # ------------------------------------------------------------------

    def keys(self):
        """Top-level blueprint keys."""
        if self.data is None:
            return {}.keys()
        return self.data.keys()

    def values(self):
        """Top-level blueprint values."""
        if self.data is None:
            return {}.values()
        return self.data.values()

    def items(self):
        """Top-level blueprint items."""
        if self.data is None:
            return {}.items()
        return self.data.items()

    def sub_manifest(self, keys: list[str]) -> "Manifest":
        """Return a new ``Manifest`` containing only the specified top-level keys.

        Parameters
        ----------
        keys : list[str]
            Top-level blueprint keys to include in the sub-manifest.

        Returns
        -------
        Manifest
            A new manifest whose blueprint is the subset of this manifest's
            blueprint restricted to *keys*.

        Raises
        ------
        RuntimeError
            If the manifest has no blueprint.
        KeyError
            If any key in *keys* is not present in the blueprint.
        """
        if self.data is None:
            raise RuntimeError("Cannot create sub-manifest — manifest is empty.")
        missing = [k for k in keys if k not in self.data]
        if missing:
            raise KeyError(f"Keys not found in manifest: {missing}")
        subset = {k: copy.deepcopy(self.data[k]) for k in keys}
        return Manifest(data=subset)

    def extend(self, other: "Manifest", *, overwrite: bool = False) -> None:
        """Merge another manifest's blueprint into this one in-place.

        Parameters
        ----------
        other : Manifest
            The manifest whose top-level keys will be added.
        overwrite : bool
            If ``False`` (default), duplicate top-level keys raise
            ``KeyError``.  If ``True``, *other*'s values silently replace
            existing ones (like ``dict.update``).

        Raises
        ------
        TypeError
            If *other* is not a ``Manifest``.
        KeyError
            If *overwrite* is ``False`` and the blueprints share keys.
        """
        if not isinstance(other, Manifest):
            raise TypeError(
                f"extend() requires a Manifest, got {type(other).__name__}"
            )
        if other.data is None:
            return

        if self.data is None:
            from .....entry.compdata.taxonomy.compdata import ComputationalData

            self._payload = ComputationalData(copy.deepcopy(other.data))
            self._state = EntryState.READY
        else:
            if not overwrite:
                overlap = set(self.data) & set(other.data)
                if overlap:
                    raise KeyError(
                        f"Duplicate top-level keys: {sorted(overlap)}"
                    )
            self.data.update(copy.deepcopy(other.data))

        if other._pending_entries:
            if self._pending_entries is None:
                self._pending_entries = list(other._pending_entries)
            else:
                self._pending_entries.extend(other._pending_entries)

    def __iadd__(self, other: Any) -> "Manifest":
        """``manifest += other`` — merge *other* in-place and return self."""
        if not isinstance(other, Manifest):
            return NotImplemented
        self.extend(other)
        return self

    def __add__(self, other: Any) -> "Manifest":
        """``manifest + other`` — return a new manifest with merged blueprints."""
        if not isinstance(other, Manifest):
            return NotImplemented

        if self.data is not None and other.data is not None:
            overlap = set(self.data) & set(other.data)
            if overlap:
                raise KeyError(
                    f"Duplicate top-level keys: {sorted(overlap)}"
                )

        merged: dict = {}
        if self.data is not None:
            merged.update(copy.deepcopy(self.data))
        if other.data is not None:
            merged.update(copy.deepcopy(other.data))

        if not merged:
            return Manifest()

        result = Manifest(data=merged)

        pending: list = []
        if self._pending_entries:
            pending.extend(self._pending_entries)
        if other._pending_entries:
            pending.extend(other._pending_entries)
        if pending:
            result._pending_entries = pending

        return result

    def __getitem__(self, key: str) -> Any:
        """Look up a top-level key in the blueprint."""
        if self.data is None:
            raise KeyError(key)
        return self.data[key]

    def __len__(self) -> int:
        """Number of top-level keys in the blueprint."""
        if self.data is None:
            return 0
        return len(self.data)

    # ------------------------------------------------------------------
    # Iteration / containment  (flattened global_id leaves)
    # ------------------------------------------------------------------

    def __iter__(self) -> Iterator[str]:
        """Yield every ``global_id`` string via a depth-first, insertion-order walk."""
        if self.data is None:
            return
        yield from Manifest._iter_global_ids(self.data)

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
    # Static / class helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _collect_future_ids(ref) -> list[str]:
        """Extract future IDs from a GroupFuture or a single future identity."""
        if ref is None:
            return []
        if hasattr(ref, "future_ids"):
            return list(ref.future_ids)
        return [ref.global_id]

    @staticmethod
    def _build_pool_kwargs(
        pool_nickname: Optional[str] = None,
        pool_id: Optional[str] = None,
    ) -> dict[str, str]:
        """Build keyword arguments for pool routing."""
        kwargs: dict[str, str] = {}
        if pool_nickname is not None:
            kwargs["pool_nickname"] = pool_nickname
        if pool_id is not None:
            kwargs["pool_id"] = pool_id
        return kwargs

    @staticmethod
    def _classify_data(data: dict) -> tuple[bool, bool]:
        """Return ``(has_entries, has_strings)`` for leaf values in *data*."""
        from .....entry import Entry as _Entry

        has_entries = False
        has_strings = False

        def _check(val: Any) -> None:
            nonlocal has_entries, has_strings
            if isinstance(val, _Entry):
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

        return has_entries, has_strings

    @staticmethod
    def _iter_entries(data: dict) -> Iterator:
        """Yield every ``Entry`` object from a nested dict."""
        from .....entry import Entry as _Entry

        for val in data.values():
            if isinstance(val, _Entry):
                yield val
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, _Entry):
                        yield item
            elif isinstance(val, dict):
                yield from Manifest._iter_entries(val)

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
        from .....entry import Entry as _Entry

        result: dict[str, Any] = {}
        for key, val in data.items():
            if isinstance(val, _Entry):
                result[key] = val.global_id
            elif isinstance(val, str):
                result[key] = val
            elif isinstance(val, list):
                result[key] = [
                    item.global_id if isinstance(item, _Entry) else item
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


from .....entry.constitution.recovery_maps import register_recovery
register_recovery(_MANIFEST_SCOPE, Manifest.recover)
