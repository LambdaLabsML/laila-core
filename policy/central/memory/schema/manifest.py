"""Manifest — a structured map of user-defined keys to ``global_id`` references.

A Manifest is an ``Entry`` whose payload is a *blueprint*: a nested
dictionary whose leaves are ``global_id`` strings (or lists thereof).
A manifest is therefore a normal `READY` entry — its payload is the
blueprint dict itself.  The ``manifest.realized`` property batch-fetches
every referenced entry through the active policy's central memory and
returns a nested dict of ``Entry`` objects mirroring the blueprint.
"""

from __future__ import annotations

import copy
from collections.abc import Iterator
from typing import Any

from pydantic import PrivateAttr

from .....entry import Entry
from .....entry.compdata import ComputationalData
from .....entry.entry_state import EntryState
from .....macros.strings import _MANIFEST_SCOPE


class Manifest(Entry):
    """Entry subclass wrapping a nested dict of ``global_id`` references.

    A manifest maps user-defined string keys to ``global_id`` strings, lists
    of ``global_id`` strings, or recursively nested dicts following the same
    rules.  It can be constructed from raw ID strings or from ``Entry``
    objects.

    The blueprint *is* the manifest's payload, so ``manifest.data`` returns
    the nested mapping of ``global_id`` strings.  ``manifest.realized``
    synchronously fetches every referenced entry via the active policy's
    central memory and returns a nested dict of ``Entry`` objects mirroring
    the blueprint structure.

    Parameters
    ----------
    data : dict, optional
        A nested dict whose leaves are ``global_id`` strings, lists of
        ``global_id`` strings, or ``Entry`` instances.  Entry instances are
        converted to a blueprint of ``global_id`` strings and stashed for
        a subsequent ``memorize()`` call.
    blueprint : dict, optional
        Alias of ``data`` for explicitness.
    uuid : str, optional
        Explicit UUID for the manifest's own identity.
    nickname : str, optional
        Human-readable name converted to a deterministic UUID.
    global_id : str, optional
        Composite identifier used to set identity.
    """

    _scopes: list[str] = PrivateAttr(default_factory=lambda: [_MANIFEST_SCOPE])
    _pending_entries: list | None = PrivateAttr(default=None)

    def __init__(self, **data: Any):
        raw_data = data.pop("data", None)
        blueprint_kwarg = data.pop("blueprint", None)

        blueprint: dict | None = None
        pending = None

        source = raw_data if raw_data is not None else blueprint_kwarg

        if source is not None:
            has_entries, has_strings = Manifest._classify_data(source)

            if has_entries and has_strings:
                raise ValueError(
                    "Manifest data must contain either all Entry objects or all "
                    "global_id strings, not a mix of both."
                )

            if has_entries:
                blueprint = Manifest._extract_blueprint(source)
                pending = list(Manifest._iter_entries(source))
            else:
                Manifest._validate_blueprint(source)
                blueprint = copy.deepcopy(source)

        entry_kwargs = dict(data)
        entry_kwargs["evolution"] = None
        if blueprint is not None:
            entry_kwargs["data"] = blueprint
            entry_kwargs.setdefault("state", EntryState.READY)

        Entry.__init__(self, **entry_kwargs)
        self._pending_entries = pending

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def blueprint(self) -> dict | None:
        """The nested dict with ``global_id`` strings as leaf values.

        Equivalent to ``manifest.data``.
        """
        return self.data

    @property
    def realized(self):
        """Synchronously fetch all referenced entries and return them.

        Blocks until every leaf ``global_id`` has been materialised via the
        active policy's central memory. Returns a nested dict of
        ``Entry`` objects mirroring the blueprint structure. No caching —
        each access re-fetches.

        Raises
        ------
        RuntimeError
            If the manifest has no blueprint.
        KeyError
            If any referenced entry is missing from the routed pool.
        """
        import laila

        bp = self.data
        if bp is None:
            raise RuntimeError("No blueprint to resolve — manifest is empty.")

        all_gids = list(self)
        if not all_gids:
            return Manifest._rebuild_with_entries(bp, {})

        memory = laila.get_active_policy().central.memory
        ref = memory.remember(entry_ids=all_gids)
        results = ref.wait(None)
        if not isinstance(results, list):
            results = [results]

        if len(results) != len(all_gids) or any(r is None for r in results):
            raise KeyError("Manifest.realized: one or more referenced entries failed to resolve.")

        resolved_map = dict(zip(all_gids, results))
        return Manifest._rebuild_with_entries(bp, resolved_map)

    @property
    def async_realized(self):
        """Awaitable that asynchronously resolves all referenced entries.

        Returns a coroutine so callers can ``await manifest.async_realized``
        inside an async context (e.g. ``async with laila.guarantee_async``).
        Mirrors the semantics of ``realized`` but awaits the underlying
        ``remember`` future instead of blocking.

        Raises
        ------
        RuntimeError
            If the manifest has no blueprint.
        KeyError
            If any referenced entry is missing from the routed pool.
        """

        async def _resolve():
            import laila

            bp = self.data
            if bp is None:
                raise RuntimeError("No blueprint to resolve — manifest is empty.")

            all_gids = list(self)
            if not all_gids:
                return Manifest._rebuild_with_entries(bp, {})

            memory = laila.get_active_policy().central.memory
            ref = memory.remember(entry_ids=all_gids)
            results = await ref
            if not isinstance(results, list):
                results = [results]

            if len(results) != len(all_gids) or any(r is None for r in results):
                raise KeyError(
                    "Manifest.async_realized: one or more referenced entries failed to resolve."
                )

            resolved_map = dict(zip(all_gids, results))
            return Manifest._rebuild_with_entries(bp, resolved_map)

        return _resolve()

    # ------------------------------------------------------------------
    # Core operations (all return GroupFuture)
    # ------------------------------------------------------------------

    def memorize(
        self,
        *,
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        batch_size: int = 128,
    ):
        """Upload all referenced entries and store the manifest itself.

        Collects any pending ``Entry`` objects provided at construction time,
        uploads them in batches, then stores the manifest itself (whose
        payload is the blueprint dict).
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
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        batch_size: int = 128,
    ):
        """Recall all referenced entries from the pool."""
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
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        batch_size: int = 128,
    ):
        """Delete all referenced entries and the manifest itself from the pool."""
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
    # Mapping-like API  (top-level blueprint keys for dict(manifest))
    # ------------------------------------------------------------------

    def keys(self):
        """Top-level blueprint keys."""
        bp = self.data
        if bp is None:
            return {}.keys()
        return bp.keys()

    def values(self):
        """Top-level blueprint values."""
        bp = self.data
        if bp is None:
            return {}.values()
        return bp.values()

    def items(self):
        """Top-level blueprint items."""
        bp = self.data
        if bp is None:
            return {}.items()
        return bp.items()

    def sub_manifest(self, keys: list[str]) -> Manifest:
        """Return a new ``Manifest`` containing only the specified top-level keys."""
        bp = self.data
        if bp is None:
            raise RuntimeError("Cannot create sub-manifest — manifest is empty.")
        missing = [k for k in keys if k not in bp]
        if missing:
            raise KeyError(f"Keys not found in manifest: {missing}")
        subset = {k: copy.deepcopy(bp[k]) for k in keys}
        return Manifest(blueprint=subset)

    def extend(self, other: Manifest, *, overwrite: bool = False) -> None:
        """Merge another manifest's blueprint into this one in-place."""
        if not isinstance(other, Manifest):
            raise TypeError(f"extend() requires a Manifest, got {type(other).__name__}")
        other_bp = other.data
        if other_bp is None:
            return

        bp = self.data
        if bp is None:
            new_bp = copy.deepcopy(other_bp)
        else:
            if not overwrite:
                overlap = set(bp) & set(other_bp)
                if overlap:
                    raise KeyError(f"Duplicate top-level keys: {sorted(overlap)}")
            new_bp = dict(bp)
            new_bp.update(copy.deepcopy(other_bp))

        self._payload = ComputationalData(new_bp)

        if other._pending_entries:
            if self._pending_entries is None:
                self._pending_entries = list(other._pending_entries)
            else:
                self._pending_entries.extend(other._pending_entries)

    def __iadd__(self, other: Any) -> Manifest:
        """``manifest += other`` — merge *other* in-place and return self."""
        if not isinstance(other, Manifest):
            return NotImplemented
        self.extend(other)
        return self

    def __add__(self, other: Any) -> Manifest:
        """``manifest + other`` — return a new manifest with merged blueprints."""
        if not isinstance(other, Manifest):
            return NotImplemented

        bp = self.data
        other_bp = other.data
        if bp is not None and other_bp is not None:
            overlap = set(bp) & set(other_bp)
            if overlap:
                raise KeyError(f"Duplicate top-level keys: {sorted(overlap)}")

        merged: dict = {}
        if bp is not None:
            merged.update(copy.deepcopy(bp))
        if other_bp is not None:
            merged.update(copy.deepcopy(other_bp))

        if not merged:
            return Manifest()

        result = Manifest(blueprint=merged)

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
        bp = self.data
        if bp is None:
            raise KeyError(key)
        return bp[key]

    def __len__(self) -> int:
        """Number of top-level keys in the blueprint."""
        bp = self.data
        if bp is None:
            return 0
        return len(bp)

    # ------------------------------------------------------------------
    # Iteration / containment  (flattened global_id leaves)
    # ------------------------------------------------------------------

    def __iter__(self) -> Iterator[str]:
        """Yield every ``global_id`` string via a depth-first, insertion-order walk."""
        bp = self.data
        if bp is None:
            return
        yield from Manifest._iter_global_ids(bp)

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
        pool_nickname: str | None = None,
        pool_id: str | None = None,
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
        """Recursively validate that *data* follows the blueprint schema."""
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
                result[key] = [item.global_id if isinstance(item, _Entry) else item for item in val]
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


from .....entry.constitution.build_maps import register_builder

register_builder(
    _MANIFEST_SCOPE,
    Manifest._build_from_dict_sync,
    Manifest._build_from_dict_async,
)
