"""Per-pool evolution / creation-timestamp index.

A :class:`PoolIndex` answers, for one *base* global id
(``LAILA:ENTRY:<uuid>``, no ``@`` suffix), the questions
``laila.remember`` asks when a reference carries search attributes:

- which evolutions are stored (and whether a constant key exists),
  so ``@evolution=-1`` (latest) / ``-k`` resolve without listing keys;
- which evolution was created at a given ``creation_timestamp``, so
  ``@creation_timestamp=<iso>`` resolves without reading every record.

Layout
------
One **shard per base**. A shard is an evolvable :class:`Entry` with
scope ``POOL_INDEX`` and the deterministic id
``LAILA:POOL_INDEX:<uuid5("pool_index:<owner uuid>:<base>")>`` whose
payload is::

    {"base": base, "evolutions": [0, 1, 2], "constant": False,
     "creation_timestamps": {"2026-...": 2, ...}}

Shards live in ``owner.index_pool`` (default: the owner pool itself;
point it at an in-memory pool for cheap index writes). Only the latest
shard evolution is kept: after shard evolution N is written, N-1 is
deleted. Rewriting a shard is O(size of that base's history), not
O(size of the pool), and shards are loaded on demand.

Consistency model
-----------------
The index is a **validated cache, never an authority**. Every hit is
confirmed by the read that follows it in central memory; a miss
invalidates the base and falls back to the key scan. A failed shard
write logs, invalidates the base, and never fails the user's
``memorize``. :meth:`PoolIndex.rebuild` repopulates every shard from
the owner's keys.

Maintenance is **write-through**: the pool's ``write`` / ``delete``
wrappers call :meth:`record` / :meth:`remove`, which update the shard
in memory and persist it immediately, all under the owner's atomic
lock. Shard keys are never indexed themselves and are hidden from the
pool's public ``keys()``.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from ...basics.definitions.identifiable_object import (
    _LAILA_IDENTIFIABLE_OBJECT,
    EVOLUTION_ATTRIBUTE,
    split_global_id_attributes,
)
from ...macros.strings import _POOL_INDEX_SCOPE, _TOPMOST_SCOPE

_LOG = logging.getLogger(__name__)

_INDEX_KEY_PREFIX = f"{_TOPMOST_SCOPE}:{_POOL_INDEX_SCOPE}:"

# Name of the search attribute keyed by an entry's creation stamp.
CREATION_TIMESTAMP_ATTRIBUTE = "creation_timestamp"


def is_index_key(key: str) -> bool:
    """``True`` for storage keys that belong to an index shard."""
    return isinstance(key, str) and key.startswith(_INDEX_KEY_PREFIX)


def _record_creation_timestamp(raw: Any) -> str | None:
    """Pull the entry ``creation_timestamp`` out of a stored record without rebuilding it.

    *raw* is whatever the pool holds: a JSON string / bytes, a record
    dict whose ``entry`` is a serialized dict, or (in-memory pools with
    no transformations) a record dict whose ``entry`` is a live
    :class:`Entry`.
    """
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            return None
    if not isinstance(raw, dict):
        return getattr(raw, "creation_timestamp", None)
    entry = raw.get("entry", raw)
    if isinstance(entry, dict):
        return entry.get("_creation_timestamp")
    return getattr(entry, "creation_timestamp", None)


def _key_evolution(key: str) -> int | None:
    """Evolution encoded in a storage key; ``None`` for a constant (no ``@``)."""
    _, attrs = split_global_id_attributes(key)
    raw = attrs.get(EVOLUTION_ATTRIBUTE)
    if raw is None or not raw.isdigit():
        return None
    return int(raw)


def _evolution_key(base: str, evolution: int | None) -> str:
    return base if evolution is None else f"{base}@{EVOLUTION_ATTRIBUTE}={evolution}"


def _rank(evolution: int | None) -> int:
    """Sort rank: constants (no evolution) sort below every evolution."""
    return -1 if evolution is None else evolution


class PoolIndex:
    """Shard-per-base evolution / creation-timestamp index of one pool.

    Created lazily by :attr:`_LAILA_IDENTIFIABLE_POOL.index`. All
    methods take the owner's atomic lock (re-entrant), so callers may
    hold it already.

    Parameters
    ----------
    owner : _LAILA_IDENTIFIABLE_POOL
        The pool whose keys are indexed. ``owner.index_pool`` (or the
        owner itself) stores the shards.
    """

    def __init__(self, owner: Any):
        self._owner = owner
        self._shards: dict[str, dict[str, Any]] = {}
        self._entries: dict[str, Any] = {}  # base -> live shard Entry
        self._missing: set[str] = set()  # bases known to have no shard

    # ------------------------------------------------------------------
    # Identity helpers
    # ------------------------------------------------------------------
    @property
    def index_pool(self):
        """Pool that stores the shards (``owner.index_pool`` or the owner)."""
        return self._owner.index_pool or self._owner

    def shard_id(self, base: str) -> str:
        """Deterministic base id of the shard for *base* (no evolution attribute)."""
        return _LAILA_IDENTIFIABLE_OBJECT.to_global_id(
            nickname=f"pool_index:{self._owner.uuid}:{base}", scopes=[_POOL_INDEX_SCOPE]
        )

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    def candidates(self, base: str) -> list[str] | None:
        """Every stored key of *base*, or ``None`` when no shard exists."""
        shard = self._shard(base)
        if shard is None:
            return None
        keys = [base] if shard["constant"] else []
        keys.extend(_evolution_key(base, e) for e in shard["evolutions"])
        return keys

    def latest(self, base: str) -> str | None:
        """Key of the highest evolution (or the constant key), or ``None``."""
        return self.nth(base, -1)

    def nth(self, base: str, n: int) -> str | None:
        """Key of the *n*-th evolution of *base*.

        ``n >= 0`` is an exact evolution; ``n < 0`` counts from the end
        of the sorted evolutions (``-1`` = latest). A constant key is
        preferred for ``n < 0`` when it is the only thing stored, and
        counts as the lowest rank otherwise. ``None`` when out of range
        or unindexed.
        """
        shard = self._shard(base)
        if shard is None:
            return None
        if n >= 0:
            return _evolution_key(base, n) if n in shard["evolutions"] else None
        ranked: list[int | None] = ([None] if shard["constant"] else []) + list(shard["evolutions"])
        idx = len(ranked) + n
        if idx < 0 or idx >= len(ranked):
            return None
        return _evolution_key(base, ranked[idx])

    def by_creation_timestamp(
        self, base: str, timestamp: str, evolution: int | None = None
    ) -> str | None:
        """Key of the evolution created at *timestamp* (exact match).

        With *evolution* given, the match must also be that evolution
        (negative values count from the end, as in :meth:`nth`).
        ``None`` when unindexed or no such stamp.
        """
        shard = self._shard(base)
        if shard is None:
            return None
        if timestamp not in shard["creation_timestamps"]:
            return None
        found = shard["creation_timestamps"][timestamp]
        key = _evolution_key(base, found)
        if evolution is not None and self.nth(base, evolution) != key:
            return None
        return key

    # ------------------------------------------------------------------
    # Maintenance (write-through)
    # ------------------------------------------------------------------
    def record(self, key: str, value: Any) -> None:
        """Index a key that was just written with record *value*, then persist the shard."""
        if is_index_key(key):
            return
        base, _ = split_global_id_attributes(key)
        evolution = _key_evolution(key)
        stamp = _record_creation_timestamp(value)
        with self._owner.atomic():
            shard = self._shard(base, create=True)
            if evolution is None:
                shard["constant"] = True
            elif evolution not in shard["evolutions"]:
                shard["evolutions"].append(evolution)
                shard["evolutions"].sort()
            if stamp is not None:
                stamps = shard["creation_timestamps"]
                # An evolution re-written with a different stamp: drop the stale one.
                for old, evo in list(stamps.items()):
                    if evo == evolution and old != stamp:
                        del stamps[old]
                # Same-millisecond collision: keep the highest evolution.
                prev = stamps.get(stamp, "__absent__")
                if prev == "__absent__" or _rank(evolution) > _rank(prev):
                    stamps[stamp] = evolution
            self._flush(base)

    def remove(self, key: str) -> None:
        """Un-index a key that was just deleted, then persist (or drop) the shard."""
        if is_index_key(key):
            return
        base, _ = split_global_id_attributes(key)
        evolution = _key_evolution(key)
        with self._owner.atomic():
            shard = self._shard(base)
            if shard is None:
                return
            if evolution is None:
                shard["constant"] = False
            elif evolution in shard["evolutions"]:
                shard["evolutions"].remove(evolution)
            shard["creation_timestamps"] = {
                ts: evo for ts, evo in shard["creation_timestamps"].items() if evo != evolution
            }
            if not shard["constant"] and not shard["evolutions"]:
                self._drop_shard(base)
            else:
                self._flush(base)

    def invalidate(self, base: str | None = None) -> None:
        """Forget the in-memory state of *base* (or of every base).

        The next query reloads the shard from the index pool; central
        memory calls this when an index hit fails validation.
        """
        with self._owner.atomic():
            if base is None:
                self._shards.clear()
                self._entries.clear()
                self._missing.clear()
            else:
                self._shards.pop(base, None)
                self._entries.pop(base, None)
                self._missing.discard(base)

    def clear(self) -> None:
        """Delete every shard of the owner's current keys from the index pool.

        Used by ``pool.empty()``: shards are addressed per base, so the
        owner's keys tell us which shards exist.
        """
        with self._owner.atomic():
            bases = {
                split_global_id_attributes(k)[0] for k in self._owner._keys() if not is_index_key(k)
            }
            for base in bases:
                self._drop_shard(base)
            self.invalidate()

    def rebuild(self) -> None:
        """Rebuild every shard from the owner's keys and records.

        Reads each record once (for its creation stamp). Replaces the
        shard contents outright, so evolutions that no longer exist are
        dropped too.
        """
        with self._owner.atomic():
            by_base: dict[str, list[str]] = {}
            for key in self._owner._keys():
                if is_index_key(key):
                    continue
                by_base.setdefault(split_global_id_attributes(key)[0], []).append(key)
            for base, keys in by_base.items():
                self._shard(base, create=True)  # load (to keep the shard entry's counter)
                shard = self._shards[base]
                shard["evolutions"] = []
                shard["constant"] = False
                shard["creation_timestamps"] = {}
                for key in keys:
                    evolution = _key_evolution(key)
                    if evolution is None:
                        shard["constant"] = True
                    else:
                        shard["evolutions"].append(evolution)
                    stamp = _record_creation_timestamp(self._owner._read(key))
                    if stamp is not None:
                        prev = shard["creation_timestamps"].get(stamp, "__absent__")
                        if prev == "__absent__" or _rank(evolution) > _rank(prev):
                            shard["creation_timestamps"][stamp] = evolution
                shard["evolutions"].sort()
                self._flush(base)

    # ------------------------------------------------------------------
    # Shard storage
    # ------------------------------------------------------------------
    def _new_shard(self, base: str) -> dict[str, Any]:
        return {"base": base, "evolutions": [], "constant": False, "creation_timestamps": {}}

    def _shard(self, base: str, create: bool = False) -> dict[str, Any] | None:
        """Return the in-memory shard for *base*, loading it from the index pool on first use."""
        with self._owner.atomic():
            shard = self._shards.get(base)
            if shard is not None:
                return shard
            if base not in self._missing:
                loaded = self._load(base)
                if loaded is not None:
                    return loaded
                self._missing.add(base)
            if not create:
                return None
            self._missing.discard(base)
            shard = self._new_shard(base)
            self._shards[base] = shard
            return shard

    def _load(self, base: str) -> dict[str, Any] | None:
        """Load the latest persisted shard of *base*; delete straggler evolutions."""
        from ...policy.central.memory.record.record import Record

        sid = self.shard_id(base)
        try:
            pool = self.index_pool
            keys = sorted(pool._candidate_keys(sid), key=lambda k: _rank(_key_evolution(k)))
            if not keys:
                return None
            latest = keys[-1]
            raw = pool._read(latest)
            if raw is None:
                return None
            entry = Record._build_sync(raw)["entry"]
            data = entry.data
            if not isinstance(data, dict):
                return None
            shard = self._new_shard(base)
            shard["evolutions"] = sorted(int(e) for e in data.get("evolutions", []))
            shard["constant"] = bool(data.get("constant", False))
            shard["creation_timestamps"] = {
                str(ts): (None if evo is None else int(evo))
                for ts, evo in dict(data.get("creation_timestamps", {})).items()
            }
            self._shards[base] = shard
            self._entries[base] = entry
            for straggler in keys[:-1]:
                try:
                    pool._delete(straggler)
                except Exception:  # pragma: no cover - best effort cleanup
                    pass
            return shard
        except Exception as exc:  # index is a cache: never let it break the caller
            _LOG.warning("pool index: could not load shard for %s: %s", base, exc)
            return None

    def _flush(self, base: str) -> None:
        """Persist the in-memory shard of *base* as the next shard evolution."""
        from ...entry.entry import Entry
        from ...entry.entry_state import EntryState
        from ...policy.central.memory.record.record import Record

        shard = self._shards.get(base)
        if shard is None:
            return
        pool = self.index_pool
        try:
            # Copy so an in-memory index pool (which stores a snapshot sharing
            # the payload object) never aliases the live shard dict.
            payload = {
                "base": shard["base"],
                "evolutions": list(shard["evolutions"]),
                "constant": bool(shard["constant"]),
                "creation_timestamps": dict(shard["creation_timestamps"]),
            }
            entry = self._entries.get(base)
            if entry is None:
                # First shard evolution: constructed with its payload so it is
                # not "locally modified" and lands as ``@evolution=0``.
                entry = Entry.contingent(
                    uuid=_LAILA_IDENTIFIABLE_OBJECT.generate_uuid_from_nickname(
                        f"pool_index:{self._owner.uuid}:{base}"
                    ),
                    scopes=[_POOL_INDEX_SCOPE],
                    evolution=0,
                    data=payload,
                    state=EntryState.READY,
                )
                self._entries[base] = entry
                previous_key = None
            else:
                previous_key = entry.global_id
                entry.data = payload
            entry.bump_evolution_if_locally_modified()
            record = Record(entry=entry)
            blob = record.serialize(transformations=pool.transformations)
            pool._write(entry.global_id, blob)
            entry.mark_memorized()
            if previous_key is not None and previous_key != entry.global_id:
                pool._delete(previous_key)
        except Exception as exc:
            _LOG.warning("pool index: could not persist shard for %s: %s", base, exc)
            self.invalidate(base)

    def _drop_shard(self, base: str) -> None:
        """Delete every persisted evolution of the shard for *base* and forget it."""
        pool = self.index_pool
        try:
            for key in pool._candidate_keys(self.shard_id(base)):
                pool._delete(key)
        except Exception as exc:
            _LOG.warning("pool index: could not drop shard for %s: %s", base, exc)
        self._shards.pop(base, None)
        self._entries.pop(base, None)
        self._missing.add(base)
