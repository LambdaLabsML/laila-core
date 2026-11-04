"""Manifest — a structured map of user-defined keys to ``global_id`` references.

A Manifest is an ``Entry`` whose payload is a *blueprint*: a nested
dictionary whose leaves are ``global_id`` strings (or lists thereof).
A manifest is therefore a normal `READY` entry — its payload is the
blueprint dict itself.  The ``manifest.realized`` property batch-fetches
every referenced entry through the active policy's central memory and
returns a nested dict of ``Entry`` objects mirroring the blueprint. The
fetch uses the memory's direct-await resolver (``_read_entries_async`` /
``_read_entries_direct``): one task for the whole batch rather than one
future per child, released as soon as the entries have been collected.

A :class:`ComplexManifest` (scope ``MANIFEST:COMPLEX``) is a manifest
whose leaves may themselves be manifests -- a model checkpoint, an
optimizer checkpoint, a dataset manifest and a config entry under one
``"everything"`` nickname. It memorizes its members recursively and,
when it is read back from a pool, undergoes *recursive blueprint
resolution*: every leaf whose gid carries a ``MANIFEST`` scope is fetched
and cast back into its typed manifest, so ``everything["model"]`` is a
``ModuleManifest`` again. ``rebuild()`` then turns those typed manifests
into the live objects they were made from.
"""

from __future__ import annotations

import abc
import copy
import hashlib
import os
import re
import sqlite3
import threading
import uuid as _uuid
from collections.abc import Callable, Hashable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, ClassVar

from pydantic import PrivateAttr

from .....entry import Entry
from .....entry.compdata import ComputationalData
from .....entry.entry_state import EntryState
from .....macros.strings import _COMPLEX_MANIFEST_SCOPE, _MANIFEST_SCOPE

# ----------------------------------------------------------------------
# Non-memorizing algorithmic help: SQL index
# ----------------------------------------------------------------------
# The Manifest SQL index is a *non-memorizing algorithmic helper* — a
# lightweight, temporary, query-side convenience that a manifest owns
# directly while it is being operated on.  It deliberately bypasses
# ``central.memory`` (see ``agentic/internal/memory.md`` for the sanctioned
# exemption): it is never memorized, never registered with a pool
# router, and never travels with the manifest on the wire.  It is a
# plain ``sqlite3`` database file under ``<laila_root>/indices`` that can
# be cleared or invalidated at any time.

_PRIMITIVE_TYPES = (str, int, float, bool, bytes, type(None))
_FORBIDDEN_SQL = ("ORDER BY", "LIMIT", "GROUP BY", "HAVING", "JOIN", "UNION")
_SQL_INDEX_TABLE = "manifest"
_SQL_ROW_IDX_COL = "__row_idx__"
_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


_SQL_META_TABLE = "__laila_index_meta__"


class _SqlResult:
    """Eagerly fetched query result with the cursor methods the index uses."""

    __slots__ = ("_rows",)

    def __init__(self, rows: list) -> None:
        self._rows = rows

    def fetchall(self) -> list:
        rows, self._rows = self._rows, []
        return rows

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def __iter__(self):
        return iter(self.fetchall())


class _LockedConnection:
    """Serialize every use of one ``sqlite3`` connection behind an ``RLock``.

    The index connection is opened with ``check_same_thread=False`` so
    queries can come from any thread (taskforce workers included), but
    CPython's ``sqlite3`` cursors and statement cache are not safe for
    concurrent use: two threads running the same statement text could
    reset each other's cursor and read truncated / duplicated rows.
    Every statement here runs *and* fetches under the lock, so callers
    get a complete, consistent result regardless of thread.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self.lock = threading.RLock()

    def execute(self, sql: str, params: Iterable[Any] = ()) -> _SqlResult:
        with self.lock:
            cursor = self._conn.execute(sql, tuple(params))
            try:
                rows = cursor.fetchall()
            finally:
                cursor.close()
            return _SqlResult(rows)

    def executemany(self, sql: str, seq_of_params: Iterable[Iterable[Any]]) -> None:
        with self.lock:
            self._conn.executemany(sql, seq_of_params).close()

    def commit(self) -> None:
        with self.lock:
            self._conn.commit()

    def close(self) -> None:
        with self.lock:
            self._conn.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


@dataclass
class _SqlState:
    """In-memory handle for a manifest's non-memorizing SQL index.

    Holds the owned ``sqlite3`` connection plus the data-side metadata
    needed to map query results back to blueprint keys.  Stored on the
    manifest as a ``PrivateAttr`` so it never affects the manifest's
    on-the-wire serialization.
    """

    conn: _LockedConnection
    db_path: str
    is_persistent: bool
    table_name: str
    columns: list
    indexed: set
    row_keys: list
    stale: bool = False


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

    SQL filtering
    -------------
    ``manifest.sql("SELECT … FROM … WHERE …")`` filters the blueprint via
    a small sqlite-backed index built on demand.  This index is a
    *non-memorizing algorithmic helper*: a lightweight, temporary,
    query-side convenience that the manifest owns directly and that
    deliberately bypasses ``central.memory`` (it is never memorized,
    never routed, and never travels with the manifest).  Subclasses can
    customise it by overriding :meth:`_sql_rows` and :meth:`_sql_project`.

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

    _DEFAULT_SCOPES: ClassVar[list[str]] = [_MANIFEST_SCOPE]
    # ``True`` on manifests whose leaves belong to them (a checkpoint's
    # tensors): ``ComplexManifest.forget`` recurses into those; a plain
    # manifest's leaves (a dataset) are shared and only dropped as entries.
    _OWNS_CHILDREN: ClassVar[bool] = False
    _pending_entries: list | None = PrivateAttr(default=None)
    _nickname: str | None = PrivateAttr(default=None)
    _sql_state: _SqlState | None = PrivateAttr(default=None)
    _sql_finalizer: Any = PrivateAttr(default=None)

    def __init__(self, **data: Any):
        raw_data = data.pop("data", None)
        blueprint_kwarg = data.pop("blueprint", None)
        nickname = data.get("nickname")

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
        self._nickname = nickname if isinstance(nickname, str) else None

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def nickname(self) -> str | None:
        """The nickname this manifest was created with (``None`` when read back from a pool)."""
        return self._nickname

    @staticmethod
    def is_manifest_gid(gid: Any) -> bool:
        """``True`` when *gid* names a manifest of any kind (``LAILA:MANIFEST[:...]:<uuid>``).

        Decided from the string alone -- the scopes are part of the gid
        -- so classifying a blueprint leaf costs no I/O.
        """
        if not isinstance(gid, str):
            return False
        from .....basics.definitions.identifiable_object import (
            _LAILA_IDENTIFIABLE_OBJECT,
            split_global_id_attributes,
        )

        try:
            # Search attributes (``@evolution=-1``) are not part of the
            # identity and would be rejected by ``process_global_id``.
            head, _ = split_global_id_attributes(gid)
            scopes = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(head)["scopes"]
        except Exception:
            return False
        return bool(scopes) and scopes[0] == _MANIFEST_SCOPE

    @property
    def data(self) -> Any | None:
        """The blueprint payload (see :attr:`Entry.data`)."""
        return Entry.data.fget(self)

    @data.setter
    def data(self, new_data: Any | None) -> None:
        """Replace the blueprint and mark any SQL index stale.

        ``sql()`` promises the index is reused *until the blueprint
        mutates*; re-assigning ``manifest.data`` is such a mutation, so
        the next query rebuilds from the new rows (same as ``extend`` /
        ``+=``).
        """
        Entry.data.fset(self, new_data)
        # ``from_dict`` builds via ``cls.__new__`` and sets ``data`` before
        # private attributes exist.
        if getattr(self, "__pydantic_private__", None) is not None:
            self.invalidate_index()

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

        known = self._preresolved()
        all_gids = [gid for gid in self if gid not in known]
        if not all_gids:
            return Manifest._rebuild_with_entries(bp, known)

        memory = laila.get_active_policy().central.memory

        def _read(pool):
            # Direct-await resolver: one task on the internal taskforce reads
            # every child concurrently, instead of one future per child.
            ref = memory._read_entries_direct(all_gids, pool=pool)
            try:
                return list(ref.wait(None).data)
            finally:
                # The manifest owns this future; release it from the bank
                # once consumed so a large realized() does not pin its
                # children.
                ref.release()

        results = None
        last_exc = None
        for pool in self._candidate_pools(memory):
            try:
                results = _read(pool)
                break
            except KeyError as exc:
                last_exc = exc
        if results is None:
            raise KeyError(
                "Manifest.realized: one or more referenced entries failed to resolve."
            ) from last_exc

        if len(results) != len(all_gids) or any(r is None for r in results):
            raise KeyError("Manifest.realized: one or more referenced entries failed to resolve.")

        resolved_map = dict(known)
        resolved_map.update(zip(all_gids, results))
        return Manifest._rebuild_with_entries(bp, resolved_map)

    def _preresolved(self) -> dict[str, Any]:
        """``{gid: object}`` for leaves that are already in hand (never re-fetched).

        A plain manifest holds nothing; a :class:`ComplexManifest`
        contributes its resolved / assigned members.
        """
        return {}

    def _candidate_pools(self, memory) -> list:
        """Pools to look for referenced entries in, most likely first.

        1. the pool this manifest was last read from / written to
           (``_source_pool_gid``), when known;
        2. the alpha pool (``None`` -- the historical behaviour);
        3. every other registered pool, so a manifest whose children live
           in a different named pool still resolves (bounded by the
           number of pools).
        """
        hint = getattr(self, "_source_pool_gid", None)
        router = getattr(memory, "pool_router", None)
        known = list(getattr(router, "pools", {}).keys()) if router is not None else []
        alpha = getattr(memory, "alpha_pool", None)
        order: list = []
        if hint is not None and hint in known:
            order.append(hint)
        if hint != alpha:
            order.append(None)
        for gid in known:
            if gid != hint and gid != alpha:
                order.append(gid)
        return order

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

            known = self._preresolved()
            all_gids = [gid for gid in self if gid not in known]
            if not all_gids:
                return Manifest._rebuild_with_entries(bp, known)

            from ...command.schema.parking import _CURRENT_SLOT

            memory = laila.get_active_policy().central.memory
            on_loop = _CURRENT_SLOT.get() is not None

            async def _read(pool):
                if on_loop:
                    # Already running on a taskforce loop: await the pool
                    # directly -- zero futures for the whole batch.
                    return list(await memory._read_entries_async(all_gids, pool=pool))
                # Foreign loop (user's asyncio.run): keep pool I/O on the
                # taskforce loops via a single submitted task.
                ref = memory._read_entries_direct(all_gids, pool=pool)
                try:
                    return list((await ref).data)
                finally:
                    ref.release()

            results = None
            last_exc = None
            for pool in self._candidate_pools(memory):
                try:
                    results = await _read(pool)
                    break
                except KeyError as exc:
                    last_exc = exc
            if results is None:
                raise KeyError(
                    "Manifest.async_realized: one or more referenced entries failed to resolve."
                ) from last_exc

            if len(results) != len(all_gids) or any(r is None for r in results):
                raise KeyError(
                    "Manifest.async_realized: one or more referenced entries failed to resolve."
                )

            resolved_map = dict(known)
            resolved_map.update(zip(all_gids, results))
            return Manifest._rebuild_with_entries(bp, resolved_map)

        return _resolve()

    # ------------------------------------------------------------------
    # Core operations (all return GroupFuture)
    # ------------------------------------------------------------------

    def memorize(
        self,
        *,
        pool=None,
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        batch_size: int = 128,
        context: dict | None = None,
    ):
        """Upload all referenced entries and store the manifest itself.

        Collects any pending ``Entry`` objects provided at construction time,
        uploads them in batches, then stores the manifest itself (whose
        payload is the blueprint dict).

        ``laila.memorize(manifest, dst_pool=...)`` dispatches here, so the
        two spellings are equivalent.

        Parameters
        ----------
        pool : pool object | str, optional
            Destination pool -- a live pool, its ``global_id`` or a
            registered nickname (same forms as ``laila.memorize(dst_pool=)``).
        pool_nickname, pool_id : str, optional
            Back-compat aliases for *pool*.
        batch_size : int, default 128
            Number of pending entries per underlying ``laila.memorize`` call.
        context : dict, optional
            The sibling members of the :class:`ComplexManifest` this
            manifest is being memorized from (``{key: live object}``).
            Ignored by a plain manifest; typed manifests use it to
            reference their siblings (an optimizer names the model whose
            parameters it drives).
        """
        import laila

        from ...command.schema.future.future.group_future import GroupFuture

        if self.data is None:
            raise RuntimeError("Nothing to memorize — manifest has no blueprint.")

        routed = Manifest._resolve_pool(pool, pool_nickname, pool_id)
        all_future_ids: list[str] = []
        policy = laila.get_active_policy()
        try:
            # Children and the manifest itself land in ``routed``: remember
            # it so ``realized`` looks there first.
            resolved = policy.central.memory._resolve_pool_ref(routed) if routed else None
            self._source_pool_gid = resolved.global_id if resolved is not None else None
        except Exception:
            pass

        if self._pending_entries:
            for i in range(0, len(self._pending_entries), batch_size):
                batch = self._pending_entries[i : i + batch_size]
                ref = laila.memorize(batch, dst_pool=routed)
                all_future_ids.extend(Manifest._collect_future_ids(ref))
            self._pending_entries = None

        # Wrapped in a list on purpose: a *bare* Manifest handed to
        # ``laila.memorize`` dispatches back to this method, whereas a
        # list stores the manifest as a plain entry (blueprint payload).
        self_ref = laila.memorize([self], dst_pool=routed)
        all_future_ids.extend(Manifest._collect_future_ids(self_ref))

        return GroupFuture(
            taskforce_id=policy.central.command.internal_taskforce,
            policy_id=policy.global_id,
            future_ids=all_future_ids,
        )

    def remember(
        self,
        *,
        pool=None,
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        batch_size: int = 128,
        persist: bool = True,
    ):
        """Recall all referenced entries from the pool.

        ``laila.remember(manifest, dst_pool=...)`` dispatches here. The
        returned ``GroupFuture``'s ``.data`` is the flat list of entries in
        ``list(manifest)`` order.

        Parameters
        ----------
        pool : pool object | str, optional
            Source pool (live pool, ``global_id`` or nickname).
        pool_nickname, pool_id : str, optional
            Back-compat aliases for *pool*.
        batch_size : int, default 128
            Number of gids per underlying ``laila.remember`` call.
        persist : bool, default True
            Forwarded to ``laila.remember`` -- cache fetched entries into
            the alpha pool.
        """
        import laila

        from ...command.schema.future.future.group_future import GroupFuture

        if self.data is None:
            raise RuntimeError("No blueprint to resolve — manifest is empty.")

        routed = Manifest._resolve_pool(pool, pool_nickname, pool_id)
        all_gids = list(self)
        all_future_ids: list[str] = []
        policy = laila.get_active_policy()

        for i in range(0, len(all_gids), batch_size):
            batch = all_gids[i : i + batch_size]
            ref = laila.remember(batch, dst_pool=routed, persist=persist)
            all_future_ids.extend(Manifest._collect_future_ids(ref))

        return GroupFuture(
            taskforce_id=policy.central.command.internal_taskforce,
            policy_id=policy.global_id,
            future_ids=all_future_ids,
        )

    def forget(
        self,
        *,
        pool=None,
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        batch_size: int = 128,
    ):
        """Delete all referenced entries and the manifest itself from the pool.

        ``laila.forget(manifest, pool=...)`` dispatches here.

        Parameters
        ----------
        pool : pool object | str, optional
            Pool to delete from (live pool, ``global_id`` or nickname).
        pool_nickname, pool_id : str, optional
            Back-compat aliases for *pool*.
        batch_size : int, default 128
            Number of gids per underlying ``laila.forget`` call.
        """
        import laila

        from ...command.schema.future.future.group_future import GroupFuture

        if self.data is None:
            raise RuntimeError("No blueprint — nothing to forget.")

        routed = Manifest._resolve_pool(pool, pool_nickname, pool_id)
        all_gids = list(self)
        all_future_ids: list[str] = []
        policy = laila.get_active_policy()

        for i in range(0, len(all_gids), batch_size):
            batch = all_gids[i : i + batch_size]
            ref = laila.forget(batch, pool=routed)
            all_future_ids.extend(Manifest._collect_future_ids(ref))

        self_ref = laila.forget(self.global_id, pool=routed)
        all_future_ids.extend(Manifest._collect_future_ids(self_ref))

        return GroupFuture(
            taskforce_id=policy.central.command.internal_taskforce,
            policy_id=policy.global_id,
            future_ids=all_future_ids,
        )

    # ------------------------------------------------------------------
    # SQL index  (non-memorizing algorithmic help)
    # ------------------------------------------------------------------

    def sql(self, query: str) -> Manifest:
        """Filter the manifest with ``SELECT … FROM … [WHERE …]``.

        A small, regex-based SQL surface over a *non-memorizing* sqlite
        index that this manifest owns directly (it bypasses
        ``central.memory`` entirely — it is never memorized and never
        leaves the local machine).  The index is built lazily on first
        use and reused until :meth:`invalidate_index` /
        :meth:`clear_index` is called or the blueprint mutates.

        Supported surface
        ------------------
        - ``SELECT <items> FROM <name> [WHERE <predicate>]`` only.
        - ``ORDER BY`` / ``LIMIT`` / ``GROUP BY`` / ``HAVING`` / ``JOIN``
          / ``UNION`` are rejected up front with a ``ValueError``.
        - ``==`` is normalized to ``=`` so casual queries work.
        - The table name in ``FROM`` is treated as an alias and ignored
          (any identifier is accepted).
        - ``SELECT`` items may be ``*``, ``<name>``, or ``<alias>.<name>``;
          the alias prefix is stripped.  Items beyond ``*`` are ignored
          by the default projection.
        - String literals in ``WHERE`` must be single-quoted
          (``WHERE owner = 'alice'``).  A bareword right-hand side is
          interpreted by sqlite as a column name; if it can't be
          resolved the resulting error is re-raised as a ``ValueError``
          suggesting single quotes.

        Returns
        -------
        Manifest
            A fresh, same-type manifest containing the matched rows.
            ``sql()`` never mutates ``self`` in place.
        """
        select_items, _from_alias, where = Manifest._parse_sql(query)

        # The manifest's own (re-entrant) lock serializes build / rebuild /
        # query / clear across threads; the connection adds its own
        # statement-level lock underneath.
        with self.atomic():
            if self._sql_state is None:
                self.build_index()
            elif self._sql_state.stale:
                self._sql_rebuild_in_place(widen=True)

            state = self._sql_state
            sql_text = f'SELECT "{_SQL_ROW_IDX_COL}" FROM "{state.table_name}"'
            if where:
                sql_text += f" WHERE {where}"

            try:
                cursor = state.conn.execute(sql_text)
                matched_idx = [row[0] for row in cursor.fetchall()]
            except sqlite3.OperationalError as exc:
                message = str(exc)
                if "no such column" in message:
                    raise ValueError(
                        f"{message}. If you meant a string literal, wrap it in single "
                        "quotes (e.g. WHERE owner = 'alice'); unquoted barewords are "
                        "treated as column names by SQL."
                    ) from exc
                raise

            matched_keys = [state.row_keys[i] for i in matched_idx]
        return self._sql_project(matched_keys, select_items)

    def build_index(
        self,
        *,
        on: Iterable[str] = (),
        composite: Iterable[tuple[str, ...]] = (),
        persist: str | None = None,
        widen: bool = True,
    ) -> None:
        """Materialize the manifest into a sqlite table + indexes.

        Idempotent: a subsequent call is a no-op while the cached index
        is fresh, and triggers an in-place rebuild if the index was
        invalidated.  Subsequent :meth:`sql` calls reuse the same
        connection until :meth:`invalidate_index` is called or the
        blueprint mutates.

        ``persist``, if provided, is the path to a file-backed sqlite db
        that survives process restarts (and is attached without a
        rebuild when it already holds a matching table); otherwise the
        index lives under ``<laila_root>/indices/<manifest-uuid>/``.
        """
        with self.atomic():
            if self._sql_state is not None:
                if self._sql_state.stale:
                    self._sql_rebuild_in_place(widen=widen)
                return
            self._sql_build_fresh(on=on, composite=composite, persist=persist)

    def invalidate_index(self) -> None:
        """Mark the cached index stale without closing the connection.

        Cheap: the next :meth:`sql` does an in-place rebuild
        (``DELETE FROM …`` then re-INSERT, indexes preserved).  Called
        automatically by :meth:`extend` / ``+=``; subclasses that mutate
        their own blueprint should call this after writing.
        """
        if self._sql_state is not None:
            self._sql_state.stale = True

    def clear_index(self, *, remove_persisted: bool = False) -> None:
        """Close the sqlite connection and drop the cached index.

        A non-persistent (temporary) index file is disposable and is
        always unlinked here.  A user-chosen ``persist=`` file is left in
        place unless ``remove_persisted`` is true.  After
        ``clear_index()`` the next :meth:`sql` rebuilds from scratch.
        """
        with self.atomic():
            finalizer = self._sql_finalizer
            if finalizer is not None:
                finalizer.detach()
                self._sql_finalizer = None

            state = self._sql_state
            if state is None:
                return
            try:
                state.conn.close()
            except Exception:
                pass
            if (not state.is_persistent) or remove_persisted:
                Manifest._sql_unlink_db(state.db_path)
            self._sql_state = None

    # ----- Subclass hooks --------------------------------------------

    def _sql_rows(self) -> tuple[list[tuple[Hashable, dict]], set]:
        """Return ``(rows, columns)`` for the SQL index.

        Each row is ``(row_key, row_dict)``.  ``row_key`` uniquely
        addresses the row inside the blueprint (any hashable; tuples
        allowed).  ``row_dict`` is the flat column→value map used for
        filtering.  ``columns`` is the union of queryable keys.

        Default implementation: one row per top-level key, using the
        value as the row dict if it is a dict, otherwise wrapping it as
        ``{"value": <scalar>}``.  Columns are the union of inner dict
        keys (or ``{"value"}`` for scalar entries).

        IMPORTANT: ``row_dict`` values must be primitive scalars
        (``str``, ``int``, ``float``, ``bool``, ``bytes``, ``None``).
        ``_LAILA_IDENTIFIABLE_OBJECT`` instances and non-primitive
        containers are rejected at :meth:`build_index` time.  If your
        blueprint references Entries, pass ``.global_id`` (a ``str``)
        instead; if it contains lists/dicts, flatten them here (e.g.
        ``",".join(tags)`` or ``json.dumps(meta)``).

        The row order must be deterministic for a given blueprint so a
        reattached on-disk index can remap rows back to keys without a
        rebuild.
        """
        blueprint = self.data or {}
        rows: list[tuple[Hashable, dict]] = []
        columns: set = set()
        for key, value in blueprint.items():
            if isinstance(value, dict):
                row = dict(value)
            else:
                row = {"value": value}
            rows.append((key, row))
            columns.update(row.keys())
        return rows, columns

    def _sql_project(self, matched_keys: list, select_items: list[str]) -> Manifest:
        """Build a new same-type manifest from matched row keys.

        Default implementation keeps the top-level entries whose key
        appears in ``matched_keys`` and ignores ``SELECT`` items beyond
        ``*``.
        """
        blueprint = self.data or {}
        subset = {k: copy.deepcopy(blueprint[k]) for k in matched_keys if k in blueprint}
        return type(self)(blueprint=subset)

    # ----- SQL parser helpers ----------------------------------------

    @staticmethod
    def _parse_sql(query: str) -> tuple[list[str], str, str | None]:
        """Parse a tiny ``SELECT … FROM … [WHERE …]`` query.

        Strips a trailing ``;``, normalizes ``==`` to ``=``, rejects
        forbidden clauses, and returns ``(select_items, from_alias,
        where_clause_or_none)``.
        """
        if not isinstance(query, str):
            raise ValueError("sql() query must be a string.")

        normalized = query.strip().rstrip(";").strip().replace("==", "=")
        upper = normalized.upper()
        for clause in _FORBIDDEN_SQL:
            pattern = r"\b" + clause.replace(" ", r"\s+") + r"\b"
            if re.search(pattern, upper):
                raise ValueError(
                    f"Unsupported SQL clause: {clause}. Only "
                    "'SELECT <items> FROM <name> [WHERE <predicate>]' is supported."
                )

        match = re.match(
            r"^\s*SELECT\s+(?P<items>.+?)\s+FROM\s+"
            r"(?P<from>[`\"']?[A-Za-z_][A-Za-z0-9_]*[`\"']?)\s*"
            r"(?:WHERE\s+(?P<where>.+))?$",
            normalized,
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            raise ValueError("Only 'SELECT <items> FROM <name> [WHERE <predicate>]' is supported.")

        select_items = [
            Manifest._strip_table_prefix(item.strip()) for item in match.group("items").split(",")
        ]
        from_alias = match.group("from").strip("`\"'")
        where = match.group("where")
        if where is not None:
            where = where.strip()
        return select_items, from_alias, where

    @staticmethod
    def _strip_table_prefix(item: str) -> str:
        """Drop an ``alias.`` prefix and surrounding quotes from a SELECT item."""
        stripped = item.strip().strip('`"')
        if stripped == "*":
            return stripped
        if "." in stripped:
            stripped = stripped.split(".", 1)[1]
        return stripped.strip('`"')

    @staticmethod
    def _validate_primitive_rows(rows: list[tuple[Hashable, dict]]) -> None:
        """Reject non-primitive row values before any sqlite state exists."""
        from .....basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT

        for row_key, row_dict in rows:
            for col, val in row_dict.items():
                if isinstance(val, _LAILA_IDENTIFIABLE_OBJECT):
                    raise TypeError(
                        f"Cannot index column {col!r}: row {row_key!r} has value "
                        f"of type {type(val).__name__} (Laila identifiable object). "
                        f"Index columns must be primitive scalars "
                        f"(str, int, float, bool, bytes, None). "
                        f"Pass `.global_id` instead."
                    )
                if not isinstance(val, _PRIMITIVE_TYPES):
                    raise TypeError(
                        f"Cannot index column {col!r}: row {row_key!r} has value "
                        f"of type {type(val).__name__}. Index columns must be "
                        f"primitive scalars. Flatten lists/dicts in your "
                        f"_sql_rows() override (e.g. ','.join(tags) or "
                        f"json.dumps(meta))."
                    )

    # ----- SQL index internals ---------------------------------------

    def _index_db_path(self) -> str:
        """Default on-disk path for this manifest's index file.

        One file per *instance* (``<pid>-<random>.laila_sqlitedb``) inside
        the per-manifest ``indices/<uuid>/`` directory: two live manifests
        that share a uuid (same nickname, or the same gid loaded twice)
        must never attach to each other's temporary table.
        """
        from .....macros.defaults import LAILA_DEFAULT_DIRECTORIES

        base = os.path.join(LAILA_DEFAULT_DIRECTORIES["indices"], self.uuid)
        os.makedirs(base, exist_ok=True)
        return os.path.join(base, f"{os.getpid()}-{_uuid.uuid4().hex[:8]}.laila_sqlitedb")

    def _sql_build_fresh(
        self,
        *,
        on: Iterable[str],
        composite: Iterable[tuple[str, ...]],
        persist: str | None,
    ) -> None:
        """Build the index from scratch (or attach a matching on-disk file)."""
        rows, columns = self._sql_rows()
        Manifest._validate_primitive_rows(rows)
        columns = sorted(columns)
        row_keys = [row_key for row_key, _ in rows]

        is_persistent = persist is not None
        if persist is not None:
            db_path = os.path.expanduser(persist)
            parent = os.path.dirname(db_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        else:
            db_path = self._index_db_path()

        conn = _LockedConnection(sqlite3.connect(db_path, check_same_thread=False))
        fingerprint = Manifest._sql_fingerprint(columns, rows)

        if Manifest._index_table_exists(conn) and Manifest._index_row_count(conn) == len(rows):
            existing_columns = Manifest._existing_columns(conn)
            if (
                set(existing_columns) == set(columns)
                and Manifest._read_fingerprint(conn) == fingerprint
            ):
                self._sql_state = _SqlState(
                    conn=conn,
                    db_path=db_path,
                    is_persistent=is_persistent,
                    table_name=_SQL_INDEX_TABLE,
                    columns=existing_columns,
                    indexed=Manifest._existing_indexes(conn),
                    row_keys=row_keys,
                    stale=False,
                )
                self._sql_register_gc_cleanup()
                return

        Manifest._create_and_populate(conn, columns, rows)
        Manifest._write_fingerprint(conn, fingerprint)
        indexed = Manifest._apply_indexes(conn, on, composite)
        self._sql_state = _SqlState(
            conn=conn,
            db_path=db_path,
            is_persistent=is_persistent,
            table_name=_SQL_INDEX_TABLE,
            columns=columns,
            indexed=indexed,
            row_keys=row_keys,
            stale=False,
        )
        self._sql_register_gc_cleanup()

    def _sql_rebuild_in_place(self, *, widen: bool = True) -> None:
        """Re-materialize rows into the existing table, preserving indexes."""
        state = self._sql_state
        if state is None:
            return

        rows, columns = self._sql_rows()
        Manifest._validate_primitive_rows(rows)
        columns = sorted(columns)
        conn = state.conn

        new_columns = [c for c in columns if c not in state.columns]
        if new_columns:
            if not widen:
                raise ValueError(f"Index rebuild needs new columns {new_columns} but widen=False.")
            for col in new_columns:
                conn.execute(f'ALTER TABLE "{state.table_name}" ADD COLUMN "{col}"')

        all_columns = list(state.columns) + new_columns
        conn.execute(f'DELETE FROM "{state.table_name}"')
        Manifest._insert_rows(conn, state.table_name, all_columns, rows)
        Manifest._write_fingerprint(conn, Manifest._sql_fingerprint(columns, rows))
        conn.commit()

        state.columns = all_columns
        state.row_keys = [row_key for row_key, _ in rows]
        state.stale = False

    @staticmethod
    def _sql_fingerprint(columns: list[str], rows: list[tuple[Hashable, dict]]) -> str:
        """Content hash of what the index table holds (columns + rows).

        Rows are validated to be primitive scalars before this runs, so
        ``repr`` is a stable, order-preserving encoding.
        """
        payload = repr((list(columns), [(row_key, row) for row_key, row in rows]))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _write_fingerprint(conn: _LockedConnection, fingerprint: str) -> None:
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{_SQL_META_TABLE}" (key TEXT PRIMARY KEY, value TEXT)'
        )
        conn.execute(
            f'INSERT OR REPLACE INTO "{_SQL_META_TABLE}" (key, value) VALUES (?, ?)',
            ("fingerprint", fingerprint),
        )
        conn.commit()

    @staticmethod
    def _read_fingerprint(conn: _LockedConnection) -> str | None:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (_SQL_META_TABLE,),
        ).fetchone()
        if row is None:
            return None
        row = conn.execute(
            f'SELECT value FROM "{_SQL_META_TABLE}" WHERE key = ?', ("fingerprint",)
        ).fetchone()
        return None if row is None else row[0]

    @staticmethod
    def _index_table_exists(conn: _LockedConnection) -> bool:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (_SQL_INDEX_TABLE,),
        ).fetchone()
        return row is not None

    @staticmethod
    def _index_row_count(conn: _LockedConnection) -> int:
        return conn.execute(f'SELECT COUNT(*) FROM "{_SQL_INDEX_TABLE}"').fetchone()[0]

    @staticmethod
    def _existing_columns(conn: _LockedConnection) -> list[str]:
        rows = conn.execute(f'PRAGMA table_info("{_SQL_INDEX_TABLE}")').fetchall()
        return [r[1] for r in rows if r[1] != _SQL_ROW_IDX_COL]

    @staticmethod
    def _existing_indexes(conn: _LockedConnection) -> set:
        rows = conn.execute(f'PRAGMA index_list("{_SQL_INDEX_TABLE}")').fetchall()
        return {r[1] for r in rows}

    @staticmethod
    def _create_and_populate(
        conn: _LockedConnection,
        columns: list[str],
        rows: list[tuple[Hashable, dict]],
    ) -> None:
        for col in columns:
            Manifest._validate_ident(col)
        col_defs = "".join(f', "{c}"' for c in columns)
        conn.execute(f'DROP TABLE IF EXISTS "{_SQL_INDEX_TABLE}"')
        conn.execute(
            f'CREATE TABLE "{_SQL_INDEX_TABLE}" '
            f'("{_SQL_ROW_IDX_COL}" INTEGER PRIMARY KEY{col_defs})'
        )
        Manifest._insert_rows(conn, _SQL_INDEX_TABLE, columns, rows)
        conn.commit()

    @staticmethod
    def _insert_rows(
        conn: _LockedConnection,
        table: str,
        columns: list[str],
        rows: list[tuple[Hashable, dict]],
    ) -> None:
        if columns:
            col_names = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join(["?"] * (len(columns) + 1))
            statement = (
                f'INSERT INTO "{table}" ("{_SQL_ROW_IDX_COL}", {col_names}) VALUES ({placeholders})'
            )
            data = [
                [idx] + [row.get(c) for c in columns] for idx, (_row_key, row) in enumerate(rows)
            ]
        else:
            statement = f'INSERT INTO "{table}" ("{_SQL_ROW_IDX_COL}") VALUES (?)'
            data = [[idx] for idx in range(len(rows))]
        if data:
            conn.executemany(statement, data)

    @staticmethod
    def _apply_indexes(
        conn: _LockedConnection,
        on: Iterable[str],
        composite: Iterable[tuple[str, ...]],
    ) -> set:
        indexed: set = set()
        for col in on:
            Manifest._validate_ident(col)
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS "idx_{_SQL_INDEX_TABLE}_{col}" '
                f'ON "{_SQL_INDEX_TABLE}" ("{col}")'
            )
            indexed.add(col)
        for tup in composite:
            cols = tuple(tup)
            for col in cols:
                Manifest._validate_ident(col)
            name = "idx_" + _SQL_INDEX_TABLE + "_" + "_".join(cols)
            cols_sql = ", ".join(f'"{c}"' for c in cols)
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS "{name}" ON "{_SQL_INDEX_TABLE}" ({cols_sql})'
            )
            indexed.add(cols)
        conn.commit()
        return indexed

    @staticmethod
    def _validate_ident(name: str) -> None:
        if not isinstance(name, str) or not _SQL_IDENT_RE.match(name):
            raise ValueError(f"Invalid column identifier for indexing: {name!r}")

    # ----- GC cleanup of temporary index files -----------------------

    def _sql_register_gc_cleanup(self) -> None:
        """Ensure the manifest's temporary index file is nuked on GC.

        ``laila.forget`` cannot cover this — the manifest object may
        outlive (or live elsewhere than) the entry that was forgotten —
        so a per-instance ``weakref.finalize`` removes the temporary
        index file when the manifest is garbage-collected (and at
        interpreter exit).  Only non-persistent (temporary) indexes are
        finalized; a user-chosen ``persist=`` file is left untouched.
        """
        import weakref

        finalizer = self._sql_finalizer
        if finalizer is not None:
            finalizer.detach()
            self._sql_finalizer = None

        state = self._sql_state
        if state is None or state.is_persistent:
            return

        self._sql_finalizer = weakref.finalize(
            self, Manifest._sql_finalize, state.conn, state.db_path
        )

    @staticmethod
    def _sql_finalize(conn: _LockedConnection, db_path: str) -> None:
        """Close the connection and delete a temporary index file."""
        try:
            conn.close()
        except Exception:
            pass
        Manifest._sql_unlink_db(db_path)

    @staticmethod
    def _sql_unlink_db(db_path: str) -> None:
        """Remove an index db file and its (now-empty) per-manifest dir."""
        if not db_path:
            return
        try:
            if os.path.exists(db_path):
                os.remove(db_path)
        except OSError:
            pass
        try:
            parent = os.path.dirname(db_path)
            if parent and os.path.isdir(parent) and not os.listdir(parent):
                os.rmdir(parent)
        except OSError:
            pass

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
        # Same bookkeeping as the ``data`` setter: the blueprint changed in
        # this process, so a later memorize of a variable manifest bumps.
        self._locally_modified = True

        if other._pending_entries:
            if self._pending_entries is None:
                self._pending_entries = list(other._pending_entries)
            else:
                self._pending_entries.extend(other._pending_entries)

        # The blueprint changed: mark any cached SQL index stale so the
        # next sql() rebuilds in place (non-memorizing algorithmic help).
        self.invalidate_index()

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

    def __setitem__(self, key: str, value: Any) -> None:
        """Assign a top-level blueprint key: ``manifest["config"] = entry``.

        Accepted values are blueprint fragments -- a ``global_id`` string,
        a list of them, a nested dict of them -- or :class:`Entry` objects
        (anywhere inside a list / dict too), which are converted to their
        gid and queued for the next :meth:`memorize`. A manifest or a
        :class:`ManifestBacked` object is rejected here: a manifest that
        holds other manifests is a :class:`ComplexManifest`
        (``laila.complex_manifest(...)``), which is the only kind the read
        path resolves recursively.

        Re-assigning an existing key replaces it. The blueprint changed,
        so the manifest is marked locally modified and any SQL index is
        invalidated (same bookkeeping as :meth:`extend`).
        """
        if not isinstance(key, str):
            raise TypeError(f"Manifest keys must be strings, got {type(key).__name__}")
        leaf, pending = self._coerce_value(key, value)
        with self.atomic():
            bp = dict(self.data) if self.data is not None else {}
            bp[key] = leaf
            self._set_blueprint(bp)
            if pending:
                if self._pending_entries is None:
                    self._pending_entries = list(pending)
                else:
                    self._pending_entries.extend(pending)

    def __delitem__(self, key: str) -> None:
        """Remove a top-level blueprint key (its entries are not forgotten)."""
        bp = self.data
        if bp is None or key not in bp:
            raise KeyError(key)
        with self.atomic():
            new_bp = dict(bp)
            del new_bp[key]
            self._set_blueprint(new_bp)

    def _set_blueprint(self, blueprint: dict) -> None:
        """Replace the blueprint in place (``__setitem__`` / ``__delitem__``)."""
        self.data = blueprint
        if self.state is not EntryState.READY:
            self.state = EntryState.READY

    def _coerce_value(self, key: str, value: Any) -> tuple[Any, list]:
        """``(blueprint fragment, pending entries)`` for ``self[key] = value``."""
        if isinstance(value, (Manifest, ManifestBacked)):
            raise TypeError(
                f"Manifest[{key!r}]: a {type(value).__name__} cannot be a member of a plain "
                "manifest; create the container with laila.complex_manifest(...) so the "
                "nested manifest is memorized and resolved recursively"
            )
        if isinstance(value, Entry):
            return value.global_id, [value]
        if isinstance(value, str):
            return value, []
        if isinstance(value, (list, dict)):
            fragment = {key: value}
            has_entries, has_strings = Manifest._classify_data(fragment)
            if has_entries and has_strings:
                raise ValueError(
                    f"Manifest[{key!r}]: a fragment must hold either Entry objects or "
                    "global_id strings, not a mix of both"
                )
            if has_entries:
                for entry in Manifest._iter_entries(fragment):
                    if isinstance(entry, Manifest):
                        raise TypeError(
                            f"Manifest[{key!r}]: nested manifests need laila.complex_manifest(...)"
                        )
                return Manifest._extract_blueprint(fragment)[key], list(
                    Manifest._iter_entries(fragment)
                )
            Manifest._validate_blueprint(fragment)
            return copy.deepcopy(value), []
        raise TypeError(
            f"Manifest[{key!r}]: expected a global_id, a list / dict of them or an Entry, "
            f"got {type(value).__name__}"
        )

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

    def floating_leaves(self) -> list[str]:
        """Leaves that resolve at *read* time rather than naming one stored record.

        A leaf is *floating* when its reference carries a negative
        ``evolution`` (``@evolution=-1`` = "whatever is latest") or a
        ``creation_timestamp`` search argument. Such leaves make the
        manifest's contents depend on the pool state at the moment of
        ``remember``; pinned manifests (reproducible datasets) should
        have none. Malformed leaves are ignored here.
        """
        from .....basics.definitions.identifiable_object import (
            EVOLUTION_ATTRIBUTE,
            split_global_id_attributes,
        )
        from .....data.schema.pool_index import CREATION_TIMESTAMP_ATTRIBUTE

        floating: list[str] = []
        for gid in self:
            try:
                _, attrs = split_global_id_attributes(gid)
            except ValueError:
                continue
            evolution = attrs.get(EVOLUTION_ATTRIBUTE, "")
            if evolution.startswith("-") or CREATION_TIMESTAMP_ATTRIBUTE in attrs:
                floating.append(gid)
        return floating

    # ------------------------------------------------------------------
    # String representation
    # ------------------------------------------------------------------

    def __str__(self) -> str:
        return self.global_id

    def __repr__(self) -> str:
        n = sum(1 for _ in self)
        return f"{type(self).__name__}({self.global_id}, entries={n})"

    # ------------------------------------------------------------------
    # Static / class helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _collect_future_ids(ref) -> list[str]:
        """Extract future IDs from a GroupFuture or a single future identity.

        The children are re-parented into the manifest-level
        :class:`GroupFuture` returned to the caller, so an intermediate
        per-batch group shell is released here (children untouched); the
        caller releases the returned group, which releases the children.
        """
        if ref is None:
            return []
        if hasattr(ref, "future_ids"):
            from ...command.schema.future.future.group_future import GroupFuture

            ids = list(ref.future_ids)
            if isinstance(ref, GroupFuture):
                ref.release(children=False)
            return ids
        return [ref.global_id]

    @staticmethod
    def _resolve_pool(
        pool=None,
        pool_nickname: str | None = None,
        pool_id: str | None = None,
    ):
        """Collapse ``pool`` / ``pool_id`` / ``pool_nickname`` into one routing value.

        Precedence mirrors the top-level shims: an explicit ``pool`` wins,
        then ``pool_id``, then ``pool_nickname``. ``None`` means the alpha
        (default) pool. The result is accepted as-is by
        ``laila.memorize(dst_pool=)`` / ``laila.remember(dst_pool=)`` /
        ``laila.forget(pool=)``.
        """
        if pool is not None:
            return pool
        if pool_id is not None:
            return pool_id
        return pool_nickname

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


class ManifestBacked(abc.ABC):
    """Marker for objects that are *backed by* a :class:`Manifest`.

    Some objects cannot subclass :class:`Manifest` (it is a pydantic model
    with a validating ``__setattr__``) but still want
    ``laila.memorize(obj)`` / ``laila.remember(obj)`` / ``laila.forget(obj)``
    to mean "turn me into my manifest and run its bulk operation" --
    ``laila.addons.torch.Module`` (a ``torch.nn.Module``) and
    ``laila.addons.torch.Optimizer`` (a ``torch.optim.Optimizer``) are
    the canonical examples. The top-level shims treat a bare
    ``ManifestBacked`` exactly like a bare ``Manifest``: they call
    ``obj.memorize(pool=...)``, ``obj.remember(pool=..., persist=...)``
    or ``obj.forget(pool=...)`` and return whatever that returns.

    Subclasses provide :attr:`manifest` (a fresh :class:`Manifest`
    describing their current state), :attr:`global_id` (the manifest's
    identity, derived from a nickname) and the three bulk operations,
    which normally delegate to a :class:`Manifest` subclass that
    implements the type-specific ``remember``. Plain ABC on purpose: it
    carries no state and composes with any base class.
    """

    @property
    @abc.abstractmethod
    def manifest(self) -> Manifest:
        """A fresh manifest snapshot of this object's current state."""

    @property
    @abc.abstractmethod
    def global_id(self) -> str:
        """The ``global_id`` of the manifest this object is stored under."""

    @abc.abstractmethod
    def memorize(self, *, pool=None, **kwargs):
        """Store the manifest (and its entries); returns a future-like."""

    @abc.abstractmethod
    def remember(self, *, pool=None, persist: bool = False, **kwargs):
        """Load the stored manifest back into this object; returns a future-like."""

    @abc.abstractmethod
    def forget(self, *, pool=None, **kwargs):
        """Delete the manifest and its entries from *pool*; returns a future-like."""


class ComplexManifest(Manifest):
    """A manifest whose members may be manifests themselves.

    Scope ``MANIFEST:COMPLEX`` -- the gid itself announces that there are
    manifests underneath, so the read path only ever walks complex
    manifests; a plain :class:`Manifest` (a dataset with a million
    leaves) is never scanned.

    Write side::

        everything = laila.complex_manifest(nickname="everything")
        everything["model"] = model            # laila.addons.torch.Module (or a bare nn.Module)
        everything["optimizer"] = optimizer    # laila.addons.torch.Optimizer (or a bare one)
        everything["dataset"] = dataset.manifest
        everything["config"] = laila.constant(data={...})
        laila.memorize(everything, dst_pool="ckpt")

    ``memorize`` is recursive: every member that is a :class:`Manifest`
    or :class:`ManifestBacked` runs its *own* ``memorize`` into the same
    pool (receiving the sibling members as ``context``), plain entries
    are batched as usual, and the complex manifest's blueprint is
    ``{key: member gid}``.

    Read side -- *recursive blueprint resolution*::

        everything = laila.remember("MANIFEST:COMPLEX:everything", dst_pool="ckpt").wait()
        everything["model"]        # a ModuleManifest (typed through its scope)
        everything["dataset"]      # the dataset Manifest (one record, never walked)
        everything["config"]       # still a gid: plain entries are not fetched here
        live = everything.rebuild()    # {"model": Module, "optimizer": Optimizer, "config": Entry, ...}
        everything["model"]        # now the rebuilt Module

    When a complex manifest arrives from a pool, every top-level leaf
    whose gid carries the ``MANIFEST`` scope is fetched from the same
    pool, hydrated into its typed class through the scope builder and
    attached as a member; a nested complex manifest is resolved
    recursively in turn. Leaves that are not manifests stay gids.
    ``rebuild()`` then asks each typed member to reconstruct its live
    object (``ModuleManifest`` -> ``Module``, ``OptimizerManifest`` ->
    ``Optimizer`` bound to the rebuilt model's parameters, ...), fetches
    plain entries and returns ``{key: object}`` in blueprint order.

    Members are top-level keys only; nested dict / list fragments behave
    exactly as in a plain manifest (gids, realized to entries).
    """

    _DEFAULT_SCOPES: ClassVar[list[str]] = [_MANIFEST_SCOPE, _COMPLEX_MANIFEST_SCOPE]
    _OWNS_CHILDREN: ClassVar[bool] = True
    # ``(type, factory)`` pairs; ``factory(value, key, container_nickname)``
    # returns the ``ManifestBacked`` / ``Manifest`` standing in for *value*.
    _ADAPTERS: ClassVar[list[tuple[type, Callable[[Any, str, str | None], Any]]]] = []

    # ``{key: object}`` -- what ``self[key]`` currently is: the assigned
    # object (write side), the typed manifest (after resolution) or the
    # live object (after rebuild).
    _members: dict = PrivateAttr(default_factory=dict)
    # ``{key: ManifestBacked}`` for members that were adapted from a bare
    # object (``everything["model"] = nn.Module``): the wrapper that
    # memorizes / forgets on the bare object's behalf.
    _adapted: dict = PrivateAttr(default_factory=dict)
    _resolved: bool = PrivateAttr(default=False)

    def __init__(self, **data: Any) -> None:
        raw_data = data.pop("data", None)
        blueprint_kwarg = data.pop("blueprint", None)
        source = raw_data if raw_data is not None else blueprint_kwarg
        super().__init__(**data)
        if source is not None:
            if not isinstance(source, dict):
                raise ValueError("ComplexManifest data must be a dict.")
            if not source:
                self._set_blueprint({})
            for key, value in source.items():
                self[key] = value

    # ------------------------------------------------------------------
    # adapters
    # ------------------------------------------------------------------
    @classmethod
    def register_adapter(cls, type_: type, factory: Callable[[Any, str, str | None], Any]) -> None:
        """Teach ``everything[key] = value`` how to wrap a bare *type_*.

        *factory(value, key, container_nickname)* must return a
        :class:`ManifestBacked` (or :class:`Manifest`) standing in for
        *value*; ``laila.addons.torch`` registers ``torch.nn.Module`` and
        ``torch.optim.Optimizer`` this way. The most recently registered
        matching type wins.
        """
        cls._ADAPTERS.insert(0, (type_, factory))

    @classmethod
    def _adapt(cls, value: Any, key: str, nickname: str | None) -> Any | None:
        for type_, factory in cls._ADAPTERS:
            if isinstance(value, type_):
                return factory(value, key, nickname)
        return None

    # ------------------------------------------------------------------
    # mapping surface
    # ------------------------------------------------------------------
    def _coerce_value(self, key: str, value: Any) -> tuple[Any, list]:
        self._adapted.pop(key, None)
        self._members.pop(key, None)
        if isinstance(value, (Manifest, ManifestBacked)):
            self._members[key] = value
            return value.global_id, []
        adapted = self._adapt(value, key, self._nickname)
        if adapted is not None:
            self._members[key] = value
            self._adapted[key] = adapted
            return adapted.global_id, []
        if isinstance(value, Entry):
            self._members[key] = value
        return super()._coerce_value(key, value)

    def __getitem__(self, key: str) -> Any:
        """The member under *key* when there is one, else the blueprint value.

        After assignment this is the object that was assigned; after a
        read from a pool it is the typed manifest the leaf resolved to;
        after :meth:`rebuild` it is the live object. ``blueprint[key]``
        is always the raw gid / fragment.
        """
        if key in self._members:
            return self._members[key]
        return super().__getitem__(key)

    def __delitem__(self, key: str) -> None:
        super().__delitem__(key)
        self._members.pop(key, None)
        self._adapted.pop(key, None)

    @property
    def members(self) -> dict[str, Any]:
        """Read-only view of the current members (``{key: object}``)."""
        return dict(self._members)

    def _snapshot(self) -> Entry:
        # The stored copy (in-memory pools) must not share the live
        # members: a later read resolves them from the pool like any
        # other pool would.
        clone = super()._snapshot()
        clone._members = {}
        clone._adapted = {}
        clone._resolved = False
        return clone

    def _preresolved(self) -> dict[str, Any]:
        bp = self.data or {}
        return {
            bp[key]: member
            for key, member in self._members.items()
            if isinstance(bp.get(key), str) and isinstance(member, Entry)
        }

    def _handle(self, key: str) -> Any:
        """The memorizable stand-in for member *key* (the adapter wrapper when there is one)."""
        return self._adapted.get(key, self._members.get(key))

    def _manifest_members(self, *, recursive: bool = True) -> list:
        """Members that are manifests (typed or plain), nested complex ones expanded."""
        out: list = []
        for key in self._members:
            handle = self._handle(key)
            if isinstance(handle, Manifest):
                out.append(handle)
                if recursive and isinstance(handle, ComplexManifest):
                    out.extend(handle._manifest_members(recursive=True))
        return out

    # ------------------------------------------------------------------
    # memorize (recursive)
    # ------------------------------------------------------------------
    def memorize(
        self,
        *,
        pool=None,
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        batch_size: int = 128,
        context: dict | None = None,
    ):
        """Memorize every member into *pool*, then the complex manifest itself.

        Members that are manifests / manifest-backed objects run their
        own ``memorize(pool=..., context=<sibling handles>)`` first (so a
        typed manifest can reference a sibling), plain pending entries
        are batched, and finally the blueprint ``{key: member gid}`` is
        stored under this manifest's gid. One :class:`GroupFuture`
        covering every write is returned.
        """
        import laila

        from ...command.schema.future.future.group_future import GroupFuture

        if self.data is None:
            raise RuntimeError("Nothing to memorize — manifest has no blueprint.")

        routed = Manifest._resolve_pool(pool, pool_nickname, pool_id)
        policy = laila.get_active_policy()
        all_future_ids: list[str] = []
        handles = {key: self._handle(key) for key in self.data if key in self._members}

        for key in list(self.data):
            handle = handles.get(key)
            if isinstance(handle, (Manifest, ManifestBacked)):
                ref = handle.memorize(pool=routed, batch_size=batch_size, context=handles)
                all_future_ids.extend(Manifest._collect_future_ids(ref))

        self_ref = Manifest.memorize(self, pool=routed, batch_size=batch_size)
        all_future_ids.extend(Manifest._collect_future_ids(self_ref))
        return GroupFuture(
            taskforce_id=policy.central.command.internal_taskforce,
            policy_id=policy.global_id,
            future_ids=all_future_ids,
        )

    # ------------------------------------------------------------------
    # recursive blueprint resolution
    # ------------------------------------------------------------------
    def _unresolved_manifest_leaves(self) -> list[tuple[str, str]]:
        """``(key, gid)`` of top-level manifest-scoped leaves without a member."""
        bp = self.data or {}
        return [
            (key, value)
            for key, value in bp.items()
            if key not in self._members
            and isinstance(value, str)
            and Manifest.is_manifest_gid(value)
        ]

    async def async_resolve(
        self,
        *,
        pool=None,
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        persist: bool = False,
    ) -> ComplexManifest:
        """Recursive blueprint resolution (awaitable form, see :meth:`resolve`)."""
        import laila

        if self._resolved or self.data is None:
            return self
        memory = laila.get_active_policy().central.memory
        leaves = self._unresolved_manifest_leaves()
        if leaves:
            gids = [gid for _, gid in leaves]
            results = await self._read_manifests_async(
                memory, gids, Manifest._resolve_pool(pool, pool_nickname, pool_id), persist
            )
            for (key, gid), entry in zip(leaves, results):
                if not isinstance(entry, Manifest):
                    raise KeyError(
                        f"{type(self).__name__}[{key!r}]: {gid} did not resolve to a manifest "
                        f"(got {type(entry).__name__})"
                    )
                self._members[key] = entry
        for member in self._members.values():
            if isinstance(member, ComplexManifest) and not member._resolved:
                await member.async_resolve(
                    pool=pool, pool_nickname=pool_nickname, pool_id=pool_id, persist=persist
                )
        self._resolved = True
        return self

    async def _read_manifests_async(self, memory, gids: list[str], pool, persist: bool) -> list:
        """Fetch *gids* trying *pool* first, then this manifest's candidate pools."""
        from ...command.schema.parking import _CURRENT_SLOT

        on_loop = _CURRENT_SLOT.get() is not None
        candidates: list = []
        if pool is not None:
            candidates.append(pool)
        for candidate in self._candidate_pools(memory):
            if candidate not in candidates:
                candidates.append(candidate)

        last_exc: Exception | None = None
        for candidate in candidates:
            try:
                if on_loop:
                    return list(
                        await memory._read_entries_async(gids, pool=candidate, persist=persist)
                    )
                ref = memory._read_entries_direct(gids, pool=candidate, persist=persist)
                try:
                    return list((await ref).data)
                finally:
                    ref.release()
            except KeyError as exc:
                last_exc = exc
        raise KeyError(
            f"{type(self).__name__}: nested manifest(s) {gids} not found in any pool"
        ) from last_exc

    def resolve(
        self,
        *,
        pool=None,
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        persist: bool = False,
    ) -> ComplexManifest:
        """Recursive blueprint resolution: cast manifest-scoped leaves into members.

        Walks the top-level blueprint; every leaf whose gid carries the
        ``MANIFEST`` scope (decided from the string, no I/O) is fetched --
        from *pool* first, then from the pool this manifest came from,
        the alpha pool and every other registered pool -- hydrated into
        its typed class (``Manifest``, ``ComplexManifest``,
        ``ModuleManifest``, ...) and stored as the member under that key.
        Nested complex manifests are resolved recursively. Leaves that
        are not manifests are left as gids. Idempotent; returns ``self``.

        This runs automatically when a complex manifest is read through
        ``laila.remember`` (pass ``resolve=False`` there to opt out).
        """
        import laila

        if (
            self._resolved
            or self.data is None
            or (
                not self._unresolved_manifest_leaves()
                and not any(
                    isinstance(m, ComplexManifest) and not m._resolved
                    for m in self._members.values()
                )
            )
        ):
            self._resolved = True
            return self

        async def _task():
            await self.async_resolve(
                pool=pool, pool_nickname=pool_nickname, pool_id=pool_id, persist=persist
            )

        ref = laila.command.submit([_task])
        try:
            ref.wait()
            ref.result  # re-raises the task's exception, if any
        finally:
            ref.release()
        return self

    # ------------------------------------------------------------------
    # rebuild
    # ------------------------------------------------------------------
    def rebuild(
        self,
        *,
        pool=None,
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        device: Any = None,
        context: dict | None = None,
        persist: bool = False,
    ) -> dict[str, Any]:
        """Turn every member back into the live object it was made from.

        Resolves first when needed, then walks the blueprint in insertion
        order: a typed manifest exposing ``rebuild`` (a model, an
        optimizer, a nested complex manifest) is rebuilt with the shared
        *context* (``{gid: rebuilt object}``, so an optimizer finds the
        model whose parameters it drives); a plain manifest (a dataset)
        is returned as is; a plain entry leaf is fetched; nested dict /
        list fragments are realized to entries. Objects assigned locally
        are returned untouched. Results replace the members, so
        ``self[key]`` is the live object afterwards and a later
        ``memorize`` re-snapshots it.

        Parameters
        ----------
        pool, pool_nickname, pool_id
            Where to read from; defaults to the pool this manifest came
            from (then the alpha pool, then every registered pool).
        device : optional
            Forwarded to members that take one (``ModuleManifest``:
            where the rebuilt model lives; ``None`` = CPU).
        context : dict, optional
            ``{gid: live object}`` seed, extended as members are rebuilt.
        persist : bool, default False
            Cache fetched entries into the alpha pool.
        """
        if self.data is None:
            raise RuntimeError("No blueprint to rebuild — manifest is empty.")
        routed = Manifest._resolve_pool(pool, pool_nickname, pool_id)
        self.resolve(pool=routed, persist=persist)
        context = {} if context is None else context

        realized = self.realized
        out: dict[str, Any] = {}
        for key, value in self.data.items():
            member = self._members.get(key)
            if member is None:
                built = realized[key]
            elif isinstance(member, ComplexManifest):
                # Rebuilt in place: the nested complex manifest stays the
                # member (same type as assigned), its own members go live.
                member.rebuild(pool=routed, device=device, context=context, persist=persist)
                built = member
            elif isinstance(member, Manifest) and hasattr(member, "rebuild"):
                built = member.rebuild(pool=routed, device=device, context=context, persist=persist)
            else:
                built = member
            if isinstance(value, str):
                context.setdefault(value, built)
                if isinstance(member, Manifest) and not isinstance(
                    built, (Manifest, ManifestBacked)
                ):
                    # A bare object came back: keep a manifest-backed
                    # handle on it so memorize / forget still work.
                    handle = getattr(member, "handle", None)
                    if handle is not None:
                        self._adapted[key] = handle
                self._members[key] = built
            out[key] = built
        return out

    @property
    def rebuilt(self) -> dict[str, Any]:
        """``rebuild()`` with defaults."""
        return self.rebuild()

    # ------------------------------------------------------------------
    # forget (owner-aware)
    # ------------------------------------------------------------------
    def forget(
        self,
        *,
        pool=None,
        pool_nickname: str | None = None,
        pool_id: str | None = None,
        batch_size: int = 128,
    ):
        """Delete the complex manifest and the members it owns from *pool*.

        Members that own their leaves (typed checkpoints, nested complex
        manifests -- ``_OWNS_CHILDREN``) are forgotten through their own
        ``forget`` (leaves included); a plain manifest member (a dataset)
        is only dropped as a record, its entries are left alone; plain
        entry leaves are forgotten. Typed members are resolved first so
        their kind is known.
        """
        import laila

        from ...command.schema.future.future.group_future import GroupFuture

        if self.data is None:
            raise RuntimeError("No blueprint — nothing to forget.")
        routed = Manifest._resolve_pool(pool, pool_nickname, pool_id)
        try:
            self.resolve(pool=routed)
        except KeyError:
            pass  # already gone: fall through to a plain delete of the leaves
        policy = laila.get_active_policy()
        all_future_ids: list[str] = []
        plain: list[str] = []

        for key, value in self.data.items():
            handle = self._handle(key)
            if isinstance(handle, ManifestBacked) or (
                isinstance(handle, Manifest) and handle._OWNS_CHILDREN
            ):
                ref = handle.forget(pool=routed, batch_size=batch_size)
                all_future_ids.extend(Manifest._collect_future_ids(ref))
            elif isinstance(value, str):
                plain.append(value)
            elif isinstance(value, (list, dict)):
                plain.extend(Manifest._iter_global_ids({key: value}))

        for i in range(0, len(plain), batch_size):
            ref = laila.forget(plain[i : i + batch_size], pool=routed)
            all_future_ids.extend(Manifest._collect_future_ids(ref))
        self_ref = laila.forget(self.global_id, pool=routed)
        all_future_ids.extend(Manifest._collect_future_ids(self_ref))
        return GroupFuture(
            taskforce_id=policy.central.command.internal_taskforce,
            policy_id=policy.global_id,
            future_ids=all_future_ids,
        )


class _RealizedManifest(Manifest):
    """A :class:`Manifest` whose referenced entries are already in hand.

    Used inside process-pool workers running a constitution body
    (``laila.build(entry, taskforce_id=<process pool>)``): the parent
    realizes the manifest and ships the entries along, so ``realized`` /
    ``async_realized`` return the pre-built nested dict instead of
    reading pools through a policy (there is none in the worker).
    """

    _prebuilt: dict = PrivateAttr(default_factory=dict)

    @property
    def realized(self):
        return self._prebuilt

    @property
    def async_realized(self):
        async def _resolve():
            return self._prebuilt

        return _resolve()


from .....entry.constitution.build_maps import register_builder

register_builder(
    _MANIFEST_SCOPE,
    Manifest._build_from_dict_sync,
    Manifest._build_from_dict_async,
)
register_builder(
    f"{_MANIFEST_SCOPE}:{_COMPLEX_MANIFEST_SCOPE}",
    ComplexManifest._build_from_dict_sync,
    ComplexManifest._build_from_dict_async,
)
