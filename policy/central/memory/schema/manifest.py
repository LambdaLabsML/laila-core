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
import os
import re
import sqlite3
from collections.abc import Hashable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Optional

from pydantic import PrivateAttr

from .....entry import Entry
from .....entry.compdata import ComputationalData
from .....entry.entry_state import EntryState
from .....macros.strings import _MANIFEST_SCOPE

# ----------------------------------------------------------------------
# Non-memorizing algorithmic help: SQL index
# ----------------------------------------------------------------------
# The Manifest SQL index is a *non-memorizing algorithmic helper* — a
# lightweight, temporary, query-side convenience that a manifest owns
# directly while it is being operated on.  It deliberately bypasses
# ``central.memory`` (see ``vault/agent/memory.md`` for the sanctioned
# exemption): it is never memorized, never registered with a pool
# router, and never travels with the manifest on the wire.  It is a
# plain ``sqlite3`` database file under ``<laila_root>/indices`` that can
# be cleared or invalidated at any time.

_PRIMITIVE_TYPES = (str, int, float, bool, bytes, type(None))
_FORBIDDEN_SQL = ("ORDER BY", "LIMIT", "GROUP BY", "HAVING", "JOIN", "UNION")
_SQL_INDEX_TABLE = "manifest"
_SQL_ROW_IDX_COL = "__row_idx__"
_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class _SqlState:
    """In-memory handle for a manifest's non-memorizing SQL index.

    Holds the owned ``sqlite3`` connection plus the data-side metadata
    needed to map query results back to blueprint keys.  Stored on the
    manifest as a ``PrivateAttr`` so it never affects the manifest's
    on-the-wire serialization.
    """

    conn: sqlite3.Connection
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

    _scopes: list[str] = PrivateAttr(default_factory=lambda: [_MANIFEST_SCOPE])
    _pending_entries: list | None = PrivateAttr(default=None)
    _sql_state: Optional[_SqlState] = PrivateAttr(default=None)
    _sql_finalizer: Any = PrivateAttr(default=None)

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
        persist: Optional[str] = None,
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
    def _parse_sql(query: str) -> tuple[list[str], str, Optional[str]]:
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
            raise ValueError(
                "Only 'SELECT <items> FROM <name> [WHERE <predicate>]' is supported."
            )

        select_items = [
            Manifest._strip_table_prefix(item.strip())
            for item in match.group("items").split(",")
        ]
        from_alias = match.group("from").strip("`\"'")
        where = match.group("where")
        if where is not None:
            where = where.strip()
        return select_items, from_alias, where

    @staticmethod
    def _strip_table_prefix(item: str) -> str:
        """Drop an ``alias.`` prefix and surrounding quotes from a SELECT item."""
        stripped = item.strip().strip("`\"")
        if stripped == "*":
            return stripped
        if "." in stripped:
            stripped = stripped.split(".", 1)[1]
        return stripped.strip("`\"")

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
        """Default on-disk path for this manifest's index file."""
        from .....macros.defaults import LAILA_DEFAULT_DIRECTORIES

        base = os.path.join(LAILA_DEFAULT_DIRECTORIES["indices"], self.uuid)
        os.makedirs(base, exist_ok=True)
        return os.path.join(base, "sql_index.laila_sqlitedb")

    def _sql_build_fresh(
        self,
        *,
        on: Iterable[str],
        composite: Iterable[tuple[str, ...]],
        persist: Optional[str],
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

        conn = sqlite3.connect(db_path, check_same_thread=False)

        if Manifest._index_table_exists(conn) and Manifest._index_row_count(conn) == len(
            rows
        ):
            existing_columns = Manifest._existing_columns(conn)
            if set(existing_columns) == set(columns):
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
                raise ValueError(
                    f"Index rebuild needs new columns {new_columns} but widen=False."
                )
            for col in new_columns:
                conn.execute(
                    f'ALTER TABLE "{state.table_name}" ADD COLUMN "{col}"'
                )

        all_columns = list(state.columns) + new_columns
        conn.execute(f'DELETE FROM "{state.table_name}"')
        Manifest._insert_rows(conn, state.table_name, all_columns, rows)
        conn.commit()

        state.columns = all_columns
        state.row_keys = [row_key for row_key, _ in rows]
        state.stale = False

    @staticmethod
    def _index_table_exists(conn: sqlite3.Connection) -> bool:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (_SQL_INDEX_TABLE,),
        ).fetchone()
        return row is not None

    @staticmethod
    def _index_row_count(conn: sqlite3.Connection) -> int:
        return conn.execute(f'SELECT COUNT(*) FROM "{_SQL_INDEX_TABLE}"').fetchone()[0]

    @staticmethod
    def _existing_columns(conn: sqlite3.Connection) -> list[str]:
        rows = conn.execute(f'PRAGMA table_info("{_SQL_INDEX_TABLE}")').fetchall()
        return [r[1] for r in rows if r[1] != _SQL_ROW_IDX_COL]

    @staticmethod
    def _existing_indexes(conn: sqlite3.Connection) -> set:
        rows = conn.execute(f'PRAGMA index_list("{_SQL_INDEX_TABLE}")').fetchall()
        return {r[1] for r in rows}

    @staticmethod
    def _create_and_populate(
        conn: sqlite3.Connection,
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
        conn: sqlite3.Connection,
        table: str,
        columns: list[str],
        rows: list[tuple[Hashable, dict]],
    ) -> None:
        if columns:
            col_names = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join(["?"] * (len(columns) + 1))
            statement = (
                f'INSERT INTO "{table}" ("{_SQL_ROW_IDX_COL}", {col_names}) '
                f"VALUES ({placeholders})"
            )
            data = [
                [idx] + [row.get(c) for c in columns]
                for idx, (_row_key, row) in enumerate(rows)
            ]
        else:
            statement = f'INSERT INTO "{table}" ("{_SQL_ROW_IDX_COL}") VALUES (?)'
            data = [[idx] for idx in range(len(rows))]
        if data:
            conn.executemany(statement, data)

    @staticmethod
    def _apply_indexes(
        conn: sqlite3.Connection,
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
                f'CREATE INDEX IF NOT EXISTS "{name}" '
                f'ON "{_SQL_INDEX_TABLE}" ({cols_sql})'
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
    def _sql_finalize(conn: sqlite3.Connection, db_path: str) -> None:
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
