"""DuckDB pool implementation."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator
from typing import Any, ClassVar

from pydantic import ConfigDict, Field, PrivateAttr

from ...entry import transformation_base64
from ...entry.compdata.transformation import TransformationSequence
from ..schema.base import _LAILA_IDENTIFIABLE_POOL
from ..schema.pool_index import is_index_key

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore

# Space reclaim. DuckDB only frees the blocks of deleted rows at a
# CHECKPOINT, and even then only for row groups that are *entirely* dead: a
# single long-lived row (a config constant, an index record) pins its row
# group and the deleted neighbours are kept forever, so a write/delete churn
# grows the file and the buffer pool without bound. The pool therefore
# accounts deleted bytes/rows (``DELETE ... RETURNING length(value)``) and,
# past either threshold, arms a deferred maintenance step that runs before
# the next data write: ``CHECKPOINT``, then -- when the deleted volume is
# comparable to what is still live -- a transactional table rewrite
# (``_compact``) that leaves only live rows, followed by another CHECKPOINT.
DUCKDB_CHECKPOINT_CHURN_BYTES = 8 << 20
DUCKDB_CHECKPOINT_CHURN_ROWS = 4096
# Compact when deleted rows >= live rows, or when deleted bytes >= this
# floor *and* >= live bytes (the live-bytes scan runs only past the floor).
DUCKDB_COMPACT_MIN_BYTES = 64 << 20


class DuckDBPool(_LAILA_IDENTIFIABLE_POOL):
    """DuckDB-backed pool storing entries in a local DuckDB database file.

    Parameters
    ----------
    file_path : str, optional
        Database file; defaults to a per-pool file under the laila pools
        directory.
    memory_limit : str | None, default ``None``
        Value for DuckDB's ``memory_limit`` setting (e.g. ``"512MB"``).
        ``None`` keeps the engine default (80 % of RAM). Note that DuckDB
        treats the limit as hard: exceeding it raises a *fatal* error that
        invalidates the connection, so only set it for workloads whose
        working set you know.
    """

    file_path: str | None = Field(default=None)
    transformations: TransformationSequence | None = Field(default=transformation_base64)
    memory_limit: str | None = Field(default=None)

    _conn: Any = PrivateAttr(default=None)
    _churn_bytes: int = PrivateAttr(default=0)
    _churn_rows: int = PrivateAttr(default=0)
    _checkpoint_pending: bool = PrivateAttr(default=False)

    _persistent_storage: ClassVar[bool] = True

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any) -> None:
        """Initialise the DuckDB connection and create the entries table."""
        super().model_post_init(__context)
        if duckdb is None:
            raise ImportError("duckdb is required for DuckDBPool")

        if self.file_path is None:
            from ...macros.defaults import LAILA_DEFAULT_DIRECTORIES

            pool_dir = os.path.join(LAILA_DEFAULT_DIRECTORIES["pools"], self.uuid)
            os.makedirs(pool_dir, exist_ok=True)
            self.file_path = os.path.join(pool_dir, "pool.duckdb")
        else:
            self.file_path = os.path.expanduser(self.file_path)
            parent = os.path.dirname(self.file_path)
            if parent:
                os.makedirs(parent, exist_ok=True)

        self._conn = duckdb.connect(self.file_path)
        if self.memory_limit:
            self._conn.execute(f"SET memory_limit='{self.memory_limit}'")
        row = self._conn.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_name = 'laila_pool_entries' AND column_name = 'pool_id'"
        ).fetchone()
        if row and row[0] > 0:
            self._conn.execute("DROP TABLE laila_pool_entries")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS laila_pool_entries (
                key VARCHAR NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

    def _connection(self):
        """Return the active DuckDB connection."""
        if self._conn is None:
            raise RuntimeError("DuckDBPool is closed.")
        return self._conn

    def close(self) -> None:
        """Close the DuckDB connection.

        Runs under the pool lock so an in-flight write finishes before
        the connection goes away; later callers hit the documented
        ``RuntimeError("... is closed")`` from ``_connection()``.
        """
        with self.atomic():
            if self._conn is not None:
                if self._checkpoint_pending:
                    self._run_pending_checkpoint()
                self._conn.close()
                self._conn = None

    def checkpoint(self, compact: bool = False) -> None:
        """Checkpoint the database now; with *compact* also rewrite the table.

        A plain ``CHECKPOINT`` frees the blocks of row groups that are
        entirely deleted. ``compact=True`` additionally rewrites
        ``laila_pool_entries`` with only its live rows (a transactional
        ``CREATE TABLE ... INSERT ... DROP ... RENAME``), which is the only
        way DuckDB gives back the space of deleted rows that share a row
        group with live ones.
        """
        with self.atomic():
            conn = self._connection()
            conn.execute("CHECKPOINT")
            if compact:
                self._compact()
            self._churn_bytes = 0
            self._churn_rows = 0
            self._checkpoint_pending = False

    def _note_churn(self, nbytes: int, rows: int = 1) -> None:
        """Account for deleted data; arm deferred maintenance past the thresholds."""
        self._churn_bytes += max(int(nbytes or 0), 0)
        self._churn_rows += rows
        if (
            self._churn_bytes >= DUCKDB_CHECKPOINT_CHURN_BYTES
            or self._churn_rows >= DUCKDB_CHECKPOINT_CHURN_ROWS
        ):
            self._checkpoint_pending = True

    def _should_compact(self) -> bool:
        """Deleted volume comparable to the live volume (rows, or bytes past the floor)."""
        conn = self._connection()
        live_rows = conn.execute("SELECT count(*) FROM laila_pool_entries").fetchone()[0]
        if self._churn_rows >= max(int(live_rows), 1):
            return True
        if self._churn_bytes < DUCKDB_COMPACT_MIN_BYTES:
            return False
        live_bytes = conn.execute(
            "SELECT coalesce(sum(length(value)), 0) FROM laila_pool_entries"
        ).fetchone()[0]
        return self._churn_bytes >= int(live_bytes)

    def _compact(self) -> None:
        """Rewrite the entries table with its live rows only (caller holds the lock)."""
        conn = self._connection()
        conn.execute("BEGIN")
        try:
            conn.execute(
                "CREATE TABLE laila_pool_entries_compact ("
                "key VARCHAR NOT NULL PRIMARY KEY, value TEXT NOT NULL)"
            )
            conn.execute(
                "INSERT INTO laila_pool_entries_compact SELECT key, value FROM laila_pool_entries"
            )
            conn.execute("DROP TABLE laila_pool_entries")
            conn.execute("ALTER TABLE laila_pool_entries_compact RENAME TO laila_pool_entries")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")

    def _run_pending_checkpoint(self) -> None:
        """Run the deferred maintenance (caller holds the lock). Never fails the write."""
        if not self._checkpoint_pending:
            return
        try:
            self._connection().execute("CHECKPOINT")
            if self._should_compact():
                self._compact()
        except Exception:  # pragma: no cover - space reclaim is best effort
            return
        self._churn_bytes = 0
        self._churn_rows = 0
        self._checkpoint_pending = False

    def _read(self, key: str) -> Any | None:
        """Retrieve the JSON value for *key*, or ``None`` if absent."""
        with self.atomic():
            row = (
                self._connection()
                .execute(
                    "SELECT value FROM laila_pool_entries WHERE key = ?",
                    [key],
                )
                .fetchone()
            )
        return json.loads(row[0]) if row is not None else None

    def _write(self, key: str, entry: Any) -> None:
        """Insert or update *entry* under *key*."""
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("DuckDBPool expects a serialized JSON string.")

        with self.atomic():
            # Index-shard writes are interleaved with the deletes that arm the
            # checkpoint; running it there (mid delete phase) does not let
            # DuckDB reuse the freed blocks. Run it on the next *data* write.
            if not is_index_key(key):
                self._run_pending_checkpoint()
            self._connection().execute(
                """
                INSERT INTO laila_pool_entries(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                [key, value],
            )

    def _delete(self, key: str) -> None:
        """Delete the row for *key* (its size feeds the deferred-checkpoint accounting)."""
        with self.atomic():
            row = (
                self._connection()
                .execute(
                    "DELETE FROM laila_pool_entries WHERE key = ? RETURNING length(value)",
                    [key],
                )
                .fetchone()
            )
            if row is not None:
                self._note_churn(row[0])

    def _empty(self) -> None:
        """Remove all entries from the pool and checkpoint so the space is reusable."""
        with self.atomic():
            conn = self._connection()
            conn.execute("DELETE FROM laila_pool_entries")
            try:
                conn.execute("CHECKPOINT")
                self._churn_bytes = 0
                self._churn_rows = 0
                self._checkpoint_pending = False
            except Exception:  # pragma: no cover - best effort
                self._checkpoint_pending = True

    def _exists(self, key: str) -> bool:
        """Return ``True`` if *key* is present."""
        with self.atomic():
            row = (
                self._connection()
                .execute(
                    "SELECT 1 FROM laila_pool_entries WHERE key = ?",
                    [key],
                )
                .fetchone()
            )
            return row is not None

    def __contains__(self, key: str) -> bool:
        """Check membership, delegates to :meth:`_exists`."""
        return self._exists(key)

    def _keys(self, as_generator: bool = False) -> Iterable[str]:
        """Return all keys in the pool.

        Parameters
        ----------
        as_generator : bool, optional
            If ``True``, return a lazy iterator instead of a list.

        Returns
        -------
        Iterable[str]
            Pool keys.
        """
        if not as_generator:
            with self.atomic():
                rows = (
                    self._connection()
                    .execute(
                        "SELECT key FROM laila_pool_entries ORDER BY key",
                    )
                    .fetchall()
                )
                return [row[0] for row in rows]

        def _gen() -> Iterator[str]:
            with self.atomic():
                rows = (
                    self._connection()
                    .execute(
                        "SELECT key FROM laila_pool_entries ORDER BY key",
                    )
                    .fetchall()
                )
                for row in rows:
                    yield row[0]

        return _gen()
