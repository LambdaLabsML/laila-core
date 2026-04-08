"""SQLite pool implementation."""
from __future__ import annotations

from typing import Optional, Any, Iterable, Iterator
from pydantic import Field, PrivateAttr
import json
import os
import sqlite3

from ..schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry.compdata.transformation import TransformationSequence
from ...entry import transformation_base64


class SQLitePool(_LAILA_IDENTIFIABLE_POOL):
    """SQLite-backed pool storing entries in a local database file."""

    file_path: Optional[str] = Field(default=None)
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)

    _conn: Optional[sqlite3.Connection] = PrivateAttr(default=None)

    class Config:
        """Pydantic model configuration."""

        arbitrary_types_allowed = True

    def model_post_init(self, __context: Any) -> None:
        """Open the SQLite database and create the entries table."""
        super().model_post_init(__context)
        if self.file_path is None:
            from ...macros.defaults import LAILA_DEFAULT_DIRECTORIES
            pool_dir = os.path.join(LAILA_DEFAULT_DIRECTORIES["pools"], self.uuid)
            os.makedirs(pool_dir, exist_ok=True)
            self.file_path = os.path.join(pool_dir, "pool.laila_sqlitedb")
        else:
            self.file_path = os.path.expanduser(self.file_path)
            parent = os.path.dirname(self.file_path)
            if parent:
                os.makedirs(parent, exist_ok=True)

        self._conn = sqlite3.connect(self.file_path, check_same_thread=False)
        row = self._conn.execute(
            "SELECT COUNT(*) FROM pragma_table_info('laila_pool_entries') WHERE name='pool_id'"
        ).fetchone()
        if row and row[0] > 0:
            self._conn.execute("DROP TABLE laila_pool_entries")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS laila_pool_entries (
                key TEXT NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def _connection(self) -> sqlite3.Connection:
        """Return the active SQLite connection."""
        if self._conn is None:
            raise RuntimeError("SQLitePool is closed.")
        return self._conn

    def close(self) -> None:
        """Close the SQLite connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _read(self, key: str) -> Optional[Any]:
        """Retrieve the JSON value for *key*, or ``None`` if absent."""
        with self.atomic():
            row = self._connection().execute(
                "SELECT value FROM laila_pool_entries WHERE key = ?",
                (key,),
            ).fetchone()
        return json.loads(row[0]) if row is not None else None

    def _write(self, key: str, entry: Any) -> None:
        """Insert or update *entry* under *key*."""
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("SQLitePool expects a serialized JSON string.")

        with self.atomic():
            self._connection().execute(
                """
                INSERT INTO laila_pool_entries(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )
            self._connection().commit()

    def _delete(self, key: str) -> None:
        """Delete the row for *key*."""
        with self.atomic():
            self._connection().execute(
                "DELETE FROM laila_pool_entries WHERE key = ?",
                (key,),
            )
            self._connection().commit()

    def _empty(self) -> None:
        """Remove all entries from the pool."""
        with self.atomic():
            self._connection().execute("DELETE FROM laila_pool_entries")
            self._connection().commit()

    def _exists(self, key: str) -> bool:
        """Return ``True`` if *key* is present in the table."""
        with self.atomic():
            row = self._connection().execute(
                "SELECT 1 FROM laila_pool_entries WHERE key = ?",
                (key,),
            ).fetchone()
            return row is not None

    def __contains__(self, key: str) -> bool:
        """Check membership, delegates to :meth:`_exists`."""
        return self._exists(key)

    def _keys(self, as_generator: bool = False) -> Iterable[str]:
        """Return all keys in the table.

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
                rows = self._connection().execute(
                    "SELECT key FROM laila_pool_entries ORDER BY key",
                ).fetchall()
                return [row[0] for row in rows]

        def _gen() -> Iterator[str]:
            with self.atomic():
                rows = self._connection().execute(
                    "SELECT key FROM laila_pool_entries ORDER BY key",
                ).fetchall()
                for row in rows:
                    yield row[0]

        return _gen()
