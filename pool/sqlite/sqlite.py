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
    file_path: Optional[str] = Field(default=None)
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)

    _conn: Optional[sqlite3.Connection] = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

    def model_post_init(self, __context: Any) -> None:
        base_dir = os.path.expanduser("~/.laila/pools/sqlite")
        os.makedirs(base_dir, exist_ok=True)

        if self.file_path is None:
            self.file_path = os.path.join(base_dir, f"{self.pool_id}.laila_sqlitedb")
        else:
            self.file_path = os.path.expanduser(self.file_path)
            parent = os.path.dirname(self.file_path)
            if parent:
                os.makedirs(parent, exist_ok=True)

        self._conn = sqlite3.connect(self.file_path, check_same_thread=False)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS laila_pool_entries (
                pool_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (pool_id, key)
            )
            """
        )
        self._conn.commit()

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SQLitePool is closed.")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __getitem__(self, key: str) -> Optional[Any]:
        with self.atomic():
            row = self._connection().execute(
                "SELECT value FROM laila_pool_entries WHERE pool_id = ? AND key = ?",
                (self.pool_id, key),
            ).fetchone()
        return json.loads(row[0]) if row is not None else None

    def __setitem__(self, key: str, entry: Any) -> None:
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("SQLitePool expects a serialized JSON string.")

        with self.atomic():
            self._connection().execute(
                """
                INSERT INTO laila_pool_entries(pool_id, key, value)
                VALUES (?, ?, ?)
                ON CONFLICT(pool_id, key) DO UPDATE SET value = excluded.value
                """,
                (self.pool_id, key, value),
            )
            self._connection().commit()

    def __delitem__(self, key: str) -> None:
        with self.atomic():
            self._connection().execute(
                "DELETE FROM laila_pool_entries WHERE pool_id = ? AND key = ?",
                (self.pool_id, key),
            )
            self._connection().commit()

    def empty(self) -> None:
        with self.atomic():
            self._connection().execute(
                "DELETE FROM laila_pool_entries WHERE pool_id = ?",
                (self.pool_id,),
            )
            self._connection().commit()

    def exists(self, key: str) -> bool:
        with self.atomic():
            row = self._connection().execute(
                "SELECT 1 FROM laila_pool_entries WHERE pool_id = ? AND key = ?",
                (self.pool_id, key),
            ).fetchone()
            return row is not None

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def keys(self, as_generator: bool = False) -> Iterable[str]:
        if not as_generator:
            with self.atomic():
                rows = self._connection().execute(
                    "SELECT key FROM laila_pool_entries WHERE pool_id = ? ORDER BY key",
                    (self.pool_id,),
                ).fetchall()
                return [row[0] for row in rows]

        def _gen() -> Iterator[str]:
            with self.atomic():
                rows = self._connection().execute(
                    "SELECT key FROM laila_pool_entries WHERE pool_id = ? ORDER BY key",
                    (self.pool_id,),
                ).fetchall()
                for row in rows:
                    yield row[0]

        return _gen()
