from __future__ import annotations

from typing import Optional, Any, Iterable, Iterator
from pydantic import Field, PrivateAttr
import json
import os

from ..schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry.compdata.transformation import TransformationSequence
from ...entry import transformation_base64

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore


class DuckDBPool(_LAILA_IDENTIFIABLE_POOL):
    file_path: Optional[str] = Field(default=None)
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)

    _conn: Any = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

    def model_post_init(self, __context: Any) -> None:
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
        if self._conn is None:
            raise RuntimeError("DuckDBPool is closed.")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __getitem__(self, key: str) -> Optional[Any]:
        with self.atomic():
            row = self._connection().execute(
                "SELECT value FROM laila_pool_entries WHERE key = ?",
                [key],
            ).fetchone()
        return json.loads(row[0]) if row is not None else None

    def __setitem__(self, key: str, entry: Any) -> None:
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("DuckDBPool expects a serialized JSON string.")

        with self.atomic():
            self._connection().execute(
                """
                INSERT INTO laila_pool_entries(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                [key, value],
            )

    def __delitem__(self, key: str) -> None:
        with self.atomic():
            self._connection().execute(
                "DELETE FROM laila_pool_entries WHERE key = ?",
                [key],
            )

    def empty(self) -> None:
        with self.atomic():
            self._connection().execute("DELETE FROM laila_pool_entries")

    def exists(self, key: str) -> bool:
        with self.atomic():
            row = self._connection().execute(
                "SELECT 1 FROM laila_pool_entries WHERE key = ?",
                [key],
            ).fetchone()
            return row is not None

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def keys(self, as_generator: bool = False) -> Iterable[str]:
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
