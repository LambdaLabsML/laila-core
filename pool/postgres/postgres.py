from __future__ import annotations

from typing import Optional, Any, Iterable, Iterator
from contextlib import suppress
from pydantic import Field, PrivateAttr
import atexit
import json
import os
import subprocess
import time
import hashlib

from ..schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry.compdata.transformation import TransformationSequence
from ...entry import transformation_base64

try:
    import psycopg
except ImportError:
    psycopg = None  # type: ignore


class PostgresPool(_LAILA_IDENTIFIABLE_POOL):
    dsn: Optional[str] = Field(default=None)
    host: Optional[str] = Field(default=None)
    port: int = Field(default=5432)
    dbname: Optional[str] = Field(default=None)
    user: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    server_start_timeout_s: float = Field(default=5.0)
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)

    _conn: Any = PrivateAttr(default=None)
    _postgres_proc: Optional[subprocess.Popen] = PrivateAttr(default=None)
    _owns_local_server: bool = PrivateAttr(default=False)
    _postgres_dir: Optional[str] = PrivateAttr(default=None)
    _socket_dir: Optional[str] = PrivateAttr(default=None)
    _local_dbname: str = PrivateAttr(default="postgres")
    _local_user: str = PrivateAttr(default="laila")

    class Config:
        arbitrary_types_allowed = True

    def model_post_init(self, __context: Any) -> None:
        self._conn = self._connect()
        with self._conn.cursor() as cur:
            cur.execute(
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
        atexit.register(self.close)

    def _connect(self):
        if psycopg is None:
            raise ImportError("psycopg is required for PostgresPool")
        if self.dsn is not None:
            return psycopg.connect(self.dsn, autocommit=False)
        if self.host is None and self.dbname is None and self.user is None and self.password is None:
            self._configure_local_server()
            self._ensure_local_server()
            return self._connect_local()
        if self.host is None or self.dbname is None or self.user is None:
            raise ValueError("PostgresPool requires either dsn, explicit host/dbname/user parameters, or no connection parameters for a managed local server.")
        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            autocommit=False,
        )

    def _configure_local_server(self) -> None:
        if self._postgres_dir is not None and self._socket_dir is not None:
            return
        base_dir = os.path.expanduser("~/.laila/pools")
        self._postgres_dir = os.path.join(base_dir, "postgres_data", self.pool_id)
        digest = hashlib.sha1(self.pool_id.encode("utf-8")).hexdigest()[:12]
        self._socket_dir = os.path.join(base_dir, "postgres_socket", digest)
        os.makedirs(self._postgres_dir, exist_ok=True)
        os.makedirs(self._socket_dir, exist_ok=True)
        self.port = 20000 + (int(hashlib.sha1(self.pool_id.encode("utf-8")).hexdigest()[:8], 16) % 20000)

    def _connect_local(self, *, connect_timeout: int = 1):
        if self._socket_dir is None:
            raise RuntimeError("Local Postgres socket directory is not configured.")
        return psycopg.connect(
            host=self._socket_dir,
            port=self.port,
            dbname=self._local_dbname,
            user=self._local_user,
            autocommit=False,
            connect_timeout=connect_timeout,
        )

    def _ensure_local_server(self) -> None:
        if self._local_server_available():
            self._owns_local_server = False
            return
        self._initdb_if_needed()
        self._start_local_server()

    def _local_server_available(self) -> bool:
        try:
            conn = self._connect_local(connect_timeout=1)
            conn.close()
            return True
        except Exception:
            return False

    def _initdb_if_needed(self) -> None:
        if self._postgres_dir is None:
            raise RuntimeError("Local Postgres data directory is not configured.")
        if os.path.exists(os.path.join(self._postgres_dir, "PG_VERSION")):
            return
        self._run_command(
            [
                "initdb",
                "-D",
                self._postgres_dir,
                "-U",
                self._local_user,
                "-A",
                "trust",
            ],
            action=f"initialize local Postgres data directory {self._postgres_dir}",
        )

    def _run_command(self, command: list[str], *, action: str) -> None:
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
        except FileNotFoundError as exc:
            raise RuntimeError(f"Unable to {action}: command not found: {command[0]}") from exc
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"Unable to {action}. Stdout: {exc.stdout.strip()} Stderr: {exc.stderr.strip()}"
            ) from exc

    def _start_local_server(self) -> None:
        if self._postgres_dir is None or self._socket_dir is None:
            raise RuntimeError("Local Postgres server is not fully configured.")
        cmd = [
            "postgres",
            "-D",
            self._postgres_dir,
            "-k",
            self._socket_dir,
            "-p",
            str(self.port),
            "-h",
            "",
        ]
        self._postgres_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self._owns_local_server = True

        deadline = time.time() + self.server_start_timeout_s
        last_err: Optional[Exception] = None
        while time.time() < deadline:
            if self._postgres_proc.poll() is not None:
                exit_code = self._postgres_proc.poll()
                stdout = ""
                stderr = ""
                if self._postgres_proc.stdout:
                    stdout = self._postgres_proc.stdout.read() or ""
                if self._postgres_proc.stderr:
                    stderr = self._postgres_proc.stderr.read() or ""
                self.close()
                raise RuntimeError(
                    f"postgres exited early (code={exit_code}). Stdout: {stdout.strip()} Stderr: {stderr.strip()}"
                )
            try:
                conn = self._connect_local(connect_timeout=1)
                conn.close()
                return
            except Exception as exc:
                last_err = exc
                time.sleep(0.1)

        stdout = ""
        stderr = ""
        if self._postgres_proc and self._postgres_proc.stdout:
            stdout = self._postgres_proc.stdout.read() or ""
        if self._postgres_proc and self._postgres_proc.stderr:
            stderr = self._postgres_proc.stderr.read() or ""
        self.close()
        raise RuntimeError(
            f"postgres failed to start for {self.pool_id}. Last error: {last_err}. Stdout: {stdout.strip()} Stderr: {stderr.strip()}"
        )

    def _connection(self):
        if self._conn is None:
            raise RuntimeError("PostgresPool is closed.")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        proc = self._postgres_proc
        self._postgres_proc = None
        if proc is not None and self._owns_local_server:
            with suppress(Exception):
                proc.terminate()
            try:
                proc.wait(timeout=2.0)
            except Exception:
                with suppress(Exception):
                    proc.kill()
                with suppress(Exception):
                    proc.wait(timeout=2.0)
        self._owns_local_server = False

    def __getitem__(self, key: str) -> Optional[Any]:
        with self.atomic():
            with self._connection().cursor() as cur:
                cur.execute(
                    "SELECT value FROM laila_pool_entries WHERE pool_id = %s AND key = %s",
                    (self.pool_id, key),
                )
                row = cur.fetchone()
        return json.loads(row[0]) if row is not None else None

    def __setitem__(self, key: str, entry: Any) -> None:
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("PostgresPool expects a serialized JSON string.")

        with self.atomic():
            with self._connection().cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO laila_pool_entries(pool_id, key, value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT(pool_id, key) DO UPDATE SET value = EXCLUDED.value
                    """,
                    (self.pool_id, key, value),
                )
            self._connection().commit()

    def __delitem__(self, key: str) -> None:
        with self.atomic():
            with self._connection().cursor() as cur:
                cur.execute(
                    "DELETE FROM laila_pool_entries WHERE pool_id = %s AND key = %s",
                    (self.pool_id, key),
                )
            self._connection().commit()

    def empty(self) -> None:
        with self.atomic():
            with self._connection().cursor() as cur:
                cur.execute(
                    "DELETE FROM laila_pool_entries WHERE pool_id = %s",
                    (self.pool_id,),
                )
            self._connection().commit()

    def exists(self, key: str) -> bool:
        with self.atomic():
            with self._connection().cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM laila_pool_entries WHERE pool_id = %s AND key = %s",
                    (self.pool_id, key),
                )
                return cur.fetchone() is not None

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def keys(self, as_generator: bool = False) -> Iterable[str]:
        if not as_generator:
            with self.atomic():
                with self._connection().cursor() as cur:
                    cur.execute(
                        "SELECT key FROM laila_pool_entries WHERE pool_id = %s ORDER BY key",
                        (self.pool_id,),
                    )
                    return [row[0] for row in cur.fetchall()]

        def _gen() -> Iterator[str]:
            with self.atomic():
                with self._connection().cursor() as cur:
                    cur.execute(
                        "SELECT key FROM laila_pool_entries WHERE pool_id = %s ORDER BY key",
                        (self.pool_id,),
                    )
                    for row in cur.fetchall():
                        yield row[0]

        return _gen()
