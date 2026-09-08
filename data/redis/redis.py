"""Redis pool implementation with a managed private redis-server."""

from __future__ import annotations

import atexit
import hashlib
import json
import os
import subprocess
import time
from collections.abc import Iterable, Iterator
from contextlib import suppress
from typing import Any

import redis
from pydantic import ConfigDict, Field, PrivateAttr

from ...entry import transformation_base64
from ...entry.compdata.transformation import TransformationSequence
from ..schema.base import _LAILA_IDENTIFIABLE_POOL


class RedisPool(_LAILA_IDENTIFIABLE_POOL):
    """
    Redis-backed Pool using a private redis-server over a UNIX socket.

    Persistence:
      - Data directory: <LAILA_DEFAULT_DIRECTORIES["pools"]>/<pool.uuid>/
      - Dump file (private): pool.rdb
      - If pool.rdb exists, Redis loads it automatically.
      - Dumps are never deleted.

    Notes:
      - No TCP ports
      - No orphan-process handling
      - Values stored are serialized JSON strings
    """

    # Redis key namespaces
    key_prefix: str = Field(default="pool")
    lock_prefix: str = Field(default="pool_lock")

    # Optional auth
    redis_password: str | None = Field(default=None)

    # Behavior
    server_start_timeout_s: float = Field(default=3.0)
    lock_timeout: int = Field(default=30)

    redis_dir: str | None = Field(default=None)

    _client: redis.Redis | None = PrivateAttr(default=None)
    _redis_proc: subprocess.Popen | None = PrivateAttr(default=None)
    _db_dump_name: str = PrivateAttr(default="laila_pool.rdb")
    _redis_socket_name: str = PrivateAttr(default="laila_redis.sock")

    transformations: TransformationSequence | None = Field(default=transformation_base64)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # ---------------- lifecycle ----------------
    def model_post_init(self, __context: Any) -> None:
        """
        Always:
          1) establish redis_dir from pool_id
          2) start redis-server (unix socket only)
          3) connect client
        """
        super().model_post_init(__context)
        from ...macros.defaults import LAILA_DEFAULT_DIRECTORIES

        pool_dir = os.path.join(LAILA_DEFAULT_DIRECTORIES["pools"], self.uuid)
        self.redis_dir = pool_dir
        os.makedirs(self.redis_dir, exist_ok=True)

        # Remove stale socket file (do NOT handle orphan processes yet)
        with suppress(FileNotFoundError):
            os.remove(self._redis_socket_path)

        self._start_redis_server()

        self._client = redis.Redis(
            unix_socket_path=self._redis_socket_path,
            password=self.redis_password,
            decode_responses=True,
        )

        atexit.register(self.close)

    def __del__(self) -> None:
        """Best-effort cleanup on garbage collection."""
        with suppress(Exception):
            self.close()

    def close(self) -> None:
        """
        Clean shutdown.
        Ask Redis to save and exit; never delete dumps.
        """
        proc = self._redis_proc
        self._redis_proc = None

        if proc is None:
            return

        if self._client is not None:
            with suppress(Exception):
                self._client.shutdown(save=True)

        with suppress(Exception):
            proc.terminate()

        try:
            proc.wait(timeout=1.5)
        except Exception:
            with suppress(Exception):
                proc.kill()
            with suppress(Exception):
                proc.wait(timeout=1.5)

        with suppress(Exception):
            os.remove(self._redis_socket_path)

    # ---------------- redis-server ----------------
    def _start_redis_server(self) -> None:
        """Launch a ``redis-server`` subprocess on a UNIX socket."""
        if not self.redis_dir:
            raise RuntimeError("Redis server not fully configured (missing redis_dir)")

        cmd = [
            "redis-server",
            "--port",
            "0",  # disable TCP
            "--unixsocket",
            self._redis_socket_path,
            "--unixsocketperm",
            "700",
            "--dir",
            self.redis_dir,
            "--dbfilename",
            self._db_dump_name,
            "--save",
            "900 1",
            "--save",
            "300 10",
            "--save",
            "60 10000",
            "--appendonly",
            "no",
            "--protected-mode",
            "yes",
        ]

        if self.redis_password:
            cmd += ["--requirepass", self.redis_password]

        self._redis_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        deadline = time.time() + self.server_start_timeout_s
        last_err: Exception | None = None

        while time.time() < deadline:
            if self._redis_proc.poll() is not None:
                exit_code = self._redis_proc.poll()
                stdout = ""
                stderr = ""
                with suppress(Exception):
                    if self._redis_proc.stdout:
                        stdout = self._redis_proc.stdout.read() or ""
                with suppress(Exception):
                    if self._redis_proc.stderr:
                        stderr = self._redis_proc.stderr.read() or ""
                self.close()
                raise RuntimeError(
                    f"redis-server exited early (code={exit_code}). "
                    f"Stdout: {stdout.strip()} Stderr: {stderr.strip()}"
                )

            try:
                r = redis.Redis(
                    unix_socket_path=self._redis_socket_path,
                    password=self.redis_password,
                    socket_connect_timeout=0.25,
                    decode_responses=True,
                )
                r.ping()
                return
            except Exception as e:
                last_err = e
                time.sleep(0.05)

        stdout = ""
        stderr = ""
        with suppress(Exception):
            if self._redis_proc and self._redis_proc.stdout:
                stdout = self._redis_proc.stdout.read() or ""
        with suppress(Exception):
            if self._redis_proc and self._redis_proc.stderr:
                stderr = self._redis_proc.stderr.read() or ""
        self.close()
        raise RuntimeError(
            f"redis-server failed to start on unix socket {self._redis_socket_path}. "
            f"Last error: {last_err}. Stdout: {stdout.strip()} Stderr: {stderr.strip()}"
        )

    # ---------------- private helpers ----------------
    @property
    def _redis_socket_path(self) -> str:
        """Deterministic UNIX socket path derived from pool UUID."""
        digest = hashlib.sha1(self.uuid.encode("utf-8")).hexdigest()[:16]
        return f"/tmp/laila_redis_{digest}.sock"

    # ---------------- key helpers ----------------
    @property
    def redis_hash_key(self) -> str:
        """Redis hash key used to store all pool entries."""
        return self.key_prefix

    @property
    def redis_lock_key(self) -> str:
        """Redis key used for distributed locking."""
        return self.lock_prefix

    def _read(self, key: str) -> Any | None:
        """Retrieve the JSON value for *key*, or ``None`` if absent."""
        if self._client is None:
            raise RuntimeError("Redis client not initialized.")
        value = self._client.hget(self.redis_hash_key, key)

        return json.loads(value) if value is not None else None

    def _write(self, key: str, entry: Any) -> None:
        """Store *entry* as a JSON string in the Redis hash."""
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if self._client is None:
            raise RuntimeError("Redis client not initialized.")
        if not isinstance(value, str):
            raise TypeError("RedisPool expects a serialized JSON string.")
        self._client.hset(self.redis_hash_key, key, value)

    def _delete(self, key: str) -> None:
        """Delete *key* from the Redis hash."""
        if self._client is None:
            raise RuntimeError("Redis client not initialized.")
        self._client.hdel(self.redis_hash_key, key)

    def _empty(self) -> None:
        """Remove all entries from the pool (deletes the Redis hash)."""
        if self._client is None:
            raise RuntimeError("Redis client not initialized.")
        self._client.delete(self.redis_hash_key)

    def _exists(self, key: str) -> bool:
        """Return ``True`` if *key* exists in the Redis hash."""
        if self._client is None:
            raise RuntimeError("Redis client not initialized.")
        return bool(self._client.hexists(self.redis_hash_key, key))

    def __contains__(self, key: str) -> bool:
        """Check membership, delegates to :meth:`_exists`."""
        return self._exists(key)

    def _keys(self, as_generator: bool = False) -> Iterable[str]:
        """Return all keys in the Redis hash.

        Parameters
        ----------
        as_generator : bool, optional
            If ``True``, return a lazy iterator using ``HSCAN``.

        Returns
        -------
        Iterable[str]
            Pool keys.
        """
        if self._client is None:
            raise RuntimeError("Redis client not initialized.")

        if not as_generator:
            return list(self._client.hkeys(self.redis_hash_key))

        def _gen() -> Iterator[str]:
            cursor = 0
            while True:
                cursor, batch = self._client.hscan(self.redis_hash_key, cursor=cursor)
                for k in batch.keys():
                    yield k
                if cursor == 0:
                    break

        return _gen()
