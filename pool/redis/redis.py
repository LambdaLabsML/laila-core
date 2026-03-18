from __future__ import annotations

from typing import Optional, Any, Iterable, Iterator
from contextlib import contextmanager, suppress
from pydantic import Field, PrivateAttr
import os
import subprocess
import time
import atexit
import hashlib
import json
import redis

from ..schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry.compdata.transformation import TransformationSequence
from ...entry import transformation_base64

class RedisPool(_LAILA_IDENTIFIABLE_POOL):
    """
    Redis-backed Pool using a private redis-server over a UNIX socket.

    Persistence:
      - Data directory: ~/.laila/pools/redis_data/<pool_id>/
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
    redis_password: Optional[str] = Field(default=None)

    # Behavior
    server_start_timeout_s: float = Field(default=3.0)
    lock_timeout: int = Field(default=30)

    # Runtime state (public / inspectable)
    redis_dir: Optional[str] = Field(default=None)
    client: Optional[redis.Redis] = Field(default=None)

    
    # Internal-only state (proper Pydantic defaults)
    _redis_proc: Optional[subprocess.Popen] = PrivateAttr(default=None)
    _db_dump_name: str = PrivateAttr(default="laila_pool.rdb")
    _redis_socket_name: str = PrivateAttr(default="laila_redis.sock")

    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)


    class Config:
        arbitrary_types_allowed = True

    # ---------------- lifecycle ----------------
    def model_post_init(self, __context: Any) -> None:
        """
        Always:
          1) establish redis_dir from pool_id
          2) start redis-server (unix socket only)
          3) connect client
        """
        base_dir = os.path.expanduser("~/.laila/pools/redis_data")
        self.redis_dir = os.path.join(base_dir, self.pool_id)
        os.makedirs(self.redis_dir, exist_ok=True)

        # Remove stale socket file (do NOT handle orphan processes yet)
        with suppress(FileNotFoundError):
            os.remove(self._redis_socket_path)

        self._start_redis_server()

        self.client = redis.Redis(
            unix_socket_path=self._redis_socket_path,
            password=self.redis_password,
            decode_responses=True,
        )

        atexit.register(self.close)

    def __del__(self) -> None:
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

        if self.client is not None:
            with suppress(Exception):
                self.client.shutdown(save=True)

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
        if not self.redis_dir:
            raise RuntimeError("Redis server not fully configured (missing redis_dir)")

        cmd = [
            "redis-server",
            "--port", "0",  # disable TCP
            "--unixsocket", self._redis_socket_path,
            "--unixsocketperm", "700",
            "--dir", self.redis_dir,
            "--dbfilename", self._db_dump_name,
            "--save", "900 1",
            "--save", "300 10",
            "--save", "60 10000",
            "--appendonly", "no",
            "--protected-mode", "yes",
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
        last_err: Optional[Exception] = None

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
        # UNIX domain socket paths must be shorter than ~108 chars on Linux.
        # Derive a short, deterministic path from pool_id.
        digest = hashlib.sha1(self.pool_id.encode("utf-8")).hexdigest()[:16]
        return f"/tmp/laila_redis_{digest}.sock"

    # ---------------- key helpers ----------------
    @property
    def redis_hash_key(self) -> str:
        return f"{self.key_prefix}:{self.pool_id}"

    @property
    def redis_lock_key(self) -> str:
        return f"{self.lock_prefix}:{self.pool_id}"


    def __getitem__(self, key: str) -> Optional[Any]:
        if self.client is None:
            raise RuntimeError("Redis client not initialized.")
        value = self.client.hget(self.redis_hash_key, key)

        return json.loads(value) if value is not None else None

    def __setitem__(self, key: str, entry: Any) -> None:
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if self.client is None:
            raise RuntimeError("Redis client not initialized.")
        if not isinstance(value, str):
            raise TypeError("RedisPool expects a serialized JSON string.")
        self.client.hset(self.redis_hash_key, key, value)

    def __delitem__(self, key: str) -> None:
        if self.client is None:
            raise RuntimeError("Redis client not initialized.")
        self.client.hdel(self.redis_hash_key, key)

    def empty(self) -> None:
        """Remove all entries from the pool (deletes the Redis hash)."""
        if self.client is None:
            raise RuntimeError("Redis client not initialized.")
        self.client.delete(self.redis_hash_key)

    def exists(self, key: str) -> bool:
        if self.client is None:
            raise RuntimeError("Redis client not initialized.")
        return bool(self.client.hexists(self.redis_hash_key, key))

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def keys(self, as_generator: bool = False) -> Iterable[str]:
        if self.client is None:
            raise RuntimeError("Redis client not initialized.")

        if not as_generator:
            return list(self.client.hkeys(self.redis_hash_key))

        def _gen() -> Iterator[str]:
            cursor = 0
            while True:
                cursor, batch = self.client.hscan(self.redis_hash_key, cursor=cursor)
                for k in batch.keys():
                    yield k
                if cursor == 0:
                    break

        return _gen()
