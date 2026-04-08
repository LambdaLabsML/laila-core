"""MongoDB-backed pool implementation with optional managed local server."""
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
    from pymongo import MongoClient
    from pymongo.errors import ServerSelectionTimeoutError
except ImportError:
    MongoClient = None  # type: ignore
    ServerSelectionTimeoutError = Exception  # type: ignore


class MongoPool(_LAILA_IDENTIFIABLE_POOL):
    """MongoDB-backed pool.

    Can connect to an existing MongoDB via ``uri`` or ``host``/``port``,
    or automatically start and manage a local ``mongod`` process.
    """

    uri: Optional[str] = Field(default=None)
    host: Optional[str] = Field(default=None)
    port: int = Field(default=27017)
    dbname: str = Field(default="laila")
    server_start_timeout_s: float = Field(default=5.0)
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)

    _client: Any = PrivateAttr(default=None)
    _mongo_proc: Optional[subprocess.Popen] = PrivateAttr(default=None)
    _owns_local_server: bool = PrivateAttr(default=False)
    _mongo_dir: Optional[str] = PrivateAttr(default=None)

    class Config:
        """Pydantic model configuration."""

        arbitrary_types_allowed = True

    def model_post_init(self, __context: Any) -> None:
        """Connect to MongoDB and ensure the entries collection is indexed."""
        super().model_post_init(__context)
        self._client = self._connect()
        self._collection().create_index("key", unique=True)
        atexit.register(self.close)

    def _connect(self):
        """Establish a ``MongoClient`` connection."""
        if MongoClient is None:
            raise ImportError("pymongo is required for MongoPool")
        if self.uri is not None:
            return MongoClient(self.uri, serverSelectionTimeoutMS=1000)
        if self.host is None:
            self._configure_local_server()
            self._ensure_local_server()
            return MongoClient(self._local_uri, serverSelectionTimeoutMS=1000)
        return MongoClient(
            host=self.host,
            port=self.port,
            serverSelectionTimeoutMS=1000,
        )

    def _configure_local_server(self) -> None:
        """Set up directory and port for a managed local mongod."""
        if self._mongo_dir is not None:
            return
        from ...macros.defaults import LAILA_DEFAULT_DIRECTORIES
        pool_dir = os.path.join(LAILA_DEFAULT_DIRECTORIES["pools"], self.uuid)
        self._mongo_dir = pool_dir
        os.makedirs(self._mongo_dir, exist_ok=True)
        self.port = 30000 + (int(hashlib.sha1(self.pool_id.encode("utf-8")).hexdigest()[:8], 16) % 20000)
        self.host = "127.0.0.1"

    @property
    def _local_uri(self) -> str:
        """MongoDB connection URI for the managed local server."""
        if self.host is None:
            raise RuntimeError("Local Mongo host is not configured.")
        return f"mongodb://{self.host}:{self.port}/"

    def _ensure_local_server(self) -> None:
        """Start a local mongod if one is not already reachable."""
        if self._local_server_available():
            self._owns_local_server = False
            return
        for attempt in range(5):
            try:
                self._start_local_server()
                return
            except RuntimeError as exc:
                if "code=48" not in str(exc):
                    raise
                time.sleep(1)
                if self._local_server_available():
                    self._owns_local_server = False
                    return
                self.port += 1

    def _local_server_available(self) -> bool:
        """Return ``True`` if the local mongod responds to a ping."""
        if MongoClient is None:
            return False
        try:
            client = MongoClient(self._local_uri, serverSelectionTimeoutMS=500)
            client.admin.command("ping")
            client.close()
            return True
        except Exception:
            return False

    def _start_local_server(self) -> None:
        """Launch a ``mongod`` subprocess and wait for readiness."""
        if self._mongo_dir is None or self.host is None:
            raise RuntimeError("Local Mongo server is not fully configured.")
        cmd = [
            "mongod",
            "--dbpath",
            self._mongo_dir,
            "--port",
            str(self.port),
            "--bind_ip",
            self.host,
            "--quiet",
        ]
        self._mongo_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self._owns_local_server = True

        deadline = time.time() + self.server_start_timeout_s
        last_err: Optional[Exception] = None
        while time.time() < deadline:
            if self._mongo_proc.poll() is not None:
                exit_code = self._mongo_proc.poll()
                stdout = ""
                stderr = ""
                if self._mongo_proc.stdout:
                    stdout = self._mongo_proc.stdout.read() or ""
                if self._mongo_proc.stderr:
                    stderr = self._mongo_proc.stderr.read() or ""
                self.close()
                raise RuntimeError(
                    f"mongod exited early (code={exit_code}). Stdout: {stdout.strip()} Stderr: {stderr.strip()}"
                )
            try:
                client = MongoClient(self._local_uri, serverSelectionTimeoutMS=500)
                client.admin.command("ping")
                client.close()
                return
            except Exception as exc:
                last_err = exc
                time.sleep(0.1)

        stdout = ""
        stderr = ""
        if self._mongo_proc and self._mongo_proc.stdout:
            stdout = self._mongo_proc.stdout.read() or ""
        if self._mongo_proc and self._mongo_proc.stderr:
            stderr = self._mongo_proc.stderr.read() or ""
        self.close()
        raise RuntimeError(
            f"mongod failed to start for {self.pool_id}. Last error: {last_err}. Stdout: {stdout.strip()} Stderr: {stderr.strip()}"
        )

    def _db(self):
        """Return the active MongoDB database handle."""
        if self._client is None:
            raise RuntimeError("MongoPool is closed.")
        return self._client[self.dbname]

    def _collection(self):
        """Return the ``laila_pool_entries`` collection."""
        return self._db()["laila_pool_entries"]

    def close(self) -> None:
        """Close the client and terminate any managed mongod process."""
        if self._client is not None:
            self._client.close()
            self._client = None
        proc = self._mongo_proc
        self._mongo_proc = None
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

    def _read(self, key: str) -> Optional[Any]:
        """Retrieve the JSON value for *key*, or ``None`` if absent."""
        with self.atomic():
            doc = self._collection().find_one({"key": key}, {"_id": 0, "value": 1})
        return json.loads(doc["value"]) if doc is not None else None

    def _write(self, key: str, entry: Any) -> None:
        """Upsert *entry* under *key*."""
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("MongoPool expects a serialized JSON string.")

        with self.atomic():
            self._collection().update_one(
                {"key": key},
                {"$set": {"value": value}},
                upsert=True,
            )

    def _delete(self, key: str) -> None:
        """Delete the document for *key*."""
        with self.atomic():
            self._collection().delete_one({"key": key})

    def _empty(self) -> None:
        """Remove all documents from the entries collection."""
        with self.atomic():
            self._collection().delete_many({})

    def _exists(self, key: str) -> bool:
        """Return ``True`` if a document for *key* exists."""
        with self.atomic():
            return self._collection().count_documents({"key": key}, limit=1) > 0

    def __contains__(self, key: str) -> bool:
        """Check membership, delegates to :meth:`_exists`."""
        return self._exists(key)

    def _keys(self, as_generator: bool = False) -> Iterable[str]:
        """Return all keys in the collection.

        Parameters
        ----------
        as_generator : bool, optional
            If ``True``, return a lazy iterator instead of a list.

        Returns
        -------
        Iterable[str]
            Pool keys.
        """
        def _iter_keys() -> Iterator[str]:
            cursor = self._collection().find(
                {},
                {"_id": 0, "key": 1},
            ).sort("key", 1)
            for doc in cursor:
                yield doc["key"]

        if not as_generator:
            with self.atomic():
                return list(_iter_keys())
        return _iter_keys()
