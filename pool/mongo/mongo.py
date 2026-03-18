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
        arbitrary_types_allowed = True

    def model_post_init(self, __context: Any) -> None:
        self._client = self._connect()
        self._collection().create_index([("pool_id", 1), ("key", 1)], unique=True)
        atexit.register(self.close)

    def _connect(self):
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
        if self._mongo_dir is not None:
            return
        base_dir = os.path.expanduser("~/.laila/pools")
        self._mongo_dir = os.path.join(base_dir, "mongo_data", self.pool_id)
        os.makedirs(self._mongo_dir, exist_ok=True)
        self.port = 30000 + (int(hashlib.sha1(self.pool_id.encode("utf-8")).hexdigest()[:8], 16) % 20000)
        self.host = "127.0.0.1"

    @property
    def _local_uri(self) -> str:
        if self.host is None:
            raise RuntimeError("Local Mongo host is not configured.")
        return f"mongodb://{self.host}:{self.port}/"

    def _ensure_local_server(self) -> None:
        if self._local_server_available():
            self._owns_local_server = False
            return
        self._start_local_server()

    def _local_server_available(self) -> bool:
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
        if self._client is None:
            raise RuntimeError("MongoPool is closed.")
        return self._client[self.dbname]

    def _collection(self):
        return self._db()["laila_pool_entries"]

    def close(self) -> None:
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

    def __getitem__(self, key: str) -> Optional[Any]:
        with self.atomic():
            doc = self._collection().find_one({"pool_id": self.pool_id, "key": key}, {"_id": 0, "value": 1})
        return json.loads(doc["value"]) if doc is not None else None

    def __setitem__(self, key: str, entry: Any) -> None:
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("MongoPool expects a serialized JSON string.")

        with self.atomic():
            self._collection().update_one(
                {"pool_id": self.pool_id, "key": key},
                {"$set": {"value": value}},
                upsert=True,
            )

    def __delitem__(self, key: str) -> None:
        with self.atomic():
            self._collection().delete_one({"pool_id": self.pool_id, "key": key})

    def empty(self) -> None:
        with self.atomic():
            self._collection().delete_many({"pool_id": self.pool_id})

    def exists(self, key: str) -> bool:
        with self.atomic():
            return self._collection().count_documents({"pool_id": self.pool_id, "key": key}, limit=1) > 0

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def keys(self, as_generator: bool = False) -> Iterable[str]:
        def _iter_keys() -> Iterator[str]:
            cursor = self._collection().find(
                {"pool_id": self.pool_id},
                {"_id": 0, "key": 1},
            ).sort("key", 1)
            for doc in cursor:
                yield doc["key"]

        if not as_generator:
            with self.atomic():
                return list(_iter_keys())
        return _iter_keys()
