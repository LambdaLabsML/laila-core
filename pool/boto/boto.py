"""Abstract boto3-based S3-compatible pool implementation.

:class:`BotoPool` is the shared chassis used by every S3-API-compatible
backend in laila (AWS S3, Cloudflare R2, BackBlaze B2, ...).
Concrete subclasses provide the bucket-vendor-specific
``_get_client`` factory; everything else -- key encoding, throttling,
async-via-aioboto3, connection-pool sizing, multi-loop client
caching, and pool teardown -- is handled here.

Sync vs async paths
-------------------
Every method has both a sync (:meth:`_read`, :meth:`_write`, ...)
implementation that drives a regular boto3 client and an async
(:meth:`_read_async`, ...) implementation that drives an aioboto3
client when ``async_default`` is ``True``. With
``async_default=False`` the async paths fall back to the inherited
default of running the sync call inline on the calling event loop.

Per-loop async client caching
-----------------------------
``aioboto3`` clients are bound to the event loop they're created on,
so we cache one client *per loop* in :attr:`_aio_clients`. That keeps
the underlying aiohttp connector pool warm (and respects
``max_pool_connections``) without leaking clients across loops --
critical when laila spins up multiple async taskforces, each with
its own loop.

Throttling
----------
Optional simple per-call sleep (:meth:`_throttle` / :meth:`_athrottle`)
to keep the pool under a request-per-second cap. Defaults to no
throttling.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Iterable, Iterator
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import quote, unquote

from pydantic import ConfigDict, Field, PrivateAttr

from ...entry import transformation_base64
from ...entry.compdata.transformation import TransformationSequence
from ..schema.base import _LAILA_IDENTIFIABLE_POOL

try:
    import boto3
    from botocore.config import Config as BotocoreConfig
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None  # type: ignore
    BotocoreConfig = None  # type: ignore
    ClientError = Exception  # type: ignore

try:
    import aioboto3
except ImportError:
    aioboto3 = None  # type: ignore


class BotoPool(_LAILA_IDENTIFIABLE_POOL):
    """
    Abstract base for pools backed by a boto3 S3-compatible client.

    Subclasses must define their own fields and implement ``_get_client``
    to return a configured ``boto3`` S3 client.

    When ``async_default`` is ``True`` (the default) the async hooks
    (``_read_async`` / ``_write_async`` / ``_delete_async`` /
    ``_exists_async``) use ``aioboto3`` for true non-blocking I/O —
    hundreds of concurrent in-flight HTTP requests on a single thread
    via a shared aiohttp connection pool. When ``async_default`` is
    ``False``, the async hooks fall back to running the sync ``boto3``
    call inline on the calling loop (which blocks that loop for the
    duration of the HTTP round-trip).

    Connection-pool sizing
    ----------------------
    ``max_pool_connections`` is forwarded to ``botocore.config.Config``
    for both the sync and the async client, so a single client can hold
    open many concurrent HTTPS connections to S3 (botocore's default of
    10 is the bottleneck for high-fanout async workloads). For the async
    path, the ``aioboto3`` client is cached *per event loop*: every
    coroutine running on the same loop reuses one client, and therefore
    one warmed aiohttp connection pool, rather than constructing a new
    one per call.
    """

    bucket_name: str = Field(...)
    max_req_per_second: float | None = Field(default=None)
    transformations: TransformationSequence | None = Field(default=transformation_base64)
    async_default: bool = Field(
        default=True,
        description=(
            "When True, async hooks use aioboto3 (true non-blocking I/O via aiohttp). "
            "When False, async hooks fall back to sync boto3 inline, blocking the calling loop."
        ),
    )
    max_pool_connections: int = Field(
        default=128,
        ge=1,
        description=(
            "Maximum number of concurrent HTTPS connections the underlying "
            "botocore/aiohttp pool is allowed to keep open per client. "
            "Forwarded to botocore.config.Config(max_pool_connections=...). "
            "The botocore default is 10, which caps high-fanout async I/O."
        ),
    )

    _client: Any = PrivateAttr(default=None)
    _aio_session: Any = PrivateAttr(default=None)
    _aio_session_lock: Any = PrivateAttr(default=None)
    # Per-event-loop cached aioboto3 clients: loop_id -> (client, ctx_mgr, loop).
    # Cached in the loop's own thread, drained on close().
    _aio_clients: dict[int, tuple[Any, Any, Any]] = PrivateAttr(default_factory=dict)
    _aio_client_locks: dict[int, Any] = PrivateAttr(default_factory=dict)

    _no_such_key_codes: set[str] = {"NoSuchKey"}

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def _get_client(self):
        """Return a configured boto3 S3 client.

        Raises
        ------
        NotImplementedError
            Must be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement _get_client")

    def _get_aio_session(self):
        """Return (and lazily build) the cached :class:`aioboto3.Session`.

        Subclasses may override to inject credentials. The default
        constructs a session with no explicit credentials so the standard
        AWS credential chain (env vars, ~/.aws/credentials, instance role)
        applies.
        """
        if aioboto3 is None:
            raise ImportError(
                "aioboto3 is required for the async path; install with `pip install aioboto3`"
            )
        if self._aio_session is None:
            self._aio_session = aioboto3.Session()
        return self._aio_session

    def _aio_client_config(self):
        """Return the ``BotocoreConfig`` used for every aioboto3 client.

        Honors ``max_pool_connections`` so the underlying aiohttp pool is
        sized to actually accommodate the configured concurrency.
        """
        return BotocoreConfig(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
            max_pool_connections=self.max_pool_connections,
        )

    @asynccontextmanager
    async def _aio_client(self):
        """Yield a shared aioboto3 S3 client cached on the running event loop.

        The first call from a given loop enters an ``async with
        session.client("s3", ...)`` context and stores the client in
        ``_aio_clients``; subsequent calls on the same loop reuse it
        directly so the aiohttp connection pool stays warm and obeys the
        configured ``max_pool_connections`` cap. The client is *not*
        torn down on context exit -- ``close()`` is responsible for
        draining all cached clients.
        """
        client = await self._get_shared_aio_client()
        yield client

    async def _get_shared_aio_client(self):
        """Return (lazily creating) the aioboto3 S3 client for the current loop."""
        loop = asyncio.get_running_loop()
        loop_id = id(loop)

        cached = self._aio_clients.get(loop_id)
        if cached is not None:
            return cached[0]

        # asyncio.Lock is bound to the loop it's first awaited on; create
        # one per loop. setdefault is GIL-atomic so two coroutines on the
        # same loop will agree on the same lock instance.
        lock = self._aio_client_locks.get(loop_id)
        if lock is None:
            lock = asyncio.Lock()
            self._aio_client_locks[loop_id] = lock

        async with lock:
            cached = self._aio_clients.get(loop_id)
            if cached is not None:
                return cached[0]

            session = self._get_aio_session()
            ctx = session.client("s3", config=self._aio_client_config())
            client = await ctx.__aenter__()
            self._aio_clients[loop_id] = (client, ctx, loop)
            return client

    def _object_key(self, key: str) -> str:
        """URL-encode a logical key for S3."""
        return quote(key, safe="")

    def _logical_key(self, object_key: str) -> str:
        """Decode an S3 object key back to the logical key."""
        return unquote(object_key)

    def _throttle(self) -> None:
        """Sleep to enforce the per-pool request rate cap."""
        if self.max_req_per_second is not None:
            time.sleep(1.0 / self.max_req_per_second)

    async def _athrottle(self) -> None:
        """Async sleep variant of :meth:`_throttle` — yields to the event loop."""
        if self.max_req_per_second is not None:
            await asyncio.sleep(1.0 / self.max_req_per_second)

    def close(self) -> None:
        """Release the boto3 client and tear down all cached aioboto3 clients.

        Each cached aioboto3 client lives on its own event loop, so we
        schedule ``ctx.__aexit__`` back onto that loop via
        :func:`asyncio.run_coroutine_threadsafe`. If the loop is no
        longer running (e.g. ``laila.terminate`` shut the taskforces
        first), fall back to draining the client on a private throwaway
        loop so the aiohttp connector doesn't leak with an
        ``Unclosed connector`` warning.
        """
        self._client = None
        for loop_id, (client, ctx, loop) in list(self._aio_clients.items()):
            try:
                if loop is not None and loop.is_running():
                    fut = asyncio.run_coroutine_threadsafe(ctx.__aexit__(None, None, None), loop)
                    try:
                        fut.result(timeout=5)
                    except Exception:
                        pass
                else:
                    # Loop is already stopped; the aioboto3 client+aiohttp
                    # connector still hold sockets. Drain on a fresh loop
                    # so we close cleanly instead of leaking.
                    try:
                        tmp_loop = asyncio.new_event_loop()
                        try:
                            tmp_loop.run_until_complete(ctx.__aexit__(None, None, None))
                        finally:
                            tmp_loop.close()
                    except Exception:
                        pass
            except Exception:
                pass
        self._aio_clients.clear()
        self._aio_client_locks.clear()
        self._aio_session = None

    def _read(self, key: str) -> Any | None:
        """Retrieve the JSON value for *key*, or ``None`` if absent."""
        self._throttle()
        try:
            resp = self._get_client().get_object(
                Bucket=self.bucket_name,
                Key=self._object_key(key),
            )
            raw = resp["Body"].read().decode("utf-8")
            return json.loads(raw)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in self._no_such_key_codes:
                return None
            raise

    def _write(self, key: str, entry: Any) -> None:
        """Store *entry* as a JSON object under *key*."""
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError(f"{type(self).__name__} expects a serialized JSON string.")

        self._throttle()
        self._get_client().put_object(
            Bucket=self.bucket_name,
            Key=self._object_key(key),
            Body=value.encode("utf-8"),
            ContentType="application/json",
        )

    def _delete(self, key: str) -> None:
        """Delete the object for *key*."""
        self._throttle()
        self._get_client().delete_object(
            Bucket=self.bucket_name,
            Key=self._object_key(key),
        )

    def _empty(self) -> None:
        """Remove all entries from the pool."""
        paginator = self._get_client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name):
            for obj in page.get("Contents", []):
                self._throttle()
                self._get_client().delete_object(
                    Bucket=self.bucket_name,
                    Key=obj["Key"],
                )

    def _exists(self, key: str) -> bool:
        """Return ``True`` if *key* exists in the bucket."""
        self._throttle()
        try:
            self._get_client().head_object(
                Bucket=self.bucket_name,
                Key=self._object_key(key),
            )
            return True
        except Exception:
            return False

    # -------- Async hooks: true async via aioboto3 when async_default=True --------

    async def _read_async(self, key: str) -> Any | None:
        """Retrieve the JSON value for *key* asynchronously, or ``None`` if absent."""
        if not self.async_default or aioboto3 is None:
            return await super()._read_async(key)
        await self._athrottle()
        try:
            async with self._aio_client() as client:
                resp = await client.get_object(
                    Bucket=self.bucket_name,
                    Key=self._object_key(key),
                )
                body = await resp["Body"].read()
                return json.loads(body.decode("utf-8"))
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in self._no_such_key_codes:
                return None
            raise

    async def _write_async(self, key: str, entry: Any) -> None:
        """Store *entry* as a JSON object under *key* asynchronously."""
        if not self.async_default or aioboto3 is None:
            return await super()._write_async(key, entry)
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError(f"{type(self).__name__} expects a serialized JSON string.")
        await self._athrottle()
        async with self._aio_client() as client:
            await client.put_object(
                Bucket=self.bucket_name,
                Key=self._object_key(key),
                Body=value.encode("utf-8"),
                ContentType="application/json",
            )

    async def _delete_async(self, key: str) -> None:
        """Delete the object for *key* asynchronously."""
        if not self.async_default or aioboto3 is None:
            return await super()._delete_async(key)
        await self._athrottle()
        async with self._aio_client() as client:
            await client.delete_object(
                Bucket=self.bucket_name,
                Key=self._object_key(key),
            )

    async def _exists_async(self, key: str) -> bool:
        """Return ``True`` if *key* exists in the bucket (async)."""
        if not self.async_default or aioboto3 is None:
            return await super()._exists_async(key)
        await self._athrottle()
        try:
            async with self._aio_client() as client:
                await client.head_object(
                    Bucket=self.bucket_name,
                    Key=self._object_key(key),
                )
                return True
        except Exception:
            return False

    def __contains__(self, key: str) -> bool:
        """Check membership, delegates to :meth:`_exists`."""
        return self._exists(key)

    def _keys(self, as_generator: bool = False) -> Iterable[str]:
        """Return all keys in the bucket.

        Parameters
        ----------
        as_generator : bool, optional
            If ``True``, return a lazy iterator instead of a list.

        Returns
        -------
        Iterable[str]
            Pool keys.
        """
        paginator = self._get_client().get_paginator("list_objects_v2")

        def _iter_keys() -> Iterator[str]:
            for page in paginator.paginate(Bucket=self.bucket_name):
                for obj in page.get("Contents", []):
                    yield self._logical_key(obj["Key"])

        if not as_generator:
            return list(_iter_keys())
        return _iter_keys()
