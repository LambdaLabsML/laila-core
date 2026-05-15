"""AWS S3 pool implementation."""
from __future__ import annotations

from typing import Optional
from pydantic import Field

from ..boto.boto import BotoPool

try:
    import boto3
    from botocore.config import Config as BotocoreConfig
except ImportError:
    boto3 = None  # type: ignore
    BotocoreConfig = None  # type: ignore

try:
    import aioboto3
except ImportError:
    aioboto3 = None  # type: ignore


class S3Pool(BotoPool):
    """
    AWS S3-backed pool. Uploads and downloads key-value data from S3.

    Uses boto3 S3 API for sync paths and ``aioboto3`` for async paths
    (true non-blocking I/O via aiohttp under the hood). Objects are
    keyed by entry global_id directly. Use one bucket per pool.
    """

    access_key_id: Optional[str] = Field(default=None)
    secret_access_key: Optional[str] = Field(default=None)
    region_name: Optional[str] = Field(default=None)

    def _get_client(self):
        """Return a boto3 S3 client configured for AWS S3."""
        if self._client is not None:
            return self._client
        if boto3 is None or BotocoreConfig is None:
            raise ImportError("boto3 is required for S3Pool")
        kwargs = {
            "config": BotocoreConfig(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
                max_pool_connections=self.max_pool_connections,
            ),
        }
        if self.access_key_id is not None:
            kwargs["aws_access_key_id"] = self.access_key_id
        if self.secret_access_key is not None:
            kwargs["aws_secret_access_key"] = self.secret_access_key
        if self.region_name is not None:
            kwargs["region_name"] = self.region_name
        self._client = boto3.client("s3", **kwargs)
        return self._client

    def _get_aio_session(self):
        """Return an :class:`aioboto3.Session` carrying this pool's AWS credentials."""
        if aioboto3 is None:
            raise ImportError(
                "aioboto3 is required for the async S3 path; install with `pip install aioboto3`"
            )
        if self._aio_session is None:
            kwargs = {}
            if self.access_key_id is not None:
                kwargs["aws_access_key_id"] = self.access_key_id
            if self.secret_access_key is not None:
                kwargs["aws_secret_access_key"] = self.secret_access_key
            if self.region_name is not None:
                kwargs["region_name"] = self.region_name
            self._aio_session = aioboto3.Session(**kwargs)
        return self._aio_session
