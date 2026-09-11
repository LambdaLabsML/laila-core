"""Cloudflare R2 pool implementation."""

from __future__ import annotations

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


class CloudflarePool(BotoPool):
    """
    Cloudflare R2-backed pool. Uploads and downloads key-value data from R2.

    Uses boto3 S3 API with R2 endpoint. Objects are keyed by entry global_id
    directly (no pool directory). Use one bucket per pool.
    """

    account_id: str = Field(...)
    access_key_id: str = Field(...)
    secret_access_key: str = Field(...)

    @property
    def endpoint_url(self) -> str:
        """The R2 S3-compatible endpoint derived from ``account_id``."""
        return f"https://{self.account_id}.r2.cloudflarestorage.com"

    def _get_client(self):
        """Return a boto3 S3 client configured for Cloudflare R2."""
        if self._client is not None:
            return self._client
        if boto3 is None or BotocoreConfig is None:
            raise ImportError("boto3 is required for CloudflarePool")
        self._client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            config=BotocoreConfig(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )
        return self._client

    def _get_aio_session(self):
        """Return a cached :class:`aioboto3.Session` carrying the R2 API token."""
        if aioboto3 is None:
            raise ImportError(
                "aioboto3 is required for the async R2 path; install with `pip install aioboto3`"
            )
        if self._aio_session is None:
            self._aio_session = aioboto3.Session(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
            )
        return self._aio_session

    def _aio_client_kwargs(self):
        """Point the aioboto3 client at the R2 endpoint instead of AWS."""
        return {"endpoint_url": self.endpoint_url}
