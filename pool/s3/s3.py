"""AWS S3 pool implementation.

Thin specialisation of :class:`BotoPool` that points at the AWS S3
endpoint with AWS-style v4 signatures. The bulk of the read/write/
delete logic lives in the parent class -- this file just plugs in
the right :func:`boto3.client` factory and the matching
:class:`aioboto3.Session` for the async path.
"""

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


class S3Pool(BotoPool):
    """AWS S3-backed pool.

    Uploads and downloads serialised entries against a single S3
    bucket -- one bucket per pool, objects keyed by entry
    ``global_id``. Sync paths use the standard :mod:`boto3` client;
    async paths (``_read_async`` / ``_write_async`` / ...) use
    :mod:`aioboto3` for true non-blocking I/O via ``aiohttp``.

    Parameters
    ----------
    access_key_id : str, optional
        AWS access key id. Falls back to the standard boto3 credential
        provider chain (env vars, ``~/.aws/credentials``, IAM role
        metadata, ...) when omitted.
    secret_access_key : str, optional
        AWS secret access key. See :attr:`access_key_id` for the
        fallback rules.
    region_name : str, optional
        AWS region for the bucket. When omitted, boto3 picks the
        region from its standard configuration sources.
    """

    access_key_id: str | None = Field(default=None)
    secret_access_key: str | None = Field(default=None)
    region_name: str | None = Field(default=None)

    def _get_client(self):
        """Return a cached :class:`boto3.client` instance for AWS S3.

        Configured with SigV4 signing, a three-attempt standard retry
        policy, and a connection pool sized by
        :attr:`max_pool_connections`. Subsequent calls return the
        cached client.
        """
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
        """Return a cached :class:`aioboto3.Session` carrying this pool's AWS credentials.

        Used by the async path. Lazy-imports :mod:`aioboto3` and
        raises :class:`ImportError` with an actionable hint if the
        optional dependency is missing.
        """
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
