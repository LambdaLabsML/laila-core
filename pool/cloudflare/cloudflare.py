from __future__ import annotations

from pydantic import Field

from ..boto.boto import BotoPool

try:
    import boto3
    from botocore.config import Config as BotocoreConfig
except ImportError:
    boto3 = None  # type: ignore
    BotocoreConfig = None  # type: ignore


class CloudflarePool(BotoPool):
    """
    Cloudflare R2-backed pool. Uploads and downloads key-value data from R2.

    Uses boto3 S3 API with R2 endpoint. Objects are keyed by entry global_id
    directly (no pool directory). Use one bucket per pool.
    """

    account_id: str = Field(...)
    access_key_id: str = Field(...)
    secret_access_key: str = Field(...)

    def _get_client(self):
        if self._client is not None:
            return self._client
        if boto3 is None or BotocoreConfig is None:
            raise ImportError("boto3 is required for CloudflarePool")
        endpoint = f"https://{self.account_id}.r2.cloudflarestorage.com"
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            config=BotocoreConfig(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )
        return self._client
