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


class S3Pool(BotoPool):
    """
    AWS S3-backed pool. Uploads and downloads key-value data from S3.

    Uses boto3 S3 API. Objects are keyed by entry global_id directly.
    Use one bucket per pool.
    """

    access_key_id: Optional[str] = Field(default=None)
    secret_access_key: Optional[str] = Field(default=None)
    region_name: Optional[str] = Field(default=None)

    def _get_client(self):
        if self._client is not None:
            return self._client
        if boto3 is None or BotocoreConfig is None:
            raise ImportError("boto3 is required for S3Pool")
        kwargs = {
            "config": BotocoreConfig(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
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
