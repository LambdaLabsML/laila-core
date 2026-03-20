from __future__ import annotations

from pydantic import Field

from ..boto.boto import BotoPool

try:
    import boto3
    from botocore.config import Config as BotocoreConfig
except ImportError:
    boto3 = None  # type: ignore
    BotocoreConfig = None  # type: ignore


class BackblazePool(BotoPool):
    """
    Backblaze B2-backed pool using the S3-compatible API.
    """

    application_key_id: str = Field(...)
    application_key: str = Field(...)
    endpoint_url: str = Field(default="https://s3.us-west-004.backblazeb2.com")

    _no_such_key_codes: set[str] = {"NoSuchKey", "404"}

    def _get_client(self):
        if self._client is not None:
            return self._client
        if boto3 is None or BotocoreConfig is None:
            raise ImportError("boto3 is required for BackblazePool")
        self._client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.application_key_id,
            aws_secret_access_key=self.application_key,
            config=BotocoreConfig(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )
        return self._client
