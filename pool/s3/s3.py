from __future__ import annotations

from typing import Optional, Any, Iterable, Iterator
from pydantic import Field, PrivateAttr
from urllib.parse import quote, unquote
import json

from ..schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry.compdata.transformation import TransformationSequence
from ...entry import transformation_base64

try:
    import boto3
    from botocore.config import Config as BotocoreConfig
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None  # type: ignore
    BotocoreConfig = None  # type: ignore
    ClientError = Exception  # type: ignore


class S3Pool(_LAILA_IDENTIFIABLE_POOL):
    """
    AWS S3-backed pool. Uploads and downloads key-value data from S3.

    Uses boto3 S3 API. Objects are keyed by entry global_id directly.
    Use one bucket per pool.
    """

    access_key_id: Optional[str] = Field(default=None)
    secret_access_key: Optional[str] = Field(default=None)
    region_name: Optional[str] = Field(default=None)
    bucket_name: str = Field(...)
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)

    _client: Any = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

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

    def _object_key(self, key: str) -> str:
        return quote(key, safe="")

    def _logical_key(self, object_key: str) -> str:
        return unquote(object_key)

    def close(self) -> None:
        self._client = None

    def __getitem__(self, key: str) -> Optional[Any]:
        with self.atomic():
            try:
                resp = self._get_client().get_object(
                    Bucket=self.bucket_name,
                    Key=self._object_key(key),
                )
                raw = resp["Body"].read().decode("utf-8")
                return json.loads(raw)
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "NoSuchKey":
                    return None
                raise

    def __setitem__(self, key: str, entry: Any) -> None:
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("S3Pool expects a serialized JSON string.")

        with self.atomic():
            self._get_client().put_object(
                Bucket=self.bucket_name,
                Key=self._object_key(key),
                Body=value.encode("utf-8"),
                ContentType="application/json",
            )

    def __delitem__(self, key: str) -> None:
        with self.atomic():
            self._get_client().delete_object(
                Bucket=self.bucket_name,
                Key=self._object_key(key),
            )

    def empty(self) -> None:
        """Remove all entries from the pool."""
        paginator = self._get_client().get_paginator("list_objects_v2")
        with self.atomic():
            for page in paginator.paginate(Bucket=self.bucket_name):
                for obj in page.get("Contents", []):
                    self._get_client().delete_object(
                        Bucket=self.bucket_name,
                        Key=obj["Key"],
                    )

    def exists(self, key: str) -> bool:
        with self.atomic():
            try:
                self._get_client().head_object(
                    Bucket=self.bucket_name,
                    Key=self._object_key(key),
                )
                return True
            except Exception:
                return False

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def keys(self, as_generator: bool = False) -> Iterable[str]:
        paginator = self._get_client().get_paginator("list_objects_v2")

        def _iter_keys() -> Iterator[str]:
            for page in paginator.paginate(Bucket=self.bucket_name):
                for obj in page.get("Contents", []):
                    yield self._logical_key(obj["Key"])

        if not as_generator:
            return list(_iter_keys())
        return _iter_keys()
