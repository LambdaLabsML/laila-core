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


class BotoPool(_LAILA_IDENTIFIABLE_POOL):
    """
    Abstract base for pools backed by a boto3 S3-compatible client.

    Subclasses must define their own fields and implement ``_get_client``
    to return a configured ``boto3`` S3 client.
    """

    bucket_name: str = Field(...)
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)

    _client: Any = PrivateAttr(default=None)

    _no_such_key_codes: set[str] = {"NoSuchKey"}

    class Config:
        arbitrary_types_allowed = True

    def _get_client(self):
        raise NotImplementedError("Subclasses must implement _get_client")

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
                if e.response.get("Error", {}).get("Code") in self._no_such_key_codes:
                    return None
                raise

    def __setitem__(self, key: str, entry: Any) -> None:
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError(f"{type(self).__name__} expects a serialized JSON string.")

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
