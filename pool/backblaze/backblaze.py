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


class BackblazePool(_LAILA_IDENTIFIABLE_POOL):
    application_key_id: str = Field(...)
    application_key: str = Field(...)
    bucket_name: str = Field(...)
    endpoint_url: str = Field(default="https://s3.us-west-004.backblazeb2.com")
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)

    _client: Any = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

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
                if e.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
                    return None
                raise

    def __setitem__(self, key: str, entry: Any) -> None:
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("BackblazePool expects a serialized JSON string.")

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
