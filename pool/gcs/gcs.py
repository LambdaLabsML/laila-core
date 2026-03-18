from __future__ import annotations

from typing import Optional, Any, Iterable, Iterator
from pydantic import Field, PrivateAttr
from urllib.parse import quote, unquote
import json

from ..schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry.compdata.transformation import TransformationSequence
from ...entry import transformation_base64

try:
    from google.cloud import storage
    from google.oauth2 import service_account
    from google.api_core.exceptions import NotFound
except ImportError:
    storage = None  # type: ignore
    service_account = None  # type: ignore
    NotFound = Exception  # type: ignore


class GCSPool(_LAILA_IDENTIFIABLE_POOL):
    service_account_info: Optional[dict[str, Any]] = Field(default=None)
    project_id: Optional[str] = Field(default=None)
    bucket_name: str = Field(...)
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)

    _client: Any = PrivateAttr(default=None)
    _bucket: Any = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

    def _get_client(self):
        if self._client is not None:
            return self._client
        if storage is None or service_account is None:
            raise ImportError("google-cloud-storage is required for GCSPool")

        kwargs: dict[str, Any] = {}
        if self.service_account_info is not None:
            credentials = service_account.Credentials.from_service_account_info(
                self.service_account_info
            )
            kwargs["credentials"] = credentials
            if self.project_id is None:
                self.project_id = self.service_account_info.get("project_id")

        if self.project_id is not None:
            kwargs["project"] = self.project_id

        self._client = storage.Client(**kwargs)
        return self._client

    def _get_bucket(self):
        if self._bucket is None:
            self._bucket = self._get_client().bucket(self.bucket_name)
        return self._bucket

    def _object_key(self, key: str) -> str:
        return quote(key, safe="")

    def _logical_key(self, object_key: str) -> str:
        return unquote(object_key)

    def close(self) -> None:
        self._client = None
        self._bucket = None

    def __getitem__(self, key: str) -> Optional[Any]:
        with self.atomic():
            blob = self._get_bucket().blob(self._object_key(key))
            try:
                raw = blob.download_as_text()
            except NotFound:
                return None
        return json.loads(raw)

    def __setitem__(self, key: str, entry: Any) -> None:
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("GCSPool expects a serialized JSON string.")

        with self.atomic():
            blob = self._get_bucket().blob(self._object_key(key))
            blob.upload_from_string(value, content_type="application/json")

    def __delitem__(self, key: str) -> None:
        with self.atomic():
            blob = self._get_bucket().blob(self._object_key(key))
            try:
                blob.delete()
            except NotFound:
                return

    def empty(self) -> None:
        with self.atomic():
            blobs = list(self._get_client().list_blobs(self.bucket_name))
            for blob in blobs:
                blob.delete()

    def exists(self, key: str) -> bool:
        with self.atomic():
            blob = self._get_bucket().blob(self._object_key(key))
            return bool(blob.exists(self._get_client()))

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def keys(self, as_generator: bool = False) -> Iterable[str]:
        def _iter_keys() -> Iterator[str]:
            for blob in self._get_client().list_blobs(self.bucket_name):
                yield self._logical_key(blob.name)

        if not as_generator:
            with self.atomic():
                return list(_iter_keys())
        return _iter_keys()
