"""Azure Blob Storage pool implementation."""
from __future__ import annotations

from typing import Optional, Any, Iterable, Iterator
from pydantic import Field, PrivateAttr
from urllib.parse import quote, unquote
import json

from ..schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry.compdata.transformation import TransformationSequence
from ...entry import transformation_base64

try:
    from azure.storage.blob import BlobServiceClient
    from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
except ImportError:
    BlobServiceClient = None  # type: ignore
    ResourceNotFoundError = Exception  # type: ignore
    ResourceExistsError = Exception  # type: ignore


class AzurePool(_LAILA_IDENTIFIABLE_POOL):
    """Azure Blob Storage-backed pool.

    Each entry is stored as a JSON blob in the configured container.
    Keys are URL-encoded to avoid path-separator issues.
    """

    connection_string: str = Field(...)
    container_name: str = Field(...)
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)

    _client: Any = PrivateAttr(default=None)
    _container: Any = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

    def _get_client(self):
        """Return the ``BlobServiceClient``, creating it on first call."""
        if self._client is not None:
            return self._client
        if BlobServiceClient is None:
            raise ImportError("azure-storage-blob is required for AzurePool")
        self._client = BlobServiceClient.from_connection_string(self.connection_string)
        return self._client

    def _get_container(self):
        """Return the container client, creating the container if needed."""
        if self._container is None:
            self._container = self._get_client().get_container_client(self.container_name)
            try:
                self._container.create_container()
            except ResourceExistsError:
                pass
        return self._container

    def _object_key(self, key: str) -> str:
        """URL-encode a logical key for blob storage."""
        return quote(key, safe="")

    def _logical_key(self, object_key: str) -> str:
        """Decode a blob name back to the logical key."""
        return unquote(object_key)

    def close(self) -> None:
        """Release client references."""
        self._client = None
        self._container = None

    def _read(self, key: str) -> Optional[Any]:
        """Retrieve the JSON value for *key*, or ``None`` if absent."""
        with self.atomic():
            blob = self._get_container().get_blob_client(self._object_key(key))
            try:
                raw = blob.download_blob().readall().decode("utf-8")
            except ResourceNotFoundError:
                return None
        return json.loads(raw)

    def _write(self, key: str, entry: Any) -> None:
        """Store *entry* as a JSON blob under *key*."""
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("AzurePool expects a serialized JSON string.")

        with self.atomic():
            blob = self._get_container().get_blob_client(self._object_key(key))
            blob.upload_blob(value.encode("utf-8"), overwrite=True, content_type="application/json")

    def _delete(self, key: str) -> None:
        """Delete the blob for *key*; no-op if absent."""
        with self.atomic():
            blob = self._get_container().get_blob_client(self._object_key(key))
            try:
                blob.delete_blob()
            except ResourceNotFoundError:
                return

    def _empty(self) -> None:
        """Remove all blobs from the container."""
        with self.atomic():
            blobs = list(self._get_container().list_blobs())
            for blob in blobs:
                self._get_container().delete_blob(blob.name)

    def _exists(self, key: str) -> bool:
        """Return ``True`` if a blob for *key* exists."""
        with self.atomic():
            blob = self._get_container().get_blob_client(self._object_key(key))
            return bool(blob.exists())

    def __contains__(self, key: str) -> bool:
        """Check membership, delegates to :meth:`_exists`."""
        return self._exists(key)

    def _keys(self, as_generator: bool = False) -> Iterable[str]:
        """Return all keys in the container.

        Parameters
        ----------
        as_generator : bool, optional
            If ``True``, return a lazy iterator instead of a list.

        Returns
        -------
        Iterable[str]
            Pool keys.
        """
        def _iter_keys() -> Iterator[str]:
            for blob in self._get_container().list_blobs():
                yield self._logical_key(blob.name)

        if not as_generator:
            with self.atomic():
                return list(_iter_keys())
        return _iter_keys()
