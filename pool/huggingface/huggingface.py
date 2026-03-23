"""Hugging Face Hub pool implementation."""
from __future__ import annotations

from typing import Optional, Any, Iterable, Iterator
from pydantic import Field, PrivateAttr
from urllib.parse import quote, unquote
import json

from ..schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry.compdata.transformation import TransformationSequence
from ...entry import transformation_base64

try:
    from huggingface_hub import HfApi, hf_hub_download
    from huggingface_hub.utils import EntryNotFoundError
except ImportError:
    HfApi = None  # type: ignore
    hf_hub_download = None  # type: ignore
    EntryNotFoundError = Exception  # type: ignore


class HuggingFacePool(_LAILA_IDENTIFIABLE_POOL):
    """
    Hugging Face Hub-backed pool.

    Stores each entry as one JSON object file in a Hub repo (public or private).
    Objects are keyed by entry global_id directly (URL-encoded).
    """

    repo_id: str = Field(...)
    repo_type: str = Field(default="model")
    revision: str = Field(default="main")
    token: Optional[str] = Field(default=None)
    path_prefix: str = Field(default="laila_pool")
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)

    _api: Any = PrivateAttr(default=None)

    class Config:
        """Pydantic model configuration."""

        arbitrary_types_allowed = True

    def _get_api(self):
        """Return the ``HfApi`` instance, creating it on first call."""
        if self._api is not None:
            return self._api
        if HfApi is None or hf_hub_download is None:
            raise ImportError("huggingface_hub is required for HuggingFacePool")
        self._api = HfApi(token=self.token)
        return self._api

    def _prefix(self) -> str:
        """Return the normalised path prefix (no leading/trailing slashes)."""
        return self.path_prefix.strip("/")

    def _object_key(self, key: str) -> str:
        """Build the in-repo file path for *key*."""
        encoded = quote(key, safe="")
        prefix = self._prefix()
        if prefix:
            return f"{prefix}/{encoded}.json"
        return f"{encoded}.json"

    def _logical_key(self, object_key: str) -> str:
        """Extract the logical key from an in-repo file path."""
        prefix = self._prefix()
        cleaned = object_key
        if prefix and cleaned.startswith(prefix + "/"):
            cleaned = cleaned[len(prefix) + 1 :]
        if cleaned.endswith(".json"):
            cleaned = cleaned[:-5]
        return unquote(cleaned)

    def close(self) -> None:
        """Release the API handle."""
        self._api = None

    def __getitem__(self, key: str) -> Optional[Any]:
        """Download and parse the JSON file for *key*, or return ``None``."""
        with self.atomic():
            try:
                path = hf_hub_download(
                    repo_id=self.repo_id,
                    filename=self._object_key(key),
                    repo_type=self.repo_type,
                    revision=self.revision,
                    token=self.token,
                )
                with open(path, "r", encoding="utf-8") as f:
                    raw = f.read()
                return json.loads(raw)
            except EntryNotFoundError:
                return None

    def __setitem__(self, key: str, entry: Any) -> None:
        """Upload *entry* as a JSON file to the Hub repo."""
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("HuggingFacePool expects a serialized JSON string.")

        with self.atomic():
            self._get_api().upload_file(
                path_or_fileobj=value.encode("utf-8"),
                path_in_repo=self._object_key(key),
                repo_id=self.repo_id,
                repo_type=self.repo_type,
                revision=self.revision,
                commit_message=f"laila: set {key}",
            )

    def __delitem__(self, key: str) -> None:
        """Delete the file for *key* from the Hub repo; no-op if absent."""
        with self.atomic():
            try:
                self._get_api().delete_file(
                    path_in_repo=self._object_key(key),
                    repo_id=self.repo_id,
                    repo_type=self.repo_type,
                    revision=self.revision,
                    commit_message=f"laila: delete {key}",
                )
            except EntryNotFoundError:
                pass

    def empty(self) -> None:
        """Remove all entries from the pool."""
        with self.atomic():
            for key in list(self.keys(as_generator=False)):
                del self[key]

    def exists(self, key: str) -> bool:
        """Return ``True`` if *key* exists in the Hub repo."""
        with self.atomic():
            try:
                hf_hub_download(
                    repo_id=self.repo_id,
                    filename=self._object_key(key),
                    repo_type=self.repo_type,
                    revision=self.revision,
                    token=self.token,
                )
                return True
            except EntryNotFoundError:
                return False

    def __contains__(self, key: str) -> bool:
        """Check membership, delegates to :meth:`exists`."""
        return self.exists(key)

    def keys(self, as_generator: bool = False) -> Iterable[str]:
        """Return all keys in the Hub repo under the configured prefix.

        Parameters
        ----------
        as_generator : bool, optional
            If ``True``, return a lazy iterator instead of a list.

        Returns
        -------
        Iterable[str]
            Pool keys.
        """
        prefix = self._prefix()
        all_files = self._get_api().list_repo_files(
            repo_id=self.repo_id,
            repo_type=self.repo_type,
            revision=self.revision,
            token=self.token,
        )

        def _iter_keys() -> Iterator[str]:
            for path in all_files:
                if prefix and not path.startswith(prefix + "/"):
                    continue
                if not path.endswith(".json"):
                    continue
                yield self._logical_key(path)

        if not as_generator:
            return list(_iter_keys())
        return _iter_keys()
