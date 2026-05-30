"""Loopback-mounted ext4 filesystem pool implementation.

A :class:`FilesystemPool` writes each entry as a single JSON file
inside a private, loopback-mounted ext4 image. The image is created
on first use, mounted via ``mount -o loop``, and not unmounted by
laila (so subsequent process runs can reuse it).

Why an image-backed mount instead of a plain directory?

- It bounds the pool's footprint to a fixed size (``_image_size_bytes``,
  64 MiB by default) so a runaway producer can't fill the host disk.
- It provides POSIX-level isolation -- the pool's contents share an
  independent inode table from the host filesystem and can be
  detached/relocated as a single ``.img`` file.

The class is Linux-only (relies on ``mkfs.ext4`` and ``mount``) and
needs root privileges or appropriate capabilities to mount.
"""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Iterable, Iterator
from contextlib import suppress
from typing import Any
from urllib.parse import quote, unquote

from pydantic import ConfigDict, Field, PrivateAttr

from ...entry import transformation_base64
from ...entry.compdata.transformation import TransformationSequence
from ..schema.base import _LAILA_IDENTIFIABLE_POOL


class FilesystemPool(_LAILA_IDENTIFIABLE_POOL):
    """Pool backed by JSON files on a loopback-mounted ext4 image.

    Each entry lives in a single ``<urlencoded-key>.json`` file inside
    the pool's mount directory. Keys are URL-encoded so any character
    is safe for use in a filename, and decoded on enumeration via
    :meth:`_logical_key`.

    The image is created with :meth:`_create_image_file`
    (``truncate`` + ``mkfs.ext4``) and mounted via :meth:`_mount_image`
    on first construction. Subsequent constructions detect the mount
    via ``/proc/self/mountinfo`` and skip both steps.

    The constructor refuses any explicit ``image_dir`` / ``image_path``
    / ``mount_dir`` overrides because the pool computes those itself
    from :data:`LAILA_DEFAULT_DIRECTORIES` and its own UUID -- letting
    callers override them would silently break the
    "one image file per pool, name derived from pool UUID" invariant.

    Notes
    -----
    The default ``transformations`` is :data:`transformation_base64`,
    which gives the JSON serialiser pure-ASCII payloads (avoiding
    encoding surprises across mount platforms).
    """

    transformations: TransformationSequence | None = Field(default=transformation_base64)
    _pool_dir: str = PrivateAttr()
    _mount_dir: str = PrivateAttr()
    _image_path: str = PrivateAttr()
    _image_size_bytes: int = PrivateAttr(default=64 * 1024 * 1024)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **data: Any):
        """Validate that reserved path fields are not overridden."""
        if "image_dir" in data:
            raise ValueError("FilesystemPool storage path is fixed and cannot be overridden.")
        if "image_path" in data:
            raise ValueError("FilesystemPool image_path is fixed and cannot be overridden.")
        if "mount_dir" in data:
            raise ValueError("FilesystemPool mount_dir is fixed and cannot be overridden.")
        super().__init__(**data)

    @property
    def pool_dir(self) -> str:
        """Root directory for this pool's artefacts."""
        return self._pool_dir

    @property
    def mount_dir(self) -> str:
        """Mount-point directory where entries are stored."""
        return self._mount_dir

    @property
    def image_path(self) -> str:
        """Path to the ext4 image file."""
        return self._image_path

    def model_post_init(self, __context: Any) -> None:
        """Create or mount the filesystem image."""
        super().model_post_init(__context)
        self._pool_dir = self._resolve_pool_dir()
        os.makedirs(self.pool_dir, exist_ok=True)
        self._image_path = self._resolve_image_path()
        self._mount_dir = self._resolve_mount_dir()
        os.makedirs(self.mount_dir, exist_ok=True)

        if self._is_mounted(self.mount_dir):
            return

        if os.path.exists(self.image_path):
            self._mount_image()
            return

        self._create_image_file()
        self._mount_image()

    def close(self) -> None:
        """No-op; the mount persists beyond pool lifetime."""
        return None

    def _resolve_pool_dir(self) -> str:
        """Derive the pool directory from default directories."""
        from ...macros.defaults import LAILA_DEFAULT_DIRECTORIES

        return os.path.join(LAILA_DEFAULT_DIRECTORIES["pools"], self.uuid)

    def _resolve_image_path(self) -> str:
        """Resolve the ``.img`` or ``.iso`` image path for this pool."""
        img_path = os.path.join(self.pool_dir, f"{self.pool_id}.img")
        iso_path = os.path.join(self.pool_dir, f"{self.pool_id}.iso")

        if os.path.exists(img_path) and os.path.exists(iso_path):
            raise RuntimeError(
                f"Ambiguous filesystem image for {self.pool_id}: both {img_path} and {iso_path} exist."
            )
        if os.path.exists(img_path):
            return img_path
        if os.path.exists(iso_path):
            return iso_path
        return img_path

    def _resolve_mount_dir(self) -> str:
        """Return the mount sub-directory path."""
        return os.path.join(self.pool_dir, "mnt")

    def _is_mounted(self, path: str) -> bool:
        """Check whether *path* is currently a mount point."""
        if os.path.ismount(path):
            return True

        normalized_path = os.path.realpath(path)
        try:
            with open("/proc/self/mountinfo", encoding="utf-8") as handle:
                for line in handle:
                    parts = line.split()
                    if len(parts) > 4 and os.path.realpath(parts[4]) == normalized_path:
                        return True
        except OSError:
            pass

        return False

    def _run_command(self, command: list[str], *, action: str) -> None:
        """Execute a subprocess command, raising on failure."""
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
        except FileNotFoundError as exc:
            raise RuntimeError(f"Unable to {action}: command not found: {command[0]}") from exc
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"Unable to {action}. Stdout: {exc.stdout.strip()} Stderr: {exc.stderr.strip()}"
            ) from exc

    def _create_image_file(self) -> None:
        """Allocate and format a new ext4 image file."""
        with open(self.image_path, "wb") as handle:
            handle.truncate(self._image_size_bytes)
        self._run_command(
            ["mkfs.ext4", "-F", self.image_path],
            action=f"format filesystem image {self.image_path}",
        )

    def _mount_image(self) -> None:
        """Loop-mount the image file at :attr:`mount_dir`."""
        self._run_command(
            ["mount", "-o", "loop", self.image_path, self.mount_dir],
            action=f"mount filesystem image {self.image_path} at {self.mount_dir}",
        )

    def _storage_key(self, key: str) -> str:
        """URL-encode *key* and append ``.json``."""
        return f"{quote(key, safe='')}.json"

    def _logical_key(self, storage_key: str) -> str:
        """Strip the ``.json`` suffix and URL-decode."""
        if storage_key.endswith(".json"):
            storage_key = storage_key[:-5]
        return unquote(storage_key)

    def _entry_path(self, key: str) -> str:
        """Full filesystem path for the given entry key."""
        return os.path.join(self.mount_dir, self._storage_key(key))

    def _read(self, key: str) -> Any | None:
        """Read and parse the JSON file for *key*, or return ``None``."""
        path = self._entry_path(key)
        if not os.path.exists(path):
            return None

        with self.atomic(), open(path, "a+", encoding="utf-8") as handle:
            handle.seek(0)
            raw = handle.read()

        if raw.strip() == "":
            return None
        return json.loads(raw)

    def _write(self, key: str, entry: Any) -> None:
        """Write *entry* as a JSON file under *key*."""
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("FilesystemPool expects a serialized JSON string.")

        path = self._entry_path(key)
        with self.atomic(), open(path, "w", encoding="utf-8") as handle:
            handle.write(value)

    def _delete(self, key: str) -> None:
        """Remove the JSON file for *key*; no-op if absent."""
        path = self._entry_path(key)
        with self.atomic(), suppress(FileNotFoundError):
            os.remove(path)

    def _empty(self) -> None:
        """Remove all ``.json`` files from the mount directory."""
        with self.atomic():
            for name in os.listdir(self.mount_dir):
                if not name.endswith(".json"):
                    continue
                with suppress(FileNotFoundError):
                    os.remove(os.path.join(self.mount_dir, name))

    def _exists(self, key: str) -> bool:
        """Return ``True`` if a file for *key* exists on disk."""
        return os.path.exists(self._entry_path(key))

    def __contains__(self, key: str) -> bool:
        """Check membership, delegates to :meth:`_exists`."""
        return self._exists(key)

    def _keys(self, as_generator: bool = False) -> Iterable[str]:
        """Return all keys in the mount directory.

        Parameters
        ----------
        as_generator : bool, optional
            If ``True``, return a lazy iterator instead of a list.

        Returns
        -------
        Iterable[str]
            Pool keys.
        """
        if not as_generator:
            with self.atomic():
                names = [name for name in os.listdir(self.mount_dir) if name.endswith(".json")]
                return [self._logical_key(name) for name in names]

        def _gen() -> Iterator[str]:
            with self.atomic():
                for name in os.listdir(self.mount_dir):
                    if name.endswith(".json"):
                        yield self._logical_key(name)

        return _gen()
