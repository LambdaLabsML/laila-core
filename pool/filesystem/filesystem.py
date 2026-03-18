from __future__ import annotations

from typing import Optional, Any, Iterable, Iterator
from urllib.parse import quote, unquote
from contextlib import suppress
import subprocess
from pydantic import Field, PrivateAttr
import json
import os

from ..schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry.compdata.transformation import TransformationSequence
from ...entry import transformation_base64


class FilesystemPool(_LAILA_IDENTIFIABLE_POOL):
    transformations: Optional[TransformationSequence] = Field(default=transformation_base64)
    _pool_dir: str = PrivateAttr()
    _mount_dir: str = PrivateAttr()
    _image_path: str = PrivateAttr()
    _image_size_bytes: int = PrivateAttr(default=64 * 1024 * 1024)

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data: Any):
        if "image_dir" in data:
            raise ValueError("FilesystemPool storage path is fixed and cannot be overridden.")
        if "image_path" in data:
            raise ValueError("FilesystemPool image_path is fixed and cannot be overridden.")
        if "mount_dir" in data:
            raise ValueError("FilesystemPool mount_dir is fixed and cannot be overridden.")
        super().__init__(**data)

    @property
    def pool_dir(self) -> str:
        return self._pool_dir

    @property
    def mount_dir(self) -> str:
        return self._mount_dir

    @property
    def image_path(self) -> str:
        return self._image_path

    def model_post_init(self, __context: Any) -> None:
        filesystem_root = os.path.expanduser("~/.laila/pools/filesystem")
        os.makedirs(filesystem_root, exist_ok=True)

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
        return None

    def _resolve_pool_dir(self) -> str:
        return os.path.expanduser(os.path.join("~/.laila/pools/filesystem", self.pool_id))

    def _resolve_image_path(self) -> str:
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
        return os.path.join(self.pool_dir, "mnt")

    def _is_mounted(self, path: str) -> bool:
        if os.path.ismount(path):
            return True

        normalized_path = os.path.realpath(path)
        try:
            with open("/proc/self/mountinfo", "r", encoding="utf-8") as handle:
                for line in handle:
                    parts = line.split()
                    if len(parts) > 4 and os.path.realpath(parts[4]) == normalized_path:
                        return True
        except OSError:
            pass

        return False

    def _run_command(self, command: list[str], *, action: str) -> None:
        try:
            subprocess.run(command, check=True, capture_output=True, text=True)
        except FileNotFoundError as exc:
            raise RuntimeError(f"Unable to {action}: command not found: {command[0]}") from exc
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"Unable to {action}. Stdout: {exc.stdout.strip()} Stderr: {exc.stderr.strip()}"
            ) from exc

    def _create_image_file(self) -> None:
        with open(self.image_path, "wb") as handle:
            handle.truncate(self._image_size_bytes)
        self._run_command(
            ["mkfs.ext4", "-F", self.image_path],
            action=f"format filesystem image {self.image_path}",
        )

    def _mount_image(self) -> None:
        self._run_command(
            ["mount", "-o", "loop", self.image_path, self.mount_dir],
            action=f"mount filesystem image {self.image_path} at {self.mount_dir}",
        )

    def _storage_key(self, key: str) -> str:
        return f"{quote(key, safe='')}.json"

    def _logical_key(self, storage_key: str) -> str:
        if storage_key.endswith(".json"):
            storage_key = storage_key[:-5]
        return unquote(storage_key)

    def _entry_path(self, key: str) -> str:
        return os.path.join(self.mount_dir, self._storage_key(key))

    def __getitem__(self, key: str) -> Optional[Any]:
        path = self._entry_path(key)
        if not os.path.exists(path):
            return None

        with self.atomic():
            with open(path, "a+", encoding="utf-8") as handle:
                handle.seek(0)
                raw = handle.read()

        if raw.strip() == "":
            return None
        return json.loads(raw)

    def __setitem__(self, key: str, entry: Any) -> None:
        value = entry
        if isinstance(value, dict):
            value = json.dumps(value)
        if not isinstance(value, str):
            raise TypeError("FilesystemPool expects a serialized JSON string.")

        path = self._entry_path(key)
        with self.atomic():
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(value)

    def __delitem__(self, key: str) -> None:
        path = self._entry_path(key)
        with self.atomic():
            with suppress(FileNotFoundError):
                os.remove(path)

    def empty(self) -> None:
        with self.atomic():
            for name in os.listdir(self.mount_dir):
                if not name.endswith(".json"):
                    continue
                with suppress(FileNotFoundError):
                    os.remove(os.path.join(self.mount_dir, name))

    def exists(self, key: str) -> bool:
        return os.path.exists(self._entry_path(key))

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def keys(self, as_generator: bool = False) -> Iterable[str]:
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
