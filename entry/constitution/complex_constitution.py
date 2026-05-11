"""Complex constitution: a single code string driven by a `Manifest`.

A `ComplexConstitution` carries one Python source string defining a
single callable `f(manifest) -> payload`.  At build time it resolves
its bound `Manifest` via the active policy's memory (using a stored
`global_id` if the live object is not on hand), forces the manifest's
references to materialize inside a `laila.guarantee` scope, and then
applies the callable to the manifest.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import PrivateAttr

from .constitution import Constitution, _exec_one_fn, _register_kind


@_register_kind("complex")
class ComplexConstitution(Constitution):
    """A one-shot constitution function bound to a `Manifest`.

    Once assigned, neither the code nor the manifest can be reassigned.
    The manifest may be provided directly as a `Manifest` object or, when
    rehydrating a serialized entry, as a `manifest_global_id` to be
    resolved lazily from the active policy's memory at build time.
    """

    _code: Optional[str] = PrivateAttr(default=None)
    _manifest: Optional[Any] = PrivateAttr(default=None)
    _manifest_global_id: Optional[str] = PrivateAttr(default=None)

    def __init__(self, **data: Any):
        """Initialise from optional `code`, `manifest`, and
        `manifest_global_id` kwargs."""
        super().__init__()
        code = data.get("code", None)
        manifest = data.get("manifest", None)
        manifest_global_id = data.get("manifest_global_id", None)
        if code is not None:
            self.code = code
        if manifest is not None:
            self.manifest = manifest
        elif manifest_global_id is not None:
            self._manifest_global_id = manifest_global_id

    @property
    def code(self) -> Optional[str]:
        """The single source string defining the constitution callable."""
        return self._code

    @code.setter
    def code(self, value: str) -> None:
        """Assign the constitution code (one-time only)."""
        if self._code is not None:
            raise AttributeError(
                "ComplexConstitution code is already set and cannot be reassigned."
            )
        if not isinstance(value, str):
            raise TypeError("code must be a Python source string")
        self._code = value

    @property
    def manifest(self):
        """The bound `Manifest`, if available."""
        return self._manifest

    @manifest.setter
    def manifest(self, value) -> None:
        """Assign the manifest (one-time only); also records its `global_id`."""
        from ...policy.central.memory.schema.manifest import Manifest

        if self._manifest is not None:
            raise AttributeError(
                "ComplexConstitution manifest is already set and cannot be reassigned."
            )
        if not isinstance(value, Manifest):
            raise TypeError("manifest must be a Manifest instance")
        self._manifest = value
        self._manifest_global_id = value.global_id

    @property
    def manifest_global_id(self) -> Optional[str]:
        """The bound manifest's `global_id`, if known."""
        return self._manifest_global_id

    def resolve_manifest(self):
        """Resolve `self._manifest` from `self._manifest_global_id` if needed."""
        if self._manifest is not None:
            return self._manifest
        if self._manifest_global_id is None:
            return None

        import laila

        ref = laila.remember(self._manifest_global_id)
        resolved = ref.wait(None)
        if isinstance(resolved, list):
            resolved = resolved[0] if resolved else None
        if resolved is None:
            raise RuntimeError(
                "Could not resolve manifest from global_id "
                f"{self._manifest_global_id!r}: not found in active memory."
            )
        self._manifest = resolved
        return self._manifest

    def build(self, payload_input: Optional[Any] = None) -> Any:
        """Resolve the manifest and apply the single constitution callable.

        `payload_input` is ignored — complex constitutions read from
        their bound manifest, never from a serialized blob.
        """
        import laila

        if self._code is None:
            raise RuntimeError("no constitution code defined.")

        target = self.resolve_manifest()
        if target is None:
            raise RuntimeError("no manifest available to run constitution.")

        with laila.guarantee:
            target.realized

        return _exec_one_fn(self._code)(target)

    def as_dict(self) -> dict:
        """Serialize as `{"_kind": "complex", "code": ..., "manifest_global_id": ...}`."""
        return {
            "_kind": "complex",
            "code": self._code,
            "manifest_global_id": self._manifest_global_id,
        }

    @classmethod
    def _from_dict(cls, in_dict: dict) -> "ComplexConstitution":
        """Build a `ComplexConstitution` from its serialized dict."""
        return cls(
            code=in_dict.get("code"),
            manifest_global_id=in_dict.get("manifest_global_id"),
        )
