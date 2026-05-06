"""Constitution logic that derives Entry data from a Manifest.

A constitution is a small Python *source string* defining exactly one
function. The function takes a single argument, a ``Manifest``, and
returns the payload for the owning Entry. The function is ``exec``'d on
demand, mirroring the ``recovery_sequence`` pattern used by
``ComputationalData``.
"""

from typing import Any, Optional, TYPE_CHECKING

from pydantic import BaseModel, PrivateAttr

if TYPE_CHECKING:
    from ...policy.central.memory.schema.manifest import Manifest


class EntryConstitution(BaseModel):
    """Encapsulates a constitution source-string and its input ``Manifest``.

    Once assigned, neither the constitution code nor the manifest can be
    reassigned. The manifest may be provided directly as a ``Manifest``
    object or, when recovering a serialized entry, as a
    ``manifest_global_id`` to be resolved lazily from the active policy's
    memory at build time.
    """

    _constitution: Optional[str] = PrivateAttr(default=None)
    _manifest: Optional[Any] = PrivateAttr(default=None)
    _manifest_global_id: Optional[str] = PrivateAttr(default=None)

    def __init__(self, **data: Any):
        """Initialise from optional ``constitution``, ``manifest``, and
        ``manifest_global_id`` kwargs."""
        super().__init__()
        constitution = data.get("constitution", None)
        manifest = data.get("manifest", None)
        manifest_global_id = data.get("manifest_global_id", None)
        if constitution is not None:
            self.constitution = constitution
        if manifest is not None:
            self.manifest = manifest
        elif manifest_global_id is not None:
            self._manifest_global_id = manifest_global_id

    @property
    def constitution(self) -> Optional[str]:
        """Source-string defining the constitution function."""
        return self._constitution

    @constitution.setter
    def constitution(self, code: str) -> None:
        """Assign the constitution source-string (one-time only).

        Raises
        ------
        AttributeError
            If a constitution has already been set.
        TypeError
            If *code* is not a ``str``.
        """
        if self._constitution is not None:
            raise AttributeError(
                "Constitution is already set and cannot be reassigned."
            )
        if not isinstance(code, str):
            raise TypeError("constitution must be a Python source string")
        self._constitution = code

    @property
    def manifest(self):
        """The ``Manifest`` driving this constitution."""
        return self._manifest

    @manifest.setter
    def manifest(self, value) -> None:
        """Assign the manifest (one-time only).

        Also records the manifest's ``global_id`` so the binding can be
        serialized and restored across roundtrips.

        Raises
        ------
        AttributeError
            If a manifest has already been set.
        TypeError
            If *value* is not a ``Manifest``.
        """
        from ...policy.central.memory.schema.manifest import Manifest

        if self._manifest is not None:
            raise AttributeError(
                "Manifest is already set and cannot be reassigned."
            )
        if not isinstance(value, Manifest):
            raise TypeError("manifest must be a Manifest instance")
        self._manifest = value
        self._manifest_global_id = value.global_id

    @property
    def manifest_global_id(self) -> Optional[str]:
        """The bound manifest's ``global_id``, if known (even pre-resolution)."""
        return self._manifest_global_id

    def resolve_manifest(self):
        """Resolve ``self._manifest`` from ``self._manifest_global_id`` if needed.

        Fetches the ``Manifest`` from the active policy's memory when the
        instance was rehydrated from a serialized blob that recorded only
        the manifest's ``global_id``. Idempotent and a no-op when the
        manifest is already bound.

        Raises
        ------
        RuntimeError
            If the manifest cannot be resolved from memory.
        """
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

    def _run(self, manifest=None):
        """Execute the constitution function against a manifest.

        Blocks on ``target.data`` inside a ``laila.guarantee`` scope so any
        futures spawned while resolving the blueprint are tracked and
        awaited before the constitution function is invoked.

        Parameters
        ----------
        manifest : Manifest, optional
            The manifest to pass to the constitution function. When
            ``None``, falls back to ``self._manifest``.

        Returns
        -------
        Any
            Whatever the constitution function returns; this becomes the
            payload of the owning Entry.

        Raises
        ------
        RuntimeError
            If no constitution source is defined, or no manifest is
            available.
        ValueError
            If the source-string does not define exactly one callable
            function.
        """
        import laila

        if self._constitution is None:
            raise RuntimeError("no constitution defined.")

        target = manifest if manifest is not None else self._manifest
        if target is None:
            raise RuntimeError("no manifest available to run constitution.")

        namespace: dict = {}
        exec(self._constitution, namespace)

        import types as _types
        fns = [
            v for k, v in namespace.items()
            if not k.startswith("__")
            and callable(v)
            and not isinstance(v, _types.ModuleType)
            and not isinstance(v, type)
        ]
        if len(fns) != 1:
            raise ValueError(
                "constitution source must define exactly one callable function"
            )

        with laila.guarantee:
            target.data

        return fns[0](target)
