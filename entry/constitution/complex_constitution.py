"""Complex constitution: a single source string driven by a :class:`Manifest`.

A :class:`ComplexConstitution` carries one Python source string that
defines a single callable ``f(manifest) -> payload``. At build time it
resolves its bound :class:`Manifest` from the active policy's memory
(using a stored ``global_id`` if the live object is not on hand), forces
every entry the manifest references to materialize inside a
``laila.guarantee`` scope, and then applies the callable to the
manifest.

Why complex constitutions exist
-------------------------------
:class:`SimpleConstitution` is a fine vehicle for inverse serialization
chains, but it cannot express *derivations* -- "this entry's payload is
``f(other_entry_a, other_entry_b)``". A complex constitution carries
both the recipe (the source string) and the inputs (the manifest) so
the derivation can be re-executed in any process that can fetch the
inputs from a shared pool.

Identity preservation across serialization
------------------------------------------
A complex constitution does not embed the manifest's *contents* in its
serialized form; it embeds only the manifest's ``global_id``. The live
manifest is then re-fetched from the active policy's memory at build
time. This keeps serialized constitutions small and ensures a build
always sees the *current* state of the referenced entries, even if
they have evolved since the constitution was first created.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import PrivateAttr

from .constitution import Constitution, _exec_one_fn, _register_kind


@_register_kind("complex")
class ComplexConstitution(Constitution):
    """A one-shot constitution function bound to a :class:`Manifest`.

    Once assigned, neither the code nor the manifest can be reassigned
    -- the constitution is intentionally immutable so that the gid
    derived from it is stable. The manifest may be provided directly as
    a live :class:`Manifest` instance or, when rehydrating a serialized
    entry, as a ``manifest_global_id`` to be resolved lazily from the
    active policy's memory at build time.

    Construction shapes
    -------------------
    .. code-block:: python

        # Direct: manifest object in hand
        c = ComplexConstitution(code=src, manifest=m)

        # Lazy: only the gid (typical after deserialization)
        c = ComplexConstitution(code=src, manifest_global_id=gid)
    """

    _code: Optional[str] = PrivateAttr(default=None)
    _manifest: Optional[Any] = PrivateAttr(default=None)
    _manifest_global_id: Optional[str] = PrivateAttr(default=None)

    def __init__(self, **data: Any):
        """Initialise from optional ``code``, ``manifest``, and
        ``manifest_global_id`` kwargs.

        Either ``manifest`` or ``manifest_global_id`` may be supplied,
        not both -- a live manifest takes precedence when both are
        present (the gid is derived from it). Omitting both is allowed
        but the resulting constitution cannot be built until a manifest
        is attached via the setter.
        """
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
        """The single Python source string defining the constitution callable.

        Read-only after first assignment. The string must define
        exactly one top-level callable taking a manifest and returning
        the payload value -- this is enforced by :func:`_exec_one_fn`
        at build time, not at assignment time, so syntactically invalid
        sources won't surface until the build runs.
        """
        return self._code

    @code.setter
    def code(self, value: str) -> None:
        """Assign the constitution code. One-time only.

        Raises
        ------
        AttributeError
            If ``code`` is already set on this instance.
        TypeError
            If ``value`` is not a string.
        """
        if self._code is not None:
            raise AttributeError(
                "ComplexConstitution code is already set and cannot be reassigned."
            )
        if not isinstance(value, str):
            raise TypeError("code must be a Python source string")
        self._code = value

    @property
    def manifest(self):
        """The bound :class:`Manifest`, if available.

        May be ``None`` immediately after deserialization (only the
        gid is preserved on disk). The first call to
        :meth:`_resolve_manifest_sync` or
        :meth:`_resolve_manifest_async` populates this slot.
        """
        return self._manifest

    @manifest.setter
    def manifest(self, value) -> None:
        """Bind a live :class:`Manifest`. One-time only.

        Also records the manifest's ``global_id`` so the binding can be
        re-established after deserialization.

        Raises
        ------
        AttributeError
            If a manifest is already bound.
        TypeError
            If ``value`` is not a :class:`Manifest`.
        """
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
        """The bound manifest's ``global_id``, if known.

        Set automatically when :attr:`manifest` is assigned, and
        restored from the serialized form by :meth:`_from_dict`. Stable
        once set -- a complex constitution does not allow rebinding to
        a different manifest.
        """
        return self._manifest_global_id

    def _resolve_manifest_sync(self):
        """Synchronously resolve :attr:`_manifest` from :attr:`_manifest_global_id`.

        Cache hit: if :attr:`_manifest` is already populated, return it
        without any I/O.

        Cache miss: submit a ``laila.remember`` for the stored gid and
        block the calling thread on the resulting future. The fetched
        manifest is then cached in :attr:`_manifest` for subsequent
        builds.

        Safe to call from any non-loop thread. If called from inside
        an async loop thread the underlying ``Future.wait()`` will
        raise :exc:`LoopBlockingWaitError` -- use
        :meth:`_resolve_manifest_async` from coroutine contexts.

        Returns
        -------
        Manifest or None
            The resolved manifest, or ``None`` if neither a live
            manifest nor a gid is bound.
        """
        if self._manifest is not None:
            return self._manifest
        if self._manifest_global_id is None:
            return None

        import laila

        ref = laila.remember(self._manifest_global_id)
        resolved = ref.wait(None)
        return self._record_resolved_manifest(resolved)

    async def _resolve_manifest_async(self):
        """Asynchronously resolve :attr:`_manifest` from its global id.

        ``await``\\ s the ``laila.remember`` future so the calling
        coroutine can yield the loop while the underlying read /
        build pipeline runs. This is the path
        :meth:`Entry._build_async` uses to pre-resolve the manifest
        before invoking the user's sync constitution body in a worker
        thread, ensuring the loop never blocks on a sync wait.
        """
        if self._manifest is not None:
            return self._manifest
        if self._manifest_global_id is None:
            return None

        import laila

        ref = laila.remember(self._manifest_global_id)
        resolved = await ref
        return self._record_resolved_manifest(resolved)

    def _resolve_manifest(self, *, asynchronous: bool = False):
        """Dispatch to :meth:`_resolve_manifest_async` (returns coroutine)
        or :meth:`_resolve_manifest_sync` (blocks)."""
        if asynchronous:
            return self._resolve_manifest_async()
        return self._resolve_manifest_sync()

    def _record_resolved_manifest(self, resolved: Any):
        """Cache the resolved manifest in :attr:`_manifest` and return it.

        ``laila.remember`` returns a list when it was given a list of
        ids; we collapse a single-element list to its lone element
        (matching the single-id call we issued) before caching.

        Raises
        ------
        RuntimeError
            If the lookup returned ``None`` -- the manifest gid is
            present in the constitution but not in the active policy's
            memory, so there is no way to materialize this entry. The
            caller almost certainly needs to ``laila.add_peer`` to the
            policy that owns the manifest, or copy the manifest into
            the active alpha pool first.
        """
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

        ``payload_input`` is ignored -- complex constitutions read from
        their bound manifest, never from a serialized blob.

        Always uses the *sync* manifest-resolution path because user
        constitution code is sync and runs inside
        :meth:`Entry._build_inplace`. Coroutine-aware callers
        (:meth:`Entry._build_async`) pre-resolve the manifest via
        ``await self._resolve_manifest_async()`` before invoking
        ``_build_inplace``, so the sync call here is a cache hit and
        never actually waits.

        The ``with laila.guarantee:`` block around ``target.realized``
        forces every entry the manifest references to be materialized
        on the active policy's alpha pool before the user's callable
        runs, so referenced entries are guaranteed to be ``READY`` when
        the callable accesses them.

        Raises
        ------
        RuntimeError
            If no constitution code is set, or the manifest cannot be
            resolved.
        """
        import laila

        if self._code is None:
            raise RuntimeError("no constitution code defined.")

        target = self._resolve_manifest_sync()
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
