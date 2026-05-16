"""Core :class:`Entry` class -- the fundamental unit of data in LAILA.

An :class:`Entry` couples three concerns into one identifiable object:

1. **Identity.** Each entry has a UUID (random, explicit, or
   nickname-derived), a list of hierarchical scopes, and an optional
   *evolution* counter. Together they form a canonical ``global_id``
   string that makes the entry addressable across processes and
   machines (see :class:`_LAILA_IDENTIFIABLE_OBJECT`).
2. **Lifecycle state.** A small :class:`EntryState` enum captures
   whether the entry's payload is materialized (``READY``), staged for
   build (``STAGED``), in flight to a pool (``POOLING``), pooled
   (``POOLED``), stale, or "not applicable" for subclasses that don't
   carry a payload (``NA``).
3. **Either** a concrete payload (wrapped in a :class:`ComputationalData`
   for type-aware serialization) **or** a :class:`Constitution`
   describing how to build the payload. The two are mutually exclusive
   at construction time but may both be transiently present during a
   build.

Two flavors
-----------
- **Constants** (``Entry.constant(data, ...)``): immutable. The
  ``evolution`` field is :data:`None` and ``evolve()`` raises.
- **Variables** (``Entry.variable(data, ...)``): mutable in place via
  :meth:`Entry.evolve` which bumps the ``evolution`` counter. Two
  entries with the same UUID but different evolutions denote different
  versions of the same logical thing.

Build paths
-----------
When created from a constitution, an entry starts ``STAGED`` and must
be materialized before its ``data`` can be read:

- ``laila.build(entry).wait()`` -- sync caller; returns the same entry
  with ``state=READY``.
- ``await laila.build(entry)`` -- async caller; non-blocking.
- ``laila.remember(entry_id)``  -- if the entry is already memorized,
  fetching it routes through the pool's :class:`SimpleConstitution`
  (the inverse-transformation chain) and returns a ``READY`` entry.

See :func:`laila.build` for the unified async build pipeline and the
distinction between :class:`SimpleConstitution` (pure CPU) and
:class:`ComplexConstitution` (manifest-driven, may await further
fetches).
"""

from __future__ import annotations

import copy
from types import NoneType
from typing import Any, ClassVar, Dict, Hashable, List, Sequence, Tuple, Union

import numpy as np
from pydantic import BaseModel, ConfigDict, model_validator
from pydantic import BaseModel, Field, PrivateAttr
from typing import Optional, Any, Callable
import uuid
import inspect
import threading
import json
import base64
from collections import deque
import re
from multiprocessing.shared_memory import SharedMemory
from uuid import uuid4
from typing import Dict, Any, Optional
import hashlib, time, os
import time

from .compdata import ComputationalData as ComputationalData
from .entry_state import EntryState
from .entry_metadata import EntryIdentityView, EntryHolisticView
from .constitution import Constitution, SimpleConstitution, ComplexConstitution

from ..macros.strings import _ENTRY_SCOPE
from ..basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from ..atomic.definitions.locally_atomic_identifiable_object import _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT
from ..utils.decorators.synchronized import synchronized
from .compdata.transformation import TransformationSequence


class Entry( 
    _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT
):
    """The fundamental unit of data in LAILA.

    An ``Entry`` wraps arbitrary Python data (tensors, images, dicts,
    DataFrames, Manifests, etc.) with three things:

    - **Identity** -- UUID + scopes + optional evolution counter; see
      :class:`_LAILA_IDENTIFIABLE_OBJECT`.
    - **State** -- a :class:`EntryState` reflecting where the entry is
      in its lifecycle (``READY`` / ``STAGED`` / ``POOLING`` / ``POOLED``
      / ``STALE`` / ``NA``).
    - **Payload XOR Constitution** -- either a materialized
      :class:`ComputationalData` payload, or a :class:`Constitution`
      describing how to build one (see :func:`laila.build`).

    Mutability
    ----------
    - Use :meth:`Entry.constant` to create an *immutable* entry whose
      ``evolution`` is :data:`None`. :meth:`evolve` raises on these.
    - Use :meth:`Entry.variable` to create a *mutable* entry. Each call
      to :meth:`evolve` bumps ``evolution`` by 1 and replaces the
      payload, leaving the UUID stable.

    Public construction
    -------------------
    Prefer the class-method factories rather than the raw constructor:

    .. code-block:: python

        const = Entry.constant({"x": 1}, nickname="config")
        var   = Entry.variable(np.zeros(8), evolution=0)
        lazy  = Entry.variable(constitution=src, manifest=m)

    Each factory enforces the right combination of identity / payload /
    constitution kwargs and gives clearer errors than the constructor.

    Thread safety
    -------------
    All mutators (data setter, constitution setter, state setter,
    :meth:`evolve`) are decorated with :func:`synchronized` and run
    under the entry's per-instance ``atomic`` lock, so concurrent
    threads can read and update the same entry without external
    locking.
    """

    model_config = ConfigDict(
        private_attributes=True,
        use_enum_values=True
    )

    _ALLOWS_NON_NA_STATE: ClassVar[bool] = True

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_ENTRY_SCOPE]))
    _state: EntryState = PrivateAttr(default=EntryState.STAGED)
    _constitution: Optional[Constitution] = PrivateAttr(default = None)
    _payload: Optional[ComputationalData] = PrivateAttr(default=None)

    def __init__(self, **data: dict):
        """Initialise an Entry from keyword arguments.

        Most callers should use the higher-level factories
        :meth:`Entry.constant` / :meth:`Entry.variable` /
        :meth:`Entry.contingent` instead -- they validate argument
        combinations and provide clearer errors. The raw constructor is
        only invoked directly by deserialization paths
        (:meth:`from_dict`, the build pipeline, etc.).

        Initialisation is split across four helpers so subclasses can
        override individual phases without rewriting the whole
        constructor:

        1. :meth:`_initialize_identity` parses ``global_id`` /
           ``uuid`` / ``evolution`` / ``nickname`` / ``scopes`` and
           threads them through the parent identifiable-object
           machinery.
        2. :meth:`_initialize_payload` wraps any ``data`` / ``payload``
           value in a :class:`ComputationalData` (subclass dispatched
           by payload type).
        3. :meth:`_initialize_constitution` accepts a live
           :class:`Constitution`, a list of source strings (auto-wrapped
           in :class:`SimpleConstitution`), or a single source string
           plus a ``manifest`` (auto-wrapped in
           :class:`ComplexConstitution`).
        4. :meth:`_initialize_state` picks the initial
           :class:`EntryState` -- ``STAGED`` when a constitution is
           attached, otherwise the explicit ``state`` kwarg or ``STAGED``.

        Parameters
        ----------
        **data : dict
            Accepted keys (all optional unless required by the chosen
            factory):

            ``data`` / ``payload``
                Raw payload value. Wrapped automatically.
            ``uuid``
                Explicit UUID string. Mutually exclusive with ``global_id``.
            ``evolution``
                Integer evolution counter, or ``None`` for constants.
            ``global_id``
                Composite ``"<scopes>:<uuid>[-evolution]"`` string.
            ``nickname``
                Human-readable name; deterministically converted to a
                UUID-5 against the active namespace.
            ``scopes``
                Override the default ``[ENTRY]`` scope list.
            ``constitution``
                A :class:`Constitution`, a list[str] (-> SimpleConstitution),
                or a str (-> ComplexConstitution, requires ``manifest``).
            ``manifest``
                A :class:`Manifest` to bind to a complex constitution.
            ``state``
                Initial :class:`EntryState`.
        """
        self._initialize_identity(data)
        self._initialize_payload(data)
        self._initialize_constitution(data)
        self._initialize_state(data)
        

    def _initialize_identity(self, data: dict) -> None:
        """Parse identity fields from *data* and initialise the parent identity.

        ``global_id`` is preferred over the (uuid, evolution, scopes)
        triple when present -- it is parsed via
        :meth:`_LAILA_IDENTIFIABLE_OBJECT.process_global_id` and the
        decoded fields populate the identity directly. Otherwise the
        explicit ``uuid``, ``evolution``, ``nickname``, and ``scopes``
        kwargs are forwarded to the parent constructor (which itself
        applies nickname-to-UUID-5 derivation when relevant).
        """
        identity_data = {}

        global_id = data.get("global_id", None)
        uuid = data.get("uuid", None)
        evolution = data.get("evolution", None)
        nickname = data.get("nickname", None)
        scopes = data.get("scopes", None)

        if global_id is not None:
            identity_data = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)
            _LAILA_IDENTIFIABLE_OBJECT.__init__(self, **identity_data)
            return

        if uuid is not None:
            identity_data["uuid"] = uuid
        
        if evolution is not None:
            identity_data["evolution"] = evolution

        if nickname is not None:
            identity_data["nickname"] = nickname

        if scopes is not None:
            identity_data["scopes"] = scopes
        
        _LAILA_IDENTIFIABLE_OBJECT.__init__(self, **identity_data)

    def _initialize_payload(self, data: dict) -> None:
        """Set the entry's payload from ``payload`` or ``data`` kwargs.

        Accepts either keyword for symmetry with the serialized
        representation (``payload`` is the on-disk name) and the
        in-memory convention (``data``). The setter wraps the raw value
        in a :class:`ComputationalData` of the appropriate subclass via
        ``ComputationalData.__new__``'s type-dispatch.
        """
        self.data = data.get("payload", data.get("data", None))

    def _initialize_constitution(self, data: dict) -> None:
        """Set up a :class:`Constitution` if one was provided.

        Three input shapes are normalized into the right
        :class:`Constitution` subclass:

        - A live :class:`Constitution` instance is used as-is. If it is
          a :class:`ComplexConstitution` whose ``manifest`` is unset and
          a separate ``manifest`` kwarg was supplied, the manifest is
          attached opportunistically (the one-shot manifest setter on
          :class:`ComplexConstitution` enforces this is harmless).
        - A ``list[str]`` is interpreted as a chain of inverse-transform
          source strings and wrapped in :class:`SimpleConstitution`.
        - A single ``str`` is interpreted as the source of a
          :class:`ComplexConstitution`'s callable; the optional
          ``manifest`` kwarg is forwarded along.

        The attached constitution is cleared by :meth:`_build_inplace`
        / :meth:`_build_async` once materialization succeeds, leaving
        ``_constitution = None`` and ``_state = READY``.

        Raises
        ------
        TypeError
            If ``constitution`` is not a :class:`Constitution`,
            ``list[str]``, or ``str``.
        """
        constitution = data.get("constitution", None)
        manifest = data.get("manifest", None)
        if constitution is None:
            return
        if isinstance(constitution, Constitution):
            if manifest is not None and isinstance(constitution, ComplexConstitution):
                if constitution.manifest is None:
                    constitution.manifest = manifest
            self.constitution = constitution
            return
        if isinstance(constitution, list):
            self.constitution = SimpleConstitution(codes=constitution)
            return
        if isinstance(constitution, str):
            kwargs = {"code": constitution}
            if manifest is not None:
                kwargs["manifest"] = manifest
            self.constitution = ComplexConstitution(**kwargs)
            return
        raise TypeError(
            "constitution must be a Constitution, list[str], or str"
        )


    def _initialize_state(self, data: dict) -> None:
        """Determine the initial :class:`EntryState` from *data*.

        Rules
        -----
        - Subclasses with ``_ALLOWS_NON_NA_STATE = False`` (e.g.
          :class:`Manifest`) are pinned to ``EntryState.NA`` regardless
          of input.
        - When a constitution is attached, the entry starts ``STAGED``
          (its payload must be built before access). The user's explicit
          ``state`` kwarg is ignored in this case to avoid a misleading
          ``READY`` flag on an unbuilt entry.
        - Otherwise the explicit ``state`` kwarg wins, defaulting to
          ``STAGED`` if absent (``Entry.constant`` / ``Entry.variable``
          override this to ``READY`` for the common case of inline data).
        """
        if not type(self)._ALLOWS_NON_NA_STATE:
            self.state = EntryState.NA
            return

        constitution = data.get("constitution", None)
        if constitution is not None:
            self.state = EntryState.STAGED
        else:
            self.state = data.get("state", EntryState.STAGED)



    ###################################################
    # Properties
    ###################################################


    @property
    def data(self) -> Optional[Any]:
        """Return the unwrapped payload value (or ``None`` if unset).

        This getter is **read-only** -- it never triggers an implicit
        build, even when a constitution is attached. The reason is
        deadlock-avoidance: if the calling thread is itself a taskforce
        loop thread (e.g. inside another constitution body), an implicit
        build would submit work to the same loop and then block on the
        loop, hanging forever. The user must therefore materialize the
        entry explicitly:

        - Sync caller: ``laila.build(entry).wait()``.
        - Async caller: ``await laila.build(entry)``.
        - Or, if the entry is already pooled, ``laila.remember(entry_id)``.

        Returns
        -------
        Any or None
            The raw value held by the entry's :class:`ComputationalData`
            wrapper, or ``None`` when the entry has neither a payload
            nor a constitution.

        Raises
        ------
        EntryNotBuiltError
            If a constitution is attached but the entry has not been
            built (no payload yet).
        """
        with self.atomic(scope="local"):
            if self._payload is not None:
                return self._payload.data
            if self._constitution is not None:
                from .exceptions import EntryNotBuiltError
                raise EntryNotBuiltError(
                    f"Entry {self.global_id} is not built. "
                    "Use `laila.build(entry).wait()` (or "
                    "`await laila.build(entry, asynchronous=True)`) "
                    "to materialize it before accessing .data."
                )
            return None


    @data.setter
    @synchronized
    def data(self, new_data: Optional[Any]) -> None:
        """Replace the payload value.

        Raw Python values (``np.ndarray``, ``dict``, ``list``, ``bytes``,
        torch tensors, etc.) are auto-wrapped in the appropriate
        :class:`ComputationalData` subclass via the registered
        ``TYPE_TO_WRAPPER`` map. Already-wrapped values pass through
        unchanged. Pass ``None`` to clear the payload entirely.

        The setter is decorated with :func:`synchronized`, so concurrent
        writes from different threads serialize on the entry's atomic
        lock.
        """
        if new_data is not None and not isinstance(new_data, ComputationalData):
            new_data = ComputationalData(new_data)
        self._payload = new_data


    @property
    @synchronized
    def constitution(self):
        """Attached :class:`Constitution`, or ``None`` once the entry has been built.

        A non-``None`` value here means "this entry knows how to
        materialize its payload but hasn't done so yet". Once
        :meth:`_build_inplace` succeeds, ``constitution`` is reset to
        ``None`` and ``state`` flips to ``READY``.
        """
        return self._constitution


    @constitution.setter
    @synchronized
    def constitution(self, new_constitution: Optional[Constitution]) -> None:
        """Attach or clear the entry's :class:`Constitution`.

        Pass ``None`` to detach (typically only the build pipeline does
        this, after a successful build). Pass any
        :class:`SimpleConstitution` or :class:`ComplexConstitution` to
        attach a new build recipe.

        Raises
        ------
        TypeError
            If *new_constitution* is neither a :class:`Constitution`
            nor ``None``.
        """
        if new_constitution is not None and not isinstance(new_constitution, Constitution):
            raise TypeError("constitution must be a Constitution or None")
        self._constitution = new_constitution


    @property
    @synchronized
    def state(self) -> EntryState:
        """Current :class:`EntryState` of this Entry.

        Set automatically by the build / serialize / hydrate paths.
        Outside callers should rarely need to assign it directly.
        """
        return self._state


    @state.setter
    @synchronized
    def state(self, new_state: EntryState):
        """Replace the lifecycle state.

        Subclasses may pin themselves to ``EntryState.NA`` by setting
        ``_ALLOWS_NON_NA_STATE = False`` (e.g. :class:`Manifest` does
        this because lifecycle is meaningless for it). On such
        subclasses any other value raises.

        Raises
        ------
        ValueError
            If *new_state* is not an :class:`EntryState`, or if this
            subclass disallows non-``NA`` states.
        """
        if not isinstance(new_state, EntryState):
            raise ValueError("state must be an EntryState")
        if not type(self)._ALLOWS_NON_NA_STATE and new_state is not EntryState.NA:
            raise ValueError(
                f"{type(self).__name__} only accepts EntryState.NA"
            )
        self._state = new_state


    @property
    @synchronized
    def metadata(self):
        """Entry metadata (not yet implemented).

        Raises
        ------
        NotImplementedError
            Always.
        """
        raise NotImplementedError("Metadata is not implemented for Entry")

    ###################################################
    # Constitution Operations
    ###################################################

    def _build_sync(self):
        """Synchronously materialize this entry by running its constitution.

        Runs :meth:`_build_inplace` directly on the calling thread --
        no command submission, no future. Mostly an escape hatch for
        unit tests and the SimpleConstitution branch of the
        deserialization pipeline; public callers go through
        :func:`laila.build` which submits :meth:`_build_async` to a
        taskforce instead.

        Returns
        -------
        Entry
            ``self``, for convenient chaining.

        Raises
        ------
        RuntimeError
            If the entry is already built (``state == READY``) or has
            no constitution attached.
        """
        if self._state == EntryState.READY:
            raise RuntimeError("Entry is already built.")
        if self._constitution is None:
            raise RuntimeError("Entry has no constitution attached.")
        self._build_inplace()
        return self

    async def _build_async(self):
        """Asynchronously materialize this entry by running its constitution.

        Two paths:

        - **Simple constitutions** are pure-CPU inverse-transformation
          chains with no I/O to ``await``. We call
          :meth:`_build_inplace` directly on the loop thread; the work
          is bounded and runs to completion before any other coroutine
          gets a turn.
        - **Complex constitutions** are user-defined builders bound to
          a :class:`Manifest`. We:

          1. ``await`` :meth:`ComplexConstitution._resolve_manifest_async`
             to fetch the manifest if only its global_id was carried
             through serialization;
          2. ``await target.async_realized`` to recursively materialize
             every entry the manifest references (via
             ``laila.remember(...)`` calls that flow through the same
             loop without ever blocking on a sync ``Future.wait()``);
          3. offload the user's *sync* constitution body to
             :func:`asyncio.to_thread` so any internal blocking calls
             (``manifest.realized``, ``Future.wait()``) don't deadlock
             the loop.

        On success the entry is mutated in place: ``_payload`` is set,
        ``_constitution`` becomes ``None``, ``_state`` becomes
        ``READY``. The coroutine resolves to ``self``.

        Returns
        -------
        Entry
            ``self``.

        Raises
        ------
        RuntimeError
            If the entry is already built, has no constitution, or
            (for complex constitutions) cannot resolve its manifest.
        """
        if self._state == EntryState.READY:
            raise RuntimeError("Entry is already built.")
        if self._constitution is None:
            raise RuntimeError("Entry has no constitution attached.")

        import asyncio

        if isinstance(self._constitution, ComplexConstitution):
            from .constitution.constitution import _exec_one_fn

            c = self._constitution
            if c._code is None:
                raise RuntimeError("no constitution code defined.")

            target = await c._resolve_manifest_async()
            if target is None:
                raise RuntimeError("no manifest available to run constitution.")

            await target.async_realized
            # User constitution code is sync and may itself call
            # `manifest.realized` (a sync wait). Offload to a worker
            # thread so it doesn't block the loop and can safely call
            # blocking ``Future.wait()`` paths.
            result = await asyncio.to_thread(_exec_one_fn(c._code), target)
            self._post_build(result)
            self.constitution = None
            self.state = EntryState.READY
            return self

        self._build_inplace()
        return self

    def _build(self, *, asynchronous: bool = False):
        """Dispatch to :meth:`_build_async` (returns coroutine) or
        :meth:`_build_sync` (runs inline)."""
        if asynchronous:
            return self._build_async()
        return self._build_sync()

    def _build_inplace(self):
        """Run the attached constitution synchronously, in place.

        Threads the current payload (if any) into the constitution as
        ``payload_input`` and routes the result through
        :meth:`_post_build`. After a successful build the constitution
        is detached and the state flips to ``READY``.

        Called from:

        - :meth:`_build_sync` -- direct sync materialization.
        - :meth:`_build_async` -- the SimpleConstitution branch.
        - :meth:`_build_from_dict_sync` -- inline rebuild during
          deserialization for SimpleConstitution entries.

        Raises
        ------
        RuntimeError
            If the entry has no constitution attached.
        """
        if self._constitution is None:
            raise RuntimeError("Entry has no constitution attached.")

        payload_input = self._payload.data if self._payload is not None else None
        result = self._constitution.build(payload_input=payload_input)
        self._post_build(result)
        self.constitution = None
        self.state = EntryState.READY

    def _post_build(self, result):
        """Place the build result into the entry's payload slot.

        Default behavior is to assign ``result`` to :attr:`data` (which
        wraps it in a :class:`ComputationalData`). Subclasses such as
        :class:`Manifest` override this hook to perform a different
        side-effect (e.g. populating internal caches instead of a
        single payload).
        """
        self.data = result


    ###################################################
    # Variable
    ###################################################

    @classmethod
    def variable(
        cls, 
        data=None,
        *, 
        uuid = None,
        evolution=None,
        state = None,
        constitution=None,
        manifest=None,
        global_id = None,
        nickname = None
    ):
        """Create a *mutable* (evolvable) Entry.

        A variable entry has an integer ``evolution`` counter starting
        at ``0`` (or the user-supplied value). Each subsequent call to
        :meth:`evolve` bumps the counter and replaces the payload,
        leaving the UUID stable -- two snapshots of "the same logical
        thing" therefore differ only in their evolution suffix.

        Argument groups
        ---------------
        Pass *exactly one* of these payload shapes:

        - ``data=`` -- a concrete payload value. The returned entry is
          ``READY`` immediately.
        - ``constitution=`` *and* ``manifest=`` -- a builder source
          string and a manifest to drive it. The returned entry is
          ``STAGED``; call :func:`laila.build(entry)` to materialize.

        Pass *at most one* of these identity shapes:

        - ``global_id=`` -- composite ``"<scopes>:<uuid>[-evolution]"``.
        - ``uuid=`` (with optional ``evolution=``).
        - ``nickname=`` -- deterministic UUID-5 derivation against the
          active namespace; useful for cross-process addressability.
        - none of the above -- a fresh random UUID-4.

        Parameters
        ----------
        data : Any, optional
            The raw payload. Mutually exclusive with
            ``constitution`` / ``manifest``.
        uuid : str, optional
            Explicit UUID. Mutually exclusive with ``global_id``.
        evolution : int, optional
            Starting evolution counter (defaults to ``0``).
        state : EntryState, optional
            Initial state; auto-set to ``READY`` when no constitution
            is given. Ignored when ``constitution`` is supplied (the
            entry must start ``STAGED``).
        constitution : str, optional
            Python source-string defining exactly one function that
            takes a :class:`Manifest` and returns the payload. When
            provided, ``manifest`` is required.
        manifest : Manifest, optional
            Manifest feeding the constitution. Required when
            ``constitution`` is set; ignored otherwise.
        global_id : str, optional
            Composite identifier.
        nickname : str, optional
            Human-readable name used to derive a deterministic UUID-5.

        Returns
        -------
        Entry
            A new variable Entry. When a constitution is supplied the
            entry starts in ``STAGED`` with the constitution attached.

        Raises
        ------
        RuntimeError
            If conflicting identity arguments are provided, or if both
            ``constitution`` and ``data`` are set.
        ValueError
            If ``constitution`` is given without ``manifest`` or vice
            versa.

        Examples
        --------
        Plain payload, random UUID:

        .. code-block:: python

            e = Entry.variable(np.zeros(8))

        Nickname-derived identity:

        .. code-block:: python

            cfg = Entry.variable({"lr": 1e-3}, nickname="train_config")

        Lazy build from a constitution + manifest:

        .. code-block:: python

            src = "def f(m):\\n    return m['x'].data + m['y'].data\\n"
            e = Entry.variable(constitution=src, manifest=Manifest({"x": x, "y": y}))
            laila.build(e).wait()
        """
        if global_id is not None and (uuid is not None or evolution is not None):
            raise RuntimeError("Cannot set both global_id and <uuid, evolution> at the same time.")

        if constitution is not None and data is not None:
            raise RuntimeError("Cannot set both constitution and data.")

        if (constitution is None) != (manifest is None):
            raise ValueError(
                "`constitution` and `manifest` must both be provided together."
            )

        if global_id is not None:
            identity_data = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)
            uuid = identity_data["uuid"]
            evolution = identity_data["evolution"]

        evolution = evolution if evolution is not None else 0

        if nickname is not None:
            uuid = cls.generate_uuid_from_nickname(nickname)

        if constitution is not None:
            return Entry(
                constitution=constitution,
                manifest=manifest,
                evolution=evolution,
                uuid=uuid,
            )

        if state is None:
            state = EntryState.READY

        return Entry(
            data = data,
            evolution = evolution,
            state = state,
            uuid = uuid
        )
        

    def evolve(self, data=None):
        """Return a new Entry representing the next evolution.

        Unlike a typical mutator, :meth:`evolve` does **not** modify
        ``self``. Instead it returns a freshly constructed
        :class:`Entry` that shares the original's identity -- same
        ``uuid`` and ``scopes`` -- but carries:

        - the new payload supplied via *data*, and
        - ``evolution = self.evolution + 1``.

        The new entry is ``state == READY``. The original entry is
        left untouched, so callers must rebind to capture the next
        version:

        .. code-block:: python

            v = laila.Entry.variable(data=[1, 2, 3])
            v = v.evolve([1, 2, 3, 4])  # new entry, same uuid, evolution += 1

        Identity snapshotting (``_uuid``, ``_scopes``, ``_evolution``)
        happens under the entry's per-instance ``atomic`` lock so that
        a concurrent reader never observes a torn (uuid, evolution)
        pair while a new evolution is being branched off.

        Parameters
        ----------
        data : Any, optional
            New payload value for the returned entry. ``None`` yields
            an entry with no payload.

        Returns
        -------
        Entry
            A new :class:`Entry` with ``uuid`` and ``scopes`` matching
            ``self`` and ``evolution`` equal to ``self.evolution + 1``.

        Raises
        ------
        RuntimeError
            If the Entry is a constant (``evolution is None``).
        NotImplementedError
            If the entry still carries an unbuilt constitution.
            Constitution-driven payload changes are out of scope here;
            materialize the entry first via :func:`laila.build` or
            attach a fresh constitution explicitly.
        """
        if self._evolution is None:
            raise RuntimeError("Can't evolve a constant.")

        if self.constitution is not None:
            raise NotImplementedError(
                "Entry has not been built yet, internal logic cannot change "
                "payload while constitution is not None."
            )

        with self.atomic(scope="local"):
            next_evolution = self._evolution + 1
            scopes_snapshot = list(self._scopes)
            uuid_snapshot = self._uuid

        return Entry.contingent(
            uuid=uuid_snapshot,
            scopes=scopes_snapshot,
            evolution=next_evolution,
            data=data,
            state=EntryState.READY,
        )



    ###################################################
    # Constant
    ###################################################
    
    @classmethod
    def constant(
        cls, 
        data, 
        *,
        global_id = None,
        uuid = None,
        nickname = None
    ):
        """Create an *immutable* Entry whose ``evolution`` is :data:`None`.

        Constants do not support :meth:`evolve` and therefore do not
        carry an evolution suffix in their global_id. The combination
        ``(uuid, scopes)`` uniquely identifies a constant.

        This factory is the right choice for immutable artefacts that
        you want to address by nickname across processes -- model
        checkpoints, configuration blobs, pretrained weights, etc.

        Parameters
        ----------
        data : Any
            The raw payload. Required (no constitution path on
            constants).
        global_id : str, optional
            Composite identifier. Must NOT include an evolution
            component (since constants don't have one).
        uuid : str, optional
            Explicit UUID. Mutually exclusive with ``global_id``.
        nickname : str, optional
            Human-readable name used to derive a deterministic UUID-5
            against the active namespace.

        Returns
        -------
        Entry
            A new constant Entry, ``state == READY``, ``evolution is None``.

        Raises
        ------
        RuntimeError
            If both ``global_id`` and ``uuid`` are supplied, or if
            ``global_id`` includes an evolution component.

        Examples
        --------
        .. code-block:: python

            checkpoint = Entry.constant(weights, nickname="resnet50_v1")
            laila.memorize(checkpoint).wait()
            # ... in another process, with the same active namespace:
            same = laila.remember(nickname="resnet50_v1").wait()
        """
        if global_id is not None and (uuid is not None):
            raise RuntimeError("Cannot set both global_id and uuid at the same time.")

        if uuid is None and global_id is not None:
            identity_data = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)
            uuid = identity_data["uuid"]
            if identity_data["evolution"] is not None:
                raise RuntimeError("Cannot have a constant with an evolution.")

        if nickname is not None:
            uuid = cls.generate_uuid_from_nickname(nickname)
            
        new_entry = Entry(
            uuid = uuid,
            data = data,
            state=EntryState.READY,
            evolution=None 
        )
        
        return new_entry



    ###################################################
    # Contingent
    ###################################################
    @classmethod
    def contingent(cls, **kwargs):
        """Create an Entry from raw keyword arguments, bypassing the
        consistency checks performed by :meth:`constant` / :meth:`variable`.

        This is the escape hatch for advanced callers (test fixtures,
        deserializers, framework code) that already know the kwargs
        are well-formed and don't want to be slowed down by mutual-
        exclusivity checks.

        End users should almost always prefer :meth:`constant` or
        :meth:`variable`.

        Parameters
        ----------
        **kwargs
            Forwarded directly to the :class:`Entry` constructor; see
            its docstring for the accepted keys.

        Returns
        -------
        Entry
            A new Entry, configured exactly as the kwargs describe.
        """
        return Entry(**kwargs)


    ###################################################
    # Serialize and Recovery
    ###################################################

    def as_dict(self) -> dict:
        """Return the unified serialized shape for this entry.

        Unlike :meth:`serialize`, this method does NOT apply any
        :class:`TransformationSequence` to the payload -- it embeds the
        raw payload value directly. Use it for in-process inspection
        (e.g. converting an entry to JSON for logging) where binary
        round-tripping is not needed.

        Returned keys (mirroring the on-disk schema):

        - ``_uuid`` : str
        - ``_evolution`` : int or None
        - ``_scopes`` : list[str]
        - ``_state`` : str (the :class:`EntryState` member name)
        - ``payload`` : Any (the unwrapped payload value, or None)
        - ``constitution`` : dict or None (output of
          :meth:`Constitution.as_dict`)

        Returns
        -------
        dict
            Plain Python dict suitable for JSON serialization (assuming
            the payload itself is JSON-friendly).
        """
        constitution_dict = (
            self._constitution.as_dict() if self._constitution is not None else None
        )
        payload_value = None
        if self._payload is not None:
            payload_value = self._payload.data
        return {
            "_uuid": self._uuid,
            "_evolution": self._evolution,
            "_scopes": list(self._scopes),
            "_state": self._state.name,
            "payload": payload_value,
            "constitution": constitution_dict,
        }

    def serialize(
        self,
        transformations: TransformationSequence = None,
    ) -> dict:
        """Serialize the Entry into a plain dict suitable for pool storage.

        Pipeline (when *transformations* is provided)
        ---------------------------------------------
        1. The payload is serialized to bytes via its
           :class:`ComputationalData` ``.serialize()`` method, which also
           emits an *inverse* code string (e.g. ``"def f(b): return
           pickle.loads(b)"``) capable of recovering the original
           Python value from the bytes.
        2. The serialized bytes are fed through the
           :class:`TransformationSequence` (e.g. zlib then base64), each
           of which emits its own inverse code string.
        3. All inverse code strings are bundled into a fresh
           :class:`SimpleConstitution` and packed into the returned dict
           under ``constitution``. On read, that constitution is run
           against the stored ``payload`` to recover the original value
           bit-for-bit.

        When *transformations* is ``None``, the live :class:`Entry`
        instance is returned unchanged -- the in-memory pool path uses
        this to skip serialization entirely.

        Parameters
        ----------
        transformations : TransformationSequence, optional
            Pipeline applied to the serialized payload bytes. Pass the
            same sequence the destination pool uses; the inverse chain
            recorded in the result lets the read side rebuild without
            knowing the pool's transformations.

        Returns
        -------
        Entry or dict
            The live ``Entry`` when *transformations* is ``None``;
            otherwise a serialized dict with keys ``_uuid``,
            ``_evolution``, ``_scopes``, ``_state``, ``payload``, and
            ``constitution``.

        Raises
        ------
        RuntimeError
            If the entry is not :class:`EntryState.READY` (we refuse to
            persist staged / stale entries because their payload is not
            yet a faithful snapshot).
        """
        if self._state != EntryState.READY:
            raise RuntimeError(
                f"Cannot serialize entry in state {self._state.name!r}; "
                "only READY entries can be serialized."
            )

        if transformations is None:
            return self

        if self._payload is not None:
            serialized_payload, payload_backward_code = self._payload.serialize()
            transformed_payload, transformation_inverse_code = transformations.forward(
                serialized_payload
            )
            codes = transformation_inverse_code + [payload_backward_code]
        else:
            transformed_payload = None
            codes = []

        constitution = SimpleConstitution(codes=codes)

        return {
            "_uuid": self._uuid,
            "_evolution": self._evolution,
            "_scopes": list(self._scopes),
            "_state": self._state.name,
            "payload": transformed_payload,
            "constitution": constitution.as_dict(),
        }

    @classmethod
    def from_dict(cls, in_dict: dict) -> "Entry":
        """Hydrate an Entry from a serialized dict *without* running its constitution.

        This is the "raw" deserialization step -- it reconstructs
        identity, attaches the stored payload as-is, and re-attaches
        the constitution if one was serialized. The recovery chain is
        NOT executed; callers must invoke :meth:`_build_inplace`
        themselves (or use :meth:`_build_from_dict_sync` /
        :meth:`_build_from_dict_async` which combine both steps).

        Parameters
        ----------
        in_dict : dict
            A dict produced by :meth:`serialize` (or :meth:`as_dict`),
            with the same keys.

        Returns
        -------
        Entry
            A fresh Entry with identity restored, payload set to the
            raw stored value (typically still encoded), constitution
            re-attached, and state taken from the dict.
        """
        entry = cls.__new__(cls)
        Entry._initialize_identity(entry, {
            "uuid": in_dict["_uuid"],
            "evolution": in_dict.get("_evolution"),
            "scopes": in_dict.get("_scopes"),
        })
        entry.data = in_dict.get("payload")
        entry.state = EntryState[in_dict.get("_state", "STAGED")]
        entry.constitution = Constitution.from_dict(in_dict.get("constitution"))
        return entry

    @classmethod
    def _build_from_dict_sync(cls, in_dict) -> "Entry":
        """Synchronously hydrate an entry from a serialized dict.

        Combines :meth:`from_dict` (raw rebuild) with
        :meth:`_build_inplace` for the SimpleConstitution case so the
        common "fetch from pool, immediately use" flow returns a
        ``READY`` entry in one call.

        Behavior by input shape
        -----------------------
        - **str** -- parsed as JSON; falls through to the dict path.
        - **dict** -- :meth:`from_dict` followed by an inline
          :meth:`_build_inplace` if the constitution is a
          :class:`SimpleConstitution`. ``ComplexConstitution`` entries
          are left STAGED -- materialising them requires ``laila.build``
          which submits to a taskforce, and we don't want to recurse
          into the taskforce machinery from a deserialization path.
        - **Entry** -- returned as-is. (Already-live entries flowing
          through serialization paths are a no-op.)

        Returns
        -------
        Entry
            The hydrated entry. ``READY`` for simple-constitution
            inputs, ``STAGED`` for complex.

        Raises
        ------
        ValueError
            If *in_dict* is a string that fails to parse as JSON.
        RuntimeError
            For any other unsupported input type.
        """
        local = in_dict
        if isinstance(local, str):
            try:
                local = json.loads(local)
            except Exception as e:
                raise ValueError("Invalid JSON string") from e

        if isinstance(local, Entry):
            return local

        if not isinstance(local, dict):
            raise RuntimeError("Invalid input for entry build.")

        entry = cls.from_dict(local)
        if isinstance(entry._constitution, SimpleConstitution):
            entry._build_inplace()
        return entry

    @classmethod
    async def _build_from_dict_async(cls, in_dict) -> "Entry":
        """Async variant of :meth:`_build_from_dict_sync`.

        Used inside the per-entry coroutines submitted by
        :meth:`_parallel_individual_fetch`. Although the body looks
        identical to the sync version, having an async entry point
        means callers can ``await`` it without context-switching to
        a worker thread: the from_dict step is pure Python, and the
        SimpleConstitution branch is pure CPU, so nothing blocks the
        loop here.

        Important: both the sync and async paths *always* run the
        SimpleConstitution chain on the freshly-hydrated entry, even
        when the serialized ``_state`` is already ``READY``. That's
        because the SimpleConstitution attached on serialize *is* the
        recipe for reversing the pool's storage transformations -- the
        ``READY`` state on disk just records what the entry's lifecycle
        was at memorize time, not whether the on-disk bytes are already
        the user's original payload.
        """
        local = in_dict
        if isinstance(local, str):
            try:
                local = json.loads(local)
            except Exception as e:
                raise ValueError("Invalid JSON string") from e

        if isinstance(local, Entry):
            return local

        if not isinstance(local, dict):
            raise RuntimeError("Invalid input for entry build.")

        entry = cls.from_dict(local)
        if isinstance(entry._constitution, SimpleConstitution):
            entry._build_inplace()
        return entry

    @classmethod
    def _build_from_dict(cls, in_dict, *, asynchronous: bool = False):
        """Router: dispatch to async or sync ``_build_from_dict``."""
        if asynchronous:
            return cls._build_from_dict_async(in_dict)
        return cls._build_from_dict_sync(in_dict)

    ###################################################
    # String Representation
    ###################################################

    def __str__(self):
        """Return the global identifier string."""
        return self.global_id
    
    def __repr__(self):
        """Return the global identifier string."""
        return self.global_id


from .constitution.build_maps import register_builder
register_builder(
    _ENTRY_SCOPE,
    Entry._build_from_dict_sync,
    Entry._build_from_dict_async,
)