"""Core ``Entry`` class — the fundamental unit of data in LAILA."""

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

    An ``Entry`` wraps arbitrary data (tensors, images, dicts, etc.) with
    identity (UUID + evolution counter), state tracking, and serialization
    capabilities.  Entries can be *constants* (immutable) or *variables*
    (evolvable).

    Create entries via the class methods ``Entry.variable(...)`` or
    ``Entry.constant(...)`` rather than calling the constructor directly.
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

        Parameters
        ----------
        **data : dict
            Accepted keys include ``data`` / ``payload``, ``uuid``,
            ``evolution``, ``global_id``, ``nickname``, ``constitution``,
            and ``state``.
        """
        self._initialize_identity(data)
        self._initialize_payload(data)
        self._initialize_constitution(data)
        self._initialize_state(data)
        

    def _initialize_identity(self, data: dict) -> None:
        """Parse identity fields from *data* and initialise the parent identity."""
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
        """Wrap raw payload data in a ``ComputationalData`` instance."""
        self.data = data.get("payload", data.get("data", None))

    def _initialize_constitution(self, data: dict) -> None:
        """Set up a ``Constitution`` if one was provided.

        Accepts:
          * a `Constitution` instance — used as-is;
          * a `str` plus a `manifest` — wrapped in a `ComplexConstitution`;
          * a `list[str]` — wrapped in a `SimpleConstitution`.

        The attached constitution is cleared by ``build()`` once
        materialization succeeds.
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
        """Determine the initial ``EntryState`` from *data*."""
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
    @synchronized
    def data(self) -> Optional[Any]:
        """Unwrapped payload data, building on first access if a constitution is attached.

        When this Entry was created with a constitution (and has not yet
        been built), the first call to ``.data`` triggers ``build()`` and
        blocks on the returned future so callers see a fully materialized
        payload.
        """
        if self._payload is None and self._constitution is not None:
            self.build().wait(None)
        if self._payload is None:
            return None
        return self._payload.data


    @data.setter
    @synchronized
    def data(self, new_data: Optional[Any]) -> None:
        """Replace the payload data.

        Raw values are wrapped in a ``ComputationalData`` automatically.
        Pass ``None`` to clear the payload.
        """
        if new_data is not None and not isinstance(new_data, ComputationalData):
            new_data = ComputationalData(new_data)
        self._payload = new_data


    @property
    @synchronized
    def constitution(self):
        """Attached ``Constitution``, or ``None`` once the entry has been built."""
        return self._constitution


    @constitution.setter
    @synchronized
    def constitution(self, new_constitution: Optional[Constitution]) -> None:
        """Attach or clear the entry's ``Constitution``.

        Raises
        ------
        TypeError
            If *new_constitution* is neither a ``Constitution`` nor ``None``.
        """
        if new_constitution is not None and not isinstance(new_constitution, Constitution):
            raise TypeError("constitution must be a Constitution or None")
        self._constitution = new_constitution


    @property
    @synchronized
    def state(self) -> EntryState:
        """Current lifecycle state of this Entry."""
        return self._state


    @state.setter
    @synchronized
    def state(self, new_state: EntryState):
        """Set a new lifecycle state.

        Raises
        ------
        ValueError
            If *new_state* is not an ``EntryState``, or if this subclass
            disallows non-``NA`` states.
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

    def build(self, taskforce=None):
        """Submit constitution-driven materialization to a taskforce.

        Returns immediately with a ``Future``. When the task completes, the
        payload is populated, ``_state`` becomes ``READY``, ``_constitution``
        is cleared, and the policy is notified — at which point the entry
        is indistinguishable from a regular built entry.

        Parameters
        ----------
        taskforce : str | _LAILA_IDENTIFIABLE_TASK_FORCE, optional
            Target taskforce (ID string or instance). Defaults to the
            active policy's alpha taskforce.

        Returns
        -------
        Future
            Resolves once the entry has been fully materialized.

        Raises
        ------
        RuntimeError
            If the entry is already built, or has no constitution attached.
        """
        if self._state == EntryState.READY:
            raise RuntimeError("Entry is already built.")
        if self._constitution is None:
            raise RuntimeError("Entry has no constitution attached.")

        def _build_task():
            self._build_inplace()
            return self

        import laila
        command = laila.get_active_policy().central.command

        if taskforce is None:
            taskforce_id = command.alpha_taskforce
        elif isinstance(taskforce, str):
            taskforce_id = taskforce
        else:
            taskforce_id = taskforce.global_id

        return command.submit([_build_task], taskforce_id=taskforce_id)

    def _build_inplace(self):
        """Run the attached constitution synchronously, in place.

        Threads the current payload (if any) into the constitution as
        ``payload_input`` and routes the result through ``_post_build``.
        Used by both the instance-level ``build()`` task body and the
        simple-entry branch of the classmethod ``Entry.build(in_dict)``.
        """
        if self._constitution is None:
            raise RuntimeError("Entry has no constitution attached.")

        payload_input = self._payload.data if self._payload is not None else None
        result = self._constitution.build(payload_input=payload_input)
        self._post_build(result)
        self.constitution = None
        self.state = EntryState.READY

    def _post_build(self, result):
        """Place the build result into the entry. Subclasses may override."""
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
        """Create a mutable (evolvable) Entry.

        Parameters
        ----------
        data : Any, optional
            The raw payload. Mutually exclusive with *constitution*/*manifest*.
        uuid : str, optional
            Explicit UUID.  Mutually exclusive with *global_id*.
        evolution : int, optional
            Starting evolution counter (defaults to ``0``).
        state : EntryState, optional
            Initial state; auto-set to ``READY`` when no constitution is given.
        constitution : str, optional
            Python source-string defining exactly one function that takes a
            ``Manifest`` and returns the payload. When provided, *manifest*
            is required; the returned entry lazily builds itself on first
            ``.data`` access (or explicit ``build()``).
        manifest : Manifest, optional
            Manifest feeding the constitution. Required when *constitution*
            is set.
        global_id : str, optional
            Composite ``uuid:evolution`` identifier.
        nickname : str, optional
            Human-readable name used to derive a deterministic UUID.

        Returns
        -------
        Entry
            A new variable Entry. When a constitution is supplied the entry
            starts in ``STAGED`` with the constitution attached.

        Raises
        ------
        RuntimeError
            If conflicting identity arguments are provided or both
            *constitution* and *data* are set.
        ValueError
            If *constitution* is given without *manifest* or vice versa.
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
        

    @synchronized
    def evolve(self, data=None):
        """Advance the evolution counter and replace the payload.

        Parameters
        ----------
        data : Any, optional
            New payload data.

        Raises
        ------
        RuntimeError
            If the Entry is a constant.
        NotImplementedError
            If the entry still carries an unbuilt constitution; access
            ``.data`` (or call ``build()``) first to materialize the payload.
        """
        if self._evolution is None:
            raise RuntimeError("Can't evolve a constant.")

        if self.constitution is not None:
            raise NotImplementedError(
                "Entry has not been built yet, internal logic cannot change "
                "payload while constitution is not None."
            )

        with self.atomic():
            self.data = data
            self._evolution = self._evolution + 1



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
        """Create an immutable Entry (evolution is ``None``).

        Parameters
        ----------
        data : Any
            The raw payload.
        global_id : str, optional
            Composite identifier (must not contain an evolution component).
        uuid : str, optional
            Explicit UUID.  Mutually exclusive with *global_id*.
        nickname : str, optional
            Human-readable name used to derive a deterministic UUID.

        Returns
        -------
        Entry
            A new constant Entry.

        Raises
        ------
        RuntimeError
            If conflicting identity arguments are provided or the
            *global_id* includes an evolution component.
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
        """Create an Entry from raw keyword arguments without validation guards.

        Parameters
        ----------
        **kwargs
            Forwarded directly to the ``Entry`` constructor.

        Returns
        -------
        Entry
        """
        return Entry(**kwargs)


    ###################################################
    # Serialize and Recovery
    ###################################################

    def as_dict(self) -> dict:
        """Return the unified serialized shape for this entry.

        Includes identity, scopes, current state, payload (if materialized
        and serialized inline), and the attached constitution (if any).
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
        """Serialise the Entry into a plain dict suitable for storage.

        Only entries in `EntryState.READY` may be serialized.

        Parameters
        ----------
        transformations : TransformationSequence, optional
            Pipeline applied to the serialised payload bytes.  If ``None``
            the raw Entry is returned.

        Returns
        -------
        Entry or dict
            The Entry itself when *transformations* is ``None``; otherwise
            a serialised dict with the keys ``_uuid``, ``_evolution``,
            ``_scopes``, ``_state``, ``payload``, and ``constitution``.

        Raises
        ------
        RuntimeError
            If the entry is not in `EntryState.READY`.
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
        """Hydrate an Entry from a serialized dict (no chain execution).

        Returns an entry with identity, payload (if any), and constitution
        attached.  State is taken from the dict; no build is run.
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
    def build_from_dict(cls, in_dict, taskforce=None):
        """Hydrate an entry from a serialized dict and return a Future.

        For a `SimpleConstitution`, the build chain runs inside the same
        task and the future resolves to a `READY` entry with its payload
        materialized.  For a `ComplexConstitution`, the future resolves
        to a `STAGED` entry with the constitution attached; the caller
        must call `entry.build()` (or access `.data`) to materialize.

        Parameters
        ----------
        in_dict : dict or str or Entry
            Serialized representation produced by `serialize`, a JSON
            string thereof, or a live `Entry` (returned as-is via a
            resolved future).
        taskforce : str or instance, optional
            Target taskforce; defaults to the active policy's alpha
            taskforce.

        Returns
        -------
        Future
            Resolves to the hydrated `Entry`.
        """
        if isinstance(in_dict, str):
            try:
                in_dict = json.loads(in_dict)
            except Exception as e:
                raise ValueError("Invalid JSON string") from e

        if isinstance(in_dict, Entry):
            entry_obj = in_dict
            def _passthrough_task():
                return entry_obj
            import laila
            command = laila.get_active_policy().central.command
            tf_id = command.alpha_taskforce if taskforce is None else (
                taskforce if isinstance(taskforce, str) else taskforce.global_id
            )
            return command.submit([_passthrough_task], taskforce_id=tf_id)

        if not isinstance(in_dict, dict):
            raise RuntimeError("Invalid input for entry build.")

        captured_dict = in_dict

        def _build_task():
            entry = cls.from_dict(captured_dict)
            if isinstance(entry._constitution, SimpleConstitution):
                entry._build_inplace()
            return entry

        import laila
        command = laila.get_active_policy().central.command
        tf_id = command.alpha_taskforce if taskforce is None else (
            taskforce if isinstance(taskforce, str) else taskforce.global_id
        )
        return command.submit([_build_task], taskforce_id=tf_id)

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
register_builder(_ENTRY_SCOPE, Entry.build_from_dict)