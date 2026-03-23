"""Core ``Entry`` class — the fundamental unit of data in LAILA."""

from __future__ import annotations

import copy
from types import NoneType
from typing import Any, Dict, Hashable, List, Sequence, Tuple, Union

import numpy as np
from PIL import Image
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
from .entry_constitution import EntryConstitution

from ..macros.strings import _ENTRY_SCOPE
from ..atomic.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
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

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_ENTRY_SCOPE]))
    _state: EntryState = PrivateAttr(default=EntryState.STAGED)
    _constitution: Optional[EntryConstitution] = PrivateAttr(default = None)
    _payload: Optional[ComputationalData] = PrivateAttr(default=None)

    #TODO: don't allow constitution and state to be set at the same time. 
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
        
        _LAILA_IDENTIFIABLE_OBJECT.__init__(self, **identity_data)

    def _initialize_payload(self, data: dict) -> None:
        """Wrap raw payload data in a ``ComputationalData`` instance."""
        payload = data.get("payload", data.get("data", None))
        if payload is not None and not isinstance(payload,ComputationalData):
            payload = ComputationalData(payload)
            
        self._payload = payload

    def _initialize_constitution(self, data: dict) -> None:
        """Set up the ``EntryConstitution`` if one was provided."""
        constitution = data.get("constitution", None)
        if constitution is not None:
            self._constitution = EntryConstitution(constitution=constitution)


    def _initialize_state(self, data: dict) -> None:
        """Determine the initial ``EntryState`` from *data*."""
        constitution = data.get("constitution", None)
        if constitution is not None:
            self._state = EntryState.STAGED
        else:
            self._state = data.get("state", EntryState.STAGED)



    ###################################################
    # Policy
    ###################################################

    def notify_policy(self):
        """Notify the active policy, if any, that this Entry has changed."""
        from .. import active_policy
        if active_policy is not None:
            active_policy.update(self)
    

    ###################################################
    # Properties
    ###################################################


    @property
    @synchronized
    def data(self) -> Optional[Any]:
        """Unwrapped payload data, or ``None`` if no payload is set."""
        if self._payload == None:
            return None
        return self._payload.data

        

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
            If *new_state* is not an ``EntryState``.
        """
        if not isinstance(new_state, EntryState):
            raise ValueError("state must be an EntryState")
        self._state = new_state
        self.notify_policy()


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
    





    ###################################################
    # Variable
    ###################################################

    @classmethod
    def variable(
        cls, 
        data,
        *, 
        uuid = None,
        evolution=None,
        state = None,
        constitution=None, 
        global_id = None,
        nickname = None
    ):
        """Create a mutable (evolvable) Entry.

        Parameters
        ----------
        data : Any
            The raw payload.
        uuid : str, optional
            Explicit UUID.  Mutually exclusive with *global_id*.
        evolution : int, optional
            Starting evolution counter (defaults to ``0``).
        state : EntryState, optional
            Initial state; auto-set to ``READY`` when no constitution is given.
        constitution : callable, optional
            Not yet implemented.
        global_id : str, optional
            Composite ``uuid:evolution`` identifier.
        nickname : str, optional
            Human-readable name used to derive a deterministic UUID.

        Returns
        -------
        Entry
            A new variable Entry.

        Raises
        ------
        RuntimeError
            If conflicting identity arguments are provided or both
            *constitution* and *data* are set.
        NotImplementedError
            If a constitution is supplied.
        """
        if global_id is not None and (uuid is not None or evolution is not None):
            raise RuntimeError("Cannot set both global_id and <uuid, evolution> at the same time.")

        if constitution and data:
            raise RuntimeError("Cannot set both constitution and data.")

        if constitution is None:
            state = EntryState.READY

        if constitution is not None:
            raise NotImplementedError("Constitution is not implemented in this release.")

        if global_id is not None:
            identity_data = _LAILA_IDENTIFIABLE_OBJECT.process_global_id(global_id)
            uuid = identity_data["uuid"]
            evolution = identity_data["evolution"]

        evolution = evolution if evolution is not None else 0
        
        if nickname is not None:
            uuid = cls.generate_uuid_from_nickname(nickname)
            
        return Entry(
            data = data,
            evolution = evolution,
            state = state,
            constitution = constitution,
            uuid = uuid
        )
        

    @synchronized
    def evolve(
        self,
        precedence:  Optional[List[str]] = None,
        constitution = None,
        data = None
    ):
        """Advance the evolution counter and replace the payload.

        Parameters
        ----------
        precedence : list of str, optional
            Reserved for constitution-based evolution.
        constitution : callable, optional
            Not yet implemented.
        data : Any, optional
            New payload data.

        Raises
        ------
        RuntimeError
            If the Entry is a constant or both *constitution* and *data* are
            provided.
        NotImplementedError
            If a constitution is supplied.
        """
        if self._evolution is None:
            raise RuntimeError("Can't evolve a constant.")
        
        if constitution and data:
            raise RuntimeError("Cannot set both constitution and data.")
        
        if constitution is not None:
            raise NotImplementedError("Constitution is not implemented in this release.")

        
        if constitution is None:
            with self.atomic():
                self._initialize_payload({"data": data})
                self._evolution = self._evolution + 1
        else:
            raise NotImplementedError("Constitution is not implemented in this release.")



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

    def serialize(
        self, 
        transformations: TransformationSequence = None,
        *,
        exclude_private: set = {"_local_lock","_payload","_state"}
    ) -> dict:
        """Serialise the Entry into a plain dict suitable for storage.

        Parameters
        ----------
        transformations : TransformationSequence, optional
            Pipeline applied to the serialised payload bytes.  If ``None``
            the raw Entry is returned.
        exclude_private : set, optional
            Private attribute names to omit from the output dict.

        Returns
        -------
        dict
            Serialised representation including ``transformed_payload`` and
            ``recovery_sequence`` keys.
        """
        if transformations is None:
            return self
        
        entry_dict = self.model_dump()

        private_attrs = {
            name: getattr(self, name)
            for name in self.__private_attributes__
            if name not in exclude_private
        }

        entry_dict.update(private_attrs)


        if self._payload is not None:
            serialized_payload, recovery_code = self._payload.serialize()
        else:
            serialized_payload = None
            recovery_code = "null_fn = lambda x:x"
            

        transformed_payload, transformation_inverse_code = transformations.forward(serialized_payload)

        transformation_inverse_code.append(recovery_code)

        entry_dict.update({
            "transformed_payload": transformed_payload,
            "recovery_sequence": transformation_inverse_code,
            "_state": self.state.name
        })


        return entry_dict
            
            
    @classmethod
    def recover(cls, in_dict: dict, notify_on_creation=False):
        """Reconstruct an Entry from a serialised dict or JSON string.

        Parameters
        ----------
        in_dict : dict or str or Entry
            Serialised representation produced by ``serialize()``, a JSON
            string thereof, or an existing Entry (returned as-is).
        notify_on_creation : bool, optional
            If ``True``, notify the active policy after construction.

        Returns
        -------
        Entry
            The recovered Entry instance.

        Raises
        ------
        ValueError
            If *in_dict* is an invalid JSON string.
        RuntimeError
            If the input type is unsupported.
        """
        if isinstance(in_dict, Entry):
            return in_dict

        if isinstance(in_dict, str):
            try:
                in_dict = json.loads(in_dict)
            except Exception as e:
                raise ValueError("Invalid JSON string") from e

        if isinstance (in_dict, dict):
            recovered = Entry(
                uuid = in_dict["_uuid"],
                evolution = in_dict["_evolution"],
                constitution = in_dict["_constitution"],
                payload = ComputationalData.recover(
                    payload_blob = in_dict["transformed_payload"],
                    recovery_sequence = in_dict["recovery_sequence"]
                ),
                state = EntryState[in_dict["_state"]],
                notify_on_creation = notify_on_creation
            )
            
            return recovered

        raise RuntimeError("Invalid input for entry recovery.")

    ###################################################
    # Policy Communication
    ###################################################


    ###################################################
    # String Representation
    ###################################################

    def __str__(self):
        """Return the global identifier string."""
        return self.global_id
    
    def __repr__(self):
        """Return the global identifier string."""
        return self.global_id