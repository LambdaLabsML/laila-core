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
from ..atomic.definitions.locall_atomic_identifiable_object import _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT
from ..utils.decorators.synchronized import synchronized
from .compdata.transformation import TransformationSequence


class Entry( 
    _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT
):

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

        
        self._initialize_identity(data)
        self._initialize_payload(data)
        self._initialize_constitution(data)
        self._initialize_state(data)
        

    def _initialize_identity(self, data: dict) -> None:
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
        payload = data.get("payload", data.get("data", None))
        if payload is not None and not isinstance(payload,ComputationalData):
            payload = ComputationalData(payload)
            
        self._payload = payload

    def _initialize_constitution(self, data: dict) -> None:
        constitution = data.get("constitution", None)
        if constitution is not None:
            self._constitution = EntryConstitution(constitution=constitution)


    def _initialize_state(self, data: dict) -> None:
        constitution = data.get("constitution", None)
        if constitution is not None:
            self._state = EntryState.STAGED
        else:
            self._state = data.get("state", EntryState.STAGED)



    ###################################################
    # Policy
    ###################################################

    def notify_policy(self):
        from .. import active_policy
        if active_policy is not None:
            active_policy.update(self)
    

    ###################################################
    # Properties
    ###################################################


    @property
    @synchronized
    def data(self) -> Optional[Any]:
        if self._payload == None:
            return None
        return self._payload.data

        

    @property
    @synchronized
    def state(self) -> EntryState:
        return self._state


    @state.setter
    @synchronized
    def state(self, new_state: EntryState):
        if not isinstance(new_state, EntryState):
            raise ValueError("state must be an EntryState")
        self._state = new_state
        self.notify_policy()


    @property
    @synchronized
    def metadata(self):
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
        return self.global_id
    
    def __repr__(self):
        return self.global_id