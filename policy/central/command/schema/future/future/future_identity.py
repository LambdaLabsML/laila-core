from ast import Str
from pydantic import BaseModel, Field, PrivateAttr
import uuid
from typing import Optional, List, Dict, Any
import json

from .future_status import FutureStatus
from .......atomic.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .......atomic.definitions.locally_atomic_identifiable_object import _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT


from .......macros.strings import _FUTURE_SCOPE


class _LAILA_IDENTIFIABLE_FUTURE(_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT):
    """
    Identity metadata for a Future.

    Parameters
    ----------
    task_id
        Unique ID for this Future instance.
    taskforce_id
        Unique ID for the taskforce that owns this Future.
    policy_id
        Unique ID for the policy instance that created this Future.
    task_group_id
        Optional group identifier used by GroupFuture.
    precedence
        Optional precedence list for execution ordering.
    purpose
        Optional human-readable purpose string.
    """


    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_FUTURE_SCOPE]))

    taskforce_id: _LAILA_IDENTIFIABLE_OBJECT|str

    policy_id: _LAILA_IDENTIFIABLE_OBJECT|str

    future_group_id: Optional[_LAILA_IDENTIFIABLE_OBJECT|str] = None

    #is this created by another future?
    precedence: Optional[_LAILA_IDENTIFIABLE_OBJECT|str] = None

    purpose: Optional[str] = None


    def __str__(self) -> str:
        """
        Return a human-readable, line-by-line representation.
        """
        lines = [
            f"future_id: {self.global_id}",
            f"taskforce_id: {self.taskforce_id}",
            f"policy_id: {self.policy_id}",
            f"future_group_id: {self.future_group_id}",
            f"precedence: {self.precedence}",
            f"purpose: {self.purpose}",
        ]
        return "\n".join(lines)

    def __repr__(self) -> str:
        """
        Return the same representation as __str__ for readability.
        """
        return self.__str__()

    def as_dict(self) -> Dict[str, Any]:
        """
        Return a dict with all identity fields.
        """
        return {
            "task_id": self.task_id,
            "taskforce_id": self.taskforce_id,
            "policy_id": self.policy_id,
            "task_group_id": self.task_group_id,
            "precedence": self.precedence,
            "purpose": self.purpose,
        }
    
    def __json__(self) -> str:
        """
        Return a JSON string for the identity fields.
        """
        return json.dumps(self.as_dict())
    
