"""Identity model for a Laila future -- lightweight metadata without result state.

A :class:`_LAILA_IDENTIFIABLE_FUTURE` carries only the *identity* of a
future: the gid of the task-force and policy that own it, optional
group / precedence / purpose annotations, and the inherited UUID +
scopes. It does NOT carry the result, exception, or status -- those
live in the concrete :class:`Future` registered in the owning policy's
``future_bank``. The identity object instead resolves all status /
result / exception accessors *through* the bank, so any process that
holds an identity can introspect the live future as long as the owning
policy is reachable in :data:`_local_policies`.

The split exists so that taskforce-level submission can return a
lightweight identity to user code without forcing the production of a
full :class:`Future` instance per submission, and so that
:class:`RemoteFuture` can present the same identity-like API on the
remote side.
"""

from ast import Str
from pydantic import BaseModel, Field, PrivateAttr
import uuid
from typing import Optional, List, Dict, Any
import json

from .future_status import FutureStatus
from .......basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .......atomic.definitions.locally_atomic_identifiable_object import _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT


from .......macros.strings import _FUTURE_SCOPE


class _LAILA_IDENTIFIABLE_FUTURE(_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT):
    """Lightweight identity record for a Future.

    Carries only the metadata needed to *find* the live future in some
    policy's ``future_bank``; the result, exception, and status are
    properties that resolve through the bank on demand.

    Parameters
    ----------
    taskforce_id : _LAILA_IDENTIFIABLE_OBJECT or str
        gid of the task-force that owns this Future.
    policy_id : _LAILA_IDENTIFIABLE_OBJECT or str
        gid of the policy instance that created this Future.
    future_group_id : _LAILA_IDENTIFIABLE_OBJECT or str, optional
        gid of the parent :class:`GroupFuture`, when this future is a
        member of one.
    precedence : _LAILA_IDENTIFIABLE_OBJECT or str, optional
        gid of a future this one depends on. Reserved for future
        explicit-dependency scheduling; currently informational only.
    purpose : str, optional
        Human-readable label that surfaces in logs and diagnostics
        (e.g. ``"memorize:42"``, ``"build:my_model"``).
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

    @property
    def status(self):
        """Read-only status resolved from the owning policy's future bank."""
        from ....... import _local_policies
        gid = self.global_id
        for policy in _local_policies.values():
            if gid in policy.future_bank:
                return policy.future_bank[gid].status
        raise KeyError(f"Future {gid} not found in any local policy bank")

    @property
    def result(self):
        """Read-only result resolved from the owning policy's future bank."""
        from ....... import _local_policies
        gid = self.global_id
        for policy in _local_policies.values():
            if gid in policy.future_bank:
                return policy.future_bank[gid].result
        raise KeyError(f"Future {gid} not found in any local policy bank")

    @property
    def data(self):
        """Read-only unwrapped Entry payload, resolved through the owning policy's future bank."""
        from ....... import _local_policies
        pid = self.policy_id.global_id if hasattr(self.policy_id, "global_id") else self.policy_id
        policy = _local_policies.get(pid)
        if policy is None:
            raise KeyError(f"Owning policy {pid} for future {self.global_id} not found locally")
        try:
            return policy.future_bank[self.global_id].data
        except KeyError:
            raise KeyError(f"Future {self.global_id} not found in policy {pid}'s future bank")

    @property
    def exception(self):
        """Read-only exception resolved from the owning policy's future bank."""
        from ....... import _local_policies
        gid = self.global_id
        for policy in _local_policies.values():
            if gid in policy.future_bank:
                return policy.future_bank[gid].exception
        raise KeyError(f"Future {gid} not found in any local policy bank")

    def wait(self, timeout=None):
        """Block until the future completes, resolved through the future bank.

        Raises
        ------
        LoopBlockingWaitError
            If called from a thread that owns an async event loop. Use
            ``await fut`` from inside coroutines instead.
        """
        from ...exceptions import _check_not_loop_thread
        _check_not_loop_thread()

        from ....... import _local_policies
        gid = self.global_id
        for policy in _local_policies.values():
            if gid in policy.future_bank:
                return policy.future_bank[gid].wait(timeout)
        raise KeyError(f"Future {gid} not found in any local policy bank")

    def __await__(self):
        """Await the underlying concrete future via the policy's future bank.

        Delegates to the concrete future's ``__await__`` (each concrete
        class — ``ConcurrentPackageFuture``, ``ComplexFuture``,
        ``GroupFuture``, ``RemoteFuture`` — implements its own non-blocking
        completion signal).
        """
        from ....... import _local_policies
        gid = self.global_id
        for policy in _local_policies.values():
            if gid in policy.future_bank:
                return policy.future_bank[gid].__await__()
        raise KeyError(f"Future {gid} not found in any local policy bank")

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
    
