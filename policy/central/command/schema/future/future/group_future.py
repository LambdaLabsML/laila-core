"""GroupFuture — an aggregate future that tracks a set of child futures."""

from __future__ import annotations
from typing import Dict, List, Optional, Callable, Any
import asyncio
import threading
from pydantic import Field, ConfigDict, PrivateAttr
import json
from typing import Iterator

from .future import Future
from .future_status import FutureStatus

from .......basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .......macros.strings import _GROUP_FUTURE_SCOPE


def _get_future_bank():
    """Return the active policy's future bank mapping."""
    from ....... import get_active_policy
    return get_active_policy().future_bank


class GroupFuture(_LAILA_IDENTIFIABLE_OBJECT):
    """Aggregate future that groups multiple child futures under one handle."""

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_GROUP_FUTURE_SCOPE]))

    taskforce_id: _LAILA_IDENTIFIABLE_OBJECT|str
    policy_id: _LAILA_IDENTIFIABLE_OBJECT|str
    model_config = ConfigDict(arbitrary_types_allowed=True)

    future_ids: List[str] = Field(default_factory=list)

    def model_post_init(self, __context: Any) -> None:
        """Register this group future with the active policy's future bank."""
        from ....... import get_active_policy
        policy = get_active_policy()
        policy.central.command._register_future_with_active_guarantees(self)
        policy.future_bank[self.global_id] = self

    def _resolve_children(self) -> List[Future]:
        """Look up child Future objects from the future bank."""
        bank = _get_future_bank()
        return [bank[fid] for fid in self.future_ids]

    # ---------- computed status ----------
    @property
    def status(self) -> Dict[str, Any]:
        """Return percentage breakdown of child statuses."""
        if not self.future_ids:
            return {
                "total": 0.0,
                "percentages": {
                    "finished": 0.0,
                    "running": 0.0,
                    "not_started": 100.0,
                    "error": 0.0,
                    "cancelled": 0.0,
                },
            }

        children = self._resolve_children()
        total = float(len(children))
        statuses = [f.status for f in children]
        running = sum(1 for s in statuses if s == FutureStatus.RUNNING)
        not_started = sum(1 for s in statuses if s == FutureStatus.NOT_STARTED)
        cancelled = sum(1 for s in statuses if s == FutureStatus.CANCELLED)
        finished = sum(1 for s in statuses if s == FutureStatus.FINISHED)
        error = sum(1 for s in statuses if s == FutureStatus.ERROR)

        return {
            "total": total,
            "percentages": {
                "finished": (finished / total) * 100.0,
                "running": (running / total) * 100.0,
                "not_started": (not_started / total) * 100.0,
                "error": (error / total) * 100.0,
                "cancelled": (cancelled / total) * 100.0,
            },
        }


    # ---------- read-only interface (except cancel passthrough) ----------

    def append(self, future_ids: List[str]) -> None:
        """Merge additional child future IDs into this group."""
        self.future_ids.extend(future_ids)
    

    def __add__(self, other: Any) -> "GroupFuture":
        """Return self with merged child future IDs from another GroupFuture."""
        if not isinstance(other, GroupFuture):
            return NotImplemented
        self.future_ids.extend(other.future_ids)
        return self


    def wait(self, timeout: Optional[float] = None) -> Any:
        """Wait for all children to complete."""
        children = self._resolve_children()
        return_values = []
        timeout_ms = None if timeout is None else int(timeout * 1000)
        for f in children:
            if hasattr(f, "wait"):
                return_values.append(f.wait(timeout_ms))
            else:
                raise RuntimeError("Future is not associated with a native future.")
        return return_values

    @property
    def result(self) -> List[Any]:
        """Collect results from all children without blocking.

        Assumes every child has already completed (e.g. after a
        ``laila.guarantee`` block).
        """
        children = self._resolve_children()
        return [f.result for f in children]

    @property
    def data(self) -> List[Any]:
        """Return the unwrapped payload data from every child future.

        Raises
        ------
        RuntimeError
            If any child future's result is not an Entry instance.
        """
        children = self._resolve_children()
        return [f.data for f in children]

    def __await__(self):
        """Await all children concurrently via ``asyncio.gather``."""
        async def _await_all():
            children = self._resolve_children()
            return await asyncio.gather(*children)
        return _await_all().__await__()

    # ---------- introspection ----------
    @property
    def what(self) -> Dict[str, Dict[str, Any]]:
        """Nested summary keyed by task_group_id."""
        group_key = self.global_id
        child_details: Dict[str, Any] = {}
        cancelled_ids = []
        not_cancelled_ids = []
        errors: Dict[str, str] = {}

        children = self._resolve_children()
        for f in children:
            fid = f.global_id
            if hasattr(f, "what"):
                det = f.what
            elif hasattr(f, "details"):
                det = f.details
            else:
                det = {
                    fid: {
                        "status": getattr(f, "status", FutureStatus.UNKNOWN).value,
                        "error": repr(f.outcome) if getattr(f, "status", None) == FutureStatus.ERROR else None,
                    }
                }
            for child_id, payload in det.items():
                child_details[child_id] = payload

            try:
                if getattr(f, "cancelled", lambda: False)():
                    cancelled_ids.append(fid)
                else:
                    not_cancelled_ids.append(fid)

                if f.status == FutureStatus.ERROR:
                    errors[fid] = repr(f.outcome)
            except Exception as e:
                errors[fid] = repr(e)

        return {
            group_key: {
                "status": self.status,
                "taskforce_id": self.taskforce_id,
                "policy_id": self.policy_id,
                "futures": child_details,
                "summary": {
                    "cancelled": cancelled_ids,
                    "not_cancelled": not_cancelled_ids,
                    "errors": errors,
                },
            }
        }


    def __str__(self) -> str:
        """Return JSON representation of the group."""
        return json.dumps(self.what)

    def __repr__(self) -> str:
        """Return JSON representation of the group."""
        return json.dumps(self.what)

    def __iter__(self) -> Iterator[str]:
        """Iterate over child future IDs."""
        return iter(self.future_ids)

    def __len__(self) -> int:
        """Return the number of child futures."""
        return len(self.future_ids)