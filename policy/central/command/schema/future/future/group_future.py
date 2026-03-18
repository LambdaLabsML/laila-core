from __future__ import annotations
from typing import Dict, Optional, Callable, Any
import asyncio
import threading
from pydantic import Field, ConfigDict, PrivateAttr
import json
from typing import Iterator

from .future import Future
from .future_status import FutureStatus

from .......atomic.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from .......macros.strings import _GROUP_FUTURE_SCOPE

class GroupFuture(_LAILA_IDENTIFIABLE_OBJECT):

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_GROUP_FUTURE_SCOPE]))

    taskforce_id: _LAILA_IDENTIFIABLE_OBJECT|str
    policy_id: _LAILA_IDENTIFIABLE_OBJECT|str
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any) -> None:
        from ....... import get_active_policy
        get_active_policy().central.command._register_future_with_active_guarantees(self)


    # Map of {child_id: Future}
    futures: Dict[str, Future] = Field(default_factory=dict)

    # ---------- computed status ----------
    @property
    def status(self) -> Dict[str, Any]:
        """
        Return percentage breakdown of child statuses.
        """
        if not self.futures:
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

        total = float(len(self.futures))
        statuses = [f.status for f in self.futures.values()]
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

    def append(self, futures: Dict[str, Future]) -> None:
        """
        Merge a mapping of child futures into this group.

        Parameters
        ----------
        futures
            Mapping of task IDs to Future instances.
        """
        self.futures |= futures
    

    def __add__(self, other: Any) -> "GroupFuture":
        """
        Return a new GroupFuture with merged child futures.

        Parameters
        ----------
        other
            Another GroupFuture to merge.
        """
        if not isinstance(other, GroupFuture):
            return NotImplemented

        self.futures |= other.futures
        return self



    def wait(self, timeout: Optional[float] = None) -> Any:
        """
        Wait for all children to complete.
        """
        return_values = []
        timeout_ms = None if timeout is None else int(timeout * 1000)
        for f in self.futures.values():
            if hasattr(f, "wait"):
                return_values.append(f.wait(timeout_ms))
            else:
                raise RuntimeError("Future is not associated with a native future.")
        return return_values

    def __await__(self):
        async def _await_all():
            return await asyncio.gather(
                *(future for future in self.futures.values())
            )

        return _await_all().__await__()

    # ---------- introspection ----------
    @property
    def what(self) -> Dict[str, Dict[str, Any]]:
        """
        Nested summary keyed by task_group_id.

        Returns
        -------
        dict
            A mapping containing group status, taskforce ID, child details,
            and summary statistics.
        """
        group_key = self.global_id
        child_details: Dict[str, Any] = {}
        cancelled_ids = []
        not_cancelled_ids = []
        errors: Dict[str, str] = {}

        for fid, f in self.futures.items():
            # Merge child details
            if hasattr(f, "what"):
                det = f.what
            elif hasattr(f, "details"):
                det = f.details
            else:
                det = {
                    f.global_id: {
                        "status": getattr(f, "status", FutureStatus.UNKNOWN).value,
                        "error": repr(f.outcome) if getattr(f, "status", None) == FutureStatus.ERROR else None,
                    }
                }
            for child_id, payload in det.items():
                child_details[child_id] = payload

            # Track cancellation & errors
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
        return json.dumps(self.what)

    def __repr__(self) -> str:
        return json.dumps(self.what)

    def __iter__(self) -> Iterator[Future]:
        return iter(self.futures.values())

    def __len__(self) -> int:
        return len(self.futures)