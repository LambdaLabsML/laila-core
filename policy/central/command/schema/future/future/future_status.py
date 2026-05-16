"""Enumeration of future lifecycle status codes.

These string values are the canonical wire representation -- they are
sent verbatim through RPC frames (see
:meth:`_LAILA_IDENTIFIABLE_POLICY._get_future_status`) and rendered in
log records, so renaming a member is a breaking change for any peer or
log consumer that hard-codes the value.
"""

from enum import Enum
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
import uuid
from typing import Dict, Optional, Callable, Any, List
import json


class FutureStatus(str, Enum):
    """Lifecycle states for every laila future.

    Members
    -------
    UNKNOWN
        Status could not be determined (typically a transient state
        for newly-created :class:`RemoteFuture` handles before the
        first poll has come back).
    NOT_STARTED
        The future has been constructed and registered but the
        underlying task has not yet begun executing.
    RUNNING
        The underlying task is actively executing.
    POLL_TIMEOUT
        A status poll on a remote/concurrent backing primitive timed
        out without a definitive answer; the next poll may resolve to
        any other state.
    FINISHED
        The task completed successfully; ``result`` is set.
    ERROR
        The task raised an exception; ``exception`` is set.
    CANCELLED
        The task was cancelled before completion (either explicitly,
        or as part of a ``shutdown(cancel_pending=True)`` sweep).

    Subclassing :class:`str` makes JSON / log serialization trivial --
    ``json.dumps(FutureStatus.RUNNING)`` produces ``"running"`` directly.
    """
    UNKNOWN = "unknown"
    NOT_STARTED = "not_started"
    RUNNING = "running"
    POLL_TIMEOUT = "poll_timeout"
    FINISHED = "finished"
    ERROR = "error"
    CANCELLED = "cancelled"


