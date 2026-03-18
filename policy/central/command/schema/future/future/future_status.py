from enum import Enum
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
import uuid
from typing import Dict, Optional, Callable, Any, List
import json


class FutureStatus(str, Enum):
    """
    Enumeration of supported Future status codes.

    Notes
    -----
    These values are used across all Future implementations and should remain
    stable for serialization and logging.
    """
    UNKNOWN = "unknown"
    NOT_STARTED = "not_started"
    RUNNING = "running"
    POLL_TIMEOUT = "poll_timeout"
    FINISHED = "finished"
    ERROR = "error"
    CANCELLED = "cancelled"


