from enum import Enum


class TaskForceStatus(str, Enum):
    """Lifecycle states for a TaskForce."""
    NOT_STARTED = "not_started"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    CRASHED = "crashed"


