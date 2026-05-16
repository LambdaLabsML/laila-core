"""Lifecycle status enumeration for task-forces.

Defines the canonical state machine used by every concrete task-force
in laila. The members are :class:`str`-valued so they serialize
cleanly in JSON / YAML / Pydantic dumps and are stable across releases
(rename them and you break wire compatibility).
"""

from enum import Enum


class TaskForceStatus(str, Enum):
    """Lifecycle states for a :class:`_LAILA_IDENTIFIABLE_TASK_FORCE`.

    The valid transitions are::

        NOT_STARTED --start()--> RUNNING
        RUNNING     --pause()--> PAUSED
        PAUSED      --start()--> RUNNING
        RUNNING     --shutdown()--> STOPPED
        PAUSED      --shutdown()--> STOPPED
        *           --(crash)--> CRASHED

    Attributes
    ----------
    NOT_STARTED
        Constructed but :meth:`start` has not yet been called.
        ``model_post_init`` auto-transitions out of this state for
        most subclasses.
    RUNNING
        Actively dispatching submitted tasks.
    PAUSED
        Quiesced via :meth:`pause`. Backend resources are still
        allocated but no new work is dispatched. Transition back to
        ``RUNNING`` via :meth:`start`.
    STOPPED
        Terminal: backend resources released by :meth:`shutdown`. The
        task-force cannot be restarted; construct a new one.
    CRASHED
        Terminal: an unrecoverable backend error was observed. Set by
        the subclass on observed worker failures.
    """
    NOT_STARTED = "not_started"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    CRASHED = "crashed"
