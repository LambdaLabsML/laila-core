"""Hint model used to guide memory routing decisions.

A :class:`MemoryHint` is a soft, declarative knob the caller can pass
into a memory operation to bias the :class:`PoolRouter`'s pool
selection without hard-coding a specific ``pool_id``. The router is
free to ignore any field it doesn't understand -- hints are advisory,
never authoritative. (For an authoritative selection, pass ``pool_id``
or ``pool_nickname`` directly to ``memorize`` / ``remember`` / ``forget``.)
"""

from pydantic import BaseModel, Field
from typing import Set, Optional, Any


class MemoryHint(BaseModel):
    """Routing hints that bias which pools are searched or written to.

    All fields are optional. Each is a soft preference; the
    :class:`PoolRouter` may honor or ignore them based on the routing
    strategy currently in effect.
    """

    pools: Optional[Set[str]] = Field(
        default=None,
        description="Which pools to search first."
    )
    
    keywords: Optional[Set[str]] = Field(
        default=None,
        description="Keywords that help find the entry in memory."
    )

    semantics: Optional[Any] = Field(
        default=None,
        description="Vector semantics that help find the entry in memory."
    )
    
    purpose: Optional[str] = Field(
        default=None,
        description="If certain pools are uniquely used for a special purpose."
    )
    
    affinity: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Closeness to local/nearby memory caches (0–1)."
    )

    