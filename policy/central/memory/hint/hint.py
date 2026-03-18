from pydantic import BaseModel, Field
from typing import Set, Optional, Any


class MemoryHint(BaseModel):
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

    