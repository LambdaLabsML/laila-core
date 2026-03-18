from __future__ import annotations
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict, PrivateAttr

from ...pool.schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry import Entry
from ..central.command.schema.base import _LAILA_IDENTIFIABLE_CENTRAL_COMMAND
from ...atomic.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from ..central.memory.schema.base import _LAILA_IDENTIFIABLE_CENTRAL_MEMORY
from ...macros.strings import _POLICY_SCOPE

class _LAILA_IDENTIFIABLE_POLICY(_LAILA_IDENTIFIABLE_OBJECT):
    
    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_POLICY_SCOPE]))
    class Central(BaseModel):
        logic: Optional[Any] = Field(default=None)
        command: Optional[_LAILA_IDENTIFIABLE_CENTRAL_COMMAND] = Field(default=None)
        communication: Optional[Any] = Field(default=None)
        memory: Optional[Any] = Field(default=None)

        model_config = ConfigDict(arbitrary_types_allowed=True)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Core components
    central: Central = Field(default_factory=Central)


    def model_post_init(self, __context: Any) -> None:
        from ...macros.defaults import (
            DefaultCentralCommand, 
            DefaultCentralMemory
        )

        if self.central.memory is None:
            self.central.memory = DefaultCentralMemory()
            
        if self.central.command is None:
            self.central.command = DefaultCentralCommand(policy_id=self.global_id)


    def add_pool(self, new_pool: _LAILA_IDENTIFIABLE_POOL) -> None:
        """Add a MemoryPool instance to the central memory registry."""
        self.central.memory[new_pool.pool_id] = new_pool
    

    def remember(
        self,
        global_id: str,
        *,
        global_fetch: bool = False,
        pool_subset: Optional[Dict[str, _LAILA_IDENTIFIABLE_POOL]] = None,
        hint: Optional[str] = None,
        _remote_called: bool = False,
    ) -> Optional[Entry]:
        
        if global_fetch:
            raise NotImplementedError
        
        entry = self.central.memory.fetch(
            key = global_id,
            pool_subset = pool_subset,
            hint = hint
        )

        return entry


    
    def memorize(
        self,
        entries: Any,
        *,
        require_local_update = False, #update only affects the main pool at first.
        require_global_update = False
    ) -> None:
        """Update central memory with a new or modified entry."""
        return self.central.memory.record(entries)

