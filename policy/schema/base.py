"""Base schema for a Laila policy and its central sub-components."""

from __future__ import annotations
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict, PrivateAttr
from ...basics.definitions.cli_capable import CLIExempt, _LAILA_CLI_CAPABLE_CLASS

from ...pool.schema.base import _LAILA_IDENTIFIABLE_POOL
from ...entry import Entry
from ..central.command.schema.base import _LAILA_IDENTIFIABLE_CENTRAL_COMMAND
from ...basics.definitions.identifiable_object import _LAILA_IDENTIFIABLE_OBJECT
from ..central.memory.schema.base import _LAILA_IDENTIFIABLE_CENTRAL_MEMORY
from ...macros.strings import _POLICY_SCOPE

class _LAILA_IDENTIFIABLE_POLICY(_LAILA_CLI_CAPABLE_CLASS, _LAILA_IDENTIFIABLE_OBJECT):
    """Top-level policy object that owns central command, memory, and logic."""

    _scopes: list[str] = PrivateAttr(default_factory=lambda: list([_POLICY_SCOPE]))
    class Central(BaseModel):
        """Container for the four central sub-systems of a policy."""

        logic: Optional[Any] = CLIExempt(default=None)
        command: Optional[_LAILA_IDENTIFIABLE_CENTRAL_COMMAND] = CLIExempt(default=None)
        communication: Optional[Any] = CLIExempt(default=None)
        memory: Optional[Any] = CLIExempt(default=None)

        model_config = ConfigDict(arbitrary_types_allowed=True)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Core components
    central: Central = CLIExempt(default_factory=Central)
    future_bank: Dict[str, Any] = CLIExempt(default_factory=dict)


    def model_post_init(self, __context: Any) -> None:
        """Lazily wire default central command, memory, and communication if not provided."""
        from ...macros.defaults import (
            DefaultCentralCommand,
            DefaultCentralMemory,
            DefaultCentralCommunication,
        )

        if self.central.memory is None:
            self.central.memory = DefaultCentralMemory()

        if self.central.command is None:
            self.central.command = DefaultCentralCommand(policy_id=self.global_id)

        if self.central.communication is None:
            self.central.communication = DefaultCentralCommunication(
                policy_id=self.global_id,
            )

        self.central.communication._local_policy = self


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
        """Fetch an entry from central memory by its *global_id*.

        Parameters
        ----------
        global_id : str
            The unique identifier of the entry to recall.
        global_fetch : bool, optional
            If ``True``, search across all known policies (not implemented).
        pool_subset : dict, optional
            Restrict the search to a subset of pools.
        hint : str, optional
            Routing hint forwarded to central memory.

        Returns
        -------
        Entry or None
            The recovered entry, or ``None`` if not found.
        """
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

