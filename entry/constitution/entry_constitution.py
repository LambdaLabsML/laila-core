"""Constitution logic that declaratively derives Entry data from other Entries."""

from typing import Callable, List, Optional, TYPE_CHECKING

from pydantic import BaseModel, PrivateAttr

from ..compdata import ComputationalData
from ..entry_state import EntryState
if TYPE_CHECKING:
    from ..entry import Entry


class EntryConstitution(BaseModel):
    """Encapsulates a callable constitution and its precedence list.

    A constitution is a function that computes an Entry's payload from a set
    of constituent Entries.  Once assigned, the constitution cannot be
    reassigned.
    """

    _constitution: Optional[Callable[[dict[str, "Entry"]], ComputationalData]] = PrivateAttr(default=None)
    _precedence: list[str] = PrivateAttr(default_factory=list)
    _alias: Optional[str] = PrivateAttr(default=None)

    @property
    def alias(self) -> Optional[str]:
        """Optional human-readable alias for this constitution."""
        return self._alias

    @alias.setter
    def alias(self, value: Optional[str]) -> None:
        """Set the alias, must be a string or ``None``."""
        if value is not None and not isinstance(value, str):
            raise ValueError("alias must be a string or None")
        self._alias = value
        self.notify_policy()

    
    @property
    def precedence(self) -> List[str]:
        """Getter for the private _precedence attribute."""
        return self._precedence

    @precedence.setter
    def precedence(self, value: List[str]) -> None:
        """Setter for the private _precedence attribute."""
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError("precedence must be a list of strings")
        self._precedence = value
        self.notify_policy()
    
    @property
    def constitution(self):
        """Return the constitution callable."""
        return self._constitution

    @constitution.setter
    def constitution(self, fn):
        """Assign the constitution callable (one-time only).

        Raises
        ------
        AttributeError
            If a constitution has already been set.
        """
        if self._constitution != None:
            raise AttributeError("Constitution is already set and cannot be reassigned.")
        self._constitution = fn
        self.notify_policy()

    def run_constitution(self, constituents: dict[str, "Entry"], aliased=True):
        """Execute the constitution function over the given constituent Entries.

        Parameters
        ----------
        constituents : dict[str, Entry]
            Mapping of entry identifiers to Entry instances.
        aliased : bool, optional
            If ``True``, build an alias-to-payload map before invoking the
            constitution.  Defaults to ``True``.

        Raises
        ------
        RuntimeError
            If the payload is already finalised or no constitution is defined.
        ValueError
            If *aliased* is ``True`` and any constituent lacks an alias.
        """
        if self.state == EntryState.READY:
            raise RuntimeError("payload finalized, cannot run constitution.")
        if self._constitution is None:
            raise RuntimeError("no constitution defined.")


        if aliased:
            # Ensure all constituents have a non-None alias
            if not all(hasattr(entry, 'alias') and entry.alias is not None for entry in constituents.values()):
                raise ValueError("All constituents must have a non-None 'alias' to construct the alias-to-payload map.")
        
            # Build the alias → payload map
            alias_payload_map = {
                entry.alias: entry.payload.data
                for entry in constituents.values()
            }

            result = self._constitution(**alias_payload_map)
        else:
            result = self._constitution(*constituents)

        self._payload = result
