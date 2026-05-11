"""Record model — wraps an Entry with provenance metadata for storage."""

from typing import Any, Optional, Mapping
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
import json
from .....entry import Entry
from .....entry.compdata.transformation import TransformationSequence


class Record(BaseModel):
    """Immutable wrapper that pairs an Entry with recorder/borrower metadata."""

    entry: Any
    recorder: Optional[str] = None
    borrower: Optional[str] = None
    record_timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec='milliseconds'))

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any) -> None:
        """Set recorder to the active policy ID when not explicitly provided."""
        from ..... import active_policy

        if self.recorder is None:
            self.recorder = active_policy.global_id
            self.borrower = None #for now, we don't want to record the borrower


    def serialize(
        self,
        transformations: TransformationSequence
    ) -> str:
        """Serialize the record by applying *transformations* to the entry."""
        record_as_dict = self.as_dict
        record_as_dict["entry"] = self.entry.serialize(transformations = transformations)

        return record_as_dict

    
    @property
    def entry_id(self) -> str:
        """Return the global ID of the wrapped entry."""
        if hasattr(self.entry, "global_id"):
            return self.entry.global_id

        if isinstance(self.entry, Mapping):
            return self.entry["_global_id"]

        raise
        
    @property
    def as_dict(
        self,
    ) -> dict:
        """Return a plain dict representation, preserving the raw entry object."""
        data = self.model_dump()
        
        #Since it model_dumps entry in a weird way
        data["entry"] = self.entry
        return data

    @classmethod
    def from_dict(
        cls,
        in_dict: dict
    ):
        """Construct a Record from a dict (not yet implemented)."""
        raise

    
    @classmethod
    def build(
        cls,
        record: Any
    ):
        """Hydrate a record dict from a JSON string or raw dict.

        Submits the entry build to ``central.command`` via
        ``build_by_scope`` and waits for the resulting future before
        returning the rebuilt record dict.
        """
        if isinstance (record, str):
            record=json.loads(record)

        from .....entry.constitution.build_maps import build_by_scope
        ref = build_by_scope(record["entry"])
        if hasattr(ref, "wait"):
            record["entry"] = ref.wait(None)
        else:
            record["entry"] = ref
        return record
