from typing import Any, Optional, Mapping
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
import json
from .....entry import Entry
from .....entry.compdata.transformation import TransformationSequence


class Record(BaseModel):
    entry: Any
    recorder: Optional[str] = None
    borrower: Optional[str] = None
    record_timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat(timespec='milliseconds'))

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any) -> None:

        from ..... import active_policy

        if self.recorder is None:
            self.recorder = active_policy.global_id
            self.borrower = None #for now, we don't want to record the borrower


    def serialize(
        self,
        transformations: TransformationSequence
    ) -> str:
        
        record_as_dict = self.as_dict
        record_as_dict["entry"] = self.entry.serialize(transformations = transformations)

        return record_as_dict

    
    @property
    def entry_id(self) -> str:
        if hasattr(self.entry, "global_id"):
            return self.entry.global_id

        if isinstance(self.entry, Mapping):
            return self.entry["_global_id"]

        raise
        
    @property
    def as_dict(
        self,
    ) -> dict:
        data = self.model_dump()
        
        #Since it model_dumps entry in a weird way
        data["entry"] = self.entry
        return data

    @classmethod
    def from_dict(
        cls,
        in_dict: dict
    ):
        raise

    
    @classmethod
    def recover(
        cls,
        record: Any
    ):
        if isinstance (record, str):
            record=json.loads(record)

        #TODO: from here on record has to be a dict
        record["entry"] = Entry.recover(record["entry"])
        return record
