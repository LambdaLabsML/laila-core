from enum import Enum, auto

class EntryState(Enum):
    READY = auto()
    POOLED = auto()
    POOLING = auto()
    STAGED = auto()
    STALE = auto()
