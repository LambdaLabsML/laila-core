"""P2: imports become `use` lines (laila twins, stdlib shims, dropped typing)."""

import json
import os
from typing import Any

import laila
from laila.entry import EntryState
from laila.policy.central.memory.schema.manifest import Manifest


def dump(e: Any) -> str:
    return json.dumps({"state": str(e.state), "cwd": os.getcwd()})
