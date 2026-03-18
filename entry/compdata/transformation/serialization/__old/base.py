import ast
import inspect
import pickle
import textwrap
from pydantic import BaseModel
from typing import Optional, ClassVar, Callable, Any
import msgpack
import numpy as np
import torch
import io


class _BaseSerializer(BaseModel):

    ds_code: ClassVar[Optional[str]] = None
    
    @staticmethod
    def serialize(obj) -> bytes:
        raise NotImplementedError("BaseSerializer is just a base class")

