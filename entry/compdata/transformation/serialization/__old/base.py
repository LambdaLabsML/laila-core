from typing import ClassVar

from pydantic import BaseModel


class _BaseSerializer(BaseModel):
    ds_code: ClassVar[str | None] = None

    @staticmethod
    def serialize(obj) -> bytes:
        raise NotImplementedError("BaseSerializer is just a base class")
