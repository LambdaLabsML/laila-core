# fernet_op.py
import textwrap
from typing import Any, Union
from pydantic import field_validator
from ..base import _data_transformation  # renamed base


class FernetEncryption(_data_transformation):
    name: str = "fernet"
    key: Union[str, bytes]
    _fernet: Any = None

    @field_validator("key")
    @classmethod
    def _coerce_key(cls, v: Union[str, bytes]) -> bytes:
        if isinstance(v, str):
            v = v.encode("utf-8")
        if not isinstance(v, (bytes, bytearray)):
            raise TypeError("Encryption.key must be bytes or str")
        return bytes(v)

    def model_post_init(self, __context: Any) -> None:
        from cryptography.fernet import Fernet
        # initialize runtime encryptor
        self._fernet = Fernet(self.key)

        # build standalone recovery code (uses optional ttl from backward_kwargs)
        self.backward_code = textwrap.dedent(f"""
            def backward(inp):
                from cryptography.fernet import Fernet, InvalidToken
                if not isinstance(inp, str):
                    raise TypeError("Encryption.backward expects a Fernet token string (str)")
                f = Fernet(key={self.key!r})
                token = inp.encode("utf-8")
                kwargs = {self.backward_kwargs!r}
                ttl = kwargs.get("ttl", None)
                try:
                    out = f.decrypt(token, ttl=ttl)
                except InvalidToken as e:
                    raise ValueError("Invalid Fernet token or TTL expired") from e
                return out.decode("utf-8")
        """)

    def forward(self, data: str) -> str:
        if not isinstance(data, str):
            raise TypeError("Encryption.forward expects a Base64 string (str)")
        return self._fernet.encrypt(data.encode("utf-8")).decode("utf-8")

    def backward(self, data: str) -> str:
        if not isinstance(data, str):
            raise TypeError("Encryption.backward expects a Fernet token string (str)")
        ttl = self.backward_kwargs.get("ttl", None)
        from cryptography.fernet import InvalidToken
        try:
            return self._fernet.decrypt(data.encode("utf-8"), ttl=ttl).decode("utf-8")
        except InvalidToken as e:
            raise ValueError("Invalid Fernet token or TTL expired") from e
