"""Fernet symmetric encryption / decryption data transformation.

The :class:`FernetEncryption` transformation slots into a
transformation pipeline as the encryption step. It wraps a UTF-8
string in a Fernet token (AES-128-CBC + HMAC-SHA256, time-stamped,
URL-safe base64 encoded) using the :mod:`cryptography` library.

A pipeline that compresses then encrypts (e.g.
``[Json, Zlib, FernetEncryption, Base64]``) gives you compact,
ciphertext-only blobs that can sit safely in any text-only pool
backend.

Optional TTL
------------
``backward_kwargs={"ttl": seconds}`` instructs :meth:`backward` to
reject tokens older than ``seconds`` (raising :class:`ValueError`).
This is useful for ephemeral payloads that should not be readable
after a deadline -- the same machinery is exposed in the standalone
recovery snippet emitted into ``backward_code``.

Security note
-------------
The ``key`` is embedded in the ``backward_code`` string so that the
constitution can stand-alone replay decryption without re-importing
this class. That means the recovery code itself must be considered
*secret* -- handle it with the same care as the key.
"""

import textwrap
from typing import Any

from pydantic import field_validator

from ..base import _data_transformation  # renamed base


class FernetEncryption(_data_transformation):
    """Reversible Fernet symmetric encryption transformation.

    Parameters
    ----------
    key : str or bytes
        Fernet-compatible encryption key.
    """

    name: str = "fernet"
    key: str | bytes
    _fernet: Any = None

    @field_validator("key")
    @classmethod
    def _coerce_key(cls, v: str | bytes) -> bytes:
        """Coerce string keys to bytes."""
        if isinstance(v, str):
            v = v.encode("utf-8")
        if not isinstance(v, (bytes, bytearray)):
            raise TypeError("Encryption.key must be bytes or str")
        return bytes(v)

    def model_post_init(self, __context: Any) -> None:
        """Initialise the Fernet encryptor and build backward recovery code."""
        from cryptography.fernet import Fernet

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
        """Encrypt a UTF-8 string and return the Fernet token as a string.

        Parameters
        ----------
        data : str
            Plain-text string to encrypt.

        Returns
        -------
        str
            Fernet token.

        Raises
        ------
        TypeError
            If *data* is not a ``str``.
        """
        if not isinstance(data, str):
            raise TypeError("Encryption.forward expects a Base64 string (str)")
        return self._fernet.encrypt(data.encode("utf-8")).decode("utf-8")

    def backward(self, data: str) -> str:
        """Decrypt a Fernet token back to the original string.

        Parameters
        ----------
        data : str
            Fernet token string.

        Returns
        -------
        str
            Original plain-text string.

        Raises
        ------
        TypeError
            If *data* is not a ``str``.
        ValueError
            If the token is invalid or TTL has expired.
        """
        if not isinstance(data, str):
            raise TypeError("Encryption.backward expects a Fernet token string (str)")
        ttl = self.backward_kwargs.get("ttl", None)
        from cryptography.fernet import InvalidToken

        try:
            return self._fernet.decrypt(data.encode("utf-8"), ttl=ttl).decode("utf-8")
        except InvalidToken as e:
            raise ValueError("Invalid Fernet token or TTL expired") from e
