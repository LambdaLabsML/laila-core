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

Key handling
------------
The key is **never** written into ``backward_code`` or into any
serialized form of the transformation (it is an ``exclude=True``
field). Like every other secret in laila it lives in ``laila.args``:

- canonical location: ``laila.args.encryption.key``
- convenience alias:  ``laila.encryption_key`` (getter / setter)

Both the writer and the reader are expected to configure the *same*
key in their own process (directly, or through
``laila.read_args(<secrets file>)``). Construct the transformation
with no ``key=`` to pick it up from there; an explicit ``key=`` still
wins and is useful for ad-hoc / test pipelines.

The recovery snippet carries only a short SHA-256 fingerprint of the
write-time key, so a reader configured with a *different* key fails
with a descriptive :class:`ValueError` instead of a bare
``InvalidToken``. A 12-hex-character prefix of a hash of a random
32-byte key discloses nothing useful about the key itself.
"""

import hashlib
import textwrap
from typing import Any

from pydantic import Field, field_validator

from ..base import _data_transformation  # renamed base

_FINGERPRINT_HEX_LEN = 12


def _coerce_key_bytes(v: Any) -> bytes:
    if isinstance(v, str):
        v = v.encode("utf-8")
    if not isinstance(v, (bytes, bytearray)):
        raise TypeError("Encryption.key must be bytes or str")
    return bytes(v)


def key_fingerprint(key: str | bytes) -> str:
    """Return the short SHA-256 fingerprint laila embeds in recovery code."""
    return hashlib.sha256(_coerce_key_bytes(key)).hexdigest()[:_FINGERPRINT_HEX_LEN]


def resolve_encryption_key(expected_fingerprint: str | None = None) -> bytes:
    """Return the process-wide encryption key configured in ``laila.args``.

    Looks up ``laila.args.encryption.key`` (also reachable as
    ``laila.encryption_key``) without auto-creating DotMap nodes.

    Parameters
    ----------
    expected_fingerprint : str, optional
        Fingerprint recorded at write time (see :func:`key_fingerprint`).
        When given, the configured key must produce the same fingerprint.

    Raises
    ------
    RuntimeError
        No key is configured in this process.
    ValueError
        A key is configured but does not match *expected_fingerprint*.
    """
    import laila

    section = laila.args.get("encryption")
    key = section.get("key") if section is not None and hasattr(section, "get") else None
    if key is None or key == "" or key == {}:
        raise RuntimeError(
            "no encryption key configured in this process; set "
            "`laila.encryption_key = <fernet key>` (i.e. laila.args.encryption.key) "
            "on both the writing and the reading side"
        )
    key_bytes = _coerce_key_bytes(key)
    if expected_fingerprint is not None and key_fingerprint(key_bytes) != expected_fingerprint:
        raise ValueError(
            "configured encryption key (laila.encryption_key) does not match the key used "
            f"at write time (fingerprint {expected_fingerprint}); set the same key as the writer"
        )
    return key_bytes


class FernetEncryption(_data_transformation):
    """Reversible Fernet symmetric encryption transformation.

    Parameters
    ----------
    key : str or bytes, optional
        Fernet-compatible encryption key. When omitted the key is read
        from ``laila.args.encryption.key`` / ``laila.encryption_key``.
        Never serialized or shown in ``repr``.
    """

    name: str = "fernet"
    key: str | bytes | None = Field(default=None, exclude=True, repr=False)
    _fernet: Any = None

    @field_validator("key")
    @classmethod
    def _coerce_key(cls, v: str | bytes | None) -> bytes | None:
        """Coerce string keys to bytes; ``None`` defers to ``laila.args``."""
        if v is None:
            return None
        return _coerce_key_bytes(v)

    def model_post_init(self, __context: Any) -> None:
        """Initialise the Fernet encryptor and build backward recovery code."""
        from cryptography.fernet import Fernet

        if self.key is None:
            self.key = resolve_encryption_key()

        self._fernet = Fernet(self.key)

        # Standalone recovery code: no key material, only its fingerprint.
        # The reader resolves the key from its own ``laila.args``.
        self.backward_code = textwrap.dedent(f"""
            def backward(inp):
                from cryptography.fernet import Fernet, InvalidToken
                from laila.entry.compdata.transformation.encryption.encryption import (
                    resolve_encryption_key,
                )
                if not isinstance(inp, str):
                    raise TypeError("Encryption.backward expects a Fernet token string (str)")
                f = Fernet(resolve_encryption_key(expected_fingerprint={key_fingerprint(self.key)!r}))
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
