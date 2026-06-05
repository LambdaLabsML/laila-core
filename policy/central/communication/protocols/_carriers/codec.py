"""Pluggable wire codec for RPC carriers (``json`` / ``msgpack``).

Every carrier serialises JSON-RPC message dicts to ``bytes`` and back.
Two codecs are supported and share *identical* semantics for
laila-specific objects (most importantly the ``__laila_future__``
tagging that lets the receiver promote a returned future into a
:class:`RemoteFuture`):

- ``"json"`` -- delegates to :func:`...protocol.encode` /
  :func:`...protocol.decode` (the canonical, human-debuggable format
  used by the TCP/IP transport) and UTF-8 encodes the result.
- ``"msgpack"`` -- a compact binary format for bandwidth-constrained
  links. Uses the same object-flattening rules as
  :class:`...protocol.LailaJSONEncoder` via a shared ``default`` hook.

The module also provides length-prefixed *framing* helpers so
stream transports (which see an undelimited byte river) can recover
message boundaries: :func:`frame` prepends a 4-byte big-endian length,
and :func:`read_frame` reads exactly one such frame from an
:class:`asyncio.StreamReader`.
"""

from __future__ import annotations

import asyncio
import struct
from typing import Any

from ... import protocol as _json_protocol

#: Supported codec tokens.
CODECS = ("json", "msgpack")

_LENGTH_PREFIX = struct.Struct(">I")
#: Hard cap on a single frame (256 MiB) to bound memory on a hostile peer.
MAX_FRAME_BYTES = 256 * 1024 * 1024


def _msgpack_default(o: Any) -> Any:
    """Flatten a laila object for msgpack using the JSON encoder's rules.

    Reuses :meth:`LailaJSONEncoder.default` so futures, pydantic models
    and identity objects serialise identically regardless of codec.
    """
    return _json_protocol.LailaJSONEncoder().default(o)


def encode(obj: Any, codec: str = "json") -> bytes:
    """Serialise *obj* (a JSON-RPC message dict) to ``bytes``.

    Parameters
    ----------
    obj : Any
        The message to serialise (typically built by
        :func:`...protocol.make_request` / ``make_result`` / ``make_error``).
    codec : str, default ``"json"``
        One of :data:`CODECS`.

    Raises
    ------
    ValueError
        If *codec* is not a recognised token.
    """
    if codec == "json":
        return _json_protocol.encode(obj).encode("utf-8")
    if codec == "msgpack":
        import msgpack

        return msgpack.packb(obj, default=_msgpack_default, use_bin_type=True)
    raise ValueError(f"Unknown codec {codec!r}; expected one of {CODECS}.")


def decode(data: bytes, codec: str = "json") -> Any:
    """Deserialise ``bytes`` produced by :func:`encode` back to a dict.

    Parameters
    ----------
    data : bytes
        Raw payload (no length prefix).
    codec : str, default ``"json"``
        One of :data:`CODECS`.

    Raises
    ------
    ValueError
        If *codec* is not a recognised token.
    """
    if codec == "json":
        if isinstance(data, (bytes, bytearray)):
            data = bytes(data).decode("utf-8")
        return _json_protocol.decode(data)
    if codec == "msgpack":
        import msgpack

        return msgpack.unpackb(bytes(data), raw=False)
    raise ValueError(f"Unknown codec {codec!r}; expected one of {CODECS}.")


def frame(payload: bytes) -> bytes:
    """Prepend a 4-byte big-endian length prefix to *payload*.

    Used by stream carriers to delimit messages on an undelimited
    byte stream.
    """
    return _LENGTH_PREFIX.pack(len(payload)) + payload


def encode_frame(obj: Any, codec: str = "json") -> bytes:
    """Convenience: :func:`encode` then :func:`frame`."""
    return frame(encode(obj, codec))


async def read_frame(reader: asyncio.StreamReader) -> bytes | None:
    """Read exactly one length-prefixed frame from *reader*.

    Returns the payload bytes (without the prefix), or ``None`` when the
    stream reaches EOF cleanly between frames.

    Raises
    ------
    ValueError
        If the advertised length exceeds :data:`MAX_FRAME_BYTES`.
    """
    try:
        header = await reader.readexactly(_LENGTH_PREFIX.size)
    except asyncio.IncompleteReadError:
        return None
    (length,) = _LENGTH_PREFIX.unpack(header)
    if length > MAX_FRAME_BYTES:
        raise ValueError(f"Frame length {length} exceeds cap {MAX_FRAME_BYTES}.")
    if length == 0:
        return b""
    try:
        return await reader.readexactly(length)
    except asyncio.IncompleteReadError:
        return None
