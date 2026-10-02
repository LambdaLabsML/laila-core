"""Stream-lane wire format shared by :mod:`.channel` and the carriers.

Lives outside ``protocols/_carriers`` so :mod:`.channel` can import it
without triggering the carrier package (which in turn imports
:mod:`.channel`). :mod:`protocols._carriers.codec` re-exports everything
here, so carrier code keeps a single ``_codec`` namespace.

Frame layout (payload of one outer length-prefixed frame on stream
carriers, or one whole datagram on packet carriers)::

    [0x01][lane u8][seq u32 BE][flags u8][data ...]

- ``0x01`` is :data:`STREAM_MARKER`. Every first byte up to
  :data:`RESERVED_MARKER_MAX` (``0x1F``) is reserved for binary control
  and is never an RPC payload (JSON starts with ``{`` = 0x7B, msgpack
  maps with 0x80-0x8F / 0xDE / 0xDF). Unknown reserved markers are
  dropped silently by the receiver.
- ``lane`` is the *receiver's* lane id; ``0`` is reserved for
  RPC/control and never allocated.
- ``seq`` is a per-lane **chunk** counter. The receiver uses gaps to
  detect a lost chunk and discards the in-progress message whole.
- ``flags``: :data:`FLAG_START` marks the first chunk of a message,
  :data:`FLAG_END` the last. Both are set on a single-chunk message
  (including the empty message ``b""``).
"""

from __future__ import annotations

import struct

#: First payload byte of a stream-lane frame.
STREAM_MARKER = 0x01
#: Every first byte ``<= RESERVED_MARKER_MAX`` is a binary control marker.
RESERVED_MARKER_MAX = 0x1F
#: ``[marker u8][lane u8][seq u32 BE][flags u8]``
STREAM_HEADER = struct.Struct(">BBIB")
STREAM_HEADER_LEN = STREAM_HEADER.size
#: Last chunk of a logical message.
FLAG_END = 0x01
#: First chunk of a logical message.
FLAG_START = 0x02
#: Chunk counters wrap at 2**32.
SEQ_MODULUS = 1 << 32


def is_stream_frame(raw: bytes) -> bool:
    """``True`` if *raw* is a stream-lane frame (first byte is the marker)."""
    return bool(raw) and raw[0] == STREAM_MARKER


def is_reserved_frame(raw: bytes) -> bool:
    """``True`` if *raw* starts with any reserved binary marker (``< 0x20``).

    Such a frame must never be handed to the RPC codec; stream frames
    are routed to the lane machinery and unknown markers are dropped.
    """
    return bool(raw) and raw[0] <= RESERVED_MARKER_MAX


def pack_stream_frame(lane: int, seq: int, flags: int, data: bytes) -> bytes:
    """Build one stream-lane frame payload (without the outer length prefix)."""
    return STREAM_HEADER.pack(STREAM_MARKER, lane, seq % SEQ_MODULUS, flags) + data


def unpack_stream_header(raw: bytes) -> tuple[int, int, int]:
    """Parse ``(lane, seq, flags)`` from a stream frame.

    Raises
    ------
    ValueError
        If *raw* is shorter than the header, carries the wrong marker, or
        names the reserved lane ``0``.
    """
    if len(raw) < STREAM_HEADER_LEN:
        raise ValueError(f"Stream frame too short: {len(raw)} < {STREAM_HEADER_LEN} bytes.")
    marker, lane, seq, flags = STREAM_HEADER.unpack_from(raw)
    if marker != STREAM_MARKER:
        raise ValueError(f"Not a stream frame: marker 0x{marker:02x}.")
    if lane == 0:
        raise ValueError("Lane 0 is reserved for RPC/control.")
    return lane, seq, flags
