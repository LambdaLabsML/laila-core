package laila

import (
	"encoding/binary"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/wire.py -- stream-lane wire format shared
// by channel.go and the carriers.
//
// Lives outside ``protocols/_carriers`` so channel.go can use it without
// the carrier package (which in turn uses channel.go). The carriers'
// ``codec`` re-exports everything here, so carrier code keeps a single
// ``_codec`` namespace.
//
// Frame layout (payload of one outer length-prefixed frame on stream
// carriers, or one whole datagram on packet carriers)::
//
//	[0x01][lane u8][seq u32 BE][flags u8][data ...]
//
//   - ``0x01`` is STREAM_MARKER. Every first byte up to
//     RESERVED_MARKER_MAX (``0x1F``) is reserved for binary control and is
//     never an RPC payload (JSON starts with ``{`` = 0x7B, msgpack maps
//     with 0x80-0x8F / 0xDE / 0xDF). Unknown reserved markers are dropped
//     silently by the receiver.
//   - ``lane`` is the *receiver's* lane id; ``0`` is reserved for
//     RPC/control and never allocated.
//   - ``seq`` is a per-lane **chunk** counter. The receiver uses gaps to
//     detect a lost chunk and discards the in-progress message whole.
//   - ``flags``: FLAG_START marks the first chunk of a message, FLAG_END
//     the last. Both are set on a single-chunk message (including the
//     empty message ``b""``).

// STREAM_MARKER is the first payload byte of a stream-lane frame.
const STREAM_MARKER = 0x01

// RESERVED_MARKER_MAX: every first byte ``<= RESERVED_MARKER_MAX`` is a
// binary control marker.
const RESERVED_MARKER_MAX = 0x1F

// STREAM_HEADER_LEN is the size of ``[marker u8][lane u8][seq u32 BE][flags u8]``.
const STREAM_HEADER_LEN = 7

// FLAG_END marks the last chunk of a logical message.
const FLAG_END = 0x01

// FLAG_START marks the first chunk of a logical message.
const FLAG_START = 0x02

// SEQ_MODULUS: chunk counters wrap at 2**32.
const SEQ_MODULUS = 1 << 32

// Is_stream_frame is ``True`` if raw is a stream-lane frame (first byte is
// the marker).
func Is_stream_frame(raw []byte) bool {
	return len(raw) > 0 && raw[0] == STREAM_MARKER
}

// Is_reserved_frame is ``True`` if raw starts with any reserved binary
// marker (``< 0x20``).
//
// Such a frame must never be handed to the RPC codec; stream frames are
// routed to the lane machinery and unknown markers are dropped.
func Is_reserved_frame(raw []byte) bool {
	return len(raw) > 0 && raw[0] <= RESERVED_MARKER_MAX
}

// Pack_stream_frame builds one stream-lane frame payload (without the
// outer length prefix).
func Pack_stream_frame(lane int, seq int64, flags int, data []byte) []byte {
	out := make([]byte, STREAM_HEADER_LEN, STREAM_HEADER_LEN+len(data))
	out[0] = STREAM_MARKER
	out[1] = byte(lane)
	binary.BigEndian.PutUint32(out[2:6], uint32(((seq%SEQ_MODULUS)+SEQ_MODULUS)%SEQ_MODULUS))
	out[6] = byte(flags)
	return append(out, data...)
}

// Unpack_stream_header parses ``(lane, seq, flags)`` from a stream frame.
//
// ValueError if raw is shorter than the header, carries the wrong marker,
// or names the reserved lane ``0``.
func Unpack_stream_header(raw []byte) (lane int, seq int64, flags int, err error) {
	if len(raw) < STREAM_HEADER_LEN {
		return 0, 0, 0, py.ValueError.Newf("Stream frame too short: %d < %d bytes.", len(raw), STREAM_HEADER_LEN)
	}
	marker := raw[0]
	lane = int(raw[1])
	seq = int64(binary.BigEndian.Uint32(raw[2:6]))
	flags = int(raw[6])
	if marker != STREAM_MARKER {
		return 0, 0, 0, py.ValueError.Newf("Not a stream frame: marker 0x%02x.", marker)
	}
	if lane == 0 {
		return 0, 0, 0, py.ValueError.New("Lane 0 is reserved for RPC/control.")
	}
	return lane, seq, flags, nil
}
