/**
 * Stream-lane wire format shared by ``channel.js`` and the carriers.
 *
 * Lives outside ``protocols/_carriers`` so ``channel.js`` can import it
 * without triggering the carrier package (which in turn imports
 * ``channel.js``). ``protocols/_carriers/codec.js`` re-exports everything
 * here, so carrier code keeps a single ``_codec`` namespace.
 *
 * Frame layout (payload of one outer length-prefixed frame on stream
 * carriers, or one whole datagram on packet carriers)::
 *
 *     [0x01][lane u8][seq u32 BE][flags u8][data ...]
 *
 * - ``0x01`` is ``STREAM_MARKER``. Every first byte up to
 *   ``RESERVED_MARKER_MAX`` (``0x1F``) is reserved for binary control and is
 *   never an RPC payload (JSON starts with ``{`` = 0x7B, msgpack maps with
 *   0x80-0x8F / 0xDE / 0xDF). Unknown reserved markers are dropped silently
 *   by the receiver.
 * - ``lane`` is the *receiver's* lane id; ``0`` is reserved for RPC/control
 *   and never allocated.
 * - ``seq`` is a per-lane **chunk** counter. The receiver uses gaps to
 *   detect a lost chunk and discards the in-progress message whole.
 * - ``flags``: ``FLAG_START`` marks the first chunk of a message,
 *   ``FLAG_END`` the last. Both are set on a single-chunk message
 *   (including the empty message ``b""``).
 */
import { ValueError } from "../../../_compat/errors.js";
import { Struct } from "../../../_compat/struct.js";

/** First payload byte of a stream-lane frame. */
export const STREAM_MARKER = 0x01;
/** Every first byte ``<= RESERVED_MARKER_MAX`` is a binary control marker. */
export const RESERVED_MARKER_MAX = 0x1f;
/** ``[marker u8][lane u8][seq u32 BE][flags u8]`` */
export const STREAM_HEADER = new Struct(">BBIB");
export const STREAM_HEADER_LEN = STREAM_HEADER.size;
/** Last chunk of a logical message. */
export const FLAG_END = 0x01;
/** First chunk of a logical message. */
export const FLAG_START = 0x02;
/** Chunk counters wrap at 2**32. */
export const SEQ_MODULUS = 2 ** 32;

/** @param {Uint8Array} raw */
function _as_bytes(raw) {
  if (raw === null || raw === undefined) return Buffer.alloc(0);
  return Buffer.isBuffer(raw) ? raw : Buffer.from(raw.buffer ? raw : Buffer.from(raw));
}

/**
 * ``True`` if *raw* is a stream-lane frame (first byte is the marker).
 * @param {Uint8Array} raw
 */
export function is_stream_frame(raw) {
  return !!raw && raw.length > 0 && raw[0] === STREAM_MARKER;
}

/**
 * ``True`` if *raw* starts with any reserved binary marker (``< 0x20``).
 *
 * Such a frame must never be handed to the RPC codec; stream frames are
 * routed to the lane machinery and unknown markers are dropped.
 * @param {Uint8Array} raw
 */
export function is_reserved_frame(raw) {
  return !!raw && raw.length > 0 && raw[0] <= RESERVED_MARKER_MAX;
}

/**
 * Build one stream-lane frame payload (without the outer length prefix).
 * @param {number} lane
 * @param {number} seq
 * @param {number} flags
 * @param {Uint8Array} data
 * @returns {Buffer}
 */
export function pack_stream_frame(lane, seq, flags, data) {
  const seq_mod = ((seq % SEQ_MODULUS) + SEQ_MODULUS) % SEQ_MODULUS;
  return Buffer.concat([STREAM_HEADER.pack(STREAM_MARKER, lane, seq_mod, flags), _as_bytes(data)]);
}

/**
 * Parse ``[lane, seq, flags]`` from a stream frame.
 *
 * @param {Uint8Array} raw
 * @returns {[number, number, number]}
 * @throws {ValueError} If *raw* is shorter than the header, carries the
 *   wrong marker, or names the reserved lane ``0``.
 */
export function unpack_stream_header(raw) {
  if (!raw || raw.length < STREAM_HEADER_LEN) {
    throw new ValueError(`Stream frame too short: ${raw ? raw.length : 0} < ${STREAM_HEADER_LEN} bytes.`);
  }
  const [marker, lane, seq, flags] = STREAM_HEADER.unpack_from(_as_bytes(raw));
  if (marker !== STREAM_MARKER) {
    throw new ValueError(`Not a stream frame: marker 0x${marker.toString(16).padStart(2, "0")}.`);
  }
  if (lane === 0) throw new ValueError("Lane 0 is reserved for RPC/control.");
  return [lane, seq, flags];
}
