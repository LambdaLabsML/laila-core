/**
 * Pluggable wire codec for RPC carriers (``json`` / ``msgpack``).
 *
 * Every carrier serialises JSON-RPC message dicts to ``bytes`` and back. Two
 * codecs are supported and share *identical* semantics for laila-specific
 * objects (most importantly the ``__laila_future__`` tagging that lets the
 * receiver promote a returned future into a ``RemoteFuture``):
 *
 * - ``"json"`` -- delegates to ``protocol.encode`` / ``protocol.decode`` (the
 *   canonical, human-debuggable format used by the TCP/IP transport) and
 *   UTF-8 encodes the result.
 * - ``"msgpack"`` -- a compact binary format for bandwidth-constrained links.
 *   Uses the same object-flattening rules as ``LailaJSONEncoder`` via a
 *   shared ``default`` hook.
 *
 * The module also provides length-prefixed *framing* helpers so stream
 * transports (which see an undelimited byte river) can recover message
 * boundaries: ``frame`` prepends a 4-byte big-endian length, and
 * ``read_frame`` reads exactly one such frame from a ``StreamReader``.
 *
 * Stream-lane frames
 * ------------------
 * Besides RPC messages, carriers multiplex opaque *stream lanes* on the same
 * connection. A stream frame is distinguished from an RPC payload by its
 * **first byte**: RPC payloads start with ``{`` (JSON, ``0x7B``) or a msgpack
 * map marker (``0x80``-``0x8F``, ``0xDE``, ``0xDF``), whereas every byte below
 * ``0x20`` is reserved for binary control. The header layout and helpers
 * are defined in ``communication/wire.js`` and re-exported here.
 */
import { IncompleteReadError } from "../../../../../_compat/asyncio.js";
import { ValueError } from "../../../../../_compat/errors.js";
import { Struct } from "../../../../../_compat/struct.js";
import { packb, unpackb } from "../../../../../_codecs/msgpack.js";
import * as _json_protocol from "../../protocol.js";

export {
  FLAG_END,
  FLAG_START,
  RESERVED_MARKER_MAX,
  SEQ_MODULUS,
  STREAM_HEADER,
  STREAM_HEADER_LEN,
  STREAM_MARKER,
  is_reserved_frame,
  is_stream_frame,
  pack_stream_frame,
  unpack_stream_header,
} from "../../wire.js";

/** Supported codec tokens. */
export const CODECS = Object.freeze(["json", "msgpack"]);

const _LENGTH_PREFIX = new Struct(">I");
/**
 * Hard cap on a single frame (256 MiB) to bound memory on a hostile peer.
 * This is the *outer* framing cap, not the stream-message cap -- see
 * ``max_stream_frame_bytes`` on the carriers for the latter.
 */
export const MAX_FRAME_BYTES = 256 * 1024 * 1024;

/**
 * Flatten a laila object for msgpack using the JSON encoder's rules.
 *
 * Reuses ``LailaJSONEncoder.default`` so futures, pydantic models and
 * identity objects serialise identically regardless of codec.
 */
function _msgpack_default(o) {
  return new _json_protocol.LailaJSONEncoder().default(o);
}

/**
 * Serialise *obj* (a JSON-RPC message dict) to ``bytes``.
 * @param {any} obj
 * @param {string} [codec] One of ``CODECS``.
 * @returns {Buffer}
 * @throws {ValueError} If *codec* is not a recognised token.
 */
export function encode(obj, codec = "json") {
  if (codec === "json") return Buffer.from(_json_protocol.encode(obj), "utf8");
  if (codec === "msgpack") return packb(obj, { default: _msgpack_default, use_bin_type: true });
  throw new ValueError(`Unknown codec ${JSON.stringify(codec)}; expected one of ('json', 'msgpack').`);
}

/**
 * Deserialise ``bytes`` produced by ``encode`` back to a dict.
 * @param {Uint8Array|string} data Raw payload (no length prefix).
 * @param {string} [codec]
 */
export function decode(data, codec = "json") {
  if (codec === "json") {
    if (typeof data !== "string") data = Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString("utf8");
    return _json_protocol.decode(data);
  }
  if (codec === "msgpack") return unpackb(Buffer.from(data), { raw: false });
  throw new ValueError(`Unknown codec ${JSON.stringify(codec)}; expected one of ('json', 'msgpack').`);
}

/**
 * Prepend a 4-byte big-endian length prefix to *payload*.
 *
 * Used by stream carriers to delimit messages on an undelimited byte stream.
 * @param {Uint8Array} payload
 * @returns {Buffer}
 */
export function frame(payload) {
  return Buffer.concat([_LENGTH_PREFIX.pack(payload.length), Buffer.isBuffer(payload) ? payload : Buffer.from(payload)]);
}

/** Convenience: ``encode`` then ``frame``. */
export function encode_frame(obj, codec = "json") {
  return frame(encode(obj, codec));
}

/**
 * Read exactly one length-prefixed frame from *reader*.
 *
 * Returns the payload bytes (without the prefix), or ``null`` when the
 * stream reaches EOF cleanly between frames.
 *
 * @param {import("../../../../../_compat/asyncio.js").StreamReader} reader
 * @returns {Promise<Buffer|null>}
 * @throws {ValueError} If the advertised length exceeds ``MAX_FRAME_BYTES``.
 */
export async function read_frame(reader) {
  let header;
  try {
    header = await reader.readexactly(_LENGTH_PREFIX.size);
  } catch (e) {
    if (e instanceof IncompleteReadError) return null;
    throw e;
  }
  const [length] = _LENGTH_PREFIX.unpack(header);
  if (length > MAX_FRAME_BYTES) throw new ValueError(`Frame length ${length} exceeds cap ${MAX_FRAME_BYTES}.`);
  if (length === 0) return Buffer.alloc(0);
  try {
    return await reader.readexactly(length);
  } catch (e) {
    if (e instanceof IncompleteReadError) return null;
    throw e;
  }
}
