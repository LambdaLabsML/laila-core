/**
 * ``.npy`` -- byte-exact ``numpy.save`` / ``numpy.load`` (``allow_pickle=False``)
 * for :class:`NDArray` (``numpy.lib.format``, versions 1.0 / 2.0 / 3.0).
 *
 * Writer (``_write_array_header`` + ``write_array``):
 *   magic ``\x93NUMPY`` + version + header-length (``<H`` for 1.0, ``<I``
 *   for 2.0/3.0) + header text + spaces + ``\n`` such that the whole prefix
 *   is a multiple of ARRAY_ALIGN (64) bytes. Header text is the repr of a
 *   dict with sorted keys:
 *     ``{'descr': '<f8', 'fortran_order': False, 'shape': (2, 3), }``
 *   followed by ``GROWTH_AXIS_MAX_DIGITS - len(repr(growth axis))`` spaces
 *   (growth axis = shape[-1] when fortran_order else shape[0]; none for 0-d).
 *   Data follows in C order, or F order when the array is Fortran-contiguous
 *   and not C-contiguous (so 1-d / 0-d arrays are always ``fortran_order``
 *   False).
 */
import { NDArray, normalize_dtype, dtype_spec } from "../_compat/ndarray.js";
import { ValueError } from "../_compat/errors.js";

const MAGIC_PREFIX = Buffer.from([0x93, 0x4e, 0x55, 0x4d, 0x50, 0x59]); // \x93NUMPY
const MAGIC_LEN = MAGIC_PREFIX.length + 2;
const ARRAY_ALIGN = 64;
const GROWTH_AXIS_MAX_DIGITS = 21;
const _MAX_HEADER_SIZE = 10000;

// version -> [header length struct size, encoding]
const _header_size_info = {
  "1,0": [2, "latin1"],
  "2,0": [4, "latin1"],
  "3,0": [4, "utf8"],
};

function _py_tuple_repr(shape) {
  if (shape.length === 0) return "()";
  if (shape.length === 1) return `(${shape[0]},)`;
  return `(${shape.join(", ")})`;
}

/** ``numpy.lib.format.magic(major, minor)`` */
export function magic(major, minor) {
  if (major < 0 || major > 255) throw new ValueError("major version must be 0 <= major < 256");
  if (minor < 0 || minor > 255) throw new ValueError("minor version must be 0 <= minor < 256");
  return Buffer.concat([MAGIC_PREFIX, Buffer.from([major, minor])]);
}

function _wrap_header(header, version) {
  const [hsize, encoding] = _header_size_info[version.join(",")];
  const hbytes = Buffer.from(header, encoding);
  if (encoding === "latin1" && hbytes.toString("latin1") !== header) throw new UnicodeEncodeErrorLite();
  const hlen = hbytes.length + 1;
  const padlen = ARRAY_ALIGN - ((MAGIC_LEN + hsize + hlen) % ARRAY_ALIGN);
  const total = hlen + padlen;
  if (hsize === 2 && total > 0xffff) throw new ValueError(`Header length ${hlen} too big for version=${version}`);
  const prefix = Buffer.alloc(MAGIC_LEN + hsize);
  magic(...version).copy(prefix, 0);
  if (hsize === 2) prefix.writeUInt16LE(total, MAGIC_LEN);
  else prefix.writeUInt32LE(total, MAGIC_LEN);
  return Buffer.concat([prefix, hbytes, Buffer.alloc(padlen, 0x20), Buffer.from("\n")]);
}
class UnicodeEncodeErrorLite extends Error {}

function _wrap_header_guess_version(header) {
  try {
    return _wrap_header(header, [1, 0]);
  } catch (e) {
    if (!(e instanceof ValueError)) throw e;
  }
  try {
    return _wrap_header(header, [2, 0]);
  } catch (e) {
    if (!(e instanceof UnicodeEncodeErrorLite)) throw e;
  }
  return _wrap_header(header, [3, 0]);
}

/** ``numpy.lib.format.header_data_from_array_1_0(array)`` */
export function header_data_from_array_1_0(array) {
  const shape = [...array.shape];
  // 0-d and 1-d arrays are both C- and F-contiguous -> C wins.
  const fortran_order = Boolean(array.fortran_order) && shape.length > 1;
  return { shape, fortran_order, descr: array.dtype };
}

function _header_text(d) {
  // sorted(d.items()): descr, fortran_order, shape
  let header = `{'descr': '${d.descr}', 'fortran_order': ${d.fortran_order ? "True" : "False"}, 'shape': ${_py_tuple_repr(d.shape)}, }`;
  const shape = d.shape;
  if (shape.length > 0) {
    const growth = d.fortran_order ? shape[shape.length - 1] : shape[0];
    header += " ".repeat(Math.max(0, GROWTH_AXIS_MAX_DIGITS - String(growth).length));
  }
  return header;
}

/** ``numpy.lib.format.write_array`` into a Buffer (``allow_pickle`` must be false). */
export function write_array(array, { version = null, allow_pickle = false } = {}) {
  if (!(array instanceof NDArray)) array = NDArray.array(array);
  const d = header_data_from_array_1_0(array);
  const header = _header_text(d);
  const prefix = version === null ? _wrap_header_guess_version(header) : _wrap_header(header, version);
  // data: F order only when fortran_order; NDArray stores data in its own
  // declared layout, so re-layout if the header disagrees.
  let data;
  if (d.fortran_order === Boolean(array.fortran_order)) data = array._raw_bytes();
  else data = array._to_c_order()._raw_bytes();
  return Buffer.concat([prefix, data]);
}

/**
 * ``np.save(buf, arr, allow_pickle=False)`` -> bytes
 * @param {NDArray|any[]} arr
 * @param {{allow_pickle?: boolean}} [opts]
 * @returns {Buffer}
 */
export function save(arr, opts = {}) {
  if (opts.allow_pickle) throw new ValueError("laila-js cannot write object arrays (allow_pickle=True is unsupported)");
  return write_array(arr, opts);
}

/** ``numpy.lib.format.read_magic`` */
export function read_magic(buf) {
  if (buf.length < MAGIC_LEN) throw new ValueError(`EOF: reading magic string, expected ${MAGIC_LEN} bytes got ${buf.length}`);
  if (Buffer.compare(buf.subarray(0, 6), MAGIC_PREFIX) !== 0)
    throw new ValueError(`the magic string is not correct; expected ${JSON.stringify(MAGIC_PREFIX.toString("latin1"))}, got ${JSON.stringify(buf.subarray(0, 6).toString("latin1"))}`);
  return [buf[6], buf[7]];
}

/** ``numpy.lib.format.descr_to_dtype`` (simple, non-structured descrs only). */
export function descr_to_dtype(descr) {
  if (typeof descr !== "string") throw new ValueError(`descr is not a valid dtype descriptor: ${JSON.stringify(descr)}`);
  // accept big-endian descrs of 1-byte types; others unsupported
  try {
    return normalize_dtype(descr);
  } catch (e) {
    if (/^>/.test(descr)) return descr; // handled by byteswap in read
    throw new ValueError(`descr is not a valid dtype descriptor: '${descr}'`);
  }
}

/**
 * Parse the Python-literal header dict. Only the exact shape numpy writes is
 * accepted (three keys, tuple of ints, bool, str descr).
 */
function _parse_header(text) {
  const m = /^\s*\{\s*'descr'\s*:\s*'([^']*)'\s*,\s*'fortran_order'\s*:\s*(True|False)\s*,\s*'shape'\s*:\s*\(([^)]*)\)\s*,?\s*\}\s*$/.exec(text);
  if (!m) {
    // tolerate alternate key order / quoting by a looser parse
    const descr = /'descr'\s*:\s*'([^']*)'/.exec(text) || /"descr"\s*:\s*"([^"]*)"/.exec(text);
    const fo = /'fortran_order'\s*:\s*(True|False)/.exec(text) || /"fortran_order"\s*:\s*(True|False)/.exec(text);
    const sh = /'shape'\s*:\s*\(([^)]*)\)/.exec(text) || /"shape"\s*:\s*\(([^)]*)\)/.exec(text);
    if (!descr || !fo || !sh) throw new ValueError(`Cannot parse header: ${JSON.stringify(text)}`);
    return { descr: descr[1], fortran_order: fo[1] === "True", shape: _parse_shape(sh[1]) };
  }
  return { descr: m[1], fortran_order: m[2] === "True", shape: _parse_shape(m[3]) };
}
function _parse_shape(s) {
  const parts = s
    .split(",")
    .map((x) => x.trim())
    .filter((x) => x.length);
  return parts.map((x) => {
    if (!/^\d+L?$/.test(x)) throw new ValueError(`shape is not valid: (${s})`);
    return parseInt(x, 10);
  });
}

/** ``numpy.lib.format.read_array_header_1_0 / 2_0`` -> [shape, fortran_order, dtype, offset] */
export function read_array_header(buf, version, { max_header_size = _MAX_HEADER_SIZE } = {}) {
  const info = _header_size_info[version.join(",")];
  if (!info) throw new ValueError(`we only support format version (1,0), (2,0), and (3,0), not (${version.join(", ")})`);
  const [hsize, encoding] = info;
  let pos = MAGIC_LEN;
  if (buf.length < pos + hsize) throw new ValueError("EOF: reading array header length");
  const hlen = hsize === 2 ? buf.readUInt16LE(pos) : buf.readUInt32LE(pos);
  pos += hsize;
  if (buf.length < pos + hlen) throw new ValueError(`EOF: reading array header, expected ${hlen} bytes got ${buf.length - pos}`);
  const header = buf.subarray(pos, pos + hlen).toString(encoding);
  pos += hlen;
  if (header.length > max_header_size)
    throw new ValueError(`Header info length (${header.length}) is large and may not be safe to load securely.\nTo allow loading, adjust \`max_header_size\` or fully trust the \`.npy\` file using \`allow_pickle=True\`.\nFor safety against large resource use or crashes, sandboxing may be necessary.`);
  const d = _parse_header(header);
  const dtype = descr_to_dtype(d.descr);
  return [d.shape, d.fortran_order, dtype, pos];
}

/**
 * ``np.load(io.BytesIO(data), allow_pickle=False)`` -> NDArray
 * @param {Uint8Array} data
 * @param {{allow_pickle?: boolean}} [opts]
 * @returns {NDArray}
 */
export function load(data, opts = {}) {
  const buf = Buffer.isBuffer(data) ? data : Buffer.from(data.buffer ?? data, data.byteOffset ?? 0, data.byteLength ?? data.length);
  const version = read_magic(buf);
  const [shape, fortran_order, dtype, offset] = read_array_header(buf, version, opts);
  if (dtype === "|O" || /O/.test(dtype))
    throw new ValueError("Object arrays cannot be loaded when allow_pickle=False");
  const spec = dtype_spec(dtype);
  const count = shape.reduce((a, b) => a * b, 1);
  const nbytes = count * spec.size;
  if (buf.length - offset < nbytes) throw new ValueError(`EOF: reading array data, expected ${nbytes} bytes got ${buf.length - offset}`);
  // raw bytes in the descr's byte order; NDArray swaps into host storage
  return new NDArray({ dtype, shape, data: buf.subarray(offset, offset + nbytes), fortran_order });
}

export const read_array = load;
export { MAGIC_PREFIX, ARRAY_ALIGN };
