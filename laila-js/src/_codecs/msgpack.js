/**
 * ``msgpack`` -- byte-exact port of msgpack-python's C ``Packer`` /
 * ``Unpacker`` for the call shapes laila uses:
 *
 *   packb(obj, use_bin_type=True[, default=...])
 *   unpackb(data, raw=False[, strict_map_key=False])
 *
 * Encoding rules (``msgpack/_packer.pyx``, ``use_bin_type=True``,
 * ``use_single_float=False``, ``strict_types=False``, ``datetime=False``):
 *   - None -> nil; bool -> true/false (checked before int)
 *   - int: positive fixint / uint8 / uint16 / uint32 / uint64, negative
 *     fixint / int8 / int16 / int32 / int64; anything outside is OverflowError
 *   - float -> float64 (0xcb)
 *   - str -> fixstr / str8 / str16 / str32 (UTF-8; lone surrogates raise)
 *   - bytes / bytearray / memoryview -> bin8 / bin16 / bin32
 *   - list / tuple -> fixarray / array16 / array32
 *   - dict -> fixmap / map16 / map32 (insertion order)
 *   - ExtType -> fixext / ext8 / ext16 / ext32
 *   - otherwise ``default(obj)`` once, else TypeError
 *   - nesting deeper than 512 raises ValueError("recursion limit exceeded")
 *
 * Decoding follows the JS value model of pytypes.js: maps become plain
 * Objects when every key is a non-index-like string, otherwise Map (rule
 * D1); integral floats come back boxed as ``PyFloat`` (rule N1); ints beyond
 * the safe range come back as BigInt; bin -> Buffer; str -> string.
 */
import { PyTuple, PyFloat, is_integral, is_plain_object, dict_items, dict_from_entries } from "../_compat/pytypes.js";
import { ValueError, TypeError as PyTypeError, OverflowError, UnicodeDecodeError, PyException } from "../_compat/errors.js";
import { is_enum_member } from "../_compat/enum.js";

const DEFAULT_RECURSE_LIMIT = 511;

/** ``msgpack.ExtType(code, data)`` */
export class ExtType {
  constructor(code, data) {
    if (!Number.isInteger(code)) throw new PyTypeError("code must be int");
    if (!(data instanceof Uint8Array)) throw new PyTypeError("data must be bytes");
    if (!(code >= -128 && code <= 127)) throw new ValueError("code must be 0~127 or -128~-1");
    this.code = code;
    this.data = Buffer.from(data);
    Object.freeze(this);
  }
  __eq__(other) {
    return other instanceof ExtType && other.code === this.code && Buffer.compare(other.data, this.data) === 0;
  }
  __repr__() {
    return `ExtType(code=${this.code}, data=${JSON.stringify(this.data.toString("latin1"))})`;
  }
}

// exceptions mirroring msgpack.exceptions
export class UnpackException extends PyException {}
export class BufferFull extends UnpackException {}
export class OutOfData extends UnpackException {}
export class FormatError extends UnpackException {}
export class StackError extends UnpackException {}
export class ExtraData extends UnpackException {
  constructor(unpacked, extra) {
    super(`unpack(b) received extra data.`);
    this.unpacked = unpacked;
    this.extra = extra;
  }
}

// --------------------------------------------------------------------------
// Packer
// --------------------------------------------------------------------------

class _Buf {
  constructor() {
    this.buf = Buffer.allocUnsafe(1024);
    this.pos = 0;
  }
  ensure(n) {
    if (this.pos + n <= this.buf.length) return;
    let cap = this.buf.length * 2;
    while (cap < this.pos + n) cap *= 2;
    const nb = Buffer.allocUnsafe(cap);
    this.buf.copy(nb, 0, 0, this.pos);
    this.buf = nb;
  }
  u8(x) {
    this.ensure(1);
    this.buf[this.pos++] = x;
  }
  u16(x) {
    this.ensure(2);
    this.buf.writeUInt16BE(x, this.pos);
    this.pos += 2;
  }
  u32(x) {
    this.ensure(4);
    this.buf.writeUInt32BE(x, this.pos);
    this.pos += 4;
  }
  u64(x) {
    this.ensure(8);
    this.buf.writeBigUInt64BE(BigInt(x), this.pos);
    this.pos += 8;
  }
  i64(x) {
    this.ensure(8);
    this.buf.writeBigInt64BE(BigInt(x), this.pos);
    this.pos += 8;
  }
  f64(x) {
    this.ensure(8);
    this.buf.writeDoubleBE(x, this.pos);
    this.pos += 8;
  }
  bytes(b) {
    this.ensure(b.length);
    this.buf.set(b, this.pos);
    this.pos += b.length;
  }
  result() {
    return Buffer.from(this.buf.subarray(0, this.pos));
  }
}

const _utf8 = new TextEncoder();

function _encode_str(s) {
  // Python's UTF-8 codec rejects lone surrogates (UnicodeEncodeError).
  if (!s.isWellFormed()) throw new ValueError(`'utf-8' codec can't encode character: surrogates not allowed`);
  return _utf8.encode(s);
}

class _Packer {
  constructor({ default: dflt = null, use_single_float = false, autoreset = true, use_bin_type = true, strict_types = false } = {}) {
    this.default = dflt;
    this.use_single_float = use_single_float;
    this.use_bin_type = use_bin_type;
    this.strict_types = strict_types;
    this.b = new _Buf();
  }

  pack_int(v) {
    // v: bigint
    const b = this.b;
    if (v >= 0n) {
      if (v < 0x80n) return b.u8(Number(v));
      if (v < 0x100n) {
        b.u8(0xcc);
        return b.u8(Number(v));
      }
      if (v < 0x10000n) {
        b.u8(0xcd);
        return b.u16(Number(v));
      }
      if (v < 0x100000000n) {
        b.u8(0xce);
        return b.u32(Number(v));
      }
      if (v < 0x10000000000000000n) {
        b.u8(0xcf);
        return b.u64(v);
      }
      throw new OverflowError("Integer value out of range");
    }
    if (v >= -0x20n) return b.u8(Number(v) & 0xff);
    if (v >= -0x80n) {
      b.u8(0xd0);
      return b.u8(Number(v) & 0xff);
    }
    if (v >= -0x8000n) {
      b.u8(0xd1);
      return b.u16(Number(v) & 0xffff);
    }
    if (v >= -0x80000000n) {
      b.u8(0xd2);
      return b.u32(Number(v) >>> 0);
    }
    if (v >= -0x8000000000000000n) {
      b.u8(0xd3);
      return b.i64(v);
    }
    throw new OverflowError("Integer value out of range");
  }

  pack_float(x) {
    if (this.use_single_float) {
      this.b.u8(0xca);
      this.b.ensure(4);
      this.b.buf.writeFloatBE(x, this.b.pos);
      this.b.pos += 4;
    } else {
      this.b.u8(0xcb);
      this.b.f64(x);
    }
  }

  pack_raw_header(n) {
    const b = this.b;
    if (n <= 0x1f) return b.u8(0xa0 | n);
    if (n <= 0xff && this.use_bin_type) {
      b.u8(0xd9);
      return b.u8(n);
    }
    if (n <= 0xffff) {
      b.u8(0xda);
      return b.u16(n);
    }
    if (n <= 0xffffffff) {
      b.u8(0xdb);
      return b.u32(n);
    }
    throw new ValueError("unicode string is too large");
  }

  pack_bin_header(n) {
    const b = this.b;
    if (!this.use_bin_type) return this.pack_raw_header(n);
    if (n <= 0xff) {
      b.u8(0xc4);
      return b.u8(n);
    }
    if (n <= 0xffff) {
      b.u8(0xc5);
      return b.u16(n);
    }
    if (n <= 0xffffffff) {
      b.u8(0xc6);
      return b.u32(n);
    }
    throw new ValueError("bytes is too large");
  }

  pack_array_header(n) {
    const b = this.b;
    if (n <= 0x0f) return b.u8(0x90 | n);
    if (n <= 0xffff) {
      b.u8(0xdc);
      return b.u16(n);
    }
    if (n <= 0xffffffff) {
      b.u8(0xdd);
      return b.u32(n);
    }
    throw new ValueError("list is too large");
  }

  pack_map_header(n) {
    const b = this.b;
    if (n <= 0x0f) return b.u8(0x80 | n);
    if (n <= 0xffff) {
      b.u8(0xde);
      return b.u16(n);
    }
    if (n <= 0xffffffff) {
      b.u8(0xdf);
      return b.u32(n);
    }
    throw new ValueError("dict is too large");
  }

  pack_ext(code, data) {
    const b = this.b;
    const n = data.length;
    if (n === 1) b.u8(0xd4);
    else if (n === 2) b.u8(0xd5);
    else if (n === 4) b.u8(0xd6);
    else if (n === 8) b.u8(0xd7);
    else if (n === 16) b.u8(0xd8);
    else if (n <= 0xff) {
      b.u8(0xc7);
      b.u8(n);
    } else if (n <= 0xffff) {
      b.u8(0xc8);
      b.u16(n);
    } else if (n <= 0xffffffff) {
      b.u8(0xc9);
      b.u32(n);
    } else throw new ValueError("ext data is too large");
    b.u8(code & 0xff);
    b.bytes(data);
  }

  pack(o, nest_limit = DEFAULT_RECURSE_LIMIT) {
    if (nest_limit < 0) throw new ValueError("recursion limit exceeded.");
    let default_used = false;
    for (;;) {
      if (o === null || o === undefined) return this.b.u8(0xc0);
      if (o === true) return this.b.u8(0xc3);
      if (o === false) return this.b.u8(0xc2);
      if (typeof o === "number") {
        if (is_integral(o)) return this.pack_int(BigInt(o));
        return this.pack_float(o);
      }
      if (typeof o === "bigint") return this.pack_int(o);
      if (o instanceof PyFloat || o instanceof Number) return this.pack_float(o.valueOf());
      if (o instanceof Boolean) return this.b.u8(o.valueOf() ? 0xc3 : 0xc2);
      if (typeof o === "string" || o instanceof String || is_enum_member(o)) {
        const raw = _encode_str(is_enum_member(o) ? String(o.value) : o.valueOf());
        this.pack_raw_header(raw.length);
        return this.b.bytes(raw);
      }
      if (o instanceof Uint8Array || o instanceof ArrayBuffer || ArrayBuffer.isView(o)) {
        const bytes = o instanceof ArrayBuffer ? new Uint8Array(o) : new Uint8Array(o.buffer, o.byteOffset, o.byteLength);
        this.pack_bin_header(bytes.length);
        return this.b.bytes(bytes);
      }
      if (o instanceof ExtType) return this.pack_ext(o.code, o.data);
      if (Array.isArray(o)) {
        this.pack_array_header(o.length);
        for (const x of o) this.pack(x, nest_limit - 1);
        return;
      }
      if (o instanceof Map || is_plain_object(o) || (o && typeof o.toDict === "function" && typeof o.items === "function")) {
        const items = dict_items(o);
        this.pack_map_header(items.length);
        for (const [k, v] of items) {
          this.pack(k, nest_limit - 1);
          this.pack(v, nest_limit - 1);
        }
        return;
      }
      if (o instanceof Set) {
        // msgpack-python has no set support: falls through to default / TypeError
      }
      if (!default_used && this.default !== null) {
        o = this.default(o);
        default_used = true;
        continue;
      }
      throw new PyTypeError(`can not serialize '${_type_name(o)}' object`);
    }
  }

  bytes() {
    return this.b.result();
  }
}

function _type_name(o) {
  if (o === null) return "NoneType";
  if (typeof o !== "object") return typeof o;
  return o.constructor?.name ?? "object";
}

/**
 * ``msgpack.packb(o, **kwargs)``
 * @param {any} o
 * @param {{use_bin_type?: boolean, default?: Function|null, use_single_float?: boolean, strict_types?: boolean}} [opts]
 * @returns {Buffer}
 */
export function packb(o, opts = {}) {
  const p = new _Packer(opts);
  p.pack(o);
  return p.bytes();
}

export class Packer {
  constructor(opts = {}) {
    this._opts = opts;
  }
  pack(o) {
    return packb(o, this._opts);
  }
}

// --------------------------------------------------------------------------
// Unpacker
// --------------------------------------------------------------------------

const _utf8_dec = new TextDecoder("utf-8", { fatal: true });

class _Unpacker {
  constructor(buf, { raw = false, strict_map_key = false, use_list = true, object_hook = null, object_pairs_hook = null, list_hook = null, ext_hook = null, unicode_errors = "strict", max_str_len = -1, max_bin_len = -1, max_array_len = -1, max_map_len = -1, max_ext_len = -1 } = {}) {
    this.buf = buf;
    this.pos = 0;
    this.raw = raw;
    this.strict_map_key = strict_map_key;
    this.use_list = use_list;
    this.object_hook = object_hook;
    this.object_pairs_hook = object_pairs_hook;
    this.list_hook = list_hook;
    this.ext_hook = ext_hook;
    this.unicode_errors = unicode_errors;
    const n = buf.length;
    this.max_str_len = max_str_len === -1 ? n : max_str_len;
    this.max_bin_len = max_bin_len === -1 ? n : max_bin_len;
    this.max_array_len = max_array_len === -1 ? n : max_array_len;
    this.max_map_len = max_map_len === -1 ? Math.floor(n / 2) : max_map_len;
    this.max_ext_len = max_ext_len === -1 ? n : max_ext_len;
  }

  need(n) {
    if (this.pos + n > this.buf.length) throw new ValueError("Unpack failed: incomplete input");
  }
  u8() {
    this.need(1);
    return this.buf[this.pos++];
  }
  u16() {
    this.need(2);
    const v = this.buf.readUInt16BE(this.pos);
    this.pos += 2;
    return v;
  }
  u32() {
    this.need(4);
    const v = this.buf.readUInt32BE(this.pos);
    this.pos += 4;
    return v;
  }
  take(n) {
    this.need(n);
    const v = this.buf.subarray(this.pos, this.pos + n);
    this.pos += n;
    return v;
  }

  _int(v) {
    // v: bigint -> number when safe
    if (v >= -9007199254740991n && v <= 9007199254740991n) return Number(v);
    return v;
  }
  _float(x) {
    return is_integral(x) ? new PyFloat(x) : x;
  }
  _str(n) {
    if (n > this.max_str_len) throw new ValueError(`${n} exceeds max_str_len(${this.max_str_len})`);
    const raw = this.take(n);
    if (this.raw) return Buffer.from(raw);
    try {
      return _utf8_dec.decode(raw);
    } catch (e) {
      if (this.unicode_errors === "strict") throw new UnicodeDecodeError(`'utf-8' codec can't decode bytes: invalid start byte`);
      return new TextDecoder("utf-8").decode(raw);
    }
  }
  _bin(n) {
    if (n > this.max_bin_len) throw new ValueError(`${n} exceeds max_bin_len(${this.max_bin_len})`);
    return Buffer.from(this.take(n));
  }
  _array(n, depth) {
    if (n > this.max_array_len) throw new ValueError(`${n} exceeds max_array_len(${this.max_array_len})`);
    const out = new Array(n);
    for (let i = 0; i < n; i++) out[i] = this.unpack(depth + 1);
    let res = this.use_list ? out : PyTuple.from_iterable(out);
    if (this.list_hook) res = this.list_hook(res);
    return res;
  }
  _map(n, depth) {
    if (n > this.max_map_len) throw new ValueError(`${n} exceeds max_map_len(${this.max_map_len})`);
    const entries = [];
    for (let i = 0; i < n; i++) {
      const k = this.unpack(depth + 1);
      if (this.strict_map_key && !(typeof k === "string" || k instanceof Uint8Array)) throw new ValueError(`${_type_name(k)} is not allowed for map key`);
      if (Array.isArray(k) && !(k instanceof PyTuple)) throw new PyTypeError("unhashable type: 'list'");
      const v = this.unpack(depth + 1);
      entries.push([k, v]);
    }
    if (this.object_pairs_hook) return this.object_pairs_hook(entries);
    let d = dict_from_entries(entries);
    if (this.object_hook) d = this.object_hook(d);
    return d;
  }
  _ext(n) {
    if (n > this.max_ext_len) throw new ValueError(`${n} exceeds max_ext_len(${this.max_ext_len})`);
    const code = this.buf.readInt8(this.pos);
    this.pos += 1;
    const data = Buffer.from(this.take(n));
    if (this.ext_hook) return this.ext_hook(code, data);
    return new ExtType(code, data);
  }

  unpack(depth = 0) {
    if (depth > DEFAULT_RECURSE_LIMIT) throw new StackError("recursion limit exceeded");
    const b = this.u8();
    if (b <= 0x7f) return b;
    if (b >= 0xe0) return b - 0x100;
    if (b >= 0xa0 && b <= 0xbf) return this._str(b & 0x1f);
    if (b >= 0x90 && b <= 0x9f) return this._array(b & 0x0f, depth);
    if (b >= 0x80 && b <= 0x8f) return this._map(b & 0x0f, depth);
    switch (b) {
      case 0xc0:
        return null;
      case 0xc2:
        return false;
      case 0xc3:
        return true;
      case 0xc4:
        return this._bin(this.u8());
      case 0xc5:
        return this._bin(this.u16());
      case 0xc6:
        return this._bin(this.u32());
      case 0xc7:
        return this._ext(this.u8());
      case 0xc8:
        return this._ext(this.u16());
      case 0xc9:
        return this._ext(this.u32());
      case 0xca: {
        this.need(4);
        const v = this.buf.readFloatBE(this.pos);
        this.pos += 4;
        return this._float(v);
      }
      case 0xcb: {
        this.need(8);
        const v = this.buf.readDoubleBE(this.pos);
        this.pos += 8;
        return this._float(v);
      }
      case 0xcc:
        return this.u8();
      case 0xcd:
        return this.u16();
      case 0xce:
        return this.u32();
      case 0xcf: {
        this.need(8);
        const v = this.buf.readBigUInt64BE(this.pos);
        this.pos += 8;
        return this._int(v);
      }
      case 0xd0: {
        this.need(1);
        return this.buf.readInt8(this.pos++);
      }
      case 0xd1: {
        this.need(2);
        const v = this.buf.readInt16BE(this.pos);
        this.pos += 2;
        return v;
      }
      case 0xd2: {
        this.need(4);
        const v = this.buf.readInt32BE(this.pos);
        this.pos += 4;
        return v;
      }
      case 0xd3: {
        this.need(8);
        const v = this.buf.readBigInt64BE(this.pos);
        this.pos += 8;
        return this._int(v);
      }
      case 0xd4:
        return this._ext(1);
      case 0xd5:
        return this._ext(2);
      case 0xd6:
        return this._ext(4);
      case 0xd7:
        return this._ext(8);
      case 0xd8:
        return this._ext(16);
      case 0xd9:
        return this._str(this.u8());
      case 0xda:
        return this._str(this.u16());
      case 0xdb:
        return this._str(this.u32());
      case 0xdc:
        return this._array(this.u16(), depth);
      case 0xdd:
        return this._array(this.u32(), depth);
      case 0xde:
        return this._map(this.u16(), depth);
      case 0xdf:
        return this._map(this.u32(), depth);
      case 0xc1:
      default:
        throw new FormatError("Unpack failed: error = -1");
    }
  }
}

/**
 * ``msgpack.unpackb(packed, **kwargs)``
 * @param {Uint8Array} packed
 * @param {{raw?: boolean, strict_map_key?: boolean, use_list?: boolean, object_hook?: Function, object_pairs_hook?: Function, list_hook?: Function, ext_hook?: Function}} [opts]
 */
export function unpackb(packed, opts = {}) {
  if (typeof packed === "string") throw new PyTypeError("a bytes-like object is required, not 'str'");
  const buf = Buffer.isBuffer(packed) ? packed : Buffer.from(packed.buffer, packed.byteOffset, packed.byteLength);
  const u = new _Unpacker(buf, opts);
  const ret = u.unpack();
  if (u.pos < buf.length) throw new ExtraData(ret, Buffer.from(buf.subarray(u.pos)));
  return ret;
}

/** Streaming unpacker (``for obj in msgpack.Unpacker(...)``), feed-based. */
export class Unpacker {
  constructor(opts = {}) {
    this._opts = opts;
    this._buf = Buffer.alloc(0);
  }
  feed(data) {
    this._buf = Buffer.concat([this._buf, Buffer.from(data)]);
  }
  *[Symbol.iterator]() {
    for (;;) {
      if (this._buf.length === 0) return;
      const u = new _Unpacker(this._buf, this._opts);
      let obj;
      try {
        obj = u.unpack();
      } catch (e) {
        if (e instanceof ValueError && /incomplete input/.test(e.message)) return;
        throw e;
      }
      this._buf = Buffer.from(this._buf.subarray(u.pos));
      yield obj;
    }
  }
}

export const dumps = packb;
export const loads = unpackb;
export const pack = (o, opts) => packb(o, opts);
export const unpack = (b, opts) => unpackb(b, opts);
export const version = [1, 1, 2];
