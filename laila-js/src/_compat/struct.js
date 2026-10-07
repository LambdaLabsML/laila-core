/**
 * Python ``struct`` module subset: ``pack``, ``unpack``, ``calcsize`` for the
 * standard-size formats (``<``, ``>``, ``!``, ``=``) with codes
 * ``x c b B ? h H i I l L q Q f d s``.
 *
 * Native-size/alignment (``@``) is not supported; laila only uses explicit
 * byte orders (``">I"``, ``">BBIB"``, ``">3sBHH"``, ``">H"``).
 */
import { ValueError, TypeError as PyTypeError } from "./errors.js";

export class StructError extends ValueError {}

const _SIZES = { x: 1, c: 1, b: 1, B: 1, "?": 1, h: 2, H: 2, i: 4, I: 4, l: 4, L: 4, q: 8, Q: 8, f: 4, d: 8, s: 1, p: 1 };

function _parse(fmt) {
  let i = 0;
  let little = false;
  const c0 = fmt[0];
  if (c0 === "<") {
    little = true;
    i = 1;
  } else if (c0 === ">" || c0 === "!") {
    i = 1;
  } else if (c0 === "=") {
    little = true; // host order; x86/arm64 are little-endian
    i = 1;
  } else if (c0 === "@") {
    throw new StructError("native alignment ('@') is not supported");
  }
  const items = [];
  while (i < fmt.length) {
    let ch = fmt[i];
    if (ch === " ") {
      i++;
      continue;
    }
    let count = "";
    while (ch >= "0" && ch <= "9") {
      count += ch;
      ch = fmt[++i];
    }
    if (!(ch in _SIZES)) throw new StructError(`bad char in struct format: ${ch}`);
    const n = count === "" ? 1 : parseInt(count, 10);
    if (ch === "s" || ch === "p") items.push({ code: ch, count: n });
    else for (let k = 0; k < n; k++) items.push({ code: ch, count: 1 });
    i++;
  }
  return { little, items };
}

/** ``struct.calcsize(fmt)`` */
export function calcsize(fmt) {
  const { items } = _parse(fmt);
  return items.reduce((a, it) => a + _SIZES[it.code] * it.count, 0);
}

/**
 * ``struct.pack(fmt, *values)``
 * @returns {Buffer}
 */
export function pack(fmt, ...values) {
  const { little, items } = _parse(fmt);
  const size = items.reduce((a, it) => a + _SIZES[it.code] * it.count, 0);
  const buf = Buffer.alloc(size);
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let off = 0;
  let vi = 0;
  for (const it of items) {
    const need = it.code === "x" ? 0 : 1;
    if (need && vi >= values.length)
      throw new StructError(`pack expected ${items.filter((x) => x.code !== "x").length} items for packing (got ${values.length})`);
    const v = values[vi];
    switch (it.code) {
      case "x":
        off += 1;
        continue;
      case "s":
      case "p": {
        const b = v instanceof Uint8Array ? v : Buffer.from(String(v), "utf8");
        buf.fill(0, off, off + it.count);
        Buffer.from(b.buffer, b.byteOffset, Math.min(b.byteLength, it.count)).copy(buf, off);
        off += it.count;
        break;
      }
      case "c":
        buf[off] = v instanceof Uint8Array ? v[0] : String(v).charCodeAt(0);
        off += 1;
        break;
      case "?":
        buf[off] = v ? 1 : 0;
        off += 1;
        break;
      case "b":
        _range(v, -128, 127, "b");
        view.setInt8(off, Number(v));
        off += 1;
        break;
      case "B":
        _range(v, 0, 255, "B");
        view.setUint8(off, Number(v));
        off += 1;
        break;
      case "h":
        _range(v, -32768, 32767, "h");
        view.setInt16(off, Number(v), little);
        off += 2;
        break;
      case "H":
        _range(v, 0, 65535, "H");
        view.setUint16(off, Number(v), little);
        off += 2;
        break;
      case "i":
      case "l":
        _range(v, -2147483648, 2147483647, it.code);
        view.setInt32(off, Number(v), little);
        off += 4;
        break;
      case "I":
      case "L":
        _range(v, 0, 4294967295, it.code);
        view.setUint32(off, Number(v), little);
        off += 4;
        break;
      case "q":
        view.setBigInt64(off, BigInt(v), little);
        off += 8;
        break;
      case "Q":
        if (BigInt(v) < 0n) throw new StructError("argument out of range");
        view.setBigUint64(off, BigInt(v), little);
        off += 8;
        break;
      case "f":
        view.setFloat32(off, Number(v), little);
        off += 4;
        break;
      case "d":
        view.setFloat64(off, Number(v), little);
        off += 8;
        break;
      default:
        throw new StructError(`bad char in struct format: ${it.code}`);
    }
    vi++;
  }
  if (vi !== values.length) throw new StructError(`pack expected ${vi} items for packing (got ${values.length})`);
  return buf;
}

function _range(v, lo, hi, code) {
  if (typeof v !== "number" && typeof v !== "bigint" && typeof v !== "boolean")
    throw new StructError("required argument is not an integer");
  const n = Number(v);
  if (!Number.isInteger(n)) throw new StructError("required argument is not an integer");
  if (n < lo || n > hi) throw new StructError(`'${code}' format requires ${lo} <= number <= ${hi}`);
}

/**
 * ``struct.unpack(fmt, buffer)`` -> array of values. 64-bit ints come back as
 * numbers when safe, otherwise BigInt.
 */
export function unpack(fmt, data) {
  const { little, items } = _parse(fmt);
  const size = items.reduce((a, it) => a + _SIZES[it.code] * it.count, 0);
  if (!(data instanceof Uint8Array)) throw new PyTypeError("a bytes-like object is required");
  if (data.byteLength !== size) throw new StructError(`unpack requires a buffer of ${size} bytes`);
  return unpack_from(fmt, data, 0, { little, items });
}

/** ``struct.unpack_from(fmt, buffer, offset=0)`` */
export function unpack_from(fmt, data, offset = 0, parsed = null) {
  const { little, items } = parsed ?? _parse(fmt);
  const size = items.reduce((a, it) => a + _SIZES[it.code] * it.count, 0);
  if (data.byteLength - offset < size)
    throw new StructError(`unpack_from requires a buffer of at least ${size + offset} bytes for unpacking ${size} bytes at offset ${offset} (actual buffer size is ${data.byteLength})`);
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const out = [];
  let off = offset;
  for (const it of items) {
    switch (it.code) {
      case "x":
        off += 1;
        break;
      case "s":
      case "p":
        out.push(Buffer.from(data.buffer, data.byteOffset + off, it.count));
        off += it.count;
        break;
      case "c":
        out.push(Buffer.from([data[off]]));
        off += 1;
        break;
      case "?":
        out.push(data[off] !== 0);
        off += 1;
        break;
      case "b":
        out.push(view.getInt8(off));
        off += 1;
        break;
      case "B":
        out.push(view.getUint8(off));
        off += 1;
        break;
      case "h":
        out.push(view.getInt16(off, little));
        off += 2;
        break;
      case "H":
        out.push(view.getUint16(off, little));
        off += 2;
        break;
      case "i":
      case "l":
        out.push(view.getInt32(off, little));
        off += 4;
        break;
      case "I":
      case "L":
        out.push(view.getUint32(off, little));
        off += 4;
        break;
      case "q": {
        const b = view.getBigInt64(off, little);
        out.push(b >= BigInt(Number.MIN_SAFE_INTEGER) && b <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(b) : b);
        off += 8;
        break;
      }
      case "Q": {
        const b = view.getBigUint64(off, little);
        out.push(b <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(b) : b);
        off += 8;
        break;
      }
      case "f":
        out.push(view.getFloat32(off, little));
        off += 4;
        break;
      case "d":
        out.push(view.getFloat64(off, little));
        off += 8;
        break;
      default:
        throw new StructError(`bad char in struct format: ${it.code}`);
    }
  }
  return out;
}

/** ``struct.Struct(fmt)`` */
export class Struct {
  constructor(fmt) {
    this.format = fmt;
    this._parsed = _parse(fmt);
    this.size = this._parsed.items.reduce((a, it) => a + _SIZES[it.code] * it.count, 0);
  }
  pack(...values) {
    return pack(this.format, ...values);
  }
  unpack(data) {
    return unpack(this.format, data);
  }
  unpack_from(data, offset = 0) {
    return unpack_from(this.format, data, offset, this._parsed);
  }
}
