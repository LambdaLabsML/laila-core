/**
 * ``NDArray`` -- the JS stand-in for ``numpy.ndarray`` (``CD_numpyarray``).
 *
 * Carries exactly what the ``.npy`` format needs: a numpy dtype string
 * (``"<f8"``, ``"<i4"``, ``"|u1"``, ``">f8"`` ...), a shape, ``fortran_order``
 * and the raw values in a TypedArray. Storage is always host (little-endian)
 * order; the dtype's byte order is applied by ``tobytes()`` / the constructor
 * when raw bytes cross the boundary, so a big-endian array round-trips with
 * its ``dtype.str`` intact exactly like numpy.
 *
 * Supported dtypes: all numpy scalar kinds laila serializes -- ``b1``,
 * ``i1..i8``, ``u1..u8``, ``f2`` (IEEE half, stored as ``Uint16Array`` bits),
 * ``f4``, ``f8``, ``c8``, ``c16`` (interleaved re/im). Construction from nested
 * JS arrays mirrors ``np.array(nested, dtype=...)``; ``tolist()`` is the
 * inverse (``Complex`` instances for complex kinds).
 */
import { ValueError, TypeError as PyTypeError } from "./errors.js";
import { PyFloat, is_integral } from "./pytypes.js";

/** ``complex`` scalar (``(1+2j)``). */
export class Complex {
  constructor(real, imag = 0) {
    this.real = Number(real);
    this.imag = Number(imag);
    Object.freeze(this);
  }
  __eq__(other) {
    if (other instanceof Complex) return this.real === other.real && this.imag === other.imag;
    if (typeof other === "number" || other instanceof Number) return this.imag === 0 && this.real === Number(other);
    return false;
  }
  __repr__() {
    const f = (x) => (Number.isInteger(x) ? `${x}` : `${x}`);
    if (this.real === 0 && !Object.is(this.real, -0)) return `${f(this.imag)}j`;
    const sign = this.imag < 0 || Object.is(this.imag, -0) ? "-" : "+";
    return `(${f(this.real)}${sign}${f(Math.abs(this.imag))}j)`;
  }
  toString() {
    return this.__repr__();
  }
  [Symbol.for("nodejs.util.inspect.custom")]() {
    return this.__repr__();
  }
}

// base (little-endian / size-1) specs; "lanes" = typed-array slots per element
const _DTYPES = {
  "<f8": { ctor: Float64Array, size: 8, kind: "f", lanes: 1 },
  "<f4": { ctor: Float32Array, size: 4, kind: "f", lanes: 1 },
  "<f2": { ctor: Uint16Array, size: 2, kind: "f", lanes: 1, half: true },
  "<i8": { ctor: BigInt64Array, size: 8, kind: "i", lanes: 1 },
  "<i4": { ctor: Int32Array, size: 4, kind: "i", lanes: 1 },
  "<i2": { ctor: Int16Array, size: 2, kind: "i", lanes: 1 },
  "|i1": { ctor: Int8Array, size: 1, kind: "i", lanes: 1 },
  "<u8": { ctor: BigUint64Array, size: 8, kind: "u", lanes: 1 },
  "<u4": { ctor: Uint32Array, size: 4, kind: "u", lanes: 1 },
  "<u2": { ctor: Uint16Array, size: 2, kind: "u", lanes: 1 },
  "|u1": { ctor: Uint8Array, size: 1, kind: "u", lanes: 1 },
  "|b1": { ctor: Uint8Array, size: 1, kind: "b", lanes: 1 },
  "<c16": { ctor: Float64Array, size: 16, kind: "c", lanes: 2 },
  "<c8": { ctor: Float32Array, size: 8, kind: "c", lanes: 2 },
};
const _ALIASES = {
  float64: "<f8",
  float: "<f8",
  double: "<f8",
  f8: "<f8",
  float32: "<f4",
  single: "<f4",
  f4: "<f4",
  float16: "<f2",
  half: "<f2",
  f2: "<f2",
  int64: "<i8",
  int: "<i8",
  i8: "<i8",
  int32: "<i4",
  i4: "<i4",
  int16: "<i2",
  i2: "<i2",
  int8: "|i1",
  i1: "|i1",
  uint64: "<u8",
  u8: "<u8",
  uint32: "<u4",
  u4: "<u4",
  uint16: "<u2",
  u2: "<u2",
  uint8: "|u1",
  u1: "|u1",
  bool: "|b1",
  bool_: "|b1",
  b1: "|b1",
  complex128: "<c16",
  complex: "<c16",
  c16: "<c16",
  complex64: "<c8",
  c8: "<c8",
  "<b1": "|b1",
  ">b1": "|b1",
  "=b1": "|b1",
  "<u1": "|u1",
  ">u1": "|u1",
  "=u1": "|u1",
  "<i1": "|i1",
  ">i1": "|i1",
  "=i1": "|i1",
};

/**
 * Canonical numpy dtype string (``dtype.str``). Big-endian descrs of
 * multi-byte types are preserved (``">i4"`` stays ``">i4"``); ``"="`` means
 * native (little-endian) order.
 */
export function normalize_dtype(dtype) {
  if (dtype === null || dtype === undefined) return "<f8";
  if (dtype === Number) return "<f8";
  if (dtype === Boolean) return "|b1";
  if (dtype === BigInt) return "<i8";
  const s = String(dtype);
  if (s in _DTYPES) return s;
  if (s in _ALIASES) return _ALIASES[s];
  if (s.length > 1 && (s[0] === ">" || s[0] === "=")) {
    const le = "<" + s.slice(1);
    if (le in _DTYPES) return s[0] === "=" ? le : s;
    if (le in _ALIASES) return _ALIASES[le];
  }
  throw new ValueError(`unsupported dtype: ${s}`);
}

/** Storage spec for a canonical dtype string (``big`` flags non-native order). */
function _spec(dtype) {
  const big = dtype[0] === ">";
  const base = big ? "<" + dtype.slice(1) : dtype;
  const spec = _DTYPES[base];
  if (!spec) throw new ValueError(`unsupported dtype: ${dtype}`);
  return big ? { ...spec, big: true, base } : { ...spec, big: false, base };
}

// --- IEEE 754 binary16 <-> binary64 (numpy ``npy_double_to_half`` semantics) -
const _DV = new DataView(new ArrayBuffer(8));

export function half_to_double(h) {
  const sign = h & 0x8000 ? -1 : 1;
  const exp = (h >>> 10) & 0x1f;
  const frac = h & 0x3ff;
  if (exp === 0) return sign * 2 ** -24 * frac;
  if (exp === 31) return frac ? NaN : sign * Infinity;
  return sign * 2 ** (exp - 15) * (1 + frac / 1024);
}

export function double_to_half(v) {
  _DV.setFloat64(0, Number(v));
  const hi = _DV.getUint32(0);
  const lo = _DV.getUint32(4);
  const sign = (hi >>> 16) & 0x8000;
  const exp = (hi >>> 20) & 0x7ff;
  const mant_hi = hi & 0xfffff;
  if (exp === 0x7ff) {
    if (mant_hi === 0 && lo === 0) return sign | 0x7c00;
    let ret = (sign | 0x7c00 | (mant_hi >>> 10)) & 0xffff;
    if (ret === (sign | 0x7c00)) ret++;
    return ret;
  }
  const e = exp - 1023 + 15;
  const mant = mant_hi * 4294967296 + lo; // 52-bit significand (exact in double)
  if (e >= 0x1f) return sign | 0x7c00;
  if (e <= 0) {
    if (e < -10) return sign;
    const full = mant + 2 ** 52;
    const div = 2 ** (43 - e);
    let half = Math.floor(full / div);
    const rem = full - half * div;
    const halfway = div / 2;
    if (rem > halfway || (rem === halfway && half & 1)) half++;
    return sign | half;
  }
  const div = 2 ** 42;
  let frac = Math.floor(mant / div);
  const rem = mant - frac * div;
  const halfway = div / 2;
  let half = sign | (e << 10) | frac;
  if (rem > halfway || (rem === halfway && half & 1)) half++;
  return half & 0xffff;
}

function _to_storage(spec, x) {
  if (spec.half) return double_to_half(x instanceof Number ? Number(x) : x);
  if (spec.size === 8 && (spec.kind === "i" || spec.kind === "u")) return typeof x === "bigint" ? x : BigInt(Math.trunc(Number(x)));
  if (spec.kind === "b") return x ? 1 : 0;
  return Number(x);
}

function _from_storage(spec, x) {
  if (spec.half) return half_to_double(x);
  if (spec.kind === "b") return x !== 0;
  if (typeof x === "bigint") return x >= BigInt(Number.MIN_SAFE_INTEGER) && x <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(x) : x;
  return x;
}

export class NDArray {
  /**
   * @param {object} opts
   * @param {string} opts.dtype  numpy dtype string
   * @param {number[]} opts.shape
   * @param {ArrayBufferView|ArrayBuffer|any[]} opts.data  values, a matching
   *   TypedArray, or raw bytes in the dtype's byte order
   * @param {boolean} [opts.fortran_order]
   */
  constructor({ dtype, shape, data, fortran_order = false }) {
    this.dtype = normalize_dtype(dtype);
    this.shape = Object.freeze([...shape].map((d) => Number(d)));
    this.fortran_order = Boolean(fortran_order);
    const spec = _spec(this.dtype);
    const n = this.shape.reduce((a, b) => a * b, 1);
    const slots = n * spec.lanes;

    if (data instanceof ArrayBuffer) data = new Uint8Array(data);
    if (data instanceof spec.ctor && !(spec.ctor === Uint8Array && spec.size !== 1) && data.length === slots) {
      // already in storage form
    } else if (ArrayBuffer.isView(data) && data.BYTES_PER_ELEMENT === 1 && data.byteLength === slots * (spec.size / spec.lanes)) {
      // raw bytes in the dtype's byte order: copy to an aligned buffer
      const buf = new ArrayBuffer(data.byteLength);
      const u8 = new Uint8Array(buf);
      u8.set(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
      const width = spec.size / spec.lanes;
      if (spec.big !== !_is_little_endian() && width > 1) _byteswap(u8, width);
      data = new spec.ctor(buf);
    } else if (ArrayBuffer.isView(data) && data.length === slots) {
      data = spec.ctor.from(data, (x) => _to_storage(spec, x));
    } else {
      const flat = Array.isArray(data) ? data : Array.from(data ?? []);
      if (spec.kind === "c") {
        if (flat.length === slots && !flat.some((x) => x instanceof Complex)) {
          data = spec.ctor.from(flat, (x) => Number(x));
        } else {
          data = new spec.ctor(flat.length * 2);
          flat.forEach((x, i) => {
            if (x instanceof Complex) {
              data[2 * i] = x.real;
              data[2 * i + 1] = x.imag;
            } else data[2 * i] = Number(x);
          });
        }
      } else data = spec.ctor.from(flat, (x) => _to_storage(spec, x));
    }
    if (data.length !== slots) throw new ValueError(`cannot reshape array of size ${data.length / spec.lanes} into shape (${this.shape.join(", ")})`);
    this.data = data;
  }

  /** ``np.array(nested, dtype=None)`` */
  static array(nested, dtype = null) {
    if (nested instanceof NDArray) return dtype === null ? nested : nested.astype(dtype);
    const shape = [];
    let cur = nested;
    while (Array.isArray(cur) || ArrayBuffer.isView(cur)) {
      shape.push(cur.length);
      cur = cur[0];
    }
    const flat = _flatten(nested, []);
    if (dtype === null) {
      if (flat.length && flat.every((x) => typeof x === "boolean")) dtype = "|b1";
      else if (flat.some((x) => x instanceof Complex)) dtype = "<c16";
      else if (flat.length && flat.every((x) => (is_integral(x) && !(x instanceof PyFloat)) || typeof x === "bigint")) dtype = "<i8";
      else dtype = "<f8";
    }
    return new NDArray({ dtype, shape, data: flat });
  }
  static arange(n, dtype = "<i8") {
    return new NDArray({ dtype, shape: [n], data: Array.from({ length: n }, (_, i) => i) });
  }
  static zeros(shape, dtype = "<f8") {
    const n = shape.reduce((a, b) => a * b, 1);
    return new NDArray({ dtype, shape, data: new Array(n).fill(0) });
  }
  static ones(shape, dtype = "<f8") {
    const n = shape.reduce((a, b) => a * b, 1);
    return new NDArray({ dtype, shape, data: new Array(n).fill(1) });
  }

  /** ``ndarray.copy()`` -- a new array with its own (C-ordered) buffer. */
  copy() {
    const c = this._to_c_order();
    return new NDArray({ dtype: this.dtype, shape: this.shape, data: c.data.slice() });
  }

  get ndim() {
    return this.shape.length;
  }
  get size() {
    return this.shape.reduce((a, b) => a * b, 1);
  }
  get itemsize() {
    return _spec(this.dtype).size;
  }
  get nbytes() {
    return this.data.byteLength;
  }
  /** ``dtype.byteorder`` (``"<"``, ``">"`` or ``"|"``). */
  get byteorder() {
    return this.dtype[0];
  }

  /** Element ``i`` of the flat storage (``flat[i]``) as a JS value. */
  _item(i) {
    const spec = _spec(this.dtype);
    if (spec.kind === "c") return new Complex(this.data[2 * i], this.data[2 * i + 1]);
    return _from_storage(spec, this.data[i]);
  }

  /** Raw bytes of the storage in the dtype's byte order (declared layout, as in memory). */
  _raw_bytes() {
    const d = this.data;
    const spec = _spec(this.dtype);
    const bytes = Buffer.from(Buffer.from(d.buffer, d.byteOffset, d.byteLength));
    const width = spec.size / spec.lanes;
    return spec.big !== !_is_little_endian() && width > 1 ? _byteswap(bytes, width) : bytes;
  }

  /**
   * ``ndarray.tobytes(order="C")``: bytes in the dtype's byte order. ``"C"``
   * (default) always yields C-order bytes; ``"F"`` Fortran order; ``"A"`` the
   * array's own layout.
   */
  tobytes(order = "C") {
    if (order === "A" || this.shape.length < 2) return this._raw_bytes();
    if (order === "C") return this._to_c_order()._raw_bytes();
    if (order === "F") return this._to_f_order()._raw_bytes();
    throw new ValueError(`order must be one of 'C', 'F', 'A'; got ${order}`);
  }

  /** Nested JS arrays (``ndarray.tolist()``); ints come back as numbers when safe. */
  tolist() {
    if (this.fortran_order && this.shape.length > 1) return this._to_c_order().tolist();
    const n = this.size;
    const vals = new Array(n);
    for (let i = 0; i < n; i++) vals[i] = this._item(i);
    if (this.shape.length === 0) return vals[0];
    const build = (dim, offset) => {
      const len = this.shape[dim];
      if (dim === this.shape.length - 1) return vals.slice(offset, offset + len);
      const stride = this.shape.slice(dim + 1).reduce((a, b) => a * b, 1);
      const out = [];
      for (let i = 0; i < len; i++) out.push(build(dim + 1, offset + i * stride));
      return out;
    };
    return build(0, 0);
  }

  _to_c_order() {
    if (!this.fortran_order) return this;
    const spec = _spec(this.dtype);
    const lanes = spec.lanes;
    const n = this.size;
    const out = new this.data.constructor(n * lanes);
    const shape = this.shape;
    const nd = shape.length;
    const idx = new Array(nd).fill(0);
    for (let c = 0; c < n; c++) {
      // c-order index -> fortran offset
      let f = 0;
      let mul = 1;
      for (let d = 0; d < nd; d++) {
        f += idx[d] * mul;
        mul *= shape[d];
      }
      for (let l = 0; l < lanes; l++) out[c * lanes + l] = this.data[f * lanes + l];
      for (let d = nd - 1; d >= 0; d--) {
        if (++idx[d] < shape[d]) break;
        idx[d] = 0;
      }
    }
    return new NDArray({ dtype: this.dtype, shape, data: out, fortran_order: false });
  }

  _to_f_order() {
    if (this.fortran_order || this.shape.length < 2) return new NDArray({ dtype: this.dtype, shape: this.shape, data: this.data, fortran_order: this.shape.length > 1 });
    const spec = _spec(this.dtype);
    const lanes = spec.lanes;
    const n = this.size;
    const out = new this.data.constructor(n * lanes);
    const shape = this.shape;
    const nd = shape.length;
    const idx = new Array(nd).fill(0);
    for (let c = 0; c < n; c++) {
      let f = 0;
      let mul = 1;
      for (let d = 0; d < nd; d++) {
        f += idx[d] * mul;
        mul *= shape[d];
      }
      for (let l = 0; l < lanes; l++) out[f * lanes + l] = this.data[c * lanes + l];
      for (let d = nd - 1; d >= 0; d--) {
        if (++idx[d] < shape[d]) break;
        idx[d] = 0;
      }
    }
    return new NDArray({ dtype: this.dtype, shape, data: out, fortran_order: true });
  }

  reshape(shape) {
    return new NDArray({ dtype: this.dtype, shape, data: this.data, fortran_order: this.fortran_order });
  }
  /** ``ndarray.astype(dtype)`` (complex -> real discards the imaginary part). */
  astype(dtype) {
    const target = normalize_dtype(dtype);
    const src = _spec(this.dtype);
    const dst = _spec(target);
    const n = this.size;
    const vals = new Array(n);
    for (let i = 0; i < n; i++) {
      let v = this._item(i);
      if (v instanceof Complex && dst.kind !== "c") v = v.real;
      if (typeof v === "boolean") v = v ? 1 : 0;
      vals[i] = v;
    }
    if (src.base === dst.base && src.kind === dst.kind) {
      return new NDArray({ dtype: target, shape: this.shape, data: this.data.slice(), fortran_order: this.fortran_order });
    }
    return new NDArray({ dtype: target, shape: this.shape, data: vals, fortran_order: this.fortran_order });
  }
  flatten() {
    return new NDArray({ dtype: this.dtype, shape: [this.size], data: this._to_c_order().data });
  }

  /**
   * ``np.array_equal(self, other, equal_nan=False)``: same shape and
   * element-wise equal values (dtype may differ, like numpy).
   */
  array_equal(other, { equal_nan = false } = {}) {
    if (!(other instanceof NDArray)) return false;
    if (this.shape.length !== other.shape.length || this.shape.some((s, i) => s !== other.shape[i])) return false;
    const a = this._to_c_order();
    const b = other._to_c_order();
    const n = a.size;
    const same = (x, y) => {
      if (x instanceof Complex || y instanceof Complex) {
        const xc = x instanceof Complex ? x : new Complex(x);
        const yc = y instanceof Complex ? y : new Complex(y);
        return same(xc.real, yc.real) && same(xc.imag, yc.imag);
      }
      if (typeof x === "bigint" || typeof y === "bigint") return BigInt(x) === BigInt(y);
      const xn = Number(x);
      const yn = Number(y);
      if (equal_nan && Number.isNaN(xn) && Number.isNaN(yn)) return true;
      return xn === yn;
    };
    for (let i = 0; i < n; i++) if (!same(a._item(i), b._item(i))) return false;
    return true;
  }
  /** ``np.array_equal`` (NaN != NaN, as in numpy's default). */
  __eq__(other) {
    return this.array_equal(other);
  }
  __repr__() {
    const body = JSON.stringify(this.tolist(), (_k, v) => (typeof v === "bigint" ? Number(v) : v instanceof Complex ? v.__repr__() : v));
    return `array(${body}, dtype=${_dtype_name(this.dtype)})`;
  }
  toString() {
    return this.__repr__();
  }
  [Symbol.for("nodejs.util.inspect.custom")]() {
    return this.__repr__();
  }
  toJSON() {
    return this.tolist();
  }
}

/** ``str(np.dtype(d))``: ``"float64"`` for native dtypes, the descr for non-native. */
function _dtype_name(d) {
  const spec = _spec(d);
  if (spec.big) return d;
  if (spec.kind === "b") return "bool";
  if (spec.kind === "c") return `complex${spec.size * 8}`;
  return `${spec.kind === "f" ? "float" : spec.kind === "i" ? "int" : "uint"}${spec.size * 8}`;
}

function _flatten(x, out) {
  if (Array.isArray(x) || ArrayBuffer.isView(x)) for (const v of x) _flatten(v, out);
  else out.push(x);
  return out;
}

function _is_little_endian() {
  return new Uint8Array(new Uint16Array([1]).buffer)[0] === 1;
}
function _byteswap(buf, size) {
  for (let i = 0; i < buf.length; i += size) buf.subarray(i, i + size).reverse();
  return buf;
}

/** ``isinstance(x, np.ndarray)`` */
export function is_ndarray(x) {
  return x instanceof NDArray;
}

export { _DTYPES as DTYPES, _spec as dtype_spec, PyTypeError as _TypeError, _dtype_name as dtype_name };
