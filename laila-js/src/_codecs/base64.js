/**
 * Python ``base64`` / ``binascii`` semantics on Buffers.
 *
 *   b64encode(s, altchars=None) -> bytes
 *   b64decode(s, altchars=None, validate=False) -> bytes
 *   urlsafe_b64encode / urlsafe_b64decode, standard_b64encode / standard_b64decode
 *
 * ``b64decode`` with ``validate=False`` discards non-alphabet characters
 * (``binascii.a2b_base64`` non-strict) and raises ``binascii.Error`` on bad
 * padding; ``validate=True`` rejects any non-alphabet character up front.
 * ``str`` inputs must be ASCII (``ValueError`` otherwise), as in Python.
 */
import { ValueError, TypeError as PyTypeError } from "../_compat/errors.js";

/** ``binascii.Error`` (a ``ValueError`` subclass). */
export class Error extends ValueError {}
export { Error as BinasciiError };

function _bytes_from_decode_data(s) {
  if (typeof s === "string" || s instanceof String) {
    const str = s.valueOf();
    for (let i = 0; i < str.length; i++)
      if (str.charCodeAt(i) > 0x7f) throw new ValueError("string argument should contain only ASCII characters");
    return Buffer.from(str, "ascii");
  }
  if (s instanceof Uint8Array) return Buffer.from(s.buffer, s.byteOffset, s.byteLength);
  if (s instanceof ArrayBuffer) return Buffer.from(s);
  throw new PyTypeError(`argument should be a bytes-like object or ASCII string, not '${s === null ? "NoneType" : s?.constructor?.name ?? typeof s}'`);
}

function _bytes_in(s, what = "a bytes-like object is required") {
  if (s instanceof Uint8Array) return Buffer.from(s.buffer, s.byteOffset, s.byteLength);
  if (s instanceof ArrayBuffer) return Buffer.from(s);
  throw new PyTypeError(`${what}, not '${typeof s === "string" ? "str" : s?.constructor?.name ?? typeof s}'`);
}

/** ``base64.b64encode(s, altchars=None)`` */
export function b64encode(s, opts = {}) {
  const altchars = opts.altchars ?? null;
  let enc = _bytes_in(s).toString("base64");
  if (altchars !== null && altchars !== undefined) {
    const alt = _bytes_from_decode_data(altchars);
    if (alt.length !== 2) throw new ValueError(`altchars must be a sequence of length 2`); // AssertionError in CPython (assert len == 2)
    const a = String.fromCharCode(alt[0]);
    const b = String.fromCharCode(alt[1]);
    enc = enc.replace(/\+/g, a).replace(/\//g, b);
  }
  return Buffer.from(enc, "latin1");
}

const _STD = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const _IS_STD = new Uint8Array(256);
for (let i = 0; i < _STD.length; i++) _IS_STD[_STD.charCodeAt(i)] = 1;

/** ``base64.b64decode(s, altchars=None, validate=False)`` */
export function b64decode(s, opts = {}) {
  const altchars = opts.altchars ?? null;
  const validate = opts.validate ?? false;
  let data = _bytes_from_decode_data(s);
  if (altchars !== null && altchars !== undefined) {
    const alt = _bytes_from_decode_data(altchars);
    if (alt.length !== 2) throw new ValueError(`altchars must be a sequence of length 2`);
    data = Buffer.from(data);
    for (let i = 0; i < data.length; i++) {
      if (data[i] === alt[0]) data[i] = 0x2b;
      else if (data[i] === alt[1]) data[i] = 0x2f;
    }
  }
  return a2b_base64(data, { strict_mode: Boolean(validate) });
}

/**
 * ``binascii.a2b_base64(data, strict_mode=False)`` -- faithful port of the
 * CPython decoder including its error messages.
 */
export function a2b_base64(data, { strict_mode = false } = {}) {
  const buf = _bytes_from_decode_data(data);
  const n = buf.length;
  const out = [];
  let quad_pos = 0;
  let leftchar = 0;
  let pads = 0;
  let padding_started = false;
  if (strict_mode && n > 0 && buf[0] === 0x3d) throw new Error("Leading padding not allowed");
  for (let i = 0; i < n; i++) {
    const c = buf[i];
    if (c === 0x3d) {
      padding_started = true;
      if (strict_mode && quad_pos === 0) throw new Error(i === 0 ? "Leading padding not allowed" : "Excess padding not allowed");
      if (quad_pos >= 2 && quad_pos + ++pads >= 4) {
        if (strict_mode && i + 1 < n) throw new Error("Excess data after padding");
        return Buffer.from(out);
      }
      continue;
    }
    if (!_IS_STD[c]) {
      if (strict_mode) throw new Error("Only base64 data is allowed");
      continue;
    }
    if (strict_mode && padding_started) throw new Error("Discontinuous padding not allowed");
    pads = 0;
    const v = _STD.indexOf(String.fromCharCode(c));
    switch (quad_pos) {
      case 0:
        quad_pos = 1;
        leftchar = v;
        break;
      case 1:
        quad_pos = 2;
        out.push((leftchar << 2) | (v >> 4));
        leftchar = v & 0x0f;
        break;
      case 2:
        quad_pos = 3;
        out.push((leftchar << 4) | (v >> 2));
        leftchar = v & 0x03;
        break;
      case 3:
        quad_pos = 0;
        out.push((leftchar << 6) | v);
        leftchar = 0;
        break;
    }
  }
  if (quad_pos !== 0) {
    if (quad_pos === 1) {
      const count = out.length;
      throw new Error(`Invalid base64-encoded string: number of data characters (${Math.floor((count / 3) * 4) + 1}) cannot be 1 more than a multiple of 4`);
    }
    throw new Error("Incorrect padding");
  }
  return Buffer.from(out);
}

export function standard_b64encode(s) {
  return b64encode(s);
}
export function standard_b64decode(s) {
  return b64decode(s);
}
export function urlsafe_b64encode(s) {
  return b64encode(s, { altchars: Buffer.from("-_") });
}
export function urlsafe_b64decode(s) {
  return b64decode(s, { altchars: Buffer.from("-_") });
}
export function b64encode_str(s, opts) {
  return b64encode(s, opts).toString("latin1");
}
