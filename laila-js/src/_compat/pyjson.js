/**
 * Python ``json`` module semantics (``json.dumps`` / ``json.loads``).
 *
 * Differences from ``JSON.stringify``/``JSON.parse`` that matter for byte
 * identity with Python-written data and wire frames:
 *   - default separators ``", "`` and ``": "`` (``","`` + ``": "`` with indent)
 *   - ``ensure_ascii=True``: non-ASCII escaped as ``\\uXXXX`` (surrogate pairs)
 *   - floats use Python ``repr`` (``1e-05``, ``1.0``), ``Infinity``/``NaN`` allowed
 *   - ints of any size are bare digits (BigInt in, BigInt out beyond 2**53)
 *   - integral JSON floats (``1.0``) decode to ``PyFloat`` so they stay floats
 *   - dicts decode per rule D1 (plain Object, or Map when a key is index-like)
 *   - non-string dict keys are stringified like Python (``True`` -> ``"true"``)
 */
import { JSONDecodeError, TypeError as PyTypeError, ValueError } from "./errors.js";
import {
  PyFloat,
  PyTuple,
  float_repr,
  is_plain_object,
  dict_items,
  dict_from_entries,
  type_name,
  is_integral,
} from "./pytypes.js";

/**
 * Python ``json.dumps``.
 * @param {any} obj
 * @param {{indent?: number|string|null, separators?: [string,string]|null, ensure_ascii?: boolean,
 *          sort_keys?: boolean, default?: (o:any)=>any, allow_nan?: boolean, skipkeys?: boolean}} [opts]
 */
export function dumps(obj, opts = {}) {
  const {
    indent = null,
    separators = null,
    ensure_ascii = true,
    sort_keys = false,
    default: dflt = null,
    allow_nan = true,
    skipkeys = false,
  } = opts;
  let item_sep;
  let key_sep;
  if (separators) [item_sep, key_sep] = separators;
  else if (indent !== null && indent !== undefined) [item_sep, key_sep] = [",", ": "];
  else [item_sep, key_sep] = [", ", ": "];
  const indent_str = indent === null || indent === undefined ? null : typeof indent === "number" ? " ".repeat(indent) : indent;
  const seen = new Set();

  const enc_str = (s) => encode_string(s, ensure_ascii);

  function enc(o, level) {
    if (o === null || o === undefined) return "null";
    if (o === true) return "true";
    if (o === false) return "false";
    const t = typeof o;
    if (t === "string") return enc_str(o);
    if (t === "number") return enc_number(o, allow_nan);
    if (t === "bigint") return o.toString();
    if (o instanceof PyFloat || o instanceof Number) return enc_float(o.valueOf(), allow_nan);
    if (o instanceof String) return enc_str(o.valueOf()); // str-Enum members
    if (Array.isArray(o)) return enc_list(o, level);
    if (o instanceof Map || is_plain_object(o) || (typeof o.toDict === "function" && typeof o.items === "function"))
      return enc_dict(o, level);
    // (``__json__`` methods are *not* consulted: CPython's encoder only knows
    // ``default``; ``_LAILA_IDENTIFIABLE_FUTURE.__json__`` is a plain helper.)
    if (dflt) {
      if (seen.has(o)) throw new ValueError("Circular reference detected");
      seen.add(o);
      try {
        return enc(dflt(o), level);
      } finally {
        seen.delete(o);
      }
    }
    throw new PyTypeError(`Object of type ${type_name(o)} is not JSON serializable`);
  }

  function enc_list(arr, level) {
    if (arr.length === 0) return "[]";
    if (seen.has(arr)) throw new ValueError("Circular reference detected");
    seen.add(arr);
    try {
      if (indent_str === null) return "[" + arr.map((v) => enc(v, level)).join(item_sep) + "]";
      const nl = "\n" + indent_str.repeat(level + 1);
      return "[" + nl + arr.map((v) => enc(v, level + 1)).join(item_sep + nl) + "\n" + indent_str.repeat(level) + "]";
    } finally {
      seen.delete(arr);
    }
  }

  function enc_key(k) {
    if (typeof k === "string") return k;
    if (k instanceof String) return k.valueOf();
    if (k === null || k === undefined) return "null";
    if (k === true) return "true";
    if (k === false) return "false";
    if (typeof k === "number") return is_integral(k) ? String(k) : float_repr(k);
    if (k instanceof PyFloat) return float_repr(k.valueOf());
    if (typeof k === "bigint") return k.toString();
    if (skipkeys) return undefined;
    throw new PyTypeError(`keys must be str, int, float, bool or None, not ${type_name(k)}`);
  }

  function enc_dict(d, level) {
    let items = dict_items(d);
    if (items.length === 0) return "{}";
    if (seen.has(d)) throw new ValueError("Circular reference detected");
    seen.add(d);
    try {
      let pairs = [];
      for (const [k, v] of items) {
        const ks = enc_key(k);
        if (ks === undefined) continue;
        pairs.push([ks, v]);
      }
      if (sort_keys) pairs.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
      if (indent_str === null)
        return "{" + pairs.map(([k, v]) => enc_str(k) + key_sep + enc(v, level)).join(item_sep) + "}";
      const nl = "\n" + indent_str.repeat(level + 1);
      return (
        "{" +
        nl +
        pairs.map(([k, v]) => enc_str(k) + key_sep + enc(v, level + 1)).join(item_sep + nl) +
        "\n" +
        indent_str.repeat(level) +
        "}"
      );
    } finally {
      seen.delete(d);
    }
  }

  return enc(obj, 0);
}

function enc_number(n, allow_nan) {
  if (is_integral(n)) return String(n);
  return enc_float(n, allow_nan);
}
function enc_float(n, allow_nan) {
  if (Number.isNaN(n) || n === Infinity || n === -Infinity) {
    if (!allow_nan) throw new ValueError("Out of range float values are not JSON compliant");
    return Number.isNaN(n) ? "NaN" : n > 0 ? "Infinity" : "-Infinity";
  }
  return float_repr(n);
}

const _ESC = { '"': '\\"', "\\": "\\\\", "\n": "\\n", "\r": "\\r", "\t": "\\t", "\b": "\\b", "\f": "\\f" };

/** Python ``json.encoder.encode_basestring[_ascii]``. */
export function encode_string(s, ensure_ascii = true) {
  let out = '"';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    const code = s.charCodeAt(i);
    const e = _ESC[ch];
    if (e !== undefined) out += e;
    else if (code < 0x20) out += "\\u" + code.toString(16).padStart(4, "0");
    else if (ensure_ascii && code > 0x7e) out += "\\u" + code.toString(16).padStart(4, "0");
    else out += ch;
  }
  return out + '"';
}

// --------------------------------------------------------------------------
// loads
// --------------------------------------------------------------------------

/**
 * Python ``json.loads``.
 * @param {string|Uint8Array} s
 * @param {{object_hook?: (o:any)=>any, parse_float?: (s:string)=>any, parse_int?: (s:string)=>any,
 *          as_map?: boolean}} [opts]  ``as_map`` forces Map for every object.
 */
export function loads(s, opts = {}) {
  if (s instanceof Uint8Array) s = Buffer.from(s.buffer, s.byteOffset, s.byteLength).toString("utf8");
  if (typeof s !== "string")
    throw new PyTypeError(`the JSON object must be str, bytes or bytearray, not ${type_name(s)}`);
  const p = new _Parser(s, opts);
  p.skip_ws();
  const v = p.value();
  p.skip_ws();
  if (p.i < s.length) p.fail("Extra data");
  return v;
}

class _Parser {
  constructor(s, opts) {
    this.s = s;
    this.i = 0;
    this.opts = opts;
  }
  fail(msg) {
    const upto = this.s.slice(0, this.i);
    const line = (upto.match(/\n/g) || []).length + 1;
    const col = this.i - upto.lastIndexOf("\n");
    throw new JSONDecodeError(`${msg}: line ${line} column ${col} (char ${this.i})`);
  }
  skip_ws() {
    const s = this.s;
    while (this.i < s.length) {
      const c = s.charCodeAt(this.i);
      if (c === 0x20 || c === 0x09 || c === 0x0a || c === 0x0d) this.i++;
      else break;
    }
  }
  value() {
    const s = this.s;
    if (this.i >= s.length) this.fail("Expecting value");
    const c = s[this.i];
    if (c === "{") return this.object();
    if (c === "[") return this.array();
    if (c === '"') return this.string();
    if (c === "-" || (c >= "0" && c <= "9")) return this.number();
    if (s.startsWith("true", this.i)) {
      this.i += 4;
      return true;
    }
    if (s.startsWith("false", this.i)) {
      this.i += 5;
      return false;
    }
    if (s.startsWith("null", this.i)) {
      this.i += 4;
      return null;
    }
    if (s.startsWith("NaN", this.i)) {
      this.i += 3;
      return NaN;
    }
    if (s.startsWith("Infinity", this.i)) {
      this.i += 8;
      return Infinity;
    }
    if (s.startsWith("-Infinity", this.i)) {
      this.i += 9;
      return -Infinity;
    }
    this.fail("Expecting value");
  }
  object() {
    this.i++; // {
    const entries = [];
    this.skip_ws();
    if (this.s[this.i] === "}") {
      this.i++;
      return this.finish_object(entries);
    }
    for (;;) {
      this.skip_ws();
      if (this.s[this.i] !== '"') this.fail("Expecting property name enclosed in double quotes");
      const k = this.string();
      this.skip_ws();
      if (this.s[this.i] !== ":") this.fail("Expecting ':' delimiter");
      this.i++;
      this.skip_ws();
      const v = this.value();
      // Python: later duplicates overwrite earlier ones (position of first kept).
      const existing = entries.findIndex((e) => e[0] === k);
      if (existing >= 0) entries[existing][1] = v;
      else entries.push([k, v]);
      this.skip_ws();
      const c = this.s[this.i];
      if (c === ",") {
        this.i++;
        continue;
      }
      if (c === "}") {
        this.i++;
        return this.finish_object(entries);
      }
      this.fail("Expecting ',' delimiter");
    }
  }
  finish_object(entries) {
    let o = this.opts.as_map ? new Map(entries) : dict_from_entries(entries);
    if (this.opts.object_hook) o = this.opts.object_hook(o);
    return o;
  }
  array() {
    this.i++; // [
    const out = [];
    this.skip_ws();
    if (this.s[this.i] === "]") {
      this.i++;
      return out;
    }
    for (;;) {
      this.skip_ws();
      out.push(this.value());
      this.skip_ws();
      const c = this.s[this.i];
      if (c === ",") {
        this.i++;
        continue;
      }
      if (c === "]") {
        this.i++;
        return out;
      }
      this.fail("Expecting ',' delimiter");
    }
  }
  string() {
    const s = this.s;
    let i = this.i + 1;
    let out = "";
    let start = i;
    for (;;) {
      if (i >= s.length) {
        this.i = start - 1;
        this.fail("Unterminated string starting at");
      }
      const c = s[i];
      if (c === '"') {
        out += s.slice(start, i);
        this.i = i + 1;
        return out;
      }
      if (c === "\\") {
        out += s.slice(start, i);
        const e = s[i + 1];
        i += 2;
        switch (e) {
          case '"':
            out += '"';
            break;
          case "\\":
            out += "\\";
            break;
          case "/":
            out += "/";
            break;
          case "b":
            out += "\b";
            break;
          case "f":
            out += "\f";
            break;
          case "n":
            out += "\n";
            break;
          case "r":
            out += "\r";
            break;
          case "t":
            out += "\t";
            break;
          case "u": {
            const hex = s.slice(i, i + 4);
            if (!/^[0-9a-fA-F]{4}$/.test(hex)) {
              this.i = i - 2;
              this.fail("Invalid \\uXXXX escape");
            }
            out += String.fromCharCode(parseInt(hex, 16));
            i += 4;
            break;
          }
          default:
            this.i = i - 2;
            this.fail("Invalid \\escape");
        }
        start = i;
        continue;
      }
      if (s.charCodeAt(i) < 0x20) {
        this.i = i;
        this.fail("Invalid control character at");
      }
      i++;
    }
  }
  number() {
    const s = this.s;
    const m = /^-?(?:0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?/.exec(s.slice(this.i, this.i + 400));
    if (!m) this.fail("Expecting value");
    this.i += m[0].length;
    const text = m[0];
    if (m[1] === undefined && m[2] === undefined) {
      if (this.opts.parse_int) return this.opts.parse_int(text);
      const n = Number(text);
      return Number.isSafeInteger(n) ? n : BigInt(text);
    }
    if (this.opts.parse_float) return this.opts.parse_float(text);
    const f = Number(text);
    return is_integral(f) ? new PyFloat(f) : f;
  }
}

/** ``json.dumps(obj, separators=(",", ":"), ensure_ascii=False)`` -- compact form used by ``JsonString``. */
export function dumps_compact(obj) {
  return dumps(obj, { separators: [",", ":"], ensure_ascii: false });
}

export { PyTuple };
