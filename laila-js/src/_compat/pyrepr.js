/**
 * Python ``repr()`` for the JS value model (see pytypes.js).
 *
 * Byte-exact with CPython for None/bool/int/float/str/bytes/list/tuple/dict/
 * set and for objects exposing ``__repr__``. Needed wherever Python embeds a
 * repr in persisted data: transformation recovery codes (``**{"ttl": 60}``)
 * and the Manifest SQL fingerprint ``sha256(repr((columns, rows)))``.
 */
import {
  PyTuple,
  PyFloat,
  PyFrozenSet,
  PyByteArray,
  float_repr,
  is_plain_object,
  dict_items,
  is_integral,
} from "./pytypes.js";

/** Python ``repr(x)``. */
export function repr(x) {
  if (x === null || x === undefined) return "None";
  if (x === true) return "True";
  if (x === false) return "False";
  switch (typeof x) {
    case "string":
      return str_repr(x);
    case "number":
      return is_integral(x) ? String(x) : float_repr(x);
    case "bigint":
      return x.toString();
    case "function":
      // Callable proxies (``_RemoteAttrChain``) carry a Python ``__repr__``.
      if (typeof x.__repr__ === "function") return x.__repr__();
      return `<function ${x.name || "<lambda>"}>`;
    case "symbol":
      return x.toString();
    default:
      break;
  }
  if (typeof x.__repr__ === "function") return x.__repr__();
  if (x instanceof PyFloat) return float_repr(x.valueOf());
  if (x instanceof Number) return float_repr(x.valueOf());
  if (x instanceof String) return str_repr(x.valueOf());
  if (x instanceof PyTuple) return x.length === 1 ? `(${repr(x[0])},)` : `(${x.map(repr).join(", ")})`;
  if (Array.isArray(x)) return `[${x.map(repr).join(", ")}]`;
  if (x instanceof PyByteArray) return `bytearray(${bytes_repr(x)})`;
  if (x instanceof Uint8Array) return bytes_repr(x);
  if (x instanceof PyFrozenSet) return x.size === 0 ? "frozenset()" : `frozenset({${[...x].map(repr).join(", ")}})`;
  if (x instanceof Set) return x.size === 0 ? "set()" : `{${[...x].map(repr).join(", ")}}`;
  if (x instanceof Map || is_plain_object(x) || typeof x.toDict === "function") {
    const items = dict_items(x);
    return `{${items.map(([k, v]) => `${repr(k)}: ${repr(v)}`).join(", ")}}`;
  }
  if (x instanceof Error) return `${x.name}(${str_repr(x.message)})`;
  if (x instanceof Date) return `datetime.datetime(${x.toISOString()})`;
  if (typeof x.toString === "function" && x.toString !== Object.prototype.toString) return x.toString();
  return `<${x.constructor?.name ?? "object"} object>`;
}

/**
 * Python ``str.__repr__``: single quotes unless the string contains a single
 * quote and no double quote; escapes ``\\``, the quote, ``\n \r \t``; other
 * non-printable code points as ``\xNN`` / ``\uNNNN`` / ``\UNNNNNNNN``;
 * printable non-ASCII is kept verbatim.
 */
export function str_repr(s) {
  let quote = "'";
  if (s.includes("'") && !s.includes('"')) quote = '"';
  let out = quote;
  for (const ch of s) {
    const cp = ch.codePointAt(0);
    if (ch === quote || ch === "\\") out += "\\" + ch;
    else if (ch === "\n") out += "\\n";
    else if (ch === "\r") out += "\\r";
    else if (ch === "\t") out += "\\t";
    else if (cp < 0x20 || cp === 0x7f) out += "\\x" + cp.toString(16).padStart(2, "0");
    else if (cp < 0x7f) out += ch;
    else if (!_is_printable(cp)) {
      if (cp <= 0xff) out += "\\x" + cp.toString(16).padStart(2, "0");
      else if (cp <= 0xffff) out += "\\u" + cp.toString(16).padStart(4, "0");
      else out += "\\U" + cp.toString(16).padStart(8, "0");
    } else out += ch;
  }
  return out + quote;
}

// Python's str.isprintable(): everything except categories Cc, Cf, Cs, Co, Cn,
// Zl, Zp, Zs (other than ASCII space). Approximated with Unicode property
// escapes, which is exact for assigned code points.
const _NONPRINTABLE = /[\p{Cc}\p{Cf}\p{Cs}\p{Co}\p{Cn}\p{Zl}\p{Zp}\p{Zs}]/u;
function _is_printable(cp) {
  if (cp === 0x20) return true;
  return !_NONPRINTABLE.test(String.fromCodePoint(cp));
}

/** Python ``bytes.__repr__`` (``b'...'``). */
export function bytes_repr(b) {
  let has_sq = false;
  let has_dq = false;
  for (const c of b) {
    if (c === 0x27) has_sq = true;
    else if (c === 0x22) has_dq = true;
  }
  const quote = has_sq && !has_dq ? '"' : "'";
  let out = "b" + quote;
  for (const c of b) {
    if (c === quote.charCodeAt(0) || c === 0x5c) out += "\\" + String.fromCharCode(c);
    else if (c === 0x09) out += "\\t";
    else if (c === 0x0a) out += "\\n";
    else if (c === 0x0d) out += "\\r";
    else if (c < 0x20 || c >= 0x7f) out += "\\x" + c.toString(16).padStart(2, "0");
    else out += String.fromCharCode(c);
  }
  return out + quote;
}

/** Python ``ascii()``: like repr but escapes all non-ASCII. */
export function ascii(x) {
  const r = repr(x);
  let out = "";
  for (const ch of r) {
    const cp = ch.codePointAt(0);
    if (cp < 0x80) out += ch;
    else if (cp <= 0xff) out += "\\x" + cp.toString(16).padStart(2, "0");
    else if (cp <= 0xffff) out += "\\u" + cp.toString(16).padStart(4, "0");
    else out += "\\U" + cp.toString(16).padStart(8, "0");
  }
  return out;
}
