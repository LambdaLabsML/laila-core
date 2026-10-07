/**
 * ``ast.literal_eval`` for the Python literal subset that laila embeds in
 * recovery code and ``.npy`` headers: dict, list, tuple, set, str (all quote
 * styles incl. triple quotes and escapes), bytes, int (incl. big), float
 * (incl. ``inf`` / ``nan`` are *not* literals in Python -- rejected), True,
 * False, None, unary minus.
 *
 * Produces values in the pytypes.js model: dict -> plain Object / Map (rule
 * D1), tuple -> PyTuple, bytes -> Buffer, integral float -> PyFloat.
 */
import { PyTuple, PyFloat, dict_from_entries } from "./pytypes.js";
import { ValueError, SyntaxError as PySyntaxError } from "./errors.js";

class _P {
  constructor(src) {
    this.s = src;
    this.i = 0;
  }
  ws() {
    for (;;) {
      while (this.i < this.s.length && /\s/.test(this.s[this.i])) this.i++;
      if (this.s[this.i] === "#") {
        while (this.i < this.s.length && this.s[this.i] !== "\n") this.i++;
        continue;
      }
      break;
    }
  }
  peek() {
    this.ws();
    return this.s[this.i];
  }
  expect(c) {
    this.ws();
    if (this.s[this.i] !== c) throw new PySyntaxError(`expected '${c}' at ${this.i}`);
    this.i++;
  }
  fail(msg) {
    throw new PySyntaxError(`${msg} at position ${this.i}`);
  }

  value() {
    const c = this.peek();
    if (c === undefined) this.fail("unexpected end of input");
    if (c === "{") return this.dict_or_set();
    if (c === "[") return this.list();
    if (c === "(") return this.tuple();
    if (c === "'" || c === '"') return this.string();
    if (c === "b" || c === "B" || c === "r" || c === "R" || c === "u" || c === "U" || c === "f") {
      const m = /^([bBrRuU]{1,2})(['"])/.exec(this.s.slice(this.i));
      if (m) return this.string();
    }
    if (c === "-" || c === "+" || /[0-9.]/.test(c)) return this.number();
    const m = /^(True|False|None)\b/.exec(this.s.slice(this.i));
    if (m) {
      this.i += m[1].length;
      return m[1] === "True" ? true : m[1] === "False" ? false : null;
    }
    this.fail("malformed node or string");
  }

  dict_or_set() {
    this.expect("{");
    if (this.peek() === "}") {
      this.i++;
      return {};
    }
    const first = this.value();
    if (this.peek() === ":") {
      this.i++;
      const entries = [[first, this.value()]];
      for (;;) {
        const c = this.peek();
        if (c === ",") {
          this.i++;
          if (this.peek() === "}") break;
          const k = this.value();
          this.expect(":");
          entries.push([k, this.value()]);
          continue;
        }
        break;
      }
      this.expect("}");
      return dict_from_entries(entries);
    }
    const items = [first];
    while (this.peek() === ",") {
      this.i++;
      if (this.peek() === "}") break;
      items.push(this.value());
    }
    this.expect("}");
    return new Set(items);
  }

  seq(close) {
    const items = [];
    let trailing = false;
    while (this.peek() !== close) {
      items.push(this.value());
      trailing = false;
      if (this.peek() === ",") {
        this.i++;
        trailing = true;
      } else break;
    }
    this.expect(close);
    return [items, trailing];
  }
  list() {
    this.expect("[");
    return this.seq("]")[0];
  }
  tuple() {
    this.expect("(");
    const [items, trailing] = this.seq(")");
    if (items.length === 1 && !trailing) return items[0]; // parenthesised expr
    return PyTuple.from_iterable(items);
  }

  number() {
    const m = /^[+-]?(?:0[xX][0-9a-fA-F_]+|0[oO][0-7_]+|0[bB][01_]+|(?:\d[\d_]*)?\.?\d[\d_]*(?:[eE][+-]?\d+)?j?|\d[\d_]*\.(?:[eE][+-]?\d+)?)/.exec(this.s.slice(this.i));
    if (!m) this.fail("malformed number");
    let txt = m[0];
    this.i += txt.length;
    const neg = txt.startsWith("-");
    if (neg || txt.startsWith("+")) txt = txt.slice(1);
    txt = txt.replace(/_/g, "");
    if (txt.endsWith("j")) this.fail("complex literals are not supported");
    const is_float = /[.eE]/.test(txt) && !/^0[xX]/.test(txt);
    if (is_float) {
      const f = parseFloat(txt) * (neg ? -1 : 1);
      return Number.isInteger(f) ? new PyFloat(f) : f;
    }
    let big;
    if (/^0[xX]/.test(txt)) big = BigInt(txt);
    else if (/^0[oO]/.test(txt)) big = BigInt(txt);
    else if (/^0[bB]/.test(txt)) big = BigInt(txt);
    else big = BigInt(txt);
    if (neg) big = -big;
    if (big >= BigInt(Number.MIN_SAFE_INTEGER) && big <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(big);
    return big;
  }

  string() {
    // prefix
    let prefix = "";
    while (/[bBrRuUfF]/.test(this.s[this.i])) prefix += this.s[this.i++];
    const is_bytes = /[bB]/.test(prefix);
    const is_raw = /[rR]/.test(prefix);
    if (/[fF]/.test(prefix)) this.fail("f-strings are not literals");
    const q = this.s[this.i];
    let quote = q;
    if (this.s.startsWith(q + q + q, this.i)) quote = q + q + q;
    this.i += quote.length;
    let out = "";
    for (;;) {
      if (this.i >= this.s.length) this.fail("unterminated string");
      if (this.s.startsWith(quote, this.i)) {
        this.i += quote.length;
        break;
      }
      const c = this.s[this.i];
      if (c === "\\" && !is_raw) {
        this.i++;
        const e = this.s[this.i++];
        switch (e) {
          case "n":
            out += "\n";
            break;
          case "t":
            out += "\t";
            break;
          case "r":
            out += "\r";
            break;
          case "0":
            out += "\0";
            break;
          case "a":
            out += "\x07";
            break;
          case "b":
            out += "\b";
            break;
          case "f":
            out += "\f";
            break;
          case "v":
            out += "\v";
            break;
          case "\\":
            out += "\\";
            break;
          case "'":
            out += "'";
            break;
          case '"':
            out += '"';
            break;
          case "\n":
            break;
          case "x": {
            const h = this.s.substr(this.i, 2);
            this.i += 2;
            out += String.fromCharCode(parseInt(h, 16));
            break;
          }
          case "u": {
            if (is_bytes) {
              out += "\\u";
              break;
            }
            const h = this.s.substr(this.i, 4);
            this.i += 4;
            out += String.fromCharCode(parseInt(h, 16));
            break;
          }
          case "U": {
            if (is_bytes) {
              out += "\\U";
              break;
            }
            const h = this.s.substr(this.i, 8);
            this.i += 8;
            out += String.fromCodePoint(parseInt(h, 16));
            break;
          }
          default:
            if (/[0-7]/.test(e)) {
              let oct = e;
              while (oct.length < 3 && /[0-7]/.test(this.s[this.i])) oct += this.s[this.i++];
              out += String.fromCharCode(parseInt(oct, 8));
            } else out += "\\" + e;
        }
        continue;
      }
      if ((c === "\n") && quote.length === 1) this.fail("EOL while scanning string literal");
      out += c;
      this.i++;
    }
    // implicit concatenation of adjacent literals
    this.ws();
    if (this.s[this.i] === "'" || this.s[this.i] === '"' || /^[bBrRuU]{1,2}['"]/.test(this.s.slice(this.i))) {
      const next = this.string();
      if (is_bytes !== next instanceof Uint8Array) this.fail("cannot mix bytes and nonbytes literals");
      if (is_bytes) return Buffer.concat([Buffer.from(out, "latin1"), next]);
      return out + next;
    }
    if (is_bytes) {
      for (let k = 0; k < out.length; k++) if (out.charCodeAt(k) > 0xff) this.fail("bytes can only contain ASCII literal characters");
      return Buffer.from(out, "latin1");
    }
    return out;
  }
}

/** ``ast.literal_eval(src)`` */
export function literal_eval(src) {
  if (typeof src !== "string") throw new ValueError("literal_eval expects a string");
  const p = new _P(src);
  const v = p.value();
  p.ws();
  if (p.i !== src.length) throw new PySyntaxError(`unexpected trailing input at ${p.i}`);
  return v;
}
