/**
 * Recovery-code emitter + recognizer.
 *
 * Every laila transformation stores a *Python* source snippet
 * (``backward_code``) inside the entry's ``SimpleConstitution`` so that the
 * payload can be rebuilt without importing the transformation class. The JS
 * port must (a) emit the exact same strings (constitutions are compared and
 * hashed byte-for-byte across runtimes) and (b) be able to *execute* those
 * snippets -- which it does by recognising the finite set of templates laila
 * emits and dispatching to native JS implementations.
 *
 * Templates (one per transformation, kwargs embedded via ``repr``):
 *   base64, zlib, json_string, msgpack, numpy, pickle, torch, fernet
 *
 * Unrecognised code raises ``ValueError`` -- laila-js never evaluates
 * arbitrary Python.
 */
import { repr } from "../_compat/pyrepr.js";
import { literal_eval } from "../_compat/pyliteral.js";
import { ValueError, TypeError as PyTypeError, NotImplementedError } from "../_compat/errors.js";
import { dict_items } from "../_compat/pytypes.js";
import * as pyjson from "../_compat/pyjson.js";
import * as b64 from "./base64.js";
import * as zlib from "./zlib.js";
import * as msgpack from "./msgpack.js";
import * as npy from "./npy.js";
import * as pickle from "./pickle.js";

// --------------------------------------------------------------------------
// Emitters -- string-identical to the Python ``model_post_init`` bodies
// --------------------------------------------------------------------------

/** ``repr(dict)`` of a kwargs mapping (plain Object / Map / DotMap). */
export function kwargs_repr(kwargs) {
  if (kwargs === null || kwargs === undefined) return "{}";
  return repr(kwargs);
}

export function emit_base64(backward_kwargs = {}) {
  return "def backward(inp):\n" + "    import base64\n" + `    kwargs = ${kwargs_repr(backward_kwargs)}\n` + "    if isinstance(inp, memoryview):\n" + "        inp = inp.tobytes()\n" + "    return base64.b64decode(inp, **kwargs)\n";
}

export function emit_zlib(backward_kwargs = {}) {
  return (
    "\n" +
    "def backward(data):\n" +
    "    import zlib, base64\n" +
    "    if not isinstance(data, str):\n" +
    '        raise TypeError("Zlib.backward expects a Base64 string (str)")\n' +
    "    compressed = base64.b64decode(data, validate=True)\n" +
    `    return zlib.decompress(compressed, **${kwargs_repr(backward_kwargs)}).decode("utf-8")\n`
  );
}

export function emit_json_string() {
  return "\n" + "def backward(data):\n" + "    import json\n" + "    if not isinstance(data, str):\n" + '        raise TypeError("JsonString.backward expects a JSON string (str)")\n' + "    return json.loads(data)\n";
}

export function emit_msgpack(backward_kwargs = {}) {
  return "\n" + "def backward(inp):\n" + "    import msgpack\n" + `    kwargs = {"raw": False, "strict_map_key": False, **${kwargs_repr(backward_kwargs)}}\n` + "    return msgpack.unpackb(inp, **kwargs)\n";
}

export function emit_numpy(backward_kwargs = {}) {
  return "\n" + "def backward(inp):\n" + "    import io\n" + "    import numpy as np\n" + "    buf = io.BytesIO(inp)\n" + `    kwargs = {'allow_pickle': False, **${kwargs_repr(backward_kwargs)}}\n` + "    return np.load(buf, **kwargs)\n";
}

export function emit_pickle(backward_kwargs = {}) {
  return "\n" + "def backward(inp):\n" + "    import pickle\n" + `    kwargs = ${kwargs_repr(backward_kwargs)}\n` + "    return pickle.loads(inp, **kwargs)\n";
}

export function emit_torch(backward_kwargs = {}) {
  return "\n" + "def backward(inp):\n" + "    import io\n" + "    import torch\n" + "    buf = io.BytesIO(inp)\n" + `    kwargs = ${kwargs_repr(backward_kwargs)}\n` + "    return torch.load(buf, **kwargs)\n";
}

export function emit_fernet(fingerprint, backward_kwargs = {}) {
  return (
    "\n" +
    "def backward(inp):\n" +
    "    from cryptography.fernet import Fernet, InvalidToken\n" +
    "    from laila.entry.compdata.transformation.encryption.encryption import (\n" +
    "        resolve_encryption_key,\n" +
    "    )\n" +
    "    if not isinstance(inp, str):\n" +
    '        raise TypeError("Encryption.backward expects a Fernet token string (str)")\n' +
    `    f = Fernet(resolve_encryption_key(expected_fingerprint=${repr(fingerprint)}))\n` +
    '    token = inp.encode("utf-8")\n' +
    `    kwargs = ${kwargs_repr(backward_kwargs)}\n` +
    '    ttl = kwargs.get("ttl", None)\n' +
    "    try:\n" +
    "        out = f.decrypt(token, ttl=ttl)\n" +
    "    except InvalidToken as e:\n" +
    '        raise ValueError("Invalid Fernet token or TTL expired") from e\n' +
    '    return out.decode("utf-8")\n'
  );
}

// --------------------------------------------------------------------------
// Recognizer
// --------------------------------------------------------------------------

function _esc(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
const _KW = "([\\s\\S]*?)";

// Build a template regex from the emitter output with placeholders.
function _template(fn, nargs) {
  const marks = Array.from({ length: nargs }, (_, i) => `\u0000${i}\u0000`);
  const src = fn(...marks);
  let re = _esc(src);
  for (const m of marks) re = re.replace(_esc(m), _KW);
  return new RegExp("^" + re + "$");
}

const _TEMPLATES = [
  { name: "base64", re: _template((k) => emit_base64(_Raw(k)), 1), fields: ["kwargs"] },
  { name: "zlib", re: _template((k) => emit_zlib(_Raw(k)), 1), fields: ["kwargs"] },
  { name: "json_string", re: _template(() => emit_json_string(), 0), fields: [] },
  { name: "msgpack", re: _template((k) => emit_msgpack(_Raw(k)), 1), fields: ["kwargs"] },
  { name: "numpy", re: _template((k) => emit_numpy(_Raw(k)), 1), fields: ["kwargs"] },
  { name: "pickle", re: _template((k) => emit_pickle(_Raw(k)), 1), fields: ["kwargs"] },
  { name: "torch", re: _template((k) => emit_torch(_Raw(k)), 1), fields: ["kwargs"] },
  { name: "fernet", re: _template((f, k) => emit_fernet(_Raw(f), _Raw(k)), 2), fields: ["fingerprint", "kwargs"] },
];

// A value whose repr() is itself (placeholder passthrough for templating).
function _Raw(s) {
  return { __repr__: () => s };
}

/**
 * Recognise a recovery-code string.
 * @returns {{name: string, kwargs: object, fingerprint?: string}|null}
 */
export function recognize(code) {
  if (typeof code !== "string") return null;
  for (const t of _TEMPLATES) {
    const m = t.re.exec(code);
    if (!m) continue;
    const out = { name: t.name, kwargs: {} };
    t.fields.forEach((f, i) => {
      const txt = m[i + 1];
      if (f === "kwargs") out.kwargs = literal_eval(txt);
      else if (f === "fingerprint") out.fingerprint = literal_eval(txt);
    });
    return out;
  }
  return null;
}

// --------------------------------------------------------------------------
// Backends -- JS implementations of each ``backward``
// --------------------------------------------------------------------------

const _BACKENDS = new Map();

/**
 * Register (or override) the JS implementation for a recognised template.
 * ``factory(info)`` receives ``{name, kwargs, fingerprint?}`` and returns the
 * ``backward(inp)`` function. The encryption transformation registers the
 * ``fernet`` backend (it needs ``laila.args`` for key resolution).
 */
export function register_backend(name, factory) {
  _BACKENDS.set(name, factory);
}

function _kw_obj(kwargs) {
  const o = {};
  for (const [k, v] of dict_items(kwargs ?? {})) o[String(k)] = v;
  return o;
}

register_backend("base64", ({ kwargs }) => {
  const kw = _kw_obj(kwargs);
  return (inp) => {
    if (inp instanceof DataView) inp = new Uint8Array(inp.buffer, inp.byteOffset, inp.byteLength);
    return b64.b64decode(inp, kw);
  };
});

register_backend("zlib", ({ kwargs }) => {
  const kw = _kw_obj(kwargs);
  return (data) => {
    if (typeof data !== "string") throw new PyTypeError("Zlib.backward expects a Base64 string (str)");
    const compressed = b64.b64decode(data, { validate: true });
    return _utf8_decode(zlib.decompress(compressed, kw));
  };
});

register_backend("json_string", () => (data) => {
  if (typeof data !== "string") throw new PyTypeError("JsonString.backward expects a JSON string (str)");
  return pyjson.loads(data);
});

register_backend("msgpack", ({ kwargs }) => {
  const kw = { raw: false, strict_map_key: false, ..._kw_obj(kwargs) };
  return (inp) => msgpack.unpackb(inp, kw);
});

register_backend("numpy", ({ kwargs }) => {
  const kw = { allow_pickle: false, ..._kw_obj(kwargs) };
  return (inp) => npy.load(inp, kw);
});

register_backend("pickle", ({ kwargs }) => {
  const kw = _kw_obj(kwargs);
  return (inp) => pickle.loads(inp, kw);
});

register_backend("torch", () => () => {
  throw new NotImplementedError("torch payloads cannot be rebuilt in laila-js (no torch runtime)");
});

const _utf8_strict = new TextDecoder("utf-8", { fatal: true });
function _utf8_decode(b) {
  return _utf8_strict.decode(b);
}

/**
 * JS counterpart of ``laila.entry.constitution.constitution._exec_one_fn``:
 * turn a recovery-code string into a callable ``backward(inp)``.
 * @param {string} code
 * @returns {(inp: any) => any}
 */
export function compile_backward(code) {
  const info = recognize(code);
  if (info === null) throw new ValueError("constitution code is not a recognised laila recovery snippet; laila-js cannot execute arbitrary Python");
  const factory = _BACKENDS.get(info.name);
  if (!factory) throw new ValueError(`no JS backend registered for recovery code '${info.name}'`);
  const fn = factory(info);
  Object.defineProperty(fn, "name", { value: "backward" });
  return fn;
}

export const TEMPLATE_NAMES = Object.freeze(_TEMPLATES.map((t) => t.name));
