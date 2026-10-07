/**
 * CPython ``pickle`` -- protocol 5 writer and protocol 0-5 reader, byte-exact
 * with the C ``_pickle`` module for the builtin types laila serialises:
 * None, bool, int, float, str, bytes, bytearray, list, tuple, dict, set,
 * frozenset (and nested combinations).
 *
 * Writer rules mirrored from ``Modules/_pickle.c`` (``dumps(obj)`` with the
 * default protocol):
 *   - PROTO 5, then a single FRAME per <= 64 KiB of output (frame headers
 *     are only emitted for frames of >= 4 bytes); bytes/str payloads of
 *     >= 64 KiB are written outside frames ("large bytes").
 *   - ints: BININT1 / BININT2 / BININT / LONG1 / LONG4 by range.
 *   - str: SHORT_BINUNICODE / BINUNICODE / BINUNICODE8 (+ MEMOIZE)
 *   - bytes: SHORT_BINBYTES / BINBYTES / BINBYTES8 (+ MEMOIZE); bytearray: BYTEARRAY8
 *   - tuple: EMPTY_TUPLE | TUPLE1..3 | MARK ... TUPLE (+ MEMOIZE unless empty)
 *   - list: EMPTY_LIST MEMOIZE then APPEND / MARK ... APPENDS in batches of 1000
 *   - dict: EMPTY_DICT MEMOIZE then SETITEM / MARK ... SETITEMS in batches of 1000
 *   - set: EMPTY_SET MEMOIZE then MARK ... ADDITEMS in batches of 1000
 *   - frozenset: MARK ... FROZENSET MEMOIZE
 *   - memo hits: BINGET / LONG_BINGET
 *
 * Memoisation (rule K1): containers and byte strings by identity, JS strings
 * by *value* (JS strings have no identity; CPython would emit BINGET for the
 * same interned object, which is what repeated literals / dict keys are).
 *
 * Type mapping follows pytypes.js: integral JS numbers are ints, ``PyFloat``
 * and fractional numbers are floats, BigInt beyond 2**53 are LONG ints, Map /
 * plain Object are dicts, PyTuple is a tuple, Set / PyFrozenSet are sets.
 */
import { PyTuple, PyFloat, PyByteArray, PyFrozenSet, is_plain_object, is_integral, dict_items, dict_from_entries } from "../_compat/pytypes.js";
import { ValueError, TypeError as PyTypeError, PyException } from "../_compat/errors.js";
import { is_enum_member } from "../_compat/enum.js";
import { Complex, NDArray } from "../_compat/ndarray.js";

// --------------------------------------------------------------------------
// numpy interop (``ndarray.__reduce_ex__`` for numpy >= 2.0)
// --------------------------------------------------------------------------

/** A reference to a Python global (``module.name``) -- pickled via ``save_global``. */
class _GlobalRef {
  constructor(module, name) {
    this.module = module;
    this.name = name;
  }
}

/**
 * ``numpy.dtype`` as it travels through a pickle:
 * ``dtype.__reduce__()`` -> ``(numpy.dtype, (name, False, True), state)`` with
 * ``state = (3, byteorder, None, None, None, -1, -1, 0)`` for the builtin
 * scalar dtypes laila's ``NDArray`` supports.
 */
class _NpDtype {
  constructor(name, align = false, copy = true) {
    this.name = String(name);
    this.align = align;
    this.copy = copy;
    this.byteorder = this.name.length && "<>|=".includes(this.name[0]) ? this.name[0] : _NP_DTYPE_SIZE1.has(this.name) ? "|" : "<";
    if ("<>|=".includes(this.name[0])) this.name = this.name.slice(1);
  }
  /** numpy ``dtype.str`` (``"<f8"``, ``"|u1"``) */
  get str() {
    return `${this.byteorder === "=" ? "<" : this.byteorder}${this.name}`;
  }
  __reduce__() {
    return [["numpy", "dtype"], [this.name, false, true], PyTuple.from_iterable([3, this.byteorder, null, null, null, -1, -1, 0])];
  }
  __setstate__(state) {
    if (Array.isArray(state) && state.length >= 2 && typeof state[1] === "string") this.byteorder = state[1];
  }
}
const _NP_DTYPE_SIZE1 = new Set(["b1", "i1", "u1"]);
const _NP_RECONSTRUCT_B = Buffer.from("b", "latin1");

function _np_dtype_of(arr) {
  // arr.dtype is canonical: "<f8", "|u1", ...
  return new _NpDtype(arr.dtype);
}

/** ``numpy._core.numeric._frombuffer(buffer, dtype, shape, order)`` */
function _np_frombuffer(args) {
  const [buf, dtype, shape, order] = args;
  const dt = dtype instanceof _NpDtype ? dtype.str : String(dtype);
  return new NDArray({ dtype: dt, shape: [...shape], data: Buffer.from(buf), fortran_order: order === "F" });
}

/** ``numpy._core.multiarray._reconstruct(ndarray, (0,), b'b')`` -> placeholder filled by BUILD. */
function _np_reconstruct(_args) {
  const inst = Object.create(NDArray.prototype);
  Object.defineProperty(inst, "__setstate__", {
    configurable: true,
    value(state) {
      // (version, shape, dtype, is_fortran, rawdata)
      const [, shape, dtype, is_fortran, raw] = state;
      const dt = dtype instanceof _NpDtype ? dtype.str : String(dtype);
      const built = new NDArray({ dtype: dt, shape: [...shape], data: Buffer.from(raw), fortran_order: Boolean(is_fortran) });
      delete inst.__setstate__;
      Object.assign(inst, built);
    },
  });
  return inst;
}

export class PickleError extends PyException {}
export class PicklingError extends PickleError {}
export class UnpicklingError extends PickleError {}

export const HIGHEST_PROTOCOL = 5;
export const DEFAULT_PROTOCOL = 5;

// opcodes
const MARK = 0x28; // (
const STOP = 0x2e; // .
const POP = 0x30; // 0
const POP_MARK = 0x31; // 1
const DUP = 0x32; // 2
const FLOAT = 0x46; // F
const INT = 0x49; // I
const BININT = 0x4a; // J
const BININT1 = 0x4b; // K
const LONG = 0x4c; // L
const BININT2 = 0x4d; // M
const NONE = 0x4e; // N
const PERSID = 0x50; // P
const BINPERSID = 0x51; // Q
const REDUCE = 0x52; // R
const STRING = 0x53; // S
const BINSTRING = 0x54; // T
const SHORT_BINSTRING = 0x55; // U
const UNICODE = 0x56; // V
const BINUNICODE = 0x58; // X
const APPEND = 0x61; // a
const BUILD = 0x62; // b
const GLOBAL = 0x63; // c
const DICT = 0x64; // d
const EMPTY_DICT = 0x7d; // }
const APPENDS = 0x65; // e
const GET = 0x67; // g
const BINGET = 0x68; // h
const INST = 0x69; // i
const LONG_BINGET = 0x6a; // j
const LIST = 0x6c; // l
const EMPTY_LIST = 0x5d; // ]
const OBJ = 0x6f; // o
const PUT = 0x70; // p
const BINPUT = 0x71; // q
const LONG_BINPUT = 0x72; // r
const SETITEM = 0x73; // s
const TUPLE = 0x74; // t
const EMPTY_TUPLE = 0x29; // )
const SETITEMS = 0x75; // u
const BINFLOAT = 0x47; // G
const PROTO = 0x80;
const NEWOBJ = 0x81;
const EXT1 = 0x82;
const EXT2 = 0x83;
const EXT4 = 0x84;
const TUPLE1 = 0x85;
const TUPLE2 = 0x86;
const TUPLE3 = 0x87;
const NEWTRUE = 0x88;
const NEWFALSE = 0x89;
const LONG1 = 0x8a;
const LONG4 = 0x8b;
const BINBYTES = 0x42; // B
const SHORT_BINBYTES = 0x43; // C
const SHORT_BINUNICODE = 0x8c;
const BINUNICODE8 = 0x8d;
const BINBYTES8 = 0x8e;
const EMPTY_SET = 0x8f;
const ADDITEMS = 0x90;
const FROZENSET = 0x91;
const NEWOBJ_EX = 0x92;
const STACK_GLOBAL = 0x93;
const MEMOIZE = 0x94;
const FRAME = 0x95;
const BYTEARRAY8 = 0x96;
const NEXT_BUFFER = 0x97;
const READONLY_BUFFER = 0x98;

const FRAME_SIZE_MIN = 4;
const FRAME_SIZE_TARGET = 64 * 1024;
const BATCHSIZE = 1000;

// --------------------------------------------------------------------------
// Writer
// --------------------------------------------------------------------------

class _Framer {
  constructor() {
    this.chunks = []; // committed output
    this.frame = []; // current frame chunks
    this.frame_len = 0;
  }
  write(buf) {
    this.frame.push(buf);
    this.frame_len += buf.length;
  }
  /** C ``_Pickler_CommitFrame``: emit FRAME header when the frame is big enough. */
  commit_frame(force = false) {
    if (this.frame_len === 0) return;
    if (this.frame_len >= FRAME_SIZE_TARGET || force) {
      if (this.frame_len >= FRAME_SIZE_MIN) {
        const hdr = Buffer.alloc(9);
        hdr[0] = FRAME;
        hdr.writeBigUInt64LE(BigInt(this.frame_len), 1);
        this.chunks.push(hdr);
      }
      this.chunks.push(...this.frame);
      this.frame = [];
      this.frame_len = 0;
    }
  }
  /** Large bytes/str payloads are written outside any frame. */
  write_large_bytes(header, payload) {
    this.commit_frame(true);
    this.chunks.push(header, payload);
  }
  finish() {
    this.commit_frame(true);
    return Buffer.concat(this.chunks);
  }
}

class _Pickler {
  constructor(protocol, opts = {}) {
    this.proto = protocol;
    this.opts = opts;
    this.framer = new _Framer();
    this.memo = new Map(); // object/string -> index
    this.memo_len = 0;
    this.out = []; // bytes buffered before framing (we frame per top-level write)
  }
  write(b) {
    if (this.proto >= 4) this.framer.write(b);
    else this.framer.chunks.push(b);
  }
  byte(op) {
    this.write(Buffer.from([op]));
  }
  memoize(obj) {
    this.memo.set(obj, this.memo_len);
    this.memo_len += 1;
    if (this.proto >= 4) this.byte(MEMOIZE);
    else this.put(this.memo_len - 1);
  }
  put(idx) {
    if (idx < 256) this.write(Buffer.from([BINPUT, idx]));
    else {
      const b = Buffer.alloc(5);
      b[0] = LONG_BINPUT;
      b.writeUInt32LE(idx, 1);
      this.write(b);
    }
  }
  get(idx) {
    if (idx < 256) this.write(Buffer.from([BINGET, idx]));
    else {
      const b = Buffer.alloc(5);
      b[0] = LONG_BINGET;
      b.writeUInt32LE(idx, 1);
      this.write(b);
    }
  }

  dump(obj) {
    if (this.proto >= 2) this.write(Buffer.from([PROTO, this.proto]));
    if (this.proto >= 4) this.framer.commit_frame(true); // PROTO is outside the first frame
    this.save(obj);
    this.byte(STOP);
    return this.framer.finish();
  }

  save(obj) {
    // Memo lookup (identity for objects, value for strings/enum members)
    const key = this._memo_key(obj);
    if (key !== undefined && this.memo.has(key)) {
      this.get(this.memo.get(key));
      return;
    }
    if (obj === null || obj === undefined) return this.byte(NONE);
    if (obj === true) return this.byte(this.proto >= 2 ? NEWTRUE : INT);
    if (obj === false) return this.byte(this.proto >= 2 ? NEWFALSE : INT);
    switch (typeof obj) {
      case "number":
        if (is_integral(obj)) return this.save_long(BigInt(obj));
        return this.save_float(obj);
      case "bigint":
        return this.save_long(obj);
      case "string":
        return this.save_str(obj);
      default:
        break;
    }
    if (obj instanceof PyFloat || obj instanceof Number) return this.save_float(obj.valueOf());
    if (is_enum_member(obj)) return this.save_str(obj.value);
    if (obj instanceof String) return this.save_str(obj.valueOf());
    if (obj instanceof PyByteArray) return this.save_bytearray(obj);
    if (obj instanceof Uint8Array) return this.save_bytes(obj);
    if (obj instanceof PyTuple) return this.save_tuple(obj);
    if (Array.isArray(obj)) return this.save_list(obj);
    if (obj instanceof PyFrozenSet) return this.save_frozenset(obj);
    if (obj instanceof Set) return this.save_set(obj);
    if (obj instanceof Map || is_plain_object(obj)) return this.save_dict(obj);
    if (obj && typeof obj.toDict === "function" && typeof obj.items === "function") return this.save_dict(obj);
    if (obj instanceof NDArray) return this.save_ndarray(obj);
    if (obj instanceof Complex) return this.save_complex(obj);
    if (obj instanceof _GlobalRef) return this.save_global(obj.module, obj.name);
    if (typeof obj === "function") return this.save_function(obj);
    if (obj && typeof obj.__reduce__ === "function") return this.save_reduce(obj);
    throw new PicklingError(`Can't pickle ${obj.constructor?.name ?? typeof obj}: JS objects without a Python counterpart are not picklable`);
  }

  /**
   * Functions pickle by *reference* exactly as in CPython: a GLOBAL of
   * ``(fn.__module__, fn.__qualname__)``. A JS function has no implicit
   * ``__module__``; a module-level function becomes picklable by tagging it
   * (``fn.__module__ = import.meta.url``). Anything else -- lambdas, closures,
   * methods -- fails like CPython's ``attribute lookup <lambda> on __main__
   * failed``.
   */
  save_function(fn) {
    const module = fn.__module__;
    const name = fn.__qualname__ ?? fn.name;
    if (typeof module !== "string" || !module || typeof name !== "string" || !name || name === "<lambda>") {
      throw new PicklingError(`Can't pickle <function ${name || "<lambda>"}>: attribute lookup ${name || "<lambda>"} on __main__ failed`);
    }
    return this.save_global(module, name);
  }

  _memo_key(obj) {
    if (obj === null || obj === undefined) return undefined;
    const t = typeof obj;
    if (t === "string") return obj.length ? `s:${obj}` : undefined; // CPython memoizes "" too, but as the same singleton
    if (t === "function") return typeof obj.__module__ === "string" ? `g:${obj.__module__}.${obj.__qualname__ ?? obj.name}` : undefined;
    if (t !== "object") return undefined;
    if (obj instanceof PyFloat || obj instanceof Number) return undefined;
    if (is_enum_member(obj)) return `s:${obj.value}`;
    if (obj instanceof String) return `s:${obj.valueOf()}`;
    // builtin numpy dtypes are singletons in CPython (``np.dtype('f4') is np.dtype('f4')``)
    if (obj instanceof _NpDtype) return `d:${obj.str}`;
    if (obj instanceof _GlobalRef) return `g:${obj.module}.${obj.name}`;
    return obj;
  }

  save_long(v) {
    if (this.proto >= 2) {
      if (v >= 0n && v <= 0xffn) return this.write(Buffer.from([BININT1, Number(v)]));
      if (v >= 0n && v <= 0xffffn) {
        const b = Buffer.alloc(3);
        b[0] = BININT2;
        b.writeUInt16LE(Number(v), 1);
        return this.write(b);
      }
      if (v >= -0x80000000n && v <= 0x7fffffffn) {
        const b = Buffer.alloc(5);
        b[0] = BININT;
        b.writeInt32LE(Number(v), 1);
        return this.write(b);
      }
      const enc = _encode_long(v);
      if (enc.length < 256) return this.write(Buffer.concat([Buffer.from([LONG1, enc.length]), enc]));
      const hdr = Buffer.alloc(5);
      hdr[0] = LONG4;
      hdr.writeInt32LE(enc.length, 1);
      return this.write(Buffer.concat([hdr, enc]));
    }
    // protocol 0/1: INT with decimal repr
    if (v >= -0x80000000n && v <= 0x7fffffffn && this.proto === 1) {
      if (v >= 0n && v <= 0xffn) return this.write(Buffer.from([BININT1, Number(v)]));
      if (v >= 0n && v <= 0xffffn) {
        const b = Buffer.alloc(3);
        b[0] = BININT2;
        b.writeUInt16LE(Number(v), 1);
        return this.write(b);
      }
      const b = Buffer.alloc(5);
      b[0] = BININT;
      b.writeInt32LE(Number(v), 1);
      return this.write(b);
    }
    return this.write(Buffer.from(`${v >= -0x80000000n && v <= 0x7fffffffn ? "I" : "L"}${v}${v >= -0x80000000n && v <= 0x7fffffffn ? "" : "L"}\n`, "latin1"));
  }

  save_float(x) {
    if (this.proto >= 1) {
      const b = Buffer.alloc(9);
      b[0] = BINFLOAT;
      b.writeDoubleBE(x, 1);
      return this.write(b);
    }
    return this.write(Buffer.from(`F${_float_repr17(x)}\n`, "latin1"));
  }

  save_str(s) {
    const data = _utf8_surrogatepass(s);
    const n = data.length;
    if (this.proto >= 4 && n <= 0xff) {
      this.write(Buffer.from([SHORT_BINUNICODE, n]));
      this.write(data);
    } else if (n <= 0xffffffff) {
      const hdr = Buffer.alloc(5);
      hdr[0] = BINUNICODE;
      hdr.writeUInt32LE(n, 1);
      if (this.proto >= 4 && n >= FRAME_SIZE_TARGET) this.framer.write_large_bytes(hdr, data);
      else {
        this.write(hdr);
        this.write(data);
      }
    } else {
      const hdr = Buffer.alloc(9);
      hdr[0] = BINUNICODE8;
      hdr.writeBigUInt64LE(BigInt(n), 1);
      this.framer.write_large_bytes(hdr, data);
    }
    this.memoize(`s:${s}`);
  }

  save_bytes(b) {
    const data = Buffer.from(b.buffer, b.byteOffset, b.byteLength);
    const n = data.length;
    if (this.proto < 3) {
      // protocol < 3: bytes become a reduce call; not needed here
      throw new PicklingError("bytes require protocol >= 3");
    }
    if (n <= 0xff) {
      this.write(Buffer.from([SHORT_BINBYTES, n]));
      this.write(data);
    } else if (n <= 0xffffffff) {
      const hdr = Buffer.alloc(5);
      hdr[0] = BINBYTES;
      hdr.writeUInt32LE(n, 1);
      if (this.proto >= 4 && n >= FRAME_SIZE_TARGET) this.framer.write_large_bytes(hdr, data);
      else {
        this.write(hdr);
        this.write(data);
      }
    } else {
      const hdr = Buffer.alloc(9);
      hdr[0] = BINBYTES8;
      hdr.writeBigUInt64LE(BigInt(n), 1);
      this.framer.write_large_bytes(hdr, data);
    }
    this.memoize(b);
  }

  /** ``complex.__reduce__`` -> ``REDUCE(builtins.complex, (real, imag))``. */
  save_complex(c) {
    this.save_global("builtins", "complex");
    this.save(PyTuple.from_iterable([new PyFloat(c.real), new PyFloat(c.imag)]));
    this.byte(REDUCE);
    this.memoize(c);
  }

  save_bytearray(b) {
    if (this.proto < 5) {
      // bytearray(b'...') via REDUCE in older protocols
      this.save_global("builtins", "bytearray");
      this.save(PyTuple.from_iterable([new Uint8Array(b)]));
      this.byte(REDUCE);
      this.memoize(b);
      return;
    }
    const data = Buffer.from(b.buffer, b.byteOffset, b.byteLength);
    const hdr = Buffer.alloc(9);
    hdr[0] = BYTEARRAY8;
    hdr.writeBigUInt64LE(BigInt(data.length), 1);
    if (data.length >= FRAME_SIZE_TARGET) this.framer.write_large_bytes(hdr, data);
    else {
      this.write(hdr);
      this.write(data);
    }
    this.memoize(b);
  }

  save_tuple(t) {
    const n = t.length;
    if (n === 0) {
      if (this.proto) return this.byte(EMPTY_TUPLE);
      this.byte(MARK);
      return this.byte(TUPLE);
    }
    if (n <= 3 && this.proto >= 2) {
      for (const x of t) this.save(x);
      if (this.memo.has(t)) {
        // recursive tuple: elements referenced the tuple itself
        for (let i = 0; i < n; i++) this.byte(POP);
        return this.get(this.memo.get(t));
      }
      this.byte(n === 1 ? TUPLE1 : n === 2 ? TUPLE2 : TUPLE3);
      return this.memoize(t);
    }
    this.byte(MARK);
    for (const x of t) this.save(x);
    if (this.memo.has(t)) {
      this.byte(this.proto ? POP_MARK : POP);
      return this.get(this.memo.get(t));
    }
    this.byte(TUPLE);
    this.memoize(t);
  }

  save_list(lst) {
    this.byte(this.proto ? EMPTY_LIST : MARK);
    if (!this.proto) this.byte(LIST);
    this.memoize(lst);
    this._batch_appends(lst);
  }
  _batch_appends(items) {
    if (this.proto === 0) {
      for (const x of items) {
        this.save(x);
        this.byte(APPEND);
      }
      return;
    }
    // Mirrors ``batch_list_exact`` in Modules/_pickle.c: a one-element list
    // uses APPEND; otherwise MARK ... APPENDS in batches of BATCHSIZE (a
    // trailing single element still gets its own MARK/APPENDS batch).
    if (items.length === 0) return;
    if (items.length === 1) {
      this.save(items[0]);
      this.byte(APPEND);
      return;
    }
    let total = 0;
    do {
      let this_batch = 0;
      this.byte(MARK);
      while (total < items.length) {
        this.save(items[total]);
        total++;
        if (++this_batch === BATCHSIZE) break;
      }
      this.byte(APPENDS);
    } while (total < items.length);
  }

  save_dict(d) {
    this.byte(this.proto ? EMPTY_DICT : MARK);
    if (!this.proto) this.byte(DICT);
    this.memoize(d);
    const items = d instanceof Map ? [...d.entries()] : is_plain_object(d) ? Object.entries(d) : d.items();
    if (this.proto === 0) {
      for (const [k, v] of items) {
        this.save(k);
        this.save(v);
        this.byte(SETITEM);
      }
      return;
    }
    // Mirrors ``batch_dict_exact``: one item -> SETITEM; otherwise
    // MARK ... SETITEMS batches, looping while a batch was *full* -- so a dict
    // of exactly k*BATCHSIZE items ends with an empty MARK/SETITEMS pair.
    if (items.length === 0) return;
    if (items.length === 1) {
      this.save(items[0][0]);
      this.save(items[0][1]);
      this.byte(SETITEM);
      return;
    }
    let pos = 0;
    let i;
    do {
      i = 0;
      this.byte(MARK);
      while (pos < items.length) {
        const [k, v] = items[pos++];
        this.save(k);
        this.save(v);
        if (++i === BATCHSIZE) break;
      }
      this.byte(SETITEMS);
    } while (i === BATCHSIZE);
  }

  save_set(s) {
    if (this.proto < 4) {
      this.save_global("builtins", "set");
      this.save(PyTuple.from_iterable([[...s]]));
      this.byte(REDUCE);
      return this.memoize(s);
    }
    this.byte(EMPTY_SET);
    this.memoize(s);
    const items = [...s];
    if (items.length === 0) return;
    // Mirrors ``save_set``: loop while the batch was full (same trailing
    // empty-batch behaviour as dicts).
    let pos = 0;
    let i;
    do {
      i = 0;
      this.byte(MARK);
      while (pos < items.length) {
        this.save(items[pos++]);
        if (++i === BATCHSIZE) break;
      }
      this.byte(ADDITEMS);
    } while (i === BATCHSIZE);
  }

  save_frozenset(s) {
    if (this.proto < 4) {
      this.save_global("builtins", "frozenset");
      this.save(PyTuple.from_iterable([[...s]]));
      this.byte(REDUCE);
      return this.memoize(s);
    }
    this.byte(MARK);
    for (const x of s) this.save(x);
    if (this.memo.has(s)) {
      this.byte(POP_MARK);
      return this.get(this.memo.get(s));
    }
    this.byte(FROZENSET);
    this.memoize(s);
  }

  save_global(module, name) {
    // CPython memoizes the class object itself (``self.memoize(obj)``), so a
    // global referenced twice is a BINGET the second time.
    const key = `g:${module}.${name}`;
    if (this.memo.has(key)) return this.get(this.memo.get(key));
    if (this.opts.collect_globals) this.opts.collect_globals.push([module, name]);
    if (this.proto >= 4) {
      this.save(module); // via ``save`` so a repeated module string is a BINGET
      this.save(name);
      this.byte(STACK_GLOBAL);
    } else {
      this.write(Buffer.from(`c${module}\n${name}\n`, "latin1"));
    }
    this.memoize(key);
  }

  /** ``save_reduce``: ``(callable, args[, state])`` -> global, args, REDUCE, memoize[, state, BUILD]. */
  save_reduce(obj) {
    const [callable, args, state] = obj.__reduce__();
    const [module, name] = callable;
    this.save_global(module, name);
    this.save(PyTuple.from_iterable(args));
    this.byte(REDUCE);
    this.memoize(this._memo_key(obj) ?? obj);
    if (state !== undefined && state !== null) {
      this.save(state);
      this.byte(BUILD);
    }
  }

  /**
   * ``ndarray.__reduce_ex__`` (numpy >= 2):
   *
   * - protocol 5 (contiguous arrays):
   *   ``numpy._core.numeric._frombuffer(bytearray(data), dtype, shape, order)``
   * - older protocols:
   *   ``numpy._core.multiarray._reconstruct(ndarray, (0,), b'b')`` + BUILD with
   *   state ``(1, shape, dtype, is_fortran, data)``.
   */
  save_ndarray(a) {
    const raw = a._raw_bytes();
    const shape = PyTuple.from_iterable(a.shape);
    const dtype = _np_dtype_of(a);
    if (this.proto >= 5) {
      this.save_global("numpy._core.numeric", "_frombuffer");
      this.save(PyTuple.from_iterable([new PyByteArray(raw), dtype, shape, a.fortran_order ? "F" : "C"]));
      this.byte(REDUCE);
      this.memoize(a);
      return;
    }
    this.save_global("numpy._core.multiarray", "_reconstruct");
    // ``b'b'`` is one interned constant in numpy, so a second array memo-hits it
    this.save(PyTuple.from_iterable([new _GlobalRef("numpy", "ndarray"), PyTuple.from_iterable([0]), _NP_RECONSTRUCT_B]));
    this.byte(REDUCE);
    this.memoize(a);
    this.save(PyTuple.from_iterable([1, shape, dtype, a.fortran_order, Buffer.from(raw)]));
    this.byte(BUILD);
  }
}

/** CPython ``encode_long``: two's-complement little-endian, minimal length (b"" for 0). */
function _encode_long(v) {
  if (v === 0n) return Buffer.alloc(0);
  let nbytes = 1;
  // find minimal byte length such that the value fits in signed nbytes
  for (;;) {
    const lo = -(1n << BigInt(8 * nbytes - 1));
    const hi = (1n << BigInt(8 * nbytes - 1)) - 1n;
    if (v >= lo && v <= hi) break;
    nbytes += 1;
  }
  let u = v < 0n ? (1n << BigInt(8 * nbytes)) + v : v;
  const out = Buffer.alloc(nbytes);
  for (let i = 0; i < nbytes; i++) {
    out[i] = Number(u & 0xffn);
    u >>= 8n;
  }
  return out;
}

function _decode_long(buf) {
  if (buf.length === 0) return 0n;
  let v = 0n;
  for (let i = buf.length - 1; i >= 0; i--) v = (v << 8n) | BigInt(buf[i]);
  if (buf[buf.length - 1] & 0x80) v -= 1n << BigInt(8 * buf.length);
  return v;
}

/** UTF-8 with ``surrogatepass`` (lone surrogates encoded as 3-byte sequences, like CPython). */
function _utf8_surrogatepass(s) {
  // Fast path: valid UTF-16 -> Buffer.from handles it; lone surrogates would be
  // replaced with U+FFFD, so check for them first.
  if (!/[\ud800-\udfff]/.test(s)) return Buffer.from(s, "utf8");
  const out = [];
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff && i + 1 < s.length) {
      const d = s.charCodeAt(i + 1);
      if (d >= 0xdc00 && d <= 0xdfff) {
        const cp = 0x10000 + ((c - 0xd800) << 10) + (d - 0xdc00);
        out.push(0xf0 | (cp >> 18), 0x80 | ((cp >> 12) & 0x3f), 0x80 | ((cp >> 6) & 0x3f), 0x80 | (cp & 0x3f));
        i++;
        continue;
      }
    }
    if (c < 0x80) out.push(c);
    else if (c < 0x800) out.push(0xc0 | (c >> 6), 0x80 | (c & 0x3f));
    else out.push(0xe0 | (c >> 12), 0x80 | ((c >> 6) & 0x3f), 0x80 | (c & 0x3f));
  }
  return Buffer.from(out);
}

function _utf8_decode_surrogatepass(buf) {
  // Decode UTF-8 allowing encoded surrogates (ED A0..BF xx).
  let s = "";
  let i = 0;
  const n = buf.length;
  let simple = true;
  for (let k = 0; k < n; k++) {
    if (buf[k] === 0xed && k + 1 < n && buf[k + 1] >= 0xa0) {
      simple = false;
      break;
    }
  }
  if (simple) return buf.toString("utf8");
  while (i < n) {
    const b = buf[i];
    if (b < 0x80) {
      s += String.fromCharCode(b);
      i += 1;
    } else if ((b & 0xe0) === 0xc0) {
      s += String.fromCharCode(((b & 0x1f) << 6) | (buf[i + 1] & 0x3f));
      i += 2;
    } else if ((b & 0xf0) === 0xe0) {
      s += String.fromCharCode(((b & 0x0f) << 12) | ((buf[i + 1] & 0x3f) << 6) | (buf[i + 2] & 0x3f));
      i += 3;
    } else {
      const cp = ((b & 0x07) << 18) | ((buf[i + 1] & 0x3f) << 12) | ((buf[i + 2] & 0x3f) << 6) | (buf[i + 3] & 0x3f);
      s += String.fromCodePoint(cp);
      i += 4;
    }
  }
  return s;
}

function _float_repr17(x) {
  if (Number.isNaN(x)) return "nan";
  if (x === Infinity) return "inf";
  if (x === -Infinity) return "-inf";
  return x.toPrecision(17).replace(/\.?0+$/, "");
}

/**
 * ``pickle.dumps(obj, protocol=None)``
 * @param {any} obj
 * @param {{protocol?: number|null}} [opts]
 * @returns {Buffer}
 */
export function dumps(obj, opts = {}) {
  let protocol = opts.protocol ?? null;
  if (protocol === null || protocol === undefined) protocol = DEFAULT_PROTOCOL;
  if (protocol < 0) protocol = HIGHEST_PROTOCOL;
  if (protocol > HIGHEST_PROTOCOL) throw new ValueError(`pickle protocol must be <= ${HIGHEST_PROTOCOL}`);
  return new _Pickler(protocol, opts).dump(obj);
}

// --------------------------------------------------------------------------
// Reader
// --------------------------------------------------------------------------

/**
 * Globals the reader knows how to reconstruct. Anything else raises
 * ``UnpicklingError`` (laila-js never executes arbitrary Python callables).
 */
const _SAFE_GLOBALS = new Map([
  ["builtins.complex", (args) => new Complex(Number(args[0] ?? 0), Number(args[1] ?? 0))],
  ["builtins.set", (args) => new Set(args[0] ?? [])],
  ["builtins.frozenset", (args) => new PyFrozenSet(args[0] ?? [])],
  ["builtins.bytearray", (args) => new PyByteArray(args[0] ?? [])],
  ["builtins.bytes", (args) => Buffer.from(args[0] ?? [])],
  ["builtins.list", (args) => [...(args[0] ?? [])]],
  ["builtins.tuple", (args) => PyTuple.from_iterable(args[0] ?? [])],
  ["builtins.dict", (args) => dict_from_entries(args[0] ? dict_items(args[0]) : [])],
  ["builtins.int", (args) => (args.length ? BigInt(args[0]) : 0)],
  ["builtins.float", (args) => (args.length ? Number(args[0]) : 0)],
  ["builtins.str", (args) => (args.length ? String(args[0]) : "")],
  ["builtins.bool", (args) => Boolean(args[0])],
  ["collections.OrderedDict", (args) => new Map(args[0] ? args[0].map((kv) => [kv[0], kv[1]]) : [])],
  ["_codecs.encode", (args) => (args[1] === "latin1" || args[1] === "latin-1" ? Buffer.from(args[0], "latin1") : Buffer.from(args[0], "utf8"))],
  ["__builtin__.set", (args) => new Set(args[0] ?? [])],
  // numpy (both the numpy 1.x ``numpy.core`` and numpy 2.x ``numpy._core`` paths)
  ["numpy.dtype", (args) => new _NpDtype(args[0], args[1], args[2])],
  ["numpy.ndarray", () => new _GlobalRef("numpy", "ndarray")],
  ["numpy._core.numeric._frombuffer", _np_frombuffer],
  ["numpy.core.numeric._frombuffer", _np_frombuffer],
  ["numpy._core.multiarray._frombuffer", _np_frombuffer],
  ["numpy._core.multiarray._reconstruct", _np_reconstruct],
  ["numpy.core.multiarray._reconstruct", _np_reconstruct],
]);

const _MARK = Symbol("pickle.MARK");

class _Unpickler {
  constructor(data, opts) {
    this.buf = data;
    this.pos = 0;
    this.stack = [];
    this.memo = new Map();
    this.proto = 0;
    this.opts = opts;
  }
  read(n) {
    if (this.pos + n > this.buf.length) throw new UnpicklingError("pickle data was truncated");
    const out = this.buf.subarray(this.pos, this.pos + n);
    this.pos += n;
    return out;
  }
  readline() {
    const end = this.buf.indexOf(0x0a, this.pos);
    if (end < 0) throw new UnpicklingError("pickle data was truncated");
    const line = this.buf.subarray(this.pos, end);
    this.pos = end + 1;
    return line.toString("latin1");
  }
  pop_mark() {
    let i = this.stack.length - 1;
    while (i >= 0 && this.stack[i] !== _MARK) i--;
    if (i < 0) throw new UnpicklingError("could not find MARK");
    const items = this.stack.splice(i + 1);
    this.stack.pop(); // the mark
    return items;
  }
  push(v) {
    this.stack.push(v);
  }
  pop() {
    if (this.stack.length === 0) throw new UnpicklingError("unpickling stack underflow");
    return this.stack.pop();
  }
  top() {
    return this.stack[this.stack.length - 1];
  }
  int_from_bigint(v) {
    return v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : v;
  }

  load() {
    for (;;) {
      if (this.pos >= this.buf.length) throw new UnpicklingError("pickle exhausted before seeing STOP");
      const op = this.buf[this.pos++];
      switch (op) {
        case PROTO:
          this.proto = this.read(1)[0];
          if (this.proto > HIGHEST_PROTOCOL) throw new ValueError(`unsupported pickle protocol: ${this.proto}`);
          break;
        case FRAME:
          this.read(8); // frame length; we read the whole buffer anyway
          break;
        case STOP:
          return this.pop();
        case NONE:
          this.push(null);
          break;
        case NEWTRUE:
          this.push(true);
          break;
        case NEWFALSE:
          this.push(false);
          break;
        case BININT1:
          this.push(this.read(1)[0]);
          break;
        case BININT2:
          this.push(this.read(2).readUInt16LE(0));
          break;
        case BININT:
          this.push(this.read(4).readInt32LE(0));
          break;
        case LONG1: {
          const n = this.read(1)[0];
          this.push(this.int_from_bigint(_decode_long(this.read(n))));
          break;
        }
        case LONG4: {
          const n = this.read(4).readInt32LE(0);
          this.push(this.int_from_bigint(_decode_long(this.read(n))));
          break;
        }
        case INT: {
          const line = this.readline();
          if (line === "01") this.push(true);
          else if (line === "00") this.push(false);
          else this.push(this.int_from_bigint(BigInt(line)));
          break;
        }
        case LONG: {
          let line = this.readline();
          if (line.endsWith("L")) line = line.slice(0, -1);
          this.push(this.int_from_bigint(BigInt(line || "0")));
          break;
        }
        case BINFLOAT: {
          const x = this.read(8).readDoubleBE(0);
          this.push(is_integral(x) ? new PyFloat(x) : x);
          break;
        }
        case FLOAT: {
          const x = parseFloat(this.readline());
          this.push(is_integral(x) ? new PyFloat(x) : x);
          break;
        }
        case SHORT_BINUNICODE: {
          const n = this.read(1)[0];
          this.push(_utf8_decode_surrogatepass(this.read(n)));
          break;
        }
        case BINUNICODE: {
          const n = this.read(4).readUInt32LE(0);
          this.push(_utf8_decode_surrogatepass(this.read(n)));
          break;
        }
        case BINUNICODE8: {
          const n = Number(this.read(8).readBigUInt64LE(0));
          this.push(_utf8_decode_surrogatepass(this.read(n)));
          break;
        }
        case UNICODE:
          this.push(this.readline().replace(/\\u([0-9a-fA-F]{4})/g, (_m, h) => String.fromCharCode(parseInt(h, 16))));
          break;
        case SHORT_BINBYTES: {
          const n = this.read(1)[0];
          this.push(Buffer.from(this.read(n)));
          break;
        }
        case BINBYTES: {
          const n = this.read(4).readUInt32LE(0);
          this.push(Buffer.from(this.read(n)));
          break;
        }
        case BINBYTES8: {
          const n = Number(this.read(8).readBigUInt64LE(0));
          this.push(Buffer.from(this.read(n)));
          break;
        }
        case BYTEARRAY8: {
          const n = Number(this.read(8).readBigUInt64LE(0));
          this.push(new PyByteArray(this.read(n)));
          break;
        }
        case SHORT_BINSTRING: {
          const n = this.read(1)[0];
          this.push(this.read(n).toString("latin1"));
          break;
        }
        case BINSTRING: {
          const n = this.read(4).readInt32LE(0);
          this.push(this.read(n).toString("latin1"));
          break;
        }
        case STRING: {
          const line = this.readline();
          this.push(line.slice(1, -1));
          break;
        }
        case EMPTY_TUPLE:
          this.push(PyTuple.from_iterable([]));
          break;
        case TUPLE1:
          this.push(PyTuple.from_iterable([this.pop()]));
          break;
        case TUPLE2: {
          const b = this.pop();
          const a = this.pop();
          this.push(PyTuple.from_iterable([a, b]));
          break;
        }
        case TUPLE3: {
          const c = this.pop();
          const b = this.pop();
          const a = this.pop();
          this.push(PyTuple.from_iterable([a, b, c]));
          break;
        }
        case TUPLE:
          this.push(PyTuple.from_iterable(this.pop_mark()));
          break;
        case MARK:
          this.push(_MARK);
          break;
        case EMPTY_LIST:
          this.push([]);
          break;
        case LIST:
          this.push(this.pop_mark());
          break;
        case APPEND: {
          const v = this.pop();
          this.top().push(v);
          break;
        }
        case APPENDS: {
          const items = this.pop_mark();
          const lst = this.top();
          for (const x of items) lst.push(x);
          break;
        }
        case EMPTY_DICT:
          this.push(new _DictBuilder());
          break;
        case DICT: {
          const items = this.pop_mark();
          const d = new _DictBuilder();
          for (let i = 0; i < items.length; i += 2) d.set(items[i], items[i + 1]);
          this.push(d);
          break;
        }
        case SETITEM: {
          const v = this.pop();
          const k = this.pop();
          this.top().set(k, v);
          break;
        }
        case SETITEMS: {
          const items = this.pop_mark();
          const d = this.top();
          for (let i = 0; i < items.length; i += 2) d.set(items[i], items[i + 1]);
          break;
        }
        case EMPTY_SET:
          this.push(new Set());
          break;
        case ADDITEMS: {
          const items = this.pop_mark();
          const s = this.top();
          for (const x of items) s.add(x);
          break;
        }
        case FROZENSET:
          this.push(new PyFrozenSet(this.pop_mark()));
          break;
        case MEMOIZE:
          this.memo.set(this.memo.size, this.top());
          break;
        case BINPUT:
          this.memo.set(this.read(1)[0], this.top());
          break;
        case LONG_BINPUT:
          this.memo.set(this.read(4).readUInt32LE(0), this.top());
          break;
        case PUT:
          this.memo.set(parseInt(this.readline(), 10), this.top());
          break;
        case BINGET:
          this.push(this._memo_get(this.read(1)[0]));
          break;
        case LONG_BINGET:
          this.push(this._memo_get(this.read(4).readUInt32LE(0)));
          break;
        case GET:
          this.push(this._memo_get(parseInt(this.readline(), 10)));
          break;
        case POP:
          this.pop();
          break;
        case POP_MARK:
          this.pop_mark();
          break;
        case DUP:
          this.push(this.top());
          break;
        case GLOBAL: {
          const module = this.readline();
          const name = this.readline();
          this.push(this._find_class(module, name));
          break;
        }
        case STACK_GLOBAL: {
          const name = this.pop();
          const module = this.pop();
          this.push(this._find_class(module, name));
          break;
        }
        case REDUCE: {
          const args = this.pop();
          const fn = this.pop();
          this.push(fn([...args]));
          break;
        }
        case NEWOBJ: {
          const args = this.pop();
          const cls = this.pop();
          this.push(cls([...args]));
          break;
        }
        case NEWOBJ_EX: {
          const kwargs = this.pop();
          const args = this.pop();
          const cls = this.pop();
          this.push(cls([...args], kwargs));
          break;
        }
        case BUILD: {
          const state = this.pop();
          const inst = this.top();
          if (inst && typeof inst.__setstate__ === "function") inst.__setstate__(_finalize(state));
          else if (inst && typeof inst === "object") Object.assign(inst, _finalize(state));
          break;
        }
        case EXT1:
        case EXT2:
        case EXT4:
        case PERSID:
        case BINPERSID:
        case INST:
        case OBJ:
        case NEXT_BUFFER:
        case READONLY_BUFFER:
          throw new UnpicklingError(`unsupported pickle opcode 0x${op.toString(16)}`);
        default:
          throw new UnpicklingError(`invalid load key, '${String.fromCharCode(op)}'.`);
      }
    }
  }
  _memo_get(i) {
    if (!this.memo.has(i)) throw new UnpicklingError(`Memo value not found at index ${i}`);
    return this.memo.get(i);
  }
  _find_class(module, name) {
    const key = `${module}.${name}`;
    const fn = _SAFE_GLOBALS.get(key) ?? this.opts.globals?.get?.(key);
    if (!fn) throw new UnpicklingError(`global '${module}.${name}' is forbidden: laila-js cannot reconstruct arbitrary Python objects`);
    return fn;
  }
}

/**
 * Dict under construction: keys may be any hashable (tuples, numbers, ...).
 * Finalised into a plain Object or Map by rule D1 once the pickle is done.
 */
class _DictBuilder {
  constructor() {
    this.entries = [];
    this.index = new Map();
  }
  set(k, v) {
    const key = _hash_key(k);
    if (this.index.has(key)) this.entries[this.index.get(key)][1] = v;
    else {
      this.index.set(key, this.entries.length);
      this.entries.push([k, v]);
    }
  }
}

function _hash_key(k) {
  if (typeof k === "string") return `s:${k}`;
  if (typeof k === "number" || typeof k === "bigint") return `n:${k}`;
  if (k === true) return "n:1";
  if (k === false) return "n:0";
  if (k === null) return "None";
  if (k instanceof PyFloat) return `n:${k.valueOf()}`;
  if (k instanceof PyTuple) return `t:(${k.map(_hash_key).join(",")})`;
  if (k instanceof Uint8Array) return `b:${Buffer.from(k).toString("latin1")}`;
  return k; // identity
}

function _finalize(v) {
  if (v instanceof _DictBuilder) return dict_from_entries(v.entries.map(([k, x]) => [_finalize(k), _finalize(x)]));
  if (Array.isArray(v)) {
    for (let i = 0; i < v.length; i++) {
      const f = _finalize(v[i]);
      if (f !== v[i]) {
        if (v instanceof PyTuple) {
          // tuples are frozen; rebuild
          return PyTuple.from_iterable(v.map(_finalize));
        }
        v[i] = f;
      }
    }
    return v;
  }
  if (v instanceof Set) {
    const items = [...v];
    let changed = false;
    const out = items.map((x) => {
      const f = _finalize(x);
      if (f !== x) changed = true;
      return f;
    });
    if (!changed) return v;
    return v instanceof PyFrozenSet ? new PyFrozenSet(out) : new Set(out);
  }
  return v;
}

/**
 * ``pickle.loads(data)``
 * @param {Uint8Array} data
 * @param {{globals?: Map<string, Function>}} [opts]
 */
export function loads(data, opts = {}) {
  if (!(data instanceof Uint8Array)) throw new PyTypeError("a bytes-like object is required, not '" + typeof data + "'");
  const buf = Buffer.isBuffer(data) ? data : Buffer.from(data.buffer, data.byteOffset, data.byteLength);
  const u = new _Unpickler(buf, opts);
  const result = u.load();
  return _finalize_deep(result, new Set());
}

function _finalize_deep(v, seen) {
  if (v === null || typeof v !== "object") return v;
  if (seen.has(v)) return v;
  seen.add(v);
  if (v instanceof _DictBuilder) {
    const entries = v.entries.map(([k, x]) => [_finalize_deep(k, seen), _finalize_deep(x, seen)]);
    return dict_from_entries(entries);
  }
  if (v instanceof PyTuple) {
    let changed = false;
    const items = v.map((x) => {
      const f = _finalize_deep(x, seen);
      if (f !== x) changed = true;
      return f;
    });
    return changed ? PyTuple.from_iterable(items) : v;
  }
  if (Array.isArray(v)) {
    for (let i = 0; i < v.length; i++) v[i] = _finalize_deep(v[i], seen);
    return v;
  }
  if (v instanceof Set) {
    const items = [...v];
    let changed = false;
    const out = items.map((x) => {
      const f = _finalize_deep(x, seen);
      if (f !== x) changed = true;
      return f;
    });
    if (!changed) return v;
    return v instanceof PyFrozenSet ? new PyFrozenSet(out) : new Set(out);
  }
  return v;
}

/** The pickle protocol a buffer was written with (``None`` if it has no PROTO opcode). */
export function protocol_of(data) {
  return data.length >= 2 && data[0] === PROTO ? data[1] : null;
}
