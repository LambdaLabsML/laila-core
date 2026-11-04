//! CPython `pickle` -- protocol 5 writer (protocol 4 and the older binary
//! protocols as in `laila-js`) and protocol 0-5 reader, byte-exact with the
//! C `_pickle` module for the builtin types laila serialises: `None`, `bool`,
//! `int`, `float`, `str`, `bytes`, `bytearray`, `list`, `tuple`, `dict`,
//! `set`, `frozenset`, `complex` and `numpy.ndarray` (and nested
//! combinations).
//!
//! ```text
//! dumps(obj, protocol=None) -> bytes
//! loads(data) -> object
//! ```
//!
//! Writer rules mirrored from `Modules/_pickle.c` (`dumps(obj)` with the
//! default protocol):
//!   - PROTO 5, then FRAME framing: a frame is committed at the next opcode
//!     boundary (`_Pickler_OpcodeBoundary`, checked at the start of every
//!     `save`) once it holds >= 64 KiB (`FRAME_SIZE_TARGET`); frame headers
//!     are only emitted for frames of >= 4 bytes (`FRAME_SIZE_MIN`);
//!     bytes/str/bytearray payloads of >= 64 KiB are written outside frames
//!     ("large bytes").
//!   - ints: BININT1 / BININT2 / BININT / LONG1 / LONG4 by range.
//!   - str: SHORT_BINUNICODE / BINUNICODE / BINUNICODE8 (+ MEMOIZE)
//!   - bytes: SHORT_BINBYTES / BINBYTES / BINBYTES8 (+ MEMOIZE); bytearray: BYTEARRAY8
//!     (protocol 5) or a `builtins.bytearray` REDUCE (older protocols)
//!   - tuple: EMPTY_TUPLE | TUPLE1..3 | MARK ... TUPLE (+ MEMOIZE unless empty)
//!   - list: EMPTY_LIST MEMOIZE then APPEND / MARK ... APPENDS in batches of 1000
//!   - dict: EMPTY_DICT MEMOIZE then SETITEM / MARK ... SETITEMS in batches of 1000
//!   - set: EMPTY_SET MEMOIZE then MARK ... ADDITEMS in batches of 1000
//!   - frozenset: MARK ... FROZENSET MEMOIZE
//!   - memo hits: BINGET / LONG_BINGET
//!
//! Memoisation: containers, byte strings and arrays by identity (the `Arc`
//! pointer is Python's `id()`), `str` by *value* -- CPython would emit BINGET
//! for the same interned object, which is what repeated literals / dict keys
//! are (and the `""` singleton).
//!
//! Strings are UTF-8; a Python `str` holding a lone surrogate cannot exist in
//! a Rust `str`, so the `surrogatepass` paths of the JS port have no twin
//! here (`loads` of such a payload raises `UnicodeDecodeError`).

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;

use num_bigint::BigInt;
use num_traits::ToPrimitive;

use crate::_compat::datetime::{DateTime, TimeDelta, TimeZone};
use crate::_compat::ndarray::{Complex, NDArray};
use crate::python::errors::ExcKind;
use crate::python::object::{IntoArg, PyMap, PyObject, PySetInner};
use crate::python::stdlib::OrderedDict;
use crate::python::sync::RwLock;
use crate::python::{Arg, LailaError, Result};

pub const HIGHEST_PROTOCOL: i32 = 5;
pub const DEFAULT_PROTOCOL: i32 = 5;

// opcodes
const MARK: u8 = 0x28; // (
const STOP: u8 = 0x2e; // .
const POP: u8 = 0x30; // 0
const POP_MARK: u8 = 0x31; // 1
const DUP: u8 = 0x32; // 2
const FLOAT: u8 = 0x46; // F
const INT: u8 = 0x49; // I
const BININT: u8 = 0x4a; // J
const BININT1: u8 = 0x4b; // K
const LONG: u8 = 0x4c; // L
const BININT2: u8 = 0x4d; // M
const NONE: u8 = 0x4e; // N
const PERSID: u8 = 0x50; // P
const BINPERSID: u8 = 0x51; // Q
const REDUCE: u8 = 0x52; // R
const STRING: u8 = 0x53; // S
const BINSTRING: u8 = 0x54; // T
const SHORT_BINSTRING: u8 = 0x55; // U
const UNICODE: u8 = 0x56; // V
const BINUNICODE: u8 = 0x58; // X
const APPEND: u8 = 0x61; // a
const BUILD: u8 = 0x62; // b
const GLOBAL: u8 = 0x63; // c
const DICT: u8 = 0x64; // d
const EMPTY_DICT: u8 = 0x7d; // }
const APPENDS: u8 = 0x65; // e
const GET: u8 = 0x67; // g
const BINGET: u8 = 0x68; // h
const INST: u8 = 0x69; // i
const LONG_BINGET: u8 = 0x6a; // j
const LIST: u8 = 0x6c; // l
const EMPTY_LIST: u8 = 0x5d; // ]
const OBJ: u8 = 0x6f; // o
const PUT: u8 = 0x70; // p
const BINPUT: u8 = 0x71; // q
const LONG_BINPUT: u8 = 0x72; // r
const SETITEM: u8 = 0x73; // s
const TUPLE: u8 = 0x74; // t
const EMPTY_TUPLE: u8 = 0x29; // )
const SETITEMS: u8 = 0x75; // u
const BINFLOAT: u8 = 0x47; // G
const PROTO: u8 = 0x80;
const NEWOBJ: u8 = 0x81;
const EXT1: u8 = 0x82;
const EXT2: u8 = 0x83;
const EXT4: u8 = 0x84;
const TUPLE1: u8 = 0x85;
const TUPLE2: u8 = 0x86;
const TUPLE3: u8 = 0x87;
const NEWTRUE: u8 = 0x88;
const NEWFALSE: u8 = 0x89;
const LONG1: u8 = 0x8a;
const LONG4: u8 = 0x8b;
const BINBYTES: u8 = 0x42; // B
const SHORT_BINBYTES: u8 = 0x43; // C
const SHORT_BINUNICODE: u8 = 0x8c;
const BINUNICODE8: u8 = 0x8d;
const BINBYTES8: u8 = 0x8e;
const EMPTY_SET: u8 = 0x8f;
const ADDITEMS: u8 = 0x90;
const FROZENSET: u8 = 0x91;
const NEWOBJ_EX: u8 = 0x92;
const STACK_GLOBAL: u8 = 0x93;
const MEMOIZE: u8 = 0x94;
const FRAME: u8 = 0x95;
const BYTEARRAY8: u8 = 0x96;
const NEXT_BUFFER: u8 = 0x97;
const READONLY_BUFFER: u8 = 0x98;

const FRAME_SIZE_MIN: usize = 4;
const FRAME_SIZE_TARGET: usize = 64 * 1024;
const BATCHSIZE: usize = 1000;

/// `pickle.PicklingError(msg)`.
pub fn pickling_error(msg: impl Into<String>) -> LailaError {
    LailaError::new(ExcKind::PicklingError, msg)
}

/// `pickle.UnpicklingError(msg)`.
pub fn unpickling_error(msg: impl Into<String>) -> LailaError {
    LailaError::new(ExcKind::UnpicklingError, msg)
}

// --------------------------------------------------------------------------
// numpy interop (`ndarray.__reduce_ex__` for numpy >= 2.0)
// --------------------------------------------------------------------------

/// A reference to a Python global (`module.name`) -- pickled via `save_global`;
/// what STACK_GLOBAL / GLOBAL push on the unpickling stack.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct GlobalRef {
    module: String,
    name: String,
}
crate::py_object!(GlobalRef);

/// `numpy.dtype` as it travels through a pickle:
/// `dtype.__reduce__()` -> `(numpy.dtype, (name, False, True), state)` with
/// `state = (3, byteorder, None, None, None, -1, -1, 0)` for the builtin
/// scalar dtypes laila's `NDArray` supports.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct NpDtype {
    /// The descr without its byte-order character (`"f8"`, `"u1"`).
    name: String,
    /// `'<'`, `'>'`, `'|'` or `'='`.
    byteorder: char,
}
crate::py_object!(NpDtype);

const NP_DTYPE_SIZE1: [&str; 3] = ["b1", "i1", "u1"];

impl NpDtype {
    /// `numpy.dtype(name, align=False, copy=True)` -- `name` may carry a byte order.
    fn new(name: &str) -> NpDtype {
        let first = name.chars().next();
        match first {
            Some(c @ ('<' | '>' | '|' | '=')) => NpDtype {
                name: name[1..].into(),
                byteorder: c,
            },
            _ => NpDtype {
                name: name.into(),
                byteorder: if NP_DTYPE_SIZE1.contains(&name) {
                    '|'
                } else {
                    '<'
                },
            },
        }
    }
    /// From a canonical `NDArray.dtype` (`"<f8"`, `"|u1"`, `">i4"`).
    fn of(arr: &NDArray) -> NpDtype {
        NpDtype::new(&arr.dtype)
    }
    /// numpy `dtype.str` (`"<f8"`, `"|u1"`).
    fn descr(&self) -> String {
        let bo = if self.byteorder == '=' {
            '<'
        } else {
            self.byteorder
        };
        format!("{bo}{}", self.name)
    }
    /// `dtype.__setstate__(state)`: `state[1]` is the byte order.
    fn __setstate__(&self, state: &Arg) -> NpDtype {
        let mut out = self.clone();
        if let Arg::Tuple(t) = state {
            if let Some(Arg::Str(bo)) = t.get(1) {
                if let Some(c) = bo.chars().next() {
                    out.byteorder = c;
                }
            }
        }
        out
    }
}

/// `numpy._core.multiarray._reconstruct(ndarray, (0,), b'b')` -> placeholder
/// filled by BUILD (the array lives in the state tuple).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct NdPlaceholder {
    id: usize,
}
crate::py_object!(NdPlaceholder);

/// Any `str`, `numpy.dtype` or dtype-like argument -> canonical descr.
fn dtype_str_of(a: &Arg) -> Result<String> {
    if let Some(d) = a.downcast_ref::<NpDtype>() {
        return Ok(d.descr());
    }
    match a {
        Arg::Str(s) => Ok(s.to_string()),
        other => Err(unpickling_error(format!(
            "numpy dtype expected, got {}",
            other.type_name()
        ))),
    }
}

fn shape_of(a: &Arg) -> Result<Vec<usize>> {
    a.__iter__()?
        .iter()
        .map(|d| {
            d.as_i64()
                .filter(|v| *v >= 0)
                .map(|v| v as usize)
                .ok_or_else(|| {
                    unpickling_error("ndarray shape must be a tuple of non-negative ints")
                })
        })
        .collect()
}

fn buffer_of(a: &Arg) -> Result<Vec<u8>> {
    a.as_bytes_like().ok_or_else(|| {
        unpickling_error(format!(
            "a bytes-like object is required, not '{}'",
            a.type_name()
        ))
    })
}

/// `numpy._core.numeric._frombuffer(buffer, dtype, shape, order)`
fn np_frombuffer(args: &[Arg]) -> Result<Arg> {
    if args.len() < 4 {
        return Err(unpickling_error("_frombuffer() takes 4 arguments"));
    }
    let data = buffer_of(&args[0])?;
    let dt = dtype_str_of(&args[1])?;
    let shape = shape_of(&args[2])?;
    let order = args[3].as_str().unwrap_or("C");
    Ok(NDArray::from_bytes(&dt, shape, order == "F", data)?.into_arg())
}

/// `ndarray.__setstate__((version, shape, dtype, is_fortran, rawdata))`.
fn np_setstate(state: &Arg) -> Result<Arg> {
    let items = state.__iter__()?;
    if items.len() < 5 {
        return Err(unpickling_error("ndarray.__setstate__ expects a 5-tuple"));
    }
    let shape = shape_of(&items[1])?;
    let dt = dtype_str_of(&items[2])?;
    let is_fortran = items[3].__bool__();
    let raw = buffer_of(&items[4])?;
    Ok(NDArray::from_bytes(&dt, shape, is_fortran, raw)?.into_arg())
}

// --------------------------------------------------------------------------
// Writer
// --------------------------------------------------------------------------

/// `_pickle.c` output buffer with FRAME bookkeeping.
#[derive(Default)]
struct Framer {
    /// Committed output.
    out: Vec<u8>,
    /// The frame currently being filled.
    frame: Vec<u8>,
}

impl Framer {
    fn write(&mut self, buf: &[u8]) {
        self.frame.extend_from_slice(buf);
    }
    /// C `_Pickler_CommitFrame`: emit the FRAME header when the frame is big enough.
    fn commit_frame(&mut self) {
        if self.frame.is_empty() {
            return;
        }
        if self.frame.len() >= FRAME_SIZE_MIN {
            self.out.push(FRAME);
            self.out
                .extend_from_slice(&(self.frame.len() as u64).to_le_bytes());
        }
        self.out.append(&mut self.frame);
    }
    /// C `_Pickler_OpcodeBoundary`: a frame that reached the target size is
    /// committed before the next opcode.
    fn opcode_boundary(&mut self) {
        if self.frame.len() >= FRAME_SIZE_TARGET {
            self.commit_frame();
        }
    }
    /// Large bytes/str payloads are written outside any frame.
    fn write_large_bytes(&mut self, header: &[u8], payload: &[u8]) {
        self.commit_frame();
        self.out.extend_from_slice(header);
        self.out.extend_from_slice(payload);
    }
    fn finish(mut self) -> Vec<u8> {
        self.commit_frame();
        self.out
    }
}

/// Memo table key: Python's `id()` for shared objects, the value for `str`
/// (interned literals), and the singletons CPython / numpy memoize once.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum MemoKey {
    Ptr(usize),
    Str(Arc<str>),
    /// `module.name` -- CPython memoizes the class object itself.
    Global(String),
    /// Builtin numpy dtypes are singletons (`np.dtype('f4') is np.dtype('f4')`).
    Dtype(String),
    /// The interned `b'b'` constant of `ndarray.__reduce__`.
    ReconstructB,
}

fn ptr_key<T: ?Sized>(a: &Arc<T>) -> MemoKey {
    MemoKey::Ptr(Arc::as_ptr(a) as *const () as usize)
}

/// Memo lookup key of an object (`None` for the atoms CPython never memoizes).
fn memo_key(obj: &Arg) -> Option<MemoKey> {
    Some(match obj {
        Arg::Str(s) => MemoKey::Str(s.clone()),
        Arg::Bytes(b) => ptr_key(b),
        Arg::ByteArray(b) => ptr_key(b),
        Arg::List(l) => ptr_key(l),
        Arg::Tuple(t) => ptr_key(t),
        Arg::Dict(d) => ptr_key(d),
        Arg::Set(s) => ptr_key(s),
        Arg::FrozenSet(s) => ptr_key(s),
        Arg::NDArray(a) => ptr_key(a),
        Arg::Object(o) => ptr_key(o),
        _ => return None,
    })
}

/// Something the writer can `save` that is not (or not only) an `Arg`:
/// the temporaries of the numpy / complex / bytearray reduce tuples. They
/// are fresh objects in CPython, so they consume a memo slot but are never
/// looked up -- modelling them as `Item`s (rather than as temporary `Arg`s
/// memoized by address) keeps the identity memo sound.
enum Item<'a> {
    Arg(Arg),
    Tuple(Vec<Item<'a>>),
    Bytes(&'a [u8]),
    ByteArray(&'a [u8]),
    Dtype(&'a NpDtype),
    Global(&'static str, &'static str),
    ReconstructB,
}

struct Pickler {
    proto: i32,
    framer: Framer,
    memo: BTreeMap<MemoKey, usize>,
    memo_len: usize,
}

impl Pickler {
    fn new(proto: i32) -> Self {
        Pickler {
            proto,
            framer: Framer::default(),
            memo: BTreeMap::new(),
            memo_len: 0,
        }
    }

    fn write(&mut self, b: &[u8]) {
        if self.proto >= 4 {
            self.framer.write(b);
        } else {
            self.framer.out.extend_from_slice(b);
        }
    }
    fn byte(&mut self, op: u8) {
        self.write(&[op]);
    }
    /// Record a memo slot (`key` is `None` for temporaries that are never referenced again).
    fn memoize(&mut self, key: Option<MemoKey>) {
        if let Some(k) = key {
            self.memo.insert(k, self.memo_len);
        }
        self.memo_len += 1;
        if self.proto >= 4 {
            self.byte(MEMOIZE);
        } else {
            self.put(self.memo_len - 1);
        }
    }
    fn put(&mut self, idx: usize) {
        if self.proto == 0 {
            self.write(format!("p{idx}\n").as_bytes());
        } else if idx < 256 {
            self.write(&[BINPUT, idx as u8]);
        } else {
            let mut b = vec![LONG_BINPUT];
            b.extend_from_slice(&(idx as u32).to_le_bytes());
            self.write(&b);
        }
    }
    fn get(&mut self, idx: usize) {
        if self.proto == 0 {
            self.write(format!("g{idx}\n").as_bytes());
        } else if idx < 256 {
            self.write(&[BINGET, idx as u8]);
        } else {
            let mut b = vec![LONG_BINGET];
            b.extend_from_slice(&(idx as u32).to_le_bytes());
            self.write(&b);
        }
    }

    fn dump(mut self, obj: &Arg) -> Result<Vec<u8>> {
        if self.proto >= 2 {
            self.write(&[PROTO, self.proto as u8]);
        }
        if self.proto >= 4 {
            self.framer.commit_frame(); // PROTO is outside the first frame
        }
        self.save(obj)?;
        self.byte(STOP);
        Ok(self.framer.finish())
    }

    /// C `_Pickler_OpcodeBoundary`: every `save` (of a real object) starts here.
    fn opcode_boundary(&mut self) {
        if self.proto >= 4 {
            self.framer.opcode_boundary();
        }
    }

    fn save(&mut self, obj: &Arg) -> Result<()> {
        self.opcode_boundary();
        // Memo lookup (identity for objects, value for strings)
        let key = memo_key(obj);
        if let Some(k) = &key {
            if let Some(&idx) = self.memo.get(k) {
                self.get(idx);
                return Ok(());
            }
        }
        match obj {
            Arg::None => self.byte(NONE),
            Arg::Bool(true) => {
                if self.proto >= 2 {
                    self.byte(NEWTRUE);
                } else {
                    self.write(b"I01\n");
                }
            }
            Arg::Bool(false) => {
                if self.proto >= 2 {
                    self.byte(NEWFALSE);
                } else {
                    self.write(b"I00\n");
                }
            }
            Arg::Int(i) => self.save_int(*i),
            Arg::Long(l) => self.save_long(l),
            Arg::Float(f) => self.save_float(*f),
            Arg::Str(s) => self.save_str(s),
            Arg::Bytes(b) => self.save_bytes(b, key)?,
            Arg::ByteArray(b) => {
                let data = b.read().clone();
                self.save_bytearray(&data, key)?;
            }
            Arg::Tuple(t) => self.save_tuple(t, key)?,
            Arg::List(l) => {
                let items = l.read().clone();
                self.save_list(&items, key)?;
            }
            Arg::Dict(d) => {
                let items: Vec<(Arg, Arg)> = d
                    .read()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                self.save_dict(&items, key)?;
            }
            Arg::Set(s) => {
                let items: Vec<Arg> = s.read().iter().cloned().collect();
                self.save_set(&items, key)?;
            }
            Arg::FrozenSet(s) => {
                let items: Vec<Arg> = s.iter().cloned().collect();
                self.save_frozenset(&items, key)?;
            }
            Arg::NDArray(a) => self.save_ndarray(a, key)?,
            Arg::Object(o) if o.as_any().is::<Complex>() => {
                let c = *o.as_any().downcast_ref::<Complex>().unwrap();
                self.save_complex(c, key)?;
            }
            Arg::Object(o) if o.as_any().is::<OrderedDict>() => {
                let od = o.as_any().downcast_ref::<OrderedDict>().unwrap();
                self.save_ordered_dict(&od.pairs(), key)?;
            }
            Arg::Object(o) if o.as_any().is::<DateTime>() => {
                let dt = *o.as_any().downcast_ref::<DateTime>().unwrap();
                self.save_datetime(&dt, key)?;
            }
            Arg::Object(o) if o.as_any().is::<TimeDelta>() => {
                let td = *o.as_any().downcast_ref::<TimeDelta>().unwrap();
                self.save_timedelta(&td, key)?;
            }
            Arg::Object(o) if o.as_any().is::<TimeZone>() => {
                let tz = *o.as_any().downcast_ref::<TimeZone>().unwrap();
                self.save_timezone(&tz, key)?;
            }
            Arg::Callable(_) => {
                // CPython pickles functions by reference; a closure has no
                // `__module__` / `__qualname__` to look up.
                return Err(pickling_error(
                    "Can't pickle <function <lambda>>: attribute lookup <lambda> on __main__ failed",
                ));
            }
            other => {
                return Err(pickling_error(format!(
                    "Can't pickle {}: objects without a Python counterpart are not picklable",
                    other.type_name()
                )))
            }
        }
        Ok(())
    }

    fn save_item(&mut self, it: &Item<'_>) -> Result<()> {
        self.opcode_boundary();
        match it {
            Item::Arg(a) => self.save(a),
            Item::Tuple(items) => self.save_tuple_items(items),
            Item::Bytes(b) => self.save_bytes(b, None),
            Item::ByteArray(b) => self.save_bytearray(b, None),
            Item::Dtype(d) => self.save_dtype(d),
            Item::Global(m, n) => self.save_global(m, n),
            Item::ReconstructB => {
                if let Some(&idx) = self.memo.get(&MemoKey::ReconstructB) {
                    self.get(idx);
                    return Ok(());
                }
                self.save_bytes(b"b", Some(MemoKey::ReconstructB))
            }
        }
    }

    fn save_int(&mut self, v: i64) {
        let fits_i32 = (i32::MIN as i64..=i32::MAX as i64).contains(&v);
        if self.proto >= 2 {
            if (0..=0xff).contains(&v) {
                return self.write(&[BININT1, v as u8]);
            }
            if (0..=0xffff).contains(&v) {
                let mut b = vec![BININT2];
                b.extend_from_slice(&(v as u16).to_le_bytes());
                return self.write(&b);
            }
            if fits_i32 {
                let mut b = vec![BININT];
                b.extend_from_slice(&(v as i32).to_le_bytes());
                return self.write(&b);
            }
            return self.save_long_bytes(&encode_long(&BigInt::from(v)));
        }
        // protocol 0/1: INT with decimal repr (protocol 1 keeps the binary forms)
        if fits_i32 && self.proto == 1 {
            if (0..=0xff).contains(&v) {
                return self.write(&[BININT1, v as u8]);
            }
            if (0..=0xffff).contains(&v) {
                let mut b = vec![BININT2];
                b.extend_from_slice(&(v as u16).to_le_bytes());
                return self.write(&b);
            }
            let mut b = vec![BININT];
            b.extend_from_slice(&(v as i32).to_le_bytes());
            return self.write(&b);
        }
        if fits_i32 {
            self.write(format!("I{v}\n").as_bytes());
        } else {
            self.write(format!("L{v}L\n").as_bytes());
        }
    }

    fn save_long(&mut self, v: &BigInt) {
        if let Some(i) = v.to_i64() {
            return self.save_int(i);
        }
        if self.proto >= 2 {
            return self.save_long_bytes(&encode_long(v));
        }
        self.write(format!("L{v}L\n").as_bytes());
    }

    /// LONG1 / LONG4 with the two's-complement payload.
    fn save_long_bytes(&mut self, enc: &[u8]) {
        if enc.len() < 256 {
            self.write(&[LONG1, enc.len() as u8]);
        } else {
            let mut hdr = vec![LONG4];
            hdr.extend_from_slice(&(enc.len() as i32).to_le_bytes());
            self.write(&hdr);
        }
        self.write(enc);
    }

    fn save_float(&mut self, x: f64) {
        if self.proto >= 1 {
            let mut b = vec![BINFLOAT];
            b.extend_from_slice(&x.to_bits().to_be_bytes());
            return self.write(&b);
        }
        self.write(format!("F{}\n", crate::python::pyrepr::float_repr(x)).as_bytes());
    }

    fn save_str(&mut self, s: &Arc<str>) {
        let data = s.as_bytes();
        let n = data.len();
        if self.proto == 0 {
            self.byte(UNICODE);
            self.write(&raw_unicode_escape(s));
            self.byte(b'\n');
        } else if self.proto >= 4 && n <= 0xff {
            self.write(&[SHORT_BINUNICODE, n as u8]);
            self.write(data);
        } else if n as u64 <= 0xffff_ffff {
            let mut hdr = vec![BINUNICODE];
            hdr.extend_from_slice(&(n as u32).to_le_bytes());
            if self.proto >= 4 && n >= FRAME_SIZE_TARGET {
                self.framer.write_large_bytes(&hdr, data);
            } else {
                self.write(&hdr);
                self.write(data);
            }
        } else {
            let mut hdr = vec![BINUNICODE8];
            hdr.extend_from_slice(&(n as u64).to_le_bytes());
            self.framer.write_large_bytes(&hdr, data);
        }
        self.memoize(Some(MemoKey::Str(s.clone())));
    }

    fn save_bytes(&mut self, data: &[u8], key: Option<MemoKey>) -> Result<()> {
        let n = data.len();
        if self.proto < 3 {
            // protocol < 3: bytes become a reduce call; not needed here
            return Err(pickling_error("bytes require protocol >= 3"));
        }
        if n <= 0xff {
            self.write(&[SHORT_BINBYTES, n as u8]);
            self.write(data);
        } else if n as u64 <= 0xffff_ffff {
            let mut hdr = vec![BINBYTES];
            hdr.extend_from_slice(&(n as u32).to_le_bytes());
            if self.proto >= 4 && n >= FRAME_SIZE_TARGET {
                self.framer.write_large_bytes(&hdr, data);
            } else {
                self.write(&hdr);
                self.write(data);
            }
        } else {
            let mut hdr = vec![BINBYTES8];
            hdr.extend_from_slice(&(n as u64).to_le_bytes());
            self.framer.write_large_bytes(&hdr, data);
        }
        self.memoize(key);
        Ok(())
    }

    /// `complex.__reduce__` -> `REDUCE(builtins.complex, (real, imag))`.
    /// `OrderedDict.__reduce__` -> `(OrderedDict, (), None, None,
    /// iter(items))`: `save_reduce` emits the class, an empty args tuple,
    /// REDUCE, the memo entry, then the items through `_batch_setitems`.
    fn save_ordered_dict(&mut self, items: &[(Arg, Arg)], key: Option<MemoKey>) -> Result<()> {
        self.save_global("collections", "OrderedDict")?;
        self.save_tuple_items(&[])?;
        self.byte(REDUCE);
        self.memoize(key);
        self.batch_setitems(items)
    }

    /// `datetime.datetime.__reduce__`: `(datetime, (bytes10[, tzinfo]))`.
    fn save_datetime(&mut self, dt: &DateTime, key: Option<MemoKey>) -> Result<()> {
        self.save_global("datetime", "datetime")?;
        let y = dt.year as u16;
        let us = dt.microsecond;
        let state = [
            (y >> 8) as u8,
            (y & 0xff) as u8,
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
            (us >> 16) as u8,
            ((us >> 8) & 0xff) as u8,
            (us & 0xff) as u8,
        ];
        match dt.tzinfo {
            None => self.save_tuple_items(&[Item::Bytes(&state)])?,
            Some(tz) => self.save_tuple_items(&[Item::Bytes(&state), Item::Arg(tz.into_arg())])?,
        }
        self.byte(REDUCE);
        self.memoize(key);
        Ok(())
    }

    /// `datetime.timedelta.__reduce__`: `(timedelta, (days, seconds, microseconds))`.
    fn save_timedelta(&mut self, td: &TimeDelta, key: Option<MemoKey>) -> Result<()> {
        self.save_global("datetime", "timedelta")?;
        self.save_tuple_items(&[
            Item::Arg(Arg::Int(td.days as i64)),
            Item::Arg(Arg::Int(td.seconds as i64)),
            Item::Arg(Arg::Int(td.microseconds as i64)),
        ])?;
        self.byte(REDUCE);
        self.memoize(key);
        Ok(())
    }

    /// `datetime.timezone.__reduce__`: `(timezone, (offset,))`.
    fn save_timezone(&mut self, tz: &TimeZone, key: Option<MemoKey>) -> Result<()> {
        self.save_global("datetime", "timezone")?;
        self.save_tuple_items(&[Item::Arg(tz.utcoffset(Arg::None).into_arg())])?;
        self.byte(REDUCE);
        self.memoize(key);
        Ok(())
    }

    fn save_complex(&mut self, c: Complex, key: Option<MemoKey>) -> Result<()> {
        self.save_global("builtins", "complex")?;
        self.save_tuple_items(&[Item::Arg(Arg::Float(c.real)), Item::Arg(Arg::Float(c.imag))])?;
        self.byte(REDUCE);
        self.memoize(key);
        Ok(())
    }

    fn save_bytearray(&mut self, data: &[u8], key: Option<MemoKey>) -> Result<()> {
        if self.proto < 5 {
            // bytearray(b'...') via REDUCE in older protocols
            self.save_global("builtins", "bytearray")?;
            self.save_tuple_items(&[Item::Bytes(data)])?;
            self.byte(REDUCE);
            self.memoize(key);
            return Ok(());
        }
        let mut hdr = vec![BYTEARRAY8];
        hdr.extend_from_slice(&(data.len() as u64).to_le_bytes());
        if data.len() >= FRAME_SIZE_TARGET {
            self.framer.write_large_bytes(&hdr, data);
        } else {
            self.write(&hdr);
            self.write(data);
        }
        self.memoize(key);
        Ok(())
    }

    fn save_tuple(&mut self, t: &Arc<[Arg]>, key: Option<MemoKey>) -> Result<()> {
        let n = t.len();
        if n == 0 {
            if self.proto > 0 {
                self.byte(EMPTY_TUPLE);
            } else {
                self.byte(MARK);
                self.byte(TUPLE);
            }
            return Ok(());
        }
        let memo_idx = |me: &Self| key.as_ref().and_then(|k| me.memo.get(k).copied());
        if n <= 3 && self.proto >= 2 {
            for x in t.iter() {
                self.save(x)?;
            }
            if let Some(idx) = memo_idx(self) {
                // recursive tuple: elements referenced the tuple itself
                for _ in 0..n {
                    self.byte(POP);
                }
                self.get(idx);
                return Ok(());
            }
            self.byte(match n {
                1 => TUPLE1,
                2 => TUPLE2,
                _ => TUPLE3,
            });
            self.memoize(key);
            return Ok(());
        }
        self.byte(MARK);
        for x in t.iter() {
            self.save(x)?;
        }
        if let Some(idx) = memo_idx(self) {
            self.byte(if self.proto > 0 { POP_MARK } else { POP });
            self.get(idx);
            return Ok(());
        }
        self.byte(TUPLE);
        self.memoize(key);
        Ok(())
    }

    /// A fresh (never shared) tuple of `Item`s -- the reduce-value tuples.
    fn save_tuple_items(&mut self, items: &[Item<'_>]) -> Result<()> {
        self.opcode_boundary();
        let n = items.len();
        if n == 0 {
            if self.proto > 0 {
                self.byte(EMPTY_TUPLE);
            } else {
                self.byte(MARK);
                self.byte(TUPLE);
            }
            return Ok(());
        }
        if n <= 3 && self.proto >= 2 {
            for x in items {
                self.save_item(x)?;
            }
            self.byte(match n {
                1 => TUPLE1,
                2 => TUPLE2,
                _ => TUPLE3,
            });
            self.memoize(None);
            return Ok(());
        }
        self.byte(MARK);
        for x in items {
            self.save_item(x)?;
        }
        self.byte(TUPLE);
        self.memoize(None);
        Ok(())
    }

    fn save_list(&mut self, items: &[Arg], key: Option<MemoKey>) -> Result<()> {
        if self.proto > 0 {
            self.byte(EMPTY_LIST);
        } else {
            self.byte(MARK);
            self.byte(LIST);
        }
        self.memoize(key);
        self.batch_appends(items)
    }

    fn batch_appends(&mut self, items: &[Arg]) -> Result<()> {
        if self.proto == 0 {
            for x in items {
                self.save(x)?;
                self.byte(APPEND);
            }
            return Ok(());
        }
        // Mirrors `batch_list_exact` in Modules/_pickle.c: a one-element list
        // uses APPEND; otherwise MARK ... APPENDS in batches of BATCHSIZE (a
        // trailing single element still gets its own MARK/APPENDS batch).
        if items.is_empty() {
            return Ok(());
        }
        if items.len() == 1 {
            self.save(&items[0])?;
            self.byte(APPEND);
            return Ok(());
        }
        let mut total = 0;
        loop {
            let mut this_batch = 0;
            self.byte(MARK);
            while total < items.len() {
                self.save(&items[total])?;
                total += 1;
                this_batch += 1;
                if this_batch == BATCHSIZE {
                    break;
                }
            }
            self.byte(APPENDS);
            if total >= items.len() {
                break;
            }
        }
        Ok(())
    }

    fn save_dict(&mut self, items: &[(Arg, Arg)], key: Option<MemoKey>) -> Result<()> {
        if self.proto > 0 {
            self.byte(EMPTY_DICT);
        } else {
            self.byte(MARK);
            self.byte(DICT);
        }
        self.memoize(key);
        self.batch_setitems(items)
    }

    /// `Pickler._batch_setitems`.
    fn batch_setitems(&mut self, items: &[(Arg, Arg)]) -> Result<()> {
        if self.proto == 0 {
            for (k, v) in items {
                self.save(k)?;
                self.save(v)?;
                self.byte(SETITEM);
            }
            return Ok(());
        }
        // Mirrors `batch_dict_exact`: one item -> SETITEM; otherwise
        // MARK ... SETITEMS batches, looping while a batch was *full* -- so a dict
        // of exactly k*BATCHSIZE items ends with an empty MARK/SETITEMS pair.
        if items.is_empty() {
            return Ok(());
        }
        if items.len() == 1 {
            self.save(&items[0].0)?;
            self.save(&items[0].1)?;
            self.byte(SETITEM);
            return Ok(());
        }
        let mut pos = 0;
        loop {
            let mut i = 0;
            self.byte(MARK);
            while pos < items.len() {
                let (k, v) = &items[pos];
                pos += 1;
                self.save(k)?;
                self.save(v)?;
                i += 1;
                if i == BATCHSIZE {
                    break;
                }
            }
            self.byte(SETITEMS);
            if i != BATCHSIZE {
                break;
            }
        }
        Ok(())
    }

    fn save_set(&mut self, items: &[Arg], key: Option<MemoKey>) -> Result<()> {
        if self.proto < 4 {
            self.save_global("builtins", "set")?;
            self.save_tuple_items(&[Item::Arg(items.to_vec().into_arg())])?;
            self.byte(REDUCE);
            self.memoize(key);
            return Ok(());
        }
        self.byte(EMPTY_SET);
        self.memoize(key);
        if items.is_empty() {
            return Ok(());
        }
        // Mirrors `save_set`: loop while the batch was full (same trailing
        // empty-batch behaviour as dicts).
        let mut pos = 0;
        loop {
            let mut i = 0;
            self.byte(MARK);
            while pos < items.len() {
                self.save(&items[pos])?;
                pos += 1;
                i += 1;
                if i == BATCHSIZE {
                    break;
                }
            }
            self.byte(ADDITEMS);
            if i != BATCHSIZE {
                break;
            }
        }
        Ok(())
    }

    fn save_frozenset(&mut self, items: &[Arg], key: Option<MemoKey>) -> Result<()> {
        if self.proto < 4 {
            self.save_global("builtins", "frozenset")?;
            self.save_tuple_items(&[Item::Arg(items.to_vec().into_arg())])?;
            self.byte(REDUCE);
            self.memoize(key);
            return Ok(());
        }
        self.byte(MARK);
        for x in items {
            self.save(x)?;
        }
        if let Some(idx) = key.as_ref().and_then(|k| self.memo.get(k).copied()) {
            self.byte(POP_MARK);
            self.get(idx);
            return Ok(());
        }
        self.byte(FROZENSET);
        self.memoize(key);
        Ok(())
    }

    fn save_global(&mut self, module: &str, name: &str) -> Result<()> {
        // CPython memoizes the class object itself (`self.memoize(obj)`), so a
        // global referenced twice is a BINGET the second time.
        self.opcode_boundary();
        let key = MemoKey::Global(format!("{module}.{name}"));
        if let Some(&idx) = self.memo.get(&key) {
            self.get(idx);
            return Ok(());
        }
        if self.proto >= 4 {
            self.save(&Arg::from(module))?; // via `save` so a repeated module string is a BINGET
            self.save(&Arg::from(name))?;
            self.byte(STACK_GLOBAL);
        } else {
            self.write(format!("c{module}\n{name}\n").as_bytes());
        }
        self.memoize(Some(key));
        Ok(())
    }

    /// `numpy.dtype.__reduce__`: global, args, REDUCE, memoize, state, BUILD.
    fn save_dtype(&mut self, d: &NpDtype) -> Result<()> {
        self.opcode_boundary();
        let key = MemoKey::Dtype(d.descr());
        if let Some(&idx) = self.memo.get(&key) {
            self.get(idx);
            return Ok(());
        }
        self.save_global("numpy", "dtype")?;
        self.save_tuple_items(&[
            Item::Arg(Arg::from(d.name.as_str())),
            Item::Arg(Arg::Bool(false)),
            Item::Arg(Arg::Bool(true)),
        ])?;
        self.byte(REDUCE);
        self.memoize(Some(key));
        self.save_tuple_items(&[
            Item::Arg(Arg::Int(3)),
            Item::Arg(Arg::Str(d.byteorder.to_string().into())),
            Item::Arg(Arg::None),
            Item::Arg(Arg::None),
            Item::Arg(Arg::None),
            Item::Arg(Arg::Int(-1)),
            Item::Arg(Arg::Int(-1)),
            Item::Arg(Arg::Int(0)),
        ])?;
        self.byte(BUILD);
        Ok(())
    }

    /// `ndarray.__reduce_ex__` (numpy >= 2):
    ///
    /// - protocol 5 (contiguous arrays):
    ///   `numpy._core.numeric._frombuffer(bytearray(data), dtype, shape, order)`
    /// - older protocols:
    ///   `numpy._core.multiarray._reconstruct(ndarray, (0,), b'b')` + BUILD with
    ///   state `(1, shape, dtype, is_fortran, data)`.
    fn save_ndarray(&mut self, a: &NDArray, key: Option<MemoKey>) -> Result<()> {
        let raw = a.raw_bytes();
        let shape = Item::Tuple(
            a.shape
                .iter()
                .map(|d| Item::Arg(Arg::Int(*d as i64)))
                .collect(),
        );
        let dtype = NpDtype::of(a);
        if self.proto >= 5 {
            self.save_global("numpy._core.numeric", "_frombuffer")?;
            self.save_tuple_items(&[
                Item::ByteArray(raw),
                Item::Dtype(&dtype),
                shape,
                Item::Arg(Arg::from(if a.fortran_order { "F" } else { "C" })),
            ])?;
            self.byte(REDUCE);
            self.memoize(key);
            return Ok(());
        }
        self.save_global("numpy._core.multiarray", "_reconstruct")?;
        // `b'b'` is one interned constant in numpy, so a second array memo-hits it
        self.save_tuple_items(&[
            Item::Global("numpy", "ndarray"),
            Item::Tuple(vec![Item::Arg(Arg::Int(0))]),
            Item::ReconstructB,
        ])?;
        self.byte(REDUCE);
        self.memoize(key);
        self.save_tuple_items(&[
            Item::Arg(Arg::Int(1)),
            shape,
            Item::Dtype(&dtype),
            Item::Arg(Arg::Bool(a.fortran_order)),
            Item::Bytes(raw),
        ])?;
        self.byte(BUILD);
        Ok(())
    }
}

/// CPython `encode_long`: two's-complement little-endian, minimal length (`b""` for 0).
fn encode_long(v: &BigInt) -> Vec<u8> {
    use num_traits::Zero;
    if v.is_zero() {
        return Vec::new();
    }
    v.to_signed_bytes_le()
}

fn decode_long(buf: &[u8]) -> BigInt {
    if buf.is_empty() {
        return BigInt::from(0);
    }
    BigInt::from_signed_bytes_le(buf)
}

/// `pickle.dumps(obj, protocol=None)` -- `None` means `DEFAULT_PROTOCOL`,
/// a negative protocol means `HIGHEST_PROTOCOL`.
pub fn dumps(obj: &Arg, protocol: Option<i32>) -> Result<Vec<u8>> {
    let mut protocol = protocol.unwrap_or(DEFAULT_PROTOCOL);
    if protocol < 0 {
        protocol = HIGHEST_PROTOCOL;
    }
    if protocol > HIGHEST_PROTOCOL {
        return Err(LailaError::value_error(format!(
            "pickle protocol must be <= {HIGHEST_PROTOCOL}"
        )));
    }
    Pickler::new(protocol).dump(obj)
}

// --------------------------------------------------------------------------
// Reader
// --------------------------------------------------------------------------

/// Globals the reader knows how to reconstruct. Anything else raises
/// `UnpicklingError` (laila never executes arbitrary Python callables).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Global {
    Complex,
    Set,
    FrozenSet,
    ByteArray,
    Bytes,
    List,
    Tuple,
    Dict,
    Int,
    Float,
    Str,
    Bool,
    OrderedDict,
    DateTime,
    TimeDelta,
    TimeZone,
    CodecsEncode,
    NpDtype,
    NpNdarray,
    NpFrombuffer,
    NpReconstruct,
}

fn safe_global(module: &str, name: &str) -> Option<Global> {
    Some(match (module, name) {
        ("builtins", "complex") => Global::Complex,
        ("builtins", "set") | ("__builtin__", "set") => Global::Set,
        ("builtins", "frozenset") => Global::FrozenSet,
        ("builtins", "bytearray") => Global::ByteArray,
        ("builtins", "bytes") => Global::Bytes,
        ("builtins", "list") => Global::List,
        ("builtins", "tuple") => Global::Tuple,
        ("builtins", "dict") => Global::Dict,
        ("builtins", "int") => Global::Int,
        ("builtins", "float") => Global::Float,
        ("builtins", "str") => Global::Str,
        ("builtins", "bool") => Global::Bool,
        ("collections", "OrderedDict") => Global::OrderedDict,
        ("datetime", "datetime") => Global::DateTime,
        ("datetime", "timedelta") => Global::TimeDelta,
        ("datetime", "timezone") => Global::TimeZone,
        ("_codecs", "encode") => Global::CodecsEncode,
        // numpy (both the numpy 1.x `numpy.core` and numpy 2.x `numpy._core` paths)
        ("numpy", "dtype") => Global::NpDtype,
        ("numpy", "ndarray") => Global::NpNdarray,
        ("numpy._core.numeric", "_frombuffer")
        | ("numpy.core.numeric", "_frombuffer")
        | ("numpy._core.multiarray", "_frombuffer") => Global::NpFrombuffer,
        ("numpy._core.multiarray", "_reconstruct") | ("numpy.core.multiarray", "_reconstruct") => {
            Global::NpReconstruct
        }
        _ => return None,
    })
}

enum StackItem {
    Mark,
    Obj(Arg),
}

fn build_set(items: Vec<Arg>) -> Result<PySetInner> {
    let mut out = PySetInner::default();
    for v in items {
        v.__hash__()?;
        out.insert(v);
    }
    Ok(out)
}

fn new_dict() -> Arg {
    Arg::Dict(Arc::new(RwLock::new(PyMap::default())))
}

fn new_list(items: Vec<Arg>) -> Arg {
    Arg::List(Arc::new(RwLock::new(items)))
}

fn latin1(b: &[u8]) -> String {
    b.iter().map(|x| *x as char).collect()
}

fn utf8(b: &[u8]) -> Result<Arg> {
    match core::str::from_utf8(b) {
        Ok(s) => Ok(Arg::from(s)),
        Err(e) => {
            let p = e.valid_up_to();
            Err(LailaError::new(
                ExcKind::UnicodeDecodeError,
                format!(
                    "'utf-8' codec can't decode byte 0x{:02x} in position {p}: invalid start byte",
                    b[p]
                ),
            ))
        }
    }
}

/// `int(text)` for the INT / LONG text opcodes.
fn parse_int(text: &str) -> Result<Arg> {
    text.parse::<BigInt>().map(Arg::from_bigint).map_err(|_| {
        LailaError::value_error(format!("invalid literal for int() with base 10: '{text}'"))
    })
}

struct Unpickler<'a> {
    buf: &'a [u8],
    pos: usize,
    stack: Vec<StackItem>,
    memo: BTreeMap<usize, Arg>,
    proto: u8,
    next_placeholder: usize,
}

impl<'a> Unpickler<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Unpickler {
            buf,
            pos: 0,
            stack: Vec::new(),
            memo: BTreeMap::new(),
            proto: 0,
            next_placeholder: 0,
        }
    }

    fn read(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(unpickling_error("pickle data was truncated"));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }
    fn read_u8(&mut self) -> Result<usize> {
        Ok(self.read(1)?[0] as usize)
    }
    fn read_u16(&mut self) -> Result<usize> {
        let b = self.read(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]) as usize)
    }
    fn read_i32(&mut self) -> Result<i32> {
        let b = self.read(4)?;
        Ok(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn read_u32(&mut self) -> Result<usize> {
        let b = self.read(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
    }
    fn read_u64(&mut self) -> Result<usize> {
        let b = self.read(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        let n = u64::from_le_bytes(a);
        usize::try_from(n).map_err(|_| unpickling_error("pickle data was truncated"))
    }
    /// A text-opcode argument line (without the `\n`), decoded as latin-1.
    fn readline(&mut self) -> Result<String> {
        let rest = &self.buf[self.pos..];
        match rest.iter().position(|b| *b == b'\n') {
            None => Err(unpickling_error("pickle data was truncated")),
            Some(end) => {
                let line = latin1(&rest[..end]);
                self.pos += end + 1;
                Ok(line)
            }
        }
    }
    fn len_i32(&mut self, what: &str) -> Result<usize> {
        let n = self.read_i32()?;
        usize::try_from(n)
            .map_err(|_| unpickling_error(format!("{what} pickle has negative byte count")))
    }

    fn push(&mut self, v: Arg) {
        self.stack.push(StackItem::Obj(v));
    }
    fn pop(&mut self) -> Result<Arg> {
        match self.stack.last() {
            Some(StackItem::Obj(_)) => match self.stack.pop() {
                Some(StackItem::Obj(v)) => Ok(v),
                _ => unreachable!(),
            },
            _ => Err(unpickling_error("unpickling stack underflow")),
        }
    }
    fn top(&self) -> Result<&Arg> {
        match self.stack.last() {
            Some(StackItem::Obj(v)) => Ok(v),
            _ => Err(unpickling_error("unpickling stack underflow")),
        }
    }
    /// Replace the top of the stack (BUILD / `__setstate__`), also in the memo.
    fn replace_top(&mut self, old: &Arg, new: Arg) -> Result<()> {
        for v in self.memo.values_mut() {
            if v.is(old) {
                *v = new.clone();
            }
        }
        self.pop()?;
        self.push(new);
        Ok(())
    }
    fn pop_mark(&mut self) -> Result<Vec<Arg>> {
        let i = self
            .stack
            .iter()
            .rposition(|it| matches!(it, StackItem::Mark))
            .ok_or_else(|| unpickling_error("could not find MARK"))?;
        let items = self
            .stack
            .drain(i + 1..)
            .map(|it| match it {
                StackItem::Obj(v) => v,
                StackItem::Mark => unreachable!(),
            })
            .collect();
        self.stack.pop(); // the mark
        Ok(items)
    }
    fn memo_get(&self, i: usize) -> Result<Arg> {
        self.memo
            .get(&i)
            .cloned()
            .ok_or_else(|| unpickling_error(format!("Memo value not found at index {i}")))
    }
    fn memo_put(&mut self, i: usize) -> Result<()> {
        let v = self.top()?.clone();
        self.memo.insert(i, v);
        Ok(())
    }

    fn find_class(&self, module: &str, name: &str) -> Result<Arg> {
        match safe_global(module, name) {
            Some(_) => Ok(GlobalRef { module: module.into(), name: name.into() }.into_arg()),
            None => Err(unpickling_error(format!(
                "global '{module}.{name}' is forbidden: laila-rs cannot reconstruct arbitrary Python objects"
            ))),
        }
    }

    /// REDUCE / NEWOBJ: call a safe global with the argument tuple.
    fn call(&mut self, callable: &Arg, args: &Arg) -> Result<Arg> {
        let g = callable
            .downcast_ref::<GlobalRef>()
            .and_then(|g| safe_global(&g.module, &g.name))
            .ok_or_else(|| {
                unpickling_error(format!("'{}' object is not callable", callable.type_name()))
            })?;
        let args = args.__iter__()?;
        let first = args.first();
        let items = |a: Option<&Arg>| -> Result<Vec<Arg>> {
            match a {
                Some(v) => v.__iter__(),
                None => Ok(Vec::new()),
            }
        };
        Ok(match g {
            Global::Complex => {
                let num = |i: usize| -> Result<f64> {
                    match args.get(i) {
                        None => Ok(0.0),
                        Some(v) => v.as_f64().ok_or_else(|| {
                            LailaError::type_error(format!("complex() argument must be a number, not '{}'", v.type_name()))
                        }),
                    }
                };
                Complex::new(num(0)?, num(1)?).into_arg()
            }
            Global::DateTime => {
                let state = first.and_then(|v| v.as_bytes_like()).ok_or_else(|| {
                    unpickling_error("datetime.datetime() argument must be a 10-byte state")
                })?;
                if state.len() != 10 {
                    return Err(unpickling_error(
                        "datetime.datetime() argument must be a 10-byte state",
                    ));
                }
                let tzinfo = match args.get(1) {
                    None | Some(Arg::None) => None,
                    Some(v) => Some(v.downcast_ref::<TimeZone>().copied().ok_or_else(|| {
                        unpickling_error("tzinfo argument must be None or of a tzinfo subclass")
                    })?),
                };
                let year = ((state[0] as i64) << 8) | state[1] as i64;
                let us = ((state[7] as i64) << 16) | ((state[8] as i64) << 8) | state[9] as i64;
                DateTime::new(
                    year,
                    state[2] as i64,
                    state[3] as i64,
                    state[4] as i64,
                    state[5] as i64,
                    state[6] as i64,
                    us,
                    tzinfo,
                )?
                .into_arg()
            }
            Global::TimeDelta => {
                let int = |i: usize| -> Result<i64> {
                    match args.get(i) {
                        None => Ok(0),
                        Some(v) => v.as_i64().ok_or_else(|| {
                            unpickling_error("timedelta() arguments must be integers")
                        }),
                    }
                };
                TimeDelta::new(int(0)?, int(1)?, int(2)?)?.into_arg()
            }
            Global::TimeZone => {
                let offset = first.and_then(|v| v.downcast_ref::<TimeDelta>().copied()).ok_or_else(|| {
                    unpickling_error("timezone() argument must be a timedelta")
                })?;
                TimeZone::new(offset)?.into_arg()
            }
            Global::Set => Arg::Set(Arc::new(RwLock::new(build_set(items(first)?)?))),
            Global::FrozenSet => Arg::FrozenSet(Arc::new(build_set(items(first)?)?)),
            Global::ByteArray => crate::python::builtins::bytearray(crate::python::Args {
                positional: args.clone(),
                keywords: Vec::new(),
            })?,
            Global::Bytes => crate::python::builtins::bytes_(crate::python::Args {
                positional: args.clone(),
                keywords: Vec::new(),
            })?,
            Global::List => new_list(items(first)?),
            Global::Tuple => Arg::Tuple(items(first)?.into()),
            Global::Dict => crate::python::builtins::dict(crate::python::Args {
                positional: args.clone(),
                keywords: Vec::new(),
            })?,
            Global::Int => match first {
                None => Arg::Int(0),
                Some(v) => match v {
                    Arg::Bool(_) | Arg::Int(_) | Arg::Long(_) => Arg::from_bigint(v.as_bigint().unwrap()),
                    Arg::Float(f) => {
                        if f.is_nan() || f.is_infinite() {
                            return Err(LailaError::value_error("cannot convert float NaN or infinity to integer"));
                        }
                        Arg::from_bigint(BigInt::from(crate::_compat::fmath::trunc(*f) as i128))
                    }
                    Arg::Str(s) => parse_int(s.trim())?,
                    other => {
                        return Err(LailaError::type_error(format!(
                            "int() argument must be a string, a bytes-like object or a real number, not '{}'",
                            other.type_name()
                        )))
                    }
                },
            },
            Global::Float => match first {
                None => Arg::Float(0.0),
                Some(v) => match v {
                    Arg::Str(s) => Arg::Float(s.trim().parse::<f64>().map_err(|_| {
                        LailaError::value_error(format!("could not convert string to float: '{s}'"))
                    })?),
                    other => Arg::Float(other.as_f64().ok_or_else(|| {
                        LailaError::type_error(format!(
                            "float() argument must be a string or a real number, not '{}'",
                            other.type_name()
                        ))
                    })?),
                },
            },
            Global::Str => match first {
                None => Arg::from(""),
                Some(v) => Arg::Str(v.__str__().into()),
            },
            Global::Bool => Arg::Bool(first.map(|v| v.__bool__()).unwrap_or(false)),
            Global::OrderedDict => {
                let d = OrderedDict::empty();
                if first.is_some() {
                    for kv in items(first)? {
                        let pair = kv.__iter__()?;
                        if pair.len() != 2 {
                            return Err(LailaError::value_error("need more than 1 value to unpack"));
                        }
                        PyObject::__setitem__(&d, &pair[0], pair[1].clone())?;
                    }
                }
                d.into_arg()
            }
            Global::CodecsEncode => {
                let text = first
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| LailaError::type_error("encode() argument 1 must be str"))?;
                let enc = args.get(1).and_then(|v| v.as_str()).unwrap_or("utf-8");
                if enc == "latin1" || enc == "latin-1" {
                    let bytes: Option<Vec<u8>> =
                        text.chars().map(|c| u8::try_from(c as u32).ok()).collect();
                    match bytes {
                        Some(b) => Arg::Bytes(b.into()),
                        None => {
                            return Err(LailaError::new(
                                ExcKind::UnicodeEncodeError,
                                "'latin-1' codec can't encode character: ordinal not in range(256)",
                            ))
                        }
                    }
                } else {
                    Arg::Bytes(text.as_bytes().into())
                }
            }
            Global::NpDtype => {
                let name = first
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| unpickling_error("numpy.dtype() argument must be a str"))?;
                NpDtype::new(name).into_arg()
            }
            Global::NpNdarray => callable.clone(),
            Global::NpFrombuffer => np_frombuffer(&args)?,
            Global::NpReconstruct => {
                self.next_placeholder += 1;
                NdPlaceholder { id: self.next_placeholder }.into_arg()
            }
        })
    }

    fn load(&mut self) -> Result<Arg> {
        loop {
            if self.pos >= self.buf.len() {
                return Err(unpickling_error("pickle exhausted before seeing STOP"));
            }
            let op = self.buf[self.pos];
            self.pos += 1;
            match op {
                PROTO => {
                    self.proto = self.read(1)?[0];
                    if self.proto as i32 > HIGHEST_PROTOCOL {
                        return Err(LailaError::value_error(format!("unsupported pickle protocol: {}", self.proto)));
                    }
                }
                FRAME => {
                    self.read(8)?; // frame length; we read the whole buffer anyway
                }
                STOP => return self.pop(),
                NONE => self.push(Arg::None),
                NEWTRUE => self.push(Arg::Bool(true)),
                NEWFALSE => self.push(Arg::Bool(false)),
                BININT1 => {
                    let v = self.read_u8()?;
                    self.push(Arg::Int(v as i64));
                }
                BININT2 => {
                    let v = self.read_u16()?;
                    self.push(Arg::Int(v as i64));
                }
                BININT => {
                    let v = self.read_i32()?;
                    self.push(Arg::Int(v as i64));
                }
                LONG1 => {
                    let n = self.read_u8()?;
                    let v = decode_long(self.read(n)?);
                    self.push(Arg::from_bigint(v));
                }
                LONG4 => {
                    let n = self.len_i32("LONG")?;
                    let v = decode_long(self.read(n)?);
                    self.push(Arg::from_bigint(v));
                }
                INT => {
                    let line = self.readline()?;
                    if line == "01" {
                        self.push(Arg::Bool(true));
                    } else if line == "00" {
                        self.push(Arg::Bool(false));
                    } else {
                        self.push(parse_int(&line)?);
                    }
                }
                LONG => {
                    let line = self.readline()?;
                    let line = line.strip_suffix('L').unwrap_or(&line);
                    self.push(if line.is_empty() { Arg::Int(0) } else { parse_int(line)? });
                }
                BINFLOAT => {
                    let b = self.read(8)?;
                    let mut a = [0u8; 8];
                    a.copy_from_slice(b);
                    self.push(Arg::Float(f64::from_bits(u64::from_be_bytes(a))));
                }
                FLOAT => {
                    let line = self.readline()?;
                    let x = line
                        .trim()
                        .parse::<f64>()
                        .map_err(|_| LailaError::value_error(format!("could not convert string to float: '{line}'")))?;
                    self.push(Arg::Float(x));
                }
                SHORT_BINUNICODE => {
                    let n = self.read_u8()?;
                    let v = utf8(self.read(n)?)?;
                    self.push(v);
                }
                BINUNICODE => {
                    let n = self.read_u32()?;
                    let v = utf8(self.read(n)?)?;
                    self.push(v);
                }
                BINUNICODE8 => {
                    let n = self.read_u64()?;
                    let v = utf8(self.read(n)?)?;
                    self.push(v);
                }
                UNICODE => {
                    let line = self.readline()?;
                    self.push(Arg::Str(raw_unicode_unescape(&line).into()));
                }
                SHORT_BINBYTES => {
                    let n = self.read_u8()?;
                    let v = self.read(n)?;
                    self.push(Arg::Bytes(v.into()));
                }
                BINBYTES => {
                    let n = self.read_u32()?;
                    let v = self.read(n)?;
                    self.push(Arg::Bytes(v.into()));
                }
                BINBYTES8 => {
                    let n = self.read_u64()?;
                    let v = self.read(n)?;
                    self.push(Arg::Bytes(v.into()));
                }
                BYTEARRAY8 => {
                    let n = self.read_u64()?;
                    let v = self.read(n)?.to_vec();
                    self.push(Arg::ByteArray(Arc::new(RwLock::new(v))));
                }
                SHORT_BINSTRING => {
                    let n = self.read_u8()?;
                    let v = latin1(self.read(n)?);
                    self.push(Arg::Str(v.into()));
                }
                BINSTRING => {
                    let n = self.len_i32("BINSTRING")?;
                    let v = latin1(self.read(n)?);
                    self.push(Arg::Str(v.into()));
                }
                STRING => {
                    let line = self.readline()?;
                    let n = line.chars().count();
                    let inner: String = if n >= 2 { line.chars().skip(1).take(n - 2).collect() } else { String::new() };
                    self.push(Arg::Str(inner.into()));
                }
                EMPTY_TUPLE => self.push(Arg::Tuple(Vec::new().into())),
                TUPLE1 => {
                    let a = self.pop()?;
                    self.push(Arg::Tuple(vec![a].into()));
                }
                TUPLE2 => {
                    let b = self.pop()?;
                    let a = self.pop()?;
                    self.push(Arg::Tuple(vec![a, b].into()));
                }
                TUPLE3 => {
                    let c = self.pop()?;
                    let b = self.pop()?;
                    let a = self.pop()?;
                    self.push(Arg::Tuple(vec![a, b, c].into()));
                }
                TUPLE => {
                    let items = self.pop_mark()?;
                    self.push(Arg::Tuple(items.into()));
                }
                MARK => self.stack.push(StackItem::Mark),
                EMPTY_LIST => self.push(new_list(Vec::new())),
                LIST => {
                    let items = self.pop_mark()?;
                    self.push(new_list(items));
                }
                APPEND => {
                    let v = self.pop()?;
                    self.list_extend(vec![v])?;
                }
                APPENDS => {
                    let items = self.pop_mark()?;
                    self.list_extend(items)?;
                }
                EMPTY_DICT => self.push(new_dict()),
                DICT => {
                    let items = self.pop_mark()?;
                    let d = new_dict();
                    for kv in items.chunks(2) {
                        if kv.len() == 2 {
                            d.__setitem__(kv[0].clone(), kv[1].clone())?;
                        }
                    }
                    self.push(d);
                }
                SETITEM => {
                    let v = self.pop()?;
                    let k = self.pop()?;
                    self.dict_setitems(vec![k, v])?;
                }
                SETITEMS => {
                    let items = self.pop_mark()?;
                    self.dict_setitems(items)?;
                }
                EMPTY_SET => self.push(Arg::Set(Arc::new(RwLock::new(PySetInner::default())))),
                ADDITEMS => {
                    let items = self.pop_mark()?;
                    match self.top()? {
                        Arg::Set(s) => {
                            let s = s.clone();
                            let mut g = s.write();
                            for x in items {
                                x.__hash__()?;
                                g.insert(x);
                            }
                        }
                        other => {
                            return Err(unpickling_error(format!(
                                "ADDITEMS: expected a set on the stack, got {}",
                                other.type_name()
                            )))
                        }
                    }
                }
                FROZENSET => {
                    let items = self.pop_mark()?;
                    self.push(Arg::FrozenSet(Arc::new(build_set(items)?)));
                }
                MEMOIZE => {
                    let i = self.memo.len();
                    self.memo_put(i)?;
                }
                BINPUT => {
                    let i = self.read_u8()?;
                    self.memo_put(i)?;
                }
                LONG_BINPUT => {
                    let i = self.read_u32()?;
                    self.memo_put(i)?;
                }
                PUT => {
                    let line = self.readline()?;
                    let i = line
                        .trim()
                        .parse::<usize>()
                        .map_err(|_| LailaError::value_error(format!("invalid literal for int() with base 10: '{line}'")))?;
                    self.memo_put(i)?;
                }
                BINGET => {
                    let i = self.read_u8()?;
                    let v = self.memo_get(i)?;
                    self.push(v);
                }
                LONG_BINGET => {
                    let i = self.read_u32()?;
                    let v = self.memo_get(i)?;
                    self.push(v);
                }
                GET => {
                    let line = self.readline()?;
                    let i = line
                        .trim()
                        .parse::<usize>()
                        .map_err(|_| LailaError::value_error(format!("invalid literal for int() with base 10: '{line}'")))?;
                    let v = self.memo_get(i)?;
                    self.push(v);
                }
                POP => {
                    // pops a MARK too (CPython splits the object and mark stacks)
                    if self.stack.pop().is_none() {
                        return Err(unpickling_error("unpickling stack underflow"));
                    }
                }
                POP_MARK => {
                    self.pop_mark()?;
                }
                DUP => {
                    let v = self.top()?.clone();
                    self.push(v);
                }
                GLOBAL => {
                    let module = self.readline()?;
                    let name = self.readline()?;
                    let g = self.find_class(&module, &name)?;
                    self.push(g);
                }
                STACK_GLOBAL => {
                    let name = self.pop()?;
                    let module = self.pop()?;
                    let (module, name) = match (module.as_str(), name.as_str()) {
                        (Some(m), Some(n)) => (m.to_string(), n.to_string()),
                        _ => return Err(unpickling_error("STACK_GLOBAL requires str")),
                    };
                    let g = self.find_class(&module, &name)?;
                    self.push(g);
                }
                REDUCE | NEWOBJ => {
                    let args = self.pop()?;
                    let callable = self.pop()?;
                    let v = self.call(&callable, &args)?;
                    self.push(v);
                }
                NEWOBJ_EX => {
                    let _kwargs = self.pop()?;
                    let args = self.pop()?;
                    let callable = self.pop()?;
                    let v = self.call(&callable, &args)?;
                    self.push(v);
                }
                BUILD => {
                    let state = self.pop()?;
                    let inst = self.top()?.clone();
                    if inst.downcast_ref::<NdPlaceholder>().is_some() {
                        let built = np_setstate(&state)?;
                        self.replace_top(&inst, built)?;
                    } else if let Some(d) = inst.downcast_ref::<NpDtype>() {
                        let built = d.__setstate__(&state).into_arg();
                        self.replace_top(&inst, built)?;
                    } else if let (Arg::Dict(_), Arg::Dict(s)) = (&inst, &state) {
                        // default `__setstate__`: `inst.__dict__.update(state)`
                        let pairs: Vec<(Arg, Arg)> = s.read().iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                        for (k, v) in pairs {
                            inst.__setitem__(k, v)?;
                        }
                    }
                }
                PERSID | BINPERSID => {
                    return Err(unpickling_error(
                        "A load persistent id instruction was encountered, but no persistent_load function was specified.",
                    ))
                }
                NEXT_BUFFER | READONLY_BUFFER => {
                    return Err(unpickling_error(
                        "pickle stream refers to out-of-band data but no *buffers* argument was given",
                    ))
                }
                EXT1 | EXT2 | EXT4 | INST | OBJ => {
                    return Err(unpickling_error(format!("unsupported pickle opcode 0x{op:x}")))
                }
                _ => return Err(unpickling_error(format!("invalid load key, '{}'.", op as char))),
            }
        }
    }

    fn list_extend(&mut self, items: Vec<Arg>) -> Result<()> {
        match self.top()? {
            Arg::List(l) => {
                l.write().extend(items);
                Ok(())
            }
            other => Err(unpickling_error(format!(
                "APPEND: expected a list on the stack, got {}",
                other.type_name()
            ))),
        }
    }

    fn dict_setitems(&mut self, items: Vec<Arg>) -> Result<()> {
        let d = self.top()?.clone();
        // A reduced `dict` subclass (`OrderedDict`) takes its items the same way.
        if !matches!(&d, Arg::Dict(_))
            && !matches!(&d, Arg::Object(o) if o.__bases__().contains(&"dict"))
        {
            return Err(unpickling_error(format!(
                "SETITEMS: expected a dict on the stack, got {}",
                d.type_name()
            )));
        }
        for kv in items.chunks(2) {
            if kv.len() == 2 {
                d.__setitem__(kv[0].clone(), kv[1].clone())?;
            }
        }
        Ok(())
    }
}

/// CPython `raw_unicode_escape` (the UNICODE text opcode of protocol 0).
fn raw_unicode_escape(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    for ch in s.chars() {
        let c = ch as u32;
        if c >= 0x10000 {
            out.extend_from_slice(format!("\\U{c:08x}").as_bytes());
        } else if c >= 256 || matches!(ch, '\\' | '\0' | '\n' | '\r' | '\x1a') {
            out.extend_from_slice(format!("\\u{c:04x}").as_bytes());
        } else {
            out.push(c as u8);
        }
    }
    out
}

/// `raw-unicode-escape` decoding of a UNICODE text line (`\uXXXX`, `\UXXXXXXXX`).
fn raw_unicode_unescape(line: &str) -> String {
    let chars: Vec<char> = line.chars().collect();
    let mut out = String::with_capacity(chars.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\\' && i + 1 < chars.len() && (chars[i + 1] == 'u' || chars[i + 1] == 'U') {
            let width = if chars[i + 1] == 'u' { 4 } else { 8 };
            if i + 2 + width <= chars.len() {
                let hex: String = chars[i + 2..i + 2 + width].iter().collect();
                if let Some(c) = u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
                    out.push(c);
                    i += 2 + width;
                    continue;
                }
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

/// `pickle.loads(data)`.
pub fn loads(data: &[u8]) -> Result<Arg> {
    Unpickler::new(data).load()
}

/// The pickle protocol a buffer was written with (`None` if it has no PROTO opcode).
pub fn protocol_of(data: &[u8]) -> Option<u8> {
    if data.len() >= 2 && data[0] == PROTO {
        Some(data[1])
    } else {
        None
    }
}
