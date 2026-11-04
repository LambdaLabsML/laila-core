//! `msgpack` -- byte-exact port of msgpack-python 1.1's C `Packer` /
//! `Unpacker` for the call shapes laila uses:
//!
//! ```text
//! packb(o, use_bin_type=True, use_single_float=False, default=None) -> bytes
//! unpackb(packed, raw=False, strict_map_key=True, use_list=True) -> object
//! Unpacker(raw=False, strict_map_key=True, use_list=True); .feed(bytes); for obj in unpacker
//! ExtType(code, data)
//! ```
//!
//! Encoding rules (`msgpack/_packer.pyx`, `use_bin_type=True`,
//! `use_single_float=False`, `strict_types=False`, `datetime=False`):
//!   - None -> nil; bool -> true/false (checked before int)
//!   - int: positive fixint / uint8 / uint16 / uint32 / uint64, negative
//!     fixint / int8 / int16 / int32 / int64; anything outside is OverflowError
//!   - float -> float64 (0xcb)
//!   - str -> fixstr / str8 / str16 / str32 (UTF-8)
//!   - bytes / bytearray -> bin8 / bin16 / bin32
//!   - list / tuple -> fixarray / array16 / array32
//!   - dict -> fixmap / map16 / map32 (insertion order)
//!   - ExtType -> fixext / ext8 / ext16 / ext32
//!   - otherwise `default(obj)` once, else TypeError
//!   - nesting deeper than 512 raises ValueError("recursion limit exceeded.")
//!
//! Decoding (`msgpack/_unpacker.pyx`): ints beyond `i64` come back as Python
//! `int`s (`Arg::Long`), bin -> `bytes`, str -> `str` (or `bytes` with
//! `raw=True`), arrays -> `list` (or `tuple` with `use_list=False`), maps ->
//! `dict`, ext -> [`ExtType`].

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt;

use num_bigint::BigInt;
use num_traits::ToPrimitive;

use crate::python::errors::ExcKind;
use crate::python::object::{Arg, Callable, IntoArg};
use crate::python::{Args, LailaError, Result};

const DEFAULT_RECURSE_LIMIT: i32 = 511;

/// `msgpack.ExtType(code, data)` -- the extension-type namedtuple.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ExtType {
    pub code: i8,
    pub data: Vec<u8>,
}

impl ExtType {
    /// `ExtType(code, data)`; `code` is `0~127` or `-128~-1` by its type.
    pub fn new(code: i8, data: impl Into<Vec<u8>>) -> Self {
        ExtType {
            code,
            data: data.into(),
        }
    }
}

/// Python's `repr`: `ExtType(code=5, data=b'\x01\x02\x03')`.
impl fmt::Debug for ExtType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ExtType(code={}, data={})",
            self.code,
            crate::python::pyrepr::bytes_repr(&self.data)
        )
    }
}

crate::py_object!(ExtType);

fn overflow_error(msg: &str) -> LailaError {
    LailaError::new(ExcKind::OverflowError, msg)
}

/// `msgpack.exceptions.ExtraData(unpacked, extra)`.
fn extra_data() -> LailaError {
    LailaError::new(ExcKind::ExtraData, "unpack(b) received extra data.")
}

/// `msgpack.exceptions.FormatError` / `StackError` (both `UnpackValueError`s,
/// raised by msgpack-python without a message: `str(e) == ""`).
fn unpack_value_error() -> LailaError {
    LailaError::new(ExcKind::UnpackValueError, "")
}

// --------------------------------------------------------------------------
// Packer
// --------------------------------------------------------------------------

/// Keyword arguments of `msgpack.Packer` that laila uses.
#[derive(Clone)]
pub struct PackOptions {
    /// `use_bin_type=True`: bytes -> bin family, str -> str8 allowed.
    pub use_bin_type: bool,
    /// `use_single_float=False`: floats as float32 instead of float64.
    pub use_single_float: bool,
    /// `strict_types=False`: when `True` only exact `list`s are arrays --
    /// a `tuple` goes to `default` / `TypeError` like any unknown object.
    pub strict_types: bool,
    /// `default=None`: hook called once for objects msgpack cannot serialize.
    pub default: Option<Callable>,
}

impl Default for PackOptions {
    fn default() -> Self {
        PackOptions {
            use_bin_type: true,
            use_single_float: false,
            strict_types: false,
            default: None,
        }
    }
}

impl fmt::Debug for PackOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PackOptions")
            .field("use_bin_type", &self.use_bin_type)
            .field("use_single_float", &self.use_single_float)
            .field("strict_types", &self.strict_types)
            .field("default", &self.default.as_ref().map(|_| "<function>"))
            .finish()
    }
}

struct Packer<'a> {
    opts: &'a PackOptions,
    b: Vec<u8>,
}

impl<'a> Packer<'a> {
    fn new(opts: &'a PackOptions) -> Self {
        Packer {
            opts,
            b: Vec::with_capacity(1024),
        }
    }

    fn u8(&mut self, x: u8) {
        self.b.push(x);
    }
    fn u16(&mut self, x: u16) {
        self.b.extend_from_slice(&x.to_be_bytes());
    }
    fn u32(&mut self, x: u32) {
        self.b.extend_from_slice(&x.to_be_bytes());
    }
    fn u64(&mut self, x: u64) {
        self.b.extend_from_slice(&x.to_be_bytes());
    }
    fn i64(&mut self, x: i64) {
        self.b.extend_from_slice(&x.to_be_bytes());
    }

    fn pack_i64(&mut self, v: i64) {
        if v >= 0 {
            self.pack_u64(v as u64);
        } else if v >= -0x20 {
            self.u8(v as u8);
        } else if v >= -0x80 {
            self.u8(0xd0);
            self.u8(v as u8);
        } else if v >= -0x8000 {
            self.u8(0xd1);
            self.u16(v as u16);
        } else if v >= -0x8000_0000 {
            self.u8(0xd2);
            self.u32(v as u32);
        } else {
            self.u8(0xd3);
            self.i64(v);
        }
    }

    fn pack_u64(&mut self, v: u64) {
        if v < 0x80 {
            self.u8(v as u8);
        } else if v < 0x100 {
            self.u8(0xcc);
            self.u8(v as u8);
        } else if v < 0x1_0000 {
            self.u8(0xcd);
            self.u16(v as u16);
        } else if v < 0x1_0000_0000 {
            self.u8(0xce);
            self.u32(v as u32);
        } else {
            self.u8(0xcf);
            self.u64(v);
        }
    }

    /// Any Python `int` (`Arg::Long`): `0 <= v < 2**64` or `-2**63 <= v < 0`.
    fn pack_bigint(&mut self, v: &BigInt) -> Result<()> {
        if let Some(i) = v.to_i64() {
            self.pack_i64(i);
            return Ok(());
        }
        if let Some(u) = v.to_u64() {
            self.pack_u64(u);
            return Ok(());
        }
        Err(overflow_error("Integer value out of range"))
    }

    fn pack_float(&mut self, x: f64) {
        if self.opts.use_single_float {
            self.u8(0xca);
            self.u32((x as f32).to_bits());
        } else {
            self.u8(0xcb);
            self.u64(x.to_bits());
        }
    }

    fn pack_raw_header(&mut self, n: usize) -> Result<()> {
        if n <= 0x1f {
            self.u8(0xa0 | n as u8);
        } else if n <= 0xff && self.opts.use_bin_type {
            self.u8(0xd9);
            self.u8(n as u8);
        } else if n <= 0xffff {
            self.u8(0xda);
            self.u16(n as u16);
        } else if let Ok(n32) = u32::try_from(n) {
            self.u8(0xdb);
            self.u32(n32);
        } else {
            return Err(LailaError::value_error("unicode string is too large"));
        }
        Ok(())
    }

    fn pack_bin_header(&mut self, n: usize) -> Result<()> {
        if !self.opts.use_bin_type {
            return self.pack_raw_header(n);
        }
        if n <= 0xff {
            self.u8(0xc4);
            self.u8(n as u8);
        } else if n <= 0xffff {
            self.u8(0xc5);
            self.u16(n as u16);
        } else if let Ok(n32) = u32::try_from(n) {
            self.u8(0xc6);
            self.u32(n32);
        } else {
            return Err(LailaError::value_error("bytes is too large"));
        }
        Ok(())
    }

    fn pack_array_header(&mut self, n: usize) -> Result<()> {
        if n <= 0x0f {
            self.u8(0x90 | n as u8);
        } else if n <= 0xffff {
            self.u8(0xdc);
            self.u16(n as u16);
        } else if let Ok(n32) = u32::try_from(n) {
            self.u8(0xdd);
            self.u32(n32);
        } else {
            return Err(LailaError::value_error("list is too large"));
        }
        Ok(())
    }

    fn pack_map_header(&mut self, n: usize) -> Result<()> {
        if n <= 0x0f {
            self.u8(0x80 | n as u8);
        } else if n <= 0xffff {
            self.u8(0xde);
            self.u16(n as u16);
        } else if let Ok(n32) = u32::try_from(n) {
            self.u8(0xdf);
            self.u32(n32);
        } else {
            return Err(LailaError::value_error("dict is too large"));
        }
        Ok(())
    }

    fn pack_ext(&mut self, code: i8, data: &[u8]) -> Result<()> {
        let n = data.len();
        match n {
            1 => self.u8(0xd4),
            2 => self.u8(0xd5),
            4 => self.u8(0xd6),
            8 => self.u8(0xd7),
            16 => self.u8(0xd8),
            _ if n <= 0xff => {
                self.u8(0xc7);
                self.u8(n as u8);
            }
            _ if n <= 0xffff => {
                self.u8(0xc8);
                self.u16(n as u16);
            }
            _ => match u32::try_from(n) {
                Ok(n32) => {
                    self.u8(0xc9);
                    self.u32(n32);
                }
                Err(_) => return Err(LailaError::value_error("ext data is too large")),
            },
        }
        self.u8(code as u8);
        self.b.extend_from_slice(data);
        Ok(())
    }

    fn pack(&mut self, o: &Arg, nest_limit: i32) -> Result<()> {
        if nest_limit < 0 {
            return Err(LailaError::value_error("recursion limit exceeded."));
        }
        let mut o = o.clone();
        let mut default_used = false;
        loop {
            match &o {
                Arg::None => self.u8(0xc0),
                Arg::Bool(true) => self.u8(0xc3),
                Arg::Bool(false) => self.u8(0xc2),
                Arg::Int(i) => self.pack_i64(*i),
                Arg::Long(l) => self.pack_bigint(l)?,
                Arg::Float(f) => self.pack_float(*f),
                Arg::Str(s) => {
                    let raw = s.as_bytes();
                    self.pack_raw_header(raw.len())?;
                    self.b.extend_from_slice(raw);
                }
                Arg::Bytes(b) => {
                    self.pack_bin_header(b.len())?;
                    self.b.extend_from_slice(b);
                }
                Arg::ByteArray(b) => {
                    let g = b.read().clone();
                    self.pack_bin_header(g.len())?;
                    self.b.extend_from_slice(&g);
                }
                Arg::List(l) => {
                    let items = l.read().clone();
                    self.pack_array_header(items.len())?;
                    for x in &items {
                        self.pack(x, nest_limit - 1)?;
                    }
                }
                Arg::Tuple(t) if !self.opts.strict_types => {
                    self.pack_array_header(t.len())?;
                    for x in t.iter() {
                        self.pack(x, nest_limit - 1)?;
                    }
                }
                Arg::Dict(d) => {
                    let items: Vec<(Arg, Arg)> = d
                        .read()
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    self.pack_map_header(items.len())?;
                    for (k, v) in &items {
                        self.pack(k, nest_limit - 1)?;
                        self.pack(v, nest_limit - 1)?;
                    }
                }
                Arg::Object(obj) if obj.as_any().is::<ExtType>() => {
                    let ext = obj.as_any().downcast_ref::<ExtType>().expect("checked");
                    self.pack_ext(ext.code, &ext.data)?;
                }
                // sets, frozensets, ndarrays, entries, complex, ...:
                // msgpack-python has no support for them -> `default(o)` once
                // (the loop re-dispatches on its result), else TypeError.
                other => {
                    if !default_used {
                        if let Some(default) = &self.opts.default {
                            let mut args = Args::new();
                            args.push_positional(other.clone());
                            o = default(args)?;
                            default_used = true;
                            continue;
                        }
                    }
                    return Err(LailaError::type_error(alloc::format!(
                        "can not serialize '{}' object",
                        other.type_name()
                    )));
                }
            }
            return Ok(());
        }
    }
}

/// `msgpack.packb(o, use_bin_type=True, use_single_float=False, default=None)`.
pub fn packb(obj: &Arg, opts: &PackOptions) -> Result<Vec<u8>> {
    let mut p = Packer::new(opts);
    p.pack(obj, DEFAULT_RECURSE_LIMIT)?;
    Ok(p.b)
}

/// `msgpack.dumps` is an alias of `packb`.
pub fn dumps(obj: &Arg, opts: &PackOptions) -> Result<Vec<u8>> {
    packb(obj, opts)
}

// --------------------------------------------------------------------------
// Unpacker
// --------------------------------------------------------------------------

/// Keyword arguments of `msgpack.Unpacker` that laila uses.
#[derive(Clone, Debug)]
pub struct UnpackOptions {
    /// `raw=False`: `True` returns str payloads as `bytes`.
    pub raw: bool,
    /// `strict_map_key=True` (msgpack 1.x default): only `str` / `bytes` keys.
    pub strict_map_key: bool,
    /// `use_list=True`: arrays as `list`; `False` -> `tuple`.
    pub use_list: bool,
}

impl Default for UnpackOptions {
    fn default() -> Self {
        UnpackOptions {
            raw: false,
            strict_map_key: true,
            use_list: true,
        }
    }
}

fn incomplete() -> LailaError {
    LailaError::value_error("Unpack failed: incomplete input")
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
    opts: &'a UnpackOptions,
}

impl<'a> Cursor<'a> {
    fn need(&self, n: usize) -> Result<()> {
        if self.pos + n > self.buf.len() {
            return Err(incomplete());
        }
        Ok(())
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        self.need(n)?;
        let v = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(v)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_be_bytes(a))
    }

    fn str(&mut self, n: usize) -> Result<Arg> {
        let raw = self.take(n)?;
        if self.opts.raw {
            return Ok(Arg::Bytes(raw.into()));
        }
        match core::str::from_utf8(raw) {
            Ok(s) => Ok(Arg::Str(s.into())),
            Err(e) => Err(utf8_decode_error(raw, e)),
        }
    }
    fn bin(&mut self, n: usize) -> Result<Arg> {
        Ok(Arg::Bytes(self.take(n)?.into()))
    }
    fn array(&mut self, n: usize, depth: i32) -> Result<Arg> {
        // never allocate more than the remaining input could hold
        let mut out = Vec::with_capacity(n.min(self.buf.len() - self.pos));
        for _ in 0..n {
            out.push(self.unpack(depth + 1)?);
        }
        Ok(if self.opts.use_list {
            out.into_arg()
        } else {
            Arg::Tuple(out.into())
        })
    }
    fn map(&mut self, n: usize, depth: i32) -> Result<Arg> {
        let d = crate::python::builtins::dict(Args::new())?;
        for _ in 0..n {
            let k = self.unpack(depth + 1)?;
            if self.opts.strict_map_key && !matches!(k, Arg::Str(_) | Arg::Bytes(_)) {
                return Err(LailaError::value_error(alloc::format!(
                    "{} is not allowed for map key when strict_map_key=True",
                    k.type_name()
                )));
            }
            let v = self.unpack(depth + 1)?;
            // unhashable keys (list, dict) raise TypeError here, like Python
            d.__setitem__(k, v)?;
        }
        Ok(d)
    }
    fn ext(&mut self, n: usize) -> Result<Arg> {
        let code = self.u8()? as i8;
        let data = self.take(n)?;
        Ok(ExtType::new(code, data).into_arg())
    }

    fn unpack(&mut self, depth: i32) -> Result<Arg> {
        if depth > DEFAULT_RECURSE_LIMIT {
            // StackError
            return Err(unpack_value_error());
        }
        let b = self.u8()?;
        if b <= 0x7f {
            return Ok(Arg::Int(b as i64));
        }
        if b >= 0xe0 {
            return Ok(Arg::Int(b as i64 - 0x100));
        }
        if (0xa0..=0xbf).contains(&b) {
            return self.str((b & 0x1f) as usize);
        }
        if (0x90..=0x9f).contains(&b) {
            return self.array((b & 0x0f) as usize, depth);
        }
        if (0x80..=0x8f).contains(&b) {
            return self.map((b & 0x0f) as usize, depth);
        }
        Ok(match b {
            0xc0 => Arg::None,
            0xc2 => Arg::Bool(false),
            0xc3 => Arg::Bool(true),
            0xc4 => {
                let n = self.u8()? as usize;
                self.bin(n)?
            }
            0xc5 => {
                let n = self.u16()? as usize;
                self.bin(n)?
            }
            0xc6 => {
                let n = self.u32()? as usize;
                self.bin(n)?
            }
            0xc7 => {
                let n = self.u8()? as usize;
                self.ext(n)?
            }
            0xc8 => {
                let n = self.u16()? as usize;
                self.ext(n)?
            }
            0xc9 => {
                let n = self.u32()? as usize;
                self.ext(n)?
            }
            0xca => Arg::Float(f32::from_bits(self.u32()?) as f64),
            0xcb => Arg::Float(f64::from_bits(self.u64()?)),
            0xcc => Arg::Int(self.u8()? as i64),
            0xcd => Arg::Int(self.u16()? as i64),
            0xce => Arg::Int(self.u32()? as i64),
            0xcf => self.u64()?.into_arg(),
            0xd0 => Arg::Int(self.u8()? as i8 as i64),
            0xd1 => Arg::Int(self.u16()? as i16 as i64),
            0xd2 => Arg::Int(self.u32()? as i32 as i64),
            0xd3 => Arg::Int(self.u64()? as i64),
            0xd4 => self.ext(1)?,
            0xd5 => self.ext(2)?,
            0xd6 => self.ext(4)?,
            0xd7 => self.ext(8)?,
            0xd8 => self.ext(16)?,
            0xd9 => {
                let n = self.u8()? as usize;
                self.str(n)?
            }
            0xda => {
                let n = self.u16()? as usize;
                self.str(n)?
            }
            0xdb => {
                let n = self.u32()? as usize;
                self.str(n)?
            }
            0xdc => {
                let n = self.u16()? as usize;
                self.array(n, depth)?
            }
            0xdd => {
                let n = self.u32()? as usize;
                self.array(n, depth)?
            }
            0xde => {
                let n = self.u16()? as usize;
                self.map(n, depth)?
            }
            0xdf => {
                let n = self.u32()? as usize;
                self.map(n, depth)?
            }
            // 0xc1 is never used -> FormatError
            _ => return Err(unpack_value_error()),
        })
    }
}

/// CPython's `UnicodeDecodeError` text for a bad UTF-8 str payload.
fn utf8_decode_error(raw: &[u8], e: core::str::Utf8Error) -> LailaError {
    let pos = e.valid_up_to();
    let byte = raw.get(pos).copied().unwrap_or(0);
    let reason = match e.error_len() {
        None => "unexpected end of data",
        Some(_) if (0x80..=0xc1).contains(&byte) || byte >= 0xf5 => "invalid start byte",
        Some(_) => "invalid continuation byte",
    };
    LailaError::new(
        ExcKind::UnicodeDecodeError,
        alloc::format!("'utf-8' codec can't decode byte 0x{byte:02x} in position {pos}: {reason}"),
    )
}

/// `msgpack.unpackb(packed, raw=False, strict_map_key=True, use_list=True)`.
pub fn unpackb(data: &[u8], opts: &UnpackOptions) -> Result<Arg> {
    let mut c = Cursor {
        buf: data,
        pos: 0,
        opts,
    };
    let ret = c.unpack(0)?;
    if c.pos < data.len() {
        return Err(extra_data());
    }
    Ok(ret)
}

/// `msgpack.loads` is an alias of `unpackb`.
pub fn loads(data: &[u8], opts: &UnpackOptions) -> Result<Arg> {
    unpackb(data, opts)
}

/// Streaming `msgpack.Unpacker()`: `feed()` bytes, iterate complete objects;
/// a partial trailing message stays buffered for the next `feed()`.
#[derive(Clone, Debug, Default)]
pub struct Unpacker {
    opts: UnpackOptions,
    buf: Vec<u8>,
}

impl Unpacker {
    pub fn new(opts: UnpackOptions) -> Self {
        Unpacker {
            opts,
            buf: Vec::new(),
        }
    }

    /// `Unpacker.feed(next_bytes)`.
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Bytes buffered but not yet consumed by a complete object.
    pub fn pending(&self) -> &[u8] {
        &self.buf
    }
}

impl Iterator for Unpacker {
    type Item = Result<Arg>;

    /// `next(unpacker)`: the next complete object, `None` when the buffer is
    /// empty or holds only a partial message (Python's `StopIteration` /
    /// `OutOfData`).
    fn next(&mut self) -> Option<Result<Arg>> {
        if self.buf.is_empty() {
            return None;
        }
        let (result, consumed) = {
            let mut c = Cursor {
                buf: &self.buf,
                pos: 0,
                opts: &self.opts,
            };
            (c.unpack(0), c.pos)
        };
        match result {
            Ok(obj) => {
                self.buf.drain(..consumed);
                Some(Ok(obj))
            }
            Err(e) if e.kind == ExcKind::ValueError && e.message.contains("incomplete input") => {
                None
            }
            Err(e) => Some(Err(e)),
        }
    }
}

/// `msgpack.version` (the msgpack-python release this port mirrors).
pub const VERSION: (u32, u32, u32) = (1, 1, 2);

/// `default=` hook helper: wraps a Rust closure as the `Callable` msgpack
/// calls with the unsupported object (`lambda o: ...`).
pub fn default_hook<F>(f: F) -> Callable
where
    F: Fn(Arg) -> Result<Arg> + Send + Sync + 'static,
{
    Arc::new(move |args: Args| {
        let o = args.positional.into_iter().next().unwrap_or(Arg::None);
        f(o)
    })
}

impl From<ExtType> for Arg {
    fn from(e: ExtType) -> Arg {
        e.into_arg()
    }
}
