//! Small stdlib shims the test suites spell by module name:
//! `textwrap.dedent`, `struct.pack` / `struct.unpack` / `struct.Struct`,
//! `hashlib.sha256(..)`, `statistics.median` and `functools.reduce`.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::object::PyObject;
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, Result};

macro_rules! module_value {
    ($ty:ident, $name:literal, $hash:expr) => {
        impl PyObject for $ty {
            fn as_any(&self) -> &dyn core::any::Any {
                self
            }
            fn type_name(&self) -> &'static str {
                "module"
            }
            fn __repr__(&self) -> String {
                alloc::format!("<module '{}'>", $name)
            }
            fn __eq__(&self, other: &dyn PyObject) -> bool {
                other.as_any().downcast_ref::<$ty>().is_some()
            }
            fn __hash__(&self) -> Result<i64> {
                Ok($hash)
            }
        }
        impl IntoArg for $ty {
            fn into_arg(self) -> Arg {
                Arg::Object(Arc::new(self))
            }
        }
        impl PyArg for $ty {
            fn to_arg(&self) -> Arg {
                Arg::Object(Arc::new(*self))
            }
        }
        impl crate::python::builtins::ModuleValue for $ty {
            fn module_value(self) -> Arg {
                self.into_arg()
            }
        }
    };
}

// ---------------------------------------------------------------------------
// textwrap

/// `import textwrap`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct textwrap_mod;
#[allow(non_upper_case_globals)]
pub static textwrap: textwrap_mod = textwrap_mod;

impl textwrap_mod {
    /// `textwrap.dedent(text)`: remove the common leading whitespace of
    /// every non-blank line (blank lines are normalised to empty).
    pub fn dedent(&self, text: impl IntoArg) -> Result<Arg> {
        let text = text.into_arg().__str__();
        let mut margin: Option<String> = None;
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let indent: String = line
                .chars()
                .take_while(|c| *c == ' ' || *c == '\t')
                .collect();
            margin = Some(match margin {
                None => indent,
                Some(m) => {
                    let common: String = m
                        .chars()
                        .zip(indent.chars())
                        .take_while(|(a, b)| a == b)
                        .map(|(a, _)| a)
                        .collect();
                    common
                }
            });
        }
        let margin = margin.unwrap_or_default();
        let mut out = String::with_capacity(text.len());
        let mut first = true;
        for line in text.split('\n') {
            if !first {
                out.push('\n');
            }
            first = false;
            if line.trim().is_empty() {
                continue;
            }
            out.push_str(line.strip_prefix(margin.as_str()).unwrap_or(line));
        }
        Ok(Arg::Str(out.into()))
    }
}
module_value!(textwrap_mod, "textwrap", 0x74_65_78_74);

// ---------------------------------------------------------------------------
// struct

/// `import struct` (the Rust keyword: spelled `r#struct` in `laila::python!`).
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct struct_mod;
#[allow(non_upper_case_globals)]
pub static r#struct: struct_mod = struct_mod;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Endian {
    Little,
    Big,
}

#[derive(Clone, Debug)]
struct Field {
    code: char,
    count: usize,
}

/// A parsed `struct` format: byte order plus the fields.
#[derive(Clone, Debug)]
pub struct StructFormat {
    endian: Endian,
    fields: Vec<Field>,
    size: usize,
}

fn field_size(code: char) -> Option<usize> {
    Some(match code {
        'x' | 'c' | 'b' | 'B' | '?' | 's' | 'p' => 1,
        'h' | 'H' | 'e' => 2,
        'i' | 'I' | 'l' | 'L' | 'f' => 4,
        'q' | 'Q' | 'd' => 8,
        _ => return None,
    })
}

impl StructFormat {
    /// Parse a format string (`">I"`, `"<BBIB"`, `"!4sH"`, ...).
    pub fn parse(fmt: &str) -> Result<StructFormat> {
        let mut chars = fmt.trim().chars().peekable();
        let endian = match chars.peek() {
            Some('<') => {
                chars.next();
                Endian::Little
            }
            Some('>') | Some('!') => {
                chars.next();
                Endian::Big
            }
            Some('=') | Some('@') => {
                chars.next();
                if cfg!(target_endian = "little") {
                    Endian::Little
                } else {
                    Endian::Big
                }
            }
            _ => {
                if cfg!(target_endian = "little") {
                    Endian::Little
                } else {
                    Endian::Big
                }
            }
        };
        let mut fields = Vec::new();
        let mut size = 0usize;
        let mut count: Option<usize> = None;
        for c in chars {
            if c.is_ascii_digit() {
                count = Some(count.unwrap_or(0) * 10 + (c as u8 - b'0') as usize);
                continue;
            }
            if c == ' ' {
                continue;
            }
            let Some(fs) = field_size(c) else {
                return Err(LailaError::new(
                    crate::python::ExcKind::ValueError,
                    "bad char in struct format",
                ));
            };
            let n = count.take().unwrap_or(1);
            if c == 's' || c == 'p' {
                fields.push(Field { code: c, count: n });
                size += n;
            } else {
                for _ in 0..n {
                    fields.push(Field { code: c, count: 1 });
                }
                size += fs * n;
            }
        }
        Ok(StructFormat {
            endian,
            fields,
            size,
        })
    }
    /// `Struct.size`
    pub fn size(&self) -> usize {
        self.size
    }
    fn values_needed(&self) -> usize {
        self.fields.iter().filter(|f| f.code != 'x').count()
    }
    /// `Struct.pack(*values)` -> bytes
    pub fn pack(&self, values: &[Arg]) -> Result<Vec<u8>> {
        if values.len() != self.values_needed() {
            return Err(LailaError::new(
                crate::python::ExcKind::ValueError,
                alloc::format!(
                    "pack expected {} items for packing (got {})",
                    self.values_needed(),
                    values.len()
                ),
            ));
        }
        let mut out = Vec::with_capacity(self.size);
        let mut vi = 0usize;
        for f in &self.fields {
            if f.code == 'x' {
                out.push(0);
                continue;
            }
            let v = &values[vi];
            vi += 1;
            match f.code {
                's' | 'p' => {
                    let mut b = v.as_bytes_like().unwrap_or_default();
                    b.resize(f.count, 0);
                    out.extend_from_slice(&b);
                }
                'c' => {
                    let b = v.as_bytes_like().unwrap_or_default();
                    out.push(b.first().copied().unwrap_or(0));
                }
                '?' => out.push(u8::from(v.__bool__())),
                'f' => {
                    let x = v.as_f64().unwrap_or(0.0) as f32;
                    let b = x.to_bits();
                    self.put(&mut out, b as u64, 4);
                }
                'd' => {
                    let x = v.as_f64().unwrap_or(0.0);
                    self.put(&mut out, x.to_bits(), 8);
                }
                code => {
                    let n = v.as_i64().ok_or_else(|| {
                        LailaError::new(
                            crate::python::ExcKind::ValueError,
                            "required argument is not an integer",
                        )
                    })?;
                    let width = field_size(code).unwrap_or(1);
                    let (lo, hi): (i128, i128) = match code {
                        'b' => (-128, 127),
                        'B' => (0, 255),
                        'h' => (-32768, 32767),
                        'H' => (0, 65535),
                        'i' | 'l' => (i32::MIN as i128, i32::MAX as i128),
                        'I' | 'L' => (0, u32::MAX as i128),
                        'q' => (i64::MIN as i128, i64::MAX as i128),
                        _ => (0, u64::MAX as i128),
                    };
                    if (n as i128) < lo || (n as i128) > hi {
                        return Err(LailaError::new(
                            crate::python::ExcKind::ValueError,
                            alloc::format!("'{code}' format requires {lo} <= number <= {hi}"),
                        ));
                    }
                    self.put(&mut out, n as u64, width);
                }
            }
        }
        Ok(out)
    }
    fn put(&self, out: &mut Vec<u8>, v: u64, width: usize) {
        let bytes = v.to_le_bytes();
        match self.endian {
            Endian::Little => out.extend_from_slice(&bytes[..width]),
            Endian::Big => out.extend(bytes[..width].iter().rev()),
        }
    }
    fn get(&self, raw: &[u8]) -> u64 {
        let mut v: u64 = 0;
        match self.endian {
            Endian::Little => {
                for b in raw.iter().rev() {
                    v = (v << 8) | *b as u64;
                }
            }
            Endian::Big => {
                for b in raw {
                    v = (v << 8) | *b as u64;
                }
            }
        }
        v
    }
    /// `Struct.unpack(buffer)` -> tuple
    pub fn unpack(&self, raw: &[u8]) -> Result<Arg> {
        if raw.len() != self.size {
            return Err(LailaError::new(
                crate::python::ExcKind::ValueError,
                alloc::format!("unpack requires a buffer of {} bytes", self.size),
            ));
        }
        let mut out = Vec::new();
        let mut pos = 0usize;
        for f in &self.fields {
            match f.code {
                'x' => pos += 1,
                's' | 'p' => {
                    out.push(Arg::Bytes(raw[pos..pos + f.count].to_vec().into()));
                    pos += f.count;
                }
                'c' => {
                    out.push(Arg::Bytes(alloc::vec![raw[pos]].into()));
                    pos += 1;
                }
                '?' => {
                    out.push(Arg::Bool(raw[pos] != 0));
                    pos += 1;
                }
                'f' => {
                    let v = self.get(&raw[pos..pos + 4]) as u32;
                    out.push(Arg::Float(f32::from_bits(v) as f64));
                    pos += 4;
                }
                'd' => {
                    let v = self.get(&raw[pos..pos + 8]);
                    out.push(Arg::Float(f64::from_bits(v)));
                    pos += 8;
                }
                code => {
                    let width = field_size(code).unwrap_or(1);
                    let v = self.get(&raw[pos..pos + width]);
                    pos += width;
                    let signed = matches!(code, 'b' | 'h' | 'i' | 'l' | 'q');
                    let n: i64 = if signed {
                        let shift = 64 - 8 * width as u32;
                        ((v << shift) as i64) >> shift
                    } else {
                        v as i64
                    };
                    out.push(Arg::Int(n));
                }
            }
        }
        Ok(Arg::Tuple(out.into()))
    }
}

/// `struct.Struct(fmt)` handle.
#[derive(Clone, Debug)]
pub struct Struct(Arc<StructFormat>);

impl Struct {
    /// `struct.Struct(fmt)`
    pub fn new(args: impl Into<Args>) -> Result<Struct> {
        let a: Args = args.into();
        let fmt = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("format"));
        Ok(Struct(Arc::new(StructFormat::parse(&fmt.__str__())?)))
    }
    /// `s.size`
    pub fn size(&self) -> i64 {
        self.0.size() as i64
    }
    /// `s.pack(*values)`
    pub fn pack(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        Ok(Arg::Bytes(self.0.pack(&a.positional)?.into()))
    }
    /// `s.unpack(buffer)`
    pub fn unpack(&self, raw: impl IntoArg) -> Result<Arg> {
        let b = raw.into_arg().as_bytes_like().unwrap_or_default();
        self.0.unpack(&b)
    }
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        PyObject::__getattr__(self, name)
    }
}
impl PyObject for Struct {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Struct"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<Struct size={}>", self.0.size())
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Struct>()
            .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "size" => Ok(Arg::Int(self.size())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Struct' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "pack" => self.pack(args),
            "unpack" => self.unpack(args.positional.first().cloned().unwrap_or(Arg::None)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Struct' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for Struct {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Struct {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

impl struct_mod {
    /// `struct.Struct(fmt)`
    #[allow(non_snake_case)]
    pub fn Struct(&self, args: impl Into<Args>) -> Result<Struct> {
        Struct::new(args)
    }
    /// `struct.pack(fmt, *values)`
    pub fn pack(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let Some((fmt, values)) = a.positional.split_first() else {
            return Err(LailaError::type_error("pack expected at least 1 argument"));
        };
        let f = StructFormat::parse(&fmt.__str__())?;
        Ok(Arg::Bytes(f.pack(values)?.into()))
    }
    /// `struct.unpack(fmt, buffer)`
    pub fn unpack(&self, fmt: impl IntoArg, raw: impl IntoArg) -> Result<Arg> {
        let f = StructFormat::parse(&fmt.into_arg().__str__())?;
        f.unpack(&raw.into_arg().as_bytes_like().unwrap_or_default())
    }
    /// `struct.calcsize(fmt)`
    pub fn calcsize(&self, fmt: impl IntoArg) -> Result<i64> {
        Ok(StructFormat::parse(&fmt.into_arg().__str__())?.size() as i64)
    }
}
module_value!(struct_mod, "struct", 0x73_74_72_75);

// ---------------------------------------------------------------------------
// hashlib

/// `import hashlib`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct hashlib_mod;
#[allow(non_upper_case_globals)]
pub static hashlib: hashlib_mod = hashlib_mod;

/// A finished digest (`hashlib.sha256(data)`): `digest()` / `hexdigest()`.
#[derive(Clone, Debug)]
pub struct Digest {
    name: &'static str,
    bytes: Vec<u8>,
}

impl Digest {
    /// `h.digest()`
    pub fn digest(&self) -> Arg {
        Arg::Bytes(self.bytes.clone().into())
    }
    /// `h.hexdigest()`
    pub fn hexdigest(&self) -> String {
        self.bytes
            .iter()
            .map(|b| alloc::format!("{b:02x}"))
            .collect()
    }
    /// `h.name`
    pub fn name(&self) -> &'static str {
        self.name
    }
}
impl PyObject for Digest {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "HASH"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<{} HASH object>", self.name)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Digest>()
            .is_some_and(|o| o.bytes == self.bytes)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self
            .bytes
            .iter()
            .take(8)
            .fold(0i64, |h, b| (h << 8) | *b as i64))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(Arg::Str(self.name.into())),
            "digest_size" => Ok(Arg::Int(self.bytes.len() as i64)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'HASH' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, _args: Args) -> Result<Arg> {
        match name {
            "digest" => Ok(self.digest()),
            "hexdigest" => Ok(Arg::Str(self.hexdigest().into())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'HASH' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for Digest {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Digest {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

fn data_of(args: Args) -> Vec<u8> {
    args.positional
        .first()
        .cloned()
        .unwrap_or_else(|| args.get("data"))
        .as_bytes_like()
        .unwrap_or_default()
}

impl hashlib_mod {
    /// `hashlib.sha256(data=b"")`
    pub fn sha256(&self, args: impl Into<Args>) -> Result<Digest> {
        use sha2::Digest as _;
        Ok(Digest {
            name: "sha256",
            bytes: sha2::Sha256::digest(data_of(args.into())).to_vec(),
        })
    }
    /// `hashlib.sha1(data=b"")`
    pub fn sha1(&self, args: impl Into<Args>) -> Result<Digest> {
        use sha1::Digest as _;
        Ok(Digest {
            name: "sha1",
            bytes: sha1::Sha1::digest(data_of(args.into())).to_vec(),
        })
    }
    /// `hashlib.sha512(data=b"")`
    pub fn sha512(&self, args: impl Into<Args>) -> Result<Digest> {
        use sha2::Digest as _;
        Ok(Digest {
            name: "sha512",
            bytes: sha2::Sha512::digest(data_of(args.into())).to_vec(),
        })
    }
}
module_value!(hashlib_mod, "hashlib", 0x68_61_73_68);

// ---------------------------------------------------------------------------
// statistics

/// `import statistics`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct statistics_mod;
#[allow(non_upper_case_globals)]
pub static statistics: statistics_mod = statistics_mod;

fn floats_of(data: Arg) -> Result<Vec<f64>> {
    let items = data.__iter__()?;
    items
        .iter()
        .map(|x| {
            x.as_f64().ok_or_else(|| {
                LailaError::type_error(alloc::format!("can't convert {} to float", x.type_name()))
            })
        })
        .collect()
}

impl statistics_mod {
    /// `statistics.median(data)`
    pub fn median(&self, data: impl IntoArg) -> Result<Arg> {
        let mut xs = floats_of(data.into_arg())?;
        if xs.is_empty() {
            return Err(LailaError::new(
                crate::python::ExcKind::ValueError,
                "no median for empty data",
            ));
        }
        xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(core::cmp::Ordering::Equal));
        let n = xs.len();
        Ok(Arg::Float(if n % 2 == 1 {
            xs[n / 2]
        } else {
            (xs[n / 2 - 1] + xs[n / 2]) / 2.0
        }))
    }
    /// `statistics.mean(data)`
    pub fn mean(&self, data: impl IntoArg) -> Result<Arg> {
        let xs = floats_of(data.into_arg())?;
        if xs.is_empty() {
            return Err(LailaError::new(
                crate::python::ExcKind::ValueError,
                "mean requires at least one data point",
            ));
        }
        Ok(Arg::Float(xs.iter().sum::<f64>() / xs.len() as f64))
    }
    /// `statistics.stdev(data)` (sample standard deviation)
    pub fn stdev(&self, data: impl IntoArg) -> Result<Arg> {
        let xs = floats_of(data.into_arg())?;
        if xs.len() < 2 {
            return Err(LailaError::new(
                crate::python::ExcKind::ValueError,
                "stdev requires at least two data points",
            ));
        }
        let m = xs.iter().sum::<f64>() / xs.len() as f64;
        let var = xs.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / (xs.len() - 1) as f64;
        Ok(Arg::Float(libm_sqrt(var)))
    }
}
fn libm_sqrt(x: f64) -> f64 {
    #[cfg(feature = "std")]
    {
        x.sqrt()
    }
    #[cfg(not(feature = "std"))]
    {
        libm::sqrt(x)
    }
}
module_value!(statistics_mod, "statistics", 0x73_74_61_74);

// ---------------------------------------------------------------------------
// functools.reduce

/// `functools.reduce(function, iterable[, initial])`
pub fn reduce(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    let f = a
        .positional
        .first()
        .cloned()
        .ok_or_else(|| LailaError::type_error("reduce expected at least 2 arguments, got 0"))?;
    let it = a
        .positional
        .get(1)
        .cloned()
        .ok_or_else(|| LailaError::type_error("reduce expected at least 2 arguments, got 1"))?;
    let items = it.__iter__()?;
    let mut iter = items.into_iter();
    let mut acc = match a.positional.get(2).cloned() {
        Some(init) => init,
        None => iter.next().ok_or_else(|| {
            LailaError::type_error("reduce() of empty iterable with no initial value")
        })?,
    };
    for x in iter {
        let mut call = Args::new();
        call.push_positional(acc);
        call.push_positional(x);
        acc = f.__call__(call)?;
    }
    Ok(acc)
}

// ---------------------------------------------------------------------------
// math

/// `import math`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct math_mod;
#[allow(non_upper_case_globals)]
pub static math: math_mod = math_mod;

module_value!(math_mod, "math", 0x6d_61_74_68);

fn float_of(x: Arg) -> Result<f64> {
    x.as_f64().ok_or_else(|| {
        LailaError::type_error(alloc::format!("must be real number, not {}", x.type_name()))
    })
}

impl math_mod {
    /// `math.inf`
    pub fn inf(&self) -> f64 {
        f64::INFINITY
    }
    /// `math.nan`
    pub fn nan(&self) -> f64 {
        f64::NAN
    }
    /// `math.pi`
    pub fn pi(&self) -> f64 {
        core::f64::consts::PI
    }
    /// `math.e`
    pub fn e(&self) -> f64 {
        core::f64::consts::E
    }
    /// `math.isnan(x)`
    pub fn isnan(&self, x: impl IntoArg) -> Result<bool> {
        Ok(float_of(x.into_arg())?.is_nan())
    }
    /// `math.isinf(x)`
    pub fn isinf(&self, x: impl IntoArg) -> Result<bool> {
        Ok(float_of(x.into_arg())?.is_infinite())
    }
    /// `math.isfinite(x)`
    pub fn isfinite(&self, x: impl IntoArg) -> Result<bool> {
        Ok(float_of(x.into_arg())?.is_finite())
    }
    /// `math.sqrt(x)`; `ValueError: math domain error` below zero.
    pub fn sqrt(&self, x: impl IntoArg) -> Result<f64> {
        let v = float_of(x.into_arg())?;
        if v < 0.0 {
            return Err(LailaError::value_error("math domain error"));
        }
        Ok(libm::sqrt(v))
    }
    /// `math.log(x[, base])`; `ValueError: math domain error` at or below zero.
    pub fn log(&self, args: impl Into<Args>) -> Result<f64> {
        let a: Args = args.into();
        let x = float_of(a.positional.first().cloned().unwrap_or(Arg::None))?;
        if x <= 0.0 {
            return Err(LailaError::value_error("math domain error"));
        }
        let ln = libm::log(x);
        match a.positional.get(1) {
            Some(b) => {
                let b = float_of(b.clone())?;
                if b <= 0.0 || b == 1.0 {
                    return Err(LailaError::value_error("math domain error"));
                }
                Ok(ln / libm::log(b))
            }
            None => Ok(ln),
        }
    }
    /// `math.exp(x)`
    pub fn exp(&self, x: impl IntoArg) -> Result<f64> {
        Ok(libm::exp(float_of(x.into_arg())?))
    }
    /// `math.floor(x)`
    pub fn floor(&self, x: impl IntoArg) -> Result<i64> {
        Ok(libm::floor(float_of(x.into_arg())?) as i64)
    }
    /// `math.ceil(x)`
    pub fn ceil(&self, x: impl IntoArg) -> Result<i64> {
        Ok(libm::ceil(float_of(x.into_arg())?) as i64)
    }
    /// `math.isclose(a, b, *, rel_tol=1e-9, abs_tol=0.0)`
    pub fn isclose(&self, args: impl Into<Args>) -> Result<bool> {
        let args: Args = args.into();
        let a = float_of(args.positional.first().cloned().unwrap_or(Arg::None))?;
        let b = float_of(args.positional.get(1).cloned().unwrap_or(Arg::None))?;
        let rel = args.get("rel_tol").as_f64().unwrap_or(1e-9);
        let abs = args.get("abs_tol").as_f64().unwrap_or(0.0);
        if a == b {
            return Ok(true);
        }
        if a.is_infinite() || b.is_infinite() {
            return Ok(false);
        }
        let diff = (a - b).abs();
        Ok(diff <= (rel * b.abs()).max(rel * a.abs()) || diff <= abs)
    }
}

// ---------------------------------------------------------------------------
// contextlib

/// `import contextlib`: `contextlib.suppress(T, ..)` is lowered by the
/// `with` rewrite to [`contextlib::suppress`].
pub mod contextlib {
    use crate::python::errors::ExcMatch;
    use crate::python::Result;

    /// `with contextlib.suppress(T..): body` -- the body's matching error
    /// is swallowed, any other propagates.
    pub fn suppress<T: ExcMatch + ?Sized>(r: Result<()>) -> Result<()> {
        match r {
            Err(e) if T::matches(&e) => Ok(()),
            other => other,
        }
    }
}
