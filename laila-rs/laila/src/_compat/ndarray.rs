//! `NDArray` -- the Rust stand-in for `numpy.ndarray` (`CD_numpyarray`).
//!
//! Carries exactly what the `.npy` format and the pickle reduce need: a numpy
//! dtype string (`"<f8"`, `"<i4"`, `"|u1"`, `">f8"`, ...), a shape,
//! `fortran_order` and the raw element bytes **in the dtype's byte order and
//! in the array's own (C or Fortran) layout** -- i.e. what `ndarray.tobytes(order="A")`
//! returns. `tobytes()` (C order) and `tolist()` re-order on the way out, so a
//! big-endian or Fortran array round-trips with its `dtype.str` intact exactly
//! like numpy.
//!
//! Supported dtypes: all numpy scalar kinds laila serializes -- `b1`,
//! `i1..i8`, `u1..u8`, `f2` (IEEE half), `f4`, `f8`, `c8`, `c16`.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::args::Args;
use crate::python::object::{Arg, IntoArg, PyArg, PyObject};
use crate::python::{LailaError, Result};

/// Python `complex` scalar (`(1+2j)`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Complex {
    pub real: f64,
    pub imag: f64,
}

impl Complex {
    pub fn new(real: f64, imag: f64) -> Self {
        Complex { real, imag }
    }
}

impl PyObject for Complex {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "complex"
    }
    fn __repr__(&self) -> String {
        let f = |x: f64| -> String {
            let s = crate::python::pyrepr::float_repr(x);
            s.strip_suffix(".0").map(ToString::to_string).unwrap_or(s)
        };
        if self.real == 0.0 && !self.real.is_sign_negative() {
            return alloc::format!("{}j", f(self.imag));
        }
        let sign = if self.imag < 0.0 || (self.imag == 0.0 && self.imag.is_sign_negative()) {
            "-"
        } else {
            "+"
        };
        alloc::format!("({}{}{}j)", f(self.real), sign, f(self.imag.abs()))
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Complex>()
            .map(|o| o == self)
            .unwrap_or(false)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arg::Float(self.real).__hash__()?
            ^ Arg::Float(self.imag).__hash__()?.wrapping_mul(1_000_003))
    }
}

impl IntoArg for Complex {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

/// One element of an array, decoded.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Scalar {
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Complex(f64, f64),
}

impl Scalar {
    pub fn as_f64(&self) -> f64 {
        match *self {
            Scalar::Bool(b) => b as i64 as f64,
            Scalar::Int(i) => i as f64,
            Scalar::UInt(u) => u as f64,
            Scalar::Float(f) => f,
            Scalar::Complex(re, _) => re,
        }
    }
    pub fn into_arg(self) -> Arg {
        match self {
            Scalar::Bool(b) => Arg::Bool(b),
            Scalar::Int(i) => Arg::Int(i),
            Scalar::UInt(u) => u.into_arg(),
            Scalar::Float(f) => Arg::Float(f),
            Scalar::Complex(re, im) => Complex::new(re, im).into_arg(),
        }
    }
    /// numpy's element conversion from a Python scalar (`np.array([..], dtype=..)`).
    pub fn from_arg(a: &Arg) -> Result<Scalar> {
        Ok(match a {
            Arg::Bool(b) => Scalar::Bool(*b),
            Arg::Int(i) => Scalar::Int(*i),
            Arg::Long(l) => {
                use num_traits::ToPrimitive;
                match (l.to_i64(), l.to_u64()) {
                    (Some(i), _) => Scalar::Int(i),
                    (None, Some(u)) => Scalar::UInt(u),
                    _ => {
                        return Err(LailaError::new(
                            crate::python::errors::ExcKind::OverflowError,
                            "Python int too large to convert to C long",
                        ))
                    }
                }
            }
            Arg::Float(f) => Scalar::Float(*f),
            Arg::Object(o) => match o.as_any().downcast_ref::<Complex>() {
                Some(c) => Scalar::Complex(c.real, c.imag),
                None => {
                    return Err(LailaError::type_error(alloc::format!(
                        "cannot convert '{}' to an array element",
                        a.type_name()
                    )))
                }
            },
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "cannot convert '{}' to an array element",
                    other.type_name()
                )))
            }
        })
    }
}

/// Storage spec for a canonical dtype string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DtypeSpec {
    /// `b` bool, `i` signed, `u` unsigned, `f` float, `c` complex.
    pub kind: char,
    /// Element size in bytes.
    pub size: usize,
    /// Big-endian storage (`">"` descr).
    pub big: bool,
}

impl DtypeSpec {
    /// Bytes per lane (complex has two lanes).
    pub fn lane_size(&self) -> usize {
        if self.kind == 'c' {
            self.size / 2
        } else {
            self.size
        }
    }
}

const BASES: &[&str] = &[
    "<f8", "<f4", "<f2", "<i8", "<i4", "<i2", "|i1", "<u8", "<u4", "<u2", "|u1", "|b1", "<c16",
    "<c8",
];

const ALIASES: &[(&str, &str)] = &[
    ("float64", "<f8"),
    ("float", "<f8"),
    ("double", "<f8"),
    ("f8", "<f8"),
    ("float32", "<f4"),
    ("single", "<f4"),
    ("f4", "<f4"),
    ("float16", "<f2"),
    ("half", "<f2"),
    ("f2", "<f2"),
    ("int64", "<i8"),
    ("int", "<i8"),
    ("i8", "<i8"),
    ("int32", "<i4"),
    ("i4", "<i4"),
    ("int16", "<i2"),
    ("i2", "<i2"),
    ("int8", "|i1"),
    ("i1", "|i1"),
    ("uint64", "<u8"),
    ("u8", "<u8"),
    ("uint32", "<u4"),
    ("u4", "<u4"),
    ("uint16", "<u2"),
    ("u2", "<u2"),
    ("uint8", "|u1"),
    ("u1", "|u1"),
    ("bool", "|b1"),
    ("bool_", "|b1"),
    ("b1", "|b1"),
    ("complex128", "<c16"),
    ("complex", "<c16"),
    ("c16", "<c16"),
    ("complex64", "<c8"),
    ("c8", "<c8"),
    ("<b1", "|b1"),
    (">b1", "|b1"),
    ("=b1", "|b1"),
    ("<u1", "|u1"),
    (">u1", "|u1"),
    ("=u1", "|u1"),
    ("<i1", "|i1"),
    (">i1", "|i1"),
    ("=i1", "|i1"),
];

fn unsupported(d: &str) -> LailaError {
    LailaError::value_error(alloc::format!("unsupported dtype: {d}"))
}

/// Canonical numpy dtype string (`dtype.str`). Big-endian descrs of multi-byte
/// types are preserved (`">i4"` stays `">i4"`); `"="` means native (little-endian).
pub fn normalize_dtype(dtype: Option<&str>) -> Result<String> {
    let s = match dtype {
        None => return Ok("<f8".into()),
        Some(s) => s,
    };
    if BASES.contains(&s) {
        return Ok(s.into());
    }
    if let Some((_, c)) = ALIASES.iter().find(|(a, _)| *a == s) {
        return Ok((*c).into());
    }
    if s.len() > 1 && (s.starts_with('>') || s.starts_with('=')) {
        let le = alloc::format!("<{}", &s[1..]);
        if BASES.contains(&le.as_str()) {
            return Ok(if s.starts_with('=') { le } else { s.into() });
        }
        if let Some((_, c)) = ALIASES.iter().find(|(a, _)| *a == le) {
            return Ok((*c).into());
        }
    }
    Err(unsupported(s))
}

/// Storage spec of a canonical dtype string.
pub fn dtype_spec(dtype: &str) -> Result<DtypeSpec> {
    let big = dtype.starts_with('>');
    let base: String = if big {
        alloc::format!("<{}", &dtype[1..])
    } else {
        dtype.into()
    };
    if !BASES.contains(&base.as_str()) {
        return Err(unsupported(dtype));
    }
    let kind = base.as_bytes()[1] as char;
    let size: usize = base[2..].parse().map_err(|_| unsupported(dtype))?;
    Ok(DtypeSpec { kind, size, big })
}

/// `str(np.dtype(d))`: `"float64"` for native dtypes, the descr for non-native.
pub fn dtype_name(dtype: &str) -> String {
    match dtype_spec(dtype) {
        Ok(spec) if !spec.big => match spec.kind {
            'b' => "bool".into(),
            'c' => alloc::format!("complex{}", spec.size * 8),
            'f' => alloc::format!("float{}", spec.size * 8),
            'i' => alloc::format!("int{}", spec.size * 8),
            _ => alloc::format!("uint{}", spec.size * 8),
        },
        _ => dtype.into(),
    }
}

fn read_uint(b: &[u8], big: bool) -> u64 {
    let mut v: u64 = 0;
    if big {
        for x in b {
            v = (v << 8) | *x as u64;
        }
    } else {
        for x in b.iter().rev() {
            v = (v << 8) | *x as u64;
        }
    }
    v
}

fn write_uint(out: &mut [u8], v: u64, big: bool) {
    let n = out.len();
    for (i, slot) in out.iter_mut().enumerate() {
        let shift = if big { (n - 1 - i) * 8 } else { i * 8 };
        *slot = (v >> shift) as u8;
    }
}

fn read_float(b: &[u8], big: bool) -> f64 {
    let bits = read_uint(b, big);
    match b.len() {
        2 => half::f16::from_bits(bits as u16).to_f64(),
        4 => f32::from_bits(bits as u32) as f64,
        _ => f64::from_bits(bits),
    }
}

fn write_float(out: &mut [u8], v: f64, big: bool) {
    let bits = match out.len() {
        2 => half::f16::from_f64(v).to_bits() as u64,
        4 => (v as f32).to_bits() as u64,
        _ => v.to_bits(),
    };
    write_uint(out, bits, big);
}

/// `numpy.ndarray`.
#[derive(Clone, Debug)]
pub struct NDArray {
    pub dtype: String,
    pub shape: Vec<usize>,
    pub fortran_order: bool,
    data: Vec<u8>,
}

impl NDArray {
    /// From raw bytes in the dtype's byte order and the given layout
    /// (what `.npy` / `_frombuffer` carry).
    pub fn from_bytes(
        dtype: &str,
        shape: Vec<usize>,
        fortran_order: bool,
        data: Vec<u8>,
    ) -> Result<Self> {
        let dtype = normalize_dtype(Some(dtype))?;
        let spec = dtype_spec(&dtype)?;
        let n: usize = shape.iter().product();
        if data.len() != n * spec.size {
            return Err(LailaError::value_error(alloc::format!(
                "cannot reshape array of size {} into shape ({})",
                data.len() / spec.size.max(1),
                shape
                    .iter()
                    .map(|d| d.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        // numpy: fortran_order is only meaningful for ndim >= 2
        let fortran_order = fortran_order && shape.len() >= 2;
        Ok(NDArray {
            dtype,
            shape,
            fortran_order,
            data,
        })
    }

    /// From decoded scalars given in **storage order** (C order, or Fortran
    /// order when `fortran_order`), like the JS fixture's `data` list.
    pub fn from_scalars(
        dtype: &str,
        shape: Vec<usize>,
        fortran_order: bool,
        values: &[Scalar],
    ) -> Result<Self> {
        let dtype = normalize_dtype(Some(dtype))?;
        let spec = dtype_spec(&dtype)?;
        let n: usize = shape.iter().product();
        if values.len() != n {
            return Err(LailaError::value_error(alloc::format!(
                "cannot reshape array of size {} into shape ({})",
                values.len(),
                shape
                    .iter()
                    .map(|d| d.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        let mut data = alloc::vec![0u8; n * spec.size];
        for (i, v) in values.iter().enumerate() {
            encode(&spec, *v, &mut data[i * spec.size..(i + 1) * spec.size]);
        }
        let fortran_order = fortran_order && shape.len() >= 2;
        Ok(NDArray {
            dtype,
            shape,
            fortran_order,
            data,
        })
    }

    /// Convenience: floats in C order.
    pub fn from_f64s(dtype: &str, shape: Vec<usize>, values: &[f64]) -> Result<Self> {
        let v: Vec<Scalar> = values.iter().map(|f| Scalar::Float(*f)).collect();
        Self::from_scalars(dtype, shape, false, &v)
    }

    /// `np.array(nested, dtype=None)` from a Python (nested list) value.
    pub fn array(nested: &Arg, dtype: Option<&str>) -> Result<Self> {
        if let Arg::NDArray(a) = nested {
            return match dtype {
                None => Ok((**a).clone()),
                Some(d) => a.astype(d),
            };
        }
        let mut shape = Vec::new();
        let mut cur = nested.clone();
        while let Arg::List(_) | Arg::Tuple(_) = &cur {
            let items = cur.__iter__()?;
            shape.push(items.len());
            match items.first() {
                Some(f) => cur = f.clone(),
                None => break,
            }
        }
        let mut flat = Vec::new();
        flatten(nested, &mut flat)?;
        let dtype = match dtype {
            Some(d) => normalize_dtype(Some(d))?,
            None => infer_dtype(&flat),
        };
        let scalars: Vec<Scalar> = flat.iter().map(Scalar::from_arg).collect::<Result<_>>()?;
        Self::from_scalars(&dtype, shape, false, &scalars)
    }

    /// `np.arange(n, dtype=..)`.
    pub fn arange(n: usize, dtype: &str) -> Result<Self> {
        let v: Vec<Scalar> = (0..n).map(|i| Scalar::Int(i as i64)).collect();
        Self::from_scalars(dtype, alloc::vec![n], false, &v)
    }

    /// `np.zeros(shape, dtype=..)`.
    pub fn zeros(shape: Vec<usize>, dtype: &str) -> Result<Self> {
        let dtype = normalize_dtype(Some(dtype))?;
        let spec = dtype_spec(&dtype)?;
        let n: usize = shape.iter().product();
        Ok(NDArray {
            dtype,
            shape,
            fortran_order: false,
            data: alloc::vec![0u8; n * spec.size],
        })
    }

    pub fn ndim(&self) -> usize {
        self.shape.len()
    }
    pub fn size(&self) -> usize {
        self.shape.iter().product()
    }
    pub fn itemsize(&self) -> usize {
        dtype_spec(&self.dtype).map(|s| s.size).unwrap_or(0)
    }
    pub fn nbytes(&self) -> usize {
        self.data.len()
    }
    /// `dtype.byteorder` (`'<'`, `'>'` or `'|'`).
    pub fn byteorder(&self) -> char {
        self.dtype.chars().next().unwrap_or('|')
    }
    pub fn spec(&self) -> DtypeSpec {
        dtype_spec(&self.dtype).expect("canonical dtype")
    }

    /// Element `i` of the flat storage (`flat[i]` in the array's own layout).
    pub fn item(&self, i: usize) -> Scalar {
        let spec = self.spec();
        decode(&spec, &self.data[i * spec.size..(i + 1) * spec.size])
    }

    /// Element `i` in C (row-major) order regardless of layout.
    /// `arr.sum()` of every element (numpy's scalar: `int` for integer and
    /// bool dtypes, `float` for floats, `complex` for complex).
    pub fn sum(&self) -> Scalar {
        let n = self.size();
        let mut is_float = false;
        let mut is_complex = false;
        let mut fi: i128 = 0;
        let (mut re, mut im) = (0.0f64, 0.0f64);
        for i in 0..n {
            match self.item_c(i) {
                Scalar::Bool(b) => fi += b as i128,
                Scalar::Int(v) => fi += v as i128,
                Scalar::UInt(v) => fi += v as i128,
                Scalar::Float(f) => {
                    is_float = true;
                    re += f;
                }
                Scalar::Complex(a, b) => {
                    is_complex = true;
                    re += a;
                    im += b;
                }
            }
        }
        if is_complex {
            Scalar::Complex(re + fi as f64, im)
        } else if is_float {
            Scalar::Float(re + fi as f64)
        } else if let Ok(i) = i64::try_from(fi) {
            Scalar::Int(i)
        } else {
            Scalar::UInt(u64::try_from(fi).unwrap_or(u64::MAX))
        }
    }

    pub fn item_c(&self, i: usize) -> Scalar {
        if !self.fortran_order {
            return self.item(i);
        }
        self.item(c_to_f_index(i, &self.shape))
    }

    /// Raw storage bytes: the dtype's byte order, the array's own layout
    /// (`tobytes(order="A")`).
    pub fn raw_bytes(&self) -> &[u8] {
        &self.data
    }

    /// `ndarray.tobytes(order="C")`; `'F'` Fortran order; `'A'` own layout.
    pub fn tobytes(&self, order: char) -> Result<Vec<u8>> {
        match order {
            'A' => Ok(self.data.clone()),
            'C' => Ok(self.to_c_order().data),
            'F' => Ok(self.to_f_order().data),
            o => Err(LailaError::value_error(alloc::format!(
                "order must be one of 'C', 'F', 'A'; got {o}"
            ))),
        }
    }

    /// A C-ordered copy (or clone when already C-ordered).
    pub fn to_c_order(&self) -> NDArray {
        if !self.fortran_order {
            return self.clone();
        }
        let size = self.spec().size;
        let n = self.size();
        let mut out = alloc::vec![0u8; self.data.len()];
        for c in 0..n {
            let f = c_to_f_index(c, &self.shape);
            out[c * size..(c + 1) * size].copy_from_slice(&self.data[f * size..(f + 1) * size]);
        }
        NDArray {
            dtype: self.dtype.clone(),
            shape: self.shape.clone(),
            fortran_order: false,
            data: out,
        }
    }

    /// A Fortran-ordered copy.
    pub fn to_f_order(&self) -> NDArray {
        if self.fortran_order || self.shape.len() < 2 {
            return self.clone();
        }
        let size = self.spec().size;
        let n = self.size();
        let mut out = alloc::vec![0u8; self.data.len()];
        for c in 0..n {
            let f = c_to_f_index(c, &self.shape);
            out[f * size..(f + 1) * size].copy_from_slice(&self.data[c * size..(c + 1) * size]);
        }
        NDArray {
            dtype: self.dtype.clone(),
            shape: self.shape.clone(),
            fortran_order: true,
            data: out,
        }
    }

    /// `ndarray.tolist()` as a Python value (nested lists; 0-d -> the scalar).
    pub fn tolist(&self) -> Arg {
        let c = self.to_c_order();
        let n = c.size();
        let vals: Vec<Arg> = (0..n).map(|i| c.item(i).into_arg()).collect();
        if c.shape.is_empty() {
            return vals.into_iter().next().unwrap_or(Arg::None);
        }
        build_nested(&c.shape, 0, 0, &vals)
    }

    /// `arr.reshape(2, 3)` / `arr.reshape((2, 3))` -- Python-shaped.
    pub fn reshape(&self, args: impl Into<Args>) -> Result<NDArray> {
        let args: Args = args.into();
        let first = args.positional.first().cloned().or_else(|| {
            let s = args.get("shape");
            (!s.is_none()).then_some(s)
        });
        let shape: Vec<usize> = if args.positional.len() > 1 {
            args.positional
                .iter()
                .map(|d| d.as_i64().unwrap_or(0).max(0) as usize)
                .collect()
        } else {
            first
                .ok_or_else(|| {
                    LailaError::type_error("reshape() missing required argument 'shape'")
                })?
                .__iter__()?
                .iter()
                .map(|d| d.as_i64().unwrap_or(0).max(0) as usize)
                .collect()
        };
        self.reshaped(shape)
    }

    /// Typed `reshape`: a new C-ordered view with `shape`.
    pub fn reshaped(&self, shape: Vec<usize>) -> Result<NDArray> {
        let n: usize = shape.iter().product();
        if n != self.size() {
            return Err(LailaError::value_error(alloc::format!(
                "cannot reshape array of size {} into shape ({})",
                self.size(),
                shape
                    .iter()
                    .map(|d| d.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        let fortran_order = self.fortran_order && shape.len() >= 2;
        Ok(NDArray {
            dtype: self.dtype.clone(),
            shape,
            fortran_order,
            data: self.data.clone(),
        })
    }

    /// `ndarray.astype(dtype)` (complex -> real discards the imaginary part).
    pub fn astype(&self, dtype: &str) -> Result<NDArray> {
        let target = normalize_dtype(Some(dtype))?;
        let n = self.size();
        let vals: Vec<Scalar> = (0..n).map(|i| self.item(i)).collect();
        Self::from_scalars(&target, self.shape.clone(), self.fortran_order, &vals)
    }

    pub fn flatten(&self) -> NDArray {
        let c = self.to_c_order();
        NDArray {
            dtype: c.dtype,
            shape: alloc::vec![c.data.len() / self.spec().size.max(1)],
            fortran_order: false,
            data: c.data,
        }
    }

    /// `np.array_equal(self, other, equal_nan=False)`: same shape and
    /// element-wise equal values (dtype may differ, like numpy).
    pub fn array_equal(&self, other: &NDArray, equal_nan: bool) -> bool {
        if self.shape != other.shape {
            return false;
        }
        let n = self.size();
        (0..n).all(|i| scalar_eq(self.item_c(i), other.item_c(i), equal_nan))
    }

    /// `repr(arr)` (shape-faithful, numpy spelling of the dtype).
    pub fn __repr__(&self) -> String {
        alloc::format!(
            "array({}, dtype={})",
            self.tolist().__repr__(),
            dtype_name(&self.dtype)
        )
    }
}

impl PartialEq for NDArray {
    fn eq(&self, other: &Self) -> bool {
        self.array_equal(other, false)
    }
}
/// `e.data == val` with `e.data` an `Arg` and `val` a typed array: Python's
/// `==` on arrays is element-wise; `assert` needs `.all()`, which is what
/// `np.array_equal` gives when the shapes agree.
impl PartialEq<NDArray> for Arg {
    fn eq(&self, other: &NDArray) -> bool {
        match self {
            Arg::Object(o) => o
                .as_any()
                .downcast_ref::<NDArray>()
                .is_some_and(|a| a.array_equal(other, false)),
            _ => false,
        }
    }
}
impl PartialEq<Arg> for NDArray {
    fn eq(&self, other: &Arg) -> bool {
        other == self
    }
}

// --------------------------------------------------------------------------
// Element-wise arithmetic (`arr + arr`, `1j * arr`, `tiny / 2`)
// --------------------------------------------------------------------------

/// A binary arithmetic operator on numpy scalars / arrays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl NumOp {
    fn apply_f64(self, a: f64, b: f64) -> f64 {
        match self {
            NumOp::Add => a + b,
            NumOp::Sub => a - b,
            NumOp::Mul => a * b,
            NumOp::Div => a / b,
        }
    }
    fn apply_complex(self, a: (f64, f64), b: (f64, f64)) -> (f64, f64) {
        match self {
            NumOp::Add => (a.0 + b.0, a.1 + b.1),
            NumOp::Sub => (a.0 - b.0, a.1 - b.1),
            NumOp::Mul => (a.0 * b.0 - a.1 * b.1, a.0 * b.1 + a.1 * b.0),
            NumOp::Div => {
                let d = b.0 * b.0 + b.1 * b.1;
                ((a.0 * b.0 + a.1 * b.1) / d, (a.1 * b.0 - a.0 * b.1) / d)
            }
        }
    }
    fn apply_scalar(self, a: Scalar, b: Scalar) -> Scalar {
        match (a, b) {
            (Scalar::Complex(..), _) | (_, Scalar::Complex(..)) => {
                let (re, im) = self.apply_complex(complex_parts(a), complex_parts(b));
                Scalar::Complex(re, im)
            }
            (Scalar::Float(_), _) | (_, Scalar::Float(_)) => {
                Scalar::Float(self.apply_f64(a.as_f64(), b.as_f64()))
            }
            _ if self == NumOp::Div => Scalar::Float(self.apply_f64(a.as_f64(), b.as_f64())),
            (Scalar::UInt(x), Scalar::UInt(y)) => Scalar::UInt(match self {
                NumOp::Add => x.wrapping_add(y),
                NumOp::Sub => x.wrapping_sub(y),
                _ => x.wrapping_mul(y),
            }),
            _ => {
                let (x, y) = (int_of(a), int_of(b));
                Scalar::Int(match self {
                    NumOp::Add => x.wrapping_add(y),
                    NumOp::Sub => x.wrapping_sub(y),
                    _ => x.wrapping_mul(y),
                })
            }
        }
    }
}

fn complex_parts(s: Scalar) -> (f64, f64) {
    match s {
        Scalar::Complex(re, im) => (re, im),
        other => (other.as_f64(), 0.0),
    }
}

fn int_of(s: Scalar) -> i64 {
    match s {
        Scalar::Bool(b) => b as i64,
        Scalar::Int(i) => i,
        Scalar::UInt(u) => u as i64,
        Scalar::Float(f) => f as i64,
        Scalar::Complex(re, _) => re as i64,
    }
}

/// numpy's result dtype for an element-wise operation between two operands
/// (`<f8` + `<f8` -> `<f8`, `complex` * `<f8` -> `<c16`, int / int -> `<f8`,
/// a Python scalar adopts the array's dtype).
fn result_dtype(op: NumOp, a: &Operand, b: &Operand) -> String {
    let kind = |o: &Operand| -> (char, usize) {
        match o {
            Operand::Array(n) => {
                let spec = n.spec();
                (spec.kind, spec.size)
            }
            Operand::Scalar(Scalar::Bool(_)) => ('b', 1),
            Operand::Scalar(Scalar::Int(_)) => ('i', 8),
            Operand::Scalar(Scalar::UInt(_)) => ('u', 8),
            Operand::Scalar(Scalar::Float(_)) => ('f', 8),
            Operand::Scalar(Scalar::Complex(..)) => ('c', 16),
        }
    };
    let ((ka, sa), (kb, sb)) = (kind(a), kind(b));
    let rank = |k: char| match k {
        'b' => 0,
        'u' => 1,
        'i' => 2,
        'f' => 3,
        _ => 4,
    };
    // A Python scalar (`arr * 2`, `arr + 0.5`) does not widen the array.
    let (k, s) = match (a, b) {
        (Operand::Array(_), Operand::Scalar(_)) if rank(ka) >= rank(kb) => (ka, sa),
        (Operand::Scalar(_), Operand::Array(_)) if rank(kb) >= rank(ka) => (kb, sb),
        _ => {
            if rank(ka) >= rank(kb) {
                (ka, sa.max(if ka == kb { sb } else { 0 }))
            } else {
                (kb, sb.max(if ka == kb { sa } else { 0 }))
            }
        }
    };
    let (k, s) = match (op, k) {
        (NumOp::Div, 'b' | 'i' | 'u') => ('f', 8),
        (_, 'b') if op != NumOp::Mul => ('i', 8),
        _ => (k, s),
    };
    if k == 'b' {
        "|b1".into()
    } else if s == 1 {
        alloc::format!("|{k}1")
    } else {
        alloc::format!("<{k}{s}")
    }
}

enum Operand {
    Array(NDArray),
    Scalar(Scalar),
}

fn operand(a: &Arg) -> Option<Operand> {
    match a {
        Arg::NDArray(n) => Some(Operand::Array((**n).clone())),
        other => Scalar::from_arg(other).ok().map(Operand::Scalar),
    }
}

/// `a <op> b` when at least one side is an `ndarray` or a `complex`:
/// element-wise over equal shapes (or a scalar broadcast), the numpy result
/// dtype. `None` when neither side is numeric in that sense.
pub fn numeric_binop(op: NumOp, a: &Arg, b: &Arg) -> Option<Result<Arg>> {
    let involved = matches!(a, Arg::NDArray(_))
        || matches!(b, Arg::NDArray(_))
        || a.downcast_ref::<Complex>().is_some()
        || b.downcast_ref::<Complex>().is_some();
    if !involved {
        return None;
    }
    let (x, y) = (operand(a)?, operand(b)?);
    Some(numeric_binop_inner(op, x, y))
}

fn numeric_binop_inner(op: NumOp, x: Operand, y: Operand) -> Result<Arg> {
    if let (Operand::Scalar(a), Operand::Scalar(b)) = (&x, &y) {
        if op == NumOp::Div && b.as_f64() == 0.0 && !matches!(b, Scalar::Complex(..)) {
            return Err(LailaError::new(
                crate::python::errors::ExcKind::ZeroDivisionError,
                "complex division by zero",
            ));
        }
        return Ok(op.apply_scalar(*a, *b).into_arg());
    }
    let dtype = result_dtype(op, &x, &y);
    let (shape, fortran) = match (&x, &y) {
        (Operand::Array(a), Operand::Array(b)) => {
            if a.shape != b.shape {
                return Err(LailaError::value_error(alloc::format!(
                    "operands could not be broadcast together with shapes ({}) ({})",
                    shape_text(&a.shape),
                    shape_text(&b.shape)
                )));
            }
            (a.shape.clone(), a.fortran_order)
        }
        (Operand::Array(a), _) | (_, Operand::Array(a)) => (a.shape.clone(), a.fortran_order),
        _ => unreachable!(),
    };
    let n: usize = shape.iter().product();
    let at = |o: &Operand, i: usize| -> Scalar {
        match o {
            Operand::Array(a) => a.item_c(i),
            Operand::Scalar(s) => *s,
        }
    };
    let values: Vec<Scalar> = (0..n)
        .map(|i| op.apply_scalar(at(&x, i), at(&y, i)))
        .collect();
    let out = NDArray::from_scalars(&dtype, shape, false, &values)?;
    Ok(if fortran { out.to_f_order() } else { out }.into_arg())
}

fn shape_text(shape: &[usize]) -> String {
    let s = shape
        .iter()
        .map(|d| d.to_string())
        .collect::<Vec<_>>()
        .join(",");
    if shape.len() == 1 {
        alloc::format!("{s},")
    } else {
        s
    }
}

impl IntoArg for NDArray {
    fn into_arg(self) -> Arg {
        Arg::NDArray(Arc::new(self))
    }
}
impl PyArg for NDArray {
    fn to_arg(&self) -> Arg {
        Arg::NDArray(Arc::new(self.clone()))
    }
}

/// Attribute-style accessors (Tier 2 lowers `a.shape` to `a.shape()`).
impl NDArray {
    /// `a.shape` as a Python tuple.
    pub fn shape(&self) -> Arg {
        Arg::Tuple(
            self.shape
                .iter()
                .map(|d| Arg::Int(*d as i64))
                .collect::<Vec<_>>()
                .into(),
        )
    }
    /// `a.dtype` -- compares equal to its descr string and its numpy name.
    pub fn dtype(&self) -> crate::python::stdlib::Dtype {
        crate::python::stdlib::Dtype(self.dtype.clone())
    }
    /// `a.flags['F_CONTIGUOUS']` stand-in: the layout flag.
    pub fn fortran_order(&self) -> bool {
        self.fortran_order
    }
    /// `a.real`: the real parts (the array itself for real dtypes).
    pub fn real(&self) -> Result<NDArray> {
        self.complex_part(false)
    }
    /// `a.imag`: the imaginary parts (zeros for real dtypes).
    pub fn imag(&self) -> Result<NDArray> {
        self.complex_part(true)
    }
    fn complex_part(&self, imag: bool) -> Result<NDArray> {
        let spec = self.spec();
        if spec.kind != 'c' {
            if !imag {
                return Ok(self.clone());
            }
            return NDArray::zeros(self.shape.clone(), &self.dtype);
        }
        let dtype = alloc::format!("{}f{}", &self.dtype[..1], spec.size / 2);
        let vals: Vec<Scalar> = (0..self.size())
            .map(|i| match self.item(i) {
                Scalar::Complex(re, im) => Scalar::Float(if imag { im } else { re }),
                other => Scalar::Float(if imag { 0.0 } else { other.as_f64() }),
            })
            .collect();
        NDArray::from_scalars(&dtype, self.shape.clone(), self.fortran_order, &vals)
    }
    /// `a[i]`: an integer index along the first axis -- the element of a
    /// 1-d array (a Python scalar), the sub-array of an n-d one (a copy).
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        let key = key.into_arg();
        let Some(i) = key.as_i64() else {
            return Err(LailaError::new(
                crate::python::errors::ExcKind::IndexError,
                alloc::format!(
                    "only integers, slices (`:`), ellipsis (`...`), numpy.newaxis (`None`) and integer or boolean arrays are valid indices (got {})",
                    key.type_name()
                ),
            ));
        };
        if self.shape.is_empty() {
            return Err(LailaError::new(
                crate::python::errors::ExcKind::IndexError,
                "too many indices for array: array is 0-dimensional, but 1 were indexed",
            ));
        }
        let n = self.shape[0];
        let idx = if i < 0 { i + n as i64 } else { i };
        if idx < 0 || idx as usize >= n {
            return Err(LailaError::new(
                crate::python::errors::ExcKind::IndexError,
                alloc::format!("index {i} is out of bounds for axis 0 with size {n}"),
            ));
        }
        let idx = idx as usize;
        let c = self.to_c_order();
        if self.shape.len() == 1 {
            return Ok(c.item(idx).into_arg());
        }
        let inner: usize = self.shape[1..].iter().product();
        let vals: Vec<Scalar> = (idx * inner..(idx + 1) * inner)
            .map(|k| c.item(k))
            .collect();
        Ok(NDArray::from_scalars(&self.dtype, self.shape[1..].to_vec(), false, &vals)?.into_arg())
    }
    /// `a.T` for 2-d arrays (transposed view materialised as a copy).
    #[allow(non_snake_case)]
    pub fn T(&self) -> NDArray {
        if self.shape.len() != 2 {
            return self.clone();
        }
        let f = self.to_f_order();
        NDArray {
            dtype: f.dtype,
            shape: alloc::vec![self.shape[1], self.shape[0]],
            fortran_order: false,
            data: f.data,
        }
    }
}

/// `arr.name` on an `Arg::NDArray`.
pub fn getattr(a: &NDArray, name: &str) -> Result<Arg> {
    Ok(match name {
        "shape" => a.shape(),
        "dtype" => a.dtype().into_arg(),
        "ndim" => Arg::Int(a.ndim() as i64),
        "size" => Arg::Int(a.size() as i64),
        "nbytes" => Arg::Int(a.nbytes() as i64),
        "itemsize" => Arg::Int(a.itemsize() as i64),
        "fortran_order" => Arg::Bool(a.fortran_order),
        "T" => a.T().into_arg(),
        "real" => a.real()?.into_arg(),
        "imag" => a.imag()?.into_arg(),
        _ => {
            return Err(LailaError::attribute_error(alloc::format!(
                "'numpy.ndarray' object has no attribute '{name}'"
            )))
        }
    })
}

/// `arr.name(*args)` on an `Arg::NDArray`.
pub fn call_method(a: &NDArray, name: &str, args: Args) -> Result<Arg> {
    let first = args.positional.first().cloned();
    Ok(match name {
        "tolist" => a.tolist(),
        "tobytes" => {
            let order = first
                .or_else(|| Some(args.get("order")))
                .filter(|o| !o.is_none());
            let order = order.map(|o| o.__str__()).unwrap_or_else(|| "C".into());
            crate::python::object::bytes(a.tobytes(order.chars().next().unwrap_or('C'))?)
        }
        "reshape" => a.reshape(args)?.into_arg(),
        "astype" => {
            let d = first.ok_or_else(|| {
                LailaError::type_error("astype() missing required argument 'dtype'")
            })?;
            let d = match &d {
                Arg::Object(o) => o.__repr__(),
                other => other.__str__(),
            };
            let d = match crate::python::stdlib::Dtype::from_arg_str(&d) {
                Some(descr) => descr,
                None => d,
            };
            a.astype(&d)?.into_arg()
        }
        "flatten" | "ravel" => a.flatten().into_arg(),
        "copy" => a.clone().into_arg(),
        "item" => {
            let i = first.and_then(|i| i.as_i64()).unwrap_or(0);
            a.item_c(i.max(0) as usize).into_arg()
        }
        "transpose" => a.T().into_arg(),
        // `arr.sum()` over the whole array: numpy's scalar (an int for
        // integer / bool dtypes, a float for float dtypes, complex for
        // complex ones).
        "sum" => a.sum().into_arg(),
        "__eq__" => Arg::Bool(
            first
                .and_then(|o| o.as_ndarray().map(|o| a.array_equal(o, false)))
                .unwrap_or(false),
        ),
        _ => {
            return Err(LailaError::attribute_error(alloc::format!(
                "'numpy.ndarray' object has no attribute '{name}'"
            )))
        }
    })
}

fn scalar_eq(x: Scalar, y: Scalar, equal_nan: bool) -> bool {
    match (x, y) {
        (Scalar::Complex(a, b), other) | (other, Scalar::Complex(a, b)) => {
            let (c, d) = match other {
                Scalar::Complex(c, d) => (c, d),
                o => (o.as_f64(), 0.0),
            };
            scalar_eq(Scalar::Float(a), Scalar::Float(c), equal_nan)
                && scalar_eq(Scalar::Float(b), Scalar::Float(d), equal_nan)
        }
        (Scalar::Int(a), Scalar::Int(b)) => a == b,
        (Scalar::UInt(a), Scalar::UInt(b)) => a == b,
        (Scalar::Int(a), Scalar::UInt(b)) | (Scalar::UInt(b), Scalar::Int(a)) => {
            a >= 0 && a as u64 == b
        }
        (Scalar::Bool(a), Scalar::Bool(b)) => a == b,
        (a, b) => {
            let (x, y) = (a.as_f64(), b.as_f64());
            if equal_nan && x.is_nan() && y.is_nan() {
                return true;
            }
            x == y
        }
    }
}

/// C-order flat index -> Fortran-order flat index for `shape`.
fn c_to_f_index(c: usize, shape: &[usize]) -> usize {
    let nd = shape.len();
    let mut rem = c;
    let mut idx = alloc::vec![0usize; nd];
    for d in (0..nd).rev() {
        idx[d] = rem % shape[d];
        rem /= shape[d];
    }
    let mut f = 0;
    let mut mul = 1;
    for d in 0..nd {
        f += idx[d] * mul;
        mul *= shape[d];
    }
    f
}

fn build_nested(shape: &[usize], dim: usize, offset: usize, vals: &[Arg]) -> Arg {
    let len = shape[dim];
    if dim == shape.len() - 1 {
        return vals[offset..offset + len].to_vec().into_arg();
    }
    let stride: usize = shape[dim + 1..].iter().product();
    let out: Vec<Arg> = (0..len)
        .map(|i| build_nested(shape, dim + 1, offset + i * stride, vals))
        .collect();
    out.into_arg()
}

fn flatten(x: &Arg, out: &mut Vec<Arg>) -> Result<()> {
    match x {
        Arg::List(_) | Arg::Tuple(_) => {
            for v in x.__iter__()? {
                flatten(&v, out)?;
            }
        }
        other => out.push(other.clone()),
    }
    Ok(())
}

fn infer_dtype(flat: &[Arg]) -> String {
    if !flat.is_empty() && flat.iter().all(|x| matches!(x, Arg::Bool(_))) {
        return "|b1".into();
    }
    if flat
        .iter()
        .any(|x| matches!(x, Arg::Object(o) if o.as_any().is::<Complex>()))
    {
        return "<c16".into();
    }
    if !flat.is_empty() && flat.iter().all(|x| matches!(x, Arg::Int(_) | Arg::Long(_))) {
        return "<i8".into();
    }
    "<f8".into()
}

fn decode(spec: &DtypeSpec, b: &[u8]) -> Scalar {
    match spec.kind {
        'b' => Scalar::Bool(b[0] != 0),
        'u' => Scalar::UInt(read_uint(b, spec.big)),
        'i' => {
            let u = read_uint(b, spec.big);
            let shift = 64 - spec.size * 8;
            Scalar::Int(((u << shift) as i64) >> shift)
        }
        'f' => Scalar::Float(read_float(b, spec.big)),
        _ => {
            let w = spec.lane_size();
            Scalar::Complex(read_float(&b[..w], spec.big), read_float(&b[w..], spec.big))
        }
    }
}

fn encode(spec: &DtypeSpec, v: Scalar, out: &mut [u8]) {
    match spec.kind {
        'b' => {
            out[0] = match v {
                Scalar::Bool(b) => b as u8,
                Scalar::Int(i) => (i != 0) as u8,
                Scalar::UInt(u) => (u != 0) as u8,
                Scalar::Float(f) => (f != 0.0) as u8,
                Scalar::Complex(a, b) => (a != 0.0 || b != 0.0) as u8,
            }
        }
        'u' => {
            let u = match v {
                Scalar::Bool(b) => b as u64,
                Scalar::Int(i) => i as u64,
                Scalar::UInt(u) => u,
                Scalar::Float(f) => f as i64 as u64,
                Scalar::Complex(a, _) => a as i64 as u64,
            };
            write_uint(out, u, spec.big);
        }
        'i' => {
            let i = match v {
                Scalar::Bool(b) => b as i64,
                Scalar::Int(i) => i,
                Scalar::UInt(u) => u as i64,
                Scalar::Float(f) => f as i64,
                Scalar::Complex(a, _) => a as i64,
            };
            write_uint(out, i as u64, spec.big);
        }
        'f' => write_float(out, v.as_f64(), spec.big),
        _ => {
            let (re, im) = match v {
                Scalar::Complex(a, b) => (a, b),
                o => (o.as_f64(), 0.0),
            };
            let w = spec.lane_size();
            write_float(&mut out[..w], re, spec.big);
            write_float(&mut out[w..], im, spec.big);
        }
    }
}
