//! `import numpy as np` -- the subset laila and its tests use, over
//! `_compat::ndarray::NDArray` (`np.array`, `np.arange`, `np.zeros`,
//! `np.linspace`, `np.array_equal`, `np.asfortranarray`, dtype names,
//! `np.nan` / `np.inf`, `np.testing.assert_array_equal`).

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::_compat::ndarray::{dtype_name, normalize_dtype, NDArray};
pub use crate::_compat::nprandom::Generator;
use crate::python::args::Args;
use crate::python::errors::{ExcKind, LailaError, Result};
use crate::python::object::{Arg, IntoArg, PyObject};

/// A numpy dtype / scalar-type name (`np.float64`, `np.int32`, `np.dtype("<f8")`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Dtype(pub String);

impl PyObject for Dtype {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "dtype"
    }
    /// `repr(np.dtype("<f8"))` -> `dtype('float64')`.
    fn __repr__(&self) -> String {
        alloc::format!("dtype('{}')", self.name())
    }
    /// `str(np.dtype("<f8"))` -> `float64`.
    fn __str__(&self) -> String {
        self.name()
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Dtype>()
            .is_some_and(|o| o.0 == self.0)
    }
    /// numpy: `np.dtype("<f8") == "float64"` and `== np.float64` are `True`.
    fn __eq_arg__(&self, other: &Arg) -> bool {
        match other {
            Arg::Object(o) => self.__eq__(&**o),
            Arg::Str(s) => normalize_dtype(Some(s))
                .map(|d| d == self.0)
                .unwrap_or(false),
            _ => false,
        }
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::from(self.0.as_str()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "name" => Arg::from(self.name().as_str()),
            "str" => Arg::from(self.str().as_str()),
            "itemsize" => Arg::Int(self.itemsize() as i64),
            "byteorder" => Arg::from(self.byteorder().as_str()),
            "kind" => Arg::from(&self.0[1..2]),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'numpy.dtype' object has no attribute '{name}'"
                )))
            }
        })
    }
}
impl IntoArg for Dtype {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self))
    }
}
impl crate::python::PyArg for Dtype {
    fn to_arg(&self) -> Arg {
        self.clone().into_arg()
    }
}
impl crate::python::builtins::IsInstance for Dtype {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<Dtype>())
    }
}
impl PartialEq<Arg> for Dtype {
    fn eq(&self, other: &Arg) -> bool {
        self.__eq_arg__(other)
    }
}
/// `out.dtype == np.float32` with `out` an `Arg`.
impl PartialEq<Dtype> for Arg {
    fn eq(&self, other: &Dtype) -> bool {
        other.__eq_arg__(self)
    }
}
impl PartialEq<&str> for Dtype {
    fn eq(&self, other: &&str) -> bool {
        self.__eq_arg__(&Arg::from(*other))
    }
}

impl Dtype {
    /// The descr behind a stringified dtype argument: accepts `"float64"`,
    /// `"<f8"` and the `repr` form `dtype('float64')`; `None` if it is not
    /// a dtype spelling.
    pub fn from_arg_str(s: &str) -> Option<String> {
        let inner = s
            .strip_prefix("dtype('")
            .and_then(|r| r.strip_suffix("')"))
            .unwrap_or(s);
        normalize_dtype(Some(inner)).ok()
    }
    /// `str(np.dtype(..))` -- "float64" for native, the descr for non-native.
    pub fn name(&self) -> String {
        dtype_name(&self.0)
    }
    /// `np.dtype(..).str` -- the descr (`'<f8'`).
    pub fn str(&self) -> String {
        self.0.clone()
    }
    pub fn itemsize(&self) -> usize {
        crate::_compat::ndarray::dtype_spec(&self.0)
            .map(|s| s.size)
            .unwrap_or(0)
    }
    pub fn byteorder(&self) -> String {
        self.0[..1].to_string()
    }
    /// `np.dtype(..).kind`: `b` / `i` / `u` / `f` / `c`.
    pub fn kind(&self) -> String {
        self.0[1..2].to_string()
    }
    pub fn __str__(&self) -> String {
        self.name()
    }
}
impl core::fmt::Display for Dtype {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.name())
    }
}

fn dtype_of(a: &Arg) -> Result<Option<String>> {
    Ok(match a {
        Arg::None => None,
        Arg::Str(s) => Some(normalize_dtype(Some(s))?),
        Arg::Object(o) => match o.as_any().downcast_ref::<Dtype>() {
            Some(d) => Some(d.0.clone()),
            None => return Err(LailaError::type_error("data type not understood")),
        },
        _ => return Err(LailaError::type_error("data type not understood")),
    })
}

fn shape_of(a: &Arg) -> Result<Vec<usize>> {
    match a {
        Arg::Int(n) => Ok(alloc::vec![(*n).max(0) as usize]),
        other => other
            .__iter__()?
            .iter()
            .map(|d| {
                d.as_i64().map(|v| v.max(0) as usize).ok_or_else(|| {
                    LailaError::type_error("'float' object cannot be interpreted as an integer")
                })
            })
            .collect(),
    }
}

fn to_ndarray(a: &Arg) -> Result<NDArray> {
    match a {
        Arg::NDArray(n) => Ok((**n).clone()),
        other => NDArray::array(other, None),
    }
}

/// `np.random` -- `default_rng(seed)` (the `Generator` / `PCG64` /
/// `SeedSequence` stack of `_compat::nprandom`, numpy's exact stream).
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct np_random;
impl np_random {
    /// `np.random.default_rng(seed=None)`
    pub fn default_rng(&self, args: impl Into<Args>) -> Result<Generator> {
        let a: Args = args.into();
        let seed = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("seed"));
        Generator::default_rng(&seed)
    }
}

/// `np.testing`
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct np_testing;
impl np_testing {
    pub fn assert_array_equal(&self, a: impl IntoArg, b: impl IntoArg) -> Result<()> {
        let (x, y) = (to_ndarray(&a.into_arg())?, to_ndarray(&b.into_arg())?);
        if !x.array_equal(&y, true) {
            return Err(LailaError::new(
                ExcKind::AssertionError,
                alloc::format!(
                    "\nArrays are not equal\n\n x: {}\n y: {}",
                    x.__repr__(),
                    y.__repr__()
                ),
            ));
        }
        Ok(())
    }
    pub fn assert_equal(&self, a: impl IntoArg, b: impl IntoArg) -> Result<()> {
        self.assert_array_equal(a, b)
    }
    pub fn assert_allclose(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        let x = to_ndarray(&a.positional.first().cloned().unwrap_or(Arg::None))?;
        let y = to_ndarray(&a.positional.get(1).cloned().unwrap_or(Arg::None))?;
        let rtol = a.get("rtol").as_f64().unwrap_or(1e-7);
        let atol = a.get("atol").as_f64().unwrap_or(0.0);
        if x.shape != y.shape {
            return Err(LailaError::new(
                ExcKind::AssertionError,
                "\nArrays are not equal\n\n(shapes differ)",
            ));
        }
        for i in 0..x.size() {
            let (p, q) = (x.item_c(i).as_f64(), y.item_c(i).as_f64());
            if p.is_nan() && q.is_nan() {
                continue;
            }
            if (p - q).abs() > atol + rtol * q.abs() {
                return Err(LailaError::new(
                    ExcKind::AssertionError,
                    alloc::format!(
                        "\nNot equal to tolerance rtol={rtol}, atol={atol}\n\n x: {}\n y: {}",
                        x.__repr__(),
                        y.__repr__()
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// `import numpy as np`
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct np_mod;
pub static np: np_mod = np_mod;

/// `np.ndarray` as a *type* (`isinstance(x, np.ndarray)`).
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ndarray;
impl crate::python::builtins::IsInstance for ndarray {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::NDArray(_))
    }
}

impl np_mod {
    /// `np.ndarray` (the class, for `isinstance`).
    pub fn ndarray(&self) -> ndarray {
        ndarray
    }
    /// `np.testing`
    pub fn testing(&self) -> np_testing {
        np_testing
    }
    /// `np.random`
    pub fn random(&self) -> np_random {
        np_random
    }
    /// `np.nan`
    pub fn nan(&self) -> f64 {
        f64::NAN
    }
    #[allow(non_snake_case)]
    pub fn NaN(&self) -> f64 {
        f64::NAN
    }
    /// `np.inf`
    pub fn inf(&self) -> f64 {
        f64::INFINITY
    }
    #[allow(non_snake_case)]
    pub fn Inf(&self) -> f64 {
        f64::INFINITY
    }
    pub fn pi(&self) -> f64 {
        core::f64::consts::PI
    }
    pub fn e(&self) -> f64 {
        core::f64::consts::E
    }
}

macro_rules! dtype_consts {
    ($($name:ident => $descr:literal),* $(,)?) => {
        impl np_mod {
            $(
                #[doc = concat!("`np.", stringify!($name), "`")]
                #[allow(non_snake_case)]
                pub fn $name(&self) -> Dtype { Dtype($descr.into()) }
            )*
        }
    };
}
dtype_consts! {
    float64 => "<f8", float32 => "<f4", float16 => "<f2", float_ => "<f8", double => "<f8", half => "<f2",
    int64 => "<i8", int32 => "<i4", int16 => "<i2", int8 => "|i1", int_ => "<i8", intp => "<i8",
    uint64 => "<u8", uint32 => "<u4", uint16 => "<u2", uint8 => "|u1", uint => "<u8",
    bool_ => "|b1", complex128 => "<c16", complex64 => "<c8",
}

impl np_mod {
    /// `np.array(object, dtype=None)`
    pub fn array(&self, args: impl Into<Args>) -> Result<NDArray> {
        let a: Args = args.into();
        let obj = a.positional.first().cloned().unwrap_or(Arg::None);
        let dt = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("dtype"));
        let dt = dtype_of(&dt)?;
        NDArray::array(&obj, dt.as_deref())
    }
    pub fn asarray(&self, args: impl Into<Args>) -> Result<NDArray> {
        self.array(args)
    }
    /// `np.save(file, arr, allow_pickle=True, fix_imports=<deprecated>)` --
    /// `file` is an `io.BytesIO`; writes the `.npy` v1.0/2.0 bytes.
    pub fn save(&self, args: impl Into<Args>) -> Result<()> {
        let a: Args = args.into();
        let file = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("file"));
        let arr = a.positional.get(1).cloned().unwrap_or_else(|| a.get("arr"));
        let arr = to_ndarray(&arr)?;
        let out = crate::_codecs::npy::save(&arr)?;
        match &file {
            Arg::Object(o) => match o.as_any().downcast_ref::<super::codecs::BytesIO>() {
                Some(b) => b.write(crate::python::object::bytes(&out)).map(|_| ()),
                None => Err(LailaError::type_error(
                    "np.save: file must be an io.BytesIO (laila never writes paths)",
                )),
            },
            _ => Err(LailaError::type_error(
                "np.save: file must be an io.BytesIO (laila never writes paths)",
            )),
        }
    }
    /// `np.load(file, mmap_mode=None, allow_pickle=False, fix_imports=True, encoding='ASCII', *, max_header_size=10000)`
    pub fn load(&self, args: impl Into<Args>) -> Result<NDArray> {
        let a: Args = args.into();
        let file = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("file"));
        let data = super::codecs::file_bytes(&file)?;
        let mhs = a.get("max_header_size");
        match mhs.as_i64() {
            Some(n) => crate::_codecs::npy::load_with(&data, n.max(0) as usize),
            None => crate::_codecs::npy::load(&data),
        }
    }
    /// `np.asfortranarray(a)`
    pub fn asfortranarray(&self, a: impl IntoArg) -> Result<NDArray> {
        Ok(to_ndarray(&a.into_arg())?.to_f_order())
    }
    pub fn ascontiguousarray(&self, a: impl IntoArg) -> Result<NDArray> {
        Ok(to_ndarray(&a.into_arg())?.to_c_order())
    }
    /// `np.arange(n, dtype=None)` / `np.arange(start, stop)`
    pub fn arange(&self, args: impl Into<Args>) -> Result<NDArray> {
        let a: Args = args.into();
        let ints: Vec<i64> = a.positional.iter().filter_map(|v| v.as_i64()).collect();
        let (start, stop) = match ints.as_slice() {
            [n] => (0, *n),
            [s, e, ..] => (*s, *e),
            _ => {
                return Err(LailaError::type_error(
                    "arange() requires stop to be specified.",
                ))
            }
        };
        let dt = dtype_of(&a.get("dtype"))?.unwrap_or_else(|| "<i8".into());
        let v: Vec<crate::_compat::ndarray::Scalar> = (start..stop)
            .map(crate::_compat::ndarray::Scalar::Int)
            .collect();
        NDArray::from_scalars(&dt, alloc::vec![v.len()], false, &v)
    }
    /// `np.zeros(shape, dtype=float)`
    pub fn zeros(&self, args: impl Into<Args>) -> Result<NDArray> {
        let a: Args = args.into();
        let shape = shape_of(&a.positional.first().cloned().unwrap_or(Arg::None))?;
        let dt = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("dtype"));
        NDArray::zeros(shape, &dtype_of(&dt)?.unwrap_or_else(|| "<f8".into()))
    }
    /// `np.ones(shape, dtype=float)`
    pub fn ones(&self, args: impl Into<Args>) -> Result<NDArray> {
        let a: Args = args.into();
        let shape = shape_of(&a.positional.first().cloned().unwrap_or(Arg::None))?;
        let dt = dtype_of(
            &a.positional
                .get(1)
                .cloned()
                .unwrap_or_else(|| a.get("dtype")),
        )?
        .unwrap_or_else(|| "<f8".into());
        let n: usize = shape.iter().product();
        let v = alloc::vec![crate::_compat::ndarray::Scalar::Int(1); n];
        NDArray::from_scalars(&dt, shape, false, &v)
    }
    /// `np.linspace(start, stop, num=50, dtype=None)`
    pub fn linspace(&self, args: impl Into<Args>) -> Result<NDArray> {
        let a: Args = args.into();
        let f = |i: usize, name: &str| -> Result<f64> {
            a.positional
                .get(i)
                .cloned()
                .map(|v| if v.is_none() { a.get(name) } else { v })
                .unwrap_or_else(|| a.get(name))
                .as_f64()
                .ok_or_else(|| {
                    LailaError::type_error(alloc::format!("linspace() missing argument '{name}'"))
                })
        };
        let (start, stop) = (f(0, "start")?, f(1, "stop")?);
        let num = a.positional.get(2).cloned().unwrap_or_else(|| a.get("num"));
        let num = if num.is_none() {
            50
        } else {
            num.as_i64().unwrap_or(50).max(0) as usize
        };
        let dt = dtype_of(&a.get("dtype"))?.unwrap_or_else(|| "<f8".into());
        let step = if num > 1 {
            (stop - start) / (num as f64 - 1.0)
        } else {
            0.0
        };
        let v: Vec<f64> = (0..num)
            .map(|i| {
                if i + 1 == num && num > 1 {
                    stop
                } else {
                    start + step * i as f64
                }
            })
            .collect();
        NDArray::from_f64s(&dt, alloc::vec![num], &v)
    }
    /// `np.array_equal(a1, a2, equal_nan=False)`
    pub fn array_equal(&self, args: impl Into<Args>) -> Result<bool> {
        let a: Args = args.into();
        let x = to_ndarray(&a.positional.first().cloned().unwrap_or(Arg::None))?;
        let y = to_ndarray(&a.positional.get(1).cloned().unwrap_or(Arg::None))?;
        Ok(x.array_equal(&y, a.get("equal_nan").__bool__()))
    }
    pub fn allclose(&self, args: impl Into<Args>) -> Result<bool> {
        Ok(self.testing().assert_allclose(args).is_ok())
    }
    /// `np.dtype(obj)`
    pub fn dtype(&self, d: impl IntoArg) -> Result<Dtype> {
        Ok(Dtype(
            dtype_of(&d.into_arg())?.unwrap_or_else(|| "<f8".into()),
        ))
    }
    pub fn isnan(&self, x: impl IntoArg) -> Result<Arg> {
        let x = x.into_arg();
        Ok(match &x {
            Arg::NDArray(n) => {
                let v: Vec<crate::_compat::ndarray::Scalar> = (0..n.size())
                    .map(|i| crate::_compat::ndarray::Scalar::Bool(n.item_c(i).as_f64().is_nan()))
                    .collect();
                NDArray::from_scalars("|b1", n.shape.clone(), false, &v)?.into_arg()
            }
            other => Arg::Bool(other.as_f64().is_some_and(f64::is_nan)),
        })
    }
    pub fn isinf(&self, x: impl IntoArg) -> bool {
        x.into_arg().as_f64().is_some_and(f64::is_infinite)
    }
    pub fn prod(&self, x: impl IntoArg) -> Result<Arg> {
        let mut acc = 1i64;
        for v in x.into_arg().__iter__()? {
            acc *= v.as_i64().unwrap_or(1);
        }
        Ok(Arg::Int(acc))
    }
    /// `np.nan_to_num(x, nan=0.0, posinf=None, neginf=None)` on scalars and
    /// arrays (element-wise, dtype preserved).
    pub fn nan_to_num(&self, args: impl Into<Args>) -> Result<Arg> {
        use crate::_compat::ndarray::Scalar;
        let a: Args = args.into();
        let x = a.positional.first().cloned().unwrap_or(Arg::None);
        let nan = a.get("nan").as_f64().unwrap_or(0.0);
        let fix = |f: f64, big: f64, small: f64| -> f64 {
            if f.is_nan() {
                nan
            } else if f.is_infinite() && f > 0.0 {
                a.get("posinf").as_f64().unwrap_or(big)
            } else if f.is_infinite() {
                a.get("neginf").as_f64().unwrap_or(small)
            } else {
                f
            }
        };
        if let Arg::NDArray(n) = &x {
            let (big, small) = match n.spec().size {
                2 if n.spec().kind == 'f' => (65504.0, -65504.0),
                4 if n.spec().kind == 'f' => (f32::MAX as f64, f32::MIN as f64),
                8 if n.spec().kind == 'c' => (f32::MAX as f64, f32::MIN as f64),
                _ => (f64::MAX, f64::MIN),
            };
            let v: Vec<Scalar> = (0..n.size())
                .map(|i| match n.item_c(i) {
                    Scalar::Float(f) => Scalar::Float(fix(f, big, small)),
                    Scalar::Complex(re, im) => {
                        Scalar::Complex(fix(re, big, small), fix(im, big, small))
                    }
                    other => other,
                })
                .collect();
            let out = NDArray::from_scalars(&n.dtype, n.shape.clone(), false, &v)?;
            return Ok(if n.fortran_order {
                out.to_f_order()
            } else {
                out
            }
            .into_arg());
        }
        Ok(match x.as_f64() {
            Some(f) if f.is_nan() || f.is_infinite() => Arg::Float(fix(f, f64::MAX, f64::MIN)),
            _ => x,
        })
    }
    /// `np.iinfo(int_type)`: `min` / `max` / `bits` / `dtype` of an integer dtype.
    pub fn iinfo(&self, int_type: impl IntoArg) -> Result<NumInfo> {
        let dtype = dtype_of(&int_type.into_arg())?.unwrap_or_else(|| "<i8".into());
        let spec = crate::_compat::ndarray::dtype_spec(&dtype)?;
        let bits = spec.size * 8;
        let (min, max) = match spec.kind {
            'i' => (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1),
            'u' => (0, (1i128 << bits) - 1),
            _ => {
                return Err(LailaError::value_error(alloc::format!(
                    "Invalid integer data type '{}'.",
                    spec.kind
                )))
            }
        };
        Ok(NumInfo {
            dtype: Dtype(dtype),
            min: int_arg(min),
            max: int_arg(max),
            eps: Arg::None,
            tiny: Arg::None,
            bits,
        })
    }
    /// `np.finfo(float_type)`: `min` / `max` / `eps` / `tiny` / `bits` /
    /// `dtype` of a floating dtype (the float16 / float32 / float64 limits).
    pub fn finfo(&self, float_type: impl IntoArg) -> Result<NumInfo> {
        let dtype = dtype_of(&float_type.into_arg())?.unwrap_or_else(|| "<f8".into());
        let spec = crate::_compat::ndarray::dtype_spec(&dtype)?;
        let lane = if spec.kind == 'c' {
            spec.size / 2
        } else {
            spec.size
        };
        if !matches!(spec.kind, 'f' | 'c') {
            return Err(LailaError::value_error(alloc::format!(
                "data type {} not inexact",
                Dtype(dtype).__repr__()
            )));
        }
        let (max, eps, tiny) = match lane {
            2 => (65504.0, 0.0009765625, 6.103515625e-05),
            4 => (
                f32::MAX as f64,
                f32::EPSILON as f64,
                f32::MIN_POSITIVE as f64,
            ),
            _ => (f64::MAX, f64::EPSILON, f64::MIN_POSITIVE),
        };
        let float_dtype = alloc::format!("<f{lane}");
        Ok(NumInfo {
            dtype: Dtype(float_dtype),
            min: Arg::Float(-max),
            max: Arg::Float(max),
            eps: Arg::Float(eps),
            tiny: Arg::Float(tiny),
            bits: lane * 8,
        })
    }
}

fn int_arg(v: i128) -> Arg {
    match i64::try_from(v) {
        Ok(i) => Arg::Int(i),
        Err(_) => (v as u64).into_arg(),
    }
}

/// `np.iinfo(..)` / `np.finfo(..)`: the limits of a numeric dtype.
#[derive(Clone, Debug, PartialEq)]
pub struct NumInfo {
    pub dtype: Dtype,
    pub min: Arg,
    pub max: Arg,
    pub eps: Arg,
    pub tiny: Arg,
    pub bits: usize,
}
impl NumInfo {
    pub fn min(&self) -> Arg {
        self.min.clone()
    }
    pub fn max(&self) -> Arg {
        self.max.clone()
    }
    pub fn eps(&self) -> Arg {
        self.eps.clone()
    }
    pub fn tiny(&self) -> Arg {
        self.tiny.clone()
    }
    pub fn smallest_normal(&self) -> Arg {
        self.tiny.clone()
    }
    pub fn bits(&self) -> usize {
        self.bits
    }
    pub fn dtype(&self) -> Dtype {
        self.dtype.clone()
    }
}
impl PyObject for NumInfo {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        if self.eps.is_none() {
            "iinfo"
        } else {
            "finfo"
        }
    }
    fn __repr__(&self) -> String {
        if self.eps.is_none() {
            alloc::format!(
                "iinfo(min={}, max={}, dtype={})",
                self.min.__repr__(),
                self.max.__repr__(),
                self.dtype.name()
            )
        } else {
            alloc::format!(
                "finfo(resolution={}, min={}, max={}, dtype={})",
                self.eps.__repr__(),
                self.min.__repr__(),
                self.max.__repr__(),
                self.dtype.name()
            )
        }
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<NumInfo>()
            .is_some_and(|o| o == self)
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::from(self.__repr__().as_str()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "min" => self.min(),
            "max" => self.max(),
            "eps" | "resolution" => self.eps(),
            "tiny" | "smallest_normal" => self.tiny(),
            "bits" => Arg::Int(self.bits as i64),
            "dtype" => self.dtype().into_arg(),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{name}'",
                    self.type_name()
                )))
            }
        })
    }
}
impl IntoArg for NumInfo {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self))
    }
}
impl crate::python::PyArg for NumInfo {
    fn to_arg(&self) -> Arg {
        self.clone().into_arg()
    }
}
