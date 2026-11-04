//! Python builtins with Python's names, for `#[laila::python]` bodies.
//!
//! `print`, `len`, `str`, `repr`, `isinstance`, `range`, `sorted`, `list`,
//! `dict`, `truthy` (the implicit `bool()` in `if`/`not`/`and`/`or`).

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use super::args::Args;
use super::errors::{ExcMarker, LailaError, Result};
use super::object::{Arg, IntoArg};

/// Python `print(*objects, sep=' ', end='\n')`.
pub fn print(args: impl Into<Args>) -> Result<()> {
    let a: Args = args.into();
    let mut sep = String::from(" ");
    let mut end = String::from("\n");
    for (k, v) in &a.keywords {
        match k.as_str() {
            "sep" => sep = v.__str__(),
            "end" => end = v.__str__(),
            "file" | "flush" => {}
            other => {
                return Err(LailaError::type_error(alloc::format!(
                    "'{other}' is an invalid keyword argument for print()"
                )))
            }
        }
    }
    let parts: Vec<String> = a.positional.iter().map(|v| v.__str__()).collect();
    super::io::write_stdout(&alloc::format!("{}{}", parts.join(&sep), end));
    Ok(())
}

/// Python `len(x)`.
pub fn len<T: PyLen + ?Sized>(x: &T) -> Result<usize> {
    x.__len__()
}
pub trait PyLen {
    fn __len__(&self) -> Result<usize>;
}
impl PyLen for Arg {
    fn __len__(&self) -> Result<usize> {
        Arg::__len__(self)
    }
}
impl<T> PyLen for Vec<T> {
    fn __len__(&self) -> Result<usize> {
        Ok(self.len())
    }
}
impl<T> PyLen for [T] {
    fn __len__(&self) -> Result<usize> {
        Ok(self.len())
    }
}
impl<T, const N: usize> PyLen for [T; N] {
    fn __len__(&self) -> Result<usize> {
        Ok(N)
    }
}
/// `len(e._payload)`: a typed `Option` getter -- `None` has no `len()`.
impl<T: PyLen> PyLen for Option<T> {
    fn __len__(&self) -> Result<usize> {
        match self {
            Some(v) => v.__len__(),
            None => Err(LailaError::type_error(
                "object of type 'NoneType' has no len()",
            )),
        }
    }
}
impl<T: PyLen + ?Sized> PyLen for &T {
    fn __len__(&self) -> Result<usize> {
        (**self).__len__()
    }
}
impl PyLen for str {
    fn __len__(&self) -> Result<usize> {
        Ok(self.chars().count())
    }
}
impl PyLen for String {
    fn __len__(&self) -> Result<usize> {
        Ok(self.chars().count())
    }
}
impl PyLen for Args {
    fn __len__(&self) -> Result<usize> {
        Ok(Args::__len__(self))
    }
}

/// Python `str(x)`.
pub fn str(x: impl IntoArg) -> String {
    x.into_arg().__str__()
}
/// Python `repr(x)`.
pub fn repr(x: impl IntoArg) -> String {
    x.into_arg().__repr__()
}
/// Python `int(x)`.
pub fn int(x: impl IntoArg) -> Result<i64> {
    let a = x.into_arg();
    match &a {
        Arg::Int(i) => Ok(*i),
        Arg::Bool(b) => Ok(*b as i64),
        Arg::Float(f) => Ok(crate::_compat::fmath::trunc(*f) as i64),
        Arg::Str(s) => s.trim().parse::<i64>().map_err(|_| {
            LailaError::value_error(alloc::format!(
                "invalid literal for int() with base 10: {}",
                a.__repr__()
            ))
        }),
        // `int(obj)` through `__int__` / `__index__` (`AtomicInt`).
        Arg::Object(o) => {
            for proto in ["__int__", "__index__"] {
                if let Ok(Arg::Int(i)) = o.__call_method__(proto, Args::new()) {
                    return Ok(i);
                }
            }
            Err(LailaError::type_error(alloc::format!(
                "int() argument must be a string, a bytes-like object or a real number, not '{}'",
                a.type_name()
            )))
        }
        other => Err(LailaError::type_error(alloc::format!(
            "int() argument must be a string, a bytes-like object or a real number, not '{}'",
            other.type_name()
        ))),
    }
}
/// Python `float(x)`.
pub fn float(x: impl IntoArg) -> Result<f64> {
    let a = x.into_arg();
    match &a {
        Arg::Int(i) => Ok(*i as f64),
        Arg::Bool(b) => Ok(*b as i64 as f64),
        Arg::Float(f) => Ok(*f),
        Arg::Str(s) => s.trim().parse::<f64>().map_err(|_| {
            LailaError::value_error(alloc::format!(
                "could not convert string to float: {}",
                a.__repr__()
            ))
        }),
        other => Err(LailaError::type_error(alloc::format!(
            "float() argument must be a string or a real number, not '{}'",
            other.type_name()
        ))),
    }
}
/// Python `bool(x)` / the implicit truth test in `if`, `not`, `and`, `or`.
pub fn truthy<T: Truthy + ?Sized>(x: &T) -> bool {
    x.__bool__()
}
/// Python `bool(x)` spelled as a call (`bool(state)`).
#[allow(clippy::wrong_self_convention)]
pub fn bool(x: impl IntoArg) -> bool {
    x.into_arg().__bool__()
}
pub trait Truthy {
    fn __bool__(&self) -> bool;
}
impl Truthy for bool {
    #[inline]
    fn __bool__(&self) -> bool {
        *self
    }
}
impl Truthy for Arg {
    fn __bool__(&self) -> bool {
        Arg::__bool__(self)
    }
}
impl<T> Truthy for Option<T> {
    fn __bool__(&self) -> bool {
        self.is_some()
    }
}
impl<T> Truthy for Vec<T> {
    fn __bool__(&self) -> bool {
        !self.is_empty()
    }
}
impl Truthy for str {
    fn __bool__(&self) -> bool {
        !self.is_empty()
    }
}
impl Truthy for String {
    fn __bool__(&self) -> bool {
        !self.is_empty()
    }
}
/// `assert uuid.UUID(out)`: an object is truthy.
impl Truthy for super::stdlib::UUID {
    fn __bool__(&self) -> bool {
        true
    }
}
impl Truthy for Args {
    fn __bool__(&self) -> bool {
        Args::__bool__(self)
    }
}
/// A module constant computed on first read (`static _HAS_REDIS:
/// LazyLock<bool>`), tested as `if not _HAS_REDIS:`.
#[cfg(feature = "std")]
impl<T: Truthy> Truthy for std::sync::LazyLock<T> {
    fn __bool__(&self) -> bool {
        (**self).__bool__()
    }
}
macro_rules! truthy_num {
    ($($t:ty),*) => { $( impl Truthy for $t { fn __bool__(&self) -> bool { *self != 0 as $t } } )* };
}
truthy_num!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize, f32, f64);

/// Python `isinstance(x, T)` where `T` is a marker (`str`, `int`, `Entry`,
/// `ValueError`, ...) or a tuple of markers.
pub fn isinstance<T: IsInstance + ?Sized>(x: &Arg) -> bool {
    T::isinstance(x)
}
pub trait IsInstance {
    fn isinstance(x: &Arg) -> bool;
}
// Markers are braced (field-less) structs so they occupy only the type
// namespace and can share a name with the builtin functions `list`, `dict`,
// `int`, `float`. `str` and `bool` use the Rust primitives directly so the
// prelude never shadows them.
macro_rules! type_markers {
    ($($name:ident => $pat:pat),* $(,)?) => {$(
        #[doc = concat!("Marker for Python's `", stringify!($name), "` in `isinstance`.")]
        #[allow(non_camel_case_types)]
        pub struct $name {}
        impl IsInstance for $name {
            fn isinstance(x: &Arg) -> core::primitive::bool { matches!(x, $pat) }
        }
    )*};
}
type_markers! {
    object => _,
    NoneType => Arg::None,
    // Python: bool is a subclass of int.
    int => Arg::Int(_) | Arg::Long(_) | Arg::Bool(_),
    float => Arg::Float(_),
    bytes => Arg::Bytes(_),
    bytearray => Arg::ByteArray(_),
    set => Arg::Set(_),
    frozenset => Arg::FrozenSet(_),
    ndarray => Arg::NDArray(_),
    tuple => Arg::Tuple(_),
    Callable => Arg::Callable(_),
}
/// Marker for Python's `list` in `isinstance` (a Rust object whose
/// `__bases__` names `list` counts, as a Python `list` subclass does).
#[allow(non_camel_case_types)]
pub struct list {}
impl IsInstance for list {
    fn isinstance(x: &Arg) -> bool {
        match x {
            Arg::List(_) => true,
            Arg::Object(o) => o.__bases__().contains(&"list"),
            _ => false,
        }
    }
}
/// Marker for Python's `dict` in `isinstance` (a Rust object whose
/// `__bases__` names `dict` counts, as a Python `dict` subclass does).
#[allow(non_camel_case_types)]
pub struct dict {}
impl IsInstance for dict {
    fn isinstance(x: &Arg) -> bool {
        match x {
            Arg::Dict(_) => true,
            Arg::Object(o) => o.__bases__().contains(&"dict"),
            _ => false,
        }
    }
}
/// Marker for Python's `complex` in `isinstance`.
#[allow(non_camel_case_types)]
pub struct complex {}
impl IsInstance for complex {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<crate::_compat::ndarray::Complex>())
    }
}
impl IsInstance for str {
    fn isinstance(x: &Arg) -> bool {
        match x {
            Arg::Str(_) => true,
            // `class FutureStatus(str, Enum)`: members are `str` instances.
            Arg::Object(o) => o.__bases__().contains(&"str"),
            _ => false,
        }
    }
}
impl IsInstance for bool {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Bool(_))
    }
}
impl<T: ExcMarker> IsInstance for T {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Exception(e) if e.is::<T>())
    }
}
macro_rules! isinstance_tuple {
    ($($t:ident),+) => {
        impl<$($t: IsInstance + ?Sized),+> IsInstance for ($(&$t,)+) {
            fn isinstance(x: &Arg) -> bool { $( $t::isinstance(x) )||+ }
        }
    };
}
isinstance_tuple!(A);
isinstance_tuple!(A, B);
isinstance_tuple!(A, B, C);

/// Python `range(stop)` / `range(start, stop)` / `range(start, stop, step)`.
pub fn range(args: impl Into<Args>) -> Result<Vec<i64>> {
    let a: Args = args.into();
    let ints: Vec<i64> = a
        .positional
        .iter()
        .map(|v| {
            v.as_i64().ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "'{}' object cannot be interpreted as an integer",
                    v.type_name()
                ))
            })
        })
        .collect::<Result<_>>()?;
    let (start, stop, step) = match ints.as_slice() {
        [stop] => (0, *stop, 1),
        [start, stop] => (*start, *stop, 1),
        [start, stop, step] => (*start, *stop, *step),
        _ => {
            return Err(LailaError::type_error(alloc::format!(
                "range expected at most 3 arguments, got {}",
                ints.len()
            )))
        }
    };
    if step == 0 {
        return Err(LailaError::value_error("range() arg 3 must not be zero"));
    }
    let mut out = Vec::new();
    let mut i = start;
    while (step > 0 && i < stop) || (step < 0 && i > stop) {
        out.push(i);
        i += step;
    }
    Ok(out)
}

/// Python `list(iterable)`.
pub fn list(x: impl IntoArg) -> Result<Arg> {
    Ok(x.into_arg().__iter__()?.into_arg())
}
/// Python `tuple(iterable)`.
pub fn tuple(x: impl IntoArg) -> Result<Arg> {
    Ok(Arg::Tuple(x.into_arg().__iter__()?.into()))
}
fn set_inner(a: &Args) -> Result<super::object::PySetInner> {
    let mut out = super::object::PySetInner::default();
    if let Some(src) = a.positional.first() {
        for v in src.__iter__()? {
            v.__hash__()?;
            out.insert(v);
        }
    }
    Ok(out)
}
/// Python `set()` / `set(iterable)`.
pub fn set(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    Ok(Arg::Set(alloc::sync::Arc::new(super::sync::RwLock::new(
        set_inner(&a)?,
    ))))
}
/// Python `frozenset()` / `frozenset(iterable)`.
pub fn frozenset(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    Ok(Arg::FrozenSet(alloc::sync::Arc::new(set_inner(&a)?)))
}
/// Python `bytearray()` / `bytearray(bytes)` / `bytearray(iterable of ints)` / `bytearray(n)`.
pub fn bytearray(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    let v: Vec<u8> = match a.positional.first() {
        None => Vec::new(),
        Some(Arg::Int(n)) => alloc::vec![0u8; (*n).max(0) as usize],
        Some(Arg::Str(_)) => {
            return Err(LailaError::type_error(
                "string argument without an encoding",
            ))
        }
        Some(src) => match src.as_bytes_like() {
            Some(b) => b,
            None => src
                .__iter__()?
                .iter()
                .map(|x| match x.as_i64() {
                    Some(i) if (0..256).contains(&i) => Ok(i as u8),
                    Some(_) => Err(LailaError::value_error("byte must be in range(0, 256)")),
                    None => Err(LailaError::type_error(alloc::format!(
                        "'{}' object cannot be interpreted as an integer",
                        x.type_name()
                    ))),
                })
                .collect::<Result<_>>()?,
        },
    };
    Ok(Arg::ByteArray(alloc::sync::Arc::new(
        super::sync::RwLock::new(v),
    )))
}
/// Python `complex(real=0, imag=0)` -- also the spelling of an imaginary
/// literal (`2j` is `complex(0, 2)`).
pub fn complex(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    let part = |i: usize, name: &str| -> Result<f64> {
        let v = a.positional.get(i).cloned().unwrap_or_else(|| a.get(name));
        match v {
            Arg::None => Ok(0.0),
            other => other.as_f64().ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "complex() argument must be a string or a number, not '{}'",
                    other.type_name()
                ))
            }),
        }
    };
    Ok(crate::_compat::ndarray::Complex::new(part(0, "real")?, part(1, "imag")?).into_arg())
}
/// Python `memoryview(bytes-like)`: a read-only view over the buffer. The
/// Rust twin has no view type; the bytes-like protocol (`as_bytes_like`) is
/// what every consumer reads, so the view is the underlying `bytes`.
pub fn memoryview(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    match a.positional.first() {
        Some(src) if a.positional.len() == 1 && a.keywords.is_empty() => {
            match src.as_bytes_like() {
                Some(b) => Ok(Arg::Bytes(b.into())),
                None => Err(LailaError::type_error(alloc::format!(
                    "memoryview: a bytes-like object is required, not '{}'",
                    src.type_name()
                ))),
            }
        }
        _ => Err(LailaError::type_error(alloc::format!(
            "memoryview() takes exactly one argument ({} given)",
            a.__len__()
        ))),
    }
}
/// Python `bytes()` / `bytes(bytearray)` / `bytes(iterable of ints)` / `bytes(n)`.
pub fn bytes_(args: impl Into<Args>) -> Result<Arg> {
    match bytearray(args)? {
        Arg::ByteArray(b) => Ok(Arg::Bytes(b.read().as_slice().into())),
        _ => unreachable!(),
    }
}
/// Python `dict()` / `dict(mapping)` / `dict(**kw)`.
pub fn dict(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    // Positional mapping first, then keywords override (CPython order).
    let out = Arg::Dict(alloc::sync::Arc::new(super::sync::RwLock::new(
        super::object::PyMap::default(),
    )));
    if let Some(src) = a.positional.first() {
        // A mapping (an object with `keys()`, e.g. a `Manifest`) copies by
        // key; any other iterable yields `(k, v)` pairs.
        let keys = match src {
            Arg::Entry(_) | Arg::Object(_) => src.__call_method__("keys", Args::new()).ok(),
            _ => None,
        };
        match keys {
            Some(keys) => {
                for k in keys.__iter__()? {
                    let v = src.__getitem__(k.clone())?;
                    out.__setitem__(k, v)?;
                }
            }
            None => {
                for k in src.__iter__()? {
                    if !matches!(src, Arg::List(_) | Arg::Tuple(_)) {
                        let v = src.__getitem__(k.clone())?;
                        out.__setitem__(k, v)?;
                    } else {
                        let pair = k.__iter__()?;
                        if pair.len() != 2 {
                            return Err(LailaError::value_error(
                                "dictionary update sequence element has wrong length",
                            ));
                        }
                        out.__setitem__(pair[0].clone(), pair[1].clone())?;
                    }
                }
            }
        }
    }
    for (k, v) in a.keywords {
        out.__setitem__(Arg::Str(k.into()), v)?;
    }
    Ok(out)
}
/// Python `next(iterator[, default])`: the first item of the port's
/// snapshot iterator (`next(iter(d))` -- the first key); `StopIteration`
/// on an empty one unless a default is given.
pub fn next(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    let it = a
        .positional
        .first()
        .cloned()
        .ok_or_else(|| LailaError::type_error("next expected at least 1 argument, got 0"))?;
    // A dynamic iterator object (a `Relay`, a generator): one `__next__`,
    // never a drain through `__iter__`.
    if let Arg::Object(o) = &it {
        if o.__is_iterator__() {
            return match o.__next__() {
                Err(e) if e.kind == ExcKind::StopIteration => match a.positional.get(1) {
                    Some(d) => Ok(d.clone()),
                    None => Err(e),
                },
                other => other,
            };
        }
    }
    match it.__iter__()?.into_iter().next() {
        Some(v) => Ok(v),
        None => match a.positional.get(1) {
            Some(d) => Ok(d.clone()),
            None => Err(LailaError::new(ExcKind::StopIteration, "")),
        },
    }
}
/// Python `abs(x)`.
pub fn abs(x: impl IntoArg) -> Result<Arg> {
    match x.into_arg() {
        Arg::Int(i) => Ok(Arg::Int(i.abs())),
        Arg::Float(f) => Ok(Arg::Float(f.abs())),
        Arg::Bool(b) => Ok(Arg::Int(b as i64)),
        other => Err(LailaError::type_error(alloc::format!(
            "bad operand type for abs(): '{}'",
            other.type_name()
        ))),
    }
}
/// Python `reversed(sequence)` -- the items in reverse order (as a list;
/// Python's iterator is consumed the same way by `for` / `list`).
pub fn reversed(x: impl IntoArg) -> Result<Arg> {
    let mut items = x.into_arg().__iter__()?;
    items.reverse();
    Ok(items.into_arg())
}
/// The argument shape of `sorted(..)`: the bare iterable, or an `Args`
/// bag carrying `key=` / `reverse=`.
pub trait SortedArgs {
    fn into_sorted_args(self) -> Args;
}
impl<T: IntoArg> SortedArgs for T {
    fn into_sorted_args(self) -> Args {
        Args::from(self.into_arg())
    }
}
impl SortedArgs for Args {
    fn into_sorted_args(self) -> Args {
        self
    }
}
/// Python `sorted(iterable, *, key=None, reverse=False)`: a stable sort
/// by Python ordering (`<`), or by `key(item)` when given.
pub fn sorted(x: impl SortedArgs) -> Result<Arg> {
    let mut a = x.into_sorted_args();
    if a.positional.is_empty() {
        return Err(LailaError::type_error("sorted expected 1 argument, got 0"));
    }
    let iterable = a.positional.remove(0);
    let key = a.get("key");
    let reverse = a.get("reverse").__bool__();
    let items = iterable.__iter__()?;
    let keyed: Vec<(Arg, Arg)> = if key.is_none() {
        items.into_iter().map(|v| (v.clone(), v)).collect()
    } else {
        let mut out = Vec::with_capacity(items.len());
        for v in items {
            let k = key.__call__(Args::from(v.clone()))?;
            out.push((k, v));
        }
        out
    };
    let mut keyed = keyed;
    let mut err = None;
    // `reverse=True` flips the comparison (stable among equal keys, as
    // CPython), not the result.
    keyed.sort_by(
        |(a, _), (b, _)| match if reverse { b.py_cmp(a) } else { a.py_cmp(b) } {
            Some(o) => o,
            None => {
                err.get_or_insert_with(|| {
                    LailaError::type_error(alloc::format!(
                        "'<' not supported between instances of '{}' and '{}'",
                        b.type_name(),
                        a.type_name()
                    ))
                });
                core::cmp::Ordering::Equal
            }
        },
    );
    if let Some(e) = err {
        return Err(e);
    }
    Ok(keyed
        .into_iter()
        .map(|(_, v)| v)
        .collect::<Vec<Arg>>()
        .into_arg())
}
/// Python `all(iterable)`.
pub fn all(x: impl IntoArg) -> Result<bool> {
    Ok(x.into_arg().__iter__()?.iter().all(|v| v.__bool__()))
}
/// Python `any(iterable)`.
pub fn any(x: impl IntoArg) -> Result<bool> {
    Ok(x.into_arg().__iter__()?.iter().any(|v| v.__bool__()))
}
/// Python `type(x).__name__`.
pub fn type_name(x: &Arg) -> &'static str {
    x.type_name()
}

// ---- protocol helpers used by `#[laila::python]` lowering ----

/// `for x in it`: the items of a Python iteration -- a snapshot `Vec` for
/// containers, or a lazy `__next__` pull for iterator objects (a `Relay`,
/// a generator), so a `break` stops consuming and a late `__next__` error
/// surfaces at the item like a Python exception.
pub enum PyIterator<T> {
    Eager(alloc::vec::IntoIter<T>),
    Lazy(Box<dyn FnMut() -> Result<Option<T>> + Send>),
}

impl<T> PyIterator<T> {
    pub fn eager(v: Vec<T>) -> Self {
        PyIterator::Eager(v.into_iter())
    }
    pub fn lazy(f: impl FnMut() -> Result<Option<T>> + Send + 'static) -> Self {
        PyIterator::Lazy(Box::new(f))
    }
    /// Drain into a `Vec` (`list(it)`).
    pub fn collect_vec(self) -> Result<Vec<T>> {
        self.collect()
    }
}

impl<T> Iterator for PyIterator<T> {
    type Item = Result<T>;
    fn next(&mut self) -> Option<Result<T>> {
        match self {
            PyIterator::Eager(it) => it.next().map(Ok),
            PyIterator::Lazy(f) => match f() {
                Ok(Some(v)) => Some(Ok(v)),
                Ok(None) => None,
                Err(e) => {
                    // a raised `__next__` ends the iteration after the error
                    *self = PyIterator::Eager(Vec::new().into_iter());
                    Some(Err(e))
                }
            },
        }
    }
}

/// `for x in it` -- Python iteration.
pub trait PyIter {
    type Item;
    fn py_iter(self) -> Result<PyIterator<Self::Item>>;
}
impl PyIter for Arg {
    type Item = Arg;
    fn py_iter(self) -> Result<PyIterator<Arg>> {
        (&self).py_iter()
    }
}
impl PyIter for &Arg {
    type Item = Arg;
    fn py_iter(self) -> Result<PyIterator<Arg>> {
        // An iterator object (`__next__`) is pulled lazily.
        if let Arg::Object(o) = self {
            if o.__is_iterator__() {
                let o = o.clone();
                return Ok(PyIterator::lazy(move || match o.__next__() {
                    Ok(v) => Ok(Some(v)),
                    Err(e) if e.kind == super::errors::ExcKind::StopIteration => Ok(None),
                    Err(e) => Err(e),
                }));
            }
        }
        Ok(PyIterator::eager(self.__iter__()?))
    }
}
impl<T> PyIter for Vec<T> {
    type Item = T;
    fn py_iter(self) -> Result<PyIterator<T>> {
        Ok(PyIterator::eager(self))
    }
}
impl<T: Clone> PyIter for &Vec<T> {
    type Item = T;
    fn py_iter(self) -> Result<PyIterator<T>> {
        Ok(PyIterator::eager(self.clone()))
    }
}
impl<T: Clone> PyIter for &[T] {
    type Item = T;
    fn py_iter(self) -> Result<PyIterator<T>> {
        Ok(PyIterator::eager(self.to_vec()))
    }
}
impl PyIter for core::ops::Range<i64> {
    type Item = i64;
    fn py_iter(self) -> Result<PyIterator<i64>> {
        Ok(PyIterator::eager(self.collect()))
    }
}
/// `for ch in "abc"`: a string iterates over its characters.
impl PyIter for &str {
    type Item = Arg;
    fn py_iter(self) -> Result<PyIterator<Arg>> {
        Ok(PyIterator::eager(Arg::Str(self.into()).__iter__()?))
    }
}
impl PyIter for &String {
    type Item = Arg;
    fn py_iter(self) -> Result<PyIterator<Arg>> {
        Ok(PyIterator::eager(
            Arg::Str(self.as_str().into()).__iter__()?,
        ))
    }
}
impl PyIter for Args {
    type Item = Arg;
    fn py_iter(self) -> Result<PyIterator<Arg>> {
        Ok(PyIterator::eager(
            self.keywords
                .into_iter()
                .map(|(k, _)| Arg::Str(k.into()))
                .collect(),
        ))
    }
}
impl PyIter for &Args {
    type Item = Arg;
    fn py_iter(self) -> Result<PyIterator<Arg>> {
        Ok(PyIterator::eager(
            self.keywords
                .iter()
                .map(|(k, _)| Arg::Str(k.as_str().into()))
                .collect(),
        ))
    }
}
impl<T> PyIter for PyIterator<T> {
    type Item = T;
    fn py_iter(self) -> Result<PyIterator<T>> {
        Ok(self)
    }
}
pub fn py_iter<T: PyIter>(x: T) -> Result<PyIterator<T::Item>> {
    x.py_iter()
}

/// `(a,) = ...` unpacking.
pub fn unpack1(x: Arg) -> Result<(Arg,)> {
    let v = unpack_n(x, 1)?;
    Ok((v.into_iter().next().unwrap(),))
}
/// `for (a, b) in ...` unpacking.
pub fn unpack2(x: Arg) -> Result<(Arg, Arg)> {
    let v = x.__iter__()?;
    if v.len() != 2 {
        return Err(LailaError::value_error(alloc::format!(
            "{} values to unpack (expected 2)",
            if v.len() > 2 {
                "too many"
            } else {
                "not enough"
            }
        )));
    }
    let mut it = v.into_iter();
    Ok((it.next().unwrap(), it.next().unwrap()))
}
pub fn unpack3(x: Arg) -> Result<(Arg, Arg, Arg)> {
    let v = x.__iter__()?;
    if v.len() != 3 {
        return Err(LailaError::value_error(alloc::format!(
            "{} values to unpack (expected 3)",
            if v.len() > 3 {
                "too many"
            } else {
                "not enough"
            }
        )));
    }
    let mut it = v.into_iter();
    Ok((it.next().unwrap(), it.next().unwrap(), it.next().unwrap()))
}

/// `a, b, c, d = ...` unpacking.
pub fn unpack4(x: Arg) -> Result<(Arg, Arg, Arg, Arg)> {
    let v = unpack_n(x, 4)?;
    let mut it = v.into_iter();
    Ok((
        it.next().unwrap(),
        it.next().unwrap(),
        it.next().unwrap(),
        it.next().unwrap(),
    ))
}
/// `a, b, c, d, e = ...` unpacking.
pub fn unpack5(x: Arg) -> Result<(Arg, Arg, Arg, Arg, Arg)> {
    let v = unpack_n(x, 5)?;
    let mut it = v.into_iter();
    Ok((
        it.next().unwrap(),
        it.next().unwrap(),
        it.next().unwrap(),
        it.next().unwrap(),
        it.next().unwrap(),
    ))
}
fn unpack_n(x: Arg, n: usize) -> Result<Vec<Arg>> {
    let v = x.__iter__()?;
    if v.len() != n {
        return Err(LailaError::value_error(alloc::format!(
            "{} values to unpack (expected {n})",
            if v.len() > n {
                "too many"
            } else {
                "not enough"
            }
        )));
    }
    Ok(v)
}

/// `item in container`.
pub trait PyContains {
    fn py_contains(&self, item: Arg) -> Result<bool>;
}
impl PyContains for Arg {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        self.__contains__(item)
    }
}
impl PyContains for Args {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        self.__contains__(item)
    }
}
impl PyContains for str {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        Arg::Str(self.into()).__contains__(item)
    }
}
impl PyContains for String {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        Arg::Str(self.as_str().into()).__contains__(item)
    }
}
impl PyContains for &str {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        Arg::Str((*self).into()).__contains__(item)
    }
}
impl<T: PyArg> PyContains for Vec<T> {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        Ok(self.iter().any(|v| v.to_arg() == item))
    }
}
impl<T: PyArg> PyContains for [T] {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        Ok(self.iter().any(|v| v.to_arg() == item))
    }
}
impl PyContains for crate::AlphaPool {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        self.__contains__(item)
    }
}
impl PyContains for crate::data::Pool {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        self.__contains__(item)
    }
}
impl PyContains for crate::entry::Entry {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        self.__contains__(item)
    }
}
impl PyContains for crate::Manifest {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        Ok(self.__contains__(item))
    }
}
#[cfg(feature = "std")]
impl PyContains for crate::policy::central::communication::PeerRegistry {
    fn py_contains(&self, item: Arg) -> Result<bool> {
        super::object::PyObject::__contains__(self, &item)
    }
}
pub fn contains<C: PyContains + ?Sized>(container: &C, item: impl IntoArg) -> Result<bool> {
    container.py_contains(item.into_arg())
}

/// `x is y`.
pub fn is_<A: PyArg + ?Sized, B: PyArg + ?Sized>(a: &A, b: &B) -> bool {
    a.to_arg().is(&b.to_arg())
}
/// `x is None`.
pub fn is_none<A: PyArg + ?Sized>(a: &A) -> bool {
    a.to_arg().is_none()
}

/// `hasattr(obj, "name")`.
pub fn hasattr<A: PyArg + ?Sized>(a: &A, name: impl IntoArg) -> bool {
    let name = name.into_arg().__str__();
    a.to_arg().__getattr__(&name).is_ok()
}

/// `open(file, mode='r', ..)` -- the builtin (host only).
#[cfg(feature = "std")]
pub use crate::python::stdlib::{open, PyFile};

/// `getattr(obj, "name")` / `getattr(obj, "name", default)`.
pub fn getattr(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    let (obj, name) = match a.positional.as_slice() {
        [o, n, ..] if a.positional.len() <= 3 => (o, n),
        _ => {
            return Err(LailaError::type_error(alloc::format!(
                "getattr expected at least 2 arguments, got {}",
                a.positional.len()
            )))
        }
    };
    let name = match name {
        Arg::Str(s) => s.clone(),
        other => {
            return Err(LailaError::type_error(alloc::format!(
                "attribute name must be string, not '{}'",
                other.type_name()
            )))
        }
    };
    match obj.__getattr__(&name) {
        Ok(v) => Ok(v),
        Err(e) if e.kind == ExcKind::AttributeError && a.positional.len() == 3 => {
            Ok(a.positional[2].clone())
        }
        Err(e) => Err(e),
    }
}

/// `dir(obj)`: the sorted attribute names of a dynamic object (module
/// objects, model instances, class objects).
pub fn dir<A: PyArg + ?Sized>(a: &A) -> Result<Arg> {
    let obj = a.to_arg();
    let mut names: Vec<String> = match &obj {
        Arg::Object(o) => o.__dir__(),
        Arg::Dict(_) => Vec::new(),
        _ => Vec::new(),
    };
    names.sort();
    names.dedup();
    Ok(names
        .into_iter()
        .map(|n| Arg::Str(n.into()))
        .collect::<Vec<_>>()
        .into_arg())
}

/// Python `hash(x)`.
pub fn hash(x: impl IntoArg) -> Result<i64> {
    x.into_arg().__hash__()
}

/// A laila class used as a *value* (`patch.object(FilesystemPool, ..)`,
/// `cls=Entry`): the class object. Every pydantic-modelled class
/// (`ModelBase`) is one.
pub trait ClassValue {
    fn class_value() -> Arg;
}
impl<T: crate::_compat::pydantic::ModelBase> ClassValue for T {
    fn class_value() -> Arg {
        T::__class__().as_pyclass().into_arg()
    }
}
/// `laila::python!` lowers a bare CamelCase name in argument position to
/// `class_value::<Name>()`.
pub fn class_value<T: ClassValue>() -> Arg {
    T::class_value()
}

/// A module constant read as a value, whichever way the Rust module spells
/// it: a `fn NAME() -> Arg` (a table built on first use, such as
/// `LAILA_DEFAULT_DIRECTORIES`), a `static NAME: LazyLock<Arg>`, or a
/// plain `const`. `laila::python!` lowers `NAME[..]` on an imported
/// `CONSTANT`-case name to `module_value(&NAME)[..]`.
pub trait ModuleValue {
    fn module_value(self) -> Arg;
}
impl<F: Fn() -> Arg> ModuleValue for &F {
    fn module_value(self) -> Arg {
        self()
    }
}
impl ModuleValue for &Arg {
    fn module_value(self) -> Arg {
        self.clone()
    }
}
/// `static NAME: LazyLock<Arg>` / `LazyLock<bool>` (`_HAS_REDIS`): the
/// value computed on first read.
#[cfg(feature = "std")]
impl<T: Clone + IntoArg> ModuleValue for &std::sync::LazyLock<T> {
    fn module_value(self) -> Arg {
        (**self).clone().into_arg()
    }
}
impl ModuleValue for &&str {
    fn module_value(self) -> Arg {
        (*self).into_arg()
    }
}
/// A `const TABLE: &[(&str, &str)]` (a Python module-level dict literal).
impl ModuleValue for &&[(&str, &str)] {
    fn module_value(self) -> Arg {
        let mut d = super::PyDictBuilder::new();
        for (k, v) in self.iter() {
            d.insert(*k, Arg::Str((*v).into()));
        }
        d.finish()
    }
}
impl ModuleValue for &bool {
    fn module_value(self) -> Arg {
        Arg::Bool(*self)
    }
}
impl ModuleValue for &i64 {
    fn module_value(self) -> Arg {
        Arg::Int(*self)
    }
}
impl ModuleValue for &i32 {
    fn module_value(self) -> Arg {
        Arg::Int(i64::from(*self))
    }
}
impl ModuleValue for &f64 {
    fn module_value(self) -> Arg {
        Arg::Float(*self)
    }
}
/// `x.name(args)` reached dynamically on a typed object or an `Arg`
/// (`laila::python!` uses it when the name is also a zero-argument `Arg`
/// forwarder: `UTC.utcoffset(None)`).
pub trait CallMethod {
    fn __laila_call_method__(&self, name: &str, args: Args) -> Result<Arg>;
}
impl CallMethod for Arg {
    fn __laila_call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        self.__call_method__(name, args)
    }
}
impl<T: super::object::PyObject> CallMethod for T {
    fn __laila_call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        super::object::PyObject::__call_method__(self, name, args)
    }
}

pub fn module_value<T: ModuleValue>(v: T) -> Arg {
    v.module_value()
}

/// A builtin class as a value (`dict`, `list`, `str`, ... in
/// `default_factory=dict`): a class object whose call runs the builtin.
pub fn builtin_class(name: &str) -> Arg {
    use super::pyclass::PyClass;
    let ctor: super::object::Callable = match name {
        "dict" => alloc::sync::Arc::new(dict),
        "list" => alloc::sync::Arc::new(|a: Args| {
            list(
                a.positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| Vec::<Arg>::new().into_arg()),
            )
        }),
        "tuple" => alloc::sync::Arc::new(|a: Args| {
            tuple(
                a.positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| Vec::<Arg>::new().into_arg()),
            )
        }),
        "set" => alloc::sync::Arc::new(set),
        "frozenset" => alloc::sync::Arc::new(frozenset),
        "str" => alloc::sync::Arc::new(|a: Args| {
            Ok(Arg::Str(
                str(a.positional.first().cloned().unwrap_or(Arg::Str("".into()))).into(),
            ))
        }),
        "int" => alloc::sync::Arc::new(|a: Args| {
            Ok(Arg::Int(int(a
                .positional
                .first()
                .cloned()
                .unwrap_or(Arg::Int(0)))?))
        }),
        "float" => alloc::sync::Arc::new(|a: Args| {
            Ok(Arg::Float(float(
                a.positional.first().cloned().unwrap_or(Arg::Float(0.0)),
            )?))
        }),
        "bool" => alloc::sync::Arc::new(|a: Args| {
            Ok(Arg::Bool(
                a.positional.first().map(|v| v.__bool__()).unwrap_or(false),
            ))
        }),
        "bytes" => alloc::sync::Arc::new(bytes_),
        "bytearray" => alloc::sync::Arc::new(bytearray),
        _ => alloc::sync::Arc::new(|_a: Args| Err(LailaError::type_error("not callable"))),
    };
    let leaked: &'static str = match name {
        "dict" => "dict",
        "list" => "list",
        "tuple" => "tuple",
        "set" => "set",
        "frozenset" => "frozenset",
        "str" => "str",
        "int" => "int",
        "float" => "float",
        "bool" => "bool",
        "bytes" => "bytes",
        "bytearray" => "bytearray",
        _ => "object",
    };
    PyClass::with_ctor(leaked, "builtins", ctor).into_arg()
}

/// Python's `callable(x)` *predicate*: functions, bound methods and class
/// objects are callable. (The Tier 1 helper that wraps a Rust closure as a
/// Python callable is `laila::python::callable`.)
pub fn is_callable<A: PyArg + ?Sized>(a: &A) -> bool {
    match a.to_arg() {
        Arg::Callable(_) => true,
        Arg::Object(o) => {
            o.as_any().is::<super::pyclass::PyClass>()
                || o.as_any().is::<super::object::Function>()
                || o.as_any().is::<super::stdlib::asyncio::CoroutineFunction>()
        }
        _ => false,
    }
}

/// `f(*args, **kwargs)` where `f` is a *value* (a `lambda`, a bound method
/// stored in a variable, an `Arg::Callable`). Tier 2 emits this for calls
/// whose callee is a bare local name that is not a known function.
pub fn call<F: CallValue + ?Sized>(f: &F, args: impl Into<Args>) -> Result<Arg> {
    f.call_value(args.into())
}

pub trait CallValue {
    fn call_value(&self, args: Args) -> Result<Arg>;
}
impl CallValue for Arg {
    fn call_value(&self, args: Args) -> Result<Arg> {
        self.__call__(args)
    }
}
/// A `static F: LazyLock<Arg>` class / function value called by name.
#[cfg(feature = "std")]
impl<T: CallValue> CallValue for std::sync::LazyLock<T> {
    fn call_value(&self, args: Args) -> Result<Arg> {
        (**self).call_value(args)
    }
}
impl CallValue for super::object::Callable {
    fn call_value(&self, args: Args) -> Result<Arg> {
        (self)(args)
    }
}
impl<F> CallValue for F
where
    F: Fn(Args) -> Result<Arg>,
{
    fn call_value(&self, args: Args) -> Result<Arg> {
        (self)(args)
    }
}

/// `a ** b`.
pub fn pow(a: impl IntoArg, b: impl IntoArg) -> Result<Arg> {
    let (a, b) = (a.into_arg(), b.into_arg());
    match (&a, &b) {
        // `int ** int` never overflows in Python: promote to a big int.
        (Arg::Int(x), Arg::Int(y)) if *y >= 0 => Ok(match x.checked_pow(*y as u32) {
            Some(v) => Arg::Int(v),
            None => Arg::from_bigint(num_bigint::BigInt::from(*x).pow(*y as u32)),
        }),
        (Arg::Long(_) | Arg::Int(_) | Arg::Bool(_), Arg::Int(y))
            if *y >= 0 && a.as_bigint().is_some() =>
        {
            Ok(Arg::from_bigint(a.as_bigint().unwrap().pow(*y as u32)))
        }
        _ => match (a.as_f64(), b.as_f64()) {
            (Some(x), Some(y)) => Ok(Arg::Float(crate::_compat::fmath::powf(x, y))),
            _ => Err(LailaError::type_error(alloc::format!(
                "unsupported operand type(s) for ** or pow(): '{}' and '{}'",
                a.type_name(),
                b.type_name()
            ))),
        },
    }
}
/// `a // b` (floor division).
pub fn floordiv(a: impl IntoArg, b: impl IntoArg) -> Result<Arg> {
    let (a, b) = (a.into_arg(), b.into_arg());
    match (&a, &b) {
        (Arg::Int(_), Arg::Int(0)) => Err(LailaError::new(
            ExcKind::ZeroDivisionError,
            "integer division or modulo by zero",
        )),
        (Arg::Int(x), Arg::Int(y)) => {
            let q = x / y;
            Ok(Arg::Int(if (x % y != 0) && ((*x < 0) != (*y < 0)) {
                q - 1
            } else {
                q
            }))
        }
        _ => match (a.as_f64(), b.as_f64()) {
            (Some(x), Some(y)) => {
                if y == 0.0 {
                    Err(LailaError::new(
                        ExcKind::ZeroDivisionError,
                        "float floor division by zero",
                    ))
                } else {
                    Ok(Arg::Float(crate::_compat::fmath::floor(x / y)))
                }
            }
            _ => Err(LailaError::type_error(alloc::format!(
                "unsupported operand type(s) for //: '{}' and '{}'",
                a.type_name(),
                b.type_name()
            ))),
        },
    }
}

/// `a / b`: Python 3 true division (always a float; `ZeroDivisionError`).
pub fn truediv(a: impl IntoArg, b: impl IntoArg) -> Result<Arg> {
    a.into_arg().__truediv__(&b.into_arg())
}
/// `a % b`: Python modulo (sign of the divisor).
pub fn pymod(a: impl IntoArg, b: impl IntoArg) -> Result<Arg> {
    a.into_arg().__mod__(&b.into_arg())
}

/// Python `iter(x)`: the port's iterators are snapshot lists (`list(it)`,
/// `for v in it` and `next(it)` all read them), so `iter` is a snapshot.
pub fn iter(x: impl IntoArg) -> Result<Arg> {
    Ok(x.into_arg().__iter__()?.into_arg())
}

/// A generator object over a snapshot: what a `def _gen(): ... yield k`
/// returns. Iterable (`list(g)`, `for k in g`, `all(.. for k in g)`), not a
/// `list` (`isinstance(g, list)` is `False`), and `hasattr(g, "__iter__")`.
/// `close()` is a no-op.
#[derive(Clone)]
pub struct Generator {
    items: alloc::sync::Arc<Vec<Arg>>,
    name: &'static str,
}

impl Generator {
    /// `<generator object name at ...>` over an item snapshot.
    pub fn new(name: &'static str, items: Vec<Arg>) -> Generator {
        Generator {
            items: alloc::sync::Arc::new(items),
            name,
        }
    }
    /// The snapshot.
    pub fn items(&self) -> &[Arg] {
        &self.items
    }
}

impl core::fmt::Debug for Generator {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&super::object::PyObject::__repr__(self))
    }
}

impl super::object::PyObject for Generator {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "generator"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<generator object {} at {:#x}>",
            self.name,
            alloc::sync::Arc::as_ptr(&self.items) as usize
        )
    }
    fn __eq__(&self, other: &dyn super::object::PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Generator>()
            .is_some_and(|o| alloc::sync::Arc::ptr_eq(&o.items, &self.items))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(alloc::sync::Arc::as_ptr(&self.items) as usize as i64)
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(self.items.as_ref().clone())
    }
    fn __bool__(&self) -> bool {
        true
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "__iter__" | "__next__" | "close" | "send" | "throw" => {
                let me = self.clone();
                Ok(Arg::Callable(alloc::sync::Arc::new(move |_a: Args| {
                    Ok(me.items.as_ref().clone().into_arg())
                })))
            }
            "__name__" => Ok(Arg::Str(self.name.into())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'generator' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, _args: Args) -> Result<Arg> {
        match name {
            "close" => Ok(Arg::None),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'generator' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for Generator {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self))
    }
}
impl PyArg for Generator {
    fn to_arg(&self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self.clone()))
    }
}

/// The `NotImplemented` singleton (what a binary-operator dunder returns to
/// let Python try the reflected operation). One shared object, so
/// `x is NotImplemented` holds for every value handed out.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct NotImplementedType;

impl super::object::PyObject for NotImplementedType {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "NotImplementedType"
    }
    fn __repr__(&self) -> String {
        String::from("NotImplemented")
    }
    fn __eq__(&self, other: &dyn super::object::PyObject) -> bool {
        other.as_any().is::<NotImplementedType>()
    }
    fn __is__(&self, other: &dyn super::object::PyObject) -> bool {
        other.as_any().is::<NotImplementedType>()
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x4e6f_7449_6d70_6c65)
    }
    fn __bool__(&self) -> bool {
        true
    }
}

#[cfg(feature = "std")]
static NOT_IMPLEMENTED: std::sync::LazyLock<Arg> =
    std::sync::LazyLock::new(|| Arg::Object(alloc::sync::Arc::new(NotImplementedType)));
#[cfg(not(feature = "std"))]
static NOT_IMPLEMENTED: spin::Lazy<Arg> =
    spin::Lazy::new(|| Arg::Object(alloc::sync::Arc::new(NotImplementedType)));

/// `NotImplemented` as an `Arg` (the shared singleton).
pub fn not_implemented() -> Arg {
    NOT_IMPLEMENTED.clone()
}

/// `x is NotImplemented`.
pub fn is_not_implemented(x: &Arg) -> bool {
    matches!(x, Arg::Object(o) if o.as_any().is::<NotImplementedType>())
}

/// The bare name `NotImplemented` in a `laila::python!` body.
#[allow(non_upper_case_globals)]
pub static NotImplemented: NotImplementedType = NotImplementedType;

impl PyArg for NotImplementedType {
    fn to_arg(&self) -> Arg {
        not_implemented()
    }
}
impl IntoArg for NotImplementedType {
    fn into_arg(self) -> Arg {
        not_implemented()
    }
}

/// `object()`: a fresh, identity-only sentinel (equal only to itself).
#[derive(Debug)]
pub struct ObjectSentinel(u64);
impl super::object::PyObject for ObjectSentinel {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "object"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<object object at 0x{:x}>", self.0)
    }
    fn __eq__(&self, other: &dyn super::object::PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<ObjectSentinel>()
            .is_some_and(|o| o.0 == self.0)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self.0 as i64)
    }
}
/// `id(x)`: the identity of a shared object (its allocation address);
/// immutable scalars hash to their value, like CPython's small-int /
/// interned-string behaviour makes `id(1) == id(1)` hold.
pub fn id<A: super::object::PyArg + ?Sized>(a: &A) -> i64 {
    fn addr<T: ?Sized>(p: &alloc::sync::Arc<T>) -> i64 {
        alloc::sync::Arc::as_ptr(p) as *const u8 as usize as i64
    }
    match a.to_arg() {
        Arg::None => 0,
        Arg::Bool(b) => 1 + b as i64,
        Arg::Int(i) => i,
        Arg::Float(f) => f.to_bits() as i64,
        Arg::Str(s) => addr(&s),
        Arg::List(l) => addr(&l),
        Arg::Dict(d) => addr(&d),
        Arg::Tuple(t) => addr(&t),
        Arg::Bytes(b) => addr(&b),
        Arg::Long(l) => addr(&l),
        Arg::ByteArray(b) => addr(&b),
        Arg::Set(s) => addr(&s),
        Arg::FrozenSet(s) => addr(&s),
        Arg::NDArray(a) => addr(&a),
        Arg::Entry(e) => e.id() as i64,
        Arg::Pool(p) => p.0.id() as i64,
        Arg::Future(f) => addr(&f.0),
        Arg::Callable(c) => addr(&c),
        Arg::Exception(e) => addr(&e),
        Arg::Object(o) => addr(&o),
    }
}

/// `setattr(obj, "name", value)`.
pub fn setattr(args: impl Into<Args>) -> Result<()> {
    let a: Args = args.into();
    let [obj, name, value] = a.positional.as_slice() else {
        return Err(LailaError::type_error(alloc::format!(
            "setattr expected 3 arguments, got {}",
            a.positional.len()
        )));
    };
    let Arg::Str(name) = name else {
        return Err(LailaError::type_error(alloc::format!(
            "attribute name must be string, not '{}'",
            name.type_name()
        )));
    };
    obj.__setattr__(name, value.clone())
}

/// `object.__setattr__(obj, "name", value)`: the base slot, bypassing any
/// overridden `__setattr__` (laila objects keep their dynamic attributes in
/// the same per-instance table either way).
pub fn object__setattr__(args: impl Into<Args>) -> Result<()> {
    setattr(args)
}

/// `object.__getattribute__(obj, "name")`.
pub fn object__getattribute__(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    let [obj, name] = a.positional.as_slice() else {
        return Err(LailaError::type_error(alloc::format!(
            "__getattribute__ expected 2 arguments, got {}",
            a.positional.len()
        )));
    };
    obj.__getattr__(&name.__str__())
}

/// `object.__delattr__(obj, "name")`.
pub fn object__delattr__(args: impl Into<Args>) -> Result<()> {
    delattr(args)
}

/// `delattr(obj, "name")`.
pub fn delattr(args: impl Into<Args>) -> Result<()> {
    let a: Args = args.into();
    let [obj, name] = a.positional.as_slice() else {
        return Err(LailaError::type_error(alloc::format!(
            "delattr expected 2 arguments, got {}",
            a.positional.len()
        )));
    };
    let Arg::Str(name) = name else {
        return Err(LailaError::type_error(alloc::format!(
            "attribute name must be string, not '{}'",
            name.type_name()
        )));
    };
    obj.__delattr__(name)
}

/// `import builtins` -- the module as a value: tests hang shared state
/// off it (`setattr(builtins, "_registry", {..})`) so code that cannot
/// see the test module (a constitution body) finds it by name.
#[derive(Clone, Copy, Debug, Default)]
pub struct builtins_mod;
#[allow(non_upper_case_globals)]
pub static builtins: builtins_mod = builtins_mod;
static BUILTINS_ATTRS: super::sync::RwLock<Vec<(String, Arg)>> =
    super::sync::RwLock::new(Vec::new());
impl super::object::PyObject for builtins_mod {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "module"
    }
    fn __repr__(&self) -> String {
        String::from("<module 'builtins' (built-in)>")
    }
    fn __eq__(&self, other: &dyn super::object::PyObject) -> bool {
        other.as_any().downcast_ref::<builtins_mod>().is_some()
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x62_75_69_6c)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        BUILTINS_ATTRS
            .read()
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .ok_or_else(|| {
                LailaError::attribute_error(alloc::format!(
                    "module 'builtins' has no attribute '{name}'"
                ))
            })
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        let mut g = BUILTINS_ATTRS.write();
        if let Some(slot) = g.iter_mut().find(|(k, _)| k == name) {
            slot.1 = value;
        } else {
            g.push((String::from(name), value));
        }
        Ok(())
    }
    fn __delattr__(&self, name: &str) -> Result<()> {
        let mut g = BUILTINS_ATTRS.write();
        let Some(pos) = g.iter().position(|(k, _)| k == name) else {
            return Err(LailaError::attribute_error(alloc::format!(
                "module 'builtins' has no attribute '{name}'"
            )));
        };
        g.remove(pos);
        Ok(())
    }
}
impl IntoArg for builtins_mod {
    fn into_arg(self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(self))
    }
}
impl super::object::PyArg for builtins_mod {
    fn to_arg(&self) -> Arg {
        Arg::Object(alloc::sync::Arc::new(*self))
    }
}

/// `object()` as a value.
pub fn object_sentinel() -> Arg {
    static NEXT: super::sync::IdCounter = super::sync::IdCounter::new();
    let id = NEXT.next();
    Arg::Object(alloc::sync::Arc::new(ObjectSentinel(id)))
}

/// `issubclass(A, B)`: two laila / `py_model!` classes (unit structs
/// implementing `ModelBase`; walks `A`'s MRO for `B`'s name), or two
/// exception classes (their `ExcKind` hierarchy).
pub fn issubclass<A: IsSubclassOf<B>, B>() -> bool {
    A::issubclass()
}

/// The `issubclass(A, B)` relation between two class markers.
pub trait IsSubclassOf<B> {
    fn issubclass() -> bool;
}
impl<A, B> IsSubclassOf<B> for A
where
    A: crate::_compat::pydantic::ModelBase,
    B: crate::_compat::pydantic::ModelBase,
{
    fn issubclass() -> bool {
        A::__class__().is_subclass(B::__class__().name)
    }
}

/// Python `zip(*iterables)` -> list of tuples (shortest iterable wins).
pub fn zip(args: impl Into<Args>) -> Result<Arg> {
    let a: Args = args.into();
    let mut columns: Vec<Vec<Arg>> = Vec::with_capacity(a.positional.len());
    for it in &a.positional {
        columns.push(it.__iter__()?);
    }
    let n = columns.iter().map(Vec::len).min().unwrap_or(0);
    let mut out: Vec<Arg> = Vec::with_capacity(n);
    for i in 0..n {
        let row: Vec<Arg> = columns.iter().map(|c| c[i].clone()).collect();
        out.push(Arg::Tuple(row.into()));
    }
    Ok(out.into_arg())
}

/// Python `enumerate(iterable, start=0)` -> list of `(index, item)` tuples.
pub fn enumerate(args: impl Into<Args>) -> Result<Arg> {
    let mut a: Args = args.into();
    let iterable = a.positional.first().cloned().ok_or_else(|| {
        LailaError::type_error("enumerate() missing required argument 'iterable'")
    })?;
    let start = match a.positional.get(1).cloned() {
        Some(s) => s,
        None if a.__contains__("start")? => a.pop("start")?,
        None => Arg::Int(0),
    };
    let start = start.as_i64().ok_or_else(|| {
        LailaError::type_error(alloc::format!(
            "'{}' object cannot be interpreted as an integer",
            start.type_name()
        ))
    })?;
    let out: Vec<Arg> = iterable
        .__iter__()?
        .into_iter()
        .enumerate()
        .map(|(i, x)| Arg::Tuple(alloc::vec![Arg::Int(start + i as i64), x].into()))
        .collect();
    Ok(out.into_arg())
}

/// Python `chr(i)`.
pub fn chr(i: impl IntoArg) -> Result<String> {
    let v = i.into_arg();
    let n = v.as_i64().ok_or_else(|| {
        LailaError::type_error(alloc::format!(
            "an integer is required (got type {})",
            v.type_name()
        ))
    })?;
    if !(0..=0x10FFFF).contains(&n) {
        return Err(LailaError::value_error("chr() arg not in range(0x110000)"));
    }
    char::from_u32(n as u32)
        .map(String::from)
        .ok_or_else(|| LailaError::value_error("chr() arg not in range(0x110000)"))
}

/// Python `ord(c)`.
pub fn ord(c: impl IntoArg) -> Result<i64> {
    let v = c.into_arg();
    match &v {
        Arg::Str(s) if s.chars().count() == 1 => Ok(s.chars().next().unwrap() as i64),
        Arg::Str(s) => Err(LailaError::type_error(alloc::format!(
            "ord() expected a character, but string of length {} found",
            s.chars().count()
        ))),
        Arg::Bytes(b) if b.len() == 1 => Ok(b[0] as i64),
        other => Err(LailaError::type_error(alloc::format!(
            "ord() expected string of length 1, but {} found",
            other.type_name()
        ))),
    }
}

fn extremum(args: Args, name: &str, pick: fn(core::cmp::Ordering) -> bool) -> Result<Arg> {
    let mut args = args;
    // `key=` ranks by `key(item)`; `default=` is returned for an empty
    // iterable (CPython: only valid in the single-iterable form).
    let key = args.get("key");
    let default = args.get("default");
    let single = args.positional.len() == 1;
    if !default.is_none() && !single {
        return Err(LailaError::type_error(alloc::format!(
            "Cannot specify a default for {name}() with multiple positional arguments"
        )));
    }
    let items: Vec<Arg> = match args.positional.len() {
        0 => {
            return Err(LailaError::type_error(alloc::format!(
                "{name} expected at least 1 argument, got 0"
            )))
        }
        1 => args.positional[0].__iter__()?,
        _ => core::mem::take(&mut args.positional),
    };
    let rank = |v: &Arg| -> Result<Arg> {
        if key.is_none() {
            Ok(v.clone())
        } else {
            key.__call__(Args::from(v.clone()))
        }
    };
    let mut it = items.into_iter();
    let mut best = match it.next() {
        Some(b) => b,
        None if !default.is_none() => return Ok(default),
        None => {
            return Err(LailaError::value_error(alloc::format!(
                "{name}() iterable argument is empty"
            )))
        }
    };
    let mut best_key = rank(&best)?;
    for v in it {
        let k = rank(&v)?;
        match k.py_cmp(&best_key) {
            Some(o) => {
                if pick(o) {
                    best = v;
                    best_key = k;
                }
            }
            None => {
                return Err(LailaError::type_error(alloc::format!(
                    "'<' not supported between instances of '{}' and '{}'",
                    k.type_name(),
                    best_key.type_name()
                )))
            }
        }
    }
    Ok(best)
}

/// Python `max(iterable)` / `max(a, b, ...)`.
pub fn max(args: impl Into<Args>) -> Result<Arg> {
    extremum(args.into(), "max", |o| o == core::cmp::Ordering::Greater)
}

/// Python `min(iterable)` / `min(a, b, ...)`.
pub fn min(args: impl Into<Args>) -> Result<Arg> {
    extremum(args.into(), "min", |o| o == core::cmp::Ordering::Less)
}

/// Python `sum(iterable, /, start=0)`: left fold with `+`, `start` first.
pub fn sum(args: impl Into<Args>) -> Result<Arg> {
    let mut a: Args = args.into();
    let iterable = a.positional.first().cloned().ok_or_else(|| {
        LailaError::type_error("sum() takes at least 1 positional argument (0 given)")
    })?;
    let mut acc = match a.positional.get(1).cloned() {
        Some(s) => s,
        None if a.__contains__("start")? => a.pop("start")?,
        None => Arg::Int(0),
    };
    if acc.as_str().is_some() {
        return Err(LailaError::type_error(
            "sum() can't sum strings [use ''.join(seq) instead]",
        ));
    }
    for x in iterable.__iter__()? {
        acc = acc.__add__(&x)?;
    }
    Ok(acc)
}

use super::errors::ExcKind;
use super::object::PyArg;
