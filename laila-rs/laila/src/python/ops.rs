//! Python operators on `Arg`: `+ - * / %`, unary `-`, and ordering.
//!
//! Arithmetic follows Python's numeric tower (`bool` ⊂ `int` ⊂ `float`),
//! sequence concatenation (`str + str`, `list + list`, `tuple + tuple`) and
//! repetition (`str * int`, `list * int`), and raises `TypeError` with
//! CPython's exact message otherwise. Because the operators can raise they
//! return `Result<Arg>`; Tier 2 wraps every arithmetic expression in
//! `py_try!(..)?`, which passes plain Rust scalars through untouched.
//!
//! Ordering (`<`, `<=`, `>`, `>=`) is `PartialOrd`: numbers compare
//! numerically, strings and bytes lexicographically, lists and tuples
//! element-wise; incomparable types return `None` (so `<` is `false`), the
//! place where Python would raise `TypeError`.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cmp::Ordering;
use core::ops::{Add, BitAnd, BitOr, BitXor, Div, Mul, Neg, Rem, Sub};

use super::errors::{LailaError, Result};
use super::object::{Arg, IntoArg, PyArg, PySetInner};
use super::sync::RwLock;
use crate::_compat::ndarray::{numeric_binop, NumOp};

fn unsupported(op: &str, a: &Arg, b: &Arg) -> LailaError {
    LailaError::type_error(alloc::format!(
        "unsupported operand type(s) for {op}: '{}' and '{}'",
        a.type_name(),
        b.type_name()
    ))
}

/// `int`-like operand (`bool` counts as `int`).
fn as_int(a: &Arg) -> Option<i64> {
    match a {
        Arg::Int(i) => Some(*i),
        Arg::Bool(b) => Some(*b as i64),
        _ => None,
    }
}

fn repeat<T: Clone>(v: &[T], n: i64) -> Vec<T> {
    let mut out = Vec::with_capacity(v.len() * n.max(0) as usize);
    for _ in 0..n.max(0) {
        out.extend_from_slice(v);
    }
    out
}

impl Arg {
    pub fn __add__(&self, other: &Arg) -> Result<Arg> {
        if let (Some(a), Some(b)) = (as_int(self), as_int(other)) {
            return Ok(Arg::Int(a.wrapping_add(b)));
        }
        if let Some(r) = numeric_binop(NumOp::Add, self, other) {
            return r;
        }
        if let (Some(a), Some(b)) = (self.as_number(), other.as_number()) {
            return Ok(Arg::Float(a + b));
        }
        match (self, other) {
            (Arg::Str(a), Arg::Str(b)) => {
                let mut s = String::with_capacity(a.len() + b.len());
                s.push_str(a);
                s.push_str(b);
                Ok(Arg::Str(s.into()))
            }
            (Arg::Bytes(a), Arg::Bytes(b)) => {
                let mut v = Vec::with_capacity(a.len() + b.len());
                v.extend_from_slice(a);
                v.extend_from_slice(b);
                Ok(Arg::Bytes(v.into()))
            }
            (Arg::List(a), Arg::List(b)) => {
                let mut v = a.read().clone();
                v.extend(b.read().iter().cloned());
                Ok(Arg::List(Arc::new(RwLock::new(v))))
            }
            (Arg::Tuple(a), Arg::Tuple(b)) => {
                let mut v: Vec<Arg> = a.to_vec();
                v.extend(b.iter().cloned());
                Ok(Arg::Tuple(v.into()))
            }
            // `group_future + group_future` (merge the children in place).
            (Arg::Future(f), _) => {
                let out = f.__add__(other.clone())?;
                if crate::python::builtins::is_not_implemented(&out) {
                    return Err(unsupported("+", self, other));
                }
                Ok(out)
            }
            // `manifest + manifest` (merge into a new manifest).
            (Arg::Entry(e), _) if crate::Manifest::from_entry(e).is_some() => {
                let m = crate::Manifest::from_entry(e).expect("checked");
                let out = m.__add__(other.clone())?;
                if crate::python::builtins::is_not_implemented(&out) {
                    return Err(unsupported("+", self, other));
                }
                Ok(out)
            }
            (Arg::Str(_), _) => Err(LailaError::type_error(alloc::format!(
                "can only concatenate str (not \"{}\") to str",
                other.type_name()
            ))),
            (Arg::List(_), _) => Err(LailaError::type_error(alloc::format!(
                "can only concatenate list (not \"{}\") to list",
                other.type_name()
            ))),
            (Arg::Tuple(_), _) => Err(LailaError::type_error(alloc::format!(
                "can only concatenate tuple (not \"{}\") to tuple",
                other.type_name()
            ))),
            _ => Err(unsupported("+", self, other)),
        }
    }

    pub fn __sub__(&self, other: &Arg) -> Result<Arg> {
        if let (Some(a), Some(b)) = (as_int(self), as_int(other)) {
            return Ok(Arg::Int(a.wrapping_sub(b)));
        }
        if let Some(r) = numeric_binop(NumOp::Sub, self, other) {
            return r;
        }
        if let (Some(a), Some(b)) = (self.as_number(), other.as_number()) {
            return Ok(Arg::Float(a - b));
        }
        if let Some(r) = self.set_op(SetOp::Difference, other) {
            return r;
        }
        Err(unsupported("-", self, other))
    }

    /// `a | b`: `int | int`, `bool | bool`, `set | set`, `dict | dict`.
    pub fn __or__(&self, other: &Arg) -> Result<Arg> {
        if let (Arg::Bool(a), Arg::Bool(b)) = (self, other) {
            return Ok(Arg::Bool(*a | *b));
        }
        if let (Some(a), Some(b)) = (as_int(self), as_int(other)) {
            return Ok(Arg::Int(a | b));
        }
        if let (Arg::Dict(a), Arg::Dict(b)) = (self, other) {
            let mut d = a.read().clone();
            for (k, v) in b.read().iter() {
                d.insert(k.clone(), v.clone());
            }
            return Ok(Arg::Dict(Arc::new(RwLock::new(d))));
        }
        if let Some(r) = self.set_op(SetOp::Union, other) {
            return r;
        }
        Err(unsupported("|", self, other))
    }

    /// `a & b`: `int & int`, `bool & bool`, `set & set`.
    pub fn __and__(&self, other: &Arg) -> Result<Arg> {
        if let (Arg::Bool(a), Arg::Bool(b)) = (self, other) {
            return Ok(Arg::Bool(*a & *b));
        }
        if let (Some(a), Some(b)) = (as_int(self), as_int(other)) {
            return Ok(Arg::Int(a & b));
        }
        if let Some(r) = self.set_op(SetOp::Intersection, other) {
            return r;
        }
        Err(unsupported("&", self, other))
    }

    /// `a ^ b`: `int ^ int`, `bool ^ bool`, `set ^ set`.
    pub fn __xor__(&self, other: &Arg) -> Result<Arg> {
        if let (Arg::Bool(a), Arg::Bool(b)) = (self, other) {
            return Ok(Arg::Bool(*a ^ *b));
        }
        if let (Some(a), Some(b)) = (as_int(self), as_int(other)) {
            return Ok(Arg::Int(a ^ b));
        }
        if let Some(r) = self.set_op(SetOp::SymmetricDifference, other) {
            return r;
        }
        Err(unsupported("^", self, other))
    }

    /// Set algebra between `set` / `frozenset` operands (the result takes
    /// the left operand's type, like Python). `None` when either side is
    /// not a set.
    fn set_op(&self, op: SetOp, other: &Arg) -> Option<Result<Arg>> {
        let a = self.set_items()?;
        let b = other.set_items()?;
        let out: super::object::PySetInner = match op {
            SetOp::Union => a.union(&b).cloned().collect(),
            SetOp::Intersection => a.intersection(&b).cloned().collect(),
            SetOp::Difference => a.difference(&b).cloned().collect(),
            SetOp::SymmetricDifference => a.symmetric_difference(&b).cloned().collect(),
        };
        Some(Ok(match self {
            Arg::FrozenSet(_) => Arg::FrozenSet(Arc::new(out)),
            _ => Arg::Set(Arc::new(RwLock::new(out))),
        }))
    }

    fn set_items(&self) -> Option<super::object::PySetInner> {
        match self {
            Arg::Set(s) => Some(s.read().clone()),
            Arg::FrozenSet(s) => Some((**s).clone()),
            _ => None,
        }
    }

    pub fn __mul__(&self, other: &Arg) -> Result<Arg> {
        if let (Some(a), Some(b)) = (as_int(self), as_int(other)) {
            return Ok(Arg::Int(a.wrapping_mul(b)));
        }
        if let Some(r) = numeric_binop(NumOp::Mul, self, other) {
            return r;
        }
        if let (Some(a), Some(b)) = (self.as_number(), other.as_number()) {
            return Ok(Arg::Float(a * b));
        }
        // sequence repetition, either operand order
        let (seq, n) = match (as_int(self), as_int(other)) {
            (Some(n), None) => (other, n),
            (None, Some(n)) => (self, n),
            _ => return Err(unsupported("*", self, other)),
        };
        match seq {
            Arg::Str(s) => Ok(Arg::Str(s.repeat(n.max(0) as usize).into())),
            Arg::Bytes(b) => Ok(Arg::Bytes(b.repeat(n.max(0) as usize).into())),
            Arg::List(l) => Ok(Arg::List(Arc::new(RwLock::new(repeat(&l.read(), n))))),
            Arg::Tuple(t) => Ok(Arg::Tuple(repeat(t, n).into())),
            _ => Err(LailaError::type_error(alloc::format!(
                "can't multiply sequence by non-int of type '{}'",
                seq.type_name()
            ))),
        }
    }

    /// Python 3 `/`: always a float.
    pub fn __truediv__(&self, other: &Arg) -> Result<Arg> {
        // `pathlib.Path / segment` and `str / Path` join paths.
        #[cfg(feature = "std")]
        {
            use super::stdlib::pathlib::Path;
            if let Some(p) = self.downcast_ref::<Path>() {
                return Ok(p.__truediv__(other)?.into_arg());
            }
            if let Some(p) = other.downcast_ref::<Path>() {
                return Ok(p.__rtruediv__(self)?.into_arg());
            }
        }
        if let Some(r) = numeric_binop(NumOp::Div, self, other) {
            return r;
        }
        match (self.as_number(), other.as_number()) {
            (Some(a), Some(b)) => {
                if b == 0.0 {
                    Err(LailaError::new(
                        super::errors::ExcKind::ZeroDivisionError,
                        "division by zero",
                    ))
                } else {
                    Ok(Arg::Float(a / b))
                }
            }
            _ => Err(unsupported("/", self, other)),
        }
    }

    /// Python `%` (result takes the sign of the divisor).
    pub fn __mod__(&self, other: &Arg) -> Result<Arg> {
        if let Arg::Str(template) = self {
            return Ok(Arg::Str(
                super::strfmt::percent_format(template, other)?.into(),
            ));
        }
        if let (Some(a), Some(b)) = (as_int(self), as_int(other)) {
            if b == 0 {
                return Err(LailaError::new(
                    super::errors::ExcKind::ZeroDivisionError,
                    "integer modulo by zero",
                ));
            }
            let r = a.wrapping_rem(b);
            return Ok(Arg::Int(if r != 0 && (r < 0) != (b < 0) {
                r + b
            } else {
                r
            }));
        }
        if let (Some(a), Some(b)) = (self.as_number(), other.as_number()) {
            if b == 0.0 {
                return Err(LailaError::new(
                    super::errors::ExcKind::ZeroDivisionError,
                    "float modulo",
                ));
            }
            let r = a % b;
            return Ok(Arg::Float(if r != 0.0 && (r < 0.0) != (b < 0.0) {
                r + b
            } else {
                r
            }));
        }
        Err(unsupported("%", self, other))
    }

    pub fn __neg__(&self) -> Result<Arg> {
        match self {
            Arg::Int(i) => Ok(Arg::Int(i.wrapping_neg())),
            Arg::Bool(b) => Ok(Arg::Int(-(*b as i64))),
            Arg::Float(f) => Ok(Arg::Float(-f)),
            _ => Err(LailaError::type_error(alloc::format!(
                "bad operand type for unary -: '{}'",
                self.type_name()
            ))),
        }
    }

    /// `a < b` etc.; `None` where Python raises `TypeError`.
    pub fn py_cmp(&self, other: &Arg) -> Option<Ordering> {
        if let (Some(a), Some(b)) = (as_int(self), as_int(other)) {
            return Some(a.cmp(&b));
        }
        if let (Some(a), Some(b)) = (self.as_number(), other.as_number()) {
            return a.partial_cmp(&b);
        }
        match (self, other) {
            (Arg::Str(a), Arg::Str(b)) => Some(a.cmp(b)),
            (Arg::Bytes(a), Arg::Bytes(b)) => Some(a.cmp(b)),
            (Arg::List(a), Arg::List(b)) => cmp_seq(&a.read(), &b.read()),
            (Arg::Tuple(a), Arg::Tuple(b)) => cmp_seq(a, b),
            // Sets order by inclusion (`a <= b` is `a.issubset(b)`); two
            // sets neither of which contains the other are incomparable.
            (Arg::Set(a), Arg::Set(b)) => cmp_set(&a.read(), &b.read()),
            (Arg::Set(a), Arg::FrozenSet(b)) => cmp_set(&a.read(), b),
            (Arg::FrozenSet(a), Arg::Set(b)) => cmp_set(a, &b.read()),
            (Arg::FrozenSet(a), Arg::FrozenSet(b)) => cmp_set(a, b),
            _ => None,
        }
    }
}

fn cmp_set(a: &PySetInner, b: &PySetInner) -> Option<Ordering> {
    let a_in_b = a.iter().all(|x| b.contains(x));
    let b_in_a = b.iter().all(|x| a.contains(x));
    match (a_in_b, b_in_a) {
        (true, true) => Some(Ordering::Equal),
        (true, false) => Some(Ordering::Less),
        (false, true) => Some(Ordering::Greater),
        (false, false) => None,
    }
}

fn cmp_seq(a: &[Arg], b: &[Arg]) -> Option<Ordering> {
    for (x, y) in a.iter().zip(b.iter()) {
        match x.py_cmp(y)? {
            Ordering::Equal => continue,
            o => return Some(o),
        }
    }
    Some(a.len().cmp(&b.len()))
}

impl PartialOrd for Arg {
    fn partial_cmp(&self, other: &Arg) -> Option<Ordering> {
        self.py_cmp(other)
    }
}

// ---- operator traits: `Arg op T`, `&Arg op T`, `T op Arg` for Rust scalars ----

macro_rules! binop {
    ($tr:ident, $m:ident, $py:ident) => {
        impl<T: IntoArg> $tr<T> for Arg {
            type Output = Result<Arg>;
            fn $m(self, rhs: T) -> Result<Arg> {
                Arg::$py(&self, &rhs.into_arg())
            }
        }
        impl<T: IntoArg> $tr<T> for &Arg {
            type Output = Result<Arg>;
            fn $m(self, rhs: T) -> Result<Arg> {
                Arg::$py(self, &rhs.into_arg())
            }
        }
        // One integer impl only: `1 + x` with `x: Arg` must infer `i64`
        // directly, so the `Output` is known before `py_try!` dispatches.
        binop!(@lhs $tr, $m, $py; i64, f64, bool, &str, String);
    };
    (@lhs $tr:ident, $m:ident, $py:ident; $($t:ty),*) => {$(
        impl $tr<Arg> for $t {
            type Output = Result<Arg>;
            fn $m(self, rhs: Arg) -> Result<Arg> {
                Arg::$py(&self.into_arg(), &rhs)
            }
        }
        impl<'a> $tr<&'a Arg> for $t {
            type Output = Result<Arg>;
            fn $m(self, rhs: &'a Arg) -> Result<Arg> {
                Arg::$py(&self.into_arg(), rhs)
            }
        }
    )*};
}
#[derive(Clone, Copy)]
enum SetOp {
    Union,
    Intersection,
    Difference,
    SymmetricDifference,
}

binop!(Add, add, __add__);
binop!(BitOr, bitor, __or__);
binop!(BitAnd, bitand, __and__);
binop!(BitXor, bitxor, __xor__);
binop!(Sub, sub, __sub__);
binop!(Mul, mul, __mul__);
binop!(Div, div, __truediv__);
binop!(Rem, rem, __mod__);

impl Neg for Arg {
    type Output = Result<Arg>;
    fn neg(self) -> Result<Arg> {
        self.__neg__()
    }
}
impl Neg for &Arg {
    type Output = Result<Arg>;
    fn neg(self) -> Result<Arg> {
        self.__neg__()
    }
}

// ---- ordering against plain Rust scalars: `n < 3`, `x >= 0.5` ----

macro_rules! partial_ord_scalar {
    ($($t:ty),*) => {$(
        impl PartialOrd<$t> for Arg {
            fn partial_cmp(&self, other: &$t) -> Option<Ordering> {
                self.py_cmp(&(*other).into_arg())
            }
        }
    )*};
}
partial_ord_scalar!(i64, i32, usize, f64);

// `time.monotonic() < deadline` where `deadline` is an `Arg`.
macro_rules! partial_ord_scalar_rev {
    ($($t:ty),*) => {$(
        impl PartialEq<Arg> for $t {
            fn eq(&self, other: &Arg) -> bool {
                other == self
            }
        }
        impl PartialOrd<Arg> for $t {
            fn partial_cmp(&self, other: &Arg) -> Option<Ordering> {
                (*self).into_arg().py_cmp(other)
            }
        }
    )*};
}
partial_ord_scalar_rev!(i64, i32, usize, f64);

// `o.uuid == U1` where the typed getter is a `String` and the module
// constant an `Arg`.
impl PartialEq<Arg> for String {
    fn eq(&self, other: &Arg) -> bool {
        other == self
    }
}
impl PartialEq<Arg> for &str {
    fn eq(&self, other: &Arg) -> bool {
        other == self
    }
}
/// `d.popitem() == ("b", 2)`: a typed pair against a Python tuple.
impl<A: PyArg, B: PyArg> PartialEq<Arg> for (A, B) {
    fn eq(&self, other: &Arg) -> bool {
        *other == Arg::Tuple(alloc::vec![self.0.to_arg(), self.1.to_arg()].into())
    }
}
impl PartialEq<Arg> for bool {
    fn eq(&self, other: &Arg) -> bool {
        *other == Arg::Bool(*self)
    }
}

impl PartialOrd<&str> for Arg {
    fn partial_cmp(&self, other: &&str) -> Option<Ordering> {
        match self {
            Arg::Str(s) => Some((**s).cmp(*other)),
            _ => None,
        }
    }
}

// ---- `a <= b`: the pool duplication operator vs. a plain comparison ----

/// `a <= b` where `a` is a pool (`cache <= origin` copies *origin* into
/// *cache* through central memory): the fallible `__le__`, a `GroupFuture`.
/// The rewriter writes `(&a).__laila_le(&b)` so this by-value impl on
/// `a += b`: the in-place protocol. A `Manifest` (typed or as an `Arg`)
/// grows in place through `__iadd__` and keeps its identity; everything
/// else is `a = a + b` ([`PlainIAdd`], the autoref fallback).
pub trait InPlaceAdd<Rhs> {
    type Output;
    fn __laila_iadd(self, rhs: Rhs) -> Self::Output;
}
impl<R: IntoArg> InPlaceAdd<R> for &crate::Manifest {
    type Output = Result<crate::Manifest>;
    fn __laila_iadd(self, rhs: R) -> Result<crate::Manifest> {
        self.iadd_op(rhs)
    }
}
impl<R: IntoArg> InPlaceAdd<R> for &Arg {
    type Output = Result<Arg>;
    fn __laila_iadd(self, rhs: R) -> Result<Arg> {
        if let Some(m) = crate::Manifest::from_arg(self) {
            return m.iadd_op(rhs).map(IntoArg::into_arg);
        }
        Arg::__add__(self, &rhs.into_arg())
    }
}
/// `t += x` on a Rust `f64` local: Python float arithmetic -- the right
/// operand may be a dynamic value (`nxt += period` with `period` an
/// `Arg`), which still leaves the local a float.
pub trait FloatIAddRhs {
    fn add_to_f64(self, lhs: f64) -> Result<f64>;
}
macro_rules! float_iadd_rhs {
    ($($t:ty),*) => {$(
        impl FloatIAddRhs for $t {
            #[inline]
            fn add_to_f64(self, lhs: f64) -> Result<f64> {
                Ok(lhs + self as f64)
            }
        }
    )*};
}
float_iadd_rhs!(f64, f32, i64, i32, u32, usize);
impl FloatIAddRhs for Arg {
    fn add_to_f64(self, lhs: f64) -> Result<f64> {
        (&self).add_to_f64(lhs)
    }
}
impl FloatIAddRhs for &Arg {
    fn add_to_f64(self, lhs: f64) -> Result<f64> {
        let out = Arg::__add__(&Arg::Float(lhs), self)?;
        out.as_f64().ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "unsupported operand type(s) for +=: 'float' and '{}'",
                self.type_name()
            ))
        })
    }
}
impl<R: FloatIAddRhs> InPlaceAdd<R> for &f64 {
    type Output = Result<f64>;
    #[inline]
    fn __laila_iadd(self, rhs: R) -> Result<f64> {
        rhs.add_to_f64(*self)
    }
}
/// The `a = a + b` fallback of [`InPlaceAdd`] (plain Rust operands).
pub trait PlainIAdd<Rhs> {
    type Output;
    fn __laila_iadd(self, rhs: Rhs) -> Self::Output;
}
impl<T: Add<R> + Clone, R> PlainIAdd<R> for &&T {
    type Output = <T as Add<R>>::Output;
    #[inline]
    fn __laila_iadd(self, rhs: R) -> Self::Output {
        T::clone(*self) + rhs
    }
}

/// `&Pool` / `&Arg` wins over the [`PlainLe`] fallback (autoref
/// specialisation, see `laila::python::context`).
pub trait PoolLe<Rhs: ?Sized> {
    fn __laila_le(self, other: &Rhs) -> Result<Arg>;
}
impl<R: PyArg + ?Sized> PoolLe<R> for &crate::data::Pool {
    fn __laila_le(self, other: &R) -> Result<Arg> {
        let out = self.__le__(other.to_arg())?;
        if crate::python::builtins::is_not_implemented(&out) {
            return Err(LailaError::type_error(alloc::format!(
                "'<=' not supported between instances of '{}' and '{}'",
                self.class_name(),
                other.to_arg().type_name()
            )));
        }
        Ok(out)
    }
}
impl<R: PyArg + ?Sized> PoolLe<R> for &Arg {
    fn __laila_le(self, other: &R) -> Result<Arg> {
        let other = other.to_arg();
        if let Arg::Pool(p) = self {
            return p.__laila_le(&other);
        }
        // `pool[manifest] <= pool[manifest]`: the wrapper's `__le__`.
        if let Arg::Object(o) = self {
            if let Some(w) = o.as_any().downcast_ref::<crate::data::PoolWrapper>() {
                let out = w.__le__(other.clone())?;
                if crate::python::builtins::is_not_implemented(&out) {
                    return Err(LailaError::type_error(alloc::format!(
                        "'<=' not supported between instances of 'PoolWrapper' and '{}'",
                        other.type_name()
                    )));
                }
                return Ok(out);
            }
        }
        match self.py_cmp(&other) {
            Some(o) => Ok(Arg::Bool(o != Ordering::Greater)),
            // Incomparable sets: `a <= b` is simply `False`.
            None if matches!(self, Arg::Set(_) | Arg::FrozenSet(_))
                && matches!(other, Arg::Set(_) | Arg::FrozenSet(_)) =>
            {
                Ok(Arg::Bool(false))
            }
            None => Err(LailaError::type_error(alloc::format!(
                "'<=' not supported between instances of '{}' and '{}'",
                self.type_name(),
                other.type_name()
            ))),
        }
    }
}
/// `x < 0` / `x > 0` / `x >= 0` / `x <= 0` against an integer literal:
/// Python compares a float with an int (`delay > 0`), which Rust's
/// `PartialOrd` does not -- a float operand against an `i64` wins over
/// the [`PlainCmp`] fallback (autoref specialisation).
pub trait FloatIntCmp<Rhs: ?Sized> {
    fn __laila_lt(self, other: &Rhs) -> bool;
    fn __laila_gt(self, other: &Rhs) -> bool;
    fn __laila_le(self, other: &Rhs) -> bool;
    fn __laila_ge(self, other: &Rhs) -> bool;
}
impl FloatIntCmp<i64> for &f64 {
    #[inline]
    fn __laila_lt(self, other: &i64) -> bool {
        *self < *other as f64
    }
    #[inline]
    fn __laila_gt(self, other: &i64) -> bool {
        *self > *other as f64
    }
    #[inline]
    fn __laila_le(self, other: &i64) -> bool {
        *self <= *other as f64
    }
    #[inline]
    fn __laila_ge(self, other: &i64) -> bool {
        *self >= *other as f64
    }
}
impl FloatIntCmp<i64> for &i64 {
    #[inline]
    fn __laila_lt(self, other: &i64) -> bool {
        *self < *other
    }
    #[inline]
    fn __laila_gt(self, other: &i64) -> bool {
        *self > *other
    }
    #[inline]
    fn __laila_le(self, other: &i64) -> bool {
        *self <= *other
    }
    #[inline]
    fn __laila_ge(self, other: &i64) -> bool {
        *self >= *other
    }
}
impl FloatIntCmp<f64> for &i64 {
    #[inline]
    fn __laila_lt(self, other: &f64) -> bool {
        (*self as f64) < *other
    }
    #[inline]
    fn __laila_gt(self, other: &f64) -> bool {
        (*self as f64) > *other
    }
    #[inline]
    fn __laila_le(self, other: &f64) -> bool {
        (*self as f64) <= *other
    }
    #[inline]
    fn __laila_ge(self, other: &f64) -> bool {
        (*self as f64) >= *other
    }
}
/// Fallback for `a < b` & co. against an integer literal: `PartialOrd`.
pub trait PlainCmp<Rhs: ?Sized> {
    fn __laila_lt(self, other: &Rhs) -> bool;
    fn __laila_gt(self, other: &Rhs) -> bool;
    fn __laila_le(self, other: &Rhs) -> bool;
    fn __laila_ge(self, other: &Rhs) -> bool;
}
impl<T: PartialOrd<R> + ?Sized, R: ?Sized> PlainCmp<R> for &&T {
    #[inline]
    fn __laila_lt(self, other: &R) -> bool {
        **self < *other
    }
    #[inline]
    fn __laila_gt(self, other: &R) -> bool {
        **self > *other
    }
    #[inline]
    fn __laila_le(self, other: &R) -> bool {
        **self <= *other
    }
    #[inline]
    fn __laila_ge(self, other: &R) -> bool {
        **self >= *other
    }
}

/// The value of `a <= b` when the mixed int/float lowering may also hit a
/// pool (`DefaultPool() <= 5`): a plain `bool` from [`FloatIntCmp`] /
/// [`PlainCmp`], or the `Result<Arg>` of [`PoolLe`]; both become a
/// `Result` so the lowering can apply `?` uniformly.
pub trait LeOut {
    type Out;
    fn le_out(self) -> Result<Self::Out>;
}
impl LeOut for bool {
    type Out = bool;
    #[inline]
    fn le_out(self) -> Result<bool> {
        Ok(self)
    }
}
impl<T> LeOut for Result<T> {
    type Out = T;
    #[inline]
    fn le_out(self) -> Result<T> {
        self
    }
}

/// Fallback for every other `a <= b`: Rust's `PartialOrd`.
pub trait PlainLe<Rhs: ?Sized> {
    fn __laila_le(self, other: &Rhs) -> Result<bool>;
}
impl<T: PartialOrd<R> + ?Sized, R: ?Sized> PlainLe<R> for &&T {
    #[inline]
    fn __laila_le(self, other: &R) -> Result<bool> {
        Ok(**self <= *other)
    }
}
