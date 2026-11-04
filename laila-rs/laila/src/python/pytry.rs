//! `py_try!`: the `?` that Tier 2 inserts after every call.
//!
//! Python raises; Rust returns `Result`. `#[laila::python]` wraps each call
//! as `py_try!(expr)?`. Autoref specialisation picks the right arm at compile
//! time without knowing the callee's type: a `Result<T, E>` value is passed
//! through (with `E: Into<LailaError>`), anything else is wrapped in `Ok`.

use super::errors::{LailaError, Result};

#[doc(hidden)]
pub struct PyTry<T>(pub T);
#[doc(hidden)]
pub struct ResultTag;
#[doc(hidden)]
pub struct PlainTag;

#[doc(hidden)]
pub trait PyTryResult {
    fn __py_tag(&self) -> ResultTag;
}
impl<T, E> PyTryResult for PyTry<&Result<T, E>> {
    fn __py_tag(&self) -> ResultTag {
        ResultTag
    }
}

#[doc(hidden)]
pub trait PyTryPlain {
    fn __py_tag(&self) -> PlainTag;
}
impl<T> PyTryPlain for &PyTry<&T> {
    fn __py_tag(&self) -> PlainTag {
        PlainTag
    }
}

impl ResultTag {
    #[inline]
    pub fn run<T, E: Into<LailaError>>(self, v: Result<T, E>) -> Result<T> {
        v.map_err(Into::into)
    }
}
impl PlainTag {
    #[inline]
    pub fn run<T>(self, v: T) -> Result<T> {
        Ok(v)
    }
}

/// `py_try!(expr)` -> `Result<T, LailaError>` whether `expr` is a `Result` or not.
#[macro_export]
macro_rules! py_try {
    ($e:expr) => {{
        let __v = $e;
        #[allow(unused_imports)]
        use $crate::python::pytry::{PyTryPlain as _, PyTryResult as _};
        let __t = (&$crate::python::pytry::PyTry(&__v)).__py_tag();
        __t.run(__v)
    }};
}
