//! Simple constitution: an ordered chain of single-callable code strings.
//!
//! A [`SimpleConstitution`] carries a list of Python source strings, each
//! defining exactly one callable. `build(payload_input)` threads
//! `payload_input` through every callable in order:
//!
//! ```text
//! out = code[0](payload_input)
//! out = code[1](out)
//! ...
//! ```
//!
//! The canonical use case is the *inverse* transformation chain emitted by
//! `Entry.serialize`. When a pool serializes an entry through
//! `base64 -> zlib -> msgpack`, the corresponding simple constitution
//! contains three source strings whose composition decodes the bytes back
//! into the original Python value -- in the reverse order: `msgpack-loads`
//! first, then `zlib-decompress`, then `base64-decode`. (The transformation
//! pipeline emits its inverses already in reverse order, so the
//! constitution itself is just a flat list applied left-to-right.)

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;

use crate::python::builtins::IsInstance;
use crate::python::object::PyObject;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, PyDictBuilder, Result};

use super::constitution::{_exec_one_fn, Constitution};
use super::trust::{check_simple_codes, sign_simple};

/// An ordered chain of single-callable code strings.
///
/// Each element of `codes` is a self-contained Python source string
/// defining exactly one top-level callable. The chain is applied
/// left-to-right at build time. An empty chain is valid and returns
/// `payload_input` unchanged. Instances are immutable; clones alias the
/// same object (a Python reference).
#[derive(Clone)]
pub struct SimpleConstitution {
    _codes: Arc<Vec<String>>,
}

impl SimpleConstitution {
    /// `@_register_kind("simple")`
    pub const _KIND: &'static str = "simple";

    /// `SimpleConstitution(codes=[...])`: the list is shallow-copied
    /// internally so subsequent mutation of the caller's list does not
    /// affect this constitution.
    ///
    /// `TypeError` if `codes` is not a list of strings.
    #[pysig("SimpleConstitution(**data)")]
    pub fn new(args: impl Into<Args>) -> Result<SimpleConstitution> {
        let codes = if data.__contains__("codes")? {
            data.get("codes")
        } else {
            Vec::<Arg>::new().into_arg()
        };
        Self::from_codes(codes)
    }

    /// Typed constructor: `codes` must be a `list` of `str`.
    pub fn from_codes(codes: impl IntoArg) -> Result<SimpleConstitution> {
        let codes = codes.into_arg();
        let Arg::List(_) = &codes else {
            return Err(LailaError::type_error(
                "codes must be a list of Python source strings",
            ));
        };
        let mut out: Vec<String> = Vec::new();
        for c in codes.__iter__()? {
            match c {
                Arg::Str(s) => out.push(s.to_string()),
                _ => {
                    return Err(LailaError::type_error(
                        "codes must be a list of Python source strings",
                    ))
                }
            }
        }
        Ok(SimpleConstitution {
            _codes: Arc::new(out),
        })
    }

    /// A defensive copy of the ordered list of source strings (a `list[str]`).
    pub fn codes(&self) -> Arg {
        self._codes
            .iter()
            .map(|c| Arg::Str(c.as_str().into()))
            .collect::<Vec<_>>()
            .into_arg()
    }

    /// The codes as Rust strings.
    pub fn codes_vec(&self) -> Vec<String> {
        (*self._codes).clone()
    }

    /// Python `is`.
    pub fn is(&self, other: &SimpleConstitution) -> bool {
        Arc::ptr_eq(&self._codes, &other._codes)
    }

    /// Thread *payload_input* through every code in order.
    ///
    /// Each step compiles its source string via `_exec_one_fn` and invokes
    /// the resulting callable on the running value. Compilation happens on
    /// every build (no cache).
    #[pysig("build(self, payload_input=None)")]
    pub fn build(&self, args: impl Into<Args>) -> Result<Arg> {
        let mut current = payload_input;
        for code in self._codes.iter() {
            current = _exec_one_fn(code)?(&current)?;
        }
        Ok(current)
    }

    /// Serialize as `{"_kind": "simple", "codes": [...], "_hmac": ...}`.
    ///
    /// `_hmac` signs the code list with the per-user constitution secret so
    /// a hand-written simple constitution round-trips for the same user;
    /// pipeline-emitted codes do not need it (they are accepted by template
    /// on the way back).
    pub fn as_dict(&self) -> Result<Arg> {
        let mut d = PyDictBuilder::new();
        d.insert("_kind", Arg::Str(Self::_KIND.into()));
        d.insert("codes", self.codes());
        d.insert("_hmac", Arg::Str(sign_simple(self.codes())?.into()));
        Ok(d.finish())
    }

    /// Rebuild a `SimpleConstitution` from its serialized dict.
    ///
    /// Missing `codes` is treated as an empty chain. The dict comes from
    /// storage or the wire, so every snippet must pass the trust gate
    /// (`trust::check_simple_codes`): it has to match a loaded
    /// transformation's recovery-code template or be byte-identical to code
    /// generated in this process. Anything else raises `ValueError` *before*
    /// it could be executed.
    pub fn _from_dict(in_dict: &Arg) -> Result<SimpleConstitution> {
        let codes = if in_dict.__contains__("codes")? {
            in_dict.__getitem__("codes")?
        } else {
            Vec::<Arg>::new().into_arg()
        };
        let hmac = if in_dict.__contains__("_hmac")? {
            in_dict.__getitem__("_hmac")?
        } else {
            Arg::None
        };
        check_simple_codes(codes.clone(), hmac)?;
        Self::from_codes(codes)
    }

    /// pydantic `repr`: private attributes are not shown.
    pub fn __repr__(&self) -> String {
        "SimpleConstitution()".into()
    }
}

impl core::fmt::Debug for SimpleConstitution {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}

impl PartialEq for SimpleConstitution {
    /// pydantic equality (private attributes included): same codes.
    fn eq(&self, other: &SimpleConstitution) -> bool {
        self.is(other) || self._codes == other._codes
    }
}

impl PyObject for SimpleConstitution {
    fn as_any(&self) -> &dyn Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<SimpleConstitution>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        "SimpleConstitution"
    }
    fn __repr__(&self) -> String {
        SimpleConstitution::__repr__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<SimpleConstitution>()
            .is_some_and(|o| self == o)
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error(
            "unhashable type: 'SimpleConstitution'",
        ))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "codes" => Ok(self.codes()),
            "_kind" => Ok(Arg::Str(Self::_KIND.into())),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'SimpleConstitution' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        super::constitution::call_method(&Constitution::Simple(self.clone()), name, args)
    }
}

impl IntoArg for SimpleConstitution {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for SimpleConstitution {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for SimpleConstitution {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<SimpleConstitution>())
    }
}
