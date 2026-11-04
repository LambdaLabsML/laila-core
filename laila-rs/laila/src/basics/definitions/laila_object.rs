//! Root base class for every laila object.
//!
//! `_LAILA_OBJECT` sits at the very bottom of the laila class hierarchy --
//! both `_LAILA_IDENTIFIABLE_OBJECT` (identity / global ids) and
//! `_LAILA_LOCALLY_ATOMIC_OBJECT` (per-instance locking) derive from it, so
//! entries, policies, pools, futures, the logger and the `Atomic*` wrappers
//! all share whatever lives here.
//!
//! Today that is a single concern: a **creation timestamp** recording when
//! the object was constructed, as an ISO-8601 UTC string with millisecond
//! precision (the same shape as `Record.record_timestamp`). It answers
//! "when was this object created?" for any laila object, independent of when
//! (or whether) it is memorized into a pool.
//!
//! Implementation note: the stamp is applied in `__init__` *after*
//! `super().__init__` returns rather than in `model_post_init` (Python:
//! pydantic wipes private attributes set before the base initialiser runs).
//! In the Rust model pipeline that is the [`ModelHooks::init_tail`] hook.
//! Deserialization paths that bypass `__init__` (e.g. `Entry.from_dict`)
//! restore or re-stamp the value by assigning `_creation_timestamp`
//! directly; the public `creation_timestamp` property is read-only.

use alloc::string::String;
use alloc::vec::Vec;

use crate::_compat::pydantic::{
    InitState, LazyClass, ModelBase, ModelClass, ModelHooks, ModelInstance,
};
use crate::python::{Arg, LailaError, Result};

/// `datetime.now(UTC).isoformat(timespec="milliseconds")` -- the current UTC
/// time as an ISO-8601 string with millisecond precision and a `+00:00`
/// offset, e.g. `"2026-10-04T23:19:28.290+00:00"`.
///
/// Shared by `_LAILA_OBJECT` and by deserializers that need to stamp objects
/// constructed outside `__init__`. Without the `std` feature there is no
/// clock: the stamp is the epoch (`"1970-01-01T00:00:00.000+00:00"`), the
/// integrator supplying a clock should call
/// [`creation_timestamp_from`] instead.
pub fn _now_creation_timestamp() -> String {
    creation_timestamp_from(now_secs())
}

/// The creation stamp for a `time.time()` reading (`secs` since the epoch).
pub fn creation_timestamp_from(secs: f64) -> String {
    crate::_compat::datetime::DateTime::now_iso_ms_from(secs)
        .unwrap_or_else(|_| String::from("1970-01-01T00:00:00.000+00:00"))
}

fn now_secs() -> f64 {
    #[cfg(feature = "std")]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    }
    #[cfg(not(feature = "std"))]
    {
        0.0
    }
}

/// Pydantic base model shared by every laila object.
///
/// Provides `creation_timestamp` -- the ISO-8601 UTC creation time of the
/// instance, captured once at construction (read-only: it is runtime
/// provenance, not configuration; a public setter would also make the
/// CLI-capable machinery treat it as a `laila.args`-configurable field and
/// mirror it into `laila.args.environment`). `None` only for instances built
/// through paths that bypass `__init__` and have not yet restored the value.
///
/// As a Rust value this is the *class*: `_LAILA_OBJECT::__class__()` is the
/// descriptor, `_LAILA_OBJECT::new(args)` constructs an instance,
/// `py_model!(X: _LAILA_OBJECT)` derives from it.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _LAILA_OBJECT;

/// The private-attribute name holding the stamp.
pub const CREATION_TIMESTAMP_ATTR: &str = "_creation_timestamp";

struct Hooks;

impl ModelHooks for Hooks {
    /// Delegate to Pydantic, then stamp the creation timestamp.
    fn init_tail(&self, inst: &ModelInstance, _st: &mut InitState) -> Result<()> {
        inst.set_private(
            CREATION_TIMESTAMP_ATTR,
            Arg::Str(_now_creation_timestamp().into()),
        );
        Ok(())
    }
    fn getattr(&self, inst: &ModelInstance, name: &str) -> Option<Result<Arg>> {
        match name {
            "creation_timestamp" => Some(Ok(inst
                .private(CREATION_TIMESTAMP_ATTR)
                .unwrap_or(Arg::None))),
            _ => None,
        }
    }
    fn setattr(&self, inst: &ModelInstance, name: &str, _value: Arg) -> Option<Result<()>> {
        match name {
            "creation_timestamp" => Some(Err(LailaError::attribute_error(alloc::format!(
                "property 'creation_timestamp' of '{}' object has no setter",
                inst.class_name()
            )))),
            _ => None,
        }
    }
    fn property_getters(&self) -> &'static [&'static str] {
        &["creation_timestamp"]
    }
    fn method_names(&self) -> &'static [&'static str] {
        &["model_post_init"]
    }
}

static HOOKS: Hooks = Hooks;

static CLASS: LazyClass = LazyClass::new(|| ModelClass {
    name: "_LAILA_OBJECT",
    module: "laila.basics.definitions.laila_object",
    bases: Vec::new(),
    fields: Vec::new(),
    private_attrs: alloc::vec![(
        String::from(CREATION_TIMESTAMP_ATTR),
        crate::_compat::pydantic::PrivateAttrInfo {
            default: Some(Arg::None),
            default_factory: None,
        }
    )],
    class_vars: Vec::new(),
    hooks: Some(&HOOKS),
});

impl ModelBase for _LAILA_OBJECT {
    fn __class__() -> &'static ModelClass {
        &CLASS
    }
}

impl _LAILA_OBJECT {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        &CLASS
    }
    /// `_LAILA_OBJECT(**data)` -- a bare instance (a model with no fields).
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<crate::python::Args>) -> Result<Arg> {
        ModelInstance::construct(&CLASS, args.into())
    }
    /// `obj.creation_timestamp` for any model instance that derives from this class.
    pub fn creation_timestamp(inst: &ModelInstance) -> Arg {
        inst.private(CREATION_TIMESTAMP_ATTR).unwrap_or(Arg::None)
    }
}

impl crate::python::builtins::CallValue for _LAILA_OBJECT {
    fn call_value(&self, args: crate::python::Args) -> Result<Arg> {
        ModelInstance::construct(&CLASS, args)
    }
}

impl crate::python::PyArg for _LAILA_OBJECT {
    fn to_arg(&self) -> Arg {
        crate::python::IntoArg::into_arg(CLASS.as_pyclass())
    }
}
impl crate::python::builtins::IsInstance for _LAILA_OBJECT {
    fn isinstance(x: &Arg) -> bool {
        // Model instances derive from the class; typed laila objects
        // (`AtomicInt`, ..) name it among their `__bases__`.
        crate::_compat::pydantic::isinstance_model(x, "_LAILA_OBJECT")
            || matches!(x, Arg::Object(o) if o.__bases__().contains(&"_LAILA_OBJECT"))
            || matches!(x, Arg::Entry(_) | Arg::Pool(_) | Arg::Future(_))
    }
}
