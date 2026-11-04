//! Abstract base classes for data transformations and the pipeline that chains them.
//!
//! A *transformation* is a reversible function on opaque data: an
//! encoding (base64), a compression (zlib), an encryption (AES), a
//! serialisation step (msgpack, pickle, numpy, torch), or a wrapper
//! into a JSON-friendly string. They are the building blocks that
//! `SimpleConstitution` strings together to recover an entry's
//! payload from its serialised form.
//!
//! Two layers live in this module:
//!
//! - [`DataTransformation`] (Python `_data_transformation`) -- the trait every
//!   concrete transformation implements; [`Transformation`] is the shared
//!   handle (`Arc<dyn DataTransformation>`) that stands in for a Python
//!   reference to an instance. Concrete transformations register their
//!   recovery-code *template* in the [`REGISTRY`] keyed by their `name`
//!   field, so the constitution machinery can decide whether a recovery
//!   snippet read back from a pool is trusted.
//! - [`TransformationSequence`] -- an ordered pipeline of transformations.
//!   `forward` applies them in order on the way out (when serialising into
//!   a pool); `backward` is invoked *implicitly* by the constitution by
//!   replaying the inverse code snippets that `forward` collects.
//!
//! An empty pipeline is an explicit identity, which lets pools default
//! their `transformations` field to `None` / `[]` without any
//! special-casing on the call sites.
//!
//! Rust note: the Python class attributes `_KNOWN_BACKWARD_CODES`,
//! `_PATTERN_CACHE`, `REGISTRY` and `_TRUST_LOCK` are process-wide statics
//! here; the built-in transformations are "imported" (registered) on first
//! use of the registry because Rust has no import-time side effects.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;

use crate::_codecs::recovery_codes;
use crate::_compat::pyliteral::literal_eval;
use crate::python::builtins::IsInstance;
use crate::python::object::PyObject;
use crate::python::pyrepr::str_repr;
use crate::python::sync::{Mutex, RwLock};
use crate::python::{Arg, Args, ExcKind, IntoArg, LailaError, PyArg, PyDictBuilder, Result};

// Sentinel ``backward_kwargs`` used to render a class's recovery-code
// *template*: its ``repr`` is located in the rendered code and replaced
// by a capture group (see ``_backward_code_pattern``).

/// `_SLOT_KWARGS = {"__laila_slot__": 0}`
pub fn _SLOT_KWARGS() -> Arg {
    let mut d = PyDictBuilder::new();
    d.insert("__laila_slot__", Arg::Int(0));
    d.finish()
}

/// `_SLOT_KWARGS_REPR = repr(_SLOT_KWARGS)`
pub fn _SLOT_KWARGS_REPR() -> String {
    _SLOT_KWARGS().__repr__()
}

/// `True` when *text* is the `repr` of a plain literal dict.
pub fn _is_literal_dict(text: &str) -> bool {
    matches!(literal_eval(text), Ok(Arg::Dict(_)))
}

/// A `(sentinel_text, validator)` pair of a recovery-code template
/// (`_template_slots`).
#[derive(Clone)]
pub struct Slot {
    pub text: String,
    pub validate: fn(&str) -> bool,
}

/// `_template_slots()` default: the `backward_kwargs` slot.
pub fn default_template_slots() -> Vec<Slot> {
    alloc::vec![Slot {
        text: _SLOT_KWARGS_REPR(),
        validate: _is_literal_dict,
    }]
}

// --------------------------------------------------------------------------
// Shared pydantic fields
// --------------------------------------------------------------------------

/// The four pydantic fields of `_data_transformation`
/// (`name`, `forward_kwargs`, `backward_kwargs`, `backward_code`).
pub struct TransformationFields {
    pub name: RwLock<String>,
    pub forward_kwargs: RwLock<Arg>,
    pub backward_kwargs: RwLock<Arg>,
    pub backward_code: RwLock<String>,
    /// Object identity: a typed transformation is a value in Rust, so the
    /// handle it becomes when read as an `Arg` (`cd.serializer = s` then
    /// `cd.serializer is s`) carries this token instead of an address.
    /// `clone()` (= `copy.copy`) mints a fresh one.
    pub ident: core::sync::atomic::AtomicUsize,
}

fn next_ident() -> core::sync::atomic::AtomicUsize {
    static NEXT: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(1);
    core::sync::atomic::AtomicUsize::new(NEXT.fetch_add(1, core::sync::atomic::Ordering::Relaxed))
}

/// `copy.copy(t)`: a snapshot of the four fields (pydantic models copy by
/// value; the kwargs dicts are shared like Python's shallow copy).
impl Clone for TransformationFields {
    fn clone(&self) -> Self {
        Self {
            name: RwLock::new(self.name.read().clone()),
            forward_kwargs: RwLock::new(self.forward_kwargs.read().clone()),
            backward_kwargs: RwLock::new(self.backward_kwargs.read().clone()),
            backward_code: RwLock::new(self.backward_code.read().clone()),
            ident: next_ident(),
        }
    }
}
impl TransformationFields {
    /// The identity token (`id(t)`).
    pub fn ident(&self) -> usize {
        self.ident.load(core::sync::atomic::Ordering::Relaxed)
    }
    /// Make `self` the same Python object as *other* (a typed value read
    /// as a handle).
    pub fn share_ident(&self, other: &TransformationFields) {
        self.ident
            .store(other.ident(), core::sync::atomic::Ordering::Relaxed);
    }
}

/// The pydantic release the Python package validates with (the version
/// appears in every `ValidationError` text).
pub const PYDANTIC_ERRORS_URL: &str = "https://errors.pydantic.dev/2.13/v/";

/// One line item of a pydantic `ValidationError`: `(loc, message, type, input)`.
pub type ValidationItem<'a> = (&'a str, &'a str, &'a str, &'a Arg);

/// `str(pydantic.ValidationError)` for *class* with the given items.
pub fn pydantic_validation_error(class: &str, items: &[ValidationItem<'_>]) -> LailaError {
    let mut s = alloc::format!(
        "{} validation error{} for {class}",
        items.len(),
        if items.len() == 1 { "" } else { "s" }
    );
    for (loc, msg, ty, input) in items {
        // An empty ``loc`` (model-level errors raised in ``model_post_init``)
        // has no location line.
        if !loc.is_empty() {
            s.push('\n');
            s.push_str(loc);
        }
        s.push_str(&alloc::format!(
            "\n  {msg} [type={ty}, input_value={}, input_type={}]\n    For further information visit {PYDANTIC_ERRORS_URL}{ty}",
            input.__repr__(),
            input.type_name()
        ));
    }
    LailaError::new(ExcKind::ValidationError, s)
}

/// `dict[str, Any]` field: the keyword must be a dict with `str` keys.
fn kwargs_field(class: &str, field: &str, v: Arg) -> Result<Arg> {
    let Arg::Dict(_) = &v else {
        return Err(pydantic_validation_error(
            class,
            &[(field, "Input should be a valid dictionary", "dict_type", &v)],
        ));
    };
    for k in v.__iter__()?.iter() {
        if !matches!(k, Arg::Str(_)) {
            // pydantic's loc names the offending key itself.
            let loc = alloc::format!("{field}.{}.[key]", k.__str__());
            return Err(pydantic_validation_error(
                class,
                &[(&loc, "Input should be a valid string", "string_type", k)],
            ));
        }
    }
    Ok(v)
}

/// `str` field.
fn str_field(class: &str, field: &str, v: Arg) -> Result<String> {
    match v {
        Arg::Str(s) => Ok(s.to_string()),
        other => Err(pydantic_validation_error(
            class,
            &[(
                field,
                "Input should be a valid string",
                "string_type",
                &other,
            )],
        )),
    }
}

impl TransformationFields {
    /// Bind the pydantic constructor keywords (`name=`, `forward_kwargs=`,
    /// `backward_kwargs=`, `backward_code=`); unknown keywords are ignored
    /// (pydantic's default `extra="ignore"`), positional arguments raise
    /// pydantic's `TypeError`.
    pub fn from_args(class: &str, default_name: &str, args: &Args) -> Result<Self> {
        if !args.positional.is_empty() {
            return Err(LailaError::type_error(alloc::format!(
                "BaseModel.__init__() takes 1 positional argument but {} were given",
                args.positional.len() + 1
            )));
        }
        let given = |k: &str| -> Result<Option<Arg>> {
            Ok(if args.__contains__(k)? {
                Some(args.get(k))
            } else {
                None
            })
        };
        let name = match given("name")? {
            Some(v) => str_field(class, "name", v)?,
            None => default_name.to_string(),
        };
        let forward_kwargs = match given("forward_kwargs")? {
            Some(v) => kwargs_field(class, "forward_kwargs", v)?,
            None => PyDictBuilder::new().finish(),
        };
        let backward_kwargs = match given("backward_kwargs")? {
            Some(v) => kwargs_field(class, "backward_kwargs", v)?,
            None => PyDictBuilder::new().finish(),
        };
        let backward_code = match given("backward_code")? {
            Some(v) => str_field(class, "backward_code", v)?,
            None => String::new(),
        };
        let f = TransformationFields {
            name: RwLock::new(name),
            forward_kwargs: RwLock::new(forward_kwargs),
            backward_kwargs: RwLock::new(backward_kwargs),
            backward_code: RwLock::new(String::new()),
            ident: next_ident(),
        };
        // pydantic assigns the field through ``__setattr__`` -> trust hook.
        f.set_backward_code(&backward_code);
        Ok(f)
    }

    /// `self.backward_code = value` -- every non-empty assignment lands in
    /// `_KNOWN_BACKWARD_CODES` (the `__setattr__` hook).
    pub fn set_backward_code(&self, code: &str) {
        *self.backward_code.write() = code.to_string();
        if !code.is_empty() {
            register_known_backward_code(code);
        }
    }
}

// --------------------------------------------------------------------------
// _data_transformation
// --------------------------------------------------------------------------

/// Python `_data_transformation`: a single reversible data transformation.
///
/// Concrete types provide `fields()` (the pydantic fields), `class_name()`
/// (`type(self).__name__`), `forward` and `backward`; everything else has
/// the Python default.
pub trait DataTransformation: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    /// `type(self).__name__`
    fn class_name(&self) -> &'static str;
    fn fields(&self) -> &TransformationFields;
    fn forward(&self, data: &Arg) -> Result<Arg>;
    fn backward(&self, data: &Arg) -> Result<Arg>;

    /// `self.name`
    fn name(&self) -> String {
        self.fields().name.read().clone()
    }
    /// `self.forward_kwargs`
    fn forward_kwargs(&self) -> Arg {
        self.fields().forward_kwargs.read().clone()
    }
    /// `self.backward_kwargs`
    fn backward_kwargs(&self) -> Arg {
        self.fields().backward_kwargs.read().clone()
    }
    /// `self.backward_code`
    fn backward_code(&self) -> String {
        self.fields().backward_code.read().clone()
    }
    /// `self.backward_code = value`
    fn set_backward_code(&self, code: &str) {
        self.fields().set_backward_code(code)
    }
    /// pydantic `__eq__`: same class and equal fields.
    fn __eq__(&self, other: &dyn DataTransformation) -> bool {
        self.class_name() == other.class_name()
            && self.name() == other.name()
            && self.forward_kwargs() == other.forward_kwargs()
            && self.backward_kwargs() == other.backward_kwargs()
            && self.backward_code() == other.backward_code()
    }
    /// pydantic `__repr__`: `Base64(name='base64', forward_kwargs={}, ...)`.
    fn __repr__(&self) -> String {
        alloc::format!(
            "{}(name={}, forward_kwargs={}, backward_kwargs={}, backward_code={})",
            self.class_name(),
            str_repr(&self.name()),
            self.forward_kwargs().__repr__(),
            self.backward_kwargs().__repr__(),
            str_repr(&self.backward_code())
        )
    }
    /// `model_dump()`: the four pydantic fields (secrets such as
    /// `FernetEncryption.key` are `exclude=True` and never appear).
    fn model_dump(&self) -> Arg {
        let mut d = PyDictBuilder::new();
        d.insert("name", self.name());
        d.insert("forward_kwargs", self.forward_kwargs());
        d.insert("backward_kwargs", self.backward_kwargs());
        d.insert("backward_code", self.backward_code());
        d.finish()
    }
    /// Extra attributes of the concrete class (`FernetEncryption.key`).
    fn __getattr_extra__(&self, _name: &str) -> Option<Result<Arg>> {
        None
    }
}

/// A shared handle to a transformation (a Python reference to an instance).
#[derive(Clone)]
pub struct Transformation(pub Arc<dyn DataTransformation>);

/// Python spelling of the base class, for `isinstance(t, _data_transformation)`.
pub use Transformation as _data_transformation;

impl Transformation {
    pub fn new<T: DataTransformation>(t: T) -> Transformation {
        Transformation(Arc::new(t))
    }
    /// The concrete type behind the handle, if it is `T`.
    pub fn downcast_ref<T: DataTransformation>(&self) -> Option<&T> {
        self.0.as_any().downcast_ref::<T>()
    }
    /// `isinstance(a, _data_transformation)` -> the handle.
    pub fn from_arg(a: &Arg) -> Option<Transformation> {
        a.downcast_ref::<Transformation>().cloned()
    }
    /// Python `is`.
    pub fn is(&self, other: &Transformation) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.0.fields().ident() == other.0.fields().ident()
    }
    pub fn name(&self) -> String {
        self.0.name()
    }
    pub fn class_name(&self) -> &'static str {
        self.0.class_name()
    }
    /// `type(t)` / `t.__class__`: the concrete class as a class object
    /// (callable: `t.__class__()` builds a fresh default instance).
    pub fn __class__(&self) -> crate::python::PyClass {
        let module = "laila.entry.compdata.transformation";
        let name = self.class_name();
        match name {
            "Base64" | "Zlib" | "FernetEncryption" | "JsonString" | "PickleSerializer"
            | "MsgpackSerializer" | "NumpySerializer" | "TorchSerializer" => {
                crate::python::PyClass::with_ctor(
                    name,
                    module,
                    Arc::new(move |a: Args| {
                        crate::entry::compdata::transformation
                            .__getattr__(name)?
                            .__call__(a)
                    }),
                )
            }
            other => crate::python::PyClass::named(other, module),
        }
    }
    pub fn forward_kwargs(&self) -> Arg {
        self.0.forward_kwargs()
    }
    pub fn backward_kwargs(&self) -> Arg {
        self.0.backward_kwargs()
    }
    pub fn backward_code(&self) -> String {
        self.0.backward_code()
    }
    pub fn set_backward_code(&self, code: &str) {
        self.0.set_backward_code(code)
    }
    pub fn forward(&self, data: impl IntoArg) -> Result<Arg> {
        self.0.forward(&data.into_arg())
    }
    /// `t.model_dump()`
    pub fn model_dump(&self, _args: impl Into<Args>) -> Arg {
        self.0.model_dump()
    }
    pub fn backward(&self, data: impl IntoArg) -> Result<Arg> {
        self.0.backward(&data.into_arg())
    }
    pub fn __repr__(&self) -> String {
        self.0.__repr__()
    }
}

impl core::fmt::Debug for Transformation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}

impl PartialEq for Transformation {
    fn eq(&self, other: &Transformation) -> bool {
        self.is(other) || self.0.__eq__(&*other.0)
    }
}

impl PyObject for Transformation {
    fn as_any(&self) -> &dyn Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Transformation>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        self.0.class_name()
    }
    fn __repr__(&self) -> String {
        Transformation::__repr__(self)
    }
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(Transformation::__class__(self))
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Transformation>()
            .is_some_and(|o| self == o)
    }
    /// pydantic models define `__eq__` without `__hash__`.
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error(alloc::format!(
            "unhashable type: '{}'",
            self.0.class_name()
        )))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(Arg::Str(self.name().into())),
            "forward_kwargs" => Ok(self.forward_kwargs()),
            "backward_kwargs" => Ok(self.backward_kwargs()),
            "backward_code" => Ok(Arg::Str(self.backward_code().into())),
            other => match self.0.__getattr_extra__(other) {
                Some(r) => r,
                None => Err(LailaError::attribute_error(alloc::format!(
                    "'{}' object has no attribute '{other}'",
                    self.0.class_name()
                ))),
            },
        }
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        let class = self.0.class_name();
        match name {
            "backward_code" => {
                let code = str_field(class, "backward_code", value)?;
                self.set_backward_code(&code);
                Ok(())
            }
            "name" => {
                *self.0.fields().name.write() = str_field(class, "name", value)?;
                Ok(())
            }
            "forward_kwargs" => {
                *self.0.fields().forward_kwargs.write() =
                    kwargs_field(class, "forward_kwargs", value)?;
                Ok(())
            }
            "backward_kwargs" => {
                *self.0.fields().backward_kwargs.write() =
                    kwargs_field(class, "backward_kwargs", value)?;
                Ok(())
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'{class}' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let one = |what: &str| -> Result<Arg> {
            match args.positional.first() {
                Some(v) if args.positional.len() == 1 && args.keywords.is_empty() => Ok(v.clone()),
                _ => Err(LailaError::type_error(alloc::format!(
                    "{}.{what}() takes 2 positional arguments but {} were given",
                    self.0.class_name(),
                    args.__len__() + 1
                ))),
            }
        };
        match name {
            "forward" => self.0.forward(&one("forward")?),
            "backward" => self.0.backward(&one("backward")?),
            "model_dump" => Ok(self.0.model_dump()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{other}'",
                self.0.class_name()
            ))),
        }
    }
}

impl IntoArg for Transformation {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Transformation {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for Transformation {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<Transformation>())
    }
}

/// Implements `IntoArg` / `PyArg` / `From<T> for Transformation` /
/// `IsInstance` for a concrete transformation type, plus the Python
/// attribute surface as *inherent* methods (`t.name`, `t.backward_code`,
/// `t.forward(x)`, ..) so typed code reads like the Python without
/// importing the [`DataTransformation`] trait.
macro_rules! transformation_object {
    ($t:ty) => {
        impl $t {
            /// `self.name`
            pub fn name(&self) -> alloc::string::String {
                <$t as $crate::entry::compdata::transformation::base::DataTransformation>::name(self)
            }
            /// `self.forward_kwargs`
            pub fn forward_kwargs(&self) -> $crate::python::Arg {
                <$t as $crate::entry::compdata::transformation::base::DataTransformation>::forward_kwargs(self)
            }
            /// `self.backward_kwargs`
            pub fn backward_kwargs(&self) -> $crate::python::Arg {
                <$t as $crate::entry::compdata::transformation::base::DataTransformation>::backward_kwargs(self)
            }
            /// `self.backward_code`
            pub fn backward_code(&self) -> alloc::string::String {
                <$t as $crate::entry::compdata::transformation::base::DataTransformation>::backward_code(self)
            }
            /// `self.backward_code = value`
            pub fn set_backward_code(&self, code: &str) {
                <$t as $crate::entry::compdata::transformation::base::DataTransformation>::set_backward_code(self, code)
            }
            /// `self.forward(data)`
            pub fn forward(&self, data: impl $crate::python::IntoArg) -> $crate::python::Result<$crate::python::Arg> {
                <$t as $crate::entry::compdata::transformation::base::DataTransformation>::forward(self, &data.into_arg())
            }
            /// `self.backward(data)`
            pub fn backward(&self, data: impl $crate::python::IntoArg) -> $crate::python::Result<$crate::python::Arg> {
                <$t as $crate::entry::compdata::transformation::base::DataTransformation>::backward(self, &data.into_arg())
            }
            /// `self.model_dump()`
            pub fn model_dump(&self, _args: impl Into<$crate::python::Args>) -> $crate::python::Arg {
                <$t as $crate::entry::compdata::transformation::base::DataTransformation>::model_dump(self)
            }
        }
        impl From<$t> for $crate::entry::compdata::transformation::base::Transformation {
            fn from(t: $t) -> Self {
                $crate::entry::compdata::transformation::base::Transformation::new(t)
            }
        }
        impl $crate::python::IntoArg for $t {
            fn into_arg(self) -> $crate::python::Arg {
                $crate::entry::compdata::transformation::base::Transformation::new(self).into_arg()
            }
        }
        /// A typed transformation read as a value after being handed to a
        /// container (`seq.append(b64)` then `[b64]`): a field-equal copy
        /// (pydantic `__eq__` compares the fields).
        impl $crate::python::PyArg for $t {
            fn to_arg(&self) -> $crate::python::Arg {
                let c = self.clone();
                <$t as $crate::entry::compdata::transformation::base::DataTransformation>::fields(&c)
                    .share_ident(<$t as $crate::entry::compdata::transformation::base::DataTransformation>::fields(self));
                $crate::python::IntoArg::into_arg(c)
            }
        }
        impl core::fmt::Debug for $t {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.write_str(&<$t as $crate::entry::compdata::transformation::base::DataTransformation>::__repr__(self))
            }
        }
        impl $crate::python::builtins::IsInstance for $t {
            fn isinstance(x: &$crate::python::Arg) -> bool {
                $crate::entry::compdata::transformation::base::Transformation::from_arg(x)
                    .is_some_and(|t| t.downcast_ref::<$t>().is_some())
            }
        }
    };
}
pub(crate) use transformation_object;

// --------------------------------------------------------------------------
// Recovery-code trust
// --------------------------------------------------------------------------
// A ``SimpleConstitution`` read back from a pool or a peer is a list
// of Python source snippets that get ``exec``'d. Only snippets this
// process could itself have produced are allowed to run:
//
// - ``_matches_backward_code`` -- template match against the class's
//   own recovery code (fixed text identical, the ``backward_kwargs``
//   slot a literal dict). Records written with other kwargs by other
//   processes / hosts / ports still match.
// - ``_KNOWN_BACKWARD_CODES`` -- exact strings generated in this
//   process (every assignment to ``backward_code`` lands here), the
//   fallback for third-party transformations whose template cannot
//   be probed.

/// What the registry knows about a transformation class: how to render
/// its recovery-code template (`_template_probe().backward_code`) and
/// which slots the template has (`_template_slots()`).
#[derive(Clone)]
pub struct TemplateSpec {
    /// `type(self).__name__`
    pub class_name: &'static str,
    /// `cls._template_probe().backward_code` (an `Err` means "no template").
    pub probe: fn() -> Result<String>,
    /// `cls._template_slots()`
    pub slots: fn() -> Vec<Slot>,
}

/// Compiled template: literal segments between the slots plus one
/// validator per slot (Python keeps a compiled `re` pattern).
pub struct CompiledTemplate {
    segments: Vec<String>,
    validators: Vec<fn(&str) -> bool>,
}

static _KNOWN_BACKWARD_CODES: RwLock<Vec<String>> = RwLock::new(Vec::new());
static _PATTERN_CACHE: RwLock<Vec<(String, Option<Arc<CompiledTemplate>>)>> =
    RwLock::new(Vec::new());
static REGISTRY_STATE: RwLock<Vec<(String, TemplateSpec)>> = RwLock::new(Vec::new());
static SEEDED: Mutex<bool> = Mutex::new(false);

/// `_data_transformation._KNOWN_BACKWARD_CODES.add(code)`
pub fn register_known_backward_code(code: &str) {
    let mut g = _KNOWN_BACKWARD_CODES.write();
    if !g.iter().any(|c| c == code) {
        g.push(code.to_string());
    }
}

/// `__init_subclass__`: register a transformation class under its `name`.
pub fn register_transformation(name: &str, spec: TemplateSpec) {
    let mut g = REGISTRY_STATE.write();
    if let Some(slot) = g.iter_mut().find(|(n, _)| n == name) {
        slot.1 = spec;
    } else {
        g.push((name.to_string(), spec));
    }
    _PATTERN_CACHE.write().retain(|(n, _)| n != name);
}

/// The built-in transformations are registered on first use (Python
/// registers them when their modules are imported).
fn ensure_seeded() {
    // The lock is held while seeding so a concurrent first caller sees the
    // complete registry.
    let mut g = SEEDED.lock();
    if !*g {
        *g = true;
        super::seed_registry();
    }
}

/// `_data_transformation.REGISTRY` -- the registered names, in
/// registration order.
pub fn REGISTRY() -> Vec<String> {
    ensure_seeded();
    REGISTRY_STATE
        .read()
        .iter()
        .map(|(n, _)| n.clone())
        .collect()
}

/// `REGISTRY[name]`
pub fn registry_get(name: &str) -> Option<TemplateSpec> {
    ensure_seeded();
    REGISTRY_STATE
        .read()
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, s)| s.clone())
}

/// Compiled template for the named class's recovery code (cached), or `None`.
pub fn _backward_code_pattern(name: &str) -> Option<Arc<CompiledTemplate>> {
    ensure_seeded();
    if let Some((_, cached)) = _PATTERN_CACHE.read().iter().find(|(n, _)| n == name) {
        return cached.clone();
    }
    let spec = registry_get(name)?;
    let code = (spec.probe)().unwrap_or_default();
    let mut result = None;
    if !code.is_empty() {
        let slots = (spec.slots)();
        // ``re.split(f"({token_re})", code)``: literal parts alternate with
        // slot tokens (leftmost, longest-alternative-first like ``re``).
        let mut segments: Vec<String> = Vec::new();
        let mut validators: Vec<fn(&str) -> bool> = Vec::new();
        let mut cur = String::new();
        let mut rest = code.as_str();
        while !rest.is_empty() {
            let mut hit: Option<&Slot> = None;
            for s in &slots {
                if !s.text.is_empty() && rest.starts_with(s.text.as_str()) {
                    hit = Some(s);
                    break;
                }
            }
            match hit {
                Some(s) => {
                    segments.push(core::mem::take(&mut cur));
                    validators.push(s.validate);
                    rest = &rest[s.text.len()..];
                }
                None => {
                    let ch = rest.chars().next().unwrap();
                    cur.push(ch);
                    rest = &rest[ch.len_utf8()..];
                }
            }
        }
        segments.push(cur);
        // A slot-less template (e.g. JsonString) is an exact match.
        result = Some(Arc::new(CompiledTemplate {
            segments,
            validators,
        }));
    }
    let mut cache = _PATTERN_CACHE.write();
    cache.retain(|(n, _)| n != name);
    cache.push((name.to_string(), result.clone()));
    result
}

/// `True` when *code* is an instance of the named class's recovery template.
pub fn _matches_backward_code(name: &str, code: &str) -> bool {
    let Some(compiled) = _backward_code_pattern(name) else {
        return false;
    };
    let Some(caps) = recovery_codes::match_segments(&compiled.segments, code) else {
        return false;
    };
    compiled.validators.iter().zip(caps).all(|(v, g)| v(g))
}

/// `_data_transformation.is_trusted_backward_code(code)`: `True` when
/// *code* may be executed as a recovery step -- generated in this process,
/// or matching the recovery-code template of a loaded transformation class.
pub fn is_trusted_backward_code(code: &Arg) -> bool {
    let Arg::Str(code) = code else {
        return false;
    };
    if code.is_empty() {
        return false;
    }
    if _KNOWN_BACKWARD_CODES.read().iter().any(|c| **c == **code) {
        return true;
    }
    REGISTRY()
        .iter()
        .any(|name| _matches_backward_code(name, code))
}

// --------------------------------------------------------------------------
// TransformationSequence
// --------------------------------------------------------------------------

/// Sequential transformation pipeline.
///
/// Holds an ordered list of transformations.
///
/// - [`forward`](TransformationSequence::forward) -- applies the
///   transformations left-to-right and returns
///   `(transformed_data, inverse_codes_in_reverse)`. The inverse-code list
///   is what the constitution will replay at recovery time, so it is built
///   in reverse order on the spot.
/// - [`append`](TransformationSequence::append) -- mutate the pipeline
///   (append one or many), returning `self` for chaining.
/// - An empty pipeline is the identity: `forward(x)` returns `(x, [])` and
///   the constitution simply returns `x` on the way back.
///
/// Pre-built sequences are exported from `laila::entry::compdata::transformation`
/// (`transformation_base64`, `transformation_base64_compression`,
/// `transformation_base64_compression_encryption`, `transformation_encryption`)
/// for the common pool defaults.
#[derive(Clone)]
pub struct TransformationSequence(Arc<RwLock<Vec<Transformation>>>);

/// `list[_data_transformation]` item validation.
fn as_transformation(class: &str, index: usize, item: &Arg) -> Result<Transformation> {
    Transformation::from_arg(item).ok_or_else(|| {
        let loc = alloc::format!("transformations.{index}");
        pydantic_validation_error(
            class,
            &[(
                &loc,
                "Input should be a valid dictionary or instance of _data_transformation",
                "model_type",
                item,
            )],
        )
    })
}

impl TransformationSequence {
    /// `TransformationSequence(transformations=None)` -- pydantic: an
    /// omitted field defaults to `[]`; an explicit value must be a list of
    /// transformations.
    pub fn new(args: impl Into<Args>) -> Result<TransformationSequence> {
        let a: Args = args.into();
        if !a.positional.is_empty() {
            return Err(LailaError::type_error(alloc::format!(
                "BaseModel.__init__() takes 1 positional argument but {} were given",
                a.positional.len() + 1
            )));
        }
        let mut items: Vec<Transformation> = Vec::new();
        if a.__contains__("transformations")? {
            let transformations = a.get("transformations");
            let seq = match &transformations {
                Arg::List(_) | Arg::Tuple(_) => transformations.__iter__()?,
                other => {
                    return Err(pydantic_validation_error(
                        "TransformationSequence",
                        &[(
                            "transformations",
                            "Input should be a valid list",
                            "list_type",
                            other,
                        )],
                    ))
                }
            };
            for (i, item) in seq.iter().enumerate() {
                items.push(as_transformation("TransformationSequence", i, item)?);
            }
        }
        Ok(Self::from_vec(items))
    }

    /// Typed constructor.
    pub fn from_vec(items: Vec<Transformation>) -> TransformationSequence {
        TransformationSequence(Arc::new(RwLock::new(items)))
    }

    /// `self.transformations` (a snapshot of the list).
    pub fn transformations(&self) -> Vec<Transformation> {
        self.0.read().clone()
    }

    /// Python `is`.
    pub fn is(&self, other: &TransformationSequence) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Apply every contained transformation in order and collect their inverses.
    ///
    /// For each transformation `t` in `transformations` (in declaration
    /// order): replace `current` with `t.forward(current)` and append
    /// `t.backward_code` to the inverse-code list. At the end the list is
    /// reversed so that recovery at the constitution layer can simply replay
    /// the codes top-to-bottom.
    ///
    /// Returns `(transformed_value, inverse_code_snippets)` where the second
    /// element is already reversed for replay.
    pub fn forward(&self, data: impl IntoArg) -> Result<(Arg, Vec<String>)> {
        let mut current = data.into_arg();
        let mut inverse_codes: Vec<String> = Vec::new();

        for transformation in self.transformations() {
            current = transformation.0.forward(&current)?;
            inverse_codes.push(transformation.backward_code());
        }

        inverse_codes.reverse();
        Ok((current, inverse_codes))
    }

    /// Iterate over the contained transformations.
    pub fn __iter__(&self) -> Vec<Transformation> {
        self.transformations()
    }

    /// Append one or more transformations to the pipeline; returns `self`
    /// for chaining.
    ///
    /// `TypeError` if *t* is not a valid transformation or list thereof.
    pub fn append(&self, t: impl IntoArg) -> Result<TransformationSequence> {
        let t = t.into_arg();
        if let Some(tr) = Transformation::from_arg(&t) {
            self.0.write().push(tr);
            return Ok(self.clone());
        }

        if let Arg::List(_) = &t {
            for _t in t.__iter__()? {
                let Some(tr) = Transformation::from_arg(&_t) else {
                    return Err(LailaError::type_error(
                        "All elements must be _data_transformation instances.",
                    ));
                };
                self.0.write().push(tr);
            }
            return Ok(self.clone());
        }

        Err(LailaError::type_error(
            "append expects a _data_transformation or list of them.",
        ))
    }

    /// Human-readable pipeline summary.
    pub fn __repr__(&self) -> String {
        let items = self.transformations();
        if items.is_empty() {
            return "TransformationSequence(identity)".into();
        }
        let names: Vec<String> = items.iter().map(Transformation::name).collect();
        alloc::format!("TransformationSequence({})", names.join(" -> "))
    }
}

impl core::fmt::Debug for TransformationSequence {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}

impl PartialEq for TransformationSequence {
    /// pydantic equality: same transformations.
    fn eq(&self, other: &TransformationSequence) -> bool {
        self.is(other) || *self.0.read() == *other.0.read()
    }
}

impl PyObject for TransformationSequence {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "TransformationSequence"
    }
    fn __repr__(&self) -> String {
        TransformationSequence::__repr__(self)
    }
    /// The sequence is a shared handle: two `Arg`s over the same list are
    /// the same object.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<TransformationSequence>()
            .is_some_and(|o| self.is(o))
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(self
            .transformations()
            .into_iter()
            .map(IntoArg::into_arg)
            .collect())
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<TransformationSequence>()
            .is_some_and(|o| self == o)
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error(
            "unhashable type: 'TransformationSequence'",
        ))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "transformations" => Ok(self.transformations().into_arg()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'TransformationSequence' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let one = || -> Result<Arg> {
            match args.positional.first() {
                Some(v) if args.positional.len() == 1 && args.keywords.is_empty() => {
                    Ok(v.clone())
                }
                _ => Err(LailaError::type_error(alloc::format!(
                    "TransformationSequence.{name}() takes 2 positional arguments but {} were given",
                    args.__len__() + 1
                ))),
            }
        };
        match name {
            "forward" => {
                let (value, codes) = self.forward(one()?)?;
                Ok((value, codes).into_arg())
            }
            "append" => Ok(self.append(one()?)?.into_arg()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'TransformationSequence' object has no attribute '{other}'"
            ))),
        }
    }
}

impl IntoArg for TransformationSequence {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for TransformationSequence {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for TransformationSequence {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<TransformationSequence>())
    }
}

/// `kwargs` dict -> `(str key, value)` pairs, for forwarding `**kwargs`
/// to a codec call.
pub(crate) fn kw_pairs(kwargs: &Arg) -> Result<Vec<(String, Arg)>> {
    let mut out = Vec::new();
    if matches!(kwargs, Arg::None) {
        return Ok(out);
    }
    for k in kwargs.__iter__()? {
        let v = kwargs.__getitem__(k.clone())?;
        out.push((k.__str__(), v));
    }
    Ok(out)
}

/// `f(**kwargs)` with a keyword the callee does not accept.
pub(crate) fn unexpected_keyword(func: &str, name: &str) -> LailaError {
    LailaError::type_error(alloc::format!(
        "{func}() got an unexpected keyword argument '{name}'"
    ))
}

/// `for t in seq`: the contained transformations, in order.
impl crate::python::builtins::PyIter for &TransformationSequence {
    type Item = Transformation;
    fn py_iter(self) -> Result<crate::python::builtins::PyIterator<Transformation>> {
        Ok(crate::python::builtins::PyIterator::eager(
            self.transformations(),
        ))
    }
}
