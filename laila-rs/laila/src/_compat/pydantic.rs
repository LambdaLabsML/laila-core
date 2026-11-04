//! The slice of `pydantic.BaseModel` that laila's base classes rely on.
//!
//! laila's object hierarchy (`_LAILA_OBJECT` -> `_LAILA_IDENTIFIABLE_OBJECT`
//! -> `_LAILA_CLI_CAPABLE_CLASS` -> policies, pools, taskforces, ...) is a
//! pydantic model tree: public annotated class attributes are *fields*
//! (`host: str = Field(default="0.0.0.0")`), underscore attributes declared
//! with `PrivateAttr` are per-instance private state, `model_fields` lists
//! the field descriptors, before/after validators and `model_post_init`
//! hooks run around construction. This module carries exactly that:
//!
//! * [`FieldInfo`] / [`Field`] (and the `CLIExempt` marker through
//!   `json_schema_extra`), [`PrivateAttrInfo`] / [`PrivateAttr`];
//! * [`ModelClass`] -- the runtime class descriptor (`py_model!` builds one
//!   per Python class; the laila base classes expose theirs);
//! * [`ModelInstance`] -- `__dict__` + `__pydantic_private__` with the
//!   construction pipeline of `BaseModel.__init__` (before validators, field
//!   defaults and lax type coercion, private defaults, `model_post_init`,
//!   after validators) and Python's attribute protocol on top;
//! * [`ModelHooks`] -- the behaviour a laila base class contributes to every
//!   subclass (identity staging, `laila.args` resolution, timestamps).
//!
//! Only the lax-mode coercions laila's config values need are ported (`int`,
//! `float`, `str`, `bool`, `dict`, `list`, `set`, `Optional[..]`); anything
//! else is stored as given. Validation errors carry pydantic's first lines
//! (`1 validation error for X` / the field name / the message and type).

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::args::Args;
use crate::python::builtins::IsInstance;
use crate::python::object::{Arg, Callable, IntoArg, PyArg, PyMap, PyObject};
use crate::python::pyclass::PyClass;
use crate::python::sync::RwLock;
use crate::python::{pysig, ExcKind, LailaError, Result};

/// `**data` as pydantic sees it: an ordered `name -> value` map.
pub type Kwargs = PyMap<String, Arg>;

// ---------------------------------------------------------------------------
// Field / PrivateAttr

/// `pydantic.fields.FieldInfo` (the subset laila reads).
#[derive(Clone)]
pub struct FieldInfo {
    /// `default=`; `None` when undefined (the field is then required unless
    /// a `default_factory` is given).
    pub default: Option<Arg>,
    /// `default_factory=`
    pub default_factory: Option<Callable>,
    /// `json_schema_extra=` -- a dict (`CLIExempt` stores `cli_exempt=True` here) or `None`.
    pub json_schema_extra: Arg,
    /// The annotation text (`"str"`, `"int | None"`, ...); empty when unknown.
    pub annotation: String,
    /// `alias`, `title`, `description`, `frozen`, ... as given.
    pub extra: Vec<(String, Arg)>,
}

impl FieldInfo {
    /// `Field()` with nothing set.
    pub fn undefined() -> FieldInfo {
        FieldInfo {
            default: None,
            default_factory: None,
            json_schema_extra: Arg::None,
            annotation: String::new(),
            extra: Vec::new(),
        }
    }
    /// `field_info.is_required()`
    pub fn is_required(&self) -> bool {
        self.default.is_none() && self.default_factory.is_none()
    }
    /// `field_info.get_default(call_default_factory=True)`
    pub fn get_default(&self) -> Result<Arg> {
        if let Some(f) = &self.default_factory {
            return f(Args::new());
        }
        Ok(self.default.clone().unwrap_or(Arg::None))
    }
    fn with_annotation(mut self, annotation: &str) -> FieldInfo {
        self.annotation = annotation.to_string();
        self
    }
}

impl core::fmt::Debug for FieldInfo {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&PyObject::__repr__(self))
    }
}

impl PyObject for FieldInfo {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "FieldInfo"
    }
    fn __repr__(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if !self.annotation.is_empty() {
            parts.push(alloc::format!("annotation={}", self.annotation));
        }
        parts.push(alloc::format!(
            "required={}",
            if self.is_required() { "True" } else { "False" }
        ));
        if let Some(d) = &self.default {
            parts.push(alloc::format!("default={}", d.__repr__()));
        }
        if self.default_factory.is_some() {
            parts.push("default_factory=<function>".to_string());
        }
        if !self.json_schema_extra.is_none() {
            parts.push(alloc::format!(
                "json_schema_extra={}",
                self.json_schema_extra.__repr__()
            ));
        }
        for (k, v) in &self.extra {
            parts.push(alloc::format!("{k}={}", v.__repr__()));
        }
        alloc::format!("FieldInfo({})", parts.join(", "))
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<FieldInfo>()
            .is_some_and(|o| core::ptr::eq(o, self))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const FieldInfo as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "json_schema_extra" => Ok(self.json_schema_extra.clone()),
            "default" => Ok(self.default.clone().unwrap_or(Arg::None)),
            "default_factory" => Ok(match &self.default_factory {
                Some(f) => Arg::Callable(f.clone()),
                None => Arg::None,
            }),
            "annotation" => Ok(Arg::Str(self.annotation.as_str().into())),
            other => {
                if let Some((_, v)) = self.extra.iter().find(|(k, _)| k == other) {
                    return Ok(v.clone());
                }
                Err(LailaError::attribute_error(alloc::format!(
                    "'FieldInfo' object has no attribute '{other}'"
                )))
            }
        }
    }
    fn __call_method__(&self, name: &str, _args: Args) -> Result<Arg> {
        match name {
            "is_required" => Ok(Arg::Bool(self.is_required())),
            "get_default" => self.get_default(),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'FieldInfo' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for FieldInfo {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for FieldInfo {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for FieldInfo {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<FieldInfo>())
    }
}

fn callable_of(v: &Arg, what: &str) -> Result<Option<Callable>> {
    match v {
        Arg::None => Ok(None),
        Arg::Callable(c) => Ok(Some(c.clone())),
        Arg::Object(o) if o.as_any().is::<PyClass>() => {
            let cls = v.clone();
            Ok(Some(Arc::new(move |a: Args| cls.__call__(a))))
        }
        other => Err(LailaError::type_error(alloc::format!(
            "{what} must be callable, got '{}'",
            other.type_name()
        ))),
    }
}

/// `pydantic.Field(default=PydanticUndefined(), *, default_factory=None, alias=None, title=None, description=None, json_schema_extra=None, frozen=None, examples=None, **extra)`
#[pysig("Field(default=PydanticUndefined(), *, default_factory=None, alias=None, title=None, description=None, json_schema_extra=None, frozen=None, examples=None, **extra)")]
#[allow(non_snake_case)]
pub fn Field(args: impl Into<Args>) -> Result<FieldInfo> {
    let default_factory = callable_of(&default_factory, "default_factory")?;
    let default = if is_undefined(&default) {
        None
    } else {
        Some(default)
    };
    if default.is_some() && default_factory.is_some() {
        return Err(LailaError::type_error(
            "cannot specify both default and default_factory",
        ));
    }
    let mut extra_pairs: Vec<(String, Arg)> = Vec::new();
    for (k, v) in [
        ("alias", alias),
        ("title", title),
        ("description", description),
        ("frozen", frozen),
        ("examples", examples),
    ] {
        if !v.is_none() {
            extra_pairs.push((k.to_string(), v));
        }
    }
    extra_pairs.extend(extra.keywords);
    Ok(FieldInfo {
        default,
        default_factory,
        json_schema_extra,
        annotation: String::new(),
        extra: extra_pairs,
    })
}

/// `pydantic.Field` spelled as a class (`Field(..)` in a `laila::python!`
/// body lowers to `Field::new(..)`).
#[allow(non_camel_case_types)]
pub struct Field {}
impl Field {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<FieldInfo> {
        self::Field(args)
    }
}

/// `PydanticUndefined`: the sentinel `Field()` uses for "no default".
#[allow(non_snake_case)]
pub fn PydanticUndefined() -> Arg {
    Arg::Str(UNDEFINED.into())
}
/// `PydanticUndefined` spelled as a class (`PydanticUndefined()` in a
/// `laila::python!` body lowers to `PydanticUndefined::new(..)`).
#[allow(non_camel_case_types)]
pub struct PydanticUndefined {}
impl PydanticUndefined {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(_args: impl Into<Args>) -> Arg {
        self::PydanticUndefined()
    }
}
const UNDEFINED: &str = "\u{0}PydanticUndefined\u{0}";
fn is_undefined(a: &Arg) -> bool {
    matches!(a, Arg::Str(s) if &**s == UNDEFINED)
}

/// `pydantic.fields.ModelPrivateAttr`.
#[derive(Clone)]
pub struct PrivateAttrInfo {
    pub default: Option<Arg>,
    pub default_factory: Option<Callable>,
}

impl PrivateAttrInfo {
    /// `attr.get_default()` (`None` when the attribute has no default at all).
    pub fn get_default(&self) -> Result<Option<Arg>> {
        if let Some(f) = &self.default_factory {
            return Ok(Some(f(Args::new())?));
        }
        Ok(self.default.clone())
    }
}

impl core::fmt::Debug for PrivateAttrInfo {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&PyObject::__repr__(self))
    }
}

impl PyObject for PrivateAttrInfo {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "ModelPrivateAttr"
    }
    fn __repr__(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(d) = &self.default {
            parts.push(alloc::format!("default={}", d.__repr__()));
        }
        if self.default_factory.is_some() {
            parts.push("default_factory=<function>".to_string());
        }
        alloc::format!("ModelPrivateAttr({})", parts.join(", "))
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PrivateAttrInfo>()
            .is_some_and(|o| core::ptr::eq(o, self))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const PrivateAttrInfo as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "default" => Ok(self.default.clone().unwrap_or(Arg::None)),
            "default_factory" => Ok(match &self.default_factory {
                Some(f) => Arg::Callable(f.clone()),
                None => Arg::None,
            }),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'ModelPrivateAttr' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, _args: Args) -> Result<Arg> {
        match name {
            "get_default" => Ok(self.get_default()?.unwrap_or(Arg::None)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'ModelPrivateAttr' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for PrivateAttrInfo {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for PrivateAttrInfo {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

/// `pydantic.PrivateAttr(default=PydanticUndefined(), *, default_factory=None)`
#[pysig("PrivateAttr(default=PydanticUndefined(), *, default_factory=None)")]
#[allow(non_snake_case)]
pub fn PrivateAttr(args: impl Into<Args>) -> Result<PrivateAttrInfo> {
    let default_factory = callable_of(&default_factory, "default_factory")?;
    let default = if is_undefined(&default) {
        None
    } else {
        Some(default)
    };
    if default.is_some() && default_factory.is_some() {
        return Err(LailaError::type_error(
            "cannot specify both default and default_factory",
        ));
    }
    Ok(PrivateAttrInfo {
        default,
        default_factory,
    })
}

/// `pydantic.PrivateAttr` spelled as a class (`PrivateAttr(..)` lowers to `PrivateAttr::new(..)`).
#[allow(non_camel_case_types)]
pub struct PrivateAttr {}
impl PrivateAttr {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<PrivateAttrInfo> {
        self::PrivateAttr(args)
    }
}

// ---------------------------------------------------------------------------
// Class descriptors

/// Per-construction scratch space shared by the base-class hooks (the twin
/// of the thread-local `_INIT_PENDING` plus the locals of `__init__`).
#[derive(Default)]
pub struct InitState {
    pub scratch: Kwargs,
}

/// The behaviour a laila base class contributes to every model that
/// inherits from it. Hooks run in Python's MRO order (see
/// [`ModelInstance::construct`]).
pub trait ModelHooks: Send + Sync {
    /// Head of `__init__` (derived classes first): may pop keyword arguments.
    fn init_head(
        &self,
        _cls: &'static ModelClass,
        _data: &mut Kwargs,
        _st: &mut InitState,
    ) -> Result<()> {
        Ok(())
    }
    /// `@model_validator(mode="before")`.
    fn before_validate(&self, _cls: &'static ModelClass, _data: &mut Kwargs) -> Result<()> {
        Ok(())
    }
    /// `model_post_init` (base-most class first, as the `super()` chains run).
    fn model_post_init(&self, _inst: &ModelInstance, _st: &InitState) -> Result<()> {
        Ok(())
    }
    /// `@model_validator(mode="after")`.
    fn after_validate(&self, _inst: &ModelInstance) -> Result<()> {
        Ok(())
    }
    /// Tail of `__init__` after `super().__init__` returned (base-most first).
    fn init_tail(&self, _inst: &ModelInstance, _st: &mut InitState) -> Result<()> {
        Ok(())
    }
    /// A property read; `None` when the base does not define `name`.
    fn getattr(&self, _inst: &ModelInstance, _name: &str) -> Option<Result<Arg>> {
        None
    }
    /// A property write; `None` when the base does not define `name`.
    fn setattr(&self, _inst: &ModelInstance, _name: &str, _value: Arg) -> Option<Result<()>> {
        None
    }
    /// A method call; `None` when the base does not define `name`.
    fn call_method(&self, _inst: &ModelInstance, _name: &str, _args: Args) -> Option<Result<Arg>> {
        None
    }
    /// A `@classmethod` call on the class object (`cls.matches_token(..)`);
    /// `None` when the base does not define `name`.
    fn call_class_method(
        &self,
        _cls: &'static ModelClass,
        _name: &str,
        _args: Args,
    ) -> Option<Result<Arg>> {
        None
    }
    /// `@classmethod` names (for `getattr(cls, name)`).
    fn class_method_names(&self) -> &'static [&'static str] {
        &[]
    }
    /// Public `property` descriptors with a setter.
    fn property_setters(&self) -> &'static [&'static str] {
        &[]
    }
    /// Read-only public properties (for `dir()`).
    fn property_getters(&self) -> &'static [&'static str] {
        &[]
    }
    /// Method names (for `dir()`).
    fn method_names(&self) -> &'static [&'static str] {
        &[]
    }
    /// `str(inst)` when the base overrides it.
    fn str(&self, _inst: &ModelInstance) -> Option<String> {
        None
    }
    /// `repr(inst)` when the base overrides it.
    fn repr(&self, _inst: &ModelInstance) -> Option<String> {
        None
    }
    /// `inst == other` when the base overrides it.
    fn eq(&self, _inst: &ModelInstance, _other: &Arg) -> Option<bool> {
        None
    }
    /// `hash(inst)` when the base overrides it.
    fn hash(&self, _inst: &ModelInstance) -> Option<Result<i64>> {
        None
    }
}

/// A pydantic-style class at runtime: name, bases, fields, private
/// attributes and class variables. `py_model!` builds one per Python class;
/// laila's base classes expose theirs through `__class__()`.
pub struct ModelClass {
    pub name: &'static str,
    pub module: &'static str,
    pub bases: Vec<&'static ModelClass>,
    /// Own public fields, in declaration order.
    pub fields: Vec<(String, FieldInfo)>,
    /// Own `PrivateAttr` declarations, in declaration order.
    pub private_attrs: Vec<(String, PrivateAttrInfo)>,
    /// Own class-level variables (`_cli_required_fields = {...}`, `_DEFAULT_SCOPES = [...]`).
    pub class_vars: Vec<(String, Arg)>,
    /// The behaviour this class contributes to subclasses (laila base classes).
    pub hooks: Option<&'static dyn ModelHooks>,
}

impl ModelClass {
    /// Python's MRO: each class precedes its bases, left-to-right
    /// (sufficient for laila's mixin chains; no C3 conflicts occur).
    pub fn mro(&'static self) -> Vec<&'static ModelClass> {
        fn walk(c: &'static ModelClass, out: &mut Vec<&'static ModelClass>) {
            out.retain(|x| !core::ptr::eq(*x, c));
            out.push(c);
            for b in &c.bases {
                walk(b, out);
            }
        }
        let mut out = Vec::new();
        walk(self, &mut out);
        out
    }

    /// `issubclass(cls, <name>)` by class name.
    pub fn is_subclass(&'static self, name: &str) -> bool {
        self.mro().iter().any(|c| c.name == name)
    }

    /// Every hook along the MRO, derived class first.
    pub fn hooks(&'static self) -> Vec<&'static dyn ModelHooks> {
        self.mro().into_iter().filter_map(|c| c.hooks).collect()
    }

    /// `cls.model_fields`: inherited fields first (pydantic's order), then own.
    pub fn all_fields(&'static self) -> Vec<(String, FieldInfo)> {
        let mut out: Vec<(String, FieldInfo)> = Vec::new();
        for c in self.mro().into_iter().rev() {
            for (n, f) in &c.fields {
                if let Some(slot) = out.iter_mut().find(|(k, _)| k == n) {
                    slot.1 = f.clone();
                } else {
                    out.push((n.clone(), f.clone()));
                }
            }
        }
        out
    }

    /// `cls.__private_attributes__`.
    pub fn all_private_attrs(&'static self) -> Vec<(String, PrivateAttrInfo)> {
        let mut out: Vec<(String, PrivateAttrInfo)> = Vec::new();
        for c in self.mro().into_iter().rev() {
            for (n, p) in &c.private_attrs {
                if let Some(slot) = out.iter_mut().find(|(k, _)| k == n) {
                    slot.1 = p.clone();
                } else {
                    out.push((n.clone(), p.clone()));
                }
            }
        }
        out
    }

    /// `cls.__private_attributes__.get(name)`.
    pub fn private_attr(&'static self, name: &str) -> Option<PrivateAttrInfo> {
        for c in self.mro() {
            if let Some((_, p)) = c.private_attrs.iter().find(|(k, _)| k == name) {
                return Some(p.clone());
            }
        }
        None
    }

    /// `getattr(cls, name)` for a class-level variable (searched along the MRO).
    pub fn class_var(&'static self, name: &str) -> Option<Arg> {
        for c in self.mro() {
            if let Some((_, v)) = c.class_vars.iter().find(|(k, _)| k == name) {
                return Some(v.clone());
            }
        }
        None
    }

    /// `cls.model_fields` as a Python dict `name -> FieldInfo`.
    pub fn model_fields(&'static self) -> Arg {
        let mut d = crate::python::PyDictBuilder::new();
        for (n, f) in self.all_fields() {
            d.insert(n, f);
        }
        d.finish()
    }

    /// `cls.__private_attributes__` as a Python dict `name -> ModelPrivateAttr`.
    pub fn __private_attributes__(&'static self) -> Arg {
        let mut d = crate::python::PyDictBuilder::new();
        for (n, p) in self.all_private_attrs() {
            d.insert(n, p);
        }
        d.finish()
    }

    /// Public `property` setters contributed by the bases (`dir()` order).
    pub fn property_setters(&'static self) -> Vec<&'static str> {
        let mut out: Vec<&'static str> = Vec::new();
        for h in self.hooks() {
            for p in h.property_setters() {
                if !out.contains(p) {
                    out.push(p);
                }
            }
        }
        out.sort_unstable();
        out
    }

    /// The class as a Python value (`type(obj)`, `DummyPool` as an argument).
    pub fn as_pyclass(&'static self) -> PyClass {
        let me: &'static ModelClass = self;
        PyClass {
            name: self.name,
            module: self.module,
            ctor: Some(Arc::new(move |a: Args| ModelInstance::construct(me, a))),
            model: Some(self),
            hooks: None,
        }
    }

    /// `cls(**data)`.
    pub fn construct(&'static self, args: Args) -> Result<Arg> {
        ModelInstance::construct(self, args)
    }

    /// `cls.<name>(*args)` for a `@classmethod` contributed by a base.
    pub fn call_class_method(&'static self, name: &str, args: Args) -> Option<Result<Arg>> {
        for h in self.hooks() {
            if let Some(r) = h.call_class_method(self, name, args.clone()) {
                return Some(r);
            }
        }
        None
    }

    /// `getattr(cls, name)` for a method or classmethod: a callable.
    /// Instance methods take the instance as their first argument
    /// (`cls.connect(proto, uri, secret)`), as Python's unbound functions.
    pub fn method_attr(&'static self, name: &str) -> Option<Arg> {
        let name_owned = String::from(name);
        for h in self.hooks() {
            if h.class_method_names().contains(&name) {
                let me: &'static ModelClass = self;
                let n = name_owned.clone();
                return Some(Arg::Callable(Arc::new(move |a: Args| {
                    me.call_class_method(&n, a).unwrap_or_else(|| {
                        Err(LailaError::attribute_error(alloc::format!(
                            "type object '{}' has no attribute '{n}'",
                            me.name
                        )))
                    })
                })));
            }
        }
        for h in self.hooks() {
            if h.method_names().contains(&name) {
                let n = name_owned.clone();
                return Some(Arg::Callable(Arc::new(move |mut a: Args| {
                    if a.positional.is_empty() {
                        return Err(LailaError::type_error(alloc::format!(
                            "{n}() missing 1 required positional argument: 'self'"
                        )));
                    }
                    let me = a.positional.remove(0);
                    me.__call_method__(&n, a)
                })));
            }
        }
        None
    }
}

impl core::fmt::Debug for ModelClass {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "<class '{}.{}'>", self.module, self.name)
    }
}

// ---------------------------------------------------------------------------
// Instances

/// An instance of a [`ModelClass`]: `__dict__` (the fields) and
/// `__pydantic_private__`.
pub struct ModelInstance {
    pub class: &'static ModelClass,
    pub fields: RwLock<Kwargs>,
    pub private: RwLock<Kwargs>,
    /// The `Arc` this instance lives in once constructed (`self` as a
    /// value for hooks that hand the instance to other objects).
    this: RwLock<Option<alloc::sync::Weak<ModelInstance>>>,
}

/// pydantic's `ValidationError` text for one error.
pub fn validation_error(cls: &str, field: &str, msg: &str, kind: &str, input: &Arg) -> LailaError {
    LailaError::new(
        ExcKind::ValidationError,
        alloc::format!(
            "1 validation error for {cls}\n{field}\n  {msg} [type={kind}, input_value={}, input_type={}]",
            input.__repr__(),
            input.type_name()
        ),
    )
}

/// `Field(ge=, gt=, le=, lt=)` numeric constraints, checked after coercion
/// (a `None` value on an optional field is not constrained).
pub fn check_numeric_constraints(
    cls: &str,
    field: &str,
    info: &FieldInfo,
    value: &Arg,
) -> Result<()> {
    if value.is_none() {
        return Ok(());
    }
    use core::cmp::Ordering;
    for (k, bound) in &info.extra {
        let ord = value.py_cmp(bound);
        let (ok, msg, kind) = match k.as_str() {
            "ge" => (
                matches!(ord, Some(Ordering::Greater | Ordering::Equal)),
                "greater than or equal to",
                "greater_than_equal",
            ),
            "gt" => (
                matches!(ord, Some(Ordering::Greater)),
                "greater than",
                "greater_than",
            ),
            "le" => (
                matches!(ord, Some(Ordering::Less | Ordering::Equal)),
                "less than or equal to",
                "less_than_equal",
            ),
            "lt" => (
                matches!(ord, Some(Ordering::Less)),
                "less than",
                "less_than",
            ),
            _ => continue,
        };
        if !ok {
            return Err(validation_error(
                cls,
                field,
                &alloc::format!("Input should be {msg} {}", bound.__repr__()),
                kind,
                value,
            ));
        }
    }
    Ok(())
}

/// Lax-mode coercion of `value` to the annotation `ann`.
pub fn coerce(cls: &str, field: &str, ann: &str, value: Arg) -> Result<Arg> {
    let ann = ann.trim();
    // Optional[X] / X | None / None | X
    let optional = ann.starts_with("Optional[")
        || ann.split('|').any(|p| p.trim() == "None")
        || ann == "Any"
        || ann.is_empty();
    if value.is_none() && optional {
        return Ok(Arg::None);
    }
    let inner = if let Some(rest) = ann.strip_prefix("Optional[") {
        rest.trim_end_matches(']').trim()
    } else {
        ann.split('|')
            .map(str::trim)
            .find(|p| *p != "None")
            .unwrap_or(ann)
    };
    let base = inner.split('[').next().unwrap_or(inner).trim();
    match base {
        "int" => match &value {
            Arg::Int(_) | Arg::Long(_) => Ok(value),
            Arg::Bool(b) => Ok(Arg::Int(i64::from(*b))),
            Arg::Float(f) if crate::_compat::fmath::fract(*f) == 0.0 => Ok(Arg::Int(*f as i64)),
            Arg::Float(_) => Err(validation_error(
                cls,
                field,
                "Input should be a valid integer, got a number with a fractional part",
                "int_from_float",
                &value,
            )),
            Arg::Str(s) => match s.trim().parse::<i64>() {
                Ok(i) => Ok(Arg::Int(i)),
                Err(_) => Err(validation_error(
                    cls,
                    field,
                    "Input should be a valid integer, unable to parse string as an integer",
                    "int_parsing",
                    &value,
                )),
            },
            _ => Err(validation_error(
                cls,
                field,
                "Input should be a valid integer",
                "int_type",
                &value,
            )),
        },
        "float" => match &value {
            Arg::Float(_) => Ok(value),
            Arg::Int(i) => Ok(Arg::Float(*i as f64)),
            Arg::Bool(b) => Ok(Arg::Float(if *b { 1.0 } else { 0.0 })),
            Arg::Str(s) => match s.trim().parse::<f64>() {
                Ok(f) => Ok(Arg::Float(f)),
                Err(_) => Err(validation_error(
                    cls,
                    field,
                    "Input should be a valid number, unable to parse string as a number",
                    "float_parsing",
                    &value,
                )),
            },
            _ => Err(validation_error(
                cls,
                field,
                "Input should be a valid number",
                "float_type",
                &value,
            )),
        },
        "str" => match &value {
            Arg::Str(_) => Ok(value),
            _ => Err(validation_error(
                cls,
                field,
                "Input should be a valid string",
                "string_type",
                &value,
            )),
        },
        "bool" => match &value {
            Arg::Bool(_) => Ok(value),
            Arg::Int(0) => Ok(Arg::Bool(false)),
            Arg::Int(1) => Ok(Arg::Bool(true)),
            Arg::Str(s) => match s.trim().to_lowercase().as_str() {
                "0" | "off" | "f" | "false" | "n" | "no" => Ok(Arg::Bool(false)),
                "1" | "on" | "t" | "true" | "y" | "yes" => Ok(Arg::Bool(true)),
                _ => Err(validation_error(
                    cls,
                    field,
                    "Input should be a valid boolean, unable to interpret input",
                    "bool_parsing",
                    &value,
                )),
            },
            _ => Err(validation_error(
                cls,
                field,
                "Input should be a valid boolean",
                "bool_type",
                &value,
            )),
        },
        "dict" | "Dict" => match &value {
            Arg::Dict(_) => Ok(value),
            Arg::Object(_) if crate::_compat::dotmap::is_dotmap(&value) => {
                Ok(crate::_compat::dotmap::DotMap::downcast(&value)
                    .map(|d| d.to_dict())
                    .unwrap_or(value))
            }
            _ => Err(validation_error(
                cls,
                field,
                "Input should be a valid dictionary",
                "dict_type",
                &value,
            )),
        },
        "list" | "List" => match &value {
            Arg::List(_) => Ok(value),
            Arg::Tuple(_) | Arg::Set(_) | Arg::FrozenSet(_) => Ok(value.__iter__()?.into_arg()),
            _ => Err(validation_error(
                cls,
                field,
                "Input should be a valid list",
                "list_type",
                &value,
            )),
        },
        "set" | "Set" => match &value {
            Arg::Set(_) => Ok(value),
            Arg::List(_) | Arg::Tuple(_) | Arg::FrozenSet(_) => {
                crate::python::builtins::set(Args::from(value))
            }
            _ => Err(validation_error(
                cls,
                field,
                "Input should be a valid set",
                "set_type",
                &value,
            )),
        },
        _ => Ok(value),
    }
}

impl ModelInstance {
    fn empty(class: &'static ModelClass) -> ModelInstance {
        ModelInstance {
            class,
            fields: RwLock::new(Kwargs::default()),
            private: RwLock::new(Kwargs::default()),
            this: RwLock::new(None),
        }
    }

    /// `self` as the shared value this instance was constructed into
    /// (`None` for a bare / copied instance).
    pub fn self_arg(&self) -> Option<Arg> {
        self.this
            .read()
            .as_ref()
            .and_then(|w| w.upgrade())
            .map(|a| Arg::Object(a as Arc<dyn PyObject>))
    }

    /// `cls(**data)` -- `BaseModel.__init__` with laila's base-class hooks.
    pub fn construct(class: &'static ModelClass, args: Args) -> Result<Arg> {
        if !args.positional.is_empty() {
            return Err(LailaError::type_error(alloc::format!(
                "BaseModel.__init__() takes 1 positional argument but {} were given",
                args.positional.len() + 1
            )));
        }
        let mut data = Kwargs::default();
        for (k, v) in args.keywords {
            data.insert(k, v);
        }
        let hooks = class.hooks();
        let mut st = InitState::default();
        // __init__ heads, derived first (``_LAILA_IDENTIFIABLE_OBJECT.__init__`` pops the identity).
        for h in &hooks {
            h.init_head(class, &mut data, &mut st)?;
        }
        // BaseModel.__init__: before validators.
        for h in &hooks {
            h.before_validate(class, &mut data)?;
        }
        let shared = Arc::new(ModelInstance::empty(class));
        *shared.this.write() = Some(Arc::downgrade(&shared));
        let inst: &ModelInstance = &shared;
        // Fields: explicit value (lax coercion) > default / default_factory > required.
        {
            let mut fields = inst.fields.write();
            for (name, info) in class.all_fields() {
                if let Some(v) = data.shift_remove(&name) {
                    let v = coerce(class.name, &name, &info.annotation, v)?;
                    check_numeric_constraints(class.name, &name, &info, &v)?;
                    fields.insert(name.clone(), v);
                } else if info.is_required() {
                    let mut d = crate::python::PyDictBuilder::new();
                    for (k, v) in data.iter() {
                        d.insert(k.as_str(), v.clone());
                    }
                    return Err(validation_error(
                        class.name,
                        &name,
                        "Field required",
                        "missing",
                        &d.finish(),
                    ));
                } else {
                    fields.insert(name, info.get_default()?);
                }
            }
            // ``extra="ignore"``: unknown keys are dropped.
        }
        // __pydantic_private__ defaults.
        {
            let mut private = inst.private.write();
            for (name, p) in class.all_private_attrs() {
                if let Some(v) = p.get_default()? {
                    private.insert(name, v);
                }
            }
        }
        // model_post_init chain: base-most first (each override calls super() first).
        for h in hooks.iter().rev() {
            h.model_post_init(inst, &st)?;
        }
        // after validators.
        for h in &hooks {
            h.after_validate(inst)?;
        }
        // __init__ tails as the super() calls return: base-most first.
        for h in hooks.iter().rev() {
            h.init_tail(inst, &mut st)?;
        }
        Ok(Arg::Object(shared as Arc<dyn PyObject>))
    }

    /// The model instance inside an `Arg`, if it is one.
    pub fn downcast(a: &Arg) -> Option<&ModelInstance> {
        a.downcast_ref::<ModelInstance>()
    }

    /// `type(inst).__name__`
    pub fn class_name(&self) -> &'static str {
        self.class.name
    }

    /// A field value (`None` when `name` is not a field).
    pub fn field(&self, name: &str) -> Option<Arg> {
        self.fields.read().get(name).cloned()
    }

    /// Set a field value without validation (`validate_assignment=False`).
    pub fn set_field(&self, name: &str, value: Arg) {
        self.fields.write().insert(name.to_string(), value);
    }

    /// A private attribute (`None` when unset).
    pub fn private(&self, name: &str) -> Option<Arg> {
        self.private.read().get(name).cloned()
    }

    /// Set a private attribute.
    pub fn set_private(&self, name: &str, value: Arg) {
        self.private.write().insert(name.to_string(), value);
    }

    /// `inst.model_dump()` -- the fields as a plain dict (nested models recurse).
    pub fn model_dump(&self) -> Arg {
        let mut d = crate::python::PyDictBuilder::new();
        for (k, v) in self.fields.read().iter() {
            let v = match ModelInstance::downcast(v) {
                Some(m) => m.model_dump(),
                None => v.clone(),
            };
            d.insert(k.as_str(), v);
        }
        d.finish()
    }

    /// `inst.__dict__` snapshot.
    pub fn __dict__(&self) -> Vec<(String, Arg)> {
        self.fields
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn pydantic_repr(&self) -> String {
        let parts: Vec<String> = self
            .fields
            .read()
            .iter()
            .map(|(k, v)| alloc::format!("{k}={}", v.__repr__()))
            .collect();
        alloc::format!("{}({})", self.class.name, parts.join(", "))
    }

    /// `self` as an `Arg` sharing this instance (the instance is always
    /// behind an `Arc` once constructed; hooks receive `&ModelInstance`).
    pub fn to_arg_shared(&self) -> Arg {
        if let Some(a) = self.self_arg() {
            return a;
        }
        // A bare instance: a shallow copy of the two maps preserves field
        // equality semantics for `__eq__`-style comparisons.
        let copy = ModelInstance {
            class: self.class,
            fields: RwLock::new(self.fields.read().clone()),
            private: RwLock::new(self.private.read().clone()),
            this: RwLock::new(None),
        };
        copy.into_arg()
    }
}

impl PyObject for ModelInstance {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        self.class.name
    }
    fn __repr__(&self) -> String {
        for h in self.class.hooks() {
            if let Some(s) = h.repr(self) {
                return s;
            }
        }
        self.pydantic_repr()
    }
    fn __str__(&self) -> String {
        for h in self.class.hooks() {
            if let Some(s) = h.str(self) {
                return s;
            }
        }
        // pydantic: str(model) is the field list without the class name
        let parts: Vec<String> = self
            .fields
            .read()
            .iter()
            .map(|(k, v)| alloc::format!("{k}={}", v.__repr__()))
            .collect();
        parts.join(" ")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        match other.as_any().downcast_ref::<ModelInstance>() {
            Some(o) => {
                for h in self.class.hooks() {
                    if let Some(r) = h.eq(self, &o.to_arg_shared()) {
                        return r;
                    }
                }
                core::ptr::eq(self.class, o.class)
                    && *self.fields.read() == *o.fields.read()
                    && *self.private.read() == *o.private.read()
            }
            None => false,
        }
    }
    fn __eq_arg__(&self, other: &Arg) -> bool {
        for h in self.class.hooks() {
            if let Some(r) = h.eq(self, other) {
                return r;
            }
        }
        match other {
            Arg::Object(o) => self.__eq__(&**o),
            _ => false,
        }
    }
    fn __hash__(&self) -> Result<i64> {
        for h in self.class.hooks() {
            if let Some(r) = h.hash(self) {
                return r;
            }
        }
        Err(LailaError::type_error(alloc::format!(
            "unhashable type: '{}'",
            self.class.name
        )))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        if let Some(v) = self.field(name) {
            return Ok(v);
        }
        for h in self.class.hooks() {
            if let Some(r) = h.getattr(self, name) {
                return r;
            }
        }
        if let Some(v) = self.private(name) {
            return Ok(v);
        }
        if let Some(v) = self.class.class_var(name) {
            return Ok(v);
        }
        match name {
            "__class__" => Ok(self.class.as_pyclass().into_arg()),
            "__dict__" => {
                let mut d = crate::python::PyDictBuilder::new();
                for (k, v) in self.__dict__() {
                    d.insert(k, v);
                }
                Ok(d.finish())
            }
            "model_fields" => Ok(self.class.model_fields()),
            // `getattr(logger, "debug")`: a method read as a value is the
            // bound method.
            _ if self
                .class
                .hooks()
                .iter()
                .any(|h| h.method_names().contains(&name)) =>
            {
                let me = self.to_arg_shared();
                let n = String::from(name);
                Ok(Arg::Callable(Arc::new(move |a: Args| {
                    me.__call_method__(&n, a)
                })))
            }
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{name}'",
                self.class.name
            ))),
        }
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        if self.fields.read().contains_key(name) {
            self.set_field(name, value);
            return Ok(());
        }
        for h in self.class.hooks() {
            if let Some(r) = h.setattr(self, name, value.clone()) {
                return r;
            }
        }
        if name.starts_with('_') {
            self.set_private(name, value);
            return Ok(());
        }
        Err(LailaError::value_error(alloc::format!(
            "\"{}\" object has no field \"{name}\"",
            self.class.name
        )))
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        for h in self.class.hooks() {
            if let Some(r) = h.call_method(self, name, args.clone()) {
                return r;
            }
        }
        match name {
            "model_dump" => Ok(self.model_dump()),
            // A declared method no hook dispatches: `__getattr__` would hand
            // back a bound method that calls straight back here.
            _ if self
                .class
                .hooks()
                .iter()
                .any(|h| h.method_names().contains(&name)) =>
            {
                Err(LailaError::not_implemented(alloc::format!(
                    "{}.{name}() is only reachable through its typed handle",
                    self.class.name
                )))
            }
            _ => {
                // A callable stored in a field / private attribute.
                let v = self.__getattr__(name)?;
                v.__call__(args)
            }
        }
    }
    fn __dir__(&self) -> Vec<String> {
        let mut out: Vec<String> = self.fields.read().keys().cloned().collect();
        out.extend(self.private.read().keys().cloned());
        for h in self.class.hooks() {
            out.extend(h.property_setters().iter().map(|s| s.to_string()));
            out.extend(h.property_getters().iter().map(|s| s.to_string()));
            out.extend(h.method_names().iter().map(|s| s.to_string()));
        }
        for c in self.class.mro() {
            out.extend(c.class_vars.iter().map(|(k, _)| k.clone()));
        }
        out.push("model_fields".to_string());
        out.push("model_dump".to_string());
        out
    }
    fn __class__(&self) -> Option<PyClass> {
        Some(self.class.as_pyclass())
    }
}

impl IntoArg for ModelInstance {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl IsInstance for ModelInstance {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<ModelInstance>())
    }
}

/// `isinstance(x, <base>)` for a model base by class name.
pub fn isinstance_model(x: &Arg, class_name: &str) -> bool {
    ModelInstance::downcast(x).is_some_and(|m| m.class.is_subclass(class_name))
}

/// `hasattr(type(obj), "model_fields")` -- `obj` is a pydantic model instance.
pub fn has_model_fields(x: &Arg) -> bool {
    ModelInstance::downcast(x).is_some()
}

/// Lazily-built class descriptor storage for `py_model!` and the base
/// classes (`std::sync::LazyLock` on std, `spin::Lazy` otherwise).
#[cfg(feature = "std")]
pub type LazyClass = std::sync::LazyLock<ModelClass>;
#[cfg(not(feature = "std"))]
pub type LazyClass = spin::Lazy<ModelClass>;

/// Sorts one `name: annotation = value` class member into the field /
/// private-attribute / class-variable lists the way pydantic's metaclass
/// does: a `FieldInfo` value is a field, a `ModelPrivateAttr` a private
/// attribute, a `ClassVar` annotation (or a name a base declares as a class
/// variable) a class variable; a bare value is
/// `Field(default=value)` (or `PrivateAttr(default=value)` for underscore names).
pub fn classify_member(
    name: &str,
    annotation: &str,
    value: Arg,
    bases: &[&'static ModelClass],
    fields: &mut Vec<(String, FieldInfo)>,
    private: &mut Vec<(String, PrivateAttrInfo)>,
    class_vars: &mut Vec<(String, Arg)>,
) {
    // ``ClassVar`` annotations and names a base declares as class variables
    // (pydantic's ``base_class_vars``) stay plain class attributes.
    if annotation.starts_with("ClassVar") || bases.iter().any(|b| b.class_var(name).is_some()) {
        class_vars.push((name.to_string(), value));
        return;
    }
    if let Some(p) = value.downcast_ref::<PrivateAttrInfo>() {
        private.push((name.to_string(), p.clone()));
        return;
    }
    if let Some(f) = value.downcast_ref::<FieldInfo>() {
        fields.push((name.to_string(), f.clone().with_annotation(annotation)));
        return;
    }
    if name.starts_with('_') {
        private.push((
            name.to_string(),
            PrivateAttrInfo {
                default: Some(value),
                default_factory: None,
            },
        ));
        return;
    }
    fields.push((
        name.to_string(),
        FieldInfo {
            default: Some(value),
            default_factory: None,
            json_schema_extra: Arg::None,
            annotation: annotation.to_string(),
            extra: Vec::new(),
        },
    ));
}

/// Unit structs standing for a Python class in `py_model!(Child: Base, ..)`
/// base lists (`_LAILA_OBJECT`, `_LAILA_IDENTIFIABLE_OBJECT`, `_LAILA_CLI_CAPABLE_CLASS`).
pub trait ModelBase {
    /// The class descriptor of the base.
    fn __class__() -> &'static ModelClass;
}
