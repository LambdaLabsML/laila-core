//! Virtual base for every laila data container.
//!
//! A *data container* is any identifiable object that holds records and
//! hands entries back to whoever reads from it. The base class fixes the
//! shape of that contract without choosing a key type -- that is left to
//! the subclasses, which is exactly where the two concrete families
//! differ:
//!
//! - `_LAILA_IDENTIFIABLE_POOL` (`data/schema/base.rs`) is a **map**: keys
//!   are entry `global_id` strings and the container is the persistence
//!   tier that `memorize` / `remember` / `forget` route to.
//! - `MultiBuffer` (`data/multibuffer/multibuffer.rs`) is a **list**: keys
//!   are integer slot indices and the container is a fixed-capacity ring
//!   with independent read/write heads -- the proxy a microcontroller uses
//!   to stand in front of a camera's frame buffer.
//!
//! Both agree on the *value* contract: what goes in is a `Record` (an entry
//! plus provenance), and what comes out is the bare `Entry`.
//!
//! Virtual in the laila sense
//! --------------------------
//! The class is instantiable as a type (Pydantic needs that to build the
//! schema, and tests use it to check identity plumbing), but the three
//! item-access methods raise `NotImplementedError` until a subclass
//! supplies them -- the same convention `BotoPool._get_client` and the
//! pool storage hooks follow.
//!
//! Rust shape: [`_LAILA_IDENTIFIABLE_DATA_CONTAINER`] is the *class* (a unit
//! struct that is both the `py_model!`-style base -- `ModelBase` -- and the
//! constructor, `_LAILA_IDENTIFIABLE_DATA_CONTAINER()`), and
//! [`DataContainer`] is the handle of a bare instance of the virtual base.
//! The two concrete families have their own handles (`Pool`,
//! `MultiBuffer`); they share the class descriptor as their MRO root and
//! the `isinstance` test below.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::_compat::pydantic::{
    LazyClass, ModelBase, ModelClass, ModelHooks, ModelInstance, PrivateAttrInfo,
};
use crate::atomic::definitions::locally_atomic_identifiable_object::LocallyAtomicIdentifiableBase;
use crate::atomic::definitions::{HasIdentity, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT};
use crate::basics::definitions::cli_capable::_LAILA_CLI_CAPABLE_CLASS;
use crate::macros::strings::_DATA_CONTAINER_SCOPE;
use crate::python::builtins::IsInstance;
use crate::python::object::{HasAttrs, PyAttrs, PyObject};
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, Result};

/// `_scopes: list[str] = PrivateAttr(default_factory=lambda: list([_DATA_CONTAINER_SCOPE]))`
fn scopes_attr(scope: &'static str) -> PrivateAttrInfo {
    PrivateAttrInfo {
        default: None,
        default_factory: Some(Arc::new(move |_a: Args| {
            Ok(alloc::vec![Arg::Str(scope.into())].into_arg())
        })),
    }
}

/// The `_scopes` private attribute of a data-container subclass
/// (`PrivateAttr(default_factory=lambda: list([<scope>]))`).
pub fn scopes_private_attr(scope: &'static str) -> (String, PrivateAttrInfo) {
    (String::from("_scopes"), scopes_attr(scope))
}

/// The error every virtual item method raises.
fn virtual_error(class_name: &str, method: &str) -> LailaError {
    LailaError::not_implemented(alloc::format!(
        "{class_name} does not implement {method}; _LAILA_IDENTIFIABLE_DATA_CONTAINER is a virtual base."
    ))
}

struct Hooks;

impl ModelHooks for Hooks {
    fn method_names(&self) -> &'static [&'static str] {
        &["__getitem__", "__setitem__", "empty"]
    }
}

static HOOKS: Hooks = Hooks;

static CLASS: LazyClass = LazyClass::new(|| ModelClass {
    name: "_LAILA_IDENTIFIABLE_DATA_CONTAINER",
    module: "laila.data.schema.data_container",
    bases: alloc::vec![
        _LAILA_CLI_CAPABLE_CLASS::__class__(),
        LocallyAtomicIdentifiableBase::__class__(),
    ],
    fields: Vec::new(),
    private_attrs: alloc::vec![scopes_private_attr(_DATA_CONTAINER_SCOPE)],
    class_vars: Vec::new(),
    hooks: Some(&HOOKS),
});

/// Virtual base class for record-holding containers.
///
/// Carries the identity contract (`global_id` / uuid / nickname / scopes),
/// per-instance atomic locking, and the CLI parameter resolution tiers.
/// Declares -- but does not implement -- the item protocol every container
/// exposes:
///
/// - `container[key]` returns the entry stored at *key* (or `None` when the
///   slot is empty).
/// - `container[key] = value` stores *value* at *key*; subclasses decide
///   how a bare entry becomes a record on the way in.
/// - `container.empty()` discards every stored record.
///
/// The meaning of *key* is entirely up to the subclass.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct _LAILA_IDENTIFIABLE_DATA_CONTAINER;

impl ModelBase for _LAILA_IDENTIFIABLE_DATA_CONTAINER {
    fn __class__() -> &'static ModelClass {
        _LAILA_IDENTIFIABLE_DATA_CONTAINER::__class__()
    }
}

impl _LAILA_IDENTIFIABLE_DATA_CONTAINER {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `_LAILA_IDENTIFIABLE_DATA_CONTAINER(**kwargs)`: a bare instance of
    /// the virtual base (identity plumbing only).
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<DataContainer> {
        let inst = ModelInstance::construct(Self::__class__(), args.into())?;
        Ok(DataContainer(Arc::new(DataContainerInner {
            inst,
            __dict__: PyAttrs::new(),
        })))
    }
    /// `cls.model_fields`
    pub fn model_fields() -> Arg {
        Self::__class__().model_fields()
    }
    /// `cls.__name__`
    pub fn __name__() -> Arg {
        Arg::Str(Self::__class__().name.into())
    }
}

impl PyArg for _LAILA_IDENTIFIABLE_DATA_CONTAINER {
    fn to_arg(&self) -> Arg {
        Self::__class__().as_pyclass().into_arg()
    }
}
impl IntoArg for _LAILA_IDENTIFIABLE_DATA_CONTAINER {
    fn into_arg(self) -> Arg {
        Self::__class__().as_pyclass().into_arg()
    }
}
impl crate::python::builtins::CallValue for _LAILA_IDENTIFIABLE_DATA_CONTAINER {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Self::new(args).map(IntoArg::into_arg)
    }
}
impl IsInstance for _LAILA_IDENTIFIABLE_DATA_CONTAINER {
    /// `isinstance(x, _LAILA_IDENTIFIABLE_DATA_CONTAINER)`: a pool, a
    /// `MultiBuffer`, a bare container, or a model instance of a subclass.
    fn isinstance(x: &Arg) -> bool {
        match x {
            Arg::Pool(_) => true,
            Arg::Object(o) => {
                o.as_any().is::<DataContainer>()
                    || o.as_any().is::<crate::data::MultiBufferHandle>()
                    || crate::_compat::pydantic::isinstance_model(
                        x,
                        "_LAILA_IDENTIFIABLE_DATA_CONTAINER",
                    )
            }
            _ => false,
        }
    }
}

struct DataContainerInner {
    /// The pydantic instance (identity, `_scopes`, CLI-resolved fields).
    inst: Arg,
    /// `object.__setattr__` attributes: `_local_lock` and friends.
    __dict__: PyAttrs,
}

/// A bare instance of the virtual base (`_LAILA_IDENTIFIABLE_DATA_CONTAINER()`).
#[derive(Clone)]
pub struct DataContainer(Arc<DataContainerInner>);

impl DataContainer {
    fn inst(&self) -> &ModelInstance {
        ModelInstance::downcast(&self.0.inst).expect("data container model instance")
    }
    /// `type(self).__name__`
    pub fn class_name(&self) -> &'static str {
        self.inst().class_name()
    }
    /// `self._scopes`
    pub fn _scopes(&self) -> Arg {
        self.inst().private("_scopes").unwrap_or(Arg::None)
    }
    /// `self.nickname` -- not a field of the base (identity only); `None`.
    pub fn nickname(&self) -> Arg {
        Arg::None
    }
    /// `container[key]` -- virtual: `NotImplementedError`.
    pub fn __getitem__(&self, _key: impl IntoArg) -> Result<Arg> {
        Err(virtual_error(self.class_name(), "__getitem__"))
    }
    /// `container[key] = value` -- virtual: `NotImplementedError`.
    pub fn __setitem__(&self, _key: impl IntoArg, _value: impl IntoArg) -> Result<()> {
        Err(virtual_error(self.class_name(), "__setitem__"))
    }
    /// `container.empty()` -- virtual: `NotImplementedError`.
    pub fn empty(&self, _args: impl Into<Args>) -> Result<()> {
        Err(virtual_error(self.class_name(), "empty"))
    }
    /// `container.<name>` for the dynamic layer.
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "_scopes" => Ok(self._scopes()),
            _ => {
                if let Some(v) = self.0.__dict__.get(name) {
                    return Ok(v);
                }
                PyObject::__getattr__(self.inst(), name)
            }
        }
    }
    /// `container.<name> = value`
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        PyObject::__setattr__(self.inst(), name, value.into_arg())
    }
    /// Python `is`.
    pub fn is(&self, other: &DataContainer) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl HasAttrs for DataContainer {
    fn __attrs__(&self) -> &PyAttrs {
        &self.0.__dict__
    }
    fn __class_name__(&self) -> &'static str {
        self.class_name()
    }
}
impl HasIdentity for DataContainer {
    fn global_id(&self) -> String {
        HasIdentity::global_id(self.inst())
    }
    fn uuid(&self) -> String {
        HasIdentity::uuid(self.inst())
    }
}
impl _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT for DataContainer {}

impl core::fmt::Debug for DataContainer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&PyObject::__repr__(self))
    }
}

impl PyObject for DataContainer {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<DataContainer>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        self.class_name()
    }
    fn __repr__(&self) -> String {
        HasIdentity::global_id(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<DataContainer>()
            .is_some_and(|o| HasIdentity::global_id(o) == HasIdentity::global_id(self))
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(HasIdentity::global_id(self).into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        DataContainer::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        DataContainer::__setattr__(self, name, value)
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        DataContainer::__getitem__(self, key.clone())
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        DataContainer::__setitem__(self, key.clone(), value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "empty" => self.empty(args).map(|()| Arg::None),
            "__getitem__" => DataContainer::__getitem__(
                self,
                args.positional.first().cloned().unwrap_or(Arg::None),
            ),
            "atomic" => crate::atomic::definitions::locally_atomic_object::DynAtomicContext::new(
                self._ensure_local_lock(),
                self.class_name(),
                args,
            )
            .map(IntoArg::into_arg),
            _ => PyObject::__call_method__(self.inst(), name, args),
        }
    }
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(self.inst().class.as_pyclass())
    }
    fn __atomic_lock__(&self) -> Option<crate::python::sync::RLock> {
        Some(self._ensure_local_lock())
    }
    fn __dir__(&self) -> Vec<String> {
        let mut out = PyObject::__dir__(self.inst());
        out.push("_scopes".to_string());
        out
    }
}

impl IntoArg for DataContainer {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for DataContainer {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
