//! Python *class objects* as values.
//!
//! In Python a class is itself an object: `aliases.future` is the `Future`
//! class, `defaults.DefaultPolicy` is `_LAILA_IDENTIFIABLE_POLICY`,
//! `isinstance(x, type)` is `True` for both, and `type(obj)` returns one.
//! Rust types are not values, so the port carries a small descriptor,
//! [`PyClass`], wherever Python code reaches for a class as a value: module
//! objects (`laila.macros.defaults`), `type(x)`, and the pydantic-style
//! models declared with `py_model!` (see `_compat::pydantic`).
//!
//! A `PyClass` may carry a constructor (`DefaultPool()` builds a pool) and,
//! for `py_model!` classes, the model descriptor that `model_fields` and
//! `_eligible_model_fields` read.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use core::any::Any;

use super::args::Args;
use super::builtins::IsInstance;
use super::errors::{LailaError, Result};
use super::object::{Arg, Callable, IntoArg, PyArg, PyObject};

/// A Python class as a value.
#[derive(Clone)]
pub struct PyClass {
    /// `cls.__name__`
    pub name: &'static str,
    /// `cls.__module__`
    pub module: &'static str,
    /// `cls(*args, **kwargs)`; `None` when the class has no Rust constructor yet.
    pub ctor: Option<Callable>,
    /// The pydantic-style model behind the class (`py_model!` classes and
    /// the laila base classes), if any.
    pub model: Option<&'static crate::_compat::pydantic::ModelClass>,
    /// Class-level attributes and class methods the Rust twin answers
    /// (`cls.protocol_name`, `cls.__subclasses__()`), if any.
    pub hooks: Option<Arc<dyn ClassHooks>>,
}

/// Class-level attributes / methods of a [`PyClass`] without a pydantic
/// model: every verb returns `None` when the class has no such name.
#[allow(unused_variables)]
pub trait ClassHooks: Send + Sync {
    /// `cls.name`.
    fn getattr(&self, name: &str) -> Option<Arg> {
        None
    }
    /// `cls.name(*args, **kwargs)`.
    fn call_method(&self, name: &str, args: Args) -> Option<Result<Arg>> {
        None
    }
}

impl PyClass {
    /// A class object without a constructor (a *reference* to a Python class
    /// whose Rust twin is not a value, e.g. `Future`).
    pub const fn named(name: &'static str, module: &'static str) -> PyClass {
        PyClass {
            name,
            module,
            ctor: None,
            model: None,
            hooks: None,
        }
    }

    /// A class object whose call constructs through `ctor`.
    pub fn with_ctor(name: &'static str, module: &'static str, ctor: Callable) -> PyClass {
        PyClass {
            name,
            module,
            ctor: Some(ctor),
            model: None,
            hooks: None,
        }
    }

    /// The class with class-level [`ClassHooks`] installed.
    pub fn with_hooks(mut self, hooks: Arc<dyn ClassHooks>) -> PyClass {
        self.hooks = Some(hooks);
        self
    }

    /// `cls.__qualname__` / the text `repr(cls)` shows.
    pub fn qualified(&self) -> String {
        if self.module.is_empty() {
            self.name.to_string()
        } else {
            alloc::format!("{}.{}", self.module, self.name)
        }
    }

    /// `cls(*args, **kwargs)`.
    pub fn __call__(&self, args: Args) -> Result<Arg> {
        match &self.ctor {
            Some(c) => c(args),
            None => Err(LailaError::not_implemented(alloc::format!(
                "{} cannot be instantiated from Rust yet",
                self.qualified()
            ))),
        }
    }

    /// `cls.__name__`.
    pub fn __name__(&self) -> Arg {
        Arg::Str(self.name.into())
    }

    /// The class object inside an `Arg`, if it is one.
    pub fn downcast(a: &Arg) -> Option<&PyClass> {
        a.downcast_ref::<PyClass>()
    }
}

impl core::fmt::Debug for PyClass {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&PyObject::__repr__(self))
    }
}

impl PyObject for PyClass {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "type"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<class '{}'>", self.qualified())
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<PyClass>()
            .is_some_and(|o| o.name == self.name && o.module == self.module)
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(self.qualified().into()).__hash__()
    }
    /// A class object is built on demand each time it is read
    /// (`laila.DefaultPool`, `type(x)`); two descriptors of one class are
    /// the same Python object.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        self.__eq__(other)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "__name__" => Ok(self.__name__()),
            "__qualname__" => Ok(self.__name__()),
            "__module__" => Ok(Arg::Str(self.module.into())),
            "model_fields" => match self.model {
                Some(m) => Ok(m.model_fields()),
                None => match self.hooks.as_ref().and_then(|h| h.getattr("model_fields")) {
                    Some(v) => Ok(v),
                    None => Err(LailaError::attribute_error(alloc::format!(
                        "type object '{}' has no attribute 'model_fields'",
                        self.name
                    ))),
                },
            },
            "__private_attributes__" => match self.model {
                Some(m) => Ok(m.__private_attributes__()),
                None => Err(LailaError::attribute_error(alloc::format!(
                    "type object '{}' has no attribute '__private_attributes__'",
                    self.name
                ))),
            },
            other => {
                if let Some(m) = self.model {
                    if let Some(v) = m.class_var(other) {
                        return Ok(v);
                    }
                    if let Some(f) = m.method_attr(other) {
                        return Ok(f);
                    }
                }
                if let Some(h) = &self.hooks {
                    if let Some(v) = h.getattr(other) {
                        return Ok(v);
                    }
                    // a class method read as a value (`cls.matches_token`)
                    let h = h.clone();
                    let n = String::from(other);
                    let probe = h.call_method(&n, Args::new());
                    if probe.is_some() || h.getattr(&n).is_some() {
                        return Ok(super::object::callable(move |a: Args| {
                            h.call_method(&n, a)
                                .unwrap_or_else(|| Err(LailaError::attribute_error(n.clone())))
                        }));
                    }
                }
                Err(LailaError::attribute_error(alloc::format!(
                    "type object '{}' has no attribute '{other}'",
                    self.name
                )))
            }
        }
    }
    fn __call__(&self, args: Args) -> Result<Arg> {
        PyClass::__call__(self, args)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match (name, self.model) {
            ("model_fields", Some(m)) if args.positional.is_empty() => Ok(m.model_fields()),
            (_, Some(m)) => {
                if let Some(r) = m.call_class_method(name, args.clone()) {
                    return r;
                }
                if let Some(f) = m.method_attr(name) {
                    return f.__call__(args);
                }
                Err(LailaError::attribute_error(alloc::format!(
                    "type object '{}' has no attribute '{name}'",
                    self.name
                )))
            }
            _ => {
                if let Some(h) = &self.hooks {
                    if let Some(r) = h.call_method(name, args.clone()) {
                        return r;
                    }
                    if let Some(v) = h.getattr(name) {
                        return v.__call__(args);
                    }
                }
                Err(LailaError::attribute_error(alloc::format!(
                    "type object '{}' has no attribute '{name}'",
                    self.name
                )))
            }
        }
    }
}

impl IntoArg for PyClass {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for PyClass {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}

/// Marker for Python's `type` in `isinstance(x, type)`: `True` for class objects.
#[allow(non_camel_case_types)]
pub struct r#type {}
impl IsInstance for r#type {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<PyClass>())
    }
}

/// `isinstance(x, cls)` where `cls` is a class *value* (`other.__class__`,
/// `type(y)`, a tuple of them) rather than a type known at compile time.
/// Exact-class match (the class object of `x` compares equal to `cls`); the
/// Rust side has no MRO for class objects built at runtime.
pub fn isinstance_value(x: &Arg, cls: &Arg) -> Result<bool> {
    match cls {
        Arg::Tuple(t) => {
            for c in t.iter() {
                if isinstance_value(x, c)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Arg::Object(o) => match o.as_any().downcast_ref::<PyClass>() {
            Some(c) => {
                if c.name == "object" && c.module == "builtins" {
                    return Ok(true);
                }
                let mine = r#type(x.clone());
                Ok(PyClass::downcast(&mine)
                    .is_some_and(|m| m.name == c.name && m.module == c.module))
            }
            None => Err(LailaError::type_error(
                "isinstance() arg 2 must be a type, a tuple of types, or a union",
            )),
        },
        _ => Err(LailaError::type_error(
            "isinstance() arg 2 must be a type, a tuple of types, or a union",
        )),
    }
}

/// `type(x)`: the class object of a value. Model instances and class-backed
/// objects report their own class; builtins report the type by name.
#[allow(non_snake_case)]
pub fn r#type(x: impl IntoArg) -> Arg {
    let x = x.into_arg();
    if let Arg::Object(o) = &x {
        if let Some(c) = o.__class__() {
            return c.into_arg();
        }
        if o.as_any().is::<PyClass>() {
            return PyClass::named("type", "builtins").into_arg();
        }
    }
    if let Arg::Entry(e) = &x {
        return e.kind().__class__().into_arg();
    }
    if let Arg::Pool(p) = &x {
        // The pool's own (sub)class: `SQLitePool` lives in
        // `laila.data.sqlite.sqlite`, not in the schema base module.
        return p.__class__().into_arg();
    }
    let module = match &x {
        Arg::Future(_) => "laila.policy.central.command.schema.future.future",
        _ => "builtins",
    };
    PyClass::named(x.type_name(), module).into_arg()
}
