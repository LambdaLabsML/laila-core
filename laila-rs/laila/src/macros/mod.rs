//! Laila macros sub-package -- aliases, defaults, and scope strings.
//!
//! Catch-all home for *constants* and *factory helpers* that need to be
//! referenced from many places in laila but don't belong in any one domain
//! module:
//!
//! - [`aliases`] -- short-form re-exports for the most-used public classes
//!   (e.g. `DefaultPolicy`, `Entry`).
//! - [`defaults`] -- the default concrete classes behind each abstract role,
//!   plus the `LAILA_DEFAULT_DIRECTORIES` table that other modules consult
//!   for "where do I put my files?".
//! - [`strings`] -- the canonical scope-string constants that show up in
//!   global ids and CLI-args paths (`_POLICY_SCOPE`, `_FUTURE_SCOPE`,
//!   `_POOL_SCOPE`, ...). Centralised here so a rename touches exactly one
//!   file.
//!
//! Each submodule is also a *module object* value of the same name
//! (`laila::macros::strings` the module, `laila::macros::strings` the
//! static), so Python code that treats the module as an object --
//! `dir(strings)`, `getattr(strings, name)`, `aliases.future`,
//! `defaults.DefaultPolicy` -- keeps its spelling in `laila::python!`.

pub mod aliases;
pub mod defaults;
pub mod strings;

/// `from laila import macros` as a value (`hasattr(macros, "defaults")`).
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct macros_mod;
impl crate::python::PyArg for macros_mod {
    fn to_arg(&self) -> crate::python::Arg {
        crate::python::mock::module_target_with("laila.macros", &["aliases", "defaults", "strings"])
    }
}

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, PyClass, PyObject, Result};

fn no_attr(module: &str, name: &str) -> LailaError {
    LailaError::attribute_error(alloc::format!(
        "module '{module}' has no attribute '{name}'"
    ))
}

fn module_repr(name: &str) -> String {
    alloc::format!("<module '{name}'>")
}

// ---------------------------------------------------------------------------
// laila.macros.strings

/// The `laila.macros.strings` module as a value.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct strings_mod;

/// `import laila.macros.strings as strings`
#[allow(non_upper_case_globals)]
pub static strings: strings_mod = strings_mod;

impl strings_mod {
    /// `getattr(strings, name)`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "__name__" => return Ok(Arg::Str(strings::__name__.into())),
            "__doc__" => return Ok(Arg::Str(strings::__doc__.into())),
            _ => {}
        }
        strings::MEMBERS
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| Arg::Str((*v).into()))
            .ok_or_else(|| no_attr(strings::__name__, name))
    }
}

impl PyObject for strings_mod {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "module"
    }
    fn __repr__(&self) -> String {
        module_repr(strings::__name__)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().is::<strings_mod>()
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(strings::__name__.into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        strings_mod::__getattr__(self, name)
    }
    fn __dir__(&self) -> Vec<String> {
        let mut out: Vec<String> = strings::MEMBERS
            .iter()
            .map(|(n, _)| n.to_string())
            .collect();
        out.push("__doc__".into());
        out.push("__name__".into());
        out
    }
}
impl PyArg for strings_mod {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}

// ---------------------------------------------------------------------------
// laila.macros.aliases

/// The `laila.macros.aliases` module as a value.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct aliases_mod;

/// `import laila.macros.aliases as aliases`
#[allow(non_upper_case_globals)]
pub static aliases: aliases_mod = aliases_mod;

impl aliases_mod {
    /// `aliases.constant(data, *, global_id=None, uuid=None, nickname=None)`
    pub fn constant(&self, args: impl Into<Args>) -> Result<crate::entry::Entry> {
        aliases::constant(args)
    }
    /// `aliases.variable(data=None, ...)`
    pub fn variable(&self, args: impl Into<Args>) -> Result<crate::entry::Entry> {
        aliases::variable(args)
    }
    /// `aliases.contingent(**kwargs)`
    pub fn contingent(&self, args: impl Into<Args>) -> Result<crate::entry::Entry> {
        aliases::contingent(args)
    }
    /// `aliases.future` -- the `Future` class object.
    pub fn future(&self) -> Arg {
        PyClass::named(
            "Future",
            "laila.policy.central.command.schema.future.future",
        )
        .into_arg()
    }
    /// `getattr(aliases, name)`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "constant" => Arg::Callable(Arc::new(|a: Args| {
                aliases::constant(a).map(IntoArg::into_arg)
            })),
            "variable" => Arg::Callable(Arc::new(|a: Args| {
                aliases::variable(a).map(IntoArg::into_arg)
            })),
            "contingent" => Arg::Callable(Arc::new(|a: Args| {
                aliases::contingent(a).map(IntoArg::into_arg)
            })),
            "future" | "Future" => self.future(),
            "Entry" => PyClass::with_ctor(
                "Entry",
                "laila.entry.schema.base",
                Arc::new(|a: Args| crate::entry::Entry::new(a).map(IntoArg::into_arg)),
            )
            .into_arg(),
            "__name__" => Arg::Str(aliases::__name__.into()),
            "__doc__" => Arg::Str(aliases::__doc__.into()),
            other => return Err(no_attr(aliases::__name__, other)),
        })
    }
}

impl PyObject for aliases_mod {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "module"
    }
    fn __repr__(&self) -> String {
        module_repr(aliases::__name__)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().is::<aliases_mod>()
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(aliases::__name__.into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        aliases_mod::__getattr__(self, name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "constant" => aliases::constant(args).map(IntoArg::into_arg),
            "variable" => aliases::variable(args).map(IntoArg::into_arg),
            "contingent" => aliases::contingent(args).map(IntoArg::into_arg),
            other => Err(no_attr(aliases::__name__, other)),
        }
    }
    fn __dir__(&self) -> Vec<String> {
        [
            "Entry",
            "Future",
            "__doc__",
            "__name__",
            "constant",
            "contingent",
            "future",
            "variable",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}
impl PyArg for aliases_mod {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}

// ---------------------------------------------------------------------------
// laila.macros.defaults

/// The `laila.macros.defaults` module as a value.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct defaults_mod;

/// `import laila.macros.defaults as defaults`
#[allow(non_upper_case_globals)]
pub static defaults: defaults_mod = defaults_mod;

/// The class object behind a `Default*` alias (or a concrete class name
/// imported by the module). The pool, pool router, multibuffer and policy
/// have Rust constructors; the others are references to Python classes
/// whose port is pending.
pub(crate) fn default_class_object(name: &str) -> Option<Arg> {
    let (concrete, module) = match defaults::default_class(name) {
        Some(x) => x,
        None => defaults::DEFAULT_CLASS_ALIASES
            .iter()
            .find(|(_, n, _)| *n == name)
            .map(|(_, n, m)| (*n, *m))?,
    };
    #[cfg(feature = "std")]
    if let Some(spec) = crate::policy::central::communication::protocols::ALL_SPECS
        .iter()
        .find(|s| s.class_name == concrete && s.module == module)
    {
        return Some(spec.class_object().into_arg());
    }
    let cls = match concrete {
        "_LAILA_IDENTIFIABLE_POOL" => defaults::DefaultPool::__class__().as_pyclass(),
        "_LAILA_IDENTIFIABLE_POOL_ROUTER" => defaults::DefaultPoolRouter::__class__().as_pyclass(),
        "MultiBuffer" => defaults::DefaultMultiBuffer::__class__().as_pyclass(),
        "_LAILA_IDENTIFIABLE_POLICY" => defaults::DefaultPolicy::class_object(),
        _ => PyClass::named(concrete, module),
    };
    Some(cls.into_arg())
}

impl defaults_mod {
    /// `defaults.DefaultPool(**kwargs)` -> a `_LAILA_IDENTIFIABLE_POOL` instance.
    #[allow(non_snake_case)]
    pub fn DefaultPool(&self, args: impl Into<Args>) -> Result<crate::data::Pool> {
        defaults::DefaultPool::new(args)
    }
    /// `defaults.DefaultPoolRouter(**kwargs)`
    #[allow(non_snake_case)]
    pub fn DefaultPoolRouter(
        &self,
        args: impl Into<Args>,
    ) -> Result<crate::policy::central::memory::router::PoolRouter> {
        defaults::DefaultPoolRouter::new(args)
    }
    /// `defaults.DefaultMultiBuffer(**kwargs)`
    #[allow(non_snake_case)]
    pub fn DefaultMultiBuffer(
        &self,
        args: impl Into<Args>,
    ) -> Result<crate::data::MultiBufferHandle> {
        defaults::DefaultMultiBuffer::new(args)
    }
    /// `defaults.DefaultPolicy(**kwargs)`
    #[allow(non_snake_case)]
    pub fn DefaultPolicy(&self, args: impl Into<Args>) -> Result<crate::policy::Policy> {
        defaults::DefaultPolicy::new(args)
    }
    /// `defaults.LAILA_UNIVERSAL_NAMESPACE`
    #[allow(non_snake_case)]
    pub fn LAILA_UNIVERSAL_NAMESPACE(&self) -> crate::python::stdlib::UUID {
        defaults::LAILA_UNIVERSAL_NAMESPACE()
    }
    /// `defaults.AUTO_INITIALIZE_POLICY`
    #[allow(non_snake_case)]
    pub fn AUTO_INITIALIZE_POLICY(&self) -> bool {
        defaults::AUTO_INITIALIZE_POLICY
    }
    /// `defaults.LAILA_DEFAULT_DIRECTORIES`
    #[allow(non_snake_case)]
    pub fn LAILA_DEFAULT_DIRECTORIES(&self) -> Arg {
        defaults::LAILA_DEFAULT_DIRECTORIES()
    }
    /// `getattr(defaults, name)`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "LAILA_UNIVERSAL_NAMESPACE" => {
                return Ok(defaults::LAILA_UNIVERSAL_NAMESPACE().into_arg())
            }
            "AUTO_INITIALIZE_POLICY" => return Ok(Arg::Bool(defaults::AUTO_INITIALIZE_POLICY)),
            "LAILA_DEFAULT_DIRECTORIES" => return Ok(defaults::LAILA_DEFAULT_DIRECTORIES()),
            "_DEFAULT_ROOT" => return Ok(Arg::Str(defaults::_DEFAULT_ROOT().into())),
            "__name__" => return Ok(Arg::Str(defaults::__name__.into())),
            "__doc__" => return Ok(Arg::Str(defaults::__doc__.into())),
            _ => {}
        }
        default_class_object(name).ok_or_else(|| no_attr(defaults::__name__, name))
    }
}

impl PyObject for defaults_mod {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "module"
    }
    fn __repr__(&self) -> String {
        module_repr(defaults::__name__)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().is::<defaults_mod>()
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(defaults::__name__.into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        defaults_mod::__getattr__(self, name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let cls = default_class_object(name).ok_or_else(|| no_attr(defaults::__name__, name))?;
        cls.__call__(args)
    }
    fn __dir__(&self) -> Vec<String> {
        let mut out: Vec<String> = alloc::vec![
            "AUTO_INITIALIZE_POLICY".into(),
            "LAILA_DEFAULT_DIRECTORIES".into(),
            "LAILA_UNIVERSAL_NAMESPACE".into(),
            "_DEFAULT_ROOT".into(),
            "__doc__".into(),
            "__name__".into(),
        ];
        for (alias, concrete, _) in defaults::DEFAULT_CLASS_ALIASES {
            out.push(alias.to_string());
            if !out.iter().any(|s| s == concrete) {
                out.push(concrete.to_string());
            }
        }
        out
    }
}
impl PyArg for defaults_mod {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}

/// `__name__` of the Python package.
pub const __name__: &str = "laila.macros";
