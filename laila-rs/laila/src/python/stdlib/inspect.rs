//! `import inspect` -- `inspect.signature(f)` over function values that
//! carry a `#[pysig]` signature (`Function`), `iscoroutinefunction`,
//! `isfunction` / `isclass`, and the `Signature` / `Parameter` objects
//! with Python's `parameters`, `name`, `default`, `kind` and `empty`.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::args::{Param, ParamKind, Signature as RawSignature};
use crate::python::object::{Function, PyObject};
use crate::python::{Arg, Args, IntoArg, LailaError, PyArg, Result};

/// `import inspect`
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct inspect_mod;
#[allow(non_upper_case_globals)]
pub static inspect: inspect_mod = inspect_mod;

impl PyObject for inspect_mod {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "module"
    }
    fn __repr__(&self) -> String {
        String::from("<module 'inspect'>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<inspect_mod>().is_some()
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x69_6e_73_70)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "Parameter" => Ok(ParameterClass.into_arg()),
            "Signature" => Ok(SignatureClass.into_arg()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "module 'inspect' has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = args.positional.first().cloned().unwrap_or(Arg::None);
        match name {
            "signature" => self.signature(first).map(IntoArg::into_arg),
            "iscoroutinefunction" => Ok(Arg::Bool(self.iscoroutinefunction(first))),
            "isfunction" => Ok(Arg::Bool(self.isfunction(first))),
            "isclass" => Ok(Arg::Bool(self.isclass(first))),
            "ismethod" => Ok(Arg::Bool(self.ismethod(first))),
            other => Err(LailaError::attribute_error(alloc::format!(
                "module 'inspect' has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for inspect_mod {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for inspect_mod {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}
impl crate::python::builtins::ModuleValue for inspect_mod {
    fn module_value(self) -> Arg {
        self.into_arg()
    }
}

impl inspect_mod {
    /// `inspect.signature(f)`: the signature of a function value. A
    /// `Function` (a nested `def` / a `#[pysig]` method read as a value)
    /// carries its own; a bare closure (`Arg::Callable`) has no
    /// introspectable signature, as a C-implemented callable in CPython
    /// (`ValueError: no signature found`).
    pub fn signature(&self, f: impl IntoArg) -> Result<Signature> {
        let f = f.into_arg();
        if let Some(func) = f.downcast_ref::<Function>() {
            return Ok(Signature::from_raw(func.sig));
        }
        if let Ok(sig) = f.__getattr__("__signature__") {
            if let Some(s) = sig.downcast_ref::<Signature>() {
                return Ok(s.clone());
            }
        }
        Err(LailaError::value_error(alloc::format!(
            "no signature found for {}",
            f.__repr__()
        )))
    }
    /// `inspect.iscoroutinefunction(f)`
    pub fn iscoroutinefunction(&self, f: impl IntoArg) -> bool {
        let f = f.into_arg();
        f.downcast_ref::<crate::python::stdlib::asyncio::CoroutineFunction>()
            .is_some()
    }
    /// `inspect.isfunction(f)`
    pub fn isfunction(&self, f: impl IntoArg) -> bool {
        let f = f.into_arg();
        matches!(f, Arg::Callable(_))
            || f.downcast_ref::<Function>().is_some()
            || self.iscoroutinefunction(f)
    }
    /// `inspect.isclass(x)`
    pub fn isclass(&self, x: impl IntoArg) -> bool {
        crate::python::PyClass::downcast(&x.into_arg()).is_some()
    }
    /// `inspect.ismethod(x)`: a bound method (a callable read off an
    /// instance; the port spells those as plain callables).
    pub fn ismethod(&self, x: impl IntoArg) -> bool {
        matches!(x.into_arg(), Arg::Callable(_))
    }
}

/// `inspect.Parameter.empty` / `inspect.Signature.empty`: the sentinel.
#[derive(Clone, Copy, Debug, Default)]
pub struct Empty;
impl PyObject for Empty {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "type"
    }
    fn __repr__(&self) -> String {
        String::from("<class 'inspect._empty'>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<Empty>().is_some()
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x65_6d_70_74)
    }
}
impl IntoArg for Empty {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

/// `inspect.Parameter` as a class value (`Parameter.empty`,
/// `Parameter.VAR_POSITIONAL`, ...).
#[derive(Clone, Copy, Debug, Default)]
pub struct ParameterClass;
impl PyObject for ParameterClass {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "type"
    }
    fn __repr__(&self) -> String {
        String::from("<class 'inspect.Parameter'>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<ParameterClass>().is_some()
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x70_61_72_61)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "empty" => Ok(Empty.into_arg()),
            "POSITIONAL_ONLY" => Ok(kind_arg("POSITIONAL_ONLY")),
            "POSITIONAL_OR_KEYWORD" => Ok(kind_arg("POSITIONAL_OR_KEYWORD")),
            "VAR_POSITIONAL" => Ok(kind_arg("VAR_POSITIONAL")),
            "KEYWORD_ONLY" => Ok(kind_arg("KEYWORD_ONLY")),
            "VAR_KEYWORD" => Ok(kind_arg("VAR_KEYWORD")),
            other => Err(LailaError::attribute_error(alloc::format!(
                "type object 'Parameter' has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for ParameterClass {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

/// `inspect.Signature` as a class value (`Signature.empty`).
#[derive(Clone, Copy, Debug, Default)]
pub struct SignatureClass;
impl PyObject for SignatureClass {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "type"
    }
    fn __repr__(&self) -> String {
        String::from("<class 'inspect.Signature'>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<SignatureClass>().is_some()
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(0x73_69_67_6e)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "empty" => Ok(Empty.into_arg()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "type object 'Signature' has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for SignatureClass {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

/// `Parameter.kind`: the `_ParameterKind` enum member (compares equal to
/// `inspect.Parameter.<KIND>`; `str()` is its name, as in Python).
fn kind_arg(name: &'static str) -> Arg {
    Arg::Str(name.into())
}

fn kind_name(k: ParamKind) -> &'static str {
    match k {
        ParamKind::PositionalOrKeyword => "POSITIONAL_OR_KEYWORD",
        ParamKind::VarPositional => "VAR_POSITIONAL",
        ParamKind::KeywordOnly => "KEYWORD_ONLY",
        ParamKind::VarKeyword => "VAR_KEYWORD",
    }
}

/// `inspect.Parameter`
#[derive(Clone, Debug)]
pub struct Parameter {
    pub name: String,
    pub kind: ParamKind,
    /// `Parameter.empty` when the parameter has no default (or its default
    /// is only known to the function body).
    pub default: Option<Arg>,
}
impl Parameter {
    fn from_raw(p: &Param) -> Parameter {
        Parameter {
            name: String::from(p.name),
            kind: p.kind,
            default: match (p.has_default, p.default) {
                (true, Some(d)) => Some(d()),
                _ => None,
            },
        }
    }
    /// `str(param)`: `name`, `name=default`, `*args`, `**kwargs`.
    pub fn __str__(&self) -> String {
        let prefix = match self.kind {
            ParamKind::VarPositional => "*",
            ParamKind::VarKeyword => "**",
            _ => "",
        };
        match &self.default {
            Some(d) => alloc::format!("{prefix}{}={}", self.name, d.__repr__()),
            None => alloc::format!("{prefix}{}", self.name),
        }
    }
}
impl PyObject for Parameter {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Parameter"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<Parameter \"{}\">", Parameter::__str__(self))
    }
    fn __str__(&self) -> String {
        Parameter::__str__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<Parameter>().is_some_and(|o| {
            o.name == self.name && o.kind == self.kind && o.default == self.default
        })
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(self.name.as_str().into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(Arg::Str(self.name.as_str().into())),
            "kind" => Ok(kind_arg(kind_name(self.kind))),
            "default" => Ok(self.default.clone().unwrap_or_else(|| Empty.into_arg())),
            "annotation" => Ok(Empty.into_arg()),
            "empty" => Ok(Empty.into_arg()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Parameter' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for Parameter {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

/// `inspect.Signature`
#[derive(Clone, Debug)]
pub struct Signature {
    pub parameters: Vec<Parameter>,
}
impl Signature {
    fn from_raw(sig: &RawSignature) -> Signature {
        Signature {
            parameters: sig.params.iter().map(Parameter::from_raw).collect(),
        }
    }
    /// `sig.parameters`: an ordered mapping `name -> Parameter`.
    pub fn parameters(&self) -> Arg {
        let mut d = crate::python::PyDictBuilder::new();
        for p in &self.parameters {
            d.insert(p.name.clone(), p.clone());
        }
        d.finish()
    }
    /// `str(sig)`: `(a, b=1, *args, **kwargs)`.
    pub fn __str__(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        let mut star_done = false;
        for p in &self.parameters {
            if p.kind == ParamKind::KeywordOnly && !star_done {
                parts.push(String::from("*"));
                star_done = true;
            }
            if p.kind == ParamKind::VarPositional {
                star_done = true;
            }
            parts.push(p.__str__());
        }
        alloc::format!("({})", parts.join(", "))
    }
}
impl PyObject for Signature {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Signature"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<Signature {}>", Signature::__str__(self))
    }
    fn __str__(&self) -> String {
        Signature::__str__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().downcast_ref::<Signature>().is_some_and(|o| {
            o.parameters.len() == self.parameters.len()
                && o.parameters
                    .iter()
                    .zip(&self.parameters)
                    .all(|(a, b)| a.__eq__(b))
        })
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(Signature::__str__(self).into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "parameters" => Ok(self.parameters()),
            "return_annotation" => Ok(Empty.into_arg()),
            "empty" => Ok(Empty.into_arg()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Signature' object has no attribute '{other}'"
            ))),
        }
    }
}
impl IntoArg for Signature {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
