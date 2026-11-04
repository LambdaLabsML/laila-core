//! `contextvars` -- `ContextVar`, `copy_context()`, `Context.run()`.
//!
//! A context is an immutable map from variable to value; the *current*
//! context is per thread, and the event loop ([`super::asyncio`]) installs
//! each task's own context while it polls it, so a value `set` inside a
//! coroutine is visible to everything that coroutine awaits (and to the
//! executor threads it offloads to through `copy_context().run(...)`) and
//! to nothing else -- Python's semantics.
//!
//! Variables are `static`s; a variable's key is its address.

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;

type Slot = Arc<dyn Any + Send + Sync>;

/// `contextvars.Context`: an immutable mapping (copied on write).
#[derive(Clone, Default)]
pub struct Context {
    vars: Arc<Vec<(usize, Slot)>>,
}

impl Context {
    fn get(&self, key: usize) -> Option<&Slot> {
        self.vars.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
    }
    fn with(&self, key: usize, value: Option<Slot>) -> Context {
        let mut vars: Vec<(usize, Slot)> = self
            .vars
            .iter()
            .filter(|(k, _)| *k != key)
            .cloned()
            .collect();
        if let Some(v) = value {
            vars.push((key, v));
        }
        Context {
            vars: Arc::new(vars),
        }
    }
    /// `ctx.run(fn, *args)`: run *f* with this context current, then
    /// restore the caller's context. The variables *f* sets are kept in
    /// this `Context` value only if it is re-read -- `run` on a copy does
    /// not leak into the caller, as in Python.
    pub fn run<R>(&self, f: impl FnOnce() -> R) -> R {
        let _g = self.enter();
        f()
    }
    /// Install this context as the current one until the guard drops
    /// (the event loop uses it around each poll).
    pub fn enter(&self) -> ContextGuard {
        let prev = current::swap(self.clone());
        ContextGuard { prev: Some(prev) }
    }
    /// The current context after a `run` / `enter` (what `ContextVar.set`
    /// inside it changed).
    pub fn current() -> Context {
        current::get()
    }
}

/// Restores the previous context on drop; also writes back the context as
/// modified inside the scope to the `Context` value that was entered
/// (through [`ContextGuard::modified`]).
pub struct ContextGuard {
    prev: Option<Context>,
}
impl ContextGuard {
    /// The context as it stands now (with the variables set inside).
    pub fn modified(&self) -> Context {
        current::get()
    }
}
impl Drop for ContextGuard {
    fn drop(&mut self) {
        if let Some(p) = self.prev.take() {
            current::swap(p);
        }
    }
}

/// `contextvars.copy_context()`.
pub fn copy_context() -> Context {
    current::get()
}

#[cfg(feature = "std")]
mod current {
    use super::Context;
    use core::cell::RefCell;
    std::thread_local! {
        static CURRENT: RefCell<Context> = RefCell::new(Context::default());
    }
    pub fn get() -> Context {
        CURRENT.with(|c| c.borrow().clone())
    }
    pub fn swap(new: Context) -> Context {
        CURRENT.with(|c| core::mem::replace(&mut *c.borrow_mut(), new))
    }
}
#[cfg(not(feature = "std"))]
mod current {
    use super::Context;
    use crate::python::sync::Mutex;
    static CURRENT: Mutex<Option<Context>> = Mutex::new(None);
    pub fn get() -> Context {
        CURRENT.lock().clone().unwrap_or_default()
    }
    pub fn swap(new: Context) -> Context {
        CURRENT.lock().replace(new).unwrap_or_default()
    }
}

/// `contextvars.ContextVar(name, default=...)`; declare as a `static`.
pub struct ContextVar<T: Clone + Send + Sync + 'static> {
    pub name: &'static str,
    default: fn() -> T,
}

/// `ContextVar.set()`'s token: carries the previous value for `reset`.
pub struct Token<T> {
    prev: Option<T>,
}

impl<T: Clone + Send + Sync + 'static> ContextVar<T> {
    pub const fn new(name: &'static str, default: fn() -> T) -> Self {
        ContextVar { name, default }
    }
    fn key(&self) -> usize {
        self as *const Self as usize
    }
    /// `var.get()`: the value in the current context, else the default.
    pub fn get(&self) -> T {
        match current::get().get(self.key()) {
            Some(v) => v
                .downcast_ref::<T>()
                .cloned()
                .unwrap_or_else(|| (self.default)()),
            None => (self.default)(),
        }
    }
    /// `var.set(value)` -> token for `reset`.
    pub fn set(&self, value: T) -> Token<T> {
        let cur = current::get();
        let prev = cur
            .get(self.key())
            .and_then(|v| v.downcast_ref::<T>().cloned());
        current::swap(cur.with(self.key(), Some(Arc::new(value))));
        Token { prev }
    }
    /// `var.reset(token)`.
    pub fn reset(&self, token: Token<T>) {
        let cur = current::get();
        let slot: Option<Slot> = token.prev.map(|p| Arc::new(p) as Slot);
        current::swap(cur.with(self.key(), slot));
    }
    /// Run *f* with the variable set to *value* (set / reset pair).
    pub fn scoped<R>(&self, value: T, f: impl FnOnce() -> R) -> R {
        let t = self.set(value);
        let r = f();
        self.reset(t);
        r
    }
}

// ---------------------------------------------------------------------------
// The variable as a dynamic value (`laila::python!` reads an imported
// `CONSTANT`-case name through `module_value(&NAME)`): `var.get()`,
// `token = var.set(v)`, `var.reset(token)` on an `Arg`.
// ---------------------------------------------------------------------------

use crate::python::object::{Arg, IntoArg, PyObject};
use crate::python::{Args, LailaError, Result};
use alloc::string::String;

/// A context variable's value type as seen from the dynamic layer.
pub trait ContextValue: Clone + Send + Sync + 'static {
    fn to_arg(&self) -> Arg;
    fn from_arg(v: &Arg) -> Result<Self>;
}
impl ContextValue for Arg {
    fn to_arg(&self) -> Arg {
        self.clone()
    }
    fn from_arg(v: &Arg) -> Result<Self> {
        Ok(v.clone())
    }
}
impl ContextValue for Arc<Vec<String>> {
    fn to_arg(&self) -> Arg {
        let items: Vec<Arg> = self.iter().map(|s| Arg::Str(s.as_str().into())).collect();
        Arg::Tuple(Arc::from(items))
    }
    fn from_arg(v: &Arg) -> Result<Self> {
        Ok(Arc::new(
            v.__iter__()?.iter().map(|x| x.__str__()).collect(),
        ))
    }
}

/// `var.set(..)`'s token as a dynamic value (consumed by `var.reset(token)`).
pub struct TokenHandle<T: ContextValue> {
    token: crate::python::sync::Mutex<Option<Token<T>>>,
}
impl<T: ContextValue> PyObject for TokenHandle<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Token"
    }
    fn __repr__(&self) -> String {
        String::from("<Token>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        core::ptr::eq(
            other.as_any() as *const dyn Any as *const u8,
            self as *const Self as *const u8,
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok((self as *const Self as usize >> 4) as i64)
    }
}

/// A `ContextVar` as a dynamic value.
pub struct ContextVarHandle<T: ContextValue>(pub &'static ContextVar<T>);
impl<T: ContextValue> PyObject for ContextVarHandle<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "ContextVar"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<ContextVar name={:?}>", self.0.name)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|o| core::ptr::eq(o.0, self.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok((self.0.key() >> 4) as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "name" => Ok(Arg::Str(self.0.name.into())),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'ContextVar' object has no attribute '{name}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = || args.positional.first().cloned().unwrap_or(Arg::None);
        match name {
            "get" => Ok(self.0.get().to_arg()),
            "set" => {
                let token = self.0.set(T::from_arg(&first())?);
                Ok(Arg::Object(Arc::new(TokenHandle {
                    token: crate::python::sync::Mutex::new(Some(token)),
                })))
            }
            "reset" => {
                let token = match &first() {
                    Arg::Object(o) => o
                        .as_any()
                        .downcast_ref::<TokenHandle<T>>()
                        .and_then(|h| h.token.lock().take()),
                    _ => None,
                }
                .ok_or_else(|| {
                    LailaError::value_error(
                        "<Token> was created by a different ContextVar or already used",
                    )
                })?;
                self.0.reset(token);
                Ok(Arg::None)
            }
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'ContextVar' object has no attribute '{name}'"
            ))),
        }
    }
}

impl<T: ContextValue> crate::python::builtins::ModuleValue for &'static ContextVar<T> {
    fn module_value(self) -> Arg {
        Arg::Object(Arc::new(ContextVarHandle(self)))
    }
}

impl<T: ContextValue> IntoArg for &'static ContextVar<T> {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(ContextVarHandle(self)))
    }
}
