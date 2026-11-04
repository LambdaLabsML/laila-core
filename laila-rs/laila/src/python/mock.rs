//! `unittest.mock` -- `patch.object` and `MagicMock`, the part of the module
//! the test tree uses to swap a method of a laila class for a recording
//! stand-in.
//!
//! A Rust method cannot be reassigned, so a patched attribute lives in a
//! process-wide registry keyed by `(class name, attribute)`; the classes
//! whose Python twin is patched in the tests consult the registry at the
//! top of each patchable method ([`patched`]). `patch.start()` installs the
//! mock and `stop()` (or the `with` guard) removes it, restoring whatever
//! was installed before -- exactly the stacking `unittest.mock` does.
//!
//! The registry is global because Python's is: a patch applies to every
//! thread. `unittest` runs tests one after another, `cargo test` runs them
//! on parallel threads, so an active patch session also holds a
//! process-wide session lock: tests that patch run one at a time (tests that
//! do not patch are unaffected).

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;

use spin::RwLock;

use super::args::Args;
use super::errors::{ExcKind, LailaError};
use super::object::{Arg, IntoArg, PyArg, PyObject};
use super::pyclass::PyClass;
use super::Result;

/// `mock.patch.object(..)` / `mock.patch(..)` through `from unittest import mock`.
pub use super::unittest::patch;

// ---------------------------------------------------------------------------
// The registry

struct Installed {
    class: String,
    attribute: String,
    mock: Arg,
}

static INSTALLED: RwLock<Vec<Installed>> = RwLock::new(Vec::new());

/// The attribute currently patched on *class* (the innermost patch), if any.
pub fn patched(class: &str, attribute: &str) -> Option<Arg> {
    INSTALLED
        .read()
        .iter()
        .rev()
        .find(|p| p.class == class && p.attribute == attribute)
        .map(|p| p.mock.clone())
}

/// Call the patched *attribute* of *class* the way `autospec=True` binds
/// it: *receiver* first, then *args*. `None` when the attribute is not
/// patched.
pub fn call_patched(
    class: &str,
    attribute: &str,
    receiver: impl IntoArg,
    args: Args,
) -> Option<Result<Arg>> {
    let mock = patched(class, attribute)?;
    let bound = match &mock {
        Arg::Object(o) => o
            .as_any()
            .downcast_ref::<MagicMock>()
            .map(|m| m.autospec())
            .unwrap_or(false),
        _ => false,
    };
    let mut call = Args::new();
    if bound {
        call.push_positional(receiver);
    }
    call.positional.extend(args.positional);
    call.keywords.extend(args.keywords);
    Some(mock.__call__(call))
}

/// Call the patched *attribute* of *class* the way Python resolves a
/// method patched **on the class** (`patch.object(Cls, "m", spy)`): a plain
/// function / closure is unbound, so *receiver* is passed first; a
/// `MagicMock` only receives it under `autospec=True`. `None` when the
/// attribute is not patched.
pub fn call_patched_method(
    class: &str,
    attribute: &str,
    receiver: impl IntoArg,
    args: Args,
) -> Option<Result<Arg>> {
    let mock = patched(class, attribute)?;
    let bound = match &mock {
        Arg::Object(o) => match o.as_any().downcast_ref::<MagicMock>() {
            Some(m) => m.autospec(),
            None => true,
        },
        _ => true,
    };
    let mut call = Args::new();
    if bound {
        call.push_positional(receiver);
    }
    call.positional.extend(args.positional);
    call.keywords.extend(args.keywords);
    Some(mock.__call__(call))
}

#[cfg(feature = "std")]
mod session {
    //! One patch session per thread; the first `start()` takes the global
    //! lock, the last `stop()` releases it.
    use std::cell::Cell;
    use std::sync::{Mutex, MutexGuard};

    static LOCK: Mutex<()> = Mutex::new(());
    thread_local! {
        static DEPTH: Cell<usize> = const { Cell::new(0) };
        static GUARD: Cell<Option<MutexGuard<'static, ()>>> = const { Cell::new(None) };
    }

    pub fn enter() {
        let depth = DEPTH.with(|d| d.get());
        if depth == 0 {
            let g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
            GUARD.with(|s| s.set(Some(g)));
        }
        DEPTH.with(|d| d.set(depth + 1));
    }

    pub fn exit() {
        let depth = DEPTH.with(|d| d.get());
        if depth == 0 {
            return;
        }
        DEPTH.with(|d| d.set(depth - 1));
        if depth == 1 {
            GUARD.with(|s| drop(s.take()));
        }
    }
}

#[cfg(not(feature = "std"))]
mod session {
    pub fn enter() {}
    pub fn exit() {}
}

fn install(class: &str, attribute: &str, mock: Arg) {
    session::enter();
    INSTALLED.write().push(Installed {
        class: class.to_string(),
        attribute: attribute.to_string(),
        mock,
    });
}

fn uninstall(class: &str, attribute: &str, mock: &Arg) {
    let mut g = INSTALLED.write();
    if let Some(i) = g
        .iter()
        .rposition(|p| p.class == class && p.attribute == attribute && p.mock.is(mock))
    {
        g.remove(i);
        drop(g);
        session::exit();
    }
}

// ---------------------------------------------------------------------------
// MagicMock

/// `side_effect` / `return_value` (`None`: unset, `DEFAULT`) and the
/// recorded calls.
struct MockState {
    side_effect: Arg,
    return_value: Option<Arg>,
    calls: Vec<Args>,
    /// Child mocks (`m.close`) and assigned attributes (`m.__enter__ = ..`):
    /// the same child comes back on every read, as in Python.
    children: Vec<(String, Arg)>,
}

impl Default for MockState {
    fn default() -> Self {
        MockState {
            side_effect: Arg::None,
            return_value: None,
            calls: Vec::new(),
            children: Vec::new(),
        }
    }
}

/// `unittest.mock.MagicMock(side_effect=.., return_value=..)`: a callable
/// that records its calls. Reading an unknown attribute returns a child
/// mock, as in Python.
pub struct MagicMock {
    name: String,
    autospec: bool,
    state: RwLock<MockState>,
}

/// `unittest.mock.Mock`: the port does not distinguish it from `MagicMock`.
pub type Mock = MagicMock;

impl MagicMock {
    /// The child mock for *name*, created on first read.
    fn child(&self, name: &str) -> Arg {
        if let Some((_, c)) = self.state.read().children.iter().find(|(k, _)| k == name) {
            return c.clone();
        }
        let c = MagicMock::create(alloc::format!("{}.{}", self.name, name), false).into_arg();
        self.state
            .write()
            .children
            .push((name.to_string(), c.clone()));
        c
    }
    fn set_child(&self, name: &str, value: Arg) {
        let mut st = self.state.write();
        if let Some(slot) = st.children.iter_mut().find(|(k, _)| k == name) {
            slot.1 = value;
        } else {
            st.children.push((name.to_string(), value));
        }
    }
    fn create(name: String, autospec: bool) -> MagicMock {
        MagicMock {
            name,
            autospec,
            state: RwLock::new(MockState::default()),
        }
    }
    /// `MagicMock(**kwargs)` / `Mock(**kwargs)` -- a dynamic object (the
    /// test reads and assigns arbitrary attributes on it), so the value.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let m = MagicMock::create(
            a.get("name")
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| String::from("mock")),
            false,
        );
        m.configure(&a);
        Ok(m.into_arg())
    }
    fn configure(&self, a: &Args) {
        let mut st = self.state.write();
        if a.__contains__("side_effect").unwrap_or(false) {
            st.side_effect = a.get("side_effect");
        }
        if a.__contains__("return_value").unwrap_or(false) {
            st.return_value = Some(a.get("return_value"));
        }
    }
    fn autospec(&self) -> bool {
        self.autospec
    }
    /// `mock.call_count`
    pub fn call_count(&self) -> usize {
        self.state.read().calls.len()
    }
    /// `mock.reset_mock()`
    pub fn reset_mock(&self, _args: impl Into<Args>) {
        self.state.write().calls.clear();
    }
    /// `mock.assert_called()`
    pub fn assert_called(&self, _args: impl Into<Args>) -> Result<()> {
        if self.call_count() == 0 {
            return Err(LailaError::new(
                ExcKind::AssertionError,
                alloc::format!("Expected '{}' to have been called.", self.name),
            ));
        }
        Ok(())
    }
    /// `mock.assert_called_once()`
    pub fn assert_called_once(&self, _args: impl Into<Args>) -> Result<()> {
        let n = self.call_count();
        if n != 1 {
            return Err(LailaError::new(
                ExcKind::AssertionError,
                alloc::format!(
                    "Expected '{}' to have been called once. Called {} times.",
                    self.name,
                    n
                ),
            ));
        }
        Ok(())
    }
    /// `mock.assert_not_called()`
    pub fn assert_not_called(&self, _args: impl Into<Args>) -> Result<()> {
        let n = self.call_count();
        if n != 0 {
            return Err(LailaError::new(
                ExcKind::AssertionError,
                alloc::format!(
                    "Expected '{}' to not have been called. Called {} times.",
                    self.name,
                    n
                ),
            ));
        }
        Ok(())
    }
    /// `mock.call_args` -- the last call (`.args` / `.kwargs`, indexable
    /// as `(args, kwargs)`), `None` before any call.
    pub fn call_args(&self) -> Arg {
        match self.state.read().calls.last() {
            Some(a) => Arg::Object(Arc::new(CallArgs {
                args: Arg::Tuple(Arc::from(a.positional.clone())),
                kwargs: a.as_dict(),
            })),
            None => Arg::None,
        }
    }
    /// `mock.call_args_list` -- each call as a `(args, kwargs)` tuple.
    pub fn call_args_list(&self) -> Arg {
        self.state
            .read()
            .calls
            .iter()
            .map(|a| {
                Arg::Tuple(Arc::from(alloc::vec![
                    Arg::Tuple(Arc::from(a.positional.clone())),
                    a.as_dict(),
                ]))
            })
            .collect::<Vec<_>>()
            .into_arg()
    }
}

/// `unittest.mock.call` as recorded on `mock.call_args`: a 2-tuple view
/// with `.args` / `.kwargs`.
#[derive(Debug, Clone)]
pub struct CallArgs {
    pub args: Arg,
    pub kwargs: Arg,
}
impl PyObject for CallArgs {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_Call"
    }
    fn __repr__(&self) -> String {
        alloc::format!("call{}", self.args.__repr__())
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<CallArgs>()
            .is_some_and(|o| o.args == self.args && o.kwargs == self.kwargs)
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error("unhashable type: '_Call'"))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "args" => Ok(self.args.clone()),
            "kwargs" => Ok(self.kwargs.clone()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'_Call' object has no attribute '{other}'"
            ))),
        }
    }
    fn __len__(&self) -> Result<usize> {
        Ok(2)
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(alloc::vec![self.args.clone(), self.kwargs.clone()])
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        match key.as_i64() {
            Some(0) | Some(-2) => Ok(self.args.clone()),
            Some(1) | Some(-1) => Ok(self.kwargs.clone()),
            _ => Err(LailaError::new(
                ExcKind::IndexError,
                "tuple index out of range",
            )),
        }
    }
}

impl PyObject for MagicMock {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "MagicMock"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<MagicMock name='{}' id='{:#x}'>",
            self.name,
            self as *const MagicMock as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        core::ptr::eq(
            other
                .as_any()
                .downcast_ref::<MagicMock>()
                .map_or(core::ptr::null(), |m| m),
            self,
        )
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const MagicMock as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "side_effect" => Ok(self.state.read().side_effect.clone()),
            "return_value" => Ok(self.state.read().return_value.clone().unwrap_or(Arg::None)),
            "call_count" => Ok((self.call_count() as i64).into_arg()),
            "called" => Ok(Arg::Bool(self.call_count() > 0)),
            "call_args" => Ok(self.call_args()),
            "call_args_list" => Ok(self.call_args_list()),
            _ => Ok(self.child(name)),
        }
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        match name {
            "side_effect" => self.state.write().side_effect = value,
            "return_value" => self.state.write().return_value = Some(value),
            _ => self.set_child(name, value),
        }
        Ok(())
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "reset_mock" => {
                self.reset_mock(args);
                Ok(Arg::None)
            }
            "assert_called" => self.assert_called(args).map(|()| Arg::None),
            "assert_called_once" => self.assert_called_once(args).map(|()| Arg::None),
            "assert_not_called" => self.assert_not_called(args).map(|()| Arg::None),
            "configure_mock" => {
                self.configure(&args);
                Ok(Arg::None)
            }
            // `m.close()`: the child mock, called (and recorded).
            _ => self.child(name).__call__(args),
        }
    }
    /// `m[key]`: `m.__getitem__ = Mock(return_value=..)` assigned by a test.
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        self.child("__getitem__").__call__(Args::from(key.clone()))
    }
    fn __call__(&self, args: Args) -> Result<Arg> {
        let (side_effect, return_value) = {
            let mut st = self.state.write();
            st.calls.push(args.clone());
            (st.side_effect.clone(), st.return_value.clone())
        };
        match side_effect {
            Arg::None => {}
            Arg::Exception(e) => return Err((*e).clone()),
            Arg::Callable(_) | Arg::Object(_) => return side_effect.__call__(args),
            other => {
                // an iterable: each call yields the next value
                let mut st = self.state.write();
                let mut items = other.__iter__()?;
                if items.is_empty() {
                    return Err(LailaError::new(ExcKind::StopIteration, ""));
                }
                let first = items.remove(0);
                st.side_effect = items.into_arg();
                return Ok(first);
            }
        }
        Ok(return_value.unwrap_or_else(|| {
            MagicMock::create(alloc::format!("{}()", self.name), false).into_arg()
        }))
    }
    fn __bool__(&self) -> bool {
        true
    }
}

impl IntoArg for MagicMock {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

// ---------------------------------------------------------------------------
// patch.object

/// The module half of a dotted `patch("pkg.module.attr", ..)` target: the
/// registry key is the module path (`"os.path"`, `"laila.data.mongo.mongo"`).
pub struct ModuleTarget {
    path: String,
    /// Submodules `hasattr(module, name)` finds (`laila.macros.defaults`).
    submodules: &'static [&'static str],
}

impl PyObject for ModuleTarget {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "module"
    }
    fn __repr__(&self) -> String {
        alloc::format!("<module '{}'>", self.path)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<ModuleTarget>()
            .is_some_and(|m| m.path == self.path)
    }
    fn __hash__(&self) -> Result<i64> {
        crate::python::builtins::hash(self.path.as_str())
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        if let Some(v) = patched(&self.path, name) {
            return Ok(v);
        }
        if self.submodules.contains(&name) {
            return Ok(module_target(&alloc::format!("{}.{name}", self.path)));
        }
        Err(LailaError::attribute_error(alloc::format!(
            "module '{}' has no attribute '{name}'",
            self.path
        )))
    }
}

/// `patch("<module>.<attr>")`'s module as a patch target.
pub fn module_target(module: &str) -> Arg {
    Arg::Object(Arc::new(ModuleTarget {
        path: module.to_string(),
        submodules: &[],
    }))
}

/// A module value that knows its submodules (`hasattr(macros, "defaults")`).
pub fn module_target_with(module: &str, submodules: &'static [&'static str]) -> Arg {
    Arg::Object(Arc::new(ModuleTarget {
        path: module.to_string(),
        submodules,
    }))
}

/// The module attribute *attribute* of *module* when `patch("module.attribute", ..)`
/// is active, called with *args*; `None` otherwise. The port's twins of
/// patchable module functions (`os.path.exists`, `MongoClient`) consult this.
pub fn call_patched_module(module: &str, attribute: &str, args: Args) -> Option<Result<Arg>> {
    let mock = patched(module, attribute)?;
    Some(mock.__call__(args))
}

/// `unittest.mock.patch.object(target, attribute, new=DEFAULT, *, autospec=None, **kwargs)`
/// -- *target* is a laila class object; the patch installs a [`MagicMock`]
/// configured by the remaining keywords (`side_effect`, `return_value`),
/// or *new* when given.
pub fn object(args: impl Into<Args>) -> Result<ObjectPatch> {
    let a: Args = args.into();
    let target = a
        .positional
        .first()
        .cloned()
        .unwrap_or_else(|| a.get("target"));
    let attribute = a
        .positional
        .get(1)
        .cloned()
        .unwrap_or_else(|| a.get("attribute"));
    let class = match &target {
        Arg::Object(o) => match o.as_any().downcast_ref::<PyClass>() {
            Some(c) => c.name.to_string(),
            None => match o.as_any().downcast_ref::<ModuleTarget>() {
                Some(m) => m.path.clone(),
                // An *instance* target (`patch.object(policy.central.memory,
                // "_record")`): the port patches by type name, so the mock
                // stands in for every instance of that type while active.
                None => o.type_name().to_string(),
            },
        },
        other => {
            return Err(LailaError::type_error(alloc::format!(
                "Need a valid target to patch. You supplied: {}",
                other.__repr__()
            )))
        }
    };
    let attribute = match attribute {
        Arg::Str(s) => s.to_string(),
        other => {
            return Err(LailaError::type_error(alloc::format!(
                "attribute name must be a string, not {}",
                other.type_name()
            )))
        }
    };
    let new = a.positional.get(2).cloned().unwrap_or_else(|| a.get("new"));
    let autospec = a.get("autospec").__bool__();
    let mock = if new.is_none() {
        let m = MagicMock::create(alloc::format!("{class}.{attribute}"), autospec);
        let mut cfg = Args::new();
        for (k, v) in &a.keywords {
            if &**k == "side_effect" || &**k == "return_value" {
                cfg.push_keyword(k, v.clone());
            }
        }
        m.configure(&cfg);
        m.into_arg()
    } else {
        new
    };
    Ok(ObjectPatch(Arc::new(ObjectPatchState {
        class,
        attribute,
        mock,
    })))
}

struct ObjectPatchState {
    class: String,
    attribute: String,
    mock: Arg,
}

/// The object `patch.object(..)` returns: `start()` / `stop()`, or a
/// context manager whose guard stops the patch when dropped.
#[derive(Clone)]
pub struct ObjectPatch(Arc<ObjectPatchState>);

impl ObjectPatch {
    /// `patcher.start()` -- install the mock; returns it.
    pub fn start(&self, _args: impl Into<Args>) -> Result<Arg> {
        install(&self.0.class, &self.0.attribute, self.0.mock.clone());
        Ok(self.0.mock.clone())
    }
    /// `patcher.stop()` -- remove the mock (no-op when not started).
    pub fn stop(&self, _args: impl Into<Args>) -> Result<()> {
        uninstall(&self.0.class, &self.0.attribute, &self.0.mock);
        Ok(())
    }
    /// `with patch.object(..) as mock:` -- the guard stops the patch on drop.
    pub fn __enter__(&self) -> Result<ObjectPatchGuard> {
        self.start(Args::new())?;
        Ok(ObjectPatchGuard {
            patch: self.clone(),
        })
    }
}

/// Stops the patch when dropped (`__exit__`).
pub struct ObjectPatchGuard {
    patch: ObjectPatch,
}

impl Drop for ObjectPatchGuard {
    fn drop(&mut self) {
        let _ = self.patch.stop(Args::new());
    }
}

/// `with patch.object(..) as m:` -- the guard answers for the mock it bound.
impl ObjectPatchGuard {
    /// The installed mock.
    pub fn mock(&self) -> Arg {
        self.patch.0.mock.clone()
    }
    /// `m.assert_called()`
    pub fn assert_called(&self, args: impl Into<Args>) -> Result<()> {
        self.mock()
            .__call_method__("assert_called", args.into())
            .map(|_| ())
    }
    /// `m.assert_called_once()`
    pub fn assert_called_once(&self, args: impl Into<Args>) -> Result<()> {
        self.mock()
            .__call_method__("assert_called_once", args.into())
            .map(|_| ())
    }
    /// `m.assert_not_called()`
    pub fn assert_not_called(&self, args: impl Into<Args>) -> Result<()> {
        self.mock()
            .__call_method__("assert_not_called", args.into())
            .map(|_| ())
    }
    /// `m.reset_mock()`
    pub fn reset_mock(&self, args: impl Into<Args>) -> Result<()> {
        self.mock()
            .__call_method__("reset_mock", args.into())
            .map(|_| ())
    }
    /// `m.call_count`
    pub fn call_count(&self) -> Result<Arg> {
        self.mock().__getattr__("call_count")
    }
    /// `m.called`
    pub fn called(&self) -> Result<Arg> {
        self.mock().__getattr__("called")
    }
    /// `m.call_args_list`
    pub fn call_args_list(&self) -> Result<Arg> {
        self.mock().__getattr__("call_args_list")
    }
    /// `m.<attr>`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        self.mock().__getattr__(name)
    }
    /// `m.<attr> = value`
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        self.mock().__setattr__(name, value.into_arg())
    }
    /// `m(..)`
    pub fn __call__(&self, args: impl Into<Args>) -> Result<Arg> {
        self.mock().__call__(args.into())
    }
}

impl IntoArg for ObjectPatchGuard {
    fn into_arg(self) -> Arg {
        // `with patch.object(..) as m:` binds the mock, not the guard.
        let mock = self.patch.0.mock.clone();
        core::mem::forget(self);
        mock
    }
}

impl PyObject for ObjectPatch {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "_patch"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<unittest.mock._patch object: {}.{}>",
            self.0.class,
            self.0.attribute
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<ObjectPatch>()
            .is_some_and(|o| Arc::ptr_eq(&o.0, &self.0))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "start" => self.start(args),
            "stop" => self.stop(args).map(|()| Arg::None),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'_patch' object has no attribute '{name}'"
            ))),
        }
    }
}

impl PyArg for ObjectPatch {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IntoArg for ObjectPatch {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
