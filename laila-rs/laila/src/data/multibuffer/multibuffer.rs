//! `MultiBuffer` -- an integer-indexed ring of records with independent
//! read/write heads.
//!
//! Where a pool is a *map* keyed by entry `global_id`, a [`MultiBuffer`] is
//! a *list*: a fixed number of slots addressed by integer index, plus two
//! cursors that walk those slots modulo the capacity. It is the container
//! a microcontroller puts in front of a device that produces data faster
//! than it can be persisted -- the canonical example being a camera with
//! a double (or triple) frame buffer.
//!
//! Value contract (shared with pools)
//! ----------------------------------
//! - `buf[i] = value` stores a `Record`. A bare `Entry` is wrapped as
//!   `Record(entry=...)` on the way in, exactly as central memory does
//!   before a pool write; raw payloads are first lifted to a constant entry.
//! - `buf[i]` returns the bare `Entry` (`record.entry`). Raw slot contents
//!   that were never passed through `__setitem__` -- bytes deposited by
//!   hardware -- are wrapped into a constant entry on the way out. Empty
//!   slots read as `None`.
//!
//! Heads
//! -----
//! `write()` targets `_write_head` and `read()` targets `_read_head`; each
//! call advances its own head by one, wrapping at `capacity`. The heads
//! are independent so a producer can run ahead of the consumer by up to
//! `capacity` slots (the usual double-buffer hand-off), and both are only
//! ever mutated under the instance's atomic lock.
//!
//! Mapped mode
//! -----------
//! When the buffer stands in for memory that *something else* fills -- a
//! DMA engine, a camera driver, a shared `bytearray` -- construct it with
//! `mapped=True` and pass that memory as `slots`. The list is used as-is
//! (never copied) so external writes are visible immediately. In this
//! mode `write()` does **not** go through `__setitem__`: it only advances
//! the write head (optionally depositing a raw value directly into the
//! slot first). `read()` is unchanged -- it still goes through
//! `__getitem__`, which is where the raw bytes at the read head become an
//! `Entry`.
//!
//! Typical microcontroller loop:
//!
//! ```text
//! frames = MultiBuffer(slots=dma_slots, mapped=True)
//! entry = frames.read()        # raw bytes at _read_head -> Entry
//! laila.memorize(entry)        # Record into the policy's pool
//! ```
//!
//! Rust shape: [`MultiBuffer`] is the class (constructor, `isinstance`,
//! `model_fields`), [`MultiBufferHandle`] the instance handle.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::_compat::pydantic::{
    validation_error, LazyClass, ModelBase, ModelClass, ModelHooks, ModelInstance,
};
use crate::atomic::definitions::{HasIdentity, _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT};
use crate::basics::definitions::cli_capable::CLIExempt;
use crate::entry::Entry;
use crate::macros::strings::_MULTI_BUFFER_SCOPE;
use crate::policy::central::memory::record::record::Record;
use crate::python::builtins::IsInstance;
use crate::python::object::{HasAttrs, PyAttrs, PyObject};
use crate::python::sync::RLock;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

use crate::data::schema::base::Held;
use crate::data::schema::data_container::{
    scopes_private_attr, _LAILA_IDENTIFIABLE_DATA_CONTAINER,
};

/// `_UNSET: Any = object()` -- the "no value" sentinel of `write()`.
const UNSET: &str = "\u{0}MultiBuffer._UNSET\u{0}";
fn is_unset(a: &Arg) -> bool {
    matches!(a, Arg::Str(s) if &**s == UNSET)
}
/// `_UNSET` as an `Arg` (the default of `write(value=_UNSET)`).
#[allow(non_snake_case)]
pub fn _UNSET() -> Arg {
    Arg::Str(UNSET.into())
}

struct Hooks;

impl ModelHooks for Hooks {
    fn model_post_init(
        &self,
        inst: &ModelInstance,
        _st: &crate::_compat::pydantic::InitState,
    ) -> Result<()> {
        // ``capacity: int = Field(default=2, ge=1)``
        let capacity = inst.field("capacity").unwrap_or(Arg::Int(2));
        if capacity.as_i64().unwrap_or(0) < 1 {
            return Err(validation_error(
                "MultiBuffer",
                "capacity",
                "Input should be greater than or equal to 1",
                "greater_than_equal",
                &capacity,
            ));
        }
        let slots = inst.field("slots").unwrap_or(Arg::None);
        if slots.is_none() {
            let n = capacity.as_i64().unwrap_or(2) as usize;
            inst.set_field("slots", alloc::vec![Arg::None; n].into_arg());
            return Ok(());
        }
        let n = match slots.__len__() {
            Ok(n) => n,
            Err(_) => {
                return Err(LailaError::type_error(
                    "MultiBuffer.slots must be a sized, indexable sequence",
                ))
            }
        };
        if n == 0 {
            return Err(LailaError::value_error(
                "MultiBuffer.slots must contain at least one slot",
            ));
        }
        inst.set_field("capacity", Arg::Int(n as i64));
        Ok(())
    }
    fn property_getters(&self) -> &'static [&'static str] {
        &["read_head", "write_head"]
    }
    fn method_names(&self) -> &'static [&'static str] {
        &[
            "__getitem__",
            "__setitem__",
            "__len__",
            "empty",
            "write",
            "read",
            "_index",
        ]
    }
}

static HOOKS: Hooks = Hooks;

static CLASS: LazyClass = LazyClass::new(|| {
    let mut capacity = Args::new();
    capacity.push_keyword("default", 2);
    capacity.push_keyword("ge", 1);
    let mut mapped = Args::new();
    mapped.push_keyword("default", false);
    let mut slots = Args::new();
    slots.push_keyword("default", Arg::None);
    let ann = |mut f: crate::_compat::pydantic::FieldInfo, a: &str| {
        f.annotation = a.to_string();
        f
    };
    ModelClass {
        name: "MultiBuffer",
        module: "laila.data.multibuffer.multibuffer",
        bases: alloc::vec![_LAILA_IDENTIFIABLE_DATA_CONTAINER::__class__()],
        fields: alloc::vec![
            (
                String::from("capacity"),
                ann(
                    crate::_compat::pydantic::Field(capacity).expect("Field"),
                    "int"
                )
            ),
            (
                String::from("mapped"),
                ann(
                    crate::_compat::pydantic::Field(mapped).expect("Field"),
                    "bool"
                )
            ),
            // Typed as Any (not list[Any]) so pydantic hands the caller's
            // object through untouched -- copying would break mapped mode.
            (
                String::from("slots"),
                ann(CLIExempt(slots).expect("CLIExempt"), "Any")
            ),
        ],
        private_attrs: alloc::vec![
            scopes_private_attr(_MULTI_BUFFER_SCOPE),
            (
                String::from("_read_head"),
                crate::_compat::pydantic::PrivateAttrInfo {
                    default: Some(Arg::Int(0)),
                    default_factory: None,
                }
            ),
            (
                String::from("_write_head"),
                crate::_compat::pydantic::PrivateAttrInfo {
                    default: Some(Arg::Int(0)),
                    default_factory: None,
                }
            ),
        ],
        class_vars: Vec::new(),
        hooks: Some(&HOOKS),
    }
});

/// Fixed-capacity ring of records with separate read and write heads.
///
/// Attributes
/// ----------
/// `capacity : int` -- number of slots. Defaults to `2` (a double buffer).
/// When `slots` is supplied explicitly this is overwritten with
/// `len(slots)`.
/// `mapped : bool` -- `True` when `slots` is externally-owned memory that
/// is filled outside laila (see module docstring). Changes only what
/// `write` does; reads always go through the item protocol.
/// `slots : list-like` -- the backing storage. Sized to `capacity` with
/// `None` when not provided. When provided it is used by reference, so a
/// caller (or hardware) mutating the same object is observed by the
/// buffer. Must support `len`, integer `__getitem__` and `__setitem__`.
#[derive(Clone, Copy, Debug, Default)]
pub struct MultiBuffer;

impl ModelBase for MultiBuffer {
    fn __class__() -> &'static ModelClass {
        MultiBuffer::__class__()
    }
}

impl MultiBuffer {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `MultiBuffer(**kwargs)`
    #[allow(clippy::new_ret_no_self)]
    pub fn new(args: impl Into<Args>) -> Result<MultiBufferHandle> {
        crate::entry::__init__();
        let inst = ModelInstance::construct(Self::__class__(), args.into())?;
        Ok(MultiBufferHandle(Arc::new(MultiBufferInner {
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
        Arg::Str("MultiBuffer".into())
    }
}

impl PyArg for MultiBuffer {
    fn to_arg(&self) -> Arg {
        Self::__class__().as_pyclass().into_arg()
    }
}
impl IntoArg for MultiBuffer {
    fn into_arg(self) -> Arg {
        Self::__class__().as_pyclass().into_arg()
    }
}
impl crate::python::builtins::CallValue for MultiBuffer {
    fn call_value(&self, args: Args) -> Result<Arg> {
        Self::new(args).map(IntoArg::into_arg)
    }
}
impl IsInstance for MultiBuffer {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<MultiBufferHandle>())
    }
}

struct MultiBufferInner {
    inst: Arg,
    __dict__: PyAttrs,
}

/// A `MultiBuffer` instance (handle; clones alias one buffer).
#[derive(Clone)]
pub struct MultiBufferHandle(Arc<MultiBufferInner>);

impl MultiBufferHandle {
    fn inst(&self) -> &ModelInstance {
        ModelInstance::downcast(&self.0.inst).expect("multibuffer model instance")
    }
    fn local_lock(&self) -> RLock {
        crate::atomic::definitions::locally_atomic_object::ensure_local_lock(&self.0.__dict__)
    }
    fn held(&self) -> Held {
        Held::acquire(&self.local_lock())
    }
    /// `self.capacity`
    pub fn capacity(&self) -> i64 {
        self.inst()
            .field("capacity")
            .and_then(|c| c.as_i64())
            .unwrap_or(2)
    }
    /// `self.mapped`
    pub fn mapped(&self) -> bool {
        self.inst()
            .field("mapped")
            .map(|m| m.__bool__())
            .unwrap_or(false)
    }
    /// `self.slots` -- the backing sequence (shared, never copied).
    pub fn slots(&self) -> Arg {
        self.inst().field("slots").unwrap_or(Arg::None)
    }
    /// `self._scopes`
    pub fn _scopes(&self) -> Arg {
        self.inst().private("_scopes").unwrap_or(Arg::None)
    }
    /// `self.scopes` -- the `scopes` property of a laila pool (a copy of
    /// `_scopes`).
    pub fn scopes(&self) -> Arg {
        self._scopes()
    }
    fn head(&self, name: &str) -> i64 {
        self.inst()
            .private(name)
            .and_then(|h| h.as_i64())
            .unwrap_or(0)
    }
    /// Index of the slot the next `read` will return.
    pub fn read_head(&self) -> i64 {
        self.head("_read_head")
    }
    /// Index of the slot the next `write` will fill.
    pub fn write_head(&self) -> i64 {
        self.head("_write_head")
    }
    /// `self._read_head`
    pub fn _read_head(&self) -> i64 {
        self.read_head()
    }
    /// `self._write_head`
    pub fn _write_head(&self) -> i64 {
        self.write_head()
    }
    /// `key % capacity` for an integer key (`TypeError` otherwise, as
    /// `MultiBuffer indices must be integers, not <type>`).
    pub fn _index(&self, key: impl IntoArg) -> Result<i64> {
        let key = key.into_arg();
        let i = match &key {
            Arg::Int(i) => *i,
            _ => {
                return Err(LailaError::type_error(alloc::format!(
                    "MultiBuffer indices must be integers, not {}",
                    key.type_name()
                )))
            }
        };
        Ok(i.rem_euclid(self.capacity()))
    }
    /// `len(buf)` -- the capacity.
    pub fn __len__(&self) -> Result<usize> {
        Ok(self.capacity() as usize)
    }

    // -------- Item protocol --------

    /// The `Entry` at slot *key* (modulo capacity), or `None`.
    ///
    /// A stored `Record` yields its `entry`. Raw contents (anything that
    /// is neither a record nor an entry -- e.g. bytes written by mapped
    /// hardware) are wrapped into a constant entry.
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        let value = {
            let _held = self.held();
            let idx = self._index(key)?;
            self.slots().__getitem__(Arg::Int(idx))?
        };
        if value.is_none() {
            return Ok(Arg::None);
        }
        if Record::isinstance(&value) {
            return value.__getattr__("entry");
        }
        if let Arg::Entry(_) = &value {
            return Ok(value);
        }
        let mut a = Args::new();
        a.push_keyword("data", value);
        Ok(Arg::Entry(Entry::constant(a)?))
    }

    /// Store a `Record` at slot *key* (modulo capacity).
    ///
    /// Accepts a `Record` (stored as-is), an `Entry` (wrapped in a fresh
    /// record), a raw payload (lifted to a constant entry, then wrapped),
    /// or `None` to clear the slot.
    pub fn __setitem__(&self, key: impl IntoArg, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        let record = if value.is_none() {
            Arg::None
        } else if Record::isinstance(&value) {
            value
        } else if let Arg::Entry(_) = &value {
            let mut a = Args::new();
            a.push_keyword("entry", value);
            Record::new(a)?
        } else {
            let mut c = Args::new();
            c.push_keyword("data", value);
            let mut a = Args::new();
            a.push_keyword("entry", Arg::Entry(Entry::constant(c)?));
            Record::new(a)?
        };
        let _held = self.held();
        let idx = self._index(key)?;
        self.slots().__setitem__(Arg::Int(idx), record)
    }

    /// Clear every slot and rewind both heads to slot `0`.
    pub fn empty(&self, _args: impl Into<Args>) -> Result<()> {
        let _held = self.held();
        let slots = self.slots();
        for i in 0..self.capacity() {
            slots.__setitem__(Arg::Int(i), Arg::None)?;
        }
        self.inst().set_private("_read_head", Arg::Int(0));
        self.inst().set_private("_write_head", Arg::Int(0));
        Ok(())
    }

    // -------- Verbs --------

    /// Fill the slot at the write head, then advance the head; returns
    /// the index of the slot that was written.
    ///
    /// Unmapped: `self[write_head] = value` (so the slot ends up holding a
    /// `Record`); *value* is required. Mapped: `__setitem__` is bypassed.
    /// If *value* is given it is deposited raw into the slot; if omitted
    /// the slot is assumed to have been filled externally and only the
    /// head moves.
    #[pysig("write(value=_UNSET())")]
    pub fn write(&self, args: impl Into<Args>) -> Result<i64> {
        let _held = self.held();
        let idx = self.write_head();
        if self.mapped() {
            if !is_unset(&value) {
                self.slots().__setitem__(Arg::Int(idx), value)?;
            }
        } else {
            if is_unset(&value) {
                return Err(LailaError::type_error(
                    "MultiBuffer.write() requires a value when not mapped",
                ));
            }
            self.__setitem__(Arg::Int(idx), value)?;
        }
        self.inst()
            .set_private("_write_head", Arg::Int((idx + 1) % self.capacity()));
        Ok(idx)
    }

    /// The `Entry` at the read head, then advance the head.
    ///
    /// Goes through `__getitem__`, so in mapped mode this is where raw
    /// accumulated bytes are wrapped into an entry. An empty slot yields
    /// `None` (the head still advances).
    pub fn read(&self, _args: impl Into<Args>) -> Result<Arg> {
        let _held = self.held();
        let idx = self.read_head();
        let entry = self.__getitem__(Arg::Int(idx))?;
        self.inst()
            .set_private("_read_head", Arg::Int((idx + 1) % self.capacity()));
        Ok(entry)
    }

    /// `buf.<name>` for the dynamic layer.
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "read_head" => Ok(Arg::Int(self.read_head())),
            "write_head" => Ok(Arg::Int(self.write_head())),
            "_scopes" => Ok(self._scopes()),
            _ => {
                if let Some(v) = self.0.__dict__.get(name) {
                    return Ok(v);
                }
                PyObject::__getattr__(self.inst(), name)
            }
        }
    }
    /// `buf.<name> = value`
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        match name {
            "read_head" | "write_head" => Err(LailaError::attribute_error(alloc::format!(
                "property '{name}' of 'MultiBuffer' object has no setter"
            ))),
            _ => PyObject::__setattr__(self.inst(), name, value.into_arg()),
        }
    }
    /// Python `is`.
    pub fn is(&self, other: &MultiBufferHandle) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// `self.uuid`
    pub fn uuid(&self) -> String {
        HasIdentity::uuid(self)
    }
    /// `self.global_id`
    pub fn global_id(&self) -> String {
        HasIdentity::global_id(self)
    }
}

impl HasAttrs for MultiBufferHandle {
    fn __attrs__(&self) -> &PyAttrs {
        &self.0.__dict__
    }
    fn __class_name__(&self) -> &'static str {
        "MultiBuffer"
    }
}
impl HasIdentity for MultiBufferHandle {
    fn global_id(&self) -> String {
        HasIdentity::global_id(self.inst())
    }
    fn uuid(&self) -> String {
        HasIdentity::uuid(self.inst())
    }
}
impl _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT for MultiBufferHandle {}

impl core::fmt::Debug for MultiBufferHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&PyObject::__repr__(self))
    }
}

impl PyObject for MultiBufferHandle {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    /// `a is b`: identity of the shared handle, not of the boxing.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<MultiBufferHandle>()
            .is_some_and(|o| self.is(o))
    }
    fn type_name(&self) -> &'static str {
        "MultiBuffer"
    }
    fn __repr__(&self) -> String {
        HasIdentity::global_id(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<MultiBufferHandle>()
            .is_some_and(|o| HasIdentity::global_id(o) == HasIdentity::global_id(self))
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(HasIdentity::global_id(self).into()).__hash__()
    }
    fn __len__(&self) -> Result<usize> {
        MultiBufferHandle::__len__(self)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        MultiBufferHandle::__getattr__(self, name)
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        MultiBufferHandle::__setattr__(self, name, value)
    }
    fn __getitem__(&self, key: &Arg) -> Result<Arg> {
        MultiBufferHandle::__getitem__(self, key.clone())
    }
    fn __setitem__(&self, key: &Arg, value: Arg) -> Result<()> {
        MultiBufferHandle::__setitem__(self, key.clone(), value)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = || args.positional.first().cloned().unwrap_or(Arg::None);
        match name {
            "write" => self.write(args).map(Arg::Int),
            "read" => self.read(args),
            "empty" => self.empty(args).map(|()| Arg::None),
            "__getitem__" => MultiBufferHandle::__getitem__(self, first()),
            "__setitem__" => MultiBufferHandle::__setitem__(
                self,
                first(),
                args.positional.get(1).cloned().unwrap_or(Arg::None),
            )
            .map(|()| Arg::None),
            "__len__" => MultiBufferHandle::__len__(self).map(|n| Arg::Int(n as i64)),
            "_index" => self._index(first()).map(Arg::Int),
            "atomic" => crate::atomic::definitions::locally_atomic_object::DynAtomicContext::new(
                self.local_lock(),
                "MultiBuffer",
                args,
            )
            .map(IntoArg::into_arg),
            _ => PyObject::__call_method__(self.inst(), name, args),
        }
    }
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(self.inst().class.as_pyclass())
    }
    fn __atomic_lock__(&self) -> Option<RLock> {
        Some(self.local_lock())
    }
    fn __dir__(&self) -> Vec<String> {
        let mut out = PyObject::__dir__(self.inst());
        out.push("_scopes".to_string());
        out
    }
}
impl crate::python::builtins::PyLen for MultiBufferHandle {
    fn __len__(&self) -> Result<usize> {
        MultiBufferHandle::__len__(self)
    }
}
impl IntoArg for MultiBufferHandle {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for MultiBufferHandle {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
