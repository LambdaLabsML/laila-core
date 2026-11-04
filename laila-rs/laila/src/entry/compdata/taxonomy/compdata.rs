//! Base [`ComputationalData`] class -- type-dispatched payload wrapper.
//!
//! Every `Entry` payload is wrapped in a `ComputationalData` (or one of its
//! registered subclasses) so the system has a uniform handle on the data
//! regardless of underlying Python type. The wrapper provides:
//!
//! - a `data` attribute exposing the unwrapped value,
//! - a swappable `serializer` (`PickleSerializer` by default) used by
//!   pool-side `serialize()` to produce bytes plus an inverse-decode source
//!   string,
//! - a `__len__` / `shape` / `__copy__` / `__deepcopy__` API that subclasses
//!   fill in for their concrete payload type.
//!
//! # Type dispatch
//! Subclasses register themselves with [`register_cdtype`] to claim
//! ownership of one or more Python types (`type(data).__name__` here, since
//! `Arg` has no subclasses). `ComputationalData(data)` then looks up the
//! payload's type in [`TYPE_TO_WRAPPER`] and instantiates the matching
//! subclass. A generic `CD_generic` fallback wraps anything unknown.
//!
//! Rust shape: the "subclasses" are the [`CDKind`] variants; each
//! `cd_*.rs` module holds the per-kind behaviour and a unit struct
//! (`CD_dict`, `CD_list`, ...) whose `new` is the direct-subclass constructor
//! that bypasses dispatch, exactly like `CD_dict(value)` in Python.

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;

use crate::python::builtins::IsInstance;
use crate::python::object::PyObject;
use crate::python::sync::{Mutex, RwLock};
use crate::python::{pysig, Arg, Args, ExcKind, IntoArg, LailaError, PyArg, Result};

use super::super::transformation::serialization::PickleSerializer;
use super::super::transformation::Transformation;
use super::{cd_dict, cd_list, cd_numpy, cd_object, cd_torch};

/// The registered `ComputationalData` subclasses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CDKind {
    /// `CD_generic` (pickle)
    Generic,
    /// `CD_dict` (msgpack)
    Dict,
    /// `CD_list` (msgpack for lists, pickle for tuples)
    List,
    /// `CD_numpyarray` (`.npy`)
    NumpyArray,
    /// `CD_torchtensor` (`torch.save`) -- never selected on this host.
    TorchTensor,
}

impl CDKind {
    /// `cls.__name__`
    pub fn class_name(self) -> &'static str {
        match self {
            CDKind::Generic => "CD_generic",
            CDKind::Dict => "CD_dict",
            CDKind::List => "CD_list",
            CDKind::NumpyArray => "CD_numpyarray",
            CDKind::TorchTensor => "CD_torchtensor",
        }
    }
}

// ---------------------------------------------------------------------
// Mapping: python type ➜ wrapper subclass
// ---------------------------------------------------------------------

static TYPE_TO_WRAPPER_STATE: RwLock<Vec<(String, CDKind)>> = RwLock::new(Vec::new());
static SEEDED: Mutex<bool> = Mutex::new(false);

fn ensure_seeded() {
    let mut g = SEEDED.lock();
    if *g {
        return;
    }
    *g = true;
    // ``taxonomy/__init__.py``: import every concrete subclass.
    register_cdtype(cd_dict::PAYLOAD_TYPES, CDKind::Dict);
    register_cdtype(cd_list::PAYLOAD_TYPES, CDKind::List);
    register_cdtype(cd_numpy::PAYLOAD_TYPES, CDKind::NumpyArray);
    register_cdtype(cd_object::PAYLOAD_TYPES, CDKind::Generic);
    // ``cd_torch`` registers nothing: torch is not importable here.
    if cd_torch::_HAVE_TORCH {
        register_cdtype(cd_torch::PAYLOAD_TYPES, CDKind::TorchTensor);
    }
}

/// `TYPE_TO_WRAPPER` -- `(type name, wrapper)` pairs in registration order.
pub fn TYPE_TO_WRAPPER() -> Vec<(String, CDKind)> {
    ensure_seeded();
    TYPE_TO_WRAPPER_STATE.read().clone()
}

/// `@register_cdtype(*payload_types)`: record `TYPE_TO_WRAPPER[t] = cls`
/// for every type name in *payload_types*.
pub fn register_cdtype(payload_types: &[&str], cls: CDKind) {
    let mut g = TYPE_TO_WRAPPER_STATE.write();
    for t in payload_types {
        if let Some(slot) = g.iter_mut().find(|(n, _)| n == t) {
            slot.1 = cls;
        } else {
            g.push((t.to_string(), cls));
        }
    }
}

/// Raise `TypeError` for payloads that have no meaningful length.
///
/// Used by scalar wrapper subclasses (single int, single float, etc.) to
/// keep `__len__` semantically honest -- `len(scalar)` should fail loudly
/// rather than silently return 1.
pub fn _scalar_len() -> LailaError {
    LailaError::type_error("Length undefined for scalars / objects without __len__")
}

/// The dispatch of `ComputationalData.__new__`: exact type match, MRO walk
/// (`object` is every type's ancestor), `CD_generic` fallback.
pub fn wrapper_for(data: &Arg) -> CDKind {
    let table = TYPE_TO_WRAPPER();
    let tn = data.type_name();
    // Exact type match
    if let Some((_, k)) = table.iter().find(|(n, _)| n == tn) {
        return *k;
    }
    // Walk MRO for ancestor matches (`OrderedDict` -> `dict`); every type
    // derives from ``object``.
    if let Arg::Object(o) = data {
        for base in o.__bases__() {
            if let Some((_, k)) = table.iter().find(|(n, _)| n == base) {
                return *k;
            }
        }
    }
    if let Some((_, k)) = table.iter().find(|(n, _)| n == "object") {
        return *k;
    }
    // Fallback
    CDKind::Generic
}

// ---------------------------------------------------------------------
// Factory / Superclass
// ---------------------------------------------------------------------

/// Generic computational-data wrapper with dynamic subclass dispatch and
/// serializer-based transformation.
///
/// Direct instantiation (`ComputationalData(value)`) routes through
/// `__new__` to the registered subclass for `type(value)`, falling back to
/// a generic object wrapper. Subclass instantiation (`CD_numpyarray(value)`)
/// bypasses dispatch -- the chosen subclass is used directly.
///
/// A shared handle (pydantic models are reference objects): every clone
/// is the same Python object (`v._payload is cd`), and `copy.copy` /
/// `copy.deepcopy` are the explicit ways to get a new one.
#[derive(Clone)]
pub struct ComputationalData(Arc<CDInner>);

/// The fields of a `ComputationalData`.
pub struct CDInner {
    kind: CDKind,
    data: Arg,
    // The serializer is created lazily on first access. Subclasses override
    // ``_SERIALIZER_CLS``.
    _serializer: RwLock<Option<Transformation>>,
}

impl core::ops::Deref for ComputationalData {
    type Target = CDInner;
    fn deref(&self) -> &CDInner {
        &self.0
    }
}

impl ComputationalData {
    /// Python `is`.
    pub fn is(&self, other: &ComputationalData) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl ComputationalData {
    /// `ComputationalData(value)` / `ComputationalData(data=value)`:
    /// dispatch to the registered subclass matching the payload type.
    ///
    /// Resolution order: exact `TYPE_TO_WRAPPER` match, MRO walk, `CD_generic`.
    ///
    /// `TypeError` if no payload was provided, if more than one positional
    /// argument is given, or if the payload is given both positionally and
    /// as `data=`.
    pub fn new(args: impl Into<Args>) -> Result<ComputationalData> {
        let a: Args = args.into();
        // ``__new__``: extract payload
        let data = match a.positional.first() {
            Some(d) => d.clone(),
            None => a.get("data"),
        };
        if data.is_none() {
            return Err(LailaError::type_error("Missing required argument 'data'"));
        }
        let chosen = wrapper_for(&data);
        Self::__init__(chosen, a)
    }

    /// `__init__(*args, **kwargs)`: construct from either a positional
    /// payload or `data=` kwarg (the subclass is already chosen).
    pub(crate) fn __init__(kind: CDKind, a: Args) -> Result<ComputationalData> {
        let mut data = a.get("data");
        if !a.positional.is_empty() {
            if a.positional.len() > 1 {
                return Err(LailaError::type_error(
                    "At most one positional argument (the payload)",
                ));
            }
            if a.__contains__("data")? {
                return Err(LailaError::type_error(
                    "Payload given both positionally and as 'data='",
                ));
            }
            data = a.positional[0].clone();
        }
        Self::validate(kind, &data)?;
        Ok(ComputationalData(Arc::new(CDInner {
            kind,
            data,
            _serializer: RwLock::new(None),
        })))
    }

    /// pydantic field validation of the concrete subclass (`data: InstanceOf[dict]`, ...).
    fn validate(kind: CDKind, data: &Arg) -> Result<()> {
        match kind {
            CDKind::Generic => Ok(()),
            CDKind::Dict => cd_dict::validate(data),
            CDKind::List => cd_list::validate(data),
            CDKind::NumpyArray => cd_numpy::validate(data),
            CDKind::TorchTensor => Err(cd_torch::no_torch()),
        }
    }

    /// Typed constructor bypassing dispatch (`CD_x(value)`).
    pub(crate) fn of_kind(kind: CDKind, data: Arg) -> Result<ComputationalData> {
        Self::__init__(kind, Args::from(data))
    }

    /// The concrete subclass.
    pub fn kind(&self) -> CDKind {
        self.kind
    }

    /// `type(self).__name__`
    pub fn __class_name__(&self) -> &'static str {
        self.kind.class_name()
    }

    /// `self.data` -- the unwrapped payload.
    pub fn data(&self) -> Arg {
        self.data.clone()
    }

    /// `type(self)._SERIALIZER_CLS()`
    fn default_serializer(&self) -> Result<Transformation> {
        match self.kind {
            CDKind::Generic => cd_object::default_serializer(),
            CDKind::Dict => cd_dict::default_serializer(),
            CDKind::List => cd_list::default_serializer(&self.data),
            CDKind::NumpyArray => cd_numpy::default_serializer(),
            CDKind::TorchTensor => cd_torch::default_serializer(),
        }
    }

    /// Return the instance serializer, constructing the class default on first use.
    pub fn _ensure_serializer(&self) -> Result<Transformation> {
        if let Some(s) = &*self._serializer.read() {
            return Ok(s.clone());
        }
        let serializer = self.default_serializer()?;
        *self._serializer.write() = Some(serializer.clone());
        Ok(serializer)
    }

    /// The serializer used by `serialize()`.
    ///
    /// Defaults to `PickleSerializer`. Subclasses with bespoke serializers
    /// (`CD_numpyarray` -> NumPy npy serializer, `CD_torchtensor` ->
    /// torch.save serializer) override `_SERIALIZER_CLS`.
    pub fn serializer(&self) -> Result<Transformation> {
        self._ensure_serializer()
    }

    /// `cd._serializer`: the cached serializer (`None` until `serializer`
    /// is first read).
    pub fn _serializer(&self) -> Arg {
        match self._serializer.read().clone() {
            Some(s) => s.into_arg(),
            None => Arg::None,
        }
    }

    /// `type(cd)` / `cd.__class__`: the dispatched subclass (`CD_numpyarray`,
    /// `CD_list`, `CD_dict`, `CD_generic`, `CD_torchtensor`).
    pub fn __class__(&self) -> crate::python::PyClass {
        crate::python::PyClass::named(self.kind.class_name(), "laila.entry.compdata.taxonomy")
    }

    /// `cd.<name>` / `cd.<name> = value` written against the typed object
    /// (the `laila::python!` attribute protocol).
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        PyObject::__getattr__(self, name)
    }
    pub fn __setattr__(&self, name: &str, value: impl IntoArg) -> Result<()> {
        PyObject::__setattr__(self, name, value.into_arg())
    }

    /// Replace the serializer. The new value must be an instance of the
    /// subclass's serializer type (`TypeError` otherwise).
    pub fn set_serializer(&self, value: impl IntoArg) -> Result<()> {
        let value = value.into_arg();
        let tr = Transformation::from_arg(&value);
        let accepted = match self.kind {
            CDKind::Generic => cd_object::accepts_serializer(tr.as_ref()),
            CDKind::Dict => cd_dict::accepts_serializer(tr.as_ref()),
            CDKind::List => cd_list::accepts_serializer(tr.as_ref()),
            CDKind::NumpyArray => cd_numpy::accepts_serializer(tr.as_ref()),
            CDKind::TorchTensor => cd_torch::accepts_serializer(tr.as_ref()),
        };
        match tr {
            Some(tr) if accepted => {
                *self._serializer.write() = Some(tr);
                Ok(())
            }
            _ => Err(LailaError::type_error(alloc::format!(
                "serializer must be a {}, got {}",
                self.serializer_expectation(),
                value.type_name()
            ))),
        }
    }

    fn serializer_expectation(&self) -> &'static str {
        match self.kind {
            CDKind::Generic => "PickleSerializer",
            CDKind::Dict => "MsgpackSerializer",
            CDKind::List => "MsgpackSerializer or PickleSerializer",
            CDKind::NumpyArray => "NumpySerializer",
            CDKind::TorchTensor => "TorchSerializer",
        }
    }

    /// Serialize `data` to bytes with the current serializer.
    ///
    /// Returns `(serialized_bytes, backward_code)`. The backward-code string
    /// defines a one-argument Python callable that, applied to
    /// `serialized_bytes`, recovers the original `data` value. `Entry.serialize`
    /// then bundles this code into the inverse-transformation chain so the
    /// read-side can rebuild without knowing the specific serializer that was
    /// used at write time.
    ///
    /// Format-specific serializers (msgpack for dicts/lists, ...) reject
    /// values their wire format cannot express -- numpy arrays nested in a
    /// dict, integers beyond 64 bits, tuples. Rather than failing the
    /// memorize, such payloads fall back to the universal `PickleSerializer`.
    #[pysig("serialize()")]
    pub fn serialize(&self, args: impl Into<Args>) -> Result<(Arg, String)> {
        let serializer = self.serializer()?;
        match serializer.0.forward(&self.data) {
            Ok(out) => Ok((out, serializer.backward_code())),
            Err(e)
                if e.kind.is_subclass_of(ExcKind::TypeError)
                    || e.kind.is_subclass_of(ExcKind::OverflowError)
                    || e.kind.is_subclass_of(ExcKind::ValueError) =>
            {
                if serializer.downcast_ref::<PickleSerializer>().is_some() {
                    return Err(e);
                }
                let fallback = Transformation::new(PickleSerializer::new(Args::new())?);
                Ok((fallback.0.forward(&self.data)?, fallback.backward_code()))
            }
            Err(e) => Err(e),
        }
    }

    // --- Descriptor behavior -------------------------------------------

    /// Descriptor protocol: class-level access yields the wrapper
    /// (`obj=None`), instance-level access yields the unwrapped payload.
    pub fn __get__(&self, obj: &Arg) -> Arg {
        if obj.is_none() {
            self.clone().into_arg()
        } else {
            self.data()
        }
    }

    /// Index into the underlying payload.
    pub fn __getitem__(&self, index: impl IntoArg) -> Result<Arg> {
        self.data.__getitem__(index)
    }

    /// Developer-friendly representation (subclass-specific).
    pub fn __repr__(&self) -> String {
        match self.kind {
            CDKind::Generic => cd_object::repr(&self.data),
            CDKind::Dict => cd_dict::repr(&self.data),
            CDKind::List => cd_list::repr(&self.data),
            CDKind::NumpyArray => cd_numpy::repr(&self.data),
            CDKind::TorchTensor => cd_torch::repr(&self.data),
        }
    }

    /// `str(self)` is `repr(self)`.
    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    /// Length of the payload (subclass-specific).
    pub fn __len__(&self) -> Result<usize> {
        match self.kind {
            CDKind::Generic => cd_object::len(&self.data),
            CDKind::Dict => cd_dict::len(&self.data),
            CDKind::List => cd_list::len(&self.data),
            CDKind::NumpyArray => cd_numpy::len(&self.data),
            CDKind::TorchTensor => cd_torch::len(&self.data),
        }
    }

    /// Shape of the payload (subclass-specific).
    pub fn shape(&self) -> Result<Arg> {
        match self.kind {
            CDKind::Generic => cd_object::shape(&self.data),
            CDKind::Dict => cd_dict::shape(&self.data),
            CDKind::List => cd_list::shape(&self.data),
            CDKind::NumpyArray => cd_numpy::shape(&self.data),
            CDKind::TorchTensor => cd_torch::shape(&self.data),
        }
    }

    /// Shallow copy (`copy.copy`).
    pub fn __copy__(&self) -> Result<ComputationalData> {
        let copied = match self.kind {
            CDKind::Generic => cd_object::copy(&self.data)?,
            CDKind::Dict => cd_dict::copy(&self.data)?,
            CDKind::List => cd_list::copy(&self.data)?,
            CDKind::NumpyArray => cd_numpy::copy(&self.data)?,
            CDKind::TorchTensor => cd_torch::copy(&self.data)?,
        };
        Self::of_kind(self.kind, copied)
    }

    /// Deep copy (`copy.deepcopy`).
    pub fn __deepcopy__(&self) -> Result<ComputationalData> {
        let copied = match self.kind {
            CDKind::Generic => cd_object::deepcopy(&self.data)?,
            CDKind::Dict => cd_dict::deepcopy(&self.data)?,
            CDKind::List => cd_list::deepcopy(&self.data)?,
            CDKind::NumpyArray => cd_numpy::deepcopy(&self.data)?,
            CDKind::TorchTensor => cd_torch::deepcopy(&self.data)?,
        };
        Self::of_kind(self.kind, copied)
    }

    /// `isinstance(x, ComputationalData)` -> the wrapper.
    pub fn from_arg(a: &Arg) -> Option<ComputationalData> {
        a.downcast_ref::<ComputationalData>().cloned()
    }
}

impl core::fmt::Debug for ComputationalData {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}

impl PartialEq for ComputationalData {
    /// pydantic equality: same subclass and equal `data`.
    fn eq(&self, other: &ComputationalData) -> bool {
        self.kind == other.kind && self.data == other.data
    }
}

impl PyObject for ComputationalData {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        self.kind.class_name()
    }
    fn __is__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<ComputationalData>()
            .is_some_and(|o| self.is(o))
    }
    /// `type(cd) is CD_dict`: the dispatched subclass object.
    fn __class__(&self) -> Option<crate::python::PyClass> {
        Some(ComputationalData::__class__(self))
    }
    fn __copy__(&self) -> Option<Arg> {
        ComputationalData::__copy__(self)
            .ok()
            .map(IntoArg::into_arg)
    }
    fn __deepcopy__(&self) -> Option<Arg> {
        ComputationalData::__deepcopy__(self)
            .ok()
            .map(IntoArg::into_arg)
    }
    fn __repr__(&self) -> String {
        ComputationalData::__repr__(self)
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<ComputationalData>()
            .is_some_and(|o| self == o)
    }
    fn __hash__(&self) -> Result<i64> {
        Err(LailaError::type_error(alloc::format!(
            "unhashable type: '{}'",
            self.kind.class_name()
        )))
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "data" => Ok(self.data()),
            "shape" => self.shape(),
            "serializer" => Ok(self.serializer()?.into_arg()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{other}'",
                self.kind.class_name()
            ))),
        }
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        match name {
            "serializer" => self.set_serializer(value),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{other}'",
                self.kind.class_name()
            ))),
        }
    }
    fn __len__(&self) -> Result<usize> {
        ComputationalData::__len__(self)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "serialize" if args.__len__() == 0 => {
                let (b, code) = self.serialize(Args::new())?;
                Ok((b, Arg::Str(code.into())).into_arg())
            }
            "__copy__" => Ok(self.__copy__()?.into_arg()),
            "__deepcopy__" => Ok(self.__deepcopy__()?.into_arg()),
            "__len__" => Ok(Arg::Int(self.__len__()? as i64)),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'{}' object has no attribute '{other}'",
                self.kind.class_name()
            ))),
        }
    }
}

impl IntoArg for ComputationalData {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for ComputationalData {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl crate::python::builtins::PyLen for ComputationalData {
    fn __len__(&self) -> Result<usize> {
        ComputationalData::__len__(self)
    }
}
impl IsInstance for ComputationalData {
    fn isinstance(x: &Arg) -> bool {
        matches!(x, Arg::Object(o) if o.as_any().is::<ComputationalData>())
    }
}

/// Declares the direct-subclass constructor type (`CD_dict(value)`), with
/// `isinstance(cd, CD_dict)` support.
macro_rules! cd_subclass {
    ($name:ident, $kind:expr) => {
        #[doc = concat!("`", stringify!($name), "(value)`: direct subclass construction (bypasses dispatch).")]
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $name;
        impl $name {
            /// The Python constructor call `CD_x(value)`: a `ComputationalData`
            /// of this subclass (so `new` deliberately does not return `Self`).
            #[allow(clippy::new_ret_no_self)]
            pub fn new(
                args: impl Into<$crate::python::Args>,
            ) -> $crate::python::Result<ComputationalData> {
                ComputationalData::__init__($kind, args.into())
            }
            /// `cls.__name__`
            pub const NAME: &'static str = stringify!($name);
        }
        impl $crate::python::builtins::IsInstance for $name {
            fn isinstance(x: &$crate::python::Arg) -> bool {
                ComputationalData::from_arg(x).is_some_and(|c| c.kind() == $kind)
            }
        }
        /// The class object as a value (`type(cd) is CD_dict`).
        impl $crate::python::PyArg for $name {
            fn to_arg(&self) -> $crate::python::Arg {
                $crate::python::IntoArg::into_arg($crate::python::PyClass::with_ctor(
                    stringify!($name),
                    "laila.entry.compdata.taxonomy",
                    alloc::sync::Arc::new(|a: $crate::python::Args| {
                        $name::new(a).map($crate::python::IntoArg::into_arg)
                    }),
                ))
            }
        }
        impl $crate::python::IntoArg for $name {
            fn into_arg(self) -> $crate::python::Arg {
                $crate::python::PyArg::to_arg(&self)
            }
        }
    };
}
pub(crate) use cd_subclass;
