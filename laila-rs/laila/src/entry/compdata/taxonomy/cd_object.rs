//! `ComputationalData` catch-all subclass for arbitrary Python objects.
//!
//! Registered against `object` so the MRO walk in `ComputationalData.__new__`
//! always finds at least this class for otherwise-unknown payload types.
//! Uses `PickleSerializer` -- the broadest-compatibility option, at the cost
//! of being Python-specific on the wire.

use alloc::string::String;

use crate::python::{Arg, Args, ExcKind, LailaError, Result};

use super::super::transformation::serialization::PickleSerializer;
use super::super::transformation::Transformation;
use super::compdata::{_scalar_len, cd_subclass, CDKind, ComputationalData};

/// `@register_cdtype(object)` -- final catch-all.
pub const PAYLOAD_TYPES: &[&str] = &["object"];

cd_subclass!(CD_generic, CDKind::Generic);

/// `_SERIALIZER_CLS = PickleSerializer`
pub(crate) fn default_serializer() -> Result<Transformation> {
    Ok(Transformation::new(PickleSerializer::new(Args::new())?))
}

/// `isinstance(value, PickleSerializer)`
pub(crate) fn accepts_serializer(t: Option<&Transformation>) -> bool {
    t.is_some_and(|t| t.downcast_ref::<PickleSerializer>().is_some())
}

/// Return the length if the payload supports `__len__`, else `_scalar_len()`.
pub(crate) fn len(data: &Arg) -> Result<usize> {
    match data.__len__() {
        Ok(n) => Ok(n),
        Err(e) if e.kind == ExcKind::TypeError && e.message.starts_with("object of type") => {
            Err(_scalar_len())
        }
        Err(e) => Err(e),
    }
}

/// Not applicable for generic objects: `AttributeError` always.
pub(crate) fn shape(data: &Arg) -> Result<Arg> {
    Err(LailaError::attribute_error(alloc::format!(
        "{} has no 'shape' attribute",
        data.type_name()
    )))
}

/// Shallow copy: `type(self)(self.data)`.
pub(crate) fn copy(data: &Arg) -> Result<Arg> {
    Ok(data.clone())
}

/// Deep copy: `type(self)(copy.deepcopy(self.data, memo))`.
pub(crate) fn deepcopy(data: &Arg) -> Result<Arg> {
    Ok(data.deepcopy())
}

/// `CD_generic({data!r})`
pub(crate) fn repr(data: &Arg) -> String {
    alloc::format!("CD_generic({})", data.__repr__())
}
