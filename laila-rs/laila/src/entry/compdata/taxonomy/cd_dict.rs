//! `ComputationalData` subclass for Python `dict` payloads.
//!
//! Dicts are serialized with msgpack rather than pickle so the on-disk
//! representation is language-neutral (other msgpack readers can decode
//! laila-pooled dicts) and substantially smaller for the common
//! shallow-string-keyed cases.
//!
//! msgpack packs in strict mode: a dict holding values it cannot express
//! faithfully (tuples, tuple keys, arbitrary objects) raises at pack time
//! and `ComputationalData.serialize` falls back to pickle, so the exact
//! Python types come back on `remember`. Such payloads give up msgpack's
//! language-neutrality; plain dicts keep it.

use alloc::string::String;

use crate::python::{Arg, Args, LailaError, Result};

use super::super::transformation::base::pydantic_validation_error;
use super::super::transformation::serialization::MsgpackSerializer;
use super::super::transformation::Transformation;
use super::compdata::{cd_subclass, CDKind, ComputationalData};

/// `@register_cdtype(dict)`
pub const PAYLOAD_TYPES: &[&str] = &["dict"];

cd_subclass!(CD_dict, CDKind::Dict);

/// `data: InstanceOf[dict]`
pub(crate) fn validate(data: &Arg) -> Result<()> {
    match data {
        Arg::Dict(_) => Ok(()),
        // `dict` subclasses (`OrderedDict`) are instances too.
        Arg::Object(o) if o.__bases__().contains(&"dict") => Ok(()),
        _ => Err(pydantic_validation_error(
            CD_dict::NAME,
            &[(
                "data",
                "Input should be an instance of dict",
                "is_instance_of",
                data,
            )],
        )),
    }
}

/// `_SERIALIZER_CLS = MsgpackSerializer`
pub(crate) fn default_serializer() -> Result<Transformation> {
    Ok(Transformation::new(MsgpackSerializer::new(Args::new())?))
}

/// `isinstance(value, MsgpackSerializer)`
pub(crate) fn accepts_serializer(t: Option<&Transformation>) -> bool {
    t.is_some_and(|t| t.downcast_ref::<MsgpackSerializer>().is_some())
}

/// Number of keys in the dict.
pub(crate) fn len(data: &Arg) -> Result<usize> {
    data.__len__()
}

/// Not applicable for dicts: `AttributeError` always.
pub(crate) fn shape(_data: &Arg) -> Result<Arg> {
    Err(LailaError::attribute_error("dict has no 'shape' attribute"))
}

/// Shallow copy: `self.data.copy()`.
pub(crate) fn copy(data: &Arg) -> Result<Arg> {
    data.copy()
}

/// Deep copy.
pub(crate) fn deepcopy(data: &Arg) -> Result<Arg> {
    Ok(data.deepcopy())
}

/// `CD_dict({data!r})`
pub(crate) fn repr(data: &Arg) -> String {
    alloc::format!("CD_dict({})", data.__repr__())
}
