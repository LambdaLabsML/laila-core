//! `ComputationalData` subclass for `list` and `tuple` payloads.
//!
//! Both Python sequence types share one wrapper. Lists serialize as msgpack
//! arrays; tuples default to pickle because msgpack has no tuple type, which
//! is what keeps the original Python type (list vs tuple) intact on
//! round-trip. Copy semantics respect the source type (tuples are immutable
//! so shallow-copy returns the same object).
//!
//! A tuple (or any other value msgpack cannot express faithfully) *nested*
//! inside a list is caught too: the msgpack serializer packs in strict mode
//! and raises, and `ComputationalData.serialize` falls back to pickle, so
//! nested tuples round-trip as tuples.

use alloc::string::String;
use alloc::vec::Vec;

use crate::python::{Arg, Args, IntoArg, Result};

use super::super::transformation::base::pydantic_validation_error;
use super::super::transformation::serialization::{MsgpackSerializer, PickleSerializer};
use super::super::transformation::Transformation;
use super::compdata::{cd_subclass, CDKind, ComputationalData};

/// `@register_cdtype(list, tuple)`
pub const PAYLOAD_TYPES: &[&str] = &["list", "tuple"];

cd_subclass!(CD_list, CDKind::List);

/// `data: InstanceOf[list] | InstanceOf[tuple]` -- pydantic reports one
/// error per union member.
pub(crate) fn validate(data: &Arg) -> Result<()> {
    match data {
        Arg::List(_) | Arg::Tuple(_) => Ok(()),
        _ => Err(pydantic_validation_error(
            CD_list::NAME,
            &[
                (
                    "data.json-or-python[json=list[any],python=is-instance[list]]",
                    "Input should be an instance of list",
                    "is_instance_of",
                    data,
                ),
                (
                    "data.`json-or-python[json=tuple[any, ...],python=is-instance[tuple]]`",
                    "Input should be an instance of tuple",
                    "is_instance_of",
                    data,
                ),
            ],
        )),
    }
}

/// Msgpack for lists; pickle for tuples.
///
/// msgpack has no tuple type -- a tuple comes back as a list -- so a tuple
/// payload defaults to `PickleSerializer`, which is the only way to honour
/// the "list vs tuple is preserved on round-trip" promise. Lists keep the
/// compact msgpack encoding.
pub(crate) fn default_serializer(data: &Arg) -> Result<Transformation> {
    if matches!(data, Arg::Tuple(_)) {
        Ok(Transformation::new(PickleSerializer::new(Args::new())?))
    } else {
        Ok(Transformation::new(MsgpackSerializer::new(Args::new())?))
    }
}

/// `isinstance(value, (MsgpackSerializer, PickleSerializer))`
pub(crate) fn accepts_serializer(t: Option<&Transformation>) -> bool {
    t.is_some_and(|t| {
        t.downcast_ref::<MsgpackSerializer>().is_some()
            || t.downcast_ref::<PickleSerializer>().is_some()
    })
}

/// Number of elements.
pub(crate) fn len(data: &Arg) -> Result<usize> {
    data.__len__()
}

/// 1-D shape tuple `(len,)`.
pub(crate) fn shape(data: &Arg) -> Result<Arg> {
    Ok(Arg::Tuple(
        alloc::vec![Arg::Int(data.__len__()? as i64)].into(),
    ))
}

/// Shallow copy: `list.copy()`; tuples are immutable and returned as-is.
pub(crate) fn copy(data: &Arg) -> Result<Arg> {
    match data {
        Arg::List(_) => data.copy(),
        other => Ok(other.clone()),
    }
}

/// Deep copy: `type(self.data)(copy.deepcopy(e, memo) for e in self.data)`.
pub(crate) fn deepcopy(data: &Arg) -> Result<Arg> {
    let items: Vec<Arg> = data.__iter__()?.iter().map(Arg::deepcopy).collect();
    Ok(match data {
        Arg::Tuple(_) => Arg::Tuple(items.into()),
        _ => items.into_arg(),
    })
}

/// `CD_list(type={list|tuple}, len={n})`
pub(crate) fn repr(data: &Arg) -> String {
    let tname = if matches!(data, Arg::List(_)) {
        "list"
    } else {
        "tuple"
    };
    alloc::format!("CD_list(type={tname}, len={})", data.__len__().unwrap_or(0))
}
