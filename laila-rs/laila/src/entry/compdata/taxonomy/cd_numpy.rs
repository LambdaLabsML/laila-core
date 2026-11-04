//! `ComputationalData` subclass for `numpy.ndarray` payloads.
//!
//! Uses `NumpySerializer` (the `.npy` format under the hood) so arrays are
//! persisted with their dtype and shape in a portable, language-neutral
//! binary format -- much more compact than pickle for numeric data and
//! readable from any process with NumPy installed.

use alloc::string::String;
use alloc::sync::Arc;

use crate::_compat::ndarray::dtype_name;
use crate::python::{Arg, Args, Result};

use super::super::transformation::base::pydantic_validation_error;
use super::super::transformation::serialization::NumpySerializer;
use super::super::transformation::Transformation;
use super::compdata::{_scalar_len, cd_subclass, CDKind, ComputationalData};

/// `@register_cdtype(np.ndarray)`
pub const PAYLOAD_TYPES: &[&str] = &["ndarray"];

cd_subclass!(CD_numpyarray, CDKind::NumpyArray);

/// `data: np.ndarray`
pub(crate) fn validate(data: &Arg) -> Result<()> {
    match data {
        Arg::NDArray(_) => Ok(()),
        _ => Err(pydantic_validation_error(
            CD_numpyarray::NAME,
            &[(
                "data",
                "Input should be an instance of ndarray",
                "is_instance_of",
                data,
            )],
        )),
    }
}

/// `_SERIALIZER_CLS = NumpySerializer`
pub(crate) fn default_serializer() -> Result<Transformation> {
    Ok(Transformation::new(NumpySerializer::new(Args::new())?))
}

/// `isinstance(value, NumpySerializer)`
pub(crate) fn accepts_serializer(t: Option<&Transformation>) -> bool {
    t.is_some_and(|t| t.downcast_ref::<NumpySerializer>().is_some())
}

/// Size of the first dimension; `_scalar_len()` for 0-D arrays.
pub(crate) fn len(data: &Arg) -> Result<usize> {
    match data {
        Arg::NDArray(a) if a.ndim() > 0 => Ok(a.shape[0]),
        _ => Err(_scalar_len()),
    }
}

/// The array shape tuple.
pub(crate) fn shape(data: &Arg) -> Result<Arg> {
    match data {
        Arg::NDArray(a) => Ok(a.shape()),
        other => other.__getattr__("shape"),
    }
}

/// Shallow (contiguous) copy of the array.
pub(crate) fn copy(data: &Arg) -> Result<Arg> {
    match data {
        Arg::NDArray(a) => Ok(Arg::NDArray(Arc::new((**a).clone()))),
        other => other.copy(),
    }
}

/// Deep copy of the array (`.copy()`).
pub(crate) fn deepcopy(data: &Arg) -> Result<Arg> {
    copy(data)
}

/// `CD_numpyarray(shape={shape}, dtype={dtype})`
pub(crate) fn repr(data: &Arg) -> String {
    match data {
        Arg::NDArray(a) => alloc::format!(
            "CD_numpyarray(shape={}, dtype={})",
            a.shape().__repr__(),
            dtype_name(&a.dtype)
        ),
        other => alloc::format!("CD_numpyarray(data={})", other.__repr__()),
    }
}
