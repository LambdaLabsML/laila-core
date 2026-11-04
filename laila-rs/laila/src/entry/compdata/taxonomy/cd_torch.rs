//! `ComputationalData` subclass for PyTorch `torch.Tensor` payloads.
//!
//! The wrapper is only registered when `torch` is importable -- the `torch`
//! extras are an optional dependency. When torch is missing the class
//! definition is skipped entirely, so the registry has no entry for
//! `torch.Tensor` and attempts to pool a tensor fall back to `CD_generic`.
//!
//! laila-rs has no torch runtime: `_HAVE_TORCH` is `false`, nothing is
//! registered in `TYPE_TO_WRAPPER`, and forcing the subclass
//! (`CD_torchtensor(value)`) raises the `ModuleNotFoundError` text Python
//! shows when `import torch` fails. The `torch` recovery-code template is
//! still recognised by the trust gate so a Python-written torch payload
//! fails with a clear `NotImplementedError` at build time instead of being
//! mis-decoded.

use alloc::string::String;

use crate::python::{Arg, LailaError, Result};

use super::super::transformation::serialization::TorchSerializer;
use super::super::transformation::Transformation;
use super::compdata::{cd_subclass, CDKind, ComputationalData};

/// `try: import torch ... except ModuleNotFoundError: _HAVE_TORCH = False`
pub const _HAVE_TORCH: bool = false;

/// `@register_cdtype(torch.Tensor)` -- never applied (no torch).
pub const PAYLOAD_TYPES: &[&str] = &[];

cd_subclass!(CD_torchtensor, CDKind::TorchTensor);

/// `import torch` on a host without torch.
pub(crate) fn no_torch() -> LailaError {
    super::super::transformation::serialization::torch::no_torch()
}

/// `_SERIALIZER_CLS = TorchSerializer`
pub(crate) fn default_serializer() -> Result<Transformation> {
    Ok(Transformation::new(TorchSerializer::new(
        crate::python::Args::new(),
    )?))
}

/// `isinstance(value, TorchSerializer)`
pub(crate) fn accepts_serializer(t: Option<&Transformation>) -> bool {
    t.is_some_and(|t| t.downcast_ref::<TorchSerializer>().is_some())
}

/// `self.data.size(0) if self.data.dim() else _scalar_len()`
pub(crate) fn len(_data: &Arg) -> Result<usize> {
    Err(no_torch())
}

/// `tuple(self.data.size())`
pub(crate) fn shape(_data: &Arg) -> Result<Arg> {
    Err(no_torch())
}

/// `self.data.clone()`
pub(crate) fn copy(_data: &Arg) -> Result<Arg> {
    Err(no_torch())
}

/// `self.data.clone()`
pub(crate) fn deepcopy(_data: &Arg) -> Result<Arg> {
    Err(no_torch())
}

/// `CD_torchtensor(shape=..., dtype=..., device=...)`
pub(crate) fn repr(data: &Arg) -> String {
    alloc::format!("CD_torchtensor(data={})", data.__repr__())
}
