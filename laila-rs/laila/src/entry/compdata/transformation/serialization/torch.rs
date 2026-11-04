//! PyTorch tensor serialisation / deserialisation transformation.
//!
//! There is no torch runtime in laila-rs. In Python this module only
//! imports when `torch` is installed (`TorchSerializer = None` otherwise);
//! here the class exists so that its recovery-code *template* is still
//! recognised (a Python-written torch payload fails with a clear
//! `NotImplementedError` instead of being mis-decoded), but constructing
//! or using the serializer raises the `ModuleNotFoundError` text Python
//! shows when `import torch` fails.

use alloc::string::String;
use alloc::vec::Vec;
use core::any::Any;

use crate::_codecs::recovery_codes::emit_torch;
use crate::python::{Arg, Args, ExcKind, LailaError, Result};

use super::super::base::{
    default_template_slots, transformation_object, DataTransformation, Slot, TemplateSpec,
    TransformationFields, _SLOT_KWARGS,
};

/// `import torch` on a host without torch.
pub(crate) fn no_torch() -> LailaError {
    // ``ModuleNotFoundError`` is an ``ImportError`` subclass; the port has
    // no separate kind for it.
    LailaError::new(ExcKind::ImportError, "No module named 'torch'")
}

/// Reversible PyTorch serialiser using `torch.save` / `torch.load`.
#[derive(Clone)]
pub struct TorchSerializer {
    fields: TransformationFields,
}

impl TorchSerializer {
    pub const NAME: &'static str = "torch";

    /// Always raises: `import torch` fails in laila-rs.
    pub fn new(args: impl Into<Args>) -> Result<TorchSerializer> {
        let _a: Args = args.into();
        Err(no_torch())
    }

    /// Build backward_code dynamically after model creation.
    pub fn model_post_init(&self, __context: Arg) {
        self.set_backward_code(&emit_torch(Some(&self.backward_kwargs())));
    }

    /// The template probe does not need the runtime: it is the rendered
    /// `backward_code` with the sentinel kwargs.
    pub fn _template_probe() -> Result<String> {
        let code = emit_torch(Some(&_SLOT_KWARGS()));
        super::super::base::register_known_backward_code(&code);
        Ok(code)
    }

    pub fn _template_slots() -> Vec<Slot> {
        default_template_slots()
    }

    pub(crate) fn template_spec() -> TemplateSpec {
        TemplateSpec {
            class_name: "TorchSerializer",
            probe: TorchSerializer::_template_probe,
            slots: TorchSerializer::_template_slots,
        }
    }
}

impl DataTransformation for TorchSerializer {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn class_name(&self) -> &'static str {
        "TorchSerializer"
    }
    fn fields(&self) -> &TransformationFields {
        &self.fields
    }
    fn forward(&self, _inp: &Arg) -> Result<Arg> {
        Err(no_torch())
    }
    fn backward(&self, _inp: &Arg) -> Result<Arg> {
        Err(no_torch())
    }
}

transformation_object!(TorchSerializer);
