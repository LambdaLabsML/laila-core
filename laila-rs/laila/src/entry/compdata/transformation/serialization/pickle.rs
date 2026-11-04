//! Pickle serialisation / deserialisation transformation.

use alloc::string::String;
use alloc::vec::Vec;
use core::any::Any;

use crate::_codecs::recovery_codes::emit_pickle;
use crate::python::object::bytes;
use crate::python::{Arg, Args, LailaError, Result};

use super::super::base::{
    default_template_slots, kw_pairs, transformation_object, unexpected_keyword,
    DataTransformation, Slot, TemplateSpec, TransformationFields, _SLOT_KWARGS,
};

/// Reversible pickle serialiser for arbitrary Python objects.
#[derive(Clone)]
pub struct PickleSerializer {
    fields: TransformationFields,
}

impl PickleSerializer {
    pub const NAME: &'static str = "pickle";

    /// `PickleSerializer(**fields)`; builds `backward_code` dynamically
    /// after model creation.
    pub fn new(args: impl Into<Args>) -> Result<PickleSerializer> {
        let a: Args = args.into();
        let fields = TransformationFields::from_args("PickleSerializer", Self::NAME, &a)?;
        let t = PickleSerializer { fields };
        t.model_post_init(Arg::None);
        Ok(t)
    }

    /// Build standalone backward recovery code.
    pub fn model_post_init(&self, __context: Arg) {
        self.set_backward_code(&emit_pickle(Some(&self.backward_kwargs())));
    }

    pub fn _template_probe() -> Result<String> {
        let mut a = Args::new();
        a.push_keyword("backward_kwargs", _SLOT_KWARGS());
        Ok(PickleSerializer::new(a)?.backward_code())
    }

    pub fn _template_slots() -> Vec<Slot> {
        default_template_slots()
    }

    pub(crate) fn template_spec() -> TemplateSpec {
        TemplateSpec {
            class_name: "PickleSerializer",
            probe: PickleSerializer::_template_probe,
            slots: PickleSerializer::_template_slots,
        }
    }
}

fn bytes_input(inp: &Arg) -> Result<Vec<u8>> {
    inp.as_bytes_like().ok_or_else(|| {
        LailaError::type_error(alloc::format!(
            "a bytes-like object is required, not '{}'",
            inp.type_name()
        ))
    })
}

impl DataTransformation for PickleSerializer {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn class_name(&self) -> &'static str {
        "PickleSerializer"
    }
    fn fields(&self) -> &TransformationFields {
        &self.fields
    }

    /// Serialize a Python object into bytes using pickle.
    fn forward(&self, inp: &Arg) -> Result<Arg> {
        let mut protocol: Option<i32> = None;
        for (k, v) in kw_pairs(&self.forward_kwargs())? {
            match k.as_str() {
                "protocol" => {
                    protocol = match v {
                        Arg::None => None,
                        other => Some(other.as_i64().ok_or_else(|| {
                            LailaError::type_error(alloc::format!(
                                "'{}' object cannot be interpreted as an integer",
                                other.type_name()
                            ))
                        })? as i32),
                    }
                }
                "fix_imports" | "buffer_callback" => {}
                other => return Err(unexpected_keyword("dumps", other)),
            }
        }
        Ok(bytes(crate::_codecs::pickle::dumps(inp, protocol)?))
    }

    /// Deserialize bytes back into a Python object using pickle.
    fn backward(&self, inp: &Arg) -> Result<Arg> {
        for (k, _) in kw_pairs(&self.backward_kwargs())? {
            match k.as_str() {
                "fix_imports" | "encoding" | "errors" | "buffers" => {}
                other => return Err(unexpected_keyword("loads", other)),
            }
        }
        crate::_codecs::pickle::loads(&bytes_input(inp)?)
    }
}

transformation_object!(PickleSerializer);
