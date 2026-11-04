//! Msgpack serialisation / deserialisation transformation.
//!
//! `backward` (and the emitted `backward_code`) unpack with
//! `strict_map_key=False` so that dict payloads keyed by `int` / `float` /
//! `bool` -- which msgpack packs without complaint -- round-trip instead of
//! raising `ValueError` on the way back.
//!
//! `forward` packs with `strict_types=True`: msgpack has no tuple type and
//! no notion of `dict`/`list`/`int`/`str` *subclasses*, and the non-strict
//! packer silently coerces them (a nested tuple comes back as a list). In
//! strict mode the packer raises `TypeError` for such values instead, and
//! `ComputationalData.serialize` falls back to the universal
//! `PickleSerializer`, so what you memorize is exactly what you remember.
//! Pass `forward_kwargs={"strict_types": False}` to opt back into coercion
//! for a specific transformation instance.

use alloc::string::String;
use alloc::vec::Vec;
use core::any::Any;

use crate::_codecs::msgpack::{PackOptions, UnpackOptions};
use crate::_codecs::recovery_codes::emit_msgpack;
use crate::python::object::bytes;
use crate::python::{Arg, Args, LailaError, Result};

use super::super::base::{
    default_template_slots, kw_pairs, transformation_object, unexpected_keyword,
    DataTransformation, Slot, TemplateSpec, TransformationFields, _SLOT_KWARGS,
};

/// Reversible msgpack serialiser for Python objects.
#[derive(Clone)]
pub struct MsgpackSerializer {
    fields: TransformationFields,
}

impl MsgpackSerializer {
    pub const NAME: &'static str = "msgpack";

    /// `MsgpackSerializer(**fields)`; builds `backward_code` dynamically
    /// after model creation.
    pub fn new(args: impl Into<Args>) -> Result<MsgpackSerializer> {
        let a: Args = args.into();
        let fields = TransformationFields::from_args("MsgpackSerializer", Self::NAME, &a)?;
        let t = MsgpackSerializer { fields };
        t.model_post_init(Arg::None);
        Ok(t)
    }

    /// Build standalone backward recovery code.
    pub fn model_post_init(&self, __context: Arg) {
        self.set_backward_code(&emit_msgpack(Some(&self.backward_kwargs())));
    }

    pub fn _template_probe() -> Result<String> {
        let mut a = Args::new();
        a.push_keyword("backward_kwargs", _SLOT_KWARGS());
        Ok(MsgpackSerializer::new(a)?.backward_code())
    }

    pub fn _template_slots() -> Vec<Slot> {
        default_template_slots()
    }

    pub(crate) fn template_spec() -> TemplateSpec {
        TemplateSpec {
            class_name: "MsgpackSerializer",
            probe: MsgpackSerializer::_template_probe,
            slots: MsgpackSerializer::_template_slots,
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

impl DataTransformation for MsgpackSerializer {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn class_name(&self) -> &'static str {
        "MsgpackSerializer"
    }
    fn fields(&self) -> &TransformationFields {
        &self.fields
    }

    /// Serialize a Python object into bytes using msgpack.
    ///
    /// `TypeError` for values msgpack cannot express faithfully (tuples,
    /// `dict`/`list`/scalar subclasses, arbitrary objects).
    fn forward(&self, inp: &Arg) -> Result<Arg> {
        // kwargs = {"use_bin_type": True, "strict_types": True, **self.forward_kwargs}
        let mut opts = PackOptions {
            use_bin_type: true,
            strict_types: true,
            ..Default::default()
        };
        for (k, v) in kw_pairs(&self.forward_kwargs())? {
            match k.as_str() {
                "use_bin_type" => opts.use_bin_type = v.__bool__(),
                "strict_types" => opts.strict_types = v.__bool__(),
                "use_single_float" => opts.use_single_float = v.__bool__(),
                "default" => {
                    opts.default = match v {
                        Arg::None => None,
                        Arg::Callable(c) => Some(c),
                        other => {
                            return Err(LailaError::type_error(alloc::format!(
                                "'{}' object is not callable",
                                other.type_name()
                            )))
                        }
                    }
                }
                "autoreset" | "unicode_errors" | "datetime" | "buf_size" => {}
                other => return Err(unexpected_keyword("packb", other)),
            }
        }
        Ok(bytes(crate::_codecs::msgpack::packb(inp, &opts)?))
    }

    /// Deserialize bytes back into a Python object using msgpack.
    fn backward(&self, inp: &Arg) -> Result<Arg> {
        // kwargs = {"raw": False, "strict_map_key": False, **self.backward_kwargs}
        let mut opts = UnpackOptions {
            raw: false,
            strict_map_key: false,
            ..Default::default()
        };
        for (k, v) in kw_pairs(&self.backward_kwargs())? {
            match k.as_str() {
                "raw" => opts.raw = v.__bool__(),
                "strict_map_key" => opts.strict_map_key = v.__bool__(),
                "use_list" => opts.use_list = v.__bool__(),
                "object_hook" | "object_pairs_hook" | "list_hook" | "unicode_errors"
                | "max_buffer_size" | "ext_hook" | "max_str_len" | "max_bin_len"
                | "max_array_len" | "max_map_len" | "max_ext_len" | "timestamp" => {}
                other => return Err(unexpected_keyword("unpackb", other)),
            }
        }
        crate::_codecs::msgpack::unpackb(&bytes_input(inp)?, &opts)
    }
}

transformation_object!(MsgpackSerializer);
