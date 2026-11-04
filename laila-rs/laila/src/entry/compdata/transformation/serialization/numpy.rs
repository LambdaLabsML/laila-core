//! NumPy array serialisation / deserialisation transformation.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;

use crate::_codecs::recovery_codes::emit_numpy;
use crate::_compat::ndarray::NDArray;
use crate::python::object::bytes;
use crate::python::{Arg, Args, LailaError, Result};

use super::super::base::{
    default_template_slots, kw_pairs, transformation_object, unexpected_keyword,
    DataTransformation, Slot, TemplateSpec, TransformationFields, _SLOT_KWARGS,
};

/// Reversible NumPy serialiser using `np.save` / `np.load`.
#[derive(Clone)]
pub struct NumpySerializer {
    fields: TransformationFields,
}

impl NumpySerializer {
    pub const NAME: &'static str = "numpy";

    /// `NumpySerializer(**fields)`; builds `backward_code` dynamically
    /// after model creation.
    pub fn new(args: impl Into<Args>) -> Result<NumpySerializer> {
        let a: Args = args.into();
        let fields = TransformationFields::from_args("NumpySerializer", Self::NAME, &a)?;
        let t = NumpySerializer { fields };
        t.model_post_init(Arg::None);
        Ok(t)
    }

    /// Build standalone backward recovery code.
    pub fn model_post_init(&self, __context: Arg) {
        self.set_backward_code(&emit_numpy(Some(&self.backward_kwargs())));
    }

    pub fn _template_probe() -> Result<String> {
        let mut a = Args::new();
        a.push_keyword("backward_kwargs", _SLOT_KWARGS());
        Ok(NumpySerializer::new(a)?.backward_code())
    }

    pub fn _template_slots() -> Vec<Slot> {
        default_template_slots()
    }

    pub(crate) fn template_spec() -> TemplateSpec {
        TemplateSpec {
            class_name: "NumpySerializer",
            probe: NumpySerializer::_template_probe,
            slots: NumpySerializer::_template_slots,
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

impl DataTransformation for NumpySerializer {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn class_name(&self) -> &'static str {
        "NumpySerializer"
    }
    fn fields(&self) -> &TransformationFields {
        &self.fields
    }

    /// Serialize a NumPy array into bytes (`np.save`-encoded).
    fn forward(&self, inp: &Arg) -> Result<Arg> {
        // kwargs = {"allow_pickle": False, **self.forward_kwargs}
        for (k, _) in kw_pairs(&self.forward_kwargs())? {
            match k.as_str() {
                "allow_pickle" | "fix_imports" => {}
                other => return Err(unexpected_keyword("save", other)),
            }
        }
        // ``np.save`` calls ``np.asanyarray`` on its argument.
        let arr: Arc<NDArray> = match inp {
            Arg::NDArray(a) => a.clone(),
            other => Arc::new(NDArray::array(other, None)?),
        };
        Ok(bytes(crate::_codecs::npy::save(&arr)?))
    }

    /// Deserialize bytes back into a NumPy array.
    fn backward(&self, inp: &Arg) -> Result<Arg> {
        // kwargs = {"allow_pickle": False, **self.backward_kwargs}
        for (k, _) in kw_pairs(&self.backward_kwargs())? {
            match k.as_str() {
                "allow_pickle" | "mmap_mode" | "fix_imports" | "encoding" | "max_header_size" => {}
                other => return Err(unexpected_keyword("load", other)),
            }
        }
        let arr = crate::_codecs::npy::load(&bytes_input(inp)?)?;
        Ok(Arg::NDArray(Arc::new(arr)))
    }
}

transformation_object!(NumpySerializer);
