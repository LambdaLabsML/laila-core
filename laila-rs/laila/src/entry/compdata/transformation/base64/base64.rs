//! Base64 encode / decode data transformation.
//!
//! The [`Base64`] transformation makes binary blobs JSON-safe by mapping
//! bytes <-> ASCII strings via `base64.b64encode` / `base64.b64decode`. It
//! is the standard "tail" step in pool transformation pipelines whose
//! backing store wants only printable text (filesystem JSON, S3 JSON
//! objects, postgres TEXT columns, ...).
//!
//! The forward direction takes `bytes`/`bytearray`/`memoryview` and returns
//! a UTF-8 `str`; the backward direction accepts either flavour and returns
//! `bytes`. `forward_kwargs` and `backward_kwargs` are forwarded to the
//! underlying base64 functions so callers can, for example, configure a
//! URL-safe alphabet via `altchars=b'-_'`.

use alloc::string::String;
use alloc::vec::Vec;
use core::any::Any;

use crate::_codecs::recovery_codes::emit_base64;
use crate::python::object::bytes;
use crate::python::{Arg, Args, LailaError, Result};

use super::super::base::{
    default_template_slots, kw_pairs, transformation_object, unexpected_keyword,
    DataTransformation, Slot, TemplateSpec, TransformationFields, _SLOT_KWARGS,
};

/// Reversible Base64 encoding transformation.
///
/// Forward encodes binary data to a Base64 UTF-8 string; backward decodes
/// it back to raw bytes.
#[derive(Clone)]
pub struct Base64 {
    fields: TransformationFields,
}

impl Base64 {
    pub const NAME: &'static str = "base64";

    /// `Base64(**fields)`; builds the standalone backward recovery code
    /// (`model_post_init`).
    pub fn new(args: impl Into<Args>) -> Result<Base64> {
        let a: Args = args.into();
        let fields = TransformationFields::from_args("Base64", Self::NAME, &a)?;
        let t = Base64 { fields };
        t.model_post_init(Arg::None);
        Ok(t)
    }

    /// Build standalone backward recovery code.
    pub fn model_post_init(&self, __context: Arg) {
        // Standalone recovery code with embedded kwargs (e.g., altchars, validate)
        self.set_backward_code(&emit_base64(Some(&self.backward_kwargs())));
    }

    /// `cls._template_probe().backward_code`
    pub fn _template_probe() -> Result<String> {
        let mut a = Args::new();
        a.push_keyword("backward_kwargs", _SLOT_KWARGS());
        Ok(Base64::new(a)?.backward_code())
    }

    /// `cls._template_slots()`
    pub fn _template_slots() -> Vec<Slot> {
        default_template_slots()
    }

    pub(crate) fn template_spec() -> TemplateSpec {
        TemplateSpec {
            class_name: "Base64",
            probe: Base64::_template_probe,
            slots: Base64::_template_slots,
        }
    }
}

/// `altchars=` keyword of `b64encode` / `b64decode`.
fn altchars_kw(v: &Arg) -> Result<Option<Vec<u8>>> {
    match v {
        Arg::None => Ok(None),
        Arg::Str(s) => Ok(Some(s.as_bytes().to_vec())),
        other => other.as_bytes_like().map(Some).ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "argument should be a bytes-like object or ASCII string, not '{}'",
                other.type_name()
            ))
        }),
    }
}

impl DataTransformation for Base64 {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn class_name(&self) -> &'static str {
        "Base64"
    }
    fn fields(&self) -> &TransformationFields {
        &self.fields
    }

    /// Encode binary data -> Base64 UTF-8 string.
    fn forward(&self, data: &Arg) -> Result<Arg> {
        let Some(raw) = data.as_bytes_like() else {
            return Err(LailaError::type_error(
                "Base64.forward expects bytes/bytearray/memoryview",
            ));
        };
        // b64encode supports altchars=...
        let mut altchars = None;
        for (k, v) in kw_pairs(&self.forward_kwargs())? {
            match k.as_str() {
                "altchars" => altchars = altchars_kw(&v)?,
                other => return Err(unexpected_keyword("b64encode", other)),
            }
        }
        let out = crate::_codecs::base64::b64encode_str(&raw, altchars.as_deref())?;
        Ok(Arg::Str(out.into()))
    }

    /// Decode Base64 (str/bytes/bytearray/memoryview) -> raw bytes.
    fn backward(&self, payload: &Arg) -> Result<Arg> {
        // base64.b64decode accepts str or bytes-like
        let decode = || -> Result<Vec<u8>> {
            let mut altchars = None;
            let mut validate = false;
            for (k, v) in kw_pairs(&self.backward_kwargs())? {
                match k.as_str() {
                    "altchars" => altchars = altchars_kw(&v)?,
                    "validate" => validate = v.__bool__(),
                    other => return Err(unexpected_keyword("b64decode", other)),
                }
            }
            crate::_codecs::base64::b64decode(payload, altchars.as_deref(), validate)
        };
        match decode() {
            Ok(out) => Ok(bytes(out)),
            Err(_) => Err(LailaError::value_error("Invalid Base64 payload")),
        }
    }
}

transformation_object!(Base64);
