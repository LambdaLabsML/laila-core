//! Zlib compression / decompression data transformation.
//!
//! The [`Zlib`] transformation slots into a transformation pipeline as the
//! compression step. It accepts a UTF-8 string, compresses it with
//! `zlib.compress`, and re-encodes the compressed bytes with base64 so the
//! result is still a plain string (matching the contract assumed by
//! downstream encoding / serialisation steps and by JSON-only pools).
//!
//! Two reasons for the integrated base64 wrap inside this single
//! transformation:
//!
//! 1. It keeps the output of every transformation in the pipeline a string,
//!    so users can chain `[Json, Zlib, Base64]` -- or just `[Json, Zlib]` --
//!    without thinking about bytes/str mismatches.
//! 2. The recovery code that the transformation emits is therefore
//!    self-contained: it only needs to import `zlib` and `base64`, with no
//!    implicit dependency on the upstream pipe step's output type.
//!
//! `forward_kwargs` / `backward_kwargs` are forwarded to `zlib.compress` /
//! `zlib.decompress` so callers can tune compression level, window bits, etc.

use alloc::string::String;
use alloc::vec::Vec;
use core::any::Any;

use crate::_codecs::recovery_codes::emit_zlib;
use crate::python::methods::utf8_decode;
use crate::python::{Arg, Args, LailaError, Result};

use super::super::base::{
    default_template_slots, kw_pairs, transformation_object, unexpected_keyword,
    DataTransformation, Slot, TemplateSpec, TransformationFields, _SLOT_KWARGS,
};

/// Reversible zlib compression transformation.
///
/// Forward compresses a UTF-8 string to a Base64-encoded compressed string;
/// backward decompresses it.
#[derive(Clone)]
pub struct Zlib {
    fields: TransformationFields,
}

fn int_kw(v: &Arg) -> Result<i64> {
    v.as_i64().ok_or_else(|| {
        LailaError::type_error(alloc::format!(
            "'{}' object cannot be interpreted as an integer",
            v.type_name()
        ))
    })
}

impl Zlib {
    pub const NAME: &'static str = "zlib";

    /// `Zlib(**fields)`; builds the standalone backward recovery code.
    pub fn new(args: impl Into<Args>) -> Result<Zlib> {
        let a: Args = args.into();
        let fields = TransformationFields::from_args("Zlib", Self::NAME, &a)?;
        let t = Zlib { fields };
        t.model_post_init(Arg::None);
        Ok(t)
    }

    /// Build standalone backward recovery code.
    pub fn model_post_init(&self, __context: Arg) {
        // Standalone recovery code mirroring `backward()` with embedded kwargs
        self.set_backward_code(&emit_zlib(Some(&self.backward_kwargs())));
    }

    pub fn _template_probe() -> Result<String> {
        let mut a = Args::new();
        a.push_keyword("backward_kwargs", _SLOT_KWARGS());
        Ok(Zlib::new(a)?.backward_code())
    }

    pub fn _template_slots() -> Vec<Slot> {
        default_template_slots()
    }

    pub(crate) fn template_spec() -> TemplateSpec {
        TemplateSpec {
            class_name: "Zlib",
            probe: Zlib::_template_probe,
            slots: Zlib::_template_slots,
        }
    }
}

impl DataTransformation for Zlib {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn class_name(&self) -> &'static str {
        "Zlib"
    }
    fn fields(&self) -> &TransformationFields {
        &self.fields
    }

    /// Compress a UTF-8 string and return a Base64-encoded result.
    ///
    /// `TypeError` if *data* is not a `str`.
    fn forward(&self, data: &Arg) -> Result<Arg> {
        let Arg::Str(text) = data else {
            return Err(LailaError::type_error(
                "Zlib.forward expects a UTF-8 string (str)",
            ));
        };
        let mut level = crate::_codecs::zlib::Z_DEFAULT_COMPRESSION;
        let mut wbits = crate::_codecs::zlib::MAX_WBITS;
        for (k, v) in kw_pairs(&self.forward_kwargs())? {
            match k.as_str() {
                "level" => level = int_kw(&v)? as i32,
                "wbits" => wbits = int_kw(&v)? as i32,
                other => return Err(unexpected_keyword("compress", other)),
            }
        }
        let compressed = crate::_codecs::zlib::compress(text.as_bytes(), level, wbits)?;
        let out = crate::_codecs::base64::b64encode_str(&compressed, None)?;
        Ok(Arg::Str(out.into()))
    }

    /// Decompress a Base64-encoded compressed string.
    ///
    /// `TypeError` if *data* is not a `str`.
    fn backward(&self, data: &Arg) -> Result<Arg> {
        if !matches!(data, Arg::Str(_)) {
            return Err(LailaError::type_error(
                "Zlib.backward expects a Base64 string (str)",
            ));
        }
        let compressed: Vec<u8> = crate::_codecs::base64::b64decode(data, None, true)?;
        let mut wbits = crate::_codecs::zlib::MAX_WBITS;
        let mut bufsize = crate::_codecs::zlib::DEF_BUF_SIZE as i64;
        for (k, v) in kw_pairs(&self.backward_kwargs())? {
            match k.as_str() {
                "wbits" => wbits = int_kw(&v)? as i32,
                "bufsize" => bufsize = int_kw(&v)?,
                other => return Err(unexpected_keyword("decompress", other)),
            }
        }
        if bufsize < 0 {
            return Err(LailaError::value_error("bufsize must be non-negative"));
        }
        let raw = crate::_codecs::zlib::decompress(&compressed, wbits, bufsize as usize)?;
        Ok(Arg::Str(utf8_decode(&raw)?.into()))
    }
}

transformation_object!(Zlib);
