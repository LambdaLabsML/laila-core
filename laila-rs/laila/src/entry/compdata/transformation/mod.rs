//! Transformation sub-package re-exporting all transformations and the pipeline.
//!
//! Also home of the pre-built sequences Python exports from `laila.entry`
//! (`transformation_base64`, `transformation_base64_compression`,
//! `transformation_base64_compression_encryption`, `transformation_encryption`).
//! They are *functions* here because a Rust static cannot hold the
//! lock-bearing pipeline value; `laila::entry` re-exports them and
//! `laila::python!` reads the bare name `transformation_base64` as the
//! call (rule P11: a Python module constant is a value-returning function).

pub mod base;
pub mod base64;
pub mod compression;
pub mod encryption;
pub mod jsonstring;
pub mod serialization;

pub use self::base::{
    _data_transformation, is_trusted_backward_code, register_known_backward_code,
    register_transformation, DataTransformation, TemplateSpec, Transformation,
    TransformationFields, TransformationSequence, REGISTRY,
};
pub use self::base64::Base64;
pub use self::compression::Zlib;
pub use self::encryption::{
    get_encryption_key, key_fingerprint, resolve_encryption_key, set_encryption_key,
    FernetEncryption,
};
pub use self::jsonstring::JsonString;
pub use self::serialization::{
    MsgpackSerializer, NumpySerializer, PickleSerializer, TorchSerializer,
};

use crate::python::{pysig, Args, Result};

/// `__init_subclass__` for the built-in transformations: register every
/// class with a non-empty `name` in `_data_transformation.REGISTRY`, and the
/// native `fernet` recovery backend.
pub(crate) fn seed_registry() {
    register_transformation(Base64::NAME, Base64::template_spec());
    register_transformation(Zlib::NAME, Zlib::template_spec());
    register_transformation(FernetEncryption::NAME, FernetEncryption::template_spec());
    register_transformation(JsonString::NAME, JsonString::template_spec());
    register_transformation(MsgpackSerializer::NAME, MsgpackSerializer::template_spec());
    register_transformation(NumpySerializer::NAME, NumpySerializer::template_spec());
    register_transformation(PickleSerializer::NAME, PickleSerializer::template_spec());
    register_transformation(TorchSerializer::NAME, TorchSerializer::template_spec());
    encryption::encryption::ensure_backend();
}

/// `transformation_base64 = TransformationSequence(transformations=[Base64()])`
/// -- base64 only. Use when the pool only needs text-safe payloads.
pub fn transformation_base64() -> TransformationSequence {
    TransformationSequence::from_vec(alloc::vec![Transformation::new(
        Base64::new(Args::new()).expect("Base64() cannot fail")
    )])
}

/// `transformation_base64_compression = TransformationSequence(transformations=[Base64(), Zlib()])`
/// -- base64 *of* zlib output.
pub fn transformation_base64_compression() -> TransformationSequence {
    TransformationSequence::from_vec(alloc::vec![
        Transformation::new(Base64::new(Args::new()).expect("Base64() cannot fail")),
        Transformation::new(Zlib::new(Args::new()).expect("Zlib() cannot fail")),
    ])
}

/// `transformation_base64_compression_encryption(key=None)` -- factory:
/// a base64 -> zlib -> Fernet pipeline; `key=None` follows
/// `laila.encryption_key`.
#[pysig("transformation_base64_compression_encryption(key=None)")]
pub fn transformation_base64_compression_encryption(
    args: impl Into<Args>,
) -> Result<TransformationSequence> {
    let mut a = Args::new();
    a.push_keyword("key", key);
    Ok(TransformationSequence::from_vec(alloc::vec![
        Transformation::new(Base64::new(Args::new())?),
        Transformation::new(Zlib::new(Args::new())?),
        Transformation::new(FernetEncryption::new(a)?),
    ]))
}

/// `transformation_encryption(key=None)` -- factory: a base64-then-Fernet
/// pipeline; `key=None` follows `laila.encryption_key`.
#[pysig("transformation_encryption(key=None)")]
pub fn transformation_encryption(args: impl Into<Args>) -> Result<TransformationSequence> {
    let mut a = Args::new();
    a.push_keyword("key", key);
    Ok(TransformationSequence::from_vec(alloc::vec![
        Transformation::new(Base64::new(Args::new())?),
        Transformation::new(FernetEncryption::new(a)?),
    ]))
}
