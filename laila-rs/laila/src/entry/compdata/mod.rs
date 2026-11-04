//! Computational-data sub-package re-exporting `ComputationalData`.
//!
//! `from laila.entry.compdata import transformation` binds the
//! transformation *module* as a value too (`transformation.Base64()`,
//! `transformation.TransformationSequence(..)`): [`transformation_mod`]
//! is that module object, exposed under the module's own name.

pub mod taxonomy;
pub mod transformation;

pub use self::taxonomy::ComputationalData;

use alloc::sync::Arc;

use crate::python::{Arg, Args, IntoArg, LailaError, PyClass, Result};

/// The `laila.entry.compdata.transformation` module as a value.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default)]
pub struct transformation_mod;

/// `from laila.entry.compdata import transformation`
#[allow(non_upper_case_globals)]
pub static transformation: transformation_mod = transformation_mod;

const _MODULE: &str = "laila.entry.compdata.transformation";

#[allow(non_snake_case)]
impl transformation_mod {
    /// `transformation.TransformationSequence(transformations=[..])`
    pub fn TransformationSequence(
        &self,
        args: impl Into<Args>,
    ) -> Result<transformation::TransformationSequence> {
        transformation::TransformationSequence::new(args)
    }
    /// `transformation.Base64()`
    pub fn Base64(&self, args: impl Into<Args>) -> Result<transformation::Base64> {
        transformation::Base64::new(args)
    }
    /// `transformation.Zlib()`
    pub fn Zlib(&self, args: impl Into<Args>) -> Result<transformation::Zlib> {
        transformation::Zlib::new(args)
    }
    /// `transformation.FernetEncryption(key=..)`
    pub fn FernetEncryption(
        &self,
        args: impl Into<Args>,
    ) -> Result<transformation::FernetEncryption> {
        transformation::FernetEncryption::new(args)
    }
    /// `transformation.JsonString()`
    pub fn JsonString(&self, args: impl Into<Args>) -> Result<transformation::JsonString> {
        transformation::JsonString::new(args)
    }
    /// `transformation.PickleSerializer()`
    pub fn PickleSerializer(
        &self,
        args: impl Into<Args>,
    ) -> Result<transformation::PickleSerializer> {
        transformation::PickleSerializer::new(args)
    }
    /// `transformation.MsgpackSerializer()`
    pub fn MsgpackSerializer(
        &self,
        args: impl Into<Args>,
    ) -> Result<transformation::MsgpackSerializer> {
        transformation::MsgpackSerializer::new(args)
    }
    /// `transformation.NumpySerializer()`
    pub fn NumpySerializer(
        &self,
        args: impl Into<Args>,
    ) -> Result<transformation::NumpySerializer> {
        transformation::NumpySerializer::new(args)
    }
    /// `transformation.TorchSerializer()`
    pub fn TorchSerializer(
        &self,
        args: impl Into<Args>,
    ) -> Result<transformation::TorchSerializer> {
        transformation::TorchSerializer::new(args)
    }
    /// `getattr(transformation, name)`: the classes as class objects.
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        macro_rules! class_object {
            ($t:ident) => {
                PyClass::with_ctor(
                    stringify!($t),
                    _MODULE,
                    Arc::new(|a: Args| transformation::$t::new(a).map(IntoArg::into_arg)),
                )
                .into_arg()
            };
        }
        Ok(match name {
            "__name__" => Arg::Str(_MODULE.into()),
            "TransformationSequence" => class_object!(TransformationSequence),
            "Base64" => class_object!(Base64),
            "Zlib" => class_object!(Zlib),
            "FernetEncryption" => class_object!(FernetEncryption),
            "JsonString" => class_object!(JsonString),
            "PickleSerializer" => class_object!(PickleSerializer),
            "MsgpackSerializer" => class_object!(MsgpackSerializer),
            "NumpySerializer" => class_object!(NumpySerializer),
            "TorchSerializer" => class_object!(TorchSerializer),
            "_data_transformation" => PyClass::named("_data_transformation", _MODULE).into_arg(),
            _ => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "module '{_MODULE}' has no attribute '{name}'"
                )))
            }
        })
    }
}
