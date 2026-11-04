//! Constitution sub-package: build dispatch and constitution hierarchy.

pub mod build_maps;
pub mod complex_constitution;
#[allow(clippy::module_inception)]
pub mod constitution;
pub mod simple_constitution;
pub mod trust;

pub use self::complex_constitution::ComplexConstitution;
pub use self::constitution::{
    builder_token, builder_token_dyn, register_builder_token, Constitution, ConstitutionFn,
};
pub use self::simple_constitution::SimpleConstitution;
