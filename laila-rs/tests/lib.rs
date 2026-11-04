//! `laila-tests`: the Python test tree, transpiled by `tools/py2rs`.
//!
//! Each `[[test]]` in `Cargo.toml` is one Python package directory; every
//! `.rs` beside its `main.rs` is the image of the `.py` of the same name.
//! Hand-written shared fixtures (the Python `conftest.py`) live here.

pub mod builders;
pub mod children;
pub mod comm_fakes;
pub mod compdata_roundtrip;
pub mod deep_eval_helpers;
pub mod hevc;
/// `import _hevc as H`: the module is also a value (`H.generate(..)`).
pub use hevc::hevc;
pub mod runtime_helpers;
pub mod streaming_helpers;
