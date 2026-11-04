//! `laila.data.huggingface` -- Hugging Face Hub pool backend.
//!
//! ```python
//! from .huggingface import HuggingFacePool
//! ```
//!
//! Host only (`huggingface` feature): the Hub's resolve / tree / commit
//! HTTP API over rustls, with the token resolution of `huggingface_hub`
//! (explicit, `HF_TOKEN`, the cached login).

#[allow(clippy::module_inception)]
pub mod huggingface;

pub use huggingface::{HfApi, HuggingFaceBackend, HuggingFacePool};
