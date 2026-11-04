//! `laila.data.backblaze` -- Backblaze B2 pool backend (S3-compatible API).
//!
//! ```python
//! from .backblaze import BackblazePool
//! ```
//!
//! Host only (`backblaze` feature).

#[allow(clippy::module_inception)]
pub mod backblaze;

pub use backblaze::{BackblazeBackend, BackblazePool, BackblazeVendor};
