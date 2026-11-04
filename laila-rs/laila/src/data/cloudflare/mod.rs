//! `laila.data.cloudflare` -- Cloudflare R2 pool backend (S3-compatible API).
//!
//! ```python
//! from .cloudflare import CloudflarePool
//! ```
//!
//! Host only (`cloudflare` feature).

#[allow(clippy::module_inception)]
pub mod cloudflare;

pub use cloudflare::{CloudflareBackend, CloudflarePool, CloudflareVendor};
