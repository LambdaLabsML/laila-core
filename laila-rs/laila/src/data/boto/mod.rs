//! `laila.data.boto` -- the shared chassis of every S3-API-compatible pool
//! (`S3Pool`, `CloudflarePool`, `BackblazePool`).
//!
//! ```python
//! from .boto import BotoPool
//! ```
//!
//! Host only (`s3` / `cloudflare` / `backblaze` features): REST over
//! rustls, SigV4-signed in-tree.

#[allow(clippy::module_inception)]
pub mod boto;

pub use boto::{BotoBackend, BotoVendor, S3Client, S3Config, S3Error};
