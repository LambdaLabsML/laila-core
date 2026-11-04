//! `laila.data.s3` -- AWS S3 pool backend.
//!
//! ```python
//! from .s3 import S3Pool
//! ```
//!
//! Host only (`s3` feature): the S3 REST API over rustls, SigV4-signed.

#[allow(clippy::module_inception)]
pub mod s3;

pub use s3::{S3Backend, S3Pool, S3Vendor};
