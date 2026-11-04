//! `laila.data.gcs` -- Google Cloud Storage pool backend.
//!
//! ```python
//! from .gcs import GCSPool
//! ```
//!
//! Host only (`gcs` feature): the Cloud Storage JSON API over rustls with
//! an OAuth2 bearer token (service account / Application Default
//! Credentials), or anonymous against `STORAGE_EMULATOR_HOST`.

#[allow(clippy::module_inception)]
pub mod gcs;

pub use gcs::{GCSBackend, GCSPool, GcsClient};
