//! `laila.data.azure` -- Azure Blob Storage pool backend.
//!
//! ```python
//! from .azure import AzurePool
//! ```
//!
//! Host only (`azure` feature): the Blob service REST API over rustls,
//! authenticated the way `azure-storage-blob` does from a connection
//! string -- SharedKey (HMAC-SHA256 over the canonicalized request), a
//! SAS token, or the Azurite development account.

#[allow(clippy::module_inception)]
pub mod azure;

pub use azure::{AzureBackend, AzurePool, BlobServiceClient};
