//! `laila.data.filesystem` -- loopback-mounted filesystem pool backend.
//!
//! ```python
//! from .filesystem import FilesystemPool
//! ```
//!
//! Host only (`std`): files, `mkfs.ext4` and `mount`.

#[allow(clippy::module_inception)]
pub mod filesystem;

pub use filesystem::{FilesystemBackend, FilesystemPool};
