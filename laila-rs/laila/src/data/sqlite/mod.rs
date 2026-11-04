//! `laila.data.sqlite` -- SQLite pool backend.
//!
//! ```python
//! from .sqlite import SQLitePool
//! ```
//!
//! Host only (`sqlite` feature): links the system `libsqlite3`.

#[allow(clippy::module_inception)]
pub mod sqlite;

pub use sqlite::{SQLiteBackend, SQLitePool};
