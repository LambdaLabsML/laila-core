//! `laila.data.mongo` -- MongoDB pool backend with an optional managed local
//! server.
//!
//! ```python
//! from .mongo import MongoPool
//! ```
//!
//! Host only (`mongo` feature): the official `mongodb` driver (sync API).

#[allow(clippy::module_inception)]
pub mod mongo;

pub use mongo::{MongoBackend, MongoPool};
