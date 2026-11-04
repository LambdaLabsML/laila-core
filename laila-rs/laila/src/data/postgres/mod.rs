//! `laila.data.postgres` -- PostgreSQL pool backend with an optional managed
//! local server.
//!
//! ```python
//! from .postgres import PostgresPool
//! ```
//!
//! Host only (`postgres` feature): the `postgres` crate (pure Rust, the
//! same wire protocol `psycopg` speaks).

#[allow(clippy::module_inception)]
pub mod postgres;

pub use postgres::{PostgresBackend, PostgresPool};
