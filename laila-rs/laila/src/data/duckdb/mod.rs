//! `laila.data.duckdb` -- DuckDB pool backend.
//!
//! ```python
//! from .duckdb import DuckDBPool
//! ```
//!
//! Host only (`duckdb` feature, the `laila-core[duckdb]` extra): links the
//! system `libduckdb` (DuckDB 1.5.5, the engine version of the Python
//! wheel).

#[allow(clippy::module_inception)]
pub mod duckdb;

pub use duckdb::{
    duckdb, DuckDBBackend, DuckDBPool, DUCKDB_CHECKPOINT_CHURN_BYTES, DUCKDB_CHECKPOINT_CHURN_ROWS,
    DUCKDB_COMPACT_MIN_BYTES,
};
