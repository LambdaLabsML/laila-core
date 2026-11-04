//! Data containers: storage pools for every supported backend, plus `MultiBuffer`.
//!
//! Everything here derives from the virtual
//! [`_LAILA_IDENTIFIABLE_DATA_CONTAINER`](schema::data_container). Two
//! families exist:
//!
//! - **Pools** (`_LAILA_IDENTIFIABLE_POOL` subclasses) -- maps keyed by
//!   entry `global_id`; the persistence tier behind `memorize` /
//!   `remember` / `forget`.
//! - [`MultiBuffer`] -- an integer-indexed ring with independent
//!   read/write heads, used as the proxy to a device's own buffer (e.g. a
//!   camera's double-buffered frames on a microcontroller).
//!
//! Each pool backend lives in its own sub-module and exposes a single
//! `*Pool` class:
//!
//! | Backend | Class | Rust status |
//! |---|---|---|
//! | (in-memory) | `_LAILA_IDENTIFIABLE_POOL` = [`DefaultPool`] | ported |
//! | HDF5 | [`HDF5Pool`] | ported (`hdf5` feature = the `[hdf5]` extra; system `libhdf5`) |
//! | SQLite | [`SQLitePool`] | ported (`sqlite` feature; system `libsqlite3`) |
//! | Filesystem (loop-mounted ext4 image) | [`FilesystemPool`] | ported (`std`; `mkfs.ext4` / `mount`) |
//! | DuckDB | [`DuckDBPool`] | ported (`duckdb` feature = the `[duckdb]` extra; system `libduckdb` 1.5.5) |
//! | AWS S3 (boto3) | [`S3Pool`] | ported (`s3` feature; SigV4 REST over `ureq`) |
//! | Cloudflare R2 (boto3) | [`CloudflarePool`] | ported (`cloudflare` feature; same chassis) |
//! | Backblaze B2 (boto3) | [`BackblazePool`] | ported (`backblaze` feature; same chassis) |
//! | Google Cloud Storage | [`GCSPool`] | ported (`gcs` feature; JSON API, service-account JWT / ADC / emulator) |
//! | Azure Blob Storage | [`AzurePool`] | ported (`azure` feature; Blob REST, SharedKey / SAS / Azurite) |
//! | Hugging Face Hub | [`HuggingFacePool`] | ported (`huggingface` feature; Hub resolve / tree / commit API) |
//! | Redis (managed `redis-server`) | [`RedisPool`] | ported (`redis` feature; `redis` crate) |
//! | PostgreSQL (optional managed `initdb` / `postgres`) | [`PostgresPool`] | ported (`postgres` feature; `postgres` crate) |
//! | MongoDB (optional managed `mongod`) | [`MongoPool`] | ported (`mongo` feature; `mongodb` sync driver) |
//!
//! `docs/PARITY.md` records, per backend, what the Python SDK did that the
//! port re-implements on the wire, and the known behavioural differences.
//!
//! Pools are designed to be composed via the proxy operators
//! (`cache << origin` / `origin >> cache`) so users can stack a fast local
//! tier in front of a slower remote tier without the rest of the codebase
//! ever knowing.
//!
//! Writing a backend
//! -----------------
//! 1. Declare the backend struct and `impl PoolBackend for It` -- override
//!    only the hooks the Python class overrides (`_read`, `_write`,
//!    `_delete`, `_exists`, `_keys`, `_empty`, the `_async` twins, `sync`,
//!    `close`, extra attributes / methods).
//! 2. Declare its [`ModelClass`](crate::_compat::pydantic::ModelClass) with
//!    [`schema::base::pool_class`]: the Python fields
//!    (`file_path: str | None = Field(default=None)` →
//!    `("file_path", field_default("str | None", Arg::None))`,
//!    `transformations = Field(default=transformation_base64)` →
//!    `field_default(.., transformation_base64())`), private attributes
//!    and `("_persistent_storage", Arg::Bool(true))`.
//! 3. Declare the handle with [`laila_pool_handle!`] and a constructor that
//!    calls [`Pool::construct`](schema::base::Pool::construct) -- the
//!    pydantic pipeline (`laila.args` resolution, identity, coercion, the
//!    `transformations=None` rejection for persistent backends) runs
//!    there; the backend's `model_post_init` (open the file / connection)
//!    runs on the handle afterwards.
//!
//! See `s3/s3.rs` for the smallest complete example (a `BotoPool`
//! vendor), `sqlite/sqlite.rs` for a complete persistent backend.
//!
//! Like the Python package, where each import is guarded by
//! `try/except ModuleNotFoundError` so a missing optional dependency only
//! hides that backend's `*Pool` class, a backend whose feature (its pip
//! extra: `duckdb`, `hdf5`, `s3`, `redis`, ...) is off is simply absent.

#[cfg(feature = "azure")]
pub mod azure;
#[cfg(feature = "backblaze")]
pub mod backblaze;
#[cfg(feature = "http")]
pub mod boto;
#[cfg(feature = "cloudflare")]
pub mod cloudflare;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "std")]
pub mod filesystem;
#[cfg(feature = "gcs")]
pub mod gcs;
#[cfg(feature = "hdf5")]
pub mod hdf5;
#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "huggingface")]
pub mod huggingface;
#[cfg(feature = "mongo")]
pub mod mongo;
pub mod multibuffer;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "s3")]
pub mod s3;
pub mod schema;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "azure")]
pub use azure::AzurePool;
#[cfg(feature = "backblaze")]
pub use backblaze::BackblazePool;
#[cfg(feature = "http")]
pub use boto::boto::BotoPool;
#[cfg(feature = "cloudflare")]
pub use cloudflare::CloudflarePool;
#[cfg(feature = "duckdb")]
pub use duckdb::DuckDBPool;
#[cfg(feature = "std")]
pub use filesystem::FilesystemPool;
#[cfg(feature = "gcs")]
pub use gcs::GCSPool;
#[cfg(feature = "hdf5")]
pub use hdf5::HDF5Pool;
#[cfg(feature = "huggingface")]
pub use huggingface::HuggingFacePool;
#[cfg(feature = "mongo")]
pub use mongo::MongoPool;
pub use multibuffer::{MultiBuffer, MultiBufferHandle};
#[cfg(feature = "postgres")]
pub use postgres::PostgresPool;
#[cfg(feature = "redis")]
pub use redis::RedisPool;
#[cfg(feature = "s3")]
pub use s3::S3Pool;
pub use schema::base::{
    AsPool, BoxFuture, MemoryBackend, Pool, PoolBackend, PoolObject, _LAILA_IDENTIFIABLE_POOL,
};
pub use schema::data_container::{DataContainer, _LAILA_IDENTIFIABLE_DATA_CONTAINER};
pub use schema::pool_index::PoolIndex;
pub use schema::pool_wrapper::PoolWrapper;
#[cfg(feature = "sqlite")]
pub use sqlite::SQLitePool;

/// `DefaultPool = _LAILA_IDENTIFIABLE_POOL` (`laila.macros.defaults`): the
/// in-memory pool. The same type, so `laila.DefaultPool is
/// _LAILA_IDENTIFIABLE_POOL` holds.
pub use schema::base::_LAILA_IDENTIFIABLE_POOL as DefaultPool;

/// `__name__` of the Python package.
pub const __name__: &str = "laila.data";

/// Make every pool class of this build known to
/// `cli_capable::_resolve_class` (Python walks `__subclasses__()`; the
/// Rust classes register on first use, so `_load_environment` touches
/// them all first).
pub fn register_pool_classes() {
    let _ = _LAILA_IDENTIFIABLE_POOL::__class__();
    let _ = MultiBuffer::__class__();
    #[cfg(feature = "std")]
    let _ = FilesystemPool::__class__();
    #[cfg(feature = "sqlite")]
    let _ = SQLitePool::__class__();
    #[cfg(feature = "duckdb")]
    let _ = DuckDBPool::__class__();
    #[cfg(feature = "hdf5")]
    let _ = HDF5Pool::__class__();
    #[cfg(feature = "http")]
    let _ = BotoPool::__class__();
    #[cfg(feature = "s3")]
    let _ = S3Pool::__class__();
    #[cfg(feature = "cloudflare")]
    let _ = CloudflarePool::__class__();
    #[cfg(feature = "backblaze")]
    let _ = BackblazePool::__class__();
    #[cfg(feature = "gcs")]
    let _ = GCSPool::__class__();
    #[cfg(feature = "azure")]
    let _ = AzurePool::__class__();
    #[cfg(feature = "huggingface")]
    let _ = HuggingFacePool::__class__();
    #[cfg(feature = "redis")]
    let _ = RedisPool::__class__();
    #[cfg(feature = "postgres")]
    let _ = PostgresPool::__class__();
    #[cfg(feature = "mongo")]
    let _ = MongoPool::__class__();
}

/// `cls(**kwargs)` for the pool class named `token` (`class_token` of an
/// environment dump): the typed constructor, which also wires the storage
/// backend. `None` when no pool class of this build has that `__name__`.
pub fn construct_pool(token: &str, kwargs: crate::python::Args) -> Option<crate::Result<Pool>> {
    #[allow(unused_imports)]
    use crate::python::IntoArg;
    // Every backend but the in-memory base is feature-gated.
    #[allow(unused_variables)]
    let as_pool = |r: crate::Result<crate::Arg>| -> crate::Result<Pool> {
        match r? {
            crate::Arg::Pool(p) => Ok(p),
            other => Err(crate::LailaError::type_error(alloc::format!(
                "{token}(..) did not construct a pool (got {})",
                other.type_name()
            ))),
        }
    };
    Some(match token {
        "_LAILA_IDENTIFIABLE_POOL" => _LAILA_IDENTIFIABLE_POOL::new(kwargs),
        #[cfg(feature = "std")]
        "FilesystemPool" => as_pool(FilesystemPool::new(kwargs).map(IntoArg::into_arg)),
        #[cfg(feature = "sqlite")]
        "SQLitePool" => as_pool(SQLitePool::new(kwargs).map(IntoArg::into_arg)),
        #[cfg(feature = "duckdb")]
        "DuckDBPool" => as_pool(DuckDBPool::new(kwargs).map(IntoArg::into_arg)),
        #[cfg(feature = "hdf5")]
        "HDF5Pool" => as_pool(HDF5Pool::new(kwargs).map(IntoArg::into_arg)),
        #[cfg(feature = "s3")]
        "S3Pool" => as_pool(S3Pool::new(kwargs).map(IntoArg::into_arg)),
        #[cfg(feature = "cloudflare")]
        "CloudflarePool" => as_pool(CloudflarePool::new(kwargs).map(IntoArg::into_arg)),
        #[cfg(feature = "backblaze")]
        "BackblazePool" => as_pool(BackblazePool::new(kwargs).map(IntoArg::into_arg)),
        #[cfg(feature = "gcs")]
        "GCSPool" => as_pool(GCSPool::new(kwargs).map(IntoArg::into_arg)),
        #[cfg(feature = "azure")]
        "AzurePool" => as_pool(AzurePool::new(kwargs).map(IntoArg::into_arg)),
        #[cfg(feature = "huggingface")]
        "HuggingFacePool" => as_pool(HuggingFacePool::new(kwargs).map(IntoArg::into_arg)),
        #[cfg(feature = "redis")]
        "RedisPool" => as_pool(RedisPool::new(kwargs).map(IntoArg::into_arg)),
        #[cfg(feature = "postgres")]
        "PostgresPool" => as_pool(PostgresPool::new(kwargs).map(IntoArg::into_arg)),
        #[cfg(feature = "mongo")]
        "MongoPool" => as_pool(MongoPool::new(kwargs).map(IntoArg::into_arg)),
        _ => return None,
    })
}
