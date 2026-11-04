//! `laila.data.schema` -- the pool schema package: the virtual data
//! container, the pool base class (`PoolBackend` / `PoolObject` / `Pool`),
//! the per-pool index and the manifest-scoped wrapper.

pub mod base;
pub mod data_container;
pub mod pool_index;
pub mod pool_wrapper;

pub use base::{
    AsPool, BoxFuture, MemoryBackend, Pool, PoolBackend, PoolObject, _LAILA_IDENTIFIABLE_POOL,
};
pub use data_container::{DataContainer, _LAILA_IDENTIFIABLE_DATA_CONTAINER};
pub use pool_index::PoolIndex;
pub use pool_wrapper::PoolWrapper;
