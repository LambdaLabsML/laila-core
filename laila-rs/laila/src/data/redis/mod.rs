//! `laila.data.redis` -- Redis pool backend with a managed private
//! `redis-server`.
//!
//! ```python
//! from .redis import RedisPool
//! ```
//!
//! Host only (`redis` feature): the `redis` crate over the server's UNIX
//! socket.

#[allow(clippy::module_inception)]
pub mod redis;

pub use redis::{RedisBackend, RedisPool};
