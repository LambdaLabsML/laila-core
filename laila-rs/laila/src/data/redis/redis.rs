//! Redis pool implementation with a managed private redis-server.
//!
//! Twin of `laila/data/redis/redis.py`: every pool launches its own
//! `redis-server` on a UNIX socket (no TCP), keeps the entries in one
//! hash (`key_prefix`), and asks the server to `SHUTDOWN SAVE` on
//! `close()` so the dump file (`laila_pool.rdb` under the pool's
//! directory) carries the data to the next instance with the same
//! identity. Where Python holds a `redis.Redis` client the port holds a
//! `redis::Connection` of the `redis` crate -- the same RESP protocol on
//! the same socket.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use parking_lot::Mutex;
use redis::{Commands, ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use sha1::{Digest, Sha1};

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::atomic::definitions::HasIdentity;
use crate::data::schema::base::{
    field_default, pool_class, private_default, Pool, PoolBackend, PoolObject, _PERSISTENT_STORAGE,
};
use crate::python::errors::ExcKind;
use crate::python::stdlib::os;
use crate::python::stdlib::subprocess::{atexit, Popen};
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

/// `redis.exceptions.RedisError` surfaced by the client (`ConnectionError`
/// for a lost socket, otherwise the closest base).
fn redis_error(e: redis::RedisError) -> LailaError {
    let kind = if e.is_connection_refusal() || e.is_io_error() || e.is_connection_dropped() {
        ExcKind::ConnectionError
    } else {
        ExcKind::Exception
    };
    LailaError::new(
        kind,
        alloc::format!(
            "redis.exceptions.{}: {e}",
            match kind {
                ExcKind::ConnectionError => "ConnectionError",
                _ => "ResponseError",
            }
        ),
    )
}

/// The managed server: the process and the client connected to it
/// (`self._redis_proc`, `self._client`). Shared with the `atexit` hook,
/// which keeps it alive until interpreter exit exactly as
/// `atexit.register(self.close)` keeps the Python pool alive.
pub struct RedisServer {
    proc_: Mutex<Option<Popen>>,
    client: Mutex<Option<redis::Connection>>,
    socket_path: String,
    password: Option<String>,
}

impl RedisServer {
    fn connection_info(&self, timeout: Option<core::time::Duration>) -> Result<redis::Connection> {
        let info = ConnectionInfo {
            addr: ConnectionAddr::Unix(self.socket_path.clone().into()),
            redis: RedisConnectionInfo {
                db: 0,
                username: None,
                password: self.password.clone(),
                protocol: redis::ProtocolVersion::RESP2,
            },
        };
        let client = redis::Client::open(info).map_err(redis_error)?;
        match timeout {
            Some(t) => client.get_connection_with_timeout(t),
            None => client.get_connection(),
        }
        .map_err(redis_error)
    }

    /// `close()`: ask Redis to save and exit; never delete dumps.
    fn close(&self) {
        let proc_ = self.proc_.lock().take();
        let Some(proc_) = proc_ else {
            return;
        };
        if let Some(mut client) = self.client.lock().take() {
            // `self._client.shutdown(save=True)` -- the server exits, so the
            // reply (if any) is an error we suppress.
            let _: core::result::Result<(), _> =
                redis::cmd("SHUTDOWN").arg("SAVE").query(&mut client);
        }
        proc_.terminate();
        if proc_.wait(Some(1.5)).is_err() {
            proc_.kill();
            let _ = proc_.wait(Some(1.5));
        }
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

/// `RedisPool`'s storage: the managed server and its client.
#[derive(Default)]
pub struct RedisBackend {
    server: Mutex<Option<Arc<RedisServer>>>,
}

impl core::fmt::Debug for RedisBackend {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("RedisBackend")
    }
}

/// `_db_dump_name`
const DB_DUMP_NAME: &str = "laila_pool.rdb";
/// `_redis_socket_name`
const REDIS_SOCKET_NAME: &str = "laila_redis.sock";

fn str_field(pool: &PoolObject, name: &str) -> Option<String> {
    match pool.field(name) {
        Some(Arg::Str(s)) => Some(String::from(&*s)),
        Some(Arg::None) | None => None,
        Some(other) => Some(other.__str__()),
    }
}

impl RedisBackend {
    /// Deterministic UNIX socket path derived from the pool UUID.
    pub fn _redis_socket_path(pool: &PoolObject) -> String {
        let digest = Sha1::digest(pool.uuid().as_bytes());
        let hex: String = digest.iter().map(|b| alloc::format!("{b:02x}")).collect();
        alloc::format!("/tmp/laila_redis_{}.sock", &hex[..16])
    }

    /// Redis hash key used to store all pool entries.
    pub fn redis_hash_key(pool: &PoolObject) -> String {
        str_field(pool, "key_prefix").unwrap_or_else(|| String::from("pool"))
    }

    /// Redis key used for distributed locking.
    pub fn redis_lock_key(pool: &PoolObject) -> String {
        str_field(pool, "lock_prefix").unwrap_or_else(|| String::from("pool_lock"))
    }

    fn server(&self) -> Result<Arc<RedisServer>> {
        self.server
            .lock()
            .clone()
            .ok_or_else(|| LailaError::runtime_error("Redis client not initialized."))
    }

    /// Run *f* on the client (`self._client`), `RuntimeError` when it is
    /// not initialised.
    fn with_client<T>(
        &self,
        f: impl FnOnce(&mut redis::Connection) -> redis::RedisResult<T>,
    ) -> Result<T> {
        let server = self.server()?;
        let mut guard = server.client.lock();
        match guard.as_mut() {
            None => Err(LailaError::runtime_error("Redis client not initialized.")),
            Some(conn) => f(conn).map_err(redis_error),
        }
    }

    /// Launch a `redis-server` subprocess on a UNIX socket.
    fn _start_redis_server(pool: &PoolObject, server: &RedisServer) -> Result<()> {
        let redis_dir = str_field(pool, "redis_dir")
            .filter(|d| !d.is_empty())
            .ok_or_else(|| {
                LailaError::runtime_error("Redis server not fully configured (missing redis_dir)")
            })?;
        let mut cmd: Vec<String> = [
            "redis-server",
            "--port",
            "0", // disable TCP
            "--unixsocket",
            &server.socket_path,
            "--unixsocketperm",
            "700",
            "--dir",
            &redis_dir,
            "--dbfilename",
            DB_DUMP_NAME,
            "--save",
            "900 1",
            "--save",
            "300 10",
            "--save",
            "60 10000",
            "--appendonly",
            "no",
            "--protected-mode",
            "yes",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        if let Some(pw) = &server.password {
            cmd.push(String::from("--requirepass"));
            cmd.push(pw.clone());
        }
        let proc_ = Popen::new(&cmd)?;
        *server.proc_.lock() = Some(proc_);

        let timeout = pool
            .field("server_start_timeout_s")
            .and_then(|v| v.as_f64())
            .unwrap_or(3.0);
        let deadline =
            std::time::Instant::now() + core::time::Duration::from_secs_f64(timeout.max(0.0));
        let mut last_err: Option<LailaError> = None;

        while std::time::Instant::now() < deadline {
            let exited = server.proc_.lock().as_ref().and_then(|p| p.poll());
            if let Some(exit_code) = exited {
                let (stdout, stderr) = {
                    let g = server.proc_.lock();
                    g.as_ref()
                        .map(|p| (p.read_stdout(), p.read_stderr()))
                        .unwrap_or_default()
                };
                server.close();
                return Err(LailaError::runtime_error(alloc::format!(
                    "redis-server exited early (code={exit_code}). Stdout: {} Stderr: {}",
                    stdout.trim(),
                    stderr.trim()
                )));
            }
            match server
                .connection_info(Some(core::time::Duration::from_millis(250)))
                .and_then(|mut r| {
                    redis::cmd("PING")
                        .query::<String>(&mut r)
                        .map_err(redis_error)
                }) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    last_err = Some(e);
                    std::thread::sleep(core::time::Duration::from_millis(50));
                }
            }
        }

        let (stdout, stderr) = {
            let g = server.proc_.lock();
            g.as_ref()
                .map(|p| (p.read_stdout(), p.read_stderr()))
                .unwrap_or_default()
        };
        server.close();
        Err(LailaError::runtime_error(alloc::format!(
            "redis-server failed to start on unix socket {}. Last error: {}. Stdout: {} Stderr: {}",
            server.socket_path,
            last_err
                .map(|e| e.message.to_string())
                .unwrap_or_else(|| String::from("None")),
            stdout.trim(),
            stderr.trim()
        )))
    }
}

impl PoolBackend for RedisBackend {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }

    /// Always: 1) establish `redis_dir` from `pool_id`, 2) start
    /// redis-server (unix socket only), 3) connect the client.
    fn model_post_init(&self, pool: &PoolObject) -> Result<()> {
        let pools = crate::macros::defaults::LAILA_DEFAULT_DIRECTORIES().__getitem__("pools")?;
        let pool_dir = os.path().join(crate::args!(pools, pool.uuid()))?;
        pool.set_field("redis_dir", pool_dir.clone());
        os.makedirs(crate::args!(pool_dir, exist_ok = true))?;

        let server = Arc::new(RedisServer {
            proc_: Mutex::new(None),
            client: Mutex::new(None),
            socket_path: Self::_redis_socket_path(pool),
            password: str_field(pool, "redis_password"),
        });
        // Remove a stale socket file (orphan processes are not handled).
        let _ = std::fs::remove_file(&server.socket_path);

        Self::_start_redis_server(pool, &server)?;

        let conn = server.connection_info(None)?;
        *server.client.lock() = Some(conn);
        *self.server.lock() = Some(server.clone());

        // `atexit.register(self.close)`
        atexit::register(move || server.close());
        Ok(())
    }

    /// Clean shutdown: ask Redis to save and exit; never delete dumps.
    fn close(&self, _pool: &PoolObject) -> Option<Result<()>> {
        if let Some(server) = self.server.lock().clone() {
            server.close();
        }
        Some(Ok(()))
    }

    /// Retrieve the JSON value for *key*, or `None` if absent.
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        let hash = Self::redis_hash_key(pool);
        let value: Option<String> = self.with_client(|c| c.hget(&hash, key))?;
        match value {
            Some(text) => crate::_compat::pyjson::loads(&text),
            None => Ok(Arg::None),
        }
    }

    /// Store *entry* as a JSON string in the Redis hash.
    fn _write(&self, pool: &PoolObject, key: &str, entry: Arg) -> Result<()> {
        let value = match entry {
            Arg::Dict(_) => Arg::Str(
                crate::_compat::pyjson::dumps(
                    &entry,
                    &crate::_compat::pyjson::DumpsOptions::default(),
                )?
                .into(),
            ),
            other => other,
        };
        self.server()?;
        let Arg::Str(value) = value else {
            return Err(LailaError::type_error(
                "RedisPool expects a serialized JSON string.",
            ));
        };
        let hash = Self::redis_hash_key(pool);
        self.with_client(|c| c.hset::<_, _, _, ()>(&hash, key, &*value))
    }

    /// Delete *key* from the Redis hash.
    fn _delete(&self, pool: &PoolObject, key: &str) -> Result<()> {
        let hash = Self::redis_hash_key(pool);
        self.with_client(|c| c.hdel::<_, _, ()>(&hash, key))
    }

    /// Remove all entries from the pool (deletes the Redis hash).
    fn _empty(&self, pool: &PoolObject) -> Result<()> {
        let hash = Self::redis_hash_key(pool);
        self.with_client(|c| c.del::<_, ()>(&hash))
    }

    /// `True` if *key* exists in the Redis hash.
    fn _exists(&self, pool: &PoolObject, key: &str) -> Result<bool> {
        let hash = Self::redis_hash_key(pool);
        self.with_client(|c| c.hexists(&hash, key))
    }

    /// Return all keys in the Redis hash.
    fn _keys(&self, pool: &PoolObject) -> Result<Vec<String>> {
        let hash = Self::redis_hash_key(pool);
        self.with_client(|c| c.hkeys(&hash))
    }

    fn __getattr__(&self, pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        match name {
            "_client" => Some(Ok(match self.server.lock().as_ref() {
                Some(s) if s.client.lock().is_some() => Arg::Str("<redis.Redis>".into()),
                _ => Arg::None,
            })),
            "_redis_proc" => Some(Ok(match self.server.lock().as_ref() {
                Some(s) if s.proc_.lock().is_some() => Arg::Str("<Popen: redis-server>".into()),
                _ => Arg::None,
            })),
            "redis_hash_key" => Some(Ok(Self::redis_hash_key(pool).into_arg())),
            "redis_lock_key" => Some(Ok(Self::redis_lock_key(pool).into_arg())),
            "_redis_socket_path" => Some(Ok(Self::_redis_socket_path(pool).into_arg())),
            "_db_dump_name" => Some(Ok(DB_DUMP_NAME.into_arg())),
            "_redis_socket_name" => Some(Ok(REDIS_SOCKET_NAME.into_arg())),
            _ => None,
        }
    }

    fn method_names(&self) -> &'static [&'static str] {
        &["close", "_start_redis_server"]
    }
}

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "RedisPool",
        "laila.data.redis.redis",
        alloc::vec![],
        alloc::vec![
            // Redis key namespaces
            (String::from("key_prefix"), field_default("str", "pool")),
            (
                String::from("lock_prefix"),
                field_default("str", "pool_lock")
            ),
            // Optional auth
            (
                String::from("redis_password"),
                field_default("str | None", Arg::None)
            ),
            // Behavior
            (
                String::from("server_start_timeout_s"),
                field_default("float", Arg::Float(3.0))
            ),
            (
                String::from("lock_timeout"),
                field_default("int", Arg::Int(30))
            ),
            (
                String::from("redis_dir"),
                field_default("str | None", Arg::None)
            ),
            (
                String::from("transformations"),
                field_default(
                    "TransformationSequence | None",
                    crate::entry::transformation_base64()
                )
            ),
        ],
        alloc::vec![
            (String::from("_client"), private_default(Arg::None)),
            (String::from("_redis_proc"), private_default(Arg::None)),
            (String::from("_db_dump_name"), private_default(DB_DUMP_NAME)),
            (
                String::from("_redis_socket_name"),
                private_default(REDIS_SOCKET_NAME)
            ),
        ],
        alloc::vec![(String::from(_PERSISTENT_STORAGE), Arg::Bool(true))],
    )
});

crate::laila_pool_handle!(
    /// Redis-backed Pool using a private redis-server over a UNIX socket.
    ///
    /// Persistence:
    /// - Data directory: `<LAILA_DEFAULT_DIRECTORIES["pools"]>/<pool.uuid>/`
    /// - Dump file (private): `laila_pool.rdb`
    /// - If the dump exists, Redis loads it automatically.
    /// - Dumps are never deleted.
    ///
    /// Notes: no TCP ports, no orphan-process handling, values stored are
    /// serialized JSON strings.
    RedisPool,
    RedisBackend
);

impl ModelBase for RedisPool {
    fn __class__() -> &'static ModelClass {
        RedisPool::__class__()
    }
}

impl RedisPool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `RedisPool(key_prefix="pool", lock_prefix="pool_lock", redis_password=None, server_start_timeout_s=3.0, lock_timeout=30, redis_dir=None, **kwargs)`
    #[pysig("RedisPool(**kwargs)")]
    pub fn new(args: impl Into<Args>) -> Result<RedisPool> {
        Ok(RedisPool(Pool::construct(
            Self::__class__(),
            kwargs,
            |_inst: &ModelInstance| Ok(Box::new(RedisBackend::default())),
        )?))
    }
    /// `pool.redis_hash_key`
    pub fn redis_hash_key(&self) -> String {
        RedisBackend::redis_hash_key(self.0.obj())
    }
    /// `pool.redis_lock_key`
    pub fn redis_lock_key(&self) -> String {
        RedisBackend::redis_lock_key(self.0.obj())
    }
    /// `pool.redis_password`
    pub fn redis_password(&self) -> Arg {
        self.0.field("redis_password").unwrap_or(Arg::None)
    }
    /// `pool.redis_dir`
    pub fn redis_dir(&self) -> Arg {
        self.0.field("redis_dir").unwrap_or(Arg::None)
    }
    /// `pool._redis_socket_path`
    pub fn _redis_socket_path(&self) -> String {
        RedisBackend::_redis_socket_path(self.0.obj())
    }
    /// `pool.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
    /// `pool._client`
    pub fn _client(&self) -> Arg {
        self.0.obj().__getattr__("_client").unwrap_or(Arg::None)
    }
}

impl crate::python::builtins::CallValue for RedisPool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        RedisPool::new(args).map(IntoArg::into_arg)
    }
}

/// `RedisPool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    RedisPool::__class__().as_pyclass().into_arg()
}

impl PyArg for RedisBackend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
