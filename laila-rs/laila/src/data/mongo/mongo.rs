//! MongoDB-backed pool implementation with optional managed local server.
//!
//! Twin of `laila/data/mongo/mongo.py`: entries are documents
//! `{"key": .., "value": ..}` of the `laila_pool_entries` collection
//! (unique index on `key`) of database `dbname`. The pool connects to an
//! existing deployment (`uri`, or `host` / `port`) or, given neither,
//! launches and manages a local `mongod` under the pool's directory,
//! reusing one that is already up. Where Python holds a `pymongo`
//! `MongoClient` the port holds the official Rust driver's sync client.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};

use mongodb::bson::{doc, Document};
use mongodb::options::{ClientOptions, IndexOptions};
use mongodb::sync::{Client, Collection};
use mongodb::IndexModel;
use parking_lot::{Mutex, RwLock};
use sha1::{Digest, Sha1};

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::atomic::definitions::HasIdentity;
use crate::data::schema::base::{
    field_default, pool_class, private_default, Pool, PoolBackend, PoolObject, _PERSISTENT_STORAGE,
};
use crate::python::errors::ExcKind;
use crate::python::mock::call_patched;
use crate::python::stdlib::os;
use crate::python::stdlib::subprocess::{atexit, Popen};
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

const CLASS_NAME: &str = "MongoPool";
const COLLECTION: &str = "laila_pool_entries";

/// `pymongo.errors.PyMongoError` surfaced by the driver
/// (`ServerSelectionTimeoutError` when no server answered).
fn mongo_error(e: mongodb::error::Error) -> LailaError {
    let text = e.to_string();
    let kind = if text.contains("Server selection timeout")
        || text.contains("Connection refused")
        || text.contains("I/O error")
    {
        ExcKind::ConnectionError
    } else {
        ExcKind::Exception
    };
    LailaError::new(
        kind,
        alloc::format!(
            "pymongo.errors.{}: {text}",
            if kind == ExcKind::ConnectionError {
                "ServerSelectionTimeoutError"
            } else {
                "PyMongoError"
            }
        ),
    )
}

/// The patched twin of a method, when `patch.object(MongoPool, name, ..)`
/// is active: the mock's result.
macro_rules! patched {
    ($pool:expr, $name:literal) => {
        if let Some(r) = call_patched(CLASS_NAME, $name, $pool.handle(), Args::new()) {
            return r.and_then(|a| crate::python::RetInto::ret_into(a));
        }
    };
}

/// `self._client`: the driver's client, or the stand-in a patched
/// `MongoClient` returned / a test assigned (`pool._client = Mock()`).
pub enum MongoClient {
    Real(Client),
    Mock(Arg),
}

impl MongoClient {
    /// `client.close()`
    fn close(self) {
        match self {
            MongoClient::Real(c) => c.shutdown().run(),
            MongoClient::Mock(m) => {
                let _ = m.__call_method__("close", Args::new());
            }
        }
    }
    fn as_arg(&self) -> Arg {
        match self {
            MongoClient::Real(_) => Arg::Str("<pymongo.MongoClient>".into()),
            MongoClient::Mock(m) => m.clone(),
        }
    }
}

/// `self._mongo_proc`: the managed `mongod` child, or the stand-in a test
/// assigned (`pool._mongo_proc = Mock()`).
pub enum MongoProc {
    Real(Popen),
    Mock(Arg),
}

impl MongoProc {
    fn terminate(&self) {
        match self {
            MongoProc::Real(p) => p.terminate(),
            MongoProc::Mock(m) => {
                let _ = m.__call_method__("terminate", Args::new());
            }
        }
    }
    fn kill(&self) {
        match self {
            MongoProc::Real(p) => p.kill(),
            MongoProc::Mock(m) => {
                let _ = m.__call_method__("kill", Args::new());
            }
        }
    }
    fn wait(&self, timeout: Option<f64>) -> Result<()> {
        match self {
            MongoProc::Real(p) => p.wait(timeout).map(|_| ()),
            MongoProc::Mock(m) => {
                let mut a = Args::new();
                if let Some(t) = timeout {
                    a.push_keyword("timeout", Arg::Float(t));
                }
                m.__call_method__("wait", a).map(|_| ())
            }
        }
    }
    fn poll(&self) -> Option<i32> {
        match self {
            MongoProc::Real(p) => p.poll(),
            MongoProc::Mock(_) => None,
        }
    }
    fn output(&self) -> (String, String) {
        match self {
            MongoProc::Real(p) => (p.read_stdout(), p.read_stderr()),
            MongoProc::Mock(_) => (String::new(), String::new()),
        }
    }
    fn as_arg(&self) -> Arg {
        match self {
            MongoProc::Real(_) => Arg::Str("<Popen: mongod>".into()),
            MongoProc::Mock(m) => m.clone(),
        }
    }
}

/// The client and the managed server, shared with the `atexit` hook
/// (`atexit.register(self.close)`).
pub struct MongoState {
    client: Mutex<Option<MongoClient>>,
    proc_: Mutex<Option<MongoProc>>,
    owns_local_server: AtomicBool,
    mongo_dir: RwLock<Option<String>>,
}

impl MongoState {
    /// Close the client and terminate any managed mongod process.
    fn close(&self) {
        if let Some(client) = self.client.lock().take() {
            client.close();
        }
        let proc_ = self.proc_.lock().take();
        if let Some(proc_) = proc_ {
            if self.owns_local_server.load(Ordering::SeqCst) {
                proc_.terminate();
                if proc_.wait(Some(2.0)).is_err() {
                    proc_.kill();
                    let _ = proc_.wait(Some(2.0));
                }
            }
        }
        self.owns_local_server.store(false, Ordering::SeqCst);
    }
}

/// `MongoPool`'s storage.
pub struct MongoBackend {
    state: Arc<MongoState>,
}

impl Default for MongoBackend {
    fn default() -> Self {
        MongoBackend {
            state: Arc::new(MongoState {
                client: Mutex::new(None),
                proc_: Mutex::new(None),
                owns_local_server: AtomicBool::new(false),
                mongo_dir: RwLock::new(None),
            }),
        }
    }
}

impl core::fmt::Debug for MongoBackend {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("MongoBackend")
    }
}

fn str_field(pool: &PoolObject, name: &str) -> Option<String> {
    match pool.field(name) {
        Some(Arg::Str(s)) => Some(String::from(&*s)),
        Some(Arg::None) | None => None,
        Some(other) => Some(other.__str__()),
    }
}

fn port_field(pool: &PoolObject) -> i64 {
    pool.field("port").and_then(|v| v.as_i64()).unwrap_or(27017)
}

/// `MongoClient(uri, serverSelectionTimeoutMS=..)` -- the stand-in when
/// `patch("laila.data.mongo.mongo.MongoClient", ..)` is active.
fn mongo_client(uri: &str, server_selection_timeout_ms: u64) -> Result<MongoClient> {
    if let Some(r) =
        crate::python::mock::call_patched_module("laila.data.mongo.mongo", "MongoClient", {
            let mut a = Args::from(uri.into_arg());
            a.push_keyword(
                "serverSelectionTimeoutMS",
                Arg::Int(server_selection_timeout_ms as i64),
            );
            a
        })
    {
        return r.map(MongoClient::Mock);
    }
    let mut options = ClientOptions::parse(uri).run().map_err(mongo_error)?;
    options.server_selection_timeout = Some(core::time::Duration::from_millis(
        server_selection_timeout_ms,
    ));
    Client::with_options(options)
        .map(MongoClient::Real)
        .map_err(mongo_error)
}

/// `client.admin.command("ping")` -- `True` when the server answered.
fn ping_client(client: MongoClient) -> Result<()> {
    match client {
        MongoClient::Real(c) => {
            let r = c
                .database("admin")
                .run_command(doc! {"ping": 1})
                .run()
                .map(|_| ())
                .map_err(mongo_error);
            c.shutdown().run();
            r
        }
        MongoClient::Mock(m) => {
            let admin = m.__getattr__("admin")?;
            admin.__call_method__("command", Args::from("ping".into_arg()))?;
            m.__call_method__("close", Args::new())?;
            Ok(())
        }
    }
}

impl MongoBackend {
    /// Establish a `MongoClient` connection.
    fn _connect(&self, pool: &PoolObject) -> Result<MongoClient> {
        if let Some(uri) = str_field(pool, "uri") {
            return mongo_client(&uri, 1000);
        }
        if str_field(pool, "host").is_none() {
            self._configure_local_server(pool)?;
            self._ensure_local_server(pool)?;
            return mongo_client(&self._local_uri(pool)?, 1000);
        }
        let uri = alloc::format!(
            "mongodb://{}:{}/",
            str_field(pool, "host").unwrap_or_default(),
            port_field(pool)
        );
        mongo_client(&uri, 1000)
    }

    /// Set up directory and port for a managed local mongod.
    pub fn _configure_local_server(&self, pool: &PoolObject) -> Result<()> {
        patched!(pool, "_configure_local_server");
        if self.state.mongo_dir.read().is_some() {
            return Ok(());
        }
        let pools = crate::macros::defaults::LAILA_DEFAULT_DIRECTORIES().__getitem__("pools")?;
        let pool_dir = os.path().join(crate::args!(pools, pool.uuid()))?;
        os.makedirs(crate::args!(pool_dir.clone(), exist_ok = true))?;
        *self.state.mongo_dir.write() = Some(pool_dir);
        let digest = Sha1::digest(pool.pool_id().as_bytes());
        let hex: String = digest.iter().map(|b| alloc::format!("{b:02x}")).collect();
        let port = 30000 + (i64::from_str_radix(&hex[..8], 16).unwrap_or(0) % 20000);
        pool.set_field("port", Arg::Int(port));
        pool.set_field("host", "127.0.0.1");
        Ok(())
    }

    /// MongoDB connection URI for the managed local server.
    pub fn _local_uri(&self, pool: &PoolObject) -> Result<String> {
        let host = str_field(pool, "host")
            .ok_or_else(|| LailaError::runtime_error("Local Mongo host is not configured."))?;
        Ok(alloc::format!("mongodb://{host}:{}/", port_field(pool)))
    }

    /// Start a local mongod if one is not already reachable.
    pub fn _ensure_local_server(&self, pool: &PoolObject) -> Result<()> {
        patched!(pool, "_ensure_local_server");
        if self._local_server_available(pool)? {
            self.state.owns_local_server.store(false, Ordering::SeqCst);
            return Ok(());
        }
        for _attempt in 0..5 {
            match self._start_local_server(pool) {
                Ok(()) => return Ok(()),
                Err(e) if e.kind == ExcKind::RuntimeError => {
                    if !e.message.contains("code=48") {
                        return Err(e);
                    }
                    std::thread::sleep(core::time::Duration::from_secs(1));
                    if self._local_server_available(pool)? {
                        self.state.owns_local_server.store(false, Ordering::SeqCst);
                        return Ok(());
                    }
                    pool.set_field("port", Arg::Int(port_field(pool) + 1));
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// `True` if the local mongod responds to a ping.
    pub fn _local_server_available(&self, pool: &PoolObject) -> Result<bool> {
        patched!(pool, "_local_server_available");
        let Ok(uri) = self._local_uri(pool) else {
            return Ok(false);
        };
        Ok(Self::ping(&uri))
    }

    fn ping(uri: &str) -> bool {
        mongo_client(uri, 500).and_then(ping_client).is_ok()
    }

    /// Launch a `mongod` subprocess and wait for readiness.
    pub fn _start_local_server(&self, pool: &PoolObject) -> Result<()> {
        patched!(pool, "_start_local_server");
        let mongo_dir = self.state.mongo_dir.read().clone();
        let host = str_field(pool, "host");
        let (Some(mongo_dir), Some(host)) = (mongo_dir, host) else {
            return Err(LailaError::runtime_error(
                "Local Mongo server is not fully configured.",
            ));
        };
        let cmd = [
            String::from("mongod"),
            String::from("--dbpath"),
            mongo_dir,
            String::from("--port"),
            port_field(pool).to_string(),
            String::from("--bind_ip"),
            host,
            String::from("--quiet"),
        ];
        *self.state.proc_.lock() = Some(MongoProc::Real(Popen::new(&cmd)?));
        self.state.owns_local_server.store(true, Ordering::SeqCst);

        let timeout = pool
            .field("server_start_timeout_s")
            .and_then(|v| v.as_f64())
            .unwrap_or(5.0);
        let deadline =
            std::time::Instant::now() + core::time::Duration::from_secs_f64(timeout.max(0.0));
        let uri = self._local_uri(pool)?;
        let mut last_err: Option<String> = None;
        while std::time::Instant::now() < deadline {
            let exited = self.state.proc_.lock().as_ref().and_then(|p| p.poll());
            if let Some(exit_code) = exited {
                let (stdout, stderr) = self.proc_output();
                self.state.close();
                return Err(LailaError::runtime_error(alloc::format!(
                    "mongod exited early (code={exit_code}). Stdout: {} Stderr: {}",
                    stdout.trim(),
                    stderr.trim()
                )));
            }
            match mongo_client(&uri, 500).and_then(ping_client) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_err = Some(e.message.to_string());
                    std::thread::sleep(core::time::Duration::from_millis(100));
                }
            }
        }
        let (stdout, stderr) = self.proc_output();
        self.state.close();
        Err(LailaError::runtime_error(alloc::format!(
            "mongod failed to start for {}. Last error: {}. Stdout: {} Stderr: {}",
            pool.pool_id(),
            last_err.unwrap_or_else(|| String::from("None")),
            stdout.trim(),
            stderr.trim()
        )))
    }

    fn proc_output(&self) -> (String, String) {
        let g = self.state.proc_.lock();
        g.as_ref().map(MongoProc::output).unwrap_or_default()
    }

    /// Return the `laila_pool_entries` collection (`self._collection()`),
    /// `RuntimeError` once closed.
    fn collection(&self, pool: &PoolObject) -> Result<Collection<Document>> {
        let guard = self.state.client.lock();
        let client = guard
            .as_ref()
            .ok_or_else(|| LailaError::runtime_error("MongoPool is closed."))?;
        let dbname = str_field(pool, "dbname").unwrap_or_else(|| String::from("laila"));
        match client {
            MongoClient::Real(c) => Ok(c.database(&dbname).collection::<Document>(COLLECTION)),
            MongoClient::Mock(_) => Err(LailaError::runtime_error(
                "MongoPool client is a test stand-in.",
            )),
        }
    }

    /// `self._client[self.dbname][COLLECTION]` on a stand-in client.
    fn mock_collection(&self, pool: &PoolObject) -> Result<Option<Arg>> {
        let guard = self.state.client.lock();
        let Some(MongoClient::Mock(m)) = guard.as_ref() else {
            return Ok(None);
        };
        let dbname = str_field(pool, "dbname").unwrap_or_else(|| String::from("laila"));
        let db = m.__getitem__(dbname)?;
        Ok(Some(db.__getitem__(COLLECTION)?))
    }
}

impl PoolBackend for MongoBackend {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }

    /// Connect to MongoDB and ensure the entries collection is indexed.
    fn model_post_init(&self, pool: &PoolObject) -> Result<()> {
        let client = self._connect(pool)?;
        *self.state.client.lock() = Some(client);
        // `self._collection().create_index([("key", ASCENDING)], unique=True)`
        if let Some(collection) = self.mock_collection(pool)? {
            let mut a = Args::from(alloc::vec![Arg::Tuple(Arc::from(alloc::vec![
                "key".into_arg(),
                Arg::Int(1)
            ]))]);
            a.push_keyword("unique", Arg::Bool(true));
            collection.__call_method__("create_index", a)?;
        } else {
            self.collection(pool)?
                .create_index(
                    IndexModel::builder()
                        .keys(doc! {"key": 1})
                        .options(IndexOptions::builder().unique(true).build())
                        .build(),
                )
                .run()
                .map_err(mongo_error)?;
        }
        let state = self.state.clone();
        atexit::register(move || state.close());
        Ok(())
    }

    /// Close the client and terminate any managed mongod process.
    fn close(&self, _pool: &PoolObject) -> Option<Result<()>> {
        self.state.close();
        Some(Ok(()))
    }

    /// Retrieve the JSON value for *key*, or `None` if absent.
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        let _held = pool.held();
        let doc = self
            .collection(pool)?
            .find_one(doc! {"key": key})
            .projection(doc! {"_id": 0, "value": 1})
            .run()
            .map_err(mongo_error)?;
        match doc {
            Some(d) => {
                let value = d.get_str("value").map_err(|e| {
                    LailaError::new(ExcKind::KeyError, alloc::format!("'value': {e}"))
                })?;
                crate::_compat::pyjson::loads(value)
            }
            None => Ok(Arg::None),
        }
    }

    /// Upsert *entry* under *key*.
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
        let Arg::Str(value) = value else {
            return Err(LailaError::type_error(
                "MongoPool expects a serialized JSON string.",
            ));
        };
        let _held = pool.held();
        self.collection(pool)?
            .update_one(doc! {"key": key}, doc! {"$set": {"value": &*value}})
            .upsert(true)
            .run()
            .map_err(mongo_error)?;
        Ok(())
    }

    /// Delete the document for *key*.
    fn _delete(&self, pool: &PoolObject, key: &str) -> Result<()> {
        let _held = pool.held();
        self.collection(pool)?
            .delete_one(doc! {"key": key})
            .run()
            .map_err(mongo_error)?;
        Ok(())
    }

    /// Remove all documents from the entries collection.
    fn _empty(&self, pool: &PoolObject) -> Result<()> {
        let _held = pool.held();
        self.collection(pool)?
            .delete_many(doc! {})
            .run()
            .map_err(mongo_error)?;
        Ok(())
    }

    /// `True` if a document for *key* exists.
    fn _exists(&self, pool: &PoolObject, key: &str) -> Result<bool> {
        let _held = pool.held();
        let n = self
            .collection(pool)?
            .count_documents(doc! {"key": key})
            .limit(1)
            .run()
            .map_err(mongo_error)?;
        Ok(n > 0)
    }

    /// Return all keys in the collection.
    fn _keys(&self, pool: &PoolObject) -> Result<Vec<String>> {
        let _held = pool.held();
        let cursor = self
            .collection(pool)?
            .find(doc! {})
            .projection(doc! {"_id": 0, "key": 1})
            .sort(doc! {"key": 1})
            .run()
            .map_err(mongo_error)?;
        let mut keys = Vec::new();
        for d in cursor {
            let d = d.map_err(mongo_error)?;
            keys.push(d.get_str("key").unwrap_or_default().to_string());
        }
        Ok(keys)
    }

    fn __getattr__(&self, pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        match name {
            "_client" => Some(Ok(self
                .state
                .client
                .lock()
                .as_ref()
                .map(MongoClient::as_arg)
                .unwrap_or(Arg::None))),
            "_mongo_proc" => Some(Ok(self
                .state
                .proc_
                .lock()
                .as_ref()
                .map(MongoProc::as_arg)
                .unwrap_or(Arg::None))),
            "_owns_local_server" => Some(Ok(Arg::Bool(
                self.state.owns_local_server.load(Ordering::SeqCst),
            ))),
            "_mongo_dir" => Some(Ok(self
                .state
                .mongo_dir
                .read()
                .clone()
                .map(IntoArg::into_arg)
                .unwrap_or(Arg::None))),
            "_local_uri" => Some(self._local_uri(pool).map(IntoArg::into_arg)),
            _ => None,
        }
    }

    fn __setattr__(&self, _pool: &PoolObject, name: &str, value: Arg) -> Option<Result<()>> {
        match name {
            // `pool._client = Mock()` / `pool._mongo_proc = Mock()`: a test
            // stand-in that `close()` drives (`client.close()`, `proc.terminate()`).
            "_client" => {
                *self.state.client.lock() = match value {
                    Arg::None => None,
                    other => Some(MongoClient::Mock(other)),
                };
                Some(Ok(()))
            }
            "_mongo_proc" => {
                *self.state.proc_.lock() = match value {
                    Arg::None => None,
                    other => Some(MongoProc::Mock(other)),
                };
                Some(Ok(()))
            }
            "_mongo_dir" => {
                *self.state.mongo_dir.write() = match value {
                    Arg::None => None,
                    other => Some(other.__str__()),
                };
                Some(Ok(()))
            }
            "_owns_local_server" => {
                self.state
                    .owns_local_server
                    .store(value.__bool__(), Ordering::SeqCst);
                Some(Ok(()))
            }
            _ => None,
        }
    }

    fn __call_method__(&self, pool: &PoolObject, name: &str, _args: Args) -> Option<Result<Arg>> {
        match name {
            "_configure_local_server" => {
                Some(self._configure_local_server(pool).map(|()| Arg::None))
            }
            "_ensure_local_server" => Some(self._ensure_local_server(pool).map(|()| Arg::None)),
            "_local_server_available" => Some(self._local_server_available(pool).map(Arg::Bool)),
            "_start_local_server" => Some(self._start_local_server(pool).map(|()| Arg::None)),
            _ => None,
        }
    }

    fn method_names(&self) -> &'static [&'static str] {
        &[
            "close",
            "_connect",
            "_configure_local_server",
            "_ensure_local_server",
            "_local_server_available",
            "_start_local_server",
            "_db",
            "_collection",
        ]
    }
}

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "MongoPool",
        "laila.data.mongo.mongo",
        alloc::vec![],
        alloc::vec![
            (String::from("uri"), field_default("str | None", Arg::None)),
            (String::from("host"), field_default("str | None", Arg::None)),
            (String::from("port"), field_default("int", Arg::Int(27017))),
            (String::from("dbname"), field_default("str", "laila")),
            (
                String::from("server_start_timeout_s"),
                field_default("float", Arg::Float(5.0))
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
            (String::from("_mongo_proc"), private_default(Arg::None)),
            (
                String::from("_owns_local_server"),
                private_default(Arg::Bool(false))
            ),
            (String::from("_mongo_dir"), private_default(Arg::None)),
        ],
        alloc::vec![(String::from(_PERSISTENT_STORAGE), Arg::Bool(true))],
    )
});

crate::laila_pool_handle!(
    /// MongoDB-backed pool.
    ///
    /// Can connect to an existing MongoDB via `uri` or `host` / `port`,
    /// or automatically start and manage a local `mongod` process.
    MongoPool,
    MongoBackend
);

impl ModelBase for MongoPool {
    fn __class__() -> &'static ModelClass {
        MongoPool::__class__()
    }
}

impl MongoPool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `MongoPool(uri=None, host=None, port=27017, dbname="laila", server_start_timeout_s=5.0, **kwargs)`
    #[pysig("MongoPool(**kwargs)")]
    pub fn new(args: impl Into<Args>) -> Result<MongoPool> {
        Ok(MongoPool(Pool::construct(
            Self::__class__(),
            kwargs,
            |_inst: &ModelInstance| Ok(Box::new(MongoBackend::default())),
        )?))
    }
    /// `pool.port`
    pub fn port(&self) -> Arg {
        self.0.field("port").unwrap_or(Arg::None)
    }
    /// `MongoPool.__new__(MongoPool)`: a bare instance (no client, no
    /// managed server); the tests drive `close` on it.
    pub fn __new__(_cls: impl Into<Args>) -> Result<MongoPool> {
        Ok(MongoPool(Pool::__new__(Self::__class__(), |_inst| {
            Ok(Box::new(MongoBackend::default()))
        })?))
    }
    /// `pool.host`
    pub fn host(&self) -> Arg {
        self.0.field("host").unwrap_or(Arg::None)
    }
    /// `pool.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
    fn backend(&self) -> &MongoBackend {
        self.0.downcast_ref::<MongoBackend>().expect("MongoBackend")
    }
    /// `pool._start_local_server()`
    pub fn _start_local_server(&self) -> Result<()> {
        self.backend()._start_local_server(self.0.obj())
    }
    /// `pool._local_server_available()`
    pub fn _local_server_available(&self) -> Result<bool> {
        self.backend()._local_server_available(self.0.obj())
    }
    /// `pool._configure_local_server()`
    pub fn _configure_local_server(&self) -> Result<()> {
        self.backend()._configure_local_server(self.0.obj())
    }
    fn private(&self, name: &str) -> Arg {
        self.0.obj().__getattr__(name).unwrap_or(Arg::None)
    }
    /// `pool._client`
    pub fn _client(&self) -> Arg {
        self.private("_client")
    }
    /// `pool._mongo_proc`
    pub fn _mongo_proc(&self) -> Arg {
        self.private("_mongo_proc")
    }
    /// `pool._owns_local_server`
    pub fn _owns_local_server(&self) -> Arg {
        self.private("_owns_local_server")
    }
    /// `pool._mongo_dir`
    pub fn _mongo_dir(&self) -> Arg {
        self.private("_mongo_dir")
    }
}

impl crate::python::builtins::CallValue for MongoPool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        MongoPool::new(args).map(IntoArg::into_arg)
    }
}

/// `MongoPool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    MongoPool::__class__().as_pyclass().into_arg()
}

impl PyArg for MongoBackend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
