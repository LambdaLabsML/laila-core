//! PostgreSQL pool implementation with optional managed local server.
//!
//! Twin of `laila/data/postgres/postgres.py`: entries are rows of the
//! `laila_pool_entries(key TEXT PRIMARY KEY, value TEXT)` table. The pool
//! connects to an existing server (`dsn`, or `host` / `port` / `dbname` /
//! `user` / `password`) or, given no connection parameters, initialises
//! and runs its own cluster under the pool's directory (`initdb` +
//! `postgres -k <socket dir>`), reusing one that is already up. Where
//! Python holds a `psycopg` connection the port holds a `postgres::Client`
//! -- the same wire protocol, so a managed cluster written by either
//! implementation serves the other.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicBool, Ordering};

use parking_lot::{Mutex, RwLock};
use postgres::{Client, NoTls};
use sha1::{Digest, Sha1};

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::atomic::definitions::HasIdentity;
use crate::data::schema::base::{
    field_default, pool_class, private_default, Pool, PoolBackend, PoolObject, _PERSISTENT_STORAGE,
};
use crate::python::errors::ExcKind;
use crate::python::mock::call_patched;
use crate::python::stdlib::os;
use crate::python::stdlib::subprocess::{self, atexit, Popen};
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

const CLASS_NAME: &str = "PostgresPool";

/// `psycopg.OperationalError` and friends surfaced by the driver.
fn pg_error(e: postgres::Error) -> LailaError {
    let kind = if e.as_db_error().is_some() {
        ExcKind::Exception
    } else {
        ExcKind::ConnectionError
    };
    LailaError::new(kind, alloc::format!("psycopg.OperationalError: {e}"))
}

/// The patched twin of a method, when `patch.object(PostgresPool, name, ..)`
/// is active: the mock's result.
macro_rules! patched {
    ($pool:expr, $name:literal) => {
        patched!($pool, $name, Args::new())
    };
    ($pool:expr, $name:literal, $args:expr) => {
        if let Some(r) = call_patched(CLASS_NAME, $name, $pool.handle(), $args) {
            return r.and_then(|a| crate::python::RetInto::ret_into(a));
        }
    };
}

/// `self._conn`: a live driver connection, or the stand-in a patched
/// `_connect_local` returned (the unit tests hand back a `Mock`).
pub enum PgConn {
    Real(Box<Client>),
    Mock(Arg),
}

impl PgConn {
    /// `cur.execute(sql); cur.fetchone()` -- whether a row came back.
    fn fetchone_is_some(&mut self, sql: &str) -> Result<bool> {
        match self {
            PgConn::Real(c) => Ok(c.query_opt(sql, &[]).map_err(pg_error)?.is_some()),
            PgConn::Mock(conn) => {
                let cur = conn.__call_method__("cursor", Args::new())?;
                let cur = cur.__enter__().unwrap_or(cur);
                cur.__call_method__("execute", Args::from(sql.into_arg()))?;
                let row = cur.__call_method__("fetchone", Args::new())?;
                Ok(!row.is_none())
            }
        }
    }
    /// `cur.execute(sql)`
    fn execute(&mut self, sql: &str) -> Result<()> {
        match self {
            PgConn::Real(c) => c.batch_execute(sql).map_err(pg_error),
            PgConn::Mock(conn) => {
                let cur = conn.__call_method__("cursor", Args::new())?;
                let cur = cur.__enter__().unwrap_or(cur);
                cur.__call_method__("execute", Args::from(sql.into_arg()))?;
                Ok(())
            }
        }
    }
    /// `conn.commit()` (the driver auto-commits single statements).
    fn commit(&mut self) -> Result<()> {
        match self {
            PgConn::Real(_) => Ok(()),
            PgConn::Mock(conn) => conn.__call_method__("commit", Args::new()).map(|_| ()),
        }
    }
    /// `conn.close()`
    fn close(self) -> Result<()> {
        match self {
            PgConn::Real(c) => (*c).close().map_err(pg_error),
            PgConn::Mock(conn) => conn.__call_method__("close", Args::new()).map(|_| ()),
        }
    }
    fn real(&mut self) -> Result<&mut Client> {
        match self {
            PgConn::Real(c) => Ok(c.as_mut()),
            PgConn::Mock(_) => Err(LailaError::runtime_error(
                "PostgresPool connection is a test stand-in.",
            )),
        }
    }
}

/// `self._postgres_proc`: the managed `postgres` child, or the stand-in a
/// test assigned (`pool._postgres_proc = Mock()`).
pub enum PgProc {
    Real(Popen),
    Mock(Arg),
}

impl PgProc {
    fn terminate(&self) {
        match self {
            PgProc::Real(p) => p.terminate(),
            PgProc::Mock(m) => {
                let _ = m.__call_method__("terminate", Args::new());
            }
        }
    }
    fn kill(&self) {
        match self {
            PgProc::Real(p) => p.kill(),
            PgProc::Mock(m) => {
                let _ = m.__call_method__("kill", Args::new());
            }
        }
    }
    fn wait(&self, timeout: Option<f64>) -> Result<()> {
        match self {
            PgProc::Real(p) => p.wait(timeout).map(|_| ()),
            PgProc::Mock(m) => {
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
            PgProc::Real(p) => p.poll(),
            PgProc::Mock(_) => None,
        }
    }
    fn output(&self) -> (String, String) {
        match self {
            PgProc::Real(p) => (p.read_stdout(), p.read_stderr()),
            PgProc::Mock(_) => (String::new(), String::new()),
        }
    }
    fn as_arg(&self) -> Arg {
        match self {
            PgProc::Real(_) => Arg::Str("<Popen: postgres>".into()),
            PgProc::Mock(m) => m.clone(),
        }
    }
}

/// The connection and the managed server, shared with the `atexit` hook
/// (`atexit.register(self.close)`).
pub struct PgState {
    conn: Mutex<Option<PgConn>>,
    proc_: Mutex<Option<PgProc>>,
    owns_local_server: AtomicBool,
    postgres_dir: RwLock<Option<String>>,
    socket_dir: RwLock<Option<String>>,
}

/// `_local_dbname`
const LOCAL_DBNAME: &str = "postgres";
/// `_local_user`
const LOCAL_USER: &str = "laila";

impl PgState {
    /// Close the connection and terminate any managed postgres process.
    fn close(&self) -> Result<()> {
        if let Some(conn) = self.conn.lock().take() {
            conn.close()?;
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
        Ok(())
    }
}

/// `PostgresPool`'s storage.
pub struct PostgresBackend {
    state: Arc<PgState>,
}

impl Default for PostgresBackend {
    fn default() -> Self {
        PostgresBackend {
            state: Arc::new(PgState {
                conn: Mutex::new(None),
                proc_: Mutex::new(None),
                owns_local_server: AtomicBool::new(false),
                postgres_dir: RwLock::new(None),
                socket_dir: RwLock::new(None),
            }),
        }
    }
}

impl core::fmt::Debug for PostgresBackend {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("PostgresBackend")
    }
}

fn str_field(pool: &PoolObject, name: &str) -> Option<String> {
    match pool.field(name) {
        Some(Arg::Str(s)) => Some(String::from(&*s)),
        Some(Arg::None) | None => None,
        Some(other) => Some(other.__str__()),
    }
}

fn port_field(pool: &PoolObject) -> u16 {
    pool.field("port")
        .and_then(|v| v.as_i64())
        .unwrap_or(5432)
        .clamp(0, 65535) as u16
}

impl PostgresBackend {
    /// Establish the connection.
    fn _connect(&self, pool: &PoolObject) -> Result<PgConn> {
        if let Some(dsn) = str_field(pool, "dsn") {
            let config: postgres::Config = dsn.parse().map_err(pg_error)?;
            return Ok(PgConn::Real(Box::new(
                config.connect(NoTls).map_err(pg_error)?,
            )));
        }
        let host = str_field(pool, "host");
        let dbname = str_field(pool, "dbname");
        let user = str_field(pool, "user");
        let password = str_field(pool, "password");
        if host.is_none() && dbname.is_none() && user.is_none() && password.is_none() {
            self._configure_local_server(pool)?;
            self._ensure_local_server(pool)?;
            return self._connect_local(pool, 1);
        }
        let (Some(host), Some(dbname), Some(user)) = (host, dbname, user) else {
            return Err(LailaError::value_error(
                "PostgresPool requires either dsn, explicit host/dbname/user parameters, or no connection parameters for a managed local server.",
            ));
        };
        let mut config = postgres::Config::new();
        config
            .host(&host)
            .port(port_field(pool))
            .dbname(&dbname)
            .user(&user);
        if let Some(pw) = password {
            config.password(pw);
        }
        Ok(PgConn::Real(Box::new(
            config.connect(NoTls).map_err(pg_error)?,
        )))
    }

    /// Set up data and socket directories for a managed local postgres.
    pub fn _configure_local_server(&self, pool: &PoolObject) -> Result<()> {
        patched!(pool, "_configure_local_server");
        if self.state.postgres_dir.read().is_some() && self.state.socket_dir.read().is_some() {
            return Ok(());
        }
        let pools = crate::macros::defaults::LAILA_DEFAULT_DIRECTORIES().__getitem__("pools")?;
        let pool_dir = os.path().join(crate::args!(pools, pool.uuid()))?;
        let postgres_dir = os.path().join(crate::args!(pool_dir.clone(), "data"))?;
        let socket_dir = os.path().join(crate::args!(pool_dir, "socket"))?;
        os.makedirs(crate::args!(postgres_dir.clone(), exist_ok = true))?;
        os.makedirs(crate::args!(socket_dir.clone(), exist_ok = true))?;
        *self.state.postgres_dir.write() = Some(postgres_dir);
        *self.state.socket_dir.write() = Some(socket_dir);
        let digest = Sha1::digest(pool.pool_id().as_bytes());
        let hex: String = digest.iter().map(|b| alloc::format!("{b:02x}")).collect();
        let port = 20000 + (i64::from_str_radix(&hex[..8], 16).unwrap_or(0) % 20000);
        pool.set_field("port", Arg::Int(port));
        Ok(())
    }

    /// Connect to the managed local postgres via UNIX socket.
    pub fn _connect_local(&self, pool: &PoolObject, connect_timeout: u64) -> Result<PgConn> {
        if let Some(r) = call_patched(CLASS_NAME, "_connect_local", pool.handle(), {
            let mut a = Args::new();
            a.push_keyword("connect_timeout", Arg::Int(connect_timeout as i64));
            a
        }) {
            return r.map(PgConn::Mock);
        }
        let socket_dir = self.state.socket_dir.read().clone().ok_or_else(|| {
            LailaError::runtime_error("Local Postgres socket directory is not configured.")
        })?;
        let mut config = postgres::Config::new();
        config
            .host(&socket_dir)
            .port(port_field(pool))
            .dbname(LOCAL_DBNAME)
            .user(LOCAL_USER)
            .connect_timeout(core::time::Duration::from_secs(connect_timeout));
        Ok(PgConn::Real(Box::new(
            config.connect(NoTls).map_err(pg_error)?,
        )))
    }

    /// Start a local postgres if one is not already reachable.
    pub fn _ensure_local_server(&self, pool: &PoolObject) -> Result<()> {
        patched!(pool, "_ensure_local_server");
        if self._local_server_available(pool)? {
            self.state.owns_local_server.store(false, Ordering::SeqCst);
            return Ok(());
        }
        self._initdb_if_needed(pool)?;
        self._start_local_server(pool)
    }

    /// `True` if the local postgres accepts connections.
    pub fn _local_server_available(&self, pool: &PoolObject) -> Result<bool> {
        patched!(pool, "_local_server_available");
        match self._connect_local(pool, 1) {
            Ok(conn) => {
                let _ = conn.close();
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    /// Run `initdb` if the data directory is uninitialised.
    pub fn _initdb_if_needed(&self, pool: &PoolObject) -> Result<()> {
        patched!(pool, "_initdb_if_needed");
        let postgres_dir = self.state.postgres_dir.read().clone().ok_or_else(|| {
            LailaError::runtime_error("Local Postgres data directory is not configured.")
        })?;
        if os.path().exists(
            os.path()
                .join(crate::args!(postgres_dir.clone(), "PG_VERSION"))?,
        )? {
            return Ok(());
        }
        self._run_command(
            pool,
            &[
                String::from("initdb"),
                String::from("-D"),
                postgres_dir.clone(),
                String::from("-U"),
                String::from(LOCAL_USER),
                String::from("-A"),
                String::from("trust"),
            ],
            &alloc::format!("initialize local Postgres data directory {postgres_dir}"),
        )
    }

    /// Execute a subprocess command, raising on failure.
    pub fn _run_command(&self, pool: &PoolObject, command: &[String], action: &str) -> Result<()> {
        patched!(pool, "_run_command", {
            let mut a = Args::from(command.to_vec());
            a.push_keyword("action", action);
            a
        });
        match subprocess::run(command) {
            // `CalledProcessError`: report the captured output.
            Ok(done) if done.returncode != 0 => Err(LailaError::runtime_error(alloc::format!(
                "Unable to {action}. Stdout: {} Stderr: {}",
                done.stdout.trim(),
                done.stderr.trim()
            ))),
            Ok(_) => Ok(()),
            Err(e) if e.kind == ExcKind::FileNotFoundError => {
                Err(LailaError::runtime_error(alloc::format!(
                    "Unable to {action}: command not found: {}",
                    command.first().map(String::as_str).unwrap_or("")
                )))
            }
            Err(e) => Err(e),
        }
    }

    /// Launch a `postgres` subprocess and wait for readiness.
    pub fn _start_local_server(&self, pool: &PoolObject) -> Result<()> {
        patched!(pool, "_start_local_server");
        let postgres_dir = self.state.postgres_dir.read().clone();
        let socket_dir = self.state.socket_dir.read().clone();
        let (Some(postgres_dir), Some(socket_dir)) = (postgres_dir, socket_dir) else {
            return Err(LailaError::runtime_error(
                "Local Postgres server is not fully configured.",
            ));
        };
        let cmd = [
            String::from("postgres"),
            String::from("-D"),
            postgres_dir,
            String::from("-k"),
            socket_dir,
            String::from("-p"),
            port_field(pool).to_string(),
            String::from("-h"),
            String::new(),
        ];
        *self.state.proc_.lock() = Some(PgProc::Real(Popen::new(&cmd)?));
        self.state.owns_local_server.store(true, Ordering::SeqCst);

        let timeout = pool
            .field("server_start_timeout_s")
            .and_then(|v| v.as_f64())
            .unwrap_or(5.0);
        let deadline =
            std::time::Instant::now() + core::time::Duration::from_secs_f64(timeout.max(0.0));
        let mut last_err: Option<LailaError> = None;
        while std::time::Instant::now() < deadline {
            let exited = self.state.proc_.lock().as_ref().and_then(|p| p.poll());
            if let Some(exit_code) = exited {
                let (stdout, stderr) = self.proc_output();
                self.state.close()?;
                return Err(LailaError::runtime_error(alloc::format!(
                    "postgres exited early (code={exit_code}). Stdout: {} Stderr: {}",
                    stdout.trim(),
                    stderr.trim()
                )));
            }
            match self._connect_local(pool, 1) {
                Ok(conn) => {
                    let _ = conn.close();
                    return Ok(());
                }
                Err(e) => {
                    last_err = Some(e);
                    std::thread::sleep(core::time::Duration::from_millis(100));
                }
            }
        }
        let (stdout, stderr) = self.proc_output();
        self.state.close()?;
        Err(LailaError::runtime_error(alloc::format!(
            "postgres failed to start for {}. Last error: {}. Stdout: {} Stderr: {}",
            pool.pool_id(),
            last_err
                .map(|e| e.message.to_string())
                .unwrap_or_else(|| String::from("None")),
            stdout.trim(),
            stderr.trim()
        )))
    }

    fn proc_output(&self) -> (String, String) {
        let g = self.state.proc_.lock();
        g.as_ref().map(PgProc::output).unwrap_or_default()
    }

    /// Run *f* on the active connection (`self._connection()`).
    fn with_connection<T>(&self, f: impl FnOnce(&mut Client) -> Result<T>) -> Result<T> {
        let mut guard = self.state.conn.lock();
        match guard.as_mut() {
            None => Err(LailaError::runtime_error("PostgresPool is closed.")),
            Some(conn) => f(conn.real()?),
        }
    }
}

impl PoolBackend for PostgresBackend {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }

    /// Connect to PostgreSQL and create the entries table.
    fn model_post_init(&self, pool: &PoolObject) -> Result<()> {
        let mut conn = self._connect(pool)?;
        let legacy = conn.fetchone_is_some(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = 'laila_pool_entries' AND column_name = 'pool_id'",
        )?;
        if legacy {
            conn.execute("DROP TABLE laila_pool_entries")?;
        }
        conn.execute(
            "
                CREATE TABLE IF NOT EXISTS laila_pool_entries (
                    key TEXT NOT NULL PRIMARY KEY,
                    value TEXT NOT NULL
                )
                ",
        )?;
        conn.commit()?;
        *self.state.conn.lock() = Some(conn);
        let state = self.state.clone();
        atexit::register(move || {
            let _ = state.close();
        });
        Ok(())
    }

    /// Close the connection and terminate any managed postgres process.
    fn close(&self, _pool: &PoolObject) -> Option<Result<()>> {
        Some(self.state.close())
    }

    /// Retrieve the JSON value for *key*, or `None` if absent.
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        let _held = pool.held();
        let row = self.with_connection(|c| {
            c.query_opt(
                "SELECT value FROM laila_pool_entries WHERE key = $1",
                &[&key],
            )
            .map_err(pg_error)
        })?;
        match row {
            Some(row) => crate::_compat::pyjson::loads(&row.get::<_, String>(0)),
            None => Ok(Arg::None),
        }
    }

    /// Insert or update *entry* under *key*.
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
                "PostgresPool expects a serialized JSON string.",
            ));
        };
        let _held = pool.held();
        self.with_connection(|c| {
            c.execute(
                "
                    INSERT INTO laila_pool_entries(key, value)
                    VALUES ($1, $2)
                    ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
                    ",
                &[&key, &&*value],
            )
            .map_err(pg_error)
        })?;
        Ok(())
    }

    /// Delete the row for *key*.
    fn _delete(&self, pool: &PoolObject, key: &str) -> Result<()> {
        let _held = pool.held();
        self.with_connection(|c| {
            c.execute("DELETE FROM laila_pool_entries WHERE key = $1", &[&key])
                .map_err(pg_error)
        })?;
        Ok(())
    }

    /// Remove all entries from the pool.
    fn _empty(&self, pool: &PoolObject) -> Result<()> {
        let _held = pool.held();
        self.with_connection(|c| {
            c.execute("DELETE FROM laila_pool_entries", &[])
                .map_err(pg_error)
        })?;
        Ok(())
    }

    /// `True` if *key* is present in the table.
    fn _exists(&self, pool: &PoolObject, key: &str) -> Result<bool> {
        let _held = pool.held();
        let row = self.with_connection(|c| {
            c.query_opt("SELECT 1 FROM laila_pool_entries WHERE key = $1", &[&key])
                .map_err(pg_error)
        })?;
        Ok(row.is_some())
    }

    /// Return all keys in the table.
    fn _keys(&self, pool: &PoolObject) -> Result<Vec<String>> {
        let _held = pool.held();
        self.with_connection(|c| {
            let rows = c
                .query("SELECT key FROM laila_pool_entries ORDER BY key", &[])
                .map_err(pg_error)?;
            Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
        })
    }

    fn __getattr__(&self, _pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        match name {
            "_conn" => Some(Ok(match self.state.conn.lock().as_ref() {
                Some(PgConn::Real(_)) => Arg::Str("<psycopg.Connection>".into()),
                Some(PgConn::Mock(m)) => m.clone(),
                None => Arg::None,
            })),
            "_postgres_proc" => Some(Ok(self
                .state
                .proc_
                .lock()
                .as_ref()
                .map(PgProc::as_arg)
                .unwrap_or(Arg::None))),
            "_owns_local_server" => Some(Ok(Arg::Bool(
                self.state.owns_local_server.load(Ordering::SeqCst),
            ))),
            "_postgres_dir" => Some(Ok(self
                .state
                .postgres_dir
                .read()
                .clone()
                .map(IntoArg::into_arg)
                .unwrap_or(Arg::None))),
            "_socket_dir" => Some(Ok(self
                .state
                .socket_dir
                .read()
                .clone()
                .map(IntoArg::into_arg)
                .unwrap_or(Arg::None))),
            "_local_dbname" => Some(Ok(LOCAL_DBNAME.into_arg())),
            "_local_user" => Some(Ok(LOCAL_USER.into_arg())),
            _ => None,
        }
    }

    fn __setattr__(&self, _pool: &PoolObject, name: &str, value: Arg) -> Option<Result<()>> {
        match name {
            // `pool._conn = Mock()` / `pool._postgres_proc = Mock()`: a test
            // stand-in that `close()` drives (`conn.close()`, `proc.terminate()`).
            "_conn" => {
                *self.state.conn.lock() = match value {
                    Arg::None => None,
                    other => Some(PgConn::Mock(other)),
                };
                Some(Ok(()))
            }
            "_postgres_proc" => {
                *self.state.proc_.lock() = match value {
                    Arg::None => None,
                    other => Some(PgProc::Mock(other)),
                };
                Some(Ok(()))
            }
            "_postgres_dir" => {
                *self.state.postgres_dir.write() = str_arg(&value);
                Some(Ok(()))
            }
            "_socket_dir" => {
                *self.state.socket_dir.write() = str_arg(&value);
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

    fn __call_method__(&self, pool: &PoolObject, name: &str, args: Args) -> Option<Result<Arg>> {
        match name {
            "_configure_local_server" => {
                Some(self._configure_local_server(pool).map(|()| Arg::None))
            }
            "_ensure_local_server" => Some(self._ensure_local_server(pool).map(|()| Arg::None)),
            "_local_server_available" => Some(self._local_server_available(pool).map(Arg::Bool)),
            "_initdb_if_needed" => Some(self._initdb_if_needed(pool).map(|()| Arg::None)),
            "_start_local_server" => Some(self._start_local_server(pool).map(|()| Arg::None)),
            "_run_command" => {
                let command: Vec<String> = args
                    .positional
                    .first()
                    .and_then(|c| c.__iter__().ok())
                    .map(|items| items.iter().map(|i| i.__str__()).collect())
                    .unwrap_or_default();
                let action = args.get("action").__str__();
                Some(
                    self._run_command(pool, &command, &action)
                        .map(|()| Arg::None),
                )
            }
            _ => None,
        }
    }

    fn method_names(&self) -> &'static [&'static str] {
        &[
            "close",
            "_connect",
            "_configure_local_server",
            "_connect_local",
            "_ensure_local_server",
            "_local_server_available",
            "_initdb_if_needed",
            "_run_command",
            "_start_local_server",
            "_connection",
        ]
    }
}

fn str_arg(a: &Arg) -> Option<String> {
    match a {
        Arg::None => None,
        other => Some(other.__str__()),
    }
}

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "PostgresPool",
        "laila.data.postgres.postgres",
        alloc::vec![],
        alloc::vec![
            (String::from("dsn"), field_default("str | None", Arg::None)),
            (String::from("host"), field_default("str | None", Arg::None)),
            (String::from("port"), field_default("int", Arg::Int(5432))),
            (
                String::from("dbname"),
                field_default("str | None", Arg::None)
            ),
            (String::from("user"), field_default("str | None", Arg::None)),
            (
                String::from("password"),
                field_default("str | None", Arg::None)
            ),
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
            (String::from("_conn"), private_default(Arg::None)),
            (String::from("_postgres_proc"), private_default(Arg::None)),
            (
                String::from("_owns_local_server"),
                private_default(Arg::Bool(false))
            ),
            (String::from("_postgres_dir"), private_default(Arg::None)),
            (String::from("_socket_dir"), private_default(Arg::None)),
            (String::from("_local_dbname"), private_default(LOCAL_DBNAME)),
            (String::from("_local_user"), private_default(LOCAL_USER)),
        ],
        alloc::vec![(String::from(_PERSISTENT_STORAGE), Arg::Bool(true))],
    )
});

crate::laila_pool_handle!(
    /// PostgreSQL-backed pool.
    ///
    /// Can connect to an existing PostgreSQL via `dsn` or explicit
    /// `host` / `port` / `dbname` / `user` parameters, or automatically
    /// start and manage a local `postgres` process.
    PostgresPool,
    PostgresBackend
);

impl ModelBase for PostgresPool {
    fn __class__() -> &'static ModelClass {
        PostgresPool::__class__()
    }
}

impl PostgresPool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `PostgresPool(dsn=None, host=None, port=5432, dbname=None, user=None, password=None, server_start_timeout_s=5.0, **kwargs)`
    #[pysig("PostgresPool(**kwargs)")]
    pub fn new(args: impl Into<Args>) -> Result<PostgresPool> {
        Ok(PostgresPool(Pool::construct(
            Self::__class__(),
            kwargs,
            |_inst: &ModelInstance| Ok(Box::new(PostgresBackend::default())),
        )?))
    }
    /// `PostgresPool.__new__(PostgresPool)`: a bare instance (no connection,
    /// no managed server); the tests drive `_initdb_if_needed` / `close` on it.
    pub fn __new__(_cls: impl Into<Args>) -> Result<PostgresPool> {
        Ok(PostgresPool(Pool::__new__(Self::__class__(), |_inst| {
            Ok(Box::new(PostgresBackend::default()))
        })?))
    }
    /// `pool.port`
    pub fn port(&self) -> Arg {
        self.0.field("port").unwrap_or(Arg::None)
    }
    /// `pool.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
    fn backend(&self) -> &PostgresBackend {
        self.0
            .downcast_ref::<PostgresBackend>()
            .expect("PostgresBackend")
    }
    /// `pool._initdb_if_needed()`
    pub fn _initdb_if_needed(&self) -> Result<()> {
        self.backend()._initdb_if_needed(self.0.obj())
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
    /// `pool._conn`
    pub fn _conn(&self) -> Arg {
        self.private("_conn")
    }
    /// `pool._postgres_proc`
    pub fn _postgres_proc(&self) -> Arg {
        self.private("_postgres_proc")
    }
    /// `pool._owns_local_server`
    pub fn _owns_local_server(&self) -> Arg {
        self.private("_owns_local_server")
    }
    /// `pool._postgres_dir`
    pub fn _postgres_dir(&self) -> Arg {
        self.private("_postgres_dir")
    }
    /// `pool._socket_dir`
    pub fn _socket_dir(&self) -> Arg {
        self.private("_socket_dir")
    }
    /// `pool._local_user`
    pub fn _local_user(&self) -> Arg {
        self.private("_local_user")
    }
    /// `pool._local_dbname`
    pub fn _local_dbname(&self) -> Arg {
        self.private("_local_dbname")
    }
}

impl crate::python::builtins::CallValue for PostgresPool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        PostgresPool::new(args).map(IntoArg::into_arg)
    }
}

/// `PostgresPool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    PostgresPool::__class__().as_pyclass().into_arg()
}

impl PyArg for PostgresBackend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
