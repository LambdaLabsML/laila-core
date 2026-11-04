//! DuckDB pool implementation.
//!
//! Twin of `laila/data/duckdb/duckdb.py`: entries are rows of the
//! `laila_pool_entries(key VARCHAR PRIMARY KEY, value TEXT)` table of a
//! local DuckDB database file, written through the DuckDB C API of the
//! same engine version as the Python wheel (1.5.5), so a `.duckdb` file
//! written by either implementation opens in the other.
//!
//! Space reclaim. DuckDB only frees the blocks of deleted rows at a
//! `CHECKPOINT`, and even then only for row groups that are *entirely*
//! dead: a single long-lived row (a config constant, an index record) pins
//! its row group and the deleted neighbours are kept forever, so a
//! write/delete churn grows the file and the buffer pool without bound.
//! The pool therefore accounts deleted bytes/rows (`DELETE ... RETURNING
//! length(value)`) and, past either threshold, arms a deferred maintenance
//! step that runs before the next data write: `CHECKPOINT`, then -- when
//! the deleted volume is comparable to what is still live -- a
//! transactional table rewrite (`_compact`) that leaves only live rows,
//! followed by another `CHECKPOINT`.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;
use core::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use duckdb::types::ValueRef;
use duckdb::{Connection, OptionalExt};
use parking_lot::Mutex;

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::atomic::definitions::HasIdentity;
use crate::data::schema::base::{
    field_default, pool_class, private_default, Pool, PoolBackend, PoolObject, _PERSISTENT_STORAGE,
};
use crate::data::schema::pool_index::is_index_key;
use crate::python::errors::ExcKind;
use crate::python::object::PyObject;
use crate::python::stdlib::os;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

/// `import duckdb` -- the engine module as a value (`duckdb is None` is
/// the Python probe for the missing extra; here the feature is on, so it
/// never is). Exposes `__name__`, `__version__` and `connect(path)`.
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct duckdb_mod;
#[allow(non_upper_case_globals)]
pub static duckdb: duckdb_mod = duckdb_mod;

impl duckdb_mod {
    /// `duckdb.__version__` -- the linked library's version (`v1.5.5`
    /// reported by the C API becomes `1.5.5`, as the wheel reports it).
    pub fn __version__(&self) -> String {
        // SAFETY: `duckdb_library_version` returns a static NUL-terminated
        // string owned by the library.
        let raw = unsafe { core::ffi::CStr::from_ptr(duckdb::ffi::duckdb_library_version()) };
        raw.to_string_lossy().trim_start_matches('v').to_string()
    }
    /// `duckdb.connect(database)` -- a raw connection object.
    pub fn connect(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let database = a
            .positional
            .first()
            .cloned()
            .unwrap_or_else(|| a.get("database"));
        let conn = match &database {
            Arg::None => Connection::open_in_memory(),
            Arg::Str(s) if &**s == ":memory:" => Connection::open_in_memory(),
            other => Connection::open(other.__str__()),
        }
        .map_err(duckdb_error)?;
        Ok(DuckDBConnection {
            conn: Arc::new(Mutex::new(Some(conn))),
        }
        .into_arg())
    }
    /// `getattr(duckdb, name)`
    pub fn __getattr__(&self, name: &str) -> Result<Arg> {
        match name {
            "__name__" => Ok(Arg::Str("duckdb".into())),
            "__version__" => Ok(self.__version__().into_arg()),
            "connect" => Ok(Arg::Callable(Arc::new(|a: Args| duckdb.connect(a)))),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "module 'duckdb' has no attribute '{name}'"
            ))),
        }
    }
}

impl PyObject for duckdb_mod {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "module"
    }
    fn __repr__(&self) -> String {
        String::from("<module 'duckdb'>")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other.as_any().is::<duckdb_mod>()
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str("duckdb".into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        duckdb_mod::__getattr__(self, name)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "connect" => self.connect(args),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "module 'duckdb' has no attribute '{name}'"
            ))),
        }
    }
    fn __dir__(&self) -> Vec<String> {
        alloc::vec![
            String::from("__name__"),
            String::from("__version__"),
            String::from("connect")
        ]
    }
}

impl PyArg for duckdb_mod {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(*self))
    }
}

/// Deleted bytes past which the deferred `CHECKPOINT` is armed (`8 << 20`).
pub const DUCKDB_CHECKPOINT_CHURN_BYTES: i64 = 8 << 20;
/// Deleted rows past which the deferred `CHECKPOINT` is armed.
pub const DUCKDB_CHECKPOINT_CHURN_ROWS: i64 = 4096;
/// Compact when deleted rows >= live rows, or when deleted bytes >= this
/// floor *and* >= live bytes (the live-bytes scan runs only past the floor).
pub const DUCKDB_COMPACT_MIN_BYTES: i64 = 64 << 20;

/// A `duckdb.Error` surfaced by the C library (`OSError` is the closest
/// kind the port carries; `assertRaises(Exception)` catches it).
fn duckdb_error(e: duckdb::Error) -> LailaError {
    LailaError::new(ExcKind::OSError, alloc::format!("duckdb.Error: {e}"))
}

/// The shared connection slot (`self._conn`; `None` once closed). Shared
/// between the backend and the `pool._conn` object handed to callers.
type ConnSlot = Arc<Mutex<Option<Connection>>>;

/// `DuckDBPool`'s storage: the connection plus the churn accounting
/// (`_churn_bytes`, `_churn_rows`, `_checkpoint_pending`).
#[derive(Default)]
pub struct DuckDBBackend {
    conn: ConnSlot,
    _churn_bytes: AtomicI64,
    _churn_rows: AtomicI64,
    _checkpoint_pending: AtomicBool,
}

impl core::fmt::Debug for DuckDBBackend {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("DuckDBBackend")
    }
}

/// A DuckDB cell as a Python value (`fetchone()[0]`).
fn value_to_arg(v: ValueRef<'_>) -> Arg {
    match v {
        ValueRef::Null => Arg::None,
        ValueRef::Boolean(b) => Arg::Bool(b),
        ValueRef::TinyInt(i) => Arg::Int(i64::from(i)),
        ValueRef::SmallInt(i) => Arg::Int(i64::from(i)),
        ValueRef::Int(i) => Arg::Int(i64::from(i)),
        ValueRef::BigInt(i) => Arg::Int(i),
        ValueRef::HugeInt(i) => Arg::from_bigint(num_bigint::BigInt::from(i)),
        ValueRef::UHugeInt(i) => Arg::from_bigint(num_bigint::BigInt::from(i)),
        ValueRef::UTinyInt(i) => Arg::Int(i64::from(i)),
        ValueRef::USmallInt(i) => Arg::Int(i64::from(i)),
        ValueRef::UInt(i) => Arg::Int(i64::from(i)),
        ValueRef::UBigInt(i) => Arg::from_bigint(num_bigint::BigInt::from(i)),
        ValueRef::Float(f) => Arg::Float(f64::from(f)),
        ValueRef::Double(f) => Arg::Float(f),
        ValueRef::Text(t) => Arg::Str(String::from_utf8_lossy(t).into()),
        ValueRef::Blob(b) => Arg::Bytes(b.into()),
        other => Arg::Str(alloc::format!("{other:?}").into()),
    }
}

/// Python parameters (`[key, value]`) as DuckDB bindings.
fn bind_params(params: &Arg) -> Result<Vec<duckdb::types::Value>> {
    use duckdb::types::Value;
    let items = match params {
        Arg::None => Vec::new(),
        other => other.__iter__()?,
    };
    items
        .iter()
        .map(|a| {
            Ok(match a {
                Arg::None => Value::Null,
                Arg::Bool(b) => Value::Boolean(*b),
                Arg::Int(i) => Value::BigInt(*i),
                Arg::Float(f) => Value::Double(*f),
                Arg::Str(s) => Value::Text(s.to_string()),
                Arg::Bytes(b) => Value::Blob(b.to_vec()),
                other => Value::Text(other.__str__()),
            })
        })
        .collect()
}

/// Run *sql* with *params* and materialise every row (`conn.execute(..)`).
fn run_query(
    conn: &Connection,
    sql: &str,
    params: &[duckdb::types::Value],
) -> duckdb::Result<Vec<Arg>> {
    let mut stmt = conn.prepare(sql)?;
    let refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| p as &dyn duckdb::ToSql).collect();
    let mut rows = stmt.query(refs.as_slice())?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let n = row.as_ref().column_count();
        let cells: Vec<Arg> = (0..n)
            .map(|i| row.get_ref(i).map(value_to_arg))
            .collect::<duckdb::Result<_>>()?;
        out.push(Arg::Tuple(cells.into()));
    }
    Ok(out)
}

/// `pool._conn`: the `duckdb.DuckDBPyConnection` -- `execute(sql, params)`
/// returns a cursor whose `fetchone()` / `fetchall()` hand back tuples.
pub struct DuckDBConnection {
    conn: ConnSlot,
}

impl DuckDBConnection {
    /// `conn.execute(sql, parameters=None)`: run the statement; the result
    /// cursor yields the rows as tuples.
    pub fn execute(&self, args: impl Into<Args>) -> Result<DuckDBCursor> {
        let args: Args = args.into();
        let sql = args
            .positional
            .first()
            .cloned()
            .unwrap_or(Arg::None)
            .__str__();
        let params = args
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| args.get("parameters"));
        let params = bind_params(&params)?;
        let guard = self.conn.lock();
        let Some(conn) = guard.as_ref() else {
            return Err(LailaError::new(
                ExcKind::OSError,
                "duckdb.ConnectionException: Connection already closed!",
            ));
        };
        let rows = run_query(conn, &sql, &params).map_err(duckdb_error)?;
        Ok(DuckDBCursor {
            rows: Mutex::new(rows.into_iter()),
        })
    }
    /// `conn.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        if let Some(conn) = self.conn.lock().take() {
            if let Err((_, e)) = conn.close() {
                return Err(duckdb_error(e));
            }
        }
        Ok(())
    }
}

impl PyObject for DuckDBConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "DuckDBPyConnection"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<duckdb.DuckDBPyConnection object at {:#x}>",
            Arc::as_ptr(&self.conn) as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<DuckDBConnection>()
            .is_some_and(|o| Arc::ptr_eq(&o.conn, &self.conn))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.conn) as usize as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "execute" => self.execute(args).map(IntoArg::into_arg),
            "close" => self.close(args).map(|()| Arg::None),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'DuckDBPyConnection' object has no attribute '{name}'"
            ))),
        }
    }
    fn __dir__(&self) -> Vec<String> {
        alloc::vec![String::from("execute"), String::from("close")]
    }
}

impl IntoArg for DuckDBConnection {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

/// The result of `conn.execute(..)`: `fetchone()` / `fetchall()`.
pub struct DuckDBCursor {
    rows: Mutex<alloc::vec::IntoIter<Arg>>,
}

impl DuckDBCursor {
    /// `cursor.fetchone()`: the next row as a tuple, `None` past the end.
    pub fn fetchone(&self) -> Arg {
        self.rows.lock().next().unwrap_or(Arg::None)
    }
    /// `cursor.fetchall()`: the remaining rows.
    pub fn fetchall(&self, _args: impl Into<Args>) -> Arg {
        self.rows.lock().by_ref().collect::<Vec<Arg>>().into_arg()
    }
}

impl PyObject for DuckDBCursor {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "DuckDBPyConnection"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<duckdb.DuckDBPyConnection object at {:#x}>",
            self as *const DuckDBCursor as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<DuckDBCursor>()
            .is_some_and(|o| core::ptr::eq(o, self))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(self as *const DuckDBCursor as usize as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "fetchone" => Ok(self.fetchone()),
            "fetchall" => Ok(self.fetchall(args)),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'DuckDBPyConnection' object has no attribute '{name}'"
            ))),
        }
    }
    fn __dir__(&self) -> Vec<String> {
        alloc::vec![String::from("fetchone"), String::from("fetchall")]
    }
}

impl IntoArg for DuckDBCursor {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

impl DuckDBBackend {
    /// Return the active DuckDB connection (`self._connection()`).
    fn with_connection<T>(&self, f: impl FnOnce(&Connection) -> duckdb::Result<T>) -> Result<T> {
        let guard = self.conn.lock();
        match guard.as_ref() {
            None => Err(LailaError::runtime_error("DuckDBPool is closed.")),
            Some(conn) => f(conn).map_err(duckdb_error),
        }
    }

    fn reset_churn(&self) {
        self._churn_bytes.store(0, Ordering::SeqCst);
        self._churn_rows.store(0, Ordering::SeqCst);
        self._checkpoint_pending.store(false, Ordering::SeqCst);
    }

    /// Account for deleted data; arm deferred maintenance past the thresholds.
    fn _note_churn(&self, nbytes: i64, rows: i64) {
        self._churn_bytes.fetch_add(nbytes.max(0), Ordering::SeqCst);
        self._churn_rows.fetch_add(rows, Ordering::SeqCst);
        if self._churn_bytes.load(Ordering::SeqCst) >= DUCKDB_CHECKPOINT_CHURN_BYTES
            || self._churn_rows.load(Ordering::SeqCst) >= DUCKDB_CHECKPOINT_CHURN_ROWS
        {
            self._checkpoint_pending.store(true, Ordering::SeqCst);
        }
    }

    /// Deleted volume comparable to the live volume (rows, or bytes past the floor).
    fn _should_compact(&self) -> Result<bool> {
        let churn_rows = self._churn_rows.load(Ordering::SeqCst);
        let churn_bytes = self._churn_bytes.load(Ordering::SeqCst);
        self.with_connection(|conn| {
            let live_rows: i64 =
                conn.query_row("SELECT count(*) FROM laila_pool_entries", [], |r| r.get(0))?;
            if churn_rows >= live_rows.max(1) {
                return Ok(true);
            }
            if churn_bytes < DUCKDB_COMPACT_MIN_BYTES {
                return Ok(false);
            }
            let live_bytes: i128 = conn.query_row(
                "SELECT coalesce(sum(length(value)), 0) FROM laila_pool_entries",
                [],
                |r| r.get(0),
            )?;
            Ok(i128::from(churn_bytes) >= live_bytes)
        })
    }

    /// Rewrite the entries table with its live rows only (caller holds the lock).
    fn _compact(&self) -> Result<()> {
        self.with_connection(|conn| {
            conn.execute_batch("BEGIN")?;
            let rewrite = || -> duckdb::Result<()> {
                conn.execute_batch(
                    "CREATE TABLE laila_pool_entries_compact (\
                     key VARCHAR NOT NULL PRIMARY KEY, value TEXT NOT NULL)",
                )?;
                conn.execute_batch(
                    "INSERT INTO laila_pool_entries_compact SELECT key, value FROM laila_pool_entries",
                )?;
                conn.execute_batch("DROP TABLE laila_pool_entries")?;
                conn.execute_batch(
                    "ALTER TABLE laila_pool_entries_compact RENAME TO laila_pool_entries",
                )?;
                conn.execute_batch("COMMIT")
            };
            if let Err(e) = rewrite() {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(e);
            }
            conn.execute_batch("CHECKPOINT")
        })
    }

    /// Run the deferred maintenance (caller holds the lock). Never fails the write.
    fn _run_pending_checkpoint(&self) {
        if !self._checkpoint_pending.load(Ordering::SeqCst) {
            return;
        }
        let maintain = || -> Result<()> {
            self.with_connection(|conn| conn.execute_batch("CHECKPOINT"))?;
            if self._should_compact()? {
                self._compact()?;
            }
            Ok(())
        };
        if maintain().is_err() {
            // pragma: no cover - space reclaim is best effort
            return;
        }
        self.reset_churn();
    }

    /// Checkpoint the database now; with *compact* also rewrite the table.
    fn checkpoint(&self, pool: &PoolObject, compact: bool) -> Result<()> {
        let _held = pool.held();
        self.with_connection(|conn| conn.execute_batch("CHECKPOINT"))?;
        if compact {
            self._compact()?;
        }
        self.reset_churn();
        Ok(())
    }
}

impl PoolBackend for DuckDBBackend {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Initialise the DuckDB connection and create the entries table.
    fn model_post_init(&self, pool: &PoolObject) -> Result<()> {
        let file_path = match pool.field("file_path").unwrap_or(Arg::None) {
            Arg::None => {
                let pools =
                    crate::macros::defaults::LAILA_DEFAULT_DIRECTORIES().__getitem__("pools")?;
                let pool_dir = os.path().join(crate::args!(pools, pool.uuid()))?;
                os.makedirs(crate::args!(pool_dir.clone(), exist_ok = true))?;
                os.path().join(crate::args!(pool_dir, "pool.duckdb"))?
            }
            given => {
                let file_path = os.path().expanduser(given)?;
                let parent = os.path().dirname(file_path.clone())?;
                if !parent.is_empty() {
                    os.makedirs(crate::args!(parent, exist_ok = true))?;
                }
                file_path
            }
        };
        pool.set_field("file_path", file_path.clone());

        let conn = Connection::open(&file_path).map_err(duckdb_error)?;
        let memory_limit = pool.field("memory_limit").unwrap_or(Arg::None);
        if memory_limit.__bool__() {
            conn.execute_batch(&alloc::format!(
                "SET memory_limit='{}'",
                memory_limit.__str__()
            ))
            .map_err(duckdb_error)?;
        }
        let legacy: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.columns \
                 WHERE table_name = 'laila_pool_entries' AND column_name = 'pool_id'",
                [],
                |row| row.get(0),
            )
            .map_err(duckdb_error)?;
        if legacy > 0 {
            conn.execute_batch("DROP TABLE laila_pool_entries")
                .map_err(duckdb_error)?;
        }
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS laila_pool_entries (
                key VARCHAR NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            )
            ",
        )
        .map_err(duckdb_error)?;
        *self.conn.lock() = Some(conn);
        Ok(())
    }

    /// Close the DuckDB connection.
    ///
    /// Runs under the pool lock so an in-flight write finishes before the
    /// connection goes away; later callers hit the documented
    /// `RuntimeError("... is closed")` from `_connection()`.
    fn close(&self, pool: &PoolObject) -> Option<Result<()>> {
        let _held = pool.held();
        if self.conn.lock().is_some() {
            if self._checkpoint_pending.load(Ordering::SeqCst) {
                self._run_pending_checkpoint();
            }
            if let Some(conn) = self.conn.lock().take() {
                if let Err((_, e)) = conn.close() {
                    return Some(Err(duckdb_error(e)));
                }
            }
        }
        Some(Ok(()))
    }

    /// Retrieve the JSON value for *key*, or `None` if absent.
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        let row: Option<String> = {
            let _held = pool.held();
            self.with_connection(|conn| {
                conn.query_row(
                    "SELECT value FROM laila_pool_entries WHERE key = ?",
                    [key],
                    |row| row.get(0),
                )
                .optional()
            })?
        };
        match row {
            Some(text) => crate::_compat::pyjson::loads(&text),
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
                "DuckDBPool expects a serialized JSON string.",
            ));
        };
        let _held = pool.held();
        // Index-shard writes are interleaved with the deletes that arm the
        // checkpoint; running it there (mid delete phase) does not let
        // DuckDB reuse the freed blocks. Run it on the next *data* write.
        if !is_index_key(key)? {
            self._run_pending_checkpoint();
        }
        self.with_connection(|conn| {
            conn.execute(
                "
                INSERT INTO laila_pool_entries(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                ",
                [key, &value],
            )
        })?;
        Ok(())
    }

    /// Delete the row for *key* (its size feeds the deferred-checkpoint accounting).
    fn _delete(&self, pool: &PoolObject, key: &str) -> Result<()> {
        let _held = pool.held();
        let row: Option<i64> = self.with_connection(|conn| {
            conn.query_row(
                "DELETE FROM laila_pool_entries WHERE key = ? RETURNING length(value)",
                [key],
                |row| row.get(0),
            )
            .optional()
        })?;
        if let Some(nbytes) = row {
            self._note_churn(nbytes, 1);
        }
        Ok(())
    }

    /// Remove all entries from the pool and checkpoint so the space is reusable.
    fn _empty(&self, pool: &PoolObject) -> Result<()> {
        let _held = pool.held();
        self.with_connection(|conn| conn.execute_batch("DELETE FROM laila_pool_entries"))?;
        match self.with_connection(|conn| conn.execute_batch("CHECKPOINT")) {
            Ok(()) => self.reset_churn(),
            // pragma: no cover - best effort
            Err(_) => self._checkpoint_pending.store(true, Ordering::SeqCst),
        }
        Ok(())
    }

    /// `True` if *key* is present.
    fn _exists(&self, pool: &PoolObject, key: &str) -> Result<bool> {
        let _held = pool.held();
        let row: Option<i64> = self.with_connection(|conn| {
            conn.query_row(
                "SELECT 1 FROM laila_pool_entries WHERE key = ?",
                [key],
                |row| row.get(0),
            )
            .optional()
        })?;
        Ok(row.is_some())
    }

    /// Return all keys in the pool.
    fn _keys(&self, pool: &PoolObject) -> Result<Vec<String>> {
        let _held = pool.held();
        self.with_connection(|conn| {
            let mut stmt = conn.prepare("SELECT key FROM laila_pool_entries ORDER BY key")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<duckdb::Result<Vec<String>>>()
        })
    }

    fn __getattr__(&self, _pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        match name {
            // `self._conn` -- `None` once closed, otherwise the connection.
            "_conn" => Some(Ok(if self.conn.lock().is_some() {
                DuckDBConnection {
                    conn: self.conn.clone(),
                }
                .into_arg()
            } else {
                Arg::None
            })),
            "_churn_bytes" => Some(Ok(Arg::Int(self._churn_bytes.load(Ordering::SeqCst)))),
            "_churn_rows" => Some(Ok(Arg::Int(self._churn_rows.load(Ordering::SeqCst)))),
            "_checkpoint_pending" => Some(Ok(Arg::Bool(
                self._checkpoint_pending.load(Ordering::SeqCst),
            ))),
            _ => None,
        }
    }

    fn __setattr__(&self, _pool: &PoolObject, name: &str, value: Arg) -> Option<Result<()>> {
        match name {
            "_churn_bytes" => self
                ._churn_bytes
                .store(value.as_i64().unwrap_or(0), Ordering::SeqCst),
            "_churn_rows" => self
                ._churn_rows
                .store(value.as_i64().unwrap_or(0), Ordering::SeqCst),
            "_checkpoint_pending" => self
                ._checkpoint_pending
                .store(value.__bool__(), Ordering::SeqCst),
            _ => return None,
        }
        Some(Ok(()))
    }

    fn __call_method__(&self, pool: &PoolObject, name: &str, args: Args) -> Option<Result<Arg>> {
        Some(match name {
            "checkpoint" => {
                let compact = args
                    .positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| args.get("compact"))
                    .__bool__();
                self.checkpoint(pool, compact).map(|()| Arg::None)
            }
            "_connection" => match self.__getattr__(pool, "_conn") {
                Some(Ok(Arg::None)) | None => {
                    Err(LailaError::runtime_error("DuckDBPool is closed."))
                }
                Some(r) => r,
            },
            "_note_churn" => {
                let nbytes = args
                    .positional
                    .first()
                    .cloned()
                    .unwrap_or_else(|| args.get("nbytes"))
                    .as_i64()
                    .unwrap_or(0);
                let rows = args
                    .positional
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| args.get("rows"))
                    .as_i64()
                    .unwrap_or(1);
                self._note_churn(nbytes, rows);
                Ok(Arg::None)
            }
            "_should_compact" => self._should_compact().map(Arg::Bool),
            "_compact" => self._compact().map(|()| Arg::None),
            "_run_pending_checkpoint" => {
                self._run_pending_checkpoint();
                Ok(Arg::None)
            }
            _ => return None,
        })
    }

    fn method_names(&self) -> &'static [&'static str] {
        &[
            "close",
            "checkpoint",
            "_connection",
            "_note_churn",
            "_should_compact",
            "_compact",
            "_run_pending_checkpoint",
        ]
    }
}

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "DuckDBPool",
        "laila.data.duckdb.duckdb",
        alloc::vec![],
        alloc::vec![
            (
                String::from("file_path"),
                field_default("str | None", Arg::None)
            ),
            (
                String::from("transformations"),
                field_default(
                    "TransformationSequence | None",
                    crate::entry::transformation_base64()
                )
            ),
            (
                String::from("memory_limit"),
                field_default("str | None", Arg::None)
            ),
        ],
        alloc::vec![
            (String::from("_conn"), private_default(Arg::None)),
            (String::from("_churn_bytes"), private_default(0i64)),
            (String::from("_churn_rows"), private_default(0i64)),
            (String::from("_checkpoint_pending"), private_default(false)),
        ],
        alloc::vec![(String::from(_PERSISTENT_STORAGE), Arg::Bool(true))],
    )
});

crate::laila_pool_handle!(
    /// DuckDB-backed pool storing entries in a local DuckDB database file.
    ///
    /// Parameters
    /// ----------
    /// file_path : str, optional
    ///     Database file; defaults to a per-pool file under the laila pools
    ///     directory.
    /// memory_limit : str | None, default `None`
    ///     Value for DuckDB's `memory_limit` setting (e.g. `"512MB"`).
    ///     `None` keeps the engine default (80 % of RAM). Note that DuckDB
    ///     treats the limit as hard: exceeding it raises a *fatal* error
    ///     that invalidates the connection, so only set it for workloads
    ///     whose working set you know.
    DuckDBPool,
    DuckDBBackend
);

impl ModelBase for DuckDBPool {
    fn __class__() -> &'static ModelClass {
        DuckDBPool::__class__()
    }
}

impl DuckDBPool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `DuckDBPool(file_path=None, transformations=transformation_base64, memory_limit=None, **kwargs)`
    #[pysig("DuckDBPool(**kwargs)")]
    pub fn new(args: impl Into<Args>) -> Result<DuckDBPool> {
        Ok(DuckDBPool(Pool::construct(
            Self::__class__(),
            kwargs,
            |_inst: &ModelInstance| Ok(Box::new(DuckDBBackend::default())),
        )?))
    }
    /// `pool.file_path`
    pub fn file_path(&self) -> Arg {
        self.0.field("file_path").unwrap_or(Arg::None)
    }
    /// `pool.memory_limit`
    pub fn memory_limit(&self) -> Arg {
        self.0.field("memory_limit").unwrap_or(Arg::None)
    }
    /// `pool._conn` -- the connection (`None` once closed).
    pub fn _conn(&self) -> Arg {
        self.0.obj().__getattr__("_conn").unwrap_or(Arg::None)
    }
    /// `pool._churn_bytes`
    pub fn _churn_bytes(&self) -> Arg {
        self.0
            .obj()
            .__getattr__("_churn_bytes")
            .unwrap_or(Arg::Int(0))
    }
    /// `pool._churn_rows`
    pub fn _churn_rows(&self) -> Arg {
        self.0
            .obj()
            .__getattr__("_churn_rows")
            .unwrap_or(Arg::Int(0))
    }
    /// `pool._checkpoint_pending`
    pub fn _checkpoint_pending(&self) -> Arg {
        self.0
            .obj()
            .__getattr__("_checkpoint_pending")
            .unwrap_or(Arg::Bool(false))
    }
    /// `pool.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
    /// `pool.checkpoint(compact=False)`
    #[pysig("checkpoint(self, compact=False)")]
    pub fn checkpoint(&self, args: impl Into<Args>) -> Result<()> {
        self.0
            .obj()
            .__call_method__("checkpoint", crate::args!(compact))
            .map(|_| ())
    }
}

impl crate::python::builtins::CallValue for DuckDBPool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        DuckDBPool::new(args).map(IntoArg::into_arg)
    }
}

/// `DuckDBPool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    DuckDBPool::__class__().as_pyclass().into_arg()
}

impl PyArg for DuckDBBackend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
