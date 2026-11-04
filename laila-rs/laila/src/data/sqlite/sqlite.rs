//! SQLite pool implementation.
//!
//! Twin of `laila/data/sqlite/sqlite.py`: entries are rows of the
//! `laila_pool_entries(key TEXT PRIMARY KEY, value TEXT)` table of a local
//! database file, written through the system `libsqlite3` (the same
//! library CPython's `sqlite3` module links), so a `.laila_sqlitedb` file
//! written by either implementation opens in the other.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension};

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::atomic::definitions::HasIdentity;
use crate::data::schema::base::{
    field_default, pool_class, private_default, Pool, PoolBackend, PoolObject, _PERSISTENT_STORAGE,
};
use crate::python::errors::ExcKind;
use crate::python::stdlib::os;
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

/// A `sqlite3.OperationalError` / `sqlite3.DatabaseError` surfaced by the
/// C library (`sqlite3.OperationalError`, an `OSError` subclass in the port).
fn sqlite_error(e: rusqlite::Error) -> LailaError {
    LailaError::new(ExcKind::OperationalError, alloc::format!("{e}"))
}

/// `SQLitePool`'s storage: the connection (`self._conn`; `None` once closed).
#[derive(Default)]
pub struct SQLiteBackend {
    conn: Mutex<Option<Connection>>,
}

impl core::fmt::Debug for SQLiteBackend {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("SQLiteBackend")
    }
}

impl SQLiteBackend {
    /// Return the active SQLite connection (`self._connection()`).
    fn with_connection<T>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T> {
        let guard = self.conn.lock();
        match guard.as_ref() {
            None => Err(LailaError::runtime_error("SQLitePool is closed.")),
            Some(conn) => f(conn).map_err(sqlite_error),
        }
    }
}

impl PoolBackend for SQLiteBackend {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }

    /// Open the SQLite database and create the entries table.
    fn model_post_init(&self, pool: &PoolObject) -> Result<()> {
        let file_path = match pool.field("file_path").unwrap_or(Arg::None) {
            Arg::None => {
                let pools =
                    crate::macros::defaults::LAILA_DEFAULT_DIRECTORIES().__getitem__("pools")?;
                let pool_dir = os.path().join(crate::args!(pools, pool.uuid()))?;
                os.makedirs(crate::args!(pool_dir.clone(), exist_ok = true))?;
                os.path()
                    .join(crate::args!(pool_dir, "pool.laila_sqlitedb"))?
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

        let conn = Connection::open(&file_path).map_err(sqlite_error)?;
        let legacy: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('laila_pool_entries') WHERE name='pool_id'",
                [],
                |row| row.get(0),
            )
            .map_err(sqlite_error)?;
        if legacy > 0 {
            conn.execute("DROP TABLE laila_pool_entries", [])
                .map_err(sqlite_error)?;
        }
        conn.execute(
            "
            CREATE TABLE IF NOT EXISTS laila_pool_entries (
                key TEXT NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            )
            ",
            [],
        )
        .map_err(sqlite_error)?;
        *self.conn.lock() = Some(conn);
        Ok(())
    }

    /// Close the SQLite connection.
    ///
    /// Runs under the pool lock so an in-flight write finishes before the
    /// connection goes away; later callers hit the documented
    /// `RuntimeError("... is closed")` from `_connection()`.
    fn close(&self, pool: &PoolObject) -> Option<Result<()>> {
        let _held = pool.held();
        if let Some(conn) = self.conn.lock().take() {
            if let Err((_, e)) = conn.close() {
                return Some(Err(sqlite_error(e)));
            }
        }
        Some(Ok(()))
    }

    /// Retrieve the JSON value for *key*, or `None` if absent.
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        let _held = pool.held();
        let row: Option<String> = self.with_connection(|conn| {
            conn.query_row(
                "SELECT value FROM laila_pool_entries WHERE key = ?",
                [key],
                |row| row.get(0),
            )
            .optional()
        })?;
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
                "SQLitePool expects a serialized JSON string.",
            ));
        };
        let _held = pool.held();
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

    /// Delete the row for *key*.
    fn _delete(&self, pool: &PoolObject, key: &str) -> Result<()> {
        let _held = pool.held();
        self.with_connection(|conn| {
            conn.execute("DELETE FROM laila_pool_entries WHERE key = ?", [key])
        })?;
        Ok(())
    }

    /// Remove all entries from the pool.
    fn _empty(&self, pool: &PoolObject) -> Result<()> {
        let _held = pool.held();
        self.with_connection(|conn| conn.execute("DELETE FROM laila_pool_entries", []))?;
        Ok(())
    }

    /// `True` if *key* is present in the table.
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

    /// Return all keys in the table.
    fn _keys(&self, pool: &PoolObject) -> Result<Vec<String>> {
        let _held = pool.held();
        self.with_connection(|conn| {
            let mut stmt = conn.prepare("SELECT key FROM laila_pool_entries ORDER BY key")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<rusqlite::Result<Vec<String>>>()
        })
    }

    fn __getattr__(&self, _pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        match name {
            // `self._conn` -- `None` once closed, otherwise the (opaque)
            // connection object.
            "_conn" => Some(Ok(if self.conn.lock().is_some() {
                Arg::Str("<sqlite3.Connection>".into())
            } else {
                Arg::None
            })),
            _ => None,
        }
    }

    fn method_names(&self) -> &'static [&'static str] {
        &["close", "_connection"]
    }
}

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "SQLitePool",
        "laila.data.sqlite.sqlite",
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
        ],
        alloc::vec![(String::from("_conn"), private_default(Arg::None))],
        alloc::vec![(String::from(_PERSISTENT_STORAGE), Arg::Bool(true))],
    )
});

crate::laila_pool_handle!(
    /// SQLite-backed pool storing entries in a local database file.
    SQLitePool,
    SQLiteBackend
);

impl ModelBase for SQLitePool {
    fn __class__() -> &'static ModelClass {
        SQLitePool::__class__()
    }
}

impl SQLitePool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `SQLitePool(file_path=None, transformations=transformation_base64, **kwargs)`
    #[pysig("SQLitePool(**kwargs)")]
    pub fn new(args: impl Into<Args>) -> Result<SQLitePool> {
        Ok(SQLitePool(Pool::construct(
            Self::__class__(),
            kwargs,
            |_inst: &ModelInstance| Ok(Box::new(SQLiteBackend::default())),
        )?))
    }
    /// `pool.file_path`
    pub fn file_path(&self) -> Arg {
        self.0.field("file_path").unwrap_or(Arg::None)
    }
    /// `pool._conn` -- the connection (`None` once closed).
    pub fn _conn(&self) -> Arg {
        self.0.obj().__getattr__("_conn").unwrap_or(Arg::None)
    }
    /// `pool.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
}

impl crate::python::builtins::CallValue for SQLitePool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        SQLitePool::new(args).map(IntoArg::into_arg)
    }
}

/// `SQLitePool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    SQLitePool::__class__().as_pyclass().into_arg()
}

impl PyArg for SQLiteBackend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
