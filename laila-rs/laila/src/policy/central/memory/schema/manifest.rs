//! Manifest -- a structured map of user-defined keys to `global_id` references.
//!
//! A Manifest is an [`Entry`] whose payload is a *blueprint*: a nested
//! dictionary whose leaves are `global_id` strings (or lists thereof). A
//! manifest is therefore a normal `READY` entry -- its payload is the
//! blueprint dict itself. The `manifest.realized` property batch-fetches
//! every referenced entry through the active policy's central memory and
//! returns a nested dict of `Entry` objects mirroring the blueprint.
//!
//! Rust shape: `Manifest` is the `Entry` handle tagged
//! [`EntryKind::Manifest`] (`type(x)`, `isinstance`, `repr`, the mapping /
//! iteration protocol and the memory verbs dispatch on the tag), wrapped in
//! a newtype that derefs to [`Entry`] so the subclass methods are typed.
//! The dynamic layer reaches them through [`getattr`] / [`call_method`],
//! which `Entry.__getattr__` / `Entry.__call_method__` consult first.
//!
//! The SQL index (`sql()` / `build_index()`, the sqlite-backed
//! *non-memorizing algorithmic helper*) needs the sqlite backend and the
//! `indices` directory of the laila root: Phase 3. The parser half
//! (`_parse_sql`, `_strip_table_prefix`, `_validate_ident`,
//! `_validate_primitive_rows`) is ported; the index methods keep their
//! names and raise `NotImplementedError`.

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::ops::Deref;

use crate::basics::definitions::identifiable_object::{
    split_global_id_attributes, EVOLUTION_ATTRIBUTE,
};
use crate::data::schema::pool_index::CREATION_TIMESTAMP_ATTRIBUTE;
use crate::entry::constitution::build_maps::register_builder;
use crate::entry::constitution::complex_constitution::{
    register_manifest_check, register_realize_hook,
};
use crate::entry::{Entry, EntryKind, EntryState};
use crate::macros::strings::_MANIFEST_SCOPE;
use crate::policy::central::command::{Future, FutureKind};
use crate::python::builtins::IsInstance;
use crate::python::object::HasAttrs;
use crate::python::pyrepr::str_repr;
use crate::python::sync::Mutex;
use crate::python::{pysig, Arg, Args, ExcKind, IntoArg, LailaError, PyArg, PyDictBuilder, Result};

// ----------------------------------------------------------------------
// Non-memorizing algorithmic help: SQL index
// ----------------------------------------------------------------------

/// Row values the index accepts (`str, int, float, bool, bytes, NoneType`).
pub const _PRIMITIVE_TYPES: &[&str] = &["str", "int", "float", "bool", "bytes", "NoneType"];
pub const _FORBIDDEN_SQL: &[&str] = &["ORDER BY", "LIMIT", "GROUP BY", "HAVING", "JOIN", "UNION"];
pub const _SQL_INDEX_TABLE: &str = "manifest";
pub const _SQL_ROW_IDX_COL: &str = "__row_idx__";
pub const _SQL_META_TABLE: &str = "__laila_index_meta__";

/// `_SQL_IDENT_RE = ^[A-Za-z_][A-Za-z0-9_]*$`
fn is_sql_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `(row_key, row_dict)` pairs of the SQL index.
pub type SqlRows = Vec<(Arg, Arg)>;

/// The `_pending_entries` private attribute.
const PENDING_ATTR: &str = "_pending_entries";
/// The `_sql_state` private attribute (the index handle; never serialized).
#[cfg(feature = "sqlite")]
const SQL_STATE_ATTR: &str = "_sql_state";

static INITIALISED: Mutex<bool> = Mutex::new(false);

/// Import-time side effects of `manifest.py`: the `MANIFEST` scope builder
/// and the `Manifest` hooks of `ComplexConstitution` (`isinstance(x,
/// Manifest)`, `with laila.guarantee: target.realized`).
pub fn __init__() {
    {
        let mut g = INITIALISED.lock();
        if *g {
            return;
        }
        *g = true;
    }
    let sync_fn: crate::entry::constitution::build_maps::SyncBuilder =
        alloc::sync::Arc::new(|d, _kw| {
            Ok(Arg::Entry(Entry::_build_from_dict_sync_as(
                EntryKind::Manifest,
                d,
            )?))
        });
    let async_fn: crate::entry::constitution::build_maps::AsyncBuilder =
        alloc::sync::Arc::new(|d, _kw| {
            Ok(crate::python::stdlib::asyncio::coroutine(async move {
                Entry::_build_from_dict_sync_as(EntryKind::Manifest, d).map(Arg::Entry)
            }))
        });
    register_builder(_MANIFEST_SCOPE, sync_fn, async_fn);
    register_manifest_check(|a| Manifest::from_arg(a).is_some());
    register_realize_hook(|target| match Manifest::from_arg(target) {
        Some(m) => m.realized().map(|_| ()),
        None => Ok(()),
    });
}

/// Entry subclass wrapping a nested dict of `global_id` references.
///
/// A manifest maps user-defined string keys to `global_id` strings, lists
/// of `global_id` strings, or recursively nested dicts following the same
/// rules. It can be constructed from raw ID strings or from `Entry`
/// objects.
///
/// The blueprint *is* the manifest's payload, so `manifest.data` returns
/// the nested mapping of `global_id` strings. `manifest.realized`
/// synchronously fetches every referenced entry via the active policy's
/// central memory and returns a nested dict of `Entry` objects mirroring
/// the blueprint structure.
///
/// Parameters: `data` / `blueprint` (a nested dict whose leaves are
/// `global_id` strings, lists of them, or `Entry` instances -- entries are
/// converted to a blueprint of `global_id` strings and stashed for a
/// subsequent `memorize()` call), `uuid`, `nickname`, `global_id` (identity,
/// as on `Entry`).
#[derive(Clone)]
pub struct Manifest(pub Entry);

impl Deref for Manifest {
    type Target = Entry;
    fn deref(&self) -> &Entry {
        &self.0
    }
}

fn type_repr(v: &Arg) -> String {
    alloc::format!("<class '{}'>", v.type_name())
}

fn list_arg(items: Vec<Arg>) -> Arg {
    items.into_arg()
}

#[cfg(feature = "sqlite")]
fn strs_to_list(v: &[String]) -> Arg {
    v.iter()
        .map(|s| Arg::Str(s.as_str().into()))
        .collect::<Vec<_>>()
        .into_arg()
}

/// The manifest's non-memorizing SQL index: the owned `sqlite3`
/// connection (`_LockedConnection`), its data-side metadata (`_SqlState`)
/// and the temporary-file finalizer (`weakref.finalize` is the handle's
/// `Drop`).
#[cfg(feature = "sqlite")]
mod sql_index {
    use alloc::string::{String, ToString};
    use alloc::sync::Arc;
    use alloc::vec::Vec;

    use rusqlite::Connection;

    use crate::python::object::PyObject;
    use crate::python::sync::{Mutex, RLock};
    use crate::python::{Arg, ExcKind, LailaError, Result};

    pub use rusqlite::types::Value;

    /// `sqlite3.OperationalError` (an `OSError` subclass in the port);
    /// a closed connection is `sqlite3.ProgrammingError`, as in CPython.
    pub fn sqlite_error(e: rusqlite::Error) -> LailaError {
        let msg = message_of(&e);
        let kind = if msg == CLOSED_MESSAGE || msg == MULTI_STATEMENT_MESSAGE {
            ExcKind::ProgrammingError
        } else {
            ExcKind::OperationalError
        };
        LailaError::new(kind, msg)
    }

    /// CPython's text for a use-after-close.
    const CLOSED_MESSAGE: &str = "Cannot operate on a closed database.";
    /// CPython's text for `execute("a; b")`.
    const MULTI_STATEMENT_MESSAGE: &str = "You can only execute one statement at a time.";

    /// Does *sql* carry a second statement after a `;` (outside quotes)?
    fn has_trailing_statement(sql: &str) -> bool {
        let mut quote: Option<char> = None;
        let mut rest: Option<&str> = None;
        for (i, ch) in sql.char_indices() {
            match quote {
                Some(q) if ch == q => quote = None,
                Some(_) => {}
                None if ch == '\'' || ch == '"' => quote = Some(ch),
                None if ch == ';' => {
                    rest = Some(&sql[i + 1..]);
                    break;
                }
                None => {}
            }
        }
        rest.is_some_and(|r| !r.trim().is_empty())
    }

    /// `str(exc)` of the sqlite error (the C library's message).
    pub fn message_of(e: &rusqlite::Error) -> String {
        match e {
            rusqlite::Error::SqliteFailure(_, Some(m)) => m.clone(),
            other => other.to_string(),
        }
    }

    /// `with self.atomic():` guard over the manifest's re-entrant lock.
    pub struct Held(RLock);
    impl Held {
        pub fn acquire(lock: &RLock) -> Held {
            lock.acquire(None);
            Held(lock.clone())
        }
    }
    impl Drop for Held {
        fn drop(&mut self) {
            let _ = self.0.release();
        }
    }

    /// Python `sqlite3` binding of a primitive row value.
    pub fn value_of(v: &Arg) -> Value {
        match v {
            Arg::None => Value::Null,
            Arg::Bool(b) => Value::Integer(i64::from(*b)),
            Arg::Int(i) => Value::Integer(*i),
            Arg::Float(f) => Value::Real(*f),
            Arg::Str(s) => Value::Text(s.to_string()),
            Arg::Bytes(b) => Value::Blob(b.to_vec()),
            other => Value::Text(other.__str__()),
        }
    }

    /// A `TEXT` parameter.
    pub fn text(s: &str) -> Value {
        Value::Text(s.to_string())
    }

    /// `sorted(columns)` (column names are validated identifiers: `str`).
    pub fn sorted_columns(columns: &[Arg]) -> Vec<String> {
        let mut out: Vec<String> = columns.iter().map(|c| c.__str__()).collect();
        out.sort();
        out
    }

    /// `_LockedConnection`: every statement runs *and* fetches under one
    /// lock, so callers get a complete, consistent result from any thread.
    pub struct LockedConnection {
        conn: Mutex<Option<Connection>>,
    }

    impl LockedConnection {
        /// `sqlite3.connect(db_path, check_same_thread=False)`
        pub fn connect(db_path: &str) -> Result<LockedConnection> {
            let conn = Connection::open(db_path).map_err(sqlite_error)?;
            Ok(LockedConnection {
                conn: Mutex::new(Some(conn)),
            })
        }

        fn with<T>(
            &self,
            f: impl FnOnce(&Connection) -> rusqlite::Result<T>,
        ) -> rusqlite::Result<T> {
            let g = self.conn.lock();
            match g.as_ref() {
                Some(c) => f(c),
                None => Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                    Some(CLOSED_MESSAGE.into()),
                )),
            }
        }

        pub fn execute(&self, sql: &str, params: &[Value]) -> Result<()> {
            self.with(|c| {
                c.execute(sql, rusqlite::params_from_iter(params.iter()))
                    .map(|_| ())
            })
            .map_err(sqlite_error)
        }

        pub fn executemany(&self, sql: &str, seq_of_params: &[Vec<Value>]) -> Result<()> {
            self.with(|c| {
                let mut stmt = c.prepare(sql)?;
                for params in seq_of_params {
                    stmt.execute(rusqlite::params_from_iter(params.iter()))?;
                }
                Ok(())
            })
            .map_err(sqlite_error)
        }

        /// The first column of every row, as integers.
        pub fn query_i64s(&self, sql: &str) -> core::result::Result<Vec<i64>, rusqlite::Error> {
            self.with(|c| {
                // `sqlite3.Cursor.execute`: one statement only.
                if has_trailing_statement(sql) {
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                        Some(MULTI_STATEMENT_MESSAGE.into()),
                    ));
                }
                let mut stmt = c.prepare(sql)?;
                let rows = stmt.query_map([], |r| r.get::<_, i64>(0))?;
                rows.collect()
            })
        }

        /// The first column of every row, as strings.
        pub fn query_strings(&self, sql: &str, params: &[Value]) -> Result<Vec<String>> {
            self.query_strings_at_params(sql, params, 0)
        }

        /// Column `at` of every row, as strings.
        pub fn query_strings_at(&self, sql: &str, at: usize) -> Result<Vec<String>> {
            self.query_strings_at_params(sql, &[], at)
        }

        fn query_strings_at_params(
            &self,
            sql: &str,
            params: &[Value],
            at: usize,
        ) -> Result<Vec<String>> {
            self.with(|c| {
                let mut stmt = c.prepare(sql)?;
                let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |r| {
                    r.get::<_, String>(at)
                })?;
                rows.collect()
            })
            .map_err(sqlite_error)
        }

        /// Every row of *sql* as a tuple of Python values (the dynamic
        /// `conn.execute(sql).fetchall()` of a test poking the index).
        pub fn query_rows(&self, sql: &str) -> Result<Vec<Arg>> {
            self.with(|c| {
                let mut stmt = c.prepare(sql)?;
                let n = stmt.column_count();
                let rows = stmt.query_map([], |row| {
                    let mut cells = Vec::with_capacity(n);
                    for i in 0..n {
                        use rusqlite::types::ValueRef;
                        cells.push(match row.get_ref(i)? {
                            ValueRef::Null => Arg::None,
                            ValueRef::Integer(v) => Arg::Int(v),
                            ValueRef::Real(v) => Arg::Float(v),
                            ValueRef::Text(t) => {
                                Arg::Str(String::from_utf8_lossy(t).as_ref().into())
                            }
                            ValueRef::Blob(b) => Arg::Bytes(b.to_vec().into()),
                        });
                    }
                    Ok(Arg::Tuple(cells.into()))
                })?;
                rows.collect()
            })
            .map_err(sqlite_error)
        }

        /// `conn.commit()` -- autocommit here (no open transaction).
        pub fn commit(&self) -> Result<()> {
            Ok(())
        }

        /// `conn.close()`; a second close is a no-op.
        pub fn close(&self) -> Result<()> {
            let mut g = self.conn.lock();
            if let Some(c) = g.take() {
                c.close().map_err(|(_, e)| sqlite_error(e))?;
            }
            Ok(())
        }
    }

    /// `_SqlState`: the owned connection plus the data-side metadata needed
    /// to map query results back to blueprint keys.
    pub struct SqlState {
        pub conn: LockedConnection,
        pub db_path: String,
        pub is_persistent: bool,
        pub table_name: String,
        pub columns: Vec<String>,
        pub indexed: Vec<Arg>,
        pub row_keys: Vec<Arg>,
        pub stale: bool,
    }

    struct Inner {
        state: Mutex<SqlState>,
        conn: Arc<LockedConnection>,
        /// `weakref.finalize` attached (detached by `clear_index`).
        finalize: Mutex<bool>,
    }

    /// `_sql_state.conn` as a dynamic value: the shared `_LockedConnection`.
    #[derive(Clone)]
    pub struct SharedConnection(pub Arc<LockedConnection>);

    impl PyObject for SharedConnection {
        fn as_any(&self) -> &dyn core::any::Any {
            self
        }
        fn type_name(&self) -> &'static str {
            "_LockedConnection"
        }
        fn __repr__(&self) -> String {
            alloc::format!(
                "<_LockedConnection at {:#x}>",
                Arc::as_ptr(&self.0) as usize
            )
        }
        fn __eq__(&self, other: &dyn PyObject) -> bool {
            other
                .as_any()
                .downcast_ref::<SharedConnection>()
                .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
        }
        /// One handle per index connection: `state.conn is state.conn`.
        fn __is__(&self, other: &dyn PyObject) -> bool {
            self.__eq__(other)
        }
        fn __hash__(&self) -> Result<i64> {
            Ok((Arc::as_ptr(&self.0) as usize >> 4) as i64)
        }
        /// `conn.execute(sql)` -> a cursor (`fetchall()` / `fetchone()`),
        /// `conn.close()`, `conn.commit()`.
        fn __call_method__(&self, name: &str, args: crate::python::Args) -> Result<Arg> {
            match name {
                "execute" => {
                    let sql = args
                        .positional
                        .first()
                        .and_then(|a| a.as_str().map(String::from))
                        .ok_or_else(|| {
                            LailaError::type_error("execute() argument 1 must be str")
                        })?;
                    let rows = self.0.query_rows(&sql)?;
                    Ok(Arg::Object(Arc::new(_SqlResult(Mutex::new(rows)))))
                }
                "commit" => self.0.commit().map(|_| Arg::None),
                "close" => self.0.close().map(|_| Arg::None),
                _ => Err(LailaError::attribute_error(alloc::format!(
                    "'_LockedConnection' object has no attribute '{name}'"
                ))),
            }
        }
    }

    /// The rows of one `conn.execute(..)`, consumed by `fetchall()` /
    /// `fetchone()`.
    #[allow(non_camel_case_types)]
    pub struct _SqlResult(Mutex<Vec<Arg>>);

    impl PyObject for _SqlResult {
        fn as_any(&self) -> &dyn core::any::Any {
            self
        }
        fn type_name(&self) -> &'static str {
            "Cursor"
        }
        fn __repr__(&self) -> String {
            alloc::format!("<sqlite3.Cursor at {:#x}>", self as *const _ as usize)
        }
        fn __eq__(&self, other: &dyn PyObject) -> bool {
            core::ptr::eq(
                other
                    .as_any()
                    .downcast_ref::<_SqlResult>()
                    .map_or(core::ptr::null(), |o| o),
                self,
            )
        }
        fn __hash__(&self) -> Result<i64> {
            Ok((self as *const _ as usize >> 4) as i64)
        }
        fn __call_method__(&self, name: &str, _args: crate::python::Args) -> Result<Arg> {
            match name {
                "fetchall" => {
                    let rows: Vec<Arg> = core::mem::take(&mut *self.0.lock());
                    Ok(crate::python::IntoArg::into_arg(rows))
                }
                "fetchone" => {
                    let mut g = self.0.lock();
                    if g.is_empty() {
                        Ok(Arg::None)
                    } else {
                        Ok(g.remove(0))
                    }
                }
                _ => Err(LailaError::attribute_error(alloc::format!(
                    "'sqlite3.Cursor' object has no attribute '{name}'"
                ))),
            }
        }
    }

    /// The `_sql_state` private attribute: a shared handle to the index.
    /// Dropping the last handle runs the temporary-file finalizer
    /// (`_sql_finalize`: close the connection, unlink the db file).
    #[derive(Clone)]
    pub struct SqlStateHandle(Arc<Inner>);

    impl SqlStateHandle {
        pub fn new(state: SqlState) -> SqlStateHandle {
            let SqlState {
                conn,
                db_path,
                is_persistent,
                table_name,
                columns,
                indexed,
                row_keys,
                stale,
            } = state;
            let conn = Arc::new(conn);
            let finalize = !is_persistent;
            SqlStateHandle(Arc::new(Inner {
                state: Mutex::new(SqlState {
                    conn: LockedConnection {
                        conn: Mutex::new(None),
                    },
                    db_path,
                    is_persistent,
                    table_name,
                    columns,
                    indexed,
                    row_keys,
                    stale,
                }),
                conn,
                finalize: Mutex::new(finalize),
            }))
        }
        pub fn conn(&self) -> Arc<LockedConnection> {
            self.0.conn.clone()
        }
        pub fn db_path(&self) -> String {
            self.0.state.lock().db_path.clone()
        }
        pub fn is_persistent(&self) -> bool {
            self.0.state.lock().is_persistent
        }
        pub fn table_name(&self) -> String {
            self.0.state.lock().table_name.clone()
        }
        pub fn columns(&self) -> Vec<String> {
            self.0.state.lock().columns.clone()
        }
        pub fn set_columns(&self, columns: Vec<String>) {
            self.0.state.lock().columns = columns;
        }
        pub fn row_keys(&self) -> Vec<Arg> {
            self.0.state.lock().row_keys.clone()
        }
        pub fn set_row_keys(&self, row_keys: Vec<Arg>) {
            self.0.state.lock().row_keys = row_keys;
        }
        pub fn stale(&self) -> bool {
            self.0.state.lock().stale
        }
        pub fn set_stale(&self, stale: bool) {
            self.0.state.lock().stale = stale;
        }
        /// `finalizer.detach()`
        pub fn detach(&self) {
            *self.0.finalize.lock() = false;
        }
    }

    impl Drop for Inner {
        fn drop(&mut self) {
            // `_sql_finalize(conn, db_path)` for a temporary index.
            if !*self.finalize.lock() {
                return;
            }
            let _ = self.conn.close();
            unlink_db(&self.state.lock().db_path);
        }
    }

    /// `manifest._sql_finalizer`: Python's `weakref.finalize` handle on a
    /// temporary index. The port finalizes in the state handle's `Drop`,
    /// so the handle is `alive` for as long as the state it watches is.
    pub struct SqlFinalizer(pub SqlStateHandle);

    impl PyObject for SqlFinalizer {
        fn as_any(&self) -> &dyn core::any::Any {
            self
        }
        fn type_name(&self) -> &'static str {
            "finalize"
        }
        fn __repr__(&self) -> String {
            alloc::format!(
                "<finalize object at {:#x}; for '_SqlState'>",
                Arc::as_ptr(&self.0 .0) as usize
            )
        }
        fn __eq__(&self, other: &dyn PyObject) -> bool {
            other
                .as_any()
                .downcast_ref::<SqlFinalizer>()
                .is_some_and(|o| Arc::ptr_eq(&self.0 .0, &o.0 .0))
        }
        fn __hash__(&self) -> Result<i64> {
            Ok((Arc::as_ptr(&self.0 .0) as usize >> 4) as i64)
        }
        fn __getattr__(&self, name: &str) -> Result<Arg> {
            match name {
                "alive" => Ok(Arg::Bool(!self.0.is_persistent())),
                _ => Err(LailaError::attribute_error(alloc::format!(
                    "'finalize' object has no attribute '{name}'"
                ))),
            }
        }
    }

    impl PyObject for SqlStateHandle {
        fn as_any(&self) -> &dyn core::any::Any {
            self
        }
        fn type_name(&self) -> &'static str {
            "_SqlState"
        }
        fn __repr__(&self) -> String {
            let s = self.0.state.lock();
            alloc::format!(
                "_SqlState(db_path={}, is_persistent={}, table_name={}, columns={}, stale={})",
                crate::python::pyrepr::str_repr(&s.db_path),
                if s.is_persistent { "True" } else { "False" },
                crate::python::pyrepr::str_repr(&s.table_name),
                super::strs_to_list(&s.columns).__repr__(),
                if s.stale { "True" } else { "False" }
            )
        }
        fn __eq__(&self, other: &dyn PyObject) -> bool {
            other
                .as_any()
                .downcast_ref::<SqlStateHandle>()
                .is_some_and(|o| Arc::ptr_eq(&self.0, &o.0))
        }
        fn __hash__(&self) -> Result<i64> {
            Ok((Arc::as_ptr(&self.0) as usize >> 4) as i64)
        }
        fn __getattr__(&self, name: &str) -> Result<Arg> {
            let s = self.0.state.lock();
            Ok(match name {
                "db_path" => Arg::Str(s.db_path.as_str().into()),
                "is_persistent" => Arg::Bool(s.is_persistent),
                "table_name" => Arg::Str(s.table_name.as_str().into()),
                "columns" => super::strs_to_list(&s.columns),
                // `indexed: set` of column names and composite tuples.
                "indexed" => crate::python::builtins::set(crate::python::Args::from(
                    crate::python::IntoArg::into_arg(s.indexed.clone()),
                ))?,
                "row_keys" => crate::python::IntoArg::into_arg(s.row_keys.clone()),
                "stale" => Arg::Bool(s.stale),
                "conn" => Arg::Object(Arc::new(SharedConnection(self.0.conn.clone()))),
                _ => {
                    return Err(LailaError::attribute_error(alloc::format!(
                        "'_SqlState' object has no attribute '{name}'"
                    )))
                }
            })
        }
        fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
            match name {
                "stale" => {
                    self.0.state.lock().stale = value.__bool__();
                    Ok(())
                }
                _ => Err(LailaError::attribute_error(alloc::format!(
                    "'_SqlState' object has no attribute '{name}'"
                ))),
            }
        }
    }

    /// Remove an index db file and its (now-empty) per-manifest dir.
    pub fn unlink_db(db_path: &str) {
        if db_path.is_empty() {
            return;
        }
        let p = std::path::Path::new(db_path);
        if p.exists() {
            let _ = std::fs::remove_file(p);
        }
        if let Some(parent) = p.parent() {
            if parent.is_dir()
                && std::fs::read_dir(parent)
                    .map(|mut d| d.next().is_none())
                    .unwrap_or(false)
            {
                let _ = std::fs::remove_dir(parent);
            }
        }
    }
}

impl Manifest {
    /// `Manifest._ALLOWS_NON_NA_STATE` -- inherited from `Entry` (`True`):
    /// a manifest is a normal `READY` entry whose payload is its blueprint.
    pub const _ALLOWS_NON_NA_STATE: bool = crate::entry::_ALLOWS_NON_NA_STATE;

    /// `_DEFAULT_SCOPES: ClassVar[list[str]] = [_MANIFEST_SCOPE]`
    pub const _DEFAULT_SCOPES: &'static [&'static str] = &[_MANIFEST_SCOPE];

    /// `Manifest.resolve_global_id(ref, *, evolution=None, prefix_scopes=None, default_scopes=None, parse_evolution=True)`:
    /// the identifiable-object classmethod with the manifest's default scope.
    #[pysig("resolve_global_id(ref, *, evolution=None, prefix_scopes=None, default_scopes=None, parse_evolution=True)")]
    pub fn resolve_global_id(args: impl Into<Args>) -> Result<String> {
        crate::basics::definitions::identifiable_object::resolve_global_id_bound(
            Self::_DEFAULT_SCOPES,
            &r#ref,
            &evolution,
            &prefix_scopes,
            &default_scopes,
            &parse_evolution,
        )
    }

    /// `Manifest(**data)`: see the type-level docs for the accepted keys.
    #[pysig("Manifest(**data)")]
    pub fn new(args: impl Into<Args>) -> Result<Manifest> {
        let mut data = data;
        let raw_data = data.pop_or("data", Arg::None);
        let blueprint_kwarg = data.pop_or("blueprint", Arg::None);

        let mut blueprint: Option<Arg> = None;
        let mut pending: Option<Vec<Arg>> = None;

        let source = if !raw_data.is_none() {
            raw_data
        } else {
            blueprint_kwarg
        };

        if !source.is_none() {
            let (has_entries, has_strings) = Manifest::_classify_data(&source)?;

            if has_entries && has_strings {
                return Err(LailaError::value_error(
                    "Manifest data must contain either all Entry objects or all \
                     global_id strings, not a mix of both.",
                ));
            }

            if has_entries {
                blueprint = Some(Manifest::_extract_blueprint(&source)?);
                pending = Some(Manifest::_iter_entries(&source)?);
            } else {
                Manifest::_validate_blueprint(&source)?;
                blueprint = Some(source.deepcopy());
            }
        }

        let mut entry_kwargs = data;
        entry_kwargs.keywords.retain(|(k, _)| k != "evolution");
        entry_kwargs.push_keyword("evolution", Arg::None);
        if let Some(bp) = blueprint {
            entry_kwargs.keywords.retain(|(k, _)| k != "data");
            entry_kwargs.push_keyword("data", bp);
            if !entry_kwargs.__contains__("state")? {
                entry_kwargs.push_keyword("state", EntryState::READY);
            }
        }

        let entry = Entry::__init__(EntryKind::Manifest, entry_kwargs)?;
        let m = Manifest(entry);
        m.set_pending_entries(pending);
        Ok(m)
    }

    /// `isinstance(x, Manifest)` -> the typed handle.
    pub fn from_arg(x: &Arg) -> Option<Manifest> {
        match x {
            Arg::Entry(e) => Manifest::from_entry(e),
            _ => None,
        }
    }

    /// The `Entry` handle as a `Manifest` when its class is `Manifest`.
    pub fn from_entry(e: &Entry) -> Option<Manifest> {
        (e.kind() == EntryKind::Manifest).then(|| Manifest(e.clone()))
    }

    /// The underlying `Entry` handle (the same object).
    pub fn entry(&self) -> &Entry {
        &self.0
    }

    /// Python `is`.
    pub fn is(&self, other: &Manifest) -> bool {
        self.0.is(&other.0)
    }

    // ------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------

    /// The blueprint payload (see `Entry.data`).
    pub fn data(&self) -> Result<Arg> {
        self.0.data()
    }

    /// `manifest.data = new_data`: replace the blueprint and mark any SQL
    /// index stale.
    pub fn set_data(&self, new_data: impl IntoArg) -> Result<()> {
        self.0.set_data(new_data)?;
        self.invalidate_index();
        Ok(())
    }

    /// The nested dict with `global_id` strings as leaf values
    /// (equivalent to `manifest.data`).
    pub fn blueprint(&self) -> Result<Arg> {
        self.data()
    }

    /// `self._pending_entries`: the `Entry` objects given at construction
    /// time, awaiting `memorize()`.
    pub fn _pending_entries(&self) -> Arg {
        self.0.__attrs__().get(PENDING_ATTR).unwrap_or(Arg::None)
    }

    /// `self._pending_entries = value`
    pub fn set_pending_entries(&self, pending: Option<Vec<Arg>>) {
        let _ = self
            .0
            .__attrs__()
            .__setattr__(PENDING_ATTR, pending.map(list_arg));
    }

    fn pending_vec(&self) -> Option<Vec<Arg>> {
        let p = self._pending_entries();
        if p.is_none() {
            None
        } else {
            p.__iter__().ok()
        }
    }

    /// `manifest.realized`: synchronously fetch all referenced entries and
    /// return them as a nested dict of `Entry` objects mirroring the
    /// blueprint structure. No caching -- each access re-fetches.
    ///
    /// `RuntimeError` if the manifest has no blueprint; `KeyError` if any
    /// referenced entry is missing from the routed pool.
    pub fn realized(&self) -> Result<Arg> {
        let bp = self.data()?;
        if bp.is_none() {
            return Err(LailaError::runtime_error(
                "No blueprint to resolve \u{2014} manifest is empty.",
            ));
        }

        let all_gids = self.__iter__()?;
        if all_gids.is_empty() {
            return Manifest::_rebuild_with_entries(&bp, &PyDictBuilder::new().finish());
        }

        let mut results: Option<Vec<Arg>> = None;
        let mut last_exc: Option<LailaError> = None;
        for pool in self._candidate_pools() {
            match self._read(&all_gids, &pool) {
                Ok(r) => {
                    results = Some(r);
                    break;
                }
                Err(e) if e.kind.is_subclass_of(ExcKind::KeyError) => last_exc = Some(e),
                Err(e) => return Err(e),
            }
        }
        let _ = last_exc; // ``raise KeyError(..) from last_exc``
        let Some(results) = results else {
            return Err(LailaError::key_error(str_repr(
                "Manifest.realized: one or more referenced entries failed to resolve.",
            )));
        };

        if results.len() != all_gids.len() || results.iter().any(Arg::is_none) {
            return Err(LailaError::key_error(str_repr(
                "Manifest.realized: one or more referenced entries failed to resolve.",
            )));
        }

        let mut resolved_map = PyDictBuilder::new();
        for (gid, r) in all_gids.iter().zip(results) {
            resolved_map.insert(gid.clone(), r);
        }
        Manifest::_rebuild_with_entries(&bp, &resolved_map.finish())
    }

    /// Fetch every gid from *pool* (`None` = alpha) through central memory
    /// (`memory._read_entries_direct(all_gids, pool=pool, persist=False)`).
    fn _read(&self, all_gids: &[Arg], pool: &Option<String>) -> Result<Vec<Arg>> {
        let mut a = Args::new();
        a.push_positional(all_gids.to_vec().into_arg());
        a.push_keyword("pool", pool.clone());
        a.push_keyword("persist", false);
        // ``list(ref.wait(None).data)`` -- the manifest owns this future;
        // release it from the bank once consumed.
        let fut = crate::laila.memory()._read_entries_direct(a)?;
        let out = fut
            .wait(Args::new())
            .and_then(|r| r.__getattr__("data"))
            .and_then(|d| d.__iter__());
        let _ = fut.release(Args::new());
        out
    }

    /// Pools to look for referenced entries in, most likely first:
    /// the pool this manifest was last read from / written to
    /// (`_source_pool_gid`), the alpha pool (`None`), then every other
    /// registered pool (`memory.pool_router.pools`).
    pub fn _candidate_pools(&self) -> Vec<Option<String>> {
        let hint = self.0._source_pool_gid();
        let policy = crate::policy::get_active_policy();
        let alpha = policy.alpha_pool_gid();
        // ``router.pools`` holds every registered pool, the alpha included.
        let known: Vec<String> = policy
            .pool_router()
            .pools()
            .__iter__()
            .map(|ks| ks.iter().map(|k| k.__str__()).collect())
            .unwrap_or_default();
        let mut order: Vec<Option<String>> = Vec::new();
        if let Some(h) = &hint {
            if known.iter().any(|k| k == h) {
                order.push(Some(h.clone()));
            }
        }
        if hint.as_deref() != Some(alpha.as_str()) {
            order.push(None);
        }
        for gid in known {
            if Some(gid.as_str()) != hint.as_deref() && gid != alpha {
                order.push(Some(gid));
            }
        }
        order
    }

    /// `await manifest.async_realized`: the awaitable twin of
    /// [`realized`](Manifest::realized) (same semantics; central memory's
    /// reads settle inline until the command taskforces land, Phase 4).
    pub async fn async_realized(&self) -> Result<Arg> {
        let bp = self.data()?;
        if bp.is_none() {
            return Err(LailaError::runtime_error(
                "No blueprint to resolve \u{2014} manifest is empty.",
            ));
        }
        let all_gids = self.__iter__()?;
        if all_gids.is_empty() {
            return Manifest::_rebuild_with_entries(&bp, &PyDictBuilder::new().finish());
        }
        let mut results: Option<Vec<Arg>> = None;
        for pool in self._candidate_pools() {
            let mut a = Args::new();
            a.push_positional(all_gids.clone().into_arg());
            a.push_keyword("pool", pool.clone());
            a.push_keyword("persist", false);
            let fut = match crate::laila.memory()._read_entries_direct(a) {
                Ok(f) => f,
                Err(e) if e.kind.is_subclass_of(ExcKind::KeyError) => continue,
                Err(e) => return Err(e),
            };
            // ``list((await ref).data)``; the manifest owns the future and
            // releases it once consumed.
            let awaited = fut.__await__().await;
            let _ = fut.release(Args::new());
            match awaited.and_then(|r| r.__getattr__("data")) {
                Ok(out) => {
                    results = Some(out.__iter__()?);
                    break;
                }
                Err(e) if e.kind.is_subclass_of(ExcKind::KeyError) => continue,
                Err(e) => return Err(e),
            }
        }
        let Some(results) = results else {
            return Err(LailaError::key_error(str_repr(
                "Manifest.async_realized: one or more referenced entries failed to resolve.",
            )));
        };
        if results.len() != all_gids.len() || results.iter().any(Arg::is_none) {
            return Err(LailaError::key_error(str_repr(
                "Manifest.async_realized: one or more referenced entries failed to resolve.",
            )));
        }
        let mut resolved_map = PyDictBuilder::new();
        for (gid, r) in all_gids.iter().zip(results) {
            resolved_map.insert(gid.clone(), r);
        }
        Manifest::_rebuild_with_entries(&bp, &resolved_map.finish())
    }

    // ------------------------------------------------------------------
    // Core operations
    // ------------------------------------------------------------------

    /// Upload all referenced entries and store the manifest itself.
    ///
    /// Collects any pending `Entry` objects provided at construction time,
    /// uploads them in batches, then stores the manifest itself (whose
    /// payload is the blueprint dict). `laila.memorize(manifest, dst_pool=...)`
    /// dispatches here. Returns a future over the whole batch (central memory
    /// settles it inline until the taskforces land; the children's results
    /// are its data).
    #[pysig("memorize(*, pool=None, pool_nickname=None, pool_id=None, batch_size=128)")]
    pub fn memorize(&self, args: impl Into<Args>) -> Result<Future> {
        if self.data()?.is_none() {
            return Err(LailaError::runtime_error(
                "Nothing to memorize \u{2014} manifest has no blueprint.",
            ));
        }
        let routed = Manifest::_resolve_pool(pool, pool_nickname, pool_id);
        let batch_size = batch_size.as_i64().unwrap_or(128).max(1) as usize;
        let mut all_results: Vec<Arg> = Vec::new();
        // Children and the manifest itself land in ``routed``: remember
        // it so ``realized`` looks there first.
        if !routed.is_none() {
            if let Ok(resolved) = crate::policy::get_active_policy().resolve_pool(&routed) {
                self.0.set_source_pool_gid(Some(resolved.global_id()));
            }
        } else {
            self.0.set_source_pool_gid(None);
        }

        if let Some(pending) = self.pending_vec() {
            if !pending.is_empty() {
                for batch in pending.chunks(batch_size) {
                    let mut a = Args::new();
                    a.push_positional(batch.to_vec().into_arg());
                    a.push_keyword("dst_pool", routed.clone());
                    let fut = crate::laila.memorize(a)?.into_local()?;
                    all_results.extend(Manifest::_collect_future_ids(&fut)?);
                }
            }
            self.set_pending_entries(None);
        }

        // Wrapped in a list on purpose: a *bare* Manifest handed to
        // ``laila.memorize`` dispatches back to this method, whereas a
        // list stores the manifest as a plain entry (blueprint payload).
        let mut a = Args::new();
        a.push_positional(alloc::vec![Arg::Entry(self.0.clone())].into_arg());
        a.push_keyword("dst_pool", routed);
        let self_ref = crate::laila.memorize(a)?.into_local()?;
        all_results.extend(Manifest::_collect_future_ids(&self_ref)?);

        Manifest::_group_over(all_results)
    }

    /// Recall all referenced entries from the pool. The returned future's
    /// data is the flat list of entries in `list(manifest)` order.
    #[pysig(
        "remember(*, pool=None, pool_nickname=None, pool_id=None, batch_size=128, persist=True)"
    )]
    pub fn remember(&self, args: impl Into<Args>) -> Result<Future> {
        if self.data()?.is_none() {
            return Err(LailaError::runtime_error(
                "No blueprint to resolve \u{2014} manifest is empty.",
            ));
        }
        let routed = Manifest::_resolve_pool(pool, pool_nickname, pool_id);
        let all_gids = self.__iter__()?;
        let batch_size = batch_size.as_i64().unwrap_or(128).max(1) as usize;
        let mut all_results: Vec<Arg> = Vec::new();
        for batch in all_gids.chunks(batch_size) {
            let mut a = Args::new();
            a.push_positional(batch.to_vec().into_arg());
            a.push_keyword("dst_pool", routed.clone());
            a.push_keyword("persist", persist.clone());
            let fut = crate::laila.remember(a)?.into_local()?;
            all_results.extend(Manifest::_collect_future_ids(&fut)?);
        }
        Manifest::_group_over(all_results)
    }

    /// Delete all referenced entries and the manifest itself from the pool.
    #[pysig("forget(*, pool=None, pool_nickname=None, pool_id=None, batch_size=128)")]
    pub fn forget(&self, args: impl Into<Args>) -> Result<Future> {
        if self.data()?.is_none() {
            return Err(LailaError::runtime_error(
                "No blueprint \u{2014} nothing to forget.",
            ));
        }
        let routed = Manifest::_resolve_pool(pool, pool_nickname, pool_id);
        let all_gids = self.__iter__()?;
        let batch_size = batch_size.as_i64().unwrap_or(128).max(1) as usize;
        let mut all_results: Vec<Arg> = Vec::new();
        for batch in all_gids.chunks(batch_size) {
            let mut a = Args::new();
            a.push_positional(batch.to_vec().into_arg());
            a.push_keyword("pool", routed.clone());
            let fut = crate::laila.forget(a)?.into_local()?;
            all_results.extend(Manifest::_collect_future_ids(&fut)?);
        }
        let mut a = Args::new();
        a.push_positional(self.0.global_id());
        a.push_keyword("pool", routed);
        let self_ref = crate::laila.forget(a)?.into_local()?;
        all_results.extend(Manifest::_collect_future_ids(&self_ref)?);
        Manifest::_group_over(all_results)
    }

    // ------------------------------------------------------------------
    // SQL index (non-memorizing algorithmic help)
    // ------------------------------------------------------------------

    #[cfg(not(feature = "sqlite"))]
    fn sql_not_available(what: &str) -> LailaError {
        LailaError::not_implemented(alloc::format!(
            "Manifest.{what} needs the sqlite index backend (the `sqlite` feature)"
        ))
    }

    /// `self._sql_state` (a `PrivateAttr`: never part of the wire form).
    #[cfg(feature = "sqlite")]
    fn sql_state_handle(&self) -> Option<sql_index::SqlStateHandle> {
        match self.0.__attrs__().get(SQL_STATE_ATTR) {
            Some(Arg::Object(o)) => o
                .as_any()
                .downcast_ref::<sql_index::SqlStateHandle>()
                .cloned(),
            _ => None,
        }
    }

    /// `m._sql_state` as a dynamic value: the `_SqlState` handle, or
    /// `None` before `build_index` / after `clear_index`.
    pub fn _sql_state(&self) -> Arg {
        #[cfg(feature = "sqlite")]
        {
            // The stored object itself: `m._sql_state is state` holds.
            match self.0.__attrs__().get(SQL_STATE_ATTR) {
                Some(v @ Arg::Object(_)) => v,
                _ => Arg::None,
            }
        }
        #[cfg(not(feature = "sqlite"))]
        {
            Arg::None
        }
    }

    #[cfg(feature = "sqlite")]
    fn set_sql_state(&self, state: Option<sql_index::SqlStateHandle>) {
        let _ = self.0.__attrs__().__setattr__(
            SQL_STATE_ATTR,
            match state {
                Some(h) => Arg::Object(alloc::sync::Arc::new(h)),
                None => Arg::None,
            },
        );
    }

    /// `with self.atomic():` -- the manifest's own (re-entrant) lock
    /// serializes build / rebuild / query / clear across threads.
    #[cfg(feature = "sqlite")]
    fn atomic(&self) -> sql_index::Held {
        sql_index::Held::acquire(
            &crate::atomic::definitions::locally_atomic_object::ensure_local_lock(
                self.0.__attrs__(),
            ),
        )
    }

    /// Filter the manifest with `SELECT … FROM … [WHERE …]`.
    ///
    /// A small, regex-based SQL surface over a *non-memorizing* sqlite
    /// index that this manifest owns directly (it bypasses
    /// `central.memory` entirely -- it is never memorized and never
    /// leaves the local machine). The index is built lazily on first use
    /// and reused until [`invalidate_index`](Manifest::invalidate_index) /
    /// [`clear_index`](Manifest::clear_index) is called or the blueprint
    /// mutates.
    ///
    /// Supported surface: `SELECT <items> FROM <name> [WHERE <predicate>]`
    /// only (`ORDER BY` / `LIMIT` / `GROUP BY` / `HAVING` / `JOIN` / `UNION`
    /// are rejected up front with a `ValueError`); `==` is normalized to
    /// `=`; the `FROM` name is an alias and ignored; string literals in
    /// `WHERE` must be single-quoted (a bareword is a column name to sqlite
    /// -- an unresolved one is re-raised as a `ValueError`).
    ///
    /// Returns a fresh, same-type manifest containing the matched rows;
    /// `sql()` never mutates `self` in place.
    #[cfg(feature = "sqlite")]
    pub fn sql(&self, query: impl IntoArg) -> Result<Manifest> {
        let (select_items, _from_alias, where_) = Manifest::_parse_sql_typed(&query.into_arg())?;

        let matched_keys = {
            let _held = self.atomic();
            match self.sql_state_handle() {
                None => self.build_index(Args::new())?,
                Some(state) if state.stale() => self._sql_rebuild_in_place(true)?,
                Some(_) => {}
            }
            let state = self
                .sql_state_handle()
                .expect("build_index leaves an index behind");
            let mut sql_text = alloc::format!(
                "SELECT \"{_SQL_ROW_IDX_COL}\" FROM \"{}\"",
                state.table_name()
            );
            if let Some(w) = &where_ {
                sql_text.push_str(" WHERE ");
                sql_text.push_str(w);
            }
            let matched_idx = match state.conn().query_i64s(&sql_text) {
                Ok(idx) => idx,
                Err(e) => {
                    let message = sql_index::message_of(&e);
                    if message.contains("no such column") {
                        return Err(LailaError::value_error(alloc::format!(
                            "{message}. If you meant a string literal, wrap it in single \
                             quotes (e.g. WHERE owner = 'alice'); unquoted barewords are \
                             treated as column names by SQL."
                        )));
                    }
                    return Err(sql_index::sqlite_error(e));
                }
            };
            let row_keys = state.row_keys();
            matched_idx
                .into_iter()
                .map(|i| row_keys[i as usize].clone())
                .collect::<Vec<Arg>>()
        };
        self._sql_project(&matched_keys, &select_items)
    }

    /// Filter the manifest with `SELECT … FROM … [WHERE …]` (needs the
    /// `sqlite` feature; the query is still parsed and validated).
    #[cfg(not(feature = "sqlite"))]
    pub fn sql(&self, query: impl IntoArg) -> Result<Manifest> {
        let (_select_items, _from_alias, _where) = Manifest::_parse_sql_typed(&query.into_arg())?;
        Err(Self::sql_not_available("sql"))
    }

    /// Materialize the manifest into a sqlite table + indexes.
    ///
    /// Idempotent: a subsequent call is a no-op while the cached index is
    /// fresh, and triggers an in-place rebuild if the index was
    /// invalidated. Subsequent [`sql`](Manifest::sql) calls reuse the same
    /// connection until [`invalidate_index`](Manifest::invalidate_index)
    /// is called or the blueprint mutates.
    ///
    /// `persist`, if provided, is the path to a file-backed sqlite db that
    /// survives process restarts (and is attached without a rebuild when
    /// it already holds a matching table); otherwise the index lives under
    /// `<laila_root>/indices/<manifest-uuid>/`.
    #[cfg(feature = "sqlite")]
    #[pysig("build_index(*, on=(), composite=(), persist=None, widen=True)")]
    pub fn build_index(&self, args: impl Into<Args>) -> Result<()> {
        let _held = self.atomic();
        if let Some(state) = self.sql_state_handle() {
            if state.stale() {
                self._sql_rebuild_in_place(widen.__bool__())?;
            }
            return Ok(());
        }
        self._sql_build_fresh(&on, &composite, &persist)
    }

    /// Materialize the manifest into a sqlite table + indexes (needs the
    /// `sqlite` feature).
    #[cfg(not(feature = "sqlite"))]
    #[pysig("build_index(*, on=(), composite=(), persist=None, widen=True)")]
    pub fn build_index(&self, args: impl Into<Args>) -> Result<()> {
        let _ = (on, composite, persist, widen);
        Err(Self::sql_not_available("build_index"))
    }

    /// Mark the cached index stale without closing the connection.
    ///
    /// Cheap: the next [`sql`](Manifest::sql) does an in-place rebuild
    /// (`DELETE FROM …` then re-INSERT, indexes preserved). Called
    /// automatically by [`extend`](Manifest::extend) / `+=`; subclasses
    /// that mutate their own blueprint should call this after writing.
    pub fn invalidate_index(&self) {
        #[cfg(feature = "sqlite")]
        if let Some(state) = self.sql_state_handle() {
            state.set_stale(true);
        }
    }

    /// Close the sqlite connection and drop the cached index.
    ///
    /// A non-persistent (temporary) index file is disposable and is always
    /// unlinked here. A user-chosen `persist=` file is left in place unless
    /// `remove_persisted` is true. After `clear_index()` the next
    /// [`sql`](Manifest::sql) rebuilds from scratch.
    #[cfg(feature = "sqlite")]
    #[pysig("clear_index(*, remove_persisted=False)")]
    pub fn clear_index(&self, args: impl Into<Args>) -> Result<()> {
        let _held = self.atomic();
        // The finalizer (`weakref.finalize`) is the handle's `Drop`: detached
        // by taking the state out of the manifest before closing by hand.
        let Some(state) = self.sql_state_handle() else {
            return Ok(());
        };
        self.set_sql_state(None);
        state.detach();
        let _ = state.conn().close();
        if !state.is_persistent() || remove_persisted.__bool__() {
            Manifest::_sql_unlink_db(state.db_path());
        }
        Ok(())
    }

    /// Close the sqlite connection and drop the cached index. Without an
    /// index this is a no-op.
    #[cfg(not(feature = "sqlite"))]
    #[pysig("clear_index(*, remove_persisted=False)")]
    pub fn clear_index(&self, args: impl Into<Args>) -> Result<()> {
        let _ = remove_persisted;
        Ok(())
    }

    /// Return `(rows, columns)` for the SQL index: one row per top-level
    /// key, the value as the row dict if it is a dict, otherwise
    /// `{"value": <scalar>}`; `columns` is the union of the row keys.
    pub fn _sql_rows(&self) -> Result<(SqlRows, Vec<Arg>)> {
        let blueprint = self.data()?;
        let blueprint = if blueprint.is_none() {
            PyDictBuilder::new().finish()
        } else {
            blueprint
        };
        let mut rows: Vec<(Arg, Arg)> = Vec::new();
        let mut columns: Vec<Arg> = Vec::new();
        for key in blueprint.__iter__()? {
            let value = blueprint.__getitem__(key.clone())?;
            let row = if let Arg::Dict(_) = &value {
                value.copy()?
            } else {
                let mut d = PyDictBuilder::new();
                d.insert("value", value);
                d.finish()
            };
            for col in row.__iter__()? {
                if !columns.contains(&col) {
                    columns.push(col);
                }
            }
            rows.push((key, row));
        }
        Ok((rows, columns))
    }

    /// Build a new same-type manifest from matched row keys (keeps the
    /// top-level entries whose key appears in `matched_keys`).
    pub fn _sql_project(&self, matched_keys: &[Arg], _select_items: &[String]) -> Result<Manifest> {
        let blueprint = self.data()?;
        let blueprint = if blueprint.is_none() {
            PyDictBuilder::new().finish()
        } else {
            blueprint
        };
        let mut subset = PyDictBuilder::new();
        for k in matched_keys {
            if blueprint.__contains__(k.clone())? {
                subset.insert(k.clone(), blueprint.__getitem__(k.clone())?.deepcopy());
            }
        }
        let mut a = Args::new();
        a.push_keyword("blueprint", subset.finish());
        Manifest::new(a)
    }

    /// Parse a tiny `SELECT … FROM … [WHERE …]` query.
    ///
    /// Strips a trailing `;`, normalizes `==` to `=`, rejects forbidden
    /// clauses, and returns `(select_items, from_alias, where_clause_or_none)`.
    pub fn _parse_sql(query: impl IntoArg) -> Result<Arg> {
        let (items, alias, where_) = Self::_parse_sql_typed(&query.into_arg())?;
        Ok((
            items
                .into_iter()
                .map(|s| Arg::Str(s.into()))
                .collect::<Vec<_>>(),
            Arg::Str(alias.into()),
            where_.map(|w| Arg::Str(w.into())).unwrap_or(Arg::None),
        )
            .into_arg())
    }

    /// [`_parse_sql`](Manifest::_parse_sql), typed:
    /// `(select_items, from_alias, where_clause_or_none)`.
    pub fn _parse_sql_typed(query: &Arg) -> Result<(Vec<String>, String, Option<String>)> {
        let Arg::Str(query) = query else {
            return Err(LailaError::value_error("sql() query must be a string."));
        };
        let normalized = query.trim().trim_end_matches(';').trim().replace("==", "=");
        let upper = normalized.to_uppercase();
        for clause in _FORBIDDEN_SQL {
            if contains_clause(&upper, clause) {
                return Err(LailaError::value_error(alloc::format!(
                    "Unsupported SQL clause: {clause}. Only \
                     'SELECT <items> FROM <name> [WHERE <predicate>]' is supported."
                )));
            }
        }
        let only = || {
            LailaError::value_error(
                "Only 'SELECT <items> FROM <name> [WHERE <predicate>]' is supported.",
            )
        };
        // ^\s*SELECT\s+(?P<items>.+?)\s+FROM\s+(?P<from>[`"']?ident[`"']?)\s*(?:WHERE\s+(?P<where>.+))?$
        let s = normalized.trim_start();
        let rest = strip_keyword(s, "SELECT").ok_or_else(only)?;
        if !rest.starts_with(char::is_whitespace) {
            return Err(only());
        }
        let rest = rest.trim_start();
        // ``.+?\s+FROM\s+``: the first FROM (as a whole word) after at least one item char.
        let mut found: Option<(usize, usize)> = None;
        let bytes = rest.as_bytes();
        let mut i = 1;
        while i + 4 <= bytes.len() {
            if rest.is_char_boundary(i)
                && rest[i..].len() >= 4
                && rest[i..i + 4].eq_ignore_ascii_case("FROM")
                && rest[..i].ends_with(char::is_whitespace)
                && rest[i + 4..].starts_with(char::is_whitespace)
            {
                found = Some((i, i + 4));
                break;
            }
            i += 1;
        }
        let (from_start, from_end) = found.ok_or_else(only)?;
        let items_text = rest[..from_start].trim_end();
        if items_text.is_empty() {
            return Err(only());
        }
        let after = rest[from_end..].trim_start();
        let quote = after.chars().next().filter(|c| "`\"'".contains(*c));
        let ident_src = match quote {
            Some(_) => &after[1..],
            None => after,
        };
        let ident_len = ident_src
            .char_indices()
            .take_while(|(k, c)| {
                if *k == 0 {
                    c.is_ascii_alphabetic() || *c == '_'
                } else {
                    c.is_ascii_alphanumeric() || *c == '_'
                }
            })
            .count();
        if ident_len == 0 {
            return Err(only());
        }
        let ident = &ident_src[..ident_len];
        let mut tail = &ident_src[ident_len..];
        if quote.is_some() {
            if let Some(t) = tail.strip_prefix(|c: char| "`\"'".contains(c)) {
                tail = t;
            }
        }
        let tail = tail.trim_start();
        let where_clause = if tail.is_empty() {
            None
        } else {
            let w = strip_keyword(tail, "WHERE").ok_or_else(only)?;
            if !w.starts_with(char::is_whitespace) || w.trim().is_empty() {
                return Err(only());
            }
            Some(w.trim().to_string())
        };
        let select_items = items_text
            .split(',')
            .map(|item| Manifest::_strip_table_prefix(item.trim()))
            .collect();
        Ok((select_items, ident.to_string(), where_clause))
    }

    /// Drop an `alias.` prefix and surrounding quotes from a SELECT item.
    pub fn _strip_table_prefix(item: &str) -> String {
        let stripped = item.trim().trim_matches(|c| c == '`' || c == '"');
        if stripped == "*" {
            return stripped.to_string();
        }
        let stripped = match stripped.split_once('.') {
            Some((_, rest)) => rest,
            None => stripped,
        };
        stripped.trim_matches(|c| c == '`' || c == '"').to_string()
    }

    /// Reject non-primitive row values before any sqlite state exists.
    pub fn _validate_primitive_rows(rows: &[(Arg, Arg)]) -> Result<()> {
        for (row_key, row_dict) in rows {
            for col in row_dict.__iter__()? {
                let val = row_dict.__getitem__(col.clone())?;
                let is_identifiable = matches!(val, Arg::Entry(_) | Arg::Pool(_) | Arg::Future(_))
                    || crate::_compat::pydantic::isinstance_model(
                        &val,
                        "_LAILA_IDENTIFIABLE_OBJECT",
                    );
                if is_identifiable {
                    return Err(LailaError::type_error(alloc::format!(
                        "Cannot index column {}: row {} has value of type {} (Laila \
                         identifiable object). Index columns must be primitive scalars \
                         (str, int, float, bool, bytes, None). Pass `.global_id` instead.",
                        col.__repr__(),
                        row_key.__repr__(),
                        val.type_name()
                    )));
                }
                if !_PRIMITIVE_TYPES.contains(&val.type_name()) {
                    return Err(LailaError::type_error(alloc::format!(
                        "Cannot index column {}: row {} has value of type {}. Index columns \
                         must be primitive scalars. Flatten lists/dicts in your _sql_rows() \
                         override (e.g. ','.join(tags) or json.dumps(meta)).",
                        col.__repr__(),
                        row_key.__repr__(),
                        val.type_name()
                    )));
                }
            }
        }
        Ok(())
    }

    /// `_validate_ident(name)`: `ValueError` unless *name* is a SQL identifier.
    pub fn _validate_ident(name: impl IntoArg) -> Result<()> {
        let name = name.into_arg();
        match &name {
            Arg::Str(s) if is_sql_ident(s) => Ok(()),
            other => Err(LailaError::value_error(alloc::format!(
                "Invalid column identifier for indexing: {}",
                other.__repr__()
            ))),
        }
    }

    // ----- SQL index internals ---------------------------------------

    /// Default on-disk path for this manifest's index file.
    ///
    /// One file per *instance* (`<pid>-<random>.laila_sqlitedb`) inside the
    /// per-manifest `indices/<uuid>/` directory: two live manifests that
    /// share a uuid (same nickname, or the same gid loaded twice) must
    /// never attach to each other's temporary table.
    #[cfg(feature = "sqlite")]
    pub fn _index_db_path(&self) -> Result<String> {
        let indices = crate::macros::defaults::default_directories()
            .into_iter()
            .find(|(k, _)| *k == "indices")
            .map(|(_, p)| p)
            .unwrap_or_default();
        let base = std::path::Path::new(&indices).join(self.0.uuid());
        std::fs::create_dir_all(&base)
            .map_err(|e| LailaError::new(ExcKind::OSError, alloc::format!("{e}")))?;
        let name = alloc::format!(
            "{}-{}.laila_sqlitedb",
            std::process::id(),
            &crate::_compat::uuid::uuid4().replace('-', "")[..8]
        );
        Ok(base.join(name).to_string_lossy().into_owned())
    }

    /// Build the index from scratch (or attach a matching on-disk file).
    #[cfg(feature = "sqlite")]
    fn _sql_build_fresh(&self, on: &Arg, composite: &Arg, persist: &Arg) -> Result<()> {
        use sql_index::{LockedConnection, SqlState, SqlStateHandle};

        let (rows, columns) = self._sql_rows()?;
        Manifest::_validate_primitive_rows(&rows)?;
        let columns = sql_index::sorted_columns(&columns);
        let row_keys: Vec<Arg> = rows.iter().map(|(k, _)| k.clone()).collect();

        let is_persistent = !persist.is_none();
        let db_path = if !persist.is_none() {
            let db_path = crate::python::stdlib::os
                .path()
                .expanduser(persist.clone())?;
            if let Some(parent) = std::path::Path::new(&db_path).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| LailaError::new(ExcKind::OSError, alloc::format!("{e}")))?;
                }
            }
            db_path
        } else {
            self._index_db_path()?
        };

        let conn = LockedConnection::connect(&db_path)?;
        let fingerprint = Manifest::_sql_fingerprint(&columns, &rows);

        if Manifest::_index_table_exists(&conn)? && Manifest::_index_row_count(&conn)? == rows.len()
        {
            let existing_columns = Manifest::_existing_columns(&conn)?;
            let same_columns = existing_columns.len() == columns.len()
                && columns.iter().all(|c| existing_columns.contains(c));
            if same_columns
                && Manifest::_read_fingerprint(&conn)?.as_deref() == Some(fingerprint.as_str())
            {
                let indexed = Manifest::existing_indexes_on(&conn)?;
                self.set_sql_state(Some(SqlStateHandle::new(SqlState {
                    conn,
                    db_path,
                    is_persistent,
                    table_name: _SQL_INDEX_TABLE.to_string(),
                    columns: existing_columns,
                    indexed,
                    row_keys,
                    stale: false,
                })));
                return Ok(());
            }
        }

        Manifest::_create_and_populate(&conn, &columns, &rows)?;
        Manifest::_write_fingerprint(&conn, &fingerprint)?;
        let indexed = Manifest::_apply_indexes(&conn, on, composite)?;
        self.set_sql_state(Some(SqlStateHandle::new(SqlState {
            conn,
            db_path,
            is_persistent,
            table_name: _SQL_INDEX_TABLE.to_string(),
            columns,
            indexed,
            row_keys,
            stale: false,
        })));
        Ok(())
    }

    /// Re-materialize rows into the existing table, preserving indexes.
    #[cfg(feature = "sqlite")]
    fn _sql_rebuild_in_place(&self, widen: bool) -> Result<()> {
        let Some(state) = self.sql_state_handle() else {
            return Ok(());
        };
        let (rows, columns) = self._sql_rows()?;
        Manifest::_validate_primitive_rows(&rows)?;
        let columns = sql_index::sorted_columns(&columns);
        let conn = state.conn();
        let table_name = state.table_name();
        let old_columns = state.columns();

        let new_columns: Vec<String> = columns
            .iter()
            .filter(|c| !old_columns.contains(c))
            .cloned()
            .collect();
        if !new_columns.is_empty() {
            if !widen {
                return Err(LailaError::value_error(alloc::format!(
                    "Index rebuild needs new columns {} but widen=False.",
                    strs_to_list(&new_columns).__repr__()
                )));
            }
            for col in &new_columns {
                conn.execute(
                    &alloc::format!("ALTER TABLE \"{table_name}\" ADD COLUMN \"{col}\""),
                    &[],
                )?;
            }
        }

        let mut all_columns = old_columns;
        all_columns.extend(new_columns);
        conn.execute(&alloc::format!("DELETE FROM \"{table_name}\""), &[])?;
        Manifest::_insert_rows(&conn, &table_name, &all_columns, &rows)?;
        Manifest::_write_fingerprint(&conn, &Manifest::_sql_fingerprint(&columns, &rows))?;
        conn.commit()?;

        state.set_columns(all_columns);
        state.set_row_keys(rows.iter().map(|(k, _)| k.clone()).collect());
        state.set_stale(false);
        Ok(())
    }

    /// Content hash of what the index table holds (columns + rows).
    ///
    /// Rows are validated to be primitive scalars before this runs, so
    /// `repr` is a stable, order-preserving encoding:
    /// `sha256(repr((list(columns), [(row_key, row) for row_key, row in rows])))`.
    #[cfg(feature = "sqlite")]
    pub fn _sql_fingerprint(columns: &[String], rows: &[(Arg, Arg)]) -> String {
        use sha2::{Digest, Sha256};
        let rows_arg: Vec<Arg> = rows
            .iter()
            .map(|(k, r)| Arg::Tuple(alloc::vec![k.clone(), r.clone()].into()))
            .collect();
        let payload =
            Arg::Tuple(alloc::vec![strs_to_list(columns), rows_arg.into_arg()].into()).__repr__();
        let digest = Sha256::digest(payload.as_bytes());
        let mut out = String::with_capacity(64);
        for b in digest {
            out.push_str(&alloc::format!("{b:02x}"));
        }
        out
    }

    #[cfg(feature = "sqlite")]
    fn _write_fingerprint(conn: &sql_index::LockedConnection, fingerprint: &str) -> Result<()> {
        conn.execute(
            &alloc::format!("CREATE TABLE IF NOT EXISTS \"{_SQL_META_TABLE}\" (key TEXT PRIMARY KEY, value TEXT)"),
            &[],
        )?;
        conn.execute(
            &alloc::format!(
                "INSERT OR REPLACE INTO \"{_SQL_META_TABLE}\" (key, value) VALUES (?, ?)"
            ),
            &[sql_index::text("fingerprint"), sql_index::text(fingerprint)],
        )?;
        conn.commit()
    }

    #[cfg(feature = "sqlite")]
    fn _read_fingerprint(conn: &sql_index::LockedConnection) -> Result<Option<String>> {
        let row = conn.query_strings(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            &[sql_index::text(_SQL_META_TABLE)],
        )?;
        if row.is_empty() {
            return Ok(None);
        }
        let row = conn.query_strings(
            &alloc::format!("SELECT value FROM \"{_SQL_META_TABLE}\" WHERE key = ?"),
            &[sql_index::text("fingerprint")],
        )?;
        Ok(row.into_iter().next())
    }

    #[cfg(feature = "sqlite")]
    fn _index_table_exists(conn: &sql_index::LockedConnection) -> Result<bool> {
        let row = conn.query_strings(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            &[sql_index::text(_SQL_INDEX_TABLE)],
        )?;
        Ok(!row.is_empty())
    }

    #[cfg(feature = "sqlite")]
    fn _index_row_count(conn: &sql_index::LockedConnection) -> Result<usize> {
        let n = conn
            .query_i64s(&alloc::format!(
                "SELECT COUNT(*) FROM \"{_SQL_INDEX_TABLE}\""
            ))
            .map_err(sql_index::sqlite_error)?;
        Ok(n.first().copied().unwrap_or(0) as usize)
    }

    #[cfg(feature = "sqlite")]
    fn _existing_columns(conn: &sql_index::LockedConnection) -> Result<Vec<String>> {
        let rows = conn.query_strings_at(
            &alloc::format!("PRAGMA table_info(\"{_SQL_INDEX_TABLE}\")"),
            1,
        )?;
        Ok(rows.into_iter().filter(|c| c != _SQL_ROW_IDX_COL).collect())
    }

    /// `Manifest._existing_indexes(conn)`: the index names on the index
    /// table, for a dynamic connection (`m._sql_state.conn`).
    pub fn _existing_indexes(conn: impl IntoArg) -> Result<Arg> {
        let conn = conn.into_arg();
        #[cfg(not(feature = "sqlite"))]
        let _ = &conn;
        #[cfg(feature = "sqlite")]
        {
            let c = match &conn {
                Arg::Object(o) => o
                    .as_any()
                    .downcast_ref::<sql_index::SharedConnection>()
                    .cloned(),
                _ => None,
            }
            .ok_or_else(|| {
                LailaError::type_error(alloc::format!(
                    "_existing_indexes() expects a _LockedConnection, got {}",
                    conn.type_name()
                ))
            })?;
            Ok(list_arg(Manifest::existing_indexes_on(&c.0)?))
        }
        #[cfg(not(feature = "sqlite"))]
        {
            Err(Self::sql_not_available("_existing_indexes"))
        }
    }

    #[cfg(feature = "sqlite")]
    fn existing_indexes_on(conn: &sql_index::LockedConnection) -> Result<Vec<Arg>> {
        let rows = conn.query_strings_at(
            &alloc::format!("PRAGMA index_list(\"{_SQL_INDEX_TABLE}\")"),
            1,
        )?;
        Ok(rows.into_iter().map(|s| Arg::Str(s.into())).collect())
    }

    #[cfg(feature = "sqlite")]
    fn _create_and_populate(
        conn: &sql_index::LockedConnection,
        columns: &[String],
        rows: &[(Arg, Arg)],
    ) -> Result<()> {
        for col in columns {
            Manifest::_validate_ident(Arg::Str(col.as_str().into()))?;
        }
        let col_defs: String = columns
            .iter()
            .map(|c| alloc::format!(", \"{c}\""))
            .collect();
        conn.execute(
            &alloc::format!("DROP TABLE IF EXISTS \"{_SQL_INDEX_TABLE}\""),
            &[],
        )?;
        conn.execute(
            &alloc::format!(
                "CREATE TABLE \"{_SQL_INDEX_TABLE}\" (\"{_SQL_ROW_IDX_COL}\" INTEGER PRIMARY KEY{col_defs})"
            ),
            &[],
        )?;
        Manifest::_insert_rows(conn, _SQL_INDEX_TABLE, columns, rows)?;
        conn.commit()
    }

    #[cfg(feature = "sqlite")]
    fn _insert_rows(
        conn: &sql_index::LockedConnection,
        table: &str,
        columns: &[String],
        rows: &[(Arg, Arg)],
    ) -> Result<()> {
        let (statement, data): (String, Vec<Vec<sql_index::Value>>) = if !columns.is_empty() {
            let col_names: Vec<String> =
                columns.iter().map(|c| alloc::format!("\"{c}\"")).collect();
            let placeholders: Vec<&str> = core::iter::repeat_n("?", columns.len() + 1).collect();
            let statement = alloc::format!(
                "INSERT INTO \"{table}\" (\"{_SQL_ROW_IDX_COL}\", {}) VALUES ({})",
                col_names.join(", "),
                placeholders.join(", ")
            );
            let mut data = Vec::with_capacity(rows.len());
            for (idx, (_row_key, row)) in rows.iter().enumerate() {
                let mut values = alloc::vec![sql_index::Value::Integer(idx as i64)];
                for c in columns {
                    let key = Arg::Str(c.as_str().into());
                    let v = if row.__contains__(key.clone())? {
                        row.__getitem__(key)?
                    } else {
                        Arg::None
                    };
                    values.push(sql_index::value_of(&v));
                }
                data.push(values);
            }
            (statement, data)
        } else {
            (
                alloc::format!("INSERT INTO \"{table}\" (\"{_SQL_ROW_IDX_COL}\") VALUES (?)"),
                (0..rows.len())
                    .map(|idx| alloc::vec![sql_index::Value::Integer(idx as i64)])
                    .collect(),
            )
        };
        if !data.is_empty() {
            conn.executemany(&statement, &data)?;
        }
        Ok(())
    }

    #[cfg(feature = "sqlite")]
    fn _apply_indexes(
        conn: &sql_index::LockedConnection,
        on: &Arg,
        composite: &Arg,
    ) -> Result<Vec<Arg>> {
        let mut indexed: Vec<Arg> = Vec::new();
        for col in on.__iter__()? {
            Manifest::_validate_ident(&col)?;
            let col = col.__str__();
            conn.execute(
                &alloc::format!(
                    "CREATE INDEX IF NOT EXISTS \"idx_{_SQL_INDEX_TABLE}_{col}\" ON \"{_SQL_INDEX_TABLE}\" (\"{col}\")"
                ),
                &[],
            )?;
            let col = Arg::Str(col.into());
            if !indexed.contains(&col) {
                indexed.push(col);
            }
        }
        for tup in composite.__iter__()? {
            let cols = tup.__iter__()?;
            for col in &cols {
                Manifest::_validate_ident(col)?;
            }
            let names: Vec<String> = cols.iter().map(|c| c.__str__()).collect();
            let name = alloc::format!("idx_{_SQL_INDEX_TABLE}_{}", names.join("_"));
            let cols_sql: Vec<String> = names.iter().map(|c| alloc::format!("\"{c}\"")).collect();
            conn.execute(
                &alloc::format!(
                    "CREATE INDEX IF NOT EXISTS \"{name}\" ON \"{_SQL_INDEX_TABLE}\" ({})",
                    cols_sql.join(", ")
                ),
                &[],
            )?;
            let tup = Arg::Tuple(cols.into());
            if !indexed.contains(&tup) {
                indexed.push(tup);
            }
        }
        conn.commit()?;
        Ok(indexed)
    }

    /// Remove an index db file and its (now-empty) per-manifest dir.
    #[cfg(feature = "sqlite")]
    pub fn _sql_unlink_db(db_path: impl IntoArg) {
        sql_index::unlink_db(&db_path.into_arg().__str__())
    }

    /// `self._sql_finalizer` -- Python's `weakref.finalize` handle on the
    /// temporary index file; the port's finalizer is the state handle's
    /// `Drop`, so the attribute is always `None`.
    pub fn _sql_finalizer(&self) -> Arg {
        #[cfg(feature = "sqlite")]
        {
            match self.0.__attrs__().get(SQL_STATE_ATTR) {
                Some(Arg::Object(o)) => {
                    match o.as_any().downcast_ref::<sql_index::SqlStateHandle>() {
                        Some(h) if !h.is_persistent() => {
                            Arg::Object(alloc::sync::Arc::new(sql_index::SqlFinalizer(h.clone())))
                        }
                        _ => Arg::None,
                    }
                }
                _ => Arg::None,
            }
        }
        #[cfg(not(feature = "sqlite"))]
        {
            Arg::None
        }
    }

    // ------------------------------------------------------------------
    // Mapping-like API (top-level blueprint keys for dict(manifest))
    // ------------------------------------------------------------------

    /// Top-level blueprint keys (`keys()` takes no arguments; the bag is
    /// accepted so `m.keys()` lowers like a pool's `keys(..)`).
    pub fn keys(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        if !a.positional.is_empty() || !a.keywords.is_empty() {
            return Err(LailaError::type_error(alloc::format!(
                "Manifest.keys() takes no arguments ({} given)",
                a.positional.len() + a.keywords.len()
            )));
        }
        let bp = self.data()?;
        if bp.is_none() {
            return Ok(list_arg(Vec::new()));
        }
        bp.keys(Args::new())
    }

    /// Top-level blueprint values.
    pub fn values(&self) -> Result<Arg> {
        let bp = self.data()?;
        if bp.is_none() {
            return Ok(list_arg(Vec::new()));
        }
        bp.values()
    }

    /// Top-level blueprint items.
    pub fn items(&self) -> Result<Arg> {
        let bp = self.data()?;
        if bp.is_none() {
            return Ok(list_arg(Vec::new()));
        }
        bp.items()
    }

    /// Return a new `Manifest` containing only the specified top-level keys.
    pub fn sub_manifest(&self, keys: impl IntoArg) -> Result<Manifest> {
        let keys = keys.into_arg().__iter__()?;
        let bp = self.data()?;
        if bp.is_none() {
            return Err(LailaError::runtime_error(
                "Cannot create sub-manifest \u{2014} manifest is empty.",
            ));
        }
        let mut missing: Vec<Arg> = Vec::new();
        for k in &keys {
            if !bp.__contains__(k.clone())? {
                missing.push(k.clone());
            }
        }
        if !missing.is_empty() {
            return Err(LailaError::key_error(str_repr(&alloc::format!(
                "Keys not found in manifest: {}",
                list_arg(missing).__repr__()
            ))));
        }
        let mut subset = PyDictBuilder::new();
        for k in keys {
            subset.insert(k.clone(), bp.__getitem__(k)?.deepcopy());
        }
        let mut a = Args::new();
        a.push_keyword("blueprint", subset.finish());
        Manifest::new(a)
    }

    /// Merge another manifest's blueprint into this one in-place.
    #[pysig("extend(other, *, overwrite=False)")]
    pub fn extend(&self, args: impl Into<Args>) -> Result<()> {
        let Some(other) = Manifest::from_arg(&other) else {
            return Err(LailaError::type_error(alloc::format!(
                "extend() requires a Manifest, got {}",
                other.type_name()
            )));
        };
        let other_bp = other.data()?;
        if other_bp.is_none() {
            return Ok(());
        }

        let bp = self.data()?;
        let new_bp = if bp.is_none() {
            other_bp.deepcopy()
        } else {
            if !overwrite.__bool__() {
                let overlap = Manifest::overlap(&bp, &other_bp)?;
                if !overlap.is_empty() {
                    return Err(LailaError::key_error(str_repr(&alloc::format!(
                        "Duplicate top-level keys: {}",
                        crate::python::builtins::sorted(list_arg(overlap))?.__repr__()
                    ))));
                }
            }
            let new_bp = bp.copy()?;
            new_bp.update(Args::from(other_bp.deepcopy()))?;
            new_bp
        };

        // ``self._payload = ComputationalData(new_bp)``; same bookkeeping as
        // the ``data`` setter: the blueprint changed in this process, so a
        // later memorize of a variable manifest bumps.
        self.0.__setattr__(
            "_payload",
            crate::entry::ComputationalData::new(new_bp)?.into_arg(),
        )?;
        self.0.__setattr__("_locally_modified", true)?;

        if let Some(other_pending) = other.pending_vec() {
            if !other_pending.is_empty() {
                match self.pending_vec() {
                    None => self.set_pending_entries(Some(other_pending)),
                    Some(mut mine) => {
                        mine.extend(other_pending);
                        self.set_pending_entries(Some(mine));
                    }
                }
            }
        }

        // The blueprint changed: mark any cached SQL index stale so the
        // next sql() rebuilds in place (non-memorizing algorithmic help).
        self.invalidate_index();
        Ok(())
    }

    fn overlap(bp: &Arg, other_bp: &Arg) -> Result<Vec<Arg>> {
        let mut out = Vec::new();
        for k in bp.__iter__()? {
            if other_bp.__contains__(k.clone())? {
                out.push(k);
            }
        }
        Ok(out)
    }

    /// `manifest += other` -- merge *other* in-place and return self.
    pub fn __iadd__(&self, other: impl IntoArg) -> Result<Arg> {
        let other = other.into_arg();
        if Manifest::from_arg(&other).is_none() {
            // ``return NotImplemented`` (the `+=` operator raises `TypeError`)
            return Ok(crate::python::builtins::not_implemented());
        }
        self.extend(Args::from(other))?;
        Ok(self.clone().into_arg())
    }

    /// `manifest += other` as the operator: `__iadd__`, with the
    /// `NotImplemented` answer raised as Python's `TypeError`.
    pub fn iadd_op(&self, other: impl IntoArg) -> Result<Manifest> {
        let other = other.into_arg();
        let out = self.__iadd__(other.clone())?;
        match Manifest::from_arg(&out) {
            Some(m) => Ok(m),
            None => Err(LailaError::type_error(alloc::format!(
                "unsupported operand type(s) for +=: 'Manifest' and '{}'",
                other.type_name()
            ))),
        }
    }

    /// `manifest + other` -- a new manifest with the merged blueprints;
    /// `NotImplemented` (the singleton) when *other* is not a manifest, so
    /// `m.__add__(x) is NotImplemented` reads as in Python (the `+`
    /// operator turns it into Python's `TypeError`).
    pub fn __add__(&self, other: impl IntoArg) -> Result<Arg> {
        let other_arg = other.into_arg();
        let Some(other) = Manifest::from_arg(&other_arg) else {
            return Ok(crate::python::builtins::not_implemented());
        };

        let bp = self.data()?;
        let other_bp = other.data()?;
        if !bp.is_none() && !other_bp.is_none() {
            let overlap = Manifest::overlap(&bp, &other_bp)?;
            if !overlap.is_empty() {
                return Err(LailaError::key_error(str_repr(&alloc::format!(
                    "Duplicate top-level keys: {}",
                    crate::python::builtins::sorted(list_arg(overlap))?.__repr__()
                ))));
            }
        }

        let merged = PyDictBuilder::new().finish();
        if !bp.is_none() {
            merged.update(Args::from(bp.deepcopy()))?;
        }
        if !other_bp.is_none() {
            merged.update(Args::from(other_bp.deepcopy()))?;
        }

        if !merged.__bool__() {
            return Manifest::new(Args::new()).map(IntoArg::into_arg);
        }

        let mut a = Args::new();
        a.push_keyword("blueprint", merged);
        let result = Manifest::new(a)?;

        let mut pending: Vec<Arg> = Vec::new();
        if let Some(p) = self.pending_vec() {
            pending.extend(p);
        }
        if let Some(p) = other.pending_vec() {
            pending.extend(p);
        }
        if !pending.is_empty() {
            result.set_pending_entries(Some(pending));
        }
        Ok(result.into_arg())
    }

    /// Look up a top-level key in the blueprint.
    pub fn __getitem__(&self, key: impl IntoArg) -> Result<Arg> {
        let key = key.into_arg();
        let bp = self.data()?;
        if bp.is_none() {
            return Err(LailaError::key_error(key.__repr__()));
        }
        bp.__getitem__(key)
    }

    /// Number of top-level keys in the blueprint.
    pub fn __len__(&self) -> Result<usize> {
        let bp = self.data()?;
        if bp.is_none() {
            return Ok(0);
        }
        bp.__len__()
    }

    // ------------------------------------------------------------------
    // Iteration / containment (flattened global_id leaves)
    // ------------------------------------------------------------------

    /// Every `global_id` string via a depth-first, insertion-order walk.
    pub fn __iter__(&self) -> Result<Vec<Arg>> {
        let bp = self.data()?;
        if bp.is_none() {
            return Ok(Vec::new());
        }
        Manifest::_iter_global_ids(&bp)
    }

    /// `True` if *global_id* appears anywhere in the blueprint leaves.
    pub fn __contains__(&self, global_id: impl IntoArg) -> bool {
        let global_id = global_id.into_arg();
        if !matches!(global_id, Arg::Str(_)) {
            return false;
        }
        self.__iter__()
            .map(|gids| gids.contains(&global_id))
            .unwrap_or(false)
    }

    /// Leaves that resolve at *read* time rather than naming one stored
    /// record: a negative `evolution` (`@evolution=-1` = "whatever is
    /// latest") or a `creation_timestamp` search argument. Malformed
    /// leaves are ignored.
    pub fn floating_leaves(&self) -> Result<Arg> {
        let mut floating: Vec<Arg> = Vec::new();
        for gid in self.__iter__()? {
            let Ok((_, attrs)) = split_global_id_attributes(&gid.__str__()) else {
                continue;
            };
            let evolution = attrs
                .iter()
                .find(|(k, _)| k == EVOLUTION_ATTRIBUTE)
                .map(|(_, v)| v.as_str())
                .unwrap_or("");
            if evolution.starts_with('-')
                || attrs.iter().any(|(k, _)| k == CREATION_TIMESTAMP_ATTRIBUTE)
            {
                floating.push(gid);
            }
        }
        Ok(list_arg(floating))
    }

    // ------------------------------------------------------------------
    // String representation
    // ------------------------------------------------------------------

    /// `str(manifest)`: the global id.
    pub fn __str__(&self) -> String {
        self.0.global_id()
    }

    /// `repr(manifest)`: `Manifest(<global_id>, entries=<n>)`.
    pub fn __repr__(&self) -> String {
        let n = self.__iter__().map(|g| g.len()).unwrap_or(0);
        alloc::format!("Manifest({}, entries={n})", self.0.global_id())
    }

    // ------------------------------------------------------------------
    // Serialize and Recovery (inherited class methods, `cls = Manifest`)
    // ------------------------------------------------------------------

    /// `Manifest.from_dict(in_dict)`
    pub fn from_dict(in_dict: impl IntoArg) -> Result<Manifest> {
        Ok(Manifest(Entry::from_dict_as(EntryKind::Manifest, in_dict)?))
    }

    /// `Manifest._build_from_dict_sync(in_dict)`
    pub fn _build_from_dict_sync(in_dict: impl IntoArg) -> Result<Manifest> {
        let e = Entry::_build_from_dict_sync_as(EntryKind::Manifest, in_dict)?;
        Manifest::from_entry(&e).ok_or_else(|| {
            LailaError::type_error(alloc::format!(
                "expected a Manifest, got {}",
                e.__class_name__()
            ))
        })
    }

    /// `Manifest._build_from_dict_async(in_dict)`
    pub async fn _build_from_dict_async(in_dict: impl IntoArg) -> Result<Manifest> {
        Manifest::_build_from_dict_sync(in_dict)
    }

    // ------------------------------------------------------------------
    // Static / class helpers
    // ------------------------------------------------------------------

    /// Extract future IDs from a GroupFuture or a single future identity.
    /// // MERGE: the `Future` has no bank yet (Phase 4): the settled result
    /// stands in for the id (`[ref.global_id]`).
    pub fn _collect_future_ids(fut: &Future) -> Result<Vec<Arg>> {
        // The children are re-parented into the manifest-level `GroupFuture`
        // returned to the caller, so an intermediate per-batch group shell
        // is released here (children untouched); the caller releases the
        // returned group, which releases the children.
        if fut.kind() == FutureKind::Group {
            let ids = fut.future_ids().__iter__()?;
            let mut a = Args::new();
            a.push_keyword("children", false);
            fut.release(a)?;
            return Ok(ids);
        }
        Ok(alloc::vec![Arg::Str(fut.global_id().into())])
    }

    /// The manifest-level `GroupFuture` over *future_ids* on the internal
    /// taskforce (what `memorize` / `remember` / `forget` hand back).
    fn _group_over(future_ids: Vec<Arg>) -> Result<Future> {
        let policy = crate::policy::get_active_policy();
        let mut a = Args::new();
        a.push_keyword(
            "taskforce_id",
            policy.central().command().internal_taskforce(),
        );
        a.push_keyword("policy_id", policy.global_id());
        a.push_keyword("future_ids", future_ids);
        Future::construct(FutureKind::Group, a)
    }

    /// Collapse `pool` / `pool_id` / `pool_nickname` into one routing value:
    /// an explicit `pool` wins, then `pool_id`, then `pool_nickname`.
    /// `None` means the alpha (default) pool.
    pub fn _resolve_pool(pool: Arg, pool_nickname: Arg, pool_id: Arg) -> Arg {
        if !pool.is_none() {
            return pool;
        }
        if !pool_id.is_none() {
            return pool_id;
        }
        pool_nickname
    }

    /// Return `(has_entries, has_strings)` for leaf values in *data*.
    pub fn _classify_data(data: &Arg) -> Result<(bool, bool)> {
        fn check(val: &Arg, has_entries: &mut bool, has_strings: &mut bool) -> Result<()> {
            match val {
                Arg::Entry(_) => *has_entries = true,
                Arg::Str(_) => *has_strings = true,
                Arg::List(_) => {
                    for item in val.__iter__()? {
                        check(&item, has_entries, has_strings)?;
                    }
                }
                Arg::Dict(_) => {
                    for v in val.values()?.__iter__()? {
                        check(&v, has_entries, has_strings)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        let mut has_entries = false;
        let mut has_strings = false;
        for v in data.values()?.__iter__()? {
            check(&v, &mut has_entries, &mut has_strings)?;
        }
        Ok((has_entries, has_strings))
    }

    /// Every `Entry` object from a nested dict.
    pub fn _iter_entries(data: &Arg) -> Result<Vec<Arg>> {
        let mut out = Vec::new();
        for val in data.values()?.__iter__()? {
            match &val {
                Arg::Entry(_) => out.push(val.clone()),
                Arg::List(_) => {
                    for item in val.__iter__()? {
                        if let Arg::Entry(_) = item {
                            out.push(item);
                        }
                    }
                }
                Arg::Dict(_) => out.extend(Manifest::_iter_entries(&val)?),
                _ => {}
            }
        }
        Ok(out)
    }

    /// Recursively validate that *data* follows the blueprint schema.
    pub fn _validate_blueprint<A: PyArg + ?Sized>(data: &A) -> Result<()> {
        let data = &data.to_arg();
        if !matches!(data, Arg::Dict(_)) {
            return Err(LailaError::value_error("Blueprint must be a dict."));
        }
        for key in data.__iter__()? {
            let val = data.__getitem__(key.clone())?;
            if !matches!(key, Arg::Str(_)) {
                return Err(LailaError::value_error(alloc::format!(
                    "Blueprint keys must be strings, got {}",
                    type_repr(&key)
                )));
            }
            match &val {
                Arg::Str(_) => continue,
                Arg::List(_) => {
                    for item in val.__iter__()? {
                        if !matches!(item, Arg::Str(_)) {
                            return Err(LailaError::value_error(alloc::format!(
                                "List values in blueprint must be strings, got {}",
                                type_repr(&item)
                            )));
                        }
                    }
                }
                Arg::Dict(_) => Manifest::_validate_blueprint(&val)?,
                other => {
                    return Err(LailaError::value_error(alloc::format!(
                        "Blueprint values must be str, list[str], or dict \u{2014} got {}",
                        type_repr(other)
                    )))
                }
            }
        }
        Ok(())
    }

    /// Convert a dict of `Entry` objects to a blueprint of `global_id` strings.
    pub fn _extract_blueprint(data: &Arg) -> Result<Arg> {
        let mut result = PyDictBuilder::new();
        for key in data.__iter__()? {
            let val = data.__getitem__(key.clone())?;
            let out = match &val {
                Arg::Entry(e) => Arg::Str(e.global_id().into()),
                Arg::Str(_) => val.clone(),
                Arg::List(_) => {
                    let mut items = Vec::new();
                    for item in val.__iter__()? {
                        items.push(match &item {
                            Arg::Entry(e) => Arg::Str(e.global_id().into()),
                            _ => item.clone(),
                        });
                    }
                    list_arg(items)
                }
                Arg::Dict(_) => Manifest::_extract_blueprint(&val)?,
                other => {
                    return Err(LailaError::value_error(alloc::format!(
                        "Unexpected value type in manifest data: {}",
                        type_repr(other)
                    )))
                }
            };
            result.insert(key, out);
        }
        Ok(result.finish())
    }

    /// Depth-first, insertion-order walk yielding every leaf `global_id`.
    pub fn _iter_global_ids(blueprint: &Arg) -> Result<Vec<Arg>> {
        let mut out = Vec::new();
        for val in blueprint.values()?.__iter__()? {
            match &val {
                Arg::Str(_) => out.push(val.clone()),
                Arg::List(_) => out.extend(val.__iter__()?),
                Arg::Dict(_) => out.extend(Manifest::_iter_global_ids(&val)?),
                _ => {}
            }
        }
        Ok(out)
    }

    /// Reconstruct the nested structure replacing `global_id` strings with entries.
    pub fn _rebuild_with_entries(blueprint: &Arg, resolved_map: &Arg) -> Result<Arg> {
        let mut result = PyDictBuilder::new();
        for key in blueprint.__iter__()? {
            let val = blueprint.__getitem__(key.clone())?;
            match &val {
                Arg::Str(_) => {
                    result.insert(key, resolved_map.__getitem__(val)?);
                }
                Arg::List(_) => {
                    let mut items = Vec::new();
                    for gid in val.__iter__()? {
                        items.push(resolved_map.__getitem__(gid)?);
                    }
                    result.insert(key, list_arg(items));
                }
                Arg::Dict(_) => {
                    result.insert(key, Manifest::_rebuild_with_entries(&val, resolved_map)?);
                }
                _ => {}
            }
        }
        Ok(result.finish())
    }
}

/// `SELECT ... FROM` clause scan: `\b<clause>\b` with `\s+` between words.
fn contains_clause(upper: &str, clause: &str) -> bool {
    let words: Vec<&str> = clause.split(' ').collect();
    // Word-boundary match: the clause words in sequence, whitespace
    // (``\s+``) between them.
    let mut pos = 0;
    while let Some(start) = find_token(upper, pos, words[0]) {
        let mut cursor = start + words[0].len();
        let mut ok = true;
        for w in &words[1..] {
            let ws = upper[cursor..].len() - upper[cursor..].trim_start().len();
            if ws == 0 {
                ok = false;
                break;
            }
            cursor += ws;
            if !upper[cursor..].starts_with(w) || !boundary_after(upper, cursor + w.len()) {
                ok = false;
                break;
            }
            cursor += w.len();
        }
        if ok {
            return true;
        }
        pos = start + 1;
    }
    false
}

fn boundary_before(s: &str, i: usize) -> bool {
    i == 0
        || !s[..i]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
}
fn boundary_after(s: &str, i: usize) -> bool {
    i >= s.len()
        || !s[i..]
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
}
fn find_token(s: &str, from: usize, word: &str) -> Option<usize> {
    let mut pos = from;
    while pos <= s.len() {
        let i = s[pos..].find(word)? + pos;
        if boundary_before(s, i) && boundary_after(s, i + word.len()) {
            return Some(i);
        }
        pos = i + 1;
    }
    None
}
fn strip_keyword<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    if s.len() >= kw.len() && s[..kw.len()].eq_ignore_ascii_case(kw) {
        Some(&s[kw.len()..])
    } else {
        None
    }
}

// ----------------------------------------------------------------------
// The dynamic surface (consulted by `Entry.__getattr__` / `__call_method__`)
// ----------------------------------------------------------------------

/// `manifest.<name>` for the `Manifest`-only attributes; `None` falls
/// through to the `Entry` attributes.
pub fn getattr(e: &Entry, name: &str) -> Result<Option<Arg>> {
    let m = Manifest(e.clone());
    Ok(Some(match name {
        "blueprint" => m.blueprint()?,
        "realized" => m.realized()?,
        // `await m.async_realized`: the property is an awaitable (a coroutine).
        "async_realized" => {
            let m = m.clone();
            crate::python::stdlib::asyncio::coroutine(async move { m.async_realized().await })
        }
        PENDING_ATTR => m._pending_entries(),
        "_sql_state" => m._sql_state(),
        "_sql_finalizer" => m._sql_finalizer(),
        _ => return Ok(None),
    }))
}

/// `manifest.<name>(*args, **kwargs)` for the `Manifest` methods; `None`
/// falls through to the `Entry` methods.
pub fn call_method(e: &Entry, name: &str, args: &Args) -> Result<Option<Arg>> {
    let m = Manifest(e.clone());
    let first = || args.positional.first().cloned().unwrap_or(Arg::None);
    Ok(Some(match name {
        "memorize" => Arg::Future(m.memorize(args.clone())?),
        "remember" => Arg::Future(m.remember(args.clone())?),
        "forget" => Arg::Future(m.forget(args.clone())?),
        "sql" => m.sql(first())?.into_arg(),
        "build_index" => {
            m.build_index(args.clone())?;
            Arg::None
        }
        "invalidate_index" => {
            m.invalidate_index();
            Arg::None
        }
        "clear_index" => {
            m.clear_index(args.clone())?;
            Arg::None
        }
        "keys" => m.keys(Args::new())?,
        "values" => m.values()?,
        "items" => m.items()?,
        "sub_manifest" => m.sub_manifest(first())?.into_arg(),
        "extend" => {
            m.extend(args.clone())?;
            Arg::None
        }
        "floating_leaves" => m.floating_leaves()?,
        "__iadd__" => m.__iadd__(first())?,
        "__add__" => m.__add__(first())?,
        "__len__" => Arg::Int(m.__len__()? as i64),
        "__iter__" => list_arg(m.__iter__()?),
        "__contains__" => Arg::Bool(m.__contains__(first())),
        "__getitem__" => m.__getitem__(first())?,
        "_candidate_pools" => m
            ._candidate_pools()
            .into_iter()
            .map(IntoArg::into_arg)
            .collect::<Vec<_>>()
            .into_arg(),
        "_parse_sql" => {
            let (items, from, where_) = Manifest::_parse_sql_typed(&first())?;
            (items, from, where_).into_arg()
        }
        "_strip_table_prefix" => Arg::Str(Manifest::_strip_table_prefix(&first().__str__()).into()),
        "_validate_ident" => {
            Manifest::_validate_ident(first())?;
            Arg::None
        }
        "_classify_data" => {
            let (a, b) = Manifest::_classify_data(&first())?;
            (a, b).into_arg()
        }
        "_iter_entries" => list_arg(Manifest::_iter_entries(&first())?),
        "_sql_state" => m._sql_state(),
        "_existing_indexes" => Manifest::_existing_indexes(first())?,
        "_validate_blueprint" => {
            Manifest::_validate_blueprint(&first())?;
            Arg::None
        }
        "_extract_blueprint" => Manifest::_extract_blueprint(&first())?,
        "_iter_global_ids" => list_arg(Manifest::_iter_global_ids(&first())?),
        "_rebuild_with_entries" => {
            let second = args.positional.get(1).cloned().unwrap_or(Arg::None);
            Manifest::_rebuild_with_entries(&first(), &second)?
        }
        "_resolve_pool" => {
            let get = |i: usize, k: &str| {
                args.positional
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| args.get(k))
            };
            Manifest::_resolve_pool(get(0, "pool"), get(1, "pool_nickname"), get(2, "pool_id"))
        }
        _ => return Ok(None),
    }))
}

impl crate::python::builtins::PyLen for Manifest {
    fn __len__(&self) -> Result<usize> {
        Manifest::__len__(self)
    }
}
/// `m1 + m2` on typed manifests: `Manifest.__add__`, with the
/// `NotImplemented` answer raised as Python's `TypeError`.
impl<T: IntoArg> core::ops::Add<T> for Manifest {
    type Output = Result<Manifest>;
    fn add(self, rhs: T) -> Result<Manifest> {
        (&self).add(rhs)
    }
}
impl<T: IntoArg> core::ops::Add<T> for &Manifest {
    type Output = Result<Manifest>;
    fn add(self, rhs: T) -> Result<Manifest> {
        let rhs = rhs.into_arg();
        let out = self.__add__(rhs.clone())?;
        match Manifest::from_arg(&out) {
            Some(m) => Ok(m),
            None => Err(LailaError::type_error(alloc::format!(
                "unsupported operand type(s) for +: 'Manifest' and '{}'",
                rhs.type_name()
            ))),
        }
    }
}
impl IntoArg for Manifest {
    fn into_arg(self) -> Arg {
        Arg::Entry(self.0)
    }
}
impl PyArg for Manifest {
    fn to_arg(&self) -> Arg {
        Arg::Entry(self.0.clone())
    }
}
impl IsInstance for Manifest {
    fn isinstance(x: &Arg) -> bool {
        Manifest::from_arg(x).is_some()
    }
}
impl PartialEq for Manifest {
    fn eq(&self, other: &Manifest) -> bool {
        self.0 == other.0
    }
}
impl core::fmt::Debug for Manifest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__repr__())
    }
}
impl core::fmt::Display for Manifest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.__str__())
    }
}
