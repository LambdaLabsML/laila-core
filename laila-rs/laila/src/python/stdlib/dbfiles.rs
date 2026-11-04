//! `import sqlite3` / `import h5py` -- the file-database modules a test
//! reaches for to re-open a pool's file after `laila.terminate()` (the
//! "file unlocked" probes). Thin handles over the same crates the pools
//! use (`rusqlite`, `hdf5`); feature-gated with them.

use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

use crate::python::sync::Mutex;
use crate::python::{Arg, Args, ExcKind, IntoArg, LailaError, PyObject, Result};

fn first_path(a: &Args, what: &str) -> Result<String> {
    a.positional
        .first()
        .map(|p| p.__str__())
        .ok_or_else(|| LailaError::type_error(alloc::format!("{what}() missing required argument")))
}

// ---------------------------------------------------------------------------
// sqlite3

/// `import sqlite3`
#[cfg(feature = "sqlite")]
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct sqlite3_mod;
#[cfg(feature = "sqlite")]
pub static sqlite3: sqlite3_mod = sqlite3_mod;

#[cfg(feature = "sqlite")]
impl sqlite3_mod {
    /// `sqlite3.connect(database)`
    pub fn connect(&self, args: impl Into<Args>) -> Result<Sqlite3Connection> {
        let a: Args = args.into();
        let path = first_path(&a, "connect")?;
        let conn = rusqlite::Connection::open(&path)
            .map_err(|e| LailaError::new(ExcKind::OperationalError, alloc::format!("{e}")))?;
        Ok(Sqlite3Connection {
            conn: Arc::new(Mutex::new(Some(conn))),
        })
    }
}

/// `sqlite3.Connection`
#[cfg(feature = "sqlite")]
#[derive(Clone)]
pub struct Sqlite3Connection {
    conn: Arc<Mutex<Option<rusqlite::Connection>>>,
}

#[cfg(feature = "sqlite")]
impl Sqlite3Connection {
    fn closed() -> LailaError {
        LailaError::new(
            ExcKind::ProgrammingError,
            "Cannot operate on a closed database.",
        )
    }
    /// `con.execute(sql)` -> the rows (as tuples), fetched eagerly.
    pub fn execute(&self, args: impl Into<Args>) -> Result<Arg> {
        let a: Args = args.into();
        let sql = first_path(&a, "execute")?;
        let guard = self.conn.lock();
        let conn = guard.as_ref().ok_or_else(Self::closed)?;
        let err =
            |e: rusqlite::Error| LailaError::new(ExcKind::OperationalError, alloc::format!("{e}"));
        let mut stmt = conn.prepare(&sql).map_err(err)?;
        let n = stmt.column_count();
        let rows: Vec<Arg> = stmt
            .query_map([], |row| {
                let mut cells = Vec::with_capacity(n);
                for i in 0..n {
                    use rusqlite::types::ValueRef;
                    cells.push(match row.get_ref(i)? {
                        ValueRef::Null => Arg::None,
                        ValueRef::Integer(v) => Arg::Int(v),
                        ValueRef::Real(v) => Arg::Float(v),
                        ValueRef::Text(t) => Arg::Str(String::from_utf8_lossy(t).as_ref().into()),
                        ValueRef::Blob(b) => Arg::Bytes(b.to_vec().into()),
                    });
                }
                Ok(Arg::Tuple(cells.into()))
            })
            .map_err(err)?
            .collect::<rusqlite::Result<_>>()
            .map_err(err)?;
        Ok(Sqlite3Cursor(Mutex::new(rows)).into_arg())
    }
    /// `con.commit()` (autocommit here: a no-op)
    pub fn commit(&self, _args: impl Into<Args>) -> Result<()> {
        Ok(())
    }
    /// `con.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        if let Some(c) = self.conn.lock().take() {
            if let Err((_, e)) = c.close() {
                return Err(LailaError::new(
                    ExcKind::OperationalError,
                    alloc::format!("{e}"),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(feature = "sqlite")]
impl core::fmt::Debug for Sqlite3Connection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("sqlite3.Connection")
    }
}

#[cfg(feature = "sqlite")]
impl PyObject for Sqlite3Connection {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Connection"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<sqlite3.Connection object at {:#x}>",
            Arc::as_ptr(&self.conn) as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Sqlite3Connection>()
            .is_some_and(|o| Arc::ptr_eq(&o.conn, &self.conn))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.conn) as usize as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "execute" => self.execute(args),
            "commit" => self.commit(args).map(|()| Arg::None),
            "close" => self.close(args).map(|()| Arg::None),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'sqlite3.Connection' object has no attribute '{name}'"
            ))),
        }
    }
    fn __dir__(&self) -> Vec<String> {
        alloc::vec![
            String::from("execute"),
            String::from("commit"),
            String::from("close")
        ]
    }
}

#[cfg(feature = "sqlite")]
impl IntoArg for Sqlite3Connection {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

/// `sqlite3.Cursor`: the rows of one `con.execute(..)`, consumed by
/// `fetchone()` / `fetchall()` / iteration.
#[cfg(feature = "sqlite")]
pub struct Sqlite3Cursor(Mutex<Vec<Arg>>);

#[cfg(feature = "sqlite")]
impl Sqlite3Cursor {
    /// `cur.fetchone()` -> the next row or `None`.
    pub fn fetchone(&self, _args: impl Into<Args>) -> Arg {
        let mut g = self.0.lock();
        if g.is_empty() {
            Arg::None
        } else {
            g.remove(0)
        }
    }
    /// `cur.fetchall()` -> the remaining rows.
    pub fn fetchall(&self, _args: impl Into<Args>) -> Arg {
        let rows: Vec<Arg> = core::mem::take(&mut *self.0.lock());
        rows.into_arg()
    }
}

#[cfg(feature = "sqlite")]
impl PyObject for Sqlite3Cursor {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Cursor"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "<sqlite3.Cursor object at {:#x}>",
            self as *const _ as usize
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Sqlite3Cursor>()
            .is_some_and(|o| core::ptr::eq(o, self))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok((self as *const _ as usize >> 4) as i64)
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Ok(core::mem::take(&mut *self.0.lock()))
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "fetchone" => Ok(self.fetchone(args)),
            "fetchall" => Ok(self.fetchall(args)),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'sqlite3.Cursor' object has no attribute '{name}'"
            ))),
        }
    }
    fn __dir__(&self) -> Vec<String> {
        alloc::vec![String::from("fetchone"), String::from("fetchall")]
    }
}

#[cfg(feature = "sqlite")]
impl IntoArg for Sqlite3Cursor {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

// ---------------------------------------------------------------------------
// h5py

/// `import h5py`
#[cfg(feature = "hdf5")]
#[derive(Clone, Copy, Debug, Default)]
#[allow(non_camel_case_types)]
pub struct h5py_mod;
#[cfg(feature = "hdf5")]
pub static h5py: h5py_mod = h5py_mod;

#[cfg(feature = "hdf5")]
impl h5py_mod {
    /// `h5py.File(name, mode="r")`: `r`, `r+`, `w`, `a` / `w-` / `x`.
    #[allow(non_snake_case)]
    pub fn File(&self, args: impl Into<Args>) -> Result<H5File> {
        let a: Args = args.into();
        let path = first_path(&a, "File")?;
        let mode = a
            .positional
            .get(1)
            .cloned()
            .unwrap_or_else(|| a.get("mode"));
        let mode = if mode.is_none() {
            String::from("r")
        } else {
            mode.__str__()
        };
        let err = |e: hdf5::Error| LailaError::new(ExcKind::OSError, alloc::format!("{e}"));
        let file = match mode.as_str() {
            "r" => hdf5::File::open(&path),
            "r+" => hdf5::File::open_rw(&path),
            "w" => hdf5::File::create(&path),
            "w-" | "x" => hdf5::File::create_excl(&path),
            "a" => hdf5::File::append(&path),
            other => {
                return Err(LailaError::value_error(alloc::format!(
                    "Invalid mode; must be one of r, r+, w, w-, x, a (got '{other}')"
                )))
            }
        }
        .map_err(err)?;
        Ok(H5File {
            file: Arc::new(Mutex::new(Some(file))),
        })
    }
}

/// `h5py.File` -- an open HDF5 file handle (`close()` releases the lock).
#[cfg(feature = "hdf5")]
#[derive(Clone)]
pub struct H5File {
    file: Arc<Mutex<Option<hdf5::File>>>,
}

#[cfg(feature = "hdf5")]
impl H5File {
    /// `f.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        if let Some(f) = self.file.lock().take() {
            f.close()
                .map_err(|e| LailaError::new(ExcKind::OSError, alloc::format!("{e}")))?;
        }
        Ok(())
    }
    /// `f.keys()`: the root group's member names.
    pub fn keys(&self, _args: impl Into<Args>) -> Result<Arg> {
        let guard = self.file.lock();
        let f = guard
            .as_ref()
            .ok_or_else(|| LailaError::value_error("Not a file (file closed)"))?;
        let names: Vec<Arg> = f
            .member_names()
            .map_err(|e| LailaError::new(ExcKind::OSError, alloc::format!("{e}")))?
            .into_iter()
            .map(|s| Arg::Str(s.as_str().into()))
            .collect();
        Ok(names.into_arg())
    }
    /// `with h5py.File(..) as f:`
    pub fn __enter__(&self) -> Result<H5File> {
        Ok(self.clone())
    }
    /// `f.__exit__(..)`
    pub fn __exit__(&self, _args: impl Into<Args>) -> Result<bool> {
        self.close(Args::new())?;
        Ok(false)
    }
}

#[cfg(feature = "hdf5")]
impl core::fmt::Debug for H5File {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("h5py.File")
    }
}

#[cfg(feature = "hdf5")]
impl PyObject for H5File {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "File"
    }
    fn __repr__(&self) -> String {
        if self.file.lock().is_some() {
            String::from("<HDF5 file (mode r+)>")
        } else {
            String::from("<Closed HDF5 file>")
        }
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<H5File>()
            .is_some_and(|o| Arc::ptr_eq(&o.file, &self.file))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.file) as usize as i64)
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        match name {
            "keys" => self.keys(args),
            "close" => self.close(args).map(|()| Arg::None),
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'File' object has no attribute '{name}'"
            ))),
        }
    }
    fn __dir__(&self) -> Vec<String> {
        alloc::vec![String::from("keys"), String::from("close")]
    }
}

#[cfg(feature = "hdf5")]
impl IntoArg for H5File {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
