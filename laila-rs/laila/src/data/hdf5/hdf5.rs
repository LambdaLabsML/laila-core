//! HDF5 file pool implementation.
//!
//! Twin of `laila/data/hdf5/hdf5.py`: each entry is stored as a UTF-8
//! (variable-length, `h5py.string_dtype(encoding="utf-8")`) scalar string
//! dataset named by the URL-encoded key, inside a single HDF5 file that
//! remains open for the lifetime of the pool. The same `libhdf5` `h5py`
//! wraps writes it, so a `.h5py` file written by either implementation
//! opens in the other.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::any::Any;

use hdf5::types::{VarLenAscii, VarLenUnicode};
use parking_lot::Mutex;

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::atomic::definitions::HasIdentity;
use crate::data::schema::base::{
    field_default, pool_class, private_default, Pool, PoolBackend, PoolObject, _PERSISTENT_STORAGE,
};
use crate::python::errors::ExcKind;
use crate::python::object::PyObject;
use crate::python::stdlib::{os, urllib};
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

/// An HDF5 library error (`OSError`, the class `h5py` raises).
fn hdf5_error(e: hdf5::Error) -> LailaError {
    LailaError::new(ExcKind::OSError, alloc::format!("{e}"))
}

/// The shared file slot (`self._file`; `None` once closed).
type FileSlot = Arc<Mutex<Option<hdf5::File>>>;

/// `HDF5Pool`'s storage: the open file (`self._file`).
#[derive(Default)]
pub struct HDF5Backend {
    file: FileSlot,
}

impl core::fmt::Debug for HDF5Backend {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("HDF5Backend")
    }
}

/// `pool._file`: the `h5py.File` -- membership, `keys()`, `flush()`.
pub struct HDF5File {
    file: FileSlot,
}

impl HDF5File {
    fn with<T>(&self, f: impl FnOnce(&hdf5::File) -> hdf5::Result<T>) -> Result<T> {
        match self.file.lock().as_ref() {
            None => Err(LailaError::value_error("Not a file (not a file)")),
            Some(file) => f(file).map_err(hdf5_error),
        }
    }
}

impl PyObject for HDF5File {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "File"
    }
    fn __repr__(&self) -> String {
        match self.file.lock().as_ref() {
            Some(file) => alloc::format!(
                "<HDF5 file \"{}\" (mode r+)>",
                os.path()
                    .basename(file.filename())
                    .unwrap_or_else(|_| file.filename())
            ),
            None => String::from("<Closed HDF5 file>"),
        }
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<HDF5File>()
            .is_some_and(|o| Arc::ptr_eq(&o.file, &self.file))
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.file) as usize as i64)
    }
    fn __bool__(&self) -> bool {
        self.file.lock().is_some()
    }
    fn __len__(&self) -> Result<usize> {
        self.with(|f| f.member_names().map(|n| n.len()))
    }
    fn __contains__(&self, item: &Arg) -> Result<bool> {
        let name = item.__str__();
        self.with(|f| Ok(f.link_exists(&name)))
    }
    fn __iter__(&self) -> Result<Vec<Arg>> {
        self.with(|f| f.member_names())
            .map(|names| names.into_iter().map(IntoArg::into_arg).collect())
    }
    fn __call_method__(&self, name: &str, _args: Args) -> Result<Arg> {
        match name {
            "keys" => self.__iter__().map(IntoArg::into_arg),
            "flush" => self.with(|f| f.flush()).map(|()| Arg::None),
            "close" => {
                if let Some(file) = self.file.lock().take() {
                    file.close().map_err(hdf5_error)?;
                }
                Ok(Arg::None)
            }
            _ => Err(LailaError::attribute_error(alloc::format!(
                "'File' object has no attribute '{name}'"
            ))),
        }
    }
    fn __dir__(&self) -> Vec<String> {
        alloc::vec![
            String::from("keys"),
            String::from("flush"),
            String::from("close")
        ]
    }
}

impl IntoArg for HDF5File {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}

impl HDF5Backend {
    /// Return the root HDF5 group (`self._root()`).
    fn with_root<T>(&self, f: impl FnOnce(&hdf5::File) -> hdf5::Result<T>) -> Result<T> {
        let guard = self.file.lock();
        match guard.as_ref() {
            None => Err(LailaError::runtime_error("HDF5Pool is closed.")),
            Some(file) => f(file).map_err(hdf5_error),
        }
    }

    /// URL-encode *key* to avoid `/` collisions in HDF5 paths.
    fn _storage_key(&self, key: &str) -> Result<String> {
        urllib.parse().quote(crate::args!(key, safe = ""))
    }

    /// Decode a storage key back to the logical key.
    fn _logical_key(&self, key: &str) -> Result<String> {
        urllib.parse().unquote(key)
    }

    /// Read the raw UTF-8 string for *key*, or `None`.
    fn _read_raw(&self, key: &str) -> Result<Option<String>> {
        let skey = self._storage_key(key)?;
        self.with_root(|root| {
            if !root.link_exists(&skey) {
                return Ok(None);
            }
            let ds = root.dataset(&skey)?;
            match ds.read_scalar::<VarLenUnicode>() {
                Ok(s) => Ok(Some(s.as_str().to_string())),
                // A dataset written with another string type (`ascii`, a
                // fixed width): `str(raw)` on the decoded bytes.
                Err(_) => ds
                    .read_scalar::<VarLenAscii>()
                    .map(|s| Some(s.as_str().to_string())),
            }
        })
    }
}

impl PoolBackend for HDF5Backend {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Open (or create) the HDF5 file.
    fn model_post_init(&self, pool: &PoolObject) -> Result<()> {
        let file_path = match pool.field("file_path").unwrap_or(Arg::None) {
            Arg::None => {
                let pools =
                    crate::macros::defaults::LAILA_DEFAULT_DIRECTORIES().__getitem__("pools")?;
                let pool_dir = os.path().join(crate::args!(pools, pool.uuid()))?;
                os.makedirs(crate::args!(pool_dir.clone(), exist_ok = true))?;
                os.path().join(crate::args!(pool_dir, "pool.h5py"))?
            }
            given => {
                os.makedirs(crate::args!(
                    os.path().dirname(os.path().expanduser(given.clone())?)?,
                    exist_ok = true
                ))?;
                os.path().expanduser(given)?
            }
        };
        pool.set_field("file_path", file_path.clone());

        // Open file and keep it open for the lifetime of the pool.
        let file = hdf5::File::append(&file_path).map_err(hdf5_error)?;
        *self.file.lock() = Some(file);
        Ok(())
    }

    /// Close the HDF5 file handle.
    ///
    /// Runs under the pool lock so an in-flight `_write`/`_delete`
    /// finishes before the handle is released; later writers hit the
    /// documented `RuntimeError("HDF5Pool is closed.")` instead of
    /// crashing inside the library.
    fn close(&self, pool: &PoolObject) -> Option<Result<()>> {
        let _held = pool.held();
        if let Some(file) = self.file.lock().take() {
            if let Err(e) = file.close() {
                return Some(Err(hdf5_error(e)));
            }
        }
        Some(Ok(()))
    }

    /// Retrieve the JSON value for *key*, or `None` if absent.
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        let raw = {
            let _held = pool.held();
            self._read_raw(key)?
        };
        match raw {
            Some(text) => crate::_compat::pyjson::loads(&text),
            None => Ok(Arg::None),
        }
    }

    /// Store *entry* as a UTF-8 HDF5 dataset under *key*.
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
                "HDF5Pool expects a serialized JSON string.",
            ));
        };

        let skey = self._storage_key(key)?;
        let _held = pool.held();
        self.with_root(|root| {
            if root.link_exists(&skey) {
                root.unlink(&skey)?;
            }
            let data: VarLenUnicode = value
                .parse()
                .map_err(|e| hdf5::Error::from(alloc::format!("{e}")))?;
            root.new_dataset::<VarLenUnicode>()
                .create(skey.as_str())?
                .write_scalar(&data)?;
            // Push the write through the HDF5 library buffers so a process
            // that dies without `close()` (crash, `os._exit`, SIGKILL)
            // still leaves a readable file. Library-level flush, not fsync.
            root.flush()
        })
    }

    /// Delete the dataset for *key* if it exists.
    fn _delete(&self, pool: &PoolObject, key: &str) -> Result<()> {
        let skey = self._storage_key(key)?;
        let _held = pool.held();
        self.with_root(|root| {
            if root.link_exists(&skey) {
                root.unlink(&skey)?;
                root.flush()?;
            }
            Ok(())
        })
    }

    /// Remove all entries from the pool.
    fn _empty(&self, pool: &PoolObject) -> Result<()> {
        let _held = pool.held();
        self.with_root(|root| {
            for name in root.member_names()? {
                root.unlink(&name)?;
            }
            root.flush()
        })
    }

    /// `True` if a dataset for *key* exists.
    fn _exists(&self, pool: &PoolObject, key: &str) -> Result<bool> {
        let skey = self._storage_key(key)?;
        let _held = pool.held();
        self.with_root(|root| Ok(root.link_exists(&skey)))
    }

    /// Return all keys in the HDF5 file.
    fn _keys(&self, pool: &PoolObject) -> Result<Vec<String>> {
        let _held = pool.held();
        let skeys = self.with_root(|root| root.member_names())?;
        skeys.iter().map(|k| self._logical_key(k)).collect()
    }

    fn __getattr__(&self, _pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        match name {
            // `self._file` -- `None` once closed, otherwise the open file.
            "_file" => Some(Ok(if self.file.lock().is_some() {
                HDF5File {
                    file: self.file.clone(),
                }
                .into_arg()
            } else {
                Arg::None
            })),
            _ => None,
        }
    }

    fn __call_method__(&self, pool: &PoolObject, name: &str, args: Args) -> Option<Result<Arg>> {
        let first = || {
            args.positional
                .first()
                .cloned()
                .unwrap_or_else(|| args.get("key"))
                .__str__()
        };
        Some(match name {
            "_root" => match self.__getattr__(pool, "_file") {
                Some(Ok(Arg::None)) | None => Err(LailaError::runtime_error("HDF5Pool is closed.")),
                Some(r) => r,
            },
            "_storage_key" => self._storage_key(&first()).map(IntoArg::into_arg),
            "_logical_key" => self._logical_key(&first()).map(IntoArg::into_arg),
            "_read_raw" => self._read_raw(&first()).map(|r| match r {
                Some(s) => s.into_arg(),
                None => Arg::None,
            }),
            // `active_policy.central.memory.global_id`
            "_resolve_memory_global_id" => crate::policy::get_active_policy()
                .central()
                .memory()
                .__getattr__("global_id"),
            _ => return None,
        })
    }

    fn method_names(&self) -> &'static [&'static str] {
        &[
            "close",
            "_root",
            "_storage_key",
            "_logical_key",
            "_read_raw",
            "_resolve_memory_global_id",
        ]
    }
}

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        "HDF5Pool",
        "laila.data.hdf5.hdf5",
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
        alloc::vec![(String::from("_file"), private_default(Arg::None))],
        alloc::vec![(String::from(_PERSISTENT_STORAGE), Arg::Bool(true))],
    )
});

crate::laila_pool_handle!(
    /// HDF5 file-backed pool.
    ///
    /// Each entry is stored as a UTF-8 string dataset inside a single HDF5
    /// file that remains open for the lifetime of the pool.
    HDF5Pool,
    HDF5Backend
);

impl ModelBase for HDF5Pool {
    fn __class__() -> &'static ModelClass {
        HDF5Pool::__class__()
    }
}

impl HDF5Pool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `HDF5Pool(file_path=None, transformations=transformation_base64, **kwargs)`
    #[pysig("HDF5Pool(**kwargs)")]
    pub fn new(args: impl Into<Args>) -> Result<HDF5Pool> {
        Ok(HDF5Pool(Pool::construct(
            Self::__class__(),
            kwargs,
            |_inst: &ModelInstance| Ok(Box::new(HDF5Backend::default())),
        )?))
    }
    /// `pool.file_path`
    pub fn file_path(&self) -> Arg {
        self.0.field("file_path").unwrap_or(Arg::None)
    }
    /// `pool.close()`
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
}

impl crate::python::builtins::CallValue for HDF5Pool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        HDF5Pool::new(args).map(IntoArg::into_arg)
    }
}

/// `HDF5Pool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    HDF5Pool::__class__().as_pyclass().into_arg()
}

impl PyArg for HDF5Backend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
