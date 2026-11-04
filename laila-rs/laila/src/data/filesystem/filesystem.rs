//! Loopback-mounted ext4 filesystem pool implementation.
//!
//! A [`FilesystemPool`] writes each entry as a single JSON file inside a
//! private, loopback-mounted ext4 image. The image is created on first
//! use, mounted via `mount -o loop`, and not unmounted by laila (so
//! subsequent process runs can reuse it).
//!
//! Why an image-backed mount instead of a plain directory?
//!
//! - It bounds the pool's footprint to a fixed size (`_image_size_bytes`,
//!   64 MiB by default) so a runaway producer can't fill the host disk.
//! - It provides POSIX-level isolation -- the pool's contents share an
//!   independent inode table from the host filesystem and can be
//!   detached/relocated as a single `.img` file.
//!
//! The class is Linux-only (relies on `mkfs.ext4` and `mount`) and needs
//! root privileges or appropriate capabilities to mount.
//!
//! Twin of `laila/data/filesystem/filesystem.py`. The methods the Python
//! tests patch on the class (`_resolve_pool_dir`, `_resolve_image_path`,
//! `_is_mounted`, `_mount_image`, `_create_image_file`, ...) consult
//! [`unittest.mock`](crate::python::mock) first, so `patch.object(
//! FilesystemPool, "_mount_image", ..)` works as in Python.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use spin::RwLock;

use crate::_compat::pydantic::{LazyClass, ModelBase, ModelClass, ModelInstance};
use crate::atomic::definitions::HasIdentity;
use crate::data::schema::base::{
    field_default, pool_class, private_default, Pool, PoolBackend, PoolObject, _PERSISTENT_STORAGE,
};
use crate::python::mock::call_patched;
use crate::python::stdlib::{os, urllib};
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, Result};

/// `_image_size_bytes` default: 64 MiB.
const IMAGE_SIZE_BYTES: i64 = 64 * 1024 * 1024;

const CLASS_NAME: &str = "FilesystemPool";

/// `FilesystemPool`'s private state (`_pool_dir`, `_mount_dir`,
/// `_image_path`, `_image_size_bytes`).
pub struct FilesystemBackend {
    _pool_dir: RwLock<Option<String>>,
    _mount_dir: RwLock<Option<String>>,
    _image_path: RwLock<Option<String>>,
    _image_size_bytes: RwLock<i64>,
}

impl Default for FilesystemBackend {
    fn default() -> Self {
        FilesystemBackend {
            _pool_dir: RwLock::new(None),
            _mount_dir: RwLock::new(None),
            _image_path: RwLock::new(None),
            _image_size_bytes: RwLock::new(IMAGE_SIZE_BYTES),
        }
    }
}

impl core::fmt::Debug for FilesystemBackend {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("FilesystemBackend")
    }
}

fn unset(name: &str) -> LailaError {
    LailaError::attribute_error(alloc::format!(
        "'FilesystemPool' object has no attribute '{name}'"
    ))
}

/// The patched twin of a method, when `patch.object(FilesystemPool, name, ..)`
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

impl FilesystemBackend {
    /// Root directory for this pool's artefacts.
    pub fn pool_dir(&self) -> Result<String> {
        self._pool_dir
            .read()
            .clone()
            .ok_or_else(|| unset("_pool_dir"))
    }
    /// Mount-point directory where entries are stored.
    pub fn mount_dir(&self) -> Result<String> {
        self._mount_dir
            .read()
            .clone()
            .ok_or_else(|| unset("_mount_dir"))
    }
    /// Path to the ext4 image file.
    pub fn image_path(&self) -> Result<String> {
        self._image_path
            .read()
            .clone()
            .ok_or_else(|| unset("_image_path"))
    }

    /// Derive the pool directory from default directories.
    pub fn _resolve_pool_dir(&self, pool: &PoolObject) -> Result<String> {
        patched!(pool, "_resolve_pool_dir");
        let pools = crate::macros::defaults::LAILA_DEFAULT_DIRECTORIES().__getitem__("pools")?;
        os.path().join(crate::args!(pools, pool.uuid()))
    }

    /// Resolve the `.img` or `.iso` image path for this pool.
    pub fn _resolve_image_path(&self, pool: &PoolObject) -> Result<String> {
        patched!(pool, "_resolve_image_path");
        let pool_dir = self.pool_dir()?;
        let pool_id = pool.pool_id();
        let img_path = os.path().join(crate::args!(
            pool_dir.clone(),
            alloc::format!("{pool_id}.img")
        ))?;
        let iso_path = os
            .path()
            .join(crate::args!(pool_dir, alloc::format!("{pool_id}.iso")))?;

        if os.path().exists(img_path.clone())? && os.path().exists(iso_path.clone())? {
            return Err(LailaError::runtime_error(alloc::format!(
                "Ambiguous filesystem image for {pool_id}: both {img_path} and {iso_path} exist."
            )));
        }
        if os.path().exists(img_path.clone())? {
            return Ok(img_path);
        }
        if os.path().exists(iso_path.clone())? {
            return Ok(iso_path);
        }
        Ok(img_path)
    }

    /// Return the mount sub-directory path.
    pub fn _resolve_mount_dir(&self, pool: &PoolObject) -> Result<String> {
        patched!(pool, "_resolve_mount_dir");
        os.path().join(crate::args!(self.pool_dir()?, "mnt"))
    }

    /// Check whether *path* is currently a mount point.
    pub fn _is_mounted(&self, pool: &PoolObject, path: &str) -> Result<bool> {
        patched!(pool, "_is_mounted", Args::from(path));
        if os.path().ismount(path)? {
            return Ok(true);
        }

        let normalized_path = os.path().realpath(path)?;
        if let Ok(text) = std::fs::read_to_string("/proc/self/mountinfo") {
            for line in text.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() > 4 && os.path().realpath(parts[4])? == normalized_path {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Execute a subprocess command, raising on failure.
    pub fn _run_command(&self, pool: &PoolObject, command: &[String], action: &str) -> Result<()> {
        patched!(pool, "_run_command", {
            let mut a = Args::from(command.to_vec());
            a.push_keyword("action", action);
            a
        });
        let (program, rest) = command.split_first().ok_or_else(|| {
            LailaError::new(
                crate::python::errors::ExcKind::IndexError,
                "list index out of range",
            )
        })?;
        match std::process::Command::new(program).args(rest).output() {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(LailaError::runtime_error(
                alloc::format!("Unable to {action}: command not found: {program}"),
            )),
            Err(e) => Err(LailaError::new(
                crate::python::errors::ExcKind::OSError,
                e.to_string(),
            )),
            Ok(out) if !out.status.success() => Err(LailaError::runtime_error(alloc::format!(
                "Unable to {action}. Stdout: {} Stderr: {}",
                String::from_utf8_lossy(&out.stdout).trim(),
                String::from_utf8_lossy(&out.stderr).trim()
            ))),
            Ok(_) => Ok(()),
        }
    }

    /// Allocate and format a new ext4 image file.
    pub fn _create_image_file(&self, pool: &PoolObject) -> Result<()> {
        patched!(pool, "_create_image_file");
        let image_path = self.image_path()?;
        {
            let handle = crate::python::builtins::open(crate::args!(image_path.clone(), "wb"))?;
            let size: i64 = *self._image_size_bytes.read();
            handle.truncate(crate::args!(size))?;
            handle.close(Args::new())?;
        }
        self._run_command(
            pool,
            &[
                String::from("mkfs.ext4"),
                String::from("-F"),
                image_path.clone(),
            ],
            &alloc::format!("format filesystem image {image_path}"),
        )
    }

    /// Loop-mount the image file at `mount_dir`.
    pub fn _mount_image(&self, pool: &PoolObject) -> Result<()> {
        patched!(pool, "_mount_image");
        let image_path = self.image_path()?;
        let mount_dir = self.mount_dir()?;
        self._run_command(
            pool,
            &[
                String::from("mount"),
                String::from("-o"),
                String::from("loop"),
                image_path.clone(),
                mount_dir.clone(),
            ],
            &alloc::format!("mount filesystem image {image_path} at {mount_dir}"),
        )
    }

    /// URL-encode *key* and append `.json`.
    pub fn _storage_key(&self, pool: &PoolObject, key: &str) -> Result<String> {
        patched!(pool, "_storage_key", Args::from(key));
        Ok(alloc::format!(
            "{}.json",
            urllib.parse().quote(crate::args!(key, safe = ""))?
        ))
    }

    /// Strip the `.json` suffix and URL-decode.
    pub fn _logical_key(&self, pool: &PoolObject, storage_key: &str) -> Result<String> {
        patched!(pool, "_logical_key", Args::from(storage_key));
        let storage_key = storage_key.strip_suffix(".json").unwrap_or(storage_key);
        urllib.parse().unquote(storage_key)
    }

    /// Full filesystem path for the given entry key.
    pub fn _entry_path(&self, pool: &PoolObject, key: &str) -> Result<String> {
        patched!(pool, "_entry_path", Args::from(key));
        os.path().join(crate::args!(
            self.mount_dir()?,
            self._storage_key(pool, key)?
        ))
    }
}

impl PoolBackend for FilesystemBackend {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }

    /// Create or mount the filesystem image.
    fn model_post_init(&self, pool: &PoolObject) -> Result<()> {
        *self._pool_dir.write() = Some(self._resolve_pool_dir(pool)?);
        os.makedirs(crate::args!(self.pool_dir()?, exist_ok = true))?;
        *self._image_path.write() = Some(self._resolve_image_path(pool)?);
        *self._mount_dir.write() = Some(self._resolve_mount_dir(pool)?);
        os.makedirs(crate::args!(self.mount_dir()?, exist_ok = true))?;

        if self._is_mounted(pool, &self.mount_dir()?)? {
            return Ok(());
        }

        if os.path().exists(self.image_path()?)? {
            self._mount_image(pool)?;
            return Ok(());
        }

        self._create_image_file(pool)?;
        self._mount_image(pool)
    }

    /// No-op; the mount persists beyond pool lifetime.
    fn close(&self, _pool: &PoolObject) -> Option<Result<()>> {
        Some(Ok(()))
    }

    /// Read and parse the JSON file for *key*, or return `None`.
    fn _read(&self, pool: &PoolObject, key: &str) -> Result<Arg> {
        let path = self._entry_path(pool, key)?;
        if !os.path().exists(path.clone())? {
            return Ok(Arg::None);
        }

        let raw = {
            let _held = pool.held();
            let handle =
                crate::python::builtins::open(crate::args!(path, "a+", encoding = "utf-8"))?;
            handle.seek(crate::args!(0))?;
            let raw = handle.read(Args::new())?.__str__();
            handle.close(Args::new())?;
            raw
        };

        if raw.trim().is_empty() {
            return Ok(Arg::None);
        }
        crate::_compat::pyjson::loads(&raw)
    }

    /// Write *entry* as a JSON file under *key*.
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
                "FilesystemPool expects a serialized JSON string.",
            ));
        };

        let path = self._entry_path(pool, key)?;
        let _held = pool.held();
        let handle = crate::python::builtins::open(crate::args!(path, "w", encoding = "utf-8"))?;
        handle.write(Arg::Str(value))?;
        handle.close(Args::new())
    }

    /// Remove the JSON file for *key*; no-op if absent.
    fn _delete(&self, pool: &PoolObject, key: &str) -> Result<()> {
        let path = self._entry_path(pool, key)?;
        let _held = pool.held();
        match os.remove(path) {
            Err(e) if e.kind == crate::python::errors::ExcKind::FileNotFoundError => Ok(()),
            other => other,
        }
    }

    /// Remove all `.json` files from the mount directory.
    fn _empty(&self, pool: &PoolObject) -> Result<()> {
        let _held = pool.held();
        let mount_dir = self.mount_dir()?;
        for name in os.listdir(crate::args!(mount_dir.clone()))?.__iter__()? {
            let name = name.__str__();
            if !name.ends_with(".json") {
                continue;
            }
            match os.remove(os.path().join(crate::args!(mount_dir.clone(), name))?) {
                Err(e) if e.kind == crate::python::errors::ExcKind::FileNotFoundError => {}
                other => other?,
            }
        }
        Ok(())
    }

    /// Return `True` if a file for *key* exists on disk.
    fn _exists(&self, pool: &PoolObject, key: &str) -> Result<bool> {
        os.path().exists(self._entry_path(pool, key)?)
    }

    /// Return all keys in the mount directory.
    fn _keys(&self, pool: &PoolObject) -> Result<Vec<String>> {
        let _held = pool.held();
        let mut names: Vec<String> = Vec::new();
        for name in os.listdir(crate::args!(self.mount_dir()?))?.__iter__()? {
            let name = name.__str__();
            if name.ends_with(".json") {
                names.push(name);
            }
        }
        names
            .iter()
            .map(|name| self._logical_key(pool, name))
            .collect()
    }

    fn __getattr__(&self, _pool: &PoolObject, name: &str) -> Option<Result<Arg>> {
        match name {
            "pool_dir" => Some(self.pool_dir().map(IntoArg::into_arg)),
            "mount_dir" => Some(self.mount_dir().map(IntoArg::into_arg)),
            "image_path" => Some(self.image_path().map(IntoArg::into_arg)),
            "_pool_dir" => Some(Ok(self._pool_dir.read().clone().into_arg())),
            "_mount_dir" => Some(Ok(self._mount_dir.read().clone().into_arg())),
            "_image_path" => Some(Ok(self._image_path.read().clone().into_arg())),
            "_image_size_bytes" => Some(Ok(Arg::Int(*self._image_size_bytes.read()))),
            _ => None,
        }
    }

    fn __setattr__(&self, _pool: &PoolObject, name: &str, value: Arg) -> Option<Result<()>> {
        match name {
            "_pool_dir" => *self._pool_dir.write() = value.as_str().map(str::to_string),
            "_mount_dir" => *self._mount_dir.write() = value.as_str().map(str::to_string),
            "_image_path" => *self._image_path.write() = value.as_str().map(str::to_string),
            "_image_size_bytes" => {
                *self._image_size_bytes.write() = value.as_i64().unwrap_or(IMAGE_SIZE_BYTES)
            }
            _ => return None,
        }
        Some(Ok(()))
    }

    fn __call_method__(&self, pool: &PoolObject, name: &str, args: Args) -> Option<Result<Arg>> {
        let first = || {
            args.positional
                .first()
                .cloned()
                .unwrap_or(Arg::None)
                .__str__()
        };
        Some(match name {
            "_resolve_pool_dir" => self._resolve_pool_dir(pool).map(IntoArg::into_arg),
            "_resolve_image_path" => self._resolve_image_path(pool).map(IntoArg::into_arg),
            "_resolve_mount_dir" => self._resolve_mount_dir(pool).map(IntoArg::into_arg),
            "_is_mounted" => self._is_mounted(pool, &first()).map(Arg::Bool),
            "_create_image_file" => self._create_image_file(pool).map(|()| Arg::None),
            "_mount_image" => self._mount_image(pool).map(|()| Arg::None),
            "_storage_key" => self._storage_key(pool, &first()).map(IntoArg::into_arg),
            "_logical_key" => self._logical_key(pool, &first()).map(IntoArg::into_arg),
            "_entry_path" => self._entry_path(pool, &first()).map(IntoArg::into_arg),
            "_run_command" => {
                let command: Vec<String> = match args.positional.first() {
                    Some(c) => match c.__iter__() {
                        Ok(items) => items.iter().map(|i| i.__str__()).collect(),
                        Err(e) => return Some(Err(e)),
                    },
                    None => Vec::new(),
                };
                let action = args.get("action").__str__();
                self._run_command(pool, &command, &action)
                    .map(|()| Arg::None)
            }
            _ => return None,
        })
    }

    fn method_names(&self) -> &'static [&'static str] {
        &[
            "close",
            "_resolve_pool_dir",
            "_resolve_image_path",
            "_resolve_mount_dir",
            "_is_mounted",
            "_run_command",
            "_create_image_file",
            "_mount_image",
            "_storage_key",
            "_logical_key",
            "_entry_path",
        ]
    }
}

static CLASS: LazyClass = LazyClass::new(|| {
    pool_class(
        CLASS_NAME,
        "laila.data.filesystem.filesystem",
        alloc::vec![],
        alloc::vec![(
            String::from("transformations"),
            field_default(
                "TransformationSequence | None",
                crate::entry::transformation_base64()
            )
        )],
        alloc::vec![
            (String::from("_pool_dir"), private_default(Arg::None)),
            (String::from("_mount_dir"), private_default(Arg::None)),
            (String::from("_image_path"), private_default(Arg::None)),
            (
                String::from("_image_size_bytes"),
                private_default(Arg::Int(IMAGE_SIZE_BYTES))
            ),
        ],
        alloc::vec![(String::from(_PERSISTENT_STORAGE), Arg::Bool(true))],
    )
});

crate::laila_pool_handle!(
    /// Pool backed by JSON files on a loopback-mounted ext4 image.
    ///
    /// Each entry lives in a single `<urlencoded-key>.json` file inside
    /// the pool's mount directory. Keys are URL-encoded so any character
    /// is safe for use in a filename, and decoded on enumeration via
    /// `_logical_key`.
    ///
    /// The image is created with `_create_image_file` (`truncate` +
    /// `mkfs.ext4`) and mounted via `_mount_image` on first construction.
    /// Subsequent constructions detect the mount via
    /// `/proc/self/mountinfo` and skip both steps.
    ///
    /// The constructor refuses any explicit `image_dir` / `image_path` /
    /// `mount_dir` overrides because the pool computes those itself from
    /// `LAILA_DEFAULT_DIRECTORIES` and its own UUID -- letting callers
    /// override them would silently break the "one image file per pool,
    /// name derived from pool UUID" invariant.
    ///
    /// The default `transformations` is `transformation_base64`, which
    /// gives the JSON serialiser pure-ASCII payloads (avoiding encoding
    /// surprises across mount platforms).
    FilesystemPool,
    FilesystemBackend
);

impl ModelBase for FilesystemPool {
    fn __class__() -> &'static ModelClass {
        FilesystemPool::__class__()
    }
}

impl FilesystemPool {
    /// The class descriptor.
    pub fn __class__() -> &'static ModelClass {
        let c: &'static ModelClass = &CLASS;
        crate::basics::definitions::cli_capable::register_class(c);
        c
    }
    /// `FilesystemPool(**data)` -- validate that reserved path fields are
    /// not overridden, then construct.
    #[pysig("FilesystemPool(**data)")]
    pub fn new(args: impl Into<Args>) -> Result<FilesystemPool> {
        if data.__contains__("image_dir")? {
            return Err(LailaError::value_error(
                "FilesystemPool storage path is fixed and cannot be overridden.",
            ));
        }
        if data.__contains__("image_path")? {
            return Err(LailaError::value_error(
                "FilesystemPool image_path is fixed and cannot be overridden.",
            ));
        }
        if data.__contains__("mount_dir")? {
            return Err(LailaError::value_error(
                "FilesystemPool mount_dir is fixed and cannot be overridden.",
            ));
        }
        Ok(FilesystemPool(Pool::construct(
            Self::__class__(),
            data,
            |_inst: &ModelInstance| Ok(Box::new(FilesystemBackend::default())),
        )?))
    }
    fn backend(&self) -> &FilesystemBackend {
        self.0
            .downcast_ref::<FilesystemBackend>()
            .expect("FilesystemPool backend")
    }
    /// Root directory for this pool's artefacts.
    pub fn pool_dir(&self) -> Result<String> {
        self.backend().pool_dir()
    }
    /// Mount-point directory where entries are stored.
    pub fn mount_dir(&self) -> Result<String> {
        self.backend().mount_dir()
    }
    /// Path to the ext4 image file.
    pub fn image_path(&self) -> Result<String> {
        self.backend().image_path()
    }
    /// `pool.close()` -- no-op; the mount persists beyond pool lifetime.
    pub fn close(&self, _args: impl Into<Args>) -> Result<()> {
        self.0.obj().close()
    }
}

impl crate::python::builtins::CallValue for FilesystemPool {
    fn call_value(&self, args: Args) -> Result<Arg> {
        FilesystemPool::new(args).map(IntoArg::into_arg)
    }
}

/// `FilesystemPool` as a class object (`type(pool)` / module attribute).
pub fn class_object() -> Arg {
    FilesystemPool::__class__().as_pyclass().into_arg()
}

impl PyArg for FilesystemBackend {
    fn to_arg(&self) -> Arg {
        class_object()
    }
}
