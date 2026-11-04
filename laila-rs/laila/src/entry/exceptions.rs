//! Exceptions raised by the `Entry` subsystem.
//!
//! `EntryNotBuiltError` is raised when `Entry.data` is accessed on an
//! unbuilt entry. An entry is *unbuilt* when it has a constitution attached
//! but no payload yet. The user must explicitly materialize it (e.g. via
//! `laila.build(entry).wait()` or `laila.remember(...)`) before its data
//! can be read.
//!
//! The class is a `RuntimeError` subclass: `except RuntimeError` catches it
//! (`ExcKind::EntryNotBuiltError.base() == RuntimeError`).

pub use crate::python::errors::EntryNotBuiltError;
