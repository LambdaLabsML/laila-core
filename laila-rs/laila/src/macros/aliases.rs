//! Convenience aliases re-exported at the `laila` package level.
//!
//! These short names are the public entry points users actually reach for
//! in everyday code:
//!
//! - [`constant`] -- alias for `Entry.constant`. Build an unversioned entry
//!   whose identity does not change as the payload is rewritten.
//! - [`variable`] -- alias for `Entry.variable`. Build an evolution-tracking
//!   entry whose identity bumps on each rewrite.
//! - [`contingent`] -- alias for `Entry.contingent`. Escape hatch that
//!   forwards raw keyword arguments straight to the `Entry` constructor,
//!   bypassing the consistency checks done by `Entry.constant` /
//!   `Entry.variable`.
//! - [`future`] -- alias for `Future`. Lower-case spelling for the common
//!   case of "I want a Future-type annotation in user code".

use crate::entry::Entry;
use crate::policy::central::command::Future;
use crate::python::{Args, Result};

/// `constant = Entry.constant`
pub fn constant(args: impl Into<Args>) -> Result<Entry> {
    Entry::constant(args)
}

/// `variable = Entry.variable`
pub fn variable(args: impl Into<Args>) -> Result<Entry> {
    Entry::variable(args)
}

/// `contingent = Entry.contingent`
pub fn contingent(args: impl Into<Args>) -> Result<Entry> {
    Entry::contingent(args)
}

/// `future = Future`
#[allow(non_camel_case_types)]
pub type future = Future;

/// `__name__` of the Python module.
pub const __name__: &str = "laila.macros.aliases";
/// `__doc__` of the Python module.
pub const __doc__: &str = "Convenience aliases re-exported at the ``laila`` package level.";
