//! Context-manager exit dispatch for the `with` rewrite (P27).
//!
//! `with cm:` lowers to `let guard = cm.__enter__()?; body; <exit>`. Most
//! guards release in `Drop` (locks, patches, temp files), but a Python
//! `__exit__` may *raise* -- `laila.guarantee` re-raises the first failed
//! in-scope future -- and a `Drop` cannot. The rewriter therefore ends the
//! block with `guard.__laila_exit()?` resolved by autoref specialisation:
//!
//! - a guard type implementing [`RaisingExit`] (by value) runs its raising
//!   `__exit__` and propagates the error;
//! - every other type falls through to the blanket [`NoopExit`] on `&T`
//!   (a no-op; the guard is then dropped as before).
//!
//! `async with` uses [`RaisingAexit`] / [`NoopAexit`] the same way with an
//! awaited exit.
//!
//! The call is written `(&guard).__laila_exit()`: the receiver is `&G`, so a
//! `RaisingExit for G` (`&self`) matches by value first and the blanket
//! `NoopExit for &&T` only through autoref -- and the guard stays alive to
//! be dropped right after, exactly as before.

use alloc::boxed::Box;
use core::future::Future;
use core::pin::Pin;

use crate::python::Result;

/// A boxed `__aexit__` future.
pub type ExitFut<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

/// A guard whose `__exit__` may raise (runs on the normal exit path).
pub trait RaisingExit {
    fn __laila_exit(&self) -> Result<()>;
}
/// Fallback for every other guard: nothing to do (its `Drop` releases).
pub trait NoopExit {
    fn __laila_exit(self) -> Result<()>;
}
impl<T: ?Sized> NoopExit for &&T {
    #[inline]
    fn __laila_exit(self) -> Result<()> {
        Ok(())
    }
}

/// `with cm() as x:` keeps `x` bound after the block in Python. The guard
/// is dropped at the end of the block (so RAII guards release there) and
/// the name is rebound to a copy when the value is `Clone` (`Arg`, a
/// `py_class!` instance) -- autoref specialisation like [`NoopExit`].
pub trait KeepClone {
    type Kept;
    fn __laila_keep(&self) -> Self::Kept;
}
/// The value a `with .. as x` block bound, wrapped so that the `Clone`
/// probe sees the value's own type (a `&T` is always `Clone`, which would
/// make the fallback ambiguous for a non-`Clone` guard).
pub struct Keep<T>(pub T);
impl<T: Clone> KeepClone for Keep<T> {
    type Kept = T;
    #[inline]
    fn __laila_keep(&self) -> T {
        self.0.clone()
    }
}
/// Fallback for a non-`Clone` guard: the name is rebound to `()`.
pub trait KeepUnit {
    type Kept;
    fn __laila_keep(&self) -> Self::Kept;
}
impl<T> KeepUnit for &Keep<T> {
    type Kept = ();
    #[inline]
    fn __laila_keep(&self) {}
}

/// A guard whose `__aexit__` may raise.
pub trait RaisingAexit {
    fn __laila_aexit(&self) -> ExitFut<'_>;
}
/// Fallback for every other async guard: nothing to do.
pub trait NoopAexit {
    fn __laila_aexit(self) -> ExitFut<'static>;
}
impl<T: ?Sized> NoopAexit for &&T {
    #[inline]
    fn __laila_aexit(self) -> ExitFut<'static> {
        Box::pin(core::future::ready(Ok(())))
    }
}

/// The outcome of a Python-defined `__exit__` (`laila::python!` generates
/// `RaisingExit` for a class that defines one): a raised error propagates;
/// any returned value (`False`, `None`, ...) is the normal exit.
pub trait ExitOutcome {
    fn __laila_exit_outcome(self) -> Result<()>;
}
impl<T> ExitOutcome for Result<T> {
    fn __laila_exit_outcome(self) -> Result<()> {
        self.map(|_| ())
    }
}
impl ExitOutcome for crate::python::Arg {
    fn __laila_exit_outcome(self) -> Result<()> {
        Ok(())
    }
}
impl ExitOutcome for bool {
    fn __laila_exit_outcome(self) -> Result<()> {
        Ok(())
    }
}
impl ExitOutcome for () {
    fn __laila_exit_outcome(self) -> Result<()> {
        Ok(())
    }
}
