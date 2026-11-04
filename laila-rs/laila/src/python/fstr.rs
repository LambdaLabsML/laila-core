//! `f"{x}"` field rendering: Python's `str(x)`.
//!
//! The `laila::python!` rewriter lowers a conversion-free field to
//! `(&&FStr(&x)).__laila_fstr()`, resolved by autoref specialization: an
//! exception (`except E as exc: f"{exc}"`) renders its *message* like
//! `str(exc)`, every other value renders through `Display` (which is
//! already `str(x)` for `Arg` and the scalars).

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use core::fmt::Display;

use super::errors::LailaError;

/// The field value, borrowed.
pub struct FStr<'a, T: ?Sized>(pub &'a T);

/// The specialised arm: picked first (by value) for `&&FStr<LailaError>`.
pub trait FStrExc {
    fn __laila_fstr(&self) -> String;
}
impl FStrExc for &FStr<'_, LailaError> {
    fn __laila_fstr(&self) -> String {
        self.0.__str__()
    }
}
impl FStrExc for &FStr<'_, Arc<LailaError>> {
    fn __laila_fstr(&self) -> String {
        self.0.__str__()
    }
}

/// The general arm: `Display` (reached after one auto-deref).
pub trait FStrDisplay {
    fn __laila_fstr(&self) -> String;
}
impl<T: Display + ?Sized> FStrDisplay for FStr<'_, T> {
    fn __laila_fstr(&self) -> String {
        self.0.to_string()
    }
}
