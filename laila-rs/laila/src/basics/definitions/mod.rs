//! Core base classes for identity and object hierarchies.

pub mod cli_capable;
pub mod identifiable_object;
pub mod laila_object;

pub use cli_capable::{CLIExempt, _LAILA_CLI_CAPABLE_CLASS};
pub use identifiable_object::{Identity, _LAILA_IDENTIFIABLE_OBJECT};
pub use laila_object::{_now_creation_timestamp, _LAILA_OBJECT};
