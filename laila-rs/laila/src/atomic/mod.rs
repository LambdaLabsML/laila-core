//! Thread-safe atomic data types and base definitions.
//!
//! Two layers live here:
//!
//! - [`definitions`] -- mixin base classes that wrap any object with a
//!   reentrant lock (`_LAILA_LOCALLY_ATOMIC_OBJECT`,
//!   `_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT`,
//!   `_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT`). All identifiable objects
//!   in laila that need critical sections inherit from one of these and gain
//!   a uniform `with self.atomic(): ...` context manager.
//! - [`types`] -- thread-safe wrappers around common Python data structures:
//!
//!   | Type           | Wrapper for                                 |
//!   |----------------|---------------------------------------------|
//!   | `AtomicDict`   | `dict`                                      |
//!   | `AtomicList`   | `list`                                      |
//!   | `AtomicStr`    | `str`                                       |
//!   | `AtomicInt`    | `int`                                       |
//!   | `AtomicFlag`   | boolean (single bit, atomically toggled)    |
//!   | `AtomicDotMap` | `dotmap.DotMap`                             |
//!
//!   Each wrapper exposes the same surface as the underlying type but guards
//!   every mutation with an internal `threading.RLock`, so they are safe to
//!   share between worker threads.

pub mod definitions;
pub mod types;

pub use types::atomic_dict::AtomicDict;
pub use types::atomic_dotmap::AtomicDotMap;
pub use types::atomic_flag::AtomicFlag;
pub use types::atomic_int::AtomicInt;
pub use types::atomic_list::AtomicList;
pub use types::atomic_str::AtomicStr;
