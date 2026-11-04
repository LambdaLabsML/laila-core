//! Internal scope-name constants used for deterministic ID generation.
//!
//! The strings defined here are the canonical *scope* names that appear in
//! every laila `global_id`. A global id is encoded as:
//!
//! ```text
//! LAILA:scope1:...:scopeN:<uuid>[@evolution=<n>]
//! ```
//!
//! The leading [`_TOPMOST_SCOPE`] (`LAILA`) is a constant here and the
//! following segments come from each subclass's `_scopes` private attribute
//! (e.g. [`_POOL_SCOPE`] for pools, [`_FUTURE_SCOPE`] for futures, ...).
//! Everything after `@` is a `key=value` attribute list; only `evolution`
//! is part of identity.
//!
//! These names are also the keys consulted by `_SCOPE_TO_ARGS_PATH` (in
//! `basics::definitions::cli_capable`) when injecting parameters from
//! `laila.args`. *Renaming a value here is a wire-format break*: persisted
//! global ids and serialised environment dumps will no longer round-trip.
//! Add new scopes freely; rename only with a migration plan.
//!
//! Special non-scope constants:
//!
//! - [`_DEFAULT_POOL_NICKNAME`] (`"_memory"`) -- the nickname used by the
//!   default in-memory pool that ships with every fresh policy.

pub const _ENTRY_SCOPE: &str = "ENTRY";
pub const _TASK_FORCE_SCOPE: &str = "TASK_FORCE";
pub const _POLICY_SCOPE: &str = "POLICY";
pub const _OBJECT_SCOPE: &str = "OBJECT";
pub const _LAILA_SCOPE: &str = "LAILA";
pub const _FUTURE_SCOPE: &str = "FUTURE";
pub const _GROUP_FUTURE_SCOPE: &str = "GROUP_FUTURE";
pub const _COMPLEX_FUTURE_SCOPE: &str = "COMPLEX_FUTURE";
pub const _POOL_SCOPE: &str = "POOL";
pub const _CENTRAL_COMMAND_SCOPE: &str = "CENTRAL_COMMAND";
pub const _CENTRAL_MEMORY_SCOPE: &str = "CENTRAL_MEMORY";
pub const _CENTRAL_LOGIC_SCOPE: &str = "CENTRAL_LOGIC";
pub const _CENTRAL_COMMUNICATION_SCOPE: &str = "CENTRAL_COMMUNICATION";
pub const _POOL_ROUTER_SCOPE: &str = "POOL_ROUTER";
pub const _COMM_PROTOCOL_SCOPE: &str = "COMM_PROTOCOL";
pub const _MANIFEST_SCOPE: &str = "MANIFEST";
pub const _LOGGER_SCOPE: &str = "LOGGER";
pub const _DATA_CONTAINER_SCOPE: &str = "DATA_CONTAINER";
pub const _MULTI_BUFFER_SCOPE: &str = "MULTI_BUFFER";
pub const _POOL_INDEX_SCOPE: &str = "POOL_INDEX";
pub const _DEFAULT_POOL_NICKNAME: &str = "_memory";

pub const _TOPMOST_SCOPE: &str = "LAILA";

/// `__doc__` of the Python module.
pub const __doc__: &str = "Internal scope-name constants used for deterministic ID generation.";
/// `__name__` of the Python module.
pub const __name__: &str = "laila.macros.strings";

/// Every public constant of the module as `(name, value)`, in source order
/// -- what `dir(strings)` / `getattr(strings, name)` enumerate.
pub const MEMBERS: &[(&str, &str)] = &[
    ("_ENTRY_SCOPE", _ENTRY_SCOPE),
    ("_TASK_FORCE_SCOPE", _TASK_FORCE_SCOPE),
    ("_POLICY_SCOPE", _POLICY_SCOPE),
    ("_OBJECT_SCOPE", _OBJECT_SCOPE),
    ("_LAILA_SCOPE", _LAILA_SCOPE),
    ("_FUTURE_SCOPE", _FUTURE_SCOPE),
    ("_GROUP_FUTURE_SCOPE", _GROUP_FUTURE_SCOPE),
    ("_COMPLEX_FUTURE_SCOPE", _COMPLEX_FUTURE_SCOPE),
    ("_POOL_SCOPE", _POOL_SCOPE),
    ("_CENTRAL_COMMAND_SCOPE", _CENTRAL_COMMAND_SCOPE),
    ("_CENTRAL_MEMORY_SCOPE", _CENTRAL_MEMORY_SCOPE),
    ("_CENTRAL_LOGIC_SCOPE", _CENTRAL_LOGIC_SCOPE),
    ("_CENTRAL_COMMUNICATION_SCOPE", _CENTRAL_COMMUNICATION_SCOPE),
    ("_POOL_ROUTER_SCOPE", _POOL_ROUTER_SCOPE),
    ("_COMM_PROTOCOL_SCOPE", _COMM_PROTOCOL_SCOPE),
    ("_MANIFEST_SCOPE", _MANIFEST_SCOPE),
    ("_LOGGER_SCOPE", _LOGGER_SCOPE),
    ("_DATA_CONTAINER_SCOPE", _DATA_CONTAINER_SCOPE),
    ("_MULTI_BUFFER_SCOPE", _MULTI_BUFFER_SCOPE),
    ("_POOL_INDEX_SCOPE", _POOL_INDEX_SCOPE),
    ("_DEFAULT_POOL_NICKNAME", _DEFAULT_POOL_NICKNAME),
    ("_TOPMOST_SCOPE", _TOPMOST_SCOPE),
];
