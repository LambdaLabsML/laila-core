/**
 * Internal scope-name constants used for deterministic ID generation.
 *
 * The strings defined here are the canonical *scope* names that appear in
 * every laila ``global_id``. A global id is encoded as::
 *
 *   LAILA:scope1:...:scopeN:<uuid>[@evolution=<n>]
 *
 * The leading ``_TOPMOST_SCOPE`` (``LAILA``) is a constant here and the
 * following segments come from each subclass's ``_scopes`` private attribute
 * (e.g. ``_POOL_SCOPE`` for pools, ``_FUTURE_SCOPE`` for futures, ...).
 * Everything after ``@`` is a ``key=value`` attribute list; only ``evolution``
 * is part of identity.
 *
 * These names are also the keys consulted by ``_SCOPE_TO_ARGS_PATH`` (in
 * ``basics/definitions/cli_capable.js``) when injecting parameters from
 * ``laila.args``. *Renaming a value here is a wire-format break*: persisted
 * global ids and serialised environment dumps will no longer round-trip. Add
 * new scopes freely; rename only with a migration plan.
 *
 * Special non-scope constants:
 *
 * - ``_DEFAULT_POOL_NICKNAME`` (``"_memory"``) -- the nickname used by the
 *   default in-memory pool that ships with every fresh policy.
 */

export const _ENTRY_SCOPE = "ENTRY";
export const _TASK_FORCE_SCOPE = "TASK_FORCE";
export const _POLICY_SCOPE = "POLICY";
export const _OBJECT_SCOPE = "OBJECT";
export const _LAILA_SCOPE = "LAILA";
export const _FUTURE_SCOPE = "FUTURE";
export const _GROUP_FUTURE_SCOPE = "GROUP_FUTURE";
export const _COMPLEX_FUTURE_SCOPE = "COMPLEX_FUTURE";
export const _POOL_SCOPE = "POOL";
export const _CENTRAL_COMMAND_SCOPE = "CENTRAL_COMMAND";
export const _CENTRAL_MEMORY_SCOPE = "CENTRAL_MEMORY";
export const _CENTRAL_LOGIC_SCOPE = "CENTRAL_LOGIC";
export const _CENTRAL_COMMUNICATION_SCOPE = "CENTRAL_COMMUNICATION";
export const _POOL_ROUTER_SCOPE = "POOL_ROUTER";
export const _COMM_PROTOCOL_SCOPE = "COMM_PROTOCOL";
export const _MANIFEST_SCOPE = "MANIFEST";
export const _LOGGER_SCOPE = "LOGGER";
export const _DATA_CONTAINER_SCOPE = "DATA_CONTAINER";
export const _MULTI_BUFFER_SCOPE = "MULTI_BUFFER";
export const _POOL_INDEX_SCOPE = "POOL_INDEX";
export const _DEFAULT_POOL_NICKNAME = "_memory";

export const _TOPMOST_SCOPE = "LAILA";
