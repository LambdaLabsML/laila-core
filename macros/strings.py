"""Internal scope-name constants used for deterministic ID generation.

The strings defined here are the canonical *scope* names that appear
in every laila :attr:`global_id`. A global id is encoded as::

    LAILA:scope1:...:scopeN:<uuid>[@evolution=<n>]

The leading :data:`_TOPMOST_SCOPE` (``LAILA``) is a constant here and
the following segments come from each subclass's ``_scopes`` private
attribute (e.g. :data:`_POOL_SCOPE` for pools,
:data:`_FUTURE_SCOPE` for futures, ...). Everything after ``@`` is a
``key=value`` attribute list; only ``evolution`` is part of identity.

These names are also the keys consulted by
:data:`_SCOPE_TO_ARGS_PATH` (in :mod:`basics.definitions.cli_capable`)
when injecting parameters from :data:`laila.args`. *Renaming a value
here is a wire-format break*: persisted global ids and serialised
environment dumps will no longer round-trip. Add new scopes freely;
rename only with a migration plan.

Special non-scope constants:

- :data:`_DEFAULT_POOL_NICKNAME` (``"_memory"``) -- the nickname
  used by the default in-memory pool that ships with every fresh
  policy.
"""

_ENTRY_SCOPE = "ENTRY"
_TASK_FORCE_SCOPE = "TASK_FORCE"
_POLICY_SCOPE = "POLICY"
_OBJECT_SCOPE = "OBJECT"
_LAILA_SCOPE = "LAILA"
_FUTURE_SCOPE = "FUTURE"
_GROUP_FUTURE_SCOPE = "GROUP_FUTURE"
_COMPLEX_FUTURE_SCOPE = "COMPLEX_FUTURE"
_POOL_SCOPE = "POOL"
_CENTRAL_COMMAND_SCOPE = "CENTRAL_COMMAND"
_CENTRAL_MEMORY_SCOPE = "CENTRAL_MEMORY"
_CENTRAL_LOGIC_SCOPE = "CENTRAL_LOGIC"
_CENTRAL_COMMUNICATION_SCOPE = "CENTRAL_COMMUNICATION"
_POOL_ROUTER_SCOPE = "POOL_ROUTER"
_COMM_PROTOCOL_SCOPE = "COMM_PROTOCOL"
_MANIFEST_SCOPE = "MANIFEST"
_LOGGER_SCOPE = "LOGGER"
_DATA_CONTAINER_SCOPE = "DATA_CONTAINER"
_MULTI_BUFFER_SCOPE = "MULTI_BUFFER"
_POOL_INDEX_SCOPE = "POOL_INDEX"
_DEFAULT_POOL_NICKNAME = "_memory"

_TOPMOST_SCOPE = "LAILA"
