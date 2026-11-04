package laila

// macros/strings.py -- internal scope-name constants used for deterministic
// ID generation.
//
// A global id is encoded as ``LAILA:scope1:...:scopeN:<uuid>[@evolution=<n>]``.
// The leading TOPMOST_SCOPE_ ("LAILA") is a constant here and the following
// segments come from each subclass's ``_scopes`` private attribute. Renaming
// a value here is a wire-format break.

const (
	ENTRY_SCOPE_                 = "ENTRY"
	TASK_FORCE_SCOPE_            = "TASK_FORCE"
	POLICY_SCOPE_                = "POLICY"
	OBJECT_SCOPE_                = "OBJECT"
	LAILA_SCOPE_                 = "LAILA"
	FUTURE_SCOPE_                = "FUTURE"
	GROUP_FUTURE_SCOPE_          = "GROUP_FUTURE"
	COMPLEX_FUTURE_SCOPE_        = "COMPLEX_FUTURE"
	POOL_SCOPE_                  = "POOL"
	CENTRAL_COMMAND_SCOPE_       = "CENTRAL_COMMAND"
	CENTRAL_MEMORY_SCOPE_        = "CENTRAL_MEMORY"
	CENTRAL_LOGIC_SCOPE_         = "CENTRAL_LOGIC"
	CENTRAL_COMMUNICATION_SCOPE_ = "CENTRAL_COMMUNICATION"
	POOL_ROUTER_SCOPE_           = "POOL_ROUTER"
	COMM_PROTOCOL_SCOPE_         = "COMM_PROTOCOL"
	MANIFEST_SCOPE_              = "MANIFEST"
	// Sub-scope of MANIFEST: a manifest whose leaves include other manifests.
	COMPLEX_MANIFEST_SCOPE_ = "COMPLEX"
	LOGGER_SCOPE_           = "LOGGER"
	DATA_CONTAINER_SCOPE_        = "DATA_CONTAINER"
	MULTI_BUFFER_SCOPE_          = "MULTI_BUFFER"
	POOL_INDEX_SCOPE_            = "POOL_INDEX"
	DEFAULT_POOL_NICKNAME_       = "_memory"

	TOPMOST_SCOPE_ = "LAILA"
)

// Macros_strings is ``dir(laila.macros.strings)`` restricted to the string
// constants: Python name -> value. Tests iterate it exactly like they walk
// the module namespace.
var Macros_strings = map[string]string{
	"_ENTRY_SCOPE":                 ENTRY_SCOPE_,
	"_TASK_FORCE_SCOPE":            TASK_FORCE_SCOPE_,
	"_POLICY_SCOPE":                POLICY_SCOPE_,
	"_OBJECT_SCOPE":                OBJECT_SCOPE_,
	"_LAILA_SCOPE":                 LAILA_SCOPE_,
	"_FUTURE_SCOPE":                FUTURE_SCOPE_,
	"_GROUP_FUTURE_SCOPE":          GROUP_FUTURE_SCOPE_,
	"_COMPLEX_FUTURE_SCOPE":        COMPLEX_FUTURE_SCOPE_,
	"_POOL_SCOPE":                  POOL_SCOPE_,
	"_CENTRAL_COMMAND_SCOPE":       CENTRAL_COMMAND_SCOPE_,
	"_CENTRAL_MEMORY_SCOPE":        CENTRAL_MEMORY_SCOPE_,
	"_CENTRAL_LOGIC_SCOPE":         CENTRAL_LOGIC_SCOPE_,
	"_CENTRAL_COMMUNICATION_SCOPE": CENTRAL_COMMUNICATION_SCOPE_,
	"_POOL_ROUTER_SCOPE":           POOL_ROUTER_SCOPE_,
	"_COMM_PROTOCOL_SCOPE":         COMM_PROTOCOL_SCOPE_,
	"_MANIFEST_SCOPE":              MANIFEST_SCOPE_,
	"_COMPLEX_MANIFEST_SCOPE":      COMPLEX_MANIFEST_SCOPE_,
	"_LOGGER_SCOPE":                LOGGER_SCOPE_,
	"_DATA_CONTAINER_SCOPE":        DATA_CONTAINER_SCOPE_,
	"_MULTI_BUFFER_SCOPE":          MULTI_BUFFER_SCOPE_,
	"_POOL_INDEX_SCOPE":            POOL_INDEX_SCOPE_,
	"_DEFAULT_POOL_NICKNAME":       DEFAULT_POOL_NICKNAME_,
	"_TOPMOST_SCOPE":               TOPMOST_SCOPE_,
}
