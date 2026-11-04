package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/lpwan/sigfox.py -- Sigfox LPWAN transport (serial AT modem). Uplinks are tiny (12 bytes) and rate-limited (~140 msgs/day); the caller keeps payloads within those limits.
//
//   - ``protocol_name`` ``"sigfox"`` (aliases: sigfox)
//   - URI scheme(s): sigfox://

// LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_ is the Sigfox LPWAN transport (serial AT modem). Uplinks are tiny (12 bytes) and rate-limited (~140 msgs/day); the caller keeps payloads within those limits.
type LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "sigfox"``).
var LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "sigfox").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("sigfox")).
	Set_classvar("matches_token", Token_aliases_classmethod_("sigfox")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("sigfox://"))

// New_LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_SIGFOX_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}
