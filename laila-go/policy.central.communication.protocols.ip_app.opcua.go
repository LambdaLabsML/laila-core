package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/opcua.py -- OPC-UA (industrial) transport (JSON-RPC over an OPC-UA secure channel via asyncua).
//
//   - ``protocol_name`` ``"opcua"`` (aliases: opcua,opc.tcp,opc-ua)
//   - URI scheme(s): opc.tcp://
//
// Built on the point-to-point stream carrier; the driver is resolved on
// start and a missing driver raises a clear capability error.

// LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_ is the OPC-UA (industrial) transport (JSON-RPC over an OPC-UA secure channel via asyncua).
type LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_ struct {
	P2PStreamRPCProtocol_

	Endpoint string `py:"endpoint" default:"opc.tcp://127.0.0.1:4840"`
}

// LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "opcua"``).
var LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_)(nil),
	P2PStreamRPCProtocol_class,
).
	Set_classvar("protocol_name", "opcua").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("opcua", "opc.tcp", "opc-ua")).
	Set_classvar("matches_token", Token_aliases_classmethod_("opcua", "opc.tcp", "opc-ua")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("opc.tcp://"))

// New_LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Open_stream_ requires the driver, then reports the configuration the
// transport still needs before peering.
func (p *LAILA_IDENTIFIABLE_OPCUA_COMM_PROTOCOL_) Open_stream_() (*py.StreamReader, Stream_writer_, error) {
	if _, err := p.Require_drivers_([]string{"asyncua"}, "opcua"); err != nil {
		return nil, nil, err
	}
	return nil, nil, py.RuntimeError.New("OPC-UA transport requires an OPC-UA server endpoint exposing a byte-stream method/variable mailbox; configure the server endpoint.")
}
