package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/grpc.py -- gRPC bidirectional-stream transport (JSON-RPC frames over a bidi gRPC stream via grpcio).
//
//   - ``protocol_name`` ``"grpc"`` (aliases: grpc)
//   - URI scheme(s): grpc://
//
// Built on the point-to-point stream carrier; the driver is resolved on
// start and a missing driver raises a clear capability error.

// LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_ is the gRPC bidirectional-stream transport (JSON-RPC frames over a bidi gRPC stream via grpcio).
type LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_ struct {
	P2PStreamRPCProtocol_

	Target string `py:"target" default:"127.0.0.1:50051"`
}

// LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "grpc"``).
var LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_)(nil),
	P2PStreamRPCProtocol_class,
).
	Set_classvar("protocol_name", "grpc").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("grpc")).
	Set_classvar("matches_token", Token_aliases_classmethod_("grpc")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("grpc://"))

// New_LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Open_stream_ requires the driver, then reports the configuration the
// transport still needs before peering.
func (p *LAILA_IDENTIFIABLE_GRPC_COMM_PROTOCOL_) Open_stream_() (*py.StreamReader, Stream_writer_, error) {
	if _, err := p.Require_drivers_([]string{"grpc"}, "grpc"); err != nil {
		return nil, nil, err
	}
	return nil, nil, py.RuntimeError.New("gRPC transport requires a generated bidi-streaming service stub; configure the gRPC channel/target before peering.")
}
