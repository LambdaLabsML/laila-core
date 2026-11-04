package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/lpwan/lora.py -- LoRa communication
// protocol (scaffold / planned transport).
//
// LoRa is a long-range, low-bandwidth radio link well suited to peer-to-peer policy gossip across kilometres where no IP network exists. A real implementation should build on the DatagramRPCProtocol_ carrier (fragmentation + ack/retry already provided) and supply a SX127x/SX126x modem endpoint (spidev) plus a node-address -> peer global_id mapping via the shared peer.connect handshake.
//
// What lands here so the rest of laila can already "see" LoRa:
//
//   - ``protocol_name`` ``"lora"`` and the ``lora://`` URI scheme, so
//     ``laila.request(peer, comm_protocol="lora")`` and URI-based peering
//     resolve to this class once a real driver exists.
//   - The full method surface of LAILA_IDENTIFIABLE_COMM_PROTOCOL_,
//     documenting the contract a LoRa driver must satisfy.

// NOT_IMPLEMENTED_lora_ is the message every LoRa transport method raises.
const NOT_IMPLEMENTED_lora_ = "The LoRa transport is a planned protocol and is not implemented yet. " +
	"Use the TCP/IP protocol (comm_protocol='tcpip') for now."

// LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_ is the planned LoRa transport (scaffold; raises until
// implemented).
//
// Subclass of LAILA_IDENTIFIABLE_COMM_PROTOCOL_ reserving the ``"lora"``
// token and ``lora://`` URI scheme. All transport methods raise
// NotImplementedError for now -- the class exists so transport selection
// (``comm_protocol="lora"``) and URI routing have a real target to
// resolve to.
type LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "lora"``).
var LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "lora").
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("lora://"))

// New_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Matches_token accepts the protocol token (plus aliases, if any).
func (p *LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``lora://`` URIs for this transport.
func (p *LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// Start brings up the LoRa link. Not implemented yet.
func (p *LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_) Start() error {
	return py.NotImplementedError.New(NOT_IMPLEMENTED_lora_)
}

// Stop tears down the LoRa link. No-op (nothing is ever started).
//
// ``stop`` must be idempotent and safe per the base contract, so the
// scaffold returns cleanly rather than raising.
func (p *LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_) Stop() error { return nil }

// Connect peers with a remote node over LoRa. Not implemented yet.
func (p *LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_) Connect(uri string, secret string) (string, error) {
	return "", py.NotImplementedError.New(NOT_IMPLEMENTED_lora_)
}

// Send_rpc sends an RPC frame over LoRa. Not implemented yet.
func (p *LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_) Send_rpc(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	return nil, py.NotImplementedError.New(NOT_IMPLEMENTED_lora_)
}

// Has_peer reports false: no LoRa peers are ever held by this scaffold.
func (p *LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL_) Has_peer(peer_id string) bool { return false }
