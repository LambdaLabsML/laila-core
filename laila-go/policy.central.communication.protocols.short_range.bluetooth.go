package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/short_range/bluetooth.py -- Bluetooth communication
// protocol (scaffold / planned transport).
//
// Bluetooth provides short-range, IP-less peer-to-peer links between nearby devices. A real implementation should build on the P2PStreamRPCProtocol_ carrier: use an RFCOMM (Bluetooth Classic) socket or a GATT characteristic (BLE) as the byte stream, and map a device address (bt://<MAC>) to a peer global_id via the shared peer.connect handshake.
//
// What lands here so the rest of laila can already "see" Bluetooth:
//
//   - ``protocol_name`` ``"bluetooth"`` and the ``bt://`` URI scheme, so
//     ``laila.request(peer, comm_protocol="bluetooth")`` and URI-based peering
//     resolve to this class once a real driver exists.
//   - The full method surface of LAILA_IDENTIFIABLE_COMM_PROTOCOL_,
//     documenting the contract a Bluetooth driver must satisfy.

// NOT_IMPLEMENTED_bluetooth_ is the message every Bluetooth transport method raises.
const NOT_IMPLEMENTED_bluetooth_ = "The Bluetooth transport is a planned protocol and is not implemented yet. " +
	"Use the TCP/IP protocol (comm_protocol='tcpip') for now."

// LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_ is the planned Bluetooth transport (scaffold; raises until
// implemented).
//
// Subclass of LAILA_IDENTIFIABLE_COMM_PROTOCOL_ reserving the ``"bluetooth"``
// token and ``bt://`` URI scheme. All transport methods raise
// NotImplementedError for now -- the class exists so transport selection
// (``comm_protocol="bluetooth"``) and URI routing have a real target to
// resolve to.
type LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_COMM_PROTOCOL_
}

// LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_class is the class descriptor (``protocol_name = "bluetooth"``).
var LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "bluetooth").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("bluetooth", "bt", "ble")).
	Set_classvar("matches_token", Token_aliases_classmethod_("bluetooth", "bt", "ble")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("bt://"))

// New_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_ is ``_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Matches_token accepts the protocol token (plus aliases, if any).
func (p *LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``bt://`` URIs for this transport.
func (p *LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// Start brings up the Bluetooth link. Not implemented yet.
func (p *LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_) Start() error {
	return py.NotImplementedError.New(NOT_IMPLEMENTED_bluetooth_)
}

// Stop tears down the Bluetooth link. No-op (nothing is ever started).
//
// ``stop`` must be idempotent and safe per the base contract, so the
// scaffold returns cleanly rather than raising.
func (p *LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_) Stop() error { return nil }

// Connect peers with a remote node over Bluetooth. Not implemented yet.
func (p *LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_) Connect(uri string, secret string) (string, error) {
	return "", py.NotImplementedError.New(NOT_IMPLEMENTED_bluetooth_)
}

// Send_rpc sends an RPC frame over Bluetooth. Not implemented yet.
func (p *LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_) Send_rpc(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	return nil, py.NotImplementedError.New(NOT_IMPLEMENTED_bluetooth_)
}

// Has_peer reports false: no Bluetooth peers are ever held by this scaffold.
func (p *LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL_) Has_peer(peer_id string) bool { return false }
