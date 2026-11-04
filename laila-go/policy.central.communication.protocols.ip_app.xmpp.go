package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/xmpp.py -- XMPP
// communication transport (broker-mediated).
//
// Carries JSON-RPC over an XMPP server via ``slixmpp``, addressing peers
// by their inbox (a per-policy JID resource). Built on the broker carrier;
// the driver is imported on connect and a missing library raises a clear
// capability error.
//
//   - ``protocol_name`` ``"xmpp"`` (aliases ``jabber``)
//   - URI scheme ``xmpp://<peer_policy_id>``

// Xmpp_driver_ is the ``slixmpp`` contract (``ClientXMPP(jid, password)``).
type Xmpp_driver_ interface {
	ClientXMPP(jid string, password string) Xmpp_client_
}

// Xmpp_client_ is a ``slixmpp.ClientXMPP`` handle.
type Xmpp_client_ interface {
	Connect(host string, port int) error
	Add_event_handler(event string, fn func(body string))
	Send_message(mto string, mbody string) error
	Disconnect() error
}

// LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_ is the XMPP broker-mediated
// transport.
//
// Fields:
//
//   - Jid (default ``""``), Password (default ``""``)
//   - Server_host (default ``"127.0.0.1"``), Server_port (default ``5222``)
type LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_ struct {
	BrokerRPCProtocol_

	Jid         string `py:"jid" default:""`
	Password    string `py:"password" default:""`
	Server_host string `py:"server_host" default:"127.0.0.1"`
	Server_port int    `py:"server_port" default:"5222"`

	Client_ Xmpp_client_ `py:"_client"`
}

// LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "xmpp"``).
var LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_)(nil),
	BrokerRPCProtocol_class,
).
	Set_classvar("protocol_name", "xmpp").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("xmpp", "jabber")).
	Set_classvar("matches_token", Token_aliases_classmethod_("xmpp", "jabber")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("xmpp://"))

// New_LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Matches_token accepts ``"xmpp"`` / ``"jabber"``.
func (p *LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``xmpp://`` URIs.
func (p *LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// Broker_connect_ requires the driver, builds the client and connects.
func (p *LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_) Broker_connect_() error {
	mods, err := p.Require_drivers_([]string{"slixmpp"}, "xmpp")
	if err != nil {
		return err
	}
	drv, ok := mods["slixmpp"].(Xmpp_driver_)
	if !ok {
		return py.RuntimeError.Newf("slixmpp driver does not implement Xmpp_driver_: %T", mods["slixmpp"])
	}
	client := drv.ClientXMPP(p.Jid, p.Password)
	if err := client.Connect(p.Server_host, p.Server_port); err != nil {
		return err
	}
	p.Client_ = client
	return nil
}

// Broker_subscribe_ installs the message handler: XMPP delivers to our
// JID directly, so messages are routed to Feed_message_.
func (p *LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_) Broker_subscribe_(topic string) error {
	if p.Client_ != nil {
		p.Client_.Add_event_handler("message", func(body string) {
			p.Feed_message_(py.Encode_latin1(body))
		})
	}
	return nil
}

// Broker_publish_ sends *data* (latin-1 decoded) to *topic* as a JID.
func (p *LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_) Broker_publish_(topic string, data []byte) error {
	if p.Client_ == nil {
		return py.AttributeError.New("'NoneType' object has no attribute 'send_message'")
	}
	return p.Client_.Send_message(topic, py.Decode_latin1(data))
}

// Broker_close_ disconnects (errors swallowed) and drops the client.
func (p *LAILA_IDENTIFIABLE_XMPP_COMM_PROTOCOL_) Broker_close_() error {
	if p.Client_ != nil {
		func() {
			defer func() { _ = recover() }()
			_ = p.Client_.Disconnect()
		}()
		p.Client_ = nil
	}
	return nil
}
