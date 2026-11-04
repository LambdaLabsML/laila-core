package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/amqp.py -- AMQP
// (RabbitMQ) communication transport.
//
// Broker-mediated JSON-RPC built on the BrokerRPCProtocol_ carrier via
// an ``aio-pika``-like driver. Each policy declares an auto-delete inbox
// queue named ``laila/inbox/<policy_id>`` and addresses peers by
// publishing to their queue through the default exchange.
//
//   - ``protocol_name`` ``"amqp"`` (aliases ``amqps`` / ``rabbitmq``)
//   - URI scheme ``amqp://<peer_policy_id>`` (the broker is configured
//     via ``broker_url``)
//
// The driver is resolved lazily (registry name ``"aio_pika"``); a
// missing driver raises a clear, actionable error and an unreachable
// broker surfaces as a connection error.

var amqp_log_ = py.Get_logger("laila.policy.central.communication.protocols.ip_app.amqp")

// INSTALL_HINT_amqp_ is the capability error for a missing AMQP driver.
const INSTALL_HINT_amqp_ = "The AMQP transport requires aio-pika. Install it with `pip install laila-core[amqp]` (or `pip install aio-pika`)."

// Amqp_driver_ is the ``aio_pika`` contract (``connect_robust(url)``).
type Amqp_driver_ interface {
	Connect_robust(url string) (Amqp_connection_, error)
}

// Amqp_connection_ is an ``aio_pika`` robust connection.
type Amqp_connection_ interface {
	Channel() (Amqp_channel_, error)
	Close() error
}

// Amqp_channel_ is an ``aio_pika`` channel: declare + consume a queue,
// publish through the default exchange.
type Amqp_channel_ interface {
	Declare_queue(name string, auto_delete bool) (Amqp_queue_, error)
	Publish(body []byte, routing_key string) error
}

// Amqp_queue_ is a declared queue.
type Amqp_queue_ interface {
	Consume(fn func(body []byte)) error
}

// LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_ is the AMQP/RabbitMQ
// broker-mediated transport.
//
// Fields:
//
//   - Broker_url (default ``"amqp://guest:guest@127.0.0.1/"``) --
//     connection URL for the AMQP broker.
type LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_ struct {
	BrokerRPCProtocol_

	Broker_url string `py:"broker_url" default:"amqp://guest:guest@127.0.0.1/"`

	Conn_    Amqp_connection_ `py:"_conn"`
	Channel_ Amqp_channel_    `py:"_channel"`
}

// LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "amqp"``).
var LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_)(nil),
	BrokerRPCProtocol_class,
).
	Set_classvar("protocol_name", "amqp").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("amqp", "amqps", "rabbitmq")).
	Set_classvar("matches_token", Token_aliases_classmethod_("amqp", "amqps", "rabbitmq")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("amqp://", "amqps://"))

// New_LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Matches_token accepts ``"amqp"`` plus aliases.
func (p *LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``amqp://`` and ``amqps://`` URIs.
func (p *LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// Broker_connect_ opens the robust connection and a channel.
func (p *LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_) Broker_connect_() error {
	comm_drivers_mu_.Lock()
	drv, ok := Comm_drivers_["aio_pika"].(Amqp_driver_)
	comm_drivers_mu_.Unlock()
	if !ok {
		return py.RuntimeError.New(INSTALL_HINT_amqp_).From(py.ImportError.New("No module named 'aio_pika'"))
	}
	conn, err := drv.Connect_robust(p.Broker_url)
	if err != nil {
		return py.ConnectionError.Newf("Could not reach AMQP broker at %s: %s", p.Broker_url, err.Error()).From(err)
	}
	p.Conn_ = conn
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	p.Channel_ = ch
	return nil
}

// Broker_subscribe_ declares the auto-delete inbox queue and consumes
// it.
func (p *LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_) Broker_subscribe_(topic string) error {
	queue, err := p.Channel_.Declare_queue(topic, true)
	if err != nil {
		return err
	}
	return queue.Consume(func(body []byte) { p.Feed_message_(body) })
}

// Broker_publish_ publishes through the default exchange.
func (p *LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_) Broker_publish_(topic string, data []byte) error {
	return p.Channel_.Publish(data, topic)
}

// Broker_close_ closes the connection (bounded to 2 s).
func (p *LAILA_IDENTIFIABLE_AMQP_COMM_PROTOCOL_) Broker_close_() error {
	if p.Conn_ != nil {
		conn := p.Conn_
		_, _ = py.Wait_for(func() (any, error) { return nil, conn.Close() }, 2.0)
		p.Conn_ = nil
		p.Channel_ = nil
	}
	return nil
}

var _ = amqp_log_
