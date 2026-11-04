package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/mqtt.py -- MQTT (and
// MQTT-SN) communication transport.
//
// Broker-mediated JSON-RPC built on the BrokerRPCProtocol_ carrier. Each
// policy subscribes to its inbox topic ``laila/inbox/<policy_id>`` on
// the MQTT broker; peers address each other by publishing to those
// topics.
//
//   - ``protocol_name`` ``"mqtt"`` (aliases ``mqtts`` / ``mqtt-sn``)
//   - URI scheme ``mqtt://<peer_policy_id>`` (the broker itself is given
//     by ``broker_host`` / ``broker_port``)
//
// The client driver is resolved lazily (registry name
// ``"paho.mqtt.client"``); a missing driver raises a clear, actionable
// error and a missing broker surfaces as a connection error.

var mqtt_log_ = py.Get_logger("laila.policy.central.communication.protocols.ip_app.mqtt")

// INSTALL_HINT_mqtt_ is the capability error for a missing MQTT driver.
const INSTALL_HINT_mqtt_ = "The MQTT transport requires paho-mqtt. Install it with `pip install laila-core[mqtt]` (or `pip install paho-mqtt`)."

// Mqtt_driver_ is the ``paho.mqtt.client`` contract (``Client()``).
type Mqtt_driver_ interface {
	Client() Mqtt_client_
}

// Mqtt_client_ is a ``paho.mqtt.client.Client`` handle.
type Mqtt_client_ interface {
	Set_on_message(fn func(payload []byte))
	Connect(host string, port int) error
	Loop_start()
	Loop_stop()
	Disconnect() error
	Subscribe(topic string) error
	Publish(topic string, data []byte) error
}

// LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_ is the MQTT broker-mediated
// transport.
//
// Fields:
//
//   - Broker_host (default ``"127.0.0.1"``) -- MQTT broker hostname.
//   - Broker_port (default ``1883``) -- MQTT broker port.
type LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_ struct {
	BrokerRPCProtocol_

	Broker_host string `py:"broker_host" default:"127.0.0.1"`
	Broker_port int    `py:"broker_port" default:"1883"`

	Client_ Mqtt_client_ `py:"_client"`
}

// LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "mqtt"``).
var LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_)(nil),
	BrokerRPCProtocol_class,
).
	Set_classvar("protocol_name", "mqtt").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("mqtt", "mqtts", "mqtt-sn")).
	Set_classvar("matches_token", Token_aliases_classmethod_("mqtt", "mqtts", "mqtt-sn")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("mqtt://", "mqtts://"))

// New_LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Matches_token accepts ``"mqtt"`` plus aliases.
func (p *LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``mqtt://`` and ``mqtts://`` URIs.
func (p *LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// Broker_connect_ creates the client and connects to the broker.
func (p *LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_) Broker_connect_() error {
	comm_drivers_mu_.Lock()
	drv, ok := Comm_drivers_["paho.mqtt.client"].(Mqtt_driver_)
	comm_drivers_mu_.Unlock()
	if !ok {
		return py.RuntimeError.New(INSTALL_HINT_mqtt_).From(py.ImportError.New("No module named 'paho'"))
	}
	client := drv.Client()
	client.Set_on_message(func(payload []byte) { p.Feed_message_(payload) })
	if err := client.Connect(p.Broker_host, p.Broker_port); err != nil {
		return py.ConnectionError.Newf("Could not reach MQTT broker at %s:%d: %s", p.Broker_host, p.Broker_port, err.Error()).From(err)
	}
	client.Loop_start()
	p.Client_ = client
	return nil
}

// Broker_subscribe_ subscribes the inbox topic.
func (p *LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_) Broker_subscribe_(topic string) error {
	return p.Client_.Subscribe(topic)
}

// Broker_publish_ publishes *data* to *topic*.
func (p *LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_) Broker_publish_(topic string, data []byte) error {
	return p.Client_.Publish(topic, data)
}

// Broker_close_ stops the network loop and disconnects.
func (p *LAILA_IDENTIFIABLE_MQTT_COMM_PROTOCOL_) Broker_close_() error {
	if p.Client_ != nil {
		func() {
			defer func() { _ = recover() }()
			p.Client_.Loop_stop()
			_ = p.Client_.Disconnect()
		}()
		p.Client_ = nil
	}
	return nil
}

var _ = mqtt_log_
