package laila

import (
	"fmt"
	"math/big"
	"os"
	"path/filepath"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/ip_app/zeromq.py -- ZeroMQ (and
// nanomsg/nng) communication transport.
//
// Brokerless message transport built on the BrokerRPCProtocol_ carrier.
// ZeroMQ has no central broker, so each policy *binds* a ``PULL`` socket
// on a deterministic endpoint derived from its ``global_id`` (its inbox)
// and addresses a peer by ``connect``-ing a ``PUSH`` socket to the
// peer's endpoint. Request/response correlation is handled by the
// carrier.
//
//   - ``protocol_name`` ``"zeromq"`` (aliases ``zmq`` / ``nanomsg`` /
//     ``nng``)
//   - URI scheme ``zmq://<peer_policy_id>``
//   - transport endpoints default to ``ipc://`` under the temp dir; set
//     ``endpoint_scheme="tcp"`` to use TCP instead.
//
// The driver is resolved lazily (registry name ``"zmq"``); a missing
// driver raises a clear, actionable error.

var zeromq_log_ = py.Get_logger("laila.policy.central.communication.protocols.ip_app.zeromq")

// INSTALL_HINT_zmq_ is the capability error for a missing ZeroMQ driver.
const INSTALL_HINT_zmq_ = "The ZeroMQ transport requires pyzmq. Install it with `pip install laila-core[zmq]` (or `pip install pyzmq`)."

// Zmq_driver_ is the ``zmq.asyncio`` contract (``Context()``).
type Zmq_driver_ interface {
	Context() Zmq_context_
}

// Zmq_context_ is a ``zmq.asyncio.Context``.
type Zmq_context_ interface {
	Pull() Zmq_socket_
	Push() Zmq_socket_
	Term()
}

// Zmq_socket_ is a PUSH or PULL socket.
type Zmq_socket_ interface {
	Bind(endpoint string) error
	Connect(endpoint string) error
	Send(data []byte) error
	Recv() ([]byte, error)
	Close(linger int)
}

// LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_ is the ZeroMQ PUSH/PULL
// brokerless transport.
type LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_ struct {
	BrokerRPCProtocol_

	Endpoint_scheme string `py:"endpoint_scheme" default:"ipc"`
	Tcp_host        string `py:"tcp_host" default:"127.0.0.1"`

	Ctx_       Zmq_context_           `py:"_ctx"`
	Zmq_       Zmq_driver_            `py:"_zmq"`
	Pull_      Zmq_socket_            `py:"_pull"`
	Pushes_    map[string]Zmq_socket_ `py:"_pushes"`
	Recv_task_ *py.Task               `py:"_recv_task"`
}

// LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "zeromq"``).
var LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_)(nil),
	BrokerRPCProtocol_class,
).
	Set_classvar("protocol_name", "zeromq").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("zeromq", "zmq", "nanomsg", "nng")).
	Set_classvar("matches_token", Token_aliases_classmethod_("zeromq", "zmq", "nanomsg", "nng")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("zmq://", "zeromq://"))

// New_LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Matches_token accepts ``"zeromq"`` plus aliases.
func (p *LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(p.Class_(), token)
}

// Can_handle_uri claims ``zmq://`` and ``zeromq://`` URIs.
func (p *LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(p.Class_(), uri)
}

// Endpoint_for_ derives the deterministic inbox endpoint of a policy.
func (p *LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_) Endpoint_for_(policy_id any) string {
	digest := py.Sha1_hex([]byte(py.Str(policy_id)))[:16]
	if p.Endpoint_scheme == "ipc" {
		return "ipc://" + filepath.Join(os.TempDir(), fmt.Sprintf("laila_zmq_%s.ipc", digest))
	}
	// tcp fallback uses a deterministic high port from the digest
	n, _ := new(big.Int).SetString(digest, 16)
	port := 20000 + int(new(big.Int).Mod(n, big.NewInt(20000)).Int64())
	return fmt.Sprintf("tcp://%s:%d", p.Tcp_host, port)
}

// Inbox_topic_ is our own inbox endpoint.
func (p *LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_) Inbox_topic_() string {
	return p.Endpoint_for_(p.Policy_id_())
}

// Peer_inbox_ is the peer's inbox endpoint.
func (p *LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_) Peer_inbox_(peer_policy_id string) string {
	return p.Endpoint_for_(peer_policy_id)
}

// Broker_connect_ creates the ZeroMQ context.
func (p *LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_) Broker_connect_() error {
	comm_drivers_mu_.Lock()
	drv, ok := Comm_drivers_["zmq"].(Zmq_driver_)
	comm_drivers_mu_.Unlock()
	if !ok {
		return py.RuntimeError.New(INSTALL_HINT_zmq_).From(py.ImportError.New("No module named 'zmq'"))
	}
	p.Zmq_ = drv
	p.Ctx_ = drv.Context()
	if p.Pushes_ == nil {
		p.Pushes_ = map[string]Zmq_socket_{}
	}
	return nil
}

// Broker_subscribe_ binds a PULL on our own inbox endpoint and consumes
// it.
func (p *LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_) Broker_subscribe_(topic string) error {
	p.Pull_ = p.Ctx_.Pull()
	if err := p.Pull_.Bind(topic); err != nil {
		return err
	}
	task, err := py.Ensure_future(func() (any, error) { return nil, p.Recv_loop_() })
	if err != nil {
		return err
	}
	p.Recv_task_ = task
	return nil
}

// Recv_loop_ pumps inbound frames from the PULL socket.
func (p *LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_) Recv_loop_() error {
	err := func() error {
		for {
			data, err := p.Pull_.Recv()
			if err != nil {
				return err
			}
			p.Feed_message_(data)
		}
	}()
	if err != nil && !py.IsInstance(err, py.CancelledError) {
		zeromq_log_.Debug("ZeroMQ recv loop ended", py.Kw{"exc_info": err})
	}
	return nil
}

// Broker_publish_ sends on a (cached) PUSH socket connected to *topic*.
func (p *LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_) Broker_publish_(topic string, data []byte) error {
	push, ok := p.Pushes_[topic]
	if !ok {
		push = p.Ctx_.Push()
		if err := push.Connect(topic); err != nil {
			return err
		}
		p.Pushes_[topic] = push
	}
	return push.Send(data)
}

// Broker_close_ cancels the receive loop and closes every socket and the
// context.
func (p *LAILA_IDENTIFIABLE_ZEROMQ_COMM_PROTOCOL_) Broker_close_() error {
	if p.Recv_task_ != nil {
		p.Recv_task_.Cancel()
	}
	for _, push := range p.Pushes_ {
		func() {
			defer func() { _ = recover() }()
			push.Close(0)
		}()
	}
	p.Pushes_ = map[string]Zmq_socket_{}
	if p.Pull_ != nil {
		func() {
			defer func() { _ = recover() }()
			p.Pull_.Close(0)
		}()
		p.Pull_ = nil
	}
	if p.Ctx_ != nil {
		func() {
			defer func() { _ = recover() }()
			p.Ctx_.Term()
		}()
		p.Ctx_ = nil
	}
	return nil
}
