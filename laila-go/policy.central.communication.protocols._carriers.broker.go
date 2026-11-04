package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/_carriers/broker.py -- broker /
// pub-sub mediated RPC carrier.
//
// BrokerRPCProtocol_ carries JSON-RPC over a message broker or pub/sub
// fabric (MQTT, AMQP, XMPP, DDS/RTPS, ZeroMQ, ...). There is no
// point-to-point socket: instead every policy subscribes to its own
// *inbox* topic and addresses a peer by publishing to the peer's inbox.
//
// Addressing & correlation:
//
//   - Each endpoint owns an inbox topic ``laila/inbox/<policy_global_id>``.
//   - A request carries a ``reply_to`` (the sender's inbox) so the
//     responder knows where to publish the reply; request/response are
//     then correlated by the JSON-RPC ``id`` via the inherited
//     pending-RPC table.
//   - Peering publishes a ``peer.connect`` to the target inbox; the
//     acceptor records ``peer_id -> reply_to`` and replies.
//
// Concrete transports supply the broker plumbing: Broker_connect_,
// Broker_subscribe_, Broker_publish_ and Broker_close_. Inbound messages
// are delivered by calling Feed_message_ (the carrier figures out request
// vs response).

var broker_carrier_log_ = py.Get_logger("laila.policy.central.communication.protocols._carriers.broker")

// Broker_hooks_ are the broker plumbing hooks a concrete transport
// overrides.
type Broker_hooks_ interface {
	Broker_connect_() error
	Broker_subscribe_(topic string) error
	Broker_publish_(topic string, data []byte) error
	Broker_close_() error
	Inbox_topic_() string
	Peer_inbox_(peer_policy_id string) string
}

// BrokerRPCProtocol_ is the carrier for broker / pub-sub fabrics.
//
// Like the other carriers it owns a dedicated event loop on a daemon
// thread; the concrete broker client is created inside that loop.
type BrokerRPCProtocol_ struct {
	CarrierRPCProtocol_

	Inbox_             *string                    `py:"_inbox"`
	Handshake_pending_ map[string]*handshakeSlot_ `py:"_handshake_pending"`
}

// BrokerRPCProtocol_class is the class descriptor.
var BrokerRPCProtocol_class = py.NewClass(
	"_BrokerRPCProtocol", (*BrokerRPCProtocol_)(nil),
	CarrierRPCProtocol_class,
)

// Broker_base_ exposes the embedded broker carrier to code holding a
// subclass.
func (b *BrokerRPCProtocol_) Broker_base_() *BrokerRPCProtocol_ { return b }

func (b *BrokerRPCProtocol_) broker_hooks() Broker_hooks_ {
	if h, ok := b.Self().(Broker_hooks_); ok {
		return h
	}
	return b
}

func (b *BrokerRPCProtocol_) init_broker_tables_() {
	b.init_tables_()
	if b.Handshake_pending_ == nil {
		b.Handshake_pending_ = map[string]*handshakeSlot_{}
	}
}

// ------------------------------------------------------------------
// Subclass hooks
// ------------------------------------------------------------------

// Broker_connect_ establishes the broker client connection. Runs in the
// loop.
func (b *BrokerRPCProtocol_) Broker_connect_() error { return py.NotImplementedError.New() }

// Broker_subscribe_ subscribes to *topic*; inbound payloads must reach
// Feed_message_.
func (b *BrokerRPCProtocol_) Broker_subscribe_(topic string) error {
	return py.NotImplementedError.New()
}

// Broker_publish_ publishes *data* to *topic*.
func (b *BrokerRPCProtocol_) Broker_publish_(topic string, data []byte) error {
	return py.NotImplementedError.New()
}

// Broker_close_ closes the broker connection. Default: no-op.
func (b *BrokerRPCProtocol_) Broker_close_() error { return nil }

// Inbox_topic_ is the topic this endpoint listens on for inbound frames.
func (b *BrokerRPCProtocol_) Inbox_topic_() string {
	return "laila/inbox/" + py.Str(b.Policy_id_())
}

// Peer_inbox_ is the inbox topic of a peer addressed by its policy
// global_id.
func (b *BrokerRPCProtocol_) Peer_inbox_(peer_policy_id string) string {
	return "laila/inbox/" + peer_policy_id
}

// Inbox is ``self._inbox`` ("" before Start).
func (b *BrokerRPCProtocol_) Inbox() string {
	if b.Inbox_ == nil {
		return ""
	}
	return *b.Inbox_
}

// ------------------------------------------------------------------
// Lifecycle
// ------------------------------------------------------------------

// Start boots the loop, connects to the broker, subscribes to the inbox.
func (b *BrokerRPCProtocol_) Start() error {
	if b.Started_ {
		return nil
	}
	b.init_broker_tables_()
	b.Ensure_executor_()
	if err := Start_loop_thread(b, b.Async_start_, max(b.Handshake_timeout, 10.0)); err != nil {
		b.Shutdown_executor_()
		return err
	}
	b.Started_ = true
	return nil
}

// Async_start_ connects and subscribes inside the loop.
func (b *BrokerRPCProtocol_) Async_start_(ready *py.Event) error {
	if err := b.broker_hooks().Broker_connect_(); err != nil {
		return err
	}
	inbox := b.broker_hooks().Inbox_topic_()
	b.Inbox_ = &inbox
	if err := b.broker_hooks().Broker_subscribe_(inbox); err != nil {
		return err
	}
	ready.Set()
	return nil
}

// Stop says goodbye, disconnects from the broker and stops the loop
// (idempotent).
func (b *BrokerRPCProtocol_) Stop() error {
	if !b.Started_ {
		return nil
	}

	shutdown := func() error {
		_ = b.Goodbye_all_()
		for _, peer_id := range b.Peer_ids_() {
			b.hooks().Unregister_peer_(peer_id)
		}
		b.Shutdown_lanes_()
		if err := b.broker_hooks().Broker_close_(); err != nil {
			return err
		}
		return Cancel_pending_tasks()
	}

	Stop_loop_thread(b, shutdown)
	b.Shutdown_lanes_()
	b.Handshake_pending_ = map[string]*handshakeSlot_{}
	b.Fail_pending_("transport stopped", nil)
	b.Shutdown_executor_()
	b.Started_ = false
	return nil
}

// Send_goodbye_ publishes ``peer.disconnect`` to the peer's inbox.
func (b *BrokerRPCProtocol_) Send_goodbye_(peer_id string) error {
	inbox := b.Connection_handle_(peer_id)
	if py.IsNone(inbox) {
		return nil
	}
	data, err := b.Encode_(b.Goodbye_message_())
	if err != nil {
		return err
	}
	return b.broker_hooks().Broker_publish_(py.Str(inbox), data)
}

// ------------------------------------------------------------------
// Inbound routing
// ------------------------------------------------------------------

// Feed_message_ is the inbound entry point: decode a frame and route it.
func (b *BrokerRPCProtocol_) Feed_message_(data []byte) {
	msg, err := b.Decode_msg_(data)
	if err != nil {
		return
	}

	if Is_request(msg) {
		method := msg.GetDefault("method", nil)
		if py.Eq(method, PEER_CONNECT_) {
			b.Handle_handshake_(msg)
		} else if Is_goodbye_(msg) {
			b.On_peer_goodbye_(dict_or_empty_(msg.GetDefault("params", nil)).GetDefault("from_id", nil))
		} else {
			b.Dispatch_request_(msg)
		}
	} else if Is_response(msg) {
		rid := py.Str(msg.GetDefault("id", nil))
		if slot, ok := b.Handshake_pending_[rid]; ok {
			if slot != nil {
				slot.msg = msg
				slot.event.Set()
			}
		} else {
			b.Complete_pending_(msg)
		}
	}
}

// Handle_handshake_ is the acceptor side of ``peer.connect`` over the
// broker.
func (b *BrokerRPCProtocol_) Handle_handshake_(msg *py.Dict) {
	params := dict_or_empty_(msg.GetDefault("params", nil))
	peer_id := params.GetDefault("from_id", nil)
	reply_to := params.GetDefault("reply_to", nil)
	if !py.Eq(params.GetDefault("secret", nil), b.Peer_secret_key) {
		b.Publish_async_(reply_to, Make_error(msg.GetDefault("id", nil), ERR_AUTH_FAILED, "Invalid peer secret key.", nil))
		return
	}
	resp := Make_result(msg.GetDefault("id", nil), py.DictOf("peer_id", b.Policy_id_()))
	b.hooks().Register_peer_(py.Str(peer_id), reply_to)
	b.Publish_async_(reply_to, resp)
}

// Dispatch_request_ runs an inbound ``rpc.call`` off the loop and
// publishes the reply to ``reply_to``.
func (b *BrokerRPCProtocol_) Dispatch_request_(msg *py.Dict) {
	reply_to := msg.GetDefault("reply_to", nil)
	b.Handle_request_frame_(msg, func(resp *py.Dict) { b.Publish_async_(reply_to, resp) }, nil)
}

// Publish_async_ encodes *obj* and schedules its publication on the loop.
func (b *BrokerRPCProtocol_) Publish_async_(topic any, obj *py.Dict) error {
	if py.IsNone(topic) {
		return nil
	}
	data, err := b.Encode_(obj)
	if err != nil {
		return err
	}
	t := py.Str(topic)
	return b.Loop_call_(func() {
		_, _ = py.Ensure_future(func() (any, error) { return nil, b.broker_hooks().Broker_publish_(t, data) })
	})
}

// ------------------------------------------------------------------
// Peering / RPC
// ------------------------------------------------------------------

// Connect handshakes with the peer whose policy id is encoded in *uri*.
func (b *BrokerRPCProtocol_) Connect(uri string, secret string) (string, error) {
	if err := b.Start(); err != nil {
		return "", err
	}
	peer_policy_id := Uri_authority(uri)
	peer_inbox := b.broker_hooks().Peer_inbox_(peer_policy_id)

	req := Make_request(
		PEER_CONNECT_,
		py.DictOf("from_id", b.Policy_id_(), "secret", secret, "reply_to", b.Inbox()),
		"",
	)
	rid := py.Str(req.GetDefault("id", nil))
	slot := &handshakeSlot_{event: py.NewEvent()}
	b.Handshake_pending_[rid] = slot
	if err := b.Publish_async_(peer_inbox, req); err != nil {
		delete(b.Handshake_pending_, rid)
		return "", err
	}

	completed := slot.event.Wait(b.Handshake_timeout)
	delete(b.Handshake_pending_, rid)
	if !completed {
		return "", py.ConnectionError.New("Peer handshake timed out.")
	}
	reply := slot.msg
	if e, found := reply.Get("error"); found {
		return "", py.ConnectionError.Newf("Peer rejected connection: %s", py.Str(error_message_(e)))
	}
	peer_id := dict_or_empty_(reply.GetDefault("result", nil)).GetDefault("peer_id", nil)
	if py.IsNone(peer_id) {
		return "", py.ConnectionError.New("Peer response missing peer_id.")
	}
	pid := py.Str(peer_id)
	b.hooks().Register_peer_(pid, peer_inbox)
	return pid, nil
}

// Send_once_ publishes an ``rpc.call`` to *peer_id*'s inbox and blocks
// for the reply.
func (b *BrokerRPCProtocol_) Send_once_(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	peer_inbox := b.Connection_handle_(peer_id)
	if py.IsNone(peer_inbox) {
		return nil, py.ConnectionError.Newf("No connection to peer %s", peer_id)
	}
	request_id, slot := b.Register_pending_(peer_id)
	req := b.Make_rpc_request_(path, args, kwargs, request_id)
	req.Set("reply_to", b.Inbox())
	if err := b.Publish_async_(peer_inbox, req); err != nil {
		b.Pending_rpcs_.Pop(request_id)
		return nil, err
	}
	return b.Await_pending_(request_id, slot)
}

var _ = broker_carrier_log_
