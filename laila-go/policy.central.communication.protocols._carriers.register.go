package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/_carriers/register.py --
// register / mailbox (master-slave bus) RPC carrier.
//
// RegisterRPCProtocol_ carries JSON-RPC over a register/byte bus that
// has no asynchronous push -- Modbus, I2C, SPI, 1-Wire, EtherNet/IP. On
// such buses a message is exchanged by writing it into a *mailbox*
// region on the peer and polling the peer's mailbox for replies.
//
// The carrier provides the reliable JSON-RPC layer (handshake,
// request/response correlation, off-thread dispatch) and a polling loop;
// a concrete transport supplies just two coroutines:
//
//   - Deliver_(data) -- write one length-prefixed frame to the peer's
//     inbox region.
//   - Poll_inbound_() -- read the next length-prefixed frame from our
//     own inbox region, or return ``None`` when nothing is pending.
//
// These buses are point-to-point (one peer per link), so addressing is
// implicit; the single peer is still tracked by ``global_id`` for
// consistency with the other carriers.

var register_carrier_log_ = py.Get_logger("laila.policy.central.communication.protocols._carriers.register")

// Register_hooks_ are the bus hooks a concrete register transport
// overrides.
type Register_hooks_ interface {
	Open_bus_() error
	Close_bus_() error
	Deliver_(data []byte) error
	Poll_inbound_() ([]byte, error)
}

// RegisterRPCProtocol_ is the carrier for polled master/slave register
// buses.
//
// Fields:
//
//   - Poll_interval (default ``0.01``) -- seconds between mailbox polls.
type RegisterRPCProtocol_ struct {
	CarrierRPCProtocol_

	Poll_interval float64 `py:"poll_interval" default:"0.01"`

	Poll_task_         *py.Task                   `py:"_poll_task"`
	Handshake_pending_ map[string]*handshakeSlot_ `py:"_handshake_pending"`
}

// RegisterRPCProtocol_class is the class descriptor.
var RegisterRPCProtocol_class = py.NewClass(
	"_RegisterRPCProtocol", (*RegisterRPCProtocol_)(nil),
	CarrierRPCProtocol_class,
)

// Register_base_ exposes the embedded register carrier to code holding a
// subclass.
func (r *RegisterRPCProtocol_) Register_base_() *RegisterRPCProtocol_ { return r }

func (r *RegisterRPCProtocol_) register_hooks() Register_hooks_ {
	if h, ok := r.Self().(Register_hooks_); ok {
		return h
	}
	return r
}

func (r *RegisterRPCProtocol_) init_register_tables_() {
	r.init_tables_()
	if r.Handshake_pending_ == nil {
		r.Handshake_pending_ = map[string]*handshakeSlot_{}
	}
}

// ------------------------------------------------------------------
// Subclass hooks
// ------------------------------------------------------------------

// Open_bus_ opens the underlying bus/device. Runs in the loop. Default
// no-op.
func (r *RegisterRPCProtocol_) Open_bus_() error { return nil }

// Close_bus_ closes the underlying bus/device. Default no-op.
func (r *RegisterRPCProtocol_) Close_bus_() error { return nil }

// Deliver_ writes one length-prefixed frame to the peer's inbox region.
func (r *RegisterRPCProtocol_) Deliver_(data []byte) error { return py.NotImplementedError.New() }

// Poll_inbound_ reads the next inbound frame from our inbox region, or
// ``None``.
func (r *RegisterRPCProtocol_) Poll_inbound_() ([]byte, error) {
	return nil, py.NotImplementedError.New()
}

// ------------------------------------------------------------------
// Lifecycle
// ------------------------------------------------------------------

// Start opens the bus and starts the polling loop (idempotent).
func (r *RegisterRPCProtocol_) Start() error {
	if r.Started_ {
		return nil
	}
	r.init_register_tables_()
	r.Ensure_executor_()
	if err := Start_loop_thread(r, r.Async_start_, max(r.Handshake_timeout, 10.0)); err != nil {
		r.Shutdown_executor_()
		return err
	}
	r.Started_ = true
	return nil
}

// Async_start_ opens the bus and starts polling inside the loop.
func (r *RegisterRPCProtocol_) Async_start_(ready *py.Event) error {
	if err := r.register_hooks().Open_bus_(); err != nil {
		return err
	}
	task, err := py.Ensure_future(func() (any, error) { return nil, r.Poll_loop_() })
	if err != nil {
		return err
	}
	r.Poll_task_ = task
	ready.Set()
	return nil
}

// Stop says goodbye, stops polling, closes the bus, stops the loop
// (idempotent).
func (r *RegisterRPCProtocol_) Stop() error {
	if !r.Started_ {
		return nil
	}

	shutdown := func() error {
		_ = r.Goodbye_all_()
		for _, peer_id := range r.Peer_ids_() {
			r.hooks().Unregister_peer_(peer_id)
		}
		r.Shutdown_lanes_()
		if r.Poll_task_ != nil {
			r.Poll_task_.Cancel()
			_, _ = r.Poll_task_.Await()
			r.Poll_task_ = nil
		}
		if err := r.register_hooks().Close_bus_(); err != nil {
			return err
		}
		return Cancel_pending_tasks()
	}

	Stop_loop_thread(r, shutdown)
	r.Shutdown_lanes_()
	r.Handshake_pending_ = map[string]*handshakeSlot_{}
	r.Fail_pending_("transport stopped", nil)
	r.Shutdown_executor_()
	r.Started_ = false
	return nil
}

// Send_goodbye_ writes ``peer.disconnect`` into the peer's mailbox.
func (r *RegisterRPCProtocol_) Send_goodbye_(peer_id string) error {
	if !r.Connections_.Contains(peer_id) {
		return nil
	}
	data, err := r.Encode_(r.Goodbye_message_())
	if err != nil {
		return err
	}
	return r.register_hooks().Deliver_(data)
}

// Poll_loop_ continuously polls the mailbox and dispatches inbound
// frames.
func (r *RegisterRPCProtocol_) Poll_loop_() error {
	err := func() error {
		for {
			data, err := r.register_hooks().Poll_inbound_()
			if err != nil {
				return err
			}
			if len(data) > 0 {
				r.Feed_message_(data)
			} else if err := py.Sleep(r.Poll_interval); err != nil {
				return err
			}
		}
	}()
	if err != nil && !py.IsInstance(err, py.CancelledError) {
		register_carrier_log_.Debug("Register poll loop ended", py.Kw{"exc_info": err})
	}
	return nil
}

// ------------------------------------------------------------------
// Inbound routing
// ------------------------------------------------------------------

// Feed_message_ decodes one inbound frame and routes it.
func (r *RegisterRPCProtocol_) Feed_message_(data []byte) {
	msg, err := r.Decode_msg_(data)
	if err != nil {
		return
	}
	if Is_request(msg) {
		if py.Eq(msg.GetDefault("method", nil), PEER_CONNECT_) {
			r.Handle_handshake_(msg)
		} else if Is_goodbye_(msg) {
			r.On_peer_goodbye_(dict_or_empty_(msg.GetDefault("params", nil)).GetDefault("from_id", nil))
		} else {
			r.Dispatch_request_(msg)
		}
	} else if Is_response(msg) {
		rid := py.Str(msg.GetDefault("id", nil))
		if slot, ok := r.Handshake_pending_[rid]; ok {
			if slot != nil {
				slot.msg = msg
				slot.event.Set()
			}
		} else {
			r.Complete_pending_(msg)
		}
	}
}

// Handle_handshake_ is the acceptor side of ``peer.connect`` on the bus.
func (r *RegisterRPCProtocol_) Handle_handshake_(msg *py.Dict) {
	params := dict_or_empty_(msg.GetDefault("params", nil))
	peer_id := params.GetDefault("from_id", nil)
	if !py.Eq(params.GetDefault("secret", nil), r.Peer_secret_key) {
		_ = r.Deliver_async_(Make_error(msg.GetDefault("id", nil), ERR_AUTH_FAILED, "Invalid peer secret key.", nil))
		return
	}
	resp := Make_result(msg.GetDefault("id", nil), py.DictOf("peer_id", r.Policy_id_()))
	r.hooks().Register_peer_(py.Str(peer_id), true)
	_ = r.Deliver_async_(resp)
}

// Dispatch_request_ runs an inbound ``rpc.call`` off the loop and writes
// the reply to the bus.
func (r *RegisterRPCProtocol_) Dispatch_request_(msg *py.Dict) {
	r.Handle_request_frame_(msg, func(resp *py.Dict) { _ = r.Deliver_async_(resp) }, nil)
}

// Deliver_async_ encodes *obj* and schedules its delivery on the loop.
func (r *RegisterRPCProtocol_) Deliver_async_(obj *py.Dict) error {
	data, err := r.Encode_(obj)
	if err != nil {
		return err
	}
	return r.Loop_call_(func() {
		_, _ = py.Ensure_future(func() (any, error) { return nil, r.register_hooks().Deliver_(data) })
	})
}

// ------------------------------------------------------------------
// Peering / RPC
// ------------------------------------------------------------------

// Connect handshakes with the single peer on the other end of the bus.
func (r *RegisterRPCProtocol_) Connect(uri string, secret string) (string, error) {
	if err := r.Start(); err != nil {
		return "", err
	}
	req := Make_request(PEER_CONNECT_, py.DictOf("from_id", r.Policy_id_(), "secret", secret), "")
	rid := py.Str(req.GetDefault("id", nil))
	slot := &handshakeSlot_{event: py.NewEvent()}
	r.Handshake_pending_[rid] = slot
	if err := r.Deliver_async_(req); err != nil {
		delete(r.Handshake_pending_, rid)
		return "", err
	}

	completed := slot.event.Wait(r.Handshake_timeout)
	delete(r.Handshake_pending_, rid)
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
	r.hooks().Register_peer_(pid, true)
	return pid, nil
}

// Send_once_ writes an ``rpc.call`` to the bus and blocks for the polled
// reply.
func (r *RegisterRPCProtocol_) Send_once_(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	if !r.Connections_.Contains(peer_id) {
		return nil, py.ConnectionError.Newf("No connection to peer %s", peer_id)
	}
	request_id, slot := r.Register_pending_(peer_id)
	req := r.Make_rpc_request_(path, args, kwargs, request_id)
	if err := r.Deliver_async_(req); err != nil {
		r.Pending_rpcs_.Pop(request_id)
		return nil, err
	}
	return r.Await_pending_(request_id, slot)
}
