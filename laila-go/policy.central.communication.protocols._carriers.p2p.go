package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/_carriers/p2p.py --
// point-to-point duplex-stream RPC carrier.
//
// P2PStreamRPCProtocol_ is the variant of the stream carrier for links
// that are a *single, already-connected* bidirectional byte stream with
// no listen/accept step: serial lines (UART/RS-232/RS-485), USB-CDC,
// Bluetooth RFCOMM, a paired BLE characteristic, etc. Both endpoints
// simply open the link; one side initiates the ``peer.connect``
// handshake and the other answers it on the same stream.
//
// A concrete transport supplies one coroutine, Open_stream_, returning
// the link's ``(reader, writer)`` pair (a py.StreamReader and a writer
// exposing ``write`` / ``drain`` / ``close``). The carrier owns the
// dedicated event loop, length-prefixed framing, the single-peer
// handshake, the receive loop, off-thread dispatch and the pending-RPC
// table.
//
// Stream lanes ride the same link through one Link_ (priority RPC queue
// + chunked stream queue, see stream.go). Because serial links are slow,
// ``stream_chunk_bytes`` defaults to ``4096`` here: a 30 KB frame at
// 1 Mbaud is ~300 ms of wire time, and a liveness ping must never wait
// behind it.

var p2p_carrier_log_ = py.Get_logger("laila.policy.central.communication.protocols._carriers.p2p")

// P2P_hooks_ are the transport hooks a concrete point-to-point transport
// overrides.
type P2P_hooks_ interface {
	Open_stream_() (*py.StreamReader, Stream_writer_, error)
	Close_stream_() error
}

// P2PStreamRPCProtocol_ is the carrier for a single point-to-point
// duplex byte stream.
//
// The per-peer handle stored in ``_connections`` is ``True``; the one
// link is held in Link_.
type P2PStreamRPCProtocol_ struct {
	CarrierRPCProtocol_

	Reader_            *py.StreamReader           `py:"_reader"`
	Writer_            Stream_writer_             `py:"_writer"`
	Link_              *Link_                     `py:"_link"`
	Recv_task_         *py.Task                   `py:"_recv_task"`
	Handshake_pending_ map[string]*handshakeSlot_ `py:"_handshake_pending"`
}

// P2PStreamRPCProtocol_class is the class descriptor (``supports_channels
// = True``).
var P2PStreamRPCProtocol_class = py.NewClass(
	"_P2PStreamRPCProtocol", (*P2PStreamRPCProtocol_)(nil),
	CarrierRPCProtocol_class,
).
	Set_classvar("supports_channels", true).
	// ``stream_chunk_bytes: int = Field(default=4096)``: serial-friendly
	// default -- small chunks keep RPC latency low.
	Set_field_default("stream_chunk_bytes", 4096)

// P2p_base_ exposes the embedded P2P carrier to code holding a subclass.
func (p *P2PStreamRPCProtocol_) P2p_base_() *P2PStreamRPCProtocol_ { return p }

func (p *P2PStreamRPCProtocol_) p2p_hooks() P2P_hooks_ {
	if h, ok := p.Self().(P2P_hooks_); ok {
		return h
	}
	return p
}

func (p *P2PStreamRPCProtocol_) init_p2p_tables_() {
	p.init_tables_()
	if p.Handshake_pending_ == nil {
		p.Handshake_pending_ = map[string]*handshakeSlot_{}
	}
}

// ------------------------------------------------------------------
// Subclass hooks
// ------------------------------------------------------------------

// Open_stream_ opens the link and returns its ``(reader, writer)`` pair.
func (p *P2PStreamRPCProtocol_) Open_stream_() (*py.StreamReader, Stream_writer_, error) {
	return nil, nil, py.NotImplementedError.New()
}

// Close_stream_ closes the link. Default: close the writer.
func (p *P2PStreamRPCProtocol_) Close_stream_() error {
	if p.Link_ != nil {
		p.Link_.Close()
		p.Link_ = nil
	}
	if p.Writer_ != nil {
		func() {
			defer func() { _ = recover() }()
			p.Writer_.Close()
		}()
	}
	p.Writer_ = nil
	p.Reader_ = nil
	return nil
}

// ------------------------------------------------------------------
// Lifecycle
// ------------------------------------------------------------------

// Start boots the loop, opens the stream, starts the receive loop.
func (p *P2PStreamRPCProtocol_) Start() error {
	if p.Started_ {
		return nil
	}
	p.init_p2p_tables_()
	p.Ensure_executor_()
	if err := Start_loop_thread(p, p.Async_start_, max(p.Handshake_timeout, 10.0)); err != nil {
		p.Shutdown_executor_()
		return err
	}
	p.Started_ = true
	return nil
}

// Async_start_ opens the stream, wraps it in a Link_ and starts the
// receive loop.
func (p *P2PStreamRPCProtocol_) Async_start_(ready *py.Event) error {
	reader, writer, err := p.p2p_hooks().Open_stream_()
	if err != nil {
		return err
	}
	p.Reader_, p.Writer_ = reader, writer
	p.Link_ = New_Link_(&p.CarrierRPCProtocol_, p.Writer_, p.Loop())
	p.Link_.Start()
	task, err := py.Ensure_future(func() (any, error) { return nil, p.Receive_loop_() })
	if err != nil {
		return err
	}
	p.Recv_task_ = task
	ready.Set()
	return nil
}

// Stop closes lanes, the stream and the loop (idempotent).
//
// Order: goodbye to the peer, unregister it, stop the receive task
// (cancel *and wait*, so ``read_frame()`` has finished with the reader
// before any fd underneath it is closed), close the stream, cancel and
// await every remaining task, stop and close the loop.
func (p *P2PStreamRPCProtocol_) Stop() error {
	if !p.Started_ {
		return nil
	}

	shutdown := func() error {
		_ = p.Goodbye_all_()
		for _, peer_id := range p.Peer_ids_() {
			p.hooks().Unregister_peer_(peer_id)
		}
		p.Shutdown_lanes_()
		if p.Recv_task_ != nil {
			p.Recv_task_.Cancel()
			_, _ = p.Recv_task_.Await()
			p.Recv_task_ = nil
		}
		if err := p.p2p_hooks().Close_stream_(); err != nil {
			return err
		}
		return Cancel_pending_tasks()
	}

	Stop_loop_thread(p, shutdown)
	p.Shutdown_lanes_()
	p.Handshake_pending_ = map[string]*handshakeSlot_{}
	p.Fail_pending_("transport stopped", nil)
	p.Shutdown_executor_()
	p.Started_ = false
	return nil
}

// Send_goodbye_ queues ``peer.disconnect`` on the link and waits for it
// to be written.
func (p *P2PStreamRPCProtocol_) Send_goodbye_(peer_id string) error {
	link := p.Link_
	if link == nil || link.Closed || !p.Connections_.Contains(peer_id) {
		return nil
	}
	p.Queue_frame_(p.Goodbye_message_())
	return link.Flush(0.5)
}

// ------------------------------------------------------------------
// Receive loop / dispatch
// ------------------------------------------------------------------

// Sole_peer_ is the single registered peer id, if any.
func (p *P2PStreamRPCProtocol_) Sole_peer_() (string, bool) {
	for _, peer_id := range p.Peer_ids_() {
		return peer_id, true
	}
	return "", false
}

// sole_peer_any_ is Sole_peer_ as ``str | None``.
func (p *P2PStreamRPCProtocol_) sole_peer_any_() any {
	if pid, ok := p.Sole_peer_(); ok {
		return pid
	}
	return nil
}

// Receive_loop_ decodes frames and routes requests/responses/stream
// chunks until EOF.
func (p *P2PStreamRPCProtocol_) Receive_loop_() error {
	defer func() {
		for _, peer_id := range p.Peer_ids_() {
			p.hooks().Unregister_peer_(peer_id)
		}
	}()
	reply := p.Make_reply_()
	err := func() error {
		for {
			raw, err := Read_frame(p.Reader_)
			if err != nil {
				return err
			}
			if raw == nil {
				return nil
			}
			if Is_reserved_frame(raw) {
				peer_id, ok := p.Sole_peer_()
				if !ok {
					// stream frame before any handshake: not ours yet
					p.Unknown_lane_drops_++
					continue
				}
				p.On_stream_frame_(peer_id, raw)
				continue
			}
			msg, err := p.Decode_msg_(raw)
			if err != nil {
				return err
			}
			if Is_request(msg) {
				if py.Eq(msg.GetDefault("method", nil), PEER_CONNECT_) {
					if err := p.Handle_handshake_(msg); err != nil {
						return err
					}
				} else if Is_goodbye_(msg) {
					// The link stays open (it is the wire itself); only the
					// peering is dropped, so a new handshake can follow.
					from_id := dict_or_empty_(msg.GetDefault("params", nil)).GetDefault("from_id", nil)
					if sole, ok := p.Sole_peer_(); ok && py.Eq(from_id, sole) {
						p.On_peer_goodbye_(from_id)
					}
				} else {
					p.Handle_request_frame_(msg, reply, p.sole_peer_any_())
				}
			} else if Is_response(msg) {
				rid := py.Str(msg.GetDefault("id", nil))
				if slot, ok := p.Handshake_pending_[rid]; ok {
					if slot != nil {
						slot.msg = msg
						slot.event.Set()
					}
				} else {
					p.Complete_pending_(msg)
				}
			}
		}
	}()
	if err != nil && !py.IsInstance(err, py.CancelledError) && !py.IsInstance(err, py.ConnectionError) {
		p2p_carrier_log_.Debug("P2P receive loop ended", py.Kw{"exc_info": err})
	}
	return nil
}

// Queue_frame_ frames *obj* and queues it on the link (loop thread).
func (p *P2PStreamRPCProtocol_) Queue_frame_(obj *py.Dict) {
	if p.Link_ == nil {
		return
	}
	data, err := p.Encode_(obj)
	if err != nil {
		return
	}
	p.Link_.Put_rpc(Frame(data))
}

// Write_frame_ queues *obj* for the writer task (kept for subclass
// compatibility).
func (p *P2PStreamRPCProtocol_) Write_frame_(obj *py.Dict) error {
	p.Queue_frame_(obj)
	return nil
}

// Handle_handshake_ is the server side of ``peer.connect`` on the link.
func (p *P2PStreamRPCProtocol_) Handle_handshake_(msg *py.Dict) error {
	params := dict_or_empty_(msg.GetDefault("params", nil))
	peer_id := params.GetDefault("from_id", nil)
	if !py.Eq(params.GetDefault("secret", nil), p.Peer_secret_key) {
		return p.Write_frame_(Make_error(msg.GetDefault("id", nil), ERR_AUTH_FAILED, "Invalid peer secret key.", nil))
	}
	pid := py.Str(peer_id)
	p.Set_peer_caps_(pid, params.GetDefault("caps", nil))
	p.hooks().Register_peer_(pid, true)
	return p.Write_frame_(Make_result(msg.GetDefault("id", nil), py.DictOf("peer_id", p.Policy_id_(), "caps", p.hooks().Local_caps_())))
}

// Make_reply_ builds a thread-safe ``reply(resp)`` that queues a frame on
// the link.
//
// Used by Handle_request_frame_; safe both inline on the I/O loop (ping /
// control fast-path) and from an inbound worker thread.
func (p *P2PStreamRPCProtocol_) Make_reply_() func(resp *py.Dict) {
	return func(resp *py.Dict) {
		_ = p.Loop_call_(func() { p.Queue_frame_(resp) })
	}
}

// ------------------------------------------------------------------
// Peering / RPC / stream
// ------------------------------------------------------------------

// Connect initiates the handshake over the already-open point-to-point
// link.
func (p *P2PStreamRPCProtocol_) Connect(uri string, secret string) (string, error) {
	if err := p.Start(); err != nil {
		return "", err
	}
	req := Make_request(
		PEER_CONNECT_,
		py.DictOf("from_id", p.Policy_id_(), "secret", secret, "caps", p.hooks().Local_caps_()),
		"",
	)
	rid := py.Str(req.GetDefault("id", nil))
	slot := &handshakeSlot_{event: py.NewEvent()}
	p.Handshake_pending_[rid] = slot
	if err := p.Loop_call_(func() { p.Queue_frame_(req) }); err != nil {
		delete(p.Handshake_pending_, rid)
		return "", err
	}

	completed := slot.event.Wait(p.Handshake_timeout)
	delete(p.Handshake_pending_, rid)
	if !completed {
		return "", py.ConnectionError.New("Peer handshake timed out.")
	}
	reply := slot.msg
	if e, found := reply.Get("error"); found {
		return "", py.ConnectionError.Newf("Peer rejected connection: %s", py.Str(error_message_(e)))
	}
	result := dict_or_empty_(reply.GetDefault("result", nil))
	peer_id := result.GetDefault("peer_id", nil)
	if py.IsNone(peer_id) {
		return "", py.ConnectionError.New("Peer response missing peer_id.")
	}
	pid := py.Str(peer_id)
	p.Set_peer_caps_(pid, result.GetDefault("caps", nil))
	p.hooks().Register_peer_(pid, true)
	return pid, nil
}

// Queue_rpc_threadsafe_ frames + queues *msg* on the link from any
// thread.
func (p *P2PStreamRPCProtocol_) Queue_rpc_threadsafe_(msg *py.Dict) error {
	if p.Link_ == nil || p.Link_.Closed {
		return py.ConnectionError.New("Point-to-point link is not open.")
	}
	return p.Loop_call_(func() { p.Queue_frame_(msg) })
}

// Send_once_ writes an ``rpc.call`` to the link and blocks for the
// response.
func (p *P2PStreamRPCProtocol_) Send_once_(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	if !p.Connections_.Contains(peer_id) {
		return nil, py.ConnectionError.Newf("No connection to peer %s", peer_id)
	}
	request_id, slot := p.Register_pending_(peer_id)
	req := p.Make_rpc_request_(path, args, kwargs, request_id)
	if err := p.Queue_rpc_threadsafe_(req); err != nil {
		p.Pending_rpcs_.Pop(request_id)
		return nil, err
	}
	return p.Await_pending_(request_id, slot)
}

// Send_oneway_ queues a control request without waiting for the reply.
func (p *P2PStreamRPCProtocol_) Send_oneway_(peer_id string, path []string, kwargs py.Kw) error {
	if !p.Connections_.Contains(peer_id) {
		return py.ConnectionError.Newf("No connection to peer %s", peer_id)
	}
	return p.Queue_rpc_threadsafe_(p.Make_rpc_request_(path, nil, kwargs, py.Uuid4().String()))
}

// Stream_enqueue_ marshals one stream message onto the link (user
// thread).
func (p *P2PStreamRPCProtocol_) Stream_enqueue_(channel *Channel, payload []byte) error {
	if !p.Connections_.Contains(channel.Peer_id) {
		return py.ConnectionError.Newf("No connection to peer %s", channel.Peer_id)
	}
	link := p.Link_
	if link == nil || link.Closed {
		return py.ConnectionError.New("Point-to-point link is not open.")
	}
	return p.Loop_call_(func() { link.Put_stream(channel, payload) })
}
