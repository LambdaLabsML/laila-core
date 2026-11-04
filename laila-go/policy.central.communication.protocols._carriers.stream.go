package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/_carriers/stream.py -- reliable,
// ordered, duplex byte-stream RPC carrier.
//
// StreamRPCProtocol_ implements the full LAILA_IDENTIFIABLE_COMM_PROTOCOL_
// contract over any transport that looks like a py.StreamReader /
// StreamWriter pair: TCP, TLS, Unix domain sockets, serial lines,
// USB-CDC, RFCOMM, ...
//
// A concrete transport only supplies two coroutines:
//
//   - Serve_ -- create and return a listening server whose per-connection
//     callback is Handle_inbound_stream_ (e.g.
//     ``py.Start_server(self.Handle_inbound_stream_, host, port)``).
//   - Open_connection_ -- open one outbound connection to a URI and return
//     its ``(reader, writer)`` pair.
//
// Everything else -- the dedicated background event loop, length-prefixed
// framing, the ``peer.connect`` handshake, the receive loop, off-thread
// inbound dispatch, and the blocking pending-RPC table -- is handled
// here.
//
// Stream lanes and writer fairness: each peer connection is wrapped in a
// Link_: the writer plus two outbound queues (RPC/control, which has
// priority, and stream) drained by a single writer task. **Every** write
// -- handshake frames, replies, Send_once_, stream chunks -- goes through
// the link; nothing else calls ``writer.write``. Stream messages are
// sliced into ``stream_chunk_bytes`` chunks and the RPC queue is drained
// between chunks, so a liveness ping is never stuck behind a 200 KB
// frame. Each write is followed by ``await writer.drain()`` and the
// transport's write-buffer high-water mark is set to two chunks, so link
// backpressure (e.g. the UART flow control) is honoured instead of
// buffering unboundedly inside asyncio.
//
// Inbound frames are discriminated on their first byte
// (Is_reserved_frame): stream frames go straight to
// CarrierRPCProtocol_.On_stream_frame_ and never touch the RPC codec,
// admission or the executor.

var stream_carrier_log_ = py.Get_logger("laila.policy.central.communication.protocols._carriers.stream")

// Stream_writer_ is the ``asyncio.StreamWriter``-like object a Link_
// drives: ``write`` / ``drain`` / ``close`` (and optionally
// ``transport``).
type Stream_writer_ interface {
	Write(data []byte)
	Drain() error
	Close()
}

// streamQItem_ is one queued ``(channel, payload)`` stream message.
type streamQItem_ struct {
	channel *Channel
	payload []byte
}

// Link_ is one peer connection: writer + prioritised outbound queues +
// writer task.
//
//   - Proto: owning carrier (for Stream_chunks_ / Channel_queue_size).
//   - Writer: StreamWriter-like object.
//
// All methods except Close_threadsafe must be called on the carrier's
// event loop thread. Put_rpc / Put_stream are marshalled there by callers
// via ``call_soon_threadsafe``.
type Link_ struct {
	Closed       bool
	Loop         *py.Loop
	Proto        *CarrierRPCProtocol_
	Rpc_q        [][]byte
	Stream_q     []streamQItem_
	Stream_q_max int
	Task         *py.Task
	Wake         *py.AsyncEvent
	Writer       Stream_writer_
}

// New_Link_ is ``_Link(proto, writer, loop)``.
func New_Link_(proto *CarrierRPCProtocol_, writer Stream_writer_, loop *py.Loop) *Link_ {
	l := &Link_{
		Proto:        proto,
		Writer:       writer,
		Loop:         loop,
		Stream_q_max: max(1, proto.Channel_queue_size),
		Wake:         py.New_AsyncEvent(),
	}
	if w, ok := writer.(interface{ Transport() *py.Transport }); ok {
		if transport := w.Transport(); transport != nil {
			high := 2 * max(1, proto.Stream_chunk_bytes)
			transport.Set_write_buffer_limits(high, high/2)
		}
	}
	return l
}

// Start starts the writer task (loop thread).
func (l *Link_) Start() {
	if l.Task == nil {
		l.Task = l.Loop.Create_task(func() (any, error) { return nil, l.Run_() })
	}
}

// Put_rpc queues one framed RPC/control payload (priority lane).
func (l *Link_) Put_rpc(data []byte) {
	if l.Closed {
		return
	}
	l.Rpc_q = append(l.Rpc_q, data)
	l.Wake.Set()
}

// Put_stream queues one stream message; evicts the oldest when the queue
// is full.
func (l *Link_) Put_stream(channel *Channel, payload []byte) {
	if l.Closed {
		return
	}
	if len(l.Stream_q) >= l.Stream_q_max {
		old := l.Stream_q[0]
		l.Stream_q = l.Stream_q[1:]
		old.channel.Tx_dropped_inc_()
	}
	l.Stream_q = append(l.Stream_q, streamQItem_{channel, payload})
	l.Wake.Set()
}

func (l *Link_) write_(data []byte) error {
	l.Writer.Write(data)
	return l.Writer.Drain()
}

func (l *Link_) Drain_rpc_() error {
	for len(l.Rpc_q) > 0 && !l.Closed {
		data := l.Rpc_q[0]
		l.Rpc_q = l.Rpc_q[1:]
		if err := l.write_(data); err != nil {
			return err
		}
	}
	return nil
}

func (l *Link_) Run_() error {
	defer func() { l.Closed = true }()
	err := func() error {
		for !l.Closed {
			if len(l.Rpc_q) == 0 && len(l.Stream_q) == 0 {
				l.Wake.Clear()
				if err := l.Wake.Wait(); err != nil {
					return err
				}
				continue
			}
			if err := l.Drain_rpc_(); err != nil {
				return err
			}
			if len(l.Stream_q) > 0 && !l.Closed {
				item := l.Stream_q[0]
				l.Stream_q = l.Stream_q[1:]
				if item.channel.Closed() || item.channel.Tx_lane_id() == nil {
					continue
				}
				for _, chunk := range l.Proto.Stream_chunks_(item.channel, item.payload) {
					if err := l.write_(Frame(chunk)); err != nil {
						return err
					}
					if err := l.Drain_rpc_(); err != nil {
						return err
					}
					if l.Closed {
						break
					}
				}
			}
		}
		return nil
	}()
	if err != nil && !py.IsInstance(err, py.CancelledError) {
		stream_carrier_log_.Debug("link writer ended", py.Kw{"exc_info": err})
	}
	return nil
}

// Flush waits (bounded; default 2.0 s) until the RPC queue has been
// written out.
func (l *Link_) Flush(timeout ...float64) error {
	t := 2.0
	if len(timeout) > 0 {
		t = timeout[0]
	}
	deadline := l.Loop.Time() + t
	for len(l.Rpc_q) > 0 && !l.Closed && l.Loop.Time() < deadline {
		if err := py.Sleep(0.005); err != nil {
			return err
		}
	}
	return nil
}

// Close stops the writer task and closes the writer (loop thread,
// idempotent).
func (l *Link_) Close() {
	l.Closed = true
	l.Wake.Set()
	if task := l.Task; task != nil && !task.Done() {
		task.Cancel()
	}
	func() {
		defer func() { _ = recover() }()
		l.Writer.Close()
	}()
}

// Close_threadsafe schedules Close on the loop from any thread.
func (l *Link_) Close_threadsafe() {
	if _, err := l.Loop.Call_soon_threadsafe(l.Close); err != nil {
		l.Closed = true
	}
}

// Stream_hooks_ are the transport hooks a concrete stream transport
// overrides.
type Stream_hooks_ interface {
	Serve_() (any, error)
	Open_connection_(uri string) (*py.StreamReader, *py.StreamWriter, error)
	On_stream_ready_(writer *py.StreamWriter)
	Close_server_(server any) error
}

// StreamRPCProtocol_ is the carrier for reliable, ordered, duplex byte
// streams.
//
// All public lifecycle methods are idempotent. The carrier owns a
// dedicated asyncio event loop on a daemon thread; transport I/O is
// scheduled there via py.Run_coroutine_threadsafe so the rest of laila
// stays synchronous. The per-peer handle stored in ``_connections`` is a
// Link_.
type StreamRPCProtocol_ struct {
	CarrierRPCProtocol_

	Server_ any `py:"_server"`
}

// StreamRPCProtocol_class is the class descriptor (``supports_channels =
// True``).
var StreamRPCProtocol_class = py.NewClass(
	"_StreamRPCProtocol", (*StreamRPCProtocol_)(nil),
	CarrierRPCProtocol_class,
).
	Set_classvar("supports_channels", true)

// Stream_base_ exposes the embedded stream carrier to code holding a
// subclass.
func (s *StreamRPCProtocol_) Stream_base_() *StreamRPCProtocol_ { return s }

// stream_hooks is the most-derived transport implementation.
func (s *StreamRPCProtocol_) stream_hooks() Stream_hooks_ {
	if h, ok := s.Self().(Stream_hooks_); ok {
		return h
	}
	return s
}

// ------------------------------------------------------------------
// Subclass hooks
// ------------------------------------------------------------------

// Serve_ creates and returns a listening server.
//
// Subclasses must bind their transport and wire Handle_inbound_stream_ as
// the per-connection callback, recording any bound-address detail they
// expose. Runs inside the carrier event loop.
func (s *StreamRPCProtocol_) Serve_() (any, error) {
	return nil, py.NotImplementedError.New()
}

// Open_connection_ opens one outbound connection to *uri*; returns its
// stream pair. Runs inside the carrier event loop.
func (s *StreamRPCProtocol_) Open_connection_(uri string) (*py.StreamReader, *py.StreamWriter, error) {
	return nil, nil, py.NotImplementedError.New()
}

// On_stream_ready_ is called once a stream is established (inbound +
// outbound).
//
// Subclasses tune the socket here (e.g. TCP transports disable Nagle via
// ``TCP_NODELAY`` for minimum small-frame latency). Receives the raw
// StreamWriter. Default: no-op.
func (s *StreamRPCProtocol_) On_stream_ready_(writer *py.StreamWriter) {}

// Close_server_ closes a server returned by Serve_. Default:
// ``close``+wait.
func (s *StreamRPCProtocol_) Close_server_(server any) error {
	if py.IsNone(server) {
		return nil
	}
	if c, ok := server.(interface{ Close() }); ok {
		c.Close()
	}
	if w, ok := server.(interface{ Wait_closed() error }); ok {
		_, _ = py.Wait_for(func() (any, error) { return nil, w.Wait_closed() }, 2.0)
	}
	return nil
}

// ------------------------------------------------------------------
// Lifecycle
// ------------------------------------------------------------------

// Start boots the event loop and starts accepting connections
// (idempotent).
func (s *StreamRPCProtocol_) Start() error {
	if s.Started_ {
		return nil
	}
	s.init_tables_()
	s.Ensure_executor_()
	if err := Start_loop_thread(s, s.Async_start_, max(s.Handshake_timeout, 10.0)); err != nil {
		s.Shutdown_executor_()
		return err
	}
	s.Started_ = true
	return nil
}

// Async_start_ brings up the server inside the loop, then signals
// *ready*.
func (s *StreamRPCProtocol_) Async_start_(ready *py.Event) error {
	server, err := s.stream_hooks().Serve_()
	if err != nil {
		return err
	}
	s.Server_ = server
	ready.Set()
	return nil
}

// Stop tears down the server, every peer stream and lane, and the loop
// (idempotent).
//
// Order: say goodbye to every peer, unregister them (closing lanes --
// waking relays -- and links, so the server's ``wait_closed()`` completes
// promptly), close the server, cancel and await every remaining task,
// then stop and **close** the loop.
func (s *StreamRPCProtocol_) Stop() error {
	if !s.Started_ {
		return nil
	}

	shutdown := func() error {
		_ = s.Goodbye_all_()
		for _, peer_id := range s.Peer_ids_() {
			s.hooks().Unregister_peer_(peer_id)
		}
		s.Connections_.Clear()
		s.Shutdown_lanes_()
		if err := s.stream_hooks().Close_server_(s.Server_); err != nil {
			return err
		}
		s.Server_ = nil
		return Cancel_pending_tasks()
	}

	Stop_loop_thread(s, shutdown)
	s.Shutdown_lanes_()
	s.Fail_pending_("transport stopped", nil)
	s.Shutdown_executor_()
	s.Started_ = false
	return nil
}

// ------------------------------------------------------------------
// Links
// ------------------------------------------------------------------

// Make_link_ wraps *writer* in a started Link_ (loop thread).
func (s *StreamRPCProtocol_) Make_link_(writer Stream_writer_) *Link_ {
	link := New_Link_(&s.CarrierRPCProtocol_, writer, s.Loop())
	link.Start()
	return link
}

// On_loop_thread_ reports whether the caller runs on the carrier loop.
func (s *StreamRPCProtocol_) On_loop_thread_() bool {
	running, err := py.Get_running_loop()
	return err == nil && running == s.Loop()
}

// Link_of_ is ``self._connections.get(peer_id)`` typed.
func (s *StreamRPCProtocol_) Link_of_(peer_id string) *Link_ {
	l, _ := s.Connection_handle_(peer_id).(*Link_)
	return l
}

// Unregister_peer_ closes lanes (base), then the link, then notifies the
// hub. Idempotent.
func (s *StreamRPCProtocol_) Unregister_peer_(peer_id string) {
	link := s.Link_of_(peer_id)
	s.CarrierRPCProtocol_.Unregister_peer_(peer_id)
	if link == nil {
		return
	}
	if s.On_loop_thread_() {
		link.Close()
	} else {
		link.Close_threadsafe()
	}
}

// Send_goodbye_ queues ``peer.disconnect`` on the peer's link and waits
// for it to be written.
func (s *StreamRPCProtocol_) Send_goodbye_(peer_id string) error {
	link := s.Link_of_(peer_id)
	if link == nil || link.Closed {
		return nil
	}
	data, err := s.Encode_(s.Goodbye_message_())
	if err != nil {
		return err
	}
	link.Put_rpc(Frame(data))
	return link.Flush(0.5)
}

// ------------------------------------------------------------------
// Peering
// ------------------------------------------------------------------

// Connect opens an outbound stream to *uri* and completes the handshake.
func (s *StreamRPCProtocol_) Connect(uri string, secret string) (string, error) {
	if err := s.Start(); err != nil {
		return "", err
	}
	fut := py.Run_coroutine_threadsafe(func() (any, error) { return s.Connect_outbound_(uri, secret) }, s.Loop())
	res, err := fut.Result(max(s.Handshake_timeout*3, 30.0))
	if err != nil {
		return "", err
	}
	return py.Str(res), nil
}

// Connect_outbound_ is the client side of the ``peer.connect`` handshake.
func (s *StreamRPCProtocol_) Connect_outbound_(uri string, secret string) (string, error) {
	reader, writer, err := s.stream_hooks().Open_connection_(uri)
	if err != nil {
		return "", err
	}
	s.stream_hooks().On_stream_ready_(writer)
	link := s.Make_link_(writer)
	req := Make_request(
		PEER_CONNECT_,
		py.DictOf("from_id", s.Policy_id_(), "secret", secret, "caps", s.hooks().Local_caps_()),
		"",
	)
	data, err := s.Encode_(req)
	if err != nil {
		link.Close()
		return "", err
	}
	link.Put_rpc(Frame(data))

	rawv, err := py.Wait_for(func() (any, error) { return Read_frame(reader) }, s.Handshake_timeout)
	if err != nil {
		link.Close()
		if py.IsInstance(err, py.TimeoutError) {
			return "", py.ConnectionError.New("Peer handshake timed out.").From(err)
		}
		return "", err
	}
	raw, _ := rawv.([]byte)
	if raw == nil {
		link.Close()
		return "", py.ConnectionError.New("Peer closed during handshake.")
	}
	if Is_reserved_frame(raw) {
		link.Close()
		return "", py.ConnectionError.New("Peer sent a stream frame before completing the handshake.")
	}

	msg, err := s.Decode_msg_(raw)
	if err != nil {
		link.Close()
		return "", err
	}
	if e, found := msg.Get("error"); found {
		link.Close()
		return "", py.ConnectionError.Newf("Peer rejected connection: %s", py.Str(error_message_(e)))
	}
	result := dict_or_empty_(msg.GetDefault("result", nil))
	peer_id := result.GetDefault("peer_id", nil)
	if py.IsNone(peer_id) {
		link.Close()
		return "", py.ConnectionError.New("Peer response missing peer_id.")
	}

	pid := py.Str(peer_id)
	s.Set_peer_caps_(pid, result.GetDefault("caps", nil))
	s.hooks().Register_peer_(pid, link)
	if _, err := py.Ensure_future(func() (any, error) { return nil, s.Receive_loop_(reader, link, pid) }); err != nil {
		return "", err
	}
	return pid, nil
}

// Handle_inbound_stream_ is the server side of the handshake, then the
// shared receive loop.
func (s *StreamRPCProtocol_) Handle_inbound_stream_(reader *py.StreamReader, writer *py.StreamWriter) error {
	rawv, err := py.Wait_for(func() (any, error) { return Read_frame(reader) }, s.Handshake_timeout)
	if err != nil {
		if py.IsInstance(err, py.TimeoutError) {
			writer.Close()
			return nil
		}
		return err
	}
	raw, _ := rawv.([]byte)
	if raw == nil || Is_reserved_frame(raw) {
		// EOF, or a stream frame before the handshake: not a peer.
		writer.Close()
		return nil
	}

	s.stream_hooks().On_stream_ready_(writer)
	link := s.Make_link_(writer)
	msg, err := s.Decode_msg_(raw)
	if err != nil {
		link.Close()
		return err
	}
	put := func(resp *py.Dict) error {
		data, err := s.Encode_(resp)
		if err != nil {
			return err
		}
		link.Put_rpc(Frame(data))
		return nil
	}
	if !Is_request(msg) || py.Str(msg.GetDefault("method", nil)) != PEER_CONNECT_ {
		if err := put(Make_error(msg.GetDefault("id", nil), ERR_INVALID_REQUEST, "First message must be a peer.connect request.", nil)); err != nil {
			return err
		}
		_ = link.Flush()
		link.Close()
		return nil
	}

	params := dict_or_empty_(msg.GetDefault("params", nil))
	peer_id := params.GetDefault("from_id", nil)
	if !py.Eq(params.GetDefault("secret", nil), s.Peer_secret_key) {
		if err := put(Make_error(msg.GetDefault("id", nil), ERR_AUTH_FAILED, "Invalid peer secret key.", nil)); err != nil {
			return err
		}
		_ = link.Flush()
		link.Close()
		return nil
	}

	if err := put(Make_result(msg.GetDefault("id", nil), py.DictOf("peer_id", s.Policy_id_(), "caps", s.hooks().Local_caps_()))); err != nil {
		return err
	}

	pid := py.Str(peer_id)
	s.Set_peer_caps_(pid, params.GetDefault("caps", nil))
	s.hooks().Register_peer_(pid, link)
	return s.Receive_loop_(reader, link, pid)
}

// Receive_loop_ decodes frames and routes requests/responses/stream
// chunks until EOF.
func (s *StreamRPCProtocol_) Receive_loop_(reader *py.StreamReader, link *Link_, peer_id string) error {
	defer s.hooks().Unregister_peer_(peer_id)
	reply := s.Make_reply_(link)
	err := func() error {
		for {
			raw, err := Read_frame(reader)
			if err != nil {
				return err
			}
			if raw == nil {
				return nil
			}
			if Is_reserved_frame(raw) {
				s.On_stream_frame_(peer_id, raw)
				continue
			}
			msg, err := s.Decode_msg_(raw)
			if err != nil {
				return err
			}
			if Is_request(msg) {
				if Is_goodbye_(msg) {
					// peer is dropping us gracefully: end this stream now
					return nil
				}
				s.Handle_request_frame_(msg, reply, peer_id)
			} else if Is_response(msg) {
				s.Complete_pending_(msg)
			}
		}
	}()
	if err != nil && !py.IsInstance(err, py.CancelledError) && !py.IsInstance(err, py.ConnectionError) {
		stream_carrier_log_.Debug("Stream receive loop for peer %s ended", peer_id, py.Kw{"exc_info": err})
	}
	return nil
}

// Make_reply_ builds a thread-safe ``reply(resp)`` that frames + queues
// on the link.
//
// Used by Handle_request_frame_; safe to call both inline on the I/O loop
// (ping / control fast-path) and from an inbound worker thread.
func (s *StreamRPCProtocol_) Make_reply_(link *Link_) func(resp *py.Dict) {
	return func(resp *py.Dict) {
		data, err := s.Encode_(resp)
		if err != nil {
			return
		}
		framed := Frame(data)
		_ = s.Loop_call_(func() { link.Put_rpc(framed) })
	}
}

// ------------------------------------------------------------------
// Outbound RPC / stream
// ------------------------------------------------------------------

// Queue_rpc_ frames *msg* and queues it on the peer's link from any
// thread.
func (s *StreamRPCProtocol_) Queue_rpc_(peer_id string, msg *py.Dict) error {
	link := s.Link_of_(peer_id)
	if link == nil || link.Closed {
		return py.ConnectionError.Newf("No connection to peer %s", peer_id)
	}
	data, err := s.Encode_(msg)
	if err != nil {
		return err
	}
	framed := Frame(data)
	return s.Loop_call_(func() { link.Put_rpc(framed) })
}

// Send_once_ sends one ``rpc.call`` to *peer_id* and blocks for the
// response.
func (s *StreamRPCProtocol_) Send_once_(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	if !s.Connections_.Contains(peer_id) {
		return nil, py.ConnectionError.Newf("No connection to peer %s", peer_id)
	}
	request_id, slot := s.Register_pending_(peer_id)
	req := s.Make_rpc_request_(path, args, kwargs, request_id)
	if err := s.Queue_rpc_(peer_id, req); err != nil {
		s.Pending_rpcs_.Pop(request_id)
		return nil, err
	}
	return s.Await_pending_(request_id, slot)
}

// Send_oneway_ queues a control request without waiting for the reply.
func (s *StreamRPCProtocol_) Send_oneway_(peer_id string, path []string, kwargs py.Kw) error {
	return s.Queue_rpc_(peer_id, s.Make_rpc_request_(path, nil, kwargs, py.Uuid4().String()))
}

// Stream_enqueue_ marshals one stream message onto the peer's link (user
// thread).
func (s *StreamRPCProtocol_) Stream_enqueue_(channel *Channel, payload []byte) error {
	link := s.Link_of_(channel.Peer_id)
	if link == nil || link.Closed {
		return py.ConnectionError.Newf("No connection to peer %s", channel.Peer_id)
	}
	return s.Loop_call_(func() { link.Put_stream(channel, payload) })
}
