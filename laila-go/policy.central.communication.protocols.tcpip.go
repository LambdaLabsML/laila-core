package laila

import (
	"fmt"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/tcpip.py -- TCP/IP (WebSocket)
// communication protocol implementation.
//
// Concrete LAILA_IDENTIFIABLE_COMM_PROTOCOL_ that uses the websockets
// layer to expose a long-lived bidirectional channel between two
// policies. The on-the-wire RPC envelope is defined in ``protocol``, the
// actual socket plumbing lives in ``connection``.
//
// Architecturally the protocol owns:
//
//   - A dedicated background asyncio event loop running on a daemon
//     thread (``_event_loop`` / ``_loop_thread``). All websocket I/O is
//     scheduled there so the rest of laila does not have to be
//     asyncio-aware.
//   - A websocket *server* (``_server``) accepting inbound peerings.
//   - A dict of *connections* (``_connections``) keyed by remote-policy
//     ``global_id``, each holding the live websocket for that peer.
//   - A *pending-RPC table* (``_pending_rpcs``) keyed by request id. Each
//     outbound Send_rpc registers a slot here, waits on a threading.Event,
//     and is woken up by the inbound dispatcher when the matching response
//     arrives.

var tcpip_log_ = py.Get_logger("laila.policy.central.communication.protocols.tcpip")

// Pending_rpc_slot_ is one entry of the pending-RPC table (Python: a
// ``dict`` with ``event`` / ``peer_id`` / ``result`` / ``error`` /
// ``exception`` keys).
type Pending_rpc_slot_ struct {
	mu        sync.Mutex
	Event     *py.Event
	Peer_id   string
	result    any
	has_res   bool
	err       any
	has_err   bool
	exception error
}

// New_pending_rpc_slot_ is ``{"event": threading.Event(), "peer_id": peer_id}``.
func New_pending_rpc_slot_(peer_id string) *Pending_rpc_slot_ {
	return &Pending_rpc_slot_{Event: py.NewEvent(), Peer_id: peer_id}
}

// Set_result is ``slot["result"] = v``.
func (s *Pending_rpc_slot_) Set_result(v any) {
	s.mu.Lock()
	s.result, s.has_res = v, true
	s.mu.Unlock()
}

// Set_error is ``slot["error"] = v``.
func (s *Pending_rpc_slot_) Set_error(v any) {
	s.mu.Lock()
	s.err, s.has_err = v, true
	s.mu.Unlock()
}

// Setdefault_exception is ``slot.setdefault("exception", exc)``.
func (s *Pending_rpc_slot_) Setdefault_exception(exc error) {
	s.mu.Lock()
	if s.exception == nil {
		s.exception = exc
	}
	s.mu.Unlock()
}

// Result is ``slot.get("result")``.
func (s *Pending_rpc_slot_) Result() any { s.mu.Lock(); defer s.mu.Unlock(); return s.result }

// Error_ is ``slot["error"]`` and ``"error" in slot``.
func (s *Pending_rpc_slot_) Error_() (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err, s.has_err
}

// Exception is ``slot.get("exception")``.
func (s *Pending_rpc_slot_) Exception() error { s.mu.Lock(); defer s.mu.Unlock(); return s.exception }

// Pending_rpcs_ is the thread-safe pending-RPC table.
type Pending_rpcs_ struct {
	mu    sync.Mutex
	slots map[string]*Pending_rpc_slot_
}

// New_pending_rpcs_ is ``PrivateAttr(default_factory=dict)``.
func New_pending_rpcs_() *Pending_rpcs_ { return &Pending_rpcs_{slots: map[string]*Pending_rpc_slot_{}} }

// Get is ``self._pending_rpcs.get(request_id)``.
func (p *Pending_rpcs_) Get(request_id string) *Pending_rpc_slot_ {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.slots[request_id]
}

// Set is ``self._pending_rpcs[request_id] = slot``.
func (p *Pending_rpcs_) Set(request_id string, slot *Pending_rpc_slot_) {
	p.mu.Lock()
	p.slots[request_id] = slot
	p.mu.Unlock()
}

// Pop is ``self._pending_rpcs.pop(request_id, None)``.
func (p *Pending_rpcs_) Pop(request_id string) *Pending_rpc_slot_ {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.slots[request_id]
	delete(p.slots, request_id)
	return s
}

// Items is ``list(self._pending_rpcs.items())``.
func (p *Pending_rpcs_) Items() []struct {
	Id   string
	Slot *Pending_rpc_slot_
} {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]struct {
		Id   string
		Slot *Pending_rpc_slot_
	}, 0, len(p.slots))
	for k, v := range p.slots {
		out = append(out, struct {
			Id   string
			Slot *Pending_rpc_slot_
		}{k, v})
	}
	return out
}

// Len is ``len(self._pending_rpcs)``.
func (p *Pending_rpcs_) Len() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.slots) }

// Clear is ``dict.clear()``.
func (p *Pending_rpcs_) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.slots = map[string]*Pending_rpc_slot_{}
}

// LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_ is the WebSocket-based
// peer-to-peer communication protocol.
//
// Implements the LAILA_IDENTIFIABLE_COMM_PROTOCOL_ contract over
// ``ws://`` / ``wss://``. RPC frames are JSON-encoded by ``protocol``;
// the connection state machine (handshake, message dispatch, disconnect)
// lives in ``connection``.
//
// Fields:
//
//   - Host (default ``"0.0.0.0"``) -- bind address for the WebSocket
//     server. ``"0.0.0.0"`` listens on every interface; pass a specific IP
//     to restrict reachability.
//   - Port (default ``0``) -- TCP port for the WebSocket server. ``0``
//     lets the OS pick a free port; the chosen port is then exposed via
//     Bound_port.
//   - Peer_secret_key -- shared secret that remote peers must present
//     during the handshake. Defaults to a fresh UUID4 hex if not set, so
//     each instance gets a unique secret out of the box.
//
// The protocol auto-bootstraps its own background event loop the first
// time Start is called, and tears it down on Stop. Clients of this class
// never see the loop directly -- they call Send_rpc / ``add_peer``
// synchronously and the loop is hidden behind
// py.Run_coroutine_threadsafe.
type LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_ struct {
	LAILA_IDENTIFIABLE_COMM_PROTOCOL_
	Loop_thread_state_

	Host            string `py:"host" default:"0.0.0.0"`
	Port            int    `py:"port" default:"0"`
	Peer_secret_key string `py:"peer_secret_key,factory=Default_peer_secret_key_"`
	// Rpc_timeout is the seconds a blocking Send_rpc waits for the response.
	Rpc_timeout float64 `py:"rpc_timeout" default:"60.0"`
	// Ping_timeout bounds a liveness Ping round trip.
	Ping_timeout float64 `py:"ping_timeout" default:"5.0"`

	Server_       *py.WebSocketServer `py:"_server"`
	Connections_  *py.Dict            `py:"_connections,factory=New_connections_"`
	Pending_rpcs_ *Pending_rpcs_      `py:"_pending_rpcs,factory=New_pending_rpcs_"`
	Started_      bool                `py:"_started"`
	Bound_port_   *int                `py:"_bound_port"`
}

// LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_class is the class descriptor.
//
// ``protocol_name = "tcpip"``; WebSocket-focused tokens: raw TCP/UDP/TLS
// have dedicated transports under ``protocols/ip_app/`` that own the
// ``tcp`` / ``udp`` / ``tls`` tokens, so ``tcpip`` claims only the
// WebSocket family (``tcpip``, ``ws``, ``wss``, ``websocket``) to keep
// the token map globally disjoint. ``can_handle_uri`` claims ``ws://``
// and ``wss://`` URIs.
var LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_)(nil),
	LAILA_IDENTIFIABLE_COMM_PROTOCOL_class,
).
	Set_classvar("protocol_name", "tcpip").
	Set_classvar("_TOKEN_ALIASES", py.NewSet("tcpip", "ws", "wss", "websocket")).
	Set_classvar("matches_token", Token_aliases_classmethod_("tcpip", "ws", "wss", "websocket")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("ws://", "wss://"))

// DefaultTCPIPProtocol / DefaultWebSocketProtocol are the macros.defaults
// aliases for the WebSocket transport class.
var (
	DefaultTCPIPProtocol     = LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_class
	DefaultWebSocketProtocol = LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_class
)

// New_DefaultTCPIPProtocol is ``DefaultTCPIPProtocol(**kw)``.
func New_DefaultTCPIPProtocol(kw ...py.Kw) (*LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_, error) {
	return New_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_(kw...)
}

// New_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Default_peer_secret_key_ is ``Field(default_factory=lambda: uuid4().hex)``.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Default_peer_secret_key_() string {
	return py.Uuid4().Hex()
}

// New_connections_ is ``PrivateAttr(default_factory=dict)``.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) New_connections_() *py.Dict { return py.NewDict() }

// New_pending_rpcs_ is ``PrivateAttr(default_factory=dict)``.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) New_pending_rpcs_() *Pending_rpcs_ {
	return New_pending_rpcs_()
}

// ``bound_port`` / ``bound_path`` are ``@property`` on every carrier that
// exposes them (tcpip, tcp, udp, unixsocket).
func init() { py.Register_property("bound_port", "bound_path") }

// Bound_port is the port the server is currently listening on.
//
// After Start, this returns the actual OS-assigned port -- useful when
// ``port=0`` was requested. Before Start it falls back to the configured
// Port (which may still be ``0``).
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Bound_port() int {
	if p.Bound_port_ != nil {
		return *p.Bound_port_
	}
	return p.Port
}

// Pending_rpc_ is ``self._pending_rpcs.get(request_id)``.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Pending_rpc_(request_id string) *Pending_rpc_slot_ {
	return p.pending_().Get(request_id)
}

func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) pending_() *Pending_rpcs_ {
	if p.Pending_rpcs_ == nil {
		p.Pending_rpcs_ = New_pending_rpcs_()
	}
	return p.Pending_rpcs_
}

func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) connections_() *py.Dict {
	if p.Connections_ == nil {
		p.Connections_ = py.NewDict()
	}
	return p.Connections_
}

// ------------------------------------------------------------------
// URI routing
// ------------------------------------------------------------------

// Matches_token accepts ``"tcpip"`` plus the WebSocket aliases (``ws``,
// ``wss``, ``websocket``).
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_class, token)
}

// Can_handle_uri claims ``ws://`` and ``wss://`` URIs for this protocol.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_class, uri)
}

// ------------------------------------------------------------------
// Lifecycle
// ------------------------------------------------------------------

// Start boots the dedicated event loop and starts accepting WebSocket
// connections.
//
// Idempotent: a second call is a no-op once ``_started`` is set. Spawns a
// daemon thread (``_loop_thread``) that owns a fresh event loop, kicks
// off the server via Async_start_, then drops into ``loop.run_forever()``.
// Blocks the caller until the server is bound and ready (or ten seconds
// elapse), so by the time this returns Bound_port is meaningful.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Start() error {
	if p.Started_ {
		return nil
	}

	if err := Start_loop_thread(p, p.Async_start_, 10.0); err != nil {
		return err
	}
	p.Started_ = true
	tcpip_log_.Info(
		"TCP/IP protocol started for policy %s on port %s",
		py.Str(p.Policy_id_()),
		py.Str(p.Bound_port()),
	)
	return nil
}

// Async_start_ boots the server inside the protocol's event loop and
// signals *ready*.
//
// Delegates to Start_server, which performs the actual websockets serve
// call and stashes the resulting server handle on ``self._server``. The
// *ready* event lets the synchronous Start caller block until the
// listener is actually bound.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Async_start_(ready *py.Event) error {
	if err := Start_server(p); err != nil {
		return err
	}
	ready.Set()
	return nil
}

// Stop tears down the server, closes every peer socket, and stops the
// loop.
//
// Idempotent: returns immediately when ``_started`` is False. Closes
// every peer socket (a WebSocket close frame is the graceful goodbye; the
// remote's receive loop ends and it unregisters us), closes the server,
// cancels *and awaits* every outstanding task, joins the loop thread and
// closes the loop, then clears the pending-RPC table so a subsequent
// Start brings the protocol back up cleanly.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Stop() error {
	if !p.Started_ {
		return nil
	}

	shutdown := func() error {
		var close_tasks []func() (any, error)
		for _, v := range p.connections_().Values() {
			ws := v.(*py.WebSocket)
			t, err := py.Ensure_future(func() (any, error) { return nil, ws.Close() })
			if err != nil {
				return err
			}
			close_tasks = append(close_tasks, t.Await)
		}
		if len(close_tasks) > 0 {
			_, _ = py.Gather(close_tasks, py.Kw{"return_exceptions": true})
		}
		for _, k := range p.connections_().Keys() {
			p.Unregister_peer_(py.Str(k))
		}

		if p.Server_ != nil {
			p.Server_.Close()
			_, _ = py.Wait_for(func() (any, error) { return nil, p.Server_.Wait_closed() }, 2.0)
			p.Server_ = nil
		}

		return Cancel_pending_tasks()
	}

	Stop_loop_thread(p, shutdown)
	p.Fail_pending_("transport stopped", nil)
	p.Bound_port_ = nil
	p.Started_ = false
	tcpip_log_.Info("TCP/IP protocol stopped for policy %s", py.Str(p.Policy_id_()))
	return nil
}

// ------------------------------------------------------------------
// Peer management
// ------------------------------------------------------------------

// Connect opens an outbound WebSocket connection to *uri* and completes
// the handshake.
//
// Auto-starts the protocol if needed (so users don't need to remember a
// separate Start call). The actual handshake is run inside the protocol's
// event loop via Connect_outbound; this function blocks the caller for up
// to thirty seconds for the handshake to finish. Returns the remote
// policy's ``global_id`` as resolved during the handshake.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Connect(uri string, secret string) (string, error) {
	if err := p.Start(); err != nil {
		return "", err
	}

	future := py.Run_coroutine_threadsafe(
		func() (any, error) { return Connect_outbound(p, uri, secret) },
		p.Loop(),
	)
	res, err := future.Result(30.0)
	if err != nil {
		return "", err
	}
	return py.Str(res), nil
}

// Register_peer_ records an established WebSocket and notifies the
// communication layer.
//
// Called by ``connection`` after a successful handshake (in either
// direction). The communication layer creates the corresponding
// RemotePolicyProxy so user code can immediately reach the new peer.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Register_peer_(peer_id string, ws *py.WebSocket) {
	p.connections_().Set(peer_id, ws)
	if c := p.Communication(); c != nil {
		c.Register_peer_(peer_id)
	}
}

// Unregister_peer_ drops a peer's WebSocket and notifies the
// communication layer.
//
// Called when the connection closes (either side). Idempotent: if the
// peer is not in the table, this is a no-op. Every RPC still in flight to
// that peer fails with ConnectionError right away instead of sitting out
// Rpc_timeout.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Unregister_peer_(peer_id string) {
	p.connections_().PopDefault(peer_id, nil)
	p.Fail_pending_(fmt.Sprintf("peer %s disconnected", peer_id), &peer_id)
	if c := p.Communication(); c != nil {
		c.Unregister_peer_(peer_id)
	}
}

// Fail_pending_ fails outstanding outbound RPCs (all, or those to
// *peer_id*) with ConnectionError.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Fail_pending_(reason string, peer_id *string) {
	for _, it := range p.pending_().Items() {
		if peer_id != nil && it.Slot.Peer_id != *peer_id {
			continue
		}
		it.Slot.Setdefault_exception(py.ConnectionError.New(reason))
		it.Slot.Event.Set()
		p.pending_().Pop(it.Id)
	}
}

// Has_peer returns ``True`` if a live WebSocket to *peer_id* is currently
// held.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Has_peer(peer_id string) bool {
	return p.connections_().Contains(peer_id)
}

// Disconnect closes the WebSocket to *peer_id* and drops it. Idempotent.
//
// The close handshake is the graceful goodbye: the remote's receive loop
// ends with ConnectionClosed and unregisters us. Bounded so a dead peer
// cannot stall the caller.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Disconnect(peer_id string) error {
	v, ok := p.connections_().Get(peer_id)
	if !ok {
		return nil
	}
	ws := v.(*py.WebSocket)
	loop := p.Loop()
	if loop != nil && !loop.Is_closed() && loop.Is_running() {
		fut := py.Run_coroutine_threadsafe(func() (any, error) { return nil, ws.Close() }, loop)
		if _, err := fut.Result(2.0); err != nil {
			tcpip_log_.Debug("closing WebSocket to %s failed", peer_id, py.Kw{"exc_info": err})
		}
	}
	p.Unregister_peer_(peer_id)
	return nil
}

// ------------------------------------------------------------------
// RPC (outbound)
// ------------------------------------------------------------------

// Ping round-trips the reserved ``__comm_ping__`` frame to *peer_id*.
//
// Answered by the remote's ``connection`` dispatcher before it touches
// the policy. ``False`` on any transport error or timeout, so the
// liveness loop drops only peers that really stopped answering.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Ping(peer_id string, timeout ...any) bool {
	if !p.Has_peer(peer_id) {
		return false
	}
	t := p.Ping_timeout
	if len(timeout) > 0 && !py.IsNone(timeout[0]) {
		if f, err := py.Float(timeout[0]); err == nil {
			t = f
		}
	}
	res, err := p.Send_(peer_id, []string{"__comm_ping__"}, nil, py.Kw{}, t)
	if err != nil {
		return false
	}
	return py.Eq(res, "pong")
}

// Send_rpc sends an RPC call over the WebSocket to *peer_id* and blocks
// for the response.
//
// Allocates a fresh request id, registers a pending-RPC slot keyed by
// that id, dispatches the encoded frame from the protocol's event loop,
// and waits on a threading.Event for up to Rpc_timeout seconds. The
// inbound dispatcher in ``connection`` flips the event when the matching
// response arrives.
//
// Raises ConnectionError if no live connection to *peer_id* is held,
// TimeoutError if no response arrived within Rpc_timeout, RuntimeError
// if the remote returned an error envelope.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Send_rpc(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	return p.Send_(peer_id, path, args, kwargs, p.Rpc_timeout)
}

// Send_ is the shared blocking send used by Send_rpc and Ping.
func (p *LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL_) Send_(peer_id string, path []string, args []any, kwargs py.Kw, timeout float64) (any, error) {
	v, ok := p.connections_().Get(peer_id)
	if !ok {
		return nil, py.ConnectionError.Newf("No connection to peer %s", peer_id)
	}
	ws := v.(*py.WebSocket)
	loop := p.Loop()
	if loop == nil || loop.Is_closed() {
		return nil, py.ConnectionError.New("TCP/IP transport is shut down.")
	}

	request_id := py.Uuid4().String()
	req := Make_request(
		"rpc.call",
		py.DictOf(
			"path", py.NewList(any_strings_(path)...),
			"args", py.NewList(args...),
			"kwargs", py.DictFromKw(kwargs),
		),
		request_id,
	)

	frame, err := Encode(req)
	if err != nil {
		return nil, err
	}
	if len(frame) > MAX_FRAME_BYTES {
		// The peer would close the socket (1009) on receipt; refuse up
		// front so the link survives and the caller learns why.
		return nil, py.ConnectionError.Newf(
			"RPC frame of %d bytes exceeds the %d byte WebSocket frame limit for peer %s.",
			len(frame), MAX_FRAME_BYTES, peer_id)
	}

	slot := New_pending_rpc_slot_(peer_id)
	event := slot.Event
	p.pending_().Set(request_id, slot)

	sent := func(f *py.CFuture) {
		var exc error
		if !f.Cancelled() {
			exc, _ = f.Exception(0)
		}
		if exc != nil {
			slot.Setdefault_exception(py.ConnectionError.Newf("send to peer %s failed: %s", peer_id, py.Repr(exc)))
			event.Set()
		}
	}

	if loop.Is_closed() {
		p.pending_().Pop(request_id)
		return nil, py.ConnectionError.New("TCP/IP transport is shut down.")
	}
	py.Run_coroutine_threadsafe(func() (any, error) { return nil, ws.Send(frame) }, loop).Add_done_callback(sent)

	completed := event.Wait(timeout)
	p.pending_().Pop(request_id)
	if !completed {
		return nil, py.TimeoutError.Newf("RPC to peer %s timed out after %ss.", peer_id, py.Str(timeout))
	}

	if exc := slot.Exception(); exc != nil {
		return nil, exc
	}

	if e, found := slot.Error_(); found {
		return nil, py.RuntimeError.Newf("Remote RPC error: %s", py.Str(error_message_(e)))
	}

	return slot.Result(), nil
}

// any_strings_ widens []string to []any.
func any_strings_(s []string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}
