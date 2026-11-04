package laila

import (
	"fmt"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/schema/base.py -- core communication
// sub-system for inter-policy peer-to-peer RPC.
//
// The LAILA_IDENTIFIABLE_COMMUNICATION_ class is the *central
// communication* subsystem of every laila Policy. Its job is to mediate
// calls between local and remote policies without any of the upstream
// code having to know which transport is in play. Concretely it owns
// three things:
//
//   - A *protocol registry* (``connections``): one or more transport-level
//     drivers (TCP/IP today, shared memory or queues in the future). Each
//     protocol implements its own listener loop, peer-handshake, and wire
//     encoding. The communication object only ever asks "do you handle
//     this URI?" or "do you have this peer connected?" -- everything else
//     is delegated.
//   - A *peer registry* (``peers``): a PeerRegistry (a ``dict``) from
//     remote-policy ``global_id`` to a PeerProxy. Proxies look like local
//     policies to the rest of the codebase but route every method call
//     through Send_rpc_; indexing one with a channel name
//     (``peers[gid]["video"]``) yields a stream Channel.
//   - An *inbound dispatcher* (Execute_rpc_): when a protocol finishes
//     deserializing an incoming RPC frame it hands the dotted attribute
//     path + args/kwargs back to the communication object, which walks the
//     path on the local policy and invokes the resolved method.
//
// The class is also responsible for *future virtualisation*: when a
// remote call returns a future-shaped envelope (as marked by the
// ``__laila_future__`` key) it transparently wraps the envelope in a
// RemoteFuture that proxies status / wait / result calls back to the
// originating peer (see Maybe_wrap_remote_future_).

var comm_log_ = py.Get_logger("laila.policy.central.communication.schema.base")

// Coerce_peer_registry_ accepts a plain mapping for ``peers`` and
// upgrades it to PeerRegistry.
func Coerce_peer_registry_(value any) any {
	if r, ok := value.(*PeerRegistry); ok {
		return r
	}
	if d, ok := value.(*py.Dict); ok {
		return New_PeerRegistry(d)
	}
	if m, ok := value.(map[string]any); ok {
		return New_PeerRegistry(py.DictFromMap(m))
	}
	return value
}

// LAILA_IDENTIFIABLE_COMMUNICATION_ is the central-communication hub for a
// policy.
//
// Owns transport protocols, the peer registry, and an inbound RPC
// dispatcher. The full interaction loop is:
//
//	local user code
//	    -> RemotePolicyProxy.foo()
//	    -> Communication._send_rpc(...)
//	    -> Protocol.send_rpc(...)
//	    ~~ wire ~~
//	    -> remote Protocol receives, decodes
//	    -> remote Communication._execute_rpc(["foo"], args, kwargs)
//	    -> result returned along the same path in reverse
//
// Fields:
//
//   - Policy_id -- ``global_id`` of the owning policy. Wired automatically
//     by the policy during construction; only set manually in tests or when
//     a communication object lives outside a policy.
//   - Peers -- remote-policy ``global_id`` -> PeerProxy. Populated by
//     Register_peer_ after a successful handshake. Plain dict semantics;
//     ``peers[gid][name]`` resolves a stream channel.
//   - Connections -- protocol ``global_id`` -> protocol instance. Each
//     protocol manages its own connections, listeners and peer set; this
//     map is just a registry that lets the communication layer find the
//     right transport for a URI or peer.
type LAILA_IDENTIFIABLE_COMMUNICATION_ struct {
	LAILA_CLI_CAPABLE_CLASS_
	LAILA_IDENTIFIABLE_OBJECT_

	Policy_id   any           `py:"policy_id,cli_exempt"`
	Peers       *PeerRegistry `py:"peers,cli_exempt,factory=New_peers_"`
	Connections *py.Dict      `py:"connections,cli_exempt,factory=New_connections_"`

	// Liveness_interval is how often (seconds) the async liveness loop
	// pings each peer.
	Liveness_interval float64 `py:"liveness_interval" default:"15.0"`
	// Liveness_enabled is whether the async liveness loop runs at all.
	Liveness_enabled bool `py:"liveness_enabled" default:"true"`

	// Max_inflight_rpcs is the max inbound RPCs this policy will run/queue
	// concurrently across all transports before rejecting new ones with
	// ``ERR_BUSY``. Bounds the per-policy backlog so a flood cannot exhaust
	// memory; embedded / low-resource configs should set this low (e.g.
	// 32-64) via ``laila.args``. Liveness pings bypass this cap entirely.
	Max_inflight_rpcs int `py:"max_inflight_rpcs" default:"1000"`

	Local_policy_       any           `py:"_local_policy"`
	Liveness_thread_    *py.Thread    `py:"_liveness_thread"`
	Liveness_stop_      *py.Event     `py:"_liveness_stop"`
	Rpc_semaphore_      *py.Semaphore `py:"_rpc_semaphore"`
	Rpc_semaphore_lock_ *sync.Mutex   `py:"_rpc_semaphore_lock"`
}

// LAILA_IDENTIFIABLE_COMMUNICATION_class is the class descriptor;
// ``_scopes`` defaults to ``[_CENTRAL_COMMUNICATION_SCOPE]``, ``peers``
// accepts a plain mapping (BeforeValidator(_coerce_peer_registry)).
var LAILA_IDENTIFIABLE_COMMUNICATION_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_COMMUNICATION", (*LAILA_IDENTIFIABLE_COMMUNICATION_)(nil),
	LAILA_CLI_CAPABLE_CLASS_class, LAILA_IDENTIFIABLE_OBJECT_class,
).
	Set_private_default("_scopes", func() any { return []string{CENTRAL_COMMUNICATION_SCOPE_} }).
	Add_field_validator("peers", func(self any, v any) (any, error) { return Coerce_peer_registry_(v), nil })

// New_LAILA_IDENTIFIABLE_COMMUNICATION_ is
// ``_LAILA_IDENTIFIABLE_COMMUNICATION(**data)``.
func New_LAILA_IDENTIFIABLE_COMMUNICATION_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_COMMUNICATION_, error) {
	c := LAILA_IDENTIFIABLE_COMMUNICATION_class.New().(*LAILA_IDENTIFIABLE_COMMUNICATION_)
	if err := c.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return c, nil
}

// New_DefaultCentralCommunication is ``DefaultCentralCommunication(**kw)``.
func New_DefaultCentralCommunication(kw ...py.Kw) (*LAILA_IDENTIFIABLE_COMMUNICATION_, error) {
	return New_LAILA_IDENTIFIABLE_COMMUNICATION_(kw...)
}

// New_peers_ is ``Field(default_factory=PeerRegistry)``.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) New_peers_() *PeerRegistry { return New_PeerRegistry() }

// New_connections_ is ``Field(default_factory=dict)``.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) New_connections_() *py.Dict { return py.NewDict() }

// Communication_base_ exposes the embedded hub to code holding a subclass.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Communication_base_() *LAILA_IDENTIFIABLE_COMMUNICATION_ {
	return c
}

// As_communication_ returns the embedded hub of any derived instance (nil
// when v is not one).
func As_communication_(v any) *LAILA_IDENTIFIABLE_COMMUNICATION_ {
	if c, ok := v.(*LAILA_IDENTIFIABLE_COMMUNICATION_); ok {
		return c
	}
	if b, ok := v.(interface {
		Communication_base_() *LAILA_IDENTIFIABLE_COMMUNICATION_
	}); ok {
		return b.Communication_base_()
	}
	return nil
}

// Model_post_init runs the CLI-capable registration like every other
// CLI-capable class.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Model_post_init(ctx any) error {
	if err := c.LAILA_IDENTIFIABLE_OBJECT_.Model_post_init(ctx); err != nil {
		return err
	}
	if c.Peers == nil {
		c.Peers = New_PeerRegistry()
	}
	if c.Connections == nil {
		c.Connections = py.NewDict()
	}
	return CLI_CAPABLE_model_post_init(c.Self())
}

// protocols_ is ``self.connections.values()`` typed.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) protocols_() []Comm_protocol_ {
	out := make([]Comm_protocol_, 0, c.Connections.Len())
	for _, v := range c.Connections.Values() {
		if p, ok := v.(Comm_protocol_); ok {
			out = append(out, p)
		}
	}
	return out
}

// ------------------------------------------------------------------
// Protocol management
// ------------------------------------------------------------------

// Add_connection registers and starts a transport protocol.
//
// Sets the protocol's back-reference, adds it to the registry, and calls
// ``protocol.start()`` so the connection is live when this method
// returns. Symmetric with Remove_connection which calls ``protocol.stop()``.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Add_connection(protocol Comm_protocol_) error {
	protocol.Comm_protocol_base_().Communication_ = c.Self()
	c.Connections.Set(protocol.Global_id(), protocol.Self())
	return protocol.Start()
}

// Remove_connection stops a transport protocol and removes it from this
// communication instance.
//
// The protocol is responsible for all its own cleanup -- closing sockets,
// unregistering peers, etc. Communication only calls ``stop()`` and
// removes the protocol from its registry.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Remove_connection(protocol Comm_protocol_) error {
	proto_id := protocol.Global_id()
	if !c.Connections.Contains(proto_id) {
		return nil
	}
	if err := protocol.Stop(); err != nil {
		return err
	}
	c.Connections.PopDefault(proto_id, nil)
	protocol.Comm_protocol_base_().Communication_ = nil
	return nil
}

// Resolve_protocol_for_uri_ finds a registered protocol that can handle
// *uri*.
//
// Each protocol class implements a ``can_handle_uri`` classmethod (e.g.
// TCP/IP returns ``True`` for ``ws://`` and ``tcp://`` URIs). The first
// registered protocol that claims the URI is returned; if none claims it,
// the first registered protocol is returned as a best-effort fallback (the
// protocol may still error out).
//
// Raises ConnectionError if no protocols are registered.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Resolve_protocol_for_uri_(uri string) (Comm_protocol_, error) {
	protos := c.protocols_()
	if len(protos) == 0 {
		return nil, py.ConnectionError.New(
			"No communication protocols configured. Call add_connection() first.")
	}
	for _, proto := range protos {
		if Comm_protocol_can_handle_uri_(proto.Class_(), uri) {
			return proto, nil
		}
	}
	return protos[0], nil
}

// Resolve_protocol_for_token_ finds a registered protocol matching a
// transport *token*.
//
// Drives the ``comm_protocol`` argument of Request. Each protocol class
// implements Comm_protocol_matches_token_ (e.g. TCP/IP answers to
// ``"tcpip"`` / ``"tcp"`` / ``"ws"``). When *token* is ``None`` the first
// registered protocol is returned.
//
// Raises ConnectionError if no protocols are registered, or none matches
// *token*.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Resolve_protocol_for_token_(token any) (Comm_protocol_, error) {
	protos := c.protocols_()
	if len(protos) == 0 {
		return nil, py.ConnectionError.New(
			"No communication protocols configured. Call add_connection() first.")
	}
	if py.IsNone(token) {
		// Transport-agnostic default: the first registered protocol.
		return protos[0], nil
	}
	tok := py.Str(token)
	for _, proto := range protos {
		if Comm_protocol_matches_token_(proto.Class_(), tok) {
			return proto, nil
		}
	}
	return nil, py.ConnectionError.Newf(
		"No registered communication protocol matches %s. Register one with add_connection().",
		py.Repr(tok))
}

// ------------------------------------------------------------------
// Lifecycle
// ------------------------------------------------------------------

// Start starts every registered protocol's listener loop.
//
// Idempotent at the protocol level -- calling Start on an already-started
// protocol is a no-op. Logged at INFO so the policy lifecycle is
// auditable.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Start() error {
	for _, proto := range c.protocols_() {
		if err := proto.Start(); err != nil {
			return err
		}
	}
	c.Start_liveness_()
	comm_log_.Info("Communication started for policy %s", py.Str(c.Policy_id))
	return nil
}

// Stop stops every registered protocol and drops all peer proxies.
//
// Note that the protocols are responsible for breaking their own
// connections (each says goodbye to its peers, closes its endpoints and
// its event loop); this method just clears the in-memory peer registry
// afterwards so subsequent code does not try to talk to detached proxies.
// Best-effort: a protocol whose ``stop()`` raises is logged and the
// remaining ones still stop.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Stop() error {
	c.Stop_liveness_()
	for _, proto := range c.protocols_() {
		if err := proto.Stop(); err != nil {
			comm_log_.Warning(
				"Protocol %s failed to stop cleanly for policy %s",
				proto.Class_().Name, py.Str(c.Policy_id), py.Kw{"exc_info": err})
		}
	}
	for _, peer_id := range c.Peers.Keys() {
		c.Unregister_peer_(py.Str(peer_id))
	}
	comm_log_.Info("Communication stopped for policy %s", py.Str(c.Policy_id))
	return nil
}

// ------------------------------------------------------------------
// Peer management
// ------------------------------------------------------------------

// Add_peer initiates a peering connection to a remote policy.
//
// Resolves the appropriate protocol for *uri* (e.g. ``"ws://host:port"``)
// and delegates the transport-level handshake with the remote policy's
// ``peer_secret_key``. Returns the ``global_id`` of the newly peered
// remote policy.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Add_peer(uri string, secret string) (string, error) {
	proto, err := c.Resolve_protocol_for_uri_(uri)
	if err != nil {
		return "", err
	}
	return proto.Connect(uri, secret)
}

// Add_tcpip_peer peers with a remote policy over TCP/IP (WebSocket).
//
// Convenience wrapper that builds the ``ws://`` URI internally from the
// remote host and port; *secret* is the remote node's ``peer_secret_key``.
// Returns the ``global_id`` of the newly peered remote policy.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Add_tcpip_peer(host string, port int, secret string) (string, error) {
	return c.Add_peer(fmt.Sprintf("ws://%s:%d", host, port), secret)
}

// Remove_peer disconnects *peer_id* via whichever protocol holds it.
// Idempotent.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Remove_peer(peer_id string) {
	for _, proto := range c.protocols_() {
		if proto.Has_peer(peer_id) {
			_ = proto.Disconnect(peer_id)
			return
		}
	}
	c.Unregister_peer_(peer_id)
}

// Holder_protocol_ returns the registered protocol currently holding
// *peer_id*, if any.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Holder_protocol_(peer_id string) Comm_protocol_ {
	for _, proto := range c.protocols_() {
		if proto.Has_peer(peer_id) {
			return proto
		}
	}
	return nil
}

// ------------------------------------------------------------------
// Liveness (async per-peer ping loop)
// ------------------------------------------------------------------

// Start_liveness_ starts the background liveness loop (idempotent).
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Start_liveness_() {
	if !c.Liveness_enabled {
		return
	}
	if c.Liveness_thread_ != nil && c.Liveness_thread_.Is_alive() {
		return
	}
	c.Liveness_stop_ = py.NewEvent()
	// Snapshot the stop event and the interval on the *caller's* goroutine.
	// Python re-reads ``self.liveness_interval`` every iteration under the
	// GIL; in Go that read would race with a user assignment
	// (``comm.Liveness_interval = 0.2``). Callers that change the interval
	// restart the loop (``Stop_liveness_`` + ``Start_liveness_``), which is
	// the documented pattern, so observable behaviour is unchanged.
	stop, interval := c.Liveness_stop_, c.Liveness_interval
	c.Liveness_thread_ = py.New_Thread(py.Kw{
		"target": func() { c.liveness_loop_(stop, interval) },
		"daemon": true,
		"name":   fmt.Sprintf("comm-liveness-%s", py.Str(c.Policy_id)),
	})
	_ = c.Liveness_thread_.Start()
}

// Stop_liveness_ stops the background liveness loop (idempotent).
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Stop_liveness_() {
	if c.Liveness_stop_ != nil {
		c.Liveness_stop_.Set()
	}
	if c.Liveness_thread_ != nil {
		_ = c.Liveness_thread_.Join(2.0)
		c.Liveness_thread_ = nil
	}
	c.Liveness_stop_ = nil
}

// Liveness_loop_ pings each peer every ``liveness_interval`` and drops
// dead ones.
//
// Runs entirely off the data path on its own thread. Each ping is bounded
// by the protocol's ``ping_timeout`` (via a worker pool) so a
// silently-dead peer cannot stall the sweep. Only protocols that are
// ``persistent`` and ``supports_ping`` are probed.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Liveness_loop_() {
	c.liveness_loop_(c.Liveness_stop_, c.Liveness_interval)
}

func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) liveness_loop_(stop *py.Event, interval float64) {
	if stop == nil {
		return
	}
	pool := py.New_ThreadPoolExecutor(4, py.Kw{"thread_name_prefix": "comm-ping"})
	defer pool.Shutdown(true)
	for !stop.Wait(interval) {
		// snapshot keys so concurrent register/unregister is safe
		for _, k := range c.Peers.Keys() {
			peer_id := py.Str(k)
			proto := c.Holder_protocol_(peer_id)
			if proto == nil {
				continue
			}
			if !(Comm_protocol_persistent_(proto) && Comm_protocol_supports_ping_(proto)) {
				continue
			}
			deadline := 5.0
			if v, err := py.Getattr(proto, "ping_timeout"); err == nil && !py.IsNone(v) {
				if f, ferr := py.Float(v); ferr == nil {
					deadline = f
				}
			}
			alive := false
			fut, err := pool.Submit(func() (any, error) { return proto.Ping(peer_id), nil })
			if err == nil {
				if res, rerr := fut.Result(deadline); rerr == nil {
					alive, _ = res.(bool)
				}
			}
			if !alive {
				_ = proto.Disconnect(peer_id)
				c.Unregister_peer_(peer_id)
			}
		}
	}
}

// Register_peer_ creates a proxy for a newly connected peer.
//
// Called by protocol instances after a successful handshake. One PeerProxy
// object is built and stored in *both* ``self.peers`` and
// ``laila.remote_policies`` so ``laila.peers[gid] is
// laila._remote_policies[gid]``.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Register_peer_(peer_id string) {
	if !c.Peers.Contains(peer_id) {
		proxy := New_PeerProxy(peer_id, c.Self())
		c.Peers.Set(peer_id, proxy)
		Remote_policies_.Set(peer_id, proxy)
		// ensure liveness monitoring is running once we have a peer
		c.Start_liveness_()
	}
}

// Unregister_peer_ removes a peer proxy after the transport connection
// closes. Also removes from ``laila.remote_policies``.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Unregister_peer_(peer_id string) {
	c.Peers.PopDefault(peer_id, nil)
	Remote_policies_.PopDefault(peer_id, nil)
}

// ------------------------------------------------------------------
// Inbound admission control (backpressure)
// ------------------------------------------------------------------

// Rpc_gate_ lazily builds the shared bounded semaphore guarding inbound
// RPCs.
//
// One semaphore per policy (shared by every transport) sized to
// Max_inflight_rpcs, so the cap is a true *per-policy* budget rather than
// per-connection.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Rpc_gate_() *py.Semaphore {
	if c.Rpc_semaphore_lock_ == nil {
		c.Rpc_semaphore_lock_ = &sync.Mutex{}
	}
	if c.Rpc_semaphore_ == nil {
		c.Rpc_semaphore_lock_.Lock()
		if c.Rpc_semaphore_ == nil {
			c.Rpc_semaphore_ = py.NewBoundedSemaphore(max(1, c.Max_inflight_rpcs))
		}
		c.Rpc_semaphore_lock_.Unlock()
	}
	return c.Rpc_semaphore_
}

// Acquire_rpc_slot_ claims an inbound-RPC slot without blocking. ``True``
// if admitted.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Acquire_rpc_slot_() bool {
	return c.Rpc_gate_().Acquire(false, -1)
}

// Release_rpc_slot_ releases a previously-claimed inbound-RPC slot.
// Idempotent-safe (releasing more than acquired is ignored).
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Release_rpc_slot_() {
	_ = c.Rpc_gate_().Release(1)
}

// ------------------------------------------------------------------
// RPC dispatch (inbound)
// ------------------------------------------------------------------

// Execute_rpc_ executes a dotted-path method call on the local policy.
//
// *path* is the attribute chain relative to the local policy object, e.g.
// ``["central", "memory", "memorize"]``; *args* / *kwargs* are forwarded
// to the resolved callable. Returns the invoked method's return value.
// Raises AttributeError if any segment of *path* does not exist on the
// target, RuntimeError when the hub has no local policy.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Execute_rpc_(path []string, args []any, kwargs py.Kw) (any, error) {
	return c.execute_rpc_(path, args, kwargs)
}

// Execute_rpc_via_ is how carriers invoke ``comm._execute_rpc(path, args,
// kwargs)``: Python attribute lookup consults the instance dict before
// the class, so a patched ``_execute_rpc`` (``mock.patch.object`` /
// ``object.__setattr__``) takes effect. The Go equivalent is a callable
// bound with ``comm.Private_set("_execute_rpc", fn)``; absent that, the
// method itself runs. fn receives ``(path []string, args []any, kwargs
// py.Kw)`` as positionals.
func Execute_rpc_via_(c *LAILA_IDENTIFIABLE_COMMUNICATION_, path []string, args []any, kwargs py.Kw) (any, error) {
	if fn, ok := c.Private_get("_execute_rpc"); ok && !py.IsNone(fn) {
		return py.Call(fn, []any{path, args, kwargs}, nil)
	}
	return c.Execute_rpc_(path, args, kwargs)
}

func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) execute_rpc_(path []string, args []any, kwargs py.Kw) (any, error) {
	obj := c.Local_policy_
	if py.IsNone(obj) {
		return nil, py.RuntimeError.New("Communication has no reference to the local policy.")
	}
	for _, segment := range path {
		next, err := py.Getattr(obj, segment)
		if err != nil {
			return nil, err
		}
		obj = next
	}
	// Entries shipped by a morphed caller (``laila.memorize(entry)`` with
	// a peer active) arrive as serialized envelopes; rebuild them so the
	// target method receives real ``Entry`` objects.
	rargs, rkwargs, err := Rehydrate_entry_args(py.NewList(args...), py.DictFromKw(kwargs))
	if err != nil {
		return nil, err
	}
	return py.Call(obj, rargs, rkwargs)
}

// ------------------------------------------------------------------
// RPC dispatch (outbound)
// ------------------------------------------------------------------

// Select_protocol_for_peer_ picks the transport that carries the call to
// *peer_id*.
//
// *comm* is an optional *communication id* -- either a registered
// connection's ``global_id`` or a protocol token (e.g. ``"tcp"``,
// ``"lora"``). When given, that specific channel is used and must already
// hold the peer; when ``None`` the first registered protocol holding
// *peer_id* is used.
//
// Raises ConnectionError if the requested channel is unknown, or no
// channel holds the peer.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Select_protocol_for_peer_(peer_id string, comm any) (Comm_protocol_, error) {
	if !py.IsNone(comm) {
		key := py.Str(comm)
		var proto Comm_protocol_
		if v, ok := c.Connections.Get(key); ok {
			proto, _ = v.(Comm_protocol_)
		}
		if proto == nil {
			p, err := c.Resolve_protocol_for_token_(key)
			if err != nil && !py.IsInstance(err, py.ConnectionError) {
				return nil, err
			}
			proto = p
		}
		if proto == nil {
			return nil, py.ConnectionError.Newf(
				"Unknown communication channel %s: not a connection id or a registered protocol token.",
				py.Repr(comm))
		}
		if !proto.Has_peer(peer_id) {
			return nil, py.ConnectionError.Newf(
				"Communication channel %s has no connection to peer %s.", py.Repr(comm), peer_id)
		}
		return proto, nil
	}
	for _, proto := range c.protocols_() {
		if proto.Has_peer(peer_id) {
			return proto, nil
		}
	}
	return nil, py.ConnectionError.Newf("No connection to peer %s", peer_id)
}

// Send_rpc_ sends an RPC call to a peer over the selected transport.
//
// If the deserialized response contains a ``__laila_future__`` marker it
// is automatically wrapped in a RemoteFuture and registered in the local
// policy's ``future_bank``.
//
// *comm* (optional) is the communication id / protocol token selecting
// the transport; it is threaded into the returned RemoteFuture so its
// later ``status`` / ``wait`` / ``result`` calls stay on the same channel.
//
// Returns the deserialized return value from the remote call, or a
// RemoteFuture when the remote returned a future. Raises ConnectionError
// if no protocol holds a connection to *peer_id* (or the named channel
// does not), RuntimeError if the remote side returned an error.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Send_rpc_(peer_id string, path []string, args []any, kwargs py.Kw, comm any) (any, error) {
	proto, err := c.Select_protocol_for_peer_(peer_id, comm)
	if err != nil {
		return nil, err
	}
	result, err := proto.Send_rpc(peer_id, path, args, kwargs)
	if err != nil {
		return nil, err
	}
	return c.Maybe_wrap_remote_future_(result, peer_id, comm)
}

// Maybe_wrap_remote_future_ promotes a future-shaped envelope into a real
// RemoteFuture.
//
// Detected by the ``__laila_future__`` flag on the deserialized result.
// The envelope carries enough identity information (uuid, evolution,
// scopes, taskforce/policy ids) to reconstruct a stable identity-only
// RemoteFuture on the local side without round-tripping the underlying
// value. The new future is then bound to *self* so subsequent
// ``status``/``wait``/``result`` calls route back to *peer_id*.
//
// RemoteFuture.Model_post_init self-registers into the active local
// policy's ``future_bank`` and guarantee stack, so this helper only has
// to build the instance and call RemoteFuture.Bind to attach the
// communication channel.
//
// Group futures (``__is_group__``) are flagged so the bound proxy uses
// the GroupFuture-shaped status payload.
func (c *LAILA_IDENTIFIABLE_COMMUNICATION_) Maybe_wrap_remote_future_(result any, peer_id string, comm any) (any, error) {
	d, ok := result.(*py.Dict)
	if !ok {
		return result, nil
	}
	if flag, _ := d.Get("__laila_future__"); !py.Truthy(flag) {
		return result, nil
	}

	remote_gid, _ := d.Get("global_id")
	parsed, err := Process_global_id(remote_gid)
	if err != nil {
		if py.IsInstance(err, py.ValueError) {
			return nil, py.ValueError.Newf("Invalid remote future gid: %s", py.Repr(remote_gid)).From(err)
		}
		return nil, err
	}
	remote_uuid := parsed["uuid"]
	evolution := parsed["evolution"]
	is_group := false
	if g, found := d.Get("__is_group__"); found {
		is_group = py.Truthy(g)
	}

	scope := FUTURE_SCOPE_
	if is_group {
		scope = GROUP_FUTURE_SCOPE_
	}
	taskforce_id, found := d.Get("taskforce_id")
	if !found {
		taskforce_id = peer_id
	}
	policy_id, found := d.Get("policy_id")
	if !found {
		policy_id = peer_id
	}
	rf, err := New_RemoteFuture(py.Kw{
		"taskforce_id": taskforce_id,
		"policy_id":    policy_id,
		"uuid":         remote_uuid,
		"scopes":       []string{scope},
		"evolution":    evolution,
	})
	if err != nil {
		return nil, err
	}
	rf.Bind(c.Self(), py.Kw{"is_group": is_group, "comm": comm})
	return rf, nil
}

func init() {
	DefaultCentralCommunication = LAILA_IDENTIFIABLE_COMMUNICATION_class

	// Positional parameter names of the verbs a peer reaches through
	// Execute_rpc_ (Go reflection has none): a Python caller may pass
	// them by keyword -- ``route(entries=[...], pool_nickname="x")`` --
	// and py.Call must bind them like Python does.
	py.Register_params("extend", "pool")
	py.Register_params("_LAILA_IDENTIFIABLE_POLICY.extend", "new_pool")
	py.Register_params("route", "entries")
	py.Register_params("memorize", "entries")
	py.Register_params("remember", "entry_ids")
	py.Register_params("_LAILA_IDENTIFIABLE_POLICY.remember", "global_id")
	py.Register_params("forget", "entry_ids")
	py.Register_params("submit", "tasks", "wait")
	py.Register_params("add_connection", "protocol")
	py.Register_params("remove_connection", "protocol")
	py.Register_params("add_peer", "uri", "secret")
	py.Register_params("add_tcpip_peer", "host", "port", "secret")
	py.Register_params("remove_peer", "peer_id")
}
