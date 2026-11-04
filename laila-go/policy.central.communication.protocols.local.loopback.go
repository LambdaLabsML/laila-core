package laila

import (
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/local/loopback.py -- in-process
// loopback communication transport.
//
// The degenerate transport: both policies live in the *same* process, so
// there is no socket at all -- an RPC is dispatched by directly invoking
// the target policy's inbound handler. It still goes through the codec
// (encode then decode) so future-shaped results are virtualised into
// RemoteFuture proxies exactly as they would be over a real wire, which
// makes loopback a faithful, dependency-free way to exercise the full
// inter-policy path in one process.
//
//   - ``protocol_name`` ``"loopback"`` (aliases ``local`` / ``inproc``)
//   - URI scheme ``loopback://<policy_global_id>``
//
// Peers are matched through a process-wide registry keyed by the owning
// policy's ``global_id``.
//
// Stream lanes work too: there is no wire, so a ``send()`` hands the
// ``bytes`` object straight to the peer carrier's inbound lane (zero
// copy), and the lane control frames go through the same direct
// Build_response_ call as RPC.

// LOOPBACK_REGISTRY_ is the process-wide registry: policy global_id ->
// loopback protocol instance.
var (
	LOOPBACK_REGISTRY_    = map[string]*LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_{}
	loopback_registry_mu_ sync.Mutex
)

// LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_ is the same-process loopback
// transport.
//
// Registers itself under its owning policy's ``global_id`` on Start, so
// another in-process policy can peer with ``loopback://<that-global-id>``.
type LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_ struct {
	CarrierRPCProtocol_
}

// LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_class is the class descriptor
// (``protocol_name = "loopback"``, ``supports_channels = True``).
var LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_)(nil),
	CarrierRPCProtocol_class,
).
	Set_classvar("protocol_name", "loopback").
	Set_classvar("supports_channels", true).
	Set_classvar("_TOKEN_ALIASES", py.NewSet("loopback", "local", "inproc")).
	Set_classvar("matches_token", Token_aliases_classmethod_("loopback", "local", "inproc")).
	Set_classvar("can_handle_uri", Uri_prefix_classmethod_("loopback://"))

// New_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_ is
// ``_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL(**data)``.
func New_LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_, error) {
	p := LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_class.New().(*LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// Matches_token accepts ``"loopback"`` plus aliases.
func (p *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_class, token)
}

// Can_handle_uri claims ``loopback://`` URIs.
func (p *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_class, uri)
}

// policy_id_str_ is ``str(pid)`` or ("", false) when the carrier has no
// owning policy.
func (p *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_) policy_id_str_() (string, bool) {
	pid := p.Policy_id_()
	if py.IsNone(pid) {
		return "", false
	}
	return py.Str(pid), true
}

// target_of_ is ``self._connections.get(peer_id)`` typed.
func (p *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_) target_of_(peer_id string) *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_ {
	t, _ := p.Connection_handle_(peer_id).(*LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_)
	return t
}

// Start registers this endpoint in the process-wide loopback registry.
func (p *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_) Start() error {
	if p.Started_ {
		return nil
	}
	p.init_tables_()
	p.Ensure_executor_()
	if pid, ok := p.policy_id_str_(); ok {
		loopback_registry_mu_.Lock()
		LOOPBACK_REGISTRY_[pid] = p
		loopback_registry_mu_.Unlock()
	}
	p.Started_ = true
	return nil
}

// Stop deregisters, tells every peer we are gone, drops them
// (idempotent).
func (p *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_) Stop() error {
	if !p.Started_ {
		return nil
	}
	if pid, ok := p.policy_id_str_(); ok {
		loopback_registry_mu_.Lock()
		if LOOPBACK_REGISTRY_[pid] == p {
			delete(LOOPBACK_REGISTRY_, pid)
		}
		loopback_registry_mu_.Unlock()
	}
	for _, peer_id := range p.Peer_ids_() {
		_ = p.Disconnect(peer_id)
	}
	p.Shutdown_lanes_()
	p.Pending_rpcs_.Clear()
	p.Shutdown_executor_()
	p.Started_ = false
	return nil
}

// Disconnect drops *peer_id* on both ends. Idempotent.
//
// There is no wire for a ``peer.disconnect`` frame, so the goodbye is a
// direct call into the peer carrier; without it the other side keeps us
// registered forever (its liveness pings still reach this live object
// and succeed).
func (p *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_) Disconnect(peer_id string) error {
	target := p.target_of_(peer_id)
	if target == nil {
		return nil
	}
	p.hooks().Unregister_peer_(peer_id)
	if my_pid, ok := p.policy_id_str_(); ok {
		target.On_peer_goodbye_(my_pid)
	}
	return nil
}

// Connect peers with another in-process policy named by *uri*.
func (p *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_) Connect(uri string, secret string) (string, error) {
	if err := p.Start(); err != nil {
		return "", err
	}
	target_pid := Uri_authority(uri)
	loopback_registry_mu_.Lock()
	target := LOOPBACK_REGISTRY_[target_pid]
	loopback_registry_mu_.Unlock()
	if target == nil {
		return "", py.ConnectionError.Newf(
			"No in-process loopback endpoint for policy %s. Start the target policy's loopback connection first.",
			py.Repr(target_pid))
	}
	if secret != target.Peer_secret_key {
		return "", py.ConnectionError.New("Invalid peer secret key.")
	}

	// Register both directions so either side can call the other. Both
	// ends are this very class, so lane capability is known.
	p.Set_peer_caps_(target_pid, target.Local_caps_())
	p.hooks().Register_peer_(target_pid, target)
	if my_pid, ok := p.policy_id_str_(); ok {
		target.Set_peer_caps_(my_pid, p.Local_caps_())
		target.hooks().Register_peer_(my_pid, p)
	}
	return target_pid, nil
}

// Send_rpc dispatches an RPC directly into the peer policy (zero-copy).
//
// Since both policies are in the same process, loopback skips the codec
// entirely and passes live objects (the returned value is the target's
// real result/future, not a serialised envelope). This is the fastest
// possible path -- no encode/decode, no socket.
func (p *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_) Send_rpc(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	target := p.target_of_(peer_id)
	if target == nil {
		return nil, py.ConnectionError.Newf("No connection to peer %s", peer_id)
	}
	req := p.Make_rpc_request_(path, args, kwargs, py.Uuid4().String())
	// Run on the TARGET so its Execute_rpc_ walks the target policy. Our
	// own policy id identifies us as the authenticated caller.
	resp := Build_response_via_(target.hooks(), req, p.Policy_id_())
	if e, found := resp.Get("error"); found {
		return nil, py.RuntimeError.Newf("Remote RPC error: %s", py.Str(error_message_(e)))
	}
	return resp.GetDefault("result", nil), nil
}

// Send_oneway_ is a control frame without a reply: same direct dispatch,
// result ignored.
func (p *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_) Send_oneway_(peer_id string, path []string, kwargs py.Kw) error {
	target := p.target_of_(peer_id)
	if target == nil {
		return nil
	}
	req := p.Make_rpc_request_(path, nil, kwargs, py.Uuid4().String())
	Build_response_via_(target.hooks(), req, p.Policy_id_())
	return nil
}

// Stream_enqueue_ delivers *payload* straight into the peer's matching
// inbound lane.
func (p *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_) Stream_enqueue_(channel *Channel, payload []byte) error {
	target := p.target_of_(channel.Peer_id)
	if target == nil {
		return py.ConnectionError.Newf("No connection to peer %s", channel.Peer_id)
	}
	my_pid, ok := p.policy_id_str_()
	if !ok {
		return py.ConnectionError.New("Loopback carrier has no owning policy id.")
	}
	lane := 0
	if tx := channel.Tx_lane_id(); tx != nil {
		lane = *tx
	}
	target.On_stream_payload_(my_pid, lane, payload)
	return nil
}

// Connect_loopback is the convenience wrapper building the
// ``loopback://`` URI.
func (p *LAILA_IDENTIFIABLE_LOOPBACK_COMM_PROTOCOL_) Connect_loopback(policy_global_id string, secret string) (string, error) {
	return p.Connect("loopback://"+policy_global_id, secret)
}
