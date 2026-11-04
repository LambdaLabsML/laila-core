package laila

import (
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/protocols/base.py -- abstract base class
// for communication-protocol implementations.
//
// A *protocol* is the transport layer that sits between
// _LAILA_IDENTIFIABLE_COMMUNICATION and the wire. Concrete subclasses know
// how to:
//
//   - Open a listener that accepts inbound peering handshakes.
//   - Initiate outbound peering handshakes against a URI + secret.
//   - Encode RPC frames (``path`` + ``args`` + ``kwargs``), send them to a
//     known peer, and decode the response.
//   - Tear themselves down cleanly on Stop.
//
// The base class only fixes the *shape* of that contract -- the specifics
// (TCP/IP via WebSockets, shared-memory queues, InfiniBand, gRPC, ...) are
// entirely up to the subclass.
//
// Each protocol instance carries a ``_communication`` back-reference set
// by LAILA_IDENTIFIABLE_COMMUNICATION_.Add_connection so it can forward
// inbound RPCs back to the policy and notify the communication layer when
// peers connect or disconnect.
//
// # Class attributes (ClassVars, read through the class descriptor)
//
//   - ``protocol_name``: short, stable token identifying this transport
//     family (e.g. ``"tcpip"``, ``"lora"``, ``"bluetooth"``). It is the
//     value users pass as ``comm_protocol`` to laila.Request and the key
//     Resolve_protocol_for_token_ matches against. Subclasses MUST
//     override it.
//   - ``persistent``: whether a peering holds a long-lived connection
//     (true) or is re-established per request (false). Persistent
//     transports avoid a handshake per call and are pinged by the
//     communication layer's liveness loop; per-request transports are not
//     pinged.
//   - ``supports_ping``: whether Ping is meaningful for this transport. The
//     liveness loop only pings protocols that are both ``persistent`` and
//     ``supports_ping``.
//   - ``supports_channels``: whether this transport can carry opaque
//     byte-stream *lanes* alongside RPC (``laila.peers[gid][name]`` /
//     laila.Relay). Stream-capable carriers set this true; a transport
//     that only speaks framed JSON-RPC leaves it false and
//     ``peers[gid][name]`` raises ConnectionError for it.
//   - ``matches_token`` / ``can_handle_uri``: the two classmethods, stored
//     as ``func(cls *py.Class, s string) bool`` classvars so they resolve
//     through the MRO exactly like Python's ``type(proto).matches_token``.
//     ``_TOKEN_ALIASES`` (a []string classvar) is the common override
//     shape for ``matches_token``.

// Comm_protocol_ is the virtual surface of _LAILA_IDENTIFIABLE_COMM_PROTOCOL
// as seen by the communication hub (every method dispatches to the most
// derived implementation through Self()).
type Comm_protocol_ interface {
	Class_() *py.Class
	Self() any
	Object_() *py.Object
	Global_id() string
	Comm_protocol_base_() *LAILA_IDENTIFIABLE_COMM_PROTOCOL_
	Start() error
	Stop() error
	Connect(uri string, secret string) (string, error)
	Disconnect(peer_id string) error
	Ping(peer_id string, timeout ...any) bool
	Send_rpc(peer_id string, path []string, args []any, kwargs py.Kw) (any, error)
	Has_peer(peer_id string) bool
}

// LAILA_IDENTIFIABLE_COMM_PROTOCOL_ is the base class for transport-layer
// protocol implementations.
//
// Subclasses (TCP/IP, shared memory, InfiniBand, etc.) implement the
// abstract interface below. Each protocol instance is registered on a
// LAILA_IDENTIFIABLE_COMMUNICATION_ via Add_connection, which sets the
// ``_communication`` back-reference and calls Start.
//
// All public lifecycle methods are required to be *idempotent*: calling
// Start on a running protocol should be a no-op, calling Stop on a
// stopped protocol should be a no-op. This keeps higher-level code in
// LAILA_IDENTIFIABLE_COMMUNICATION_ free of "is this protocol already up?"
// book-keeping.
type LAILA_IDENTIFIABLE_COMM_PROTOCOL_ struct {
	LAILA_CLI_CAPABLE_CLASS_
	LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_

	Communication_ any `py:"_communication"`
}

// LAILA_IDENTIFIABLE_COMM_PROTOCOL_class is the class descriptor
// (``_scopes`` defaults to ``[_COMM_PROTOCOL_SCOPE]``,
// ``arbitrary_types_allowed``).
var LAILA_IDENTIFIABLE_COMM_PROTOCOL_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_COMM_PROTOCOL", (*LAILA_IDENTIFIABLE_COMM_PROTOCOL_)(nil),
	LAILA_CLI_CAPABLE_CLASS_class, LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_class,
).
	Set_private_default("_scopes", func() any { return []string{COMM_PROTOCOL_SCOPE_} }).
	Set_classvar("protocol_name", "base").
	Set_classvar("persistent", true).
	Set_classvar("supports_ping", true).
	Set_classvar("supports_channels", false)

// Comm_protocol_base_ exposes the embedded base to code holding a subclass.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Comm_protocol_base_() *LAILA_IDENTIFIABLE_COMM_PROTOCOL_ {
	return b
}

// Model_post_init runs the CLI-capable registration like every other
// CLI-capable class.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Model_post_init(ctx any) error {
	if err := b.LAILA_IDENTIFIABLE_OBJECT_.Model_post_init(ctx); err != nil {
		return err
	}
	return CLI_CAPABLE_model_post_init(b.Self())
}

// Communication is ``self._communication`` typed (nil when unattached or
// attached to a stand-in).
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Communication() *LAILA_IDENTIFIABLE_COMMUNICATION_ {
	c, _ := b.Communication_.(*LAILA_IDENTIFIABLE_COMMUNICATION_)
	return c
}

// Policy_id_ is ``self._communication.policy_id if self._communication
// else None``.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Policy_id_() any {
	if py.IsNone(b.Communication_) {
		return nil
	}
	if c := b.Communication(); c != nil {
		return py.Deref_any(c.Policy_id)
	}
	v, err := py.Getattr(b.Communication_, "policy_id")
	if err != nil {
		return nil
	}
	return v
}

// ---------------------------------------------------------------------------
// classmethod helpers
// ---------------------------------------------------------------------------

// Comm_protocol_name_of_ is ``cls.protocol_name``.
func Comm_protocol_name_of_(cls *py.Class) string {
	v, _ := cls.Classvar("protocol_name")
	s, _ := v.(string)
	return s
}

// Comm_protocol_name_ is ``type(proto).protocol_name``.
func Comm_protocol_name_(proto any) string {
	cls := py.Classof(proto)
	if cls == nil {
		return ""
	}
	return Comm_protocol_name_of_(cls)
}

func classvarBool_(cls *py.Class, name string, def bool) bool {
	if cls == nil {
		return def
	}
	v, ok := cls.Classvar(name)
	if !ok {
		return def
	}
	return py.Truthy(v)
}

// Comm_protocol_supports_channels_ is ``type(proto).supports_channels``.
func Comm_protocol_supports_channels_(proto any) bool {
	return classvarBool_(py.Classof(proto), "supports_channels", false)
}

// Comm_protocol_persistent_ is ``getattr(proto, "persistent", True)``.
func Comm_protocol_persistent_(proto any) bool {
	return classvarBool_(py.Classof(proto), "persistent", true)
}

// Comm_protocol_supports_ping_ is ``getattr(proto, "supports_ping", True)``.
func Comm_protocol_supports_ping_(proto any) bool {
	return classvarBool_(py.Classof(proto), "supports_ping", true)
}

// Comm_protocol_matches_token_ is the classmethod ``cls.matches_token(token)``:
// true if this protocol answers to the transport token.
//
// The default compares token case-insensitively against
// ``protocol_name``; a class that sets the ``_TOKEN_ALIASES`` classvar
// accepts any of those aliases instead, and a class that sets the
// ``matches_token`` classvar (``func(cls, token) bool``) replaces the
// logic entirely.
//
// Used by Resolve_protocol_for_token_ to dispatch laila.Request over the
// requested transport.
func Comm_protocol_matches_token_(cls *py.Class, token string) bool {
	if fn, ok := cls.Classvar("matches_token"); ok {
		if f, ok := fn.(func(*py.Class, string) bool); ok {
			return f(cls, token)
		}
	}
	if aliases, ok := cls.Classvar("_TOKEN_ALIASES"); ok {
		for _, a := range py.Iter(aliases) {
			if strings.ToLower(token) == py.Str(a) {
				return true
			}
		}
		return false
	}
	return strings.ToLower(token) == strings.ToLower(Comm_protocol_name_of_(cls))
}

// Comm_protocol_can_handle_uri_ is the classmethod ``cls.can_handle_uri(uri)``:
// true if this protocol claims responsibility for uri.
//
// Used by Resolve_protocol_for_uri_ to dispatch outbound peering.
// Subclasses opt in by setting the ``can_handle_uri`` classvar
// (``func(cls, uri) bool``), typically inspecting the URI scheme; the
// default returns false.
func Comm_protocol_can_handle_uri_(cls *py.Class, uri string) bool {
	if fn, ok := cls.Classvar("can_handle_uri"); ok {
		if f, ok := fn.(func(*py.Class, string) bool); ok {
			return f(cls, uri)
		}
	}
	return false
}

// Uri_prefix_classmethod_ builds a ``can_handle_uri`` classvar that claims
// any URI starting with one of prefixes.
func Uri_prefix_classmethod_(prefixes ...string) func(*py.Class, string) bool {
	return func(_ *py.Class, uri string) bool {
		for _, p := range prefixes {
			if strings.HasPrefix(uri, p) {
				return true
			}
		}
		return false
	}
}

// Token_aliases_classmethod_ builds a ``matches_token`` classvar accepting
// ``token.lower() in aliases``.
func Token_aliases_classmethod_(aliases ...string) func(*py.Class, string) bool {
	return func(_ *py.Class, token string) bool {
		t := strings.ToLower(token)
		for _, a := range aliases {
			if t == a {
				return true
			}
		}
		return false
	}
}

// Matches_token is the instance-side ``type(self).matches_token(token)``.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Matches_token(token string) bool {
	return Comm_protocol_matches_token_(b.Class_(), token)
}

// Can_handle_uri is the instance-side ``type(self).can_handle_uri(uri)``.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Can_handle_uri(uri string) bool {
	return Comm_protocol_can_handle_uri_(b.Class_(), uri)
}

// Protocol_name is ``type(self).protocol_name``.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Protocol_name() string {
	return Comm_protocol_name_of_(b.Class_())
}

func init() {
	// ``protocol_name`` is a ClassVar[str] in Python: ``getattr(proto,
	// "protocol_name")`` yields the string, not a bound method.
	py.Register_property("protocol_name")
}

// ---------------------------------------------------------------------------
// Abstract interface
// ---------------------------------------------------------------------------

// Start starts the protocol's listener loop and any background workers.
// Subclasses must implement; the base raises NotImplementedError.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Start() error {
	return py.NotImplementedError.New()
}

// Stop tears down all connections and releases transport resources.
//
// Subclasses must implement; the base raises NotImplementedError.
// Implementations should close open connections, terminate worker
// threads, and call LAILA_IDENTIFIABLE_COMMUNICATION_.Unregister_peer_ for
// each peer that goes away.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Stop() error {
	return py.NotImplementedError.New()
}

// Connect establishes an outbound connection/peering to uri using secret.
//
// Peer *orchestration* (resolving which protocol handles a URI,
// registering the proxy) is owned by LAILA_IDENTIFIABLE_COMMUNICATION_; a
// protocol only knows how to Connect and Disconnect its own wire. On
// success the protocol must call
// LAILA_IDENTIFIABLE_COMMUNICATION_.Register_peer_ so a RemotePolicyProxy
// is created in the local registry. Returns the remote policy's
// ``global_id``.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Connect(uri string, secret string) (string, error) {
	return "", py.NotImplementedError.New()
}

// Disconnect tears down the connection to a single peer_id.
//
// Idempotent and safe by default (a transport that holds no peer has
// nothing to disconnect). Carriers override this to close the peer's
// handle and call LAILA_IDENTIFIABLE_COMMUNICATION_.Unregister_peer_.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Disconnect(peer_id string) error { return nil }

// Ping returns true if peer_id is reachable right now (``ping(peer_id,
// timeout=None)``).
//
// Used by the communication layer's async liveness loop to detect dead
// peers without touching the data path. The default returns false;
// carriers implement a lightweight round-trip.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Ping(peer_id string, timeout ...any) bool { return false }

// Send_rpc sends an RPC frame to peer_id and blocks for the deserialized
// response.
//
// The frame layout is the protocol's choice; the only contract is that
// the remote LAILA_IDENTIFIABLE_COMMUNICATION_.Execute_rpc_ is invoked
// with ``path``, ``args``, ``kwargs`` and that the result (or a
// future-shaped envelope) flows back here.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Send_rpc(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	return nil, py.NotImplementedError.New()
}

// Has_peer returns true if this protocol currently holds a live connection
// to peer_id.
//
// Used by LAILA_IDENTIFIABLE_COMMUNICATION_.Send_rpc_ to pick the right
// protocol when more than one is registered.
func (b *LAILA_IDENTIFIABLE_COMM_PROTOCOL_) Has_peer(peer_id string) bool { return false }

// ---------------------------------------------------------------------------
// protocols/__init__.py -- registry helpers
// ---------------------------------------------------------------------------

// Iter_comm_protocols returns every concrete transport class currently
// loaded (every subclass of _LAILA_IDENTIFIABLE_COMM_PROTOCOL whose
// ``protocol_name`` is neither ``"base"`` nor ``"carrier"``).
func Iter_comm_protocols() []*py.Class {
	seen := map[string]*py.Class{}
	order := []string{}
	var walk func(cls *py.Class)
	walk = func(cls *py.Class) {
		for _, sub := range cls.Subclasses() {
			if _, ok := seen[sub.Name]; !ok {
				order = append(order, sub.Name)
			}
			seen[sub.Name] = sub
			walk(sub)
		}
	}
	walk(LAILA_IDENTIFIABLE_COMM_PROTOCOL_class)
	out := []*py.Class{}
	for _, n := range order {
		c := seen[n]
		name := Comm_protocol_name_of_(c)
		if name != "base" && name != "carrier" {
			out = append(out, c)
		}
	}
	return out
}

// Comm_protocol_for_token returns the transport class answering to token,
// or nil.
func Comm_protocol_for_token(token string) *py.Class {
	for _, cls := range Iter_comm_protocols() {
		if Comm_protocol_matches_token_(cls, token) {
			return cls
		}
	}
	return nil
}
