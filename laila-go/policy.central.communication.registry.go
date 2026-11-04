package laila

import (
	"fmt"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/registry.py -- peer registry and
// channel-aware peer proxy.
//
// PeerRegistry is the ``dict`` subtype stored on ``communication.peers``.
// It keeps exact dict semantics (``len``, iteration, ``in``, ``.get`` /
// ``.pop`` / ``.clear``) for every existing caller and adds exactly one
// resolver, PeerRegistry.Channel, which turns ``(peer, name,
// transport-selector)`` into a Channel by asking the carrier that holds
// the peer. It holds no live stream state and no back-reference to the
// communication object.
//
// PeerProxy is the value type stored in the registry. It is a
// RemotePolicyProxy that additionally supports item access::
//
//	laila.peers[gid]["default"]      # the ordinary RPC proxy itself
//	laila.peers[gid]["video"]        # a Channel (opened lazily, cached)
//	laila.peers[gid].via("uart")["video"]
//
// Because dunder lookup happens on the type, ``__getitem__`` coexists with
// the proxy's catch-all ``__getattr__``; ``proxy.central.memory.remember()``
// is still a single RPC.

// DEFAULT_CHANNEL is the reserved channel name that resolves to the RPC
// proxy itself.
const DEFAULT_CHANNEL = "default"

// PeerRegistry is ``dict[str, PeerProxy]`` with a single channel resolver.
//
// Channel names never select a transport; that stays on PeerProxy.Via /
// ``laila.request(gid, comm_protocol=...)``.
type PeerRegistry struct {
	*py.Dict
}

// New_PeerRegistry is ``PeerRegistry()`` / ``PeerRegistry(mapping)``.
func New_PeerRegistry(initial ...any) *PeerRegistry {
	r := &PeerRegistry{Dict: py.NewDict()}
	for _, m := range initial {
		if !py.IsNone(m) {
			r.Dict.Update(m)
		}
	}
	return r
}

// Repr is the dict repr.
func (r *PeerRegistry) Repr() string { return py.Repr(r.Dict) }

func (r *PeerRegistry) String() string { return r.Repr() }

// Channel resolves ``(peer_id, name)`` to a Channel on the right carrier.
//
//  1. ``proto = comm._select_protocol_for_peer(peer_id, selector)``.
//  2. Require ``type(proto).supports_channels``.
//  3. Return ``proto.open_channel(peer_id, name)`` (cached inside the
//     carrier; the first call may perform one blocking RPC).
//
// ConnectionError if no (matching) transport holds the peer, the transport
// has no stream lanes, or the peer cannot open the lane. ValueError if
// name is the reserved ``"default"``.
func (r *PeerRegistry) Channel(comm *LAILA_IDENTIFIABLE_COMMUNICATION_, peer_id string, name string, selector any) (*Channel, error) {
	if name == DEFAULT_CHANNEL {
		return nil, py.ValueError.Newf("%s is the RPC proxy, not a stream channel.", py.Repr(DEFAULT_CHANNEL))
	}
	proto, err := comm.Select_protocol_for_peer_(peer_id, selector)
	if err != nil {
		return nil, err
	}
	if !Comm_protocol_supports_channels_(proto) {
		return nil, py.ConnectionError.Newf(
			"%s has no stream lanes; use a stream-capable transport (e.g. tcp://, serial, loopback) for laila.peers[gid][name].",
			py.Repr(Comm_protocol_name_(proto)))
	}
	opener, ok := proto.(interface {
		Open_channel(peer_id string, name string) (*Channel, error)
	})
	if !ok {
		return nil, py.ConnectionError.Newf("%s has no stream lanes.", py.Repr(Comm_protocol_name_(proto)))
	}
	return opener.Open_channel(peer_id, name)
}

// PeerProxy is a RemotePolicyProxy with ``[name]`` channel access.
//
// Adds only methods -- no new instance attributes -- so identity checks
// (``isinstance(x, RemotePolicyProxy)``), laila.Activate_policy (morph
// mode) and laila.Request keep working unchanged.
//
// ``proxy[name]`` is *not* a cheap dict lookup: the first access to a
// negotiated lane performs one blocking RPC (bounded by the carrier's
// ``rpc_timeout``) and may raise ConnectionError. Later accesses hit the
// carrier's cache.
type PeerProxy struct {
	RemotePolicyProxy
}

// PeerProxy_class is the class descriptor.
var PeerProxy_class = py.NewClass("PeerProxy", (*PeerProxy)(nil), RemotePolicyProxy_class)

// New_PeerProxy is ``PeerProxy(peer_id, communication, comm_selector=None)``.
func New_PeerProxy(peer_id string, communication any, comm_selector ...any) *PeerProxy {
	p := PeerProxy_class.New().(*PeerProxy)
	p.init_proxy_(peer_id, communication, comm_selector...)
	return p
}

// Getitem is ``__getitem__``: ``"default"`` -> this proxy; any other name
// -> a Channel. TypeError for non-string names.
func (p *PeerProxy) Getitem(name any) (any, error) {
	s, ok := name.(string)
	if !ok {
		return nil, py.TypeError.Newf("Channel names are strings, got %s; use 'default' for the RPC proxy.", py.Pytypename(name))
	}
	if s == DEFAULT_CHANNEL {
		return p.Self(), nil
	}
	comm := p.Comm()
	if comm == nil {
		return nil, py.ConnectionError.New("PeerProxy is not bound to a communication hub.")
	}
	return comm.Peers.Channel(comm, p.Peer_id_, s, p.Comm_selector_)
}

// Channel is the typed form of ``proxy[name]`` for a stream channel.
func (p *PeerProxy) Channel(name string) (*Channel, error) {
	v, err := p.Getitem(name)
	if err != nil {
		return nil, err
	}
	ch, ok := v.(*Channel)
	if !ok {
		return nil, py.ValueError.Newf("%s is the RPC proxy, not a stream channel.", py.Repr(name))
	}
	return ch, nil
}

// Channels lists the names of channels currently open to this peer (union
// over carriers).
//
// Never opens anything. With a Via selector only that transport is
// consulted.
func (p *PeerProxy) Channels() ([]string, error) {
	comm := p.Comm()
	names := []string{}
	if comm == nil {
		return names, nil
	}
	var protos []Comm_protocol_
	if !py.IsNone(p.Comm_selector_) {
		proto, err := comm.Select_protocol_for_peer_(p.Peer_id_, p.Comm_selector_)
		if err != nil {
			return nil, err
		}
		protos = []Comm_protocol_{proto}
	} else {
		for _, v := range comm.Connections.Values() {
			proto, ok := v.(Comm_protocol_)
			if ok && proto.Has_peer(p.Peer_id_) {
				protos = append(protos, proto)
			}
		}
	}
	for _, proto := range protos {
		fn, ok := proto.(interface{ Channel_names(peer_id string) []string })
		if !ok {
			continue
		}
		for _, n := range fn.Channel_names(p.Peer_id_) {
			if !containsStr_(names, n) {
				names = append(names, n)
			}
		}
	}
	return names, nil
}

func containsStr_(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// Via returns a PeerProxy bound to transport comm (see the base class).
func (p *PeerProxy) Via(comm any) *PeerProxy {
	return New_PeerProxy(p.Peer_id_, p.Comm_, comm)
}

// Repr is ``PeerProxy('gid')`` / ``PeerProxy('gid', via='tcp')``.
func (p *PeerProxy) Repr() string {
	if !py.IsNone(p.Comm_selector_) {
		return fmt.Sprintf("PeerProxy(%s, via=%s)", py.Repr(p.Peer_id_), py.Repr(p.Comm_selector_))
	}
	return fmt.Sprintf("PeerProxy(%s)", py.Repr(p.Peer_id_))
}

func (p *PeerProxy) String() string { return p.Repr() }
