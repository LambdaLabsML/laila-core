package laila

import (
	"fmt"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/communication/proxy.py -- transparent remote-policy proxy
// for inter-policy RPC.
//
// A RemotePolicyProxy is the local stand-in for a *remote* policy. From
// the caller's point of view it looks (and quacks) like a local
// _LAILA_IDENTIFIABLE_POLICY -- you can pass it to laila.Activate_policy,
// query its ``global_id``, and reach its ``central.memory.memorize`` /
// ``central.command.submit`` methods exactly as though it were local. The
// difference is that every terminal call is sent over the wire instead of
// executed in-process.
//
// The trick that keeps the call site clean is *attribute-chain
// accumulation* via RemoteAttrChain_. Each attribute access on a proxy
// (py.Getattr) returns a chain object that records the dotted path so far
// without doing any I/O. Only when the chain is finally invoked (py.Call /
// Call) does a single RPC frame get serialised and dispatched through the
// communication layer. That means
//
//	proxy.central.memory.remember("x")
//
// results in exactly one network round-trip, with the remote side
// receiving ``["central", "memory", "remember"]`` plus ``args=("x",),
// kwargs={}``.

// RemotePolicyProxy is the client-side proxy representing a peered remote
// policy.
//
// Exposes Global_id so that laila.Activate_policy and other identity
// checks work transparently. All *other* attribute access (py.Getattr) is
// captured as the start of a chain (RemoteAttrChain_) and only triggers a
// network round-trip when the chain is finally invoked.
//
//   - Peer_id_: the remote policy's ``global_id``.
//   - Comm_: the local central-communication instance that owns the
//     underlying transport to the peer.
//   - Comm_selector_: the transport selector bound by Via (nil = any).
type RemotePolicyProxy struct {
	py.Object
	Peer_id_       string `py:"_peer_id"`
	Comm_          any    `py:"_comm"`
	Comm_selector_ any    `py:"_comm_selector"`
}

// RemotePolicyProxy_class is the class descriptor.
var RemotePolicyProxy_class = py.NewClass("RemotePolicyProxy", (*RemotePolicyProxy)(nil))

// New_RemotePolicyProxy is ``RemotePolicyProxy(peer_id, communication,
// comm_selector=None)``.
func New_RemotePolicyProxy(peer_id string, communication any, comm_selector ...any) *RemotePolicyProxy {
	p := RemotePolicyProxy_class.New().(*RemotePolicyProxy)
	p.init_proxy_(peer_id, communication, comm_selector...)
	return p
}

func (p *RemotePolicyProxy) init_proxy_(peer_id string, communication any, comm_selector ...any) {
	// Keep the outermost object (a *PeerProxy when built through
	// New_PeerProxy) as ``self``: Class.New already bound it, and
	// re-initialising with the embedded pointer would make Self() -- and
	// therefore ``proxy["default"] is proxy`` -- resolve to the inner type.
	var self any = p
	if p.Bound() {
		self = p.Self()
	}
	_ = py.Init(self, p.Class_(), py.Kw{})
	p.Peer_id_ = peer_id
	p.Comm_ = communication
	if len(comm_selector) > 0 {
		p.Comm_selector_ = comm_selector[0]
	}
}

// Comm is the owning hub as a *LAILA_IDENTIFIABLE_COMMUNICATION_ (nil
// when unbound or bound to a stand-in).
func (p *RemotePolicyProxy) Comm() *LAILA_IDENTIFIABLE_COMMUNICATION_ {
	c, _ := p.Comm_.(*LAILA_IDENTIFIABLE_COMMUNICATION_)
	return c
}

// Global_id is the remote policy's ``global_id`` (resolved without I/O).
func (p *RemotePolicyProxy) Global_id() string { return p.Peer_id_ }

// Via returns a proxy bound to a specific transport comm.
//
// comm is a *communication id* -- a registered connection's ``global_id``
// or a protocol token (``"tcp"``, ``"lora"``, ...). Every RPC issued
// through the returned proxy (and every follow-up call on the futures it
// returns) travels over that channel. The original proxy is unchanged.
//
//	laila.peers[gid].via("lora").central.memory.remember(eid)
func (p *RemotePolicyProxy) Via(comm any) *RemotePolicyProxy {
	return New_RemotePolicyProxy(p.Peer_id_, p.Comm_, comm)
}

// Getattr is ``__getattr__``: begin a remote attribute chain rooted at
// name.
//
// Called (through py.Getattr) for any attribute that is not a real field
// / method of the proxy. Returns a RemoteAttrChain_; further attribute
// accesses extend the path, Call materialises the RPC.
func (p *RemotePolicyProxy) Getattr(name string) (any, error) {
	return New_RemoteAttrChain_(p.Comm_, p.Peer_id_, []string{name}, p.Comm_selector_), nil
}

// Attr is the typed form of ``proxy.<name>``: a chain rooted at name.
func (p *RemotePolicyProxy) Attr(name string) *RemoteAttrChain_ {
	return New_RemoteAttrChain_(p.Comm_, p.Peer_id_, []string{name}, p.Comm_selector_)
}

// Repr is ``RemotePolicyProxy('gid')`` / ``RemotePolicyProxy('gid',
// via='tcp')``.
func (p *RemotePolicyProxy) Repr() string {
	if !py.IsNone(p.Comm_selector_) {
		return fmt.Sprintf("RemotePolicyProxy(%s, via=%s)", py.Repr(p.Peer_id_), py.Repr(p.Comm_selector_))
	}
	return fmt.Sprintf("RemotePolicyProxy(%s)", py.Repr(p.Peer_id_))
}

func (p *RemotePolicyProxy) String() string { return p.Repr() }

// RemoteAttrChain_ is the accumulator for a dotted attribute path; it
// flushes as an RPC on Call.
//
// Each attribute access appends to the path *immutably* (a new chain
// object is returned, the existing one is unchanged), so the chain is safe
// to share across threads. Calling the chain sends one RPC frame
// containing the full path plus the call's positional and keyword
// arguments.
//
//   - Comm_: local communication instance used to dispatch the RPC.
//   - Peer_id_: target peer ``global_id``.
//   - Path_: attribute segments accumulated so far. Never mutated in place;
//     a new slice is created per Getattr.
type RemoteAttrChain_ struct {
	Comm_          any
	Peer_id_       string
	Path_          []string
	Comm_selector_ any
}

// New_RemoteAttrChain_ is ``_RemoteAttrChain(communication, peer_id, path,
// comm_selector=None)``.
func New_RemoteAttrChain_(communication any, peer_id string, path []string, comm_selector any) *RemoteAttrChain_ {
	return &RemoteAttrChain_{Comm_: communication, Peer_id_: peer_id, Path_: path, Comm_selector_: comm_selector}
}

// Getattr is ``__getattr__``: a *new* chain whose path is the current
// path plus name.
func (c *RemoteAttrChain_) Getattr(name string) (any, error) {
	return c.Attr(name), nil
}

// Attr is the typed form of ``chain.<name>``.
func (c *RemoteAttrChain_) Attr(name string) *RemoteAttrChain_ {
	path := make([]string, 0, len(c.Path_)+1)
	path = append(path, c.Path_...)
	path = append(path, name)
	return New_RemoteAttrChain_(c.Comm_, c.Peer_id_, path, c.Comm_selector_)
}

// Call is ``__call__``: dispatch the accumulated path + arguments as a
// single RPC.
//
// Blocks the calling thread until the remote responds. If the remote
// returns a future-shaped envelope, the communication layer transparently
// wraps it in a RemoteFuture before returning. The bound transport
// selector (if any) is passed through so the call -- and the future it
// yields -- stay on the chosen channel.
func (c *RemoteAttrChain_) Call(args []any, kwargs py.Kw) (any, error) {
	comm, ok := c.Comm_.(*LAILA_IDENTIFIABLE_COMMUNICATION_)
	if !ok {
		// A stand-in hub (tests): dispatch through its ``_send_rpc``.
		return callm(c.Comm_, "_send_rpc", c.Peer_id_, c.Path_, py.Tuple(args), py.DictFromKw(kwargs), py.Kw{"comm": c.Comm_selector_})
	}
	return comm.Send_rpc_(c.Peer_id_, c.Path_, args, kwargs, c.Comm_selector_)
}

// Invoke is Call with Go-style positional arguments (a trailing py.Kw is
// treated as keyword arguments).
func (c *RemoteAttrChain_) Invoke(args ...any) (any, error) {
	pos, kw := py.SplitKw(args)
	return c.Call(pos, kw)
}

// Repr is ``_RemoteAttrChain('gid', 'central.memory.remember')``.
func (c *RemoteAttrChain_) Repr() string {
	return fmt.Sprintf("_RemoteAttrChain(%s, %s)", py.Repr(c.Peer_id_), py.Repr(strings.Join(c.Path_, ".")))
}

func (c *RemoteAttrChain_) String() string { return c.Repr() }
