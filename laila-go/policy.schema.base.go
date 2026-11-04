package laila

import (
	"fmt"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/schema/base.py -- base schema for a Laila policy and its central
// sub-components.
//
// A *policy* is the unit of ownership for everything stateful in laila:
// storage pools, task-forces, peer connections, futures, and the
// manifests/hints that drive memory routing. The base class defined here
// is the bridge between the user-facing top-level API (``laila.memory``,
// ``laila.command``, ...) and the four central sub-systems that actually
// do the work.
//
// Policies are themselves identifiable. Their ``global_id`` is what peers
// use to address them across the network (see RemotePolicyProxy), and
// what laila.Activate_policy records as the active gid so the top-level
// shortcuts know which subsystem instance to resolve against.

// DefaultCentralMemory / DefaultCentralCommunication are the class
// descriptors behind the ``Default*`` aliases; they are assigned by the
// modules that declare the classes (memory and communication phases) and
// stay nil until those are linked, in which case the corresponding
// Central slot is left unset.
var (
	DefaultCentralMemory        *py.Class
	DefaultCentralCommunication *py.Class
)

// Central is the container struct for the four central sub-systems of a
// policy (``_LAILA_IDENTIFIABLE_POLICY.Central``).
//
// Holds optional references to ``logic``, ``command``, ``memory``, and
// ``communication``. All four are CLIExempt so they do not appear in
// ``laila.args`` resolution paths -- they are wired directly by
// LAILA_IDENTIFIABLE_POLICY_.Model_post_init, not from CLI args.
type Central struct {
	py.Object

	Logic         any `py:"logic,cli_exempt"`
	Command       any `py:"command,cli_exempt"`
	Communication any `py:"communication,cli_exempt"`
	Memory        any `py:"memory,cli_exempt"`
}

// Central_class is the class descriptor (``arbitrary_types_allowed``).
var Central_class = py.NewClass("Central", (*Central)(nil))

// New_Central is ``Central(**data)``.
func New_Central(kw ...py.Kw) (*Central, error) {
	c := Central_class.New().(*Central)
	if err := py.Init(c, Central_class, py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return c, nil
}

// LAILA_IDENTIFIABLE_POLICY_ is the top-level policy object owning
// central command, memory, communication, and logic.
//
// A policy bundles three runtime concerns into one identifiable object:
//
//  1. **Memory** -- where entries live and how they're routed.
//  2. **Command** -- how work runs and how its lifecycle is tracked.
//  3. **Communication** -- how this policy talks to peers.
//
// Plus a Future_bank keyed by future ``global_id`` that every Future
// self-registers into on construction. The bank is the single source of
// truth that lets remote-policy RPCs and ``laila.runtime.wait`` look
// futures up by id. It is a plain dict with strong references and
// **nothing is evicted automatically**: a future (and its result payload)
// stays in the bank until the holder calls ``future.release()``.
//
// # Lazy wiring
//
// Every central sub-system has a default implementation
// (DefaultCentralCommand / DefaultCentralMemory /
// DefaultCentralCommunication) that Model_post_init instantiates if the
// user did not pass one explicitly. This means ``policy =
// DefaultPolicy()`` returns a fully-functional policy with no further
// setup -- pools, taskforces, and a stopped communication instance ready
// to start on first ``add_peer``.
type LAILA_IDENTIFIABLE_POLICY_ struct {
	LAILA_CLI_CAPABLE_CLASS_
	LAILA_IDENTIFIABLE_OBJECT_

	// Core components
	Central *Central `py:"central,cli_exempt,factory=New_central_"`
	// Future_bank is the live registry of every future this policy owns,
	// keyed by ``global_id``. Entries persist until Future.Release /
	// GroupFuture.Release is called on them; there is no automatic
	// pruning.
	Future_bank *py.Dict `py:"future_bank,cli_exempt,factory=New_future_bank_"`
}

// LAILA_IDENTIFIABLE_POLICY_class is the class descriptor; ``_scopes``
// defaults to ``[_POLICY_SCOPE]``.
var LAILA_IDENTIFIABLE_POLICY_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_POLICY", (*LAILA_IDENTIFIABLE_POLICY_)(nil),
	LAILA_CLI_CAPABLE_CLASS_class, LAILA_IDENTIFIABLE_OBJECT_class,
).Set_private_default("_scopes", func() any { return []string{POLICY_SCOPE_} })

func init() { DefaultPolicy = LAILA_IDENTIFIABLE_POLICY_class }

// New_LAILA_IDENTIFIABLE_POLICY_ is ``_LAILA_IDENTIFIABLE_POLICY(**data)``.
func New_LAILA_IDENTIFIABLE_POLICY_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_POLICY_, error) {
	p := LAILA_IDENTIFIABLE_POLICY_class.New().(*LAILA_IDENTIFIABLE_POLICY_)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

// New_central_ is the ``central`` default factory.
func (p *LAILA_IDENTIFIABLE_POLICY_) New_central_() *Central { return py.Must(New_Central()) }

// New_future_bank_ is the ``future_bank`` default factory (``dict``).
func (p *LAILA_IDENTIFIABLE_POLICY_) New_future_bank_() *py.Dict { return py.NewDict() }

// Policy_base_ exposes the embedded base to code holding a subclass.
func (p *LAILA_IDENTIFIABLE_POLICY_) Policy_base_() *LAILA_IDENTIFIABLE_POLICY_ { return p }

// Get_future_bank_ satisfies Future_bank_holder.
func (p *LAILA_IDENTIFIABLE_POLICY_) Get_future_bank_() *py.Dict { return p.Future_bank }

// Model_post_init lazily wires the default central sub-systems if the
// user didn't supply them.
//
// For each of ``memory`` / ``command`` / ``communication``, if the slot is
// nil we instantiate the corresponding ``Default<Subsystem>`` from
// macros.defaults.
//
// After wiring, ``communication._local_policy`` is back-reffed to self so
// inbound RPC dispatch can find the local subsystems by walking attribute
// paths from the policy.
//
// ``logic`` is intentionally left nil -- it is reserved for future
// higher-level orchestration.
func (p *LAILA_IDENTIFIABLE_POLICY_) Model_post_init(ctx any) error {
	if err := p.LAILA_IDENTIFIABLE_OBJECT_.Model_post_init(ctx); err != nil {
		return err
	}
	if err := CLI_CAPABLE_model_post_init(p.Self()); err != nil {
		return err
	}

	if py.IsNone(p.Central.Memory) && DefaultCentralMemory != nil {
		mem, err := newInstance(DefaultCentralMemory, py.Kw{})
		if err != nil {
			return err
		}
		p.Central.Memory = mem
	}

	if py.IsNone(p.Central.Command) {
		cmd, err := newInstance(DefaultCentralCommand, py.Kw{"policy_id": p.Global_id()})
		if err != nil {
			return err
		}
		p.Central.Command = cmd
	}

	if py.IsNone(p.Central.Communication) && DefaultCentralCommunication != nil {
		comm, err := newInstance(DefaultCentralCommunication, py.Kw{"policy_id": p.Global_id()})
		if err != nil {
			return err
		}
		p.Central.Communication = comm
	}

	if !py.IsNone(p.Central.Communication) {
		if err := py.Setattr(p.Central.Communication, "_local_policy", p.Self()); err != nil {
			return err
		}
	}
	// Owner back-reference so memory operations run on *this* policy's
	// command even when another policy is active.
	if !py.IsNone(p.Central.Memory) {
		if err := py.Setattr(p.Central.Memory, "_policy_ref", py.WeakRef_self(p)); err != nil {
			return err
		}
	}
	// Weak, construction-time registration so futures stamped with this
	// gid land in *this* bank even before the policy is activated.
	_ = attempt(func() error {
		Live_policies_.Set(py.Str(p.Global_id()), py.WeakRef_self(p))
		return nil
	})
	return nil
}

// Extend registers a new pool with this policy's central memory.
//
// Equivalent to ``self.central.memory.extend(new_pool, ...)``, which
// delegates to the underlying PoolRouter. The pool's ``global_id``
// becomes its routing key; once registered, subsequent Memorize /
// Remember calls can target it with ``pool_id=`` (or with
// ``pool_nickname=`` if a nickname was supplied to PoolRouter.Extend).
func (p *LAILA_IDENTIFIABLE_POLICY_) Extend(new_pool any) error {
	_, err := callm(p.Central.Memory, "extend", new_pool)
	return err
}

// Remember fetches a single entry from central memory by its
// ``global_id`` (``remember(global_id, *, global_fetch=False,
// pool_subset=None, hint=None, _remote_called=False)``).
//
// This is a *low-level*, blocking convenience used internally and from
// tests; most callers should use the top-level laila.Remember (which
// returns a future and supports cache-back into the alpha pool).
//
//   - global_fetch: if True, search across all known policies
//     (multi-policy federation). Currently raises NotImplementedError.
//   - pool_subset: restrict the search to a subset of pools.
//   - hint: routing hint forwarded to central memory's pool resolver.
//   - _remote_called: internal flag set when this method is invoked
//     through an inbound RPC; reserved for future per-source bookkeeping.
//
// Returns the recovered entry, or nil if not found in any inspected pool.
func (p *LAILA_IDENTIFIABLE_POLICY_) Remember(global_id string, kw ...py.Kw) (*Entry, error) {
	k := py.MergeKw(kw...)
	if py.Truthy(k.Get("global_fetch", false)) {
		return nil, py.NotImplementedError.New()
	}
	if !py.IsNone(k.Get("pool_subset", nil)) || !py.IsNone(k.Get("hint", nil)) {
		return nil, py.NotImplementedError.New("pool_subset / hint routing is not implemented yet")
	}

	ref, err := callm(p.Central.Memory, "remember", []any{global_id}, py.Kw{"persist": false})
	if err == nil {
		var entry any
		entry, err = callm(ref, "wait")
		if err == nil {
			if lst, ok := entry.(*py.List); ok {
				if lst.Len() > 0 {
					entry = lst.Items()[0]
				} else {
					entry = nil
				}
			} else if lst, ok := entry.([]any); ok {
				if len(lst) > 0 {
					entry = lst[0]
				} else {
					entry = nil
				}
			}
			e, _ := entry.(*Entry)
			return e, nil
		}
	}
	if py.IsInstance(err, py.KeyError) {
		return nil, nil
	}
	return nil, err
}

// Memorize persists entries into central memory
// (``memorize(entries, *, require_local_update=False,
// require_global_update=False)``).
//
// Blocking thin wrapper around ``self.central.memory.memorize(entries)``
// (the write lands in the alpha pool before this returns). Most callers
// should use the top-level laila.Memorize, which returns the future
// instead. The propagation kwargs (``require_local_update`` /
// ``require_global_update``) are placeholders for future replication
// semantics and are currently ignored -- writes affect only the routed
// pool.
func (p *LAILA_IDENTIFIABLE_POLICY_) Memorize(entries any, kw ...py.Kw) error {
	fut, err := callm(p.Central.Memory, "memorize", entries)
	if err != nil {
		return err
	}
	_, err = callm(fut, "wait")
	return err
}

// ------------------------------------------------------------------
// RPC helpers for remote future introspection
// ------------------------------------------------------------------

// bank_future_ is ``self.future_bank.get(future_id)`` with the shared
// KeyError when missing.
func (p *LAILA_IDENTIFIABLE_POLICY_) bank_future_(future_id string) (any, error) {
	future := p.Future_bank.GetDefault(future_id, nil)
	if py.IsNone(future) {
		return nil, py.KeyError.New(fmt.Sprintf("Future %s not in bank", future_id))
	}
	return future, nil
}

// Get_future_status_ is the RPC returning the status of a local future.
//
// Invoked by RemoteFuture (running on a peer process) to poll the *real*
// future that lives in this policy's Future_bank. The returned value is
// unwrapped from any Enum so it survives JSON serialization on the wire.
//
// Raises KeyError if future_id is not in this policy's bank (either it
// was never created here or it has been garbage-collected).
func (p *LAILA_IDENTIFIABLE_POLICY_) Get_future_status_(future_id string) (any, error) {
	future, err := p.bank_future_(future_id)
	if err != nil {
		return nil, err
	}
	status, err := py.Getattr(future, "status")
	if err != nil {
		return nil, err
	}
	if v, ok := py.Getattr(status, "value"); ok == nil {
		return v, nil
	}
	return status, nil
}

// Get_future_exception_ is the RPC returning a JSON-serializable view of
// a local future's exception.
//
// Returns nil when the future succeeded. Otherwise returns ``{"type":
// <ExcClass>, "message": <str>}`` -- the exception's full type isn't
// reconstructed on the remote side, but its name and message are
// preserved for diagnostics.
func (p *LAILA_IDENTIFIABLE_POLICY_) Get_future_exception_(future_id string) (*py.Dict, error) {
	future, err := p.bank_future_(future_id)
	if err != nil {
		return nil, err
	}
	excv, err := py.Getattr(future, "exception")
	if err != nil {
		return nil, err
	}
	exc, _ := excv.(error)
	if exc == nil {
		return nil, nil
	}
	out := py.NewDict()
	out.Set("type", py.Typename(exc))
	out.Set("message", exc.Error())
	return out, nil
}

// Get_future_result_id_ is the RPC returning the result entry's
// ``global_id`` for a local future.
//
// Used by RemoteFuture to discover *which entry* a peer's future
// produced, so the peer can then ``laila.remember`` it from the
// appropriate pool.
//
// For GroupFuture, returns a list of child result ids (one per member
// future) -- the caller is responsible for zipping it with
// ``future.future_ids``.
//
// Returns nil if the future has no recorded result id (e.g. the future
// is still running, or it produced a non-entry result).
func (p *LAILA_IDENTIFIABLE_POLICY_) Get_future_result_id_(future_id string) (any, error) {
	future, err := p.bank_future_(future_id)
	if err != nil {
		return nil, err
	}
	// ``result_global_id`` is spelled ``*string`` on the Go futures;
	// RPC payloads carry ``str | None``.
	if py.Hasattr(future, "result_global_id") {
		v, err := py.Getattr(future, "result_global_id")
		if err != nil {
			return nil, err
		}
		return py.Deref_any(v), nil
	}
	if py.Hasattr(future, "future_ids") {
		fids, err := py.Getattr(future, "future_ids")
		if err != nil {
			return nil, err
		}
		ids := py.NewList()
		for _, fid := range py.Iter(fids) {
			child := p.Future_bank.GetDefault(fid, nil)
			if !py.IsNone(child) && py.Hasattr(child, "result_global_id") {
				v, err := py.Getattr(child, "result_global_id")
				if err != nil {
					return nil, err
				}
				ids.Append(py.Deref_any(v))
			} else {
				ids.Append(nil)
			}
		}
		return ids, nil
	}
	return nil, nil
}

// Wait_future_ is the RPC blocking on a local future and returning its
// result-entry gid (``_wait_future(future_id, timeout=None)``).
//
// Companion to Get_future_result_id_ used by RemoteFuture to wait
// synchronously for completion. The wait happens on this policy's
// process; the peer's call is already inside its own thread of execution
// and is fine to block.
func (p *LAILA_IDENTIFIABLE_POLICY_) Wait_future_(future_id string, timeout ...any) (any, error) {
	future, err := p.bank_future_(future_id)
	if err != nil {
		return nil, err
	}
	if _, err := callm(future, "wait", timeout_kw_(timeout)); err != nil {
		return nil, err
	}
	if py.Hasattr(future, "result_global_id") {
		v, err := py.Getattr(future, "result_global_id")
		if err != nil {
			return nil, err
		}
		return py.Deref_any(v), nil
	}
	return nil, nil
}

// timeout_kw_ normalises the port's optional ``timeout`` argument shapes
// into ``wait(timeout=...)``.
func timeout_kw_(timeout []any) py.Kw {
	if len(timeout) == 0 {
		return py.Kw{"timeout": nil}
	}
	if kw, ok := timeout[0].(py.Kw); ok {
		return kw
	}
	return py.Kw{"timeout": timeout[0]}
}

// Serialize_future_result_ returns a local future's result entry(ies),
// wire-serialized.
//
// Uses the canonical, self-describing
// ``Entry.serialize(transformation_base64)`` form so a RemoteFuture on a
// peer can rebuild the real Entry via ``build_by_scope`` -- the payload
// actually crosses the wire (no shared pool needed). For a GroupFuture a
// list of per-child blobs is returned.
func (p *LAILA_IDENTIFIABLE_POLICY_) Serialize_future_result_(future any) (any, error) {
	if py.Hasattr(future, "future_ids") {
		results, err := py.Getattr(future, "result")
		if err != nil {
			return nil, err
		}
		out := py.NewList()
		for _, e := range py.Iter(results) {
			if py.IsNone(e) {
				out.Append(nil)
				continue
			}
			blob, err := e.(*Entry).Serialize(Transformation_base64)
			if err != nil {
				return nil, err
			}
			out.Append(blob)
		}
		return out, nil
	}
	result, err := py.Getattr(future, "result")
	if err != nil {
		return nil, err
	}
	if py.IsNone(result) {
		return nil, nil
	}
	return result.(*Entry).Serialize(Transformation_base64)
}

// Get_future_result_entry_ is the RPC returning a local future's result
// entry(ies), wire-serialized.
//
// The data-bearing counterpart of Get_future_result_id_: used by
// RemoteFuture to materialize the real entry over the wire instead of a
// bare gid. Blocks until the future resolves.
func (p *LAILA_IDENTIFIABLE_POLICY_) Get_future_result_entry_(future_id string) (any, error) {
	future, err := p.bank_future_(future_id)
	if err != nil {
		return nil, err
	}
	return p.Serialize_future_result_(future)
}

// Wait_future_entry_ is the RPC blocking on a local future and returning
// its result entry(ies), serialized.
func (p *LAILA_IDENTIFIABLE_POLICY_) Wait_future_entry_(future_id string, timeout ...any) (any, error) {
	future, err := p.bank_future_(future_id)
	if err != nil {
		return nil, err
	}
	if _, err := callm(future, "wait", timeout_kw_(timeout)); err != nil {
		return nil, err
	}
	return p.Serialize_future_result_(future)
}
