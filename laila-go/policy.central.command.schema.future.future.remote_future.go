package laila

import (
	"fmt"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/command/schema/future/future/remote_future.py --
// pydantic-backed proxy for a future that lives on a remote policy.
//
// RemoteFuture is the client-side handle returned whenever an RPC call on
// a peer yields a serialized future payload (the peer's Future
// ``model_dump`` includes a ``__laila_future__`` marker that
// LAILA_IDENTIFIABLE_COMMUNICATION_.Maybe_wrap_remote_future_ rehydrates
// into a RemoteFuture on this side).
//
// # Why it inherits from _LAILA_IDENTIFIABLE_FUTURE
//
// The identity base class gives a remote future the *same* gid /
// ``future_bank`` / guarantee-scope behaviour as a local future, so
// existing helpers (``laila.runtime.wait``, ``with laila.guarantee:``,
// remote-fetch chains) can treat both flavors uniformly. Only the status /
// result / exception / wait accessors override the base to forward over
// the owning Communication channel instead of reading local state.
//
// # Result handling
//
// RemoteFuture fully mirrors a local Future: Result / Wait block until the
// peer's future completes, transfer the result entry's bytes over the
// wire (serialized with ``transformation_base64`` on the peer, rebuilt via
// Build_by_scope here), and return the real Entry; Data returns its
// payload. The materialized result is cached so repeated access never
// re-transfers. Status / Exception remain lightweight proxies that do not
// move the payload. The cheap result-gid pointer is still available on the
// peer via ``_get_future_result_id`` for callers that want it.

// RemoteFuture is a proxy for a future that lives on a remote policy's
// future bank.
//
// The local instance carries only identity data plus a back-reference to
// the communication channel that serves the owning peer; every status or
// result query is forwarded over that channel.
//
//   - Taskforce_id: ``global_id`` of the taskforce on the remote side
//     (falls back to ``peer_id`` when the remote payload omits it).
//   - Policy_id: ``global_id`` of the remote policy that owns the future.
//   - uuid: the trailing uuid segment of the remote future's ``global_id``
//     so the local identity's ``global_id`` matches its counterpart on the
//     peer.
type RemoteFuture struct {
	LAILA_IDENTIFIABLE_FUTURE_
	Comm_             any  `py:"_comm"`
	Is_group_         bool `py:"_is_group"`
	Comm_selector_    any  `py:"_comm_selector"`
	Materialized_     any  `py:"_materialized"`
	Materialized_set_ bool `py:"_materialized_set"`
}

// RemoteFuture_class is the class descriptor (``arbitrary_types_allowed``).
var RemoteFuture_class = py.NewClass("RemoteFuture", (*RemoteFuture)(nil), LAILA_IDENTIFIABLE_FUTURE_class)

// New_RemoteFuture is ``RemoteFuture(**data)``.
func New_RemoteFuture(kw ...py.Kw) (*RemoteFuture, error) {
	f := RemoteFuture_class.New().(*RemoteFuture)
	if err := f.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return f, nil
}

// Bind attaches the communication channel and group-flag after
// construction (``bind(communication, *, is_group=False, comm=None)``).
//
// Called by LAILA_IDENTIFIABLE_COMMUNICATION_.Maybe_wrap_remote_future_
// once the proxy has been built. The comm selector (a communication id /
// protocol token, or nil) is remembered so every follow-up Status / Wait /
// Result call stays on the same transport that produced the future.
func (f *RemoteFuture) Bind(communication any, kw ...py.Kw) {
	opts := py.MergeKw(kw...)
	f.Comm_ = communication
	f.Is_group_ = py.Truthy(opts.Get("is_group", false))
	f.Comm_selector_ = opts.Get("comm", nil)
}

// Model_post_init applies staged identity fields and self-registers with
// the active local policy.
//
// Mirrors Future.Model_post_init so a remote future participates in
// guarantee scopes and the future bank just like a local one. The
// communication channel is bound separately via Bind after construction
// completes.
func (f *RemoteFuture) Model_post_init(ctx any) error {
	if err := f.LAILA_IDENTIFIABLE_FUTURE_.Model_post_init(ctx); err != nil {
		return err
	}
	// Always the *caller's* side: ``policy_id`` names the peer, and the
	// handle shares the peer future's uuid, so registering by
	// ``policy_id`` (as local futures do) would clobber the peer's own
	// bank slot when both policies live in one process.
	policy, err := Get_active_local_policy_()
	if err != nil {
		return err
	}
	cmd := getattr(getattr(policy, "central"), "command")
	if r, ok := cmd.(Guarantee_registrar); ok {
		r.Register_future_with_active_guarantees_(f.Self())
	} else if _, err := callm(cmd, "_register_future_with_active_guarantees", f.Self()); err != nil {
		return err
	}
	bank := future_bank_of_(policy)
	if bank == nil {
		return py.AttributeError.Newf("'%s' object has no attribute 'future_bank'", py.Pytypename(policy))
	}
	bank.Set(f.Global_id(), f.Self())
	return nil
}

// Is_group is true when the remote future is a GroupFuture.
func (f *RemoteFuture) Is_group() bool { return f.Is_group_ }

// send_rpc_ is ``self._comm._send_rpc(str(self.policy_id), path,
// (self.global_id,), kwargs, comm=self._comm_selector)``.
func (f *RemoteFuture) send_rpc_(path []string, kwargs py.Kw) (any, error) {
	if kwargs == nil {
		kwargs = py.Kw{}
	}
	if comm, ok := f.Comm_.(*LAILA_IDENTIFIABLE_COMMUNICATION_); ok {
		return comm.Send_rpc_(py.Str(f.Policy_id), path, []any{f.Global_id()}, kwargs, f.Comm_selector_)
	}
	if py.IsNone(f.Comm_) {
		return nil, py.AttributeError.New("'NoneType' object has no attribute '_send_rpc'")
	}
	return callm(f.Comm_, "_send_rpc", py.Str(f.Policy_id), path, py.Tuple{f.Global_id()}, py.DictFromKw(kwargs), py.Kw{"comm": f.Comm_selector_})
}

// Status queries the remote policy for this future's current status.
func (f *RemoteFuture) Status() FutureStatus {
	s, err := f.Status_()
	if err != nil {
		panic(err)
	}
	st, ok := s.(FutureStatus)
	if !ok {
		return FutureStatus(py.Str(s))
	}
	return st
}

// Status_ is the error-returning form of Status: the FutureStatus (or the
// raw value when the peer returned something unrecognisable).
func (f *RemoteFuture) Status_() (any, error) {
	raw, err := f.send_rpc_([]string{"_get_future_status"}, nil)
	if err != nil {
		return nil, err
	}
	if st, ok := raw.(FutureStatus); ok {
		return st, nil
	}
	if s, ok := raw.(string); ok {
		if st := FutureStatus(s); st.Valid() {
			return st, nil
		}
		if st, err := FutureStatus_by_name(s); err == nil {
			return st, nil
		}
		return raw, nil
	}
	return raw, nil
}

// Rebuild_ rebuilds a live Entry (or list for a group) from a wire blob.
func (f *RemoteFuture) Rebuild_(blob any) (any, error) {
	if f.Is_group_ {
		out := py.NewList()
		for _, b := range py.Iter(blob) {
			if py.IsNone(b) {
				out.Append(nil)
				continue
			}
			e, err := Build_by_scope(b, py.Kw{"asynchronous": false})
			if err != nil {
				return nil, err
			}
			out.Append(e)
		}
		return out, nil
	}
	if py.IsNone(blob) {
		return nil, nil
	}
	return Build_by_scope(blob, py.Kw{"asynchronous": false})
}

// Result returns the rebuilt result Entry from the remote future.
//
// Mirrors a local Future.Result: blocks until the peer's future
// completes, transfers the entry over the wire, and returns the rebuilt
// Entry (a list for a group future). Cached so a second access does not
// re-transfer.
func (f *RemoteFuture) Result() (any, error) {
	if f.Materialized_set_ {
		return f.Materialized_, nil
	}
	blob, err := f.send_rpc_([]string{"_get_future_result_entry"}, nil)
	if err != nil {
		return nil, err
	}
	entry, err := f.Rebuild_(blob)
	if err != nil {
		return nil, err
	}
	f.Materialized_ = entry
	f.Materialized_set_ = true
	return entry, nil
}

// Data returns the payload of the result entry (mirrors local
// Future.Data).
//
// Blocking: materializes the entry from the peer if needed, then unwraps
// its ``data`` (a list of payloads for a group future).
func (f *RemoteFuture) Data() (any, error) {
	result, err := f.Result()
	if err != nil {
		return nil, err
	}
	if f.Is_group_ {
		out := py.NewList()
		for _, e := range py.Iter(result) {
			if py.IsNone(e) {
				out.Append(nil)
				continue
			}
			d, err := py.Getattr(e, "data")
			if err != nil {
				return nil, err
			}
			out.Append(d)
		}
		return out, nil
	}
	if py.IsNone(result) {
		return nil, nil
	}
	return py.Getattr(result, "data")
}

// Exception returns the remote exception as an error (nil when the peer
// reports none). See Exception_ for the raw serialized dict.
func (f *RemoteFuture) Exception() error {
	raw, err := f.Exception_()
	if err != nil {
		return err
	}
	if py.IsNone(raw) {
		return nil
	}
	if d, ok := raw.(*py.Dict); ok {
		t := py.Str(d.GetDefault("type", "RuntimeError"))
		msg := py.Str(d.GetDefault("message", ""))
		return py.RuntimeError.New(t + ": " + msg).With("remote", d)
	}
	return py.RuntimeError.New(py.Str(raw))
}

// Exception_ returns a serialized representation of the remote
// exception, if any (``exception`` property).
func (f *RemoteFuture) Exception_() (any, error) {
	return f.send_rpc_([]string{"_get_future_exception"}, nil)
}

// Wait blocks until the remote future completes; returns the rebuilt
// Entry (``wait(timeout=None)``).
//
// Mirrors a local Future.Wait: transfers the result entry over the wire
// and returns the rebuilt Entry (cached). LoopBlockingWaitError if called
// from a thread that owns an async event loop.
func (f *RemoteFuture) Wait(timeout ...any) (any, error) {
	if err := Check_not_loop_thread_(); err != nil {
		return nil, err
	}
	if f.Materialized_set_ {
		return f.Materialized_, nil
	}
	var t any
	if tv, has := Timeout_arg_(timeout); has {
		t = tv
	}
	// The RPC blocks on a remote future; park the local slot meanwhile.
	blob, err := Park_sync(func() (any, error) {
		return f.send_rpc_([]string{"_wait_future_entry"}, py.Kw{"timeout": t})
	})
	if err != nil {
		return nil, err
	}
	entry, err := f.Rebuild_(blob)
	if err != nil {
		return nil, err
	}
	f.Materialized_ = entry
	f.Materialized_set_ = true
	return entry, nil
}

// Await is ``__await__``: await the remote future without blocking the
// calling event loop.
//
// The blocking RPC Wait is offloaded to a worker thread via py.To_thread,
// so the event loop stays free to service other coroutines while the wait
// is in flight. The current taskforce slot (if any) is parked for the
// duration.
func (f *RemoteFuture) Await() (any, error) {
	run := func() (any, error) {
		return py.To_thread(func() (any, error) { return f.Wait(nil) })
	}
	return Park_async(py.Coroutine(run))
}

// Repr is ``RemoteFuture('gid', policy='pid')`` (``RemoteGroupFuture`` for
// a group).
func (f *RemoteFuture) Repr() string {
	kind := "RemoteFuture"
	if f.Is_group_ {
		kind = "RemoteGroupFuture"
	}
	return fmt.Sprintf("%s(%s, policy=%s)", kind, py.Repr(f.Global_id()), py.Repr(f.Policy_id))
}

func init() { py.Register_property("is_group", "status", "result", "data", "exception") }
