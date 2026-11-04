package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// policy/central/command/schema/future/future/group_future.py --
// GroupFuture: aggregate future tracking a fixed set of children.
//
// A group future is the natural return type of any "submit N tasks
// together" operation (see CentralCommand.Submit with multiple tasks,
// CentralMemory.Memorize with multiple entries, the manifest-level
// helpers, etc.). It owns no execution itself; instead it references the
// children by ``global_id`` and delegates ``status``, ``result``, ``wait``,
// and ``__await__`` to them.
//
// Children are looked up through the future banks on every access -- the
// owning policy's bank first (``policy_id``), then every other local
// policy's; this means the group keeps working even if the local
// references to the children are dropped, as long as they remain in a
// bank, and regardless of which policy is currently active.

// Iter_future_banks_ yields candidate future banks: the owning policy's
// first, then every other local one.
//
// A group handle must keep resolving its children after the user
// activates another policy, so lookups are not tied to the *active*
// policy (the same rule Future.Release and ``runtime.wait`` follow).
func Iter_future_banks_(policy_id any) ([]*py.Dict, error) {
	pid := Gid_of_(policy_id)
	var owner any
	if s, ok := pid.(string); ok {
		owner, _ = Local_policies_.Get(s)
		if py.IsNone(owner) {
			owner, _ = Live_policies_.Get(s)
		}
	}
	seen := map[uintptr]struct{}{}
	banks := []*py.Dict{}
	if !py.IsNone(owner) {
		seen[py.Id(owner)] = struct{}{}
		banks = append(banks, future_bank_of_(owner))
	}
	for _, policy := range Future_bank_policies_() {
		id := py.Id(policy)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		banks = append(banks, future_bank_of_(policy))
	}
	if len(seen) == 0 {
		// No local policy registered yet: fall back to (lazily creating)
		// the default one, as every other bank accessor does.
		pol, err := Get_active_local_policy_()
		if err != nil {
			return nil, err
		}
		banks = append(banks, future_bank_of_(pol))
	}
	return banks, nil
}

// GroupFuture is the aggregate future grouping multiple child futures
// under one handle.
//
// The aggregate's status is a *percentage breakdown* of its children (see
// Status) rather than a single FutureStatus, because aggregating
// heterogeneous outcomes into a single state quickly becomes lossy. Use
// Wait (or Await) to block until every child completes; Result then
// collects the children's results in registration order.
type GroupFuture struct {
	LAILA_IDENTIFIABLE_OBJECT_
	Taskforce_id any      `py:"taskforce_id,required"`
	Policy_id    any      `py:"policy_id,required"`
	Future_ids   []string `py:"future_ids,factory=New_future_ids_"`
}

// GroupFuture_class is the class descriptor (``_DEFAULT_SCOPES =
// [_GROUP_FUTURE_SCOPE]``, ``arbitrary_types_allowed``).
var GroupFuture_class = py.NewClass("GroupFuture", (*GroupFuture)(nil), LAILA_IDENTIFIABLE_OBJECT_class).
	Set_classvar("_DEFAULT_SCOPES", []string{GROUP_FUTURE_SCOPE_})

// New_GroupFuture is ``GroupFuture(**data)``. Required: ``taskforce_id``,
// ``policy_id``; optional ``future_ids`` plus the identity keywords.
func New_GroupFuture(kw ...py.Kw) (*GroupFuture, error) {
	g := GroupFuture_class.New().(*GroupFuture)
	if err := g.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return g, nil
}

// New_future_ids_ is the ``future_ids`` default factory (``list``).
func (g *GroupFuture) New_future_ids_() []string { return []string{} }

// Model_post_init registers this group future with its owning local
// policy's future bank.
//
// The owner is the local policy named by Policy_id when there is one,
// else the active local policy (see Owning_local_policy_).
func (g *GroupFuture) Model_post_init(ctx any) error {
	if err := g.LAILA_IDENTIFIABLE_OBJECT_.Model_post_init(ctx); err != nil {
		return err
	}
	policy, err := Owning_local_policy_(g.Policy_id)
	if err != nil {
		return err
	}
	cmd := getattr(getattr(policy, "central"), "command")
	if r, ok := cmd.(Guarantee_registrar); ok {
		r.Register_future_with_active_guarantees_(g.Self())
	} else if _, err := callm(cmd, "_register_future_with_active_guarantees", g.Self()); err != nil {
		return err
	}
	bank := future_bank_of_(policy)
	if bank == nil {
		return py.AttributeError.Newf("'%s' object has no attribute 'future_bank'", py.Pytypename(policy))
	}
	bank.Set(g.Global_id(), g.Self())
	_ = attempt(func() error {
		lg, err := Get_logger()
		if err != nil {
			return err
		}
		lg.Record_group_future_created(g.Self())
		return nil
	})
	return nil
}

// Resolve_children_ looks up child future objects from the future bank.
//
// With strict=false children that have already been released from the
// bank are skipped instead of raising KeyError -- used by the
// introspection paths (Status, What, Repr) which must stay safe to call
// after Release.
func (g *GroupFuture) Resolve_children_(strict bool) ([]any, error) {
	banks, err := Iter_future_banks_(g.Policy_id)
	if err != nil {
		return nil, err
	}
	children := []any{}
	for _, fid := range g.Future_ids {
		found := false
		for _, bank := range banks {
			if bank == nil {
				continue
			}
			if child, ok := bank.Get(fid); ok && !py.IsNone(child) {
				children = append(children, child)
				found = true
				break
			}
		}
		if !found && strict {
			return nil, py.KeyError.New(fid)
		}
	}
	return children, nil
}

// Release releases this group -- and by default every child -- from the
// future bank (``release(*, children=True)``).
//
// Children are reachable only through the bank (the group stores
// Future_ids), so the group owns their release. Pass ``children=False``
// to drop only the group shell, e.g. when the child ids have been
// re-parented into another group. Idempotent: children that are already
// gone are skipped. Nothing calls this automatically -- the holder of the
// group handle must.
func (g *GroupFuture) Release_with(kw ...py.Kw) {
	opts := py.MergeKw(kw...)
	children := py.Truthy(opts.Get("children", true))
	if children {
		for _, fid := range append([]string(nil), g.Future_ids...) {
			for _, policy := range Future_bank_policies_() {
				bank := future_bank_of_(policy)
				if bank == nil {
					continue
				}
				child, ok := bank.Get(fid)
				if ok && !py.IsNone(child) {
					if r, ok := child.(interface{ Release() }); ok {
						r.Release()
					} else {
						bank.PopDefault(fid, nil)
					}
					break
				}
			}
		}
	}
	gid := g.Global_id()
	for _, policy := range Future_bank_policies_() {
		bank := future_bank_of_(policy)
		if bank != nil && py.Is(bank.GetDefault(gid, nil), g.Self()) {
			bank.PopDefault(gid, nil)
			return
		}
	}
}

// Release is ``release()`` with the default ``children=True``.
func (g *GroupFuture) Release() { g.Release_with() }

// ---------- computed status ----------

// Status returns a percentage breakdown of child statuses.
//
// The returned dict has shape::
//
//	{
//	    "total": float,
//	    "percentages": {
//	        "finished":    float,
//	        "running":     float,
//	        "not_started": float,
//	        "error":       float,
//	        "cancelled":   float,
//	    }
//	}
//
// The percentages always sum to 100 except in the empty-group edge case
// (everything is reported as 100% ``not_started`` for a group with no
// children, so callers can rely on the same dict shape regardless of
// population). ``total`` is always the number of child ids in the group;
// percentages are computed over the children still present in the future
// bank, so a group whose children were released reports the empty-group
// percentages rather than raising. Non-terminal transient states
// (``POLL_TIMEOUT`` -- a poll timed out, the task is still in flight --
// and ``UNKNOWN``) are counted as ``running``.
func (g *GroupFuture) Status() (*py.Dict, error) {
	var children []any
	if len(g.Future_ids) > 0 {
		var err error
		children, err = g.Resolve_children_(false)
		if err != nil {
			return nil, err
		}
	}
	if len(children) == 0 {
		return py.DictOf(
			"total", float64(len(g.Future_ids)),
			"percentages", py.DictOf(
				"finished", 0.0,
				"running", 0.0,
				"not_started", 100.0,
				"error", 0.0,
				"cancelled", 0.0,
			),
		), nil
	}

	live := float64(len(children))
	var running, not_started, cancelled, finished, errored int
	for _, f := range children {
		s, err := child_status_(f)
		if err != nil {
			return nil, err
		}
		switch s {
		case FutureStatus_RUNNING, FutureStatus_POLL_TIMEOUT, FutureStatus_UNKNOWN:
			running++
		case FutureStatus_NOT_STARTED:
			not_started++
		case FutureStatus_CANCELLED:
			cancelled++
		case FutureStatus_FINISHED:
			finished++
		case FutureStatus_ERROR:
			errored++
		}
	}

	return py.DictOf(
		"total", float64(len(g.Future_ids)),
		"percentages", py.DictOf(
			"finished", (float64(finished)/live)*100.0,
			"running", (float64(running)/live)*100.0,
			"not_started", (float64(not_started)/live)*100.0,
			"error", (float64(errored)/live)*100.0,
			"cancelled", (float64(cancelled)/live)*100.0,
		),
	), nil
}

// child_status_ is ``f.status`` for any bank member (a concrete future's
// FutureStatus; a RemoteFuture may resolve it through RPC).
func child_status_(f any) (FutureStatus, error) {
	switch c := f.(type) {
	case interface{ Status() FutureStatus }:
		return c.Status(), nil
	case interface{ Status() (FutureStatus, error) }:
		return c.Status()
	}
	v, err := py.Getattr(f, "status")
	if err != nil {
		return "", err
	}
	return FutureStatus_of(v)
}

// ---------- read-only interface (except cancel passthrough) ----------

// Append merges additional child future IDs into this group.
func (g *GroupFuture) Append(future_ids []string) {
	g.Future_ids = append(g.Future_ids, future_ids...)
}

// Add is ``__add__``: returns self with merged child future IDs from
// another GroupFuture (TypeError for anything else -- ``NotImplemented``
// with no reflected operand).
func (g *GroupFuture) Add(other any) (*GroupFuture, error) {
	o, ok := other.(*GroupFuture)
	if !ok {
		return nil, py.TypeError.Newf("unsupported operand type(s) for +: 'GroupFuture' and '%s'", py.Pytypename(other))
	}
	g.Future_ids = append(g.Future_ids, o.Future_ids...)
	return g, nil
}

// Wait blocks until every child future completes; returns their results.
//
// Children are waited *sequentially*. The same timeout value is passed to
// each child individually (in seconds), so the total wall time can be up
// to ``timeout * len(children)`` -- callers that need a hard total budget
// should compute it themselves and pass smaller per-child timeouts.
//
// Returns one entry per child (a *py.List), in the order Future_ids.
//
// Raises LoopBlockingWaitError if called from a thread that owns an async
// event loop (use ``await group_future`` from coroutines instead) and
// RuntimeError if a child does not expose a ``wait`` method.
func (g *GroupFuture) Wait(timeout ...any) (any, error) {
	if err := Check_not_loop_thread_(); err != nil {
		return nil, err
	}

	wait_all_ := func() (any, error) {
		children, err := g.Resolve_children_(true)
		if err != nil {
			return nil, err
		}
		return_values := py.NewList()
		for _, f := range children {
			w, ok := f.(Future_waiter)
			if !ok {
				return nil, py.RuntimeError.New("Future is not associated with a native future.")
			}
			v, err := w.Wait(timeout...)
			if err != nil {
				return nil, err
			}
			return_values.Append(v)
		}
		return return_values, nil
	}

	// Park once for the whole group so nested child waits do not
	// release/re-acquire the slot per child.
	return Park_sync(wait_all_)
}

// Result collects results from all children without blocking.
//
// Assumes every child has already completed (e.g. after a
// ``laila.guarantee`` block).
func (g *GroupFuture) Result() (any, error) {
	children, err := g.Resolve_children_(true)
	if err != nil {
		return nil, err
	}
	out := py.NewList()
	for _, f := range children {
		v, err := f.(interface{ Result() (any, error) }).Result()
		if err != nil {
			return nil, err
		}
		out.Append(v)
	}
	return out, nil
}

// Data returns the unwrapped payload data from every child future.
//
// Raises RuntimeError if any child future's result is not an Entry
// instance.
func (g *GroupFuture) Data() (any, error) {
	children, err := g.Resolve_children_(true)
	if err != nil {
		return nil, err
	}
	out := py.NewList()
	for _, f := range children {
		v, err := f.(interface{ Data() (any, error) }).Data()
		if err != nil {
			return nil, err
		}
		out.Append(v)
	}
	return out, nil
}

// Await awaits all children concurrently via ``asyncio.gather``.
//
// Parks the current taskforce slot (if any) for the whole group.
func (g *GroupFuture) Await() (any, error) {
	await_all_ := func() (any, error) {
		children, err := g.Resolve_children_(true)
		if err != nil {
			return nil, err
		}
		fns := make([]func() (any, error), len(children))
		for i, c := range children {
			fns[i] = c.(py.Awaitable).Await
		}
		results, err := py.Gather(fns)
		if err != nil {
			return nil, err
		}
		return py.NewList(results...), nil
	}
	return Park_async(py.Coroutine(await_all_))
}

// Exception is the first child exception, or nil when no child has
// failed.
//
// Mirrors Future.Exception so that group handles work with
// ``laila.runtime.exception(...)`` and the other status helpers.
func (g *GroupFuture) Exception() error {
	children, err := g.Resolve_children_(false)
	if err != nil {
		return nil
	}
	for _, f := range children {
		if e, ok := getattr(f, "exception").(error); ok && e != nil {
			return e
		}
	}
	return nil
}

// ---------- introspection ----------

// What is a nested summary keyed by task_group_id.
func (g *GroupFuture) What() (*py.Dict, error) {
	group_key := g.Global_id()
	child_details := py.NewDict()
	cancelled_ids := py.NewList()
	not_cancelled_ids := py.NewList()
	errors := py.NewDict()

	children, err := g.Resolve_children_(false)
	if err != nil {
		return nil, err
	}
	for _, f := range children {
		fid := py.Str(getattr(f, "global_id"))
		var det *py.Dict
		if py.Hasattr(f, "what") {
			det, _ = getattr(f, "what").(*py.Dict)
		} else if py.Hasattr(f, "details") {
			det, _ = getattr(f, "details").(*py.Dict)
		}
		if det == nil {
			status := getattr(f, "status")
			if py.IsNone(status) {
				status = FutureStatus_UNKNOWN
			}
			var errRepr any
			if py.Eq(status, FutureStatus_ERROR) {
				errRepr = py.Repr(getattr(f, "exception"))
			}
			det = py.DictOf(fid, py.DictOf(
				"status", enum_value_(status),
				"error", errRepr,
			))
		}
		for _, it := range det.Items() {
			child_details.Set(it.Key, it.Value)
		}

		if err := attempt(func() error {
			cancelled := false
			if c, ok := f.(interface{ Cancelled() bool }); ok {
				cancelled = c.Cancelled()
			}
			if cancelled {
				cancelled_ids.Append(fid)
			} else {
				not_cancelled_ids.Append(fid)
			}
			st, err := child_status_(f)
			if err != nil {
				return err
			}
			if st == FutureStatus_ERROR {
				errors.Set(fid, py.Repr(getattr(f, "exception")))
			}
			return nil
		}); err != nil {
			errors.Set(fid, py.Repr(err))
		}
	}

	status, err := g.Status()
	if err != nil {
		return nil, err
	}
	return py.DictOf(
		group_key, py.DictOf(
			"status", status,
			"taskforce_id", g.Taskforce_id,
			"policy_id", g.Policy_id,
			"futures", child_details,
			"summary", py.DictOf(
				"cancelled", cancelled_ids,
				"not_cancelled", not_cancelled_ids,
				"errors", errors,
			),
		),
	), nil
}

// String returns the JSON representation of the group.
func (g *GroupFuture) String() string {
	what, err := g.What()
	if err != nil {
		panic(err)
	}
	return py.Must(py.Json_dumps(what))
}

// Repr returns the JSON representation of the group.
func (g *GroupFuture) Repr() string { return g.String() }

// Iter iterates over child future IDs (``__iter__``).
func (g *GroupFuture) Iter() []any { return stringsToAny(g.Future_ids) }

// Len returns the number of child futures (``__len__``).
func (g *GroupFuture) Len() int { return len(g.Future_ids) }

func init() {
	py.Register_property("what")
}
