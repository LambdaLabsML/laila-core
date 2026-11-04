package laila

import (
	"fmt"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/command/schema/future/future/complex_future.py --
// ComplexFuture: sequential composition of futures (Mode A: declarative
// pipeline).
//
// A ComplexFuture represents a pipeline of stages, executed one at a time
// in declared order. The number of stages and the function that produces
// each stage's Future (given the prior stage's result) is fixed at
// construction time. The actual stage Future instances are built lazily --
// stage ``k`` is constructed when stage ``k-1`` flips to
// FutureStatus_FINISHED.
//
// This is "Mode A" composition (immutable ``stage_fns``, lazy stage
// construction). Dynamic-shape pipelines are expressed via composition:
//
//   - A ``stage_fn`` may return another ComplexFuture or GroupFuture --
//     pipelines nest to arbitrary depth.
//   - A ``stage_fn`` may branch -- pick one of several futures based on
//     the prior stage's result.
//   - A ``stage_fn`` may return a GroupFuture -- fan out parallel work
//     whose aggregated result feeds the next stage.
//
// # Failure semantics
//
//   - If any stage flips to ``ERROR`` or ``CANCELLED``, the parent
//     ComplexFuture propagates that status (and exception) and no further
//     stages are constructed.
//   - Empty ``stage_fns`` is rejected at construction with ValueError.
//   - A ``stage_fn`` that returns a non-future or raises synchronously
//     when invoked transitions the parent to ``ERROR``.
//
// # Implementation notes
//
//   - Stage callbacks are wired through Future.Add_status_callback for
//     leaf futures. Group-future stages are polled on a daemon thread (see
//     Watch_group_future_) because GroupFuture exposes its status as a
//     percentage breakdown rather than a single FutureStatus enum member.
//   - Re-entrant locking via Stage_lock_ keeps the stage state-machine
//     consistent under concurrent callback delivery from worker threads.
//   - Pydantic v2's ``validate_python`` wipes any private-attribute writes
//     done before ``super().__init__`` returns, so the raw ``stage_fns``
//     argument is shuttled across the boundary via the thread-local PARK_
//     dict and re-attached in Model_post_init.

// StageFn is ``Callable[[Any], Future | GroupFuture |
// _LAILA_IDENTIFIABLE_FUTURE]``.
type StageFn = func(prior_result any) (any, error)

// PARK_ is the thread-local hand-off used by ComplexFuture to ferry the
// ``stage_fns`` argument from Init_ to Model_post_init across Pydantic
// v2's ``validate_python`` boundary, which wipes any private attribute
// writes made before it returns. Keyed by py.Get_ident(); populated and
// cleared inside ComplexFuture.Init_.
var PARK_ = map[int64][]any{}
var park_mu_ sync.Mutex

// stage_outcome_ is the ``_Holder`` duck type handed to On_stage_done_ /
// On_stage_failed_ by the group watcher (and satisfied by every Future).
type stage_outcome_ struct {
	result    any
	exception error
	status    FutureStatus
}

func (h *stage_outcome_) Result() (any, error) { return h.result, nil }
func (h *stage_outcome_) Exception() error      { return h.exception }
func (h *stage_outcome_) Status() FutureStatus  { return h.status }

// Status_callback_target is the ``add_status_callback`` surface of a leaf
// stage future (Future subclasses and RemoteFuture).
type Status_callback_target interface {
	Add_status_callback(status FutureStatus, fn Future_callback)
}

// ComplexFuture is the sequential composition of Future stages with
// declarative shape.
//
// ``stage_fns`` is an ordered list of callables. Stage 0 is invoked with
// nil; every subsequent stage receives the prior stage's resolved result.
// Each callable must return a Future, GroupFuture, or
// LAILA_IDENTIFIABLE_FUTURE_ (a future identity, which is resolved against
// the active local policy's future bank).
//
// Raises ValueError if ``stage_fns`` is empty at construction.
type ComplexFuture struct {
	Future
	Stage_future_ids []string `py:"stage_future_ids,factory=New_stage_future_ids_"`

	Stage_fns_   []any     `py:"_stage_fns"`
	Stage_lock_  *py.RLock `py:"_stage_lock"`
	Current_idx_ int       `py:"_current_idx"`
	Terminated_  bool      `py:"_terminated"`
}

// ComplexFuture_class is the class descriptor (``arbitrary_types_allowed``).
var ComplexFuture_class = py.NewClass("ComplexFuture", (*ComplexFuture)(nil), Future_class).
	Set_private_default("_stage_fns", func() any { return []any{} }).
	Set_private_default("_stage_lock", func() any { return py.NewRLock() }).
	Set_private_default("_current_idx", func() any { return -1 })

// New_ComplexFuture is ``ComplexFuture(**data)``. Required:
// ``taskforce_id``, ``policy_id``, ``stage_fns`` (list of callables).
func New_ComplexFuture(kw ...py.Kw) (*ComplexFuture, error) {
	f := ComplexFuture_class.New().(*ComplexFuture)
	if err := f.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return f, nil
}

// New_stage_future_ids_ is the ``stage_future_ids`` default factory.
func (f *ComplexFuture) New_stage_future_ids_() []string { return []string{} }

// Init_ validates ``stage_fns`` and parks it for Model_post_init.
func (f *ComplexFuture) Init_(data py.Kw) error {
	raw := data.Pop("stage_fns", nil)
	var stage_fns []any
	switch v := raw.(type) {
	case nil:
	case []any:
		stage_fns = v
	case py.Tuple:
		stage_fns = []any(v)
	case *py.List:
		stage_fns = v.Items()
	case []StageFn:
		for _, fn := range v {
			stage_fns = append(stage_fns, fn)
		}
	default:
		if py.Truthy(raw) {
			return py.TypeError.New("stage_fns must be a list or tuple of callables")
		}
	}
	if len(stage_fns) == 0 {
		return py.ValueError.New("ComplexFuture requires at least one stage")
	}
	for idx, fn := range stage_fns {
		if !py.Callable(fn) {
			return py.TypeError.Newf("stage_fns[%d] must be callable, got %s", idx, py.Pytypename(fn))
		}
	}
	// Pydantic v2 wipes private attributes during validate_python (called
	// inside super().__init__), so we cannot stash stage_fns on `self`
	// before the super call. Instead, hand them off via a thread-local
	// park so model_post_init can pick them up after validation.
	ident := py.Get_ident()
	park_mu_.Lock()
	PARK_[ident] = append([]any(nil), stage_fns...)
	park_mu_.Unlock()
	defer func() {
		park_mu_.Lock()
		delete(PARK_, ident)
		park_mu_.Unlock()
	}()
	return f.Future.Init_(data)
}

// Model_post_init registers with the active policy's future bank, then
// kicks off stage 0.
//
// Picks up the parked ``stage_fns`` list from PARK_ (set by Init_),
// reattaches it as a private attribute, then synchronously triggers
// construction of the first stage with ``prior_result=None``. Subsequent
// stages are constructed lazily inside the per-stage completion
// callbacks.
func (f *ComplexFuture) Model_post_init(ctx any) error {
	if err := f.Future.Model_post_init(ctx); err != nil {
		return err
	}
	park_mu_.Lock()
	parked, ok := PARK_[py.Get_ident()]
	park_mu_.Unlock()
	if ok {
		f.Stage_fns_ = append([]any(nil), parked...)
	}
	f.Kick_off_stage_(0, nil)
	return nil
}

// Current_stage_index is the 0-based index of the most recently
// constructed stage (-1 before the first stage has been kicked off).
func (f *ComplexFuture) Current_stage_index() int { return f.Current_idx_ }

// Num_stages is the total number of stages declared at construction.
func (f *ComplexFuture) Num_stages() int { return len(f.Stage_fns_) }

// Stage_futures resolves and returns the list of stage futures constructed
// so far. Lookups go through the active policy's future bank.
func (f *ComplexFuture) Stage_futures() []any {
	out := []any{}
	for _, fid := range f.Stage_future_ids {
		for _, policy := range Future_bank_policies_() {
			bank := future_bank_of_(policy)
			if bank == nil {
				continue
			}
			if v, ok := bank.Get(fid); ok {
				out = append(out, v)
				break
			}
		}
	}
	return out
}

// Release releases every stage future constructed so far, then this
// future.
func (f *ComplexFuture) Release() {
	for _, stage := range f.Stage_futures() {
		if r, ok := stage.(interface{ Release() }); ok {
			r.Release()
		}
	}
	f.Future.Release()
}

// Resolve_future_ returns the underlying Future or GroupFuture for value.
//
// Accepts a Future, GroupFuture, or a future identity (which is resolved
// against the active local policy's future bank).
func (f *ComplexFuture) Resolve_future_(value any) (any, error) {
	if py.Isinstance(value, Future_class) {
		return value, nil
	}
	if py.Isinstance(value, GroupFuture_class) {
		return value, nil
	}
	if py.Isinstance(value, LAILA_IDENTIFIABLE_FUTURE_class) {
		gid := value.(interface{ Global_id() string }).Global_id()
		for _, policy := range Future_bank_policies_() {
			bank := future_bank_of_(policy)
			if bank == nil {
				continue
			}
			if v, ok := bank.Get(gid); ok {
				return v, nil
			}
		}
		return nil, py.KeyError.Newf("Future %s not found in any local policy bank", gid)
	}
	return nil, py.TypeError.Newf(
		"stage_fn must return a Future / GroupFuture / future_identity, got %s", py.Pytypename(value))
}

// Kick_off_stage_ constructs stage idx and registers chaining callbacks
// on it.
func (f *ComplexFuture) Kick_off_stage_(idx int, prior_result any) {
	f.Stage_lock_.Lock_()
	defer f.Stage_lock_.Unlock()
	if f.Terminated_ {
		return
	}
	stage_fut, err := func() (stage any, err error) {
		defer py.Recover(&err)
		if idx >= len(f.Stage_fns_) {
			return nil, py.IndexError.New("list index out of range")
		}
		fn := f.Stage_fns_[idx]
		produced, err := py.Call(fn, []any{prior_result}, nil)
		if err != nil {
			return nil, err
		}
		return f.Resolve_future_(produced)
	}()
	if err != nil {
		f.Fail_(err, FutureStatus_ERROR)
		return
	}

	f.Stage_future_ids = append(f.Stage_future_ids, stage_fut.(interface{ Global_id() string }).Global_id())
	f.Current_idx_ = idx

	if gf, ok := stage_fut.(*GroupFuture); ok {
		f.Watch_group_future_(idx, gf)
	} else {
		target := stage_fut.(Status_callback_target)
		i := idx
		target.Add_status_callback(FutureStatus_FINISHED, func(sf any) { f.On_stage_done_(i, sf) })
		target.Add_status_callback(FutureStatus_ERROR, func(sf any) { f.On_stage_failed_(i, sf) })
		target.Add_status_callback(FutureStatus_CANCELLED, func(sf any) { f.On_stage_failed_(i, sf) })
	}

	if f.Status_ == FutureStatus_NOT_STARTED {
		f.Set_status(FutureStatus_RUNNING)
	}
}

// Watch_group_future_ drives a GroupFuture stage via a daemon-thread poll
// loop.
//
// GroupFuture exposes its status as a percentage breakdown rather than a
// single FutureStatus, so it does not support the callback hook used for
// Future chaining. We poll until all children are terminal, then
// synthesize the aggregated outcome and forward to On_stage_done_ /
// On_stage_failed_ exactly as we would for a leaf future.
func (f *ComplexFuture) Watch_group_future_(idx int, gf *GroupFuture) {
	poll_ := func() {
		poll_interval_s := 0.01
		for {
			st, err := gf.Status()
			if err != nil {
				f.On_stage_failed_(idx, &stage_outcome_{exception: err, status: FutureStatus_ERROR})
				return
			}
			pct := st.GetDefault("percentages", py.NewDict()).(*py.Dict)
			terminal_pct := py.Must(py.Float(pct.GetDefault("finished", 0.0))) +
				py.Must(py.Float(pct.GetDefault("error", 0.0))) +
				py.Must(py.Float(pct.GetDefault("cancelled", 0.0)))
			if terminal_pct >= 100.0 {
				break
			}
			py.Sleep_sync(poll_interval_s)
		}
		results, err := gf.Result()
		if err != nil {
			f.On_stage_failed_(idx, &stage_outcome_{exception: err, status: FutureStatus_ERROR})
			return
		}
		f.On_stage_done_(idx, &stage_outcome_{result: results})
	}
	t := py.New_Thread(py.Kw{"target": poll_, "name": fmt.Sprintf("ComplexFuture-GF-watch-%d", idx), "daemon": true})
	_ = t.Start()
}

// On_stage_done_ handles a stage finishing successfully -- kicks off the
// next one or finalizes.
func (f *ComplexFuture) On_stage_done_(idx int, fut any) {
	var (
		value any
		nxt   int
	)
	proceed := func() bool {
		f.Stage_lock_.Lock_()
		defer f.Stage_lock_.Unlock()
		if f.Terminated_ {
			return false
		}
		if idx != f.Current_idx_ {
			return false
		}
		v, err := fut.(interface{ Result() (any, error) }).Result()
		if err != nil {
			f.Fail_(err, FutureStatus_ERROR)
			return false
		}
		value = v
		nxt = idx + 1
		if nxt >= len(f.Stage_fns_) {
			f.Terminated_ = true
			f.Set_exception(nil)
			f.Set_result(value)
			f.Set_status(FutureStatus_FINISHED)
			return false
		}
		return true
	}()
	if proceed {
		f.Kick_off_stage_(nxt, value)
	}
}

// On_stage_failed_ propagates a stage's ERROR/CANCELLED to the parent.
func (f *ComplexFuture) On_stage_failed_(idx int, fut any) {
	f.Stage_lock_.Lock_()
	defer f.Stage_lock_.Unlock()
	if f.Terminated_ {
		return
	}
	if idx != f.Current_idx_ {
		return
	}
	var exc error
	if e, ok := fut.(interface{ Exception() error }); ok {
		exc = e.Exception()
	}
	status := FutureStatus_ERROR
	if s, ok := fut.(interface{ Status() FutureStatus }); ok {
		status = s.Status()
	}
	f.Fail_(exc, status)
}

// Fail_ marks the parent as failed/cancelled and seals further progress.
func (f *ComplexFuture) Fail_(exc error, status FutureStatus) {
	f.Terminated_ = true
	f.Set_exception(exc)
	f.Set_result(nil)
	f.Set_status(status)
}

// Wait blocks until the pipeline terminates.
//
// Polls the parent status (which is driven by stage callbacks). When the
// parent has reached a terminal status, returns the result or raises the
// captured exception.
//
// Raises LoopBlockingWaitError if called from a thread that owns an async
// event loop.
func (f *ComplexFuture) Wait(timeout ...any) (any, error) {
	if err := Check_not_loop_thread_(); err != nil {
		return nil, err
	}
	t, has := Timeout_arg_(timeout)
	return Park_sync(func() (any, error) { return f.Wait_impl_(t, has) })
}

// terminal_outcome_ is the shared "read status under the lock and settle"
// step of Wait_impl_ / Await: (value, error, done).
func (f *ComplexFuture) terminal_outcome_() (any, error, bool) {
	f.Lock()
	status := f.Status_
	exc := f.Exception_
	f.Unlock()

	if status == FutureStatus_FINISHED {
		v, err := f.Materialize_result_()
		return v, err, true
	}
	if status == FutureStatus_ERROR || status == FutureStatus_CANCELLED {
		if exc != nil {
			return nil, exc, true
		}
		return nil, py.RuntimeError.Newf("ComplexFuture ended with status=%s and no exception.", status.Repr()), true
	}
	return nil, nil, false
}

// Wait_impl_ polls the parent status until terminal or deadline.
func (f *ComplexFuture) Wait_impl_(timeout float64, has bool) (any, error) {
	deadline := 0.0
	if has {
		deadline = py.Monotonic() + timeout
	}
	poll_interval_s := 0.01
	for {
		if v, err, done := f.terminal_outcome_(); done {
			return v, err
		}
		if has && py.Monotonic() >= deadline {
			f.Default_callbacks_[FutureStatus_POLL_TIMEOUT](f.Self())
			return nil, py.TimeoutError.New()
		}
		py.Sleep_sync(poll_interval_s)
	}
}

// Await awaits the pipeline's terminal status by yielding to the event
// loop. Parks the current taskforce slot (if any) while pending.
func (f *ComplexFuture) Await() (any, error) {
	await_terminal_ := func() (any, error) {
		poll_interval_s := 0.01
		for {
			if v, err, done := f.terminal_outcome_(); done {
				return v, err
			}
			if err := py.Sleep(poll_interval_s); err != nil {
				return nil, err
			}
		}
	}
	return Park_async(py.Coroutine(await_terminal_))
}

func init() {
	py.Register_property("current_stage_index", "num_stages")
}
