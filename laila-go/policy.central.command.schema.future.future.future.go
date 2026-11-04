package laila

import (
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/command/schema/future/future/future.py -- abstract Future
// base class: identity, status, callbacks, lifecycle.
//
// A Future is the laila-side analogue of ``concurrent.futures.Future`` /
// ``asyncio.Future``, but extended with three things the standard library
// doesn't provide:
//
//   - Identity. A future has a stable ``global_id`` (UUID + scope) so it
//     can be referenced across processes (see RemoteFuture) and looked up
//     in the owning policy's ``future_bank`` after the local handle has
//     gone out of scope.
//   - Result-as-Entry. Reading ``future.result`` always yields an Entry:
//     non-Entry values set by the producer are wrapped in Entry_constant
//     lazily on first read, so every future ultimately resolves to an
//     addressable entry that can be ``laila.remember``'d on a peer without
//     the runner paying for the wrap up front.
//   - Explicit release. Every future is registered in the owning policy's
//     ``future_bank`` and stays there until Future.Release is called --
//     nothing evicts futures automatically.
//   - Status callbacks. Multiple callbacks can be registered against any
//     FutureStatus transition via Add_status_callback; late registrations
//     on already-fired statuses fire immediately to close the obvious
//     race.
//
// Concrete subclasses provide the actual ``wait`` / ``__await__`` /
// producer-side completion logic: ConcurrentPackageFuture (backed by a
// py.CFuture from a thread or process pool), ComplexFuture (chains of
// dependent futures; deprecated), GroupFuture (aggregates multiple child
// futures) and RemoteFuture (proxies a future that lives on a peer).

// Future_callback is ``Callable[[Future], Any]``: a status callback. It
// receives the concrete future (``self``), never the blocking ``result``.
type Future_callback = func(f any)

func Make_status_setter_(status FutureStatus) Future_callback {
	set_ := func(f any) {
		if s, ok := f.(interface{ Set_status(FutureStatus) }); ok {
			s.Set_status(status)
		}
	}
	// ``_set.__name__ = f"_set_status_{status.name.lower()}"``
	_ = "_set_status_" + strings.ToLower(status.Name())
	return set_
}

// DEFAULT_STATUS_CALLBACKS_ is the shared "set my status to X" table used
// by every Future.
//
// Stateless, so one dict serves all instances -- building seven closures
// per future was measurable on the per-task hot path.
var DEFAULT_STATUS_CALLBACKS_ = func() map[FutureStatus]Future_callback {
	m := map[FutureStatus]Future_callback{}
	for _, st := range FutureStatus_members_ {
		m[st] = Make_status_setter_(st)
	}
	return m
}()

// Future_waiter is the hook concrete subclasses override: the base Result
// / Data accessors dispatch ``self.wait(timeout=None)`` through it.
type Future_waiter interface {
	Wait(timeout ...any) (any, error)
}

// Guarantee_registrar is ``policy.central.command`` as seen from the
// Future constructor (``_register_future_with_active_guarantees``).
type Guarantee_registrar interface {
	Register_future_with_active_guarantees_(fut any)
}

// Future is the abstract base class for all in-process laila futures.
//
// Inherits identity (``taskforce_id``, ``policy_id``, ``future_group_id``,
// ``precedence``, ``purpose`` and the usual ``uuid`` / ``scopes``) from
// LAILA_IDENTIFIABLE_FUTURE_, and adds:
//
//   - ``status`` (FutureStatus) -- the lifecycle marker.
//   - ``result`` -- the (possibly auto-wrapped) Entry outcome.
//   - ``exception`` -- the failure outcome.
//   - ``_result_global_id`` -- the gid of the result entry (handy for
//     cross-policy remembering).
//   - ``callbacks`` / ``_status_callbacks`` -- per-status hook lists.
//
// On construction the future self-registers into the active local
// policy's ``future_bank`` and into every open guarantee scope on the
// calling thread. That is what makes ``with laila.guarantee:`` work
// without any explicit registration on the caller's part.
//
// Subclasses must override Wait (and typically Await) to hook into their
// concrete completion mechanism.
type Future struct {
	LAILA_IDENTIFIABLE_FUTURE_
	Status_              FutureStatus `py:"_status"`
	Return_value_        any          `py:"_return_value"`
	Exception_           error        `py:"_exception"`
	Result_global_id_    *string      `py:"_result_global_id"`
	Result_pending_wrap_ bool         `py:"_result_pending_wrap"`
	Timeout_ms_          int          `py:"_timeout_ms"`

	// Default_callbacks_ is shared and immutable (see
	// DEFAULT_STATUS_CALLBACKS_); Status_callbacks_ is created in
	// Model_post_init. Neither uses ``PrivateAttr(default_factory=...)``
	// because pydantic re-inspects a private factory's signature on every
	// instantiation -- futures are constructed on the per-task hot path.
	Default_callbacks_ map[FutureStatus]Future_callback   `py:"_default_callbacks"`
	Status_callbacks_  map[FutureStatus][]Future_callback `py:"_status_callbacks"`
	// Callbacks mirrors the *last* callback registered per status (a
	// single-slot view); Status_callbacks_ is the registry that actually
	// fires on transitions.
	Callbacks map[FutureStatus]Future_callback `py:"callbacks,factory=New_callbacks_"`
}

// Future_class is the class descriptor (``arbitrary_types_allowed``).
var Future_class = py.NewClass("Future", (*Future)(nil), LAILA_IDENTIFIABLE_FUTURE_class).
	Set_private_default("_status", func() any { return FutureStatus_NOT_STARTED }).
	Set_private_default("_timeout_ms", func() any { return 100 })

// New_Future is ``Future(**data)``. Required: ``taskforce_id``,
// ``policy_id``; optional ``future_group_id``, ``precedence``,
// ``purpose``, ``callbacks`` plus the identity keywords.
func New_Future(kw ...py.Kw) (*Future, error) {
	f := Future_class.New().(*Future)
	if err := f.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return f, nil
}

// New_callbacks_ is the ``callbacks`` default factory (``dict``).
func (f *Future) New_callbacks_() map[FutureStatus]Future_callback {
	return map[FutureStatus]Future_callback{}
}

// Future_base_ exposes the embedded Future to code holding a subclass.
func (f *Future) Future_base_() *Future { return f }

// Model_post_init applies staged identity, wires default per-status
// callbacks and self-registers with the active local policy.
//
// Performs four things:
//
//  1. Chain to LAILA_IDENTIFIABLE_OBJECT_.Model_post_init so Global_id
//     reflects any explicit ``uuid=``.
//  2. Point Default_callbacks_ at the shared "set my status to X when
//     callback X fires" table.
//  3. Resolve the owning *local* policy: the one named by Policy_id when
//     it is local, else the active local policy (Get_active_local_policy_
//     lazily activates a DefaultPolicy if needed).
//  4. Register ``self`` in every currently-open guarantee scope on this
//     thread, then drop a reference into ``policy.future_bank`` so the
//     future can be looked up by gid even after the local handle goes out
//     of scope.
//
// Logging the creation event is best-effort -- failures here never block
// construction.
func (f *Future) Model_post_init(ctx any) error {
	if err := f.LAILA_IDENTIFIABLE_FUTURE_.Model_post_init(ctx); err != nil {
		return err
	}
	f.Setup_default_callbacks_()
	policy, err := Owning_local_policy_(f.Policy_id)
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
	_ = attempt(func() error {
		lg, err := Get_logger()
		if err != nil {
			return err
		}
		lg.Record_future_created(f.Self())
		return nil
	})
	return nil
}

// Setup_default_callbacks_ attaches the shared default status-transition
// table and an empty per-instance status-callback registry.
func (f *Future) Setup_default_callbacks_() {
	f.Default_callbacks_ = DEFAULT_STATUS_CALLBACKS_
	f.Status_callbacks_ = map[FutureStatus][]Future_callback{}
}

// Status returns the current status code for this Future (synchronized).
func (f *Future) Status() FutureStatus {
	f.Lock()
	defer f.Unlock()
	return f.Status_
}

// Set_status sets the current status code for this Future (synchronized).
//
// After updating the internal state and emitting the standard logger
// transition, fires every callback registered for status via
// Add_status_callback. Callback exceptions are swallowed to avoid
// disrupting the producer (taskforce runner) thread.
func (f *Future) Set_status(status FutureStatus) {
	f.Lock()
	defer f.Unlock()
	prev := f.Status_
	f.Status_ = status
	if prev != status {
		_ = attempt(func() error {
			lg, err := Get_logger()
			if err != nil {
				return err
			}
			lg.Record_future_transition(f.Self(), status, prev)
			return nil
		})
		cbs := append([]Future_callback(nil), f.Status_callbacks_[status]...)
		for _, cb := range cbs {
			f.safe_callback_(cb)
		}
	}
}

// safe_callback_ is ``try: cb(self) except Exception: pass``.
func (f *Future) safe_callback_(cb Future_callback) {
	defer func() { _ = recover() }()
	cb(f.Self())
}

// Add_status_callback registers fn to fire when this future transitions
// into status.
//
// Multiple callbacks per status are supported (registered in insertion
// order, fired in insertion order). The callback runs on whatever thread
// set the status; it must therefore be cheap and non-blocking. Exceptions
// raised inside fn are swallowed.
//
// If the future is *already* in status at registration time, fn is
// invoked immediately and synchronously to close the obvious race between
// registration and a producer that completed first.
func (f *Future) Add_status_callback(status FutureStatus, fn Future_callback) {
	f.Lock()
	f.Status_callbacks_[status] = append(f.Status_callbacks_[status], fn)
	fire := f.Status_ == status
	f.Unlock()
	if fire {
		f.safe_callback_(fn)
	}
}

// Result is the future's result, blocking until completion if necessary.
//
//   - If the future has already finished successfully, returns the
//     recorded result without blocking.
//   - If the future ended in error or was cancelled, the recorded
//     exception is re-raised.
//   - Otherwise, the lock is released and we ``wait(timeout=None)`` for
//     completion -- releasing the lock first is critical because the
//     producer thread that ultimately sets the result will need to
//     re-acquire the same atomic lock to do so.
func (f *Future) Result() (any, error) {
	res, done, err := f.result_if_done_()
	if done {
		return res, err
	}
	if _, err := f.Self().(Future_waiter).Wait(nil); err != nil {
		return nil, err
	}
	f.Lock()
	defer f.Unlock()
	if f.Status_ == FutureStatus_ERROR || f.Status_ == FutureStatus_CANCELLED {
		return nil, f.raise_exception_()
	}
	return f.Materialize_result_()
}

// result_if_done_ is the first ``with self.atomic():`` block of Result.
func (f *Future) result_if_done_() (any, bool, error) {
	f.Lock()
	defer f.Unlock()
	if f.Status_ == FutureStatus_ERROR || f.Status_ == FutureStatus_CANCELLED {
		return nil, true, f.raise_exception_()
	}
	if f.Status_ == FutureStatus_FINISHED {
		res, err := f.Materialize_result_()
		return res, true, err
	}
	return nil, false, nil
}

// raise_exception_ is ``raise self._exception`` (``raise None`` is a
// TypeError in Python).
func (f *Future) raise_exception_() error {
	if f.Exception_ == nil {
		return py.TypeError.New("exceptions must derive from BaseException")
	}
	return f.Exception_
}

// Set_result records the future's result; non-Entry values are wrapped
// lazily (synchronized).
//
//   - nil clears both the value and the result-id slot.
//   - A live Entry is stored as-is and its ``global_id`` is captured into
//     Result_global_id_ immediately.
//   - Anything else is stored raw and wrapped via Entry_constant on first
//     read (``result`` / ``data`` / Result_global_id), so the producer's
//     hot path does not pay for an entry nobody may ever look at. The
//     outcome is still always addressable across processes once read.
func (f *Future) Set_result(result any) {
	f.Lock()
	defer f.Unlock()
	if py.IsNone(result) {
		f.Return_value_ = nil
		f.Result_global_id_ = nil
		f.Result_pending_wrap_ = false
	} else if py.Isinstance(result, Entry_class) {
		f.Return_value_ = result
		gid := result.(interface{ Global_id() string }).Global_id()
		f.Result_global_id_ = &gid
		f.Result_pending_wrap_ = false
	} else {
		f.Return_value_ = result
		f.Result_global_id_ = nil
		f.Result_pending_wrap_ = true
	}
}

// Materialize_result_ returns the result as an Entry, wrapping a raw value
// on first use.
//
// Must be called with the atomic lock held (it is re-entrant, so callers
// already inside ``with self.atomic()`` are fine).
func (f *Future) Materialize_result_() (any, error) {
	f.Lock()
	defer f.Unlock()
	if f.Result_pending_wrap_ {
		wrapped, err := Entry_constant(f.Return_value_)
		if err != nil {
			return nil, err
		}
		f.Return_value_ = wrapped
		gid := wrapped.Global_id()
		f.Result_global_id_ = &gid
		f.Result_pending_wrap_ = false
	}
	return f.Return_value_, nil
}

// Result_global_id is the ``global_id`` of the result entry, or nil if
// there is no result yet.
//
// Forces the lazy Entry_constant wrap for raw results so the id is stable
// from the first time anyone asks for it. Never blocks.
func (f *Future) Result_global_id() (*string, error) {
	f.Lock()
	defer f.Unlock()
	if f.Result_pending_wrap_ {
		if _, err := f.Materialize_result_(); err != nil {
			return nil, err
		}
	}
	return f.Result_global_id_, nil
}

// Data returns ``self.result.data`` -- the unwrapped payload value.
//
// Convenience accessor for the common case of "I just want the value the
// task produced". Blocks until the future finishes (via the underlying
// Result getter) and then unwraps the entry.
//
// Raises RuntimeError if the result is not an Entry instance (which can
// happen for futures that explicitly set nil results, or for raw
// non-Entry results from legacy code paths).
func (f *Future) Data() (any, error) {
	result, err := f.Self().(interface{ Result() (any, error) }).Result()
	if err != nil {
		return nil, err
	}
	if !py.Isinstance(result, Entry_class) {
		return nil, py.RuntimeError.Newf(
			"Future result is not an Entry (got %s); cannot access .data", py.Pytypename(result))
	}
	return result.(interface{ Data() (any, error) }).Data()
}

// Exception returns the current exception value.
func (f *Future) Exception() error { return f.Exception_ }

// Set_exception sets the exception value (synchronized).
func (f *Future) Set_exception(exception error) {
	f.Lock()
	defer f.Unlock()
	f.Exception_ = exception
}

// Add_callback registers a callback for a specific status transition.
func (f *Future) Add_callback(status FutureStatus, fn Future_callback) {
	f.Lock()
	f.Callbacks[status] = fn
	f.Unlock()
	f.Add_status_callback(status, fn)
}

// Remove_callback removes a callback for a specific status.
func (f *Future) Remove_callback(status FutureStatus, fn Future_callback) {
	f.Lock()
	defer f.Unlock()
	if bucket, ok := f.Status_callbacks_[status]; ok {
		kept := bucket[:0]
		for _, cb := range bucket {
			if !py.Is(cb, fn) {
				kept = append(kept, cb)
			}
		}
		if len(kept) == 0 {
			delete(f.Status_callbacks_, status)
		} else {
			f.Status_callbacks_[status] = kept
		}
	}
	if cur, ok := f.Callbacks[status]; ok && py.Is(cur, fn) {
		delete(f.Callbacks, status)
	}
}

// Clear_callbacks clears the callback for a specific status.
func (f *Future) Clear_callbacks(status FutureStatus) {
	f.Lock()
	defer f.Unlock()
	delete(f.Status_callbacks_, status)
	delete(f.Callbacks, status)
}

// Clear_all_callbacks clears all registered callbacks.
func (f *Future) Clear_all_callbacks() {
	f.Lock()
	defer f.Unlock()
	for k := range f.Status_callbacks_ {
		delete(f.Status_callbacks_, k)
	}
	for k := range f.Callbacks {
		delete(f.Callbacks, k)
	}
}

// Trigger_callback fires every callback registered for status with this
// future.
//
// Callbacks receive the future itself (never the blocking ``result``),
// mirroring what the status setter does.
//
// TODO: This needs to go through the central command.
func (f *Future) Trigger_callback(status FutureStatus) {
	f.Lock()
	cbs := append([]Future_callback(nil), f.Status_callbacks_[status]...)
	f.Unlock()
	for _, fn := range cbs {
		f.safe_callback_(fn)
	}
}

// Future_identity returns a lightweight identity handle for this future.
func (f *Future) Future_identity() (*LAILA_IDENTIFIABLE_FUTURE_, error) {
	return New_LAILA_IDENTIFIABLE_FUTURE_(py.Kw{
		"taskforce_id":    f.Taskforce_id,
		"policy_id":       f.Policy_id,
		"future_group_id": f.Future_group_id,
		"precedence":      f.Precedence,
		"purpose":         f.Purpose,
		"uuid":            f.Uuid_,
	})
}

// Release removes this future from its owning policy's ``future_bank``.
//
// The bank holds a strong reference to every future (and therefore to its
// result payload) until this is called; nothing releases a future
// automatically. Call it once the result has been consumed and drop your
// own handle to let the object be collected.
//
// Idempotent and legal in any state: a still-running task keeps its own
// reference to the future, so releasing early only removes the gid-based
// lookup (``laila.runtime.status(gid)``, RemoteFuture RPCs, identity
// handles) -- completion is unaffected. Open guarantee scopes are left
// untouched; they are transient and drained on exit.
func (f *Future) Release() {
	gid := f.Global_id()
	self := f.Self()
	pid := Gid_of_(f.Policy_id)
	var policy any
	if s, ok := pid.(string); ok {
		policy, _ = Local_policies_.Get(s)
		if py.IsNone(policy) {
			policy, _ = Live_policies_.Get(s)
		}
	}
	if !py.IsNone(policy) {
		if bank := future_bank_of_(policy); bank != nil && py.Is(bank.GetDefault(gid, nil), self) {
			bank.PopDefault(gid, nil)
			return
		}
	}
	for _, policy := range Future_bank_policies_() {
		bank := future_bank_of_(policy)
		if bank != nil && py.Is(bank.GetDefault(gid, nil), self) {
			bank.PopDefault(gid, nil)
			return
		}
	}
}

// Wait blocks until the future completes. Subclasses must override.
//
// Concrete subclasses must implement this and additionally guard against
// being invoked from an async loop thread (call Check_not_loop_thread_
// first) -- waiting synchronously on a future from within the same loop
// that is supposed to complete it is a hang.
//
// Raises NotImplementedError always, on the abstract base.
func (f *Future) Wait(timeout ...any) (any, error) {
	return nil, py.NotImplementedError.Newf(
		"%s does not implement wait(); use a concrete subclass such as ConcurrentPackageFuture.",
		py.Pytypename(f.Self()))
}

// Await is ``__await__`` on the abstract base: not awaitable.
func (f *Future) Await() (any, error) {
	return nil, py.TypeError.Newf("object %s can't be used in 'await' expression", py.Pytypename(f.Self()))
}

// Finished returns true if the Future has finished successfully.
func (f *Future) Finished() bool { return f.Self().(Future_like).Status() == FutureStatus_FINISHED }

// Cancelled returns true if the Future was cancelled.
func (f *Future) Cancelled() bool { return f.Self().(Future_like).Status() == FutureStatus_CANCELLED }

// Error returns true if the Future finished with an error.
func (f *Future) Error() bool { return f.Self().(Future_like).Status() == FutureStatus_ERROR }

// Not_started returns true if the Future has not started.
func (f *Future) Not_started() bool { return f.Self().(Future_like).Status() == FutureStatus_NOT_STARTED }

// Running returns true if the Future is currently running.
func (f *Future) Running() bool { return f.Self().(Future_like).Status() == FutureStatus_RUNNING }

func init() {
	py.Register_property("result_global_id", "future_identity")
}
