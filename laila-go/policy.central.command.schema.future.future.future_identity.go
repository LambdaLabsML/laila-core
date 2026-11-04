package laila

import (
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/command/schema/future/future/future_identity.py --
// identity model for a Laila future: lightweight metadata without result
// state.
//
// A LAILA_IDENTIFIABLE_FUTURE_ carries only the *identity* of a future:
// the gid of the task-force and policy that own it, optional group /
// precedence / purpose annotations, and the inherited UUID + scopes. It
// does NOT carry the result, exception, or status -- those live in the
// concrete Future registered in the owning policy's ``future_bank``. The
// identity object instead resolves all status / result / exception
// accessors *through* the bank, so any process that holds an identity can
// introspect the live future as long as the owning policy is reachable via
// Future_bank_policies_.
//
// The split exists so that a future can be referenced by metadata alone
// (e.g. across a process boundary) and so that RemoteFuture can present
// the same identity-like API on the remote side. Local submission paths
// return the concrete Future itself -- it *is* an identity (subclass), so
// no separate handle is built per task.

// Future_like is the method surface every concrete future in a
// ``future_bank`` exposes (Future, ConcurrentPackageFuture, GroupFuture,
// ComplexFuture, RemoteFuture). The identity handle resolves through it.
type Future_like interface {
	Global_id() string
	Status() FutureStatus
	Result() (any, error)
	Data() (any, error)
	Exception() error
	// Wait is ``wait(timeout=None)``: Wait(), Wait(0.5), Wait(nil) or
	// Wait(py.Kw{"timeout": 0.5}).
	Wait(timeout ...any) (any, error)
	Release()
	// Await is ``__await__``: a non-blocking completion signal for use
	// inside a py coroutine.
	Await() (any, error)
}

// Future_bank_holder is the duck type of a local policy as seen by the
// future machinery: it has a ``future_bank`` (gid -> concrete future).
type Future_bank_holder interface {
	Get_future_bank_() *py.Dict
}

// future_bank_of_ is ``policy.future_bank`` for any policy-shaped value
// (typed holder or dynamic attribute); nil when absent.
func future_bank_of_(pol any) *py.Dict {
	if h, ok := pol.(Future_bank_holder); ok {
		return h.Get_future_bank_()
	}
	if d, ok := getattr(pol, "future_bank").(*py.Dict); ok {
		return d
	}
	return nil
}

// Timeout_arg_ decodes the port's ``wait(timeout=None)`` argument shapes:
// no argument / nil -> (0, false); a number -> (seconds, true); a Kw with
// ``timeout`` -> that value.
func Timeout_arg_(args []any) (float64, bool) {
	if len(args) == 0 {
		return 0, false
	}
	v := args[0]
	if kw, ok := v.(py.Kw); ok {
		v = kw.Get("timeout", nil)
	}
	return Optional_float_(v)
}

// Gid_of_ is ``getattr(x, "global_id", x)``.
func Gid_of_(x any) any {
	if g, ok := x.(interface{ Global_id() string }); ok {
		return g.Global_id()
	}
	return x
}

// LAILA_IDENTIFIABLE_FUTURE_ is the lightweight identity record for a
// Future.
//
// Carries only the metadata needed to *find* the live future in some
// policy's ``future_bank``; the result, exception, and status are
// properties that resolve through the bank on demand.
//
// Fields:
//   - Taskforce_id: gid of the task-force that owns this Future
//     (LAILA_IDENTIFIABLE_OBJECT_ or str).
//   - Policy_id: gid of the policy instance that created this Future.
//   - Future_group_id: gid of the parent GroupFuture, when this future is
//     a member of one.
//   - Precedence: gid of a future this one depends on. Reserved for future
//     explicit-dependency scheduling; currently informational only.
//   - Purpose: human-readable label that surfaces in logs and diagnostics
//     (e.g. ``"memorize:42"``, ``"build:my_model"``).
type LAILA_IDENTIFIABLE_FUTURE_ struct {
	LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_
	Taskforce_id    any     `py:"taskforce_id,required"`
	Policy_id       any     `py:"policy_id,required"`
	Future_group_id any     `py:"future_group_id"`
	Precedence      any     `py:"precedence"`
	Purpose         *string `py:"purpose"`
}

// LAILA_IDENTIFIABLE_FUTURE_class is the class descriptor
// (``_DEFAULT_SCOPES = [_FUTURE_SCOPE]``).
var LAILA_IDENTIFIABLE_FUTURE_class = py.NewClass(
	"_LAILA_IDENTIFIABLE_FUTURE", (*LAILA_IDENTIFIABLE_FUTURE_)(nil),
	LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_class,
).Set_classvar("_DEFAULT_SCOPES", []string{FUTURE_SCOPE_})

// New_LAILA_IDENTIFIABLE_FUTURE_ is ``_LAILA_IDENTIFIABLE_FUTURE(**data)``.
func New_LAILA_IDENTIFIABLE_FUTURE_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_FUTURE_, error) {
	o := LAILA_IDENTIFIABLE_FUTURE_class.New().(*LAILA_IDENTIFIABLE_FUTURE_)
	if err := o.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return o, nil
}

// String returns a human-readable, line-by-line representation.
func (f *LAILA_IDENTIFIABLE_FUTURE_) String() string {
	lines := []string{
		"future_id: " + f.Global_id(),
		"taskforce_id: " + py.Str(f.Taskforce_id),
		"policy_id: " + py.Str(f.Policy_id),
		"future_group_id: " + py.Str(f.Future_group_id),
		"precedence: " + py.Str(f.Precedence),
		"purpose: " + py.Str(py.Deref_any(f.Purpose)),
	}
	return strings.Join(lines, "\n")
}

// Repr returns the same representation as String for readability.
func (f *LAILA_IDENTIFIABLE_FUTURE_) Repr() string { return f.String() }

// bank_future_ finds the concrete future for this gid in any local bank.
func (f *LAILA_IDENTIFIABLE_FUTURE_) bank_future_() (Future_like, error) {
	gid := f.Global_id()
	for _, policy := range Future_bank_policies_() {
		bank := future_bank_of_(policy)
		if bank == nil {
			continue
		}
		if v, ok := bank.Get(gid); ok {
			return v.(Future_like), nil
		}
	}
	return nil, py.KeyError.Newf("Future %s not found in any local policy bank", gid)
}

// Status is the read-only status resolved from the owning policy's future
// bank (KeyError when the future is not in any local bank).
func (f *LAILA_IDENTIFIABLE_FUTURE_) Status() (FutureStatus, error) {
	fut, err := f.bank_future_()
	if err != nil {
		return "", err
	}
	return fut.Status(), nil
}

// Result is the read-only result resolved from the owning policy's future
// bank.
func (f *LAILA_IDENTIFIABLE_FUTURE_) Result() (any, error) {
	fut, err := f.bank_future_()
	if err != nil {
		return nil, err
	}
	return fut.Result()
}

// Data is the read-only unwrapped Entry payload, resolved through the
// owning policy's future bank.
func (f *LAILA_IDENTIFIABLE_FUTURE_) Data() (any, error) {
	pid := Gid_of_(f.Policy_id)
	var policy any
	if s, ok := pid.(string); ok {
		policy, _ = Local_policies_.Get(s)
		if py.IsNone(policy) {
			policy, _ = Live_policies_.Get(s)
		}
	}
	if py.IsNone(policy) {
		return nil, py.KeyError.Newf("Owning policy %s for future %s not found locally", py.Str(pid), f.Global_id())
	}
	bank := future_bank_of_(policy)
	var fut any
	found := false
	if bank != nil {
		fut, found = bank.Get(f.Global_id())
	}
	if !found {
		return nil, py.KeyError.Newf("Future %s not found in policy %s's future bank", f.Global_id(), py.Str(pid))
	}
	return fut.(Future_like).Data()
}

// Exception is the read-only exception resolved from the owning policy's
// future bank.
func (f *LAILA_IDENTIFIABLE_FUTURE_) Exception() (error, error) {
	fut, err := f.bank_future_()
	if err != nil {
		return nil, err
	}
	return fut.Exception(), nil
}

// Wait blocks until the future completes, resolved through the future
// bank.
//
// Raises LoopBlockingWaitError if called from a thread that owns an async
// event loop. Use ``await fut`` from inside coroutines instead.
func (f *LAILA_IDENTIFIABLE_FUTURE_) Wait(timeout ...any) (any, error) {
	if err := Check_not_loop_thread_(); err != nil {
		return nil, err
	}
	fut, err := f.bank_future_()
	if err != nil {
		return nil, err
	}
	return fut.Wait(timeout...)
}

// Release removes the underlying concrete future from its policy's future
// bank.
//
// Delegates to the concrete future's Future.Release. A no-op when the
// future is no longer registered (already released or never created
// locally). Futures are *never* released automatically -- whoever owns the
// handle must call this once the result has been consumed, otherwise the
// future (and its result payload) stays resident for the life of the
// process.
func (f *LAILA_IDENTIFIABLE_FUTURE_) Release() {
	gid := f.Global_id()
	for _, policy := range Future_bank_policies_() {
		bank := future_bank_of_(policy)
		if bank == nil {
			continue
		}
		fut, ok := bank.Get(gid)
		if ok && !py.IsNone(fut) {
			if py.Is(fut, f.Self()) {
				bank.PopDefault(gid, nil)
			} else {
				fut.(Future_like).Release()
			}
			return
		}
	}
}

// Await awaits the underlying concrete future via the policy's future
// bank.
//
// Delegates to the concrete future's ``__await__`` (each concrete class --
// ConcurrentPackageFuture, ComplexFuture, GroupFuture, RemoteFuture --
// implements its own non-blocking completion signal).
func (f *LAILA_IDENTIFIABLE_FUTURE_) Await() (any, error) {
	fut, err := f.bank_future_()
	if err != nil {
		return nil, err
	}
	return fut.Await()
}

// As_dict returns a dict with all identity fields.
func (f *LAILA_IDENTIFIABLE_FUTURE_) As_dict() *py.Dict {
	return py.DictOf(
		"global_id", f.Global_id(),
		"taskforce_id", Gid_of_(f.Taskforce_id),
		"policy_id", Gid_of_(f.Policy_id),
		"future_group_id", Gid_of_(f.Future_group_id),
		"precedence", Gid_of_(f.Precedence),
		"purpose", py.Deref_any(f.Purpose),
	)
}

// Json is ``__json__``: a JSON string for the identity fields.
func (f *LAILA_IDENTIFIABLE_FUTURE_) Json() (string, error) {
	return py.Json_dumps(f.As_dict())
}

func init() {
	py.Register_property("status", "result", "data", "exception", "future_bank")
}
