package laila

import (
	"fmt"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// runtime/__init__.py -- runtime introspection for futures across the
// policy universe.
//
// This module is the *policy-agnostic* facade for asking questions about
// futures: status, result, exception, or simply blocking until
// completion. Unlike most of laila, it deliberately does *not* consult
// Get_active_policy -- callers may pass a future from any local policy
// and the helpers here will route the request to the right
// ``future_bank`` automatically.
//
// # Acceptable references
//
// Every public helper accepts any of:
//
//   - Future / GroupFuture / RemoteFuture -- the concrete future object,
//     used as-is.
//   - LAILA_IDENTIFIABLE_FUTURE_ -- a future *identity* whose
//     ``global_id`` is resolved against the local policy banks.
//   - string -- a raw ``global_id``, also resolved against the local
//     policy banks.
//
// When resolution is needed, the helpers walk every live local policy
// (Future_bank_policies_) and return the first hit. Missing ids raise
// KeyError.
//
// The flat Go package hosts both ``laila.runtime.<fn>`` and the
// top-level ``laila.<fn>`` shims that forward to them (``laila.status``,
// ``laila.wait``); they share one definition here.

// is_concrete_future_ is the ``isinstance(x, RemoteFuture | Future |
// GroupFuture)`` check (RemoteFuture is registered by the communication
// module and matched by class name).
func is_concrete_future_(future_ref any) bool {
	if rf, ok := py.Class_by_name("RemoteFuture"); ok && py.Isinstance(future_ref, rf) {
		return true
	}
	return py.Isinstance(future_ref, Future_class) || py.Isinstance(future_ref, GroupFuture_class)
}

// Runtime_resolve_future_ is ``laila.runtime._resolve_future``: resolves
// future_ref to a concrete future object.
//
// Accepts Future, GroupFuture, RemoteFuture, LAILA_IDENTIFIABLE_FUTURE_,
// or a raw ``global_id`` string. Concrete futures are returned
// unchanged; identities and strings are looked up in every local
// policy's ``future_bank`` (first hit wins).
//
// Raises KeyError if future_ref is an identity or string that does not
// match any future in any local policy bank, TypeError if future_ref is
// none of the supported reference types.
func Runtime_resolve_future_(future_ref any) (any, error) {
	if is_concrete_future_(future_ref) {
		return future_ref, nil
	}

	if s, ok := future_ref.(string); ok {
		for _, policy := range Future_bank_policies_() {
			if bank := future_bank_of_(policy); bank != nil && bank.Contains(s) {
				return bank.Getitem(s)
			}
		}
		return nil, py.KeyError.New(fmt.Sprintf("Future %s not found in any local policy bank", s))
	}

	if py.Isinstance(future_ref, LAILA_IDENTIFIABLE_FUTURE_class) {
		gid := global_id_of_(future_ref)
		for _, policy := range Future_bank_policies_() {
			if bank := future_bank_of_(policy); bank != nil && bank.Contains(gid) {
				return bank.Getitem(gid)
			}
		}
		return nil, py.KeyError.New(fmt.Sprintf("Future %s not found in any local policy bank", gid))
	}

	return nil, py.TypeError.Newf("Cannot resolve future for %s", py.Pytypename(future_ref))
}

// Resolve_future_ is the top-level ``laila._resolve_future``: look up the
// actual future object from a heterogeneous reference.
//
// Same accepted shapes as Runtime_resolve_future_, but identities and
// strings are searched in the *activated* local policies first
// (Local_policies_) and then in the active policy's bank, whose KeyError
// propagates when the gid is unknown.
func Resolve_future_(future_ref any) (any, error) {
	if is_concrete_future_(future_ref) {
		return future_ref, nil
	}

	lookup := func(gid string) (any, error) {
		for _, policy := range Local_policies_.Values() {
			if bank := future_bank_of_(policy); bank != nil && bank.Contains(gid) {
				return bank.Getitem(gid)
			}
		}
		active, err := Get_active_policy()
		if err != nil {
			return nil, err
		}
		bank := future_bank_of_(active)
		if bank == nil {
			return nil, py.KeyError.New(gid)
		}
		return bank.Getitem(gid)
	}

	if s, ok := future_ref.(string); ok {
		return lookup(s)
	}
	if py.Isinstance(future_ref, LAILA_IDENTIFIABLE_FUTURE_class) {
		return lookup(global_id_of_(future_ref))
	}
	return nil, py.TypeError.Newf("Cannot resolve future for %s", py.Pytypename(future_ref))
}

// Status returns the current status of future_ref without blocking
// (``laila.runtime.status`` / ``laila.status``).
//
// For Future and RemoteFuture this is a single FutureStatus value; for
// GroupFuture it is the aggregate status payload (counts, percentages,
// top-level progress) defined by GroupFuture.Status.
func Status(future_ref any) (any, error) {
	fut, err := Runtime_resolve_future_(future_ref)
	if err != nil {
		return nil, err
	}
	return py.Getattr(fut, "status")
}

// Result returns the result of future_ref, blocking until it is
// available (``laila.runtime.result``).
//
// The blocking semantics are inherited from the underlying future: a
// LoopBlockingWaitError may be raised if called from the same thread
// that owns an async taskforce loop. Use Wait explicitly if you need to
// bound the wait.
func Result(future_ref any) (any, error) {
	fut, err := Runtime_resolve_future_(future_ref)
	if err != nil {
		return nil, err
	}
	return py.Getattr(fut, "result")
}

// Exception returns the exception captured by future_ref, or nil
// (``laila.runtime.exception``).
//
// Returns nil for futures that completed successfully or are still in
// flight; only futures whose terminal status is ``ERROR`` or
// ``CANCELLED`` carry an exception value.
func Exception(future_ref any) (error, error) {
	fut, err := Runtime_resolve_future_(future_ref)
	if err != nil {
		return nil, err
	}
	v, err := py.Getattr(fut, "exception")
	if err != nil {
		return nil, err
	}
	exc, _ := v.(error)
	return exc, nil
}

// Wait blocks until future_ref terminates and returns its result
// (``laila.runtime.wait(future_ref, timeout=None)`` / ``laila.wait``).
//
// The wait honours the same timeout semantics as Future.Wait:
// ``timeout=None`` waits indefinitely; a positive timeout raises
// TimeoutError on expiry. As with Result, calling from inside an async
// taskforce loop thread raises LoopBlockingWaitError instead of
// dead-locking.
func Wait(future_ref any, timeout ...any) (any, error) {
	fut, err := Runtime_resolve_future_(future_ref)
	if err != nil {
		return nil, err
	}
	return callm(fut, "wait", timeout_kw_(timeout))
}
