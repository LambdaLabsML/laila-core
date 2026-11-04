package laila

import (
	"reflect"
	"sort"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// utils/decorators/synchronized.py -- decorator that acquires atomic locks
// on identifiable arguments before calling a method.
//
// Synchronized inspects ``self`` *and* every positional and keyword
// argument; for each that is a _LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT
// it enters the corresponding ``atomic()`` block before calling the
// wrapped method, releasing the locks again on exit (or on error).
//
// Lock-acquisition order is the *id-sorted* order of the involved objects
// (pointer addresses here), so two callers who want overlapping subsets
// of the same locks always acquire them in the same order -- which keeps
// @synchronized deadlock-free.
//
// Port note: Python decorates ``def m(self, *args, **kwargs)``; the Go
// shape is a function taking (self, args..., Kw) in the port's variadic
// convention.

// Synchronized wraps *method* so all identifiable arguments are locked
// before invocation (``synchronized(method, *, scope="local")``).
//
// scope "global" raises NotImplementedError at call time.
func Synchronized(method func(self any, args ...any) (any, error), kw ...py.Kw) func(self any, args ...any) (any, error) {
	opts := py.MergeKw(kw...)
	scope := py.Str(opts.Get("scope", "local"))
	return func(self any, args ...any) (any, error) {
		if scope != "local" {
			return nil, py.NotImplementedError.New("Global synchronization is not implemented.")
		}
		lock_cls := LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_class
		pos, kwargs := py.SplitKw(args)
		candidates := append([]any{self}, pos...)
		for _, k := range py.Sorted_strings(keysOf(kwargs)) {
			candidates = append(candidates, kwargs[k])
		}
		var lock_targets []any
		for _, obj := range candidates {
			if py.Isinstance(obj, lock_cls) {
				lock_targets = append(lock_targets, obj)
			}
		}
		if len(lock_targets) == 0 {
			return method(self, args...)
		}
		sort.SliceStable(lock_targets, func(i, j int) bool {
			return reflect.ValueOf(lock_targets[i]).Pointer() < reflect.ValueOf(lock_targets[j]).Pointer()
		})
		var releases []func()
		defer func() {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
		}()
		for _, obj := range lock_targets {
			_, release, err := Synchronized_atomic_(obj.(Atomicer), py.Kw{"scope": scope})
			if err != nil {
				return nil, err
			}
			releases = append(releases, release)
		}
		return method(self, args...)
	}
}

// Atomicer is the ``atomic(**kw)`` context-manager surface shared by the
// locally and globally atomic classes.
type Atomicer interface {
	Atomic(kw ...py.Kw) (any, func(), error)
}

// Synchronized_atomic_ is how the wrapper enters each target's atomic
// block. It is a variable so tests can spy on it, the way the Python
// suite monkeypatches ``_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT.atomic``.
var Synchronized_atomic_ = func(obj Atomicer, kw py.Kw) (any, func(), error) {
	return obj.Atomic(kw)
}
