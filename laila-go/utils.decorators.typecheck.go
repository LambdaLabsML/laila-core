package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// utils/decorators/typecheck.py -- decorators for lightweight runtime type
// coercion.
//
// Ensure_list removes the boilerplate at the top of every "accept either a
// single item or an iterable" function: the decorator binds the call to
// the function's signature, finds the named parameter and -- if its bound
// value is not a list / set / frozenset -- wraps it in a single-element
// list before forwarding the call.
//
// Port note: Go has no ``inspect.signature``, so the decorated function
// declares its Python signature explicitly:
//
//	f := Ensure_list("items")(py.Sig("f", py.P("items")), func(args ...any) (any, error) { ... })

// Ensure_list wraps a single value in a list when *arg_name* is not a
// list / set / frozenset. Returns a decorator taking the function's
// signature and body; TypeError at call time if *arg_name* is not bound.
func Ensure_list(arg_name string) func(sig *py.Signature, fn func(args ...any) (any, error)) func(args ...any) (any, error) {
	return func(sig *py.Signature, fn func(args ...any) (any, error)) func(args ...any) (any, error) {
		return func(args ...any) (any, error) {
			pos, kw := py.SplitKw(args)
			bound, err := sig.Bind_partial(pos, kw)
			if err != nil {
				return nil, err
			}
			if !bound.Has(arg_name) {
				return nil, py.TypeError.Newf("Argument '%s' not found in %s", arg_name, sig.Name)
			}
			value := bound.Get(arg_name)
			if !Is_list_like_(value) {
				bound.Args[arg_name] = py.NewList(value)
			}
			cargs, ckw := bound.Call_args(sig)
			if len(ckw) > 0 {
				cargs = append(cargs, ckw)
			}
			return fn(cargs...)
		}
	}
}

// Is_list_like_ is ``isinstance(value, (list, set, frozenset))``.
func Is_list_like_(value any) bool {
	switch value.(type) {
	case *py.List, []any, *py.Set, py.FrozenSet, []string:
		return true
	}
	return false
}
