package py

import (
	"reflect"
	"unsafe"
)

// eface mirrors the runtime layout of an empty interface.
type eface struct {
	typ  unsafe.Pointer
	data unsafe.Pointer
}

// Id is ``id(v)``: an identity token for pointer-shaped values -- pointers,
// maps, channels, slices (their backing array) and *func values*. For a
// func the token is the closure object itself, so two distinct closures
// made from the same literal are different (as in Python) while the same
// top-level function is always identical to itself. Non-reference values
// (ints, strings, structs) yield 0: they have no identity to compare.
func Id(v any) uintptr {
	if v == nil {
		return 0
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Chan, reflect.UnsafePointer:
		return rv.Pointer()
	case reflect.Slice:
		if rv.Len() == 0 {
			return 0
		}
		return rv.Pointer()
	case reflect.Func:
		// The interface data word points at the funcval (closure object).
		return uintptr((*eface)(unsafe.Pointer(&v)).data)
	}
	return 0
}

// Is is ``a is b``: identical reference, or equal non-reference values of
// the same type (Python interns small ints / short strings, which is the
// behaviour callers relying on ``is`` for scalars observe).
func Is(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	ia, ib := Id(a), Id(b)
	if ia != 0 || ib != 0 {
		return ia == ib && reflect.TypeOf(a) == reflect.TypeOf(b)
	}
	ta, tb := reflect.TypeOf(a), reflect.TypeOf(b)
	if ta != tb || !ta.Comparable() {
		return false
	}
	return a == b
}

// Index_of_identity is ``seq.index(x)`` by identity; -1 when absent.
func Index_of_identity(seq []any, x any) int {
	for i, v := range seq {
		if Is(v, x) {
			return i
		}
	}
	return -1
}
