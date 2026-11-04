// Port of tests/functional/utils/decorators/unit_tests/test_decorators.py.
package unit_tests

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// identity is ``def f(items): return items`` under @ensure_list(name).
func identity(name string) func(args ...any) (any, error) {
	return laila.Ensure_list(name)(py.Sig("f", py.P(name)), func(args ...any) (any, error) {
		return args[0], nil
	})
}

func call(t *testing.T, f func(args ...any) (any, error), args ...any) any {
	t.Helper()
	v, err := f(args...)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	return v
}

func TestEnsureList(t *testing.T) {
	t.Run("single_string_becomes_list", func(t *testing.T) {
		if !py.Eq(call(t, identity("items"), "a"), py.NewList("a")) {
			t.Error("wrap")
		}
	})
	t.Run("list_unchanged", func(t *testing.T) {
		original := py.NewList("x", "y")
		if call(t, identity("items"), original) != original {
			t.Error("identity lost")
		}
	})
	t.Run("set_unchanged", func(t *testing.T) {
		original := py.NewSet(1, 2)
		if call(t, identity("items"), original) != original {
			t.Error("identity lost")
		}
	})
	t.Run("frozenset_unchanged", func(t *testing.T) {
		original := py.FrozenSet{Set: py.NewSet(3, 4)}
		if got := call(t, identity("items"), original); got.(py.FrozenSet).Set != original.Set {
			t.Error("identity lost")
		}
	})
	t.Run("missing_named_arg_raises_type_error", func(t *testing.T) {
		f := laila.Ensure_list("items")(py.Sig("f"), func(args ...any) (any, error) { return nil, nil })
		_, err := f()
		if !errors.Is(err, py.TypeError) || !strings.Contains(err.Error(), "items") {
			t.Errorf("want TypeError mentioning items, got %v", err)
		}
	})
	t.Run("keyword_argument", func(t *testing.T) {
		if !py.Eq(call(t, identity("items"), py.Kw{"items": "k"}), py.NewList("k")) {
			t.Error("kwarg")
		}
	})
	// preserves_wrapped_function_name: ``functools.wraps`` has no Go
	// equivalent -- the signature keeps its own name.
	t.Run("integer_none_tuple_dict_boolean_become_list", func(t *testing.T) {
		f := identity("x")
		if !py.Eq(call(t, f, 42), py.NewList(42)) {
			t.Error("int")
		}
		if !py.Eq(call(t, f, nil), py.NewList(nil)) {
			t.Error("None")
		}
		if !py.Eq(call(t, f, py.Tuple{1, 2}), py.NewList(py.Tuple{1, 2})) {
			t.Error("tuple")
		}
		d := py.DictOf("a", 1)
		if !py.Eq(call(t, f, d), py.NewList(d)) {
			t.Error("dict")
		}
		if !py.Eq(call(t, f, true), py.NewList(true)) {
			t.Error("bool")
		}
	})
	t.Run("empty_list_and_set_unchanged", func(t *testing.T) {
		f := identity("x")
		if !py.Eq(call(t, f, py.NewList()), py.NewList()) {
			t.Error("empty list")
		}
		if !py.Eq(call(t, f, py.NewSet()), py.NewSet()) {
			t.Error("empty set")
		}
	})
	t.Run("with_default_value", func(t *testing.T) {
		f := laila.Ensure_list("x")(py.Sig("f", py.PD("x", "default")), func(args ...any) (any, error) { return args[0], nil })
		if !py.Eq(call(t, f), py.NewList("default")) {
			t.Error("default")
		}
	})
	t.Run("multiple_args_only_named_wrapped", func(t *testing.T) {
		f := laila.Ensure_list("b")(py.Sig("f", py.P("a"), py.P("b"), py.PD("c", 10)), func(args ...any) (any, error) {
			return py.Tuple(args), nil
		})
		got := call(t, f, 1, "hello", py.Kw{"c": 20}).(py.Tuple)
		if len(got) != 3 || got[0] != 1 || !py.Eq(got[1], py.NewList("hello")) || got[2] != 20 {
			t.Errorf("got %s", py.Repr(got))
		}
	})
	t.Run("wrong_arg_name_raises", func(t *testing.T) {
		f := laila.Ensure_list("nonexistent")(py.Sig("f", py.P("x")), func(args ...any) (any, error) { return args[0], nil })
		if _, err := f(42); !errors.Is(err, py.TypeError) {
			t.Errorf("want TypeError, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// synchronized
// ---------------------------------------------------------------------------

func newLockable(t *testing.T) *laila.LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_ {
	t.Helper()
	o, err := laila.New_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_()
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// spyAtomic monkeypatches Synchronized_atomic_ (the port's equivalent of
// ``_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT.atomic``) and records the
// objects it is entered on. Restored on cleanup.
func spyAtomic(t *testing.T) *[]any {
	t.Helper()
	entered := &[]any{}
	orig := laila.Synchronized_atomic_
	laila.Synchronized_atomic_ = func(obj laila.Atomicer, kw py.Kw) (any, func(), error) {
		*entered = append(*entered, obj)
		return orig(obj, kw)
	}
	t.Cleanup(func() { laila.Synchronized_atomic_ = orig })
	return entered
}

func id(v any) uintptr { return reflect.ValueOf(v).Pointer() }

func TestSynchronized(t *testing.T) {
	t.Run("non_lockable_runs_without_lock", func(t *testing.T) {
		work := laila.Synchronized(func(self any, args ...any) (any, error) { return args[0].(int) + 1, nil })
		if v, err := work(struct{}{}, 2); err != nil || v != 3 {
			t.Errorf("got %v %v", v, err)
		}
	})
	t.Run("lockable_runs_under_atomic", func(t *testing.T) {
		entered := spyAtomic(t)
		work := laila.Synchronized(func(self any, args ...any) (any, error) { return len(*entered), nil })
		lockable := newLockable(t)
		if v, _ := work(struct{}{}, lockable); v != 1 {
			t.Errorf("entered = %d", v)
		}
		if (*entered)[0] != lockable {
			t.Error("wrong target")
		}
	})
	t.Run("multiple_lockable_args_sorted_by_id", func(t *testing.T) {
		entered := spyAtomic(t)
		work := laila.Synchronized(func(self any, args ...any) (any, error) { return true, nil })
		a, b := newLockable(t), newLockable(t)
		if _, err := work(struct{}{}, a, b); err != nil {
			t.Fatal(err)
		}
		var order []uintptr
		for _, o := range *entered {
			order = append(order, id(o))
		}
		want := []uintptr{id(a), id(b)}
		sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
		if !reflect.DeepEqual(order, want) {
			t.Errorf("order %v, want %v", order, want)
		}
	})
	t.Run("lockable_in_kwargs", func(t *testing.T) {
		entered := spyAtomic(t)
		work := laila.Synchronized(func(self any, args ...any) (any, error) { return len(*entered), nil })
		lockable := newLockable(t)
		if _, err := work(struct{}{}, py.Kw{"target": lockable}); err != nil {
			t.Fatal(err)
		}
		if len(*entered) != 1 || (*entered)[0] != lockable {
			t.Error("kwarg target not locked")
		}
	})
	t.Run("return_value_preserved", func(t *testing.T) {
		work := laila.Synchronized(func(self any, args ...any) (any, error) { return py.DictOf("key", args[0]), nil })
		if v, _ := work(struct{}{}, 99); !py.Eq(v, py.DictOf("key", 99)) {
			t.Error("return value")
		}
	})
	t.Run("exception_propagates", func(t *testing.T) {
		work := laila.Synchronized(func(self any, args ...any) (any, error) { return nil, py.ValueError.New("sync-boom") })
		_, err := work(struct{}{})
		if !errors.Is(err, py.ValueError) || !strings.Contains(err.Error(), "sync-boom") {
			t.Errorf("got %v", err)
		}
	})
	t.Run("mixed_lockable_and_plain_args", func(t *testing.T) {
		entered := spyAtomic(t)
		work := laila.Synchronized(func(self any, args ...any) (any, error) { return py.Tuple(args), nil })
		lockable := newLockable(t)
		if _, err := work(struct{}{}, lockable, 42, "plain"); err != nil {
			t.Fatal(err)
		}
		if len(*entered) != 1 || (*entered)[0] != lockable {
			t.Error("mixed args")
		}
	})
	t.Run("same_lockable_passed_twice", func(t *testing.T) {
		entered := spyAtomic(t)
		work := laila.Synchronized(func(self any, args ...any) (any, error) { return true, nil })
		lockable := newLockable(t)
		if _, err := work(struct{}{}, lockable, lockable); err != nil {
			t.Fatal(err)
		}
		unique := map[uintptr]bool{}
		for _, o := range *entered {
			unique[id(o)] = true
		}
		if len(unique) != 1 {
			t.Errorf("unique ids = %d", len(unique))
		}
	})
}
