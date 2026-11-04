// Port of tests/functional/atomics/unit_tests/test_atomic_dict_functional.py
// (50 single-threaded functional cases).
package unit_tests

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func makeDict(t *testing.T, d ...any) *laila.AtomicDict {
	t.Helper()
	var ad *laila.AtomicDict
	var err error
	if len(d) == 0 || d[0] == nil {
		ad, err = laila.New_AtomicDict(py.NewDict())
	} else {
		ad, err = laila.New_AtomicDict(d...)
	}
	if err != nil {
		t.Fatal(err)
	}
	return ad
}

func rangeDict(n int) *py.Dict {
	d := py.NewDict()
	for i := 0; i < n; i++ {
		d.Set(fmt.Sprint(i), i)
	}
	return d
}

func keysEq(t *testing.T, d *laila.AtomicDict, want ...any) {
	t.Helper()
	if !py.Eq(d.Keys(), want) {
		t.Errorf("keys = %s, want %s", py.Repr(d.Keys()), py.Repr(want))
	}
}

func item(t *testing.T, d *laila.AtomicDict, key any) any {
	t.Helper()
	v, err := d.Getitem(key)
	if err != nil {
		t.Fatalf("d[%v]: %v", key, err)
	}
	return v
}

func TestAtomicDictFunctional(t *testing.T) {
	t.Run("001_init_variants", func(t *testing.T) {
		d1, err := laila.New_AtomicDict(py.Kw{"data": py.DictOf("a", 1)})
		if err != nil {
			t.Fatal(err)
		}
		d2, err := laila.New_AtomicDict(py.DictOf("b", 2))
		if err != nil {
			t.Fatal(err)
		}
		if item(t, d1, "a") != 1 || item(t, d2, "b") != 2 {
			t.Error("init")
		}
	})
	t.Run("002_conflicting_init", func(t *testing.T) {
		_, err := laila.New_AtomicDict(py.DictOf("a", 1), py.Kw{"data": py.DictOf("b", 2)})
		if !errors.Is(err, py.TypeError) {
			t.Errorf("want TypeError, got %v", err)
		}
	})
	t.Run("003_update_order", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1))
		d.Update(py.DictOf("b", 2))
		d.Update([]any{py.Tuple{"c", 3}})
		d.Update(nil, py.Kw{"d": 4, "e": 5})
		keys := d.Keys()
		if len(keys) != 5 || keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
			t.Errorf("keys = %v", keys)
		}
		// kwargs order (d, e) is not observable in Go maps; the set is.
		if !py.Eq(py.NewSet(keys[3:]...), py.NewSet("d", "e")) {
			t.Errorf("keys = %v", keys)
		}
	})
	t.Run("004_pop_default", func(t *testing.T) {
		v, err := makeDict(t).Pop("x", 7)
		if err != nil || v != 7 {
			t.Errorf("pop = %v, %v", v, err)
		}
	})
	t.Run("005_pop_raises", func(t *testing.T) {
		if _, err := makeDict(t).Pop("x"); !errors.Is(err, py.KeyError) {
			t.Errorf("want KeyError, got %v", err)
		}
	})
	t.Run("006_popitem_end", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1, "b", 2, "c", 3))
		k, v, err := d.Popitem()
		if err != nil || k != "c" || v != 3 {
			t.Errorf("popitem = %v %v %v", k, v, err)
		}
		keysEq(t, d, "a", "b")
	})
	t.Run("007_pop_next_start", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1, "b", 2, "c", 3))
		k, v, err := d.Pop_next()
		if err != nil || k != "a" || v != 1 {
			t.Errorf("pop_next = %v %v %v", k, v, err)
		}
		keysEq(t, d, "b", "c")
	})
	t.Run("008_popitem_empty", func(t *testing.T) {
		if _, _, err := makeDict(t).Popitem(); !errors.Is(err, py.KeyError) {
			t.Errorf("want KeyError, got %v", err)
		}
	})
	t.Run("009_pop_next_empty", func(t *testing.T) {
		if _, _, err := makeDict(t).Pop_next(); !errors.Is(err, py.KeyError) {
			t.Errorf("want KeyError, got %v", err)
		}
	})
	t.Run("010_setdefault", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1))
		if d.Setdefault("a", 9) != 1 || d.Setdefault("b", 2) != 2 || item(t, d, "b") != 2 {
			t.Error("setdefault")
		}
	})
	t.Run("011_compute_lifecycle", func(t *testing.T) {
		d := makeDict(t)
		_, _ = d.Compute("x", func(v any) (any, error) {
			if v == nil {
				return 1, nil
			}
			return v.(int) + 1, nil
		})
		_, _ = d.Compute("x", func(v any) (any, error) { return v.(int) + 4, nil })
		if item(t, d, "x") != 5 {
			t.Error("compute")
		}
		_, _ = d.Compute("x", func(any) (any, error) { return nil, nil })
		if d.Contains("x") {
			t.Error("compute(None) should delete")
		}
	})
	t.Run("012_increment", func(t *testing.T) {
		d := makeDict(t)
		_, _ = d.Increment("k")
		_, _ = d.Increment("k", 4)
		_, _ = d.Increment("k", -2)
		if !py.Eq(item(t, d, "k"), 3) {
			t.Errorf("k = %v", item(t, d, "k"))
		}
	})
	t.Run("013_contains", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1))
		if !d.Contains("a") || d.Contains("b") {
			t.Error("contains")
		}
	})
	t.Run("014_iter_snapshot", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1, "b", 2))
		list1 := d.Iter()
		d.Setitem("c", 3)
		if !py.Eq(list1, []any{"a", "b"}) {
			t.Error("snapshot")
		}
		keysEq(t, d, "a", "b", "c")
	})
	t.Run("015_views", func(t *testing.T) {
		d := makeDict(t, py.DictOf("x", 1, "y", 2))
		keysEq(t, d, "x", "y")
		if !py.Eq(d.Values(), []any{1, 2}) {
			t.Error("values")
		}
		items := d.Items()
		if len(items) != 2 || items[0].Key != "x" || items[0].Value != 1 || items[1].Key != "y" || items[1].Value != 2 {
			t.Errorf("items = %v", items)
		}
	})
	t.Run("016_index_helpers", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 10, "b", 20, "c", 30))
		it, err := d.Item_at(0)
		if err != nil || it.Key != "a" || it.Value != 10 {
			t.Error("item_at")
		}
		if k, _ := d.Key_at(2); k != "c" {
			t.Error("key_at")
		}
		if v, _ := d.Value_at(-1); v != 30 {
			t.Error("value_at(-1)")
		}
	})
	t.Run("017_index_out_of_range", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1))
		if _, err := d.Item_at(2); !errors.Is(err, py.IndexError) {
			t.Errorf("want IndexError, got %v", err)
		}
	})
	t.Run("018_trim_window", func(t *testing.T) {
		d := makeDict(t, rangeDict(5))
		d.Trim(py.Ptr(1), py.Ptr(4))
		keysEq(t, d, "1", "2", "3")
	})
	t.Run("019_trim_negative_end_none", func(t *testing.T) {
		d := makeDict(t, rangeDict(5))
		d.Trim(py.Ptr(-3), nil)
		keysEq(t, d, "2", "3", "4")
	})
	t.Run("020_trim_clears", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1, "b", 2))
		d.Trim(py.Ptr(10), py.Ptr(2))
		if d.Len() != 0 {
			t.Error("trim should clear")
		}
	})
	t.Run("021_reindex", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1, "b", 2))
		d.Reindex()
		keysEq(t, d, "a", "b")
	})
	t.Run("022_clear", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1))
		d.Clear()
		if d.Len() != 0 || len(d.Keys()) != 0 {
			t.Error("clear")
		}
	})
	t.Run("023_repr_ordered", func(t *testing.T) {
		r := py.Repr(makeDict(t, py.DictOf("a", 1, "b", 2)))
		if !strings.Contains(r, "AtomicDict") || strings.Index(r, "a") >= strings.Index(r, "b") {
			t.Errorf("repr = %s", r)
		}
	})
	t.Run("024_pretty_format", func(t *testing.T) {
		s := makeDict(t, py.DictOf("a", 1, "b", 2)).Pretty()
		if !strings.HasPrefix(strings.TrimSpace(s), "AtomicDict {") || !strings.Contains(s, "'a': 1") {
			t.Errorf("pretty = %q", s)
		}
	})
	t.Run("025_update_iterable", func(t *testing.T) {
		d := makeDict(t)
		d.Update([]any{py.Tuple{"a", 1}, py.Tuple{"b", 2}})
		items := d.Items()
		if len(items) != 2 || items[0].Key != "a" || items[1].Key != "b" {
			t.Errorf("items = %v", items)
		}
	})
	t.Run("026_update_mapping", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1))
		d.Update(py.DictOf("b", 2, "c", 3))
		keysEq(t, d, "a", "b", "c")
	})
	t.Run("027_iter_snapshot_again", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1))
		snap := d.Iter()
		d.Setitem("b", 2)
		if !py.Eq(snap, []any{"a"}) {
			t.Error("snapshot")
		}
	})
	t.Run("028_get_default_type", func(t *testing.T) {
		d := makeDict(t)
		if !py.Eq(d.Get("x", py.Tuple{"z"}), py.Tuple{"z"}) {
			t.Error("get default type")
		}
	})
	t.Run("029_overwrite_keeps_order", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1, "b", 2))
		d.Setitem("a", 9)
		keysEq(t, d, "a", "b")
		if item(t, d, "a") != 9 {
			t.Error("value")
		}
	})
	t.Run("030_compute_order", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1, "b", 2))
		_, _ = d.Compute("a", func(v any) (any, error) { return v.(int) + 1, nil })
		keysEq(t, d, "a", "b")
	})
	t.Run("031_compute_insert_end", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1))
		_, _ = d.Compute("z", func(v any) (any, error) {
			if v == nil {
				return 10, nil
			}
			return v, nil
		})
		keysEq(t, d, "a", "z")
	})
	t.Run("032_increment_insert_end", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1))
		_, _ = d.Increment("z", 3)
		keysEq(t, d, "a", "z")
	})
	t.Run("033_len", func(t *testing.T) {
		d := makeDict(t)
		d.Setitem("a", 1)
		d.Setitem("b", 2)
		_ = d.Delitem("a")
		d.Setitem("c", 3)
		if d.Len() != 2 {
			t.Error("len")
		}
	})
	t.Run("034_contains_after_trim", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1, "b", 2))
		d.Trim(py.Ptr(1), py.Ptr(2))
		if d.Contains("a") {
			t.Error("contains after trim")
		}
	})
	t.Run("035_items_reflect_mutation", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1))
		d.Setitem("a", 9)
		items := d.Items()
		if len(items) != 1 || items[0].Value != 9 {
			t.Error("items")
		}
	})
	t.Run("036_current_in_atomic", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1))
		_, release := d.Atomic("h")
		defer release()
		dd, err := laila.AtomicDict_current()
		if err != nil || dd.Get("a") != 1 {
			t.Errorf("current: %v %v", dd, err)
		}
	})
	t.Run("037_current_outside", func(t *testing.T) {
		if _, err := laila.AtomicDict_current(); !errors.Is(err, py.RuntimeError) {
			t.Errorf("want RuntimeError, got %v", err)
		}
	})
	// 038_hint_type: ``atomic(hint=123)`` raising TypeError is a compile-time
	// guarantee in Go (``hint ...string``).
	t.Run("039_run_atomic_return", func(t *testing.T) {
		d := makeDict(t, py.DictOf("x", 1))
		v, err := d.Run_atomic(func() (any, error) {
			dd, err := laila.AtomicDict_current()
			if err != nil {
				return nil, err
			}
			x, err := dd.Getitem("x")
			if err != nil {
				return nil, err
			}
			return x.(int) + 1, nil
		}, "r")
		if err != nil || v != 2 {
			t.Errorf("run_atomic = %v, %v", v, err)
		}
	})
	t.Run("040_run_atomic_exception", func(t *testing.T) {
		d := makeDict(t, py.DictOf("x", 1))
		_, err := d.Run_atomic(func() (any, error) { return nil, py.ValueError.New("x") }, "x")
		if !errors.Is(err, py.ValueError) {
			t.Errorf("want ValueError, got %v", err)
		}
		d.Setitem("y", 2) // lock must have been released
		if item(t, d, "y") != 2 {
			t.Error("still usable")
		}
	})
	t.Run("041_atomicview_ops", func(t *testing.T) {
		d := makeDict(t)
		func() {
			_, release := d.Atomic("v")
			defer release()
			v := &laila.AtomicDict_AtomicView{P_: d}
			v.Update(nil, py.Kw{"a": 1})
			v.Setitem("b", 2)
			_ = v.Delitem("a")
		}()
		items := d.Items()
		if len(items) != 1 || items[0].Key != "b" || items[0].Value != 2 {
			t.Errorf("items = %v", items)
		}
	})
	t.Run("042_compute_multiple", func(t *testing.T) {
		d := makeDict(t)
		for _, k := range []string{"a", "b", "c"} {
			_, _ = d.Compute(k, func(any) (any, error) { return 1, nil })
		}
		keysEq(t, d, "a", "b", "c")
	})
	t.Run("043_update_overwrite_order", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1, "b", 2))
		d.Update(py.DictOf("a", 9))
		keysEq(t, d, "a", "b")
		if item(t, d, "a") != 9 {
			t.Error("value")
		}
	})
	t.Run("044_trim_full", func(t *testing.T) {
		d := makeDict(t, rangeDict(3))
		d.Trim(py.Ptr(0), py.Ptr(3))
		keysEq(t, d, "0", "1", "2")
	})
	t.Run("045_trim_negative_start_clamp", func(t *testing.T) {
		d := makeDict(t, rangeDict(3))
		d.Trim(py.Ptr(-10), py.Ptr(2))
		keysEq(t, d, "0", "1")
	})
	t.Run("046_trim_end_clamp", func(t *testing.T) {
		d := makeDict(t, rangeDict(3))
		d.Trim(py.Ptr(1), py.Ptr(99))
		keysEq(t, d, "1", "2")
	})
	t.Run("047_remove_readd_order", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1, "b", 2))
		_ = d.Delitem("a")
		d.Setitem("a", 9)
		keysEq(t, d, "b", "a")
	})
	t.Run("048_len_after_many_ops", func(t *testing.T) {
		d := makeDict(t)
		for i := 0; i < 10; i++ {
			d.Setitem(fmt.Sprint(i), i)
		}
		for i := 0; i < 10; i += 2 {
			_ = d.Delitem(fmt.Sprint(i))
		}
		if d.Len() != 5 {
			t.Error("len")
		}
	})
	t.Run("049_contains_object_type", func(t *testing.T) {
		if makeDict(t, py.DictOf("a", 1)).Contains(123) {
			t.Error("contains(123)")
		}
	})
	t.Run("050_keys_values_alignment", func(t *testing.T) {
		d := makeDict(t, py.DictOf("a", 1, "b", 2))
		d.Setitem("b", 20)
		d.Setitem("a", 10)
		keys, values := d.Keys(), d.Values()
		if !py.Eq(keys, []any{"a", "b"}) || !py.Eq(values, []any{10, 20}) {
			t.Errorf("keys=%v values=%v", keys, values)
		}
	})
}
