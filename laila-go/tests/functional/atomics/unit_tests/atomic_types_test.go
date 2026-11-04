// Port of tests/functional/atomics/unit_tests/test_atomic_{flag,int,str,
// list,dotmap}.py.
package unit_tests

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// runThreads is the ``threads = [Thread(target=worker, args=(i,)) ...];
// start; join`` idiom.
func runThreads(n int, worker func(i int)) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			worker(i)
		}(i)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// AtomicFlag
// ---------------------------------------------------------------------------

func TestAtomicFlag(t *testing.T) {
	newFlag := func(t *testing.T) *laila.AtomicFlag {
		f, err := laila.New_AtomicFlag()
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	t.Run("default_false", func(t *testing.T) {
		if newFlag(t).Is_set() {
			t.Error("default should be False")
		}
	})
	t.Run("set_clear_toggle", func(t *testing.T) {
		f := newFlag(t)
		f.Set()
		if !f.Is_set() {
			t.Error("set")
		}
		f.Toggle()
		if f.Is_set() {
			t.Error("toggle")
		}
		f.Clear()
		if f.Is_set() {
			t.Error("clear")
		}
	})
	t.Run("set_to", func(t *testing.T) {
		f := newFlag(t)
		f.Set_to(true)
		if !f.Is_set() {
			t.Error("set_to(True)")
		}
		f.Set_to(false)
		if f.Is_set() {
			t.Error("set_to(False)")
		}
	})
	t.Run("atomic_context_direct_mutation", func(t *testing.T) {
		f := newFlag(t)
		func() {
			locked, release := f.Atomic()
			defer release()
			locked.Value = true
		}()
		if !f.Is_set() {
			t.Error("atomic mutation lost")
		}
	})
	t.Run("concurrent_toggle_is_safe", func(t *testing.T) {
		f := newFlag(t)
		runThreads(10, func(int) {
			for i := 0; i < 1000; i++ {
				f.Toggle()
			}
		})
		_ = f.Is_set() // either value is acceptable; -race is the assertion
	})
}

// ---------------------------------------------------------------------------
// AtomicInt
// ---------------------------------------------------------------------------

func TestAtomicInt(t *testing.T) {
	newInt := func(t *testing.T) *laila.AtomicInt {
		i, err := laila.New_AtomicInt()
		if err != nil {
			t.Fatal(err)
		}
		return i
	}
	t.Run("default_zero", func(t *testing.T) {
		if newInt(t).Get() != 0 {
			t.Error("default")
		}
	})
	t.Run("set_add_increment_decrement", func(t *testing.T) {
		i := newInt(t)
		if err := i.Set_to(5); err != nil {
			t.Fatal(err)
		}
		if i.Get() != 5 || i.Add(3) != 8 || i.Increment() != 9 || i.Decrement() != 8 {
			t.Errorf("arithmetic: %d", i.Get())
		}
	})
	t.Run("reset", func(t *testing.T) {
		i := newInt(t)
		_ = i.Set_to(99)
		i.Reset()
		if i.Get() != 0 {
			t.Error("reset")
		}
	})
	t.Run("atomic_context", func(t *testing.T) {
		i := newInt(t)
		func() {
			locked, release := i.Atomic()
			defer release()
			locked.Value += 10
		}()
		if i.Get() != 10 {
			t.Error("atomic")
		}
	})
	t.Run("concurrent_increments", func(t *testing.T) {
		i := newInt(t)
		runThreads(8, func(int) {
			for k := 0; k < 500; k++ {
				i.Increment()
			}
		})
		if i.Get() != 8*500 {
			t.Errorf("got %d", i.Get())
		}
	})
}

// ---------------------------------------------------------------------------
// AtomicStr
// ---------------------------------------------------------------------------

func TestAtomicStr(t *testing.T) {
	newStr := func(t *testing.T) *laila.AtomicStr {
		s, err := laila.New_AtomicStr()
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	t.Run("default_empty", func(t *testing.T) {
		s := newStr(t)
		if s.Get() != "" || s.Length() != 0 {
			t.Error("default")
		}
	})
	t.Run("set_append_clear", func(t *testing.T) {
		s := newStr(t)
		s.Set("ab")
		if s.Get() != "ab" {
			t.Error("set")
		}
		if s.Append("cd") != "abcd" || s.Length() != 4 {
			t.Error("append")
		}
		s.Clear()
		if s.Get() != "" {
			t.Error("clear")
		}
	})
	t.Run("atomic_context", func(t *testing.T) {
		s := newStr(t)
		func() {
			locked, release := s.Atomic()
			defer release()
			locked.Set("locked")
		}()
		if s.Get() != "locked" {
			t.Error("atomic")
		}
	})
	t.Run("str_dunder", func(t *testing.T) {
		s := newStr(t)
		s.Set("hello")
		if py.Str(s) != "hello" || s.String() != "hello" {
			t.Error("__str__")
		}
	})
	t.Run("concurrent_appends", func(t *testing.T) {
		s := newStr(t)
		runThreads(5, func(i int) {
			ch := string(rune('A' + i))
			for k := 0; k < 100; k++ {
				s.Append(ch)
			}
		})
		if s.Length() != 5*100 {
			t.Errorf("length %d", s.Length())
		}
	})
}

// ---------------------------------------------------------------------------
// AtomicList
// ---------------------------------------------------------------------------

func TestAtomicList(t *testing.T) {
	newList := func(t *testing.T, kw ...py.Kw) *laila.AtomicList {
		l, err := laila.New_AtomicList(kw...)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	t.Run("default_empty", func(t *testing.T) {
		lst := newList(t)
		if lst.Len() != 0 || len(lst.To_list()) != 0 {
			t.Error("default")
		}
	})
	t.Run("append_extend_insert_pop", func(t *testing.T) {
		lst := newList(t)
		lst.Append(1)
		lst.Extend([]any{2, 3})
		lst.Insert(1, 9)
		if !py.Eq(lst.To_list(), []any{1, 9, 2, 3}) {
			t.Errorf("to_list = %v", lst.To_list())
		}
		v, err := lst.Pop()
		if err != nil || v != 3 {
			t.Errorf("pop = %v, %v", v, err)
		}
		if !py.Eq(lst.To_list(), []any{1, 9, 2}) {
			t.Error("after pop")
		}
	})
	t.Run("set_get_slice", func(t *testing.T) {
		lst := newList(t, py.Kw{"value": []any{0, 1, 2, 3, 4}})
		if err := lst.Set_at(2, 99); err != nil {
			t.Fatal(err)
		}
		if v, _ := lst.Get_at(2); v != 99 {
			t.Error("get_at")
		}
		sl, err := lst.Slice(py.Ptr(1), py.Ptr(4))
		if err != nil || !py.Eq(sl, []any{1, 99, 3}) {
			t.Errorf("slice = %v, %v", sl, err)
		}
	})
	t.Run("trim", func(t *testing.T) {
		lst := newList(t, py.Kw{"value": []any{0, 1, 2, 3, 4}})
		lst.Trim(py.Ptr(1), py.Ptr(4))
		if !py.Eq(lst.To_list(), []any{1, 2, 3}) {
			t.Errorf("trim = %v", lst.To_list())
		}
	})
	t.Run("atomic_context_batch", func(t *testing.T) {
		lst := newList(t)
		func() {
			raw, release := lst.Atomic()
			defer release()
			raw.Extend([]any{1, 2, 3})
		}()
		if !py.Eq(lst.To_list(), []any{1, 2, 3}) {
			t.Error("batch")
		}
	})
	t.Run("concurrent_appends", func(t *testing.T) {
		lst := newList(t)
		runThreads(6, func(i int) {
			for k := 0; k < 200; k++ {
				lst.Append(i)
			}
		})
		if lst.Len() != 6*200 {
			t.Errorf("len %d", lst.Len())
		}
	})
}

// ---------------------------------------------------------------------------
// AtomicDotMap
// ---------------------------------------------------------------------------

func TestAtomicDotMap(t *testing.T) {
	t.Run("set_get_attr", func(t *testing.T) {
		dm := laila.New_AtomicDotMap()
		dm.Set("s3_bucket_region", "us-east-1")
		if dm.Get("s3_bucket_region") != "us-east-1" {
			t.Error("get")
		}
		if v, err := py.Getattr(dm, "s3_bucket_region"); err != nil || v != "us-east-1" {
			t.Errorf("dynamic getattr: %v %v", v, err)
		}
	})
	t.Run("delete_attr", func(t *testing.T) {
		dm := laila.New_AtomicDotMap()
		dm.Set("token", "abc")
		if err := dm.Delattr("token"); err != nil {
			t.Fatal(err)
		}
		if dm.Get("token") != nil {
			t.Error("deleted attr should read None")
		}
	})
	t.Run("to_dict_keys_items", func(t *testing.T) {
		dm := laila.New_AtomicDotMap()
		dm.Set("a", 1)
		dm.Set("b", 2)
		if !py.Eq(dm.To_dict(), py.DictOf("a", 1, "b", 2)) {
			t.Errorf("to_dict = %s", py.Repr(dm.To_dict()))
		}
		keys := dm.Keys()
		if len(keys) != 2 || !py.Eq(py.NewSet(keys...), py.NewSet("a", "b")) {
			t.Errorf("keys = %v", keys)
		}
		items := dm.Items()
		if len(items) != 2 {
			t.Errorf("items = %v", items)
		}
	})
	t.Run("repr", func(t *testing.T) {
		dm := laila.New_AtomicDotMap()
		dm.Set("x", 3)
		if !strings.Contains(py.Repr(dm), "AtomicDotMap") {
			t.Errorf("repr = %s", py.Repr(dm))
		}
	})
	t.Run("concurrent_sets", func(t *testing.T) {
		dm := laila.New_AtomicDotMap()
		runThreads(10, func(i int) {
			if err := dm.Setattr(fmt.Sprintf("k%d", i), i); err != nil {
				t.Error(err)
			}
		})
		data := dm.To_dict()
		if data.Len() != 10 {
			t.Errorf("len %d", data.Len())
		}
		for i := 0; i < 10; i++ {
			if data.GetDefault(fmt.Sprintf("k%d", i), nil) != i {
				t.Errorf("k%d", i)
			}
		}
	})
}
