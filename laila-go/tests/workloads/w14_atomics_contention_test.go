//go:build workload

// Port of tests/workloads/test_w14_atomics_contention.py.
//
// W14 -- atomic containers under heavy multi-threaded contention.
//
// AtomicDict / AtomicList / AtomicInt / AtomicFlag / AtomicStr / AtomicDotMap
// hammered by many threads, scaling behaviour of order-tracked operations,
// ``current()`` context propagation, lock re-entrancy, pickling/deepcopy of
// locked objects and the policy-scoped global lock.
package workloads

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// new_adict14 is ``AtomicDict(*args)``.
func new_adict14(t *testing.T, args ...any) *laila.AtomicDict {
	t.Helper()
	d, err := laila.New_AtomicDict(args...)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// dict_range14 is ``{f"k{i}": i for i in range(n)}`` (``str_keys``) or
// ``{i: i for i in range(n)}``.
func dict_range14(n int, str_keys bool) *py.Dict {
	d := py.NewDict()
	for i := 0; i < n; i++ {
		if str_keys {
			d.Set(fmt.Sprintf("k%d", i), i)
		} else {
			d.Set(i, i)
		}
	}
	return d
}

// items_eq14 is ``d.items() == [(k, v), ...]``.
func items_eq14(got []py.Item, want ...py.Item) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if !py.Eq(got[i].Key, want[i].Key) || !py.Eq(got[i].Value, want[i].Value) {
			return false
		}
	}
	return true
}

// any_list14 is ``list(range(start, stop))`` as ``[]any``.
func any_list14(start, stop int) []any {
	out := make([]any, 0, max(0, stop-start))
	for i := start; i < stop; i++ {
		out = append(out, i)
	}
	return out
}

// ---------------------------------------------------------------------------
// AtomicDict
// ---------------------------------------------------------------------------

func TestAtomicDict_concurrent_increment_no_lost_updates(t *testing.T) {
	Setup(t)
	d := new_adict14(t)
	n_threads, per := 16, N_scaled(5000)

	w := func(i int) error {
		for j := 0; j < per; j++ {
			if _, err := d.Increment("k"); err != nil {
				return err
			}
		}
		return nil
	}

	check(t, Go_threads(w, n_threads))
	assertEq(t, must(d.Getitem("k")), n_threads*per)
}

func TestAtomicDict_concurrent_disjoint_writers_and_readers(t *testing.T) {
	Setup(t)
	d := new_adict14(t)
	stop := py.NewEvent()
	var mu sync.Mutex
	errs := []string{}

	writer := func(i int) error {
		for j := 0; j < N_scaled(3000); j++ {
			d.Setitem(fmt.Sprintf("%d-%d", i, j), j)
			if j%10 == 0 {
				_, _ = d.Pop(fmt.Sprintf("%d-%d", i, j-5), nil)
			}
		}
		return nil
	}

	reader := func(_ int) {
		for !stop.Is_set() {
			err := func() (err error) {
				defer py.Recover(&err)
				for _, it := range d.Items() {
					if _, ok := it.Value.(int); !ok {
						return py.AssertionError.Newf("%s", py.Repr(it.Value))
					}
				}
				d.Len()
				d.Iter()
				d.Keys()
				d.Values()
				return nil
			}()
			if err != nil { // pragma: no cover
				mu.Lock()
				errs = append(errs, py.Repr(err))
				mu.Unlock()
				return
			}
		}
	}

	rs := make([]*py.Thread, 4)
	for i := range rs {
		i := i
		rs[i] = py.New_Thread(py.Kw{"target": func() { reader(i) }, "daemon": true})
	}
	for _, r := range rs {
		check(t, r.Start())
	}
	err := Go_threads(writer, 8)
	stop.Set()
	for _, r := range rs {
		_ = r.Join(-1)
	}
	check(t, err)
	assertEq(t, errs, []string{})
	assertTrue(t, d.Len() == d.Data.Len() && d.Len() == len(d.Keys()))
}

func TestAtomicDict_order_matches_dict_after_churn(t *testing.T) {
	Setup(t)
	d := new_adict14(t)
	rnd := rand.New(rand.NewSource(1))
	for i := 0; i < N_scaled(20000); i++ {
		op := rnd.Float64()
		k := fmt.Sprintf("k%d", rnd.Intn(501))
		if op < 0.5 {
			d.Setitem(k, i)
		} else if op < 0.8 {
			_, _ = d.Pop(k, nil)
		} else if op < 0.9 {
			if d.Len() != 0 {
				_, _, _ = d.Pop_next()
			}
		} else {
			if d.Len() != 0 {
				_, _, _ = d.Popitem()
			}
		}
	}
	assertEq(t, d.Keys(), d.Data.Keys())
}

func TestAtomicDict_pop_by_key_is_linear_in_size(t *testing.T) {
	Setup(t)
	// ``data`` is the order authority, so a keyed delete is O(1) and
	// draining a large dict by key is linear.
	drain := func(n int) float64 {
		d := new_adict14(t, dict_range14(n, true))
		sw := Timed(func() {
			for i := n - 1; i >= 0; i-- { // newest first: worst case for list.remove
				must(d.Pop(fmt.Sprintf("k%d", i)))
			}
		})
		return sw.Elapsed
	}

	small := drain(N_scaled(5000))
	large := drain(N_scaled(20000))
	check(t, Assert_subquadratic(small, large, 4, 3.0))
}

func TestAtomicDict_pop_next_is_constant_time(t *testing.T) {
	Setup(t)
	drain := func(n int) float64 {
		d := new_adict14(t, dict_range14(n, true))
		sw := Timed(func() {
			for i := 0; i < n; i++ {
				_, _, err := d.Pop_next()
				check(t, err)
			}
		})
		return sw.Elapsed
	}

	check(t, Assert_subquadratic(drain(N_scaled(20000)), drain(N_scaled(80000)), 4))
}

func TestAtomicDict_item_at_and_trim_at_scale(t *testing.T) {
	Setup(t)
	n := N_scaled(50000)
	d := new_adict14(t, dict_range14(n, false))
	it := must(d.Item_at(0))
	assertTrue(t, py.Eq(it.Key, 0) && py.Eq(it.Value, 0))
	it = must(d.Item_at(-1))
	assertTrue(t, py.Eq(it.Key, n-1) && py.Eq(it.Value, n-1))
	assertEq(t, must(d.Key_at(n/2)), n/2)
	d.Trim(py.Ptr(10), py.Ptr(n-10))
	assertTrue(t, d.Len() == n-20 && py.Eq(must(d.Key_at(0)), 10))
	d.Trim(py.Ptr(-5), nil)
	assertEq(t, d.Keys(), any_list14(n-15, n-10))
	d.Trim(py.Ptr(3), py.Ptr(2))
	assertEq(t, d.Len(), 0)
}

func TestAtomicDict_out_of_band_edit_detected_by_walkers(t *testing.T) {
	Setup(t)
	d := new_adict14(t, py.DictOf("a", 1, "b", 2))
	check(t, d.Data.Del("a"))
	d.Data.Set("c", 3)
	// same length -> O(1) sync misses it, but O(n) accessors must cope
	assertTrue(t, items_eq14(d.Items(), py.Item{Key: "b", Value: 2}, py.Item{Key: "c", Value: 3}))
	assertEq(t, d.Values(), []any{2, 3})
	assertTrue(t, strings.Contains(py.Repr(d), "AtomicDict"))
	it := must(d.Item_at(1))
	assertTrue(t, py.Eq(it.Key, "c") && py.Eq(it.Value, 3))
}

func TestAtomicDict_out_of_band_edit_and_keys_snapshot(t *testing.T) {
	Setup(t)
	d := new_adict14(t, py.DictOf("a", 1, "b", 2))
	check(t, d.Data.Del("a"))
	d.Data.Set("c", 3)
	// keys()/iter read ``data`` directly, so a same-length out-of-band
	// edit is visible immediately (no reindex() needed)
	assertEq(t, d.Keys(), []any{"b", "c"})
	assertEq(t, d.Iter(), []any{"b", "c"})
}

func TestAtomicDict_compute_contention(t *testing.T) {
	Setup(t)
	d := new_adict14(t)

	w := func(i int) error {
		for j := 0; j < N_scaled(3000); j++ {
			if _, err := d.Compute("c", func(v any) (any, error) {
				if py.IsNone(v) {
					return 1, nil
				}
				return py.Add(v, 1)
			}); err != nil {
				return err
			}
		}
		if _, err := d.Compute(fmt.Sprintf("del%d", i), func(v any) (any, error) { return 1, nil }); err != nil {
			return err
		}
		_, err := d.Compute(fmt.Sprintf("del%d", i), func(v any) (any, error) { return nil, nil })
		return err
	}

	check(t, Go_threads(w, 8))
	assertEq(t, must(d.Getitem("c")), 8*N_scaled(3000))
	for _, k := range d.Keys() {
		assertTrue(t, !strings.HasPrefix(py.Str(k), "del"), k)
	}
}

func TestAtomicDict_atomic_block_is_exclusive_and_reentrant(t *testing.T) {
	Setup(t)
	d := new_adict14(t)
	inside := 0
	peak := 0

	w := func(i int) error {
		for j := 0; j < 500; j++ {
			if err := func() error {
				view, release := d.Atomic("outer")
				defer release()
				inside++
				peak = max(peak, inside)
				func() {
					_, release := d.Atomic("inner")
					defer release()
					view.Setitem("k", i)
					cur, err := laila.AtomicDict_current()
					if err != nil {
						panic(err)
					}
					if cur != d {
						panic(py.AssertionError.New("AtomicDict.current() is not d"))
					}
				}()
				py.Sleep_sync(0)
				inside--
				return nil
			}(); err != nil {
				return err
			}
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	assertEq(t, peak, 1)
}

func TestAtomicDict_current_is_context_local(t *testing.T) {
	Setup(t)
	a, b := new_adict14(t), new_adict14(t)
	var mu sync.Mutex
	seen := map[int]bool{}

	w := func(i int) error {
		d := b
		if i%2 != 0 {
			d = a
		}
		_, release := d.Atomic()
		defer release()
		py.Sleep_sync(0.01)
		cur, err := laila.AtomicDict_current()
		if err != nil {
			return err
		}
		mu.Lock()
		seen[i] = cur == d
		mu.Unlock()
		return nil
	}

	check(t, Go_threads(w, 8))
	for _, ok := range seen {
		assertTrue(t, ok)
	}
	if _, err := laila.AtomicDict_current(); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("want RuntimeError, got %v", err)
	}
}

func TestAtomicDict_run_atomic_exception_releases_lock(t *testing.T) {
	Setup(t)
	d := new_adict14(t)
	if _, err := d.Run_atomic(func() (any, error) { return nil, py.ValueError.New("x") }); !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
	assertTrue(t, d.Lock_.Acquire(true, 1))
	check(t, d.Lock_.Release())
	if _, err := laila.AtomicDict_current(); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("want RuntimeError, got %v", err)
	}
}

// ``d.atomic(hint=5)`` raising TypeError is a compile-time guarantee in Go
// (``Atomic(hint ...string)`` / ``Run_atomic(fn, hint ...string)``); the
// dynamic-dispatch path (``py.Call`` with ``hint=5``) is what a non-str hint
// can still reach, and it must reject it the same way.
func TestAtomicDict_hint_type_checked(t *testing.T) {
	Setup(t)
	d := new_adict14(t)
	atomic := must(py.Getattr(d, "atomic"))
	if _, err := py.Call(atomic, nil, py.Kw{"hint": 5}); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
	run_atomic := must(py.Getattr(d, "run_atomic"))
	if _, err := py.Call(run_atomic, []any{lam(1)}, py.Kw{"hint": 5}); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

func TestAtomicDict_constructor_variants(t *testing.T) {
	Setup(t)
	assertEq(t, must(new_adict14(t, py.DictOf("a", 1)).Getitem("a")), 1)
	assertEq(t, must(new_adict14(t, py.Kw{"data": py.DictOf("a", 1)}).Getitem("a")), 1)
	if _, err := laila.New_AtomicDict(py.DictOf("a", 1), py.Kw{"data": py.DictOf("b", 2)}); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
	if _, err := laila.New_AtomicDict(py.DictOf("a", 1), py.DictOf("b", 2)); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
	if _, err := laila.New_AtomicDict(py.Kw{"bogus": 1}); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

func TestAtomicDict_large_update_and_setdefault(t *testing.T) {
	Setup(t)
	d := new_adict14(t)
	d.Update(dict_range14(N_scaled(100000), false))
	d.Update(py.NewList(py.Tuple{-1, -1}), py.Kw{"extra": 1})
	assertEq(t, d.Len(), N_scaled(100000)+2)
	assertEq(t, d.Setdefault(0, 99), 0)
	assertEq(t, d.Setdefault("new", 7), 7)
	assertTrue(t, py.IsNone(d.Get("missing")))
	if _, err := d.Pop("missing"); !errors.Is(err, py.KeyError) {
		t.Fatalf("want KeyError, got %v", err)
	}
}

func TestAtomicDict_pickle_and_deepcopy_under_lock(t *testing.T) {
	Setup(t)
	d := new_adict14(t, py.DictOf("a", py.NewList(1, 2)))
	var blob []byte
	var c *laila.AtomicDict
	func() {
		_, release := d.Atomic()
		defer release()
		var err error
		blob, err = py.Pickle_dumps(d)
		check(t, err)
		c = py.Deepcopy(d).(*laila.AtomicDict)
	}()
	d2 := must(py.Pickle_loads(blob)).(*laila.AtomicDict)
	assertTrue(t, py.Eq(must(d2.Getitem("a")), py.NewList(1, 2)) && py.Eq(must(c.Getitem("a")), py.NewList(1, 2)))
	must(c.Getitem("a")).(*py.List).Append(3)
	assertEq(t, must(d.Getitem("a")), py.NewList(1, 2))
	// restored copies have their own, unlocked locks
	assertTrue(t, d2.Lock_.Acquire(true, 1))
	check(t, d2.Lock_.Release())
}

func TestAtomicDict_pretty_large(t *testing.T) {
	Setup(t)
	data := py.NewDict()
	for i := 0; i < 5000; i++ {
		data.Set(i, strings.Repeat("x", 10))
	}
	d := new_adict14(t, data)
	s := d.Pretty()
	assertEq(t, strings.Count(s, "\n"), 5001)
}

func TestAtomicDict_popitem_popnext_interleaved_concurrently(t *testing.T) {
	Setup(t)
	d := new_adict14(t, dict_range14(N_scaled(40000), false))
	got := []int{}
	var lock sync.Mutex

	w := func(i int) error {
		local := []int{}
		for {
			var k any
			var err error
			if i%2 != 0 {
				k, _, err = d.Pop_next()
			} else {
				k, _, err = d.Popitem()
			}
			if err != nil {
				if errors.Is(err, py.KeyError) {
					break
				}
				return err
			}
			local = append(local, k.(int))
		}
		lock.Lock()
		got = append(got, local...)
		lock.Unlock()
		return nil
	}

	check(t, Go_threads(w, 8))
	sort.Ints(got)
	assertEq(t, got, Range(N_scaled(40000)))
	assertTrue(t, d.Len() == 0 && len(d.Keys()) == 0)
}

// ---------------------------------------------------------------------------
// AtomicList / Int / Flag / Str / DotMap
// ---------------------------------------------------------------------------

func TestAtomicList_concurrent_append_pop(t *testing.T) {
	Setup(t)
	lst := must(laila.New_AtomicList())

	w := func(i int) error {
		for j := 0; j < N_scaled(5000); j++ {
			lst.Append(i*100000 + j)
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	assertEq(t, lst.Len(), 8*N_scaled(5000))
	popped := []any{}
	var lock sync.Mutex

	p := func(_ int) error {
		local := []any{}
		for {
			v, err := lst.Pop()
			if err != nil {
				if errors.Is(err, py.IndexError) {
					break
				}
				return err
			}
			local = append(local, v)
		}
		lock.Lock()
		popped = append(popped, local...)
		lock.Unlock()
		return nil
	}

	check(t, Go_threads(p, 8))
	assertTrue(t, len(popped) == 8*N_scaled(5000) && lst.Len() == 0)
}

func TestAtomicList_slice_ops_and_trim(t *testing.T) {
	Setup(t)
	lst := must(laila.New_AtomicList())
	lst.Extend(Range_list(N_scaled(100000)))
	assertEq(t, must(lst.Getitem(py.NewSlice(10, 20))), py.NewList(any_list14(10, 20)...))
	check(t, lst.Setitem(py.NewSlice(0, 5), py.NewList(9, 9, 9)))
	assertTrue(t, py.Eq(must(lst.Getitem(0)), 9) && lst.Len() == N_scaled(100000)-2)
	check(t, lst.Delitem(py.NewSlice(0, 3)))
	assertEq(t, must(lst.Getitem(0)), 5)
	lst.Trim(py.Ptr(0), py.Ptr(10))
	assertEq(t, lst.To_list(), any_list14(5, 15))
	assertTrue(t, must(lst.Index(7)) == 2 && lst.Count(7) == 1)
	lst.Insert(0, -1)
	check(t, lst.Remove(-1))
	check(t, lst.Set_at(0, 100))
	assertEq(t, must(lst.Get_at(0)), 100)
	assertEq(t, must(lst.Slice(py.Ptr(0), py.Ptr(3))), []any{100, 6, 7})
}

func TestAtomicList_atomic_block_exposes_raw_list(t *testing.T) {
	Setup(t)
	lst := must(laila.New_AtomicList())

	w := func(i int) error {
		for j := 0; j < 2000; j++ {
			raw, release := lst.Atomic()
			raw.Append(i)
			raw.Append(i)
			release()
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	items := lst.To_list()
	assertEq(t, len(items), 8*4000)
	// pairs appended atomically must stay adjacent
	for k := 0; k < len(items); k += 2 {
		assertTrue(t, py.Eq(items[k], items[k+1]), k)
	}
}

func TestAtomicList_iteration_snapshot_while_mutating(t *testing.T) {
	Setup(t)
	lst := must(laila.New_AtomicList())
	lst.Extend(Range_list(1000))
	stop := py.NewEvent()

	mut := func() {
		for !stop.Is_set() {
			lst.Append(1)
			_, _ = lst.Pop(0)
		}
	}

	th := py.New_Thread(py.Kw{"target": mut, "daemon": true})
	check(t, th.Start())
	func() {
		defer func() {
			stop.Set()
			_ = th.Join(-1)
		}()
		for i := 0; i < 200; i++ {
			n := len(lst.Iter())
			assertTrue(t, n == 1000 || n == 1001, n)
		}
	}()
}

func TestScalars_atomic_int_contention(t *testing.T) {
	Setup(t)
	a := must(laila.New_AtomicInt())

	w := func(i int) error {
		for j := 0; j < N_scaled(20000); j++ {
			a.Increment()
			a.Add(2)
			a.Decrement()
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	assertEq(t, a.Get(), 8*N_scaled(20000)*2)
	func() {
		held, release := a.Atomic()
		defer release()
		check(t, held.Set_to(5))
	}()
	assertEq(t, a.Int(), 5)
	a.Reset()
	assertEq(t, a.Get(), 0)
}

func TestScalars_atomic_flag_toggle_parity(t *testing.T) {
	Setup(t)
	f := must(laila.New_AtomicFlag())

	w := func(i int) error {
		for j := 0; j < N_scaled(10001); j++ {
			f.Toggle()
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	assertEq(t, f.Bool(), (8*N_scaled(10001))%2 != 0)
	f.Set()
	assertTrue(t, f.Is_set())
	f.Clear()
	assertTrue(t, !f.Bool())
	f.Set_to(true)
	assertTrue(t, f.Is_set())
}

func TestScalars_atomic_str_append_lengths(t *testing.T) {
	Setup(t)
	s := must(laila.New_AtomicStr())

	w := func(i int) error {
		for j := 0; j < N_scaled(3000); j++ {
			s.Append(string(rune(97 + i)))
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	assertEq(t, s.Length(), 8*N_scaled(3000))
	set := map[rune]bool{}
	for _, r := range s.String() {
		set[r] = true
	}
	got := []string{}
	for r := range set {
		got = append(got, string(r))
	}
	sort.Strings(got)
	want := []string{}
	for i := 0; i < 8; i++ {
		want = append(want, string(rune(97+i)))
	}
	assertEq(t, got, want)
	s.Set("x")
	assertEq(t, s.Get(), "x")
	s.Clear()
	assertEq(t, s.Length(), 0)
}

func TestScalars_atomic_dotmap_contention(t *testing.T) {
	Setup(t)
	m := laila.New_AtomicDotMap()

	w := func(i int) error {
		for j := 0; j < N_scaled(3000); j++ {
			m.Set(fmt.Sprintf("a%d", i), j)
			if _, ok := m.Get(fmt.Sprintf("a%d", i)).(int); !ok {
				return py.AssertionError.Newf("a%d is not an int", i)
			}
		}
		if err := m.Delattr(fmt.Sprintf("a%d", i)); err != nil {
			return err
		}
		m.Set(fmt.Sprintf("b%d", i), i)
		return nil
	}

	check(t, Go_threads(w, 8))
	keys := []string{}
	for _, k := range m.Keys() {
		keys = append(keys, py.Str(k))
	}
	sort.Strings(keys)
	want := []string{}
	for i := 0; i < 8; i++ {
		want = append(want, fmt.Sprintf("b%d", i))
	}
	sort.Strings(want)
	assertEq(t, keys, want)
	assertTrue(t, py.IsNone(m.Get("missing")))
	from_items := py.NewDict()
	for _, it := range m.Items() {
		from_items.Set(it.Key, it.Value)
	}
	assertEq(t, m.To_dict(), from_items)
	assertTrue(t, strings.Contains(py.Repr(m), "AtomicDotMap"))
}

func TestScalars_atomic_str_append_is_linear_in_total_length(t *testing.T) {
	Setup(t)
	// append pops the value out of the instance dict before ``+=`` so
	// CPython extends the string in place (amortised linear build-up)
	build := func(n int) float64 {
		s := must(laila.New_AtomicStr())
		chunk := strings.Repeat("x", 64)
		sw := Timed(func() {
			for i := 0; i < n; i++ {
				s.Append(chunk)
			}
		})
		return sw.Elapsed
	}

	t1 := build(N_scaled(20000))
	t2 := build(N_scaled(80000))
	check(t, Assert_subquadratic(t1, t2, 4, 3.0))
}

// ---------------------------------------------------------------------------
// lock mixins
// ---------------------------------------------------------------------------

// Obj14_ is the test's ``_Obj(_LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT)``.
type Obj14_ struct {
	laila.LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_
	Counter int `py:"counter" default:"0"`
}

var Obj14_class = py.NewClass("_Obj", (*Obj14_)(nil), laila.LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_class)

// New_Obj14_ is ``_Obj(**kw)``.
func New_Obj14_(kw ...py.Kw) (*Obj14_, error) {
	o := Obj14_class.New().(*Obj14_)
	if err := o.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return o, nil
}

// Bump is ``bump(self)``.
func (o *Obj14_) Bump() int {
	o.Counter += 1
	return o.Counter
}

// GObj14_ is the test's ``_GObj(_LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT)``.
type GObj14_ struct {
	laila.LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_
	Counter int `py:"counter" default:"0"`
}

var GObj14_class = py.NewClass("_GObj", (*GObj14_)(nil), laila.LAILA_GLOBALLY_ATOMIC_IDENTIFIABLE_OBJECT_class)

// New_GObj14_ is ``_GObj(**kw)``.
func New_GObj14_(kw ...py.Kw) (*GObj14_, error) {
	o := GObj14_class.New().(*GObj14_)
	if err := o.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return o, nil
}

// Bump is ``bump(self)``: a gated public subclass method (``secret=``
// travels in kw).
func (o *GObj14_) Bump(kw ...py.Kw) (int, error) {
	if err := o.Gate_("bump", py.MergeKw(kw...)); err != nil {
		return 0, err
	}
	o.Counter += 1
	return o.Counter, nil
}

func TestLockMixins_local_atomic_exclusive(t *testing.T) {
	Setup(t)
	o := must(New_Obj14_())

	w := func(i int) error {
		for j := 0; j < N_scaled(5000); j++ {
			_, release, err := o.Atomic()
			if err != nil {
				return err
			}
			o.Bump()
			release()
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	assertEq(t, o.Counter, 8*N_scaled(5000))
}

func TestLockMixins_local_lock_timeout(t *testing.T) {
	Setup(t)
	o := must(New_Obj14_())
	held := py.NewEvent()
	release := py.NewEvent()

	holder := func() {
		_, rel, err := o.Atomic()
		if err != nil {
			panic(err)
		}
		defer rel()
		held.Set()
		release.Wait(10)
	}

	th := py.New_Thread(py.Kw{"target": holder})
	check(t, th.Start())
	held.Wait(5)
	if _, rel, err := o.Atomic(py.Kw{"timeout_s": 0.05}); !errors.Is(err, py.TimeoutError) {
		if rel != nil {
			rel()
		}
		t.Fatalf("want TimeoutError, got %v", err)
	}
	assertEq(t, o.Locked(), true)
	release.Set()
	check(t, th.Join(-1))
	assertEq(t, o.Locked(), false)
}

func TestLockMixins_invalid_scope_rejected(t *testing.T) {
	Setup(t)
	o := must(New_Obj14_())
	if _, rel, err := o.Atomic(py.Kw{"scope": "global"}); !errors.Is(err, py.ValueError) {
		if rel != nil {
			rel()
		}
		t.Fatalf("want ValueError, got %v", err)
	}
}

func TestLockMixins_deepcopy_and_pickle_of_locked_object(t *testing.T) {
	Setup(t)
	o := must(New_Obj14_(py.Kw{"counter": 3}))
	var c, p *Obj14_
	func() {
		_, release, err := o.Atomic()
		check(t, err)
		defer release()
		c = py.Deepcopy(o).(*Obj14_)
		blob, err := py.Pickle_dumps(o)
		check(t, err)
		p = must(py.Pickle_loads(blob)).(*Obj14_)
	}()
	assertTrue(t, c.Counter == 3 && p.Counter == 3)
	assertEq(t, c.Global_id(), o.Global_id())
	assertTrue(t, c.Lock(1))
	c.Unlock()
	assertTrue(t, p.Lock(1))
	p.Unlock()
}

func TestLockMixins_many_objects_many_threads(t *testing.T) {
	Setup(t)
	objs := make([]*Obj14_, 64)
	for i := range objs {
		objs[i] = must(New_Obj14_())
	}

	w := func(i int) error {
		rnd := rand.New(rand.NewSource(int64(i)))
		for j := 0; j < N_scaled(3000); j++ {
			o := objs[rnd.Intn(64)]
			_, release, err := o.Atomic()
			if err != nil {
				return err
			}
			o.Bump()
			release()
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	total := 0
	for _, o := range objs {
		total += o.Counter
	}
	assertEq(t, total, 8*N_scaled(3000))
}

func TestLockMixins_global_lock_blocks_other_policy(t *testing.T) {
	Setup(t)
	g := must(New_GObj14_())
	s := must(g.Lock_global("P1"))
	assertTrue(t, s != nil)
	assertTrue(t, must(g.Lock_global("P2")) == nil)
	assertEq(t, *must(g.Lock_global("P1")), *s) // nested hold by the owner
	assertTrue(t, g.Global_locked())
	if _, rel, err := g.Atomic(py.Kw{"scope": "global", "policy_id": "P2"}); !errors.Is(err, py.PermissionError) {
		if rel != nil {
			rel()
		}
		t.Fatalf("want PermissionError, got %v", err)
	}
	g.Unlock_global(py.Ptr("wrong"))
	assertTrue(t, g.Global_locked())
	g.Unlock_global(s) // inner hold
	assertTrue(t, g.Global_locked())
	g.Unlock_global(s) // outer hold
	assertTrue(t, !g.Global_locked())
	func() {
		_, release, err := g.Atomic(py.Kw{"scope": "global", "policy_id": "P2"})
		check(t, err)
		defer release()
		assertTrue(t, g.Global_locked())
	}()
	assertTrue(t, !g.Global_locked())
}

func TestLockMixins_global_lock_gate_rejects_calls_without_secret(t *testing.T) {
	Setup(t)
	g := must(New_GObj14_())
	s := must(g.Lock_global("P1"))
	if _, err := g.Bump(); !errors.Is(err, py.PermissionError) {
		t.Fatalf("want PermissionError, got %v", err)
	}
	g.Unlock_global(s)
	assertEq(t, must(g.Bump()), 1)
}

func TestLockMixins_global_lock_gate_passes_with_secret(t *testing.T) {
	Setup(t)
	g := must(New_GObj14_())
	s := must(g.Lock_global("P1"))
	assertEq(t, must(g.Bump(py.Kw{"secret": *s})), 1)
	if _, err := g.Bump(py.Kw{"secret": "nope"}); !errors.Is(err, py.PermissionError) {
		t.Fatalf("want PermissionError, got %v", err)
	}
	g.Unlock_global(s)
	assertEq(t, must(g.Bump()), 2)
}

func TestLockMixins_global_atomic_block_hides_secret_from_body(t *testing.T) {
	Setup(t)
	g := must(New_GObj14_())
	func() {
		_, release, err := g.Atomic(py.Kw{"scope": "global", "policy_id": "P1"})
		check(t, err)
		defer release()
		// the holder's own block passes the gate without a secret
		assertEq(t, must(g.Bump()), 1)
		assertTrue(t, g.Global_locked())
	}()
	assertTrue(t, !g.Global_locked())
	assertEq(t, must(g.Bump()), 2)
}

func TestLockMixins_global_lock_is_reference_counted(t *testing.T) {
	Setup(t)
	g := must(New_GObj14_())
	s1 := must(g.Lock_global("P1"))
	s2 := must(g.Lock_global("P1"))
	assertEq(t, *s1, *s2)
	g.Unlock_global(s2) // inner release
	assertTrue(t, g.Global_locked())
	g.Unlock_global(s1) // outer release
	assertTrue(t, !g.Global_locked())
	assertTrue(t, must(g.Lock_global("P2")) != nil)
}

func TestLockMixins_global_lock_contention(t *testing.T) {
	Setup(t)
	g := must(New_GObj14_())
	wins := []int{}
	var lock sync.Mutex

	w := func(i int) error {
		for j := 0; j < 500; j++ {
			s, err := g.Lock_global(fmt.Sprintf("P%d", i))
			if err != nil {
				return err
			}
			if s != nil {
				lock.Lock()
				wins = append(wins, i)
				lock.Unlock()
				n, err := g.Bump(py.Kw{"secret": *s})
				if err != nil {
					return err
				}
				if n < 1 {
					return py.AssertionError.Newf("bump returned %d", n)
				}
				g.Unlock_global(s)
			}
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	assertTrue(t, len(wins) > 0)
	assertTrue(t, !g.Global_locked())
	assertEq(t, g.Counter, len(wins))
}
