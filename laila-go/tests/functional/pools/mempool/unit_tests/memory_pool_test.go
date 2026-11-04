// Port of tests/functional/pools/mempool/unit_tests/test_memory_pool.py.
package unit_tests

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

type MemoryPool = laila.LAILA_IDENTIFIABLE_POOL_

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func assertEq(t *testing.T, got, want any) {
	t.Helper()
	if !py.Eq(got, want) {
		t.Fatalf("got %s, want %s", py.Repr(got), py.Repr(want))
	}
}

func assertCountEqual(t *testing.T, got any, want ...any) {
	t.Helper()
	g := must(py.Sorted(py.Iter(got)))
	w := must(py.Sorted(want))
	if !py.Eq(g, w) {
		t.Fatalf("got %s, want %s", py.Repr(g), py.Repr(w))
	}
}

func newPool(t *testing.T) *MemoryPool {
	t.Helper()
	return must(laila.New_LAILA_IDENTIFIABLE_POOL_())
}

func set(t *testing.T, p *MemoryPool, k string, v any) {
	t.Helper()
	if err := p.Setitem(k, v); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, p *MemoryPool, k string) any {
	t.Helper()
	return must(p.Getitem(k))
}

func exists(t *testing.T, p *MemoryPool, k string) bool {
	t.Helper()
	return must(p.Exists(k))
}

func del(t *testing.T, p *MemoryPool, k string) {
	t.Helper()
	if err := p.Delitem(k); err != nil {
		t.Fatal(err)
	}
}

func keys(t *testing.T, p *MemoryPool, as_generator bool) any {
	t.Helper()
	return must(p.Keys(py.Kw{"as_generator": as_generator}))
}

func strs(n, m int) []any {
	out := []any{}
	for i := n; i < m; i++ {
		out = append(out, fmt.Sprint(i))
	}
	return out
}

// 1
func TestPoolIdFormat(t *testing.T) {
	p := newPool(t)
	if !strings.HasPrefix(p.Pool_id(), "LAILA:POOL:") {
		t.Fatalf("pool_id = %s", p.Pool_id())
	}
}

// 2
func TestResourceStartsEmpty(t *testing.T) {
	p := newPool(t)
	assertEq(t, p.Resource, py.NewDict())
	if exists(t, p, "missing") {
		t.Fatal("missing should not exist")
	}
}

// 3
func TestSetAndGetRoundtrip(t *testing.T) {
	p := newPool(t)
	set(t, p, "a", 123)
	assertEq(t, get(t, p, "a"), 123)
}

// 4
func TestGetMissingReturnsNone(t *testing.T) {
	p := newPool(t)
	assertEq(t, get(t, p, "nope"), nil)
}

// 5
func TestSetNoneAndGetReturnsNoneButExistsTrue(t *testing.T) {
	p := newPool(t)
	set(t, p, "n", nil)
	if !exists(t, p, "n") {
		t.Fatal("n should exist")
	}
	assertEq(t, get(t, p, "n"), nil)
}

// 6
func TestDelitemSilentIfMissing(t *testing.T) {
	p := newPool(t)
	del(t, p, "ghost") // should not raise
	if exists(t, p, "ghost") {
		t.Fatal("ghost")
	}
}

// 7
func TestDelitemRemovesExisting(t *testing.T) {
	p := newPool(t)
	set(t, p, "x", "val")
	if !exists(t, p, "x") {
		t.Fatal("x")
	}
	del(t, p, "x")
	if exists(t, p, "x") {
		t.Fatal("x still exists")
	}
	assertEq(t, get(t, p, "x"), nil)
}

// 8
func TestStoreEntryByGlobalId(t *testing.T) {
	p := newPool(t)
	e := must(laila.Constant(42))
	set(t, p, e.Global_id(), e)
	if get(t, p, e.Global_id()) != any(e) {
		t.Fatal("stored entry identity")
	}
}

// 9
func TestGetitemRoundtrip(t *testing.T) {
	p := newPool(t)
	set(t, p, "k", "v")
	assertEq(t, get(t, p, "k"), "v")
	assertEq(t, get(t, p, "missing"), nil)
}

// 10
func TestExistsTrueWhenKeyPresent(t *testing.T) {
	p := newPool(t)
	set(t, p, "present", 7)
	if !exists(t, p, "present") {
		t.Fatal("present")
	}
}

// 11
func TestExistsFalseWhenKeyAbsent(t *testing.T) {
	p := newPool(t)
	if exists(t, p, "absent") {
		t.Fatal("absent")
	}
}

// 12
func TestKeysSnapshotReturnsList(t *testing.T) {
	p := newPool(t)
	set(t, p, "a", 1)
	set(t, p, "b", 2)
	ks := keys(t, p, false)
	if _, ok := ks.(*py.List); !ok {
		t.Fatalf("keys type %T", ks)
	}
	assertCountEqual(t, ks, "a", "b")
}

// 13
func TestKeysSnapshotIsNotAffectedByLaterMutations(t *testing.T) {
	p := newPool(t)
	set(t, p, "a", 1)
	snap := must(p.Keys()) // default False
	set(t, p, "b", 2)
	assertCountEqual(t, snap, "a")
}

// 14
func TestKeysGeneratorYieldsAllCurrentKeys(t *testing.T) {
	p := newPool(t)
	want := []any{}
	for i := 0; i < 10; i++ {
		set(t, p, fmt.Sprintf("k%d", i), i)
		want = append(want, fmt.Sprintf("k%d", i))
	}
	it, ok := keys(t, p, true).(py.Iterator)
	if !ok {
		t.Fatal("expected an iterator")
	}
	got := []any{}
	for k, err := range it {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, k)
	}
	assertCountEqual(t, got, want...)
}

// 15
func TestKeysGeneratorHoldsLockDuringIterationSameThreadMutationRaises(t *testing.T) {
	// Mutating dict while iterating should raise RuntimeError (dict behavior)
	p := newPool(t)
	for i := 0; i < 5; i++ {
		set(t, p, fmt.Sprint(i), i)
	}
	it := keys(t, p, true).(py.Iterator)
	var got error
	for _, err := range it {
		if err != nil {
			got = err
			break
		}
		// __setitem__ acquires the same RLock reentrantly; underlying dict iteration still errors
		set(t, p, "99", 99)
	}
	if !errors.Is(got, py.RuntimeError) {
		t.Fatalf("want RuntimeError, got %v", got)
	}
}

// 16
func TestReentrancyOfAtomic(t *testing.T) {
	// Acquire atomic and call a method that also acquires it: must not deadlock
	p := newPool(t)
	_, release, err := p.Atomic()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	set(t, p, "r", 1) // __setitem__ acquires again (RLock), should be fine
	assertEq(t, get(t, p, "r"), 1)
}

// 17
func TestMultipleInstancesHaveIndependentResources(t *testing.T) {
	p1, p2 := newPool(t), newPool(t)
	set(t, p1, "x", 1)
	set(t, p2, "x", 2)
	assertEq(t, get(t, p1, "x"), 1)
	assertEq(t, get(t, p2, "x"), 2)
}

// 18
func TestOverwriteValue(t *testing.T) {
	p := newPool(t)
	set(t, p, "k", "v1")
	set(t, p, "k", "v2")
	assertEq(t, get(t, p, "k"), "v2")
}

// 19
func TestTypesAnyValue(t *testing.T) {
	p := newPool(t)
	complex_value := py.DictOf("a", py.NewList(1, 2, py.DictOf("b", "c")))
	set(t, p, "complex", complex_value)
	assertEq(t, get(t, p, "complex"), complex_value)
}

// 20
func TestConcurrentWritesNoLoss(t *testing.T) {
	p := newPool(t)
	const N = 50
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = p.Setitem(fmt.Sprint(i), i)
		}(i)
	}
	wg.Wait()
	// ensure all keys present
	for i := 0; i < N; i++ {
		assertEq(t, get(t, p, fmt.Sprint(i)), i)
	}
}

// 21
func TestConcurrentWritesAndReads(t *testing.T) {
	p := newPool(t)
	stop := make(chan struct{})
	var mu sync.Mutex
	read_errors := []string{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // writer
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = p.Setitem(fmt.Sprint(i), i)
			i = (i + 1) % 10 // overwrite hot set
		}
	}()
	go func() { // reader
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// take a snapshot to avoid long-lived lock in the test
			for _, k := range py.Iter(must(p.Keys())) {
				v := must(p.Getitem(k))
				if v == nil {
					continue
				}
				if py.Pytypename(v) == "int" { // isinstance(v, int)
					continue
				}
				if laila.As_entry_(v) != nil {
					continue
				}
				mu.Lock()
				read_errors = append(read_errors, "bad value "+py.Repr(v))
				mu.Unlock()
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	if len(read_errors) != 0 {
		t.Fatalf("read_errors = %v", read_errors)
	}
}

// 22
func TestKeysGeneratorPartialConsumptionThenClose(t *testing.T) {
	p := newPool(t)
	for i := 0; i < 5; i++ {
		set(t, p, fmt.Sprint(i), i)
	}
	it := keys(t, p, true).(py.Iterator)
	// consume one element, then close the generator explicitly
	var first any
	for k, err := range it {
		if err != nil {
			t.Fatal(err)
		}
		first = k
		break
	}
	if !py.NewList(strs(0, 5)...).Contains(first) {
		t.Fatalf("first = %v", first)
	}
	// the lock must have been released by the close
	set(t, p, "after", 1)
}

// 23
func TestStoreEntryAndExists(t *testing.T) {
	p := newPool(t)
	e := must(laila.Constant(py.DictOf("x", 1)))
	set(t, p, e.Global_id(), e)
	if !exists(t, p, e.Global_id()) {
		t.Fatal("entry should exist")
	}
}

// 24
func TestGetitemMissing(t *testing.T) {
	p := newPool(t)
	assertEq(t, get(t, p, "no-such"), nil)
}

// 25
func TestDeleteThenRecreate(t *testing.T) {
	p := newPool(t)
	set(t, p, "z", 9)
	del(t, p, "z")
	if exists(t, p, "z") {
		t.Fatal("z")
	}
	set(t, p, "z", 10)
	assertEq(t, get(t, p, "z"), 10)
}

// 26
func TestKeysSnapshotTypeAndContentsAfterManyMutations(t *testing.T) {
	p := newPool(t)
	for i := 0; i < 20; i++ {
		set(t, p, fmt.Sprint(i), i)
	}
	for i := 0; i < 10; i++ {
		del(t, p, fmt.Sprint(i))
	}
	snap := must(p.Keys())
	if _, ok := snap.(*py.List); !ok {
		t.Fatalf("keys type %T", snap)
	}
	assertCountEqual(t, snap, strs(10, 20)...)
}

// 27
func TestAtomicContextManagerUsableDirectly(t *testing.T) {
	p := newPool(t)
	func() {
		_, release, err := p.Atomic()
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		p.Resource.Set("inside", true)
	}()
	if !exists(t, p, "inside") {
		t.Fatal("inside")
	}
	if get(t, p, "inside") != true {
		t.Fatal("inside is not True")
	}
}

// 28
func TestGeneratorExhaustionAllowsFutureWrites(t *testing.T) {
	p := newPool(t)
	for i := 0; i < 5; i++ {
		set(t, p, fmt.Sprint(i), i)
	}
	for range keys(t, p, true).(py.Iterator) { // exhaust
	}
	// Now a write should proceed fine
	set(t, p, "new", "ok")
	assertEq(t, get(t, p, "new"), "ok")
}

// 29
func TestManyConcurrentStoreEntry(t *testing.T) {
	p := newPool(t)
	entries := []*laila.Entry{}
	for i := 0; i < 50; i++ {
		entries = append(entries, must(laila.Constant(fmt.Sprintf("load-%d", i))))
	}
	var wg sync.WaitGroup
	for _, e := range entries {
		wg.Add(1)
		go func(e *laila.Entry) {
			defer wg.Done()
			_ = p.Setitem(e.Global_id(), e)
		}(e)
	}
	wg.Wait()
	for _, e := range entries {
		if laila.As_entry_(get(t, p, e.Global_id())) == nil {
			t.Fatalf("%s: not an Entry", e.Global_id())
		}
	}
}

// 30
func TestExistsAfterSetNone(t *testing.T) {
	p := newPool(t)
	set(t, p, "maybe", nil)
	if !exists(t, p, "maybe") {
		t.Fatal("maybe")
	}
	assertEq(t, get(t, p, "maybe"), nil)
}
