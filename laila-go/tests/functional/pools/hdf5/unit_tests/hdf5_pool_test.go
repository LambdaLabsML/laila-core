// Port of tests/functional/pools/hdf5/unit_tests/test_hdf5_pool.py.
//
// ``from laila.data import HDF5Pool`` fails in Python without h5py (the
// module imports it unguarded), so the whole class is skipped; here that
// is ``laila.Driver_available("h5py")`` with the blank driver import
// standing in for ``pip install laila-core[hdf5]``.
package unit_tests

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	_ "github.com/amirzadehlambda/laila/laila-go/drivers/hdf5"
	"github.com/amirzadehlambda/laila/laila-go/py"
)


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

func dumps(v any) string { return must(py.Json_dumps(v)) }

func make_pool_(t *testing.T, kw ...py.Kw) *laila.HDF5Pool {
	t.Helper()
	opts := py.MergeKw(kw...)
	if !opts.Has("file_path") {
		tmp := must(os.MkdirTemp("", "laila_hdf5_test_"))
		opts["file_path"] = filepath.Join(tmp, "pool.h5py")
	}
	return must(laila.New_HDF5Pool(opts))
}

// HAVE_HDF5POOL is ``from laila.data import HDF5Pool`` succeeding.
var HAVE_HDF5POOL = laila.Driver_available("h5py")

func skipUnlessHDF5(t *testing.T) {
	t.Helper()
	if !HAVE_HDF5POOL {
		t.Skip("HDF5Pool import failed")
	}
}

// make_pool_ creates an HDF5Pool with a temp file_path to avoid the
// active_policy dependency.

// setUp / tearDown of TestHDF5Pool.
func setUp(t *testing.T) *laila.HDF5Pool {
	t.Helper()
	skipUnlessHDF5(t)
	pool := make_pool_(t)
	t.Cleanup(func() {
		_ = pool.Close()
		if fp := pool.File_path; fp != nil {
			if _, err := os.Stat(*fp); err == nil {
				_ = os.Remove(*fp)
				if d := filepath.Dir(*fp); strings.Contains(d, "laila_hdf5_test_") {
					_ = os.Remove(d)
				}
			}
		}
	})
	return pool
}

func set(t *testing.T, p *laila.HDF5Pool, k string, v any) {
	t.Helper()
	if err := p.Setitem(k, v); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, p *laila.HDF5Pool, k string) any {
	t.Helper()
	return must(p.Getitem(k))
}

func exists(t *testing.T, p *laila.HDF5Pool, k string) bool {
	t.Helper()
	return must(p.Exists(k))
}

func del(t *testing.T, p *laila.HDF5Pool, k string) {
	t.Helper()
	if err := p.Delitem(k); err != nil {
		t.Fatal(err)
	}
}

func keys(t *testing.T, p *laila.HDF5Pool, kw ...py.Kw) any {
	t.Helper()
	return must(p.Keys(kw...))
}

func TestPoolIdFormat(t *testing.T) {
	pool := setUp(t)
	if !strings.HasPrefix(pool.Pool_id(), "LAILA:POOL:") {
		t.Fatalf("pool_id = %s", pool.Pool_id())
	}
}

func TestFilePathIsSet(t *testing.T) {
	pool := setUp(t)
	if pool.File_path == nil || !strings.HasSuffix(*pool.File_path, ".h5py") {
		t.Fatalf("file_path = %v", pool.File_path)
	}
}

func TestGetMissingReturnsNone(t *testing.T) {
	pool := setUp(t)
	assertEq(t, get(t, pool, "missing"), nil)
	if exists(t, pool, "missing") {
		t.Fatal("missing")
	}
}

func TestSetAndGetRoundtripString(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "a", dumps("123"))
	assertEq(t, get(t, pool, "a"), "123")
}

func TestSetNonStringRaisesTypeError(t *testing.T) {
	pool := setUp(t)
	if err := pool.Setitem("bad", 123); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

func TestSetNoneRaisesTypeError(t *testing.T) {
	pool := setUp(t)
	if err := pool.Setitem("none", nil); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

func TestDelitemSilentIfMissing(t *testing.T) {
	pool := setUp(t)
	del(t, pool, "ghost")
	if exists(t, pool, "ghost") {
		t.Fatal("ghost")
	}
}

func TestDelitemRemovesExisting(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "x", dumps("val"))
	if !exists(t, pool, "x") {
		t.Fatal("x")
	}
	del(t, pool, "x")
	if exists(t, pool, "x") {
		t.Fatal("x still exists")
	}
	assertEq(t, get(t, pool, "x"), nil)
}

func TestExistsTrueWhenKeyPresent(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "present", dumps(7))
	if !exists(t, pool, "present") || !must(pool.Contains("present")) {
		t.Fatal("present")
	}
}

func TestKeysSnapshotReturnsList(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "a", dumps(1))
	set(t, pool, "b", dumps(2))
	ks := keys(t, pool, py.Kw{"as_generator": false})
	if _, ok := ks.(*py.List); !ok {
		t.Fatalf("keys type %T", ks)
	}
	assertCountEqual(t, ks, "a", "b")
}

func TestKeysGeneratorYieldsAllCurrentKeys(t *testing.T) {
	pool := setUp(t)
	want := []any{}
	for i := 0; i < 10; i++ {
		set(t, pool, fmt.Sprintf("k%d", i), dumps(i))
		want = append(want, fmt.Sprintf("k%d", i))
	}
	it, ok := keys(t, pool, py.Kw{"as_generator": true}).(py.Iterator)
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

func TestKeysSnapshotIsNotAffectedByLaterMutations(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "a", dumps(1))
	snap := keys(t, pool)
	set(t, pool, "b", dumps(2))
	assertCountEqual(t, snap, "a")
}

func TestOverwriteValue(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "k", dumps("v1"))
	set(t, pool, "k", dumps("v2"))
	assertEq(t, get(t, pool, "k"), "v2")
}

func TestMultipleInstancesHaveIndependentFiles(t *testing.T) {
	skipUnlessHDF5(t)
	tmp1 := must(os.MkdirTemp("", "laila_hdf5_test_"))
	tmp2 := must(os.MkdirTemp("", "laila_hdf5_test_"))
	p1 := must(laila.New_HDF5Pool(py.Kw{"file_path": filepath.Join(tmp1, "p1.h5py")}))
	p2 := must(laila.New_HDF5Pool(py.Kw{"file_path": filepath.Join(tmp2, "p2.h5py")}))
	defer func() {
		_ = p1.Close()
		_ = p2.Close()
		_ = os.RemoveAll(tmp1)
		_ = os.RemoveAll(tmp2)
	}()
	set(t, p1, "x", dumps(1))
	set(t, p2, "x", dumps(2))
	assertEq(t, get(t, p1, "x"), 1)
	assertEq(t, get(t, p2, "x"), 2)
}

func TestConcurrentWritesNoLoss(t *testing.T) {
	pool := setUp(t)
	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = pool.Setitem(fmt.Sprint(i), dumps(i))
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		assertEq(t, get(t, pool, fmt.Sprint(i)), i)
	}
}

func TestConcurrentWritesAndReads(t *testing.T) {
	pool := setUp(t)
	stop := make(chan struct{})
	var mu sync.Mutex
	read_errors := []string{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = pool.Setitem(fmt.Sprint(i), dumps(i))
			i = (i + 1) % 10
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, k := range py.Iter(must(pool.Keys())) {
				v := must(pool.Getitem(k))
				if v == nil {
					continue
				}
				if py.Pytypename(v) != "int" { // isinstance(v, int)
					mu.Lock()
					read_errors = append(read_errors, "bad value "+py.Repr(v))
					mu.Unlock()
				}
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

func TestStoreEntryPayloadAsObject(t *testing.T) {
	pool := setUp(t)
	e := must(laila.Constant(py.DictOf("x", 1)))
	payload := py.DictOf("global_id", e.Global_id(), "data", must(e.Data()))
	set(t, pool, e.Global_id(), payload)
	assertEq(t, get(t, pool, e.Global_id()), payload)
}

func TestCloseIsIdempotent(t *testing.T) {
	pool := setUp(t)
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteThenRecreate(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "z", dumps(9))
	del(t, pool, "z")
	if exists(t, pool, "z") {
		t.Fatal("z")
	}
	set(t, pool, "z", dumps(10))
	assertEq(t, get(t, pool, "z"), 10)
}

func TestAtomicReentrantSameThread(t *testing.T) {
	pool := setUp(t)
	_, r1, err := pool.Atomic()
	if err != nil {
		t.Fatal(err)
	}
	set(t, pool, "r", dumps(1))
	_, r2, err := pool.Atomic()
	if err != nil {
		t.Fatal(err)
	}
	set(t, pool, "r", dumps(2))
	r2()
	r1()
	assertEq(t, get(t, pool, "r"), 2)
}

func TestAtomicThreadSafetyForReadModifyWrite(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "counter", dumps(0))
	n_threads, n_steps := 10, 50
	var wg sync.WaitGroup
	for i := 0; i < n_threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < n_steps; j++ {
				func() {
					_, release, err := pool.Atomic()
					if err != nil {
						panic(err)
					}
					defer release()
					current := must(py.Int(must(pool.Getitem("counter"))))
					_ = pool.Setitem("counter", dumps(current+1))
				}()
			}
		}()
	}
	wg.Wait()
	assertEq(t, must(py.Int(get(t, pool, "counter"))), n_threads*n_steps)
}

func TestCustomFilePathUsed(t *testing.T) {
	skipUnlessHDF5(t)
	tmp := must(os.MkdirTemp("", "laila_hdf5_test_"))
	defer os.RemoveAll(tmp)
	path := filepath.Join(tmp, "custom.h5py")
	p := must(laila.New_HDF5Pool(py.Kw{"file_path": path}))
	defer p.Close()
	assertEq(t, *p.File_path, path)
	set(t, p, "k", dumps(1))
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	assertEq(t, get(t, p, "k"), 1)
}

func TestPersistenceAcrossPoolInstances(t *testing.T) {
	skipUnlessHDF5(t)
	tmp := must(os.MkdirTemp("", "laila_hdf5_test_"))
	defer os.RemoveAll(tmp)
	path := filepath.Join(tmp, "persist.h5py")
	p1 := must(laila.New_HDF5Pool(py.Kw{"file_path": path}))
	set(t, p1, "persistent", dumps(py.DictOf("a", 1)))
	if err := p1.Close(); err != nil {
		t.Fatal(err)
	}

	p2 := must(laila.New_HDF5Pool(py.Kw{"file_path": path}))
	defer p2.Close()
	assertEq(t, get(t, p2, "persistent"), py.DictOf("a", 1))
}

func TestKeyWithSpecialCharsRoundtrip(t *testing.T) {
	pool := setUp(t)
	key := "key/with:special=chars"
	set(t, pool, key, dumps("value"))
	if !exists(t, pool, key) {
		t.Fatal("key")
	}
	assertEq(t, get(t, pool, key), "value")
	del(t, pool, key)
	if exists(t, pool, key) {
		t.Fatal("key still exists")
	}
}
