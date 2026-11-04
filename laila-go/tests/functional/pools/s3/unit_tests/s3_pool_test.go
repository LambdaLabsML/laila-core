// Port of tests/functional/pools/s3/unit_tests/test_s3_pool.py.
//
// “from laila.data import S3Pool“ always succeeds in Python (the SDK
// import inside the module is guarded); the blank driver import is
// “pip install laila-core[s3]“. S3 pool tests. Requires existing bucket laila-test-1.
// The class is skipped without credentials, exactly like the Python
// “skipUnless“ decorators.
package unit_tests

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	_ "github.com/amirzadehlambda/laila/laila-go/drivers/s3"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// HAVE_S3POOL: the class import never fails.
const HAVE_S3POOL = true

var SECRETS_PATH = filepath.Join(must(os.UserHomeDir()), ".laila", "secrets", "s3_test.toml")

// _secrets / HAVE_DEV_SECRETS mirror the module-level try/except: read
// the TOML into laila.args when present and require the provider's keys.
var _secrets, HAVE_DEV_SECRETS = func() (*py.DotMap, bool) {
	if _, err := os.Stat(SECRETS_PATH); err != nil {
		return nil, false
	}
	if err := laila.Read_args(SECRETS_PATH); err != nil {
		return nil, false
	}
	s := laila.Args
	return s, true
}()

// _make_pool creates S3Pool. Uses “~/.laila/secrets/s3_test.toml“ for
// credentials.
func _make_pool(kw ...py.Kw) (*laila.S3Pool, error) {
	opts := py.MergeKw(kw...)
	if HAVE_DEV_SECRETS && _secrets != nil {
		bucket := opts.Pop("bucket_name", "laila-test-1")
		return laila.New_S3Pool(py.Kw{
			"bucket_name":       bucket,
			"access_key_id":     _secrets.Get("AWS_ACCESS_KEY"),
			"secret_access_key": _secrets.Get("AWS_SECRET_ACCESS_KEY"),
			"region_name":       _secrets.Get("AWS_REGION"),
		}, opts)
	}
	if !opts.Has("bucket_name") {
		opts["bucket_name"] = "laila-test-1"
	}
	return laila.New_S3Pool(opts)
}

// skipUnlessS3Pool applies the class-level skipUnless decorators.
func skipUnlessS3Pool(t *testing.T) {
	t.Helper()
	if !HAVE_S3POOL {
		t.Skip("S3Pool import failed")
	}
	if !(HAVE_DEV_SECRETS) {
		t.Skip("dev_secrets.toml required for S3 tests")
	}
}

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

// setUp is “setUp“ + “tearDown“: a fresh pool per test, emptied
// (errors swallowed) and closed afterwards.
func setUp(t *testing.T) *laila.S3Pool {
	t.Helper()
	skipUnlessS3Pool(t)
	pool := must(_make_pool())
	t.Cleanup(func() {
		_ = pool.Empty()
		_ = pool.Close()
	})
	return pool
}

func set(t *testing.T, p *laila.S3Pool, k string, v any) {
	t.Helper()
	if err := p.Setitem(k, v); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, p *laila.S3Pool, k string) any {
	t.Helper()
	return must(p.Getitem(k))
}

func exists(t *testing.T, p *laila.S3Pool, k string) bool {
	t.Helper()
	return must(p.Exists(k))
}

func del(t *testing.T, p *laila.S3Pool, k string) {
	t.Helper()
	if err := p.Delitem(k); err != nil {
		t.Fatal(err)
	}
}

func keys(t *testing.T, p *laila.S3Pool, kw ...py.Kw) any {
	t.Helper()
	return must(p.Keys(kw...))
}

func TestPoolIdFormat(t *testing.T) {
	pool := setUp(t)
	if !strings.HasPrefix(pool.Pool_id(), "LAILA:POOL:") {
		t.Fatalf("pool_id = %s", pool.Pool_id())
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

func TestEmptyRemovesAll(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "a", dumps(1))
	set(t, pool, "b", dumps(2))
	if err := pool.Empty(); err != nil {
		t.Fatal(err)
	}
	assertEq(t, get(t, pool, "a"), nil)
	assertEq(t, get(t, pool, "b"), nil)
	assertEq(t, keys(t, pool), py.NewList())
}

func TestMultipleInstancesShareBucketNamespace(t *testing.T) {
	setUp(t)
	p1 := must(_make_pool(py.Kw{"bucket_name": "laila-test-1"}))
	p2 := must(_make_pool(py.Kw{"bucket_name": "laila-test-1"}))
	if p1.Pool_id() == p2.Pool_id() {
		t.Fatal("pool ids equal")
	}
	defer func() {
		_ = p1.Empty()
		_ = p1.Close()
		_ = p2.Close()
	}()
	set(t, p1, "shared_key", dumps(42))
	assertEq(t, get(t, p2, "shared_key"), 42)
}
func TestConcurrentWritesNoLoss(t *testing.T) {
	pool := setUp(t)
	const n = 10
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
	n_threads, n_steps := 3, 5
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

func TestPersistenceAcrossPoolInstances(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "persistent", dumps(py.DictOf("a", 1)))
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}

	p2 := must(_make_pool(py.Kw{"bucket_name": pool.Bucket_name}))
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
