// Port of tests/functional/pools/redispool/unit_tests/test_redis_pool.py.
//
// ``from laila.data import RedisPool`` always succeeds in Python (the
// redis import inside the module is guarded); the blank driver import is
// ``pip install laila-core[redis]``. Every RedisPool starts its own
// private ``redis-server`` process, so the whole class is skipped when
// the binary is not on PATH, exactly like the Python ``skipUnless``.
package unit_tests

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	_ "github.com/amirzadehlambda/laila/laila-go/drivers/redis"
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

// HAVE_REDISPOOL: the class import never fails.
const HAVE_REDISPOOL = true

var HAVE_REDIS_SERVER = func() bool {
	_, err := exec.LookPath("redis-server")
	return err == nil
}()

// skipUnlessRedis applies the class-level skipUnless decorators.
func skipUnlessRedis(t *testing.T) {
	t.Helper()
	if !HAVE_REDISPOOL {
		t.Skip("RedisPool import failed")
	}
	if !HAVE_REDIS_SERVER {
		t.Skip("redis-server not found")
	}
}

var (
	shared_once sync.Once
	shared_pool *laila.RedisPool
	shared_err  error
)

// setUp is ``setUpClass`` + ``setUp``: one shared pool, emptied per test.
func setUp(t *testing.T) *laila.RedisPool {
	t.Helper()
	skipUnlessRedis(t)
	shared_once.Do(func() {
		shared_pool, shared_err = laila.New_RedisPool(py.Kw{"nickname": "redis-test-shared"})
	})
	if shared_err != nil {
		t.Fatal(shared_err)
	}
	if err := shared_pool.Empty(); err != nil {
		t.Fatal(err)
	}
	return shared_pool
}

// TestMain is ``tearDownClass``: close the shared pool.
func TestMain(m *testing.M) {
	code := m.Run()
	if shared_pool != nil {
		_ = shared_pool.Close()
	}
	os.Exit(code)
}

func set(t *testing.T, p *laila.RedisPool, k string, v any) {
	t.Helper()
	if err := p.Setitem(k, v); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, p *laila.RedisPool, k string) any {
	t.Helper()
	return must(p.Getitem(k))
}

func exists(t *testing.T, p *laila.RedisPool, k string) bool {
	t.Helper()
	return must(p.Exists(k))
}

func del(t *testing.T, p *laila.RedisPool, k string) {
	t.Helper()
	if err := p.Delitem(k); err != nil {
		t.Fatal(err)
	}
}

func keys(t *testing.T, p *laila.RedisPool, kw ...py.Kw) any {
	t.Helper()
	return must(p.Keys(kw...))
}

// 1
func TestPoolIdFormat(t *testing.T) {
	pool := setUp(t)
	if !strings.HasPrefix(pool.Pool_id(), "LAILA:POOL:") {
		t.Fatalf("pool_id = %s", pool.Pool_id())
	}
}

// 2
func TestClientIsInitialized(t *testing.T) {
	pool := setUp(t)
	if pool.Client_ == nil {
		t.Fatal("_client is None")
	}
}

// 3
func TestGetMissingReturnsNone(t *testing.T) {
	pool := setUp(t)
	assertEq(t, get(t, pool, "missing"), nil)
	if exists(t, pool, "missing") {
		t.Fatal("missing")
	}
}

// 4
func TestSetAndGetRoundtripString(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "a", dumps("123"))
	assertEq(t, get(t, pool, "a"), "123")
}

// 5
func TestSetNonStringRaisesTypeError(t *testing.T) {
	pool := setUp(t)
	if err := pool.Setitem("bad", 123); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

// 6
func TestSetNoneRaisesTypeError(t *testing.T) {
	pool := setUp(t)
	if err := pool.Setitem("none", nil); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

// 7
func TestDelitemSilentIfMissing(t *testing.T) {
	pool := setUp(t)
	del(t, pool, "ghost")
	if exists(t, pool, "ghost") {
		t.Fatal("ghost")
	}
}

// 8
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

// 9
func TestExistsTrueWhenKeyPresent(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "present", dumps(7))
	if !exists(t, pool, "present") || !must(pool.Contains("present")) {
		t.Fatal("present")
	}
}

// 10
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

// 11
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

// 12
func TestKeysSnapshotIsNotAffectedByLaterMutations(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "a", dumps(1))
	snap := keys(t, pool)
	set(t, pool, "b", dumps(2))
	assertCountEqual(t, snap, "a")
}

// 13
func TestOverwriteValue(t *testing.T) {
	pool := setUp(t)
	set(t, pool, "k", dumps("v1"))
	set(t, pool, "k", dumps("v2"))
	assertEq(t, get(t, pool, "k"), "v2")
}

// 14
func TestMultipleInstancesHaveIndependentServers(t *testing.T) {
	setUp(t)
	p1 := must(laila.New_RedisPool())
	p2 := must(laila.New_RedisPool())
	defer func() {
		_ = p1.Close()
		_ = p2.Close()
	}()
	set(t, p1, "x", dumps(1))
	set(t, p2, "x", dumps(2))
	assertEq(t, get(t, p1, "x"), 1)
	assertEq(t, get(t, p2, "x"), 2)
}

// 15
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

// 16
func TestConcurrentWritesAndReads(t *testing.T) {
	pool := setUp(t)
	stop := py.NewEvent()
	var mu sync.Mutex
	read_errors := []string{}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // writer
		defer wg.Done()
		i := 0
		for !stop.Is_set() {
			_ = pool.Setitem(fmt.Sprint(i), dumps(i))
			i = (i + 1) % 10
		}
	}()
	go func() { // reader
		defer wg.Done()
		for !stop.Is_set() {
			ks, err := pool.Keys()
			if err != nil {
				continue
			}
			for k, err := range py.Iter(ks) {
				if err != nil {
					break
				}
				v, _ := pool.Getitem(py.Str(k))
				if v != nil {
					if _, ok := v.(int); !ok {
						mu.Lock()
						read_errors = append(read_errors, fmt.Sprintf("bad value %s", py.Repr(v)))
						mu.Unlock()
					}
				}
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	stop.Set()
	wg.Wait()
	if len(read_errors) != 0 {
		t.Fatalf("read_errors = %v", read_errors)
	}
}

// 17
func TestStoreEntryPayloadAsObject(t *testing.T) {
	pool := setUp(t)
	e := must(laila.Constant(py.DictOf("x", 1)))
	payload := py.DictOf("global_id", e.Global_id(), "data", must(e.Data()))
	set(t, pool, e.Global_id(), payload)
	assertEq(t, get(t, pool, e.Global_id()), payload)
}

// 18
func TestCloseIsIdempotent(t *testing.T) {
	setUp(t)
	p := must(laila.New_RedisPool())
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

// 19
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

// 20
func TestCustomKeyPrefixAffectsRedisHashKey(t *testing.T) {
	setUp(t)
	p := must(laila.New_RedisPool(py.Kw{"key_prefix": "custom_pool"}))
	defer p.Close()
	assertEq(t, p.Redis_hash_key(), "custom_pool")
}

// 21
func TestCustomLockPrefixAffectsRedisLockKey(t *testing.T) {
	setUp(t)
	p := must(laila.New_RedisPool(py.Kw{"lock_prefix": "custom_lock"}))
	defer p.Close()
	assertEq(t, p.Redis_lock_key(), "custom_lock")
}

// 22
func TestRedisPasswordParameterRoundtrip(t *testing.T) {
	setUp(t)
	p := must(laila.New_RedisPool(py.Kw{"redis_password": "test-pass"}))
	defer p.Close()
	if p.Redis_password == nil {
		t.Fatal("redis_password is None")
	}
	assertEq(t, *p.Redis_password, "test-pass")
	// If initialization succeeded, auth + ping path worked.
	if p.Client_ == nil {
		t.Fatal("_client is None")
	}
}

// 23
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

// 24
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

// 25
func TestAtomicTimeoutWhenLockHeldByOtherThread(t *testing.T) {
	pool := setUp(t)
	ready := py.NewEvent()
	release := py.NewEvent()
	errs := make(chan error, 1)

	done := make(chan struct{})
	go func() { // holder
		defer close(done)
		_, rel, err := pool.Atomic()
		if err != nil {
			errs <- err
			return
		}
		defer rel()
		ready.Set()
		release.Wait(1.0)
	}()
	ready.Wait(1.0)

	_, rel, err := pool.Atomic(py.Kw{"timeout_s": 0.05})
	if rel != nil {
		rel()
	}
	if !errors.Is(err, py.TimeoutError) {
		t.Fatalf("want TimeoutError, got %v", err)
	}

	release.Set()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("holder did not finish")
	}
	select {
	case e := <-errs:
		t.Fatalf("errors = [%v]", e)
	default:
	}
}
