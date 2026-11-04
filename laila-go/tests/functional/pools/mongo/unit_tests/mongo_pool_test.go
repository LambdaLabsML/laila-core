// Port of tests/functional/pools/mongo/unit_tests/test_mongo_pool.py.
//
// “from laila.data import MongoPool“ always succeeds in Python (the
// pymongo import inside the module is guarded); the blank driver import
// is “pip install laila-core[mongo]“. TestMongoPool needs a URI or the
// “mongod“ binary and is skipped otherwise, exactly like the Python
// class; TestMongoPoolLocalBootstrap patches the bootstrap steps (and
// “laila.data.mongo.mongo.MongoClient“ through the driver registry) and
// always runs.
package unit_tests

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	_ "github.com/amirzadehlambda/laila/laila-go/drivers/mongo"
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

// HAVE_MONGOPOOL: the class import never fails.
const HAVE_MONGOPOOL = true

var MONGO_URI = func() *string {
	for _, k := range []string{"LAILA_MONGO_URI", "MONGO_URI"} {
		if v := os.Getenv(k); v != "" {
			return &v
		}
	}
	return nil
}()

var HAVE_MONGOD = func() bool {
	_, err := exec.LookPath("mongod")
	return err == nil
}()

func _make_pool(kw ...py.Kw) (*laila.MongoPool, error) {
	opts := py.MergeKw(kw...)
	if MONGO_URI != nil && !opts.Has("uri") {
		opts["uri"] = *MONGO_URI
	}
	return laila.New_MongoPool(opts)
}

// skipUnlessMongo applies the class-level skipUnless decorators.
func skipUnlessMongo(t *testing.T) {
	t.Helper()
	if !HAVE_MONGOPOOL {
		t.Skip("MongoPool import failed")
	}
	if MONGO_URI == nil && !HAVE_MONGOD {
		t.Skip("Mongo URI or mongod binary required for MongoPool tests")
	}
}

var (
	shared_once sync.Once
	shared_pool *laila.MongoPool
	shared_err  error
)

// setUp: “setUpClass“ builds one shared pool (“nickname=
// "mongo-test-shared"“) and every test empties it first.
func setUp(t *testing.T) *laila.MongoPool {
	t.Helper()
	skipUnlessMongo(t)
	shared_once.Do(func() { shared_pool, shared_err = _make_pool(py.Kw{"nickname": "mongo-test-shared"}) })
	if shared_err != nil {
		t.Fatal(shared_err)
	}
	_ = shared_pool.Empty()
	return shared_pool
}

// TestMain is “tearDownClass“: empty and close the shared pool.
func TestMain(m *testing.M) {
	code := m.Run()
	if shared_pool != nil {
		_ = shared_pool.Empty()
		_ = shared_pool.Close()
	}
	os.Exit(code)
}

func set(t *testing.T, p *laila.MongoPool, k string, v any) {
	t.Helper()
	if err := p.Setitem(k, v); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, p *laila.MongoPool, k string) any {
	t.Helper()
	return must(p.Getitem(k))
}

func exists(t *testing.T, p *laila.MongoPool, k string) bool {
	t.Helper()
	return must(p.Exists(k))
}

func del(t *testing.T, p *laila.MongoPool, k string) {
	t.Helper()
	if err := p.Delitem(k); err != nil {
		t.Fatal(err)
	}
}

func keys(t *testing.T, p *laila.MongoPool, kw ...py.Kw) any {
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

func TestMultipleInstancesHaveIndependentServers(t *testing.T) {
	setUp(t)
	p1 := must(_make_pool(py.Kw{"nickname": "mongo-indep-one"}))
	p2 := must(_make_pool(py.Kw{"nickname": "mongo-indep-two"}))
	defer func() {
		_ = p1.Empty()
		_ = p2.Empty()
		_ = p1.Close()
		_ = p2.Close()
	}()
	set(t, p1, "x", dumps(1))
	set(t, p2, "x", dumps(2))
	assertEq(t, get(t, p1, "x"), 1)
	assertEq(t, get(t, p2, "x"), 2)
}

func TestConcurrentWritesNoLoss(t *testing.T) {
	pool := setUp(t)
	const n = 20
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
	setUp(t)
	p := must(_make_pool())
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
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
	setUp(t)
	shared_nick := "persistence-test-mongo"
	p1 := must(_make_pool(py.Kw{"nickname": shared_nick}))
	set(t, p1, "persistent", dumps(py.DictOf("a", 1)))
	if err := p1.Close(); err != nil {
		t.Fatal(err)
	}

	p2 := must(_make_pool(py.Kw{"nickname": shared_nick}))
	defer func() {
		_ = p2.Empty()
		_ = p2.Close()
	}()
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

// ---------------------------------------------------------------------------
// TestMongoPoolLocalBootstrap
// ---------------------------------------------------------------------------

// fake_client mirrors the “Mock()“ client (“client[db][collection]
// .create_index“ is a no-op, “close“ counted).
type fake_client struct {
	close_calls atomic.Int64
}

type fake_collection struct{}

func (fake_collection) Create_index(key string, unique bool) error { return nil }
func (fake_collection) Find_one(filter *py.Dict, projection *py.Dict) (*py.Dict, error) {
	return nil, nil
}
func (fake_collection) Update_one(filter *py.Dict, update *py.Dict, upsert bool) error { return nil }
func (fake_collection) Delete_one(filter *py.Dict) error                               { return nil }
func (fake_collection) Delete_many(filter *py.Dict) error                              { return nil }
func (fake_collection) Count_documents(filter *py.Dict, limit int) (int, error)        { return 0, nil }
func (fake_collection) Find_sorted(filter *py.Dict, projection *py.Dict, sort_key string, direction int) ([]*py.Dict, error) {
	return nil, nil
}

func (c *fake_client) Collection(dbname string, name string) laila.Pymongo_collection {
	return fake_collection{}
}
func (c *fake_client) Admin_command_ping() error { return nil }
func (c *fake_client) Close() error              { c.close_calls.Add(1); return nil }

// fake_driver is “patch("laila.data.mongo.mongo.MongoClient",
// return_value=fake_client)“: the “pymongo“ module global swapped for
// the duration of the test.
type fake_driver struct{ client *fake_client }

func (d fake_driver) MongoClient_uri(uri string, ms int) (laila.Pymongo_client, error) {
	return d.client, nil
}
func (d fake_driver) MongoClient_host(host string, port int, ms int) (laila.Pymongo_client, error) {
	return d.client, nil
}

// fake_proc mirrors the “Mock()“ process (“terminate“ counted).
type fake_proc struct {
	terminate_calls atomic.Int64
}

func (p *fake_proc) Poll() *int                        { code := 0; return &code }
func (p *fake_proc) Wait(timeout float64) (int, error) { return 0, nil }
func (p *fake_proc) Terminate() error                  { p.terminate_calls.Add(1); return nil }
func (p *fake_proc) Kill() error                       { return nil }
func (p *fake_proc) Stdout_read() string               { return "" }
func (p *fake_proc) Stderr_read() string               { return "" }

func patch_bootstrap(t *testing.T, available bool, patch_start bool, client *fake_client) *atomic.Int64 {
	t.Helper()
	start_calls := &atomic.Int64{}
	orig_available := laila.MongoPool_local_server_available_
	orig_start := laila.MongoPool_start_local_server_
	orig_driver := laila.Driver_("pymongo")
	laila.MongoPool_local_server_available_ = func(*laila.MongoPool) bool { return available }
	if patch_start {
		laila.MongoPool_start_local_server_ = func(*laila.MongoPool) error { start_calls.Add(1); return nil }
	}
	laila.Register_driver("pymongo", fake_driver{client: client})
	t.Cleanup(func() {
		laila.MongoPool_local_server_available_ = orig_available
		laila.MongoPool_start_local_server_ = orig_start
		if orig_driver != nil {
			laila.Register_driver("pymongo", orig_driver)
		} else {
			laila.Unregister_driver("pymongo")
		}
	})
	return start_calls
}

func TestLocalServerReusedWhenAvailable(t *testing.T) {
	fake := &fake_client{}
	patch_bootstrap(t, true, false, fake)
	pool := must(laila.New_MongoPool())
	defer pool.Close()
	if !strings.HasPrefix(*pool.Mongo_dir_, laila.LAILA_DEFAULT_DIRECTORIES["pools"]) {
		t.Fatalf("_mongo_dir = %s", *pool.Mongo_dir_)
	}
	assertEq(t, *pool.Host, "127.0.0.1")
}

func TestLocalServerStartedWhenMissing(t *testing.T) {
	fake := &fake_client{}
	start_mock := patch_bootstrap(t, false, true, fake)
	pool := must(laila.New_MongoPool())
	defer pool.Close()
	assertEq(t, start_mock.Load(), int64(1))
}

func TestCloseStopsOwnedLocalServer(t *testing.T) {
	pool := laila.MongoPool_class.New().(*laila.MongoPool) // MongoPool.__new__(MongoPool)
	client := &fake_client{}
	proc := &fake_proc{}
	pool.Client_ = client
	pool.Mongo_proc_ = proc
	pool.Owns_local_server_ = true
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	assertEq(t, client.close_calls.Load(), int64(1))
	assertEq(t, proc.terminate_calls.Load(), int64(1))
}
