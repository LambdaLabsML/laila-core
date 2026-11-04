// Port of tests/functional/pools/postgres/unit_tests/test_postgres_pool.py.
//
// ``from laila.data import PostgresPool`` always succeeds in Python (the
// psycopg import inside the module is guarded); the blank driver import
// is ``pip install laila-core[postgres]``. TestPostgresPool needs a DSN
// or the ``initdb`` / ``postgres`` binaries and is skipped otherwise,
// exactly like the Python class; TestPostgresPoolLocalBootstrap patches
// the bootstrap steps and always runs.
package unit_tests

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	_ "github.com/amirzadehlambda/laila/laila-go/drivers/postgres"
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

// HAVE_POSTGRESPOOL: the class import never fails.
const HAVE_POSTGRESPOOL = true

var POSTGRES_DSN = func() *string {
	for _, k := range []string{"LAILA_POSTGRES_DSN", "POSTGRES_DSN"} {
		if v := os.Getenv(k); v != "" {
			return &v
		}
	}
	return nil
}()

var HAVE_INITDB = func() bool {
	_, err := exec.LookPath("initdb")
	return err == nil
}()

func _make_pool(kw ...py.Kw) (*laila.PostgresPool, error) {
	opts := py.MergeKw(kw...)
	if POSTGRES_DSN != nil && !opts.Has("dsn") {
		opts["dsn"] = *POSTGRES_DSN
	}
	return laila.New_PostgresPool(opts)
}

// skipUnlessPostgres applies the class-level skipUnless decorators.
func skipUnlessPostgres(t *testing.T) {
	t.Helper()
	if !HAVE_POSTGRESPOOL {
		t.Skip("PostgresPool import failed")
	}
	if POSTGRES_DSN == nil && !HAVE_INITDB {
		t.Skip("Postgres DSN or initdb/postgres binary required")
	}
}

var (
	shared_once sync.Once
	shared_pool *laila.PostgresPool
	shared_err  error
)

// setUp: ``setUpClass`` builds one shared pool (``nickname=
// "postgres-test-shared"``) and every test empties it first.
func setUp(t *testing.T) *laila.PostgresPool {
	t.Helper()
	skipUnlessPostgres(t)
	shared_once.Do(func() { shared_pool, shared_err = _make_pool(py.Kw{"nickname": "postgres-test-shared"}) })
	if shared_err != nil {
		t.Fatal(shared_err)
	}
	_ = shared_pool.Empty()
	return shared_pool
}

// TestMain is ``tearDownClass``: empty and close the shared pool.
func TestMain(m *testing.M) {
	code := m.Run()
	if shared_pool != nil {
		_ = shared_pool.Empty()
		_ = shared_pool.Close()
	}
	os.Exit(code)
}

func set(t *testing.T, p *laila.PostgresPool, k string, v any) {
	t.Helper()
	if err := p.Setitem(k, v); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, p *laila.PostgresPool, k string) any {
	t.Helper()
	return must(p.Getitem(k))
}

func exists(t *testing.T, p *laila.PostgresPool, k string) bool {
	t.Helper()
	return must(p.Exists(k))
}

func del(t *testing.T, p *laila.PostgresPool, k string) {
	t.Helper()
	if err := p.Delitem(k); err != nil {
		t.Fatal(err)
	}
}

func keys(t *testing.T, p *laila.PostgresPool, kw ...py.Kw) any {
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
	p1 := must(_make_pool(py.Kw{"nickname": "postgres-pool-one"}))
	p2 := must(_make_pool(py.Kw{"nickname": "postgres-pool-two"}))
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
	shared_nick := "persistence-test-postgres"
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
// TestPostgresPoolLocalBootstrap
// ---------------------------------------------------------------------------

// fake_connection mirrors the ``Mock()`` connection whose cursor returns
// no rows (``fetchone() -> None``, ``fetchall() -> []``).
type fake_connection struct {
	close_calls atomic.Int64
}

type fake_rows struct{}

func (fake_rows) Fetchone() ([]any, error)  { return nil, nil }
func (fake_rows) Fetchall() ([][]any, error) { return [][]any{}, nil }

func (c *fake_connection) Execute(sql string, params ...any) (laila.Sql_rows_, error) {
	return fake_rows{}, nil
}
func (c *fake_connection) Commit() error { return nil }
func (c *fake_connection) Close() error  { c.close_calls.Add(1); return nil }

// fake_proc mirrors the ``Mock()`` process (``terminate`` counted).
type fake_proc struct {
	terminate_calls atomic.Int64
}

func (p *fake_proc) Poll() *int                        { code := 0; return &code }
func (p *fake_proc) Wait(timeout float64) (int, error) { return 0, nil }
func (p *fake_proc) Terminate() error                  { p.terminate_calls.Add(1); return nil }
func (p *fake_proc) Kill() error                       { return nil }
func (p *fake_proc) Stdout_read() string               { return "" }
func (p *fake_proc) Stderr_read() string               { return "" }

// bootstrap_patches is ``patch.object(PostgresPool, ...)`` for the four
// bootstrap steps; restored on cleanup.
type bootstrap_patches struct {
	initdb_calls atomic.Int64
	start_calls  atomic.Int64
}

func patch_bootstrap(t *testing.T, available bool, conn laila.Psycopg_connection) *bootstrap_patches {
	t.Helper()
	m := &bootstrap_patches{}
	orig_available := laila.PostgresPool_local_server_available_
	orig_initdb := laila.PostgresPool_initdb_if_needed_
	orig_start := laila.PostgresPool_start_local_server_
	orig_connect := laila.PostgresPool_connect_local_
	laila.PostgresPool_local_server_available_ = func(*laila.PostgresPool) bool { return available }
	laila.PostgresPool_initdb_if_needed_ = func(*laila.PostgresPool) error { m.initdb_calls.Add(1); return nil }
	laila.PostgresPool_start_local_server_ = func(*laila.PostgresPool) error { m.start_calls.Add(1); return nil }
	laila.PostgresPool_connect_local_ = func(*laila.PostgresPool, int) (laila.Psycopg_connection, error) { return conn, nil }
	t.Cleanup(func() {
		laila.PostgresPool_local_server_available_ = orig_available
		laila.PostgresPool_initdb_if_needed_ = orig_initdb
		laila.PostgresPool_start_local_server_ = orig_start
		laila.PostgresPool_connect_local_ = orig_connect
	})
	return m
}

func TestLocalServerReusedWhenAvailable(t *testing.T) {
	fake_conn := &fake_connection{}
	m := patch_bootstrap(t, true, fake_conn)
	pool := must(laila.New_PostgresPool())
	defer pool.Close()
	assertEq(t, m.initdb_calls.Load(), int64(0))
	assertEq(t, m.start_calls.Load(), int64(0))
	expected_pool_dir := filepath.Join(laila.LAILA_DEFAULT_DIRECTORIES["pools"], pool.Uuid())
	assertEq(t, *pool.Socket_dir_, filepath.Join(expected_pool_dir, "socket"))
	assertEq(t, *pool.Postgres_dir_, filepath.Join(expected_pool_dir, "data"))
}

func TestLocalServerStartedWhenMissing(t *testing.T) {
	fake_conn := &fake_connection{}
	m := patch_bootstrap(t, false, fake_conn)
	pool := must(laila.New_PostgresPool())
	defer pool.Close()
	assertEq(t, m.initdb_calls.Load(), int64(1))
	assertEq(t, m.start_calls.Load(), int64(1))
}

// patch_run_command is ``patch.object(PostgresPool, "_run_command")`` +
// ``patch("os.path.exists", return_value=...)``.
func patch_run_command(t *testing.T, path_exists bool) *atomic.Int64 {
	t.Helper()
	calls := &atomic.Int64{}
	orig_exists := laila.Os_path_exists_
	orig_run := laila.PostgresPool_run_command_
	laila.Os_path_exists_ = func(string) bool { return path_exists }
	laila.PostgresPool_run_command_ = func(*laila.PostgresPool, []string, string) error { calls.Add(1); return nil }
	t.Cleanup(func() {
		laila.Os_path_exists_ = orig_exists
		laila.PostgresPool_run_command_ = orig_run
	})
	return calls
}

func TestInitdbSkippedWhenClusterExists(t *testing.T) {
	pool := laila.PostgresPool_class.New().(*laila.PostgresPool) // PostgresPool.__new__(PostgresPool)
	dir := "/tmp/test-postgres-dir"
	pool.Postgres_dir_ = &dir
	run_mock := patch_run_command(t, true)
	if err := laila.PostgresPool_initdb_if_needed_(pool); err != nil {
		t.Fatal(err)
	}
	assertEq(t, run_mock.Load(), int64(0))
}

func TestInitdbRunsWhenClusterMissing(t *testing.T) {
	pool := laila.PostgresPool_class.New().(*laila.PostgresPool)
	dir := "/tmp/test-postgres-dir"
	pool.Postgres_dir_ = &dir
	pool.Local_user_ = "laila"
	run_mock := patch_run_command(t, false)
	if err := laila.PostgresPool_initdb_if_needed_(pool); err != nil {
		t.Fatal(err)
	}
	assertEq(t, run_mock.Load(), int64(1))
}

func TestCloseStopsOwnedLocalServer(t *testing.T) {
	pool := laila.PostgresPool_class.New().(*laila.PostgresPool)
	conn := &fake_connection{}
	proc := &fake_proc{}
	pool.Conn_ = conn
	pool.Postgres_proc_ = proc
	pool.Owns_local_server_ = true
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	assertEq(t, conn.close_calls.Load(), int64(1))
	assertEq(t, proc.terminate_calls.Load(), int64(1))
}
