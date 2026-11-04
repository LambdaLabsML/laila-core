//go:build workload

// Port of tests/workloads/test_w23_fault_injection.py.
//
// W23 -- fault injection.
//
// Failing / slow storage backends under the memory API, corrupted and
// read-only persistent files, failing builders, BaseException-raising tasks,
// hostile bytes on transport ports, half-open connections, encryption key
// rotation mid-run and failing origins behind a cache proxy. The invariant
// under test is always: errors surface as exceptions, nothing hangs, and
// the component keeps serving afterwards.
package workloads

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T23_ = 120.0

// InjectedFailure is ``class InjectedFailure(OSError)``.
var InjectedFailure = py.NewType("InjectedFailure", py.OSError)

// FlakyPool is an in-memory pool whose storage hooks fail on a schedule.
type FlakyPool struct {
	laila.LAILA_IDENTIFIABLE_POOL_

	Fail_every_write int     `py:"fail_every_write" default:"0"`
	Fail_every_read  int     `py:"fail_every_read" default:"0"`
	Read_delay       float64 `py:"read_delay" default:"0.0"`

	Nw_   int        `py:"-"` // _nw: PrivateAttr(default=0)
	Nr_   int        `py:"-"` // _nr: PrivateAttr(default=0)
	Lock_ sync.Mutex `py:"-"` // _lock: PrivateAttr(default_factory=threading.Lock)
}

var FlakyPool_class = py.NewClass("FlakyPool", (*FlakyPool)(nil), laila.LAILA_IDENTIFIABLE_POOL_class)

// New_FlakyPool is ``FlakyPool(**kw)``.
func New_FlakyPool(kw ...py.Kw) (*FlakyPool, error) {
	p := FlakyPool_class.New().(*FlakyPool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *FlakyPool) Write_(key string, value any) error {
	p.Lock_.Lock()
	p.Nw_++
	n := p.Nw_
	p.Lock_.Unlock()
	if p.Fail_every_write != 0 && n%p.Fail_every_write == 0 {
		return InjectedFailure.Newf("injected write failure #%d", n)
	}
	return p.LAILA_IDENTIFIABLE_POOL_.Write_(key, value)
}

func (p *FlakyPool) Read_(key string) (any, error) {
	p.Lock_.Lock()
	p.Nr_++
	n := p.Nr_
	p.Lock_.Unlock()
	if p.Read_delay != 0 {
		py.Sleep_sync(p.Read_delay)
	}
	if p.Fail_every_read != 0 && n%p.Fail_every_read == 0 {
		return nil, InjectedFailure.Newf("injected read failure #%d", n)
	}
	return p.LAILA_IDENTIFIABLE_POOL_.Read_(key)
}

// w23_outcomes is ``_outcomes(futs, timeout=_T)``: wait on every future,
// splitting results from exceptions. A verb that failed *synchronously*
// (an ``error`` in place of a future) counts as an exception too.
func w23_outcomes(futs []any, timeout ...float64) (ok []any, bad []error) {
	to := T23_
	if len(timeout) > 0 {
		to = timeout[0]
	}
	for _, f := range futs {
		if err, is_err := f.(error); is_err {
			bad = append(bad, err)
			continue
		}
		v, err := Wait(f, to)
		if err != nil {
			bad = append(bad, err)
		} else {
			ok = append(ok, v)
		}
	}
	return ok, bad
}

// w23_fut collapses ``(future, err)`` into one slot for w23_outcomes.
func w23_fut(f any, err error) any {
	if err != nil {
		return err
	}
	return f
}

// w23_remember is ``laila.remember(ref, pool_nickname=nick, persist=False)``.
func w23_remember(ref any, nick string) (any, error) {
	return laila.Remember(ref, py.Kw{"pool_nickname": nick, "persist": false})
}

// w23_memorize is ``laila.memorize(x, pool_nickname=nick)``.
func w23_memorize(x any, nick string) (any, error) {
	return laila.Memorize(x, py.Kw{"pool_nickname": nick})
}

// w23_wait_err is ``with pytest.raises(Exception): fn().wait(_T)`` -- the
// error raised either by the verb or by its future.
func w23_wait_err(fn func() (any, error), timeout ...float64) error {
	to := T23_
	if len(timeout) > 0 {
		to = timeout[0]
	}
	f, err := fn()
	if err != nil {
		return err
	}
	_, err = Wait(f, to)
	return err
}

// w23_submit is ``laila.command.submit(tasks)``.
func w23_submit(tasks []any) (any, error) {
	cmd, err := laila.Command()
	if err != nil {
		return nil, err
	}
	return cmd.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_).Submit(tasks)
}


// w23_remote_roundtrip is the "the server still serves" probe shared by
// the hostile-transport tests: memorize *value* into B's "store" and read
// it back through A.
func w23_remote_roundtrip(t *testing.T, tp *TwoPolicies, value int) {
	t.Helper()
	e := must(laila.Entry_constant(value))
	must(Wait(must(laila.Memorize(e, py.Kw{"dst_policy": tp.Bid, "dst_pool": "store"})), T23_))
	got := must(Wait(must(laila.Remember(e.Global_id(), py.Kw{"dst_policy": tp.Bid, "dst_pool": "store", "persist": false})), T23_))
	assertEq(t, Unwrap(got), value)
}

// ---------------------------------------------------------------------------
// flaky storage
// ---------------------------------------------------------------------------

func TestFlakyStorage_write_failures_surface_and_retry_converges(t *testing.T) {
	policy := Fresh_policy(t)
	pool := must(New_FlakyPool(py.Kw{"fail_every_write": 3}))
	mem_extend(t, policy, pool, "flaky")
	n := N_scaled(300, 60)
	es, ids := constants(t, n, func(i int) any { return i })
	futs := make([]any, n)
	for i, e := range es {
		futs[i] = w23_fut(w23_memorize(e, "flaky"))
	}
	ok, bad := w23_outcomes(futs)
	assertTrue(t, len(bad) > 0, "expected injected failures")
	for _, b := range bad {
		assertTrue(t, errors.Is(b, InjectedFailure), bad[:min(2, len(bad))])
	}
	assertEq(t, len(ok)+len(bad), n)
	Release_all(futs...)
	// the index must not advertise keys whose write failed
	stored := 0
	for _, e := range es {
		if must(pool.Contains(e.Global_id())) {
			stored++
		}
	}
	assertEq(t, stored, len(ok))
	// retry the missing ones until every entry is stored (no hang, bounded attempts)
	missing := []*laila.Entry{}
	for _, e := range es {
		if !must(pool.Contains(e.Global_id())) {
			missing = append(missing, e)
		}
	}
	for attempt := 0; attempt < 10; attempt++ {
		if len(missing) == 0 {
			break
		}
		fs := make([]any, len(missing))
		for i, e := range missing {
			fs[i] = w23_fut(w23_memorize(e, "flaky"))
		}
		w23_outcomes(fs)
		Release_all(fs...)
		still := []*laila.Entry{}
		for _, e := range missing {
			if !must(pool.Contains(e.Global_id())) {
				still = append(still, e)
			}
		}
		missing = still
	}
	assertTrue(t, len(missing) == 0, len(missing))
	got := must(Wait(must(w23_remember(ids, "flaky")), T23_))
	assertEq(t, w22_sorted_ints(got, ""), Range(n))
	Quiesce(policy)
}

func TestFlakyStorage_failed_write_leaves_no_phantom_key(t *testing.T) {
	policy := Fresh_policy(t)
	pool := must(New_FlakyPool(py.Kw{"fail_every_write": 2}))
	mem_extend(t, policy, pool, "flaky")
	es, _ := constants(t, N_scaled(100, 20), func(i int) any { return i })
	futs := make([]any, len(es))
	for i, e := range es {
		futs[i] = w23_fut(w23_memorize(e, "flaky"))
	}
	_, bad := w23_outcomes(futs)
	Release_all(futs...)
	assertTrue(t, len(bad) > 0)
	listed := py.NewSet()
	for _, k := range pkeys(t, laila.As_pool_(pool)).Items() {
		listed.Add(k)
	}
	present := py.NewSet()
	for _, e := range es {
		if must(pool.Contains(e.Global_id())) {
			present.Add(e.Global_id())
		}
	}
	phantom := 0
	for _, k := range listed.Items() {
		if !present.Contains(k) {
			phantom++
		}
	}
	assertTrue(t, py.Eq(listed, present), fmt.Sprintf("%d phantom keys listed but unreadable", phantom))
	// the index also must not resolve a never-stored variable
	v := must(laila.Variable(py.Kw{"data": 1}))
	pool2 := must(New_FlakyPool(py.Kw{"fail_every_write": 1}))
	mem_extend(t, policy, pool2, "always-fails")
	assertTrue(t, w23_wait_err(func() (any, error) { return w23_memorize(v, "always-fails") }) != nil)
	assertTrue(t, w23_wait_err(func() (any, error) {
		return w23_remember(fmt.Sprintf("%s@evolution=-1", strings.SplitN(v.Global_id(), "@", 2)[0]), "always-fails")
	}) != nil)
	Quiesce(policy)
}

func TestFlakyStorage_read_failures_do_not_poison_future_reads(t *testing.T) {
	policy := Fresh_policy(t)
	pool := must(New_FlakyPool(py.Kw{"fail_every_read": 4}))
	mem_extend(t, policy, pool, "flaky")
	n := N_scaled(200, 40)
	es, _ := constants(t, n, func(i int) any { return i })
	must(Wait(must(w23_memorize(entries_list(es), "flaky")), T23_))
	failures := 0
	for _, e := range es {
		succeeded := false
		for attempt := 0; attempt < 6; attempt++ {
			got, err := Wait(must(w23_remember(e.Global_id(), "flaky")), T23_)
			if err != nil {
				if errors.Is(err, InjectedFailure) {
					failures++
					continue
				}
				t.Fatal(err)
			}
			assertEq(t, Unwrap(got), must(e.Data()))
			succeeded = true
			break
		}
		if !succeeded {
			t.Fatal("read never succeeded after 6 attempts")
		}
	}
	assertTrue(t, failures > 0)
	Quiesce(policy)
}

// A sync ``_read`` blocks the event loop it runs on; 1 loop x 2 slots and
// 50 concurrent remembers must still finish (serially), never deadlock.
func TestFlakyStorage_slow_sync_backend_under_starved_taskforce_no_deadlock(t *testing.T) {
	policy := Fresh_policy(t)
	pool := must(New_FlakyPool(py.Kw{"read_delay": 0.02}))
	mem_extend(t, policy, pool, "slow")
	es, _ := constants(t, N_scaled(50, 10), func(i int) any { return i })
	must(Wait(must(w23_memorize(entries_list(es), "slow")), T23_))
	cmd := Cmd(policy)
	saved_alpha, saved_internal := cmd.Alpha_taskforce, cmd.Internal_taskforce
	tiny := must(laila.New_PythonAsyncThreadPoolTaskForce(py.Kw{
		"policy_id":            policy.Global_id(),
		"num_workers":          1,
		"max_async_per_thread": 2,
		"sync_workers":         2,
		"rank":                 1,
	}))
	cmd.Taskforces.Set(tiny.Global_id(), tiny)
	cmd.Alpha_taskforce, cmd.Internal_taskforce = tiny.Global_id(), tiny.Global_id()
	defer func() {
		cmd.Alpha_taskforce, cmd.Internal_taskforce = saved_alpha, saved_internal
		_ = tiny.Shutdown(py.Kw{"wait": true, "cancel_pending": true})
		cmd.Taskforces.PopDefault(tiny.Global_id(), nil)
	}()
	var ok []any
	var bad []error
	var futs []any
	sw := Timed(func() {
		for _, e := range es {
			futs = append(futs, w23_fut(w23_remember(e.Global_id(), "slow")))
		}
		ok, bad = w23_outcomes(futs)
	})
	assertTrue(t, len(bad) == 0, bad)
	assertEq(t, w22_sorted_ints(ok, ""), Range(len(es)))
	assertTrue(t, sw.Elapsed < 60)
	Release_all(futs...)
}

func TestFlakyStorage_failing_origin_behind_cache_proxy(t *testing.T) {
	Fresh_policy(t)
	origin := must(New_FlakyPool(py.Kw{"fail_every_read": 2, "index_enabled": false}))
	cache := default_pool(t, py.Kw{"index_enabled": false})
	must(cache.Lshift(origin)) // cache << origin
	for i := 0; i < 100; i++ {
		pset(t, laila.As_pool_(origin), fmt.Sprintf("k%d", i), py.DictOf("i", i))
	}
	n_errors, hits := 0, 0
	for i := 0; i < 100; i++ {
		v, err := cache.Getitem(fmt.Sprintf("k%d", i))
		if err != nil {
			if !errors.Is(err, InjectedFailure) {
				t.Fatal(err)
			}
			n_errors++
			continue
		}
		assertEq(t, v, py.DictOf("i", i))
		hits++
	}
	assertTrue(t, n_errors > 0 && hits > 0)
	// a failed read-through must not have cached a None/poison
	for i := 0; i < 100; i++ {
		if must(cache.Contains(fmt.Sprintf("k%d", i))) {
			assertEq(t, pget(t, cache, fmt.Sprintf("k%d", i)), py.DictOf("i", i))
		}
	}
	// keep retrying: every key is eventually served and cached (bounded attempts)
	for attempt := 0; attempt < 20; attempt++ {
		if pkeys(t, cache).Len() == 100 {
			break
		}
		for i := 0; i < 100; i++ {
			if must(cache.Contains(fmt.Sprintf("k%d", i))) {
				continue
			}
			v, err := cache.Getitem(fmt.Sprintf("k%d", i))
			if err != nil {
				if !errors.Is(err, InjectedFailure) {
					t.Fatal(err)
				}
				continue
			}
			assertEq(t, v, py.DictOf("i", i))
		}
	}
	assertEq(t, pkeys(t, cache).Len(), 100)
}

// ---------------------------------------------------------------------------
// persistent file faults
// ---------------------------------------------------------------------------

func TestPersistentFileFaults_corrupted_row_is_isolated(t *testing.T) {
	policy := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	pool := must(Make_pool("sqlite", disk_tmp))
	mem_extend(t, policy, pool, "db")
	es, _ := constants(t, 50, func(i int) any { return i })
	defer func() {
		Quiesce(policy)
		Close_pool(pool)
	}()
	must(Wait(must(w23_memorize(entries_list(es), "db")), T23_))
	victim := es[7].Global_id()
	conn := must(pool.(*laila.SQLitePool).Connection_())
	must(conn.Execute("UPDATE laila_pool_entries SET value = ? WHERE key = ?", "not json {{{", victim))
	check(t, conn.Commit())
	assertTrue(t, w23_wait_err(func() (any, error) { return w23_remember(victim, "db") }) != nil,
		"expected the corrupted row to raise")
	// every other key still reads, and the pool accepts new writes
	others := py.NewList()
	for _, e := range es {
		if e.Global_id() != victim {
			others.Append(e.Global_id())
		}
	}
	got := must(Wait(must(w23_remember(others, "db")), T23_)).(*py.List)
	assertEq(t, got.Len(), 49)
	e := must(laila.Entry_constant("after-corruption"))
	must(Wait(must(w23_memorize(e, "db")), T23_))
	assertEq(t, Unwrap(must(Wait(must(w23_remember(e.Global_id(), "db")), T23_))), "after-corruption")
	// overwrite the corrupted row heals it
	must(Wait(must(w23_memorize(es[7], "db")), T23_))
	assertEq(t, Unwrap(must(Wait(must(w23_remember(victim, "db")), T23_))), 7)
}

func TestPersistentFileFaults_read_only_file_fails_writes_clearly_reads_ok(t *testing.T) {
	policy := Fresh_policy(t)
	Skip_unless(t, os.Geteuid() != 0, "root ignores file modes")
	disk_tmp := Disk_tmp(t)
	pool := must(Make_pool("sqlite", disk_tmp))
	path := file_path(pool)
	es, ids := constants(t, 20, func(i int) any { return i })
	mem_extend(t, policy, pool, "db")
	must(Wait(must(w23_memorize(entries_list(es), "db")), T23_))
	Quiesce(policy)
	Close_pool(pool)
	check(t, os.Chmod(path, 0o444))
	ro := must(laila.New_SQLitePool(py.Kw{"file_path": path}))
	mem_extend(t, policy, ro, "ro")
	defer func() {
		Quiesce(policy)
		Close_pool(ro)
		_ = os.Chmod(path, 0o644)
	}()
	got := must(Wait(must(w23_remember(ids, "ro")), T23_))
	assertEq(t, w22_sorted_ints(got, ""), Range(20))
	err := w23_wait_err(func() (any, error) { return w23_memorize(must(laila.Entry_constant("x")), "ro") })
	assertTrue(t, err != nil, "expected the write to a read-only file to raise")
	msg := strings.ToLower(err.Error())
	assertTrue(t, strings.Contains(msg, "readonly") || strings.Contains(msg, "read-only") ||
		strings.Contains(msg, "permission") || err != nil)
	// reads still fine after the failed write
	got = must(Wait(must(w23_remember(es[0].Global_id(), "ro")), T23_))
	assertEq(t, Unwrap(got), 0)
}

func TestPersistentFileFaults_file_unlinked_under_open_pool(t *testing.T) {
	policy := Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	pool := must(Make_pool("sqlite", disk_tmp))
	path := file_path(pool)
	mem_extend(t, policy, pool, "db")
	func() {
		defer func() {
			Quiesce(policy)
			Close_pool(pool)
		}()
		es, _ := constants(t, 20, func(i int) any { return i })
		must(Wait(must(w23_memorize(entries_list(es), "db")), T23_))
		check(t, os.Remove(path))
		// sqlite refuses writes on an unlinked database file: the error must
		// surface as an exception, reads keep working on the open inode
		err := w23_wait_err(func() (any, error) { return w23_memorize(must(laila.Entry_constant("late")), "db") })
		assertTrue(t, err != nil, "expected the write to an unlinked file to raise")
		assertTrue(t, strings.Contains(strings.ToLower(err.Error()), "readonly") || err != nil)
		got := must(Wait(must(w23_remember(es[0].Global_id(), "db")), T23_))
		assertEq(t, Unwrap(got), 0)
	}()
	_, stat_err := os.Stat(path)
	assertTrue(t, os.IsNotExist(stat_err))
}

// ---------------------------------------------------------------------------
// failing tasks
// ---------------------------------------------------------------------------

const BOOM23_ = "def boom(manifest):\n    raise ValueError('builder failed on purpose')\n"
const OK23_ = "def ok(manifest):\n    return sum(e.data for e in manifest.realized.values())\n"

func init() {
	laila.Register_constitution(BOOM23_, func(arg any) (any, error) {
		return nil, py.ValueError.New("builder failed on purpose")
	})
	laila.Register_constitution(OK23_, func(arg any) (any, error) {
		d, err := laila.As_manifest_(arg).Realized()
		if err != nil {
			return nil, err
		}
		var total any = 0
		for _, e := range d.(*py.Dict).Values() {
			v, err := laila.As_entry_(e).Data()
			if err != nil {
				return nil, err
			}
			if total, err = py.Add(total, v); err != nil {
				return nil, err
			}
		}
		return total, nil
	})
}

func TestFailingTasks_hundred_failing_builders_leave_taskforce_healthy(t *testing.T) {
	policy := Fresh_policy(t)
	data := py.NewDict()
	for i := 0; i < 20; i++ {
		data.Set(fmt.Sprintf("k%d", i), must(laila.Entry_constant(i)))
	}
	m := must(laila.New_Manifest(py.Kw{"data": data}))
	must(must(m.Memorize()).Wait(T23_))
	bad := make([]*laila.Entry, N_scaled(100, 20))
	for i := range bad {
		bad[i] = must(laila.Variable(py.Kw{"constitution": BOOM23_, "manifest": m}))
	}
	futs := make([]any, len(bad))
	for i, e := range bad {
		futs[i] = w23_fut(laila.Build(e))
	}
	ok, errs := w23_outcomes(futs)
	assertTrue(t, len(ok) == 0 && len(errs) == len(bad), fmt.Sprint(len(ok), len(errs)))
	for _, x := range errs {
		assertTrue(t, strings.Contains(x.Error(), "on purpose"), x)
	}
	for _, e := range bad {
		assertTrue(t, e.State() != laila.EntryState_READY)
	}
	Release_all(futs...)
	good := make([]*laila.Entry, 20)
	for i := range good {
		good[i] = must(laila.Variable(py.Kw{"constitution": OK23_, "manifest": m}))
	}
	futs = make([]any, len(good))
	for i, e := range good {
		futs[i] = w23_fut(laila.Build(e))
	}
	_, errs = w23_outcomes(futs)
	assertTrue(t, len(errs) == 0, errs)
	for _, e := range good {
		assertEq(t, must(e.Data()), sum_range(20))
	}
	Release_all(futs...)
	Quiesce(policy)
}

// Fatal23_ is ``class Fatal(BaseException)``.
var Fatal23_ = py.NewType("Fatal", py.BaseException)

func TestFailingTasks_base_exception_in_task_does_not_kill_loop(t *testing.T) {
	Fresh_policy(t)
	fatal := py.CoroutineFunc(func(_ ...any) (any, error) { return nil, Fatal23_.New("non-Exception escape") })
	fine := py.CoroutineFunc(func(_ ...any) (any, error) { return 1, nil })
	f := must(w23_submit([]any{fatal}))
	_, err := Wait(f, 30)
	assertTrue(t, err != nil, "expected the BaseException to surface")
	// the loop thread must have survived: subsequent tasks run
	var ok []any
	var errs []error
	var fs []any
	sw := Timed(func() {
		for i := 0; i < 50; i++ {
			fs = append(fs, w23_fut(w23_submit([]any{fine})))
		}
		ok, errs = w23_outcomes(fs, 30)
	})
	assertTrue(t, len(errs) == 0 && len(ok) == 50, fmt.Sprint(len(ok), errs))
	assertTrue(t, sw.Elapsed < 30)
	Release_all(append([]any{f}, fs...)...)
}

func TestFailingTasks_exception_storm_from_many_threads(t *testing.T) {
	policy := Fresh_policy(t)
	boom := func(i int) py.CoroutineFunc {
		return func(_ ...any) (any, error) { return nil, py.RuntimeError.Newf("boom-%d", i) }
	}
	per := N_scaled(100, 20)
	worker := func(th int) (any, error) {
		n_err := 0
		for i := 0; i < per; i++ {
			coro := boom(i)
			f, err := w23_submit([]any{func() (any, error) { return py.Coro(coro), nil }}) // lambda i=i: boom(i)
			if err != nil {
				return nil, err
			}
			if _, err := Wait(f, 30); err != nil {
				if !errors.Is(err, py.RuntimeError) {
					return nil, err
				}
				if err.Error() != fmt.Sprintf("boom-%d", i) {
					return nil, py.AssertionError.Newf("%q != boom-%d", err.Error(), i)
				}
				n_err++
			}
			Release_all(f)
		}
		return n_err, nil
	}
	res := must(Run_threads(worker, 6))
	for _, r := range res {
		assertEq(t, r, per)
	}
	assertTrue(t, policy.Future_bank.Len() < 50, policy.Future_bank.Len())
}

// ---------------------------------------------------------------------------
// hostile transport
// ---------------------------------------------------------------------------

// w23_garbage is ``_garbage(port, payloads)``: one short-lived connection
// per payload, each sent raw.
func w23_garbage(t *testing.T, port int, payloads [][]byte) {
	t.Helper()
	for _, p := range payloads {
		s, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
		check(t, err)
		func() {
			defer s.Close()
			_, _ = s.Write(p)
			time.Sleep(50 * time.Millisecond)
		}()
	}
}

// w23_urandom is ``os.urandom(n)``.
func w23_urandom(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func TestHostileTransport_garbage_bytes_do_not_kill_server(t *testing.T) {
	for _, kind := range []string{"tcp", "ws"} {
		kind := kind
		t.Run("kind="+kind, func(t *testing.T) {
			Fresh_policy(t)
			tp := must(New_TwoPolicies(kind, py.Kw{"liveness_interval": 60.0}).Enter())
			defer tp.Exit()
			must(tp.Extend_b("store"))
			port := Bound_port(tp.Pb)
			payloads := [][]byte{
				bytes.Repeat([]byte{0x00}, 10),
				[]byte("GET / HTTP/1.1\r\n\r\n"),
				w23_urandom(4096),
				bytes.Repeat([]byte("{"), 1000),
				bytes.Repeat([]byte{0xff, 0xfe}, 2000),
			}
			w23_garbage(t, port, payloads)
			// half-open connections that never handshake
			socks := []net.Conn{}
			for i := 0; i < 20; i++ {
				s, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
				check(t, err)
				socks = append(socks, s)
			}
			func() {
				defer func() {
					for _, s := range socks {
						s.Close()
					}
				}()
				w23_remote_roundtrip(t, tp, 1)
				assertTrue(t, tp.Pa.Ping(tp.Bid, 10))
			}()
			Quiesce(tp.A)
			Quiesce(tp.B)
		})
	}
}

func TestHostileTransport_udp_garbage_datagrams(t *testing.T) {
	Fresh_policy(t)
	tp := must(New_TwoPolicies("udp", py.Kw{"liveness_interval": 60.0}).Enter())
	defer tp.Exit()
	must(tp.Extend_b("store"))
	port := Bound_port(tp.Pb)
	s, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", port))
	check(t, err)
	func() {
		defer s.Close()
		for i := 0; i < 500; i++ {
			_, _ = s.Write(w23_urandom(200))
		}
		_, _ = s.Write(append([]byte{0x01}, bytes.Repeat([]byte{0x00}, 1000)...))
	}()
	w23_remote_roundtrip(t, tp, 2)
	Quiesce(tp.A)
	Quiesce(tp.B)
}

func TestHostileTransport_connect_storm_wrong_secret(t *testing.T) {
	Fresh_policy(t)
	tp := must(New_TwoPolicies("tcp", py.Kw{"liveness_interval": 60.0}).Enter())
	defer tp.Exit()
	must(tp.Extend_b("store"))
	uri := must(tp.Uri_for(tp.B, tp.Pb))
	attackers := []*laila.LAILA_IDENTIFIABLE_POLICY_{}
	func() {
		defer func() {
			for _, p := range attackers {
				_ = Comm(p).Stop()
				_ = Cmd(p).Shutdown(py.Kw{"wait": true, "cancel_pending": true})
				laila.Local_policies_.PopDefault(p.Global_id(), nil)
			}
		}()
		for i := 0; i < N_scaled(30, 10); i++ {
			p := As_policy(must(laila.New_DefaultPolicy()))
			Comm(p).Liveness_interval = 60.0
			check(t, Comm(p).Add_connection(must(laila.New_DefaultTCPProtocol(py.Kw{"host": "127.0.0.1"}))))
			attackers = append(attackers, p)
			_, err := Comm(p).Add_peer(uri, fmt.Sprintf("wrong-%d", i))
			assertTrue(t, err != nil, "expected the wrong secret to be rejected")
		}
		assertEq(t, tp.Cb().Peers.Len(), 1)
		w23_remote_roundtrip(t, tp, 3)
	}()
	Quiesce(tp.A)
	Quiesce(tp.B)
}

// ---------------------------------------------------------------------------
// key rotation
// ---------------------------------------------------------------------------

// F-W23-01 (fixed): FernetEncryption() follows laila.encryption_key live.
func TestKeyRotation_rotate_encryption_key_mid_run(t *testing.T) {
	policy := Fresh_policy(t)
	k1, k2 := py.Fernet_generate_key(), py.Fernet_generate_key()
	laila.Set_encryption_key(k1)
	pool := must(laila.New_DefaultPool(py.Kw{"transformations": must(laila.New_TransformationSequence(py.Kw{
		"transformations": []any{must(laila.New_Base64()), must(laila.New_FernetEncryption())}}))}))
	mem_extend(t, policy, pool, "enc")
	defer func() {
		laila.Set_encryption_key(nil)
		Quiesce(policy)
	}()
	a, _ := constants(t, 50, func(i int) any { return i })
	must(Wait(must(w23_memorize(entries_list(a), "enc")), T23_))
	laila.Set_encryption_key(k2)
	b, _ := constants(t, 50, func(i int) any { return 100 + i })
	must(Wait(must(w23_memorize(entries_list(b), "enc")), T23_))
	// under k2: b readable, a not
	assertEq(t, Unwrap(must(Wait(must(w23_remember(b[0].Global_id(), "enc")), T23_))), 100)
	assertTrue(t, w23_wait_err(func() (any, error) { return w23_remember(a[0].Global_id(), "enc") }) != nil)
	laila.Set_encryption_key(k1)
	assertEq(t, Unwrap(must(Wait(must(w23_remember(a[0].Global_id(), "enc")), T23_))), 0)
	assertTrue(t, w23_wait_err(func() (any, error) { return w23_remember(b[0].Global_id(), "enc") }) != nil)
	laila.Set_encryption_key(nil)
	assertTrue(t, w23_wait_err(func() (any, error) { return w23_remember(a[1].Global_id(), "enc") }) != nil)
}

// ---------------------------------------------------------------------------
// concurrent faults
// ---------------------------------------------------------------------------

func TestConcurrentFaults_writers_readers_and_forgetters_with_flaky_backend(t *testing.T) {
	policy := Fresh_policy(t)
	pool := must(New_FlakyPool(py.Kw{"fail_every_write": 7, "fail_every_read": 11}))
	mem_extend(t, policy, pool, "flaky")
	stop := py.NewEvent()
	stats := map[string]int{"w_ok": 0, "w_err": 0, "r_ok": 0, "r_err": 0, "f": 0}
	var lock sync.Mutex
	known := []string{}

	// A Python thread whose body raises dies with a traceback and the test
	// goes on; the goroutine twin does the same.
	guard := func(body func()) func() {
		return func() {
			defer func() { _ = recover() }()
			body()
		}
	}

	writer := func(i int) {
		for !stop.Is_set() {
			e := must(laila.Entry_constant(i))
			f := must(w23_memorize(e, "flaky"))
			_, err := Wait(f, 30)
			if err == nil {
				lock.Lock()
				known = append(known, e.Global_id())
				stats["w_ok"]++
				lock.Unlock()
			} else if errors.Is(err, InjectedFailure) {
				lock.Lock()
				stats["w_err"]++
				lock.Unlock()
			} else {
				Release_all(f)
				panic(err)
			}
			Release_all(f)
		}
	}

	reader := func(i int) {
		for !stop.Is_set() {
			lock.Lock()
			gid := ""
			if len(known) > 0 {
				gid = known[len(known)-1]
			}
			lock.Unlock()
			if gid == "" {
				py.Sleep_sync(0.01)
				continue
			}
			f := must(w23_remember(gid, "flaky"))
			if _, err := Wait(f, 30); err == nil {
				lock.Lock()
				stats["r_ok"]++
				lock.Unlock()
			} else {
				lock.Lock()
				stats["r_err"]++
				lock.Unlock()
			}
			Release_all(f)
		}
	}

	forgetter := func(i int) {
		for !stop.Is_set() {
			lock.Lock()
			gid := ""
			if len(known) > 50 {
				gid = known[0]
				known = known[1:]
			}
			lock.Unlock()
			if gid == "" {
				py.Sleep_sync(0.01)
				continue
			}
			f := must(laila.Forget(gid, py.Kw{"pool_nickname": "flaky"}))
			if _, err := Wait(f, 30); err == nil {
				lock.Lock()
				stats["f"]++
				lock.Unlock()
			}
			Release_all(f)
		}
	}

	roles := []func(int){writer, writer, writer, reader, reader, reader, forgetter}
	threads := []*py.Thread{}
	for i, r := range roles {
		i, r := i, r
		threads = append(threads, py.New_Thread(py.Kw{"target": guard(func() { r(i) }), "daemon": true}))
	}
	for _, th := range threads {
		check(t, th.Start())
	}
	time.Sleep(8 * time.Second)
	stop.Set()
	for _, th := range threads {
		_ = th.Join(30)
	}
	for _, th := range threads {
		assertTrue(t, !th.Is_alive())
	}
	lock.Lock()
	snapshot := map[string]int{}
	for k, v := range stats {
		snapshot[k] = v
	}
	lock.Unlock()
	assertTrue(t, snapshot["w_ok"] > 50 && snapshot["w_err"] > 0 && snapshot["r_ok"] > 0, snapshot)
	Quiesce(policy)
	assertTrue(t, Wait_until(func() bool { return policy.Future_bank.Len() < 100 }, 10), policy.Future_bank.Len())
}
