//go:build workload

// Port of tests/workloads/test_w17_policy_lifecycle.py.
//
// W17 -- policy lifecycle under churn.
//
// Many coexisting policies, rapid activation switching from several
// threads, repeated terminate/recreate cycles (thread and fd hygiene),
// namespace-derived nickname identity, and the behaviour of handles that
// outlive their policy.
package workloads

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T17_ = 120.0

// clean_slate17 is the ``clean_slate`` fixture: terminate before and after
// so each test starts with no policies.
func clean_slate17(t *testing.T) {
	t.Helper()
	Setup(t)
	laila.Terminate()
	t.Cleanup(func() {
		laila.Terminate()
		laila.Set_active_policy_gid_("")
	})
}

// laila_threads17 is ``_laila_threads()``.
func laila_threads17() []string {
	out := []string{}
	for _, n := range Live_thread_names() {
		if strings.Contains(n, "AsyncTF") || strings.Contains(n, "Dispatcher") || strings.Contains(n, "ComplexFuture") {
			out = append(out, n)
		}
	}
	return out
}

// new_policy17 is ``DefaultPolicy()``.
func new_policy17(t *testing.T) *laila.LAILA_IDENTIFIABLE_POLICY_ {
	t.Helper()
	return As_policy(must(laila.New_DefaultPolicy()))
}

// new_tf17 is ``DefaultTaskForce(policy_id=gid, num_workers=n)``.
func new_tf17(t *testing.T, gid string, num_workers int) *laila.PythonAsyncThreadPoolTaskForce {
	t.Helper()
	return must(laila.New_DefaultTaskForce(py.Kw{"policy_id": gid, "num_workers": num_workers})).(*laila.PythonAsyncThreadPoolTaskForce)
}

// active_cmd17 is ``laila.command`` (the active policy's central command,
// lazily creating a default policy like the Python property).
func active_cmd17() (*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_, error) {
	p, err := laila.Get_active_policy()
	if err != nil {
		return nil, err
	}
	return Cmd(As_policy(p)), nil
}

// submit17 is ``laila.command.submit(tasks)``.
func submit17(tasks []any) (any, error) {
	cmd, err := active_cmd17()
	if err != nil {
		return nil, err
	}
	return cmd.Submit(tasks)
}

// memorize17 is ``laila.memorize(e, **kw).wait(_T)``.
func memorize17(t *testing.T, e any, kw ...py.Kw) {
	t.Helper()
	args := []any{e}
	for _, k := range kw {
		args = append(args, k)
	}
	must(Wait(must(laila.Memorize(args...)), T17_))
}

// remember17 is ``unwrap(laila.remember(*args).wait(_T))``.
func remember17(t *testing.T, args ...any) any {
	t.Helper()
	return Unwrap(must(Wait(must(laila.Remember(args...)), T17_)))
}

// mem_extend17 is ``laila.memory.extend(pool, pool_nickname=nick)`` on the
// active policy.
func mem_extend17(t *testing.T, pool any, nick string) {
	t.Helper()
	check(t, Mem(As_policy(must(laila.Get_active_policy()))).Extend(pool, py.Kw{"pool_nickname": nick}))
}

// ---------------------------------------------------------------------------
// many policies
// ---------------------------------------------------------------------------

func TestManyPolicies_fifty_policies_coexist_with_isolated_memory(t *testing.T) {
	clean_slate17(t)
	pols := make([]*laila.LAILA_IDENTIFIABLE_POLICY_, N_scaled(50))
	for i := range pols {
		pols[i] = new_policy17(t)
	}
	gids := []string{}
	for i, p := range pols {
		laila.Activate_policy(p)
		e := must(laila.Entry_constant(i))
		memorize17(t, e)
		gids = append(gids, e.Global_id())
	}
	for i, p := range pols {
		laila.Activate_policy(p)
		assertEq(t, remember17(t, gids[i]), i)
		// entries memorized under other policies are not visible here
		other := gids[(i+1)%len(gids)]
		f, err := laila.Remember(other)
		if err == nil {
			_, err = Wait(f, T17_)
		}
		assertTrue(t, err != nil, "remember of another policy's entry must raise")
		Quiesce(p)
	}
	assertEq(t, laila.Local_policies_.Len(), len(pols))
}

func TestManyPolicies_policy_creation_cost_is_flat(t *testing.T) {
	clean_slate17(t)
	make_ := func(n int) float64 {
		sw := Timed(func() {
			for i := 0; i < n; i++ {
				laila.Activate_policy(new_policy17(t))
			}
		})
		return sw.Elapsed
	}

	t1 := make_(N_scaled(20))
	t2 := make_(N_scaled(20))
	// creating policies late (with many existing) must not be much slower
	assertTrue(t, t2 < t1*3+1.0, t1, t2)
}

func TestManyPolicies_rapid_activation_switching_from_threads(t *testing.T) {
	clean_slate17(t)
	pols := make([]*laila.LAILA_IDENTIFIABLE_POLICY_, 8)
	for i := range pols {
		pols[i] = new_policy17(t)
	}
	for _, p := range pols {
		laila.Activate_policy(p)
	}
	var mu sync.Mutex
	errs := []any{}

	w := func(i int) error {
		p := pols[i]
		for j := 0; j < N_scaled(200); j++ {
			laila.Activate_policy(p)
			err := func() error {
				e, err := laila.Entry_constant(py.Tuple{i, j})
				if err != nil {
					return err
				}
				f, err := laila.Memorize(e)
				if err != nil {
					return err
				}
				if _, err := Wait(f, T17_); err != nil {
					return err
				}
				// the active policy is process-global, so by the time we
				// remember, another thread may have switched it; the
				// entry must still be reachable through *its* policy
				laila.Activate_policy(p)
				g, err := laila.Remember(e.Global_id())
				if err != nil {
					return err
				}
				v, err := Wait(g, T17_)
				if err != nil {
					return err
				}
				out := Unwrap(v)
				if !py.Eq(py.Tuple(py.Iter(out)), py.Tuple{i, j}) {
					mu.Lock()
					errs = append(errs, py.Tuple{i, j, out})
					mu.Unlock()
				}
				return nil
			}()
			if err != nil {
				mu.Lock()
				errs = append(errs, py.Tuple{i, j, py.Repr(err)})
				mu.Unlock()
			}
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	for _, p := range pols {
		Quiesce(p)
	}
	if len(errs) > 0 {
		// ``pytest.xfail(...)``: an expected failure, reported as a skip.
		t.Skipf(
			"xfail: design limitation (documented): the active policy is process-global, not "+
				"thread-local; %d of %d memorize/remember pairs "+
				"from concurrently switching threads were routed to the wrong policy",
			len(errs), 8*N_scaled(200),
		)
	}
}

func TestManyPolicies_entry_memorized_in_one_policy_remembered_via_explicit_pool(t *testing.T) {
	clean_slate17(t)
	a, b := new_policy17(t), new_policy17(t)
	laila.Activate_policy(a)
	shared := must(laila.New_DefaultPool())
	mem_extend17(t, shared, "shared")
	laila.Activate_policy(b)
	mem_extend17(t, shared, "shared")
	e := must(laila.Entry_constant("x"))
	laila.Activate_policy(a)
	memorize17(t, e, py.Kw{"pool_nickname": "shared"})
	laila.Activate_policy(b)
	assertEq(t, remember17(t, e.Global_id(), py.Kw{"pool_nickname": "shared"}), "x")
	Quiesce(a)
	Quiesce(b)
}

// ---------------------------------------------------------------------------
// terminate cycles
// ---------------------------------------------------------------------------

func TestTerminateCycles_terminate_recreate_many_times_no_thread_leak(t *testing.T) {
	clean_slate17(t)
	base := len(laila_threads17())
	for i := 0; i < N_scaled(30); i++ {
		p := new_policy17(t)
		laila.Activate_policy(p)
		Cmd(p).Add_taskforce(new_tf17(t, p.Global_id(), 2))
		tasks := make([]any, 20)
		for j := range tasks {
			tasks[j] = lam(1)
		}
		gf := gfut(must(submit17(tasks)))
		must(gf.Wait(T17_))
		gf.Release()
		errs := laila.Terminate()
		assertEq(t, errs, []string{})
	}
	py.Sleep_sync(0.5)
	assertTrue(t, len(laila_threads17()) <= base+2, laila_threads17())
}

func TestTerminateCycles_terminate_closes_persistent_pools(t *testing.T) {
	clean_slate17(t)
	disk_tmp := Disk_tmp(t)
	fds0 := Open_fds()
	for i := 0; i < 10; i++ {
		p := new_policy17(t)
		laila.Activate_policy(p)
		sq := must(Make_pool("sqlite", disk_tmp))
		mem_extend17(t, sq, "sq")
		memorize17(t, must(laila.Entry_constant(i)), py.Kw{"pool_nickname": "sq"})
		Quiesce(p)
		laila.Terminate()
	}
	runtime.GC()
	py.Sleep_sync(0.2)
	leaked := []string{}
	for f := range Open_fds() {
		if _, before := fds0[f]; !before && (strings.Contains(f, "sqlite") || strings.Contains(f, "laila")) {
			leaked = append(leaked, f)
		}
	}
	sort.Strings(leaked)
	assertEq(t, leaked, []string{})
}

func TestTerminateCycles_terminate_with_inflight_work(t *testing.T) {
	clean_slate17(t)
	p := new_policy17(t)
	laila.Activate_policy(p)
	gate := py.NewEvent()
	futs := make([]any, 10)
	for i := range futs {
		futs[i] = must(submit17([]any{func() (any, error) { return gate.Wait(10), nil }}))
	}
	py.Sleep_sync(0.2)
	gate.Set()
	var errs []string
	sw := Timed(func() {
		errs = laila.Terminate(py.Kw{"wait": true, "cancel_pending": true})
	})
	assertTrue(t, sw.Elapsed < 30, sw.Elapsed)
	assertEq(t, errs, []string{})
	for _, f := range futs {
		st := cpf(f).Status()
		assertTrue(t, st == laila.FutureStatus_FINISHED || st == laila.FutureStatus_CANCELLED, st)
	}
}

// BadPool17 is the test's ``class BadPool(DefaultPool): def close(self):
// raise RuntimeError("close failed")``.
type BadPool17 struct {
	laila.LAILA_IDENTIFIABLE_POOL_
}

var BadPool17_class = py.NewClass("BadPool", (*BadPool17)(nil), laila.DefaultPool)

// New_BadPool17 is ``BadPool()``.
func New_BadPool17() (*BadPool17, error) {
	p := BadPool17_class.New().(*BadPool17)
	if err := p.Init_(py.Kw{}); err != nil {
		return nil, err
	}
	return p, nil
}

// Close is ``close(self)``: always raises.
func (p *BadPool17) Close() error { return py.RuntimeError.New("close failed") }

func TestTerminateCycles_terminate_is_idempotent_and_reports_errors_as_list(t *testing.T) {
	clean_slate17(t)
	assertEq(t, laila.Terminate(), []string{})
	assertEq(t, laila.Terminate(), []string{})
	laila.Activate_policy(new_policy17(t))

	mem_extend17(t, must(New_BadPool17()), "bad")
	errs := laila.Terminate()
	found := false
	for _, e := range errs {
		if strings.Contains(e, "close failed") {
			found = true
		}
	}
	assertTrue(t, found, errs)
	// the process is still usable afterwards
	must(laila.Get_active_policy())
	assertEq(t, Unwrap(must(Wait(must(submit17([]any{lam(3)})), T17_))), 3)
}

func TestTerminateCycles_orphan_taskforces_swept(t *testing.T) {
	clean_slate17(t)
	laila.Activate_policy(new_policy17(t))
	orphans := make([]*laila.PythonAsyncThreadPoolTaskForce, 5)
	for i := range orphans {
		orphans[i] = new_tf17(t, As_policy(must(laila.Get_active_policy())).Global_id(), 1)
	}
	laila.Terminate()
	for _, tf := range orphans {
		assertEq(t, tf.Status, laila.TaskForceStatus_STOPPED)
	}
}

func TestTerminateCycles_lazy_default_after_terminate(t *testing.T) {
	clean_slate17(t)
	laila.Terminate()
	assertEq(t, laila.Active_policy_gid_(), "")
	p := must(laila.Get_active_policy())
	assertTrue(t, py.Isinstance(p, laila.DefaultPolicy))
	e := must(laila.Entry_constant(1))
	memorize17(t, e)
	assertEq(t, remember17(t, e.Global_id()), 1)
	Quiesce(p)
}

func TestTerminateCycles_stale_handles_after_terminate(t *testing.T) {
	clean_slate17(t)
	p := new_policy17(t)
	laila.Activate_policy(p)
	f := cpf(must(submit17([]any{lam(1)})))
	must(f.Wait(T17_))
	e := must(laila.Entry_constant(1))
	memorize17(t, e)
	Quiesce(p)
	laila.Terminate()
	// futures from a dead policy: status still readable, wait returns cached
	assertEq(t, f.Status(), laila.FutureStatus_FINISHED)
	assertEq(t, Unwrap(must(f.Wait(1))), 1)
	_, err := laila.Status(f.Global_id())
	assertTrue(t, err != nil, "runtime.status on a dead policy's future must raise")
	// submitting to the dead policy's command raises rather than hanging
	if _, err := Cmd(p).Submit([]any{lam(1)}); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("want RuntimeError, got %v", err)
	}
}

func TestTerminateCycles_submit_during_terminate_from_another_thread(t *testing.T) {
	clean_slate17(t)
	p := new_policy17(t)
	laila.Activate_policy(p)
	stop := py.NewEvent()
	var mu sync.Mutex
	outcomes := map[string]int{"ok": 0, "err": 0}

	submitter := func() {
		for !stop.Is_set() {
			err := func() (err error) {
				defer py.Recover(&err)
				f, err := submit17([]any{lam(1)})
				if err != nil {
					return err
				}
				_, err = Wait(f, 5)
				return err
			}()
			mu.Lock()
			if err == nil {
				outcomes["ok"]++
			} else {
				outcomes["err"]++
			}
			mu.Unlock()
		}
	}

	th := py.New_Thread(py.Kw{"target": submitter, "daemon": true})
	check(t, th.Start())
	py.Sleep_sync(0.3)
	laila.Terminate(py.Kw{"wait": true, "cancel_pending": true})
	stop.Set()
	_ = th.Join(30)
	assertTrue(t, !th.Is_alive())
	mu.Lock()
	defer mu.Unlock()
	assertTrue(t, outcomes["ok"] > 0, outcomes)
}

// ---------------------------------------------------------------------------
// namespaces
// ---------------------------------------------------------------------------

func TestNamespaces_nickname_identity_is_namespace_scoped(t *testing.T) {
	clean_slate17(t)
	laila.Activate_policy(new_policy17(t))
	laila.Set_active_namespace("wl17.ns.a")
	ga := must(laila.Entry_constant(1, py.Kw{"nickname": "shared-name"})).Global_id()
	laila.Set_active_namespace("wl17.ns.b")
	gb := must(laila.Entry_constant(1, py.Kw{"nickname": "shared-name"})).Global_id()
	laila.Set_active_namespace("wl17.ns.a")
	ga2 := must(laila.Entry_constant(2, py.Kw{"nickname": "shared-name"})).Global_id()
	assertTrue(t, ga == ga2 && ga != gb)
	ns := laila.Get_active_namespace()
	assertEq(t, ns, py.Uuid5(py.NAMESPACE_DNS, "wl17.ns.a"))
}

func TestNamespaces_thousands_of_nicknames_no_collisions(t *testing.T) {
	clean_slate17(t)
	laila.Activate_policy(new_policy17(t))
	laila.Set_active_namespace("wl17.collide")
	gids := map[string]struct{}{}
	for i := 0; i < N_scaled(20000); i++ {
		gids[must(laila.Entry_constant(i, py.Kw{"nickname": fmt.Sprintf("n%d", i)})).Global_id()] = struct{}{}
	}
	assertEq(t, len(gids), N_scaled(20000))
}

func TestNamespaces_nickname_round_trip_across_terminate(t *testing.T) {
	clean_slate17(t)
	disk_tmp := Disk_tmp(t)
	laila.Set_active_namespace("wl17.persist")
	p := new_policy17(t)
	laila.Activate_policy(p)
	sq := must(Make_pool("sqlite", disk_tmp))
	path := *sq.(*laila.SQLitePool).File_path
	mem_extend17(t, sq, "sq")
	memorize17(t, must(laila.Entry_constant(py.DictOf("v", 1), py.Kw{"nickname": "cfg"})), py.Kw{"pool_nickname": "sq"})
	Quiesce(p)
	laila.Terminate()
	// new process-state, same namespace -> same nickname -> same gid
	p2 := new_policy17(t)
	laila.Activate_policy(p2)
	mem_extend17(t, must(laila.New_SQLitePool(py.Kw{"file_path": path})), "sq")
	out := remember17(t, py.Kw{"nickname": "cfg", "pool_nickname": "sq"})
	assertEq(t, out, py.DictOf("v", 1))
	Quiesce(p2)
}

func TestNamespaces_namespace_switch_under_threads(t *testing.T) {
	clean_slate17(t)
	laila.Activate_policy(new_policy17(t))
	var mu sync.Mutex
	seen := map[string]map[string]struct{}{}

	w := func(i int) error {
		ns := fmt.Sprintf("wl17.t%d", i%2)
		for j := 0; j < 500; j++ {
			laila.Set_active_namespace(ns)
			e, err := laila.Entry_constant(0, py.Kw{"nickname": "k"})
			if err != nil {
				return err
			}
			g := e.Global_id()
			mu.Lock()
			if _, ok := seen[g]; !ok {
				seen[g] = map[string]struct{}{}
			}
			seen[g][ns] = struct{}{}
			mu.Unlock()
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	// at most two distinct gids can legitimately exist (one per namespace)
	assertTrue(t, len(seen) <= 2, len(seen))
}
