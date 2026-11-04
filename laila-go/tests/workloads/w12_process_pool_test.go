//go:build workload

// Port of tests/workloads/test_w12_process_pool.py.
//
// W12 -- process-pool taskforce workloads.
//
// Volume, large payloads crossing the pickle boundary, worker crashes,
// cancellation, shutdown semantics and laila usage inside workers.
//
// Workers are this test binary re-executed (see TestMain); the tasks are
// the registered ``_w12_workers`` functions (w12_workers.go), shipped by
// name the way spawn pickles a module-level function.
package workloads

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T12_ = 180.0

// new_pp_ is ``_new_pp(policy, num_workers=4)``.
func new_pp_(policy *laila.LAILA_IDENTIFIABLE_POLICY_, num_workers ...int) *laila.PythonProcessPoolTaskForce {
	n := 4
	if len(num_workers) > 0 {
		n = num_workers[0]
	}
	tf := must(laila.New_PythonProcessPoolTaskForce(py.Kw{"policy_id": policy.Global_id(), "num_workers": n}))
	py.Must(0, tf.Start())
	Cmd(policy).Taskforces.Set(tf.Global_id(), tf)
	return tf
}

// drop_pp_ is ``_drop(policy, tf, **kw)``.
func drop_pp_(policy *laila.LAILA_IDENTIFIABLE_POLICY_, tf *laila.PythonProcessPoolTaskForce, kw ...py.Kw) {
	defer Cmd(policy).Taskforces.PopDefault(tf.Global_id(), nil)
	_ = tf.Shutdown(py.MergeKw(kw...))
}

// pp_policy_ is the module-scoped ``pp_policy`` fixture: one policy with a
// 4-worker process pool shared by every test that asks for ``pp``.
var (
	pp_once_   sync.Once
	pp_policy_ *laila.LAILA_IDENTIFIABLE_POLICY_
	pp_tf_     *laila.PythonProcessPoolTaskForce
)

func pp_policy(t *testing.T) (*laila.LAILA_IDENTIFIABLE_POLICY_, *laila.PythonProcessPoolTaskForce) {
	t.Helper()
	pp_once_.Do(func() {
		Setup(t)
		original := must(laila.Get_active_policy())
		policy := must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
		laila.Activate_policy(policy)
		tf := new_pp_(policy, 4)
		pp_policy_, pp_tf_ = policy, tf
		Add_module_teardown(func() {
			Quiesce(policy)
			drop_pp_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
			_ = Cmd(policy).Shutdown(py.Kw{"wait": true, "cancel_pending": true})
			laila.Local_policies_.PopDefault(policy.Global_id(), nil)
			laila.Activate_policy(original)
		})
	})
	return pp_policy_, pp_tf_
}

// pp is the ``pp`` fixture: the shared pool, with the shared policy
// activated for the test and quiesced afterwards.
func pp(t *testing.T) *laila.PythonProcessPoolTaskForce {
	t.Helper()
	policy, tf := pp_policy(t)
	previous := must(laila.Get_active_policy())
	laila.Activate_policy(policy)
	t.Cleanup(func() {
		Quiesce(policy)
		laila.Activate_policy(previous)
	})
	return tf
}

// submit_pp is ``_submit(tf, tasks, **kw)``: the direct ``tf.submit`` path
// (NB: bypasses ``laila.command.submit`` -- see
// ``TestCommandPath.test_command_submit_to_process_pool``).
func submit_pp(tf *laila.PythonProcessPoolTaskForce, tasks []any, kw ...py.Kw) (any, error) {
	return tf.Submit(tasks, py.MergeKw(kw...))
}

// partial is ``functools.partial(fn, *args)``.
func partial(fn any, args ...any) any { return py.Partial(fn, args...) }

func ppf(f any) *laila.ProcessPackageFuture { return f.(*laila.ProcessPackageFuture) }

func partials(fn any, n int, arg func(i int) []any) []any {
	tasks := make([]any, n)
	for i := range tasks {
		tasks[i] = partial(fn, arg(i)...)
	}
	return tasks
}

// ---------------------------------------------------------------------------
// command path
// ---------------------------------------------------------------------------

func TestCommandPath_command_submit_to_process_pool(t *testing.T) {
	tf := pp(t)
	f := must(must(laila.Command()).(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_).Submit([]any{W.Pid}, py.Kw{"taskforce_id": tf.Global_id()}))
	assertTrue(t, !py.Eq(Unwrap(must(Wait(f, T12_))), os.Getpid()))
	Release_all(f)
}

const W12_RETURN_2_CODE_ = "def f(manifest):\n    return 2\n"

func init() {
	laila.Register_constitution(W12_RETURN_2_CODE_, func(arg any) (any, error) { return 2, nil })
}

func TestCommandPath_build_on_process_pool(t *testing.T) {
	tf := pp(t)
	m := must(laila.New_Manifest(py.Kw{"data": py.DictOf("a", must(laila.Entry_constant(1)))}))
	must(must(m.Memorize()).Wait(T12_))
	e := must(laila.Variable(py.Kw{"constitution": W12_RETURN_2_CODE_, "manifest": m}))
	must(Wait(must(laila.Build(e, py.Kw{"taskforce_id": tf.Global_id()})), T12_))
	assertEq(t, must(e.Data()), 2)
}

func TestCommandPath_guarantee_scope_tracks_direct_tf_submit(t *testing.T) {
	tf := pp(t)
	// futures register with guarantee scopes in model_post_init, so even
	// the direct tf.submit() path is tracked
	var f any
	check(t, laila.Guarantee.With(func() error {
		var err error
		f, err = tf.Submit([]any{partial(W.Sleep_then, 1, 0.3)})
		return err
	}))
	assertEq(t, ppf(f).Status(), laila.FutureStatus_FINISHED)
	Release_all(f)
}

// ---------------------------------------------------------------------------
// volume
// ---------------------------------------------------------------------------

func TestVolume_pool_warms_up(t *testing.T) {
	tf := pp(t)
	f := must(submit_pp(tf, []any{W.Pid}))
	assertTrue(t, !py.Eq(Unwrap(must(Wait(f, T12_))), os.Getpid()))
	Release_all(f)
}

func TestVolume_five_thousand_tiny_tasks(t *testing.T) {
	tf := pp(t)
	n := N_scaled(5000)
	var gf *laila.GroupFuture
	var out *py.List
	sw := Timed(func() {
		gf = gfut(must(submit_pp(tf, partials(W.Square, n, func(i int) []any { return []any{i} }))))
		out = unwrap_all(must(gf.Wait(T12_)))
	})
	want := py.NewList()
	for i := 0; i < n; i++ {
		want.Append(i * i)
	}
	assertEq(t, out, want)
	gf.Release()
	assertTrue(t, sw.Elapsed < 0.01*float64(n)+10, sw.Elapsed)
}

func TestVolume_imap_streaming(t *testing.T) {
	tf := pp(t)
	n := N_scaled(2000)
	futs := []any{}
	for f, err := range must(tf.Imap(partials(W.Ident, n, func(i int) []any { return []any{i} }))) {
		check(t, err)
		futs = append(futs, f)
	}
	vals := py.NewList()
	for _, f := range futs {
		_, ok := f.(*laila.ProcessPackageFuture)
		assertTrue(t, ok, py.Pytypename(f))
		vals.Append(Unwrap(must(Wait(f, T12_))))
	}
	assertEq(t, vals, range_list(n))
	Release_all(futs...)
}

func TestVolume_submission_slots_backpressure(t *testing.T) {
	// Semaphore caps native submissions at 2*num_workers; the dispatcher
	// must keep draining the queue as results arrive.
	tf := pp(t)
	gf := gfut(must(submit_pp(tf, partials(W.Sleep_then, 200, func(i int) []any { return []any{i, 0.01} }))))
	assertEq(t, unwrap_all(must(gf.Wait(T12_))), range_list(200))
	gf.Release()
	assertTrue(t, Wait_until(func() bool { return tf.Queue_len() == 0 }, 10))
}

func TestVolume_work_distributed_across_workers(t *testing.T) {
	tf := pp(t)
	gf := gfut(must(submit_pp(tf, partials(W.Sleep_then, 32, func(i int) []any { return []any{i, 0.05} }))))
	must(gf.Wait(T12_))
	gf.Release()
	tasks := make([]any, 32)
	for i := range tasks {
		tasks[i] = W.Pid
	}
	pids := must(submit_pp(tf, tasks, py.Kw{"wait": true}))
	distinct := py.NewSet()
	for _, p := range py.Iter(pids) {
		distinct.Add(Unwrap(p))
	}
	assertTrue(t, distinct.Len() >= 2, distinct.Len())
}

func TestVolume_cpu_bound_parallel_speedup(t *testing.T) {
	tf := pp(t)
	n := 2_000_000
	s1 := Timed(func() { must(submit_pp(tf, []any{partial(W.Cpu_burn, n)}, py.Kw{"wait": true})) })
	s4 := Timed(func() {
		must(submit_pp(tf, []any{partial(W.Cpu_burn, n), partial(W.Cpu_burn, n)}, py.Kw{"wait": true}))
	})
	// two tasks on >=2 cores should not take 2x a single task
	assertTrue(t, s4.Elapsed < s1.Elapsed*1.8+0.5, s1.Elapsed, s4.Elapsed)
}

func TestVolume_sustained_batches(t *testing.T) {
	tf := pp(t)
	before := As_policy(must(laila.Get_active_policy())).Future_bank.Len()
	for r := 0; r < N_scaled(100); r++ {
		gf := gfut(must(submit_pp(tf, partials(W.Ident, 10, func(i int) []any { return []any{r*10 + i} }))))
		want := py.NewList()
		for i := 0; i < 10; i++ {
			want.Append(r*10 + i)
		}
		assertEq(t, unwrap_all(must(gf.Wait(T12_))), want)
		gf.Release()
	}
	assertEq(t, As_policy(must(laila.Get_active_policy())).Future_bank.Len(), before)
}

// ---------------------------------------------------------------------------
// payloads
// ---------------------------------------------------------------------------

func TestPayloads_large_result_crosses_pipe(t *testing.T) {
	for _, mb := range []int{1, 16, 64} {
		mb := mb
		t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
			tf := pp(t)
			f := must(submit_pp(tf, []any{partial(W.Big_bytes, mb*1024*1024)}))
			out := Unwrap(must(Wait(f, T12_)))
			assertEq(t, must(py.Len(out)), mb*1024*1024)
			Release_all(f)
		})
	}
}

func TestPayloads_large_argument_crosses_pipe(t *testing.T) {
	for _, mb := range []int{1, 32} {
		mb := mb
		t.Run(fmt.Sprintf("mb=%d", mb), func(t *testing.T) {
			tf := pp(t)
			blob := make([]byte, mb*1024*1024)
			for i := range blob {
				blob[i] = 1
			}
			f := must(submit_pp(tf, []any{partial(W.Echo_bytes, blob)}))
			assertEq(t, Unwrap(must(Wait(f, T12_))), len(blob))
			Release_all(f)
		})
	}
}

func TestPayloads_many_medium_results_concurrently(t *testing.T) {
	tf := pp(t)
	gf := gfut(must(submit_pp(tf, partials(W.Big_bytes, 64, func(i int) []any { return []any{512 * 1024} }))))
	vals := must(gf.Wait(T12_))
	for _, v := range py.Iter(vals) {
		assertEq(t, must(py.Len(Unwrap(v))), 512*1024)
	}
	gf.Release()
}

func TestPayloads_deeply_nested_result(t *testing.T) {
	tf := pp(t)
	f := must(submit_pp(tf, []any{partial(W.Nested_dict, 400)}))
	d := Unwrap(must(Wait(f, T12_)))
	depth := 0
	for {
		dd, ok := d.(*py.Dict)
		if !ok || dd.Contains("leaf") {
			break
		}
		d = dd.Values()[0]
		depth++
	}
	assertEq(t, depth, 400)
	Release_all(f)
}

func TestPayloads_numpy_in_worker(t *testing.T) {
	tf := pp(t)
	f := must(submit_pp(tf, []any{partial(W.Numpy_sum, 1_000_000)}))
	assertEq(t, Unwrap(must(Wait(f, T12_))), float64(sum_range(1_000_000)))
	Release_all(f)
}

func TestPayloads_callable_instance_and_partial(t *testing.T) {
	tf := pp(t)
	// ``W.Callable(7)`` is a callable instance in Python; its Go twin is the
	// registered ``Callable`` task bound to ``k`` (the pickled state).
	a := must(submit_pp(tf, []any{partial(W.Callable, 7)}))
	b := must(submit_pp(tf, []any{&py.PartialFunc{Func: W.Partial_target, Args: []any{2}, Keywords: py.Kw{"b": 3}}}))
	assertEq(t, Unwrap(must(Wait(a, T12_))), 21)
	assertEq(t, Unwrap(must(Wait(b, T12_))), 5)
	Release_all(a, b)
}

func TestPayloads_lambda_rejected_at_submit(t *testing.T) {
	tf := pp(t)
	if _, err := tf.Submit([]any{lam(1)}); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

func TestPayloads_closure_rejected_at_submit(t *testing.T) {
	tf := pp(t)
	k := 3
	inner := func() (any, error) { return k, nil }
	if _, err := tf.Submit([]any{inner}); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

func TestPayloads_unpicklable_argument_rejected(t *testing.T) {
	tf := pp(t)
	if _, err := tf.Submit([]any{partial(W.Ident, py.NewLock())}); !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// failures
// ---------------------------------------------------------------------------

func TestFailures12_hundreds_of_raising_tasks(t *testing.T) {
	tf := pp(t)
	futs := make([]any, 300)
	for i := range futs {
		futs[i] = must(submit_pp(tf, []any{partial(W.Raise_value, i)}))
	}
	for i, f := range futs {
		_, err := Wait(f, T12_)
		if !errors.Is(err, py.ValueError) || !regexp.MustCompile(fmt.Sprintf("proc-boom-%d$", i)).MatchString(err.Error()) {
			t.Fatalf("want ValueError proc-boom-%d, got %v", i, err)
		}
		assertEq(t, ppf(f).Status(), laila.FutureStatus_ERROR)
	}
	Release_all(futs...)
}

func TestFailures12_group_with_mixed_outcomes(t *testing.T) {
	tf := pp(t)
	tasks := partials(W.Ident, 20, func(i int) []any { return []any{i} })
	tasks = append(tasks[:10], append([]any{partial(W.Raise_value, 99)}, tasks[10:]...)...)
	gf := gfut(must(submit_pp(tf, tasks)))
	if _, err := gf.Wait(T12_); !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
	pct := func(key string) float64 {
		st := must(gf.Status())
		return py.Must(py.Float(must(must(st.Getitem("percentages")).(*py.Dict).Getitem(key))))
	}
	assertTrue(t, Wait_until(func() bool { return pct("finished") > 90 }, 20))
	assertTrue(t, pct("error") > 0)
	gf.Release()
}

func TestFailures12_worker_hard_exit_surfaces_as_error(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_pp_(policy, 4)
	defer drop_pp_(policy, tf, py.Kw{"wait": false, "cancel_pending": true})
	f := must(submit_pp(tf, []any{partial(W.Exit_hard, 3)}))
	_, err := Wait(f, T12_)
	assertTrue(t, err != nil, "want an exception")
	assertEq(t, ppf(f).Status(), laila.FutureStatus_ERROR)
	Release_all(f)
}

func TestFailures12_broken_pool_fails_fast_or_recovers(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_pp_(policy, 4)
	defer drop_pp_(policy, tf, py.Kw{"wait": false, "cancel_pending": true})
	crash := must(submit_pp(tf, []any{partial(W.Exit_hard, 3)}))
	_, err := Wait(crash, T12_)
	assertTrue(t, err != nil, "want an exception")
	Release_all(crash)
	assertTrue(t, Wait_until(func() bool { return !tf.Dispatcher_.Is_alive() }, 5) || true)
	g := must(submit_pp(tf, []any{W.Pid}))
	sw := Timed(func() { _, _ = Wait(g, 10) })
	Release_all(g)
	// either the pool recovered, or the failure was reported promptly
	st := ppf(g).Status()
	assertTrue(t, st == laila.FutureStatus_FINISHED || st == laila.FutureStatus_ERROR || st == laila.FutureStatus_CANCELLED, st)
	assertTrue(t, sw.Elapsed < 5)
}

func TestFailures12_worker_segfault_surfaces_as_error(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_pp_(policy, 4)
	defer drop_pp_(policy, tf, py.Kw{"wait": false, "cancel_pending": true})
	f := must(submit_pp(tf, []any{W.Segv}))
	_, err := Wait(f, T12_)
	assertTrue(t, err != nil, "want an exception")
	assertEq(t, ppf(f).Status(), laila.FutureStatus_ERROR)
	Release_all(f)
}

func TestFailures12_broken_pool_queued_tasks_do_not_hang_shutdown(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_pp_(policy, 4)
	futs := make([]any, 40)
	for i := range futs {
		futs[i] = must(submit_pp(tf, []any{partial(W.Sleep_then, i, 0.2)}))
	}
	crash := must(submit_pp(tf, []any{partial(W.Exit_hard, 1)}))
	_, err := Wait(crash, T12_)
	assertTrue(t, err != nil, "want an exception")
	sw := Timed(func() { drop_pp_(policy, tf, py.Kw{"wait": true, "cancel_pending": true}) })
	assertTrue(t, sw.Elapsed < 60)
	Release_all(append(futs, crash)...)
}

func TestFailures12_timeout_on_slow_worker(t *testing.T) {
	tf := pp(t)
	f := must(submit_pp(tf, []any{partial(W.Sleep_then, 1, 1.0)}))
	if _, err := Wait(f, 0.05); !errors.Is(err, py.TimeoutError) {
		t.Fatalf("want TimeoutError, got %v", err)
	}
	assertEq(t, Unwrap(must(Wait(f, T12_))), 1)
	Release_all(f)
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------

func TestLifecycle12_shutdown_cancel_pending(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_pp_(policy, 4)
	futs := make([]any, 60)
	for i := range futs {
		futs[i] = must(submit_pp(tf, []any{partial(W.Sleep_then, i, 0.3)}))
	}
	time.Sleep(500 * time.Millisecond)
	drop_pp_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	seen_cancelled := false
	for _, f := range futs {
		s := ppf(f).Status()
		if s == laila.FutureStatus_CANCELLED {
			seen_cancelled = true
		}
		assertTrue(t, s == laila.FutureStatus_CANCELLED || s == laila.FutureStatus_FINISHED || s == laila.FutureStatus_ERROR, s)
	}
	assertTrue(t, seen_cancelled)
	Release_all(futs...)
}

func TestLifecycle12_shutdown_wait_completes_dispatched(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_pp_(policy, 4)
	futs := make([]any, 8)
	for i := range futs {
		futs[i] = must(submit_pp(tf, []any{partial(W.Sleep_then, i, 0.05)}))
	}
	assertTrue(t, Wait_until(func() bool {
		for _, f := range futs {
			if ppf(f).Status() == laila.FutureStatus_RUNNING {
				return true
			}
		}
		return false
	}, 30))
	drop_pp_(policy, tf, py.Kw{"wait": true, "cancel_pending": false})
	statuses := []laila.FutureStatus{}
	for _, f := range futs {
		statuses = append(statuses, ppf(f).Status())
	}
	for _, s := range statuses {
		assertTrue(t, s == laila.FutureStatus_FINISHED, statuses)
	}
	Release_all(futs...)
}

func TestLifecycle12_submit_after_shutdown_raises(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_pp_(policy, 4)
	drop_pp_(policy, tf, py.Kw{"wait": true})
	if _, err := tf.Submit([]any{W.Pid}); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("want RuntimeError, got %v", err)
	}
}

func TestLifecycle12_pause_not_supported(t *testing.T) {
	tf := pp(t)
	if err := tf.Pause(); !errors.Is(err, py.NotImplementedError) {
		t.Fatalf("want NotImplementedError, got %v", err)
	}
	assertEq(t, tf.Status, laila.TaskForceStatus_RUNNING)
}

func TestLifecycle12_min_workers_enforced(t *testing.T) {
	policy := Fresh_policy(t)
	_, err := laila.New_PythonProcessPoolTaskForce(py.Kw{"policy_id": policy.Global_id(), "num_workers": 1})
	assertTrue(t, err != nil, "want an exception")
}

func TestLifecycle12_pool_restart_cycles(t *testing.T) {
	policy := Fresh_policy(t)
	for i := 0; i < 3; i++ {
		tf := new_pp_(policy, 4)
		assertEq(t, Unwrap(must(Wait(must(submit_pp(tf, []any{partial(W.Ident, 1)})), T12_))), 1)
		drop_pp_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	}
	Quiesce(policy)
}

func TestLifecycle12_group_future_shape(t *testing.T) {
	tf := pp(t)
	gf := must(tf.Submit([]any{W.Pid, W.Pid}))
	_, isGroup := gf.(*laila.GroupFuture)
	assertTrue(t, isGroup, py.Pytypename(gf))
	single := must(tf.Submit([]any{W.Pid}))
	_, isSingle := single.(*laila.ProcessPackageFuture)
	assertTrue(t, isSingle, py.Pytypename(single))
	must(Wait(gf, T12_))
	must(Wait(single, T12_))
	Release_all(gf, single)
}

func TestLifecycle12_laila_usable_inside_worker(t *testing.T) {
	tf := pp(t)
	f := must(submit_pp(tf, []any{W.Laila_in_child}))
	assertTrue(t, Unwrap(must(Wait(f, T12_))) == true)
	Release_all(f)
}

func TestLifecycle12_memory_hog_workers_sequential(t *testing.T) {
	// 4 workers x 200MB fits in 7GB; ensures results come back and memory is released
	tf := pp(t)
	gf := gfut(must(submit_pp(tf, partials(W.Memory_hog, 8, func(i int) []any { return []any{200} }))))
	for _, v := range unwrap_all(must(gf.Wait(T12_))).Items() {
		assertEq(t, v, 200*1024*1024)
	}
	gf.Release()
}

func TestLifecycle12_concurrent_submitters(t *testing.T) {
	tf := pp(t)
	worker := func(i int) (any, error) {
		gf, err := submit_pp(tf, partials(W.Ident, 50, func(j int) []any { return []any{i*100 + j} }))
		if err != nil {
			return nil, err
		}
		vals, err := gfut(gf).Wait(T12_)
		if err != nil {
			return nil, err
		}
		gfut(gf).Release()
		want := py.NewList()
		for j := 0; j < 50; j++ {
			want.Append(i*100 + j)
		}
		return py.Eq(unwrap_all(vals), want), nil
	}
	for _, ok := range must(Run_threads(worker, 8)) {
		assertTrue(t, ok == true)
	}
}
