//go:build workload

// Port of tests/workloads/test_w11_async_taskforce_soak.py.
//
// W11 -- async-thread-pool taskforce under sustained load.
//
// Throughput, fan-out, deep nesting on starved taskforces, parking
// accounting, shutdown semantics, cancellation, pause/resume, status
// callbacks and future-bank hygiene.
//
// Task shapes follow the port's convention: a sync body is a plain
// ``func() (any, error)``, a coroutine is a ``py.CoroutineFunc`` and
// ``await`` is ``py.Await`` / ``py.Sleep`` inside it.
package workloads

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T11_ = 120.0

type tf_kw struct {
	num_workers, max_async_per_thread, sync_workers, rank int
}

// new_tf_ is ``_new_tf(policy, *, num_workers=1, max_async_per_thread=1,
// sync_workers=None, rank=2)``: constructed, started and registered.
func new_tf_(policy *laila.LAILA_IDENTIFIABLE_POLICY_, o tf_kw) *laila.PythonAsyncThreadPoolTaskForce {
	if o.num_workers == 0 {
		o.num_workers = 1
	}
	if o.max_async_per_thread == 0 {
		o.max_async_per_thread = 1
	}
	if o.rank == 0 {
		o.rank = 2
	}
	kw := py.Kw{
		"policy_id":            policy.Global_id(),
		"num_workers":          o.num_workers,
		"max_async_per_thread": o.max_async_per_thread,
		"rank":                 o.rank,
	}
	if o.sync_workers != 0 {
		kw["sync_workers"] = o.sync_workers
	}
	tf := must(laila.New_PythonAsyncThreadPoolTaskForce(kw))
	py.Must(0, tf.Start())
	Cmd(policy).Taskforces.Set(tf.Global_id(), tf)
	return tf
}

// drop_ is ``_drop(policy, tf, **kw)``.
func drop_(policy *laila.LAILA_IDENTIFIABLE_POLICY_, tf *laila.PythonAsyncThreadPoolTaskForce, kw ...py.Kw) {
	defer Cmd(policy).Taskforces.PopDefault(tf.Global_id(), nil)
	_ = tf.Shutdown(py.MergeKw(kw...))
}

// tf_fixture is the ``tf`` fixture (2x8, 4 sync workers).
func tf_fixture(t *testing.T, policy *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.PythonAsyncThreadPoolTaskForce {
	t.Helper()
	tf := new_tf_(policy, tf_kw{num_workers: 2, max_async_per_thread: 8, sync_workers: 4})
	t.Cleanup(func() {
		Quiesce(policy)
		drop_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	})
	return tf
}

// tiny_fixture is the ``tiny`` fixture (1x1, 1 sync worker).
func tiny_fixture(t *testing.T, policy *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.PythonAsyncThreadPoolTaskForce {
	t.Helper()
	tf := new_tf_(policy, tf_kw{num_workers: 1, max_async_per_thread: 1, sync_workers: 1})
	t.Cleanup(func() {
		Quiesce(policy)
		drop_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	})
	return tf
}

// submit_ is ``_submit(tf, tasks, **kw)`` = ``laila.command.submit(tasks,
// taskforce_id=tf.global_id, **kw)``.
func submit_(tf *laila.PythonAsyncThreadPoolTaskForce, tasks []any, kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	opts["taskforce_id"] = tf.Global_id()
	cmd, err := laila.Command()
	if err != nil {
		return nil, err
	}
	return cmd.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_).Submit(tasks, opts)
}

// lam is ``lambda: value``.
func lam(value any) any { return func() (any, error) { return value, nil } }

// coro is ``async def f(*args)``.
func coro(fn func(args ...any) (any, error)) py.CoroutineFunc { return py.CoroutineFunc(fn) }

// cpf is the concrete single future returned by a one-task submit.
func cpf(f any) *laila.ConcurrentPackageFuture { return f.(*laila.ConcurrentPackageFuture) }

// gfut is the GroupFuture returned by a multi-task submit.
func gfut(f any) *laila.GroupFuture { return f.(*laila.GroupFuture) }

// unwrap_all is ``[unwrap(v) for v in vals]``.
func unwrap_all(vals any) *py.List {
	out := py.NewList()
	for _, v := range py.Iter(vals) {
		out.Append(Unwrap(v))
	}
	return out
}

// sleep_then is ``lambda: (time.sleep(s), value)[1]``.
func sleep_then(s float64, value any) any {
	return func() (any, error) {
		py.Sleep_sync(s)
		return value, nil
	}
}

// ---------------------------------------------------------------------------
// throughput
// ---------------------------------------------------------------------------

func TestThroughput_ten_thousand_tiny_tasks(t *testing.T) {
	for _, kind := range []string{"sync", "async"} {
		kind := kind
		t.Run("kind="+kind, func(t *testing.T) {
			policy := Fresh_policy(t)
			tf := tf_fixture(t, policy)
			n := N_scaled(10_000)
			mk := func(i int) any {
				if kind == "sync" {
					return func() (any, error) { return i * 2, nil }
				}
				return coro(func(args ...any) (any, error) { return i * 2, nil })
			}
			tasks := make([]any, n)
			for i := range tasks {
				tasks[i] = mk(i)
			}
			var gf *laila.GroupFuture
			var out *py.List
			sw := Timed(func() {
				gf = gfut(must(submit_(tf, tasks)))
				out = unwrap_all(must(gf.Wait(T11_)))
			})
			want := py.NewList()
			for i := 0; i < n; i++ {
				want.Append(i * 2)
			}
			assertEq(t, out, want)
			// < 2ms per task end-to-end on a 2-CPU box
			assertTrue(t, sw.Elapsed < 0.004*float64(n)+5, sw.Elapsed)
			gf.Release()
			assertEq(t, policy.Future_bank.Len(), 0)
		})
	}
}

func TestThroughput_imap_streaming_fifty_thousand(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	n := N_scaled(50_000)
	tasks := make([]any, n)
	for i := range tasks {
		tasks[i] = lam(i)
	}
	futs := []any{}
	for f, err := range must(tf.Imap(tasks)) {
		check(t, err)
		futs = append(futs, f)
	}
	assertEq(t, len(futs), n)
	for _, f := range futs {
		must(Wait(f, T11_))
	}
	assertEq(t, Unwrap(must(cpf(futs[len(futs)-1]).Result())), n-1)
	Release_all(futs...)
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestThroughput_group_wait_timeout_is_per_child(t *testing.T) {
	// Documented: total wall time can be timeout * len(children).
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	tasks := make([]any, 20)
	for i := range tasks {
		tasks[i] = sleep_then(0.05, nil)
	}
	gf := gfut(must(submit_(tf, tasks)))
	sw := Timed(func() { must(gf.Wait(5.0)) })
	assertTrue(t, sw.Elapsed < 5.0)
	gf.Release()
}

func TestThroughput_submit_wait_true_returns_values(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	tasks := make([]any, 100)
	for i := range tasks {
		tasks[i] = lam(i)
	}
	out := must(submit_(tf, tasks, py.Kw{"wait": true}))
	assertEq(t, unwrap_all(out), range_list(100))
	single := must(submit_(tf, []any{lam("x")}, py.Kw{"wait": true}))
	assertEq(t, Unwrap(single), "x")
}

func TestThroughput_sustained_many_small_batches(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	rounds := N_scaled(300)
	for r := 0; r < rounds; r++ {
		tasks := make([]any, 10)
		want := py.NewList()
		for i := range tasks {
			tasks[i] = lam(r*10 + i)
			want.Append(r*10 + i)
		}
		gf := gfut(must(submit_(tf, tasks)))
		assertEq(t, unwrap_all(must(gf.Wait(T11_))), want)
		gf.Release()
	}
	assertEq(t, policy.Future_bank.Len(), 0)
	assertEq(t, tf.Inflight(), 0)
}

func TestThroughput_cpu_heavy_sync_bodies_respect_sync_workers(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{num_workers: 2, max_async_per_thread: 16, sync_workers: 2})
	defer drop_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	var lock sync.Mutex
	peak, cur := 0, 0
	body := func() (any, error) {
		lock.Lock()
		cur++
		peak = max(peak, cur)
		lock.Unlock()
		py.Sleep_sync(0.02)
		lock.Lock()
		cur--
		lock.Unlock()
		return nil, nil
	}
	tasks := make([]any, 60)
	for i := range tasks {
		tasks[i] = body
	}
	gf := gfut(must(submit_(tf, tasks)))
	must(gf.Wait(T11_))
	gf.Release()
	assertTrue(t, peak <= 2, peak)
}

func TestThroughput_async_bodies_interleave_up_to_cap(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{num_workers: 1, max_async_per_thread: 50})
	defer drop_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	var lock sync.Mutex
	peak, cur := 0, 0
	body := coro(func(args ...any) (any, error) {
		lock.Lock()
		cur++
		peak = max(peak, cur)
		lock.Unlock()
		if err := py.Sleep(0.05); err != nil {
			return nil, err
		}
		lock.Lock()
		cur--
		lock.Unlock()
		return nil, nil
	})
	tasks := make([]any, 200)
	for i := range tasks {
		tasks[i] = body
	}
	gf := gfut(must(submit_(tf, tasks)))
	must(gf.Wait(T11_))
	gf.Release()
	assertTrue(t, 40 <= peak && peak <= 50, peak)
}

func TestThroughput_dispatcher_balances_across_loops(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{num_workers: 4, max_async_per_thread: 4})
	defer drop_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	var lock sync.Mutex
	// ``threading.get_ident()`` identifies the loop *thread* in Python; the
	// port runs each task on its own goroutine, so the running event loop
	// (one per loop thread) is the twin of that identity.
	seen := map[*py.Loop]bool{}
	body := coro(func(args ...any) (any, error) {
		lock.Lock()
		seen[py.Current_loop()] = true
		lock.Unlock()
		return nil, py.Sleep(0.05)
	})
	tasks := make([]any, 64)
	for i := range tasks {
		tasks[i] = body
	}
	gf := gfut(must(submit_(tf, tasks)))
	must(gf.Wait(T11_))
	gf.Release()
	assertEq(t, len(seen), 4)
}

// ---------------------------------------------------------------------------
// nesting / parking on starved taskforces
// ---------------------------------------------------------------------------

func TestParking_deep_recursive_sync_nesting_on_1x1(t *testing.T) {
	for _, depth := range []int{10, 50, 200} {
		depth := depth
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			policy := Fresh_policy(t)
			tiny := tiny_fixture(t, policy)
			var rec func(d int) (any, error)
			rec = func(d int) (any, error) {
				if d == 0 {
					return 0, nil
				}
				f, err := submit_(tiny, []any{func() (any, error) { return rec(d - 1) }})
				if err != nil {
					return nil, err
				}
				v, err := Wait(f, T11_)
				if err != nil {
					return nil, err
				}
				return Unwrap(v).(int) + 1, nil
			}
			f := must(submit_(tiny, []any{func() (any, error) { return rec(depth) }}))
			assertEq(t, Unwrap(must(Wait(f, T11_))), depth)
			assertTrue(t, tiny.Inflight() == 0 && tiny.Parked() == 0)
		})
	}
}

func TestParking_deep_recursive_async_nesting_on_1x1(t *testing.T) {
	for _, depth := range []int{10, 100} {
		depth := depth
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			policy := Fresh_policy(t)
			tiny := tiny_fixture(t, policy)
			var rec func(d int) py.CoroutineFunc
			rec = func(d int) py.CoroutineFunc {
				return coro(func(args ...any) (any, error) {
					if d == 0 {
						return 0, nil
					}
					f, err := submit_(tiny, []any{rec(d - 1)})
					if err != nil {
						return nil, err
					}
					v, err := py.Await(f)
					if err != nil {
						return nil, err
					}
					return Unwrap(v).(int) + 1, nil
				})
			}
			f := must(submit_(tiny, []any{rec(depth)}))
			assertEq(t, Unwrap(must(Wait(f, T11_))), depth)
		})
	}
}

// sum_unwrapped is ``sum(unwrap(v) for v in vals)``.
func sum_unwrapped(vals any) (int, error) {
	total := 0
	for _, v := range py.Iter(vals) {
		n, err := py.Int(Unwrap(v))
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func TestParking_wide_fanout_from_inside_task_on_1x1(t *testing.T) {
	policy := Fresh_policy(t)
	tiny := tiny_fixture(t, policy)
	n := 500
	parent := func() (any, error) {
		tasks := make([]any, n)
		for i := range tasks {
			tasks[i] = lam(i)
		}
		gf, err := submit_(tiny, tasks)
		if err != nil {
			return nil, err
		}
		vals, err := Wait(gf, T11_)
		if err != nil {
			return nil, err
		}
		return sum_unwrapped(vals)
	}
	f := must(submit_(tiny, []any{parent}))
	assertEq(t, Unwrap(must(Wait(f, T11_))), sum_range(n))
}

func TestParking_many_concurrent_parents_each_fanning_out(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{num_workers: 2, max_async_per_thread: 2, sync_workers: 2})
	defer drop_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	parent := func(k int) (any, error) {
		tasks := make([]any, 20)
		for i := range tasks {
			tasks[i] = lam(i + k)
		}
		gf, err := submit_(tf, tasks)
		if err != nil {
			return nil, err
		}
		vals, err := Wait(gf, T11_)
		if err != nil {
			return nil, err
		}
		return sum_unwrapped(vals)
	}
	tasks := make([]any, 40)
	want := py.NewList()
	for k := range tasks {
		k := k
		tasks[k] = func() (any, error) { return parent(k) }
		s := 0
		for i := 0; i < 20; i++ {
			s += i + k
		}
		want.Append(s)
	}
	gf := gfut(must(submit_(tf, tasks)))
	assertEq(t, unwrap_all(must(gf.Wait(T11_))), want)
	gf.Release()
	assertTrue(t, tf.Inflight() == 0 && tf.Parked() == 0)
}

func TestParking_mixed_sync_async_tree(t *testing.T) {
	policy := Fresh_policy(t)
	tiny := tiny_fixture(t, policy)
	leaf := func(i int) py.CoroutineFunc {
		return coro(func(args ...any) (any, error) {
			if err := py.Sleep(0.001); err != nil {
				return nil, err
			}
			return i, nil
		})
	}
	mid := func(i int) (any, error) {
		f, err := submit_(tiny, []any{leaf(i)})
		if err != nil {
			return nil, err
		}
		v, err := Wait(f, T11_)
		if err != nil {
			return nil, err
		}
		return Unwrap(v).(int) * 2, nil
	}
	root := coro(func(args ...any) (any, error) {
		tasks := make([]any, 30)
		for i := range tasks {
			i := i
			tasks[i] = func() (any, error) { return mid(i) }
		}
		gf, err := submit_(tiny, tasks)
		if err != nil {
			return nil, err
		}
		vals, err := py.Await(gf)
		if err != nil {
			return nil, err
		}
		return sum_unwrapped(vals)
	})
	f := must(submit_(tiny, []any{root}))
	assertEq(t, Unwrap(must(Wait(f, T11_))), 2*sum_range(30))
}

func TestParking_group_future_await_from_coroutine_parks_once(t *testing.T) {
	policy := Fresh_policy(t)
	tiny := tiny_fixture(t, policy)
	root := coro(func(args ...any) (any, error) {
		tasks := make([]any, 50)
		for i := range tasks {
			tasks[i] = lam(i)
		}
		gf, err := submit_(tiny, tasks)
		if err != nil {
			return nil, err
		}
		vals, err := py.Await(gf)
		if err != nil {
			return nil, err
		}
		return sum_unwrapped(vals)
	})
	f := must(submit_(tiny, []any{root}))
	assertEq(t, Unwrap(must(Wait(f, T11_))), sum_range(50))
}

func TestParking_blocking_wait_inside_loop_thread_is_rejected(t *testing.T) {
	policy := Fresh_policy(t)
	tiny := tiny_fixture(t, policy)
	bad := coro(func(args ...any) (any, error) {
		f, err := submit_(tiny, []any{lam(1)})
		if err != nil {
			return nil, err
		}
		return Wait(f, T11_) // blocking wait on the loop thread
	})
	f := must(submit_(tiny, []any{bad}))
	if _, err := Wait(f, T11_); !errors.Is(err, laila.LoopBlockingWaitError) {
		t.Fatalf("want LoopBlockingWaitError, got %v", err)
	}
}

func TestParking_result_access_inside_sync_body_parks(t *testing.T) {
	policy := Fresh_policy(t)
	tiny := tiny_fixture(t, policy)
	parent := func() (any, error) {
		f, err := submit_(tiny, []any{lam(7)})
		if err != nil {
			return nil, err
		}
		// .result blocks; must park, not deadlock on 1x1
		v, err := Wait(f, T11_)
		if err != nil {
			return nil, err
		}
		return Unwrap(v), nil
	}
	assertEq(t, Unwrap(must(Wait(must(submit_(tiny, []any{parent})), T11_))), 7)
}

func TestParking_slot_ctx_visible_inside_task(t *testing.T) {
	policy := Fresh_policy(t)
	tiny := tiny_fixture(t, policy)
	body := func() (any, error) {
		s := laila.CURRENT_SLOT_.Value()
		return s != nil && py.Is(s.Tf, tiny), nil
	}
	assertTrue(t, Unwrap(must(Wait(must(submit_(tiny, []any{body})), T11_))) == true)
}

func TestParking_parked_counter_returns_to_zero_after_storm(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{num_workers: 1, max_async_per_thread: 2, sync_workers: 2})
	defer drop_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	parent := func() (any, error) {
		tasks := make([]any, 5)
		for i := range tasks {
			tasks[i] = sleep_then(0.005, nil)
		}
		gf, err := submit_(tf, tasks)
		if err != nil {
			return nil, err
		}
		if _, err := Wait(gf, T11_); err != nil {
			return nil, err
		}
		return 1, nil
	}
	tasks := make([]any, 100)
	for i := range tasks {
		tasks[i] = parent
	}
	gf := gfut(must(submit_(tf, tasks)))
	assertEq(t, must(sum_unwrapped(must(gf.Wait(T11_)))), 100)
	gf.Release()
	assertTrue(t, Wait_until(func() bool { return tf.Inflight() == 0 && tf.Parked() == 0 }, 10))
}

func TestParking_remember_inside_task_on_1x1(t *testing.T) {
	policy := Fresh_policy(t)
	tiny := tiny_fixture(t, policy)
	e := must(laila.Entry_constant(123))
	must(Wait(must(laila.Memorize(e)), T11_))
	body := func() (any, error) {
		f, err := laila.Remember(e.Global_id())
		if err != nil {
			return nil, err
		}
		v, err := Wait(f, T11_)
		if err != nil {
			return nil, err
		}
		return Unwrap(v), nil
	}
	assertEq(t, Unwrap(must(Wait(must(submit_(tiny, []any{body})), T11_))), 123)
}

// ---------------------------------------------------------------------------
// cycles & ranks
// ---------------------------------------------------------------------------

func TestGuards_rank_violation_from_inside_task(t *testing.T) {
	policy := Fresh_policy(t)
	low := new_tf_(policy, tf_kw{rank: 1})
	high := new_tf_(policy, tf_kw{rank: 3})
	defer drop_(policy, low, py.Kw{"wait": true, "cancel_pending": true})
	defer drop_(policy, high, py.Kw{"wait": true, "cancel_pending": true})
	body := func() (any, error) {
		f, err := submit_(high, []any{lam(1)})
		if err != nil {
			return nil, err
		}
		return Wait(f, T11_)
	}
	f := must(submit_(low, []any{body}))
	if _, err := Wait(f, T11_); !errors.Is(err, laila.NestedCommandSubmitError) {
		t.Fatalf("want NestedCommandSubmitError, got %v", err)
	}
}

func TestGuards_downward_rank_allowed(t *testing.T) {
	policy := Fresh_policy(t)
	low := new_tf_(policy, tf_kw{rank: 1})
	high := new_tf_(policy, tf_kw{rank: 3})
	defer drop_(policy, low, py.Kw{"wait": true, "cancel_pending": true})
	defer drop_(policy, high, py.Kw{"wait": true, "cancel_pending": true})
	body := func() (any, error) {
		f, err := submit_(low, []any{lam(5)})
		if err != nil {
			return nil, err
		}
		v, err := Wait(f, T11_)
		if err != nil {
			return nil, err
		}
		return Unwrap(v), nil
	}
	assertEq(t, Unwrap(must(Wait(must(submit_(high, []any{body})), T11_))), 5)
}

// wl11_self_ is ``builtins._wl11_self``.
var (
	wl11_self_mu_ sync.Mutex
	wl11_self_    *laila.Entry
)

const WL11_SELF_CODE_ = "def f(manifest):\n" +
	"    import builtins, laila\n" +
	"    e = builtins._wl11_self\n" +
	"    return laila.build(e).wait(60)\n"

func init() {
	laila.Register_constitution(WL11_SELF_CODE_, func(arg any) (any, error) {
		wl11_self_mu_.Lock()
		e := wl11_self_
		wl11_self_mu_.Unlock()
		if e == nil {
			return nil, py.AttributeError.New("module 'builtins' has no attribute '_wl11_self'")
		}
		f, err := laila.Build(e)
		if err != nil {
			return nil, err
		}
		return Wait(f, 60)
	})
}

func TestGuards_self_cycle_via_build_is_detected(t *testing.T) {
	policy := Fresh_policy(t)
	tiny := tiny_fixture(t, policy)
	m := must(laila.New_Manifest(py.Kw{"data": py.DictOf("a", must(laila.Entry_constant(1)))}))
	must(must(m.Memorize()).Wait(T11_))
	e := must(laila.Variable(py.Kw{"constitution": WL11_SELF_CODE_, "manifest": m}))
	wl11_self_mu_.Lock()
	wl11_self_ = e
	wl11_self_mu_.Unlock()
	defer func() {
		wl11_self_mu_.Lock()
		wl11_self_ = nil
		wl11_self_mu_.Unlock()
	}()
	f, err := laila.Build(e, py.Kw{"taskforce_id": tiny.Global_id()})
	if err == nil {
		_, err = Wait(f, T11_)
	}
	if !(errors.Is(err, laila.CyclicDependencyError) || errors.Is(err, py.RuntimeError)) {
		t.Fatalf("want CyclicDependencyError/RuntimeError, got %v", err)
	}
}

func TestGuards_submit_to_unknown_taskforce_raises(t *testing.T) {
	policy := Fresh_policy(t)
	_, err := Cmd(policy).Submit([]any{lam(1)}, py.Kw{"taskforce_id": "LAILA:TASKFORCE:nope"})
	if !errors.Is(err, py.KeyError) {
		t.Fatalf("want KeyError, got %v", err)
	}
}

func TestGuards_submit_to_stopped_taskforce_raises(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{})
	check(t, tf.Shutdown(py.Kw{"wait": true}))
	defer Cmd(policy).Taskforces.PopDefault(tf.Global_id(), nil)
	if _, err := submit_(tf, []any{lam(1)}); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("want RuntimeError, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// failures
// ---------------------------------------------------------------------------

func TestFailures11_thousand_raising_tasks(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	boom := func(i int) (any, error) { return nil, py.ValueError.Newf("boom-%d", i) }
	n := N_scaled(1000)
	futs := make([]any, n)
	for i := range futs {
		i := i
		futs[i] = must(submit_(tf, []any{func() (any, error) { return boom(i) }}))
	}
	for i, f := range futs {
		_, err := Wait(f, T11_)
		if !errors.Is(err, py.ValueError) || !regexp.MustCompile(fmt.Sprintf("boom-%d$", i)).MatchString(err.Error()) {
			t.Fatalf("want ValueError boom-%d, got %v", i, err)
		}
		assertEq(t, cpf(f).Status(), laila.FutureStatus_ERROR)
		assertTrue(t, errors.Is(cpf(f).Exception(), py.ValueError))
	}
	Release_all(futs...)
	assertEq(t, tf.Inflight(), 0)
}

func TestFailures11_group_wait_raises_first_child_error_but_others_finish(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	var mu sync.Mutex
	done := []int{}
	ok := func(i int) (any, error) {
		py.Sleep_sync(0.01)
		mu.Lock()
		done = append(done, i)
		mu.Unlock()
		return i, nil
	}
	bad := func() (any, error) { return nil, py.KeyError.New("x") }
	tasks := []any{}
	for i := 0; i < 10; i++ {
		i := i
		tasks = append(tasks, func() (any, error) { return ok(i) })
	}
	tasks = append(tasks, bad)
	for i := 10; i < 20; i++ {
		i := i
		tasks = append(tasks, func() (any, error) { return ok(i) })
	}
	gf := gfut(must(submit_(tf, tasks)))
	if _, err := gf.Wait(T11_); !errors.Is(err, py.KeyError) {
		t.Fatalf("want KeyError, got %v", err)
	}
	assertTrue(t, Wait_until(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(done) == 20
	}, 20))
	gf.Release()
}

func TestFailures11_exception_in_nested_child_propagates_through_parent(t *testing.T) {
	policy := Fresh_policy(t)
	tiny := tiny_fixture(t, policy)
	child := func() (any, error) { return nil, py.RuntimeError.New("inner") }
	parent := func() (any, error) {
		f, err := submit_(tiny, []any{child})
		if err != nil {
			return nil, err
		}
		return Wait(f, T11_)
	}
	_, err := Wait(must(submit_(tiny, []any{parent})), T11_)
	if !errors.Is(err, py.RuntimeError) || !strings.Contains(err.Error(), "inner") {
		t.Fatalf("want RuntimeError inner, got %v", err)
	}
	assertTrue(t, tiny.Inflight() == 0 && tiny.Parked() == 0)
}

var Weird = py.NewType("Weird", py.Exception)

func TestFailures11_base_exception_subclass_in_task(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	body := func() (any, error) { return nil, Weird.New() }
	f := must(submit_(tf, []any{body}))
	if _, err := Wait(f, T11_); !errors.Is(err, Weird) {
		t.Fatalf("want Weird, got %v", err)
	}
}

func TestFailures11_timeout_then_eventual_completion(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	f := must(submit_(tf, []any{sleep_then(0.5, 9)}))
	if _, err := Wait(f, 0.05); !errors.Is(err, py.TimeoutError) {
		t.Fatalf("want TimeoutError, got %v", err)
	}
	assertEq(t, Unwrap(must(Wait(f, T11_))), 9)
	assertEq(t, cpf(f).Status(), laila.FutureStatus_FINISHED)
}

func TestFailures11_many_timeouts_do_not_leak_subscriptions(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	f := must(submit_(tf, []any{sleep_then(1.0, 1)}))
	for i := 0; i < 200; i++ {
		if _, err := Wait(f, 0.001); !errors.Is(err, py.TimeoutError) {
			t.Fatalf("want TimeoutError, got %v", err)
		}
	}
	assertEq(t, Unwrap(must(Wait(f, T11_))), 1)
}

func TestFailures11_coroutine_raising_after_await(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	body := coro(func(args ...any) (any, error) {
		if err := py.Sleep(0.01); err != nil {
			return nil, err
		}
		return nil, py.IndexError.New("late")
	})
	if _, err := Wait(must(submit_(tf, []any{body})), T11_); !errors.Is(err, py.IndexError) {
		t.Fatalf("want IndexError, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------

func all_status(futs []any, s laila.FutureStatus) bool {
	for _, f := range futs {
		if cpf(f).Status() != s {
			return false
		}
	}
	return true
}

func TestLifecycle_shutdown_wait_completes_inflight(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{num_workers: 2, max_async_per_thread: 4, sync_workers: 4})
	gate := py.NewEvent()
	var mu sync.Mutex
	done := []int{}
	body := func(i int) (any, error) {
		gate.Wait(30)
		mu.Lock()
		done = append(done, i)
		mu.Unlock()
		return nil, nil
	}
	futs := make([]any, 8)
	for i := range futs {
		i := i
		futs[i] = must(submit_(tf, []any{func() (any, error) { return body(i) }}))
	}
	assertTrue(t, Wait_until(func() bool { return all_status(futs, laila.FutureStatus_RUNNING) }, 10))
	gate.Set()
	check(t, tf.Shutdown(py.Kw{"wait": true, "cancel_pending": false}))
	Cmd(policy).Taskforces.PopDefault(tf.Global_id(), nil)
	assertEq(t, tf.Status, laila.TaskForceStatus_STOPPED)
	assertEq(t, len(done), 8)
	assertTrue(t, all_status(futs, laila.FutureStatus_FINISHED))
	Release_all(futs...)
}

func TestLifecycle_shutdown_wait_without_cancel_runs_or_cancels_queued(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{num_workers: 1, max_async_per_thread: 1, sync_workers: 1})
	gate := py.NewEvent()
	first := must(submit_(tf, []any{func() (any, error) { return gate.Wait(30), nil }}))
	assertTrue(t, Wait_until(func() bool { return cpf(first).Status() == laila.FutureStatus_RUNNING }, 10))
	queued := make([]any, 10)
	for i := range queued {
		queued[i] = must(submit_(tf, []any{lam(i)}))
	}
	gate.Set()
	func() {
		defer Cmd(policy).Taskforces.PopDefault(tf.Global_id(), nil)
		check(t, tf.Shutdown(py.Kw{"wait": true, "cancel_pending": false}))
	}()
	cpf(first).Release()
	defer Release_all(queued...)
	for _, f := range queued {
		st := cpf(f).Status()
		assertTrue(t, st == laila.FutureStatus_FINISHED || st == laila.FutureStatus_CANCELLED, st)
	}
	for _, f := range queued {
		if _, err := Wait(f, 2.0); err != nil && !errors.Is(err, py.RuntimeError) {
			t.Fatal(err)
		}
	}
}

func TestLifecycle_shutdown_cancel_pending_marks_queued_cancelled(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{num_workers: 1, max_async_per_thread: 1, sync_workers: 1})
	gate := py.NewEvent()
	blocker := func() (any, error) { return gate.Wait(30), nil }
	first := must(submit_(tf, []any{blocker}))
	assertTrue(t, Wait_until(func() bool { return cpf(first).Status() == laila.FutureStatus_RUNNING }, 10))
	rest := make([]any, 50)
	for i := range rest {
		rest[i] = must(submit_(tf, []any{lam(1)}))
	}
	gate.Set()
	check(t, tf.Shutdown(py.Kw{"wait": true, "cancel_pending": true}))
	Cmd(policy).Taskforces.PopDefault(tf.Global_id(), nil)
	statuses := map[laila.FutureStatus]bool{}
	for _, f := range rest {
		statuses[cpf(f).Status()] = true
	}
	for s := range statuses {
		assertTrue(t, s == laila.FutureStatus_CANCELLED || s == laila.FutureStatus_FINISHED, s)
	}
	assertTrue(t, statuses[laila.FutureStatus_CANCELLED])
	for _, f := range rest {
		if cpf(f).Status() == laila.FutureStatus_CANCELLED {
			if _, err := Wait(f, 1); !errors.Is(err, py.RuntimeError) {
				t.Fatalf("want RuntimeError, got %v", err)
			}
		}
	}
	cpf(first).Release()
	Release_all(rest...)
}

func TestLifecycle_shutdown_nowait_cancel_inflight_coroutines(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{num_workers: 1, max_async_per_thread: 4})
	var mu sync.Mutex
	cancelled := 0
	body := coro(func(args ...any) (any, error) {
		err := py.Sleep(30)
		if errors.Is(err, py.CancelledError) {
			mu.Lock()
			cancelled++
			mu.Unlock()
		}
		return nil, err
	})
	futs := make([]any, 4)
	for i := range futs {
		futs[i] = must(submit_(tf, []any{body}))
	}
	assertTrue(t, Wait_until(func() bool { return all_status(futs, laila.FutureStatus_RUNNING) }, 10))
	check(t, tf.Shutdown(py.Kw{"wait": true, "cancel_pending": true}))
	Cmd(policy).Taskforces.PopDefault(tf.Global_id(), nil)
	assertTrue(t, all_status(futs, laila.FutureStatus_CANCELLED))
	mu.Lock()
	assertEq(t, cancelled, 4)
	mu.Unlock()
	Release_all(futs...)
}

func TestLifecycle_pause_rejects_submit_and_resume_accepts(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{})
	defer drop_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	assertEq(t, Unwrap(must(Wait(must(submit_(tf, []any{lam(1)})), T11_))), 1)
	check(t, tf.Pause())
	assertEq(t, tf.Status, laila.TaskForceStatus_PAUSED)
	if _, err := submit_(tf, []any{lam(2)}); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("want RuntimeError, got %v", err)
	}
	check(t, tf.Start())
	assertEq(t, Unwrap(must(Wait(must(submit_(tf, []any{lam(3)})), T11_))), 3)
}

// thread_names_matching is ``[t.name for t in threading.enumerate() if pred(t.name)]``.
func thread_names_matching(pred func(name string) bool) []string {
	out := []string{}
	for _, th := range py.Enumerate() {
		if pred(th.Name()) {
			out = append(out, th.Name())
		}
	}
	sort.Strings(out)
	return out
}

func TestLifecycle_repeated_pause_resume_cycles(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{num_workers: 2, max_async_per_thread: 4})
	defer drop_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	for i := 0; i < 50; i++ {
		check(t, tf.Pause())
		check(t, tf.Start())
		assertEq(t, Unwrap(must(Wait(must(submit_(tf, []any{lam(i)})), T11_))), i)
	}
	gid := tf.Global_id()
	tag := gid[len(gid)-8:]
	loops := thread_names_matching(func(n string) bool { return strings.Contains(n, fmt.Sprintf("AsyncTF-%s-Loop", tag)) })
	// pause/start must not re-spawn loop threads
	assertTrue(t, len(loops) == 2, loops)
}

func TestLifecycle_shutdown_twice_is_idempotent(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{})
	check(t, tf.Shutdown(py.Kw{"wait": true}))
	check(t, tf.Shutdown(py.Kw{"wait": true}))
	Cmd(policy).Taskforces.PopDefault(tf.Global_id(), nil)
	assertEq(t, tf.Status, laila.TaskForceStatus_STOPPED)
}

func TestLifecycle_threads_reclaimed_after_shutdown(t *testing.T) {
	policy := Fresh_policy(t)
	tag_ids := []string{}
	for i := 0; i < 10; i++ {
		tf := new_tf_(policy, tf_kw{num_workers: 2, max_async_per_thread: 2})
		gid := tf.Global_id()
		tag_ids = append(tag_ids, gid[len(gid)-8:])
		tasks := make([]any, 10)
		for j := range tasks {
			tasks[j] = lam(1)
		}
		must(Wait(must(submit_(tf, tasks)), T11_))
		drop_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	}
	time.Sleep(500 * time.Millisecond)
	live := thread_names_matching(func(n string) bool {
		for _, tg := range tag_ids {
			if strings.Contains(n, tg) {
				return true
			}
		}
		return false
	})
	assertTrue(t, len(live) == 0, live)
}

func TestLifecycle_restart_after_shutdown(t *testing.T) {
	policy := Fresh_policy(t)
	tf := new_tf_(policy, tf_kw{})
	check(t, tf.Shutdown(py.Kw{"wait": true}))
	check(t, tf.Start())
	defer drop_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
	assertEq(t, tf.Status, laila.TaskForceStatus_RUNNING)
	assertEq(t, Unwrap(must(Wait(must(submit_(tf, []any{lam(42)})), T11_))), 42)
}

func TestLifecycle_context_manager_usage(t *testing.T) {
	policy := Fresh_policy(t)
	tf := must(laila.New_PythonAsyncThreadPoolTaskForce(py.Kw{"policy_id": policy.Global_id(), "num_workers": 1}))
	Cmd(policy).Taskforces.Set(tf.Global_id(), tf)
	defer Cmd(policy).Taskforces.PopDefault(tf.Global_id(), nil)
	must(tf.Enter())
	var body_err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				body_err = py.PanicToError(r)
			}
		}()
		assertEq(t, Unwrap(must(Wait(must(submit_(tf, []any{lam(1)})), T11_))), 1)
	}()
	check(t, tf.Exit(body_err))
	check(t, body_err)
	assertEq(t, tf.Status, laila.TaskForceStatus_STOPPED)
}

func TestLifecycle_many_taskforces_concurrently(t *testing.T) {
	policy := Fresh_policy(t)
	tfs := make([]*laila.PythonAsyncThreadPoolTaskForce, 16)
	for i := range tfs {
		tfs[i] = new_tf_(policy, tf_kw{num_workers: 1, max_async_per_thread: 4})
	}
	defer func() {
		for _, tf := range tfs {
			drop_(policy, tf, py.Kw{"wait": true, "cancel_pending": true})
		}
	}()
	gfs := []*laila.GroupFuture{}
	for _, tf := range tfs {
		tasks := make([]any, 50)
		for i := range tasks {
			tasks[i] = lam(i)
		}
		gfs = append(gfs, gfut(must(submit_(tf, tasks))))
	}
	for _, gf := range gfs {
		assertEq(t, unwrap_all(must(gf.Wait(T11_))), range_list(50))
		gf.Release()
	}
}

func TestLifecycle_submit_from_many_threads_simultaneously(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	worker := func(i int) (any, error) {
		tasks := make([]any, 100)
		want := py.NewList()
		for j := range tasks {
			tasks[j] = lam(i*1000 + j)
			want.Append(i*1000 + j)
		}
		gf, err := submit_(tf, tasks)
		if err != nil {
			return nil, err
		}
		vals, err := gfut(gf).Wait(T11_)
		if err != nil {
			return nil, err
		}
		gfut(gf).Release()
		return py.Eq(unwrap_all(vals), want), nil
	}
	out := must(Run_threads(worker, 16))
	for _, ok := range out {
		assertTrue(t, ok == true)
	}
	assertEq(t, policy.Future_bank.Len(), 0)
}

// ---------------------------------------------------------------------------
// future bank / callbacks / guarantee
// ---------------------------------------------------------------------------

func TestFutureBank_bank_grows_without_release(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	n := N_scaled(5000)
	futs := make([]any, n)
	for i := range futs {
		futs[i] = must(submit_(tf, []any{lam(1)}))
	}
	for _, f := range futs {
		must(Wait(f, T11_))
	}
	assertEq(t, policy.Future_bank.Len(), n)
	Release_all(futs...)
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestFutureBank_group_release_releases_children(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	tasks := make([]any, 500)
	for i := range tasks {
		tasks[i] = lam(1)
	}
	gf := gfut(must(submit_(tf, tasks)))
	must(gf.Wait(T11_))
	assertEq(t, policy.Future_bank.Len(), 501)
	gf.Release()
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestFutureBank_group_release_children_false_keeps_children(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	tasks := make([]any, 10)
	for i := range tasks {
		tasks[i] = lam(1)
	}
	gf := gfut(must(submit_(tf, tasks)))
	must(gf.Wait(T11_))
	gf.Release_with(py.Kw{"children": false})
	assertEq(t, policy.Future_bank.Len(), 10)
	for _, fid := range gf.Future_ids {
		must(policy.Future_bank.Getitem(fid)).(interface{ Release() }).Release()
	}
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestFutureBank_release_twice_is_idempotent(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	f := cpf(must(submit_(tf, []any{lam(1)})))
	must(f.Wait(T11_))
	f.Release()
	f.Release()
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestFutureBank_status_callbacks_fire_once_each(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	var lock sync.Mutex
	counts := map[laila.FutureStatus]int{}
	mk := func(s laila.FutureStatus) laila.Future_callback {
		return func(_f any) {
			lock.Lock()
			counts[s]++
			lock.Unlock()
		}
	}
	futs := []any{}
	for i := 0; i < 200; i++ {
		f := cpf(must(submit_(tf, []any{lam(1)})))
		for _, s := range []laila.FutureStatus{laila.FutureStatus_RUNNING, laila.FutureStatus_FINISHED} {
			f.Add_status_callback(s, mk(s))
		}
		futs = append(futs, f)
	}
	for _, f := range futs {
		must(Wait(f, T11_))
	}
	lock.Lock()
	assertEq(t, counts[laila.FutureStatus_FINISHED], 200)
	lock.Unlock()
	// late registration on an already-fired status fires immediately
	late := 0
	cpf(futs[0]).Add_status_callback(laila.FutureStatus_FINISHED, func(_f any) { late = 1 })
	assertEq(t, late, 1)
	Release_all(futs...)
}

func TestFutureBank_callback_raising_does_not_poison_future(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	f := cpf(must(submit_(tf, []any{lam(5)})))
	bad := func(_f any) { panic(py.RuntimeError.New("cb")) }
	f.Add_status_callback(laila.FutureStatus_FINISHED, bad)
	v, err := f.Wait(T11_)
	if err != nil {
		t.Fatalf("callback exception leaked into wait: %v", err)
	}
	assertEq(t, Unwrap(v), 5)
	f.Release()
}

func TestFutureBank_guarantee_waits_for_all(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	var mu sync.Mutex
	done := []int{}
	body := func(i int) (any, error) {
		py.Sleep_sync(0.01)
		mu.Lock()
		done = append(done, i)
		mu.Unlock()
		return nil, nil
	}
	check(t, laila.Guarantee.With(func() error {
		for i := 0; i < 100; i++ {
			i := i
			if _, err := submit_(tf, []any{func() (any, error) { return body(i) }}); err != nil {
				return err
			}
		}
		return nil
	}))
	mu.Lock()
	assertEq(t, len(done), 100)
	mu.Unlock()
	Quiesce(policy)
}

func TestFutureBank_guarantee_reraises_first_error(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	bad := func() (any, error) { return nil, py.ValueError.New("g") }
	err := laila.Guarantee.With(func() error {
		for i := 0; i < 20; i++ {
			if _, err := submit_(tf, []any{lam(1)}); err != nil {
				return err
			}
		}
		_, err := submit_(tf, []any{bad})
		return err
	})
	if !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
	Quiesce(policy)
}

func TestFutureBank_nested_guarantee_scopes(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	var mu sync.Mutex
	order := []string{}
	appender := func(s float64, tag string) any {
		return func() (any, error) {
			py.Sleep_sync(s)
			mu.Lock()
			order = append(order, tag)
			mu.Unlock()
			return nil, nil
		}
	}
	snapshot := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), order...)
	}
	check(t, laila.Guarantee.With(func() error {
		if _, err := submit_(tf, []any{appender(0.2, "outer")}); err != nil {
			return err
		}
		if err := laila.Guarantee.With(func() error {
			_, err := submit_(tf, []any{appender(0.05, "inner")})
			return err
		}); err != nil {
			return err
		}
		if got := snapshot(); !py.Eq(got, []string{"inner"}) {
			return py.AssertionError.Newf("%v", got)
		}
		return nil
	}))
	assertEq(t, snapshot(), []string{"inner", "outer"})
	Quiesce(policy)
}

func TestFutureBank_guarantee_async(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	var mu sync.Mutex
	done := []int{}
	main := func() (any, error) {
		if err := laila.Guarantee_async.With(func() error {
			for i := 0; i < 50; i++ {
				i := i
				if _, err := submit_(tf, []any{func() (any, error) {
					mu.Lock()
					done = append(done, i)
					mu.Unlock()
					return nil, nil
				}}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		return len(done), nil
	}
	assertEq(t, must(Run(main)), 50)
	Quiesce(policy)
}

func TestFutureBank_future_result_is_entry(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	f := cpf(must(submit_(tf, []any{lam(py.DictOf("a", 1))})))
	must(f.Wait(T11_))
	res := must(f.Result())
	e := laila.As_entry_(res)
	assertTrue(t, e != nil, py.Pytypename(res))
	assertEq(t, must(f.Data()), py.DictOf("a", 1))
	assertEq(t, *must(f.Result_global_id()), e.Global_id())
	f.Release()
}

func TestFutureBank_future_result_entry_passthrough(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	e := must(laila.Entry_constant(9))
	f := cpf(must(submit_(tf, []any{lam(e)})))
	must(f.Wait(T11_))
	assertTrue(t, py.Is(must(f.Result()), e))
	f.Release()
}

func TestFutureBank_runtime_status_wait_helpers(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	f := cpf(must(submit_(tf, []any{lam(3)})))
	assertEq(t, Unwrap(must(laila.Wait(f.Global_id(), T11_))), 3)
	assertEq(t, must(laila.Status(f.Global_id())), laila.FutureStatus_FINISHED)
	f.Release()
}

func TestFutureBank_group_future_composition(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	ones, twos := make([]any, 5), make([]any, 5)
	for i := range ones {
		ones[i], twos[i] = lam(1), lam(2)
	}
	a := gfut(must(submit_(tf, ones)))
	b := gfut(must(submit_(tf, twos)))
	c := must(a.Add(b))
	assertEq(t, c.Len(), 10)
	vals := unwrap_all(must(c.Wait(T11_))).Items()
	ints := []int{}
	for _, v := range vals {
		ints = append(ints, v.(int))
	}
	sort.Ints(ints)
	assertEq(t, ints, []int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2})
	c.Release()
	a.Release_with(py.Kw{"children": false})
	b.Release_with(py.Kw{"children": false})
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestFutureBank_group_status_percentages(t *testing.T) {
	policy := Fresh_policy(t)
	tf := tf_fixture(t, policy)
	gate := py.NewEvent()
	tasks := make([]any, 10)
	for i := range tasks {
		tasks[i] = func() (any, error) { return gate.Wait(30), nil }
	}
	gf := gfut(must(submit_(tf, tasks)))
	pct := func(key string) float64 {
		st := must(gf.Status())
		return py.Must(py.Float(must(must(st.Getitem("percentages")).(*py.Dict).Getitem(key))))
	}
	assertTrue(t, Wait_until(func() bool { return pct("running") > 0 }, 10))
	gate.Set()
	must(gf.Wait(T11_))
	st := must(gf.Status())
	assertEq(t, pct("finished"), 100.0)
	assertEq(t, must(st.Getitem("total")), 10)
	gf.Release()
}
