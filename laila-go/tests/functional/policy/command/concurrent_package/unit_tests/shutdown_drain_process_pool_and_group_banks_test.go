// Port of tests/functional/policy/command/concurrent_package/unit_tests/
// test_shutdown_drain_process_pool_and_group_banks.py.
//
// Phase 4 regressions: graceful shutdown drains the queue (F-20), process
// pools are reachable through ``Command.submit`` / ``laila.build``
// (F-21), a dead worker does not wedge the dispatcher (F-22), group
// futures resolve their children across policies (F-23),
// ``submit(wait=True)`` releases the futures it consumed (F-24) and idle
// sync-executor threads are reaped (F-W24-02).
package unit_tests

import (
	"errors"
	"os"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T_ = 60.0

func pid_(args ...any) (any, error) { return os.Getpid(), nil }

func exit_hard_(args ...any) (any, error) {
	code, _ := py.Int(args[0])
	py.Exit_(code)
	return nil, nil
}

var (
	pid_task       = laila.Register_process_task("test_shutdown_drain_process_pool_and_group_banks._pid", pid_)
	exit_hard_task = laila.Register_process_task("test_shutdown_drain_process_pool_and_group_banks._exit_hard", exit_hard_)
)

// unwrap_ is ``getattr(value, "data", value)``.
func unwrap_(t *testing.T, value any) any {
	t.Helper()
	if e, ok := value.(*laila.Entry); ok {
		d, err := e.Data()
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	return value
}

func wait_until_(pred func() bool, timeout float64) bool {
	deadline := py.Monotonic() + timeout
	for py.Monotonic() < deadline {
		if pred() {
			return true
		}
		py.Sleep_sync(0.01)
	}
	return pred()
}

// case_ is the ``_Case`` fixture: a fresh activated policy per test.
type case_ struct {
	t        *testing.T
	original any
	policy   *laila.LAILA_IDENTIFIABLE_POLICY_
	command  *laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_
}

func setUp_(t *testing.T) *case_ {
	t.Helper()
	c := &case_{t: t}
	c.original = must_active_policy(t)
	policy, err := laila.New_LAILA_IDENTIFIABLE_POLICY_()
	if err != nil {
		t.Fatal(err)
	}
	c.policy = policy
	laila.Activate_policy(policy)
	c.command = policy.Central.Command.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_)
	t.Cleanup(c.tearDown)
	return c
}

func must_active_policy(t *testing.T) any {
	t.Helper()
	p, err := laila.Get_active_policy()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (c *case_) tearDown() {
	_ = c.command.Shutdown(py.Kw{"wait": true, "cancel_pending": true})
	laila.Activate_policy(c.original)
	laila.Local_policies_.PopDefault(c.policy.Global_id(), nil)
}

func (c *case_) attach(tf any) any {
	c.command.Taskforces.Set(global_id(tf), tf)
	return tf
}

func (c *case_) drop(tf any, kw py.Kw) {
	defer c.command.Taskforces.PopDefault(global_id(tf), nil)
	if err := tf.(interface{ Shutdown(args ...any) error }).Shutdown(kw); err != nil {
		c.t.Fatal(err)
	}
}

func global_id(obj any) string {
	return obj.(interface{ Global_id() string }).Global_id()
}

func (c *case_) submit(tasks []any, kw py.Kw) any {
	c.t.Helper()
	out, err := c.command.Submit(tasks, kw)
	if err != nil {
		c.t.Fatal(err)
	}
	return out
}

func (c *case_) new_async_tf() *laila.PythonAsyncThreadPoolTaskForce {
	c.t.Helper()
	tf, err := laila.New_PythonAsyncThreadPoolTaskForce(py.Kw{
		"policy_id":            c.policy.Global_id(),
		"num_workers":          1,
		"max_async_per_thread": 1,
		"sync_workers":         1,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.attach(tf)
	return tf
}

func status_of(f any) laila.FutureStatus {
	return f.(interface{ Status() laila.FutureStatus }).Status()
}

func wait_(t *testing.T, f any, timeout any) any {
	t.Helper()
	var (
		v   any
		err error
	)
	if timeout == nil {
		v, err = callm_wait(f, py.Kw{"timeout": nil})
	} else {
		v, err = callm_wait(f, py.Kw{"timeout": timeout})
	}
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func callm_wait(f any, kw py.Kw) (any, error) {
	m, err := py.Getattr(f, "wait")
	if err != nil {
		return nil, err
	}
	return py.Call(m, nil, kw)
}

func release_(f any) { f.(interface{ Release() }).Release() }

// ---------------------------------------------------------------------------
// TestShutdownDrain
// ---------------------------------------------------------------------------

func TestShutdownDrain_wait_true_runs_queued_tasks(t *testing.T) {
	// F-20: ``shutdown(wait=True)`` dispatches and finishes queued tasks.
	c := setUp_(t)
	tf := c.new_async_tf()
	gate := py.NewEvent()
	first := c.submit([]any{func() (any, error) { return gate.Wait(30), nil }}, py.Kw{"taskforce_id": tf.Global_id()})
	if !wait_until_(func() bool { return status_of(first) == laila.FutureStatus_RUNNING }, 10) {
		t.Fatal("first task never started")
	}
	queued := []any{}
	for i := 0; i < 10; i++ {
		i := i
		queued = append(queued, c.submit([]any{func() (any, error) { return i, nil }}, py.Kw{"taskforce_id": tf.Global_id()}))
	}
	gate.Set()
	c.drop(tf, py.Kw{"wait": true, "cancel_pending": false})
	for i, f := range queued {
		if st := status_of(f); st != laila.FutureStatus_FINISHED {
			t.Fatalf("queued[%d] status %v", i, st)
		}
	}
	for i, f := range queued {
		if v := unwrap_(t, wait_(t, f, 1.0)); !py.Eq(v, i) {
			t.Fatalf("queued[%d] = %v", i, v)
		}
	}
	if tf.Status != laila.TaskForceStatus_STOPPED {
		t.Fatalf("tf status %v", tf.Status)
	}
	release_(first)
	for _, f := range queued {
		release_(f)
	}
}

func TestShutdownDrain_cancel_pending_still_cancels(t *testing.T) {
	c := setUp_(t)
	tf := c.new_async_tf()
	gate := py.NewEvent()
	first := c.submit([]any{func() (any, error) { return gate.Wait(30), nil }}, py.Kw{"taskforce_id": tf.Global_id()})
	if !wait_until_(func() bool { return status_of(first) == laila.FutureStatus_RUNNING }, 10) {
		t.Fatal("first task never started")
	}
	queued := []any{}
	for i := 0; i < 5; i++ {
		queued = append(queued, c.submit([]any{func() (any, error) { return 1, nil }}, py.Kw{"taskforce_id": tf.Global_id()}))
	}
	// Python sets the gate *before* shutting down and relies on the GIL
	// keeping the main thread ahead of the dispatcher, so the cancel pass
	// runs before queued[0] is placed on the freed slot. Go schedules the
	// dispatcher truly in parallel, so release the gate only after the
	// shutdown has flagged the stop (the single slot is still held by
	// ``first`` until then, so nothing can be dispatched in between).
	go func() {
		time.Sleep(200 * time.Millisecond)
		gate.Set()
	}()
	c.drop(tf, py.Kw{"wait": true, "cancel_pending": true})
	for i, f := range queued {
		if st := status_of(f); st != laila.FutureStatus_CANCELLED {
			t.Fatalf("queued[%d] status %v", i, st)
		}
	}
	release_(first)
	for _, f := range queued {
		release_(f)
	}
}

func TestShutdownDrain_shutdown_from_own_task_does_not_deadlock(t *testing.T) {
	// F-20 self-call guard: draining from inside a task would wait for
	// itself.
	c := setUp_(t)
	tf := c.new_async_tf()
	done := py.NewEvent()
	body := func() (any, error) {
		defer done.Set()
		return nil, tf.Shutdown(py.Kw{"wait": true, "cancel_pending": false})
	}
	fut := c.submit([]any{body}, py.Kw{"taskforce_id": tf.Global_id()})
	if !done.Wait(T_) {
		t.Fatal("body did not finish")
	}
	if tf.Status != laila.TaskForceStatus_STOPPED {
		t.Fatalf("tf status %v", tf.Status)
	}
	c.command.Taskforces.PopDefault(tf.Global_id(), nil)
	release_(fut)
}

// ---------------------------------------------------------------------------
// TestSubmitWaitReleases
// ---------------------------------------------------------------------------

func TestSubmitWaitReleases_wait_true_leaves_no_future_behind(t *testing.T) {
	// F-24
	c := setUp_(t)
	before := c.policy.Future_bank.Len()
	for i := 0; i < 20; i++ {
		out := c.submit([]any{
			func() (any, error) { return 1, nil },
			func() (any, error) { return 2, nil },
		}, py.Kw{"wait": true})
		vals := []any{}
		for _, v := range py.Iter(out) {
			vals = append(vals, unwrap_(t, v))
		}
		if !py.Eq(vals, []any{1, 2}) {
			t.Fatalf("vals %v", vals)
		}
		if v := unwrap_(t, c.submit([]any{func() (any, error) { return 3, nil }}, py.Kw{"wait": true})); !py.Eq(v, 3) {
			t.Fatalf("single %v", v)
		}
	}
	if c.policy.Future_bank.Len() != before {
		t.Fatalf("bank grew: %d -> %d", before, c.policy.Future_bank.Len())
	}
}

func TestSubmitWaitReleases_wait_true_inside_guarantee_scope(t *testing.T) {
	c := setUp_(t)
	before := c.policy.Future_bank.Len()
	var out any
	err := laila.Guarantee.With(func() error {
		var err error
		out, err = c.command.Submit([]any{func() (any, error) { return 5, nil }}, py.Kw{"wait": true})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if v := unwrap_(t, out); !py.Eq(v, 5) {
		t.Fatalf("out %v", v)
	}
	if c.policy.Future_bank.Len() != before {
		t.Fatalf("bank grew: %d -> %d", before, c.policy.Future_bank.Len())
	}
}

// ---------------------------------------------------------------------------
// TestGroupFutureBanks
// ---------------------------------------------------------------------------

func TestGroupFutureBanks_group_resolves_after_policy_switch(t *testing.T) {
	// F-23
	c := setUp_(t)
	tasks := make([]any, 5)
	for i := range tasks {
		tasks[i] = func() (any, error) { return 1, nil }
	}
	gf := c.submit(tasks, nil).(*laila.GroupFuture)
	wait_(t, gf, T_)
	other, err := laila.New_LAILA_IDENTIFIABLE_POLICY_()
	if err != nil {
		t.Fatal(err)
	}
	laila.Activate_policy(other)
	func() {
		defer func() {
			laila.Activate_policy(c.policy)
			_ = other.Central.Command.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_).Shutdown(py.Kw{"wait": true, "cancel_pending": true})
			laila.Local_policies_.PopDefault(other.Global_id(), nil)
		}()
		if n := len(py.Iter(wait_(t, gf, T_))); n != 5 {
			t.Fatalf("len %d", n)
		}
		result, err := gf.Result()
		if err != nil {
			t.Fatal(err)
		}
		vals := []any{}
		for _, v := range py.Iter(result) {
			vals = append(vals, unwrap_(t, v))
		}
		if !py.Eq(vals, []any{1, 1, 1, 1, 1}) {
			t.Fatalf("result %v", vals)
		}
		data, err := gf.Data()
		if err != nil {
			t.Fatal(err)
		}
		if !py.Eq(data, []any{1, 1, 1, 1, 1}) {
			t.Fatalf("data %v", data)
		}
		st, err := gf.Status()
		if err != nil {
			t.Fatal(err)
		}
		if !py.Eq(st.GetDefault("percentages", nil).(*py.Dict).GetDefault("finished", nil), 100.0) {
			t.Fatalf("status %v", st)
		}
	}()
	gf.Release()
	if c.policy.Future_bank.Len() != 0 {
		t.Fatalf("bank not empty: %d", c.policy.Future_bank.Len())
	}
}

func TestGroupFutureBanks_released_children_raise_key_error_strictly(t *testing.T) {
	c := setUp_(t)
	gf := c.submit([]any{
		func() (any, error) { return 1, nil },
		func() (any, error) { return 2, nil },
	}, nil).(*laila.GroupFuture)
	wait_(t, gf, T_)
	gf.Release()
	if _, err := gf.Result(); !errors.Is(err, py.KeyError) {
		t.Fatalf("expected KeyError, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestProcessPoolViaCommand
// ---------------------------------------------------------------------------

func (c *case_) pp(n int) *laila.PythonProcessPoolTaskForce {
	c.t.Helper()
	tf, err := laila.New_PythonProcessPoolTaskForce(py.Kw{"policy_id": c.policy.Global_id(), "num_workers": n})
	if err != nil {
		c.t.Fatal(err)
	}
	c.attach(tf)
	return tf
}

func TestProcessPoolViaCommand_command_submit_reaches_worker(t *testing.T) {
	// F-21
	c := setUp_(t)
	pp := c.pp(4)
	defer c.drop(pp, py.Kw{"wait": false, "cancel_pending": true})
	f := c.submit([]any{pid_task}, py.Kw{"taskforce_id": pp.Global_id()})
	if v := unwrap_(t, wait_(t, f, T_)); py.Eq(v, os.Getpid()) {
		t.Fatalf("ran in the parent: %v", v)
	}
	release_(f)
	gf := c.submit([]any{pid_task, pid_task}, py.Kw{"taskforce_id": pp.Global_id()})
	for _, v := range py.Iter(wait_(t, gf, T_)) {
		if py.Eq(unwrap_(t, v), os.Getpid()) {
			t.Fatalf("ran in the parent: %v", v)
		}
	}
	release_(gf)
	var g any
	err := laila.Guarantee.With(func() error {
		var err error
		g, err = c.command.Submit([]any{pid_task}, py.Kw{"taskforce_id": pp.Global_id()})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if st := status_of(g); st != laila.FutureStatus_FINISHED {
		t.Fatalf("status %v", st)
	}
	release_(g)
}

// build_on_pp_src_ is the constitution shipped to the worker. Its Go body
// is bound at package init so the spawned worker binary (which re-runs
// package init, as spawn re-imports ``__main__``) can resolve it too.
const build_on_pp_src_ = "def f(manifest):\n" +
	"    import os\n" +
	"    d = manifest.realized\n" +
	"    return (d['a'].data + sum(d['n']['b'][0].data), os.getpid())\n"

func init() {
	laila.Register_constitution(build_on_pp_src_, func(arg any) (any, error) {
		d, err := laila.As_manifest_(arg).Realized()
		if err != nil {
			return nil, err
		}
		dd := d.(*py.Dict)
		a, err := laila.As_entry_(py.Must(dd.Getitem("a"))).Data()
		if err != nil {
			return nil, err
		}
		b0 := py.Must(py.Must(py.Must(dd.Getitem("n")).(*py.Dict).Getitem("b")).(*py.List).Get(0))
		bdata, err := laila.As_entry_(b0).Data()
		if err != nil {
			return nil, err
		}
		total := py.Must(py.Int(a))
		for _, v := range py.Iter(bdata) {
			total += py.Must(py.Int(v))
		}
		return py.Tuple{total, os.Getpid()}, nil
	})
}

func TestProcessPoolViaCommand_build_on_process_pool(t *testing.T) {
	// F-21: the body runs in a worker against the realized manifest.
	c := setUp_(t)
	pp := c.pp(4)
	defer c.drop(pp, py.Kw{"wait": false, "cancel_pending": true})
	m, err := laila.New_Manifest(py.Kw{"data": py.DictOf(
		"a", py.Must(laila.Entry_constant(3)),
		"n", py.DictOf("b", py.NewList(py.Must(laila.Entry_constant(py.NewList(1, 2))))),
	)})
	if err != nil {
		t.Fatal(err)
	}
	wait_(t, py.Must(m.Memorize()), T_)
	e, err := laila.Variable(py.Kw{"constitution": build_on_pp_src_, "manifest": m})
	if err != nil {
		t.Fatal(err)
	}
	wait_(t, py.Must(laila.Build(e, py.Kw{"taskforce_id": pp.Global_id()})), T_)
	result := py.Iter(py.Must(e.Data()))
	total, worker_pid := result[0], result[1]
	if !py.Eq(total, 6) {
		t.Fatalf("total %v", total)
	}
	if py.Eq(worker_pid, os.Getpid()) {
		t.Fatalf("ran in the parent: %v", worker_pid)
	}
	// Idempotent: a second build is a no-op resolving to the entry.
	again := wait_(t, py.Must(laila.Build(e, py.Kw{"taskforce_id": pp.Global_id()})), T_)
	if laila.As_entry_(again) != e {
		t.Fatalf("second build resolved to %v, not the entry", again)
	}
}

func TestProcessPoolViaCommand_broken_pool_recovers(t *testing.T) {
	// F-22
	c := setUp_(t)
	pp := c.pp(4)
	defer c.drop(pp, py.Kw{"wait": false, "cancel_pending": true})
	crash := c.submit([]any{py.Partial(exit_hard_task, 3)}, py.Kw{"taskforce_id": pp.Global_id()})
	if _, err := callm_wait(crash, py.Kw{"timeout": T_}); err == nil {
		t.Fatal("expected the crashed task to raise")
	}
	if st := status_of(crash); st != laila.FutureStatus_ERROR {
		t.Fatalf("status %v", st)
	}
	release_(crash)
	t0 := py.Monotonic()
	g := c.submit([]any{pid_task}, py.Kw{"taskforce_id": pp.Global_id()})
	if v := unwrap_(t, wait_(t, g, T_)); py.Eq(v, os.Getpid()) {
		t.Fatalf("ran in the parent: %v", v)
	}
	if el := py.Monotonic() - t0; el >= 30 {
		t.Fatalf("recovery took %.1fs", el)
	}
	if !pp.Dispatcher_.Is_alive() {
		t.Fatal("dispatcher died")
	}
	if pp.Status != laila.TaskForceStatus_RUNNING {
		t.Fatalf("status %v", pp.Status)
	}
	release_(g)
}

// ---------------------------------------------------------------------------
// TestSyncExecutorRecycling
// ---------------------------------------------------------------------------

func TestSyncExecutorRecycling_idle_sync_threads_are_reaped(t *testing.T) {
	// F-W24-02
	c := setUp_(t)
	tfv, err := c.command.Taskforces.Getitem(c.command.Alpha_taskforce)
	if err != nil {
		t.Fatal(err)
	}
	tf := tfv.(*laila.PythonAsyncThreadPoolTaskForce)
	base := py.Active_count()

	one := func(i int) py.Coroutine { return func() (any, error) { return i, nil } }

	for s := 0; s < 1500; s += 500 {
		tasks := make([]any, 500)
		for i := range tasks {
			i := s + i
			tasks[i-s] = func() (any, error) { return one(i), nil }
		}
		gf := c.submit(tasks, nil)
		if n := len(py.Iter(wait_(t, gf, T_))); n != 500 {
			t.Fatalf("len %d", n)
		}
		release_(gf)
	}
	if py.Active_count() <= base+4 {
		t.Fatalf("expected a burst of sync threads, active=%d base=%d", py.Active_count(), base)
	}
	if !wait_until_(func() bool { return py.Active_count() <= base+4 }, 15) {
		t.Fatalf("idle threads not reaped: active=%d base=%d", py.Active_count(), base)
	}
	// The taskforce keeps working after the executor swap.
	if v := unwrap_(t, c.submit([]any{func() (any, error) { return 42, nil }}, py.Kw{"wait": true})); !py.Eq(v, 42) {
		t.Fatalf("got %v", v)
	}
	if tf.Inflight() != 0 {
		t.Fatalf("inflight %d", tf.Inflight())
	}
}
