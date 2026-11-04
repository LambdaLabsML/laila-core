// Port of tests/functional/policy/command/concurrent_package/unit_tests/
// test_parking.py -- functional tests for slot parking, sync offload,
// compute permits and ranks.
//
// Everything here runs on *deliberately tiny* taskforces (one loop
// thread, one or two slots) so that any hold-and-wait between a parent
// task and the children it submits would deadlock without parking. Every
// blocking wait carries a timeout so a regression fails instead of
// hanging the suite.
package unit_tests

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const PT_ = 30.0 // generous per-wait timeout: a deadlock shows up as TimeoutError

// coro is ``async def f(*args)``.
func coro(fn func(args ...any) (any, error)) py.CoroutineFunc { return py.CoroutineFunc(fn) }

// lam is ``lambda: value``.
func lam(value any) any { return func() (any, error) { return value, nil } }

type tf_opts struct {
	num_workers, max_async_per_thread, sync_workers, rank int
}

func new_ptf(t *testing.T, o tf_opts) *laila.PythonAsyncThreadPoolTaskForce {
	t.Helper()
	if o.num_workers == 0 {
		o.num_workers = 1
	}
	if o.max_async_per_thread == 0 {
		o.max_async_per_thread = 1
	}
	if o.rank == 0 {
		o.rank = 2
	}
	policy := active_policy(t)
	kwargs := py.Kw{
		"policy_id":            py.Str(py.Must(py.Getattr(policy, "global_id"))),
		"num_workers":          o.num_workers,
		"max_async_per_thread": o.max_async_per_thread,
		"rank":                 o.rank,
	}
	if o.sync_workers != 0 {
		kwargs["sync_workers"] = o.sync_workers
	}
	tf, err := laila.New_PythonAsyncThreadPoolTaskForce(kwargs)
	if err != nil {
		t.Fatal(err)
	}
	if err := tf.Start(); err != nil {
		t.Fatal(err)
	}
	command_of(t, policy).Taskforces.Set(tf.Global_id(), tf)
	return tf
}

func drop_tf(t *testing.T, tf *laila.PythonAsyncThreadPoolTaskForce, kw ...py.Kw) {
	t.Helper()
	defer command_of(t, active_policy(t)).Taskforces.PopDefault(tf.Global_id(), nil)
	if err := tf.Shutdown(py.MergeKw(kw...)); err != nil {
		t.Fatal(err)
	}
}

// cmd is ``laila.command``.
func cmd(t *testing.T) *laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_ {
	t.Helper()
	if _, err := laila.Get_active_policy(); err != nil {
		t.Fatal(err)
	}
	c, err := laila.Command()
	if err != nil {
		t.Fatal(err)
	}
	return c.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_)
}

func submit_to(t *testing.T, c *laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_, tasks []any, tf_id string) any {
	t.Helper()
	out, err := c.Submit(tasks, py.Kw{"taskforce_id": tf_id})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func wait_unwrapped(t *testing.T, f any, timeout float64) any {
	t.Helper()
	v, err := callm_wait(f, py.Kw{"timeout": timeout})
	if err != nil {
		t.Fatal(err)
	}
	return unwrap_data(t, v)
}

// await_unwrap is ``_unwrap(await f)`` inside a coroutine body.
func await_unwrap(f any) (any, error) {
	v, err := py.Await(f)
	if err != nil {
		return nil, err
	}
	for {
		e, ok := v.(*laila.Entry)
		if !ok {
			return v, nil
		}
		v, err = e.Data()
		if err != nil {
			return nil, err
		}
	}
}

func unwrap_all(t *testing.T, vals any) []any {
	t.Helper()
	out := []any{}
	for _, v := range py.Iter(vals) {
		out = append(out, unwrap_data(t, v))
	}
	return out
}

// ---------------------------------------------------------------------------
// TestNestedSubmissionsDoNotDeadlock: parent tasks that wait on children
// submitted to the *same* taskforce.
// ---------------------------------------------------------------------------

func TestParking_001_async_parent_awaits_child_on_1x1(t *testing.T) {
	c := cmd(t)
	tf := new_ptf(t, tf_opts{})
	defer drop_tf(t, tf)
	outer := coro(func(args ...any) (any, error) {
		inner, err := c.Submit([]any{lam(41)}, py.Kw{"taskforce_id": tf.Global_id()})
		if err != nil {
			return nil, err
		}
		v, err := await_unwrap(inner)
		if err != nil {
			return nil, err
		}
		return py.Must(py.Int(v)) + 1, nil
	})
	fut := submit_to(t, c, []any{outer}, tf.Global_id())
	if v := wait_unwrapped(t, fut, PT_); !py.Eq(v, 42) {
		t.Fatalf("got %v", v)
	}
}

func TestParking_002_sync_parent_blocks_on_child_on_1x1(t *testing.T) {
	// A sync body (offloaded to an executor thread) calls ``.wait()``.
	c := cmd(t)
	tf := new_ptf(t, tf_opts{})
	defer drop_tf(t, tf)
	outer := func() (any, error) {
		inner, err := c.Submit([]any{lam("child")}, py.Kw{"taskforce_id": tf.Global_id()})
		if err != nil {
			return nil, err
		}
		v, err := callm_wait(inner, py.Kw{"timeout": PT_})
		if err != nil {
			return nil, err
		}
		for {
			e, ok := v.(*laila.Entry)
			if !ok {
				break
			}
			if v, err = e.Data(); err != nil {
				return nil, err
			}
		}
		return v.(string) + "-parent", nil
	}
	fut := submit_to(t, c, []any{outer}, tf.Global_id())
	if v := wait_unwrapped(t, fut, PT_); !py.Eq(v, "child-parent") {
		t.Fatalf("got %v", v)
	}
}

func TestParking_003_deep_recursion_on_1x1(t *testing.T) {
	c := cmd(t)
	tf := new_ptf(t, tf_opts{})
	defer drop_tf(t, tf)
	var rec py.CoroutineFunc
	rec = func(args ...any) (any, error) {
		n := py.Must(py.Int(args[0]))
		if n == 0 {
			return 0, nil
		}
		child, err := c.Submit([]any{rec.Partial(n - 1)}, py.Kw{"taskforce_id": tf.Global_id()})
		if err != nil {
			return nil, err
		}
		v, err := await_unwrap(child)
		if err != nil {
			return nil, err
		}
		return 1 + py.Must(py.Int(v)), nil
	}
	fut := submit_to(t, c, []any{rec.Partial(60)}, tf.Global_id())
	if v := wait_unwrapped(t, fut, PT_); !py.Eq(v, 60) {
		t.Fatalf("got %v", v)
	}
	if tf.Inflight() != 0 || tf.Parked() != 0 {
		t.Fatalf("inflight=%d parked=%d", tf.Inflight(), tf.Parked())
	}
}

func TestParking_004_wide_fanout_under_saturation_respects_cap(t *testing.T) {
	// Many parents each awaiting several children on a 2x2 taskforce.
	// Also asserts the concurrency invariant: at most ``num_workers *
	// max_async_per_thread`` coroutines are *executing* (not parked) at
	// any instant.
	c := cmd(t)
	cap_ := 2
	tf := new_ptf(t, tf_opts{num_workers: 2, max_async_per_thread: cap_})
	defer drop_tf(t, tf)
	var (
		lock sync.Mutex
		now  int
		peak int
	)
	enter_ := func() {
		lock.Lock()
		now++
		peak = max(peak, now)
		lock.Unlock()
	}
	leave_ := func() {
		lock.Lock()
		now--
		lock.Unlock()
	}
	leaf := coro(func(args ...any) (any, error) {
		enter_()
		if err := py.Sleep(0.001); err != nil {
			return nil, err
		}
		leave_()
		return args[0], nil
	})
	parent := coro(func(args ...any) (any, error) {
		base := py.Must(py.Int(args[0]))
		enter_()
		leaves := make([]any, 5)
		for k := range leaves {
			leaves[k] = leaf.Partial(base + k)
		}
		gf, err := c.Submit(leaves, py.Kw{"taskforce_id": tf.Global_id()})
		if err != nil {
			return nil, err
		}
		leave_() // parked while awaiting
		results, err := py.Await(gf)
		if err != nil {
			return nil, err
		}
		enter_()
		total := 0
		for _, r := range py.Iter(results) {
			for {
				e, ok := r.(*laila.Entry)
				if !ok {
					break
				}
				if r, err = e.Data(); err != nil {
					return nil, err
				}
			}
			total += py.Must(py.Int(r))
		}
		leave_()
		return total, nil
	})
	n_parents := 120
	parents := make([]any, n_parents)
	for p := range parents {
		parents[p] = parent.Partial(10 * p)
	}
	gf := submit_to(t, c, parents, tf.Global_id())
	outs := unwrap_all(t, py.Must(callm_wait(gf, py.Kw{"timeout": PT_})))
	expected := []any{}
	for p := 0; p < n_parents; p++ {
		s := 0
		for k := 0; k < 5; k++ {
			s += 10*p + k
		}
		expected = append(expected, s)
	}
	if !py.Eq(outs, expected) {
		t.Fatalf("outs %v", outs)
	}
	lock.Lock()
	pk := peak
	lock.Unlock()
	if pk > 2*cap_ {
		t.Fatalf("peak %d > %d", pk, 2*cap_)
	}
	if tf.Inflight() != 0 || tf.Parked() != 0 {
		t.Fatalf("inflight=%d parked=%d", tf.Inflight(), tf.Parked())
	}
}

func TestParking_005_group_future_awaited_inside_slot_on_1x1(t *testing.T) {
	c := cmd(t)
	tf := new_ptf(t, tf_opts{})
	defer drop_tf(t, tf)
	outer := coro(func(args ...any) (any, error) {
		gf, err := c.Submit([]any{lam(1), lam(2), lam(3)}, py.Kw{"taskforce_id": tf.Global_id()})
		if err != nil {
			return nil, err
		}
		results, err := py.Await(gf)
		if err != nil {
			return nil, err
		}
		s := 0
		for _, r := range py.Iter(results) {
			v, err := r.(*laila.Entry).Data()
			if err != nil {
				return nil, err
			}
			s += py.Must(py.Int(v))
		}
		return s, nil
	})
	fut := submit_to(t, c, []any{outer}, tf.Global_id())
	if v := wait_unwrapped(t, fut, PT_); !py.Eq(v, 6) {
		t.Fatalf("got %v", v)
	}
}

func TestParking_006_sync_parent_group_wait_inside_slot_on_1x1(t *testing.T) {
	c := cmd(t)
	tf := new_ptf(t, tf_opts{})
	defer drop_tf(t, tf)
	outer := func() (any, error) {
		gf, err := c.Submit([]any{lam(1), lam(2), lam(3)}, py.Kw{"taskforce_id": tf.Global_id()})
		if err != nil {
			return nil, err
		}
		results, err := callm_wait(gf, py.Kw{"timeout": PT_})
		if err != nil {
			return nil, err
		}
		s := 0
		for _, r := range py.Iter(results) {
			v, err := r.(*laila.Entry).Data()
			if err != nil {
				return nil, err
			}
			s += py.Must(py.Int(v))
		}
		return s, nil
	}
	fut := submit_to(t, c, []any{outer}, tf.Global_id())
	if v := wait_unwrapped(t, fut, PT_); !py.Eq(v, 6) {
		t.Fatalf("got %v", v)
	}
}

func TestParking_007_remember_inside_submitted_coroutine_on_tiny_taskforces(t *testing.T) {
	// The motivating user pattern: ``await laila.remember`` inside a job.
	alpha := new_ptf(t, tf_opts{rank: 2})
	internal := new_ptf(t, tf_opts{rank: 1})
	c := cmd(t)
	saved_alpha, saved_internal := c.Alpha_taskforce, c.Internal_taskforce
	c.Alpha_taskforce = alpha.Global_id()
	c.Internal_taskforce = internal.Global_id()
	defer func() {
		c.Alpha_taskforce, c.Internal_taskforce = saved_alpha, saved_internal
		drop_tf(t, alpha)
		drop_tf(t, internal)
	}()
	entries := py.NewList()
	for i := 0; i < 40; i++ {
		entries.Append(py.Must(laila.Entry_constant(i)))
	}
	if _, err := callm_wait(py.Must(laila.Memorize(entries)), py.Kw{"timeout": PT_}); err != nil {
		t.Fatal(err)
	}

	my_f := coro(func(args ...any) (any, error) {
		gid := args[0]
		ref, err := laila.Remember(gid)
		if err != nil {
			return nil, err
		}
		entry, err := await_unwrap_entry(ref)
		if err != nil {
			return nil, err
		}
		d, err := entry.Data()
		if err != nil {
			return nil, err
		}
		n, err := py.Int(d)
		if err != nil {
			return nil, err
		}
		return n * 2, nil
	})
	tasks := []any{}
	for _, e := range entries.Items() {
		tasks = append(tasks, py.Partial(my_f, e.(*laila.Entry).Global_id()))
	}
	futs, err := c.Submit(tasks)
	if err != nil {
		t.Fatal(err)
	}
	got := unwrap_all(t, wait_unwrapped_raw(t, futs, PT_))
	want := []any{}
	for i := 0; i < 40; i++ {
		want = append(want, 2*i)
	}
	if !py.Eq(got, want) {
		t.Fatalf("got %v", got)
	}
}

// await_unwrap_entry is ``entry = await laila.remember(gid)``: the awaited
// value is the entry (a one-element list when remember was given a list).
func await_unwrap_entry(f any) (*laila.Entry, error) {
	v, err := py.Await(f)
	if err != nil {
		return nil, err
	}
	if lst, ok := v.(*py.List); ok && lst.Len() == 1 {
		v = py.Must(lst.Get(0))
	}
	e := laila.As_entry_(v)
	if e == nil {
		return nil, py.TypeError.Newf("expected an Entry, got %s", py.Pytypename(v))
	}
	return e, nil
}

// wait_unwrapped_raw is ``futs.wait(timeout=_T)`` without unwrapping.
func wait_unwrapped_raw(t *testing.T, f any, timeout float64) any {
	t.Helper()
	v, err := callm_wait(f, py.Kw{"timeout": timeout})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestParking_008_child_error_propagates_and_slot_is_restored(t *testing.T) {
	c := cmd(t)
	tf := new_ptf(t, tf_opts{})
	defer drop_tf(t, tf)
	bad_child := coro(func(args ...any) (any, error) { return nil, py.ValueError.New("child failed") })
	outer := coro(func(args ...any) (any, error) {
		child, err := c.Submit([]any{bad_child}, py.Kw{"taskforce_id": tf.Global_id()})
		if err != nil {
			return nil, err
		}
		if _, err := py.Await(child); err != nil {
			if errors.Is(err, py.ValueError) {
				return fmt.Sprintf("caught:%s", err.Error()), nil
			}
			return nil, err
		}
		return "not raised", nil
	})
	fut := submit_to(t, c, []any{outer}, tf.Global_id())
	if v := wait_unwrapped(t, fut, PT_); !py.Eq(v, "caught:child failed") {
		t.Fatalf("got %v", v)
	}
	// Slot must be usable again afterwards.
	again := submit_to(t, c, []any{lam(5)}, tf.Global_id())
	if v := wait_unwrapped(t, again, PT_); !py.Eq(v, 5) {
		t.Fatalf("got %v", v)
	}
	if tf.Inflight() != 0 {
		t.Fatalf("inflight %d", tf.Inflight())
	}
}

func TestParking_009_parked_parent_resumes_before_new_roots(t *testing.T) {
	// A parked parent waiting for a slot beats roots queued at the
	// dispatcher.
	//
	// One slot. ``A`` submits ``c`` and parks; ``c`` runs. ``B`` is queued
	// while ``c`` runs and takes the slot when ``c`` finishes (``A`` has
	// not yet observed ``c``'s completion). ``A`` then wants its slot back
	// but ``B`` holds it, so ``A`` registers as a parked waiter. ``D`` is
	// queued at the dispatcher afterwards. When ``B`` finishes the slot
	// must be handed to ``A`` (waiter) before ``D`` (new root).
	c := cmd(t)
	tf := new_ptf(t, tf_opts{})
	defer drop_tf(t, tf)
	var (
		lock  sync.Mutex
		order []string
	)
	log := func(tag string) {
		lock.Lock()
		order = append(order, tag)
		lock.Unlock()
	}
	child := coro(func(args ...any) (any, error) {
		log("c")
		if err := py.Sleep(0.1); err != nil {
			return nil, err
		}
		return 1, nil
	})
	a := coro(func(args ...any) (any, error) {
		log("A-start")
		ch, err := c.Submit([]any{child}, py.Kw{"taskforce_id": tf.Global_id()})
		if err != nil {
			return nil, err
		}
		if _, err := py.Await(ch); err != nil {
			return nil, err
		}
		log("A-resume")
		return "A", nil
	})
	b := coro(func(args ...any) (any, error) {
		log("B")
		if err := py.Sleep(0.4); err != nil {
			return nil, err
		}
		return "B", nil
	})
	d := coro(func(args ...any) (any, error) {
		log("D")
		return "D", nil
	})
	fa := submit_to(t, c, []any{a}, tf.Global_id())
	py.Sleep_sync(0.05) // A parked, c running
	fb := submit_to(t, c, []any{b}, tf.Global_id())
	py.Sleep_sync(0.2) // c done; either A already resumed or B holds the slot
	fd := submit_to(t, c, []any{d}, tf.Global_id())
	for _, f := range []any{fa, fb, fd} {
		if _, err := callm_wait(f, py.Kw{"timeout": PT_}); err != nil {
			t.Fatal(err)
		}
	}
	lock.Lock()
	defer lock.Unlock()
	if !py.Eq(order[:2], []string{"A-start", "c"}) {
		t.Fatalf("order %v", order)
	}
	// Whether A won the race for c's slot directly or had to wait for B,
	// it must come back before the later-queued root D.
	idx := func(tag string) int {
		for i, s := range order {
			if s == tag {
				return i
			}
		}
		return -1
	}
	if !(idx("A-resume") < idx("D")) {
		t.Fatalf("order %v", order)
	}
}

// ---------------------------------------------------------------------------
// TestSyncOffloadAndPermits
// ---------------------------------------------------------------------------

func TestParking_010_sync_body_does_not_block_loop(t *testing.T) {
	c := cmd(t)
	tf := new_ptf(t, tf_opts{num_workers: 1, max_async_per_thread: 4, sync_workers: 4})
	defer drop_tf(t, tf)
	sleepy := func() (any, error) {
		py.Sleep_sync(0.4)
		return "slept", nil
	}
	quick := coro(func(args ...any) (any, error) { return "quick", nil })
	slow := submit_to(t, c, []any{sleepy, sleepy, sleepy}, tf.Global_id())
	t0 := py.Monotonic()
	q := submit_to(t, c, []any{quick}, tf.Global_id())
	if v := wait_unwrapped(t, q, PT_); !py.Eq(v, "quick") {
		t.Fatalf("got %v", v)
	}
	if el := py.Monotonic() - t0; el >= 0.3 {
		t.Fatalf("quick task blocked for %.3fs", el)
	}
	if outs := unwrap_all(t, py.Must(callm_wait(slow, py.Kw{"timeout": PT_}))); !py.Eq(outs, []any{"slept", "slept", "slept"}) {
		t.Fatalf("outs %v", outs)
	}
}

func TestParking_011_sync_workers_bounds_executing_bodies(t *testing.T) {
	c := cmd(t)
	tf := new_ptf(t, tf_opts{num_workers: 1, max_async_per_thread: 8, sync_workers: 2})
	defer drop_tf(t, tf)
	var (
		lock sync.Mutex
		now  int
		peak int
	)
	body := func() (any, error) {
		lock.Lock()
		now++
		peak = max(peak, now)
		lock.Unlock()
		py.Sleep_sync(0.05)
		lock.Lock()
		now--
		lock.Unlock()
		return 1, nil
	}
	tasks := make([]any, 8)
	for i := range tasks {
		tasks[i] = body
	}
	gf := submit_to(t, c, tasks, tf.Global_id())
	if _, err := callm_wait(gf, py.Kw{"timeout": PT_}); err != nil {
		t.Fatal(err)
	}
	lock.Lock()
	defer lock.Unlock()
	if peak > 2 {
		t.Fatalf("peak %d", peak)
	}
}

func TestParking_012_permit_released_while_blocked_on_child(t *testing.T) {
	// One permit, one slot: nested sync bodies still complete.
	c := cmd(t)
	tf := new_ptf(t, tf_opts{num_workers: 1, max_async_per_thread: 1, sync_workers: 1})
	defer drop_tf(t, tf)
	wait_int := func(f any) (int, error) {
		v, err := callm_wait(f, py.Kw{"timeout": PT_})
		if err != nil {
			return 0, err
		}
		for {
			e, ok := v.(*laila.Entry)
			if !ok {
				break
			}
			if v, err = e.Data(); err != nil {
				return 0, err
			}
		}
		return py.Int(v)
	}
	leaf := func() (any, error) { return 1, nil }
	mid := func() (any, error) {
		f, err := c.Submit([]any{leaf}, py.Kw{"taskforce_id": tf.Global_id()})
		if err != nil {
			return nil, err
		}
		v, err := wait_int(f)
		if err != nil {
			return nil, err
		}
		return v + 1, nil
	}
	top := func() (any, error) {
		f, err := c.Submit([]any{mid}, py.Kw{"taskforce_id": tf.Global_id()})
		if err != nil {
			return nil, err
		}
		v, err := wait_int(f)
		if err != nil {
			return nil, err
		}
		return v + 1, nil
	}
	fut := submit_to(t, c, []any{top}, tf.Global_id())
	if v := wait_unwrapped(t, fut, PT_); !py.Eq(v, 3) {
		t.Fatalf("got %v", v)
	}
}

func TestParking_013_slot_context_visible_inside_sync_body(t *testing.T) {
	c := cmd(t)
	tf := new_ptf(t, tf_opts{})
	defer drop_tf(t, tf)
	body := func() (any, error) {
		ctx := laila.CURRENT_SLOT_.Value()
		return ctx != nil && py.Is(ctx.Tf, tf), nil
	}
	fut := submit_to(t, c, []any{body}, tf.Global_id())
	if v := wait_unwrapped(t, fut, PT_); !py.Eq(v, true) {
		t.Fatalf("got %v", v)
	}
	if laila.CURRENT_SLOT_.Value() != nil {
		t.Fatal("slot context leaked onto the test thread")
	}
}

func TestParking_014_lambda_returning_coroutine_still_works(t *testing.T) {
	c := cmd(t)
	tf := new_ptf(t, tf_opts{})
	defer drop_tf(t, tf)
	inner := func() (any, error) {
		if err := py.Sleep(0.001); err != nil {
			return nil, err
		}
		return "coro", nil
	}
	fut := submit_to(t, c, []any{coro_task(inner)}, tf.Global_id())
	if v := wait_unwrapped(t, fut, PT_); !py.Eq(v, "coro") {
		t.Fatalf("got %v", v)
	}
}

// ---------------------------------------------------------------------------
// TestShutdownAndCancellation
// ---------------------------------------------------------------------------

func TestParking_015_running_task_marked_cancelled_on_shutdown(t *testing.T) {
	c := cmd(t)
	tf := new_ptf(t, tf_opts{})
	defer drop_tf(t, tf)
	hold := coro(func(args ...any) (any, error) { return nil, py.Sleep(30) })
	fut := submit_to(t, c, []any{hold}, tf.Global_id())
	py.Sleep_sync(0.1)
	if st := status_of(fut); st != laila.FutureStatus_RUNNING {
		t.Fatalf("status %v", st)
	}
	drop_tf(t, tf, py.Kw{"wait": true, "cancel_pending": true})
	if st := status_of(fut); st != laila.FutureStatus_CANCELLED {
		t.Fatalf("status %v", st)
	}
	if _, err := callm_wait(fut, py.Kw{"timeout": 1}); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}

func TestParking_016_parked_task_does_not_hang_shutdown(t *testing.T) {
	c := cmd(t)
	tf := new_ptf(t, tf_opts{})
	defer drop_tf(t, tf)
	child := coro(func(args ...any) (any, error) { return nil, py.Sleep(30) })
	parent := coro(func(args ...any) (any, error) {
		f, err := c.Submit([]any{child}, py.Kw{"taskforce_id": tf.Global_id()})
		if err != nil {
			return nil, err
		}
		return py.Await(f)
	})
	fut := submit_to(t, c, []any{parent}, tf.Global_id())
	py.Sleep_sync(0.2)
	t0 := py.Monotonic()
	drop_tf(t, tf, py.Kw{"wait": true, "cancel_pending": true})
	if el := py.Monotonic() - t0; el >= 5.0 {
		t.Fatalf("shutdown took %.1fs", el)
	}
	if st := status_of(fut); st != laila.FutureStatus_CANCELLED && st != laila.FutureStatus_ERROR {
		t.Fatalf("status %v", st)
	}
}

// ---------------------------------------------------------------------------
// TestTaskforceSplitAndRank
// ---------------------------------------------------------------------------

func TestParking_017_default_command_has_alpha_and_internal(t *testing.T) {
	c := cmd(t)
	if py.IsNone(c.Alpha_taskforce) || py.IsNone(c.Internal_taskforce) {
		t.Fatal("missing default taskforces")
	}
	if py.Eq(c.Alpha_taskforce, c.Internal_taskforce) {
		t.Fatal("alpha and internal must differ")
	}
	alpha := py.Must(c.Taskforces.Getitem(c.Alpha_taskforce)).(*laila.PythonAsyncThreadPoolTaskForce)
	internal := py.Must(c.Taskforces.Getitem(c.Internal_taskforce)).(*laila.PythonAsyncThreadPoolTaskForce)
	if alpha.Rank != 2 || internal.Rank != 1 {
		t.Fatalf("ranks alpha=%d internal=%d", alpha.Rank, internal.Rank)
	}
}

func TestParking_018_alpha_task_may_submit_to_internal(t *testing.T) {
	c := cmd(t)
	alpha := new_ptf(t, tf_opts{rank: 2})
	internal := new_ptf(t, tf_opts{rank: 1})
	defer drop_tf(t, alpha)
	defer drop_tf(t, internal)
	outer := coro(func(args ...any) (any, error) {
		f, err := c.Submit([]any{lam("ok")}, py.Kw{"taskforce_id": internal.Global_id()})
		if err != nil {
			return nil, err
		}
		return await_unwrap(f)
	})
	fut := submit_to(t, c, []any{outer}, alpha.Global_id())
	if v := wait_unwrapped(t, fut, PT_); !py.Eq(v, "ok") {
		t.Fatalf("got %v", v)
	}
}

func TestParking_019_internal_task_may_not_submit_to_alpha(t *testing.T) {
	c := cmd(t)
	alpha := new_ptf(t, tf_opts{rank: 2})
	internal := new_ptf(t, tf_opts{rank: 1})
	defer drop_tf(t, alpha)
	defer drop_tf(t, internal)
	outer := coro(func(args ...any) (any, error) {
		f, err := c.Submit([]any{lam("nope")}, py.Kw{"taskforce_id": alpha.Global_id()})
		if err != nil {
			return nil, err
		}
		return py.Await(f)
	})
	fut := submit_to(t, c, []any{outer}, internal.Global_id())
	if _, err := callm_wait(fut, py.Kw{"timeout": PT_}); !errors.Is(err, laila.NestedCommandSubmitError) {
		t.Fatalf("expected NestedCommandSubmitError, got %v", err)
	}
}

func TestParking_020_same_rank_and_same_taskforce_allowed(t *testing.T) {
	c := cmd(t)
	a := new_ptf(t, tf_opts{rank: 2})
	b := new_ptf(t, tf_opts{rank: 2})
	defer drop_tf(t, a)
	defer drop_tf(t, b)
	outer := coro(func(args ...any) (any, error) {
		fx, err := c.Submit([]any{lam(1)}, py.Kw{"taskforce_id": a.Global_id()})
		if err != nil {
			return nil, err
		}
		x, err := await_unwrap(fx)
		if err != nil {
			return nil, err
		}
		fy, err := c.Submit([]any{lam(2)}, py.Kw{"taskforce_id": b.Global_id()})
		if err != nil {
			return nil, err
		}
		y, err := await_unwrap(fy)
		if err != nil {
			return nil, err
		}
		return py.Must(py.Int(x)) + py.Must(py.Int(y)), nil
	})
	fut := submit_to(t, c, []any{outer}, a.Global_id())
	if v := wait_unwrapped(t, fut, PT_); !py.Eq(v, 3) {
		t.Fatalf("got %v", v)
	}
}

func TestParking_021_user_thread_unrestricted(t *testing.T) {
	c := cmd(t)
	fut := submit_to(t, c, []any{lam(9)}, py.Str(c.Internal_taskforce))
	if v := wait_unwrapped(t, fut, PT_); !py.Eq(v, 9) {
		t.Fatalf("got %v", v)
	}
}

func TestParking_022_internal_operations_run_on_internal_taskforce(t *testing.T) {
	c := cmd(t)
	e := py.Must(laila.Entry_constant("x"))
	fut := py.Must(laila.Memorize(e))
	if _, err := callm_wait(fut, py.Kw{"timeout": PT_}); err != nil {
		t.Fatal(err)
	}
	if tid := py.Must(py.Getattr(fut, "taskforce_id")); !py.Eq(tid, c.Internal_taskforce) {
		t.Fatalf("memorize ran on %v, want internal %v", tid, c.Internal_taskforce)
	}
	rf := py.Must(laila.Remember(e.Global_id()))
	if _, err := callm_wait(rf, py.Kw{"timeout": PT_}); err != nil {
		t.Fatal(err)
	}
	if tid := py.Must(py.Getattr(rf, "taskforce_id")); !py.Eq(tid, c.Internal_taskforce) {
		t.Fatalf("remember ran on %v, want internal %v", tid, c.Internal_taskforce)
	}
}

// ---------------------------------------------------------------------------
// TestCycleGuard
// ---------------------------------------------------------------------------

func TestParking_023_remember_on_active_chain_raises(t *testing.T) {
	cmd(t)
	e := py.Must(laila.Entry_constant(1))
	if _, err := callm_wait(py.Must(laila.Memorize(e)), py.Kw{"timeout": PT_}); err != nil {
		t.Fatal(err)
	}
	token := laila.RESOLVE_CHAIN_.Set([]string{e.Global_id()})
	_, err := laila.Remember(e.Global_id())
	laila.RESOLVE_CHAIN_.Reset(token)
	if !errors.Is(err, laila.CyclicDependencyError) {
		t.Fatalf("expected CyclicDependencyError, got %v", err)
	}
	v := wait_unwrapped(t, py.Must(laila.Remember(e.Global_id())), PT_)
	if !py.Eq(v, 1) {
		t.Fatalf("got %v", v)
	}
}

func TestParking_024_chain_propagates_into_child_root_tasks(t *testing.T) {
	c := cmd(t)
	child := coro(func(args ...any) (any, error) {
		return append([]string(nil), laila.RESOLVE_CHAIN_.Value()...), nil
	})
	parent := coro(func(args ...any) (any, error) {
		token := laila.RESOLVE_CHAIN_.Set(append(append([]string(nil), laila.RESOLVE_CHAIN_.Value()...), "LAILA:ENTRY:p"))
		defer laila.RESOLVE_CHAIN_.Reset(token)
		f, err := c.Submit([]any{child})
		if err != nil {
			return nil, err
		}
		return await_unwrap(f)
	})
	out, err := c.Submit([]any{parent})
	if err != nil {
		t.Fatal(err)
	}
	chain := wait_unwrapped(t, out, PT_)
	got := []string{}
	for _, s := range py.Iter(chain) {
		got = append(got, py.Str(s))
	}
	sort.Strings(got)
	if !py.Eq(got, []string{"LAILA:ENTRY:p"}) {
		t.Fatalf("chain %v", got)
	}
}
