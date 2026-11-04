//go:build workload

// Port of tests/workloads/test_w10_constitution_graphs.py.
//
// Constitution graphs pushed to their limits: wide fan-out builds, deep
// build chains walked from constitution bodies, diamond / DAG dependency
// graphs, cycles of length 2..N, builder exceptions, staged entries
// memorized before build, rebuild after forget, and heavy CPU builders on
// starved taskforces.
//
// Constitution *source* stays the Python text (it is what gets stored,
// signed and compared); the Go body bound to each text through
// ``laila.Register_constitution`` is what runs. The Python module's
// ``builtins._laila_workload_registry`` is the process-wide ``registry_``
// below, read by the dependency bodies at call time exactly as the Python
// bodies ``getattr(builtins, ...)`` on every invocation.
package workloads

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T_ = 300.0

const SUM_ALL_ = "def combine(manifest):\n    d = manifest.realized\n    return sum(e.data for e in d.values())\n"

const CPU_SUM_ = "def combine(manifest):\n" +
	"    d = manifest.realized\n" +
	"    acc = 0\n" +
	"    for e in d.values():\n" +
	"        v = e.data\n" +
	"        for _ in range(20000):\n" +
	"            acc = (acc * 31 + v) % 1000003\n" +
	"    return acc\n"

const RAISE_ = "def boom(manifest):\n    raise ValueError('builder failed on purpose')\n"

const SLOW_ = "def slow(manifest):\n    import time\n    time.sleep(0.05)\n    return len(manifest.realized)\n"

// workload_registry is ``builtins._laila_workload_registry``: a dict of
// name -> Entry shared with the constitution bodies (threads write it).
type workload_registry struct {
	mu sync.Mutex
	m  map[string]*laila.Entry
}

func (r *workload_registry) Get(k string) *laila.Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[k]
}

func (r *workload_registry) Set(k string, e *laila.Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[k] = e
}

var (
	registry_mu_ sync.Mutex
	registry_    *workload_registry
)

// registry is the ``registry`` fixture: installs a fresh registry for the
// test and removes it at teardown.
func registry(t *testing.T) *workload_registry {
	t.Helper()
	reg := &workload_registry{m: map[string]*laila.Entry{}}
	registry_mu_.Lock()
	registry_ = reg
	registry_mu_.Unlock()
	t.Cleanup(func() {
		registry_mu_.Lock()
		if registry_ == reg {
			registry_ = nil
		}
		registry_mu_.Unlock()
	})
	return reg
}

func current_registry() (*workload_registry, error) {
	registry_mu_.Lock()
	defer registry_mu_.Unlock()
	if registry_ == nil {
		return nil, py.AttributeError.New("module 'builtins' has no attribute '_laila_workload_registry'")
	}
	return registry_, nil
}

// realized_entries is ``[e for e in manifest.realized.values()]``.
func realized_entries(arg any) ([]*laila.Entry, error) {
	d, err := laila.As_manifest_(arg).Realized()
	if err != nil {
		return nil, err
	}
	out := []*laila.Entry{}
	for _, v := range d.(*py.Dict).Values() {
		out = append(out, laila.As_entry_(v))
	}
	return out, nil
}

// sum_all_ is the Go body of SUM_ALL_.
func sum_all_(arg any) (any, error) {
	es, err := realized_entries(arg)
	if err != nil {
		return nil, err
	}
	var total any = 0
	for _, e := range es {
		v, err := e.Data()
		if err != nil {
			return nil, err
		}
		if total, err = py.Add(total, v); err != nil {
			return nil, err
		}
	}
	return total, nil
}

// cpu_sum_ is the Go body of CPU_SUM_.
func cpu_sum_(arg any) (any, error) {
	es, err := realized_entries(arg)
	if err != nil {
		return nil, err
	}
	acc := 0
	for _, e := range es {
		d, err := e.Data()
		if err != nil {
			return nil, err
		}
		v := d.(int)
		for i := 0; i < 20000; i++ {
			acc = (acc*31 + v) % 1000003
		}
	}
	return acc, nil
}

// raise_ is the Go body of RAISE_.
func raise_(arg any) (any, error) {
	return nil, py.ValueError.New("builder failed on purpose")
}

// slow_ is the Go body of SLOW_.
func slow_(arg any) (any, error) {
	time.Sleep(50 * time.Millisecond)
	d, err := laila.As_manifest_(arg).Realized()
	if err != nil {
		return nil, err
	}
	return d.(*py.Dict).Len(), nil
}

// dep_body_ is ``_dep_body(*keys)``: the source text of a body that first
// builds every registry entry in *keys* (recursively); the matching Go
// body is registered for that exact text.
func dep_body_(keys ...string) string {
	klist := py.NewList()
	for _, k := range keys {
		klist.Append(k)
	}
	src := "def dep(manifest):\n" +
		"    import builtins, laila\n" +
		"    reg = getattr(builtins, " + py.Repr("_laila_workload_registry") + ")\n" +
		"    total = 0\n" +
		"    for k in " + py.Repr(klist) + ":\n" +
		"        prev = reg[k]\n" +
		"        if prev.state.name != 'READY':\n" +
		"            try:\n" +
		"                laila.build(prev).wait(300)\n" +
		"            except RuntimeError:\n" +
		"                # TOCTOU: a sibling built it between our check and our build\n" +
		"                # (laila.build on a READY entry raises instead of no-op, see FINDINGS)\n" +
		"                if prev.state.name != 'READY':\n" +
		"                    raise\n" +
		"        total += prev.data\n" +
		"    d = manifest.realized\n" +
		"    return total + d['x'].data\n"
	keys = append([]string(nil), keys...)
	laila.Register_constitution(src, func(arg any) (any, error) {
		reg, err := current_registry()
		if err != nil {
			return nil, err
		}
		var total any = 0
		for _, k := range keys {
			prev := reg.Get(k)
			if prev == nil {
				return nil, py.KeyError.New(k)
			}
			if prev.State() != laila.EntryState_READY {
				f, err := laila.Build(prev)
				if err == nil {
					_, err = Wait(f, 300)
				}
				if err != nil {
					// Only the plain RuntimeError is swallowed (CyclicDependencyError
					// is a RuntimeError subclass but must propagate).
					if !errors.Is(err, py.RuntimeError) || errors.Is(err, laila.CyclicDependencyError) ||
						prev.State() != laila.EntryState_READY {
						return nil, err
					}
				}
			}
			v, err := prev.Data()
			if err != nil {
				return nil, err
			}
			if total, err = py.Add(total, v); err != nil {
				return nil, err
			}
		}
		d, err := laila.As_manifest_(arg).Realized()
		if err != nil {
			return nil, err
		}
		x, err := laila.As_entry_(must(d.(*py.Dict).Getitem("x"))).Data()
		if err != nil {
			return nil, err
		}
		return py.Add(total, x)
	})
	return src
}

func init() {
	laila.Register_constitution(SUM_ALL_, sum_all_)
	laila.Register_constitution(CPU_SUM_, cpu_sum_)
	laila.Register_constitution(RAISE_, raise_)
	laila.Register_constitution(SLOW_, slow_)
}

// quiescent is ``_TinyTaskforces.quiescent()``.
func (tt *tiny_taskforces) quiescent() bool {
	for _, tf := range []*laila.PythonAsyncThreadPoolTaskForce{tt.alpha, tt.internal} {
		if tf.Inflight() != 0 || tf.Parked() != 0 {
			return false
		}
	}
	return true
}

// leaf_manifest_ is ``_leaf_manifest(n, base=0)``.
func leaf_manifest_(n int, base ...int) *laila.Manifest {
	b := 0
	if len(base) > 0 {
		b = base[0]
	}
	d := py.NewDict()
	for i := 0; i < n; i++ {
		d.Set(fmt.Sprintf("k%d", i), must(laila.Entry_constant(b+i)))
	}
	m := must(laila.New_Manifest(py.Kw{"data": d}))
	f := must(m.Memorize())
	must(f.Wait(T_))
	f.Release()
	return m
}

// variable_ is ``Entry.variable(constitution=code, manifest=m)``.
func variable_(code any, m *laila.Manifest) *laila.Entry {
	return must(laila.Variable(py.Kw{"constitution": code, "manifest": m}))
}

// build_wait is ``laila.build(e).wait(_T)`` collapsing the synchronous
// CyclicDependencyError of Build and the wait error into one.
func build_wait(e *laila.Entry) (any, error) {
	f, err := laila.Build(e)
	if err != nil {
		return nil, err
	}
	return Wait(f, T_)
}

// build_all is ``[laila.build(e) for e in es]``.
func build_all(t *testing.T, es []*laila.Entry) []any {
	t.Helper()
	futs := make([]any, len(es))
	for i, e := range es {
		futs[i] = must(laila.Build(e))
	}
	return futs
}

func wait_all_(t *testing.T, futs []any) {
	t.Helper()
	for _, f := range futs {
		must(Wait(f, T_))
	}
}

// sum_range is ``sum(range(n))``.
func sum_range(n int) int { return n * (n - 1) / 2 }

func data_of(t *testing.T, e *laila.Entry) any {
	t.Helper()
	return must(e.Data())
}

// ---------------------------------------------------------------------------
// wide fan-out
// ---------------------------------------------------------------------------

func TestWideFanOut_many_builds_over_wide_manifest(t *testing.T) {
	for _, lb := range [][2]int{{200, 50}, {2000, 10}} {
		lb := lb
		t.Run(fmt.Sprintf("n_leaves=%d,n_builds=%d", lb[0], lb[1]), func(t *testing.T) {
			Fresh_policy(t)
			n_leaves, n_builds := N_scaled(lb[0], 50), N_scaled(lb[1], 5)
			m := leaf_manifest_(n_leaves)
			es := make([]*laila.Entry, n_builds)
			for i := range es {
				es[i] = variable_(SUM_ALL_, m)
			}
			var futs []any
			sw := Timed(func() {
				futs = build_all(t, es)
				wait_all_(t, futs)
			})
			for _, e := range es {
				assertTrue(t, e.State() == laila.EntryState_READY && py.Eq(data_of(t, e), sum_range(n_leaves)))
			}
			Release_all(futs...)
			assertTrue(t, sw.Elapsed < 240)
		})
	}
}

func TestWideFanOut_wide_fan_out_on_starved_taskforce(t *testing.T) {
	Fresh_policy(t)
	tfs := new_tiny_taskforces(t, 1, 1)
	m := leaf_manifest_(N_scaled(150, 30))
	es := make([]*laila.Entry, N_scaled(20, 5))
	for i := range es {
		es[i] = variable_(SUM_ALL_, m)
	}
	futs := build_all(t, es)
	wait_all_(t, futs)
	for _, e := range es {
		assertEq(t, data_of(t, e), sum_range(N_scaled(150, 30)))
	}
	assertTrue(t, tfs.quiescent())
	tfs.Exit()
}

func TestWideFanOut_cpu_heavy_builders_in_parallel(t *testing.T) {
	Fresh_policy(t)
	m := leaf_manifest_(20)
	es := make([]*laila.Entry, N_scaled(16, 4))
	for i := range es {
		es[i] = variable_(CPU_SUM_, m)
	}
	sw := Timed(func() { wait_all_(t, build_all(t, es)) })
	distinct := py.NewSet()
	for _, e := range es {
		distinct.Add(data_of(t, e))
	}
	assertEq(t, distinct.Len(), 1)
	assertTrue(t, sw.Elapsed < 240)
}

func TestWideFanOut_builds_from_many_threads(t *testing.T) {
	Fresh_policy(t)
	m := leaf_manifest_(50)
	worker := func(tt int) (any, error) {
		out := []any{}
		for i := 0; i < N_scaled(10, 3); i++ {
			e := variable_(SUM_ALL_, m)
			if _, err := build_wait(e); err != nil {
				return nil, err
			}
			v, err := e.Data()
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	}
	res := must(Run_threads(worker, 8))
	for _, lst := range res {
		for _, v := range lst.([]any) {
			assertEq(t, v, sum_range(50))
		}
	}
}

// ---------------------------------------------------------------------------
// deep chains
// ---------------------------------------------------------------------------

// x_manifest is ``Manifest(data={"x": Entry.constant(data=v)})`` memorized.
func x_manifest(v int) *laila.Manifest {
	m := must(laila.New_Manifest(py.Kw{"data": py.DictOf("x", must(laila.Entry_constant(v)))}))
	must(must(m.Memorize()).Wait(T_))
	return m
}

func TestDeepChains_linear_build_chain_from_bodies(t *testing.T) {
	for _, depth := range []int{15, 40} {
		depth := depth
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			Fresh_policy(t)
			reg := registry(t)
			tfs := new_tiny_taskforces(t, 1, 1)
			base := x_manifest(1)
			reg.Set("e0", variable_(SUM_ALL_, base))
			for i := 1; i <= depth; i++ {
				m := x_manifest(1)
				reg.Set(fmt.Sprintf("e%d", i), variable_(dep_body_(fmt.Sprintf("e%d", i-1)), m))
			}
			sw := Timed(func() { must(build_wait(reg.Get(fmt.Sprintf("e%d", depth)))) })
			for i := 0; i <= depth; i++ {
				e := reg.Get(fmt.Sprintf("e%d", i))
				assertEq(t, e.State(), laila.EntryState_READY)
				assertEq(t, data_of(t, e), i+1)
			}
			assertTrue(t, tfs.quiescent())
			tfs.Exit()
			assertTrue(t, sw.Elapsed < 240)
		})
	}
}

func TestDeepChains_diamond_dag(t *testing.T) {
	// root -> (a, b) -> leaf; leaf must be built exactly once.
	Fresh_policy(t)
	reg := registry(t)
	tfs := new_tiny_taskforces(t, 2, 2)
	ml, ma, mb, mr := x_manifest(1), x_manifest(1), x_manifest(1), x_manifest(1)
	reg.Set("leaf", variable_(SUM_ALL_, ml))
	reg.Set("a", variable_(dep_body_("leaf"), ma))
	reg.Set("b", variable_(dep_body_("leaf"), mb))
	reg.Set("root", variable_(dep_body_("a", "b"), mr))
	must(build_wait(reg.Get("root")))
	assertEq(t, data_of(t, reg.Get("leaf")), 1)
	assertTrue(t, py.Eq(data_of(t, reg.Get("a")), 2) && py.Eq(data_of(t, reg.Get("b")), 2))
	assertEq(t, data_of(t, reg.Get("root")), 5)
	tfs.Exit()
}

func TestDeepChains_wide_dag_layers(t *testing.T) {
	Fresh_policy(t)
	reg := registry(t)
	layers, width := 5, N_scaled(8, 3)
	for L := 0; L < layers; L++ {
		for w := 0; w < width; w++ {
			m := x_manifest(1)
			deps := []string{}
			if L > 0 {
				for k := 0; k < width; k++ {
					deps = append(deps, fmt.Sprintf("l%dw%d", L-1, k))
				}
			}
			var body string
			if len(deps) > 0 {
				body = dep_body_(deps...)
			} else {
				body = SUM_ALL_
			}
			reg.Set(fmt.Sprintf("l%dw%d", L, w), variable_(body, m))
		}
	}
	tops := []*laila.Entry{}
	for w := 0; w < width; w++ {
		tops = append(tops, reg.Get(fmt.Sprintf("l%dw%d", layers-1, w)))
	}
	wait_all_(t, build_all(t, tops))
	expected := []int{1}
	for i := 1; i < layers; i++ {
		expected = append(expected, width*expected[len(expected)-1]+1)
	}
	for _, e := range tops {
		assertEq(t, data_of(t, e), expected[len(expected)-1])
	}
	for w := 0; w < width; w++ {
		assertEq(t, data_of(t, reg.Get(fmt.Sprintf("l0w%d", w))), 1)
	}
}

func TestDeepChains_concurrent_builds_of_shared_dependency(t *testing.T) {
	// Many roots depending on the same STAGED leaf built concurrently.
	Fresh_policy(t)
	reg := registry(t)
	ml := x_manifest(1)
	reg.Set("leaf", variable_(SLOW_, ml))
	roots := []*laila.Entry{}
	for i := 0; i < N_scaled(12, 4); i++ {
		m := x_manifest(i)
		roots = append(roots, variable_(dep_body_("leaf"), m))
	}
	futs := build_all(t, roots)
	errs := []string{}
	for _, f := range futs {
		if _, err := Wait(f, T_); err != nil {
			errs = append(errs, py.Repr(err))
		}
	}
	assertTrue(t, len(errs) == 0, errs)
	assertEq(t, reg.Get("leaf").State(), laila.EntryState_READY)
	for i, r := range roots {
		assertEq(t, data_of(t, r), 1+i)
	}
}

// ---------------------------------------------------------------------------
// cycles
// ---------------------------------------------------------------------------

func TestCycles_cycle_detected_not_hang(t *testing.T) {
	for _, length := range []int{2, 3, 8} {
		length := length
		t.Run(fmt.Sprintf("length=%d", length), func(t *testing.T) {
			Fresh_policy(t)
			reg := registry(t)
			tfs := new_tiny_taskforces(t, 2, 2)
			for i := 0; i < length; i++ {
				m := x_manifest(i)
				reg.Set(fmt.Sprintf("c%d", i), variable_(dep_body_(fmt.Sprintf("c%d", (i+1)%length)), m))
			}
			var err error
			sw := Timed(func() { _, err = build_wait(reg.Get("c0")) })
			if !errors.Is(err, laila.CyclicDependencyError) {
				t.Fatalf("want CyclicDependencyError, got %v", err)
			}
			assertTrue(t, sw.Elapsed < 120)
			for i := 0; i < length; i++ {
				assertEq(t, reg.Get(fmt.Sprintf("c%d", i)).State(), laila.EntryState_STAGED)
			}
			tfs.Exit()
		})
	}
}

func TestCycles_self_cycle(t *testing.T) {
	Fresh_policy(t)
	reg := registry(t)
	m := x_manifest(1)
	reg.Set("s", variable_(dep_body_("s"), m))
	if _, err := build_wait(reg.Get("s")); !errors.Is(err, laila.CyclicDependencyError) {
		t.Fatalf("want CyclicDependencyError, got %v", err)
	}
}

func TestCycles_cycle_then_recover_by_breaking_it(t *testing.T) {
	Fresh_policy(t)
	reg := registry(t)
	ma := x_manifest(1)
	mb := x_manifest(2)
	reg.Set("a", variable_(dep_body_("b"), ma))
	reg.Set("b", variable_(dep_body_("a"), mb))
	if _, err := build_wait(reg.Get("a")); !errors.Is(err, laila.CyclicDependencyError) {
		t.Fatalf("want CyclicDependencyError, got %v", err)
	}
	reg.Set("b", variable_(SUM_ALL_, mb))
	must(build_wait(reg.Get("a")))
	assertEq(t, data_of(t, reg.Get("a")), 3)
}

func TestCycles_many_independent_cycles_concurrently(t *testing.T) {
	Fresh_policy(t)
	reg := registry(t)
	n := N_scaled(10, 3)
	for i := 0; i < n; i++ {
		for _, side := range []string{"a", "b"} {
			m := x_manifest(1)
			other := "a"
			if side == "a" {
				other = "b"
			}
			reg.Set(fmt.Sprintf("%s%d", side, i), variable_(dep_body_(fmt.Sprintf("%s%d", other, i)), m))
		}
	}
	futs := []any{}
	for i := 0; i < n; i++ {
		futs = append(futs, must(laila.Build(reg.Get(fmt.Sprintf("a%d", i)))))
	}
	failures := 0
	for _, f := range futs {
		if _, err := Wait(f, T_); errors.Is(err, laila.CyclicDependencyError) {
			failures++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	assertEq(t, failures, n)
}

// ---------------------------------------------------------------------------
// failures
// ---------------------------------------------------------------------------

func TestFailures_builder_exception_surfaces_and_entry_stays_staged(t *testing.T) {
	Fresh_policy(t)
	m := leaf_manifest_(5)
	es := make([]*laila.Entry, N_scaled(50, 10))
	for i := range es {
		es[i] = variable_(RAISE_, m)
	}
	futs := build_all(t, es)
	for _, f := range futs {
		if _, err := Wait(f, T_); !errors.Is(err, py.ValueError) {
			t.Fatalf("want ValueError, got %v", err)
		}
	}
	for _, e := range es {
		assertEq(t, e.State(), laila.EntryState_STAGED)
	}
}

func TestFailures_failed_build_can_be_retried(t *testing.T) {
	Fresh_policy(t)
	m := leaf_manifest_(5)
	e := variable_(RAISE_, m)
	for i := 0; i < 2; i++ {
		if _, err := build_wait(e); !errors.Is(err, py.ValueError) {
			t.Fatalf("want ValueError, got %v", err)
		}
	}
	assertEq(t, e.State(), laila.EntryState_STAGED)
}

func TestFailures_missing_manifest_leaf_fails_build(t *testing.T) {
	Fresh_policy(t)
	m := must(laila.New_Manifest(py.Kw{"data": py.DictOf("x", "LAILA:ENTRY:00000000-0000-4000-8000-00000000beef")}))
	e := variable_(SUM_ALL_, m)
	_, err := build_wait(e)
	assertTrue(t, err != nil, "want an exception")
	assertEq(t, e.State(), laila.EntryState_STAGED)
}

func TestFailures_bad_code_fails_fast(t *testing.T) {
	Fresh_policy(t)
	m := leaf_manifest_(3)
	e, err := laila.Variable(py.Kw{"constitution": "def a(m): return 1\ndef b(m): return 2\n", "manifest": m})
	if err == nil {
		_, err = build_wait(e)
	}
	assertTrue(t, err != nil, "want ValueError / SyntaxError / Exception")
}

// F-19 (fixed): build is idempotent and concurrent builds coalesce.
func TestFailures_build_of_ready_entry_is_noop(t *testing.T) {
	Fresh_policy(t)
	e := must(laila.Entry_constant(5))
	must(build_wait(e))
	assertEq(t, data_of(t, e), 5)
}

func TestFailures_concurrent_builds_of_same_staged_entry_both_succeed(t *testing.T) {
	Fresh_policy(t)
	m := leaf_manifest_(3)
	e := variable_(SLOW_, m)
	futs := []any{}
	for i := 0; i < 8; i++ {
		futs = append(futs, must(laila.Build(e)))
	}
	wait_all_(t, futs)
	assertTrue(t, e.State() == laila.EntryState_READY && py.Eq(data_of(t, e), 3))
}

func TestFailures_data_access_before_build_raises(t *testing.T) {
	Fresh_policy(t)
	m := leaf_manifest_(3)
	e := variable_(SUM_ALL_, m)
	_, err := e.Data()
	assertTrue(t, err != nil, "want an exception")
}

// ---------------------------------------------------------------------------
// staged persistence
// ---------------------------------------------------------------------------

func TestStagedPersistence_memorize_staged_entry_rejected_or_built(t *testing.T) {
	Fresh_policy(t)
	m := leaf_manifest_(5)
	e := variable_(SUM_ALL_, m)
	f, err := laila.Memorize(e)
	if err == nil {
		_, err = Wait(f, T_)
	}
	if err != nil {
		if errors.Is(err, py.RuntimeError) {
			return // documented: only READY entries can be serialized
		}
		t.Fatal(err)
	}
	// mem pool stores by reference; must still be buildable later
	must(build_wait(e))
	assertEq(t, data_of(t, e), sum_range(5))
}

func TestStagedPersistence_build_then_memorize_then_remember_many(t *testing.T) {
	Fresh_policy(t)
	m := leaf_manifest_(10)
	es := make([]*laila.Entry, N_scaled(100, 20))
	ids := py.NewList()
	for i := range es {
		es[i] = variable_(SUM_ALL_, m)
		ids.Append(es[i].Global_id())
	}
	wait_all_(t, build_all(t, es))
	must(Wait(must(laila.Memorize(entries_list(es))), T_))
	got := must(Wait(must(laila.Remember(ids, py.Kw{"persist": false})), T_)).(*py.List)
	for _, g := range got.Items() {
		assertEq(t, data_of(t, laila.As_entry_(g)), sum_range(10))
	}
}

func TestStagedPersistence_manifest_global_id_lazy_resolution(t *testing.T) {
	Fresh_policy(t)
	m := leaf_manifest_(7)
	c := must(laila.New_ComplexConstitution(py.Kw{"code": SUM_ALL_, "manifest_global_id": m.Global_id()}))
	e := variable_(c, m)
	must(build_wait(e))
	assertEq(t, data_of(t, e), sum_range(7))
}

func TestStagedPersistence_rebuild_after_forgetting_leaves_fails(t *testing.T) {
	Fresh_policy(t)
	m := leaf_manifest_(5)
	e := variable_(SUM_ALL_, m)
	must(build_wait(e))
	must(Wait(must(laila.Forget(py.NewList(m.Iter()...))), T_))
	e2 := variable_(SUM_ALL_, m)
	_, err := build_wait(e2)
	assertTrue(t, err != nil, "want an exception")
}

// ---------------------------------------------------------------------------
// long running
// ---------------------------------------------------------------------------

func TestLongRunning_sustained_build_stream(t *testing.T) {
	policy := Fresh_policy(t)
	m := leaf_manifest_(20)
	deadline := py.Monotonic() + float64(N_scaled(20, 5))
	built := 0
	for py.Monotonic() < deadline {
		batch := make([]*laila.Entry, 10)
		for i := range batch {
			batch[i] = variable_(SUM_ALL_, m)
		}
		futs := build_all(t, batch)
		wait_all_(t, futs)
		Release_all(futs...)
		built += len(batch)
	}
	assertTrue(t, built >= 50)
	assertEq(t, policy.Future_bank.Len(), 0)
}

func TestLongRunning_builds_interleaved_with_memorize_from_threads(t *testing.T) {
	Fresh_policy(t)
	m := leaf_manifest_(20)
	stop := py.NewEvent()
	var mu sync.Mutex
	errs := []string{}
	builder := func(tt int) {
		for !stop.Is_set() {
			e := variable_(SUM_ALL_, m)
			if _, err := build_wait(e); err != nil {
				mu.Lock()
				errs = append(errs, py.Repr(err))
				mu.Unlock()
			}
		}
	}
	writer := func(tt int) {
		for i := 0; i < N_scaled(300, 50); i++ {
			must(Wait(must(laila.Memorize(must(laila.Constant(i)))), T_))
		}
		stop.Set()
	}
	must(Run_threads(func(tt int) (any, error) {
		if tt == 0 {
			writer(tt)
		} else {
			builder(tt)
		}
		return nil, nil
	}, 4))
	assertTrue(t, len(errs) == 0, errs)
}
