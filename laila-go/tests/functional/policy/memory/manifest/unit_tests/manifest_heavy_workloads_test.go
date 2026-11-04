// Port of tests/functional/policy/memory/manifest/unit_tests/test_manifest_heavy_workloads.py.
//
// Heavy manifest workloads on starved taskforces.
//
// These tests exercise the real recursion paths in laila -- ``laila.build``
// -> ``ComplexConstitution`` -> ``manifest.async_realized`` ->
// ``laila.remember`` -> per-entry fetch coroutines, plus constitution bodies
// that themselves call ``manifest.realized`` / ``laila.build`` -- while the
// alpha and internal taskforces are shrunk to one loop thread with one or
// two slots each. Without slot parking every one of these would deadlock;
// with it they must complete *and* produce the right values.
//
// All waits carry a timeout so a scheduler regression fails loudly instead
// of hanging the suite.
//
// Constitution bodies are Python source in the original; the port binds a
// Go builder to each exact source text (Register_constitution), so the
// stored / signed identity of every entry is unchanged.
package unit_tests

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const _T_heavy = 120.0

// _REGISTRY is ``builtins._laila_heavy_manifest_test_registry``.
var (
	_REGISTRY_mu sync.Mutex
	_REGISTRY    map[string]*laila.Entry
)

// _unwrap is ``while hasattr(v, "data"): v = v.data``.
func _unwrap(v any) any {
	for {
		e := laila.As_entry_(v)
		if e == nil {
			return v
		}
		v = must(e.Data())
	}
}

// TinyTaskforces_ swaps the command's alpha/internal taskforces for
// 1-thread pools.
type TinyTaskforces_ struct {
	alpha_slots, internal_slots, sync_workers int

	saved_alpha, saved_internal any
	alpha, internal             *laila.PythonAsyncThreadPoolTaskForce
}

func newTinyTaskforces(alpha_slots, internal_slots int) *TinyTaskforces_ {
	return &TinyTaskforces_{alpha_slots: alpha_slots, internal_slots: internal_slots, sync_workers: 2}
}

func (s *TinyTaskforces_) enter(t *testing.T) *TinyTaskforces_ {
	t.Helper()
	cmd := _command()
	s.saved_alpha, s.saved_internal = cmd.Alpha_taskforce, cmd.Internal_taskforce
	pid := must(laila.Get_active_policy()).(*laila.LAILA_IDENTIFIABLE_POLICY_).Global_id()
	s.alpha = must(laila.New_PythonAsyncThreadPoolTaskForce(py.Kw{
		"policy_id":            pid,
		"num_workers":          1,
		"max_async_per_thread": s.alpha_slots,
		"sync_workers":         s.sync_workers,
		"rank":                 2,
	}))
	s.internal = must(laila.New_PythonAsyncThreadPoolTaskForce(py.Kw{
		"policy_id":            pid,
		"num_workers":          1,
		"max_async_per_thread": s.internal_slots,
		"sync_workers":         s.sync_workers,
		"rank":                 1,
	}))
	cmd.Taskforces.Set(s.alpha.Global_id(), s.alpha)
	cmd.Taskforces.Set(s.internal.Global_id(), s.internal)
	cmd.Alpha_taskforce = s.alpha.Global_id()
	cmd.Internal_taskforce = s.internal.Global_id()
	t.Cleanup(s.exit)
	return s
}

func (s *TinyTaskforces_) exit() {
	cmd := _command()
	cmd.Alpha_taskforce, cmd.Internal_taskforce = s.saved_alpha, s.saved_internal
	for _, tf := range []*laila.PythonAsyncThreadPoolTaskForce{s.alpha, s.internal} {
		_ = tf.Shutdown(py.Kw{"wait": true, "cancel_pending": true})
		cmd.Taskforces.PopDefault(tf.Global_id(), nil)
	}
}

func (s *TinyTaskforces_) assert_quiescent(t *testing.T) {
	t.Helper()
	// A runner flips its future to FINISHED *then* releases its slot (same
	// order as Python). Under CPython the waiter rarely gets the GIL back
	// in between; Go threads really run in parallel, so give the runner's
	// ``finally`` a moment to land before asserting.
	deadline := time.Now().Add(2 * time.Second)
	for {
		quiet := true
		for _, tf := range []*laila.PythonAsyncThreadPoolTaskForce{s.alpha, s.internal} {
			if tf.Inflight() != 0 || tf.Parked() != 0 {
				quiet = false
			}
		}
		if quiet || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	for _, tf := range []*laila.PythonAsyncThreadPoolTaskForce{s.alpha, s.internal} {
		if tf.Inflight() != 0 {
			t.Fatalf("%s still has slots occupied", tf.Global_id())
		}
		if tf.Parked() != 0 {
			t.Fatalf("%s still has parked waiters", tf.Global_id())
		}
	}
}

const _SUM_ALL = "def combine(manifest):\n" +
	"    d = manifest.realized\n" +
	"    return sum(e.data for e in d.values())\n"

// Walks a chain of nested manifests: every level has an 'x' leaf and an
// optional 'inner' manifest. Each ``.realized`` on an inner manifest is a
// fresh nested remember issued from the sync body (executor thread).
const _WALK_NESTED = "def walk(manifest):\n" +
	"    total = 0\n" +
	"    cur = manifest\n" +
	"    depth = 0\n" +
	"    while cur is not None:\n" +
	"        d = cur.realized\n" +
	"        total += d['x'].data\n" +
	"        cur = d.get('inner')\n" +
	"        depth += 1\n" +
	"    return (total, depth)\n"

// _build_body_for is the constitution that builds another STAGED entry
// (from a registry) first.
func _build_body_for(key string) string {
	return "def dep(manifest):\n" +
		"    import builtins, laila\n" +
		fmt.Sprintf("    reg = getattr(builtins, %s)\n", py.Repr("_laila_heavy_manifest_test_registry")) +
		fmt.Sprintf("    prev = reg[%s]\n", py.Repr(key)) +
		"    if prev.state.name != 'READY':\n" +
		"        laila.build(prev).wait(120)\n" +
		"    d = manifest.realized\n" +
		"    return prev.data + d['x'].data\n"
}

// sumRealized is ``sum(e.data for e in manifest.realized.values())``.
func sumRealized(m *laila.Manifest) (int, error) {
	realized, err := m.Realized()
	if err != nil {
		return 0, err
	}
	total := 0
	for _, v := range realized.(*py.Dict).Values() {
		data, err := laila.As_entry_(v).Data()
		if err != nil {
			return 0, err
		}
		n, err := py.Int(data)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// Go builders bound to the Python source texts above.
func init() {
	laila.Register_constitution(_SUM_ALL, func(arg any) (any, error) {
		return sumRealized(laila.As_manifest_(arg))
	})
	laila.Register_constitution(_WALK_NESTED, func(arg any) (any, error) {
		total, depth := 0, 0
		cur := laila.As_manifest_(arg)
		for cur != nil {
			realized, err := cur.Realized()
			if err != nil {
				return nil, err
			}
			dd := realized.(*py.Dict)
			x, err := dd.Getitem("x")
			if err != nil {
				return nil, err
			}
			data, err := laila.As_entry_(x).Data()
			if err != nil {
				return nil, err
			}
			n, err := py.Int(data)
			if err != nil {
				return nil, err
			}
			total += n
			cur = laila.As_manifest_(dd.GetDefault("inner", nil))
			depth++
		}
		return py.Tuple{total, depth}, nil
	})
}

// registerDep binds the builder for ``_build_body_for(key)``.
func registerDep(t *testing.T, key string) string {
	t.Helper()
	src := _build_body_for(key)
	laila.Register_constitution(src, func(arg any) (any, error) {
		_REGISTRY_mu.Lock()
		prev := _REGISTRY[key]
		_REGISTRY_mu.Unlock()
		if prev.State() != laila.EntryState_READY {
			fut, err := laila.Build(prev)
			if err != nil {
				return nil, err
			}
			if _, err := fut.(waiter).Wait(120.0); err != nil {
				return nil, err
			}
		}
		realized, err := laila.As_manifest_(arg).Realized()
		if err != nil {
			return nil, err
		}
		x, err := realized.(*py.Dict).Getitem("x")
		if err != nil {
			return nil, err
		}
		prev_data, err := prev.Data()
		if err != nil {
			return nil, err
		}
		x_data, err := laila.As_entry_(x).Data()
		if err != nil {
			return nil, err
		}
		return must(py.Int(prev_data)) + must(py.Int(x_data)), nil
	})
	t.Cleanup(func() { laila.Unregister_constitution(src) })
	return src
}

func setRegistry(t *testing.T, registry map[string]*laila.Entry) {
	t.Helper()
	_REGISTRY_mu.Lock()
	_REGISTRY = registry
	_REGISTRY_mu.Unlock()
	t.Cleanup(func() {
		_REGISTRY_mu.Lock()
		_REGISTRY = nil
		_REGISTRY_mu.Unlock()
	})
}

func setUpHeavy(t *testing.T) {
	t.Helper()
	must(laila.Get_active_policy())
}

func memorizeWait(t *testing.T, m *laila.Manifest) {
	t.Helper()
	must(must(m.Memorize()).Wait(_T_heavy))
}

func variable(t *testing.T, constitution string, m *laila.Manifest) *laila.Entry {
	t.Helper()
	return must(laila.Variable(py.Kw{"constitution": constitution, "manifest": m}))
}

func buildAll(t *testing.T, entries []*laila.Entry) {
	t.Helper()
	futs := []any{}
	for _, e := range entries {
		futs = append(futs, must(laila.Build(e)))
	}
	for _, f := range futs {
		must(f.(waiter).Wait(_T_heavy))
	}
}

func manifestOf(entries []*laila.Entry) *laila.Manifest {
	data := py.NewDict()
	for i, e := range entries {
		data.Set(fmt.Sprintf("k%d", i), e)
	}
	return manifest(py.Kw{"data": data})
}

func constants(n int) []*laila.Entry {
	out := make([]*laila.Entry, n)
	for i := range out {
		out[i] = constant(i)
	}
	return out
}

func sumRange(n int) int { return n * (n - 1) / 2 }

// ------------------------------------------------------------------
// W1: wide fan-out -- many concurrent builds over one wide manifest
// ------------------------------------------------------------------
func TestHeavy001WideManifestManyConcurrentBuildsOn1x1(t *testing.T) {
	setUpHeavy(t)
	// 20 parents x (150 async_realized + 150 body-realized) fetches, all
	// funnelled through a single internal slot.
	n_leaves, n_builds := 150, 20
	leaves := constants(n_leaves)
	m := manifestOf(leaves)
	tfs := newTinyTaskforces(1, 1).enter(t)
	memorizeWait(t, m)
	entries := []*laila.Entry{}
	for i := 0; i < n_builds; i++ {
		entries = append(entries, variable(t, _SUM_ALL, m))
	}
	buildAll(t, entries)
	expected := sumRange(n_leaves)
	for _, e := range entries {
		if e.State() != laila.EntryState_READY {
			t.Fatalf("state %v", e.State())
		}
		assertEq(t, must(e.Data()), expected)
	}
	tfs.assert_quiescent(t)
}

// ------------------------------------------------------------------
// W2: deep nesting -- bodies recursively realize nested manifests
// ------------------------------------------------------------------
func TestHeavy002NestedManifestChainWalkedFromSyncBodies(t *testing.T) {
	setUpHeavy(t)
	depth := 8
	tfs := newTinyTaskforces(1, 1).enter(t)
	var inner *laila.Manifest
	for level := 0; level < depth; level++ {
		payload := d("x", constant(level))
		if inner != nil {
			payload.Set("inner", inner)
		}
		m := manifest(py.Kw{"data": payload})
		memorizeWait(t, m)
		inner = m
	}
	outer := inner

	n_builds := 12
	entries := []*laila.Entry{}
	for i := 0; i < n_builds; i++ {
		entries = append(entries, variable(t, _WALK_NESTED, outer))
	}
	buildAll(t, entries)
	for _, e := range entries {
		out := must(e.Data()).(py.Tuple)
		total, seen_depth := out[0], out[1]
		assertEq(t, seen_depth, depth)
		assertEq(t, total, sumRange(depth))
	}
	tfs.assert_quiescent(t)
}

// ------------------------------------------------------------------
// W3: shared leaves -- many manifests over the same leaf set
// ------------------------------------------------------------------
func TestHeavy003ManyManifestsSharingLeavesBuiltConcurrently(t *testing.T) {
	setUpHeavy(t)
	leaves := constants(60)
	tfs := newTinyTaskforces(2, 2).enter(t)
	must(must(laila.Memorize(py.ListFrom(anys(leaves)))).(waiter).Wait(_T_heavy))
	type pair struct {
		m        *laila.Manifest
		expected int
	}
	manifests := []pair{}
	for j := 0; j < 25; j++ {
		// rotating window of 20 leaves each
		window := []*laila.Entry{}
		expected := 0
		for k := 0; k < 20; k++ {
			e := leaves[(j+k)%len(leaves)]
			window = append(window, e)
			expected += must(py.Int(must(e.Data())))
		}
		manifests = append(manifests, pair{manifestOf(window), expected})
	}
	for _, p := range manifests {
		memorizeWait(t, p.m)
	}
	entries := []*laila.Entry{}
	for _, p := range manifests {
		entries = append(entries, variable(t, _SUM_ALL, p.m))
	}
	buildAll(t, entries)
	for i, e := range entries {
		assertEq(t, must(e.Data()), manifests[i].expected)
	}
	tfs.assert_quiescent(t)
}

func anys(entries []*laila.Entry) []any {
	out := make([]any, len(entries))
	for i, e := range entries {
		out[i] = e
	}
	return out
}

// ------------------------------------------------------------------
// W4: build chain -- each body builds the previous STAGED entry first
// ------------------------------------------------------------------
func TestHeavy004BuildChainThroughConstitutionBodiesOn1x1(t *testing.T) {
	setUpHeavy(t)
	depth := 15
	registry := map[string]*laila.Entry{}
	setRegistry(t, registry)
	tfs := newTinyTaskforces(1, 1).enter(t)
	base_manifest := manifest(py.Kw{"data": d("x", constant(1))})
	memorizeWait(t, base_manifest)
	// Level 0 has no dependency: plain sum.
	prev := variable(t, _SUM_ALL, base_manifest)
	registry["e0"] = prev
	for i := 1; i <= depth; i++ {
		m := manifest(py.Kw{"data": d("x", constant(1))})
		memorizeWait(t, m)
		e := variable(t, registerDep(t, fmt.Sprintf("e%d", i-1)), m)
		registry[fmt.Sprintf("e%d", i)] = e
		prev = e
	}
	// Build only the top; every level below is built recursively
	// from inside the constitution bodies.
	must(must(laila.Build(registry[fmt.Sprintf("e%d", depth)])).(waiter).Wait(_T_heavy))
	for i := 0; i <= depth; i++ {
		e := registry[fmt.Sprintf("e%d", i)]
		if e.State() != laila.EntryState_READY {
			t.Fatalf("e%d state %v", i, e.State())
		}
		assertEq(t, must(e.Data()), i+1)
	}
	tfs.assert_quiescent(t)
}

// ------------------------------------------------------------------
// W5: cyclic dependency between two constitutions is detected
// ------------------------------------------------------------------
func TestHeavy005CyclicBuildDependencyRaisesInsteadOfHanging(t *testing.T) {
	setUpHeavy(t)
	registry := map[string]*laila.Entry{}
	setRegistry(t, registry)
	newTinyTaskforces(2, 2).enter(t)
	ma := manifest(py.Kw{"data": d("x", constant(1))})
	mb := manifest(py.Kw{"data": d("x", constant(2))})
	memorizeWait(t, ma)
	memorizeWait(t, mb)
	a := variable(t, registerDep(t, "b"), ma)
	b := variable(t, registerDep(t, "a"), mb)
	registry["a"], registry["b"] = a, b
	fut, err := laila.Build(a)
	if err == nil {
		_, err = fut.(waiter).Wait(_T_heavy)
	}
	if err == nil || !errors.Is(err, laila.CyclicDependencyError) {
		t.Fatalf("expected CyclicDependencyError, got %v", err)
	}
	if a.State() != laila.EntryState_STAGED {
		t.Fatalf("a state %v", a.State())
	}
	if b.State() != laila.EntryState_STAGED {
		t.Fatalf("b state %v", b.State())
	}
}

// ------------------------------------------------------------------
// W6: concurrent sync realize from many user threads on 1 slot
// ------------------------------------------------------------------
func TestHeavy006ConcurrentRealizedFromThreadsOn1x1(t *testing.T) {
	setUpHeavy(t)
	leaves := constants(50)
	m := manifestOf(leaves)
	tfs := newTinyTaskforces(1, 1).enter(t)
	memorizeWait(t, m)
	results := []int{}
	errs := []error{}
	var lock sync.Mutex

	worker := func() {
		total, err := sumRealized(m)
		lock.Lock()
		defer lock.Unlock()
		if err != nil {
			errs = append(errs, err)
			return
		}
		results = append(results, total)
	}

	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker()
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Duration(_T_heavy * float64(time.Second))):
		t.Fatal("realized() hung")
	}
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	want := make([]int, 24)
	for i := range want {
		want[i] = sumRange(50)
	}
	assertEq(t, results, want)
	tfs.assert_quiescent(t)
}

// ------------------------------------------------------------------
// W7: wide remember/memorize group futures on 1 slot
// ------------------------------------------------------------------
func TestHeavy007WideMemorizeAndRememberOn1x1(t *testing.T) {
	setUpHeavy(t)
	n := 400
	entries := constants(n)
	tfs := newTinyTaskforces(1, 1).enter(t)
	must(must(laila.Memorize(py.ListFrom(anys(entries)))).(waiter).Wait(_T_heavy))
	gids := py.NewList()
	for _, e := range entries {
		gids.Append(e.Global_id())
	}
	got := must(must(laila.Remember(gids)).(waiter).Wait(_T_heavy))
	data := []any{}
	for _, g := range py.Iter(got) {
		data = append(data, entryData(t, g))
	}
	want := []any{}
	for i := 0; i < n; i++ {
		want = append(want, i)
	}
	assertEq(t, data, want)
	tfs.assert_quiescent(t)
}

// ------------------------------------------------------------------
// W8: user job mixing await-remember with nested builds
// ------------------------------------------------------------------
func TestHeavy008UserJobsAwaitingRememberAndBuildConcurrently(t *testing.T) {
	setUpHeavy(t)
	leaves := constants(30)
	m := manifestOf(leaves)
	tfs := newTinyTaskforces(1, 1).enter(t)
	memorizeWait(t, m)

	job := coro(func(args ...any) (any, error) {
		gid := args[0]
		ref, err := laila.Remember(gid)
		if err != nil {
			return nil, err
		}
		leaf, err := py.Await(ref)
		if err != nil {
			return nil, err
		}
		staged, err := laila.Variable(py.Kw{"constitution": _SUM_ALL, "manifest": m})
		if err != nil {
			return nil, err
		}
		fut, err := laila.Build(staged)
		if err != nil {
			return nil, err
		}
		built, err := py.Await(fut)
		if err != nil {
			return nil, err
		}
		leaf_data, err := laila.As_entry_(leaf).Data()
		if err != nil {
			return nil, err
		}
		return must(py.Int(leaf_data)) + must(py.Int(_unwrap(built))), nil
	})

	tasks := []any{}
	for _, e := range leaves {
		tasks = append(tasks, job.Partial(e.Global_id())) // lambda g=e.global_id: job(g)
	}
	gf := must(_command().Submit(tasks))
	outs := []any{}
	for _, r := range py.Iter(must(gf.(waiter).Wait(_T_heavy))) {
		outs = append(outs, _unwrap(r))
	}
	want := []any{}
	for i := 0; i < 30; i++ {
		want = append(want, i+sumRange(30))
	}
	assertEq(t, outs, want)
	tfs.assert_quiescent(t)
}
