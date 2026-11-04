// Port of tests/functional/policy/memory/manifest/unit_tests/test_manifest_direct_resolver.py.
//
// Functional tests for the direct-await resolver behind
// ``Manifest.realized`` / ``async_realized``
// (``central.memory._read_entries_async`` / ``_read_entries_direct``).
package unit_tests

import (
	"errors"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const _T = 30.0

type waiter interface{ Wait(timeout ...any) (any, error) }
type releaser interface{ Release() }

func _bank() *py.Dict {
	return must(laila.Get_active_policy()).(*laila.LAILA_IDENTIFIABLE_POLICY_).Future_bank
}

func _memory() *laila.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_ {
	return laila.As_central_memory_(must(laila.Memory()))
}

func _command() *laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_ {
	return must(laila.Command()).(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_)
}

func _make_manifest(n int) (*laila.Manifest, int) {
	data := py.NewDict()
	for i := 0; i < n; i++ {
		data.Set("k"+py.Str(i), constant(i))
	}
	m := manifest(py.Kw{"data": data})
	ref := must(m.Memorize())
	must(ref.Wait(_T))
	ref.Release()
	return m, n * (n - 1) / 2
}

// sumData is ``sum(e.data for e in d.values())``.
func sumData(t *testing.T, d any) int {
	t.Helper()
	total := 0
	for _, v := range d.(*py.Dict).Values() {
		total += must(py.Int(entryData(t, v)))
	}
	return total
}

// coro is ``async def f(*args)``.
func coro(fn func(args ...any) (any, error)) py.CoroutineFunc { return py.CoroutineFunc(fn) }

// futData is ``fut.wait(_T).data``.
func futData(t *testing.T, fut any) any {
	t.Helper()
	out := must(fut.(waiter).Wait(_T))
	return entryData(t, out)
}

func setUpDirect(t *testing.T) {
	t.Helper()
	must(laila.Get_active_policy())
}

func Test001RealizedUsesOneFutureAndReleasesIt(t *testing.T) {
	setUpDirect(t)
	m, expected := _make_manifest(200)
	before := _bank().Len()
	d := must(m.Realized())
	assertEq(t, sumData(t, d), expected)
	// The single batch future was released; no per-child futures exist.
	assertEq(t, _bank().Len(), before)
}

func Test002AsyncRealizedFromForeignLoop(t *testing.T) {
	setUpDirect(t)
	m, expected := _make_manifest(50)
	before := _bank().Len()

	_go := func() (any, error) {
		d, err := m.Async_realized().Await()
		if err != nil {
			return nil, err
		}
		return sumData(t, d), nil
	}

	assertEq(t, must(py.Run(_go)), expected)
	assertEq(t, _bank().Len(), before)
}

func Test003AsyncRealizedInsideTaskforceSlotCreatesNoFutures(t *testing.T) {
	setUpDirect(t)
	m, expected := _make_manifest(50)

	_task := coro(func(args ...any) (any, error) {
		bank_before := _bank().Len()
		d, err := m.Async_realized().Await()
		if err != nil {
			return nil, err
		}
		// Direct path: no future was created for the batch at all.
		bank_after := _bank().Len()
		return py.Tuple{sumData(t, d), bank_after - bank_before}, nil
	})

	fut := must(_command().Submit([]any{_task}))
	out := futData(t, fut).(py.Tuple)
	fut.(releaser).Release()
	assertEq(t, out[0], expected)
	assertEq(t, out[1], 0)
}

func Test004ReadEntriesAsyncPreservesOrderAndTypes(t *testing.T) {
	setUpDirect(t)
	m, _ := _make_manifest(30)
	gids := m.Iter()
	memory := _memory()

	_go := func() (any, error) {
		return memory.Read_entries_async_(gids)
	}

	entries := must(py.Run(_go)).(*py.List)
	got := []any{}
	for _, e := range entries.Items() {
		entry := laila.As_entry_(e)
		if entry == nil {
			t.Fatalf("%T is not an Entry", e)
		}
		if entry.State() != laila.EntryState_READY {
			t.Fatalf("state %v", entry.State())
		}
		got = append(got, entry.Global_id())
	}
	assertEq(t, got, gids)
}

func Test005MissingEntryRaisesKeyerror(t *testing.T) {
	setUpDirect(t)
	m, _ := _make_manifest(3)
	missing := constant(999).Global_id()
	broken := manifest(py.Kw{"data": d("a", m.Iter()[0], "b", missing)})
	_, err := broken.Realized()
	assertRaises(t, py.KeyError, err)
}

func Test006PersistCachesIntoAlphaPool(t *testing.T) {
	setUpDirect(t)
	memory := _memory()
	secondary := must(laila.New_DefaultPool())
	must(0, memory.Extend(secondary, py.Kw{"pool_nickname": "direct_secondary"}))
	router := memory.Pool_router.(*laila.LAILA_IDENTIFIABLE_POOL_ROUTER_)
	alpha := laila.As_pool_(must(router.Pools.Getitem(memory.Alpha_pool)))
	e := constant("side")
	ref := must(laila.Memorize(e, py.Kw{"dst_pool": "direct_secondary"}))
	must(ref.(waiter).Wait(_T))
	ref.(releaser).Release()
	if alpha.Resource.Contains(e.Global_id()) {
		t.Fatal("entry should not be in the alpha pool yet")
	}

	_go := coro(func(args ...any) (any, error) {
		return memory.Read_entries_async_([]any{e.Global_id()}, py.Kw{"pool": "direct_secondary"})
	})

	fut := must(_command().Submit([]any{_go}))
	out := futData(t, fut).(*py.List)
	fut.(releaser).Release()
	assertEq(t, entryData(t, must(out.Get(0))), "side")
	if !alpha.Resource.Contains(e.Global_id()) {
		t.Fatal("entry should have been persisted into the alpha pool")
	}

	_go_nopersist := coro(func(args ...any) (any, error) {
		return memory.Read_entries_async_([]any{e.Global_id()}, py.Kw{"pool": "direct_secondary", "persist": false})
	})

	// persist=False path still resolves.
	fut = must(_command().Submit([]any{_go_nopersist}))
	assertEq(t, entryData(t, must(futData(t, fut).(*py.List).Get(0))), "side")
	fut.(releaser).Release()
}

func Test007DirectResolverChecksTheResolveChain(t *testing.T) {
	setUpDirect(t)
	m, _ := _make_manifest(3)
	gids := m.Iter()
	memory := _memory()

	_go := func() (any, error) {
		// Pretend gids[1] is already being resolved up the causal chain.
		token := laila.RESOLVE_CHAIN_.Set([]string{py.Str(gids[1])})
		defer laila.RESOLVE_CHAIN_.Reset(token)
		return memory.Read_entries_async_(gids)
	}

	_, err := py.Run(_go)
	if err == nil || !errors.Is(err, laila.CyclicDependencyError) {
		t.Fatalf("expected CyclicDependencyError, got %v", err)
	}

	// And the chain is extended per child while reading: a child
	// constitution that re-enters the same manifest must be rejected.
	_go_clean := func() (any, error) {
		return memory.Read_entries_async_(gids)
	}

	assertEq(t, must(py.Run(_go_clean)).(*py.List).Len(), 3)
}

func Test008RealizedStillWorksInsideSyncConstitutionBody(t *testing.T) {
	setUpDirect(t)
	// A constitution body on the internal taskforce's sync executor
	// calling manifest.realized -> _read_entries_direct (internal ->
	// internal submit is rank-legal) must complete and be released.
	m, expected := _make_manifest(40)
	src := "def combine(manifest):\n    return sum(e.data for e in manifest.realized.values())\n"
	laila.Register_constitution(src, func(arg any) (any, error) {
		realized, err := laila.As_manifest_(arg).Realized()
		if err != nil {
			return nil, err
		}
		total := 0
		for _, v := range realized.(*py.Dict).Values() {
			n, err := py.Int(must(laila.As_entry_(v).Data()))
			if err != nil {
				return nil, err
			}
			total += n
		}
		return total, nil
	})
	t.Cleanup(func() { laila.Unregister_constitution(src) })
	v := must(laila.Variable(py.Kw{"constitution": src, "manifest": m}))
	before := _bank().Len()
	fut := must(laila.Build(v))
	out := must(fut.(waiter).Wait(_T))
	fut.(releaser).Release()
	assertEq(t, entryData(t, out), expected)
	if v.State() != laila.EntryState_READY {
		t.Fatalf("state %v", v.State())
	}
	assertEq(t, _bank().Len(), before)
}
