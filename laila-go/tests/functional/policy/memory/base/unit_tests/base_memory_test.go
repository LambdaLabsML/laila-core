// Port of tests/functional/policy/memory/base/unit_tests/test_base_memory.py.
//
// Python stubs the memory's private dispatch methods with
// ``unittest.mock.Mock``; the Go port does the same through a subclass
// (mockMemory) whose overrides are reached via the Central_memory_hooks
// dispatch, recording calls the way Mock does.
package unit_tests

import (
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
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

func assertRaises(t *testing.T, exc *py.Type, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", exc.Name)
	}
	if !errors.Is(err, exc) {
		t.Fatalf("expected %s, got %v", exc.Name, err)
	}
}

type waiter interface{ Wait(timeout ...any) (any, error) }

// wait is ``future.wait()`` on a Future / GroupFuture handle.
func wait(t *testing.T, ref any) any {
	t.Helper()
	w, ok := ref.(waiter)
	if !ok {
		t.Fatalf("%T has no wait()", ref)
	}
	return must(w.Wait())
}

func waitErr(ref any) (any, error) {
	w, ok := ref.(waiter)
	if !ok {
		return nil, py.TypeError.Newf("%T has no wait()", ref)
	}
	return w.Wait()
}

func keysOf(t *testing.T, p *laila.LAILA_IDENTIFIABLE_POOL_, kw ...py.Kw) []string {
	t.Helper()
	out := []string{}
	for _, k := range py.Iter(must(p.Keys(kw...))) {
		out = append(out, py.Str(k))
	}
	return out
}

func sortedKeys(t *testing.T, p *laila.LAILA_IDENTIFIABLE_POOL_, kw ...py.Kw) []string {
	ks := keysOf(t, p, kw...)
	sort.Strings(ks)
	return ks
}

func entryOf(t *testing.T, v any) *laila.Entry {
	t.Helper()
	e := laila.As_entry_(v)
	if e == nil {
		t.Fatalf("%T is not an Entry", v)
	}
	return e
}

func newPool(t *testing.T, kw ...py.Kw) *laila.LAILA_IDENTIFIABLE_POOL_ {
	t.Helper()
	return must(laila.New_LAILA_IDENTIFIABLE_POOL_(kw...))
}

// ---------------------------------------------------------------------
// mockMemory -- ``Mock``-able subclass of the central memory.
// ---------------------------------------------------------------------

type call struct {
	args []any
	kw   py.Kw
}

// mock is a ``unittest.mock.Mock(return_value=...)`` stand-in.
type mock struct {
	return_value any
	calls        []call
}

func (m *mock) assert_called_once(t *testing.T, name string) {
	t.Helper()
	if len(m.calls) != 1 {
		t.Fatalf("%s: expected 1 call, got %d", name, len(m.calls))
	}
}

func (m *mock) assert_not_called(t *testing.T, name string) {
	t.Helper()
	if len(m.calls) != 0 {
		t.Fatalf("%s: expected no calls, got %d", name, len(m.calls))
	}
}

func (m *mock) call_args() call { return m.calls[len(m.calls)-1] }

type mockMemory struct {
	laila.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_

	Record_m                     *mock `py:"-"`
	Parallel_individual_record_m *mock `py:"-"`
	Batch_accelerated_record_m   *mock `py:"-"`
	Fetch_m                      *mock `py:"-"`
	Parallel_individual_fetch_m  *mock `py:"-"`
	Batch_accelerated_fetch_m    *mock `py:"-"`
	Delete_m                     *mock `py:"-"`
	Parallel_individual_delete_m *mock `py:"-"`
	Batch_accelerated_delete_m   *mock `py:"-"`
}

var mockMemory_class = py.NewClass("_LAILA_IDENTIFIABLE_CENTRAL_MEMORY", (*mockMemory)(nil),
	laila.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_class)

func newMockMemory(t *testing.T) *mockMemory {
	t.Helper()
	m := mockMemory_class.New().(*mockMemory)
	if err := m.Init_(py.Kw{}); err != nil {
		t.Fatal(err)
	}
	return m
}

func (m *mockMemory) Record_(entries *py.List, pool *laila.LAILA_IDENTIFIABLE_POOL_) (any, error) {
	if m.Record_m != nil {
		m.Record_m.calls = append(m.Record_m.calls, call{args: []any{entries, pool}})
		return m.Record_m.return_value, nil
	}
	return m.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_.Record_(entries, pool)
}

func (m *mockMemory) Parallel_individual_record_(entries *py.List, pool *laila.LAILA_IDENTIFIABLE_POOL_) (any, error) {
	if m.Parallel_individual_record_m != nil {
		m.Parallel_individual_record_m.calls = append(m.Parallel_individual_record_m.calls, call{args: []any{entries, pool}})
		return m.Parallel_individual_record_m.return_value, nil
	}
	return m.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_.Parallel_individual_record_(entries, pool)
}

func (m *mockMemory) Batch_accelerated_record_(entries *py.List, pool *laila.LAILA_IDENTIFIABLE_POOL_) (any, error) {
	if m.Batch_accelerated_record_m != nil {
		m.Batch_accelerated_record_m.calls = append(m.Batch_accelerated_record_m.calls, call{args: []any{entries, pool}})
		return m.Batch_accelerated_record_m.return_value, nil
	}
	return m.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_.Batch_accelerated_record_(entries, pool)
}

func (m *mockMemory) Fetch_(entry_ids []string, kw ...py.Kw) (any, error) {
	if m.Fetch_m != nil {
		m.Fetch_m.calls = append(m.Fetch_m.calls, call{args: []any{entry_ids}, kw: py.MergeKw(kw...)})
		return m.Fetch_m.return_value, nil
	}
	return m.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_.Fetch_(entry_ids, kw...)
}

func (m *mockMemory) Parallel_individual_fetch_(entry_ids []string, kw ...py.Kw) (any, error) {
	if m.Parallel_individual_fetch_m != nil {
		m.Parallel_individual_fetch_m.calls = append(m.Parallel_individual_fetch_m.calls, call{args: []any{entry_ids}, kw: py.MergeKw(kw...)})
		return m.Parallel_individual_fetch_m.return_value, nil
	}
	return m.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_.Parallel_individual_fetch_(entry_ids, kw...)
}

func (m *mockMemory) Batch_accelerated_fetch_(keys []string, kw ...py.Kw) (any, error) {
	if m.Batch_accelerated_fetch_m != nil {
		m.Batch_accelerated_fetch_m.calls = append(m.Batch_accelerated_fetch_m.calls, call{args: []any{keys}, kw: py.MergeKw(kw...)})
		return m.Batch_accelerated_fetch_m.return_value, nil
	}
	return m.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_.Batch_accelerated_fetch_(keys, kw...)
}

func (m *mockMemory) Delete_(entry_ids []string, pool *laila.LAILA_IDENTIFIABLE_POOL_) (any, error) {
	if m.Delete_m != nil {
		m.Delete_m.calls = append(m.Delete_m.calls, call{args: []any{entry_ids, pool}})
		return m.Delete_m.return_value, nil
	}
	return m.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_.Delete_(entry_ids, pool)
}

func (m *mockMemory) Parallel_individual_delete_(entry_ids []string, pool *laila.LAILA_IDENTIFIABLE_POOL_) (any, error) {
	if m.Parallel_individual_delete_m != nil {
		m.Parallel_individual_delete_m.calls = append(m.Parallel_individual_delete_m.calls, call{args: []any{entry_ids}, kw: py.Kw{"pool": pool}})
		return m.Parallel_individual_delete_m.return_value, nil
	}
	return m.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_.Parallel_individual_delete_(entry_ids, pool)
}

func (m *mockMemory) Batch_accelerated_delete_(entry_ids []string, pool *laila.LAILA_IDENTIFIABLE_POOL_) (any, error) {
	if m.Batch_accelerated_delete_m != nil {
		m.Batch_accelerated_delete_m.calls = append(m.Batch_accelerated_delete_m.calls, call{args: []any{entry_ids}, kw: py.Kw{"pool": pool}})
		return m.Batch_accelerated_delete_m.return_value, nil
	}
	return m.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_.Batch_accelerated_delete_(entry_ids, pool)
}

// ---------------------------------------------------------------------
// TestBaseMemory fixture
// ---------------------------------------------------------------------

type baseCase struct {
	t        *testing.T
	original any
	policy   *laila.LAILA_IDENTIFIABLE_POLICY_
	memory   *mockMemory
}

func setUp(t *testing.T) *baseCase {
	t.Helper()
	c := &baseCase{t: t}
	c.original = must(laila.Get_active_policy())
	c.policy = must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.Activate_policy(c.policy)
	c.memory = newMockMemory(t)
	c.policy.Central.Memory = c.memory
	t.Cleanup(func() {
		if cmd, ok := c.policy.Central.Command.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_); ok {
			_ = cmd.Shutdown(py.Kw{"wait": true, "cancel_pending": true})
		}
		laila.Activate_policy(c.original)
		laila.Local_policies_.PopDefault(c.policy.Global_id(), nil)
	})
	return c
}

func (c *baseCase) router() *laila.LAILA_IDENTIFIABLE_POOL_ROUTER_ {
	return c.memory.Pool_router.(*laila.LAILA_IDENTIFIABLE_POOL_ROUTER_)
}

func (c *baseCase) extend(pool any, kw ...py.Kw) {
	c.t.Helper()
	if err := c.memory.Extend(pool, kw...); err != nil {
		c.t.Fatal(err)
	}
}

func TestModelPostInitSetsPoolRouter(t *testing.T) {
	c := setUp(t)
	if c.memory.Pool_router == nil {
		t.Fatal("pool_router is None")
	}
	if !py.Isinstance(c.memory.Pool_router, laila.LAILA_IDENTIFIABLE_POOL_ROUTER_class) {
		t.Fatal("pool_router type")
	}
}

func TestExtendAddsIntoActualRouter(t *testing.T) {
	c := setUp(t)
	pool := newPool(t)
	c.extend(pool, py.Kw{"affinity": 0.25, "pool_nickname": "secondary"})
	if !c.router().Pools.Contains(pool.Global_id()) {
		t.Fatal("pool not in router.pools")
	}
	assertEq(t, c.router().Pools_nicknames.GetDefault("secondary", nil), pool.Global_id())
}

func TestRecordDispatchesToNonBatch(t *testing.T) {
	c := setUp(t)
	pool := newPool(t, py.Kw{"batch_accelerated": false})
	c.memory.Parallel_individual_record_m = &mock{return_value: "non-batch"}
	c.memory.Batch_accelerated_record_m = &mock{return_value: "batch"}

	out := must(c.memory.Record_(py.NewList("e"), pool))
	assertEq(t, out, "non-batch")
	c.memory.Parallel_individual_record_m.assert_called_once(t, "_parallel_individual_record")
	c.memory.Batch_accelerated_record_m.assert_not_called(t, "_batch_accelerated_record")
}

func TestRecordDispatchesToBatch(t *testing.T) {
	c := setUp(t)
	pool := newPool(t, py.Kw{"batch_accelerated": true})
	c.memory.Parallel_individual_record_m = &mock{return_value: "non-batch"}
	c.memory.Batch_accelerated_record_m = &mock{return_value: "batch"}

	out := must(c.memory.Record_(py.NewList("e"), pool))
	assertEq(t, out, "batch")
	c.memory.Batch_accelerated_record_m.assert_called_once(t, "_batch_accelerated_record")
	c.memory.Parallel_individual_record_m.assert_not_called(t, "_parallel_individual_record")
}

func TestFetchBorrowTrueRaisesNotImplemented(t *testing.T) {
	c := setUp(t)
	_, err := c.memory.Fetch_([]string{"id1"}, py.Kw{"pool": newPool(t), "borrow": true})
	assertRaises(t, py.NotImplementedError, err)
}

func TestFetchDispatchesByBatchFlag(t *testing.T) {
	c := setUp(t)
	c.memory.Parallel_individual_fetch_m = &mock{return_value: "non-batch-fetch"}
	c.memory.Batch_accelerated_fetch_m = &mock{return_value: "batch-fetch"}

	out_non_batch := must(c.memory.Fetch_([]string{"id1"}, py.Kw{"pool": newPool(t, py.Kw{"batch_accelerated": false})}))
	out_batch := must(c.memory.Fetch_([]string{"id1"}, py.Kw{"pool": newPool(t, py.Kw{"batch_accelerated": true})}))

	assertEq(t, out_non_batch, "non-batch-fetch")
	assertEq(t, out_batch, "batch-fetch")
}

func TestDeleteDispatchesByBatchFlag(t *testing.T) {
	c := setUp(t)
	c.memory.Parallel_individual_delete_m = &mock{return_value: "non-batch-delete"}
	c.memory.Batch_accelerated_delete_m = &mock{return_value: "batch-delete"}

	out_non_batch := must(c.memory.Delete_([]string{"id1"}, newPool(t, py.Kw{"batch_accelerated": false})))
	out_batch := must(c.memory.Delete_([]string{"id1"}, newPool(t, py.Kw{"batch_accelerated": true})))

	assertEq(t, out_non_batch, "non-batch-delete")
	assertEq(t, out_batch, "batch-delete")
}

func TestMemorizeReturnsFuturesForDefaultPool(t *testing.T) {
	c := setUp(t)
	c.memory.Record_m = &mock{return_value: "futures"}

	out := must(c.memory.Memorize(py.NewList(must(laila.Constant(1)))))
	assertEq(t, out, "futures")
	c.memory.Record_m.assert_called_once(t, "_record")
}

func TestMemorizeReturnsFuturesForNonDefaultPool(t *testing.T) {
	c := setUp(t)
	routed := newPool(t)
	c.extend(routed, py.Kw{"pool_nickname": "secondary"})
	c.memory.Record_m = &mock{return_value: "futures"}

	out := must(c.memory.Memorize(py.NewList(must(laila.Constant(1))), py.Kw{"pool_id": routed.Global_id()}))
	assertEq(t, out, "futures")
}

func TestMemorizeSingleEntryIsWrappedToList(t *testing.T) {
	c := setUp(t)
	routed := newPool(t)
	c.extend(routed, py.Kw{"pool_nickname": "secondary"})
	c.memory.Record_m = &mock{return_value: "futures"}

	e := must(laila.Constant(1))
	must(c.memory.Memorize(e, py.Kw{"pool_id": routed.Global_id()}))

	recorded_entries, ok := c.memory.Record_m.call_args().args[0].(*py.List)
	if !ok {
		t.Fatal("entries should be a list")
	}
	assertEq(t, recorded_entries.Len(), 1)
	if must(recorded_entries.Get(0)) != any(e) {
		t.Fatal("entries[0] is not e")
	}
}

func TestRememberReturnsFuturesForDefaultPool(t *testing.T) {
	c := setUp(t)
	c.memory.Fetch_m = &mock{return_value: "fetched"}

	out := must(c.memory.Remember(py.NewList("id1")))
	assertEq(t, out, "fetched")
	c.memory.Fetch_m.assert_called_once(t, "_fetch")
}

func TestRememberReturnsFuturesForNonDefaultPool(t *testing.T) {
	c := setUp(t)
	routed := newPool(t)
	c.extend(routed, py.Kw{"pool_nickname": "secondary"})
	c.memory.Fetch_m = &mock{return_value: "fetched"}

	out := must(c.memory.Remember(py.NewList("id1"), py.Kw{"pool_id": routed.Global_id(), "persist": false}))
	assertEq(t, out, "fetched")
	c.memory.Fetch_m.assert_called_once(t, "_fetch")
}

func TestForgetReturnsFuturesForDefaultPool(t *testing.T) {
	c := setUp(t)
	c.memory.Delete_m = &mock{return_value: "deleted"}

	out := must(c.memory.Forget(py.NewList("id1")))
	assertEq(t, out, "deleted")
	c.memory.Delete_m.assert_called_once(t, "_delete")
}

func TestForgetReturnsFuturesForNonDefaultPool(t *testing.T) {
	c := setUp(t)
	routed := newPool(t)
	c.extend(routed, py.Kw{"pool_nickname": "secondary"})
	c.memory.Delete_m = &mock{return_value: "deleted"}

	out := must(c.memory.Forget(py.NewList("id1"), py.Kw{"pool_id": routed.Global_id()}))
	assertEq(t, out, "deleted")
	c.memory.Delete_m.assert_called_once(t, "_delete")
}

func TestRouterHasDefaultPoolNicknameMapping(t *testing.T) {
	c := setUp(t)
	if !c.router().Pools_nicknames.Contains(laila.DEFAULT_POOL_NICKNAME_) {
		t.Fatal("default nickname missing")
	}
	default_pool_id := c.router().Pools_nicknames.GetDefault(laila.DEFAULT_POOL_NICKNAME_, nil)
	if !c.router().Pools.Contains(default_pool_id) {
		t.Fatal("default pool missing from pools")
	}
}

func TestRouterRouteByPoolIdReturnsAddedPool(t *testing.T) {
	c := setUp(t)
	routed := newPool(t)
	c.extend(routed, py.Kw{"pool_nickname": "secondary"})
	out := must(c.router().Route(py.NewList("x"), py.Kw{"pool_id": routed.Global_id()}))
	if out != any(routed) {
		t.Fatal("route(pool_id) is not routed")
	}
}

func TestRouterRouteByNicknameReturnsAddedPool(t *testing.T) {
	c := setUp(t)
	routed := newPool(t)
	c.extend(routed, py.Kw{"pool_nickname": "secondary"})
	out := must(c.router().Route(py.NewList("x"), py.Kw{"pool_nickname": "secondary"}))
	if out != any(routed) {
		t.Fatal("route(pool_nickname) is not routed")
	}
}

func TestRouterPoolIdPrecedesPoolNickname(t *testing.T) {
	c := setUp(t)
	pool_a, pool_b := newPool(t), newPool(t)
	c.extend(pool_a, py.Kw{"pool_nickname": "A"})
	c.extend(pool_b, py.Kw{"pool_nickname": "B"})
	out := must(c.router().Route(py.NewList("x"), py.Kw{"pool_id": pool_a.Global_id(), "pool_nickname": "B"}))
	if out != any(pool_a) {
		t.Fatal("pool_id should win")
	}
}

func TestRecordNonBatchDispatchCallsNonBatchOnce(t *testing.T) {
	c := setUp(t)
	pool := newPool(t, py.Kw{"batch_accelerated": false})
	c.memory.Parallel_individual_record_m = &mock{return_value: "ok"}
	c.memory.Batch_accelerated_record_m = &mock{return_value: "no"}
	entries := py.NewList("e1", "e2")
	must(c.memory.Record_(entries, pool))
	c.memory.Parallel_individual_record_m.assert_called_once(t, "_parallel_individual_record")
	ca := c.memory.Parallel_individual_record_m.call_args()
	if ca.args[0] != any(entries) || ca.args[1] != any(pool) {
		t.Fatal("assert_called_once_with([\"e1\", \"e2\"], pool)")
	}
	c.memory.Batch_accelerated_record_m.assert_not_called(t, "_batch_accelerated_record")
}

func TestFetchNonBatchDispatchCallsNonBatchOnce(t *testing.T) {
	c := setUp(t)
	pool := newPool(t, py.Kw{"batch_accelerated": false})
	c.memory.Parallel_individual_fetch_m = &mock{return_value: "ok"}
	c.memory.Batch_accelerated_fetch_m = &mock{return_value: "no"}
	must(c.memory.Fetch_([]string{"id1"}, py.Kw{"pool": pool}))
	c.memory.Parallel_individual_fetch_m.assert_called_once(t, "_parallel_individual_fetch")
	ca := c.memory.Parallel_individual_fetch_m.call_args()
	assertEq(t, ca.args[0], []string{"id1"})
	if ca.kw["pool"] != any(pool) {
		t.Fatal("assert_called_once_with([\"id1\"], pool=pool)")
	}
	c.memory.Batch_accelerated_fetch_m.assert_not_called(t, "_batch_accelerated_fetch")
}

func TestDeleteNonBatchDispatchCallsNonBatchOnce(t *testing.T) {
	c := setUp(t)
	pool := newPool(t, py.Kw{"batch_accelerated": false})
	c.memory.Parallel_individual_delete_m = &mock{return_value: "ok"}
	c.memory.Batch_accelerated_delete_m = &mock{return_value: "no"}
	must(c.memory.Delete_([]string{"id1"}, pool))
	c.memory.Parallel_individual_delete_m.assert_called_once(t, "_parallel_individual_delete")
	ca := c.memory.Parallel_individual_delete_m.call_args()
	assertEq(t, ca.args[0], []string{"id1"})
	if ca.kw["pool"] != any(pool) {
		t.Fatal("assert_called_once_with([\"id1\"], pool=pool)")
	}
	c.memory.Batch_accelerated_delete_m.assert_not_called(t, "_batch_accelerated_delete")
}

func TestRememberSingleIdIsWrappedToList(t *testing.T) {
	c := setUp(t)
	routed := newPool(t)
	c.extend(routed, py.Kw{"pool_nickname": "secondary"})
	c.memory.Fetch_m = &mock{return_value: "fetched"}

	must(c.memory.Remember("id1", py.Kw{"pool_id": routed.Global_id(), "persist": false}))

	assertEq(t, c.memory.Fetch_m.call_args().args[0], []string{"id1"})
}

func TestRememberSetIdsAreWrappedToList(t *testing.T) {
	c := setUp(t)
	routed := newPool(t)
	c.extend(routed, py.Kw{"pool_nickname": "secondary"})
	c.memory.Fetch_m = &mock{return_value: "fetched"}

	must(c.memory.Remember(py.NewSet("id1", "id2"), py.Kw{"pool_id": routed.Global_id(), "persist": false}))

	fetched_ids := c.memory.Fetch_m.call_args().args[0].([]string)
	sort.Strings(fetched_ids)
	assertEq(t, fetched_ids, []string{"id1", "id2"})
}

func TestForgetSingleIdIsWrappedToList(t *testing.T) {
	c := setUp(t)
	routed := newPool(t)
	c.extend(routed, py.Kw{"pool_nickname": "secondary"})
	c.memory.Delete_m = &mock{return_value: "deleted"}

	must(c.memory.Forget("id1", py.Kw{"pool_id": routed.Global_id()}))

	assertEq(t, c.memory.Delete_m.call_args().args[0], []string{"id1"})
}

func TestForgetFrozensetIdsAreWrappedToList(t *testing.T) {
	c := setUp(t)
	routed := newPool(t)
	c.extend(routed, py.Kw{"pool_nickname": "secondary"})
	c.memory.Delete_m = &mock{return_value: "deleted"}

	must(c.memory.Forget(py.FrozenSet{Set: py.NewSet("id1", "id2")}, py.Kw{"pool_id": routed.Global_id()}))

	deleted_ids := c.memory.Delete_m.call_args().args[0].([]string)
	sort.Strings(deleted_ids)
	assertEq(t, deleted_ids, []string{"id1", "id2"})
}

// resultData is ``r.data if hasattr(r, "data") else r``.
func resultData(r any) any {
	if e := laila.As_entry_(r); e != nil {
		return must(e.Data())
	}
	return r
}

func TestDuplicatePoolCopiesEntriesAndReturnsGroupFuture(t *testing.T) {
	c := setUp(t)
	src_pool, dest_pool := newPool(t), newPool(t)
	c.extend(src_pool, py.Kw{"pool_nickname": "src"})
	c.extend(dest_pool, py.Kw{"pool_nickname": "dest"})

	entries := py.NewList(
		must(laila.Constant("alpha", py.Kw{"nickname": "dup-alpha"})),
		must(laila.Constant(py.DictOf("x", 1), py.Kw{"nickname": "dup-beta"})),
	)

	upload_gf := must(c.memory.Memorize(entries, py.Kw{"pool_id": src_pool.Global_id()}))
	wait(t, upload_gf)

	duplicate_futures := must(c.memory.Duplicate_pool_(src_pool, dest_pool, py.Kw{"inflight_max_entries": 1}))
	assertEq(t, duplicate_futures.Len(), 2)

	duplicated_results := wait(t, duplicate_futures)
	duplicated_entry_ids := []any{}
	for _, r := range py.Iter(duplicated_results) {
		duplicated_entry_ids = append(duplicated_entry_ids, resultData(r))
	}
	want := []any{}
	for _, e := range entries.Items() {
		want = append(want, laila.As_entry_(e).Global_id())
	}
	assertEq(t, must(py.Sorted(duplicated_entry_ids)), must(py.Sorted(want)))
	status := must(laila.Status(duplicate_futures)).(*py.Dict)
	assertEq(t, status.GetDefault("percentages", nil).(*py.Dict).GetDefault("finished", nil), 100.0)

	remembered_gf := must(c.memory.Remember(py.NewList(want...), py.Kw{"pool_id": dest_pool.Global_id()}))
	remembered_entries := wait(t, remembered_gf)
	got := []any{}
	for _, e := range py.Iter(remembered_entries) {
		got = append(got, must(entryOf(t, e).Data()))
	}
	assertEq(t, got, []any{"alpha", py.DictOf("x", 1)})
}

func TestDuplicatePoolEmptySourceReturnsEmptyGroupFuture(t *testing.T) {
	c := setUp(t)
	src_pool, dest_pool := newPool(t), newPool(t)
	c.extend(src_pool, py.Kw{"pool_nickname": "src-empty"})
	c.extend(dest_pool, py.Kw{"pool_nickname": "dest-empty"})

	duplicate_futures := must(c.memory.Duplicate_pool_(src_pool, dest_pool))
	assertEq(t, duplicate_futures.Len(), 0)
	assertEq(t, wait(t, duplicate_futures), py.NewList())
}

func TestDuplicatePoolInflightMaxEntriesLessThanOneRaises(t *testing.T) {
	c := setUp(t)
	src_pool, dest_pool := newPool(t), newPool(t)
	c.extend(src_pool, py.Kw{"pool_nickname": "src-invalid"})
	c.extend(dest_pool, py.Kw{"pool_nickname": "dest-invalid"})

	_, err := c.memory.Duplicate_pool_(src_pool, dest_pool, py.Kw{"inflight_max_entries": 0})
	assertRaises(t, py.ValueError, err)
}

func TestPoolLeOperatorDuplicatesSourceIntoDestination(t *testing.T) {
	c := setUp(t)
	src_pool, dest_pool := newPool(t), newPool(t)
	c.extend(src_pool, py.Kw{"pool_nickname": "src-op"})
	c.extend(dest_pool, py.Kw{"pool_nickname": "dest-op"})

	entry := must(laila.Constant("via-operator", py.Kw{"nickname": "dup-operator"}))
	bank := c.policy.Future_bank
	identity := must(c.memory.Memorize(entry, py.Kw{"pool_id": src_pool.Global_id()}))
	wait(t, bank.GetDefault(py.Str(laila.Gid_of_(identity)), nil))

	duplicate_futures := must(dest_pool.Le(src_pool))
	if _, ok := duplicate_futures.(*laila.GroupFuture); !ok {
		t.Fatalf("%T is not a GroupFuture", duplicate_futures)
	}
	dup_results := wait(t, duplicate_futures)
	dup_ids := []any{}
	for _, r := range py.Iter(dup_results) {
		dup_ids = append(dup_ids, resultData(r))
	}
	assertEq(t, dup_ids, []any{entry.Global_id()})

	remember_identity := must(c.memory.Remember(entry.Global_id(), py.Kw{"pool_id": dest_pool.Global_id()}))
	remembered_entry := wait(t, bank.GetDefault(py.Str(laila.Gid_of_(remember_identity)), nil))
	assertEq(t, must(entryOf(t, remembered_entry).Data()), "via-operator")
}

// ---------------------------------------------------------------------
// _CountingPool / _CustomIndexedPool
// ---------------------------------------------------------------------

// CountingPool is an in-memory pool that counts ``_keys`` scans (to
// assert index short-circuits).
type CountingPool struct {
	laila.LAILA_IDENTIFIABLE_POOL_
	Scan_count int `py:"-"`
}

var CountingPool_class = py.NewClass("_CountingPool", (*CountingPool)(nil), laila.LAILA_IDENTIFIABLE_POOL_class)

func newCountingPool(t *testing.T, kw ...py.Kw) *CountingPool {
	t.Helper()
	p := CountingPool_class.New().(*CountingPool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *CountingPool) Keys_(kw ...py.Kw) (any, error) {
	p.Scan_count += 1
	return p.LAILA_IDENTIFIABLE_POOL_.Keys_(kw...)
}

// CustomIndexedPool is a pool with its own ``_search_keys`` answer (an
// external index attachment).
type CustomIndexedPool struct {
	CountingPool
}

var CustomIndexedPool_class = py.NewClass("_CustomIndexedPool", (*CustomIndexedPool)(nil), CountingPool_class)

func newCustomIndexedPool(t *testing.T, kw ...py.Kw) *CustomIndexedPool {
	t.Helper()
	p := CustomIndexedPool_class.New().(*CustomIndexedPool)
	if err := p.Init_(py.MergeKw(kw...)); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *CustomIndexedPool) Search_keys_(base_gid string, attributes *py.Dict) (any, error) {
	prefix := base_gid + "@"
	_, release, err := p.Atomic()
	if err != nil {
		return nil, err
	}
	defer release()
	out := py.NewList()
	for _, k := range p.Resource.Keys() {
		ks := py.Str(k)
		if ks == base_gid || strings.HasPrefix(ks, prefix) {
			out.Append(ks)
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------
// TestEvolutionAwareMemorizeRemember -- locally-modified driven evolution
// bumps and attribute-based ``remember``.
// ---------------------------------------------------------------------

type evoCase struct {
	t        *testing.T
	original any
	policy   *laila.LAILA_IDENTIFIABLE_POLICY_
	memory   *laila.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_
	pool     *CountingPool
}

func setUpEvo(t *testing.T) *evoCase {
	t.Helper()
	c := &evoCase{t: t}
	c.original = must(laila.Get_active_policy())
	c.policy = must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.Activate_policy(c.policy)
	c.memory = laila.As_central_memory_(c.policy.Central.Memory)
	c.pool = newCountingPool(t)
	if err := c.memory.Extend(c.pool, py.Kw{"pool_nickname": "evo"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd, ok := c.policy.Central.Command.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_); ok {
			_ = cmd.Shutdown(py.Kw{"wait": true, "cancel_pending": true})
		}
		laila.Activate_policy(c.original)
		laila.Local_policies_.PopDefault(c.policy.Global_id(), nil)
	})
	return c
}

func (c *evoCase) memorize(e any) any {
	c.t.Helper()
	return wait(c.t, must(c.memory.Memorize(e, py.Kw{"pool_id": c.pool.Global_id()})))
}

func (c *evoCase) rememberErr(ref any) (*laila.Entry, error) {
	fut, err := c.memory.Remember(ref, py.Kw{"pool_id": c.pool.Global_id(), "persist": false})
	if err != nil {
		return nil, err
	}
	out, err := waitErr(fut)
	if err != nil {
		return nil, err
	}
	return laila.As_entry_(out), nil
}

func (c *evoCase) remember(ref any) *laila.Entry {
	c.t.Helper()
	e, err := c.rememberErr(ref)
	if err != nil {
		c.t.Fatal(err)
	}
	return e
}

func (c *evoCase) poolKeys(kw ...py.Kw) []string { return keysOf(c.t, &c.pool.LAILA_IDENTIFIABLE_POOL_, kw...) }

func ts(e *laila.Entry) string { return *e.Creation_timestamp() }

func evo(e *laila.Entry) any {
	if e.Evolution() == nil {
		return nil
	}
	return *e.Evolution()
}

func setData(t *testing.T, e *laila.Entry, v any) {
	t.Helper()
	if err := e.Set_data(v); err != nil {
		t.Fatal(err)
	}
}

func (c *evoCase) storeThreeEvolutions() (*laila.Entry, []string) {
	c.t.Helper()
	v := must(laila.Variable(py.Kw{"data": py.NewList(0), "nickname": "evo-nick"}))
	stamps := []string{}
	c.memorize(v)
	stamps = append(stamps, ts(v))
	for _, i := range []int{1, 2} {
		time.Sleep(2 * time.Millisecond) // creation timestamps have millisecond precision
		setData(c.t, v, py.NewList(i))
		c.memorize(v)
		stamps = append(stamps, ts(v))
	}
	return v, stamps
}

func TestFreshVariableIsNotLocallyModifiedAndKeepsItsEvolution(t *testing.T) {
	c := setUpEvo(t)
	v := must(laila.Variable(py.Kw{"data": py.NewList(1), "evolution": 5}))
	if v.Locally_modified() {
		t.Fatal("fresh variable should not be locally modified")
	}
	c.memorize(v)
	assertEq(t, evo(v), 5)
	assertEq(t, c.poolKeys(), []string{v.Global_id()})
}

func TestMemorizeUnchangedVariableTwiceIsIdempotent(t *testing.T) {
	c := setUpEvo(t)
	v := must(laila.Variable(py.Kw{"data": py.NewList(1)}))
	c.memorize(v)
	c.memorize(v)
	assertEq(t, evo(v), 0)
	assertEq(t, len(c.poolKeys()), 1)
}

func TestMemorizeAfterDataReassignmentBumpsEvolution(t *testing.T) {
	c := setUpEvo(t)
	v := must(laila.Variable(py.Kw{"data": py.NewList(1)}))
	c.memorize(v)
	hb0 := ts(v)
	time.Sleep(2 * time.Millisecond)
	setData(t, v, py.NewList(2))
	if !v.Locally_modified() {
		t.Fatal("should be locally modified")
	}
	c.memorize(v)
	if v.Locally_modified() {
		t.Fatal("should be clean after memorize")
	}
	assertEq(t, evo(v), 1)
	if !strings.HasSuffix(v.Global_id(), "@evolution=1") {
		t.Fatalf("gid = %s", v.Global_id())
	}
	if ts(v) == hb0 {
		t.Fatal("creation_timestamp should change")
	}
	keys := sortedKeys(t, &c.pool.LAILA_IDENTIFIABLE_POOL_)
	assertEq(t, len(keys), 2)
	// The earlier evolution kept its own payload (no aliasing).
	assertEq(t, must(c.remember(keys[0]).Data()), py.NewList(1))
	assertEq(t, must(c.remember(keys[1]).Data()), py.NewList(2))
}

func TestConstantIsNeverBumped(t *testing.T) {
	c := setUpEvo(t)
	k := must(laila.Constant(1))
	c.memorize(k)
	setData(t, k, 2)
	c.memorize(k)
	if k.Evolution() != nil {
		t.Fatal("constant evolution should be None")
	}
	assertEq(t, len(c.poolKeys()), 1)
	assertEq(t, must(c.remember(k.Global_id()).Data()), 2)
}

func TestRememberedEntryStartsClean(t *testing.T) {
	c := setUpEvo(t)
	v, _ := c.storeThreeEvolutions()
	r := c.remember(v.Global_id())
	if r.Locally_modified() {
		t.Fatal("remembered entry should be clean")
	}
	c.memorize(r)
	assertEq(t, evo(r), 2)
	assertEq(t, len(c.poolKeys()), 3)
	setData(t, r, py.NewList(99))
	c.memorize(r)
	assertEq(t, evo(r), 3)
	assertEq(t, len(c.poolKeys()), 4)
}

func TestRememberWithoutEvolutionReturnsHighest(t *testing.T) {
	c := setUpEvo(t)
	c.storeThreeEvolutions()
	r := c.remember("ENTRY:evo-nick")
	assertEq(t, evo(r), 2)
	assertEq(t, must(r.Data()), py.NewList(2))
}

func TestRememberWithExplicitEvolutionIsExact(t *testing.T) {
	c := setUpEvo(t)
	c.storeThreeEvolutions()
	r := c.remember("ENTRY:evo-nick@evolution=1")
	assertEq(t, evo(r), 1)
	assertEq(t, must(r.Data()), py.NewList(1))
}

func TestRememberByCreationTimestamp(t *testing.T) {
	c := setUpEvo(t)
	_, stamps := c.storeThreeEvolutions()
	r := c.remember("ENTRY:evo-nick@creation_timestamp=" + stamps[1])
	assertEq(t, evo(r), 1)
	assertEq(t, must(r.Data()), py.NewList(1))
	r = c.remember("ENTRY:evo-nick@evolution=2,creation_timestamp=" + stamps[2])
	assertEq(t, evo(r), 2)
}

func TestRememberCreationTimestampMismatchRaisesKeyError(t *testing.T) {
	c := setUpEvo(t)
	_, stamps := c.storeThreeEvolutions()
	_, err := c.rememberErr("ENTRY:evo-nick@evolution=1,creation_timestamp=" + stamps[2])
	assertRaises(t, py.KeyError, err)
	_, err = c.rememberErr("ENTRY:evo-nick@creation_timestamp=1970-01-01T00:00:00.000+00:00")
	assertRaises(t, py.KeyError, err)
}

func TestRememberUnsupportedAttributeRaisesValueError(t *testing.T) {
	c := setUpEvo(t)
	c.storeThreeEvolutions()
	_, err := c.rememberErr("ENTRY:evo-nick@foo=bar")
	assertRaises(t, py.ValueError, err)
}

func TestRememberMissingVariableRaisesKeyError(t *testing.T) {
	c := setUpEvo(t)
	_, err := c.rememberErr("ENTRY:never-stored")
	assertRaises(t, py.KeyError, err)
}

func TestRememberPrefersExactConstantKey(t *testing.T) {
	c := setUpEvo(t)
	k := must(laila.Constant("const", py.Kw{"nickname": "mixed"}))
	c.memorize(k)
	r := c.remember("ENTRY:mixed")
	if r.Evolution() != nil {
		t.Fatal("evolution should be None")
	}
	assertEq(t, must(r.Data()), "const")
}

func TestHighestEvolutionFoundThroughProxyChain(t *testing.T) {
	c := setUpEvo(t)
	v, stamps := c.storeThreeEvolutions()
	front := newPool(t)
	must(front.Lshift(c.pool))
	if err := c.memory.Extend(front, py.Kw{"pool_nickname": "front"}); err != nil {
		t.Fatal(err)
	}
	r := entryOf(t, wait(t, must(c.memory.Remember("ENTRY:evo-nick", py.Kw{"pool_id": front.Global_id(), "persist": false}))))
	assertEq(t, evo(r), 2)
	// The winner is cached into the front tier, nothing else.
	assertEq(t, keysOf(t, front), []string{v.Global_id()})
	r = entryOf(t, wait(t, must(c.memory.Remember(
		"ENTRY:evo-nick@creation_timestamp="+stamps[0],
		py.Kw{"pool_id": front.Global_id(), "persist": false}))))
	assertEq(t, evo(r), 0)
}

func TestIndexedPoolResolvesWithoutScanning(t *testing.T) {
	c := setUpEvo(t)
	_, stamps := c.storeThreeEvolutions()
	before := c.pool.Scan_count
	assertEq(t, evo(c.remember("ENTRY:evo-nick")), 2)
	assertEq(t, evo(c.remember("ENTRY:evo-nick@evolution=-1")), 2)
	assertEq(t, evo(c.remember("ENTRY:evo-nick@evolution=-2")), 1)
	assertEq(t, evo(c.remember("ENTRY:evo-nick@creation_timestamp="+stamps[0])), 0)
	assertEq(t, c.pool.Scan_count, before)
}

func TestIndexDisabledPoolFallsBackToScan(t *testing.T) {
	c := setUpEvo(t)
	plain := newCountingPool(t, py.Kw{"index_enabled": false})
	if err := c.memory.Extend(plain, py.Kw{"pool_nickname": "plain"}); err != nil {
		t.Fatal(err)
	}
	v := must(laila.Variable(py.Kw{"data": py.NewList(0), "nickname": "plain-nick"}))
	wait(t, must(c.memory.Memorize(v, py.Kw{"pool_id": plain.Global_id()})))
	setData(t, v, py.NewList(1))
	wait(t, must(c.memory.Memorize(v, py.Kw{"pool_id": plain.Global_id()})))
	pp := &plain.LAILA_IDENTIFIABLE_POOL_
	assertEq(t, keysOf(t, pp, py.Kw{"include_index": true}), sortedKeys(t, pp)) // no shards
	before := plain.Scan_count
	r := entryOf(t, wait(t, must(c.memory.Remember("ENTRY:plain-nick@evolution=-1", py.Kw{"pool_id": plain.Global_id(), "persist": false}))))
	assertEq(t, evo(r), 1)
	if plain.Scan_count <= before {
		t.Fatalf("scan_count %d should exceed %d", plain.Scan_count, before)
	}
}

func TestCustomSearchKeysHookShortCircuitsScan(t *testing.T) {
	c := setUpEvo(t)
	indexed := newCustomIndexedPool(t, py.Kw{"index_enabled": false})
	if err := c.memory.Extend(indexed, py.Kw{"pool_nickname": "indexed"}); err != nil {
		t.Fatal(err)
	}
	v := must(laila.Variable(py.Kw{"data": py.NewList(0), "nickname": "indexed-nick"}))
	wait(t, must(c.memory.Memorize(v, py.Kw{"pool_id": indexed.Global_id()})))
	setData(t, v, py.NewList(1))
	wait(t, must(c.memory.Memorize(v, py.Kw{"pool_id": indexed.Global_id()})))
	before := indexed.Scan_count
	r := entryOf(t, wait(t, must(c.memory.Remember("ENTRY:indexed-nick", py.Kw{"pool_id": indexed.Global_id(), "persist": false}))))
	assertEq(t, evo(r), 1)
	assertEq(t, indexed.Scan_count, before)
}

func TestNegativeEvolutionOutOfRangeRaises(t *testing.T) {
	c := setUpEvo(t)
	c.storeThreeEvolutions()
	_, err := c.rememberErr("ENTRY:evo-nick@evolution=-99")
	assertRaises(t, py.KeyError, err)
	_, err = c.rememberErr("ENTRY:never-there@evolution=-1")
	assertRaises(t, py.KeyError, err)
}

func TestNegativeEvolutionWithTimestampMustAgree(t *testing.T) {
	c := setUpEvo(t)
	_, stamps := c.storeThreeEvolutions()
	r := c.remember("ENTRY:evo-nick@evolution=-1,creation_timestamp=" + stamps[2])
	assertEq(t, evo(r), 2)
	_, err := c.rememberErr("ENTRY:evo-nick@evolution=-1,creation_timestamp=" + stamps[0])
	assertRaises(t, py.KeyError, err)
}

func TestStaleIndexHitSelfHeals(t *testing.T) {
	c := setUpEvo(t)
	v, _ := c.storeThreeEvolutions()
	// Delete the latest evolution behind the index's back.
	if err := c.pool.Delete_(v.Global_id()); err != nil {
		t.Fatal(err)
	}
	r := c.remember("ENTRY:evo-nick@evolution=-1")
	assertEq(t, evo(r), 1)
	base, _, _ := strings.Cut(v.Global_id(), "@")
	assertEq(t, c.pool.Index().Candidates(base), []string{base + "@evolution=0", base + "@evolution=1"})
}

func TestForgetNegativeEvolutionDeletesLatest(t *testing.T) {
	c := setUpEvo(t)
	v, _ := c.storeThreeEvolutions()
	wait(t, must(c.memory.Forget("ENTRY:evo-nick@evolution=-1", py.Kw{"pool_id": c.pool.Global_id()})))
	if must(c.pool.Exists(v.Global_id())) {
		t.Fatal("latest should be gone")
	}
	assertEq(t, evo(c.remember("ENTRY:evo-nick")), 1)
	// An evolution-less forget stays exact-key: nothing named exactly ``base``.
	wait(t, must(c.memory.Forget("ENTRY:evo-nick", py.Kw{"pool_id": c.pool.Global_id()})))
	assertEq(t, evo(c.remember("ENTRY:evo-nick")), 1)
}

func TestWriteBackIndexesFrontTier(t *testing.T) {
	c := setUpEvo(t)
	v, _ := c.storeThreeEvolutions()
	front := newPool(t)
	must(front.Lshift(c.pool))
	if err := c.memory.Extend(front, py.Kw{"pool_nickname": "front2"}); err != nil {
		t.Fatal(err)
	}
	wait(t, must(c.memory.Remember("ENTRY:evo-nick", py.Kw{"pool_id": front.Global_id(), "persist": false})))
	base, _, _ := strings.Cut(v.Global_id(), "@")
	latest := front.Index().Latest(base)
	if latest == nil || *latest != v.Global_id() {
		t.Fatalf("latest = %v, want %s", latest, v.Global_id())
	}
}

func TestDuplicatePoolDoesNotCopyIndexShards(t *testing.T) {
	c := setUpEvo(t)
	v, _ := c.storeThreeEvolutions()
	dest := newPool(t)
	if err := c.memory.Extend(dest, py.Kw{"pool_nickname": "dest-idx"}); err != nil {
		t.Fatal(err)
	}
	wait(t, must(dest.Le(c.pool)))
	pp := &c.pool.LAILA_IDENTIFIABLE_POOL_
	assertEq(t, sortedKeys(t, dest), sortedKeys(t, pp))
	shards := func(p *laila.LAILA_IDENTIFIABLE_POOL_) []string {
		out := []string{}
		for _, k := range keysOf(t, p, py.Kw{"include_index": true}) {
			if strings.Contains(k, "POOL_INDEX") {
				out = append(out, k)
			}
		}
		return out
	}
	src_shards, dest_shards := shards(pp), shards(dest)
	assertEq(t, len(src_shards), 1)
	assertEq(t, len(dest_shards), 1)
	if py.Eq(src_shards, dest_shards) { // dest has its *own* shard
		t.Fatal("shards should differ")
	}
	base, _, _ := strings.Cut(v.Global_id(), "@")
	latest := dest.Index().Latest(base)
	if latest == nil || *latest != v.Global_id() {
		t.Fatalf("latest = %v, want %s", latest, v.Global_id())
	}
}
