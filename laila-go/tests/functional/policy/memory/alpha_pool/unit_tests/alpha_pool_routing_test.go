// Port of tests/functional/policy/memory/alpha_pool/unit_tests/test_alpha_pool_routing.py.
//
// Verify that memorize, remember, and forget without pool_nickname route
// to the alpha pool.
package unit_tests

import (
	"sort"
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

type waiter interface{ Wait(timeout ...any) (any, error) }

func wait(t *testing.T, ref any) any {
	t.Helper()
	w, ok := ref.(waiter)
	if !ok {
		t.Fatalf("%T has no wait()", ref)
	}
	return must(w.Wait())
}

var TERMINAL_ = map[string]bool{"finished": true, "error": true, "cancelled": true}

func is_done_(future any) bool {
	s := must(laila.Status(future))
	if d, ok := s.(*py.Dict); ok {
		pct, _ := d.GetDefault("percentages", py.NewDict()).(*py.Dict)
		total := 0.0
		for _, k := range []string{"finished", "error", "cancelled"} {
			total += must(py.Float(pct.GetDefault(k, 0)))
		}
		return total >= 100.0
	}
	if fs, ok := s.(laila.FutureStatus); ok {
		return TERMINAL_[fs.Value()]
	}
	return TERMINAL_[py.Str(s)]
}

// flush_ waits until all submitted tasks finish without killing the
// taskforce.
func flush_(t *testing.T, policy *laila.LAILA_IDENTIFIABLE_POLICY_, timeout float64) {
	t.Helper()
	bank := policy.Future_bank
	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))
	for time.Now().Before(deadline) {
		all := true
		for _, f := range bank.Values() {
			if !is_done_(f) {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("TimeoutError: Tasks did not finish in time")
}

// ---------------------------------------------------------------------
// patch.object(memory, "_record"/"_fetch"/"_delete") stand-in: the policy
// is created while DefaultCentralMemory points at a subclass whose
// dispatch hooks can be stubbed.
// ---------------------------------------------------------------------

type call struct {
	args []any
	kw   py.Kw
}

type mock struct{ calls []call }

type patchableMemory struct {
	laila.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_

	Record_m *mock `py:"-"`
	Fetch_m  *mock `py:"-"`
	Delete_m *mock `py:"-"`
}

var patchableMemory_class = py.NewClass("_LAILA_IDENTIFIABLE_CENTRAL_MEMORY", (*patchableMemory)(nil),
	laila.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_class)

func (m *patchableMemory) Record_(entries *py.List, pool *laila.LAILA_IDENTIFIABLE_POOL_) (any, error) {
	if m.Record_m != nil {
		m.Record_m.calls = append(m.Record_m.calls, call{args: []any{entries, pool}})
		return &struct{}{}, nil // Mock()
	}
	return m.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_.Record_(entries, pool)
}

func (m *patchableMemory) Fetch_(entry_ids []string, kw ...py.Kw) (any, error) {
	if m.Fetch_m != nil {
		m.Fetch_m.calls = append(m.Fetch_m.calls, call{args: []any{entry_ids}, kw: py.MergeKw(kw...)})
		return &struct{}{}, nil
	}
	return m.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_.Fetch_(entry_ids, kw...)
}

func (m *patchableMemory) Delete_(entry_ids []string, pool *laila.LAILA_IDENTIFIABLE_POOL_) (any, error) {
	if m.Delete_m != nil {
		m.Delete_m.calls = append(m.Delete_m.calls, call{args: []any{entry_ids, pool}})
		return &struct{}{}, nil
	}
	return m.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_.Delete_(entry_ids, pool)
}

// ---------------------------------------------------------------------
// TestAlphaPoolRouting -- operations with no pool_nickname must hit the
// alpha pool.
// ---------------------------------------------------------------------

type alphaCase struct {
	t        *testing.T
	original any
	policy   *laila.LAILA_IDENTIFIABLE_POLICY_
	memory   *patchableMemory
}

func setUp(t *testing.T) *alphaCase {
	t.Helper()
	c := &alphaCase{t: t}
	c.original = must(laila.Get_active_policy())
	prev := laila.DefaultCentralMemory
	laila.DefaultCentralMemory = patchableMemory_class
	c.policy = must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.DefaultCentralMemory = prev
	laila.Activate_policy(c.policy)
	c.memory = c.policy.Central.Memory.(*patchableMemory)
	t.Cleanup(func() {
		if cmd, ok := c.policy.Central.Command.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_); ok {
			_ = cmd.Shutdown(py.Kw{"wait": true, "cancel_pending": true})
		}
		laila.Activate_policy(c.original)
		laila.Local_policies_.PopDefault(c.policy.Global_id(), nil)
	})
	return c
}

func (c *alphaCase) router() *laila.LAILA_IDENTIFIABLE_POOL_ROUTER_ {
	return c.memory.Pool_router.(*laila.LAILA_IDENTIFIABLE_POOL_ROUTER_)
}

func (c *alphaCase) alpha_pool() *laila.LAILA_IDENTIFIABLE_POOL_ {
	return laila.As_pool_(c.router().Pools.GetDefault(c.memory.Alpha_pool, nil))
}

func (c *alphaCase) flush() { c.t.Helper(); flush_(c.t, c.policy, 2.0) }

func (c *alphaCase) bankWait(identity any) any {
	c.t.Helper()
	return wait(c.t, c.policy.Future_bank.GetDefault(py.Str(laila.Gid_of_(identity)), nil))
}

func constants(t *testing.T, data ...any) *py.List {
	t.Helper()
	out := py.NewList()
	for _, d := range data {
		out.Append(must(laila.Constant(d)))
	}
	return out
}

func gids(entries *py.List) []any {
	out := []any{}
	for _, e := range entries.Items() {
		out = append(out, laila.As_entry_(e).Global_id())
	}
	return out
}

// ---- Alpha pool setup ----

func TestAlphaPoolIsSetOnDefaultMemory(t *testing.T) {
	c := setUp(t)
	if py.IsNone(c.memory.Alpha_pool) {
		t.Fatal("alpha_pool is None")
	}
}

func TestAlphaPoolMatchesDefaultNickname(t *testing.T) {
	c := setUp(t)
	default_id := c.router().Pools_nicknames.GetDefault(laila.DEFAULT_POOL_NICKNAME_, nil)
	assertEq(t, c.memory.Alpha_pool, default_id)
}

func TestRouterDefaultRouteReturnsAlphaPool(t *testing.T) {
	c := setUp(t)
	routed := must(c.router().Route(py.NewList("any")))
	if laila.As_pool_(routed) != c.alpha_pool() {
		t.Fatal("default route is not the alpha pool")
	}
}

// ---- memorize -> alpha pool ----

func TestMemorizeNoNicknameReturnsFuture(t *testing.T) {
	c := setUp(t)
	entry := must(laila.Constant(42))
	result := must(c.memory.Memorize(entry))
	if result == nil || !py.Isinstance(result, laila.LAILA_IDENTIFIABLE_FUTURE_class) {
		t.Fatalf("result = %T", result)
	}
}

func TestMemorizeNoNicknameMultipleReturnsGroupFuture(t *testing.T) {
	c := setUp(t)
	result := must(c.memory.Memorize(constants(t, 0, 1, 2)))
	if _, ok := result.(*laila.GroupFuture); !ok {
		t.Fatalf("result = %T", result)
	}
}

func TestMemorizeNoNicknameRecordsIntoAlphaPool(t *testing.T) {
	c := setUp(t)
	entry := must(laila.Constant("hello", py.Kw{"nickname": "alpha-memo-test"}))
	must(c.memory.Memorize(entry))
	c.flush()
	if !c.alpha_pool().Resource.Contains(entry.Global_id()) {
		t.Fatal("entry not in alpha pool")
	}
}

func TestMemorizeNoNicknameCallsRecordWithAlphaPool(t *testing.T) {
	c := setUp(t)
	entry := must(laila.Constant("probe"))
	c.memory.Record_m = &mock{}
	defer func() { c.memory.Record_m = nil }()
	must(c.memory.Memorize(entry))
	if len(c.memory.Record_m.calls) != 1 {
		t.Fatalf("_record called %d times", len(c.memory.Record_m.calls))
	}
	used_pool := c.memory.Record_m.calls[0].args[1]
	if used_pool != any(c.alpha_pool()) {
		t.Fatal("_record was not called with the alpha pool")
	}
}

func TestMemorizeMultipleEntriesAllLandInAlphaPool(t *testing.T) {
	c := setUp(t)
	entries := constants(t, 0, 1, 2, 3, 4)
	must(c.memory.Memorize(entries))
	c.flush()
	for _, gid := range gids(entries) {
		if !c.alpha_pool().Resource.Contains(gid) {
			t.Fatalf("%v not in alpha pool", gid)
		}
	}
}

// ---- remember -> alpha pool ----

func TestRememberNoNicknameReturnsFuture(t *testing.T) {
	c := setUp(t)
	entry := must(laila.Constant("remember-me"))
	must(c.memory.Memorize(entry))
	c.flush()
	result := must(c.memory.Remember(py.NewList(entry.Global_id())))
	if result == nil || !py.Isinstance(result, laila.LAILA_IDENTIFIABLE_FUTURE_class) {
		t.Fatalf("result = %T", result)
	}
}

func TestRememberNoNicknameCallsFetchWithAlphaPool(t *testing.T) {
	c := setUp(t)
	c.memory.Fetch_m = &mock{}
	defer func() { c.memory.Fetch_m = nil }()
	must(c.memory.Remember(py.NewList("some-id")))
	if len(c.memory.Fetch_m.calls) != 1 {
		t.Fatalf("_fetch called %d times", len(c.memory.Fetch_m.calls))
	}
	used_pool := c.memory.Fetch_m.calls[0].kw.Get("pool", nil)
	if used_pool != any(c.alpha_pool()) {
		t.Fatal("_fetch was not called with the alpha pool")
	}
}

func TestRememberNoNicknameRecoversEntry(t *testing.T) {
	c := setUp(t)
	entry := must(laila.Constant(py.DictOf("key", "value"), py.Kw{"nickname": "recall-test"}))
	must(c.memory.Memorize(entry))
	c.flush()

	future_id := must(c.memory.Remember(py.NewList(entry.Global_id())))
	c.flush()
	recalled := laila.As_entry_(c.bankWait(future_id))
	assertEq(t, must(recalled.Data()), py.DictOf("key", "value"))
}

// ---- forget -> alpha pool ----

func TestForgetNoNicknameReturnsFuture(t *testing.T) {
	c := setUp(t)
	result := must(c.memory.Forget(py.NewList("any-id")))
	if result == nil || !py.Isinstance(result, laila.LAILA_IDENTIFIABLE_FUTURE_class) {
		t.Fatalf("result = %T", result)
	}
}

func TestForgetNoNicknameDeletesFromAlphaPool(t *testing.T) {
	c := setUp(t)
	entry := must(laila.Constant("to-be-forgotten"))
	must(c.memory.Memorize(entry))
	c.flush()
	if !c.alpha_pool().Resource.Contains(entry.Global_id()) {
		t.Fatal("entry not in alpha pool")
	}

	must(c.memory.Forget(entry.Global_id()))
	c.flush()
	if c.alpha_pool().Resource.Contains(entry.Global_id()) {
		t.Fatal("entry still in alpha pool")
	}
}

func TestForgetNoNicknameCallsDeleteWithAlphaPool(t *testing.T) {
	c := setUp(t)
	c.memory.Delete_m = &mock{}
	defer func() { c.memory.Delete_m = nil }()
	must(c.memory.Forget(py.NewList("id1")))
	if len(c.memory.Delete_m.calls) != 1 {
		t.Fatalf("_delete called %d times", len(c.memory.Delete_m.calls))
	}
	ca := c.memory.Delete_m.calls[0]
	used_pool := ca.kw.Get("pool", nil)
	if used_pool == nil && len(ca.args) > 1 {
		used_pool = ca.args[1]
	}
	if used_pool != any(c.alpha_pool()) {
		t.Fatal("_delete was not called with the alpha pool")
	}
}

// ---- End-to-end: memorize -> remember -> forget ----

func TestEndToEndMemorizeRememberForgetInAlphaPool(t *testing.T) {
	c := setUp(t)
	entries := constants(t, "a", "b", "c")

	must(c.memory.Memorize(entries))
	c.flush()
	for _, gid := range gids(entries) {
		if !c.alpha_pool().Resource.Contains(gid) {
			t.Fatalf("%v not in alpha pool", gid)
		}
	}

	recall_gf := must(c.memory.Remember(py.NewList(gids(entries)...)))
	c.flush()
	recalled := c.bankWait(recall_gf)
	got := []string{}
	for _, r := range py.Iter(recalled) {
		got = append(got, py.Str(must(laila.As_entry_(r).Data())))
	}
	sort.Strings(got)
	assertEq(t, got, []string{"a", "b", "c"})

	must(c.memory.Forget(py.NewList(gids(entries)...)))
	c.flush()
	for _, gid := range gids(entries) {
		if c.alpha_pool().Resource.Contains(gid) {
			t.Fatalf("%v still in alpha pool", gid)
		}
	}
}

// ---- Contrast: explicit pool_nickname bypasses alpha pool ----

func TestMemorizeWithExplicitNicknameSkipsAlphaPool(t *testing.T) {
	c := setUp(t)
	secondary := must(laila.New_LAILA_IDENTIFIABLE_POOL_())
	if err := c.memory.Extend(secondary, py.Kw{"pool_nickname": "secondary"}); err != nil {
		t.Fatal(err)
	}

	entry := must(laila.Constant("not-alpha"))
	futures := must(c.memory.Memorize(entry, py.Kw{"pool_nickname": "secondary"}))
	if futures == nil {
		t.Fatal("futures is None")
	}
	c.bankWait(futures)

	if !secondary.Resource.Contains(entry.Global_id()) {
		t.Fatal("entry not in secondary")
	}
	if c.alpha_pool().Resource.Contains(entry.Global_id()) {
		t.Fatal("entry leaked into alpha pool")
	}
}

func TestForgetWithExplicitNicknameSkipsAlphaPool(t *testing.T) {
	c := setUp(t)
	secondary := must(laila.New_LAILA_IDENTIFIABLE_POOL_())
	if err := c.memory.Extend(secondary, py.Kw{"pool_nickname": "secondary"}); err != nil {
		t.Fatal(err)
	}

	entry := must(laila.Constant("secondary-data"))
	store_futures := must(c.memory.Memorize(entry, py.Kw{"pool_nickname": "secondary"}))
	c.bankWait(store_futures)

	forget_futures := must(c.memory.Forget(entry.Global_id(), py.Kw{"pool_nickname": "secondary"}))
	if forget_futures == nil {
		t.Fatal("forget_futures is None")
	}
	c.bankWait(forget_futures)

	if secondary.Resource.Contains(entry.Global_id()) {
		t.Fatal("entry still in secondary")
	}
}
