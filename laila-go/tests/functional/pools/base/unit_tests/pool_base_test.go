// Port of tests/functional/pools/base/unit_tests/test_pool_base.py.
package unit_tests

import (
	"errors"
	"fmt"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

type Pool = laila.LAILA_IDENTIFIABLE_POOL_

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

func assertCountEqual(t *testing.T, got any, want ...any) {
	t.Helper()
	g := must(py.Sorted(py.Iter(got)))
	w := must(py.Sorted(want))
	if !py.Eq(g, w) {
		t.Fatalf("got %s, want %s", py.Repr(g), py.Repr(w))
	}
}

func newPool(t *testing.T, kw ...py.Kw) *Pool {
	t.Helper()
	return must(laila.New_LAILA_IDENTIFIABLE_POOL_(kw...))
}

func set(t *testing.T, p *Pool, k string, v any) {
	t.Helper()
	if err := p.Setitem(k, v); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, p *Pool, k string) any {
	t.Helper()
	return must(p.Getitem(k))
}

func del(t *testing.T, p *Pool, k string) {
	t.Helper()
	if err := p.Delitem(k); err != nil {
		t.Fatal(err)
	}
}

func exists(t *testing.T, p *Pool, k string) bool {
	t.Helper()
	return must(p.Exists(k))
}

func existsLocal(t *testing.T, p *Pool, k string) bool {
	t.Helper()
	return must(p.Exists_(k))
}

func contains(t *testing.T, p *Pool, k string) bool {
	t.Helper()
	return must(p.Contains(k))
}

func keys(t *testing.T, p *Pool, kw ...py.Kw) any {
	t.Helper()
	return must(p.Keys(kw...))
}

func empty(t *testing.T, p *Pool) {
	t.Helper()
	if err := p.Empty(); err != nil {
		t.Fatal(err)
	}
}

func setProxyTo(t *testing.T, cache *Pool, origin any) {
	t.Helper()
	if err := cache.Set_proxy_to(origin); err != nil {
		t.Fatal(err)
	}
}

func setProxy(t *testing.T, origin *Pool, cache any) {
	t.Helper()
	if err := origin.Set_proxy(cache); err != nil {
		t.Fatal(err)
	}
}

func lshift(t *testing.T, a, b *Pool) *Pool {
	t.Helper()
	return laila.As_pool_(must(a.Lshift(b)))
}

func rshift(t *testing.T, a, b *Pool) *Pool {
	t.Helper()
	return laila.As_pool_(must(a.Rshift(b)))
}

// ---- TestPoolBaseGetItem ----

func TestGetExistingKey(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k1", "value")
	assertEq(t, get(t, pool, "k1"), "value")
}

func TestGetMissingKeyReturnsNone(t *testing.T) {
	pool := newPool(t)
	assertEq(t, get(t, pool, "nonexistent"), nil)
}

func TestGetKeyStoringNoneReturnsNone(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", nil)
	assertEq(t, get(t, pool, "k"), nil)
}

func TestMissingAndNoneIndistinguishable(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "stored_none", nil)
	assertEq(t, get(t, pool, "stored_none"), get(t, pool, "never_set"))
}

func TestOverwriteValue(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", "first")
	set(t, pool, "k", "second")
	assertEq(t, get(t, pool, "k"), "second")
}

// ---- TestPoolBaseSetItem ----

func TestSetStringValue(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", "hello")
	assertEq(t, get(t, pool, "k"), "hello")
}

func TestSetDictValue(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", py.DictOf("nested", true))
	assertEq(t, get(t, pool, "k"), py.DictOf("nested", true))
}

func TestSetListValue(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", py.NewList(1, 2, 3))
	assertEq(t, get(t, pool, "k"), py.NewList(1, 2, 3))
}

func TestSetIntValue(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", 42)
	assertEq(t, get(t, pool, "k"), 42)
}

func TestSetNoneValue(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", nil)
	assertEq(t, get(t, pool, "k"), nil)
}

func TestSetBytesValue(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", []byte("binary"))
	assertEq(t, get(t, pool, "k"), []byte("binary"))
}

// ---- TestPoolBaseDelItem ----

func TestDeleteExistingKey(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", "val")
	del(t, pool, "k")
	assertEq(t, get(t, pool, "k"), nil)
}

func TestDeleteMissingKeyNoError(t *testing.T) {
	pool := newPool(t)
	del(t, pool, "nonexistent")
}

func TestDeleteTwiceNoError(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", "val")
	del(t, pool, "k")
	del(t, pool, "k")
}

// ---- TestPoolBaseEmpty ----

func TestEmptyClearsAll(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "a", 1)
	set(t, pool, "b", 2)
	empty(t, pool)
	assertEq(t, keys(t, pool), py.NewList())
}

func TestEmptyOnEmptyPool(t *testing.T) {
	pool := newPool(t)
	empty(t, pool)
	assertEq(t, keys(t, pool), py.NewList())
}

// ---- TestPoolBaseExists ----

func TestExistsTrue(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", "val")
	if !exists(t, pool, "k") {
		t.Fatal("k")
	}
}

func TestExistsFalse(t *testing.T) {
	pool := newPool(t)
	if exists(t, pool, "missing") {
		t.Fatal("missing")
	}
}

func TestExistsAfterDelete(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", "val")
	del(t, pool, "k")
	if exists(t, pool, "k") {
		t.Fatal("k")
	}
}

func TestContainsOperator(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", "val")
	if !contains(t, pool, "k") || contains(t, pool, "missing") {
		t.Fatal("contains")
	}
}

func TestExistsWithNoneValue(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", nil)
	if !exists(t, pool, "k") {
		t.Fatal("k")
	}
}

// ---- TestPoolBaseKeys ----

func TestKeysReturnsListByDefault(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "a", 1)
	set(t, pool, "b", 2)
	ks := keys(t, pool)
	if _, ok := ks.(*py.List); !ok {
		t.Fatalf("keys type %T", ks)
	}
	assertCountEqual(t, ks, "a", "b")
}

func TestKeysAsGenerator(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "a", 1)
	gen := keys(t, pool, py.Kw{"as_generator": true})
	if _, ok := gen.(*py.List); ok {
		t.Fatal("generator should not be a list")
	}
	got := []any{}
	for k, err := range gen.(py.Iterator) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, k)
	}
	assertCountEqual(t, got, "a")
}

func TestKeysEmptyPool(t *testing.T) {
	pool := newPool(t)
	assertEq(t, keys(t, pool), py.NewList())
}

func TestKeysAfterEmpty(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "a", 1)
	empty(t, pool)
	assertEq(t, keys(t, pool), py.NewList())
}

// ---- TestPoolBaseSync ----

func TestSyncRaises(t *testing.T) {
	pool := newPool(t)
	if err := pool.Sync(); !errors.Is(err, py.NotImplementedError) {
		t.Fatalf("want NotImplementedError, got %v", err)
	}
}

// ---- TestPoolBasePoolId ----

func TestPoolIdMatchesGlobalId(t *testing.T) {
	pool := newPool(t)
	assertEq(t, pool.Pool_id(), pool.Global_id())
}

func TestDifferentPoolsHaveDifferentIds(t *testing.T) {
	a, b := newPool(t), newPool(t)
	if a.Pool_id() == b.Pool_id() {
		t.Fatal("pool ids should differ")
	}
}

// ---- TestPoolBaseLeOperator ----
// ``NotImplemented`` from ``__le__`` surfaces as TypeError in Go.

func TestLeWithIncompatibleTypeReturnsNotImplemented(t *testing.T) {
	pool := newPool(t)
	_, err := pool.Le(42)
	if !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

func TestLeWithNoneReturnsNotImplemented(t *testing.T) {
	pool := newPool(t)
	_, err := pool.Le(nil)
	if !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

// ---- TestPoolBaseConcurrency ----

func TestAtomicContextManager(t *testing.T) {
	pool := newPool(t)
	func() {
		_, release, err := pool.Atomic()
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		pool.Resource.Set("k", "val")
	}()
	assertEq(t, get(t, pool, "k"), "val")
}

func TestMultipleSequentialWrites(t *testing.T) {
	pool := newPool(t)
	for i := 0; i < 100; i++ {
		set(t, pool, fmt.Sprintf("key-%d", i), i)
	}
	assertEq(t, keys(t, pool).(*py.List).Len(), 100)
	for i := 0; i < 100; i++ {
		assertEq(t, get(t, pool, fmt.Sprintf("key-%d", i)), i)
	}
}

// ---- TestProxyProperties: proxy / proxy_to property wiring ----

func TestProxyToDefaultIsNone(t *testing.T) {
	pool := newPool(t)
	if pool.Proxy_to() != nil {
		t.Fatal("proxy_to should be None")
	}
}

func TestProxyToSetter(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxyTo(t, cache, origin)
	if cache.Proxy_to() != any(origin) {
		t.Fatal("proxy_to")
	}
}

func TestProxyToSetterNoneClears(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxyTo(t, cache, origin)
	setProxyTo(t, cache, nil)
	if cache.Proxy_to() != nil {
		t.Fatal("proxy_to should be None")
	}
}

func TestProxySetterSetsTargetProxyTo(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxy(t, origin, cache)
	if cache.Proxy_to() != any(origin) {
		t.Fatal("proxy_to")
	}
}

func TestProxyGetterAlwaysReturnsNone(t *testing.T) {
	pool := newPool(t)
	if pool.Proxy() != nil {
		t.Fatal("proxy should be None")
	}
}

func TestProxySetterWithNoneIsNoop(t *testing.T) {
	origin := newPool(t)
	setProxy(t, origin, nil)
}

// ---- TestProxyOperators: << and >> chain operators ----

func TestLshiftSetsProxyTo(t *testing.T) {
	mem, hdf5 := newPool(t), newPool(t)
	result := lshift(t, mem, hdf5)
	if mem.Proxy_to() != any(hdf5) || result != hdf5 {
		t.Fatal("lshift")
	}
}

func TestRshiftSetsProxyTo(t *testing.T) {
	s3, hdf5 := newPool(t), newPool(t)
	result := rshift(t, s3, hdf5)
	if hdf5.Proxy_to() != any(s3) || result != hdf5 {
		t.Fatal("rshift")
	}
}

func TestLshiftChainThreePools(t *testing.T) {
	mem, hdf5, s3 := newPool(t), newPool(t), newPool(t)
	lshift(t, lshift(t, mem, hdf5), s3)
	if mem.Proxy_to() != any(hdf5) || hdf5.Proxy_to() != any(s3) || s3.Proxy_to() != nil {
		t.Fatal("chain")
	}
}

func TestRshiftChainThreePools(t *testing.T) {
	s3, hdf5, mem := newPool(t), newPool(t), newPool(t)
	rshift(t, rshift(t, s3, hdf5), mem)
	if hdf5.Proxy_to() != any(s3) || mem.Proxy_to() != any(hdf5) || s3.Proxy_to() != nil {
		t.Fatal("chain")
	}
}

// ---- TestProxyRead: __getitem__ with proxy_to fallback and caching ----

func TestReadLocalHitNoProxy(t *testing.T) {
	pool := newPool(t)
	set(t, pool, "k", "local")
	assertEq(t, get(t, pool, "k"), "local")
}

func TestReadMissNoProxyReturnsNone(t *testing.T) {
	pool := newPool(t)
	assertEq(t, get(t, pool, "k"), nil)
}

func TestReadMissFallsBackToProxyTo(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	set(t, origin, "k", "from_origin")
	setProxyTo(t, cache, origin)
	assertEq(t, get(t, cache, "k"), "from_origin")
}

func TestReadFallbackCachesInLocal(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	set(t, origin, "k", "from_origin")
	setProxyTo(t, cache, origin)
	_ = get(t, cache, "k")
	if !existsLocal(t, cache, "k") {
		t.Fatal("_exists")
	}
	assertEq(t, must(cache.Read_("k")), "from_origin")
}

func TestReadLocalHitDoesNotQueryOrigin(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	set(t, origin, "k", "origin_val")
	set(t, cache, "k", "local_val")
	setProxyTo(t, cache, origin)
	assertEq(t, get(t, cache, "k"), "local_val")
}

func TestReadMissOnBothReturnsNone(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxyTo(t, cache, origin)
	assertEq(t, get(t, cache, "missing"), nil)
}

func TestReadThreeLevelChain(t *testing.T) {
	mem, hdf5, s3 := newPool(t), newPool(t), newPool(t)
	lshift(t, lshift(t, mem, hdf5), s3)
	set(t, s3, "deep", "origin_value")
	assertEq(t, get(t, mem, "deep"), "origin_value")
	if !existsLocal(t, hdf5, "deep") || !existsLocal(t, mem, "deep") {
		t.Fatal("_exists")
	}
}

// hdf5[key] must never go to mem even if mem.proxy_to == hdf5.
func TestReadDoesNotPropagateUpward(t *testing.T) {
	mem, hdf5, s3 := newPool(t), newPool(t), newPool(t)
	lshift(t, lshift(t, mem, hdf5), s3)
	set(t, mem, "only_in_mem", "mem_val")
	assertEq(t, get(t, hdf5, "only_in_mem"), nil)
	assertEq(t, get(t, s3, "only_in_mem"), nil)
}

// ---- TestProxyWrite: __setitem__ is local only, no propagation ----

func TestWriteDoesNotPropagateToOrigin(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxyTo(t, cache, origin)
	set(t, cache, "k", "val")
	if exists(t, origin, "k") {
		t.Fatal("propagated")
	}
}

func TestWriteDoesNotPropagateToProxy(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxy(t, origin, cache)
	set(t, origin, "k", "val")
	if exists(t, cache, "k") {
		t.Fatal("propagated")
	}
}

func TestWriteInChainIsLocalOnly(t *testing.T) {
	mem, hdf5, s3 := newPool(t), newPool(t), newPool(t)
	lshift(t, lshift(t, mem, hdf5), s3)
	set(t, hdf5, "new", "data")
	if exists(t, mem, "new") || exists(t, s3, "new") || !exists(t, hdf5, "new") {
		t.Fatal("write locality")
	}
}

// ---- TestProxyDelete: __delitem__ is local only, no propagation ----

func TestDeleteDoesNotPropagateToOrigin(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxyTo(t, cache, origin)
	set(t, origin, "k", "val")
	set(t, cache, "k", "val")
	del(t, cache, "k")
	if !exists(t, origin, "k") {
		t.Fatal("propagated")
	}
}

func TestDeleteDoesNotPropagateToProxy(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxy(t, origin, cache)
	set(t, cache, "k", "val")
	set(t, origin, "k", "val")
	del(t, origin, "k")
	if !exists(t, cache, "k") {
		t.Fatal("propagated")
	}
}

func TestDeleteInChainIsLocalOnly(t *testing.T) {
	mem, hdf5, s3 := newPool(t), newPool(t), newPool(t)
	lshift(t, lshift(t, mem, hdf5), s3)
	set(t, s3, "k", "v")
	_ = get(t, mem, "k")
	del(t, hdf5, "k")
	if !exists(t, mem, "k") || exists(t, hdf5, "k") || !exists(t, s3, "k") {
		t.Fatal("delete locality")
	}
}

// ---- TestProxyExists: exists is local only, no propagation ----

func TestExistsDoesNotCheckOrigin(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxyTo(t, cache, origin)
	set(t, origin, "k", "val")
	if exists(t, cache, "k") {
		t.Fatal("exists consulted origin")
	}
}

func TestExistsAfterCacheFill(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxyTo(t, cache, origin)
	set(t, origin, "k", "val")
	_ = get(t, cache, "k")
	if !exists(t, cache, "k") {
		t.Fatal("not cached")
	}
}

func TestContainsIsLocalOnly(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxyTo(t, cache, origin)
	set(t, origin, "k", "val")
	if contains(t, cache, "k") {
		t.Fatal("contains consulted origin")
	}
}

// ---- TestProxyEmpty: empty is local only, no propagation ----

func TestEmptyDoesNotClearOrigin(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxyTo(t, cache, origin)
	set(t, origin, "k", "origin_val")
	set(t, cache, "k", "cache_val")
	empty(t, cache)
	if exists(t, cache, "k") || !exists(t, origin, "k") {
		t.Fatal("empty locality")
	}
}

func TestEmptyDoesNotClearProxy(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxy(t, origin, cache)
	set(t, cache, "k", "cache_val")
	empty(t, origin)
	if !exists(t, cache, "k") {
		t.Fatal("empty propagated")
	}
}

// ---- TestProxyKeys: keys returns own keys only ----

func TestKeysDoesNotIncludeOriginKeys(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxyTo(t, cache, origin)
	set(t, origin, "remote", "val")
	set(t, cache, "local", "val")
	assertEq(t, keys(t, cache), py.NewList("local"))
}

func TestKeysAfterCacheFill(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	setProxyTo(t, cache, origin)
	set(t, origin, "remote", "val")
	_ = get(t, cache, "remote")
	if !keys(t, cache).(*py.List).Contains("remote") {
		t.Fatal("remote not cached")
	}
}

// ---- TestProxyEdgeCases ----

func TestReassignProxyTo(t *testing.T) {
	cache, origin_a, origin_b := newPool(t), newPool(t), newPool(t)
	set(t, origin_a, "k", "a_val")
	set(t, origin_b, "k", "b_val")
	setProxyTo(t, cache, origin_a)
	assertEq(t, get(t, cache, "k"), "a_val")
	empty(t, cache)
	setProxyTo(t, cache, origin_b)
	assertEq(t, get(t, cache, "k"), "b_val")
}

func TestDetachProxyTo(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	set(t, origin, "k", "val")
	setProxyTo(t, cache, origin)
	_ = get(t, cache, "k")
	setProxyTo(t, cache, nil)
	empty(t, cache)
	assertEq(t, get(t, cache, "k"), nil)
}

func TestCachedValueSurvivesOriginDelete(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	set(t, origin, "k", "val")
	setProxyTo(t, cache, origin)
	_ = get(t, cache, "k")
	del(t, origin, "k")
	assertEq(t, get(t, cache, "k"), "val")
}

func TestOriginUpdateNotReflectedInStaleCache(t *testing.T) {
	cache, origin := newPool(t), newPool(t)
	set(t, origin, "k", "old")
	setProxyTo(t, cache, origin)
	_ = get(t, cache, "k")
	set(t, origin, "k", "new")
	assertEq(t, get(t, cache, "k"), "old")
}

func TestFourLevelChain(t *testing.T) {
	l1, l2, l3, l4 := newPool(t), newPool(t), newPool(t), newPool(t)
	lshift(t, lshift(t, lshift(t, l1, l2), l3), l4)
	set(t, l4, "deep", "bottom")
	assertEq(t, get(t, l1, "deep"), "bottom")
	for _, level := range []*Pool{l1, l2, l3, l4} {
		if !exists(t, level, "deep") {
			t.Fatal("deep missing at a level")
		}
	}
}
