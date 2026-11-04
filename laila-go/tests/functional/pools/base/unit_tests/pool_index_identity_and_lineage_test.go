// Port of tests/functional/pools/base/unit_tests/test_pool_index_identity_and_lineage.py.
//
// Phase 3 regressions: index identity, constant shards, plain keys, proxy
// cycles, DuckDB checkpointing and chain-wide lineage resolution (F-03,
// F-04, F-07, F-08, F-09, F-10, F-11, F-12). The DuckDB cases are ported
// with the DuckDB backend (Phase 5).
package unit_tests

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func unwrap_(value any) any { return value }

type lineageCase struct {
	t        *testing.T
	original any
	policy   *laila.LAILA_IDENTIFIABLE_POLICY_
	memory   *laila.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_
	tmp      string
}

func setUpLineage(t *testing.T) *lineageCase {
	t.Helper()
	c := &lineageCase{t: t}
	c.original = must(laila.Get_active_policy())
	c.policy = must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.Activate_policy(c.policy)
	c.memory = laila.As_central_memory_(c.policy.Central.Memory)
	c.tmp = t.TempDir()
	t.Cleanup(func() {
		if cmd, ok := c.policy.Central.Command.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_); ok {
			_ = cmd.Shutdown(py.Kw{"wait": true, "cancel_pending": true})
		}
		laila.Activate_policy(c.original)
		laila.Local_policies_.PopDefault(c.policy.Global_id(), nil)
	})
	return c
}

func (c *lineageCase) extend(pool *Pool, nickname string) *Pool {
	c.t.Helper()
	if err := c.memory.Extend(pool, py.Kw{"pool_nickname": nickname}); err != nil {
		c.t.Fatal(err)
	}
	return pool
}

func (c *lineageCase) memorize(pool *Pool, e any) {
	c.t.Helper()
	wait(c.t, must(c.memory.Memorize(e, py.Kw{"pool_id": pool.Global_id()})))
}

func (c *lineageCase) remember(pool *Pool, ref string) *laila.Entry {
	c.t.Helper()
	return laila.As_entry_(wait(c.t, must(c.memory.Remember(ref, py.Kw{"pool_id": pool.Global_id(), "persist": false}))))
}

func (c *lineageCase) lineage(pool *Pool, n int, start int, v *laila.Entry) *laila.Entry {
	c.t.Helper()
	if v == nil {
		v = must(laila.Variable(py.Kw{"data": py.DictOf("v", 0)}))
	}
	for i := start; i < start+n; i++ {
		if err := v.Set_data(py.DictOf("v", i)); err != nil {
			c.t.Fatal(err)
		}
		c.memorize(pool, v)
	}
	return v
}

func sqlitePool(t *testing.T, kw py.Kw) *laila.SQLitePool {
	t.Helper()
	return must(laila.New_SQLitePool(kw))
}

func data(t *testing.T, e *laila.Entry) any {
	t.Helper()
	return must(e.Data())
}

// ---- TestIndexIdentity: F-03, the identity is persisted and adopted by
// a fresh instance over the same file ----

func TestReopenWithFreshUuidFindsShards(t *testing.T) {
	c := setUpLineage(t)
	path := filepath.Join(c.tmp, "p.sqlite")
	first := sqlitePool(t, py.Kw{"file_path": path})
	base := "LAILA:ENTRY:11111111-1111-4111-8111-111111111111"
	for e := 0; e < 5; e++ {
		set(t, &first.LAILA_IDENTIFIABLE_POOL_, fmt.Sprintf("%s@evolution=%d", base, e), py.DictOf("e", e))
	}
	identity := first.Index().Identity()
	assertEq(t, identity, first.Uuid()) // first instance: its own uuid
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := sqlitePool(t, py.Kw{"file_path": path}) // new random uuid
	if second.Uuid() == identity {
		t.Fatal("second uuid should be fresh")
	}
	assertEq(t, second.Index().Identity(), identity)
	assertEq(t, strp(second.Index().Latest(base)), base+"@evolution=4")
	// No scan was needed: the shard key is constant.
	assertEq(t, second.Index().Shard_id(base), first.Index().Shard_id(base))
	// Writing through the second instance keeps a single shard.
	set(t, &second.LAILA_IDENTIFIABLE_POOL_, base+"@evolution=5", py.DictOf("e", 5))
	shards := []string{}
	for _, k := range keysOf(t, &second.LAILA_IDENTIFIABLE_POOL_, py.Kw{"include_index": true}) {
		if laila.Is_index_key(k) {
			shards = append(shards, k)
		}
	}
	assertEq(t, py.Sorted_strings(shards), py.Sorted_strings([]string{second.Index().Shard_id(base), laila.INDEX_IDENTITY_KEY}))
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIdentityRecordHiddenFromKeys(t *testing.T) {
	c := setUpLineage(t)
	pool := sqlitePool(t, py.Kw{"file_path": filepath.Join(c.tmp, "q.sqlite")})
	p := &pool.LAILA_IDENTIFIABLE_POOL_
	set(t, p, "LAILA:ENTRY:22222222-2222-4222-8222-222222222222", py.DictOf("x", 1))
	if keys(t, p).(*py.List).Contains(laila.INDEX_IDENTITY_KEY) {
		t.Fatal("identity key visible")
	}
	if !py.NewList(py.Iter(keys(t, p, py.Kw{"include_index": true}))...).Contains(laila.INDEX_IDENTITY_KEY) {
		t.Fatal("identity key missing from raw keys")
	}
	_ = pool.Close()
}

func TestInMemoryPoolNeverPersistsIdentity(t *testing.T) {
	setUpLineage(t)
	pool := newPool(t)
	set(t, pool, "LAILA:ENTRY:33333333-3333-4333-8333-333333333333", py.DictOf("x", 1))
	assertEq(t, pool.Index().Identity(), pool.Uuid())
	for _, k := range keysOf(t, pool, py.Kw{"include_index": true}) {
		if k == laila.INDEX_IDENTITY_KEY {
			t.Fatal("identity key persisted in memory pool")
		}
	}
}

func TestLegacyEvolvableShardsArePurgedOnce(t *testing.T) {
	c := setUpLineage(t)
	path := filepath.Join(c.tmp, "legacy.sqlite")
	pool := sqlitePool(t, py.Kw{"file_path": path})
	legacy := "LAILA:POOL_INDEX:44444444-4444-4444-8444-444444444444"
	if err := pool.Write_(legacy+"@evolution=0", py.DictOf("old", true)); err != nil {
		t.Fatal(err)
	}
	if err := pool.Write_(legacy+"@evolution=1", py.DictOf("old", true)); err != nil {
		t.Fatal(err)
	}
	assertEq(t, strp(pool.Index().Latest("LAILA:ENTRY:55555555-5555-4555-8555-555555555555")), nil)
	remaining := []string{}
	for _, k := range keysOf(t, &pool.LAILA_IDENTIFIABLE_POOL_, py.Kw{"include_index": true}) {
		if strings.HasPrefix(k, legacy) {
			remaining = append(remaining, k)
		}
	}
	assertEq(t, remaining, []string{})
	_ = pool.Close()
}

// ---- TestConstantShardsAndPlainKeys: F-12 constant shard keys, no
// listing; F-04 plain keys are not indexed ----

// CountingPool is ``pool._keys = counting``.
type CountingPool struct {
	laila.LAILA_IDENTIFIABLE_POOL_
	Scans int `py:"-"`
}

var CountingPool_class = py.NewClass("_LAILA_IDENTIFIABLE_POOL", (*CountingPool)(nil), laila.LAILA_IDENTIFIABLE_POOL_class)

func (p *CountingPool) Keys_(kw ...py.Kw) (any, error) {
	p.Scans += 1
	return p.LAILA_IDENTIFIABLE_POOL_.Keys_(kw...)
}

func TestShardMaintenanceNeverListsKeys(t *testing.T) {
	c := setUpLineage(t)
	pool := CountingPool_class.New().(*CountingPool)
	if err := pool.Init_(py.Kw{}); err != nil {
		t.Fatal(err)
	}
	p := &pool.LAILA_IDENTIFIABLE_POOL_
	for i := 0; i < 50; i++ {
		set(t, p, fmt.Sprintf("LAILA:ENTRY:66666666-6666-4666-8666-%012d", i), py.DictOf("i", i))
	}
	v := must(laila.Variable(py.Kw{"data": py.DictOf("v", 0)}))
	c.extend(p, "cs")
	c.lineage(p, 5, 1, v)
	assertEq(t, pool.Scans, 0)
	shards := []string{}
	for _, k := range keysOf(t, p, py.Kw{"include_index": true}) {
		if laila.Is_index_key(k) {
			shards = append(shards, k)
		}
	}
	assertEq(t, len(shards), 51)
	for _, k := range shards {
		if strings.Contains(k, "@") {
			t.Fatalf("shard %s has an evolution suffix", k)
		}
	}
}

func TestPlainKeysNotIndexed(t *testing.T) {
	setUpLineage(t)
	pool := newPool(t)
	for i := 0; i < 20; i++ {
		set(t, pool, fmt.Sprintf("plain-%d", i), py.DictOf("i", i))
	}
	for _, k := range keysOf(t, pool, py.Kw{"include_index": true}) {
		if laila.Is_index_key(k) {
			t.Fatalf("plain key produced shard %s", k)
		}
	}
	for i := 0; i < 20; i++ {
		del(t, pool, fmt.Sprintf("plain-%d", i))
	}
	assertEq(t, keysOf(t, pool, py.Kw{"include_index": true}), []string{})
}

// ---- TestPersistentPoolConfig: F-08 ``transformations=None`` is rejected
// for persistent backends ----

func TestNoneTransformationsRejected(t *testing.T) {
	c := setUpLineage(t)
	_, err := laila.New_SQLitePool(py.Kw{"file_path": filepath.Join(c.tmp, "x.sqlite"), "transformations": nil})
	if !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
}

func TestInMemoryPoolAcceptsNone(t *testing.T) {
	setUpLineage(t)
	pool := newPool(t, py.Kw{"transformations": nil})
	if pool.Transformations != nil {
		t.Fatal("transformations should be None")
	}
}

// ---- TestProxyCycles: F-09 cycles are rejected at wiring time; reads
// never recurse ----

func assertValueError(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
}

func TestSelfProxyRejected(t *testing.T) {
	setUpLineage(t)
	a := newPool(t, py.Kw{"index_enabled": false})
	assertValueError(t, a.Set_proxy(a))
	assertValueError(t, a.Set_proxy_to(a))
	assertEq(t, get(t, a, "missing"), nil)
}

func TestTwoAndThreeTierCyclesRejected(t *testing.T) {
	setUpLineage(t)
	a := newPool(t, py.Kw{"index_enabled": false})
	b := newPool(t, py.Kw{"index_enabled": false})
	k := newPool(t, py.Kw{"index_enabled": false})
	lshift(t, a, b)
	_, err := b.Lshift(a)
	assertValueError(t, err)
	lshift(t, b, k)
	assertValueError(t, k.Set_proxy_to(a))
	_, err = a.Rshift(k) // k would proxy a: a -> b -> k -> a
	assertValueError(t, err)
	assertEq(t, get(t, a, "missing"), nil)
	assertEq(t, get(t, k, "missing"), nil)
	// Legit rewiring still works.
	setProxyTo(t, k, nil)
	if k.Proxy_to() != nil {
		t.Fatal("proxy_to should be None")
	}
}

func TestReadThroughWritesBackToEveryFrontTier(t *testing.T) {
	setUpLineage(t)
	s := newPool(t, py.Kw{"index_enabled": false})
	m := newPool(t, py.Kw{"index_enabled": false})
	k := newPool(t, py.Kw{"index_enabled": false})
	lshift(t, lshift(t, k, m), s)
	set(t, s, "k", py.DictOf("v", 1))
	assertEq(t, get(t, k, "k"), py.DictOf("v", 1))
	if !exists(t, m, "k") || !exists(t, k, "k") {
		t.Fatal("write-back")
	}
}

// ---- TestLineageAcrossTiers: F-11 the highest evolution across the
// chain wins; F-10 unbounded self-repair ----

func TestNewerInOriginThanCache(t *testing.T) {
	c := setUpLineage(t)
	origin, cache := newPool(t), newPool(t)
	lshift(t, cache, origin)
	c.extend(origin, "origin")
	c.extend(cache, "cache")
	v := c.lineage(cache, 5, 1, nil)
	c.lineage(origin, 5, 6, v)
	base := base_(v)
	for _, ref := range []string{base, base + "@evolution=-1"} {
		assertEq(t, unwrap_(data(t, c.remember(cache, ref))), py.DictOf("v", 10))
	}
	// -k ranks over the union of both tiers.
	assertEq(t, unwrap_(data(t, c.remember(cache, base+"@evolution=-6"))), py.DictOf("v", 5))
}

func TestMixedIndexSettingsAcrossTiers(t *testing.T) {
	c := setUpLineage(t)
	origin := newPool(t, py.Kw{"index_enabled": false})
	cache := newPool(t)
	lshift(t, cache, origin)
	c.extend(origin, "origin")
	c.extend(cache, "cache")
	v := c.lineage(cache, 3, 1, nil)
	c.lineage(origin, 3, 4, v)
	assertEq(t, unwrap_(data(t, c.remember(cache, base_(v)+"@evolution=-1"))), py.DictOf("v", 6))
}

func TestManyStaleIndexEntriesSelfHeal(t *testing.T) {
	c := setUpLineage(t)
	pool := c.extend(newPool(t), "p")
	n := 120
	v := c.lineage(pool, n, 1, nil)
	base := base_(v)
	for k := n; k > n-100; k-- {
		if err := pool.Delete_(fmt.Sprintf("%s@evolution=%d", base, k)); err != nil { // behind the index's back
			t.Fatal(err)
		}
	}
	assertEq(t, unwrap_(data(t, c.remember(pool, base+"@evolution=-1"))), py.DictOf("v", n-100))
	assertEq(t, strp(pool.Index().Latest(base)), fmt.Sprintf("%s@evolution=%d", base, n-100))
}

func TestFullyStaleIndexFallsBackToScan(t *testing.T) {
	c := setUpLineage(t)
	pool := c.extend(newPool(t), "p")
	v := c.lineage(pool, 3, 1, nil)
	base := base_(v)
	// An evolution written while the index was off (it knows nothing about
	// it), then delete everything the index does know.
	e7 := must(laila.Variable(py.Kw{"data": py.DictOf("v", 7), "uuid": v.Uuid(), "evolution": 7}))
	pool.Index_enabled = false
	c.memorize(pool, e7)
	pool.Index_enabled = true
	for _, k := range []int{1, 2, 3} {
		if err := pool.Delete_(fmt.Sprintf("%s@evolution=%d", base, k)); err != nil {
			t.Fatal(err)
		}
	}
	assertEq(t, pool.Index().Candidates(base), []string{base + "@evolution=1", base + "@evolution=2", base + "@evolution=3"})
	assertEq(t, evo(c.remember(pool, base+"@evolution=-1")), 7)
}
