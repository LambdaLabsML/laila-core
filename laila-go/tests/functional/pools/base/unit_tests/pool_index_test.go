// Port of tests/functional/pools/base/unit_tests/test_pool_index.py.
//
// PoolIndex: per-base shards, write-through maintenance, persistence,
// hiding.
package unit_tests

import (
	"strings"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func base_(entry *laila.Entry) string {
	b, _, _ := strings.Cut(entry.Global_id(), "@")
	return b
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

func keysOf(t *testing.T, p *Pool, kw ...py.Kw) []string {
	t.Helper()
	out := []string{}
	for _, k := range py.Iter(must(p.Keys(kw...))) {
		out = append(out, py.Str(k))
	}
	return out
}

func strp(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

func intp(i int) *int { return &i }

func evo(e *laila.Entry) any {
	if e.Evolution() == nil {
		return nil
	}
	return *e.Evolution()
}

type indexCase struct {
	t        *testing.T
	original any
	policy   *laila.LAILA_IDENTIFIABLE_POLICY_
	memory   *laila.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_
}

func setUpIndex(t *testing.T) *indexCase {
	t.Helper()
	c := &indexCase{t: t}
	c.original = must(laila.Get_active_policy())
	c.policy = must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.Activate_policy(c.policy)
	c.memory = laila.As_central_memory_(c.policy.Central.Memory)
	t.Cleanup(func() {
		if cmd, ok := c.policy.Central.Command.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_); ok {
			_ = cmd.Shutdown(py.Kw{"wait": true, "cancel_pending": true})
		}
		laila.Activate_policy(c.original)
		laila.Local_policies_.PopDefault(c.policy.Global_id(), nil)
	})
	return c
}

func (c *indexCase) extend(pool any) {
	c.t.Helper()
	uuid := py.Str(must(py.Getattr(pool, "uuid")))
	if err := c.memory.Extend(pool, py.Kw{"pool_nickname": "p-" + uuid[:8]}); err != nil {
		c.t.Fatal(err)
	}
}

func (c *indexCase) pool(kw ...py.Kw) *Pool {
	c.t.Helper()
	pool := newPool(c.t, kw...)
	c.extend(pool)
	return pool
}

func (c *indexCase) memorize(pool any, e any) {
	c.t.Helper()
	gid := py.Str(must(py.Getattr(pool, "global_id")))
	wait(c.t, must(c.memory.Memorize(e, py.Kw{"pool_id": gid})))
}

func (c *indexCase) remember(ref string, pool any) *laila.Entry {
	c.t.Helper()
	gid := py.Str(must(py.Getattr(pool, "global_id")))
	return laila.As_entry_(wait(c.t, must(c.memory.Remember(ref, py.Kw{"pool_id": gid, "persist": false}))))
}

func (c *indexCase) three(pool any, nickname string) (*laila.Entry, []string) {
	c.t.Helper()
	v := must(laila.Variable(py.Kw{"data": py.NewList(0), "nickname": nickname}))
	stamps := []string{}
	c.memorize(pool, v)
	stamps = append(stamps, *v.Creation_timestamp())
	for _, i := range []int{1, 2} {
		time.Sleep(2 * time.Millisecond)
		if err := v.Set_data(py.NewList(i)); err != nil {
			c.t.Fatal(err)
		}
		c.memorize(pool, v)
		stamps = append(stamps, *v.Creation_timestamp())
	}
	return v, stamps
}

// ---- shard shape and queries ----

func TestOneShardPerBaseHiddenFromKeys(t *testing.T) {
	c := setUpIndex(t)
	pool := c.pool()
	c.three(pool, "idx-nick")
	k := must(laila.Constant("c"))
	c.memorize(pool, k)
	visible := keysOf(t, pool)
	raw := keysOf(t, pool, py.Kw{"include_index": true})
	assertEq(t, len(visible), 4)
	assertEq(t, len(raw), 6) // 4 entries + 2 shards (one per base)
	shards := []string{}
	for _, k := range raw {
		if laila.Is_index_key(k) {
			shards = append(shards, k)
		}
	}
	assertEq(t, len(shards), 2)
	for _, k := range shards {
		if !strings.HasPrefix(k, "LAILA:POOL_INDEX:") {
			t.Fatalf("shard key %s", k)
		}
	}
	for _, k := range visible {
		if laila.Is_index_key(k) {
			t.Fatalf("index key %s visible", k)
		}
	}
	for k, err := range must(pool.Keys(py.Kw{"as_generator": true})).(py.Iterator) {
		if err != nil {
			t.Fatal(err)
		}
		if laila.Is_index_key(k) {
			t.Fatalf("index key %v visible in generator", k)
		}
	}
}

func TestQueries(t *testing.T) {
	c := setUpIndex(t)
	pool := c.pool()
	v, stamps := c.three(pool, "idx-nick")
	base := base_(v)
	idx := pool.Index()
	assertEq(t, idx.Candidates(base), []string{base + "@evolution=0", base + "@evolution=1", base + "@evolution=2"})
	assertEq(t, strp(idx.Latest(base)), base+"@evolution=2")
	assertEq(t, strp(idx.Nth(base, -1)), base+"@evolution=2")
	assertEq(t, strp(idx.Nth(base, -3)), base+"@evolution=0")
	assertEq(t, strp(idx.Nth(base, -4)), nil)
	assertEq(t, strp(idx.Nth(base, 1)), base+"@evolution=1")
	assertEq(t, strp(idx.Nth(base, 7)), nil)
	assertEq(t, strp(idx.By_creation_timestamp(base, stamps[1], nil)), base+"@evolution=1")
	assertEq(t, strp(idx.By_creation_timestamp(base, stamps[1], intp(1))), base+"@evolution=1")
	assertEq(t, strp(idx.By_creation_timestamp(base, stamps[1], intp(-2))), base+"@evolution=1")
	assertEq(t, strp(idx.By_creation_timestamp(base, stamps[1], intp(2))), nil)
	assertEq(t, strp(idx.By_creation_timestamp(base, "1970-01-01T00:00:00.000+00:00", nil)), nil)
	if idx.Candidates("LAILA:ENTRY:00000000-0000-0000-0000-000000000000") != nil {
		t.Fatal("unknown base should have no candidates")
	}
}

func TestConstantIsIndexedAndPreferredAsLatest(t *testing.T) {
	c := setUpIndex(t)
	pool := c.pool()
	k := must(laila.Constant("c", py.Kw{"nickname": "const-nick"}))
	c.memorize(pool, k)
	base := k.Global_id()
	assertEq(t, pool.Index().Candidates(base), []string{base})
	assertEq(t, strp(pool.Index().Latest(base)), base)
	assertEq(t, strp(pool.Index().By_creation_timestamp(base, *k.Creation_timestamp(), nil)), base)
}

// ---- maintenance ----

func TestRemoveUpdatesShardAndDropsItWhenEmpty(t *testing.T) {
	c := setUpIndex(t)
	pool := c.pool()
	v, _ := c.three(pool, "idx-nick")
	base := base_(v)
	del(t, pool, base+"@evolution=2")
	assertEq(t, strp(pool.Index().Latest(base)), base+"@evolution=1")
	del(t, pool, base+"@evolution=1")
	del(t, pool, base+"@evolution=0")
	if pool.Index().Candidates(base) != nil {
		t.Fatal("candidates should be None")
	}
	assertEq(t, keysOf(t, pool, py.Kw{"include_index": true}), []string{}) // shard dropped too
}

func TestLatestOnlyShardRetention(t *testing.T) {
	c := setUpIndex(t)
	pool := c.pool()
	v, _ := c.three(pool, "idx-nick")
	shards := []string{}
	for _, k := range keysOf(t, pool, py.Kw{"include_index": true}) {
		if laila.Is_index_key(k) {
			shards = append(shards, k)
		}
	}
	assertEq(t, len(shards), 1)
	// Three writes rewrite one *constant* shard key in place (no
	// ``@evolution`` suffix, no straggler cleanup).
	if strings.Contains(shards[0], "@") {
		t.Fatalf("shard key %s has an evolution suffix", shards[0])
	}
	assertEq(t, shards[0], pool.Index().Shard_id(base_(v)))
}

func TestRewrittenEvolutionReplacesStaleTimestamp(t *testing.T) {
	c := setUpIndex(t)
	pool := c.pool()
	a := must(laila.Variable(py.Kw{"data": 1, "evolution": 5, "uuid": "11111111-1111-4111-8111-111111111111"}))
	c.memorize(pool, a)
	time.Sleep(2 * time.Millisecond)
	b := must(laila.Variable(py.Kw{"data": 2, "evolution": 5, "uuid": "11111111-1111-4111-8111-111111111111"}))
	c.memorize(pool, b)
	base := base_(a)
	stamps := pool.Index().Shards_[base].Creation_timestamps
	assertEq(t, stamps, py.DictOf(*b.Creation_timestamp(), 5))
}

func TestSameTimestampCollisionKeepsHighestEvolution(t *testing.T) {
	c := setUpIndex(t)
	pool := c.pool()
	uid := "22222222-2222-4222-8222-222222222222"
	a := must(laila.Variable(py.Kw{"data": 1, "evolution": 0, "uuid": uid}))
	b := must(laila.Variable(py.Kw{"data": 2, "evolution": 1, "uuid": uid}))
	b.Creation_timestamp_ = a.Creation_timestamp_
	c.memorize(pool, a)
	c.memorize(pool, b)
	base := base_(a)
	assertEq(t, strp(pool.Index().By_creation_timestamp(base, *a.Creation_timestamp(), nil)), base+"@evolution=1")
}

func TestIndexKeysAreNeverIndexed(t *testing.T) {
	c := setUpIndex(t)
	pool := c.pool()
	c.three(pool, "idx-nick")
	for _, key := range keysOf(t, pool, py.Kw{"include_index": true}) {
		if laila.Is_index_key(key) {
			b, _, _ := strings.Cut(key, "@")
			if pool.Index().Candidates(b) != nil {
				t.Fatalf("index key %s got indexed", key)
			}
		}
	}
}

// FragilePool is ``type(pool)._write = boom``: index-shard writes fail.
type FragilePool struct {
	laila.LAILA_IDENTIFIABLE_POOL_
	Boom bool `py:"-"`
}

var FragilePool_class = py.NewClass("_LAILA_IDENTIFIABLE_POOL", (*FragilePool)(nil), laila.LAILA_IDENTIFIABLE_POOL_class)

func (p *FragilePool) Write_(key string, value any) error {
	if p.Boom && laila.Is_index_key(key) {
		return py.RuntimeError.New("index storage down")
	}
	return p.LAILA_IDENTIFIABLE_POOL_.Write_(key, value)
}

func TestFlushFailureDegradesWithoutFailingMemorize(t *testing.T) {
	c := setUpIndex(t)
	pool := FragilePool_class.New().(*FragilePool)
	if err := pool.Init_(py.Kw{}); err != nil {
		t.Fatal(err)
	}
	c.extend(pool)
	v := must(laila.Variable(py.Kw{"data": py.NewList(0), "nickname": "fragile"}))
	pool.Boom = true
	func() {
		defer func() { pool.Boom = false }()
		c.memorize(pool, v) // must not raise
	}()
	if !must(pool.Exists(v.Global_id())) {
		t.Fatal("entry should exist")
	}
	if pool.Index().Candidates(base_(v)) != nil { // shard was invalidated
		t.Fatal("shard should be invalidated")
	}
	// ... and remember still works through the scan fallback.
	r := c.remember("ENTRY:fragile", pool)
	assertEq(t, must(r.Data()), py.NewList(0))
}

// ---- persistence ----

func TestForeignIndexPoolKeepsDataPoolClean(t *testing.T) {
	c := setUpIndex(t)
	ram := newPool(t)
	data := c.pool(py.Kw{"index_pool": ram})
	c.three(data, "foreign-nick")
	assertEq(t, len(keysOf(t, data, py.Kw{"include_index": true})), 3)
	shards := keysOf(t, ram, py.Kw{"include_index": true})
	assertEq(t, len(shards), 1)
	if !laila.Is_index_key(shards[0]) {
		t.Fatal("not an index key")
	}
	r := c.remember("ENTRY:foreign-nick@evolution=-1", data)
	assertEq(t, evo(r), 2)
	empty(t, data)
	assertEq(t, keysOf(t, ram, py.Kw{"include_index": true}), []string{})
	assertEq(t, keysOf(t, data, py.Kw{"include_index": true}), []string{})
}

// CountingSQLitePool is ``second._keys = counting``.
type CountingSQLitePool struct {
	laila.SQLitePool
	Scans int `py:"-"`
}

var CountingSQLitePool_class = py.NewClass("SQLitePool", (*CountingSQLitePool)(nil), laila.SQLitePool_class)

func (p *CountingSQLitePool) Keys_(kw ...py.Kw) (any, error) {
	p.Scans += 1
	return p.SQLitePool.Keys_(kw...)
}

func TestShardReloadsFromStorageInFreshPoolObject(t *testing.T) {
	c := setUpIndex(t)
	first := must(laila.New_SQLitePool())
	if err := c.memory.Extend(first, py.Kw{"pool_nickname": "sq-first"}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, k := range keysOf(t, &first.LAILA_IDENTIFIABLE_POOL_, py.Kw{"include_index": true}) {
			_ = first.Delete_(k)
		}
	}()
	v, stamps := c.three(first, "persist-nick")
	base := base_(v)
	// A new object over the same storage: empty in-memory index.
	second := CountingSQLitePool_class.New().(*CountingSQLitePool)
	if err := second.Init_(py.Kw{"uuid": first.Uuid()}); err != nil {
		t.Fatal(err)
	}
	if err := c.memory.Extend(second, py.Kw{"pool_nickname": "sq-second"}); err != nil {
		t.Fatal(err)
	}
	assertEq(t, len(second.Index().Shards_), 0)
	r := c.remember("ENTRY:persist-nick@evolution=-1", second)
	assertEq(t, evo(r), 2)
	r = c.remember("ENTRY:persist-nick@creation_timestamp="+stamps[0], second)
	assertEq(t, evo(r), 0)
	// The shard lives under a constant key: loading it is one read,
	// never a key scan.
	assertEq(t, second.Scans, 0)
	assertEq(t, strp(second.Index().Latest(base)), v.Global_id())
}

func TestRebuildOverUnindexedKeys(t *testing.T) {
	c := setUpIndex(t)
	pool := c.pool(py.Kw{"index_enabled": false})
	v, stamps := c.three(pool, "rebuild-nick")
	assertEq(t, keysOf(t, pool, py.Kw{"include_index": true}), py.Sorted_strings(keysOf(t, pool)))
	pool.Index_enabled = true
	base := base_(v)
	if pool.Index().Candidates(base) != nil {
		t.Fatal("candidates should be None before rebuild")
	}
	pool.Index().Rebuild()
	assertEq(t, strp(pool.Index().Latest(base)), base+"@evolution=2")
	assertEq(t, strp(pool.Index().By_creation_timestamp(base, stamps[1], nil)), base+"@evolution=1")
	// Rebuild replaces stale contents outright.
	if err := pool.Delete_(base + "@evolution=2"); err != nil {
		t.Fatal(err)
	}
	pool.Index().Rebuild()
	assertEq(t, strp(pool.Index().Latest(base)), base+"@evolution=1")
}

func TestShardIdIsDeterministicPerOwner(t *testing.T) {
	a := newPool(t, py.Kw{"uuid": "33333333-3333-4333-8333-333333333333"})
	b := newPool(t, py.Kw{"uuid": "44444444-4444-4444-8444-444444444444"})
	base := "LAILA:ENTRY:55555555-5555-4555-8555-555555555555"
	assertEq(t, laila.New_PoolIndex(a).Shard_id(base), laila.New_PoolIndex(a).Shard_id(base))
	if laila.New_PoolIndex(a).Shard_id(base) == laila.New_PoolIndex(b).Shard_id(base) {
		t.Fatal("shard ids should differ per owner")
	}
	if !laila.Is_index_key(laila.New_PoolIndex(a).Shard_id(base)) {
		t.Fatal("shard id is not an index key")
	}
}
