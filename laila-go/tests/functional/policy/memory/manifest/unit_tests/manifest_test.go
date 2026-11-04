// Port of tests/functional/policy/memory/manifest/unit_tests/test_manifest.py.
package unit_tests

import (
	"errors"
	"strings"
	"testing"

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

func assertNe(t *testing.T, got, want any) {
	t.Helper()
	if py.Eq(got, want) {
		t.Fatalf("%s == %s", py.Repr(got), py.Repr(want))
	}
}

func assertIs(t *testing.T, got, want any) {
	t.Helper()
	if got != want {
		t.Fatalf("%s is not %s", py.Repr(got), py.Repr(want))
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

func assertSetEq(t *testing.T, got any, want ...any) {
	t.Helper()
	g := py.NewSet(py.Iter(got)...)
	w := py.NewSet(want...)
	if !py.Eq(g, w) {
		t.Fatalf("got %s, want %s", py.Repr(g), py.Repr(w))
	}
}

func _make_gid(scope ...string) string {
	s := "ENTRY"
	if len(scope) > 0 {
		s = scope[0]
	}
	return "LAILA:" + s + ":" + py.Str(py.Uuid4())
}

func manifest(kw ...py.Kw) *laila.Manifest { return must(laila.New_Manifest(kw...)) }

func constant(data any) *laila.Entry { return must(laila.Entry_constant(data)) }

func d(kv ...any) *py.Dict { return py.DictOf(kv...) }

func blueprint(m *laila.Manifest) *py.Dict {
	bp := must(m.Blueprint())
	if bp == nil {
		return nil
	}
	return bp.(*py.Dict)
}

func getitem(t *testing.T, m *laila.Manifest, key any) any {
	t.Helper()
	return must(m.Getitem(key))
}

func sub(t *testing.T, v any, key any) any {
	t.Helper()
	return must(v.(*py.Dict).Getitem(key))
}

func entryData(t *testing.T, v any) any {
	t.Helper()
	e := laila.As_entry_(v)
	if e == nil {
		t.Fatalf("%T is not an Entry", v)
	}
	return must(e.Data())
}

func gidsOf(entries []*laila.Entry) *py.List {
	out := py.NewList()
	for _, e := range entries {
		out.Append(e.Global_id())
	}
	return out
}

// =============================================================================
// TestManifestConstruction -- construction modes and validation.
// =============================================================================

// ── 1. From raw dict of global_id strings ──

func Test001FromStringDictSetsBlueprint(t *testing.T) {
	gid_a, gid_b := _make_gid(), _make_gid()
	m := manifest(py.Kw{"data": d("a", gid_a, "b", gid_b)})
	assertEq(t, blueprint(m), d("a", gid_a, "b", gid_b))
}

func Test002FromNestedStringDict(t *testing.T) {
	gid := _make_gid()
	data := d("outer", d("inner", gid))
	m := manifest(py.Kw{"data": data})
	assertEq(t, sub(t, sub(t, blueprint(m), "outer"), "inner"), gid)
}

func Test003FromListOfGids(t *testing.T) {
	gids := py.NewList(_make_gid(), _make_gid(), _make_gid())
	m := manifest(py.Kw{"data": d("batch", gids)})
	assertEq(t, sub(t, blueprint(m), "batch"), gids)
}

// ── 2. From dict of Entry objects ──

func Test004FromEntryDictExtractsBlueprint(t *testing.T) {
	e1 := constant("hello")
	e2 := constant("world")
	m := manifest(py.Kw{"data": d("e1", e1, "e2", e2)})
	assertEq(t, sub(t, blueprint(m), "e1"), e1.Global_id())
	assertEq(t, sub(t, blueprint(m), "e2"), e2.Global_id())
}

func Test005FromEntryList(t *testing.T) {
	entries := []*laila.Entry{constant(0), constant(1), constant(2)}
	m := manifest(py.Kw{"data": d("items", py.NewList(entries[0], entries[1], entries[2]))})
	assertEq(t, sub(t, blueprint(m), "items"), gidsOf(entries))
}

func Test006FromNestedEntryDict(t *testing.T) {
	e := constant(42)
	m := manifest(py.Kw{"data": d("layer", d("deep", e))})
	assertEq(t, sub(t, sub(t, blueprint(m), "layer"), "deep"), e.Global_id())
}

// ── 3. Validation edge cases ──

func Test007MixedEntriesAndStringsRaises(t *testing.T) {
	e := constant(1)
	_, err := laila.New_Manifest(py.Kw{"data": d("entry", e, "string", _make_gid())})
	assertRaises(t, py.ValueError, err)
}

func Test008NonStringKeyRaises(t *testing.T) {
	_, err := laila.New_Manifest(py.Kw{"data": d(42, _make_gid())})
	assertRaises(t, py.ValueError, err)
}

func Test009InvalidValueTypeRaises(t *testing.T) {
	_, err := laila.New_Manifest(py.Kw{"data": d("bad", 123)})
	assertRaises(t, py.ValueError, err)
}

func Test010ListWithNonStringItemRaises(t *testing.T) {
	_, err := laila.New_Manifest(py.Kw{"data": d("bad_list", py.NewList(_make_gid(), 42))})
	assertRaises(t, py.ValueError, err)
}

func Test011NonDictBlueprintRaises(t *testing.T) {
	assertRaises(t, py.ValueError, laila.Manifest_validate_blueprint_("not a dict"))
}

// =============================================================================
// TestManifestIdentity -- identity, scopes, and string representations.
// =============================================================================

func Test012HasGlobalIdAndManifestScope(t *testing.T) {
	m := manifest(py.Kw{"data": d("k", _make_gid())})
	if !strings.Contains(m.Global_id(), "MANIFEST") {
		t.Fatal(m.Global_id())
	}
	assertEq(t, m.Scopes(), []string{"MANIFEST"})
}

func Test013ExplicitUuid(t *testing.T) {
	raw_uuid := py.Str(py.Uuid4())
	m := manifest(py.Kw{"data": d("k", _make_gid()), "uuid": raw_uuid})
	assertEq(t, m.Uuid(), raw_uuid)
}

func Test014NicknameProducesDeterministicUuid(t *testing.T) {
	m1 := manifest(py.Kw{"data": d("k", _make_gid()), "nickname": "test_manifest"})
	m2 := manifest(py.Kw{"data": d("k", _make_gid()), "nickname": "test_manifest"})
	assertEq(t, m1.Uuid(), m2.Uuid())
}

func Test015StrReturnsGlobalId(t *testing.T) {
	m := manifest(py.Kw{"data": d("k", _make_gid())})
	assertEq(t, py.Str(m), m.Global_id())
}

func Test016ReprContainsEntryCount(t *testing.T) {
	gids := []string{_make_gid(), _make_gid()}
	m := manifest(py.Kw{"data": d("a", gids[0], "b", gids[1])})
	r := py.Repr(m)
	if !strings.Contains(r, "entries=2") {
		t.Fatal(r)
	}
	if !strings.Contains(r, "Manifest(") {
		t.Fatal(r)
	}
}

func Test017ManifestIsEntrySubclass(t *testing.T) {
	m := manifest(py.Kw{"data": d("k", _make_gid())})
	if !py.Isinstance(m, laila.Entry_class) {
		t.Fatal("Manifest should be an Entry")
	}
}

func Test018ManifestEvolutionIsNone(t *testing.T) {
	m := manifest(py.Kw{"data": d("k", _make_gid())})
	if m.Evolution_ != nil {
		t.Fatalf("evolution %v, want None", *m.Evolution_)
	}
}

// =============================================================================
// TestManifestMappingAPI -- dict(manifest), keys, values, items,
// __getitem__, __len__.
// =============================================================================

type mappingCase struct {
	gid_a, gid_b string
	m            *laila.Manifest
}

func setUpMapping() *mappingCase {
	c := &mappingCase{gid_a: _make_gid(), gid_b: _make_gid()}
	c.m = manifest(py.Kw{"data": d("a", c.gid_a, "b", c.gid_b)})
	return c
}

func Test019DictReturnsBlueprint(t *testing.T) {
	c := setUpMapping()
	dd := py.NewDict()
	for _, it := range must(c.m.Items()) {
		dd.Set(it.Key, it.Value)
	}
	assertEq(t, dd, d("a", c.gid_a, "b", c.gid_b))
}

func Test020LenReturnsTopLevelCount(t *testing.T) {
	c := setUpMapping()
	assertEq(t, c.m.Len(), 2)
}

func Test021GetitemReturnsValue(t *testing.T) {
	c := setUpMapping()
	assertEq(t, getitem(t, c.m, "a"), c.gid_a)
}

func Test022GetitemMissingKeyRaises(t *testing.T) {
	c := setUpMapping()
	_, err := c.m.Getitem("missing")
	assertRaises(t, py.KeyError, err)
}

func Test023KeysValuesItems(t *testing.T) {
	c := setUpMapping()
	assertSetEq(t, must(c.m.Keys()), "a", "b")
	assertSetEq(t, must(c.m.Values()), c.gid_a, c.gid_b)
	items := []any{}
	for _, it := range must(c.m.Items()) {
		items = append(items, py.Tuple{it.Key, it.Value})
	}
	assertSetEq(t, items, py.Tuple{"a", c.gid_a}, py.Tuple{"b", c.gid_b})
}

// =============================================================================
// TestManifestIteration -- __iter__, __contains__, list(manifest).
// =============================================================================

func Test024IterFlatDict(t *testing.T) {
	gid_a, gid_b := _make_gid(), _make_gid()
	m := manifest(py.Kw{"data": d("a", gid_a, "b", gid_b)})
	assertEq(t, m.Iter(), []any{gid_a, gid_b})
}

func Test025IterNestedDepthFirst(t *testing.T) {
	gid_1, gid_2, gid_3 := _make_gid(), _make_gid(), _make_gid()
	m := manifest(py.Kw{"data": d(
		"top", gid_1,
		"nested", d("inner", gid_2),
		"last", gid_3,
	)})
	assertEq(t, m.Iter(), []any{gid_1, gid_2, gid_3})
}

func Test026IterWithLists(t *testing.T) {
	gid_a, gid_b, gid_c := _make_gid(), _make_gid(), _make_gid()
	m := manifest(py.Kw{"data": d(
		"single", gid_a,
		"batch", py.NewList(gid_b, gid_c),
	)})
	assertEq(t, m.Iter(), []any{gid_a, gid_b, gid_c})
}

func Test027ContainsFindsLeafGid(t *testing.T) {
	gid := _make_gid()
	m := manifest(py.Kw{"data": d("nested", d("deep", gid))})
	if !m.Contains(gid) {
		t.Fatal("gid should be in manifest")
	}
}

func Test028ContainsReturnsFalseForMissing(t *testing.T) {
	m := manifest(py.Kw{"data": d("k", _make_gid())})
	if m.Contains(_make_gid()) {
		t.Fatal("unexpected membership")
	}
}

func Test029ContainsNonStringReturnsFalse(t *testing.T) {
	m := manifest(py.Kw{"data": d("k", _make_gid())})
	if m.Contains(42) {
		t.Fatal("unexpected membership")
	}
}

// =============================================================================
// TestManifestEmptyState -- edge cases for manifests with no data.
// =============================================================================

func Test030EmptyManifestLenZero(t *testing.T) {
	m := manifest()
	assertEq(t, m.Len(), 0)
	assertEq(t, m.Iter(), []any{})
}

func Test031EmptyManifestKeysValuesItems(t *testing.T) {
	m := manifest()
	assertEq(t, must(m.Keys()), []any{})
	assertEq(t, must(m.Values()), []any{})
	assertEq(t, len(must(m.Items())), 0)
}

func Test032EmptyManifestGetitemRaises(t *testing.T) {
	m := manifest()
	_, err := m.Getitem("anything")
	assertRaises(t, py.KeyError, err)
}

func Test033RememberOnEmptyManifestRaises(t *testing.T) {
	m := manifest()
	_, err := m.Remember()
	assertRaises(t, py.RuntimeError, err)
}

func Test034MemorizeOnEmptyManifestRaises(t *testing.T) {
	m := manifest()
	_, err := m.Memorize()
	assertRaises(t, py.RuntimeError, err)
}

func Test035ForgetOnEmptyManifestRaises(t *testing.T) {
	m := manifest()
	_, err := m.Forget()
	assertRaises(t, py.RuntimeError, err)
}

func Test036ResolvedOnEmptyManifestRaises(t *testing.T) {
	m := manifest()
	_, err := m.Realized()
	assertRaises(t, py.RuntimeError, err)
}

// =============================================================================
// TestManifestSubManifest -- the sub_manifest method.
// =============================================================================

type subCase struct {
	gid_a, gid_b, gid_c string
	m                   *laila.Manifest
}

func setUpSub() *subCase {
	c := &subCase{gid_a: _make_gid(), gid_b: _make_gid(), gid_c: _make_gid()}
	c.m = manifest(py.Kw{"data": d(
		"sample_1", d("mod_a", c.gid_a),
		"sample_2", d("mod_b", c.gid_b),
		"sample_3", d("mod_c", c.gid_c),
	)})
	return c
}

func Test051SubManifestReturnsManifest(t *testing.T) {
	c := setUpSub()
	s := must(c.m.Sub_manifest(py.NewList("sample_1")))
	if !py.Isinstance(s, laila.Manifest_class) {
		t.Fatalf("%T", s)
	}
}

func Test052SubManifestSingleKey(t *testing.T) {
	c := setUpSub()
	s := must(c.m.Sub_manifest(py.NewList("sample_2")))
	assertEq(t, must(s.Keys()), []any{"sample_2"})
	assertEq(t, getitem(t, s, "sample_2"), d("mod_b", c.gid_b))
}

func Test053SubManifestMultipleKeys(t *testing.T) {
	c := setUpSub()
	s := must(c.m.Sub_manifest(py.NewList("sample_1", "sample_3")))
	assertSetEq(t, must(s.Keys()), "sample_1", "sample_3")
	assertEq(t, getitem(t, s, "sample_1"), d("mod_a", c.gid_a))
	assertEq(t, getitem(t, s, "sample_3"), d("mod_c", c.gid_c))
}

func Test054SubManifestAllKeys(t *testing.T) {
	c := setUpSub()
	s := must(c.m.Sub_manifest(py.NewList("sample_1", "sample_2", "sample_3")))
	assertEq(t, s.Len(), 3)
	assertEq(t, blueprint(s), blueprint(c.m))
}

func Test055SubManifestPreservesNestedStructure(t *testing.T) {
	gid_1, gid_2 := _make_gid(), _make_gid()
	m := manifest(py.Kw{"data": d(
		"sample_1", d("mod_a", gid_1, "mod_b", gid_2),
		"sample_2", d("mod_a", gid_1),
	)})
	s := must(m.Sub_manifest(py.NewList("sample_1")))
	assertEq(t, getitem(t, s, "sample_1"), d("mod_a", gid_1, "mod_b", gid_2))
}

func Test056SubManifestPreservesListLeaves(t *testing.T) {
	gids := py.NewList(_make_gid(), _make_gid())
	m := manifest(py.Kw{"data": d(
		"sample_1", d("batch", gids),
		"sample_2", d("single", _make_gid()),
	)})
	s := must(m.Sub_manifest(py.NewList("sample_1")))
	assertEq(t, sub(t, getitem(t, s, "sample_1"), "batch"), gids)
}

func Test057SubManifestMissingKeyRaises(t *testing.T) {
	c := setUpSub()
	_, err := c.m.Sub_manifest(py.NewList("sample_1", "nonexistent"))
	assertRaises(t, py.KeyError, err)
}

func Test058SubManifestEmptyManifestRaises(t *testing.T) {
	m := manifest()
	_, err := m.Sub_manifest(py.NewList("any_key"))
	assertRaises(t, py.RuntimeError, err)
}

func Test059SubManifestIsIndependentCopy(t *testing.T) {
	c := setUpSub()
	s := must(c.m.Sub_manifest(py.NewList("sample_1")))
	getitem(t, s, "sample_1").(*py.Dict).Set("mod_a", "MUTATED")
	assertEq(t, sub(t, getitem(t, c.m, "sample_1"), "mod_a"), c.gid_a)
}

func Test060SubManifestHasOwnIdentity(t *testing.T) {
	c := setUpSub()
	s := must(c.m.Sub_manifest(py.NewList("sample_1")))
	assertNe(t, s.Global_id(), c.m.Global_id())
}

func Test061SubManifestIterable(t *testing.T) {
	c := setUpSub()
	s := must(c.m.Sub_manifest(py.NewList("sample_1", "sample_2")))
	assertSetEq(t, s.Iter(), c.gid_a, c.gid_b)
}

// =============================================================================
// TestManifestMemoryRoundtrip -- memorize, remember, forget through the
// policy layer.
// =============================================================================

type roundtripCase struct {
	original any
	policy   *laila.LAILA_IDENTIFIABLE_POLICY_
	pool     *laila.LAILA_IDENTIFIABLE_POOL_
}

func setUpRoundtrip(t *testing.T) *roundtripCase {
	t.Helper()
	c := &roundtripCase{}
	c.original = must(laila.Get_active_policy())
	c.policy = must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.Activate_policy(c.policy)
	laila.Local_policies_.Set(c.policy.Global_id(), c.policy)

	c.pool = must(laila.New_LAILA_IDENTIFIABLE_POOL_())
	must(0, laila.As_central_memory_(must(laila.Memory())).Extend(c.pool, py.Kw{"pool_nickname": "test_pool"}))

	t.Cleanup(func() {
		if cmd, ok := c.policy.Central.Command.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_); ok {
			_ = cmd.Shutdown(py.Kw{"wait": true, "cancel_pending": true})
		}
		laila.Local_policies_.PopDefault(c.policy.Global_id(), nil)
		laila.Activate_policy(c.original)
	})
	return c
}

func wait(t *testing.T, f *laila.GroupFuture) any {
	t.Helper()
	return must(f.Wait())
}

func exists(t *testing.T, pool *laila.LAILA_IDENTIFIABLE_POOL_, key string) bool {
	t.Helper()
	return must(pool.Exists(key))
}

func Test037MemorizeReturnsGroupFuture(t *testing.T) {
	setUpRoundtrip(t)
	e := constant("test")
	m := manifest(py.Kw{"data": d("k", e)})
	future := must(m.Memorize(py.Kw{"pool_nickname": "test_pool"}))
	if !py.Isinstance(future, laila.GroupFuture_class) {
		t.Fatalf("%T", future)
	}
	wait(t, future)
}

func Test038RememberReturnsGroupFuture(t *testing.T) {
	setUpRoundtrip(t)
	e := constant("test")
	m := manifest(py.Kw{"data": d("k", e)})
	wait(t, must(m.Memorize(py.Kw{"pool_nickname": "test_pool"})))

	m2 := manifest(py.Kw{"data": blueprint(m)})
	future := must(m2.Remember(py.Kw{"pool_nickname": "test_pool"}))
	if !py.Isinstance(future, laila.GroupFuture_class) {
		t.Fatalf("%T", future)
	}
	results := wait(t, future)
	assertEq(t, must(py.Len(results)), 1)
}

func Test039ForgetReturnsGroupFuture(t *testing.T) {
	setUpRoundtrip(t)
	e := constant("test")
	m := manifest(py.Kw{"data": d("k", e)})
	wait(t, must(m.Memorize(py.Kw{"pool_nickname": "test_pool"})))

	future := must(m.Forget(py.Kw{"pool_nickname": "test_pool"}))
	if !py.Isinstance(future, laila.GroupFuture_class) {
		t.Fatalf("%T", future)
	}
	wait(t, future)
}

func Test040MemorizeAndResolvedRoundtrip(t *testing.T) {
	setUpRoundtrip(t)
	e1 := constant("alpha")
	e2 := constant("beta")
	m := manifest(py.Kw{"data": d("a", e1, "b", e2)})

	wait(t, must(m.Memorize()))

	if blueprint(m) == nil {
		t.Fatal("blueprint is None")
	}
	assertEq(t, sub(t, blueprint(m), "a"), e1.Global_id())

	m2 := manifest(py.Kw{"data": blueprint(m)})
	resolved := must(m2.Realized())
	assertEq(t, entryData(t, sub(t, resolved, "a")), "alpha")
	assertEq(t, entryData(t, sub(t, resolved, "b")), "beta")
}

func Test041MemorizeAndForgetDeletesEntries(t *testing.T) {
	c := setUpRoundtrip(t)
	e := constant("ephemeral")
	m := manifest(py.Kw{"data": d("k", e)})
	wait(t, must(m.Memorize(py.Kw{"pool_nickname": "test_pool"})))

	if !exists(t, c.pool, e.Global_id()) {
		t.Fatal("entry should exist")
	}

	wait(t, must(m.Forget(py.Kw{"pool_nickname": "test_pool"})))

	if exists(t, c.pool, e.Global_id()) {
		t.Fatal("entry should be gone")
	}
}

func Test042MemorizeStoresManifestItself(t *testing.T) {
	c := setUpRoundtrip(t)
	e := constant(99)
	m := manifest(py.Kw{"data": d("k", e), "nickname": "self_store_test"})
	wait(t, must(m.Memorize(py.Kw{"pool_nickname": "test_pool"})))

	if !exists(t, c.pool, m.Global_id()) {
		t.Fatal("manifest should be stored")
	}
}

func Test043ForgetDeletesManifestItself(t *testing.T) {
	c := setUpRoundtrip(t)
	e := constant("keep_manifest")
	m := manifest(py.Kw{"data": d("k", e)})
	wait(t, must(m.Memorize(py.Kw{"pool_nickname": "test_pool"})))

	if !exists(t, c.pool, m.Global_id()) {
		t.Fatal("manifest should be stored")
	}

	wait(t, must(m.Forget(py.Kw{"pool_nickname": "test_pool"})))

	if exists(t, c.pool, e.Global_id()) {
		t.Fatal("entry should be gone")
	}
	if exists(t, c.pool, m.Global_id()) {
		t.Fatal("manifest should be gone")
	}
}

func Test044NestedEntryMemorizeResolvedRoundtrip(t *testing.T) {
	setUpRoundtrip(t)
	e1 := constant("layer1")
	e2 := constant("layer2")
	e3 := constant("layer3")
	m := manifest(py.Kw{"data": d(
		"top", e1,
		"group", d("nested", e2),
		"batch", py.NewList(e3),
	)})

	wait(t, must(m.Memorize()))

	m2 := manifest(py.Kw{"data": blueprint(m)})
	resolved := must(m2.Realized())

	assertEq(t, entryData(t, sub(t, resolved, "top")), "layer1")
	assertEq(t, entryData(t, sub(t, sub(t, resolved, "group"), "nested")), "layer2")
	assertEq(t, entryData(t, must(sub(t, resolved, "batch").(*py.List).Get(0))), "layer3")
}

func Test045BatchSizeRespected(t *testing.T) {
	c := setUpRoundtrip(t)
	entries := []*laila.Entry{}
	items := py.NewList()
	for i := 0; i < 5; i++ {
		e := constant(i)
		entries = append(entries, e)
		items.Append(e)
	}
	m := manifest(py.Kw{"data": d("items", items)})

	wait(t, must(m.Memorize(py.Kw{"pool_nickname": "test_pool", "batch_size": 2})))

	for _, e := range entries {
		if !exists(t, c.pool, e.Global_id()) {
			t.Fatalf("%s missing", e.Global_id())
		}
	}
}

func Test046RememberWithPoolNickname(t *testing.T) {
	setUpRoundtrip(t)
	e1 := constant("recall_me")
	m := manifest(py.Kw{"data": d("k", e1)})
	wait(t, must(m.Memorize(py.Kw{"pool_nickname": "test_pool"})))

	m2 := manifest(py.Kw{"data": blueprint(m)})
	future := must(m2.Remember(py.Kw{"pool_nickname": "test_pool"}))
	results := wait(t, future)
	assertEq(t, must(py.Len(results)), 1)
	assertEq(t, entryData(t, py.Iter(results)[0]), "recall_me")
}

// =============================================================================
// TestPoolWrapper -- pool[manifest] -> PoolWrapper and the <= copy operator.
// =============================================================================

type wrapperCase struct {
	original            any
	policy              *laila.LAILA_IDENTIFIABLE_POLICY_
	src_pool, dest_pool *laila.LAILA_IDENTIFIABLE_POOL_
}

func setUpWrapper(t *testing.T) *wrapperCase {
	t.Helper()
	c := &wrapperCase{}
	c.original = must(laila.Get_active_policy())
	c.policy = must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.Activate_policy(c.policy)
	laila.Local_policies_.Set(c.policy.Global_id(), c.policy)

	c.src_pool = must(laila.New_LAILA_IDENTIFIABLE_POOL_())
	c.dest_pool = must(laila.New_LAILA_IDENTIFIABLE_POOL_())
	memory := laila.As_central_memory_(must(laila.Memory()))
	must(0, memory.Extend(c.src_pool, py.Kw{"pool_nickname": "src"}))
	must(0, memory.Extend(c.dest_pool, py.Kw{"pool_nickname": "dest"}))

	t.Cleanup(func() {
		if cmd, ok := c.policy.Central.Command.(*laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_); ok {
			_ = cmd.Shutdown(py.Kw{"wait": true, "cancel_pending": true})
		}
		laila.Local_policies_.PopDefault(c.policy.Global_id(), nil)
		laila.Activate_policy(c.original)
	})
	return c
}

func Test047PoolGetitemManifestReturnsPoolWrapper(t *testing.T) {
	c := setUpWrapper(t)
	m := manifest(py.Kw{"data": d("k", _make_gid())})
	wrapper, ok := must(c.src_pool.Getitem(m)).(*laila.PoolWrapper)
	if !ok {
		t.Fatal("pool[manifest] should be a PoolWrapper")
	}
	assertIs(t, wrapper.Pool, c.src_pool)
	assertIs(t, wrapper.Manifest, m)
}

func Test048PoolGetitemStringStillWorks(t *testing.T) {
	c := setUpWrapper(t)
	must(0, c.src_pool.Setitem("test_key", "test_value"))
	assertEq(t, must(c.src_pool.Getitem("test_key")), "test_value")
}

func Test049PoolWrapperLeCopiesManifestEntries(t *testing.T) {
	c := setUpWrapper(t)
	e1 := constant("copy_me")
	e2 := constant("copy_me_too")
	m := manifest(py.Kw{"data": d("a", e1, "b", e2)})

	wait(t, must(m.Memorize(py.Kw{"pool_id": c.src_pool.Global_id()})))

	if !exists(t, c.src_pool, e1.Global_id()) {
		t.Fatal("src should hold e1")
	}
	if exists(t, c.dest_pool, e1.Global_id()) {
		t.Fatal("dest should not hold e1 yet")
	}

	dest := must(c.dest_pool.Getitem(m)).(*laila.PoolWrapper)
	src := must(c.src_pool.Getitem(m)).(*laila.PoolWrapper)
	future, ok := must(dest.Le(src)).(*laila.GroupFuture)
	if !ok {
		t.Fatal("<= should return a GroupFuture")
	}
	wait(t, future)

	if !exists(t, c.dest_pool, e1.Global_id()) {
		t.Fatal("dest should hold e1")
	}
	if !exists(t, c.dest_pool, e2.Global_id()) {
		t.Fatal("dest should hold e2")
	}
}

func Test050PoolWrapperLeNonWrapperReturnsNotImplemented(t *testing.T) {
	c := setUpWrapper(t)
	m := manifest(py.Kw{"data": d("k", _make_gid())})
	wrapper := must(c.src_pool.Getitem(m)).(*laila.PoolWrapper)
	// NotImplemented surfaces as the interpreter's TypeError in the port.
	_, err := wrapper.Le("not a wrapper")
	assertRaises(t, py.TypeError, err)
}

// =============================================================================
// TestManifestCombine -- extend, __iadd__, and __add__.
// =============================================================================

// ── extend ──

func Test062ExtendMergesDisjointBlueprints(t *testing.T) {
	gid_a, gid_b := _make_gid(), _make_gid()
	m1 := manifest(py.Kw{"data": d("a", gid_a)})
	m2 := manifest(py.Kw{"data": d("b", gid_b)})
	must(0, m1.Extend(m2))
	assertEq(t, blueprint(m1), d("a", gid_a, "b", gid_b))
}

func Test063ExtendRaisesOnDuplicateKeys(t *testing.T) {
	gid := _make_gid()
	m1 := manifest(py.Kw{"data": d("a", gid)})
	m2 := manifest(py.Kw{"data": d("a", _make_gid())})
	assertRaises(t, py.KeyError, m1.Extend(m2))
}

func Test064ExtendOverwriteAllowsDuplicateKeys(t *testing.T) {
	gid_old, gid_new := _make_gid(), _make_gid()
	m1 := manifest(py.Kw{"data": d("a", gid_old)})
	m2 := manifest(py.Kw{"data": d("a", gid_new)})
	must(0, m1.Extend(m2, py.Kw{"overwrite": true}))
	assertEq(t, getitem(t, m1, "a"), gid_new)
}

func Test065ExtendIntoEmptyManifest(t *testing.T) {
	gid := _make_gid()
	m1 := manifest()
	m2 := manifest(py.Kw{"data": d("k", gid)})
	must(0, m1.Extend(m2))
	assertEq(t, blueprint(m1), d("k", gid))
	assertEq(t, m1.Len(), 1)
}

func Test066ExtendWithEmptyOtherIsNoop(t *testing.T) {
	gid := _make_gid()
	m1 := manifest(py.Kw{"data": d("a", gid)})
	m2 := manifest()
	must(0, m1.Extend(m2))
	assertEq(t, blueprint(m1), d("a", gid))
}

func Test067ExtendNonManifestRaisesTypeError(t *testing.T) {
	m := manifest(py.Kw{"data": d("a", _make_gid())})
	assertRaises(t, py.TypeError, m.Extend(d("b", _make_gid())))
}

func Test068ExtendPreservesNestedStructure(t *testing.T) {
	gid_a, gid_b := _make_gid(), _make_gid()
	m1 := manifest(py.Kw{"data": d("layer1", d("inner", gid_a))})
	m2 := manifest(py.Kw{"data": d("layer2", d("inner", gid_b))})
	must(0, m1.Extend(m2))
	assertEq(t, sub(t, getitem(t, m1, "layer1"), "inner"), gid_a)
	assertEq(t, sub(t, getitem(t, m1, "layer2"), "inner"), gid_b)
}

func Test069ExtendPreservesListLeaves(t *testing.T) {
	gids_a := py.NewList(_make_gid(), _make_gid())
	gids_b := py.NewList(_make_gid())
	m1 := manifest(py.Kw{"data": d("batch_a", gids_a)})
	m2 := manifest(py.Kw{"data": d("batch_b", gids_b)})
	must(0, m1.Extend(m2))
	assertEq(t, getitem(t, m1, "batch_a"), gids_a)
	assertEq(t, getitem(t, m1, "batch_b"), gids_b)
}

func pendingGids(t *testing.T, m *laila.Manifest) *py.Set {
	t.Helper()
	if m.Pending_entries_ == nil {
		t.Fatal("_pending_entries is None")
	}
	out := py.NewSet()
	for _, e := range m.Pending_entries_.Items() {
		out.Add(laila.As_entry_(e).Global_id())
	}
	return out
}

func Test070ExtendCombinesPendingEntries(t *testing.T) {
	e1 := constant("one")
	e2 := constant("two")
	m1 := manifest(py.Kw{"data": d("a", e1)})
	m2 := manifest(py.Kw{"data": d("b", e2)})
	must(0, m1.Extend(m2))
	pending := pendingGids(t, m1)
	if !pending.Contains(e1.Global_id()) || !pending.Contains(e2.Global_id()) {
		t.Fatal(py.Repr(pending))
	}
}

func Test071ExtendDeepCopiesOtherBlueprint(t *testing.T) {
	gid := _make_gid()
	m1 := manifest(py.Kw{"data": d("a", _make_gid())})
	m2 := manifest(py.Kw{"data": d("b", d("nested", gid))})
	must(0, m1.Extend(m2))
	sub(t, blueprint(m2), "b").(*py.Dict).Set("nested", "MUTATED")
	assertEq(t, sub(t, getitem(t, m1, "b"), "nested"), gid)
}

// ── __iadd__ ──

func Test072IaddMergesInPlaceAndReturnsSelf(t *testing.T) {
	gid_a, gid_b := _make_gid(), _make_gid()
	m1 := manifest(py.Kw{"data": d("a", gid_a)})
	m2 := manifest(py.Kw{"data": d("b", gid_b)})
	original_id := m1.Global_id()
	result := must(m1.Iadd(m2))
	assertIs(t, result, m1)
	assertEq(t, m1.Global_id(), original_id)
	assertEq(t, blueprint(m1), d("a", gid_a, "b", gid_b))
}

func Test073IaddOperatorSyntax(t *testing.T) {
	gid_a, gid_b := _make_gid(), _make_gid()
	m1 := manifest(py.Kw{"data": d("a", gid_a)})
	m2 := manifest(py.Kw{"data": d("b", gid_b)})
	identity_before := m1
	m1 = must(m1.Iadd(m2)).(*laila.Manifest) // m1 += m2
	assertIs(t, m1, identity_before)
	assertEq(t, blueprint(m1), d("a", gid_a, "b", gid_b))
}

func Test074IaddRaisesOnDuplicateKeys(t *testing.T) {
	m1 := manifest(py.Kw{"data": d("a", _make_gid())})
	m2 := manifest(py.Kw{"data": d("a", _make_gid())})
	_, err := m1.Iadd(m2)
	assertRaises(t, py.KeyError, err)
}

func Test075IaddNonManifestReturnsNotImplemented(t *testing.T) {
	m := manifest(py.Kw{"data": d("a", _make_gid())})
	// NotImplemented surfaces as the interpreter's TypeError in the port.
	_, err := m.Iadd("not a manifest")
	assertRaises(t, py.TypeError, err)
}

// ── __add__ ──

func Test076AddReturnsNewManifest(t *testing.T) {
	gid_a, gid_b := _make_gid(), _make_gid()
	m1 := manifest(py.Kw{"data": d("a", gid_a)})
	m2 := manifest(py.Kw{"data": d("b", gid_b)})
	m3, ok := must(m1.Add(m2)).(*laila.Manifest)
	if !ok {
		t.Fatal("m1 + m2 should be a Manifest")
	}
	assertEq(t, blueprint(m3), d("a", gid_a, "b", gid_b))
}

func Test077AddDoesNotMutateOriginals(t *testing.T) {
	gid_a, gid_b := _make_gid(), _make_gid()
	m1 := manifest(py.Kw{"data": d("a", gid_a)})
	m2 := manifest(py.Kw{"data": d("b", gid_b)})
	must(m1.Add(m2))
	assertEq(t, blueprint(m1), d("a", gid_a))
	assertEq(t, blueprint(m2), d("b", gid_b))
}

func Test078AddRaisesOnDuplicateKeys(t *testing.T) {
	m1 := manifest(py.Kw{"data": d("a", _make_gid())})
	m2 := manifest(py.Kw{"data": d("a", _make_gid())})
	_, err := m1.Add(m2)
	assertRaises(t, py.KeyError, err)
}

func Test079AddNonManifestReturnsNotImplemented(t *testing.T) {
	m := manifest(py.Kw{"data": d("a", _make_gid())})
	// NotImplemented surfaces as the interpreter's TypeError in the port.
	_, err := m.Add("not a manifest")
	assertRaises(t, py.TypeError, err)
}

func Test080AddNewManifestHasOwnIdentity(t *testing.T) {
	m1 := manifest(py.Kw{"data": d("a", _make_gid())})
	m2 := manifest(py.Kw{"data": d("b", _make_gid())})
	m3 := must(m1.Add(m2)).(*laila.Manifest)
	assertNe(t, m3.Global_id(), m1.Global_id())
	assertNe(t, m3.Global_id(), m2.Global_id())
}

func Test081AddTwoEmptyManifests(t *testing.T) {
	m1 := manifest()
	m2 := manifest()
	m3, ok := must(m1.Add(m2)).(*laila.Manifest)
	if !ok {
		t.Fatal("m1 + m2 should be a Manifest")
	}
	assertEq(t, m3.Len(), 0)
}

func Test082AddEmptyPlusNonEmpty(t *testing.T) {
	gid := _make_gid()
	m1 := manifest()
	m2 := manifest(py.Kw{"data": d("k", gid)})
	m3 := must(m1.Add(m2)).(*laila.Manifest)
	assertEq(t, blueprint(m3), d("k", gid))
}

func Test083AddCombinesPendingEntries(t *testing.T) {
	e1 := constant("one")
	e2 := constant("two")
	m1 := manifest(py.Kw{"data": d("a", e1)})
	m2 := manifest(py.Kw{"data": d("b", e2)})
	m3 := must(m1.Add(m2)).(*laila.Manifest)
	pending := pendingGids(t, m3)
	if !pending.Contains(e1.Global_id()) || !pending.Contains(e2.Global_id()) {
		t.Fatal(py.Repr(pending))
	}
}

func Test084AddDeepCopiesBlueprints(t *testing.T) {
	gid := _make_gid()
	m1 := manifest(py.Kw{"data": d("a", d("nested", gid))})
	m2 := manifest(py.Kw{"data": d("b", _make_gid())})
	m3 := must(m1.Add(m2)).(*laila.Manifest)
	sub(t, blueprint(m1), "a").(*py.Dict).Set("nested", "MUTATED")
	assertEq(t, sub(t, getitem(t, m3, "a"), "nested"), gid)
}
