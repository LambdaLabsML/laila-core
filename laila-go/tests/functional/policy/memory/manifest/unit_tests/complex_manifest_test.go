// Port of tests/functional/policy/memory/manifest/unit_tests/test_complex_manifest.py.
//
// ``ComplexManifest``: composition, recursive memorize, recursive blueprint
// resolution on arrival, ``rebuild`` and owner-aware ``forget`` -- torch-free.
package unit_tests

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	_ "github.com/amirzadehlambda/laila/laila-go/drivers/hdf5"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// HAVE_HDF5 is ``from laila.data import HDF5Pool`` succeeding (h5py installed).
var HAVE_HDF5 = laila.Driver_available("h5py")

// _wait is ``ref.wait(_T); return ref.result`` with the release in a
// ``finally``.
func _wait(ref any) (any, error) {
	defer ref.(releaser).Release()
	if _, err := ref.(waiter).Wait(_T); err != nil {
		return nil, err
	}
	return py.Getattr(ref, "result")
}

// _remember is ``laila.remember(gid, dst_pool=pool, persist=False, **kwargs)``
// waited.
func _remember(gid any, pool any, kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	opts["dst_pool"] = pool
	opts["persist"] = false
	ref, err := laila.Remember(gid, opts)
	if err != nil {
		return nil, err
	}
	return _wait(ref)
}

func mustRemember(t *testing.T, gid any, pool any, kw ...py.Kw) any {
	t.Helper()
	out, err := _remember(gid, pool, kw...)
	if err != nil {
		t.Fatalf("remember(%s) failed: %v", py.Repr(gid), err)
	}
	return out
}

// complexManifest is ``laila.complex_manifest(**kw)``.
func complexManifest(kw ...py.Kw) *laila.ComplexManifest {
	return must(laila.New_ComplexManifest(kw...))
}

// classOf is ``type(v)``.
func classOf(v any) *py.Class { return py.Classof(v) }

func assertIsClass(t *testing.T, v any, cls *py.Class) {
	t.Helper()
	if classOf(v) != cls {
		t.Fatalf("type(%s) is %v, want %s", py.Repr(v), classOf(v), cls.Name)
	}
}

func assertIsInstance(t *testing.T, v any, cls *py.Class) {
	t.Helper()
	if !py.Isinstance(v, cls) {
		t.Fatalf("%s is not an instance of %s", py.Repr(v), cls.Name)
	}
}

func assertNotIn(t *testing.T, container any, key any) {
	t.Helper()
	for _, k := range py.Iter(container) {
		if py.Eq(k, key) {
			t.Fatalf("%s unexpectedly in %s", py.Repr(key), py.Repr(container))
		}
	}
}

func assertIn(t *testing.T, container any, key any) {
	t.Helper()
	for _, k := range py.Iter(container) {
		if py.Eq(k, key) {
			return
		}
	}
	t.Fatalf("%s not in %s", py.Repr(key), py.Repr(container))
}

func gidList(ms ...any) []any {
	out := []any{}
	for _, m := range ms {
		switch x := m.(type) {
		case string:
			out = append(out, x)
		case *laila.Manifest:
			out = append(out, x.Iter()...)
		case *laila.ComplexManifest:
			out = append(out, x.Iter()...)
		}
	}
	return out
}

func setitem(t *testing.T, m interface{ Setitem(any, any) error }, key any, value any) {
	t.Helper()
	if err := m.Setitem(key, value); err != nil {
		t.Fatalf("%s[%s] = %s: %v", py.Repr(m), py.Repr(key), py.Repr(value), err)
	}
}

func cget(t *testing.T, c *laila.ComplexManifest, key any) any {
	t.Helper()
	return must(c.Getitem(key))
}

// _FakeBacked is a ``ManifestBacked`` stand-in: its manifest holds one
// entry with its value.
type _FakeBacked struct {
	value          any
	nickname       string
	memorize_calls []py.Kw
}

func newFakeBacked(value any, nickname string) *_FakeBacked {
	return &_FakeBacked{value: value, nickname: nickname}
}

func (f *_FakeBacked) Manifest() (*laila.Manifest, error) {
	leaf, err := laila.Entry_constant(f.value, py.Kw{"nickname": f.nickname + "/value"})
	if err != nil {
		return nil, err
	}
	return laila.New_Manifest(py.Kw{"data": d("value", leaf), "nickname": f.nickname})
}

func (f *_FakeBacked) Global_id() string {
	return must(laila.New_Manifest(py.Kw{"nickname": f.nickname})).Global_id()
}

func (f *_FakeBacked) Memorize(kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	pool := opts.Pop("pool", nil)
	f.memorize_calls = append(f.memorize_calls, opts)
	m, err := f.Manifest()
	if err != nil {
		return nil, err
	}
	fut, err := m.Memorize(py.Kw{"pool": pool})
	if err != nil {
		return nil, err
	}
	return fut, nil
}

func (f *_FakeBacked) Remember(kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	m, err := f.Manifest()
	if err != nil {
		return nil, err
	}
	fut, err := m.Remember(py.Kw{"pool": opts.Get("pool", nil), "persist": opts.Get("persist", false)})
	if err != nil {
		return nil, err
	}
	return fut, nil
}

func (f *_FakeBacked) Forget(kw ...py.Kw) (any, error) {
	opts := py.MergeKw(kw...)
	m, err := f.Manifest()
	if err != nil {
		return nil, err
	}
	fut, err := m.Forget(py.Kw{"pool": opts.Get("pool", nil)})
	if err != nil {
		return nil, err
	}
	return fut, nil
}

var _ laila.ManifestBacked = (*_FakeBacked)(nil)

// _Bare is a plain object with no laila knowledge.
type _Bare struct{ value any }

// =============================================================================
// TestBuilderLookup
// =============================================================================

func TestJoinedScopeAndPrefixFallback(t *testing.T) {
	complex_pair, ok := laila.BUILDER_MAP["MANIFEST:COMPLEX"]
	if !ok {
		t.Fatal("MANIFEST:COMPLEX not in BUILDER_MAP")
	}
	same := func(a, b laila.Builder_pair) bool {
		return reflect.ValueOf(a.Sync).Pointer() == reflect.ValueOf(b.Sync).Pointer() &&
			reflect.ValueOf(a.Async).Pointer() == reflect.ValueOf(b.Async).Pointer()
	}
	pair, ok := laila.Lookup_builder([]string{"MANIFEST", "COMPLEX"})
	if !ok || !same(pair, complex_pair) {
		t.Fatal("lookup_builder([MANIFEST, COMPLEX]) is not the MANIFEST:COMPLEX pair")
	}
	// An unknown refinement falls back to the generic manifest builder.
	pair, ok = laila.Lookup_builder([]string{"MANIFEST", "NOT_A_THING"})
	if !ok || !same(pair, laila.BUILDER_MAP["MANIFEST"]) {
		t.Fatal("lookup_builder([MANIFEST, NOT_A_THING]) did not fall back to MANIFEST")
	}
	pair, ok = laila.Lookup_builder(nil)
	if !ok || !same(pair, laila.BUILDER_MAP["ENTRY"]) {
		t.Fatal("lookup_builder(None) is not the ENTRY pair")
	}
	if _, ok := laila.Lookup_builder([]string{"NO_SUCH_SCOPE"}); ok {
		t.Fatal("lookup_builder([NO_SUCH_SCOPE]) should be None")
	}
}

func TestIsManifestGidIsStringOnly(t *testing.T) {
	if !laila.Manifest_is_manifest_gid(manifest(py.Kw{"nickname": "x"}).Global_id()) {
		t.Fatal("Manifest gid not recognised")
	}
	if !laila.Manifest_is_manifest_gid(complexManifest(py.Kw{"nickname": "x"}).Global_id()) {
		t.Fatal("ComplexManifest gid not recognised")
	}
	if !laila.Manifest_is_manifest_gid("LAILA:MANIFEST:" + py.Str(py.Uuid4()) + "@evolution=-1") {
		t.Fatal("floating manifest gid not recognised")
	}
	if laila.Manifest_is_manifest_gid(constant(1).Global_id()) {
		t.Fatal("entry gid recognised as manifest")
	}
	if laila.Manifest_is_manifest_gid("garbage") {
		t.Fatal("garbage recognised as manifest")
	}
	if laila.Manifest_is_manifest_gid(nil) {
		t.Fatal("None recognised as manifest")
	}
}

func TestScopesAndNicknameReference(t *testing.T) {
	c := complexManifest(py.Kw{"nickname": "everything"})
	assertEq(t, c.Scopes(), []string{"MANIFEST", "COMPLEX"})
	assertEq(t, c.Global_id(), must(laila.Resolve_global_id_ref("MANIFEST:COMPLEX:everything")))
	assertEq(t, c.Nickname(), "everything")
	assertIs(t, laila.Complex_manifest, laila.ComplexManifest_class)
}

// =============================================================================
// TestSetItem -- ``__setitem__`` / ``__delitem__`` on plain and complex manifests.
// =============================================================================

func TestPlainManifestAcceptsFragmentsAndEntries(t *testing.T) {
	m := manifest(py.Kw{"nickname": "plain_" + py.Uuid4().Hex()[:6]})
	assertEq(t, m.Len(), 0)
	e := constant(1)
	setitem(t, m, "a", e)
	setitem(t, m, "b", "LAILA:ENTRY:"+py.Str(py.Uuid4()))
	setitem(t, m, "c", d("x", constant(2), "y", py.NewList(constant(3))))
	setitem(t, m, "d", py.NewList("LAILA:ENTRY:"+py.Str(py.Uuid4())))
	assertEq(t, getitem(t, m, "a"), e.Global_id()) // plain manifests return the gid
	assertSetEq(t, must(m.Keys()), "a", "b", "c", "d")
	assertEq(t, m.State(), laila.EntryState_READY)
	assertEq(t, m.Pending_entries_.Len(), 3)
	if !m.Locally_modified() {
		t.Fatal("manifest should be locally modified")
	}
	must(0, m.Delitem("b"))
	assertNotIn(t, must(m.Keys()), "b")
	assertRaises(t, py.KeyError, m.Delitem("b"))
	setitem(t, m, "a", constant(9)) // re-assign replaces
	assertNe(t, getitem(t, m, "a"), e.Global_id())
}

func TestPlainManifestRejectsManifestsAndJunk(t *testing.T) {
	m := manifest(py.Kw{"nickname": "plain"})
	assertRaises(t, py.TypeError, m.Setitem("ds", manifest(py.Kw{"data": d("q", constant(0))})))
	assertRaises(t, py.TypeError, m.Setitem("ds", d("nested", manifest(py.Kw{"data": d("q", constant(0))}))))
	assertRaises(t, py.TypeError, m.Setitem("x", 42))
	assertRaises(t, py.TypeError, m.Setitem(1, constant(0)))
	assertRaises(t, py.ValueError, m.Setitem("mix", d("a", constant(0), "b", "LAILA:ENTRY:"+py.Str(py.Uuid4()))))
	assertRaises(t, py.ValueError, m.Setitem("bad", d("a", 3.5)))
}

func TestComplexManifestMembers(t *testing.T) {
	c := complexManifest(py.Kw{"nickname": "everything"})
	ds := manifest(py.Kw{"nickname": "ds", "data": d("q", constant(0))})
	sub := complexManifest(py.Kw{"nickname": "sub", "data": d("k", constant(5))})
	backed := newFakeBacked(7, "backed")
	cfg := constant(d("lr", 0.1))
	setitem(t, c, "dataset", ds)
	setitem(t, c, "sub", sub)
	setitem(t, c, "backed", backed)
	setitem(t, c, "config", cfg)
	setitem(t, c, "frag", d("x", constant(1)))
	assertIs(t, cget(t, c, "dataset"), ds)
	assertIs(t, cget(t, c, "sub"), sub)
	assertIs(t, cget(t, c, "backed"), backed)
	assertIs(t, cget(t, c, "config"), cfg)
	bp := blueprint(&c.Manifest)
	assertEq(t, sub2(t, bp, "dataset"), ds.Global_id())
	assertEq(t, sub2(t, bp, "backed"), backed.Global_id())
	assertEq(t, sub2(t, bp, "config"), cfg.Global_id())
	assertSetEq(t, sub2(t, bp, "frag").(*py.Dict).Keys(), "x")
	assertSetEq(t, c.Members().Keys(), "dataset", "sub", "backed", "config")
	assertSetEq(t, must(c.Keys()), "dataset", "sub", "backed", "config", "frag")
	must(0, c.Delitem("backed"))
	assertNotIn(t, c.Members().Keys(), "backed")
	assertRaises(t, py.TypeError, c.Setitem("junk", &_Bare{1}))
}

// sub2 is ``v[key]`` with the error folded into must (dict lookups).
func sub2(t *testing.T, v *py.Dict, key any) any {
	t.Helper()
	return must(v.Getitem(key))
}

func TestAdapterRegistry(t *testing.T) {
	calls := [][2]any{}
	factory := func(value any, key string, nickname any) (any, error) {
		calls = append(calls, [2]any{key, nickname})
		return newFakeBacked(value.(*_Bare).value, py.Str(nickname)+"/"+key), nil
	}
	bare_type := reflect.TypeOf(&_Bare{})
	laila.ComplexManifest_register_adapter(bare_type, factory)
	defer laila.ComplexManifest_unregister_adapter_(bare_type)

	c := complexManifest(py.Kw{"nickname": "adapt"})
	bare := &_Bare{3}
	setitem(t, c, "thing", bare)
	assertIs(t, cget(t, c, "thing"), bare) // you get back what you put in
	assertEq(t, len(calls), 1)
	assertEq(t, calls[0][0], "thing")
	assertEq(t, calls[0][1], "adapt")
	assertEq(t, sub2(t, blueprint(&c.Manifest), "thing"), newFakeBacked(3, "adapt/thing").Global_id())
}

func TestConstructorDataGoesThroughSetitem(t *testing.T) {
	ds := manifest(py.Kw{"nickname": "ds2", "data": d("q", constant(0))})
	c := complexManifest(py.Kw{"nickname": "ctor", "data": d("dataset", ds, "config", constant(1))})
	assertIs(t, cget(t, c, "dataset"), ds)
	assertEq(t, c.State(), laila.EntryState_READY)
	_, err := laila.New_ComplexManifest(py.Kw{"data": py.NewList("not", "a", "dict")})
	assertRaises(t, py.ValueError, err)
	assertEq(t, blueprint(&complexManifest(py.Kw{"nickname": "empty", "data": d()}).Manifest), d())
}

// =============================================================================
// _PoolCase -- a registered secondary in-memory pool, so reads do not hit alpha.
// =============================================================================

type poolCase struct {
	tag       string
	memory    *laila.LAILA_IDENTIFIABLE_CENTRAL_MEMORY_
	pool_nick string
	pool      *laila.LAILA_IDENTIFIABLE_POOL_
}

func setUpPoolCase(t *testing.T) *poolCase {
	t.Helper()
	pc := &poolCase{tag: py.Uuid4().Hex()[:8]}
	pc.memory = _memory()
	pc.pool_nick = "complex_" + pc.tag
	pc.pool = laila.As_pool_(must(laila.New_DefaultPool()))
	must(0, pc.memory.Extend(pc.pool, py.Kw{"pool_nickname": pc.pool_nick}))
	t.Cleanup(func() {
		router := pc.memory.Pool_router.(*laila.LAILA_IDENTIFIABLE_POOL_ROUTER_)
		_, _ = router.Pools.Pop(pc.pool.Global_id())
		_, _ = router.Pools_nicknames.Pop(pc.pool_nick)
	})
	return pc
}

func (pc *poolCase) nick(name string) string { return name + "_" + pc.tag }

type everything struct {
	c      *laila.ComplexManifest
	ds     *laila.Manifest
	sub    *laila.ComplexManifest
	backed *_FakeBacked
}

func (pc *poolCase) build_everything(t *testing.T) everything {
	t.Helper()
	ds := manifest(py.Kw{"nickname": pc.nick("ds"), "data": d("q", constant("dataset leaf"))})
	sub := complexManifest(py.Kw{"nickname": pc.nick("sub")})
	setitem(t, sub, "k", constant(5))
	setitem(t, sub, "inner_ds", manifest(py.Kw{
		"nickname": pc.nick("inner_ds"), "data": d("z", constant("inner leaf")),
	}))
	backed := newFakeBacked(7, pc.nick("backed"))
	c := complexManifest(py.Kw{"nickname": pc.nick("everything")})
	setitem(t, c, "dataset", ds)
	setitem(t, c, "sub", sub)
	setitem(t, c, "backed", backed)
	setitem(t, c, "config", constant(d("lr", 0.1)))
	setitem(t, c, "frag", d("x", constant(1), "l", py.NewList(constant(2))))
	return everything{c: c, ds: ds, sub: sub, backed: backed}
}

// memorizeGuaranteed is ``with laila.guarantee: laila.memorize(obj, dst_pool=pool)``.
func memorizeGuaranteed(t *testing.T, obj any, pool string) {
	t.Helper()
	must(0, laila.Guarantee.With(func() error {
		_, err := laila.Memorize(obj, py.Kw{"dst_pool": pool})
		return err
	}))
}

func forgetGuaranteed(t *testing.T, obj any, pool string) {
	t.Helper()
	must(0, laila.Guarantee.With(func() error {
		_, err := laila.Forget(obj, py.Kw{"pool": pool})
		return err
	}))
}

// =============================================================================
// TestMemorizeRememberRebuildForget
// =============================================================================

func TestRecursiveMemorizeThenResolutionOnArrival(t *testing.T) {
	pc := setUpPoolCase(t)
	ev := pc.build_everything(t)
	c, ds, sub, backed := ev.c, ev.ds, ev.sub, ev.backed
	memorizeGuaranteed(t, c, pc.pool_nick)
	// Every member and every pending leaf landed in the pool.
	assertSetEq(t, mapKeys(backed.memorize_calls[0]), "batch_size", "context")
	if !backed.memorize_calls[0]["context"].(*py.Dict).Contains("dataset") {
		t.Fatal("context did not carry the dataset sibling")
	}
	gids := append([]any{c.Global_id(), ds.Global_id(), sub.Global_id(), backed.Global_id()},
		gidList(ds, sub, c)...)
	for _, gid := range gids {
		if py.IsNone(mustRemember(t, gid, pc.pool_nick)) {
			t.Fatalf("%s not stored", gid)
		}
	}

	back := laila.As_complex_manifest_(mustRemember(t, "MANIFEST:COMPLEX:"+pc.nick("everything"), pc.pool_nick))
	assertIsInstance(t, back, laila.ComplexManifest_class)
	if back == c {
		t.Fatal("remember returned the live object")
	}
	assertEq(t, blueprint(&back.Manifest), blueprint(&c.Manifest))
	// Manifest-scoped leaves were cast into typed members ...
	assertIsClass(t, cget(t, back, "dataset"), laila.Manifest_class)
	assertEq(t, blueprint(laila.As_manifest_(cget(t, back, "dataset"))), blueprint(ds))
	assertIsClass(t, cget(t, back, "sub"), laila.ComplexManifest_class)
	assertIsClass(t, cget(t, back, "backed"), laila.Manifest_class)
	// ... recursively ...
	back_sub := laila.As_complex_manifest_(cget(t, back, "sub"))
	assertIsClass(t, cget(t, back_sub, "inner_ds"), laila.Manifest_class)
	assertEq(t, cget(t, back_sub, "k"), sub2(t, blueprint(&sub.Manifest), "k"))
	// ... and plain leaves stayed gids.
	assertEq(t, cget(t, back, "config"), sub2(t, blueprint(&c.Manifest), "config"))
	assertEq(t, cget(t, back, "frag"), sub2(t, blueprint(&c.Manifest), "frag"))
	assertSetEq(t, back.Members().Keys(), "dataset", "sub", "backed")
	// A nested plain manifest is fetched as one record, never walked.
	assertEq(t, getitem(t, laila.As_manifest_(cget(t, back, "dataset")), "q"), sub2(t, blueprint(ds), "q"))
}

func mapKeys(kw py.Kw) []any {
	out := []any{}
	for k := range kw {
		out = append(out, k)
	}
	return out
}

func TestPlainManifestWithManifestLeavesIsNotScanned(t *testing.T) {
	pc := setUpPoolCase(t)
	ev := pc.build_everything(t)
	memorizeGuaranteed(t, ev.c, pc.pool_nick)
	plain := manifest(py.Kw{"nickname": pc.nick("plain"), "data": blueprint(&ev.c.Manifest).Copy()})
	memorizeGuaranteed(t, plain, pc.pool_nick)
	back := mustRemember(t, plain.Global_id(), pc.pool_nick)
	assertIsClass(t, back, laila.Manifest_class)
	assertEq(t, getitem(t, laila.As_manifest_(back), "dataset"), ev.ds.Global_id()) // still a gid
}

func TestResolveFalseAndExplicitResolve(t *testing.T) {
	pc := setUpPoolCase(t)
	ev := pc.build_everything(t)
	memorizeGuaranteed(t, ev.c, pc.pool_nick)
	bare_any := mustRemember(t, ev.c.Global_id(), pc.pool_nick, py.Kw{"resolve": false})
	assertIsInstance(t, bare_any, laila.ComplexManifest_class)
	bare := laila.As_complex_manifest_(bare_any)
	assertEq(t, cget(t, bare, "dataset"), ev.ds.Global_id())
	assertEq(t, bare.Members(), d())
	assertIs(t, must(bare.Resolve(py.Kw{"pool": pc.pool_nick})), bare)
	assertIsClass(t, cget(t, bare, "dataset"), laila.Manifest_class)
	assertIsClass(t, cget(t, laila.As_complex_manifest_(cget(t, bare, "sub")), "inner_ds"), laila.Manifest_class)
	must(bare.Resolve()) // idempotent
}

func TestRealizedSubstitutesMembers(t *testing.T) {
	pc := setUpPoolCase(t)
	ev := pc.build_everything(t)
	memorizeGuaranteed(t, ev.c, pc.pool_nick)
	back := laila.As_complex_manifest_(mustRemember(t, ev.c.Global_id(), pc.pool_nick))
	realized := must(back.Realized()).(*py.Dict)
	assertIs(t, sub2(t, realized, "dataset"), cget(t, back, "dataset"))
	assertIs(t, sub2(t, realized, "sub"), cget(t, back, "sub"))
	assertEq(t, entryData(t, sub2(t, realized, "config")), d("lr", 0.1))
	assertEq(t, entryData(t, sub2(t, sub2(t, realized, "frag").(*py.Dict), "x")), 1)
	assertEq(t, entryData(t, must(sub2(t, sub2(t, realized, "frag").(*py.Dict), "l").(*py.List).Get(0))), 2)
}

func TestRebuildRoundTripTypes(t *testing.T) {
	pc := setUpPoolCase(t)
	ev := pc.build_everything(t)
	c, ds, sub, backed := ev.c, ev.ds, ev.sub, ev.backed
	memorizeGuaranteed(t, c, pc.pool_nick)
	// Locally assigned objects come back untouched.
	local := must(c.Rebuild())
	assertIs(t, sub2(t, local, "dataset"), ds)
	assertIs(t, sub2(t, local, "sub"), sub)
	assertIs(t, sub2(t, local, "backed"), backed)

	back := laila.As_complex_manifest_(mustRemember(t, c.Global_id(), pc.pool_nick))
	live := must(back.Rebuild(py.Kw{"pool": pc.pool_nick}))
	assertEq(t, live.Keys(), blueprint(&c.Manifest).Keys())
	assertIsClass(t, sub2(t, live, "dataset"), laila.Manifest_class)
	assertIsClass(t, sub2(t, live, "sub"), laila.ComplexManifest_class)
	assertIsClass(t, sub2(t, live, "backed"), laila.Manifest_class)
	assertIsClass(t, sub2(t, live, "config"), laila.Entry_class)
	assertEq(t, entryData(t, sub2(t, live, "config")), d("lr", 0.1))
	assertEq(t, entryData(t, sub2(t, sub2(t, live, "frag").(*py.Dict), "x")), 1)
	// The nested complex manifest was rebuilt in place.
	live_sub := laila.As_complex_manifest_(sub2(t, live, "sub"))
	assertEq(t, entryData(t, cget(t, live_sub, "k")), 5)
	assertIsClass(t, cget(t, live_sub, "inner_ds"), laila.Manifest_class)
	// Members are now the live objects.
	assertIs(t, cget(t, back, "config"), sub2(t, live, "config"))
	assertIs(t, sub2(t, must(back.Rebuilt()), "config"), sub2(t, live, "config"))
}

func TestForgetIsOwnerAware(t *testing.T) {
	pc := setUpPoolCase(t)
	ev := pc.build_everything(t)
	c, ds, sub, backed := ev.c, ev.ds, ev.sub, ev.backed
	memorizeGuaranteed(t, c, pc.pool_nick)
	back := laila.As_complex_manifest_(mustRemember(t, c.Global_id(), pc.pool_nick))
	forgetGuaranteed(t, back, pc.pool_nick)
	c_bp := blueprint(&c.Manifest)
	gone := []any{c.Global_id(), sub.Global_id()}
	gone = append(gone, sub.Iter()...)
	gone = append(gone, backed.Global_id(), sub2(t, c_bp, "config"))
	gone = append(gone, laila.Manifest_iter_global_ids_(sub2(t, c_bp, "frag").(*py.Dict))...)
	for _, gid := range gone {
		_, err := _remember(gid, pc.pool_nick)
		if err == nil || !errors.Is(err, py.KeyError) {
			t.Fatalf("%s should be gone, got %v", gid, err)
		}
	}
	// The dataset (and the fake's plain manifest) are plain manifests:
	// dropped as records, their leaves are kept.
	_, err := _remember(ds.Global_id(), pc.pool_nick)
	assertRaises(t, py.KeyError, err)
	assertEq(t, entryData(t, mustRemember(t, sub2(t, blueprint(ds), "q"), pc.pool_nick)), "dataset leaf")
	backed_bp := blueprint(must(backed.Manifest()))
	assertEq(t, entryData(t, mustRemember(t, sub2(t, backed_bp, "value"), pc.pool_nick)), 7)
	inner := laila.As_manifest_(cget(t, sub, "inner_ds"))
	assertEq(t, entryData(t, mustRemember(t, sub2(t, blueprint(inner), "z"), pc.pool_nick)), "inner leaf")
}

func TestMissingNestedManifestRaises(t *testing.T) {
	pc := setUpPoolCase(t)
	c := complexManifest(py.Kw{"nickname": pc.nick("broken")})
	setitem(t, c, "ghost", manifest(py.Kw{"nickname": pc.nick("ghost")}).Global_id())
	memorizeGuaranteed(t, py.NewList(c), pc.pool_nick)
	_, err := _remember(c.Global_id(), pc.pool_nick)
	assertRaises(t, py.KeyError, err)
	bare := laila.As_complex_manifest_(mustRemember(t, c.Global_id(), pc.pool_nick, py.Kw{"resolve": false}))
	_, err = bare.Resolve(py.Kw{"pool": pc.pool_nick})
	assertRaises(t, py.KeyError, err)
}

func TestSelfReferenceIsACycle(t *testing.T) {
	pc := setUpPoolCase(t)
	c := complexManifest(py.Kw{"nickname": pc.nick("loop")})
	setitem(t, c, "me", c.Global_id())
	memorizeGuaranteed(t, py.NewList(c), pc.pool_nick)
	_, err := _remember(c.Global_id(), pc.pool_nick)
	if err == nil || !(errors.Is(err, laila.CyclicDependencyError) || errors.Is(err, py.KeyError) || errors.Is(err, py.RuntimeError)) {
		t.Fatalf("expected CyclicDependencyError / KeyError / RuntimeError, got %v", err)
	}
}

func TestRememberOfComplexManifestObjectFetchesLeaves(t *testing.T) {
	pc := setUpPoolCase(t)
	ev := pc.build_everything(t)
	memorizeGuaranteed(t, ev.c, pc.pool_nick)
	ref := must(laila.Remember(ev.c, py.Kw{"dst_pool": pc.pool_nick, "persist": false}))
	defer ref.(releaser).Release()
	must(ref.(waiter).Wait(_T))
	entries := must(py.Getattr(ref, "data"))
	assertEq(t, len(py.Iter(entries)), len(ev.c.Iter()))
}

// =============================================================================
// TestPersistAcrossPools -- cache-back also carries the resolved members, and
// resolution falls back to alpha.
// =============================================================================

func TestPersistCachesMembersAndAlphaReadResolves(t *testing.T) {
	if !HAVE_HDF5 {
		t.Skip("h5py is required")
	}
	tmp := must(os.MkdirTemp("", "laila_complex_"))
	orig_dirs := map[string]string{}
	for k, v := range laila.LAILA_DEFAULT_DIRECTORIES {
		orig_dirs[k] = v
	}
	laila.Set_default_directory(tmp)
	tag := py.Uuid4().Hex()[:8]
	memory := _memory()
	pool_nick := "h5_" + tag
	pool := must(laila.New_HDF5Pool(py.Kw{"nickname": pool_nick, "file_path": filepath.Join(tmp, "p.h5")}))
	must(0, memory.Extend(pool, py.Kw{"pool_nickname": pool_nick}))
	t.Cleanup(func() {
		router := memory.Pool_router.(*laila.LAILA_IDENTIFIABLE_POOL_ROUTER_)
		_, _ = router.Pools.Pop(pool.Global_id())
		_, _ = router.Pools_nicknames.Pop(pool_nick)
		_ = pool.Close()
		for k := range laila.LAILA_DEFAULT_DIRECTORIES {
			delete(laila.LAILA_DEFAULT_DIRECTORIES, k)
		}
		for k, v := range orig_dirs {
			laila.LAILA_DEFAULT_DIRECTORIES[k] = v
		}
		_ = os.RemoveAll(tmp)
	})

	ds := manifest(py.Kw{"nickname": "ds_" + tag, "data": d("q", constant(1))})
	c := complexManifest(py.Kw{"nickname": "ev_" + tag})
	setitem(t, c, "dataset", ds)
	setitem(t, c, "config", constant("cfg"))
	memorizeGuaranteed(t, c, pool_nick)
	back := laila.As_complex_manifest_(must(_wait(must(laila.Remember(c.Global_id(), py.Kw{"dst_pool": pool_nick, "persist": true})))))
	assertIsClass(t, cget(t, back, "dataset"), laila.Manifest_class)
	router := memory.Pool_router.(*laila.LAILA_IDENTIFIABLE_POOL_ROUTER_)
	alpha := laila.As_pool_(must(router.Pools.Getitem(memory.Alpha_pool)))
	if !alpha.Resource.Contains(c.Global_id()) {
		t.Fatal("complex manifest was not cached into alpha")
	}
	if !alpha.Resource.Contains(ds.Global_id()) {
		t.Fatal("resolved member was not cached into alpha")
	}
	// Reading from alpha now resolves without touching the HDF5 pool.
	from_alpha := laila.As_complex_manifest_(must(_wait(must(laila.Remember(c.Global_id(), py.Kw{"persist": false})))))
	assertIsClass(t, cget(t, from_alpha, "dataset"), laila.Manifest_class)
	// Fresh read, no persist: resolve=False really leaves the gid.
	bare := laila.As_complex_manifest_(must(_wait(must(laila.Remember(c.Global_id(),
		py.Kw{"dst_pool": pool_nick, "persist": false, "resolve": false})))))
	assertEq(t, cget(t, bare, "dataset"), ds.Global_id())
	forgetGuaranteed(t, back, pool_nick)
}
