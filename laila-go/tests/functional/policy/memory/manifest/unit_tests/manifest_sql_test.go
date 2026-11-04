// Port of tests/functional/policy/memory/manifest/unit_tests/test_manifest_sql.py.
//
// Tests for Manifest.Sql() -- the non-memorizing algorithmic SQL index.
//
// The SQL index is a purely local, temporary, query-side helper that the
// manifest owns directly (it bypasses central.memory entirely), so these
// tests need no active policy / pool wiring. Each test runs against an
// isolated temp laila root so index files never touch the real
// ``~/.laila``.
package unit_tests

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// DoubleManifest_ is ``class _DoubleManifest(Manifest)``: a subclass
// emitting two rows per top-level key (nested shape).
type DoubleManifest_ struct {
	laila.Manifest
}

var DoubleManifest_class = py.NewClass("_DoubleManifest", (*DoubleManifest_)(nil), laila.Manifest_class)

func newDoubleManifest(kw ...py.Kw) *DoubleManifest_ {
	m := DoubleManifest_class.New().(*DoubleManifest_)
	must(0, m.Init_(py.MergeKw(kw...)))
	return m
}

func (m *DoubleManifest_) Sql_rows_() ([]laila.Sql_row_, *py.Set, error) {
	bp := blueprint(&m.Manifest)
	if bp == nil {
		bp = py.NewDict()
	}
	rows := []laila.Sql_row_{}
	for _, it := range bp.Items() {
		var owner any
		if dd, ok := it.Value.(*py.Dict); ok {
			owner = dd.GetDefault("owner", nil)
		}
		rows = append(rows, laila.Sql_row_{Key: py.Tuple{it.Key, 0}, Row: py.DictOf("owner", owner, "half", 0)})
		rows = append(rows, laila.Sql_row_{Key: py.Tuple{it.Key, 1}, Row: py.DictOf("owner", owner, "half", 1)})
	}
	return rows, py.NewSet("owner", "half"), nil
}

func (m *DoubleManifest_) Sql_project_(matched_keys []any, select_items []string) (any, error) {
	bp := blueprint(&m.Manifest)
	if bp == nil {
		bp = py.NewDict()
	}
	top := []any{}
	seen := py.NewSet()
	for _, row_key := range matched_keys {
		k := row_key.(py.Tuple)[0]
		if !seen.Contains(k) {
			seen.Add(k)
			top = append(top, k)
		}
	}
	subset := py.NewDict()
	for _, k := range top {
		if v, ok := bp.Get(k); ok {
			subset.Set(k, py.Deepcopy(v))
		}
	}
	return laila.New_manifest_of_(m.Class_(), py.Kw{"blueprint": subset})
}

// EntryRowManifest_ is ``class _EntryRowManifest(Manifest)``: a subclass
// whose _sql_rows accidentally yields an Entry in a row dict.
type EntryRowManifest_ struct {
	laila.Manifest
}

var EntryRowManifest_class = py.NewClass("_EntryRowManifest", (*EntryRowManifest_)(nil), laila.Manifest_class)

func newEntryRowManifest(kw ...py.Kw) *EntryRowManifest_ {
	m := EntryRowManifest_class.New().(*EntryRowManifest_)
	must(0, m.Init_(py.MergeKw(kw...)))
	return m
}

func (m *EntryRowManifest_) Sql_rows_() ([]laila.Sql_row_, *py.Set, error) {
	return []laila.Sql_row_{{Key: "k", Row: py.DictOf("bad", constant("x"))}}, py.NewSet("bad"), nil
}

// sqlBase is ``_SqlBase.setUp`` / ``tearDown``: an isolated laila root.
type sqlBase struct {
	tmp_root string
}

func setUpSql(t *testing.T) *sqlBase {
	t.Helper()
	orig_dirs := map[string]string{}
	for k, v := range laila.LAILA_DEFAULT_DIRECTORIES {
		orig_dirs[k] = v
	}
	b := &sqlBase{tmp_root: must(os.MkdirTemp("", "laila_sql_test_"))}
	laila.Set_default_directory(b.tmp_root)
	t.Cleanup(func() {
		for k := range laila.LAILA_DEFAULT_DIRECTORIES {
			delete(laila.LAILA_DEFAULT_DIRECTORIES, k)
		}
		for k, v := range orig_dirs {
			laila.LAILA_DEFAULT_DIRECTORIES[k] = v
		}
		_ = os.RemoveAll(b.tmp_root)
	})
	return b
}

func sqlKeys(t *testing.T, result any) []any {
	t.Helper()
	m := laila.As_manifest_(result)
	if m == nil {
		t.Fatalf("%T is not a Manifest", result)
	}
	return must(m.Keys())
}

func exists_(path string) bool { return py.Exists(path) }

// =============================================================================
// TestManifestSqlBasics
// =============================================================================

func setUpSqlBasics(t *testing.T) *laila.Manifest {
	t.Helper()
	setUpSql(t)
	return manifest(py.Kw{"data": d(
		"a", d("owner", "alice"),
		"b", d("owner", "bob"),
		"c", d("owner", "alice"),
	)})
}

func TestSelectStarWithWhere(t *testing.T) {
	m := setUpSqlBasics(t)
	result := must(m.Sql("SELECT * FROM ds WHERE owner = 'alice'"))
	if !py.Isinstance(result, laila.Manifest_class) {
		t.Fatalf("%T", result)
	}
	assertSetEq(t, sqlKeys(t, result), "a", "c")
}

func TestScalarFilterSingle(t *testing.T) {
	m := setUpSqlBasics(t)
	result := must(m.Sql("SELECT * FROM ds WHERE owner = 'bob'"))
	assertSetEq(t, sqlKeys(t, result), "b")
}

func TestEmptyMatchReturnsEmptySameType(t *testing.T) {
	m := setUpSqlBasics(t)
	result := must(m.Sql("SELECT * FROM ds WHERE owner = 'nobody'"))
	if !py.Isinstance(result, laila.Manifest_class) {
		t.Fatalf("%T", result)
	}
	assertEq(t, laila.As_manifest_(result).Len(), 0)
}

func TestNoWhereIsIdentityCopyFreshId(t *testing.T) {
	m := setUpSqlBasics(t)
	result := must(m.Sql("SELECT * FROM ds"))
	assertSetEq(t, sqlKeys(t, result), "a", "b", "c")
	assertNe(t, laila.As_manifest_(result).Global_id(), m.Global_id())
}

func TestMultiItemSelectIgnoresExtras(t *testing.T) {
	m := setUpSqlBasics(t)
	result := must(m.Sql("SELECT a, b FROM ds WHERE owner = 'alice'"))
	assertSetEq(t, sqlKeys(t, result), "a", "c")
}

func TestDoubleEqualsNormalized(t *testing.T) {
	m := setUpSqlBasics(t)
	result := must(m.Sql("SELECT * FROM ds WHERE owner == 'bob'"))
	assertSetEq(t, sqlKeys(t, result), "b")
}

func TestAliasPrefixedSelectItem(t *testing.T) {
	m := setUpSqlBasics(t)
	result := must(m.Sql("SELECT ds.owner FROM ds WHERE owner = 'bob'"))
	assertSetEq(t, sqlKeys(t, result), "b")
}

func TestReturnsFreshInstanceNotMutating(t *testing.T) {
	m := setUpSqlBasics(t)
	before := blueprint(m).Copy()
	must(m.Sql("SELECT * FROM ds WHERE owner = 'alice'"))
	assertEq(t, blueprint(m), before)
}

// =============================================================================
// TestManifestSqlErrors
// =============================================================================

func setUpSqlErrors(t *testing.T) *laila.Manifest {
	t.Helper()
	setUpSql(t)
	return manifest(py.Kw{"data": d("a", d("owner", "alice"))})
}

func TestUnquotedRhsBarewordRaisesHelpfulValueerror(t *testing.T) {
	m := setUpSqlErrors(t)
	_, err := m.Sql("SELECT * FROM ds WHERE owner = alice")
	assertRaises(t, py.ValueError, err)
	if !strings.Contains(strings.ToLower(err.Error()), "quote") {
		t.Fatal(err)
	}
}

func TestRejectOrderBy(t *testing.T) {
	m := setUpSqlErrors(t)
	_, err := m.Sql("SELECT * FROM ds ORDER BY owner")
	assertRaises(t, py.ValueError, err)
}

func TestRejectLimit(t *testing.T) {
	m := setUpSqlErrors(t)
	_, err := m.Sql("SELECT * FROM ds LIMIT 5")
	assertRaises(t, py.ValueError, err)
}

func TestRejectGroupBy(t *testing.T) {
	m := setUpSqlErrors(t)
	_, err := m.Sql("SELECT * FROM ds GROUP BY owner")
	assertRaises(t, py.ValueError, err)
}

func TestRejectHaving(t *testing.T) {
	m := setUpSqlErrors(t)
	_, err := m.Sql("SELECT * FROM ds HAVING owner = 'alice'")
	assertRaises(t, py.ValueError, err)
}

func TestRejectJoin(t *testing.T) {
	m := setUpSqlErrors(t)
	_, err := m.Sql("SELECT * FROM ds JOIN other ON ds.x = other.x")
	assertRaises(t, py.ValueError, err)
}

func TestRejectUnion(t *testing.T) {
	m := setUpSqlErrors(t)
	_, err := m.Sql("SELECT * FROM ds UNION SELECT * FROM other")
	assertRaises(t, py.ValueError, err)
}

func TestNonStringQueryRaises(t *testing.T) {
	m := setUpSqlErrors(t)
	_, err := m.Sql(123)
	assertRaises(t, py.ValueError, err)
}

func TestMalformedQueryRaises(t *testing.T) {
	m := setUpSqlErrors(t)
	_, err := m.Sql("DELETE FROM ds")
	assertRaises(t, py.ValueError, err)
}

// =============================================================================
// TestManifestSqlHeterogeneous
// =============================================================================

func setUpSqlHeterogeneous(t *testing.T) *laila.Manifest {
	t.Helper()
	setUpSql(t)
	return manifest(py.Kw{"data": d(
		"a", d("owner", "alice", "team", "x"),
		"b", d("owner", "bob"),
	)})
}

func TestMissingColumnIsNullAndQueryable(t *testing.T) {
	m := setUpSqlHeterogeneous(t)
	result := must(m.Sql("SELECT * FROM ds WHERE team = 'x'"))
	assertSetEq(t, sqlKeys(t, result), "a")
}

func TestIsNullFilter(t *testing.T) {
	m := setUpSqlHeterogeneous(t)
	result := must(m.Sql("SELECT * FROM ds WHERE team IS NULL"))
	assertSetEq(t, sqlKeys(t, result), "b")
}

// =============================================================================
// TestManifestSqlIndexLifecycle
// =============================================================================

func setUpSqlLifecycle(t *testing.T) *laila.Manifest {
	t.Helper()
	setUpSql(t)
	return manifest(py.Kw{"data": d(
		"a", d("owner", "alice"),
		"b", d("owner", "bob"),
	)})
}

func TestBuildIndexOnCreatesIndex(t *testing.T) {
	m := setUpSqlLifecycle(t)
	must(0, m.Build_index(py.Kw{"on": py.NewList("owner")}))
	if !m.Sql_state_.Indexed.Contains("owner") {
		t.Fatal(py.Repr(m.Sql_state_.Indexed))
	}
	index_names := must(laila.Manifest_existing_indexes_(m.Sql_state_.Conn))
	found := false
	for _, name := range index_names.Items() {
		if strings.Contains(py.Str(name), "owner") {
			found = true
		}
	}
	if !found {
		t.Fatal(py.Repr(index_names))
	}
}

func TestDefaultIndexPathUnderIndicesDir(t *testing.T) {
	m := setUpSqlLifecycle(t)
	must(0, m.Build_index())
	if !strings.HasPrefix(m.Sql_state_.Db_path, laila.LAILA_DEFAULT_DIRECTORIES["indices"]) {
		t.Fatal(m.Sql_state_.Db_path)
	}
}

func TestBuildIndexIdempotent(t *testing.T) {
	m := setUpSqlLifecycle(t)
	must(0, m.Build_index())
	state1 := m.Sql_state_
	must(0, m.Build_index())
	assertIs(t, m.Sql_state_, state1)
}

func TestMutationMarksStaleAndRebuilds(t *testing.T) {
	m := setUpSqlLifecycle(t)
	must(m.Sql("SELECT * FROM ds WHERE owner = 'alice'"))
	assertEq(t, m.Sql_state_.Stale, false)

	must(0, m.Extend(manifest(py.Kw{"data": d("c", d("owner", "alice"))})))
	assertEq(t, m.Sql_state_.Stale, true)

	result := must(m.Sql("SELECT * FROM ds WHERE owner = 'alice'"))
	assertSetEq(t, sqlKeys(t, result), "a", "c")
	assertEq(t, m.Sql_state_.Stale, false)
}

func TestInvalidateThenSqlRebuilds(t *testing.T) {
	m := setUpSqlLifecycle(t)
	must(m.Sql("SELECT * FROM ds"))
	m.Invalidate_index()
	assertEq(t, m.Sql_state_.Stale, true)
	must(m.Sql("SELECT * FROM ds"))
	assertEq(t, m.Sql_state_.Stale, false)
}

func TestClearIndexResetsState(t *testing.T) {
	m := setUpSqlLifecycle(t)
	must(0, m.Build_index())
	must(0, m.Clear_index())
	if m.Sql_state_ != nil {
		t.Fatal("_sql_state should be None")
	}
	// rebuilds fine afterwards
	result := must(m.Sql("SELECT * FROM ds WHERE owner = 'bob'"))
	assertSetEq(t, sqlKeys(t, result), "b")
}

// =============================================================================
// TestManifestSqlPersistence
// =============================================================================

func TestPersistRoundtripAttachesWithoutRebuild(t *testing.T) {
	b := setUpSql(t)
	path := filepath.Join(b.tmp_root, "explicit", "idx.laila_sqlitedb")
	bp := d("a", d("owner", "alice"), "b", d("owner", "bob"))

	m1 := manifest(py.Kw{"data": py.Deepcopy(bp)})
	must(0, m1.Build_index(py.Kw{"on": py.NewList("owner"), "persist": path}))
	must(0, m1.Clear_index()) // close connection, keep file

	if !exists_(path) {
		t.Fatal("persisted index missing")
	}

	m2 := manifest(py.Kw{"data": py.Deepcopy(bp)})
	must(0, m2.Build_index(py.Kw{"persist": path})) // no `on=` -> if it attaches, the
	// previously-created "owner" index is still present.
	found := false
	for _, n := range m2.Sql_state_.Indexed.Items() {
		if strings.Contains(py.Str(n), "owner") {
			found = true
		}
	}
	if !found {
		t.Fatal(py.Repr(m2.Sql_state_.Indexed))
	}

	result := must(m2.Sql("SELECT * FROM ds WHERE owner = 'alice'"))
	assertSetEq(t, sqlKeys(t, result), "a")
}

func TestClearIndexRemovePersistedUnlinksAndRebuilds(t *testing.T) {
	b := setUpSql(t)
	path := filepath.Join(b.tmp_root, "explicit2", "idx.laila_sqlitedb")
	m := manifest(py.Kw{"data": d("a", d("owner", "alice"))})
	must(0, m.Build_index(py.Kw{"persist": path}))
	if !exists_(path) {
		t.Fatal("persisted index missing")
	}

	must(0, m.Clear_index(py.Kw{"remove_persisted": true}))
	if exists_(path) {
		t.Fatal("persisted index should be unlinked")
	}

	result := must(m.Sql("SELECT * FROM ds WHERE owner = 'alice'"))
	assertSetEq(t, sqlKeys(t, result), "a")
}

// =============================================================================
// TestManifestSqlGarbageCollection
// =============================================================================

// gcCollect is ``gc.collect()``: run the collector and give the cleanup
// goroutine a chance to run the manifest's finalizer.
func gcCollect() {
	for i := 0; i < 3; i++ {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
}

// waitGone polls for the finalizer to remove *path* (Go cleanups run
// asynchronously after the collection, unlike CPython's synchronous
// ``gc.collect()``).
func waitGone(path string) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !exists_(path) {
			return true
		}
		gcCollect()
	}
	return !exists_(path)
}

// buildTemporaryIndex builds a manifest + temporary index in its own
// frame so no stack slot keeps the manifest reachable after return.
func buildTemporaryIndex(t *testing.T, via_sql bool) string {
	t.Helper()
	m := manifest(py.Kw{"data": d("a", d("owner", "alice"))})
	if via_sql {
		must(m.Sql("SELECT * FROM ds WHERE owner = 'alice'"))
	} else {
		must(0, m.Build_index())
	}
	path := m.Sql_state_.Db_path
	if !exists_(path) {
		t.Fatal("index file missing")
	}
	return path
}

func TestGcNukesTemporaryIndexFile(t *testing.T) {
	setUpSql(t)
	path := buildTemporaryIndex(t, false)

	gcCollect() // del m; gc.collect()
	if !waitGone(path) {
		t.Fatal("temporary index file should be removed when the manifest is GC'd")
	}
}

func TestGcNukesTemporaryIndexBuiltViaSql(t *testing.T) {
	setUpSql(t)
	path := buildTemporaryIndex(t, true)

	gcCollect()
	if !waitGone(path) {
		t.Fatal("temporary index file should be removed when the manifest is GC'd")
	}
}

func TestPersistentIndexSurvivesGc(t *testing.T) {
	b := setUpSql(t)
	path := filepath.Join(b.tmp_root, "keep", "idx.laila_sqlitedb")
	func() {
		m := manifest(py.Kw{"data": d("a", d("owner", "alice"))})
		must(0, m.Build_index(py.Kw{"persist": path}))
		if !exists_(path) {
			t.Fatal("persisted index missing")
		}
	}()

	gcCollect()
	time.Sleep(100 * time.Millisecond)
	if !exists_(path) {
		t.Fatal("a user-chosen persist= file must NOT be nuked on GC")
	}
}

func TestClearIndexRemovesTemporaryFile(t *testing.T) {
	setUpSql(t)
	m := manifest(py.Kw{"data": d("a", d("owner", "alice"))})
	must(0, m.Build_index())
	path := m.Sql_state_.Db_path
	if !exists_(path) {
		t.Fatal("index file missing")
	}

	must(0, m.Clear_index())
	if exists_(path) {
		t.Fatal("temporary index file should be removed")
	}
}

func TestClearIndexKeepsPersistentFileByDefault(t *testing.T) {
	b := setUpSql(t)
	path := filepath.Join(b.tmp_root, "keep2", "idx.laila_sqlitedb")
	m := manifest(py.Kw{"data": d("a", d("owner", "alice"))})
	must(0, m.Build_index(py.Kw{"persist": path}))
	must(0, m.Clear_index()) // remove_persisted defaults to False
	if !exists_(path) {
		t.Fatal("persisted index should be kept")
	}
}

// =============================================================================
// TestManifestSqlSubclassHooks
// =============================================================================

func TestDoubleManifestTwoRowsPerKey(t *testing.T) {
	setUpSql(t)
	m := newDoubleManifest(py.Kw{"data": d("a", d("owner", "alice"), "b", d("owner", "bob"))})
	result := must(m.Sql("SELECT * FROM ds WHERE half = 0 AND owner = 'alice'"))
	if _, ok := result.(*DoubleManifest_); !ok {
		t.Fatalf("%T is not a _DoubleManifest", result)
	}
	assertSetEq(t, sqlKeys(t, result), "a")
}

func TestDoubleManifestDedupesTopLevelKeys(t *testing.T) {
	setUpSql(t)
	m := newDoubleManifest(py.Kw{"data": d("a", d("owner", "alice"))})
	result := must(m.Sql("SELECT * FROM ds WHERE owner = 'alice'"))
	// both rows for "a" match, but project keeps a single top-level key
	assertSetEq(t, sqlKeys(t, result), "a")
}

// =============================================================================
// TestManifestSqlPrimitiveEnforcement
// =============================================================================

func TestEntryInRowRaisesTypeerrorNoState(t *testing.T) {
	setUpSql(t)
	m := newEntryRowManifest(py.Kw{"data": d("k", _make_gid())})
	err := m.Build_index()
	assertRaises(t, py.TypeError, err)
	if !strings.Contains(err.Error(), "global_id") {
		t.Fatal(err)
	}
	if m.Sql_state_ != nil {
		t.Fatal("_sql_state should be None")
	}
}

func TestListInRowRaisesTypeerrorWithFlattenGuidance(t *testing.T) {
	setUpSql(t)
	m := manifest(py.Kw{"data": d("a", d("tags", py.NewList(_make_gid(), _make_gid())))})
	err := m.Build_index()
	assertRaises(t, py.TypeError, err)
	if !strings.Contains(err.Error(), "latten") { // "flatten"/"Flatten"
		t.Fatal(err)
	}
	if m.Sql_state_ != nil {
		t.Fatal("_sql_state should be None")
	}
}

func TestGidStringValuesPass(t *testing.T) {
	setUpSql(t)
	gid := _make_gid()
	m := manifest(py.Kw{"data": d("a", gid, "b", _make_gid())})
	result := must(m.Sql("SELECT * FROM ds WHERE value = '" + gid + "'"))
	assertSetEq(t, sqlKeys(t, result), "a")
}

// =============================================================================
// TestManifestSqlMemoryUntouched -- ``inspect.signature(...)`` stand-in:
// the port's verbs take ``**kw``, so the keyword surface is the set of
// option names the method body reads. Parse the source and collect them.
// =============================================================================

func manifestMethodKeywords(t *testing.T, method string) map[string]bool {
	t.Helper()
	_, here, _, _ := runtime.Caller(0)
	src := filepath.Join(filepath.Dir(here), "..", "..", "..", "..", "..", "..", "policy.central.memory.schema.manifest.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, src, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Name.Name != method {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Get" || len(call.Args) == 0 {
				return true
			}
			if lit, ok := call.Args[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
				params[strings.Trim(lit.Value, `"`)] = true
			}
			return true
		})
	}
	if len(params) == 0 {
		t.Fatalf("no keyword options found for Manifest.%s", method)
	}
	return params
}

func TestMemorizeHasNoIndexFlags(t *testing.T) {
	setUpSql(t)
	params := manifestMethodKeywords(t, "Memorize")
	if params["persist_index"] {
		t.Fatal("persist_index should not be a memorize() option")
	}
}

func TestRememberHasNoIndexFlags(t *testing.T) {
	setUpSql(t)
	params := manifestMethodKeywords(t, "Remember")
	if params["load_index"] {
		t.Fatal("load_index should not be a remember() option")
	}
}

func TestForgetSignatureUnchanged(t *testing.T) {
	setUpSql(t)
	params := manifestMethodKeywords(t, "Forget")
	if params["load_index"] {
		t.Fatal("load_index should not be a forget() option")
	}
	if params["persist_index"] {
		t.Fatal("persist_index should not be a forget() option")
	}
}
