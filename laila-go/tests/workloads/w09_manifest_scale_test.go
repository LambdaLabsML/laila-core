//go:build workload

// Port of tests/workloads/test_w09_manifest_scale.py.
//
// Manifests at scale: tens of thousands of referenced entries, deep
// nesting, realized()/async_realized under starved taskforces, the SQL index
// on 50k-row blueprints (build, query, invalidate, persist, concurrency),
// missing references, and manifests of manifests.
package workloads

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entries_ is ``_entries(n, tag="e")``.
func entries_(n int, tag ...string) []*laila.Entry {
	tg := "e"
	if len(tag) > 0 {
		tg = tag[0]
	}
	es := make([]*laila.Entry, n)
	for i := range es {
		es[i] = must(laila.Constant(py.DictOf("tag", tg, "i", i)))
	}
	return es
}

// keyed is ``{f"{prefix}{i}": e for i, e in enumerate(es)}``.
func keyed(prefix string, es []*laila.Entry) *py.Dict {
	d := py.NewDict()
	for i, e := range es {
		d.Set(fmt.Sprintf("%s%d", prefix, i), e)
	}
	return d
}

// keyed_gids is ``{f"{prefix}{i}": e.global_id for i, e in enumerate(es)}``.
func keyed_gids(prefix string, es []*laila.Entry) *py.Dict {
	d := py.NewDict()
	for i, e := range es {
		d.Set(fmt.Sprintf("%s%d", prefix, i), e.Global_id())
	}
	return d
}

// range_list is ``list(range(n))``.
func range_list(n int) *py.List {
	l := py.NewList()
	for i := 0; i < n; i++ {
		l.Append(i)
	}
	return l
}

// manifest_of is ``Manifest(data=data)``.
func manifest_of(data any, kw ...py.Kw) *laila.Manifest {
	opts := py.MergeKw(kw...)
	opts["data"] = data
	return must(laila.New_Manifest(opts))
}

// realized is ``m.realized`` as a dict.
func realized(t *testing.T, m *laila.Manifest) *py.Dict {
	t.Helper()
	return must(m.Realized()).(*py.Dict)
}

// remembered_manifest is ``laila.remember(gid, persist=False).wait(60)``
// asserted to be a Manifest.
func remembered_manifest(t *testing.T, gid string, kw ...py.Kw) *laila.Manifest {
	t.Helper()
	opts := py.MergeKw(kw...)
	opts["persist"] = false
	back := must(Wait(must(laila.Remember(gid, opts)), 60))
	m := laila.As_manifest_(back)
	assertTrue(t, m != nil, fmt.Sprintf("remembered %s, want Manifest", py.Pytypename(back)))
	return m
}

// tiny_taskforces is the ``_TinyTaskforces`` context manager: swap the
// policy's alpha / internal taskforces for single-worker ones with the
// given async slots; ``Exit`` restores and shuts them down.
type tiny_taskforces struct {
	cmd      *laila.LAILA_IDENTIFIABLE_CENTRAL_COMMAND_
	saved    [2]any
	alpha    *laila.PythonAsyncThreadPoolTaskForce
	internal *laila.PythonAsyncThreadPoolTaskForce
}

func new_tiny_taskforces(t *testing.T, alpha_slots, internal_slots int, sync_workers ...int) *tiny_taskforces {
	t.Helper()
	sw := 2
	if len(sync_workers) > 0 {
		sw = sync_workers[0]
	}
	policy := As_policy(must(laila.Active_policy()))
	cmd := Cmd(policy)
	tt := &tiny_taskforces{cmd: cmd, saved: [2]any{cmd.Alpha_taskforce, cmd.Internal_taskforce}}
	pid := policy.Global_id()
	tt.alpha = must(laila.New_PythonAsyncThreadPoolTaskForce(py.Kw{
		"policy_id": pid, "num_workers": 1, "max_async_per_thread": alpha_slots,
		"sync_workers": sw, "rank": 2,
	}))
	tt.internal = must(laila.New_PythonAsyncThreadPoolTaskForce(py.Kw{
		"policy_id": pid, "num_workers": 1, "max_async_per_thread": internal_slots,
		"sync_workers": sw, "rank": 1,
	}))
	cmd.Taskforces.Set(tt.alpha.Global_id(), tt.alpha)
	cmd.Taskforces.Set(tt.internal.Global_id(), tt.internal)
	cmd.Alpha_taskforce = tt.alpha.Global_id()
	cmd.Internal_taskforce = tt.internal.Global_id()
	t.Cleanup(tt.Exit)
	return tt
}

// Exit is ``__exit__``; idempotent so the t.Cleanup fallback is harmless.
func (tt *tiny_taskforces) Exit() {
	if tt.cmd == nil {
		return
	}
	cmd := tt.cmd
	tt.cmd = nil
	cmd.Alpha_taskforce, cmd.Internal_taskforce = tt.saved[0], tt.saved[1]
	for _, tf := range []*laila.PythonAsyncThreadPoolTaskForce{tt.alpha, tt.internal} {
		_ = tf.Shutdown(py.Kw{"wait": true, "cancel_pending": true})
		cmd.Taskforces.PopDefault(tf.Global_id(), nil)
	}
}

// ---------------------------------------------------------------------------
// flat manifests
// ---------------------------------------------------------------------------

func TestFlatManifests_memorize_and_realize_flat(t *testing.T) {
	for _, n0 := range []int{1_000, 5_000, 15_000} {
		n0 := n0
		t.Run(fmt.Sprintf("n=%d", n0), func(t *testing.T) {
			Fresh_policy(t)
			n := N_scaled(n0, 200)
			es := entries_(n)
			m := manifest_of(keyed("item", es))
			w := Timed(func() { must(m.Memorize()).Wait(900) })
			var real *py.Dict
			r := Timed(func() { real = realized(t, m) })
			assertEq(t, real.Len(), n)
			assertEq(t, must(Unwrap(must(real.Getitem(fmt.Sprintf("item%d", n-1)))).(*py.Dict).Getitem("i")), n-1)
			assertTrue(t, w.Elapsed/float64(n) < 0.02 && r.Elapsed/float64(n) < 0.02, w.Elapsed, r.Elapsed)
		})
	}
}

func TestFlatManifests_realize_cost_scales_linearly(t *testing.T) {
	Fresh_policy(t)
	ns, nl := N_scaled(1_000, 200), N_scaled(5_000, 1_000)
	ms := manifest_of(keyed("i", entries_(ns)))
	ml := manifest_of(keyed("i", entries_(nl)))
	must(must(ms.Memorize()).Wait(600))
	must(must(ml.Memorize()).Wait(900))
	s := Timed(func() { must(ms.Realized()) })
	l := Timed(func() { must(ml.Realized()) })
	assertTrue(t, l.Elapsed <= max(s.Elapsed, 0.05)*(float64(nl)/float64(ns))*4)
}

func TestFlatManifests_list_leaves(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(5_000, 300)
	es := entries_(n)
	all := py.NewList()
	for _, e := range es {
		all.Append(e)
	}
	m := manifest_of(py.DictOf("all", all))
	must(must(m.Memorize()).Wait(600))
	real := realized(t, m)
	got := must(real.Getitem("all")).(*py.List)
	assertEq(t, got.Len(), n)
	is := py.NewList()
	for _, e := range got.Items() {
		is.Append(must(Unwrap(e).(*py.Dict).Getitem("i")))
	}
	assertEq(t, is, range_list(n))
}

func TestFlatManifests_manifest_from_gid_strings(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(3_000, 200)
	es := entries_(n)
	must(Wait(must(laila.Memorize(entries_list(es))), 300))
	m := manifest_of(keyed_gids("k", es))
	must(Wait(must(laila.Memorize(py.NewList(m))), 60))
	back := remembered_manifest(t, m.Global_id())
	assertEq(t, must(py.Len(must(back.Data()))), n)
	assertEq(t, realized(t, back).Len(), n)
}

func TestFlatManifests_remember_manifest_via_memory_then_realize(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(2_000, 100)
	m := manifest_of(keyed("k", entries_(n)))
	must(must(m.Memorize()).Wait(300))
	back := remembered_manifest(t, m.Global_id())
	assertEq(t, realized(t, back).Len(), n)
}

func TestFlatManifests_missing_reference_raises_keyerror(t *testing.T) {
	policy := Fresh_policy(t)
	es := entries_(50)
	must(Wait(must(laila.Memorize(entries_list(es))), 60))
	bp := keyed_gids("k", es)
	bp.Set("ghost", "LAILA:ENTRY:00000000-0000-4000-8000-0000000000aa")
	m := manifest_of(bp)
	var err error
	sw := Timed(func() { _, err = m.Realized() })
	if !errors.Is(err, py.KeyError) {
		t.Fatalf("want KeyError, got %v", err)
	}
	assertTrue(t, sw.Elapsed < 30)
	Quiesce(policy)
}

func TestFlatManifests_many_missing_references_fail_fast(t *testing.T) {
	policy := Fresh_policy(t)
	bp := py.NewDict()
	for i := 0; i < N_scaled(2_000, 200); i++ {
		bp.Set(fmt.Sprintf("k%d", i), fmt.Sprintf("LAILA:ENTRY:00000000-0000-4000-8000-0000000%05x", i))
	}
	m := manifest_of(bp)
	var err error
	sw := Timed(func() { _, err = m.Realized() })
	if !errors.Is(err, py.KeyError) {
		t.Fatalf("want KeyError, got %v", err)
	}
	assertTrue(t, sw.Elapsed < 120)
	Quiesce(policy)
}

func TestFlatManifests_empty_manifest(t *testing.T) {
	Fresh_policy(t)
	m := manifest_of(py.NewDict())
	must(must(m.Memorize()).Wait(30))
	assertEq(t, must(m.Realized()), py.NewDict())
}

func TestFlatManifests_realized_is_fresh_each_time(t *testing.T) {
	Fresh_policy(t)
	v := must(laila.Variable(py.Kw{"data": py.DictOf("v", 1)}))
	must(Wait(must(laila.Memorize(v)), 30))
	m := manifest_of(py.DictOf("cfg", strings.Split(v.Global_id(), "@")[0]+"@evolution=-1"))
	assertEq(t, Unwrap(must(realized(t, m).Getitem("cfg"))), py.DictOf("v", 1))
	check(t, v.Set_data(py.DictOf("v", 2)))
	must(Wait(must(laila.Memorize(v)), 30))
	assertEq(t, Unwrap(must(realized(t, m).Getitem("cfg"))), py.DictOf("v", 2))
}

// ---------------------------------------------------------------------------
// nested manifests
// ---------------------------------------------------------------------------

func TestNestedManifests_deep_nesting(t *testing.T) {
	for _, df := range [][2]int{{5, 4}, {10, 2}, {50, 1}} {
		depth, fanout := df[0], df[1]
		t.Run(fmt.Sprintf("depth=%d,fanout=%d", depth, fanout), func(t *testing.T) {
			Fresh_policy(t)
			leaf_count := 0
			var build func(d int) any
			build = func(d int) any {
				if d == 0 {
					leaf_count++
					return must(laila.Constant(py.DictOf("leaf", leaf_count)))
				}
				out := py.NewDict()
				for i := 0; i < fanout; i++ {
					out.Set(fmt.Sprintf("n%d", i), build(d-1))
				}
				return out
			}
			bp := build(depth)
			m := manifest_of(bp)
			must(must(m.Memorize()).Wait(900))
			real := must(m.Realized())
			var walk func(x any) int
			walk = func(x any) int {
				if d, ok := x.(*py.Dict); ok {
					s := 0
					for _, v := range d.Values() {
						s += walk(v)
					}
					return s
				}
				return 1
			}
			assertEq(t, walk(real), leaf_count)
		})
	}
}

func TestNestedManifests_wide_and_deep(t *testing.T) {
	Fresh_policy(t)
	width := N_scaled(200, 30)
	bp := py.NewDict()
	for g := 0; g < width; g++ {
		group := py.NewDict()
		for i := 0; i < 10; i++ {
			group.Set(fmt.Sprintf("item%d", i), must(laila.Constant(py.DictOf("g", g, "i", i))))
		}
		bp.Set(fmt.Sprintf("group%d", g), group)
	}
	m := manifest_of(bp)
	must(must(m.Memorize()).Wait(900))
	real := realized(t, m)
	assertEq(t, real.Len(), width)
	for _, v := range real.Values() {
		assertEq(t, v.(*py.Dict).Len(), 10)
	}
}

func TestNestedManifests_manifest_of_manifests(t *testing.T) {
	Fresh_policy(t)
	inner := []*laila.Manifest{}
	for g := 0; g < N_scaled(30, 5); g++ {
		mi := manifest_of(keyed("i", entries_(50, fmt.Sprintf("g%d", g))))
		must(must(mi.Memorize()).Wait(300))
		inner = append(inner, mi)
	}
	outer_bp := py.NewDict()
	for g, mi := range inner {
		outer_bp.Set(fmt.Sprintf("m%d", g), mi.Global_id())
	}
	outer := manifest_of(outer_bp)
	must(Wait(must(laila.Memorize(py.NewList(outer))), 60))
	real := realized(t, outer)
	total := 0
	for _, v := range real.Values() {
		mv := laila.As_manifest_(v)
		assertTrue(t, mv != nil, py.Pytypename(v))
		total += realized(t, mv).Len()
	}
	assertEq(t, total, len(inner)*50)
}

func TestNestedManifests_shared_entries_across_manifests(t *testing.T) {
	Fresh_policy(t)
	es := entries_(N_scaled(1_000, 100))
	must(Wait(must(laila.Memorize(entries_list(es))), 120))
	ms := []*laila.Manifest{}
	for j := 0; j < 4; j++ {
		slice := []*laila.Entry{}
		for i := j; i < len(es); i += 4 {
			slice = append(slice, es[i])
		}
		ms = append(ms, manifest_of(keyed_gids("k", slice)))
	}
	for _, m := range ms {
		must(Wait(must(laila.Memorize(py.NewList(m))), 60))
	}
	total := 0
	for _, m := range ms {
		total += realized(t, m).Len()
	}
	assertEq(t, total, len(es))
}

// ---------------------------------------------------------------------------
// starved taskforces
// ---------------------------------------------------------------------------

func TestStarvedTaskforces_realize_with_one_slot(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(300, 50)
	es := entries_(n)
	must(Wait(must(laila.Memorize(entries_list(es))), 120))
	m := manifest_of(keyed_gids("k", es))
	tt := new_tiny_taskforces(t, 1, 1)
	var real *py.Dict
	sw := Timed(func() { real = realized(t, m) })
	assertEq(t, real.Len(), n)
	tt.Exit()
	assertTrue(t, sw.Elapsed < 300)
}

func TestStarvedTaskforces_nested_realize_inside_task_with_one_slot(t *testing.T) {
	policy := Fresh_policy(t)
	es := entries_(N_scaled(100, 20))
	must(Wait(must(laila.Memorize(entries_list(es))), 60))
	m := manifest_of(keyed_gids("k", es))
	task := func(args ...any) (any, error) {
		real, err := m.Realized()
		if err != nil {
			return nil, err
		}
		return real.(*py.Dict).Len(), nil
	}
	tt := new_tiny_taskforces(t, 1, 1)
	f := must(Cmd(policy).Submit([]any{task}))
	assertEq(t, Unwrap(must(Wait(f, 300))), len(es))
	tt.Exit()
}

func TestStarvedTaskforces_memorize_manifest_with_pending_entries_one_slot(t *testing.T) {
	Fresh_policy(t)
	tt := new_tiny_taskforces(t, 1, 1)
	m := manifest_of(keyed("k", entries_(N_scaled(300, 50))))
	must(must(m.Memorize()).Wait(600))
	assertEq(t, realized(t, m).Len(), N_scaled(300, 50))
	tt.Exit()
}

func TestStarvedTaskforces_async_realized_from_many_coroutines(t *testing.T) {
	Fresh_policy(t)
	es := entries_(N_scaled(300, 50))
	must(Wait(must(laila.Memorize(entries_list(es))), 120))
	m := manifest_of(keyed_gids("k", es))
	many := func() (any, error) {
		coros := make([]func() (any, error), 8)
		for i := range coros {
			coros[i] = m.Async_realized()
		}
		outs, err := py.Gather(coros)
		if err != nil {
			return nil, err
		}
		lens := py.NewList()
		for _, o := range outs {
			lens.Append(o.(*py.Dict).Len())
		}
		return lens, nil
	}
	want := py.NewList()
	for i := 0; i < 8; i++ {
		want.Append(len(es))
	}
	assertEq(t, must(Run(many)), want)
}

func TestStarvedTaskforces_concurrent_realize_threads(t *testing.T) {
	Fresh_policy(t)
	es := entries_(N_scaled(500, 50))
	must(Wait(must(laila.Memorize(entries_list(es))), 120))
	m := manifest_of(keyed_gids("k", es))
	counts := must(Run_threads(func(i int) (any, error) {
		real, err := m.Realized()
		if err != nil {
			return nil, err
		}
		return real.(*py.Dict).Len(), nil
	}, 8))
	for _, c := range counts {
		assertEq(t, c, len(es))
	}
}

// ---------------------------------------------------------------------------
// SQL index
// ---------------------------------------------------------------------------

// sql_bp_ is ``TestSqlIndex._bp(n)``.
func sql_bp_(n int) *py.Dict {
	owners := []string{"alice", "bob", "carol", "dave"}
	bp := py.NewDict()
	for i := 0; i < n; i++ {
		bp.Set(fmt.Sprintf("row%d", i), py.DictOf(
			"owner", owners[i%4],
			"size", fmt.Sprintf("%06d", i),
			"team", fmt.Sprintf("t%d", i%10),
			"ref", fmt.Sprintf("LAILA:ENTRY:00000000-0000-4000-8000-0000000%05x", i),
		))
	}
	return bp
}

// sql_rows is ``m.sql(q).data`` as a dict.
func sql_rows(t *testing.T, m *laila.Manifest, q string) *py.Dict {
	t.Helper()
	sub := laila.As_manifest_(must(m.Sql(q)))
	assertTrue(t, sub != nil)
	d := must(sub.Data())
	if py.IsNone(d) {
		return py.NewDict()
	}
	return d.(*py.Dict)
}

func TestSqlIndex_build_and_query_large(t *testing.T) {
	for _, n0 := range []int{5_000, 50_000} {
		n0 := n0
		t.Run(fmt.Sprintf("n=%d", n0), func(t *testing.T) {
			Fresh_policy(t)
			n := N_scaled(n0, 500)
			m := manifest_of(sql_bp_(n))
			b := Timed(func() { check(t, m.Build_index(py.Kw{"on": py.NewList("owner", "size")})) })
			var sub *py.Dict
			q := Timed(func() { sub = sql_rows(t, m, "SELECT * FROM rows WHERE owner = 'alice'") })
			assertEq(t, sub.Len(), (n+3)/4)
			sub2 := sql_rows(t, m, "SELECT * FROM rows WHERE size > '000100' AND owner = 'bob'")
			for _, r := range sub2.Values() {
				row := r.(*py.Dict)
				assertTrue(t, py.Str(must(row.Getitem("size"))) > "000100" && py.Str(must(row.Getitem("owner"))) == "bob")
			}
			assertTrue(t, b.Elapsed < 120 && q.Elapsed < 10, b.Elapsed, q.Elapsed)
		})
	}
}

func TestSqlIndex_repeated_queries_reuse_index(t *testing.T) {
	Fresh_policy(t)
	m := manifest_of(sql_bp_(N_scaled(20_000, 1_000)))
	must(m.Sql("SELECT * FROM r WHERE owner = 'alice'"))
	sw := Timed(func() {
		for i := 0; i < 50; i++ {
			must(m.Sql(fmt.Sprintf("SELECT * FROM r WHERE team = 't%d'", i%10)))
		}
	})
	assertTrue(t, sw.Elapsed < 60)
}

func TestSqlIndex_invalidate_and_rebuild(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(10_000, 500)
	m := manifest_of(sql_bp_(n))
	assertEq(t, sql_rows(t, m, "SELECT * FROM r WHERE owner = 'alice'").Len(), (n+3)/4)
	m.Invalidate_index()
	extra := 0
	if n%4 > 1 {
		extra = 1
	}
	assertEq(t, sql_rows(t, m, "SELECT * FROM r WHERE owner = 'bob'").Len(), n/4+extra)
}

var extra_alice_row_ = func() *py.Dict {
	return py.DictOf(
		"owner", "alice",
		"size", "-1",
		"team", "tz",
		"ref", "LAILA:ENTRY:00000000-0000-4000-8000-00000000ffff",
	)
}

// F-18 (fixed): Manifest.data setter invalidates the SQL index.
func TestSqlIndex_blueprint_reassignment_marks_stale(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(2_000, 200)
	m := manifest_of(sql_bp_(n))
	assertEq(t, sql_rows(t, m, "SELECT * FROM r WHERE owner = 'alice'").Len(), (n+3)/4)
	bp := must(m.Data()).(*py.Dict).Copy()
	bp.Set("extra", extra_alice_row_())
	check(t, m.Set_data(bp))
	assertEq(t, sql_rows(t, m, "SELECT * FROM r WHERE owner = 'alice'").Len(), (n+3)/4+1)
}

func TestSqlIndex_extend_marks_stale(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(2_000, 200)
	m := manifest_of(sql_bp_(n))
	assertEq(t, sql_rows(t, m, "SELECT * FROM r WHERE owner = 'alice'").Len(), (n+3)/4)
	must(m.Iadd(manifest_of(py.DictOf("extra", extra_alice_row_()))))
	assertEq(t, sql_rows(t, m, "SELECT * FROM r WHERE owner = 'alice'").Len(), (n+3)/4+1)
}

func TestSqlIndex_extend_many_times_rebuild_cost(t *testing.T) {
	Fresh_policy(t)
	m := manifest_of(sql_bp_(N_scaled(2_000, 200)))
	must(m.Sql("SELECT * FROM r WHERE owner = 'alice'"))
	sw := Timed(func() {
		for i := 0; i < N_scaled(30, 5); i++ {
			must(m.Iadd(manifest_of(py.DictOf(fmt.Sprintf("x%d", i), py.DictOf(
				"owner", "zed",
				"size", fmt.Sprintf("%06d", i),
				"team", "tx",
				"ref", fmt.Sprintf("LAILA:ENTRY:00000000-0000-4000-8000-0000000%05x", i+70000),
			)))))
			assertEq(t, sql_rows(t, m, "SELECT * FROM r WHERE owner = 'zed'").Len(), i+1)
		}
	})
	assertTrue(t, sw.Elapsed < 120)
}

func TestSqlIndex_persisted_index_reattach(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(10_000, 500)
	bp := sql_bp_(n)
	path := filepath.Join(Disk_tmp(t), "idx.sqlite")
	m := manifest_of(bp, py.Kw{"uuid": "11111111-1111-4111-8111-111111111111"})
	check(t, m.Build_index(py.Kw{"on": py.NewList("owner"), "persist": path}))
	must(m.Sql("SELECT * FROM r WHERE owner = 'alice'"))
	check(t, m.Clear_index())
	m2 := manifest_of(bp, py.Kw{"uuid": "11111111-1111-4111-8111-111111111111"})
	var out *py.Dict
	sw := Timed(func() {
		check(t, m2.Build_index(py.Kw{"on": py.NewList("owner"), "persist": path}))
		out = sql_rows(t, m2, "SELECT * FROM r WHERE owner = 'carol'")
	})
	extra := 0
	if n%4 > 2 {
		extra = 1
	}
	assertEq(t, out.Len(), n/4+extra)
	assertTrue(t, sw.Elapsed < 60)
}

func TestSqlIndex_unsupported_sql_rejected_promptly(t *testing.T) {
	Fresh_policy(t)
	m := manifest_of(sql_bp_(100))
	for _, q := range []string{
		"SELECT * FROM r ORDER BY size",
		"SELECT * FROM r LIMIT 5",
		"DELETE FROM r",
		"SELECT * FROM r WHERE owner = alice",
	} {
		if _, err := m.Sql(q); !errors.Is(err, py.ValueError) {
			t.Fatalf("%q: want ValueError, got %v", q, err)
		}
	}
}

func TestSqlIndex_concurrent_sql_queries_threads(t *testing.T) {
	Fresh_policy(t)
	m := manifest_of(sql_bp_(N_scaled(10_000, 500)))
	owners := []string{"alice", "bob", "carol", "dave"}
	worker := func(tt int) (any, error) {
		out := 0
		for i := 0; i < 20; i++ {
			sub, err := m.Sql(fmt.Sprintf("SELECT * FROM r WHERE owner = '%s'", owners[(tt+i)%4]))
			if err != nil {
				return nil, err
			}
			out += laila.As_manifest_(sub).Len()
		}
		return out, nil
	}
	totals := must(Run_threads(worker, 8))
	for _, tot := range totals {
		assertTrue(t, tot.(int) > 0)
	}
}

func TestSqlIndex_invalidate_while_querying(t *testing.T) {
	Fresh_policy(t)
	m := manifest_of(sql_bp_(N_scaled(5_000, 500)))
	stop := py.NewEvent()
	var mu sync.Mutex
	errs := []string{}
	querier := func(tt int) {
		for !stop.Is_set() {
			if _, err := m.Sql("SELECT * FROM r WHERE owner = 'alice'"); err != nil {
				mu.Lock()
				errs = append(errs, py.Repr(err))
				mu.Unlock()
			}
		}
	}
	invalidator := func(tt int) {
		for i := 0; i < 20; i++ {
			m.Invalidate_index()
			if err := m.Build_index(); err != nil {
				mu.Lock()
				errs = append(errs, py.Repr(err))
				mu.Unlock()
			}
		}
		stop.Set()
	}
	must(Run_threads(func(tt int) (any, error) {
		if tt == 0 {
			invalidator(tt)
		} else {
			querier(tt)
		}
		return nil, nil
	}, 4))
	assertTrue(t, len(errs) == 0, errs)
}

func TestSqlIndex_index_dir_cleanup_on_clear(t *testing.T) {
	Fresh_policy(t)
	m := manifest_of(sql_bp_(500))
	must(m.Sql("SELECT * FROM r WHERE owner = 'alice'"))
	check(t, m.Clear_index(py.Kw{"remove_persisted": true}))
	must(m.Sql("SELECT * FROM r WHERE owner = 'alice'")) // rebuilds
}

func TestSqlIndex_sql_rows_with_non_primitive_rejected(t *testing.T) {
	Fresh_policy(t)
	es := entries_(5)
	must(Wait(must(laila.Memorize(entries_list(es))), 30))
	refs := py.NewList()
	for _, e := range es {
		refs.Append(e.Global_id())
	}
	m := manifest_of(py.DictOf("a", py.DictOf("ref", refs)))
	if err := m.Build_index(); !(errors.Is(err, py.TypeError) || errors.Is(err, py.ValueError)) {
		t.Fatalf("want TypeError/ValueError, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// manifests on backends
// ---------------------------------------------------------------------------

func TestManifestOnBackends_manifest_on_sqlite_pool(t *testing.T) {
	policy := Fresh_policy(t)
	raw := must(Make_pool("sqlite", Disk_tmp(t)))
	p := laila.As_pool_(raw)
	mem_extend(t, policy, raw, "p")
	defer func() {
		Quiesce(policy)
		Close_pool(raw)
	}()
	n := N_scaled(600, 100)
	m := manifest_of(keyed("k", entries_(n)))
	must(must(m.Memorize(py.Kw{"pool_nickname": "p"})).Wait(900))
	back := remembered_manifest(t, m.Global_id(), py.Kw{"pool_nickname": "p"})
	real := must(back.Remember(py.Kw{"pool_nickname": "p"}))
	if real != nil {
		must(real.Wait(600))
	}
	assertTrue(t, must(p.Exists(m.Global_id())))
}

func TestManifestOnBackends_forget_manifest_and_children(t *testing.T) {
	Fresh_policy(t)
	n := N_scaled(500, 50)
	es := entries_(n)
	m := manifest_of(keyed("k", es))
	must(must(m.Memorize()).Wait(300))
	f := must(m.Forget())
	must(f.Wait(300))
	assertTrue(t, !must(alpha_pool(t).Exists(m.Global_id())))
	for _, e := range es {
		assertTrue(t, !must(alpha_pool(t).Exists(e.Global_id())))
	}
}
