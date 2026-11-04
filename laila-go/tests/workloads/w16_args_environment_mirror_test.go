//go:build workload

// Port of tests/workloads/test_w16_args_environment_mirror.py.
//
// W16 -- ``laila.args`` and the environment mirror at scale.
//
// The CLI-capable base class re-dumps the whole owning policy into
// ``laila.args.environment.policies[<gid>]`` after *every* CLI-capable
// construction. These workloads measure what that costs when a policy has
// hundreds of pools / taskforces, exercise the dump -> reload round trip on
// large graphs, file-based arg loading (TOML/JSON/.env/terminal) with
// thousands of keys, and the hooks on ``laila.args.environment``.
package workloads

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const T16_ = 120.0

// clean_args16 is the ``clean_args`` fixture: snapshot/restore
// ``laila.args`` keys other than ``environment``.
func clean_args16(t *testing.T) {
	t.Helper()
	before := map[string]any{}
	for _, k := range laila.Args.Keys() {
		if k != "environment" {
			v, _ := laila.Args.Lookup(k)
			before[k] = v
		}
	}
	t.Cleanup(func() {
		for _, k := range laila.Args.Keys() {
			if _, ok := before[k]; !ok && k != "environment" {
				func() {
					defer func() { _ = recover() }()
					laila.Args.Delete(k)
				}()
			}
		}
		for k, v := range before {
			laila.Args.Set(k, v)
		}
	})
}

// mirror16 is ``_mirror(policy)``: the policy's entry under
// ``laila.args.environment.policies`` (nil when absent).
func mirror16(policy *laila.LAILA_IDENTIFIABLE_POLICY_) *py.DotMap {
	env, ok := laila.Args.Lookup("environment")
	if !ok || py.IsNone(env) {
		return nil
	}
	envm, ok := env.(*py.DotMap)
	if !ok {
		return nil
	}
	pols, _ := envm.Lookup("policies")
	pm, ok := pols.(*py.DotMap)
	if !ok {
		return nil
	}
	m, ok := pm.Lookup(policy.Global_id())
	if !ok {
		return nil
	}
	dm, _ := m.(*py.DotMap)
	return dm
}

// dotlen16 is ``len(m.a.b.c)`` on a DotMap path.
func dotlen16(m *py.DotMap, path string) int {
	return must(py.Len(m.Get(path)))
}

// dig16 is ``d[k1][k2]...`` on nested dicts.
func dig16(d any, keys ...string) any {
	for _, k := range keys {
		d = must(py.Getitem(d, k))
	}
	return d
}

// active16 is the active policy as its base struct.
func active16() *laila.LAILA_IDENTIFIABLE_POLICY_ {
	return As_policy(must(laila.Get_active_policy()))
}

// extend16 is ``laila.memory.extend(DefaultPool(), pool_nickname=nick)``.
func extend16(t *testing.T, pool any, nick string) {
	t.Helper()
	check(t, Mem(active16()).Extend(pool, py.Kw{"pool_nickname": nick}))
}

// new_pool16 is ``DefaultPool(**kw)``.
func new_pool16(t *testing.T, kw ...py.Kw) any {
	t.Helper()
	return must(laila.New_DefaultPool(kw...))
}

// new_tf16 is ``DefaultTaskForce(policy_id=gid, num_workers=n, **kw)``.
func new_tf16(t *testing.T, gid string, num_workers int, kw ...py.Kw) *laila.PythonAsyncThreadPoolTaskForce {
	t.Helper()
	opts := py.MergeKw(kw...)
	opts["policy_id"] = gid
	opts["num_workers"] = num_workers
	return must(laila.New_DefaultTaskForce(opts)).(*laila.PythonAsyncThreadPoolTaskForce)
}

// set_environment16 is ``laila.args.environment = value`` (the load hook
// raises through the assignment).
func set_environment16(value any) (err error) {
	defer py.Recover(&err)
	laila.Args.Set("environment", value)
	return nil
}

// write16 writes *content* to ``os.path.join(dir, name)``.
func write16(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	check(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

// json16 is ``json.dump(payload, f)``.
func json16(t *testing.T, payload any) string {
	t.Helper()
	return must(py.Json_dumps(payload))
}

// arg16 is ``laila.args.<path>``.
func arg16(path string) any { return laila.Args.Get(path) }

// ---------------------------------------------------------------------------
// mirror refresh cost
// ---------------------------------------------------------------------------

func TestMirrorCost_mirror_tracks_pools_and_taskforces(t *testing.T) {
	policy := Fresh_policy(t)
	pools := make([]any, 20)
	for i := range pools {
		pools[i] = new_pool16(t)
	}
	for i, p := range pools {
		extend16(t, p, fmt.Sprintf("p%d", i))
	}
	tfs := make([]*laila.PythonAsyncThreadPoolTaskForce, 5)
	for i := range tfs {
		tfs[i] = new_tf16(t, policy.Global_id(), 1)
	}
	for _, tf := range tfs {
		Cmd(policy).Add_taskforce(tf)
	}
	laila.Refresh_args_environment_(policy)
	m := mirror16(policy)
	assertTrue(t, m != nil)
	assertTrue(t, dotlen16(m, "central.memory.pool_router.pools") >= 21)
	assertTrue(t, dotlen16(m, "central.command.taskforces") >= 6)
	for _, tf := range tfs {
		check(t, tf.Shutdown(py.Kw{"wait": true, "cancel_pending": true}))
	}
}

func TestMirrorCost_pool_construction_cost_grows_with_pool_count(t *testing.T) {
	Fresh_policy(t)

	add := func(n int) float64 {
		sw := Timed(func() {
			for i := 0; i < n; i++ {
				extend16(t, new_pool16(t), fmt.Sprintf("x%d-%d", i, n))
			}
		})
		return sw.Elapsed
	}

	small := add(N_scaled(100))
	large := add(N_scaled(400))
	check(t, Assert_subquadratic(small, large, 4, 2.5))
}

func TestMirrorCost_build_environment_cost_with_many_pools(t *testing.T) {
	policy := Fresh_policy(t)
	for i := 0; i < N_scaled(300); i++ {
		extend16(t, new_pool16(t), fmt.Sprintf("b%d", i))
	}
	var env *py.Dict
	sw := Timed(func() {
		for i := 0; i < 10; i++ {
			env = laila.Build_environment(policy)
		}
	})
	assertTrue(t, sw.Elapsed < 30, sw.Elapsed)
	assertTrue(t, must(py.Len(dig16(env, "policy", "central", "memory", "pool_router", "pools"))) >= N_scaled(300)+1)
}

func TestMirrorCost_entry_construction_does_not_touch_mirror(t *testing.T) {
	Fresh_policy(t)
	for i := 0; i < 50; i++ {
		extend16(t, new_pool16(t), fmt.Sprintf("e%d", i))
	}
	sw := Timed(func() {
		for i := 0; i < N_scaled(20000); i++ {
			must(laila.Entry_constant(i))
		}
	})
	assertTrue(t, sw.Elapsed < 20, sw.Elapsed)
}

func TestMirrorCost_future_creation_does_not_touch_mirror(t *testing.T) {
	policy := Fresh_policy(t)
	for i := 0; i < 50; i++ {
		extend16(t, new_pool16(t), fmt.Sprintf("f%d", i))
	}
	var gf *laila.GroupFuture
	sw := Timed(func() {
		tasks := make([]any, N_scaled(5000))
		for i := range tasks {
			tasks[i] = lam(1)
		}
		gf = gfut(must(Cmd(policy).Submit(tasks)))
		must(gf.Wait(T16_))
	})
	gf.Release()
	assertTrue(t, sw.Elapsed < 30, sw.Elapsed)
}

func TestMirrorCost_concurrent_construction_mirror_consistency(t *testing.T) {
	policy := Fresh_policy(t)
	w := func(i int) error {
		for j := 0; j < 20; j++ {
			p, err := laila.New_DefaultPool()
			if err != nil {
				return err
			}
			if err := Mem(policy).Extend(p, py.Kw{"pool_nickname": fmt.Sprintf("c%d-%d", i, j)}); err != nil {
				return err
			}
		}
		return nil
	}

	check(t, Go_threads(w, 8))
	laila.Refresh_args_environment_(policy)
	m := mirror16(policy)
	assertTrue(t, m != nil)
	assertTrue(t, dotlen16(m, "central.memory.pool_router.pools") >= 161)
}

func TestMirrorCost_mirror_cleared_on_terminate(t *testing.T) {
	policy := Fresh_policy(t)
	extend16(t, new_pool16(t), "t")
	assertTrue(t, mirror16(policy) != nil)
	laila.Terminate()
	env, _ := laila.Args.Lookup("environment")
	policies, _ := env.(*py.DotMap).Lookup("policies")
	assertTrue(t, !py.Truthy(policies), py.Repr(policies))
}

// ---------------------------------------------------------------------------
// dump / reload round-trip
// ---------------------------------------------------------------------------

func TestRoundTrip_reload_policy_with_many_pools_and_taskforces(t *testing.T) {
	Setup(t)
	disk_tmp := Disk_tmp(t)
	laila.Terminate()
	pol := As_policy(must(laila.New_DefaultPolicy()))
	laila.Activate_policy(pol)
	for i := 0; i < N_scaled(50); i++ {
		extend16(t, new_pool16(t), fmt.Sprintf("rp%d", i))
	}
	for i := 0; i < 5; i++ {
		Cmd(pol).Add_taskforce(new_tf16(t, pol.Global_id(), 1))
	}
	sq := must(Make_pool("sqlite", disk_tmp))
	extend16(t, sq, "sq")
	e := must(laila.Entry_constant(py.DictOf("k", 1)))
	must(Wait(must(laila.Memorize(e, py.Kw{"pool_nickname": "sq"})), T16_))
	Quiesce(pol)
	env := laila.Build_environment(pol)
	gid := pol.Global_id()
	pool_count := must(py.Len(dig16(env, "policy", "central", "memory", "pool_router", "pools")))
	tf_count := must(py.Len(dig16(env, "policy", "central", "command", "taskforces")))

	sw := Timed(func() {
		check(t, set_environment16(py.DictOf("policies", py.DictOf(gid, dig16(env, "policy")), "active_gid", gid)))
	})
	assertTrue(t, sw.Elapsed < 120, sw.Elapsed)
	new := As_policy(must(laila.Get_active_policy()))
	assertTrue(t, new != pol && new.Global_id() == gid)
	router := Router(new)
	assertEq(t, router.Pools.Len(), pool_count)
	assertEq(t, Cmd(new).Taskforces.Len(), tf_count)
	assertTrue(t, router.Pools_nicknames.Contains("sq"))
	// the sqlite pool was reopened on the same file -> data survives
	out := Unwrap(must(Wait(must(laila.Remember(e.Global_id(), py.Kw{"pool_nickname": "sq"})), T16_)))
	assertEq(t, out, py.DictOf("k", 1))
	Quiesce(new)
	laila.Terminate()
}

func TestRoundTrip_reload_preserves_uuids_of_every_component(t *testing.T) {
	Setup(t)
	laila.Terminate()
	pol := As_policy(must(laila.New_DefaultPolicy()))
	laila.Activate_policy(pol)
	pools := make([]*laila.LAILA_IDENTIFIABLE_POOL_, 30)
	for i := range pools {
		pools[i] = laila.As_pool_(new_pool16(t))
	}
	for i, p := range pools {
		extend16(t, p, fmt.Sprintf("u%d", i))
	}
	env := laila.Build_environment(pol)
	gid := pol.Global_id()
	check(t, set_environment16(py.DictOf("policies", py.DictOf(gid, dig16(env, "policy")))))
	new := As_policy(must(laila.Get_active_policy()))
	for _, p := range pools {
		assertTrue(t, Router(new).Pools.Contains(p.Global_id()), p.Global_id())
	}
	for i, p := range pools {
		assertEq(t, must(Router(new).Pools_nicknames.Getitem(fmt.Sprintf("u%d", i))), p.Global_id())
	}
	laila.Terminate()
}

func TestRoundTrip_reload_cycles_do_not_leak_threads(t *testing.T) {
	Setup(t)
	laila.Terminate()
	pol := As_policy(must(laila.New_DefaultPolicy()))
	laila.Activate_policy(pol)
	for i := 0; i < 3; i++ {
		Cmd(pol).Add_taskforce(new_tf16(t, pol.Global_id(), 2))
	}
	env := laila.Build_environment(pol)
	gid := pol.Global_id()
	// ``threading.active_count()`` is ``len(threading.enumerate())``: the
	// live py.Thread objects (taskforce loops, dispatchers).
	base := len(py.Enumerate())
	for i := 0; i < 10; i++ {
		check(t, set_environment16(py.DictOf("policies", py.DictOf(gid, dig16(env, "policy")))))
	}
	laila.Terminate()

	py.Sleep_sync(1.0)
	assertTrue(t, len(py.Enumerate()) <= base+4, len(py.Enumerate()))
}

func TestRoundTrip_reload_multiple_policies_requires_active_gid(t *testing.T) {
	Setup(t)
	laila.Terminate()
	a, b := As_policy(must(laila.New_DefaultPolicy())), As_policy(must(laila.New_DefaultPolicy()))
	laila.Activate_policy(a)
	laila.Activate_policy(b)
	env := py.DictOf(
		"policies", py.DictOf(
			a.Global_id(), dig16(laila.Build_environment(a), "policy"),
			b.Global_id(), dig16(laila.Build_environment(b), "policy"),
		),
	)
	if err := set_environment16(env); !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
	env.Set("active_gid", b.Global_id())
	check(t, set_environment16(env))
	assertEq(t, As_policy(must(laila.Get_active_policy())).Global_id(), b.Global_id())
	assertEq(t, laila.Local_policies_.Len(), 2)
	laila.Terminate()
}

func TestRoundTrip_malformed_environments(t *testing.T) {
	Setup(t)
	laila.Terminate()
	laila.Activate_policy(must(laila.New_DefaultPolicy()))
	for _, env := range []*py.Dict{
		py.DictOf("policies", py.DictOf("not-a-gid", py.NewDict())),
		py.DictOf("nope", 1),
		py.DictOf("policies", py.NewDict(), "active_gid", "LAILA:POLICY:x"),
		py.DictOf("policies", 5),
	} {
		if err := laila.Load_environment_(env); !errors.Is(err, py.ValueError) {
			t.Fatalf("%s: want ValueError, got %v", py.Repr(env), err)
		}
	}
	// after a failed load the process has no live policies but recovers lazily
	must(laila.Get_active_policy())
	laila.Terminate()
}

func TestRoundTrip_empty_policies_creates_default(t *testing.T) {
	Setup(t)
	laila.Terminate()
	check(t, set_environment16(py.NewDotMap(py.DictOf("policies", py.NewDict(), "active_gid", nil))))
	p := must(laila.Get_active_policy())
	assertTrue(t, py.Isinstance(p, laila.DefaultPolicy))
	laila.Terminate()
}

func TestRoundTrip_reload_with_duplicate_uuid_rejected(t *testing.T) {
	Setup(t)
	laila.Terminate()
	pol := As_policy(must(laila.New_DefaultPolicy()))
	laila.Activate_policy(pol)
	env := dig16(laila.Build_environment(pol), "policy")
	gid := pol.Global_id()
	// same uuid under two different gid strings (different scope suffix)
	err := laila.Load_environment_(py.DictOf("policies", py.DictOf(gid, env, strings.Replace(gid, "POLICY", "POLICY2", 1), env)))
	if !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
	laila.Terminate()
}

// ---------------------------------------------------------------------------
// args loading
// ---------------------------------------------------------------------------

func TestArgsLoading_json_with_thousands_of_keys(t *testing.T) {
	Setup(t)
	disk_tmp := Disk_tmp(t)
	clean_args16(t)
	inner := py.NewDict()
	for i := 0; i < N_scaled(5000); i++ {
		inner.Set(fmt.Sprintf("k%d", i), py.DictOf("v", i, "s", strings.Repeat("x", 50)))
	}
	payload := py.DictOf("wl16", inner)
	path := write16(t, disk_tmp, "args.json", json16(t, payload))
	sw := Timed(func() {
		check(t, laila.Read_args(path))
	})
	assertTrue(t, sw.Elapsed < 60, sw.Elapsed)
	// documented: one level of nesting is flattened with ``_``
	assertEq(t, arg16("wl16_k0.v"), 0)
	assertEq(t, arg16(fmt.Sprintf("wl16_k%d.s", N_scaled(5000)-1)), strings.Repeat("x", 50))
}

func TestArgsLoading_toml_nested(t *testing.T) {
	Setup(t)
	disk_tmp := Disk_tmp(t)
	clean_args16(t)
	lines := []string{"[wl16toml]"}
	for i := 0; i < N_scaled(2000); i++ {
		lines = append(lines, fmt.Sprintf("key%d = %d", i, i))
	}
	lines = append(lines, "[wl16toml.nested.deep]")
	lines = append(lines, `name = "deep"`)
	path := write16(t, disk_tmp, "args.toml", strings.Join(lines, "\n")+"\n")
	check(t, laila.Read_args(path))
	assertEq(t, arg16("wl16toml_key5"), 5)
	assertEq(t, arg16("wl16toml_nested.deep.name"), "deep")
}

func TestArgsLoading_file_config_drives_pool_defaults(t *testing.T) {
	Fresh_policy(t)
	disk_tmp := Disk_tmp(t)
	clean_args16(t)
	p := laila.As_pool_(new_pool16(t))
	payload := py.DictOf(
		"policy", py.DictOf("central", py.DictOf("memory", py.DictOf("pools", py.DictOf(p.Global_id(), py.DictOf("batch_accelerated", true))))),
	)
	path := write16(t, disk_tmp, "cfg.json", json16(t, payload))
	check(t, laila.Read_args(path))
	q := laila.As_pool_(new_pool16(t, py.Kw{"uuid": p.Uuid()}))
	assertEq(t, q.Batch_accelerated, true)
}

func TestArgsLoading_env_file(t *testing.T) {
	Setup(t)
	disk_tmp := Disk_tmp(t)
	clean_args16(t)
	var sb strings.Builder
	sb.WriteString("# comment\n\n")
	for i := 0; i < N_scaled(2000); i++ {
		sb.WriteString(fmt.Sprintf("WL16_ENV_%d=value%d\n", i, i))
	}
	sb.WriteString("WL16_BOOL=true\nWL16_INT=42\nWL16_JSON=[1,2]\nWL16_Q='quoted'\nbroken line\n")
	path := write16(t, disk_tmp, "vars.env", sb.String())
	check(t, laila.Read_args(path))
	assertEq(t, arg16("WL16_ENV_7"), "value7")
	assertTrue(t, arg16("WL16_BOOL") == true && py.Eq(arg16("WL16_INT"), 42))
	assertTrue(t, py.Eq(arg16("WL16_JSON"), py.NewList(1, 2)) && py.Eq(arg16("WL16_Q"), "quoted"))
}

func TestArgsLoading_dot_env_file_name(t *testing.T) {
	Setup(t)
	disk_tmp := Disk_tmp(t)
	clean_args16(t)
	path := write16(t, disk_tmp, ".env", "WL16_DOTENV=1\n")
	check(t, laila.Read_args(path))
	assertEq(t, arg16("WL16_DOTENV"), 1)
}

func TestArgsLoading_terminal_key_value_tokens(t *testing.T) {
	Setup(t)
	clean_args16(t)
	argv := []string{}
	for i := 0; i < N_scaled(1000); i++ {
		argv = append(argv, fmt.Sprintf("wl16term_k%d=%d", i, i))
	}
	argv = append(argv, "wl16term_flag=yes")
	check(t, laila.Read_args("terminal", py.Kw{"terminal_args": argv}))
	assertEq(t, arg16("wl16term_k7"), 7)
	assertEq(t, arg16("wl16term_flag"), "yes")
}

func TestArgsLoading_terminal_dashed_args(t *testing.T) {
	Setup(t)
	clean_args16(t)
	check(t, laila.Read_args("terminal", py.Kw{"terminal_args": []string{"--wl16dash", "7", "--wl16eq=8"}}))
	assertEq(t, arg16("wl16dash"), 7)
	assertEq(t, arg16("wl16eq"), 8)
}

func TestArgsLoading_merge_semantics(t *testing.T) {
	Setup(t)
	disk_tmp := Disk_tmp(t)
	clean_args16(t)
	p1 := write16(t, disk_tmp, "a.json", json16(t, py.DictOf("wl16m_a", 1, "wl16m_b", 1)))
	p2 := write16(t, disk_tmp, "b.json", json16(t, py.DictOf("wl16m_b", 2, "wl16m_c", 3)))
	check(t, laila.Read_args(p1))
	check(t, laila.Read_args(p2))
	assertTrue(t, py.Eq(arg16("wl16m_a"), 1) && py.Eq(arg16("wl16m_b"), 2) && py.Eq(arg16("wl16m_c"), 3))
}

func TestArgsLoading_plain_dict_assignment_becomes_dotmap(t *testing.T) {
	Setup(t)
	clean_args16(t)
	laila.Args.Set("wl16d", py.DictOf("x", py.DictOf("y", 1)))
	_, is_dotmap := arg16("wl16d").(*py.DotMap)
	assertTrue(t, is_dotmap)
	assertEq(t, arg16("wl16d.x.y"), 1)
	laila.Args.Set("wl16e", py.DictOf("z", 2))
	assertEq(t, arg16("wl16e.z"), 2)
}

func TestArgsLoading_args_drive_pool_defaults(t *testing.T) {
	Fresh_policy(t)
	clean_args16(t)
	laila.Args.Set("policy", py.DictOf("central", py.DictOf("memory", py.DictOf("pools", py.NewDict()))))
	p := laila.As_pool_(new_pool16(t))
	assertEq(t, p.Batch_accelerated, false)
	arg16("policy.central.memory.pools").(*py.DotMap).Set(p.Global_id(), py.NewDotMap(py.DictOf("batch_accelerated", true)))
	q := laila.As_pool_(new_pool16(t, py.Kw{"uuid": p.Uuid()}))
	assertEq(t, q.Batch_accelerated, true)
}

func TestArgsLoading_args_drive_hundreds_of_pools(t *testing.T) {
	Fresh_policy(t)
	clean_args16(t)
	laila.Args.Set("policy", py.DictOf("central", py.DictOf("memory", py.DictOf("pools", py.NewDict()))))
	pools := make([]*laila.LAILA_IDENTIFIABLE_POOL_, N_scaled(300))
	for i := range pools {
		pools[i] = laila.As_pool_(new_pool16(t))
	}
	for _, p := range pools {
		arg16("policy.central.memory.pools").(*py.DotMap).Set(p.Global_id(), py.NewDotMap(
			py.DictOf("batch_accelerated", true),
		))
	}
	var rebuilt []*laila.LAILA_IDENTIFIABLE_POOL_
	sw := Timed(func() {
		for _, p := range pools {
			rebuilt = append(rebuilt, laila.As_pool_(new_pool16(t, py.Kw{"uuid": p.Uuid()})))
		}
	})
	for _, q := range rebuilt {
		assertTrue(t, q.Batch_accelerated)
	}
	assertTrue(t, sw.Elapsed < 120, sw.Elapsed)
}

func TestArgsLoading_args_subtree_empty_dotmap_is_absent(t *testing.T) {
	policy := Fresh_policy(t)
	clean_args16(t)
	laila.Args.Set("policy", py.DictOf("central", py.DictOf("command", py.DictOf("taskforces", py.NewDict()))))
	tf := new_tf16(t, policy.Global_id(), 1)
	arg16("policy.central.command.taskforces").(*py.DotMap).Set(tf.Global_id(), py.NewDotMap(
		py.DictOf("num_workers", py.NewDotMap()),
	))
	tf2 := new_tf16(t, policy.Global_id(), 1, py.Kw{"uuid": tf.Uuid()})
	assertEq(t, tf2.Num_workers, 1)
	check(t, tf.Shutdown(py.Kw{"wait": true, "cancel_pending": true}))
	check(t, tf2.Shutdown(py.Kw{"wait": true, "cancel_pending": true}))
}

func TestArgsLoading_environment_assignment_of_inert_values(t *testing.T) {
	policy := Fresh_policy(t)
	// these must NOT trigger a reload (which would terminate fresh_policy)
	check(t, set_environment16(py.NewDict()))
	check(t, set_environment16(nil))
	check(t, set_environment16(py.NewDotMap()))
	assertTrue(t, laila.Local_policies_.Contains(policy.Global_id()))
	laila.Refresh_args_environment_(policy)
	assertTrue(t, mirror16(policy) != nil)
}

func TestArgsLoading_read_args_missing_file(t *testing.T) {
	Setup(t)
	assertTrue(t, laila.Read_args("/nonexistent/path/args.json") != nil)
}

func TestArgsLoading_read_args_unknown_extension(t *testing.T) {
	Setup(t)
	disk_tmp := Disk_tmp(t)
	path := write16(t, disk_tmp, "args.yaml", "a: 1\n")
	if err := laila.Read_args(path); !errors.Is(err, py.ValueError) {
		t.Fatalf("want ValueError, got %v", err)
	}
}

func TestArgsLoading_xml_two_levels(t *testing.T) {
	Setup(t)
	disk_tmp := Disk_tmp(t)
	clean_args16(t)
	var body strings.Builder
	for i := 0; i < N_scaled(1000); i++ {
		body.WriteString(fmt.Sprintf("<k%d>%d</k%d>", i, i, i))
	}
	path := write16(t, disk_tmp, "args.xml", fmt.Sprintf("<root><wl16xml>%s</wl16xml><wl16flat>7</wl16flat></root>", body.String()))
	check(t, laila.Read_args(path))
	assertEq(t, arg16("wl16xml_k5"), 5)
	assertEq(t, arg16("wl16flat"), 7)
}
