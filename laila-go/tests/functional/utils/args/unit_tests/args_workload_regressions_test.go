// Port of tests/functional/utils/args/unit_tests/test_args_workload_regressions.py.
//
// - F-29: constructing CLI-capable objects marks the owning policy dirty
//   (O(1)); the ``laila.args.environment`` mirror is written only when read.
// - F-30: a nested config file populates the nested ``laila.args`` tree in
//   addition to the flattened ``a_b`` keys.
// - F-31: a file literally named ``.env`` loads as dotenv, and terminal
//   tokens accept ``--key value``, ``--key=value`` and bare ``--flag``.
package unit_tests

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const ARG_PREFIX_ = "wlreg_"

func purgeTestArgs() {
	for _, key := range laila.Args.Keys() {
		if strings.HasPrefix(key, ARG_PREFIX_) || key == "policy" || key == "db" {
			laila.Args.Delete(key)
		}
	}
}

type argsCase struct {
	t   *testing.T
	tmp string
}

func newArgsCase(t *testing.T) *argsCase {
	t.Helper()
	purgeTestArgs()
	t.Cleanup(purgeTestArgs)
	return &argsCase{t: t, tmp: t.TempDir()}
}

func (c *argsCase) write(name, content string) string {
	c.t.Helper()
	path := filepath.Join(c.tmp, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		c.t.Fatal(err)
	}
	return path
}

func jsonOf(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func attr(t *testing.T, obj any, name string) any {
	t.Helper()
	v, err := py.Getattr(obj, name)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func readArgs(t *testing.T, source string, kw ...py.Kw) {
	t.Helper()
	if err := laila.Read_args(source, kw...); err != nil {
		t.Fatalf("read_args(%s): %v", source, err)
	}
}

func TestNestedMerge(t *testing.T) {
	t.Run("json_tree_is_flattened_and_nested", func(t *testing.T) {
		c := newArgsCase(t)
		path := c.write("cfg.json", jsonOf(t, map[string]any{
			"wlreg_db": map[string]any{"host": "h", "deep": map[string]any{"k": "v", "n": "3"}},
		}))
		readArgs(t, path)
		if arg("wlreg_db_host") != "h" {
			t.Errorf("flat: %v", arg("wlreg_db_host"))
		}
		if arg("wlreg_db.host") != "h" || arg("wlreg_db.deep.k") != "v" {
			t.Errorf("nested: %s", py.Repr(arg("wlreg_db")))
		}
		// scalars inside the tree are coerced like flat values
		if !py.Eq(arg("wlreg_db.deep.n"), 3) {
			t.Errorf("coerce: %v", arg("wlreg_db.deep.n"))
		}
	})
	t.Run("second_file_merges_into_existing_branch", func(t *testing.T) {
		c := newArgsCase(t)
		p1 := c.write("a.json", jsonOf(t, map[string]any{"wlreg_t": map[string]any{"x": map[string]any{"a": 1}}}))
		p2 := c.write("b.json", jsonOf(t, map[string]any{"wlreg_t": map[string]any{"x": map[string]any{"b": 2}, "y": 3}}))
		readArgs(t, p1)
		readArgs(t, p2)
		if !py.Eq(arg("wlreg_t.x.a"), 1) || !py.Eq(arg("wlreg_t.x.b"), 2) || !py.Eq(arg("wlreg_t.y"), 3) {
			t.Errorf("merge: %s", py.Repr(arg("wlreg_t")))
		}
	})
	t.Run("nested_file_drives_cli_capable_defaults", func(t *testing.T) {
		c := newArgsCase(t)
		p, err := laila.New_DefaultPool()
		if err != nil {
			t.Fatal(err)
		}
		gid := py.Str(attr(t, p, "global_id"))
		payload := map[string]any{"policy": map[string]any{"central": map[string]any{"memory": map[string]any{
			"pools": map[string]any{gid: map[string]any{"batch_accelerated": true}},
		}}}}
		readArgs(t, c.write("pool.json", jsonOf(t, payload)))
		q, err := laila.New_DefaultPool(py.Kw{"uuid": attr(t, p, "uuid")})
		if err != nil {
			t.Fatal(err)
		}
		if !py.Truthy(attr(t, q, "batch_accelerated")) {
			t.Error("file default not applied")
		}
	})
	t.Run("merge_into_atomic_dotmap_target", func(t *testing.T) {
		c := newArgsCase(t)
		ad := laila.New_AtomicDotMap()
		r, err := laila.New_ArgReader(py.Kw{"target": ad})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.From_json(c.write("ad.json", `{"db": {"host": "h", "deep": {"k": "v"}}}`)); err != nil {
			t.Fatal(err)
		}
		if ad.Get("db_host") != "h" {
			t.Errorf("flat: %v", ad.Get("db_host"))
		}
		deep, err := py.Getitem(ad.Get("db"), "deep")
		if err != nil {
			t.Fatal(err)
		}
		k, err := py.Getitem(deep, "k")
		if err != nil || k != "v" {
			t.Errorf("nested: %v %v", k, err)
		}
	})
	t.Run("merge_into_plain_dotmap_target", func(t *testing.T) {
		newArgsCase(t)
		target := py.NewDotMap()
		r1, _ := laila.New_ArgReader(py.Kw{"target": target})
		r1.Apply_(py.DictOf("a", py.DictOf("b", py.DictOf("c", 1))))
		r2, _ := laila.New_ArgReader(py.Kw{"target": target})
		r2.Apply_(py.DictOf("a", py.DictOf("b", py.DictOf("d", 2))))
		if !py.Eq(target.Get("a.b.c"), 1) || !py.Eq(target.Get("a.b.d"), 2) {
			t.Errorf("tree: %s", py.Repr(target))
		}
		// flattened one-level keys stay last-writer-wins; only the tree merges
		if !py.Eq(target.Get("a_b"), py.DictOf("d", 2)) {
			t.Errorf("a_b = %s", py.Repr(target.Get("a_b")))
		}
	})
}

func TestDotEnvAndTerminal(t *testing.T) {
	t.Run("file_named_dot_env", func(t *testing.T) {
		c := newArgsCase(t)
		t.Cleanup(func() { laila.Args.Delete("WLREG_DOTENV"); laila.Args.Delete("WLREG_FLAG") })
		readArgs(t, c.write(".env", "WLREG_DOTENV=1\nWLREG_FLAG=true\n"))
		if !py.Eq(arg("WLREG_DOTENV"), 1) || arg("WLREG_FLAG") != true {
			t.Errorf("dotenv: %v %v", arg("WLREG_DOTENV"), arg("WLREG_FLAG"))
		}
	})
	t.Run("terminal_dashed_forms", func(t *testing.T) {
		newArgsCase(t)
		readArgs(t, "terminal", py.Kw{"terminal_args": []string{
			"--wlreg_a", "7", "--wlreg_b=8", "--wlreg_flag", "wlreg_c=9", "-wlreg_d", "x",
		}})
		if !py.Eq(arg("wlreg_a"), 7) || !py.Eq(arg("wlreg_b"), 8) || arg("wlreg_flag") != true ||
			!py.Eq(arg("wlreg_c"), 9) || arg("wlreg_d") != "x" {
			t.Errorf("args = %s", py.Repr(laila.Args))
		}
	})
	t.Run("trailing_flag_is_true", func(t *testing.T) {
		newArgsCase(t)
		readArgs(t, "terminal", py.Kw{"terminal_args": []string{"--wlreg_last"}})
		if arg("wlreg_last") != true {
			t.Errorf("wlreg_last = %v", arg("wlreg_last"))
		}
	})
}

// TestLazyEnvironmentMirror (F-29): constructing CLI-capable objects marks
// the owning policy dirty (O(1)); the ``laila.args.environment`` mirror is
// written only when read.
func TestLazyEnvironmentMirror(t *testing.T) {
	setUp := func(t *testing.T) {
		t.Helper()
		laila.Terminate()
		t.Cleanup(func() { laila.Terminate() })
	}
	gid := func(obj any) string { return py.Str(attr(t, obj, "global_id")) }
	dirty := func(g string) bool { return laila.DIRTY_POLICY_GIDS_.Contains(g) }

	t.Run("construction_marks_dirty_and_read_flushes", func(t *testing.T) {
		setUp(t)
		pol, err := laila.New_DefaultPolicy()
		if err != nil {
			t.Fatal(err)
		}
		laila.Activate_policy(pol)
		_ = laila.Args.Get("environment") // flush whatever activation queued
		if dirty(gid(pol)) {
			t.Fatal("policy still dirty after a read")
		}
		p, err := laila.New_DefaultPool()
		if err != nil {
			t.Fatal(err)
		}
		mem, err := laila.Memory()
		if err != nil {
			t.Fatal(err)
		}
		if err := laila.As_central_memory_(mem).Extend(p, py.Kw{"pool_nickname": "lazy0"}); err != nil {
			t.Fatal(err)
		}
		if !dirty(gid(pol)) {
			t.Fatal("extend did not mark the policy dirty")
		}
		mirror := laila.Args.Get("environment.policies." + gid(pol)).(*py.DotMap)
		if dirty(gid(pol)) {
			t.Fatal("policy still dirty after the mirror was read")
		}
		if _, ok := mirror.Lookup("central.memory.pool_router.pools." + gid(p)); !ok {
			t.Fatalf("pool %s missing from mirror: %s", gid(p), py.Repr(mirror))
		}
	})
	t.Run("mirror_cleared_on_terminate", func(t *testing.T) {
		setUp(t)
		pol, err := laila.New_DefaultPolicy()
		if err != nil {
			t.Fatal(err)
		}
		laila.Activate_policy(pol)
		_ = laila.Args.Get("environment")
		laila.Terminate()
		if laila.DIRTY_POLICY_GIDS_.Len() != 0 {
			t.Fatalf("dirty gids left: %s", py.Repr(laila.DIRTY_POLICY_GIDS_))
		}
		env, _ := laila.Args.Lookup("environment")
		var policies any
		if dm, ok := env.(*py.DotMap); ok {
			policies, _ = dm.Lookup("policies")
		}
		if py.Truthy(policies) {
			t.Fatalf("policies mirror not cleared: %s", py.Repr(policies))
		}
	})
	t.Run("pool_construction_cost_is_not_quadratic", func(t *testing.T) {
		setUp(t)
		pol, err := laila.New_DefaultPolicy()
		if err != nil {
			t.Fatal(err)
		}
		laila.Activate_policy(pol)
		memAny, err := laila.Memory()
		if err != nil {
			t.Fatal(err)
		}
		mem := laila.As_central_memory_(memAny)

		add := func(n int) float64 {
			t0 := time.Now()
			for i := 0; i < n; i++ {
				p, err := laila.New_DefaultPool()
				if err != nil {
					t.Fatal(err)
				}
				if err := mem.Extend(p, py.Kw{"pool_nickname": fmt.Sprintf("cost%d-%d", n, i)}); err != nil {
					t.Fatal(err)
				}
			}
			return time.Since(t0).Seconds()
		}

		small := add(60)
		large := add(240)
		// the first batch also warms caches; 4x pools must stay well under 16x
		if limit := math.Max(small*8, 0.2) + 0.2; large >= limit {
			t.Fatalf("(%v, %v)", small, large)
		}
	})
}
