//go:build workload

// Port of tests/workloads/test_w03_persistent_backend_specifics.py:
// backend-specific behaviour of the persistent pools: cross-process access
// to the same file, durability after a hard kill (no ``close()``), close
// racing with writers, disk reclamation after deletes, and error surfacing
// for unserialisable payloads.
package workloads

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// open_kind_ is the ``from laila.data.<kind> import <Pool> as P; P(**kw)``
// prologue shared by the child scripts.
func open_kind_(kind string, kw py.Kw) (any, error) {
	switch kind {
	case "sqlite":
		return laila.New_SQLitePool(kw)
	case "duckdb":
		return laila.New_DuckDBPool(kw)
	}
	return laila.New_HDF5Pool(kw)
}

// w03_child is ``_CHILD``: open the pool at ``path`` (index disabled), then
// either report the key count (``read``) or write ``n`` keys and exit --
// cleanly (``write``) or via ``os._exit`` (``hardkill``).
func w03_child(args map[string]string) int {
	kind, path, mode := args["kind"], args["path"], args["mode"]
	n, _ := strconv.Atoi(args["n"])
	laila.Set_default_directory(args["home"])
	raw, err := open_kind_(kind, py.Kw{"file_path": path, "index_enabled": false})
	if err != nil {
		fmt.Printf("OPEN_FAILED %s\n", py.Typename(err))
		return 3
	}
	pool := laila.As_pool_(raw)
	if mode == "read" {
		keys, err := pool.Keys()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		fmt.Printf("KEYS %d\n", keys.(*py.List).Len())
		Close_pool(raw)
		return 0
	}
	for i := 0; i < n; i++ {
		if err := pool.Setitem(fmt.Sprintf("child%d", i), py.DictOf("i", i)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	fmt.Printf("WROTE %d\n", n)
	if mode == "hardkill" {
		os.Exit(0) // ``os._exit(0)``: no close, no deferred cleanup
	}
	Close_pool(raw)
	return 0
}

// w03_close_race_child is ``_CLOSE_RACE_CHILD``: 8 writer threads hammer
// the pool, ``close()`` lands under them, every writer must fail with
// RuntimeError (never crash, never keep writing).
func w03_close_race_child(args map[string]string) int {
	kind, path := args["kind"], args["path"]
	laila.Set_default_directory(args["home"])
	raw, err := open_kind_(kind, py.Kw{"file_path": path, "index_enabled": false})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	pool := laila.As_pool_(raw)
	var mu sync.Mutex
	errs := []string{}
	var deadline atomicFloat
	var wg sync.WaitGroup
	writer := func(i int) {
		defer wg.Done()
		j := 0
		for {
			if d := deadline.Load(); d != 0 && py.Monotonic() >= d {
				break
			}
			err := pool.Setitem(fmt.Sprintf("w%d-%d", i, j), py.DictOf("j", j))
			if err != nil {
				mu.Lock()
				if errors.Is(err, py.RuntimeError) {
					errs = append(errs, "RuntimeError")
				} else {
					errs = append(errs, py.Typename(err))
				}
				mu.Unlock()
				return
			}
			j++
		}
		mu.Lock()
		errs = append(errs, "NoErrorAfterClose")
		mu.Unlock()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go writer(i)
	}
	time.Sleep(500 * time.Millisecond)
	Close_pool(raw)
	deadline.Store(py.Monotonic() + 5)
	joined := make(chan struct{})
	go func() { wg.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(30 * time.Second):
	}
	mu.Lock()
	uniq := map[string]struct{}{}
	for _, e := range errs {
		uniq[e] = struct{}{}
	}
	names := make([]string, 0, len(uniq))
	for k := range uniq {
		names = append(names, k)
	}
	sort.Strings(names)
	fmt.Printf("ERRS %s N=%d\n", strings.Join(names, ","), len(errs))
	mu.Unlock()
	return 0
}

// atomicFloat is the ``deadline = [None]`` cell shared by the writers.
type atomicFloat struct {
	mu sync.Mutex
	v  float64
}

func (a *atomicFloat) Load() float64 { a.mu.Lock(); defer a.mu.Unlock(); return a.v }
func (a *atomicFloat) Store(v float64) { a.mu.Lock(); a.v = v; a.mu.Unlock() }

func init() {
	Child_scripts["w03_child"] = w03_child
	Child_scripts["w03_close_race_child"] = w03_close_race_child
}

// child_ is ``_child(kind, path, home, n=0, mode="write", timeout=120)``.
func child_(t *testing.T, kind, path, home string, n int, mode string, timeout ...float64) *ScriptResult {
	t.Helper()
	res, err := Run_script("w03_child", map[string]string{
		"kind": kind, "path": path, "home": home, "n": strconv.Itoa(n), "mode": mode,
	}, timeout...)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// ---------------------------------------------------------------------------
// cross-process
// ---------------------------------------------------------------------------

func TestCrossProcess_second_process_while_open(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		// sqlite shares the file; duckdb/hdf5 hold an exclusive lock.
		pool, raw := with_pool(t, kind)
		disk_tmp := Disk_tmp(t)
		pset(t, pool, "parent", py.DictOf("p", 1))
		res := child_(t, kind, file_path(raw), disk_tmp, N_scaled(200, 50), "write")
		if kind == "sqlite" {
			assertTrue(t, strings.Contains(res.Stdout, "WROTE"), Tail(res.Stderr, 1500))
			keys := pkeys(t, pool)
			assertTrue(t, keys.Contains("parent") && keys.Contains("child0"))
		} else {
			// exclusive-lock backends: the child must fail *cleanly* at open
			assertTrue(t, res.Returncode == 3, fmt.Sprint(res.Stdout, Tail(res.Stderr, 1500)))
			assertTrue(t, strings.Contains(res.Stdout, "OPEN_FAILED"))
		}
	})
}

func TestCrossProcess_second_process_after_close(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, raw := with_pool(t, kind)
		disk_tmp := Disk_tmp(t)
		n := N_scaled(300, 50)
		for i := 0; i < n; i++ {
			pset(t, pool, fmt.Sprintf("k%d", i), py.DictOf("i", i))
		}
		path := file_path(raw)
		Close_pool(raw)
		res := child_(t, kind, path, disk_tmp, 0, "read")
		assertTrue(t, res.Returncode == 0, Tail(res.Stderr, 1500))
		assertTrue(t, strings.Contains(res.Stdout, fmt.Sprintf("KEYS %d", n)), res.Stdout)
	})
}

func TestCrossProcess_handoff_ping_pong_between_processes(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		_, raw := with_pool(t, kind)
		disk_tmp := Disk_tmp(t)
		path := file_path(raw)
		Close_pool(raw)
		total := 0
		for round := 0; round < N_scaled(4, 2); round++ {
			res := child_(t, kind, path, disk_tmp, 50, "write")
			assertTrue(t, res.Returncode == 0, Tail(res.Stderr, 1500))
			total = 50 // child keys overwrite the same names
			res = child_(t, kind, path, disk_tmp, 0, "read")
			assertTrue(t, strings.Contains(res.Stdout, fmt.Sprintf("KEYS %d", total)), res.Stdout)
		}
	})
}

func TestCrossProcess_durability_after_hard_kill(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		// Writes acknowledged before ``os._exit`` must survive a reopen.
		_, raw := with_pool(t, kind)
		disk_tmp := Disk_tmp(t)
		path := file_path(raw)
		Close_pool(raw)
		n := N_scaled(300, 50)
		res := child_(t, kind, path, disk_tmp, n, "hardkill")
		assertTrue(t, strings.Contains(res.Stdout, fmt.Sprintf("WROTE %d", n)), Tail(res.Stderr, 1500))
		res = child_(t, kind, path, disk_tmp, 0, "read")
		// F-05 (fixed): HDF5Pool flushes after every write/delete.
		assertTrue(t, res.Returncode == 0, Tail(res.Stderr, 1500))
		assertTrue(t, strings.Contains(res.Stdout, fmt.Sprintf("KEYS %d", n)), res.Stdout)
	})
}

func TestCrossProcess_concurrent_sqlite_writers_two_processes(t *testing.T) {
	Setup(t)
	Skip_unless(t, contains_str(PERSISTENT_KINDS, "sqlite"), "sqlite missing")
	disk_tmp := Disk_tmp(t)
	raw := must(Make_pool("sqlite", disk_tmp, py.Kw{"index_enabled": false}))
	defer Close_pool(raw)
	pool := laila.As_pool_(raw)
	n := N_scaled(300, 50)
	proc := must(Popen_script("w03_child", map[string]string{
		"kind": "sqlite", "home": disk_tmp, "path": file_path(raw), "n": strconv.Itoa(n), "mode": "write",
	}))
	errs := []string{}
	for i := 0; i < n; i++ {
		if err := pool.Setitem(fmt.Sprintf("parent%d", i), py.DictOf("i", i)); err != nil {
			errs = append(errs, err.Error())
		}
	}
	res := must(proc.Communicate(300))
	assertTrue(t, res.Returncode == 0, Tail(res.Stderr, 2000))
	assertEq(t, errs, []string{})
	parents, children := 0, 0
	for _, k := range pkeys(t, pool).Items() {
		if strings.HasPrefix(py.Str(k), "parent") {
			parents++
		}
		if strings.HasPrefix(py.Str(k), "child") {
			children++
		}
	}
	assertEq(t, parents, n)
	assertEq(t, children, n)
}

// ---------------------------------------------------------------------------
// close racing writers
// ---------------------------------------------------------------------------

func TestCloseRaces_close_while_threads_write_is_safe(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		// F-06 (fixed): close() runs under the pool lock.
		_, raw := with_pool(t, kind)
		disk_tmp := Disk_tmp(t)
		Close_pool(raw)
		// the race is probabilistic (sqlite segfaults in ~1/3 of runs, hdf5 raises
		// ValueError in ~1/2): sample it several times and require *every* run clean
		type outcome struct {
			rc  int
			out string
		}
		outcomes := []outcome{}
		for i := 0; i < 10; i++ {
			path := filepath.Join(disk_tmp, fmt.Sprintf("race-%d.db", i))
			res := must(Run_script("w03_close_race_child", map[string]string{
				"kind": kind, "home": disk_tmp, "path": path,
			}, 180))
			outcomes = append(outcomes, outcome{res.Returncode, strings.TrimSpace(res.Stdout)})
		}
		for _, o := range outcomes {
			assertTrue(t, o.rc == 0, fmt.Sprintf("child crashed: %v", outcomes))
		}
		for _, o := range outcomes {
			assertTrue(t, o.out == "ERRS RuntimeError N=8", fmt.Sprint(outcomes))
		}
	})
}

func TestCloseRaces_double_close_is_idempotent(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, raw := with_pool(t, kind)
		Close_pool(raw)
		Close_pool(raw)
		if _, err := pool.Keys(); !errors.Is(err, py.RuntimeError) {
			t.Fatalf("expected RuntimeError, got %v", err)
		}
	})
}

func TestCloseRaces_close_then_reopen_many_times_no_fd_leak(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		disk_tmp := Disk_tmp(t)
		raw := must(Make_pool(kind, disk_tmp))
		path := file_path(raw)
		pset(t, laila.As_pool_(raw), "warm", py.DictOf("v", 0)) // lazily initialises policy-side plumbing (fds) before baseline
		Close_pool(raw)
		open := func() (*Pool, any) {
			q := must(open_kind_(kind, py.Kw{"file_path": path}))
			return laila.As_pool_(q), q
		}
		q, qraw := open()
		pset(t, q, "k", py.DictOf("v", 1))
		Close_pool(qraw)
		base_fds := len(Open_fds())
		for i := 0; i < N_scaled(30, 10); i++ {
			q, qraw = open()
			pset(t, q, "k", py.DictOf("v", 1))
			Close_pool(qraw)
		}
		assertTrue(t, len(Open_fds()) <= base_fds+3)
	})
}

// ---------------------------------------------------------------------------
// disk reclamation
// ---------------------------------------------------------------------------

func TestDiskReclaim_delete_all_then_rewrite_does_not_grow_unbounded(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		// F-07 (fixed): DuckDBPool arms a deferred CHECKPOINT from delete churn
		// and runs it before the next write, so freed blocks are reused.
		pool, raw := with_pool(t, kind)
		text := strings.Repeat("z", 256*1024)
		n := N_scaled(40, 10)
		sizes := []int64{}
		for cycle := 0; cycle < N_scaled(4, 2); cycle++ {
			for i := 0; i < n; i++ {
				pset(t, pool, fmt.Sprintf("b%d", i), py.DictOf("blob", text))
			}
			for i := 0; i < n; i++ {
				pdel(t, pool, fmt.Sprintf("b%d", i))
			}
			st := must(os.Stat(file_path(raw)))
			sizes = append(sizes, st.Size())
		}
		// free space must be reused: last size within 3x of the first
		assertTrue(t, sizes[len(sizes)-1] <= sizes[0]*3+(16<<20), fmt.Sprint(sizes))
	})
}

func TestDiskReclaim_empty_keeps_file_usable(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		pool, _ := with_pool(t, kind)
		for i := 0; i < N_scaled(300, 50); i++ {
			pset(t, pool, fmt.Sprintf("k%d", i), py.DictOf("i", i))
		}
		check(t, pool.Empty())
		pset(t, pool, "after", py.DictOf("ok", true))
		assertEq(t, pget(t, pool, "after"), py.DictOf("ok", true))
		assertEq(t, pkeys(t, pool), py.NewList("after"))
	})
}

// ---------------------------------------------------------------------------
// error surfacing through the policy
// ---------------------------------------------------------------------------

func TestErrorSurfacing_unserialisable_payload_error_surfaces_promptly(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		_, raw := with_pool(t, kind)
		mem_extend(t, policy, raw, "raw")
		// Neither msgpack nor the pickle fallback can serialise a lambda.
		e := must(laila.Constant(py.DictOf("fn", func(x any) any { return x })))
		f := must(laila.Memorize(e, py.Kw{"pool_nickname": "raw"}))
		var err error
		sw := Timed(func() { _, err = Wait(f, 60) })
		assertTrue(t, err != nil, "expected the memorize to fail")
		assertTrue(t, sw.Elapsed < 55, "error did not surface promptly")
	})
}

func TestErrorSurfacing_none_transformations_rejected_at_construction(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		// F-08: a persistent backend cannot store a live Entry; the
		// misconfiguration is rejected up front instead of failing every memorize.
		_, err := Make_pool(kind, Disk_tmp(t), py.Kw{"transformations": nil})
		if !errors.Is(err, py.ValueError) || !strings.Contains(err.Error(), "transformations=None") {
			t.Fatalf("expected ValueError(transformations=None), got %v", err)
		}
	})
}

func TestErrorSurfacing_timestamp_attribute_resolution_at_volume(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		_, raw := with_pool(t, kind)
		mem_extend(t, policy, raw, "p")
		v := must(laila.Variable(py.Kw{"data": py.DictOf("v", 0)}))
		stamps := []string{}
		for i := 1; i <= N_scaled(30, 10); i++ {
			check(t, v.Set_data(py.DictOf("v", i)))
			must(Wait(must(laila.Memorize(v, py.Kw{"pool_nickname": "p"})), 60))
			stamps = append(stamps, *v.Creation_timestamp())
			// Creation timestamps have millisecond resolution (Python and
			// Go alike) and the shard maps stamp -> evolution, so two
			// evolutions stamped in the same millisecond are
			// indistinguishable. h5py is slow enough that the Python test
			// never collides; the Go HDF5 driver completes a memorize in
			// well under a millisecond, so step past the current one.
			time.Sleep(2 * time.Millisecond)
		}
		base := strings.SplitN(v.Global_id(), "@", 2)[0]
		target := stamps[len(stamps)/2]
		got := must(Wait(must(laila.Remember(
			fmt.Sprintf("%s@creation_timestamp=%s", base, target),
			py.Kw{"pool_nickname": "p", "persist": false})), 60))
		assertEq(t, Unwrap(got), py.DictOf("v", len(stamps)/2+1))
	})
}

func TestErrorSurfacing_forget_missing_keys_is_tolerated(t *testing.T) {
	for_kinds(t, func(t *testing.T, kind string) {
		policy := Fresh_policy(t)
		pool, raw := with_pool(t, kind)
		mem_extend(t, policy, raw, "p")
		ids := py.NewList()
		for i := 0; i < 50; i++ {
			ids.Append(fmt.Sprintf("LAILA:ENTRY:00000000-0000-4000-8000-00000000%04x", i))
		}
		must(Wait(must(laila.Forget(ids, py.Kw{"pool_nickname": "p"})), 60))
		assertEq(t, pkeys(t, pool), py.NewList())
	})
}
