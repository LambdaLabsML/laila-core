//go:build workload

// Port of tests/workloads/conftest.py: shared fixtures for the
// long-running workload / stress suite.
//
// Every file in this package carries ``//go:build workload`` (the Python
// modules are auto-marked ``slow`` and ``workload`` so the default CI run
// skips the directory); run it explicitly with::
//
//	cd laila-go/tests && go test -tags workload -timeout 0 ./workloads/
//
// Tunables (environment variables):
//
//   - ``LAILA_WORKLOAD_SCALE`` (float, default ``1.0``) multiplies the size
//     of every workload (item counts, payload bytes, thread counts).
//   - ``LAILA_WORKLOAD_TIMEOUT`` (seconds, default ``900``) per-test hang
//     watchdog. On expiry the stack of every goroutine is dumped to stderr
//     and the process exits -- a hang is itself a finding, and a hung suite
//     is useless.
//   - ``LAILA_WORKLOAD_TMP`` overrides the disk-backed scratch root
//     (defaults to ``<repo>/.workload_tmp``). ``/tmp`` is tmpfs on many
//     boxes, so large on-disk pools must not live there.
//
// Tests that document *suspected defects* are marked ``suspected_bug`` and
// declared ``xfail(strict=True)`` in Python; here they call Xfail_strict
// which inverts the outcome (a passing body fails the test, "the signal to
// drop the marker"). The catalogue lives in ``tests/workloads/FINDINGS.md``.
package workloads

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// TIMEOUT is ``float(os.environ.get("LAILA_WORKLOAD_TIMEOUT", "900"))``.
var TIMEOUT = func() float64 {
	if v := os.Getenv("LAILA_WORKLOAD_TIMEOUT"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return 900.0
}()

var repo_root_ = func() string {
	// laila-go/tests/workloads -> laila
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(file))))
}()

var workload_root_ string

// TestMain: child-peer mode, the session-scoped ``workload_root`` fixture
// (scratch space on real disk routed to every default on-disk location so
// the suite never touches ~/.laila and cleans up after itself), and the
// final laila.Terminate.
func TestMain(m *testing.M) {
	// The spawn-context ``__main__`` guard: process-pool workers are this
	// binary re-executed with LAILA_PROCESS_WORKER=1 (no-op otherwise).
	laila.Process_worker_main()
	if os.Getenv(CHILD_ENV) != "" {
		os.Exit(Child_main())
	}
	if os.Getenv(SCRIPT_ENV) != "" {
		os.Exit(Script_main())
	}
	base := os.Getenv("LAILA_WORKLOAD_TMP")
	if base == "" {
		base = filepath.Join(repo_root_, ".workload_tmp")
	}
	_ = os.MkdirAll(base, 0o755)
	workload_root_ = filepath.Join(base, fmt.Sprintf("run-%d-%s", os.Getpid(), py.Uuid4().Hex()[:6]))
	_ = os.MkdirAll(workload_root_, 0o755)
	laila.Set_default_directory(filepath.Join(workload_root_, "laila_home"))

	code := m.Run()

	Run_module_teardowns_()
	laila.Terminate()
	_ = os.RemoveAll(workload_root_)
	_ = os.Remove(base) // only when empty, like ``os.rmdir``
	os.Exit(code)
}

// ---------------------------------------------------------------------------
// module-scoped fixtures
// ---------------------------------------------------------------------------

var (
	module_teardowns_mu_ sync.Mutex
	module_teardowns_    []func()
)

// Add_module_teardown registers the finaliser of a ``scope="module"``
// fixture; they run (last registered first) after the whole package ran.
func Add_module_teardown(fn func()) {
	module_teardowns_mu_.Lock()
	module_teardowns_ = append(module_teardowns_, fn)
	module_teardowns_mu_.Unlock()
}

// Run_module_teardowns_ runs the registered module finalisers.
func Run_module_teardowns_() {
	module_teardowns_mu_.Lock()
	fns := module_teardowns_
	module_teardowns_ = nil
	module_teardowns_mu_.Unlock()
	for i := len(fns) - 1; i >= 0; i-- {
		func() {
			defer func() { _ = recover() }()
			fns[i]()
		}()
	}
}

// ---------------------------------------------------------------------------
// scratch space on real disk
// ---------------------------------------------------------------------------

// Workload_root is the session fixture ``workload_root``.
func Workload_root() string { return workload_root_ }

// Disk_tmp is the ``disk_tmp`` fixture: a per-test directory under the
// workload root, removed at cleanup.
func Disk_tmp(t *testing.T) string {
	t.Helper()
	name := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, t.Name())
	if len(name) > 80 {
		name = name[:80]
	}
	path := filepath.Join(workload_root_, name+"-"+py.Uuid4().Hex()[:6])
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(path) })
	return path
}

// ---------------------------------------------------------------------------
// scale / timeout
// ---------------------------------------------------------------------------

// Scale is the ``scale`` fixture.
func Scale() float64 { return SCALE }

// Hang_watchdog is the autouse ``_hang_watchdog`` fixture: dump every
// goroutine's stack and abort if a single test exceeds TIMEOUT. Call it at
// the top of each test (Setup does).
func Hang_watchdog(t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	timer := time.AfterFunc(time.Duration(TIMEOUT*float64(time.Second)), func() {
		select {
		case <-done:
			return
		default:
		}
		fmt.Fprintf(os.Stderr, "\n=== workload watchdog: %s exceeded %.0fs; dumping all goroutines ===\n", t.Name(), TIMEOUT)
		debug.SetTraceback("all")
		buf := make([]byte, 64<<20)
		n := runtime.Stack(buf, true)
		os.Stderr.Write(buf[:n])
		os.Exit(2)
	})
	t.Cleanup(func() { close(done); timer.Stop() })
}

// Restore_namespace is the autouse ``_restore_namespace`` fixture: the uuid5
// nickname namespace is process-global and survives ``laila.terminate()``;
// tests that call ``set_active_namespace`` must not leak it into later
// tests (child-process peers derive nickname gids from the *default*
// namespace).
func Restore_namespace(t *testing.T) {
	t.Helper()
	saved := laila.Active_namespace_()
	t.Cleanup(func() { laila.Set_active_namespace_(saved) })
}

// Setup applies the autouse fixtures to a test.
func Setup(t *testing.T) {
	t.Helper()
	Hang_watchdog(t)
	Restore_namespace(t)
}

// ---------------------------------------------------------------------------
// policies
// ---------------------------------------------------------------------------

// Fresh_policy is the ``fresh_policy`` fixture: activate an isolated policy
// for the test and restore the old one after.
func Fresh_policy(t *testing.T) *laila.LAILA_IDENTIFIABLE_POLICY_ {
	t.Helper()
	Setup(t)
	original, err := laila.Get_active_policy()
	if err != nil {
		t.Fatal(err)
	}
	policy, err := laila.New_LAILA_IDENTIFIABLE_POLICY_()
	if err != nil {
		t.Fatal(err)
	}
	laila.Activate_policy(policy)
	t.Cleanup(func() {
		_ = Comm(policy).Stop()
		_ = Cmd(policy).Shutdown(py.Kw{"wait": true, "cancel_pending": true})
		for _, pool := range Router(policy).Pools.Values() {
			Close_pool(pool)
		}
		laila.Local_policies_.PopDefault(policy.Global_id(), nil)
		orig_gid := py.Str(py.Must(py.Getattr(original, "global_id")))
		if laila.Local_policies_.Contains(orig_gid) {
			laila.Activate_policy(original)
		} else {
			// a lifecycle test terminated everything; let the next caller
			// lazily create a fresh default policy
			laila.Set_active_policy_gid_("")
		}
	})
	return policy
}

// Run is the ``run`` fixture: run a coroutine to completion on a private
// event loop (``asyncio.run(coro)``).
func Run(coro func() (any, error)) (any, error) { return py.Run(coro) }

// ---------------------------------------------------------------------------
// resource probes
// ---------------------------------------------------------------------------

// ResourceProbe snapshots threads / fds / RSS / future_bank for leak
// assertions.
type ResourceProbe struct{}

// Threads is ``threading.active_count()`` -- goroutines here.
func (ResourceProbe) Threads() int { return runtime.NumGoroutine() }

// Fds is the number of open descriptors.
func (ResourceProbe) Fds() int {
	runtime.GC()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return len(entries)
}

// Rss_mb is the resident set size in MiB.
func (ResourceProbe) Rss_mb() float64 {
	raw, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0
	}
	return float64(pages) * float64(os.Getpagesize()) / (1024 * 1024)
}

// Bank is ``len(policy.future_bank)``.
func (ResourceProbe) Bank(policy ...any) int {
	var p any
	if len(policy) > 0 && !py.IsNone(policy[0]) {
		p = policy[0]
	} else {
		p = py.Must(laila.Get_active_policy())
	}
	return As_policy(p).Future_bank.Len()
}

// Snapshot is ``snapshot(policy=None)``.
func (r ResourceProbe) Snapshot(policy ...any) *py.Dict {
	return py.DictOf("threads", r.Threads(), "fds", r.Fds(), "rss_mb", r.Rss_mb(), "bank", r.Bank(policy...))
}

// Probe is the ``probe`` fixture.
func Probe() ResourceProbe { return ResourceProbe{} }

// ---------------------------------------------------------------------------
// markers
// ---------------------------------------------------------------------------

// Xfail_strict is ``@pytest.mark.suspected_bug`` + ``xfail(strict=True)``:
// the body must *fail*; when it passes the defect was fixed and the test
// fails to signal "drop the marker".
func Xfail_strict(t *testing.T, reason string, body func() error) {
	t.Helper()
	err := body()
	if err == nil {
		t.Fatalf("XPASS(strict): %s -- the documented defect no longer reproduces; drop the suspected_bug marker", reason)
	}
	t.Logf("xfail (suspected_bug): %s: %v", reason, err)
}

// Skip_unless is ``pytest.skip`` unless cond.
func Skip_unless(t *testing.T, cond bool, reason string) {
	t.Helper()
	if !cond {
		t.Skip(reason)
	}
}

// must unwraps (v, err), panicking on error (inline expressions).
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// check fails the test on err.
func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// assertEq is ``assert got == want``.
func assertEq(t *testing.T, got, want any) {
	t.Helper()
	if !py.Eq(got, want) {
		t.Fatalf("got %s, want %s", py.Repr(got), py.Repr(want))
	}
}

// assertTrue is ``assert cond``.
func assertTrue(t *testing.T, cond bool, msg ...any) {
	t.Helper()
	if !cond {
		if len(msg) > 0 {
			t.Fatal(msg...)
		}
		t.Fatal("assertion failed")
	}
}

// unused-import guard for files that only need the package name.
var _ = debug.SetTraceback
