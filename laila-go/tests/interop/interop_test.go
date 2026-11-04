// Live Python <-> Go interop over the real wire.
//
// Port of laila-js/tests/interop/interop.test.js, with the Go port in the
// place of the JavaScript one. Both directions, over the WebSocket
// (``tcpip``) and raw TCP transports:
//
//   - **Go client -> Python server**: the Go port peers to a CPython
//     ``laila`` process (``py_peer.py server``), exchanges RPCs, pulls
//     entries with ``laila.remember(..., policy_id=py)`` (CPython pickle
//     blobs rebuilt in Go), pushes entries with ``laila.memorize(...,
//     policy_id=py)`` (Go pickle blobs rebuilt in CPython) and checks the
//     Python side via probe RPCs.
//   - **Python client -> Go server**: a CPython process (``py_peer.py
//     client``) peers to a Go-hosted policy and runs the mirror-image
//     checks, reporting one JSON line per check.
//
// Requires a CPython with ``laila`` importable. Resolution order:
// ``$LAILA_PYTHON`` (interpreter, default ``python3`` on PATH) and
// ``$LAILA_PYTHONPATH`` (default: the directory containing the ``laila``
// package, i.e. the parent of the repository root). When unavailable the
// suite is skipped, not failed. Skipped under ``go test -short`` (each
// transport spawns a CPython process; ~1 minute wall-clock in total).
package interop

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// ------------------------------------------------------------------ #
// helpers
// ------------------------------------------------------------------ #

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

func assertTrue(t *testing.T, cond bool, msg string) {
	t.Helper()
	if !cond {
		t.Fatal(msg)
	}
}

func sleep(s float64) { time.Sleep(time.Duration(s * float64(time.Second))) }

func wait_until(pred func() bool, timeout float64) bool {
	deadline := py.Monotonic() + timeout
	for py.Monotonic() < deadline {
		if pred() {
			return true
		}
		sleep(0.01)
	}
	return pred()
}

// blob_ is ``bytes([i % 256 for i in range(n)])``.
func blob_(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 256)
	}
	return b
}

func reversed_(b []byte) []byte {
	out := make([]byte, len(b))
	for i, x := range b {
		out[len(b)-1-i] = x
	}
	return out
}

var STREAM_SIZES = []int{1, 100, 4096, 65536 + 7}

// REFERENCE_PAYLOAD is the Python ``_reference_payload()``: every
// wire-relevant scalar / container kind.
func reference_payload_(message string) *py.Dict {
	return py.DictOf(
		"message", message,
		"int", 7,
		"float", 2.5,
		"one", 1.0,
		"bool", true,
		"none", nil,
		"list", py.NewList(1, "two", 3.0, false, nil),
		"nested", py.DictOf("a", py.DictOf("b", py.NewList(1, 2, py.DictOf("c", "d")))),
		"unicode", "ключ 键 🔑",
	)
}

// assert_reference compares a value that crossed the wire with the
// Python reference payload.
func assert_reference(t *testing.T, data any, message string) {
	t.Helper()
	d, ok := data.(*py.Dict)
	if !ok {
		t.Fatalf("payload is %T, want *py.Dict: %s", data, py.Repr(data))
	}
	get := func(k string) any { v, _ := d.Get(k); return v }
	assertEq(t, get("message"), message)
	assertEq(t, get("int"), 7)
	assertEq(t, get("float"), 2.5)
	assertEq(t, get("one"), 1.0)
	assertEq(t, get("bool"), true)
	assertEq(t, get("none"), nil)
	assertEq(t, get("list"), py.NewList(1, "two", 3.0, false, nil))
	assertEq(t, get("nested"), py.DictOf("a", py.DictOf("b", py.NewList(1, 2, py.DictOf("c", "d")))))
	assertEq(t, get("unicode"), "ключ 键 🔑")
}

func comm_(p *laila.LAILA_IDENTIFIABLE_POLICY_) *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ {
	return laila.As_communication_(p.Central.Communication)
}

func peer_proxy_(t *testing.T, p *laila.LAILA_IDENTIFIABLE_POLICY_, gid string) *laila.PeerProxy {
	t.Helper()
	v, err := comm_(p).Peers.Getitem(gid)
	if err != nil {
		t.Fatalf("laila.peers[%s]: %v", gid, err)
	}
	proxy, ok := v.(*laila.PeerProxy)
	if !ok {
		t.Fatalf("laila.peers[%s] is %T", gid, v)
	}
	return proxy
}

// wait_if_future_ is ``if fut is not None and hasattr(fut, "wait"): fut.wait(timeout)``.
func wait_if_future_(t *testing.T, fut any, timeout float64) any {
	t.Helper()
	if py.IsNone(fut) {
		return nil
	}
	if w, ok := fut.(interface{ Wait(timeout ...any) (any, error) }); ok {
		return must(w.Wait(timeout))
	}
	return fut
}

func data_of_(t *testing.T, ref any) any {
	t.Helper()
	d, ok := ref.(interface{ Data() (any, error) })
	if !ok {
		t.Fatalf("%T has no .data", ref)
	}
	return must(d.Data())
}

func bytes_of_(t *testing.T, e any) []byte {
	t.Helper()
	switch b := data_of_(t, e).(type) {
	case []byte:
		return b
	case string:
		return []byte(b)
	default:
		t.Fatalf("stream payload is %T", b)
		return nil
	}
}

// entry_of_ is the live Entry behind a memory-pool Record
// (``{entry, recorder, ...}``) -- mirror of the JS ``_entry_of``.
func entry_of_(pool *laila.LAILA_IDENTIFIABLE_POOL_, gid string) (*laila.Entry, error) {
	rec, err := pool.Getitem(gid)
	if err != nil {
		return nil, err
	}
	var entry any = rec
	switch r := rec.(type) {
	case *laila.Record:
		entry = r.Entry
	case *py.Dict:
		if v, ok := r.Get("entry"); ok {
			entry = v
		}
	}
	if e, ok := entry.(*laila.Entry); ok {
		return e, nil
	}
	built, err := laila.Build_by_scope(entry)
	if err != nil {
		return nil, err
	}
	if e := laila.As_entry_(built); e != nil {
		return e, nil
	}
	return nil, fmt.Errorf("pool record for %s rebuilt to %T", gid, built)
}

// probe_entry_ describes what *pool* holds under *gid* (plain JSON) --
// mirror of ``py_peer._probe_entry``.
func probe_entry_(pool *laila.LAILA_IDENTIFIABLE_POOL_, gid string) (any, error) {
	present, err := pool.Contains(gid)
	if err != nil {
		return nil, err
	}
	if !present {
		return py.DictOf("present", false), nil
	}
	entry, err := entry_of_(pool, gid)
	if err != nil {
		return nil, err
	}
	data, err := entry.Data()
	if err != nil {
		return nil, err
	}
	out := py.DictOf("present", true)
	if arr, ok := data.(*py.NDArray); ok {
		out.Set("type", "ndarray")
		out.Set("dtype", arr.Dtype)
		out.Set("shape", py.NewList(any_ints_(arr.Shape)...))
		sum := 0.0
		for i := 0; i < arr.Size(); i++ {
			v, err := arr.Item(i)
			if err != nil {
				return nil, err
			}
			f, err := py.Float(v)
			if err != nil {
				return nil, err
			}
			sum += f
		}
		out.Set("sum", sum)
		return out, nil
	}
	switch data.(type) {
	case *py.Dict, map[string]any:
		out.Set("type", "dict")
	case *py.List, []any:
		out.Set("type", "list")
	case string:
		out.Set("type", "str")
	case bool:
		out.Set("type", "bool")
	case int, int64:
		out.Set("type", "int")
	case float64:
		out.Set("type", "float")
	case nil:
		out.Set("type", "NoneType")
	default:
		out.Set("type", py.Pytypename(data))
	}
	out.Set("data", data)
	return out, nil
}

func any_ints_(xs []int) []any {
	out := make([]any, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}

// ------------------------------------------------------------------ #
// CPython discovery + subprocess management
// ------------------------------------------------------------------ #

// python_laila_root_ is ``$LAILA_PYTHONPATH`` or the directory that makes
// ``import laila`` resolve to the reference Python package of this
// checkout (tests/interop -> laila-go -> laila (the package) -> its
// parent), "" when it cannot be found.
func python_laila_root_() string {
	if v := os.Getenv("LAILA_PYTHONPATH"); v != "" {
		return v
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	dir := filepath.Dir(file)
	for i := 0; i < 12; i++ {
		if _, err := os.Stat(filepath.Join(dir, "laila", "__init__.py")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

// python_exe_ honours ``$LAILA_PYTHON``, else ``python3`` / ``python`` on PATH.
func python_exe_() string {
	if v := os.Getenv("LAILA_PYTHON"); v != "" {
		return v
	}
	for _, name := range []string{"python3", "python"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	return ""
}

func py_peer_script_() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "py_peer.py")
}

func py_env_(root string) []string {
	pp := root
	if cur := os.Getenv("PYTHONPATH"); cur != "" {
		pp += string(os.PathListSeparator) + cur
	}
	return append(os.Environ(), "PYTHONPATH="+pp)
}

var (
	have_python_once sync.Once
	have_python_     bool
	have_python_why  string
)

// require_python_ skips the test unless a CPython with ``laila``,
// ``websockets`` and ``numpy`` is importable.
func require_python_(t *testing.T) (exe, root string) {
	t.Helper()
	if testing.Short() {
		t.Skip("interop harness spawns CPython peers; skipped under -short")
	}
	exe, root = python_exe_(), python_laila_root_()
	have_python_once.Do(func() {
		if exe == "" || root == "" {
			have_python_why = fmt.Sprintf("no CPython laila (python=%q, root=%q)", exe, root)
			return
		}
		probe := exec.Command(exe, "-c", "import laila, websockets, numpy")
		probe.Env = py_env_(root)
		if out, err := probe.CombinedOutput(); err != nil {
			have_python_why = fmt.Sprintf("no CPython laila (%s, PYTHONPATH=%s): %v: %s", exe, root, err, strings.TrimSpace(string(out)))
			return
		}
		have_python_ = true
	})
	if !have_python_ {
		t.Skip(have_python_why)
	}
	return exe, root
}

type py_server_ struct {
	proc *exec.Cmd
	info map[string]string
}

// spawn_py_server runs ``py_peer.py server <transport>`` and waits for
// ``READY``; the process is killed on test cleanup.
func spawn_py_server(t *testing.T, transport string) *py_server_ {
	t.Helper()
	exe, root := require_python_(t)
	proc := exec.Command(exe, py_peer_script_(), "server", transport)
	proc.Env = py_env_(root)
	proc.Stderr = os.Stderr
	stdout, err := proc.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := proc.Start(); err != nil {
		t.Fatal(err)
	}
	s := &py_server_{proc: proc, info: map[string]string{}}
	t.Cleanup(func() {
		_ = proc.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = proc.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = proc.Process.Kill()
			<-done
		}
	})

	lines := make(chan string)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- strings.TrimSpace(scanner.Text())
		}
	}()
	timeout := time.After(60 * time.Second)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("python peer exited before READY: %v", s.info)
			}
			if line == "READY" {
				// keep draining so the child never blocks on a full pipe
				go func() {
					for range lines {
					}
				}()
				return s
			}
			if k, v, ok := strings.Cut(line, "="); ok {
				s.info[k] = v
			}
		case <-timeout:
			t.Fatal("python peer did not become ready")
		}
	}
}

func (s *py_server_) port() int { return must(strconv.Atoi(s.info["PORT"])) }

func (s *py_server_) uri(transport string) string {
	if transport == "tcpip" {
		return fmt.Sprintf("ws://127.0.0.1:%d", s.port())
	}
	return fmt.Sprintf("tcp://127.0.0.1:%d", s.port())
}

// run_py_client runs ``py_peer.py client ...`` to completion and returns
// ``{check: payload}`` (each payload a *py.Dict decoded from the JSON
// line the Python side printed).
func run_py_client(t *testing.T, uri, secret, remote_id, entry_id string) map[string]*py.Dict {
	t.Helper()
	exe, root := require_python_(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	proc := exec.CommandContext(ctx, exe, py_peer_script_(), "client", uri, secret, remote_id, entry_id)
	proc.Env = py_env_(root)
	proc.Stderr = os.Stderr
	stdout, err := proc.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := proc.Start(); err != nil {
		t.Fatal(err)
	}
	checks := map[string]*py.Dict{}
	done := false
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 1<<20), 1<<24)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "DONE" {
			done = true
			continue
		}
		if strings.HasPrefix(line, "{") {
			v, err := py.Json_loads(line)
			if err != nil {
				t.Fatalf("bad JSON line from python client: %q: %v", line, err)
			}
			obj := v.(*py.Dict)
			name, _ := obj.Get("check")
			checks[py.Str(name)] = obj
		}
	}
	werr := proc.Wait()
	if !done || werr != nil {
		t.Fatalf("python client failed (%v; done=%v); checks so far: %s", werr, done, py.Repr(checks))
	}
	return checks
}

func check_(t *testing.T, checks map[string]*py.Dict, name string) *py.Dict {
	t.Helper()
	c, ok := checks[name]
	if !ok {
		t.Fatalf("python client reported no %q check; have %v", name, keys_(checks))
	}
	return c
}

func keys_(m map[string]*py.Dict) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func get_(d *py.Dict, k string) any { v, _ := d.Get(k); return v }

func new_transport_(transport string) laila.Comm_protocol_ {
	kw := py.Kw{"host": "127.0.0.1", "port": 0}
	if transport == "tcpip" {
		return must(laila.New_DefaultTCPIPProtocol(kw))
	}
	return must(laila.New_DefaultTCPProtocol(kw))
}

func bound_port_(proto laila.Comm_protocol_) int {
	return proto.(interface{ Bound_port() int }).Bound_port()
}

func secret_of_(proto laila.Comm_protocol_) string {
	return py.Str(must(py.Getattr(proto, "peer_secret_key")))
}

// ------------------------------------------------------------------ #
// Go client -> Python server
// ------------------------------------------------------------------ #

type goClient_ struct {
	transport string
	py        *py_server_
	local     *laila.LAILA_IDENTIFIABLE_POLICY_
	remote_id string
	pa        laila.Comm_protocol_
}

func (c *goClient_) uri() string { return c.py.uri(c.transport) }

func setUpGoClient_(t *testing.T, transport string) *goClient_ {
	t.Helper()
	py_srv := spawn_py_server(t, transport)
	original := must(laila.Get_active_policy())
	c := &goClient_{transport: transport, py: py_srv}
	t.Cleanup(func() {
		if c.local != nil {
			_ = comm_(c.local).Stop()
		}
		laila.Activate_policy(original)
	})
	c.local = must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.Activate_policy(c.local)
	pool := laila.As_pool_(must(laila.New_DefaultPool()))
	must(0, laila.As_central_memory_(c.local.Central.Memory).Extend(pool, py.Kw{"pool_nickname": "go-local"}))
	c.pa = new_transport_(transport)
	must(0, comm_(c.local).Add_connection(c.pa))
	c.remote_id = must(laila.Add_peer(c.uri(), py_srv.info["SECRET"]))
	return c
}

func TestGoClientToPythonServer(t *testing.T) {
	for _, transport := range []string{"tcpip", "tcp"} {
		t.Run(transport, func(t *testing.T) {
			c := setUpGoClient_(t, transport)

			t.Run("handshake_yields_the_python_policy_id", func(t *testing.T) {
				assertEq(t, c.remote_id, c.py.info["POLICY_ID"])
				assertTrue(t, comm_(c.local).Peers.Contains(c.remote_id), "remote_id in laila.peers")
			})

			t.Run("ping_and_rpc_echo_of_mixed_json_types", func(t *testing.T) {
				assertTrue(t, c.pa.(interface {
					Ping(peer_id string, timeout ...any) bool
				}).Ping(c.remote_id), "ping")
				proxy := peer_proxy_(t, c.local, c.remote_id)
				payload := py.DictOf(
					"k", py.NewList(1, 2.5, "s", nil, true),
					"u", "ключ 键 🔑",
					"nested", py.DictOf("a", py.NewDict()),
				)
				assertEq(t, must(proxy.Attr("echo").Invoke(payload)), payload)
				assertEq(t, must(proxy.Attr("echo").Invoke("plain")), "plain")
				assertEq(t, must(proxy.Attr("peer_count").Invoke()), 1)
			})

			t.Run("remember_policy_id_py_rebuilds_the_cpython_entry_in_go", func(t *testing.T) {
				res := must(laila.Remember(py.Kw{
					"entry_ids": c.py.info["ENTRY_ID"], "pool_nickname": "remote-store",
					"policy_id": c.remote_id, "persist": false,
				}))
				assertEq(t, must(py.Getattr(must(laila.Get_active_policy()), "global_id")), c.local.Global_id())
				assert_reference(t, data_of_(t, res), "hello-from-python")
				entry, ok := wait_if_future_(t, res, 10).(*laila.Entry)
				assertTrue(t, ok, "res.wait() is an Entry")
				assertEq(t, entry.Global_id(), c.py.info["ENTRY_ID"])
			})

			t.Run("remember_policy_id_py_of_a_numpy_array_yields_an_ndarray", func(t *testing.T) {
				res := must(laila.Remember(py.Kw{
					"entry_ids": c.py.info["ARRAY_ENTRY_ID"], "pool_nickname": "remote-store",
					"policy_id": c.remote_id, "persist": false,
				}))
				entry := wait_if_future_(t, res, 10).(*laila.Entry)
				arr, ok := data_of_(t, entry).(*py.NDArray)
				assertTrue(t, ok, "data is an NDArray")
				assertEq(t, arr.Dtype, "<f4")
				assertEq(t, arr.Shape, []int{3, 4})
				assertEq(t, must(arr.Tolist()), py.NewList(
					py.NewList(0.0, 1.0, 2.0, 3.0),
					py.NewList(4.0, 5.0, 6.0, 7.0),
					py.NewList(8.0, 9.0, 10.0, 11.0),
				))
			})

			t.Run("remember_policy_id_py_with_default_persist_resolves_to_the_rebuilt_entry", func(t *testing.T) {
				// The peer path ignores ``persist`` (as in Python: only ``pool``
				// is forwarded); the local future resolves to the rebuilt Entry.
				fut := must(laila.Remember(py.Kw{
					"entry_ids": c.py.info["ENTRY_ID"], "pool_nickname": "remote-store", "policy_id": c.remote_id,
				}))
				entry, ok := wait_if_future_(t, fut, 10).(*laila.Entry)
				assertTrue(t, ok, "fut.wait() is an Entry")
				assertEq(t, entry.Global_id(), c.py.info["ENTRY_ID"])
				assertEq(t, must(py.Getattr(must(laila.Get_active_policy()), "global_id")), c.local.Global_id())
			})

			t.Run("memorize_policy_id_py_stores_a_go_entry_in_the_cpython_pool", func(t *testing.T) {
				e1 := must(laila.Constant(py.DictOf("from", "go", "n", py.NewList(1, 2, 3), "f", 0.5, "t", true, "z", nil), py.Kw{"nickname": "go-push"}))
				e2 := must(laila.Constant(py.NDArray_int64([]int{2, 3}, []int64{1, 1, 1, 1, 1, 1}), py.Kw{"nickname": "go-push-arr"}))
				f1 := must(laila.Memorize(py.Kw{"entries": e1, "policy_id": c.remote_id, "pool_nickname": "remote-store"}))
				f2 := must(laila.Memorize(py.Kw{"entries": e2, "policy_id": c.remote_id, "pool_nickname": "remote-store"}))
				assertEq(t, data_of_(t, f1), e1.Global_id())
				assertEq(t, data_of_(t, f2), e2.Global_id())
				proxy := peer_proxy_(t, c.local, c.remote_id)
				assertEq(t, must(proxy.Attr("probe_entry").Invoke(e1.Global_id())), py.DictOf(
					"present", true, "type", "dict",
					"data", py.DictOf("from", "go", "n", py.NewList(1, 2, 3), "f", 0.5, "t", true, "z", nil),
				))
				probe_arr := must(proxy.Attr("probe_entry").Invoke(e2.Global_id())).(*py.Dict)
				assertEq(t, get_(probe_arr, "sum"), 6.0) // CPython ``6.0``
				must(0, probe_arr.Del("sum"))
				assertEq(t, probe_arr, py.DictOf("present", true, "type", "ndarray", "dtype", "int64", "shape", py.NewList(2, 3)))
				assertEq(t, must(proxy.Attr("probe_entry").Invoke("LAILA:ENTRY:absent")), py.DictOf("present", false))
				// and it round-trips back through CPython's pickle
				back := wait_if_future_(t, must(laila.Remember(e2.Global_id(), py.Kw{
					"policy_id": c.remote_id, "pool_nickname": "remote-store", "persist": false,
				})), 10).(*laila.Entry)
				arr, ok := data_of_(t, back).(*py.NDArray)
				assertTrue(t, ok, "back.data is an NDArray")
				assertEq(t, must(arr.Tolist()), py.NewList(py.NewList(1, 1, 1), py.NewList(1, 1, 1)))
			})

			t.Run("256kb_payload_crosses_the_wire_in_both_directions", func(t *testing.T) {
				// Regression: frames above ~2 * StreamReader.limit (128 KiB)
				// deadlocked the raw-TCP reader (transport paused, never resumed)
				// on the way in and out.
				big := blob_(256 * 1024)
				e := must(laila.Constant(big, py.Kw{"nickname": "go-big"}))
				wait_if_future_(t, must(laila.Memorize(e, py.Kw{"policy_id": c.remote_id, "pool_nickname": "remote-store"})), 30)
				back := wait_if_future_(t, must(laila.Remember(e.Global_id(), py.Kw{
					"policy_id": c.remote_id, "pool_nickname": "remote-store", "persist": false,
				})), 30).(*laila.Entry)
				got, ok := data_of_(t, back).([]byte)
				assertTrue(t, ok, "back.data is bytes")
				assertEq(t, len(got), len(big))
				assertEq(t, py.Sha1_hex(got), py.Sha1_hex(big))
			})

			t.Run("forget_policy_py_deletes_on_the_cpython_side", func(t *testing.T) {
				e := must(laila.Constant(py.DictOf("bye", 1), py.Kw{"nickname": "go-forget"}))
				wait_if_future_(t, must(laila.Memorize(e, py.Kw{"policy_id": c.remote_id, "pool_nickname": "remote-store"})), 10)
				proxy := peer_proxy_(t, c.local, c.remote_id)
				present := func() bool {
					v := must(proxy.Attr("probe_entry").Invoke(e.Global_id())).(*py.Dict)
					return py.Eq(get_(v, "present"), true)
				}
				assertTrue(t, present(), "entry present before forget")
				f := must(laila.Forget(e.Global_id(), py.Kw{"policy": c.remote_id, "pool": "remote-store"}))
				wait_if_future_(t, f, 10)
				assertTrue(t, wait_until(func() bool { return !present() }, 10), "entry still present on the CPython side")
			})

			t.Run("stream_lane_go_sends_cpython_echoes_reversed", func(t *testing.T) {
				if transport == "tcpip" {
					t.Skip("WebSocket transport carries RPC only")
				}
				ch := must(peer_proxy_(t, c.local, c.remote_id).Channel("video"))
				assertTrue(t, ch.Opened(), "channel opened")
				payloads := make([][]byte, len(STREAM_SIZES))
				for i, n := range STREAM_SIZES {
					payloads[i] = blob_(n)
					must(0, ch.Send(payloads[i]))
				}
				timeout := 20.0
				relay := ch.Relay(&timeout)
				for i, p := range payloads {
					g := must(relay.Next())
					assertEq(t, bytes_of_(t, g), reversed_(p))
					assertEq(t, g.Stream().Seq, i)
					assertEq(t, g.Stream().Peer_id, c.remote_id)
					assertEq(t, g.Stream().Channel, "video")
				}
				assertEq(t, ch.Dropped(), 0)
				assertEq(t, ch.Tx_seq(), len(payloads))
				ch.Close()
			})

			t.Run("remote_error_surfaces_as_a_go_error", func(t *testing.T) {
				_, err := peer_proxy_(t, c.local, c.remote_id).Attr("no_such_method").Invoke()
				if err == nil {
					t.Fatal("expected an error from the remote")
				}
				msg := err.Error()
				if !(errors.Is(err, py.AttributeError) || strings.Contains(msg, "AttributeError") ||
					strings.Contains(msg, "no attribute") || strings.Contains(msg, "Remote RPC error")) {
					t.Fatalf("unexpected remote error: %v", err)
				}
			})

			t.Run("remove_peer_is_noticed_by_cpython_re_peer_works", func(t *testing.T) {
				proxy := peer_proxy_(t, c.local, c.remote_id)
				assertEq(t, must(proxy.Attr("peer_count").Invoke()), 1)
				comm_(c.local).Remove_peer(c.remote_id)
				assertTrue(t, !comm_(c.local).Peers.Contains(c.remote_id), "remote_id still in laila.peers")
				sleep(0.3)
				again := must(laila.Add_peer(c.uri(), c.py.info["SECRET"]))
				assertEq(t, again, c.remote_id)
				assertTrue(t, wait_until(func() bool {
					v, err := peer_proxy_(t, c.local, c.remote_id).Attr("peer_count").Invoke()
					return err == nil && py.Eq(v, 1)
				}, 5), "CPython did not see the re-peer")
			})
		})
	}
}

// ------------------------------------------------------------------ #
// Python client -> Go server
// ------------------------------------------------------------------ #

type goServer_ struct {
	policy    *laila.LAILA_IDENTIFIABLE_POLICY_
	pool      *laila.LAILA_IDENTIFIABLE_POOL_
	proto     laila.Comm_protocol_
	ref_entry *laila.Entry
	checks    map[string]*py.Dict
}

// echo_lane_ echoes every message on stream lane *name* back reversed --
// mirror of ``py_peer._echo_lane``. Ends when *stop* is closed.
func echo_lane_(policy *laila.LAILA_IDENTIFIABLE_POLICY_, name string, stop <-chan struct{}) {
	comm := comm_(policy)
	for {
		var proxy *laila.PeerProxy
		for proxy == nil {
			select {
			case <-stop:
				return
			default:
			}
			for _, pid := range comm.Peers.Keys() {
				v, err := comm.Peers.Getitem(pid)
				if err != nil {
					continue
				}
				p, ok := v.(*laila.PeerProxy)
				if !ok {
					continue
				}
				names, err := p.Channels()
				if err != nil {
					continue
				}
				for _, n := range names {
					if n == name {
						proxy = p
						break
					}
				}
				if proxy != nil {
					break
				}
			}
			if proxy == nil {
				sleep(0.02)
			}
		}
		ch, err := proxy.Channel(name)
		if err == nil {
			_ = ch.Relay(nil).Range(func(e *laila.StreamEntry) error {
				d, err := e.Data()
				if err != nil {
					return err
				}
				b, _ := d.([]byte)
				return ch.Send(reversed_(b))
			})
		}
		sleep(0.05)
	}
}

func setUpGoServer_(t *testing.T, transport string) *goServer_ {
	t.Helper()
	require_python_(t)
	original := must(laila.Get_active_policy())
	s := &goServer_{}
	stop := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		if s.policy != nil {
			_ = comm_(s.policy).Stop()
		}
		laila.Activate_policy(original)
	})
	s.policy = must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.Activate_policy(s.policy)
	s.pool = laila.As_pool_(must(laila.New_DefaultPool()))
	must(0, laila.As_central_memory_(s.policy.Central.Memory).Extend(s.pool, py.Kw{"pool_nickname": "remote-store"}))
	s.ref_entry = must(laila.Constant(reference_payload_("hello-from-go"), py.Kw{"nickname": "go-ref"}))
	wait_if_future_(t, must(laila.Memorize(s.ref_entry, py.Kw{"pool_nickname": "remote-store"})), 10)
	// RPC helpers the Python side calls (``object.__setattr__(policy, ...)``).
	s.policy.Private_set("echo", func(x any) any { return x })
	s.policy.Private_set("probe_entry", func(gid string) (any, error) { return probe_entry_(s.pool, gid) })
	s.proto = new_transport_(transport)
	must(0, comm_(s.policy).Add_connection(s.proto))

	uri := fmt.Sprintf("tcp://127.0.0.1:%d", bound_port_(s.proto))
	if transport == "tcpip" {
		uri = fmt.Sprintf("ws://127.0.0.1:%d", bound_port_(s.proto))
	}
	// Echo lane "video" back reversed while the CPython client runs
	// (mirror of ``py_peer._echo_lane``); ends when the lane closes.
	if transport != "tcpip" {
		go echo_lane_(s.policy, "video", stop)
	}
	s.checks = run_py_client(t, uri, secret_of_(s.proto), s.policy.Global_id(), s.ref_entry.Global_id())
	return s
}

func TestPythonClientToGoServer(t *testing.T) {
	for _, transport := range []string{"tcpip", "tcp"} {
		t.Run(transport, func(t *testing.T) {
			s := setUpGoServer_(t, transport)

			t.Run("cpython_peered_to_the_go_policy", func(t *testing.T) {
				peered := check_(t, s.checks, "peered")
				if !py.Eq(get_(peered, "ok"), true) {
					t.Fatal(py.Repr(peered))
				}
				assertEq(t, get_(check_(t, s.checks, "ping"), "ok"), true)
			})

			t.Run("rpc_echo_answered_by_go", func(t *testing.T) {
				assertEq(t, get_(check_(t, s.checks, "echo"), "value"),
					py.DictOf("k", py.NewList(1, 2.5, "s", nil, true), "u", "ключ"))
			})

			t.Run("cpython_remember_policy_id_go_rebuilt_the_go_entry", func(t *testing.T) {
				r := check_(t, s.checks, "remember")
				assertEq(t, get_(r, "gid"), s.ref_entry.Global_id())
				assertEq(t, get_(r, "active_restored"), true)
				assert_reference(t, get_(r, "data"), "hello-from-go")
			})

			t.Run("cpython_memorize_policy_id_go_landed_in_the_go_pool", func(t *testing.T) {
				m := check_(t, s.checks, "memorize")
				assertEq(t, get_(m, "gids"), get_(m, "expected"))
				expected := get_(m, "expected").(*py.List)
				gid_dict, gid_arr := py.Str(expected.Items()[0]), py.Str(expected.Items()[1])
				assertTrue(t, must(s.pool.Contains(gid_dict)), "dict entry not in pool")
				assertTrue(t, must(s.pool.Contains(gid_arr)), "array entry not in pool")
				assertEq(t, data_of_(t, must(entry_of_(s.pool, gid_dict))), py.DictOf("from", "python", "n", py.NewList(1, 2, 3)))
				arr, ok := data_of_(t, must(entry_of_(s.pool, gid_arr))).(*py.NDArray)
				assertTrue(t, ok, "array entry data is not an NDArray")
				assertEq(t, arr.Dtype, "<i8")
				assertEq(t, arr.Shape, []int{2, 3})
				assertEq(t, must(arr.Tolist()), py.NewList(py.NewList(4, 4, 4), py.NewList(4, 4, 4)))
				// what CPython saw through the probe RPC matches
				probe := check_(t, s.checks, "probe")
				assertEq(t, get_(probe, "pushed"), py.DictOf("present", true, "type", "dict", "data", py.DictOf("from", "python", "n", py.NewList(1, 2, 3))))
				assertEq(t, get_(probe, "pushed_arr"), py.DictOf("present", true, "type", "ndarray", "dtype", "<i8", "shape", py.NewList(2, 3), "sum", 24.0))
			})

			t.Run("cpython_remembered_its_numpy_array_back_from_go_intact", func(t *testing.T) {
				assertEq(t, check_(t, s.checks, "remember_array"), py.DictOf(
					"check", "remember_array", "is_ndarray", true, "dtype", "int64", "shape", py.NewList(2, 3), "sum", 24.0,
				))
			})

			t.Run("stream_lane_cpython_sends_go_echoes_reversed", func(t *testing.T) {
				if transport == "tcpip" {
					t.Skip("WebSocket transport carries RPC only")
				}
				st := check_(t, s.checks, "stream")
				sent := make([]any, len(STREAM_SIZES))
				seqs := make([]any, len(STREAM_SIZES))
				for i, n := range STREAM_SIZES {
					sent[i] = py.Sha1_hex(blob_(n))
					seqs[i] = i
				}
				assertEq(t, get_(st, "sent"), py.NewList(sent...))
				assertEq(t, get_(st, "got"), get_(st, "sent"))
				assertEq(t, get_(st, "seqs"), py.NewList(seqs...))
				assertEq(t, get_(st, "channel"), py.NewList("video"))
				assertEq(t, get_(st, "peer"), py.NewList(s.policy.Global_id()))
				assertEq(t, get_(st, "dropped"), 0)
			})

			t.Run("cpython_remove_peer_was_honoured", func(t *testing.T) {
				assertEq(t, get_(check_(t, s.checks, "removed"), "has_peer"), false)
				assertTrue(t, wait_until(func() bool { return comm_(s.policy).Peers.Len() == 0 }, 5), "peer registry not emptied")
			})
		})
	}
}
