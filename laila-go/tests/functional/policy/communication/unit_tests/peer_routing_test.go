// Port of tests/functional/policy/communication/unit_tests/test_peer_routing.py.
//
// Tests for peer-routed memory: the policy_id kwarg on remember/memorize.
//
// Covers three layers:
//
//  1. The transport *token* registry (``matches_token`` /
//     ``_resolve_protocol_for_token``) and the LoRa / Bluetooth protocol
//     scaffolds -- pure, no networking.
//  2. ``_route_to_policy`` semantics against a second *local* policy:
//     ``memorize(policy_id=...)`` / ``remember(policy_id=...)`` round-trip
//     and active-policy restoration.
//  3. An end-to-end ``remember(policy_id=...)`` round-trip against a real
//     peer running in a **subprocess** (the production cross-process path
//     -- a single process cannot host the full future round-trip because
//     the active-policy pointer is process-global).
//
// Layer 3 runs the peer as a *Python* laila process: it is the Go<->Python
// interop harness for the WebSocket (tcpip) wire. It is skipped when no
// importable ``laila`` Python package is reachable from this checkout.
package unit_tests

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// ------------------------------------------------------------------ #
// 1. Transport token registry + protocol scaffolds (no networking)
// ------------------------------------------------------------------ #

func TestProtocolTokens_tcpip_protocol_name(t *testing.T) {
	assertEq(t, laila.Comm_protocol_name_of_(laila.DefaultTCPIPProtocol), "tcpip")
}

func TestProtocolTokens_tcpip_matches_tokens_and_aliases(t *testing.T) {
	for _, token := range []string{"tcpip", "TCPIP", "ws", "wss", "websocket"} {
		if !laila.Comm_protocol_matches_token_(laila.DefaultTCPIPProtocol, token) {
			t.Fatal(token)
		}
	}
}

func TestProtocolTokens_tcpip_no_longer_claims_raw_tcp_token(t *testing.T) {
	// raw TCP is now a dedicated transport; tcpip is WebSocket-only.
	if laila.Comm_protocol_matches_token_(laila.DefaultTCPIPProtocol, "tcp") {
		t.Fatal("tcpip must not claim 'tcp'")
	}
}

func TestProtocolTokens_tcpip_rejects_other_tokens(t *testing.T) {
	if laila.Comm_protocol_matches_token_(laila.DefaultTCPIPProtocol, "lora") {
		t.Fatal("lora")
	}
	if laila.Comm_protocol_matches_token_(laila.DefaultTCPIPProtocol, "bluetooth") {
		t.Fatal("bluetooth")
	}
}

func TestProtocolTokens_lora_scaffold(t *testing.T) {
	assertEq(t, laila.Comm_protocol_name_of_(laila.DefaultLoRaProtocol), "lora")
	if !laila.Comm_protocol_matches_token_(laila.DefaultLoRaProtocol, "lora") {
		t.Fatal("lora token")
	}
	if !laila.Comm_protocol_can_handle_uri_(laila.DefaultLoRaProtocol, "lora://node-7") {
		t.Fatal("lora://node-7")
	}
	if laila.Comm_protocol_can_handle_uri_(laila.DefaultLoRaProtocol, "ws://host:1") {
		t.Fatal("ws://host:1")
	}
	proto := must(laila.New_DefaultLoRaProtocol())
	if proto.Has_peer("anyone") {
		t.Fatal("has_peer")
	}
	assertRaises(t, py.NotImplementedError, proto.Start())
	_, err := proto.Connect("lora://x", "secret")
	assertRaises(t, py.NotImplementedError, err)
	_, err = proto.Send_rpc("x", []string{"a"}, nil, py.Kw{})
	assertRaises(t, py.NotImplementedError, err)
}

func TestProtocolTokens_bluetooth_scaffold(t *testing.T) {
	assertEq(t, laila.Comm_protocol_name_of_(laila.DefaultBluetoothProtocol), "bluetooth")
	for _, token := range []string{"bluetooth", "bt", "ble", "BT"} {
		if !laila.Comm_protocol_matches_token_(laila.DefaultBluetoothProtocol, token) {
			t.Fatal(token)
		}
	}
	if !laila.Comm_protocol_can_handle_uri_(laila.DefaultBluetoothProtocol, "bt://AA:BB:CC") {
		t.Fatal("bt://AA:BB:CC")
	}
	if laila.Comm_protocol_can_handle_uri_(laila.DefaultBluetoothProtocol, "lora://x") {
		t.Fatal("lora://x")
	}
	proto := must(laila.New_DefaultBluetoothProtocol())
	if proto.Has_peer("anyone") {
		t.Fatal("has_peer")
	}
	assertRaises(t, py.NotImplementedError, proto.Start())
}

func TestResolveProtocolForToken_no_connections_raises(t *testing.T) {
	hub := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "test"}))
	_, err := hub.Resolve_protocol_for_token_("tcpip")
	assertRaises(t, py.ConnectionError, err)
}

func TestResolveProtocolForToken_none_defaults_to_tcpip(t *testing.T) {
	hub := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "test"}))
	tcp := must(laila.New_DefaultTCPIPProtocol())
	must(0, hub.Add_connection(tcp))
	defer hub.Stop()
	assertIs_(t, must(hub.Resolve_protocol_for_token_(nil)), tcp)
	assertIs_(t, must(hub.Resolve_protocol_for_token_("websocket")), tcp)
}

func TestResolveProtocolForToken_unregistered_token_raises(t *testing.T) {
	hub := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "test"}))
	tcp := must(laila.New_DefaultTCPIPProtocol())
	must(0, hub.Add_connection(tcp))
	defer hub.Stop()
	_, err := hub.Resolve_protocol_for_token_("lora")
	assertRaises(t, py.ConnectionError, err)
}

// ------------------------------------------------------------------ #
// 2. policy_id routing + active-policy restoration (local target)
// ------------------------------------------------------------------ #

type policyIdLocalRouting_ struct {
	A, L *laila.LAILA_IDENTIFIABLE_POLICY_
	pool *laila.LAILA_IDENTIFIABLE_POOL_
}

func setUpPolicyIdLocalRouting_(t *testing.T) *policyIdLocalRouting_ {
	t.Helper()
	original := must(laila.Get_active_policy())
	s := &policyIdLocalRouting_{
		A: must(laila.New_LAILA_IDENTIFIABLE_POLICY_()),
		L: must(laila.New_LAILA_IDENTIFIABLE_POLICY_()),
	}
	laila.Activate_policy(s.L) // register L in _local_policies
	s.pool = laila.As_pool_(must(laila.New_DefaultPool()))
	must(0, laila.As_central_memory_(s.L.Central.Memory).Extend(s.pool, py.Kw{"pool_nickname": "l-store"}))
	laila.Activate_policy(s.A)
	t.Cleanup(func() { laila.Activate_policy(original) })
	return s
}

// wait_if_future_ is ``if fut is not None and hasattr(fut, "wait"): fut.wait(timeout=10)``.
func wait_if_future_(t *testing.T, fut any) {
	t.Helper()
	if py.IsNone(fut) {
		return
	}
	if w, ok := fut.(interface{ Wait(timeout ...any) (any, error) }); ok {
		must(w.Wait(10))
	}
}

func data_of_(t *testing.T, ref any) any {
	t.Helper()
	d, ok := ref.(interface{ Data() (any, error) })
	if !ok {
		t.Fatalf("%T has no .data", ref)
	}
	return must(d.Data())
}

func active_gid_(t *testing.T) string {
	t.Helper()
	return py.Str(must(py.Getattr(must(laila.Get_active_policy()), "global_id")))
}

func TestPolicyIdLocalRouting_unknown_policy_id_raises(t *testing.T) {
	setUpPolicyIdLocalRouting_(t)
	_, err := laila.Memorize(py.Kw{"entries": must(laila.Constant(1)), "policy_id": "LAILA:POLICY:nope"})
	assertRaises(t, py.ConnectionError, err)
}

func TestPolicyIdLocalRouting_memorize_into_local_target_and_active_restored(t *testing.T) {
	s := setUpPolicyIdLocalRouting_(t)
	entry := must(laila.Constant(map[string]any{"x": 1}, py.Kw{"nickname": "le"}))
	gid := entry.Global_id()

	fut := must(laila.Memorize(py.Kw{"entries": entry, "policy_id": s.L.Global_id(), "pool_nickname": "l-store"}))
	wait_if_future_(t, fut)

	// active policy restored to A after the routed call
	assertEq(t, active_gid_(t), s.A.Global_id())
	// the entry physically landed in L's pool, not A's
	if !must(s.pool.Contains(gid)) {
		t.Fatalf("%s not in L's pool", gid)
	}
}

func TestPolicyIdLocalRouting_remember_from_local_target_round_trips(t *testing.T) {
	s := setUpPolicyIdLocalRouting_(t)
	entry := must(laila.Constant(map[string]any{"msg": "round-trip"}, py.Kw{"nickname": "le2"}))
	gid := entry.Global_id()
	fut := must(laila.Memorize(py.Kw{"entries": entry, "policy_id": s.L.Global_id(), "pool_nickname": "l-store"}))
	wait_if_future_(t, fut)

	res := must(laila.Remember(py.Kw{
		"entry_ids":     gid,
		"policy_id":     s.L.Global_id(),
		"pool_nickname": "l-store",
		"persist":       false,
	}))
	assertEq(t, active_gid_(t), s.A.Global_id())
	assertEq(t, data_of_(t, res), py.DictOf("msg", "round-trip"))
}

// ------------------------------------------------------------------ #
// 3. End-to-end remember(policy_id=...) against a subprocess peer
// ------------------------------------------------------------------ #

// REMOTE_SCRIPT_ is ``_REMOTE_SCRIPT``: the Python-side peer.
const REMOTE_SCRIPT_ = `
import time, uuid, laila
from laila.macros.defaults import DefaultPolicy, DefaultPool, DefaultTCPIPProtocol

policy = DefaultPolicy()
laila.activate_policy(policy)
laila.memory.extend(pool=DefaultPool(), pool_nickname="remote-store")

entry = laila.constant(data={"message": "hello-from-remote"}, nickname="remote-entry")
with laila.guarantee:
    laila.memorize(entries=entry, pool_nickname="remote-store")

tcp = DefaultTCPIPProtocol(host="127.0.0.1", port=0, peer_secret_key=uuid.uuid4().hex)
laila.communication.add_connection(tcp)

print(f"PORT={tcp.bound_port}", flush=True)
print(f"SECRET={tcp.peer_secret_key}", flush=True)
print(f"ENTRY_ID={entry.global_id}", flush=True)
print(f"POLICY_ID={policy.global_id}", flush=True)
print("READY", flush=True)
while True:
    time.sleep(1)
`

// python_laila_root_ locates the directory that makes ``import laila``
// resolve to the reference Python package of this checkout (the parent of
// the ``laila/`` package directory, i.e. of the repository root), or ""
// when it cannot be found.
func python_laila_root_() string {
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

// python_exe_ is ``sys.executable`` for the subprocess peer: honours
// LAILA_PYTHON, else the first ``python3`` / ``python`` on PATH.
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

type peerRoutingSubprocess_ struct {
	proc      *exec.Cmd
	info      map[string]string
	local     *laila.LAILA_IDENTIFIABLE_POLICY_
	remote_id string
}

func setUpPeerRoutingSubprocess_(t *testing.T) *peerRoutingSubprocess_ {
	t.Helper()
	exe := python_exe_()
	root := python_laila_root_()
	if exe == "" || root == "" {
		t.Skip("no Python interpreter / laila package available for the subprocess peer")
	}
	probe := exec.Command(exe, "-c", "import laila")
	probe.Env = append(os.Environ(), "PYTHONPATH="+root)
	if out, err := probe.CombinedOutput(); err != nil {
		t.Skipf("python laila is not importable (%v): %s", err, strings.TrimSpace(string(out)))
	}

	original := must(laila.Get_active_policy())
	proc := exec.Command(exe, "-c", REMOTE_SCRIPT_)
	proc.Env = append(os.Environ(), "PYTHONPATH="+root)
	proc.Stderr = os.Stderr
	stdout, err := proc.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := proc.Start(); err != nil {
		t.Fatal(err)
	}
	s := &peerRoutingSubprocess_{proc: proc, info: map[string]string{}}
	t.Cleanup(func() {
		if s.local != nil {
			_ = comm(s.local).Stop()
		}
		_ = proc.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = proc.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = proc.Process.Kill()
			<-done
		}
		laila.Activate_policy(original)
	})

	scanner := bufio.NewScanner(stdout)
	ready := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "READY" {
			ready = true
			break
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			s.info[k] = v
		}
	}
	if !ready {
		t.Fatalf("python peer exited before READY: %v", s.info)
	}
	s.local = must(laila.New_LAILA_IDENTIFIABLE_POLICY_())
	laila.Activate_policy(s.local)
	hub := laila.As_communication_(must(laila.Communication()))
	must(0, hub.Add_connection(must(laila.New_DefaultTCPIPProtocol(py.Kw{
		"host": "127.0.0.1", "port": 0, "peer_secret_key": s.info["SECRET"],
	}))))
	port, err := strconv.Atoi(s.info["PORT"])
	if err != nil {
		t.Fatal(err)
	}
	s.remote_id = must(hub.Add_tcpip_peer("127.0.0.1", port, s.info["SECRET"]))
	sleep(0.3)
	return s
}

func TestPeerRoutingSubprocess_peered_with_subprocess_policy(t *testing.T) {
	s := setUpPeerRoutingSubprocess_(t)
	assertEq(t, s.remote_id, s.info["POLICY_ID"])
}

func TestPeerRoutingSubprocess_remember_with_policy_id_fetches_from_peer(t *testing.T) {
	s := setUpPeerRoutingSubprocess_(t)
	// Remote remember now returns the rebuilt Entry directly (real data
	// crosses the wire, no shared pool / gid round-trip needed).
	res := must(laila.Remember(py.Kw{
		"entry_ids":     s.info["ENTRY_ID"],
		"pool_nickname": "remote-store",
		"policy_id":     s.info["POLICY_ID"],
		"persist":       false,
	}))
	// active policy restored after the routed call
	assertEq(t, active_gid_(t), s.local.Global_id())
	// remember(policy_id=) returns a LOCAL future (A-owned); .data is the
	// payload, .wait() the rebuilt Entry -- real data crossed the wire.
	assertEq(t, data_of_(t, res), py.DictOf("message", "hello-from-remote"))
	waited := must(res.(interface{ Wait(timeout ...any) (any, error) }).Wait())
	assertEq(t, must(py.Getattr(waited, "global_id")), s.info["ENTRY_ID"])
}
