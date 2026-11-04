// Go ``laila`` peer harness for the N-language interop matrix.
//
// Port of tests/interop/harness/py_harness.py: implements the harness CLI
// contract documented in ``tests/interop/README.md`` (the same contract
// ``laila-c/tests/interop/c_harness.cpp`` and
// ``laila-js/tests/interop/js_harness.js`` implement), so the orchestrator
// in ``tests/interop/`` can drive an unmodified Go ``laila`` policy exactly
// like any other language.
//
// Modes
//
//	serve <scheme> <port> <secret> <value> [peer_uri ...]
//	    Host a policy listening on <scheme> (``tcp`` or ``ws``); seed <value>
//	    into the alpha pool and into a named ``remote-store`` pool; peer with
//	    every <peer_uri> using <secret>. Prints ``POLICY``, ``ALPHA_GID``,
//	    ``STORE_GID``, one ``PEER <uri> <gid>`` per peer, then ``READY <port>``
//	    and idles until SIGTERM / SIGINT.
//	remember <uri> <secret> <gid> [pool_nickname]      -> VALUE <string>
//	memorize <uri> <secret> <value> [pool_nickname]    -> MEMO <gid>
//	memorize_typed <uri> <secret> <kind> [pool_nickname]
//	                                                   -> MEMO <gid>
//	remember_typed <uri> <secret> <gid> <kind>         -> TYPED_OK <kind> | TYPED_MISMATCH <kind> <repr>
//	memorize_json <uri> <secret> <json> [pool_nickname]
//	                                                   -> MEMO <gid>
//	remember_json <uri> <secret> <gid> [pool_nickname] -> JSON <json>
//	memorize_bytes <uri> <secret> <n> <seed> [pool_nickname]
//	                                                   -> MEMO <gid> <fnv1a64>
//	remember_hash <uri> <secret> <gid> [pool_nickname] -> HASH <fnv1a64> <len>
//	relay <via_uri> <secret> <from_policy_gid> <gid> [pool_nickname]
//	                                                   -> VALUE <string>
//	forget <uri> <secret> <gid> [pool_nickname]        -> FORGOT <gid>
//
// Any failure prints ``ERROR <message>`` and exits 1.
//
// Build: ``cd laila-go/tests && go build -o interop/go_harness/go_harness ./interop/go_harness``
// (``tests/interop/langs/go.py`` does this on demand with
// ``LAILA_INTEROP_BUILD_GO=1``).
package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

const STORE = "remote-store"

// TYPED holds the fixed typed payloads. Every harness must build
// byte-compatible values for these kinds; keep in sync with ``TYPED`` in
// py_harness.py / js_harness.js and ``build_typed`` in c_harness.cpp.
var TYPED = map[string]any{
	"int":    1234567,
	"double": 3.5,
	"bool":   true,
	"bytes":  []byte{0x00, 0x01, 0xfe, 0xff},
	"dict":   py.DictOf("a", 1, "b", py.NewList(1, 2, 3)),
	"numpy":  py.NDArray_float32([]int{2, 2}, []float32{1, 2, 3, 4}),
}

var stdout = bufio.NewWriter(os.Stdout)

func say(line string) {
	_, _ = stdout.WriteString(line + "\n")
	_ = stdout.Flush()
}

// xorshift_bytes is the deterministic byte stream shared by every harness:
// xorshift32 seeded with *seed* (forced non-zero), one low byte per step.
// Keep in sync with ``xorshift_bytes`` in py_harness.py / js_harness.js /
// c_harness.cpp.
func xorshift_bytes(n int, seed uint32) []byte {
	x := seed
	if x == 0 {
		x = 1
	}
	out := make([]byte, n)
	for i := range out {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		out[i] = byte(x & 0xff)
	}
	return out
}

// fnv1a64 is FNV-1a 64 as 16 lowercase hex digits (laila-C has no SHA-256,
// so this is the common checksum across harnesses).
func fnv1a64(data []byte) string {
	h := uint64(0xCBF29CE484222325)
	for _, b := range data {
		h = (h ^ uint64(b)) * 0x100000001B3
	}
	return fmt.Sprintf("%016x", h)
}

// wait_ is ``fut.wait(timeout=60)`` when *fut* has a ``wait``; else *fut*.
func wait_(fut any, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	if py.IsNone(fut) {
		return nil, nil
	}
	if w, ok := fut.(interface{ Wait(timeout ...any) (any, error) }); ok {
		return w.Wait(60.0)
	}
	return fut, nil
}

func activate_() (*laila.LAILA_IDENTIFIABLE_POLICY_, error) {
	policy, err := laila.New_LAILA_IDENTIFIABLE_POLICY_()
	if err != nil {
		return nil, err
	}
	laila.Activate_policy(policy)
	return policy, nil
}

func comm_() (*laila.LAILA_IDENTIFIABLE_COMMUNICATION_, error) {
	c, err := laila.Communication()
	if err != nil {
		return nil, err
	}
	return laila.As_communication_(c), nil
}

// add_transports_ registers a ``tcp://`` and a ``ws://`` protocol on the
// active policy so it can dial either scheme; the one matching
// *serve_scheme* binds *port*.
func add_transports_(secret string, serve_scheme string, port int) (map[string]laila.Comm_protocol_, error) {
	comm, err := comm_()
	if err != nil {
		return nil, err
	}
	protos := map[string]laila.Comm_protocol_{}
	for _, scheme := range []string{"tcp", "ws"} {
		p := 0
		if scheme == serve_scheme {
			p = port
		}
		kw := py.Kw{"host": "127.0.0.1", "port": p, "peer_secret_key": secret}
		var proto laila.Comm_protocol_
		if scheme == "tcp" {
			proto, err = laila.New_DefaultTCPProtocol(kw)
		} else {
			proto, err = laila.New_DefaultTCPIPProtocol(kw)
		}
		if err != nil {
			return nil, err
		}
		if err := comm.Add_connection(proto); err != nil {
			return nil, err
		}
		protos[scheme] = proto
	}
	return protos, nil
}

func bound_port_(proto laila.Comm_protocol_) int {
	return proto.(interface{ Bound_port() int }).Bound_port()
}

// typed_matches_ is ``type(got) is type(expected) and got == expected``.
func typed_matches_(kind string, got any) bool {
	expected := TYPED[kind]
	switch kind {
	case "numpy":
		g, ok := got.(*py.NDArray)
		e := expected.(*py.NDArray)
		return ok && g.Dtype == e.Dtype && py.Eq(g.Shape, e.Shape) && g.Eq(e)
	case "int":
		switch got.(type) {
		case int, int64, int32:
			return py.Eq(got, expected)
		}
		return false
	case "double":
		_, ok := got.(float64)
		return ok && py.Eq(got, expected)
	case "bool":
		_, ok := got.(bool)
		return ok && py.Eq(got, expected)
	case "bytes":
		_, ok := got.([]byte)
		return ok && py.Eq(got, expected)
	case "dict":
		_, ok := got.(*py.Dict)
		return ok && py.Eq(got, expected)
	}
	return false
}

func data_(entry any) (any, error) {
	d, ok := entry.(interface{ Data() (any, error) })
	if !ok {
		return nil, py.TypeError.Newf("%s has no .data", py.Pytypename(entry))
	}
	return d.Data()
}

func nick_(a []string, i int) any {
	if len(a) > i {
		return a[i]
	}
	return nil
}

// ---------------------------------------------------------------------------
// modes
// ---------------------------------------------------------------------------

func do_serve(scheme string, port int, secret string, value string, peers []string) error {
	policy, err := activate_()
	if err != nil {
		return err
	}
	alpha, err := laila.Constant(value)
	if err != nil {
		return err
	}
	if _, err := wait_(laila.Memorize(alpha)); err != nil {
		return err
	}
	pool, err := laila.New_DefaultPool()
	if err != nil {
		return err
	}
	if err := laila.As_central_memory_(policy.Central.Memory).Extend(laila.As_pool_(pool), py.Kw{"pool_nickname": STORE}); err != nil {
		return err
	}
	store, err := laila.Constant(value)
	if err != nil {
		return err
	}
	if _, err := wait_(laila.Memorize(store, py.Kw{"pool_nickname": STORE})); err != nil {
		return err
	}
	protos, err := add_transports_(secret, scheme, port)
	if err != nil {
		return err
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	say("POLICY " + policy.Global_id())
	say("ALPHA_GID " + alpha.Global_id())
	say("STORE_GID " + store.Global_id())
	for _, uri := range peers {
		pid, err := laila.Add_peer(uri, secret)
		if err != nil {
			return err
		}
		say("PEER " + uri + " " + pid)
	}
	say("READY " + strconv.Itoa(bound_port_(protos[scheme])))

	<-stop
	return nil
}

func do_remember(uri, secret, gid string, nick any) error {
	if _, err := activate_(); err != nil {
		return err
	}
	if _, err := add_transports_(secret, "", 0); err != nil {
		return err
	}
	pid, err := laila.Add_peer(uri, secret)
	if err != nil {
		return err
	}
	entry, err := wait_(laila.Remember(gid, py.Kw{"dst_policy": pid, "dst_pool": nick, "persist": false}))
	if err != nil {
		return err
	}
	data, err := data_(entry)
	if err != nil {
		return err
	}
	say("VALUE " + py.Str(data))
	return nil
}

// push_ peers with *uri* and memorizes a new constant holding *data* there
// (into the pool *nick* when given). Returns the entry.
func push_(uri, secret string, data any, nick any) (*laila.Entry, error) {
	if _, err := activate_(); err != nil {
		return nil, err
	}
	if _, err := add_transports_(secret, "", 0); err != nil {
		return nil, err
	}
	pid, err := laila.Add_peer(uri, secret)
	if err != nil {
		return nil, err
	}
	e, err := laila.Constant(data)
	if err != nil {
		return nil, err
	}
	if _, err := wait_(laila.Memorize(e, py.Kw{"dst_policy": pid, "dst_pool": nick})); err != nil {
		return nil, err
	}
	return e, nil
}

// pull_ peers with *uri* and reads *gid* (from the pool *nick* when given).
func pull_(uri, secret, gid string, nick any) (any, error) {
	if _, err := activate_(); err != nil {
		return nil, err
	}
	if _, err := add_transports_(secret, "", 0); err != nil {
		return nil, err
	}
	pid, err := laila.Add_peer(uri, secret)
	if err != nil {
		return nil, err
	}
	entry, err := wait_(laila.Remember(gid, py.Kw{"dst_policy": pid, "dst_pool": nick, "persist": false}))
	if err != nil {
		return nil, err
	}
	return data_(entry)
}

func do_memorize(uri, secret, value string, nick any) error {
	e, err := push_(uri, secret, value, nick)
	if err != nil {
		return err
	}
	say("MEMO " + e.Global_id())
	return nil
}

func do_memorize_typed(uri, secret, kind string, nick any) error {
	v, ok := TYPED[kind]
	if !ok {
		return py.ValueError.Newf("unknown typed kind: %s", kind)
	}
	e, err := push_(uri, secret, v, nick)
	if err != nil {
		return err
	}
	say("MEMO " + e.Global_id())
	return nil
}

func do_memorize_json(uri, secret, text string, nick any) error {
	v, err := py.Json_loads(text)
	if err != nil {
		return err
	}
	e, err := push_(uri, secret, v, nick)
	if err != nil {
		return err
	}
	say("MEMO " + e.Global_id())
	return nil
}

func do_remember_json(uri, secret, gid string, nick any) error {
	got, err := pull_(uri, secret, gid, nick)
	if err != nil {
		return err
	}
	if arr, ok := got.(*py.NDArray); ok {
		got, err = arr.Tolist()
		if err != nil {
			return err
		}
	}
	// ensure_ascii keeps the reply a single ASCII line whatever the stdout codec.
	ensure_ascii, allow_nan := true, false
	s, err := py.Json_dumps_opts(got, py.JsonOpts{Ensure_ascii: &ensure_ascii, Allow_nan: &allow_nan})
	if err != nil {
		return err
	}
	say("JSON " + s)
	return nil
}

func do_memorize_bytes(uri, secret string, n int, seed uint32, nick any) error {
	data := xorshift_bytes(n, seed)
	e, err := push_(uri, secret, data, nick)
	if err != nil {
		return err
	}
	say("MEMO " + e.Global_id() + " " + fnv1a64(data))
	return nil
}

func do_remember_hash(uri, secret, gid string, nick any) error {
	got, err := pull_(uri, secret, gid, nick)
	if err != nil {
		return err
	}
	b, ok := got.([]byte)
	if !ok {
		say("HASH_TYPE " + py.Pytypename(got))
		return nil
	}
	say("HASH " + fnv1a64(b) + " " + strconv.Itoa(len(b)))
	return nil
}

func do_remember_typed(uri, secret, gid, kind string) error {
	if _, ok := TYPED[kind]; !ok {
		return py.ValueError.Newf("unknown typed kind: %s", kind)
	}
	if _, err := activate_(); err != nil {
		return err
	}
	if _, err := add_transports_(secret, "", 0); err != nil {
		return err
	}
	pid, err := laila.Add_peer(uri, secret)
	if err != nil {
		return err
	}
	entry, err := wait_(laila.Remember(gid, py.Kw{"dst_policy": pid, "persist": false}))
	if err != nil {
		return err
	}
	got, err := data_(entry)
	if err != nil {
		return err
	}
	if typed_matches_(kind, got) {
		say("TYPED_OK " + kind)
		return nil
	}
	say("TYPED_MISMATCH " + kind + " " + py.Pytypename(got) + ":" + py.Repr(got))
	return nil
}

// do_relay is the 3-party case: this policy (A) asks the policy at
// *via_uri* (B) to pull *gid* from the policy *from_gid* (C, which B is
// already peered to; optionally from C's pool *nick*) into B's own memory;
// then A reads it back from B.
func do_relay(via_uri, secret, from_gid, gid string, nick any) error {
	if _, err := activate_(); err != nil {
		return err
	}
	if _, err := add_transports_(secret, "", 0); err != nil {
		return err
	}
	via, err := laila.Add_peer(via_uri, secret)
	if err != nil {
		return err
	}
	if _, err := wait_(laila.Remember(gid, py.Kw{"src_policy": via, "dst_policy": from_gid, "dst_pool": nick})); err != nil {
		return err
	}
	entry, err := wait_(laila.Remember(gid, py.Kw{"dst_policy": via, "persist": false}))
	if err != nil {
		return err
	}
	data, err := data_(entry)
	if err != nil {
		return err
	}
	say("VALUE " + py.Str(data))
	return nil
}

func do_forget(uri, secret, gid string, nick any) error {
	if _, err := activate_(); err != nil {
		return err
	}
	if _, err := add_transports_(secret, "", 0); err != nil {
		return err
	}
	pid, err := laila.Add_peer(uri, secret)
	if err != nil {
		return err
	}
	if _, err := wait_(laila.Forget(gid, py.Kw{"policy": pid, "pool": nick})); err != nil {
		return err
	}
	say("FORGOT " + gid)
	return nil
}

func usage() int {
	fmt.Fprintln(os.Stderr, strings.TrimSpace(`
usage: go_harness <mode> [args...]
  serve <scheme> <port> <secret> <value> [peer_uri ...]
  remember <uri> <secret> <gid> [pool_nickname]
  memorize <uri> <secret> <value> [pool_nickname]
  memorize_typed <uri> <secret> <kind> [pool_nickname]
  remember_typed <uri> <secret> <gid> <kind>
  memorize_json <uri> <secret> <json> [pool_nickname]
  remember_json <uri> <secret> <gid> [pool_nickname]
  memorize_bytes <uri> <secret> <n> <seed> [pool_nickname]
  remember_hash <uri> <secret> <gid> [pool_nickname]
  relay <via_uri> <secret> <from_policy_gid> <gid> [pool_nickname]
  forget <uri> <secret> <gid> [pool_nickname]`))
	return 2
}

func atoi_(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, py.ValueError.Newf("invalid literal for int() with base 10: %s", py.Repr(s))
	}
	return n, nil
}

func run(argv []string) (code int) {
	if len(argv) < 2 {
		return usage()
	}
	mode, a := argv[1], argv[2:]
	defer func() {
		if c, err := comm_(); err == nil {
			_ = c.Stop()
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			say("ERROR " + fail_(py.PanicToError(r)))
			code = 1
		}
	}()
	var err error
	switch {
	case mode == "serve" && len(a) >= 4:
		var port int
		if port, err = atoi_(a[1]); err == nil {
			err = do_serve(a[0], port, a[2], a[3], a[4:])
		}
	case mode == "remember" && len(a) >= 3:
		err = do_remember(a[0], a[1], a[2], nick_(a, 3))
	case mode == "memorize" && len(a) >= 3:
		err = do_memorize(a[0], a[1], a[2], nick_(a, 3))
	case mode == "memorize_typed" && len(a) >= 3:
		err = do_memorize_typed(a[0], a[1], a[2], nick_(a, 3))
	case mode == "remember_typed" && len(a) >= 4:
		err = do_remember_typed(a[0], a[1], a[2], a[3])
	case mode == "memorize_json" && len(a) >= 3:
		err = do_memorize_json(a[0], a[1], a[2], nick_(a, 3))
	case mode == "remember_json" && len(a) >= 3:
		err = do_remember_json(a[0], a[1], a[2], nick_(a, 3))
	case mode == "memorize_bytes" && len(a) >= 4:
		var n, seed int
		if n, err = atoi_(a[2]); err == nil {
			if seed, err = atoi_(a[3]); err == nil {
				err = do_memorize_bytes(a[0], a[1], n, uint32(seed), nick_(a, 4))
			}
		}
	case mode == "remember_hash" && len(a) >= 3:
		err = do_remember_hash(a[0], a[1], a[2], nick_(a, 3))
	case mode == "relay" && len(a) >= 4:
		err = do_relay(a[0], a[1], a[2], a[3], nick_(a, 4))
	case mode == "forget" && len(a) >= 3:
		err = do_forget(a[0], a[1], a[2], nick_(a, 3))
	default:
		return usage()
	}
	if err != nil {
		say("ERROR " + fail_(err))
		return 1
	}
	return 0
}

// fail_ is ``f"{type(exc).__name__}: {exc}"`` on one line.
func fail_(err error) string {
	msg := strings.ReplaceAll(strings.TrimSpace(err.Error()), "\n", " ")
	return py.Typename(err) + ": " + msg
}

func main() {
	code := run(os.Args)
	_ = stdout.Flush()
	// Policies own background goroutines; don't let a straggler keep the
	// harness alive (``os._exit``).
	os.Exit(code)
}
