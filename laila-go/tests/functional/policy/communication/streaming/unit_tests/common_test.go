// Shared test-side helpers for the streaming suite (the Python files use
// unittest's assert* family and a couple of module-level helpers).
package unit_tests

import (
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

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

func assertRaises(t *testing.T, exc *py.Type, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", exc.Name)
	}
	if !errors.Is(err, exc) {
		t.Fatalf("expected %s, got %v", exc.Name, err)
	}
}

func assertTrue(t *testing.T, ok bool, msg string) {
	t.Helper()
	if !ok {
		t.Fatal(msg)
	}
}

func assertIn_(t *testing.T, needle string, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), needle) {
		t.Fatalf("expected %q in %v", needle, err)
	}
}

func sleep(s float64) { time.Sleep(time.Duration(s * float64(time.Second))) }

// blob_ is ``_blob(n, tag)``: deterministic non-trivial payload (not all
// one byte).
func blob_(n int, tag int) []byte {
	seed := py.Sha256([]byte(py.Str(n) + ":" + py.Str(tag)))
	if n == 0 {
		return []byte{}
	}
	reps := (n + len(seed) - 1) / len(seed)
	out := make([]byte, 0, reps*len(seed))
	for i := 0; i < reps; i++ {
		out = append(out, seed...)
	}
	return out[:n]
}

// channel_ is ``comm.peers[gid][name]``.
func channel_(t *testing.T, c *laila.LAILA_IDENTIFIABLE_COMMUNICATION_, gid string, name string) *laila.Channel {
	t.Helper()
	v, ok := c.Peers.Get(gid)
	if !ok {
		t.Fatalf("KeyError: %s", gid)
	}
	ch, err := v.(*laila.PeerProxy).Channel(name)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

// proxy_ is ``comm.peers[gid]``.
func proxy_(t *testing.T, c *laila.LAILA_IDENTIFIABLE_COMMUNICATION_, gid string) *laila.PeerProxy {
	t.Helper()
	v, ok := c.Peers.Get(gid)
	if !ok {
		t.Fatalf("KeyError: %s", gid)
	}
	return v.(*laila.PeerProxy)
}

// channels_ is ``comm.peers[gid].channels()``.
func channels_(t *testing.T, c *laila.LAILA_IDENTIFIABLE_COMMUNICATION_, gid string) []string {
	t.Helper()
	v, ok := c.Peers.Get(gid)
	if !ok {
		return []string{}
	}
	names, err := v.(*laila.PeerProxy).Channels()
	if err != nil {
		t.Fatal(err)
	}
	return names
}

// relay_ is ``laila.relay(channel, timeout=...)``.
func relay_(t *testing.T, ch *laila.Channel, timeout ...float64) *laila.Relay {
	t.Helper()
	kw := py.Kw{}
	if len(timeout) > 0 {
		kw["timeout"] = timeout[0]
	}
	r, err := laila.Get_relay(ch, nil, kw)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// next_ is ``next(relay)`` (fatal on any error).
func next_(t *testing.T, r *laila.Relay) *laila.StreamEntry {
	t.Helper()
	e, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// bytes_of_ is ``bytes(entry.data)``.
func bytes_of_(t *testing.T, e any) []byte {
	t.Helper()
	d, ok := e.(interface{ Data() (any, error) })
	if !ok {
		t.Fatalf("%T has no .data", e)
	}
	v := must(d.Data())
	switch b := v.(type) {
	case []byte:
		return b
	case string:
		return []byte(b)
	}
	t.Fatalf("not bytes: %s", py.Repr(v))
	return nil
}

// wait_result_ is ``f.wait(timeout=...)`` followed by ``back[0] if
// isinstance(back, list) else back``.
func wait_result_(t *testing.T, f any, timeout float64) any {
	t.Helper()
	res := f
	if py.Hasattr(f, "wait") {
		fn := must(py.Getattr(f, "wait"))
		res = must(py.Call(fn, nil, py.Kw{"timeout": timeout}))
	}
	if lst, ok := res.(*py.List); ok {
		return lst.Items()[0]
	}
	return res
}

func sorted_(names []string) []string {
	out := append([]string(nil), names...)
	sort.Strings(out)
	return out
}

func str_list_(names []string) *py.List {
	l := py.NewList()
	for _, n := range names {
		l.Append(n)
	}
	return l
}
