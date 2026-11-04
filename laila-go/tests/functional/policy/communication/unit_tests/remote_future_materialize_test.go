// Port of tests/functional/policy/communication/unit_tests/test_remote_future_materialize.py.
//
// RemoteFuture full-mirror: materialize the Entry over the wire + cache.
//
// Uses a fake communication channel (no real network) so the materialize /
// cache / payload semantics are tested deterministically:
//   - ``.result`` rebuilds the real Entry from the serialized blob the peer
//     returns (not a gid),
//   - ``.data`` returns the payload (mirrors local ``Future.data``),
//   - the result is cached: repeated ``.result``/``.data`` does not re-issue
//     the result RPC,
//   - ``.status`` stays a lightweight proxy,
//   - group futures return lists.
package unit_tests

import (
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// FakeComm_ is ``_FakeComm``: stand-in communication channel recording
// the RPC paths it serves.
type FakeComm_ struct {
	result_payload any
	calls          []string
}

func New_FakeComm_(result_payload any) *FakeComm_ {
	return &FakeComm_{result_payload: result_payload}
}

// Send_rpc_ is ``_send_rpc(policy_id, path, args, kwargs, comm=None)``.
func (c *FakeComm_) Send_rpc_(policy_id any, path []string, args any, kwargs any, kw ...py.Kw) any {
	c.calls = append(c.calls, path[0])
	switch path[0] {
	case "_get_future_result_entry", "_wait_future_entry":
		return c.result_payload
	case "_get_future_status":
		return "FINISHED"
	case "_get_future_exception":
		return nil
	}
	return nil
}

func (c *FakeComm_) count(name string) int {
	n := 0
	for _, p := range c.calls {
		if p == name {
			n++
		}
	}
	return n
}

// mk_remote_ is ``_mk_remote(scope, comm, *, is_group=False)``.
func mk_remote_(t *testing.T, scope string, comm *FakeComm_, is_group bool) *laila.RemoteFuture {
	t.Helper()
	rf := must(laila.New_RemoteFuture(py.Kw{
		"uuid":         py.Uuid4().Hex(),
		"scopes":       []string{scope},
		"policy_id":    "LAILA:POLICY:peer",
		"taskforce_id": "LAILA:TASK_FORCE:tf",
	}))
	rf.Bind(comm, py.Kw{"is_group": is_group})
	return rf
}

func serialize_b64_(t *testing.T, e *laila.Entry) any {
	t.Helper()
	return must(e.Serialize(laila.Transformation_base64))
}

func TestRemoteFutureMaterialize_result_is_rebuilt_entry_and_cached(t *testing.T) {
	entry := must(laila.Constant(py.DictOf("k", 99, "s", "hi")))
	fake := New_FakeComm_(serialize_b64_(t, entry))
	rf := mk_remote_(t, laila.FUTURE_SCOPE_, fake, false)

	out := must(rf.Result())
	assertEq(t, must(py.Getattr(out, "data")), py.DictOf("k", 99, "s", "hi")) // real Entry, real data
	assertEq(t, must(rf.Data()), py.DictOf("k", 99, "s", "hi"))              // .data -> payload
	_ = must(rf.Result())                                                      // access again
	// materialized once, then served from cache
	assertEq(t, fake.count("_get_future_result_entry"), 1)
}

func TestRemoteFutureMaterialize_status_is_lightweight_proxy(t *testing.T) {
	entry := must(laila.Constant(1))
	fake := New_FakeComm_(serialize_b64_(t, entry))
	rf := mk_remote_(t, laila.FUTURE_SCOPE_, fake, false)
	if !strings.HasSuffix(py.Str(rf.Status()), "FINISHED") {
		t.Fatal(py.Str(rf.Status()))
	}
	if fake.count("_get_future_status") == 0 {
		t.Fatal("_get_future_status not called")
	}
	// asking status does not transfer the payload
	if fake.count("_get_future_result_entry") != 0 {
		t.Fatal("status transferred the payload")
	}
}

func TestRemoteFutureMaterialize_wait_returns_entry(t *testing.T) {
	entry := must(laila.Constant(py.DictOf("v", py.NewList(1, 2))))
	fake := New_FakeComm_(serialize_b64_(t, entry))
	rf := mk_remote_(t, laila.FUTURE_SCOPE_, fake, false)
	got := must(rf.Wait())
	assertEq(t, must(py.Getattr(got, "data")), py.DictOf("v", py.NewList(1, 2)))
}

func TestRemoteFutureMaterialize_group_future_returns_lists(t *testing.T) {
	e1 := must(laila.Constant("a"))
	e2 := must(laila.Constant("b"))
	blobs := py.NewList(serialize_b64_(t, e1), serialize_b64_(t, e2))
	fake := New_FakeComm_(blobs)
	rf := mk_remote_(t, laila.GROUP_FUTURE_SCOPE_, fake, true)
	entries := must(rf.Result())
	datas := py.NewList()
	for _, e := range py.Iter(entries) {
		datas.Append(must(py.Getattr(e, "data")))
	}
	assertEq(t, datas, py.NewList("a", "b"))
	assertEq(t, must(rf.Data()), py.NewList("a", "b"))
}
