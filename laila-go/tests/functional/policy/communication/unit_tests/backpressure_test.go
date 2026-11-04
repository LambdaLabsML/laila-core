// Port of tests/functional/policy/communication/unit_tests/test_backpressure.py.
//
// Tests for inbound RPC backpressure + sender backoff.
//
// Three layers, all transport-agnostic (exercised on the shared carrier
// base + hub, so every concrete transport inherits the behaviour):
//
//  1. Hub admission control -- the per-policy bounded semaphore behind
//     ``_acquire_rpc_slot`` / ``_release_rpc_slot`` and ``max_inflight_rpcs``.
//  2. Carrier inbound dispatch -- ``_handle_request_frame`` answers a
//     liveness ping *before* admission, rejects over-capacity requests with
//     ``ERR_BUSY`` (never queuing them), and runs admitted work on the
//     worker pool, releasing the slot afterwards.
//  3. Sender backoff -- the base ``send_rpc`` retries ``_send_once`` with
//     exponential backoff on ``BackpressureError`` and surfaces the error if
//     the peer stays saturated.
package unit_tests

import (
	"sync"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// FakeCarrier_ is ``_FakeCarrier``: the bare carrier with a protocol name
// and a swappable ``_send_once``.
type FakeCarrier_ struct {
	laila.CarrierRPCProtocol_

	send_once func(peer_id string, path []string, args []any, kwargs py.Kw) (any, error)
}

var FakeCarrier_class = py.NewClass("_FakeCarrier", (*FakeCarrier_)(nil), laila.CarrierRPCProtocol_class).
	Set_classvar("protocol_name", "fake")

func New_FakeCarrier_(kw ...py.Kw) (*FakeCarrier_, error) {
	c := FakeCarrier_class.New().(*FakeCarrier_)
	if err := c.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return c, nil
}

// Send_once_ is ``c._send_once = ...``: the instance override when set.
func (c *FakeCarrier_) Send_once_(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
	if c.send_once != nil {
		return c.send_once(peer_id, path, args, kwargs)
	}
	return c.CarrierRPCProtocol_.Send_once_(peer_id, path, args, kwargs)
}

func rpc_(path []string, args *py.List, kwargs *py.Dict) *py.Dict {
	if args == nil {
		args = py.NewList()
	}
	if kwargs == nil {
		kwargs = py.NewDict()
	}
	p := py.NewList()
	for _, s := range path {
		p.Append(s)
	}
	return laila.Make_request("rpc.call", py.DictOf("path", p, "args", args, "kwargs", kwargs), "")
}

func TestHubAdmission_cap_admits_then_rejects_then_releases(t *testing.T) {
	c := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "t"}))
	c.Max_inflight_rpcs = 2
	assertEq(t, c.Acquire_rpc_slot_(), true)
	assertEq(t, c.Acquire_rpc_slot_(), true)
	assertEq(t, c.Acquire_rpc_slot_(), false) // at capacity
	c.Release_rpc_slot_()
	assertEq(t, c.Acquire_rpc_slot_(), true)
}

func TestHubAdmission_no_hub_always_admits(t *testing.T) {
	c := must(New_FakeCarrier_())
	assertEq(t, c.Try_admit_(), true)
}

func carrier_(t *testing.T, cap int) (*FakeCarrier_, *laila.LAILA_IDENTIFIABLE_COMMUNICATION_) {
	t.Helper()
	c := must(laila.New_LAILA_IDENTIFIABLE_COMMUNICATION_(py.Kw{"policy_id": "t"}))
	c.Max_inflight_rpcs = cap
	fc := must(New_FakeCarrier_())
	fc.Communication_ = c
	return fc, c
}

func TestHandleRequestFrame_ping_bypasses_admission(t *testing.T) {
	c, comm := carrier_(t, 1)
	comm.Acquire_rpc_slot_() // exhaust capacity
	got := py.NewDict()
	c.Handle_request_frame_(rpc_([]string{"__comm_ping__"}, nil, nil), func(resp *py.Dict) { got.Update(resp) }, nil)
	assertEq(t, got.GetDefault("result", nil), "pong")
}

func TestHandleRequestFrame_over_capacity_replies_busy_without_queuing(t *testing.T) {
	c, comm := carrier_(t, 1)
	comm.Acquire_rpc_slot_() // the only slot is taken
	got := py.NewDict()
	c.Handle_request_frame_(rpc_([]string{"central", "memory", "x"}, nil, nil), func(resp *py.Dict) { got.Update(resp) }, nil)
	errd, _ := got.GetDefault("error", py.NewDict()).(*py.Dict)
	assertEq(t, errd.GetDefault("code", nil), laila.ERR_BUSY)
	// Nothing was submitted to the executor.
	if c.Inbound_executor_ != nil {
		t.Fatal("executor was created")
	}
}

// stub_ is the ``_Stub`` local policy with an ``echo`` method.
type stub_ struct{}

func (s *stub_) Echo(value any) any { return value }

func TestHandleRequestFrame_admitted_request_runs_and_releases_slot(t *testing.T) {
	c, comm := carrier_(t, 2)
	comm.Local_policy_ = &stub_{}
	done := py.NewEvent()
	box := py.NewDict()
	var mu sync.Mutex
	c.Handle_request_frame_(rpc_([]string{"echo"}, py.NewList("hi"), nil), func(resp *py.Dict) {
		mu.Lock()
		box.Update(resp)
		mu.Unlock()
		done.Set()
	}, nil)
	if !done.Wait(5) {
		t.Fatal("reply never arrived")
	}
	mu.Lock()
	assertEq(t, box.GetDefault("result", nil), "hi")
	mu.Unlock()
	// Slot was released: full capacity is available again.
	assertEq(t, comm.Acquire_rpc_slot_(), true)
	assertEq(t, comm.Acquire_rpc_slot_(), true)
	c.Shutdown_executor_()
}

func TestSenderBackoff_retries_then_succeeds(t *testing.T) {
	c := must(New_FakeCarrier_(py.Kw{"rpc_backoff_base": 0.001, "rpc_backoff_max": 0.005, "max_rpc_retries": 5}))
	n := 0
	c.send_once = func(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
		n++
		if n < 3 {
			return nil, laila.BackpressureError.New("busy")
		}
		return "result", nil
	}
	assertEq(t, must(c.Send_rpc("p", []string{"x"}, nil, nil)), "result")
	assertEq(t, n, 3)
}

func TestSenderBackoff_gives_up_after_max_retries(t *testing.T) {
	c := must(New_FakeCarrier_(py.Kw{"rpc_backoff_base": 0.001, "rpc_backoff_max": 0.005, "max_rpc_retries": 2}))
	n := 0
	c.send_once = func(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
		n++
		return nil, laila.BackpressureError.New("still busy")
	}
	_, err := c.Send_rpc("p", []string{"x"}, nil, nil)
	assertRaises(t, laila.BackpressureError, err)
	// initial try + max_rpc_retries
	assertEq(t, n, 3)
}

func TestSenderBackoff_non_busy_error_is_not_retried(t *testing.T) {
	c := must(New_FakeCarrier_(py.Kw{"rpc_backoff_base": 0.001, "max_rpc_retries": 5}))
	n := 0
	c.send_once = func(peer_id string, path []string, args []any, kwargs py.Kw) (any, error) {
		n++
		return nil, py.RuntimeError.New("hard failure")
	}
	_, err := c.Send_rpc("p", []string{"x"}, nil, nil)
	assertRaises(t, py.RuntimeError, err)
	assertEq(t, n, 1)
}
