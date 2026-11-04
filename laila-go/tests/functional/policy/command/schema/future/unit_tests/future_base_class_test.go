// Port of tests/functional/policy/command/schema/future/unit_tests/
// future_base_class_tests.py.
package unit_tests

import (
	"errors"
	"strings"
	"testing"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
)

func make_future(t *testing.T) *laila.Future {
	t.Helper()
	fut, err := laila.New_Future(py.Kw{
		"taskforce_id": "LAILA:TASK_FORCE:test-taskforce",
		"policy_id":    "LAILA:POLICY:test-policy",
	})
	if err != nil {
		t.Fatal(err)
	}
	return fut
}

func TestFutureBase_001_requires_identity_fields(t *testing.T) {
	if _, err := laila.New_Future(); !errors.Is(err, py.ValidationError) {
		t.Fatalf("expected ValidationError, got %v", err)
	}
}

func TestFutureBase_002_default_status_and_predicates(t *testing.T) {
	fut := make_future(t)
	if fut.Status() != laila.FutureStatus_NOT_STARTED {
		t.Fatalf("status %v", fut.Status())
	}
	if !fut.Not_started() || fut.Running() || fut.Finished() || fut.Error() || fut.Cancelled() {
		t.Fatal("predicates wrong")
	}
}

func TestFutureBase_003_status_setter_updates_predicates(t *testing.T) {
	fut := make_future(t)
	fut.Set_status(laila.FutureStatus_RUNNING)
	if !fut.Running() {
		t.Fatal("running")
	}
	fut.Set_status(laila.FutureStatus_FINISHED)
	if !fut.Finished() {
		t.Fatal("finished")
	}
	fut.Set_status(laila.FutureStatus_ERROR)
	if !fut.Error() {
		t.Fatal("error")
	}
	fut.Set_status(laila.FutureStatus_CANCELLED)
	if !fut.Cancelled() {
		t.Fatal("cancelled")
	}
}

func TestFutureBase_004_result_requires_concrete_wait_when_not_finished(t *testing.T) {
	fut := make_future(t)
	if _, err := fut.Result(); !errors.Is(err, py.NotImplementedError) {
		t.Fatalf("expected NotImplementedError, got %v", err)
	}
}

func TestFutureBase_005_result_roundtrip_when_marked_finished(t *testing.T) {
	fut := make_future(t)
	fut.Set_result(py.DictFromKw(py.Kw{"ok": 1}))
	fut.Set_status(laila.FutureStatus_FINISHED)
	res, err := fut.Result()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*laila.Entry); !ok {
		t.Fatalf("expected Entry, got %T", res)
	}
	data, err := fut.Data()
	if err != nil {
		t.Fatal(err)
	}
	if !py.Eq(data, py.DictFromKw(py.Kw{"ok": 1})) {
		t.Fatalf("data %v", data)
	}
}

func TestFutureBase_006_result_raises_exception_for_error_status(t *testing.T) {
	fut := make_future(t)
	fut.Set_exception(py.RuntimeError.New("boom"))
	fut.Set_status(laila.FutureStatus_ERROR)
	if _, err := fut.Result(); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}

func TestFutureBase_007_result_raises_exception_for_cancelled_status(t *testing.T) {
	fut := make_future(t)
	fut.Set_exception(py.RuntimeError.New("cancelled"))
	fut.Set_status(laila.FutureStatus_CANCELLED)
	if _, err := fut.Result(); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}

func TestFutureBase_008_exception_property_roundtrip(t *testing.T) {
	fut := make_future(t)
	err := py.ValueError.New("x")
	fut.Set_exception(err)
	if fut.Exception() != error(err) {
		t.Fatal("exception identity lost")
	}
}

func TestFutureBase_009_callback_add_and_remove(t *testing.T) {
	fut := make_future(t)
	var cb laila.Future_callback = func(_ any) {}
	fut.Add_callback(laila.FutureStatus_FINISHED, cb)
	if cur, ok := fut.Callbacks[laila.FutureStatus_FINISHED]; !ok || !py.Is(cur, cb) {
		t.Fatal("callback not registered")
	}
	fut.Remove_callback(laila.FutureStatus_FINISHED, cb)
	if cur, ok := fut.Callbacks[laila.FutureStatus_FINISHED]; ok && cur != nil {
		t.Fatal("callback not removed")
	}
}

func TestFutureBase_010_identity_and_global_id_are_available(t *testing.T) {
	fut := make_future(t)
	if !strings.HasPrefix(fut.Global_id(), "LAILA:") || !strings.Contains(fut.Global_id(), ":FUTURE:") {
		t.Fatalf("global_id %s", fut.Global_id())
	}
	if !fut.Identity().Contains("uuid") {
		t.Fatal("identity lacks uuid")
	}
}

func TestFutureBase_011_data_returns_entry_payload_for_raw_result(t *testing.T) {
	fut := make_future(t)
	fut.Set_result(py.DictFromKw(py.Kw{"key": "value"}))
	fut.Set_status(laila.FutureStatus_FINISHED)
	data, err := fut.Data()
	if err != nil {
		t.Fatal(err)
	}
	if !py.Eq(data, py.DictFromKw(py.Kw{"key": "value"})) {
		t.Fatalf("data %v", data)
	}
}

func TestFutureBase_012_data_returns_entry_payload_for_entry_result(t *testing.T) {
	fut := make_future(t)
	entry, err := laila.Entry_constant(py.NewList(1, 2, 3))
	if err != nil {
		t.Fatal(err)
	}
	fut.Set_result(entry)
	fut.Set_status(laila.FutureStatus_FINISHED)
	data, err := fut.Data()
	if err != nil {
		t.Fatal(err)
	}
	if !py.Eq(data, py.NewList(1, 2, 3)) {
		t.Fatalf("data %v", data)
	}
}

func TestFutureBase_013_data_raises_when_result_is_none(t *testing.T) {
	fut := make_future(t)
	fut.Return_value_ = nil
	fut.Set_status(laila.FutureStatus_FINISHED)
	if _, err := fut.Data(); !errors.Is(err, py.RuntimeError) {
		t.Fatalf("expected RuntimeError, got %v", err)
	}
}
