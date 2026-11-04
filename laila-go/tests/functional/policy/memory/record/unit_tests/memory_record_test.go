// Port of tests/functional/policy/memory/record/unit_tests/test_memory_record.py.
//
// Unit tests for laila.policy.central.memory.record.record: Record
// (current version). ``creator`` and ``borrower`` are NOT required at
// runtime. Covers 9 cases total.
package unit_tests

import (
	"errors"
	"testing"

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

func record(t *testing.T, entry any, kw ...py.Kw) *laila.Record {
	t.Helper()
	opts := py.MergeKw(kw...)
	opts["entry"] = entry
	opts["recorder"] = "c"
	opts["borrower"] = "b"
	return must(laila.New_Record(opts))
}

// 1
func TestGenesisEntryIdFromGlobalId(t *testing.T) {
	e := must(laila.Constant(py.NewList(1, 2, 3, 4)))
	r := record(t, e)
	assertEq(t, must(r.Entry_id()), e.Global_id())
}

// 2
func TestArchetypeEntryIdFromGlobalId(t *testing.T) {
	e := must(laila.Variable(py.Kw{"data": py.NewList(1, 2, 3, 4)}))
	r := record(t, e)
	assertEq(t, must(r.Entry_id()), e.Global_id())
}

// 3
func TestDictEntryIdFromGlobalIdKey(t *testing.T) {
	payload := py.DictOf("_global_id", "LAILA:ENTRY:XYZ", "data", py.NewList(1, 2, 3, 4))
	r := record(t, payload)
	assertEq(t, must(r.Entry_id()), "LAILA:ENTRY:XYZ")
}

// 4
func TestDictMissingGlobalIdRaisesKeyError(t *testing.T) {
	r := record(t, py.DictOf("data", py.NewList(1, 2, 3)))
	_, err := r.Entry_id()
	if !errors.Is(err, py.KeyError) {
		t.Fatalf("want KeyError, got %v", err)
	}
}

// 5
func TestInvalidEntryTypeRaises(t *testing.T) {
	r := record(t, &struct{}{})
	_, err := r.Entry_id()
	if !errors.Is(err, py.TypeError) {
		t.Fatalf("want TypeError, got %v", err)
	}
}

// 6
func TestRecordTimestampDefaultFactoryReturnsString(t *testing.T) {
	r := record(t, py.DictOf("_global_id", "LAILA:ENTRY:T1"))
	if len(r.Record_timestamp) == 0 {
		t.Fatal("record_timestamp should be a non-empty string")
	}
}

// 7
func TestRecordTimestampCanBeProvided(t *testing.T) {
	ts := "2020-01-02T03:04:05"
	r := record(t, py.DictOf("_global_id", "LAILA:ENTRY:T2"), py.Kw{"record_timestamp": ts})
	assertEq(t, r.Record_timestamp, ts)
}

// 8
func TestModelDumpDoesNotIncludeEntryIdProperty(t *testing.T) {
	r := record(t, py.DictOf("_global_id", "LAILA:ENTRY:T4"))
	d := py.Model_dump(r)
	for _, k := range []string{"entry", "recorder", "borrower", "record_timestamp"} {
		if !d.Contains(k) {
			t.Fatalf("%q missing from model_dump", k)
		}
	}
	if d.Contains("entry_id") { // property, not a field
		t.Fatal("entry_id must not be dumped")
	}
}

// 9
func TestAsDictIncludesEntryPayload(t *testing.T) {
	r := record(t, py.DictOf("_global_id", "LAILA:ENTRY:T5"))
	d := r.As_dict()
	if !d.Contains("entry") {
		t.Fatal("entry missing")
	}
	assertEq(t, d.GetDefault("entry", nil), py.DictOf("_global_id", "LAILA:ENTRY:T5"))
}
