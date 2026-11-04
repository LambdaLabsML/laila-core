// Port of tests/functional/pools/multibuffer/unit_tests/test_multibuffer.py.
package unit_tests

import (
	"bytes"
	"errors"
	"slices"
	"strings"
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

func assertIs(t *testing.T, got, want any) {
	t.Helper()
	if got != want {
		t.Fatalf("%s is not %s", py.Repr(got), py.Repr(want))
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

func constant(data any) *laila.Entry { return must(laila.Constant(data)) }

func mb(kw ...py.Kw) *laila.MultiBuffer { return must(laila.New_MultiBuffer(kw...)) }

func nones(n int) *py.List { return py.NewList(make([]any, n)...) }

func slots(b *laila.MultiBuffer) *py.List { return b.Slots.(*py.List) }

func slotRecord(t *testing.T, b *laila.MultiBuffer, i int) *laila.Record {
	t.Helper()
	v := must(slots(b).Get(i))
	r, ok := v.(*laila.Record)
	if !ok {
		t.Fatalf("slot %d holds %T, want *Record", i, v)
	}
	return r
}

func asEntry(t *testing.T, v any) *laila.Entry {
	t.Helper()
	if _, isRecord := v.(*laila.Record); isRecord {
		t.Fatalf("%T is a Record", v)
	}
	e := laila.As_entry_(v)
	if e == nil {
		t.Fatalf("%T is not an Entry", v)
	}
	return e
}

func entryData(t *testing.T, v any) any {
	t.Helper()
	return must(asEntry(t, v).Data())
}

// =============================================================================
// TestMultiBufferConstruction
// =============================================================================

func TestDefaultIsADoubleBuffer(t *testing.T) {
	b := mb()
	assertEq(t, b.Capacity, 2)
	assertEq(t, b.Len(), 2)
	assertEq(t, b.Slots, nones(2))
	assertEq(t, b.Mapped, false)
}

func TestExplicitCapacitySizesSlots(t *testing.T) {
	b := mb(py.Kw{"capacity": 4})
	assertEq(t, b.Slots, nones(4))
}

func TestCapacityMustBePositive(t *testing.T) {
	if _, err := laila.New_MultiBuffer(py.Kw{"capacity": 0}); err == nil {
		t.Fatal("expected an exception")
	}
}

func TestExplicitSlotsUsedByReference(t *testing.T) {
	external := nones(3)
	b := mb(py.Kw{"slots": external})
	assertIs(t, b.Slots, external)
}

func TestExplicitSlotsOverrideCapacity(t *testing.T) {
	b := mb(py.Kw{"slots": nones(3), "capacity": 7})
	assertEq(t, b.Capacity, 3)
	assertEq(t, b.Len(), 3)
}

func TestEmptySlotsRejected(t *testing.T) {
	_, err := laila.New_MultiBuffer(py.Kw{"slots": py.NewList()})
	assertRaises(t, py.ValueError, err)
}

func TestUnsizedSlotsRejected(t *testing.T) {
	_, err := laila.New_MultiBuffer(py.Kw{"slots": 42})
	assertRaises(t, py.TypeError, err)
}

func TestHeadsStartAtZero(t *testing.T) {
	b := mb()
	assertEq(t, b.Read_head(), 0)
	assertEq(t, b.Write_head(), 0)
}

func TestHeadsAreReadOnly(t *testing.T) {
	b := mb()
	assertRaises(t, py.AttributeError, py.Setattr(b, "read_head", 1))
}

// =============================================================================
// TestMultiBufferItemProtocol
// =============================================================================

func TestSetitemEntryStoresRecord(t *testing.T) {
	b := mb()
	entry := constant("a")
	must(0, b.Setitem(0, entry))
	assertIs(t, slotRecord(t, b, 0).Entry, entry)
}

func TestSetitemRecordStoredAsIs(t *testing.T) {
	b := mb()
	record := must(laila.New_Record(py.Kw{"entry": constant("a")}))
	must(0, b.Setitem(1, record))
	assertIs(t, must(slots(b).Get(1)), record)
}

func TestSetitemRawPayloadLiftedToRecordOfConstant(t *testing.T) {
	b := mb()
	must(0, b.Setitem(0, []byte("\xde\xad")))
	r := slotRecord(t, b, 0)
	assertEq(t, entryData(t, r.Entry), []byte("\xde\xad"))
}

func TestSetitemNoneClearsSlot(t *testing.T) {
	b := mb()
	must(0, b.Setitem(0, constant("a")))
	must(0, b.Setitem(0, nil))
	assertEq(t, must(slots(b).Get(0)), nil)
}

func TestGetitemReturnsEntryNotRecord(t *testing.T) {
	b := mb()
	entry := constant("a")
	must(0, b.Setitem(0, entry))
	out := must(b.Getitem(0))
	asEntry(t, out)
	assertIs(t, out, entry)
}

func TestGetitemEmptySlotIsNone(t *testing.T) {
	assertEq(t, must(mb().Getitem(0)), nil)
}

func TestGetitemRawSlotWrappedIntoConstantEntry(t *testing.T) {
	b := mb()
	must(0, slots(b).Set(1, []byte("raw-frame")))
	out := must(b.Getitem(1))
	e := asEntry(t, out)
	assertEq(t, must(e.Data()), []byte("raw-frame"))
	if e.Evolution_ != nil {
		t.Fatalf("evolution %v, want None", *e.Evolution_)
	}
}

func TestGetitemBareEntryInSlotPassesThrough(t *testing.T) {
	b := mb()
	entry := constant("a")
	must(0, slots(b).Set(0, entry))
	assertIs(t, must(b.Getitem(0)), entry)
}

func TestIndexWrapsModuloCapacity(t *testing.T) {
	b := mb(py.Kw{"capacity": 3})
	entry := constant("w")
	must(0, b.Setitem(4, entry))
	assertIs(t, must(b.Getitem(1)), entry)
	assertIs(t, must(b.Getitem(-2)), entry)
	assertIs(t, slotRecord(t, b, 1).Entry, entry)
}

func TestNonIntegerIndexRejected(t *testing.T) {
	b := mb()
	_, err := b.Getitem("0")
	assertRaises(t, py.TypeError, err)
	assertRaises(t, py.TypeError, b.Setitem(true, constant("x")))
	_, err = b.Getitem(py.NewSlice(0, 1))
	assertRaises(t, py.TypeError, err)
}

// =============================================================================
// TestMultiBufferReadWrite
// =============================================================================

func TestWriteGoesThroughSetitemAndAdvances(t *testing.T) {
	b := mb(py.Kw{"capacity": 3})
	entry := constant("a")
	idx := must(b.Write(entry))
	assertEq(t, idx, 0)
	slotRecord(t, b, 0)
	assertEq(t, b.Write_head(), 1)
	assertEq(t, b.Read_head(), 0)
}

func TestReadGoesThroughGetitemAndAdvances(t *testing.T) {
	b := mb(py.Kw{"capacity": 3})
	entry := constant("a")
	must(b.Write(entry))
	out := must(b.Read())
	assertIs(t, out, entry)
	assertEq(t, b.Read_head(), 1)
	assertEq(t, b.Write_head(), 1)
}

func TestReadReturnsWhatIsAtReadHead(t *testing.T) {
	b := mb(py.Kw{"capacity": 3})
	entries := []*laila.Entry{}
	for i := 0; i < 3; i++ {
		entries = append(entries, constant(i))
	}
	for _, e := range entries {
		must(b.Write(e))
	}
	assertIs(t, must(b.Read()), entries[0])
	assertIs(t, must(b.Read()), entries[1])
	assertIs(t, must(b.Read()), entries[2])
}

func TestReadOfEmptySlotReturnsNoneAndAdvances(t *testing.T) {
	b := mb()
	assertEq(t, must(b.Read()), nil)
	assertEq(t, b.Read_head(), 1)
}

func TestHeadsWrapIndependently(t *testing.T) {
	b := mb(py.Kw{"capacity": 2})
	a, bb, c := constant("a"), constant("b"), constant("c")
	must(b.Write(a))
	must(b.Write(bb))
	assertEq(t, b.Write_head(), 0)
	assertEq(t, b.Read_head(), 0)
	must(b.Write(c)) // overwrites slot 0
	assertEq(t, b.Write_head(), 1)
	assertIs(t, must(b.Read()), c)
	assertIs(t, must(b.Read()), bb)
	assertEq(t, b.Read_head(), 0)
}

func TestDoubleBufferProducerConsumerHandoff(t *testing.T) {
	b := mb()
	for i := 0; i < 10; i++ {
		must(b.Write(constant(i)))
		assertEq(t, entryData(t, must(b.Read())), i)
		assertEq(t, b.Read_head(), b.Write_head())
	}
}

func TestWriteRequiresValueWhenNotMapped(t *testing.T) {
	_, err := mb().Write()
	assertRaises(t, py.TypeError, err)
}

func TestWriteRawPayloadWhenNotMappedIsRecorded(t *testing.T) {
	b := mb()
	must(b.Write([]byte("frame")))
	slotRecord(t, b, 0)
	assertEq(t, entryData(t, must(b.Read())), []byte("frame"))
}

// Spy is ``class Spy(MultiBuffer)`` overriding ``__setitem__`` /
// ``__getitem__`` to log the keys it sees.
type Spy struct {
	laila.MultiBuffer

	calls *[]py.Tuple `py:"-"`
	logGets bool       `py:"-"`
}

var Spy_class = py.NewClass("Spy", (*Spy)(nil), laila.MultiBuffer_class)

func newSpy(calls *[]py.Tuple, logGets bool, kw ...py.Kw) *Spy {
	s := Spy_class.New().(*Spy)
	s.calls = calls
	s.logGets = logGets
	must(0, s.Init_(py.MergeKw(kw...)))
	return s
}

func (s *Spy) Setitem(key any, value any) error {
	if s.logGets {
		*s.calls = append(*s.calls, py.Tuple{"set", key})
	} else {
		*s.calls = append(*s.calls, py.Tuple{key})
	}
	return s.MultiBuffer.Setitem(key, value)
}

func (s *Spy) Getitem(key any) (any, error) {
	if s.logGets {
		*s.calls = append(*s.calls, py.Tuple{"get", key})
	}
	return s.MultiBuffer.Getitem(key)
}

func TestUsesSetitemAndGetitemOverrides(t *testing.T) {
	calls := []py.Tuple{}
	spy := newSpy(&calls, true, py.Kw{"capacity": 2})
	must(spy.Write(constant(1)))
	must(spy.Read())
	assertEq(t, calls, []py.Tuple{{"set", 0}, {"get", 0}})
}

// =============================================================================
// TestMultiBufferEmpty
// =============================================================================

func TestEmptyClearsSlotsAndRewindsHeads(t *testing.T) {
	b := mb(py.Kw{"capacity": 3})
	must(b.Write(constant(1)))
	must(b.Write(constant(2)))
	must(b.Read())
	must(0, b.Empty())
	assertEq(t, b.Slots, nones(3))
	assertEq(t, b.Read_head(), 0)
	assertEq(t, b.Write_head(), 0)
}

func TestEmptyMutatesExternalSlotsInPlace(t *testing.T) {
	external := py.NewList([]byte("a"), []byte("b"))
	b := mb(py.Kw{"slots": external, "mapped": true})
	must(0, b.Empty())
	assertEq(t, external, nones(2))
}

// =============================================================================
// TestMultiBufferMappedMode -- the camera story: hardware fills the
// slots, laila only moves heads and wraps reads.
// =============================================================================

func TestWriteBypassesSetitem(t *testing.T) {
	calls := []py.Tuple{}
	dma := nones(2)
	spy := newSpy(&calls, false, py.Kw{"slots": dma, "mapped": true})
	must(0, dma.Set(0, []byte("\x00")))
	idx := must(spy.Write())
	assertEq(t, idx, 0)
	assertEq(t, calls, []py.Tuple{})
	assertEq(t, spy.Write_head(), 1)
	assertEq(t, must(dma.Get(0)), []byte("\x00"))
}

func TestWriteWithValueDepositsRawBytes(t *testing.T) {
	dma := nones(2)
	b := mb(py.Kw{"slots": dma, "mapped": true})
	must(b.Write([]byte("\x01\x02")))
	assertEq(t, must(dma.Get(0)), []byte("\x01\x02"))
	if _, isRecord := must(dma.Get(0)).(*laila.Record); isRecord {
		t.Fatal("slot should hold raw bytes, not a Record")
	}
}

func TestReadWrapsHardwareBytesIntoEntry(t *testing.T) {
	dma := nones(2)
	b := mb(py.Kw{"slots": dma, "mapped": true})
	must(0, dma.Set(0, []byte("frame-0")))
	must(b.Write())
	out := must(b.Read())
	assertEq(t, entryData(t, out), []byte("frame-0"))
	assertEq(t, b.Read_head(), 1)
}

func TestExternallyMutatedSlotVisibleWithoutWrite(t *testing.T) {
	dma := nones(2)
	b := mb(py.Kw{"slots": dma, "mapped": true})
	must(0, dma.Set(0, []byte("late")))
	assertEq(t, entryData(t, must(b.Read())), []byte("late"))
}

func TestBytearrayBackedSlots(t *testing.T) {
	// bytearray(2) -> a mutable []byte buffer mutated in place.
	buf0, buf1 := make([]byte, 2), make([]byte, 2)
	dma := py.NewList(buf0, buf1)
	b := mb(py.Kw{"slots": dma, "mapped": true})
	copy(buf0, []byte("\xaa\xbb"))
	must(b.Write())
	if got := entryData(t, must(b.Read())).([]byte); !bytes.Equal(got, []byte("\xaa\xbb")) {
		t.Fatalf("got %q", got)
	}
}

func TestCameraLoopMemorizeRememberRoundtrip(t *testing.T) {
	policy := must(laila.Get_active_policy()).(*laila.LAILA_IDENTIFIABLE_POLICY_)
	dma := nones(2)
	frames := mb(py.Kw{"slots": dma, "mapped": true})

	captured := []*laila.Entry{}
	for i := 0; i < 4; i++ {
		must(0, dma.Set(frames.Write_head(), bytes.Repeat([]byte{byte(i)}, 4))) // "hardware" fills the slot
		must(frames.Write())
		entry := asEntry(t, must(frames.Read()))
		identity := must(laila.Memorize(py.Kw{"entries": entry}))
		must(must(policy.Future_bank.Getitem(laila.Gid_of_(identity))).(waiter).Wait())
		captured = append(captured, entry)
	}

	for i, entry := range captured {
		var ref any
		must(0, laila.Guarantee.With(func() error {
			var err error
			ref, err = laila.Remember(py.Kw{"entry_ids": entry.Global_id()})
			return err
		}))
		back := must(must(policy.Future_bank.Getitem(laila.Gid_of_(ref))).(resulter).Result())
		e := asEntry(t, back)
		assertEq(t, e.Global_id(), entry.Global_id())
		assertEq(t, must(e.Data()), bytes.Repeat([]byte{byte(i)}, 4))
	}
}

type waiter interface{ Wait(timeout ...any) (any, error) }
type resulter interface{ Result() (any, error) }

// =============================================================================
// TestMultiBufferCliContract
// =============================================================================

func TestSlotsIsCliExempt(t *testing.T) {
	eligible := laila.Eligible_model_fields_(laila.MultiBuffer_class)
	if slices.Contains(eligible, "slots") {
		t.Fatal("slots must be CLI-exempt")
	}
	if !slices.Contains(eligible, "capacity") {
		t.Fatal("capacity must be CLI-eligible")
	}
	if !slices.Contains(eligible, "mapped") {
		t.Fatal("mapped must be CLI-eligible")
	}
}

func TestExplicitUuidRespected(t *testing.T) {
	b := mb(py.Kw{"uuid": "12345678-1234-5678-1234-567812345678"})
	assertEq(t, b.Uuid(), "12345678-1234-5678-1234-567812345678")
}

func TestGlobalIdHasMultibufferScope(t *testing.T) {
	if !strings.Contains(mb().Global_id(), "MULTI_BUFFER") {
		t.Fatal(mb().Global_id())
	}
}
