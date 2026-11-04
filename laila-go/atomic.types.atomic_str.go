package laila

import (
	"fmt"
	"unsafe"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// atomic/types/atomic_str.py -- thread-safe mutable string.
//
// AtomicStr wraps a string with an RLock so set / append / clear / get are
// race-free between goroutines. Every "mutation" re-binds ``value``; the
// lock ensures readers always see a consistent snapshot.

// AtomicStr is a thread-safe string with atomic set, append, clear and
// get operations.
type AtomicStr struct {
	LAILA_LOCALLY_ATOMIC_OBJECT_
	Value string    `py:"value" default:""`
	Lock_ *py.RLock `py:"_lock"`
	// Buf_ is the growable backing store of Value used by Append (see
	// there); not a Python attribute.
	Buf_ []byte `py:"-"`
}

// AtomicStr_class is the class descriptor of AtomicStr.
var AtomicStr_class = py.NewClass("AtomicStr", (*AtomicStr)(nil), LAILA_LOCALLY_ATOMIC_OBJECT_class).
	Set_private_default("_lock", func() any { return py.NewRLock() })

// New_AtomicStr is ``AtomicStr(value="")``.
func New_AtomicStr(kw ...py.Kw) (*AtomicStr, error) {
	o := AtomicStr_class.New().(*AtomicStr)
	if err := o.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return o, nil
}

// Set replaces the string with ``str(new_value)``.
func (s *AtomicStr) Set(new_value any) {
	s.Lock_.Lock_()
	defer s.Lock_.Unlock()
	s.Value = py.Str(new_value)
}

// Get returns the current string value.
func (s *AtomicStr) Get() string {
	s.Lock_.Lock_()
	defer s.Lock_.Unlock()
	return s.Value
}

// Append appends ``str(suffix)`` and returns the resulting string.
//
// Amortised O(len(suffix)): Python pops ``value`` out of the instance dict
// so CPython can extend the only reference in place. Go strings are
// immutable views, so the port grows a byte buffer (``Buf_``) and points
// ``value`` at its prefix without copying: bytes inside an already
// published prefix are never written again, so earlier values stay valid.
// When ``value`` was rebound elsewhere (or the object was copied) the
// buffer is rebuilt from it -- one copy, like Python's.
func (s *AtomicStr) Append(suffix any) string {
	s.Lock_.Lock_()
	defer s.Lock_.Unlock()
	suf := py.Str(suffix)
	if len(s.Buf_) == 0 || len(s.Buf_) != len(s.Value) || unsafe.StringData(s.Value) != unsafe.SliceData(s.Buf_) {
		s.Buf_ = append(make([]byte, 0, 2*(len(s.Value)+len(suf))), s.Value...)
	}
	s.Buf_ = append(s.Buf_, suf...)
	s.Value = unsafe.String(unsafe.SliceData(s.Buf_), len(s.Buf_))
	return s.Value
}

// Clear resets the string to empty.
func (s *AtomicStr) Clear() {
	s.Lock_.Lock_()
	defer s.Lock_.Unlock()
	s.Value = ""
}

// Length returns len(value) in code points (Python ``len``).
func (s *AtomicStr) Length() int {
	s.Lock_.Lock_()
	defer s.Lock_.Unlock()
	return py.Must(py.Len(s.Value))
}

// Atomic returns the string with its lock held for batched operations
// (``with s.atomic() as locked: locked.value = "x"``); defer the release.
func (s *AtomicStr) Atomic() (*AtomicStr, func()) {
	s.Lock_.Lock_()
	return s, s.Lock_.Unlock
}

// String is ``__str__``: the string value.
func (s *AtomicStr) String() string { return s.Get() }

// Repr is ``AtomicStr('abc')``.
func (s *AtomicStr) Repr() string { return fmt.Sprintf("AtomicStr(%s)", py.Repr_str(s.Get())) }
