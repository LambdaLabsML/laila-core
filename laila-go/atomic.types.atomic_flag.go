package laila

import (
	"fmt"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// atomic/types/atomic_flag.py -- thread-safe boolean flag.
//
// AtomicFlag is the simplest of the atomic types: a single boolean guarded
// by an RLock so set / clear / toggle / is_set are safe to call from
// multiple goroutines. Use it for "has this happened yet?" signals where a
// full py.Event is overkill because nobody ever needs to *wait* on it.

// AtomicFlag is a thread-safe boolean flag with atomic set / clear /
// toggle / get operations.
type AtomicFlag struct {
	LAILA_LOCALLY_ATOMIC_OBJECT_
	Value bool      `py:"value" default:"false"`
	Lock_ *py.RLock `py:"_lock"`
}

// AtomicFlag_class is the class descriptor of AtomicFlag.
var AtomicFlag_class = py.NewClass("AtomicFlag", (*AtomicFlag)(nil), LAILA_LOCALLY_ATOMIC_OBJECT_class).
	Set_private_default("_lock", func() any { return py.NewRLock() })

// New_AtomicFlag is ``AtomicFlag(value=False)``.
func New_AtomicFlag(kw ...py.Kw) (*AtomicFlag, error) {
	o := AtomicFlag_class.New().(*AtomicFlag)
	if err := o.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return o, nil
}

// Set sets the flag to true.
func (f *AtomicFlag) Set() {
	f.Lock_.Lock_()
	defer f.Lock_.Unlock()
	f.Value = true
}

// Clear sets the flag to false.
func (f *AtomicFlag) Clear() {
	f.Lock_.Lock_()
	defer f.Lock_.Unlock()
	f.Value = false
}

// Toggle inverts the flag value.
func (f *AtomicFlag) Toggle() {
	f.Lock_.Lock_()
	defer f.Lock_.Unlock()
	f.Value = !f.Value
}

// Is_set returns the current flag value.
func (f *AtomicFlag) Is_set() bool {
	f.Lock_.Lock_()
	defer f.Lock_.Unlock()
	return f.Value
}

// Bool is ``__bool__``: ``if flag:`` reads the flag value.
func (f *AtomicFlag) Bool() bool { return f.Is_set() }

// Set_to sets the flag to *state*.
func (f *AtomicFlag) Set_to(state any) {
	f.Lock_.Lock_()
	defer f.Lock_.Unlock()
	f.Value = py.Truthy(state)
}

// Atomic returns the flag with its lock held for batched operations
// (``with flag.atomic() as locked: locked.value = True``); defer the
// returned release.
func (f *AtomicFlag) Atomic() (*AtomicFlag, func()) {
	f.Lock_.Lock_()
	return f, f.Lock_.Unlock
}

// Repr is ``AtomicFlag(True)``.
func (f *AtomicFlag) Repr() string {
	f.Lock_.Lock_()
	defer f.Lock_.Unlock()
	return fmt.Sprintf("AtomicFlag(%s)", py.Repr(f.Value))
}
