package laila

import (
	"fmt"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// atomic/types/atomic_int.py -- thread-safe integer counter.
//
// AtomicInt wraps an int with an RLock so increment / decrement / add /
// set / get are race-free even under heavy contention (``i += 1`` is a
// read-modify-write).

// AtomicInt is a thread-safe integer with atomic add, increment,
// decrement, set and get operations.
type AtomicInt struct {
	LAILA_LOCALLY_ATOMIC_OBJECT_
	Value int       `py:"value" default:"0"`
	Lock_ *py.RLock `py:"_lock"`
}

// AtomicInt_class is the class descriptor of AtomicInt.
var AtomicInt_class = py.NewClass("AtomicInt", (*AtomicInt)(nil), LAILA_LOCALLY_ATOMIC_OBJECT_class).
	Set_private_default("_lock", func() any { return py.NewRLock() })

// New_AtomicInt is ``AtomicInt(value=0)``.
func New_AtomicInt(kw ...py.Kw) (*AtomicInt, error) {
	o := AtomicInt_class.New().(*AtomicInt)
	if err := o.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return o, nil
}

// Set_to sets the integer to ``int(new_value)``.
func (i *AtomicInt) Set_to(new_value any) error {
	n, err := py.Int(new_value)
	if err != nil {
		return err
	}
	i.Lock_.Lock_()
	defer i.Lock_.Unlock()
	i.Value = n
	return nil
}

// Get returns the current integer value.
func (i *AtomicInt) Get() int {
	i.Lock_.Lock_()
	defer i.Lock_.Unlock()
	return i.Value
}

// Add adds *delta* to the value and returns the result.
func (i *AtomicInt) Add(delta int) int {
	i.Lock_.Lock_()
	defer i.Lock_.Unlock()
	i.Value += delta
	return i.Value
}

// Increment increments by 1 and returns the new value.
func (i *AtomicInt) Increment() int { return i.Add(1) }

// Decrement decrements by 1 and returns the new value.
func (i *AtomicInt) Decrement() int { return i.Add(-1) }

// Reset resets the value to zero.
func (i *AtomicInt) Reset() {
	i.Lock_.Lock_()
	defer i.Lock_.Unlock()
	i.Value = 0
}

// Atomic returns the integer with its lock held for batched operations
// (``with i.atomic() as locked: locked.value += 10``); defer the release.
func (i *AtomicInt) Atomic() (*AtomicInt, func()) {
	i.Lock_.Lock_()
	return i, i.Lock_.Unlock
}

// Int is ``__int__``: use in integer expressions.
func (i *AtomicInt) Int() int { return i.Get() }

// Repr is ``AtomicInt(5)``.
func (i *AtomicInt) Repr() string { return fmt.Sprintf("AtomicInt(%d)", i.Get()) }
