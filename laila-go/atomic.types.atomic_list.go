package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// atomic/types/atomic_list.py -- thread-safe generic list with snapshot
// iterators and atomic batching.
//
//   - The standard mutable-sequence API (Getitem, Setitem, Append, Extend,
//     Pop, Remove, Clear, ...) -- all guarded by the lock.
//   - *Snapshot* iteration: Iter returns a copy taken under the lock.
//   - Atomic() yields the raw list while the lock is held so a sequence of
//     operations appears atomic to other goroutines.
//
// Port note: ``AtomicList[T]`` is ``AtomicList`` with ``list[Any]`` items;
// the type parameter is erased (pydantic generics have no Go equivalent
// in the class registry).

// AtomicList is a thread-safe list with common list operations.
type AtomicList struct {
	LAILA_LOCALLY_ATOMIC_OBJECT_
	Value *py.List  `py:"value,factory=Default_value_"`
	Lock_ *py.RLock `py:"_lock"`
}

// AtomicList_class is the class descriptor of AtomicList.
var AtomicList_class = py.NewClass("AtomicList", (*AtomicList)(nil), LAILA_LOCALLY_ATOMIC_OBJECT_class).
	Set_private_default("_lock", func() any { return py.NewRLock() })

// New_AtomicList is ``AtomicList(value=[])``.
func New_AtomicList(kw ...py.Kw) (*AtomicList, error) {
	o := AtomicList_class.New().(*AtomicList)
	if err := o.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return o, nil
}

// Default_value_ is ``Field(default_factory=list)``.
func (l *AtomicList) Default_value_() *py.List { return py.NewList() }

// Len is ``__len__``: the number of elements.
func (l *AtomicList) Len() int {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	return l.Value.Len()
}

// Repr is ``AtomicList([...])``.
func (l *AtomicList) Repr() string {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	return "AtomicList(" + py.Repr(l.Value) + ")"
}

// Iter is ``__iter__``: iterates over a snapshot of the list.
func (l *AtomicList) Iter() []any {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	return l.Value.Items()
}

// Getitem is ``__getitem__``: the element at an int index, or a snapshot
// slice (``*py.List``) for a py.Slice.
func (l *AtomicList) Getitem(idx any) (any, error) {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	if s, ok := idx.(py.Slice); ok {
		return l.Value.Getslice(s)
	}
	i, err := py.Int(idx)
	if err != nil {
		return nil, py.TypeError.Newf("list indices must be integers or slices, not %s", py.Pytypename(idx))
	}
	return l.Value.Get(i)
}

// Setitem is ``__setitem__``: sets element(s) at *idx* (int or py.Slice).
func (l *AtomicList) Setitem(idx any, item any) error {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	if s, ok := idx.(py.Slice); ok {
		return l.Value.Setslice(s, item)
	}
	i, err := py.Int(idx)
	if err != nil {
		return py.TypeError.Newf("list indices must be integers or slices, not %s", py.Pytypename(idx))
	}
	return l.Value.Set(i, item)
}

// Delitem is ``__delitem__``: deletes element(s) at *idx* (int or py.Slice).
func (l *AtomicList) Delitem(idx any) error {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	if s, ok := idx.(py.Slice); ok {
		return l.Value.Delslice(s)
	}
	i, err := py.Int(idx)
	if err != nil {
		return py.TypeError.Newf("list indices must be integers or slices, not %s", py.Pytypename(idx))
	}
	return l.Value.Del(i)
}

// Append appends *item* to the end of the list.
func (l *AtomicList) Append(item any) {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	l.Value.Append(item)
}

// Extend extends the list with elements from *items*.
func (l *AtomicList) Extend(items any) {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	l.Value.Extend(items)
}

// Insert inserts *item* before *index*.
func (l *AtomicList) Insert(index int, item any) {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	l.Value.Insert(index, item)
}

// Clear removes all elements.
func (l *AtomicList) Clear() {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	l.Value.Clear()
}

// Remove removes the first occurrence of *item* (ValueError when absent).
func (l *AtomicList) Remove(item any) error {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	return l.Value.Remove(item)
}

// Pop removes and returns the item at *index* (default last).
func (l *AtomicList) Pop(index ...int) (any, error) {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	i := -1
	if len(index) > 0 {
		i = index[0]
	}
	return l.Value.Pop(i)
}

// Count returns the number of occurrences of *item*.
func (l *AtomicList) Count(item any) int {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	return l.Value.Count(item)
}

// Index returns the index of the first occurrence of *item*
// (``index(item, start=0, stop=None)``).
func (l *AtomicList) Index(item any, start_stop ...int) (int, error) {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	start := 0
	if len(start_stop) > 0 {
		start = start_stop[0]
	}
	if len(start_stop) > 1 {
		return l.Value.Index_range(item, start, start_stop[1])
	}
	return l.Value.Index_range(item, start)
}

// To_list returns a snapshot (shallow copy) of the list.
func (l *AtomicList) To_list() []any {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	return l.Value.Items()
}

// Set_at sets the element at *index* to *item*.
func (l *AtomicList) Set_at(index int, item any) error {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	return l.Value.Set(index, item)
}

// Get_at returns the element at *index*.
func (l *AtomicList) Get_at(index int) (any, error) {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	return l.Value.Get(index)
}

// Slice is a snapshot slice, equivalent to ``self.value[start:stop:step]``
// (nil is None).
func (l *AtomicList) Slice(start, stop *int, step ...*int) ([]any, error) {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	s := py.Slice{Start: start, Stop: stop}
	if len(step) > 0 {
		s.Step = step[0]
	}
	out, err := l.Value.Getslice(s)
	if err != nil {
		return nil, err
	}
	return out.Items(), nil
}

// Trim keeps only items in [start:stop] in place, like list slicing;
// supports negative indices (nil is None).
func (l *AtomicList) Trim(start, stop *int) {
	l.Lock_.Lock_()
	defer l.Lock_.Unlock()
	n := l.Value.Len()
	s, e := 0, n
	if start != nil {
		s = *start
		if s < 0 {
			s = n + s
		}
	}
	if stop != nil {
		e = *stop
		if e < 0 {
			e = n + e
		}
	}
	s = max(0, min(s, n))
	e = max(0, min(e, n))
	if s >= e {
		l.Value.Clear()
	} else {
		l.Value.Replace(l.Value.Items()[s:e])
	}
}

// Atomic exposes the underlying list while the lock is held
// (``with lst.atomic() as L: L.append(x)``); defer the returned release.
func (l *AtomicList) Atomic() (*py.List, func()) {
	l.Lock_.Lock_()
	return l.Value, l.Lock_.Unlock
}
