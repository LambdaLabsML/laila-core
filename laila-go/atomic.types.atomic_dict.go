package laila

import (
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// atomic/types/atomic_dict.py -- thread-safe, insertion-ordered dictionary
// with atomic context support.
//
// AtomicDict is a drop-in MutableMapping whose every mutation is guarded by
// an internal RLock. On top of the standard mapping API it adds:
//
//   - insertion-ordered iteration: the backing ``data`` dict is the single
//     order authority, so keyed deletes are O(1) and iteration never goes
//     stale after out-of-band edits of ``data``. A positional index
//     (``_order``) is rebuilt lazily only for the positional accessors;
//   - positional accessors (Item_at / Key_at / Value_at) and an in-place
//     slice (Trim) against the insertion order;
//   - Atomic(), yielding a view able to batch mutations *under the lock*,
//     plus the context-local AtomicDict_current() accessor;
//   - Compute and Increment for read-modify-write patterns.
//
// Port note: ``AtomicDict[K, V]`` is ``AtomicDict`` with ``any`` keys and
// values (type parameters are erased).

// AtomicDict is a thread-safe, insertion-ordered dict with index/slice ops
// and robust order syncing.
type AtomicDict struct {
	LAILA_LOCALLY_ATOMIC_OBJECT_
	Data   *py.Dict  `py:"data,factory=Default_data_"`
	Lock_  *py.RLock `py:"_lock"`
	Order_ []any     `py:"_order"` // nil is None
}

// AtomicDict_class is the class descriptor of AtomicDict.
var AtomicDict_class = py.NewClass("AtomicDict", (*AtomicDict)(nil), LAILA_LOCALLY_ATOMIC_OBJECT_class).
	Set_private_default("_lock", func() any { return py.NewRLock() })

// AtomicDict_current_ is the ``_current`` ClassVar: the context-local
// AtomicDict under an atomic block (thread- & task-safe).
var AtomicDict_current_ = py.NewContextVar[*AtomicDict]("AtomicDict_current", nil)

// New_AtomicDict is ``AtomicDict(*args, **kwargs)``: at most one positional
// dict argument, or ``data=`` through a trailing py.Kw.
//
// TypeError on conflicting or unexpected arguments.
func New_AtomicDict(args ...any) (*AtomicDict, error) {
	o := AtomicDict_class.New().(*AtomicDict)
	if err := o.Init_(args...); err != nil {
		return nil, err
	}
	return o, nil
}

// Init_ is ``__init__(*args, **kwargs)``.
func (d *AtomicDict) Init_(args ...any) error {
	pos, kwargs := py.SplitKw(args)
	kwargs = kwargs.Copy()
	if len(pos) > 0 && isDictLike(pos[0]) {
		if kwargs.Has("data") {
			return py.TypeError.New("Cannot pass both dict positional arg and data keyword arg")
		}
		kwargs["data"] = pos[0]
		pos = pos[1:]
	}
	if len(pos) > 0 {
		return py.TypeError.New("AtomicDict accepts at most one positional dict argument")
	}
	data := kwargs.Pop("data", nil)
	if data == nil {
		data = py.NewDict()
	}
	if len(kwargs) > 0 {
		keys := make([]string, 0, len(kwargs))
		for k := range kwargs {
			keys = append(keys, k)
		}
		return py.TypeError.Newf("Unexpected keyword arguments: %s", strings.Join(py.Sorted_strings(keys), ", "))
	}
	// ``model_construct(data=data)``: no validation, private defaults applied.
	if err := py.Assign_fields(d.Self(), d.Class_(), py.Kw{"data": data}, false); err != nil {
		return err
	}
	d.Mark_field_set("data")
	// ``model_construct`` skips ``_LAILA_OBJECT.__init__``; stamp here.
	ts := Now_creation_timestamp_()
	d.Creation_timestamp_ = &ts
	d.Order_ = nil
	return nil
}

func isDictLike(v any) bool {
	switch v.(type) {
	case *py.Dict, py.Kw, map[string]any:
		return true
	}
	return false
}

// Default_data_ is ``Field(default_factory=dict)``.
func (d *AtomicDict) Default_data_() *py.Dict { return py.NewDict() }

// Positional_ is ``_positional()``: the positional key index, rebuilt from
// ``data`` when stale. ``data`` is the order authority; ``_order`` is only
// a cache for the positional accessors, dropped by every mutation.
func (d *AtomicDict) Positional_() []any {
	order := d.Order_
	if order == nil || len(order) != d.Data.Len() {
		order = d.Data.Keys()
		d.Order_ = order
	}
	return order
}

// Reindex rebuilds the positional index from the underlying dict.
func (d *AtomicDict) Reindex() {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	d.Order_ = d.Data.Keys()
}

// Set_nolock_ inserts or updates *key* without acquiring the lock.
func (d *AtomicDict) Set_nolock_(key, value any) {
	if !d.Data.Contains(key) {
		order := d.Order_
		if order != nil && len(order) == d.Data.Len() {
			d.Order_ = append(order, key)
		} else {
			d.Order_ = nil
		}
	}
	d.Data.Set(key, value)
}

// Del_nolock_ deletes *key* without acquiring the lock (O(1)).
func (d *AtomicDict) Del_nolock_(key any) error {
	if err := d.Data.Del(key); err != nil {
		return err
	}
	d.Order_ = nil
	return nil
}

// Getitem is ``d[key]``: KeyError if missing.
func (d *AtomicDict) Getitem(key any) (any, error) {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	return d.Data.Getitem(key)
}

// Setitem is ``d[key] = value`` under the lock.
func (d *AtomicDict) Setitem(key, value any) {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	d.Set_nolock_(key, value)
}

// Delitem is ``del d[key]`` under the lock.
func (d *AtomicDict) Delitem(key any) error {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	return d.Del_nolock_(key)
}

// Len is ``len(d)``.
func (d *AtomicDict) Len() int {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	return d.Data.Len()
}

// Iter is ``iter(d)``: keys in insertion order (snapshot).
func (d *AtomicDict) Iter() []any {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	return d.Data.Keys()
}

// Contains is ``key in d``.
func (d *AtomicDict) Contains(key any) bool {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	return d.Data.Contains(key)
}

// Repr is ``AtomicDict({...})``.
func (d *AtomicDict) Repr() string {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	return "AtomicDict(" + py.Repr(d.Data) + ")"
}

// Get returns the value for *key*, or *default* (nil) if absent.
func (d *AtomicDict) Get(key any, def ...any) any {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	var dv any
	if len(def) > 0 {
		dv = def[0]
	}
	return d.Data.GetDefault(key, dv)
}

// Setdefault returns the value for *key*, inserting *default* if absent.
func (d *AtomicDict) Setdefault(key, def any) any {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	if v, ok := d.Data.Get(key); ok {
		return v
	}
	d.Set_nolock_(key, def)
	return def
}

// Pop removes and returns the value for *key*, or *default* if absent;
// KeyError when absent and no default is given.
func (d *AtomicDict) Pop(key any, def ...any) (any, error) {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	if val, ok := d.Data.Get(key); ok {
		if err := d.Del_nolock_(key); err != nil {
			return nil, err
		}
		return val, nil
	}
	if len(def) == 0 {
		return nil, py.KeyError.New(key)
	}
	return def[0], nil
}

// Popitem removes and returns the last inserted (key, value) pair.
func (d *AtomicDict) Popitem() (any, any, error) {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	if d.Data.Len() == 0 {
		return nil, nil, py.KeyError.New("AtomicDict is empty")
	}
	k, v, err := d.Data.Popitem()
	if err != nil {
		return nil, nil, err
	}
	order := d.Order_
	if order != nil && len(order) > 0 && py.Eq(order[len(order)-1], k) {
		d.Order_ = order[:len(order)-1]
	} else {
		d.Order_ = nil
	}
	return k, v, nil
}

// Pop_next removes and returns the *first* inserted (key, value) pair.
// Hot path for queue-style consumers: the backing dict is insertion-
// ordered and authoritative, so its first key *is* the oldest entry. O(1).
func (d *AtomicDict) Pop_next() (any, any, error) {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	if d.Data.Len() == 0 {
		return nil, nil, py.KeyError.New("AtomicDict is empty")
	}
	k, v, err := d.Data.PopFirst()
	if err != nil {
		return nil, nil, err
	}
	d.Order_ = nil
	return k, v, nil
}

// Clear removes all items.
func (d *AtomicDict) Clear() {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	d.Data.Clear()
	d.Order_ = nil
}

// Update merges items from *other* (a mapping or an iterable of pairs;
// nil for none) and/or keyword arguments.
func (d *AtomicDict) Update(other any, kw ...py.Kw) {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	d.update_nolock_(other, py.MergeKw(kw...))
}

func (d *AtomicDict) update_nolock_(other any, kwargs py.Kw) {
	if !py.IsNone(other) {
		if hasKeys(other) {
			for _, it := range mappingItems(other) {
				d.Set_nolock_(it.Key, it.Value)
			}
		} else {
			for _, pair := range py.Iter(other) {
				kv := py.Iter(pair)
				d.Set_nolock_(kv[0], kv[1])
			}
		}
	}
	for _, k := range py.Sorted_strings(keysOf(kwargs)) {
		d.Set_nolock_(k, kwargs[k])
	}
}

func keysOf(kw py.Kw) []string {
	out := make([]string, 0, len(kw))
	for k := range kw {
		out = append(out, k)
	}
	return out
}

// Keys returns a snapshot list of keys in insertion order.
func (d *AtomicDict) Keys() []any {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	return d.Data.Keys()
}

// Values returns a snapshot list of values in insertion order.
func (d *AtomicDict) Values() []any {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	return d.Data.Values()
}

// Items returns a snapshot list of (key, value) pairs in insertion order.
func (d *AtomicDict) Items() []py.Item {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	return d.Data.Items()
}

// Item_at returns the (key, value) pair at positional *index* (supports
// negative indexing); IndexError when out of range.
func (d *AtomicDict) Item_at(index int) (py.Item, error) {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	order := d.Positional_()
	n := len(order)
	if index < 0 {
		index += n
	}
	if index < 0 || index >= n {
		return py.Item{}, py.IndexError.New("Index out of range")
	}
	k := order[index]
	if !d.Data.Contains(k) { // same-length out-of-band edit of ``data``
		order = d.Data.Keys()
		d.Order_ = order
		k = order[index]
	}
	v, _ := d.Data.Get(k)
	return py.Item{Key: k, Value: v}, nil
}

// Key_at returns the key at positional *index*.
func (d *AtomicDict) Key_at(index int) (any, error) {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	it, err := d.Item_at(index)
	if err != nil {
		return nil, err
	}
	return it.Key, nil
}

// Value_at returns the value at positional *index*.
func (d *AtomicDict) Value_at(index int) (any, error) {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	it, err := d.Item_at(index)
	if err != nil {
		return nil, err
	}
	return it.Value, nil
}

// Trim keeps only items in [start:end) by insertion order (end exclusive;
// nil is None).
func (d *AtomicDict) Trim(start, end *int) {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	order := d.Data.Keys()
	d.Order_ = order
	n := len(order)

	s, e := 0, n
	if start != nil {
		s = *start
	}
	if end != nil {
		e = *end
	}
	if s < 0 {
		s += n
	}
	if e < 0 {
		e += n
	}
	s = max(s, 0)
	e = min(e, n)

	if s >= e {
		d.Data.Clear()
		d.Order_ = nil
		return
	}

	keep_keys := append([]any(nil), order[s:e]...)
	keep_set := py.NewSet(keep_keys...)
	for _, k := range order {
		if !keep_set.Contains(k) {
			_ = d.Data.Del(k)
		}
	}
	d.Order_ = keep_keys
}

// Compute atomically computes a new value for *key*: fn receives the
// current value (or nil) and returns the new value; nil removes the key.
// Returns the new value, or nil if the key was removed.
func (d *AtomicDict) Compute(key any, fn func(cur any) (any, error)) (any, error) {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	cur := d.Data.GetDefault(key, nil)
	next, err := fn(cur)
	if err != nil {
		return nil, err
	}
	if next == nil {
		if d.Data.Contains(key) {
			if err := d.Del_nolock_(key); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	d.Set_nolock_(key, next)
	return next, nil
}

// Increment adds *delta* (default 1) to the value at *key* (starting from
// *default*, 0) and returns the result (``increment(key, delta=1, default=0)``).
func (d *AtomicDict) Increment(key any, delta_default ...any) (any, error) {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	var delta, def any = 1, 0
	if len(delta_default) > 0 {
		delta = delta_default[0]
	}
	if len(delta_default) > 1 {
		def = delta_default[1]
	}
	val := d.Data.GetDefault(key, def)
	val, err := py.Add(val, delta)
	if err != nil {
		return nil, err
	}
	d.Set_nolock_(key, val)
	return val, nil
}

// Pretty returns a human-readable, multi-line representation
// (``pretty(indent=2)``).
func (d *AtomicDict) Pretty(indent ...int) string {
	d.Lock_.Lock_()
	defer d.Lock_.Unlock()
	ind := 2
	if len(indent) > 0 {
		ind = indent[0]
	}
	lines := []string{"AtomicDict {"}
	for _, it := range d.Data.Items() {
		lines = append(lines, strings.Repeat(" ", ind)+py.Repr(it.Key)+": "+py.Repr(it.Value)+",")
	}
	lines = append(lines, "}")
	return strings.Join(lines, "\n")
}

// AtomicDict_AtomicView is the ``_AtomicView`` proxy yielded by Atomic()
// for lock-held mutations.
type AtomicDict_AtomicView struct {
	P_ *AtomicDict `py:"_p"`
}

// Setitem is ``view[key] = value`` (lock already held).
func (v *AtomicDict_AtomicView) Setitem(key, value any) { v.P_.Set_nolock_(key, value) }

// Delitem is ``del view[key]`` (lock already held).
func (v *AtomicDict_AtomicView) Delitem(key any) error { return v.P_.Del_nolock_(key) }

// Update merges *other* and/or keyword arguments (lock already held).
func (v *AtomicDict_AtomicView) Update(other any, kw ...py.Kw) {
	v.P_.update_nolock_(other, py.MergeKw(kw...))
}

// AtomicDict_Atomic is the ``_Atomic`` context manager: holds the dict
// lock and exposes an AtomicDict_AtomicView.
type AtomicDict_Atomic struct {
	P_     *AtomicDict `py:"_p"`
	Hint   string      `py:"hint"`
	Token_ *py.Token   `py:"_token"`
}

// New_AtomicDict_Atomic is ``_Atomic(parent, hint="")``.
func New_AtomicDict_Atomic(parent *AtomicDict, hint ...string) *AtomicDict_Atomic {
	h := ""
	if len(hint) > 0 {
		h = hint[0]
	}
	return &AtomicDict_Atomic{P_: parent, Hint: h}
}

// Enter is ``__enter__``: acquires the lock and exposes the dict as
// "current".
func (a *AtomicDict_Atomic) Enter() *AtomicDict_AtomicView {
	a.P_.Lock_.Lock_()
	a.Token_ = AtomicDict_current_.Set(a.P_)
	return &AtomicDict_AtomicView{P_: a.P_}
}

// Exit is ``__exit__``: restores "current" and releases the lock.
func (a *AtomicDict_Atomic) Exit() {
	if a.Token_ != nil {
		_ = AtomicDict_current_.Reset(a.Token_)
		a.Token_ = nil
	}
	a.P_.Lock_.Unlock()
}

// Atomic returns the view for batched, lock-held operations
// (``with d.atomic(hint="...") as view``); defer the returned release.
func (d *AtomicDict) Atomic(hint ...string) (*AtomicDict_AtomicView, func()) {
	cm := New_AtomicDict_Atomic(d, hint...)
	return cm.Enter(), cm.Exit
}

// AtomicDict_current is ``AtomicDict.current()``: the AtomicDict currently
// held in an atomic block; RuntimeError outside one.
func AtomicDict_current() (*AtomicDict, error) {
	d := AtomicDict_current_.GetOr(nil)
	if d == nil {
		return nil, py.RuntimeError.New("AtomicDict.current() called outside an AtomicDict context")
	}
	return d, nil
}

// Run_atomic runs *fn* while holding the lock and returns its result.
func (d *AtomicDict) Run_atomic(fn func() (any, error), hint ...string) (any, error) {
	_, release := d.Atomic(hint...)
	defer release()
	return fn()
}
