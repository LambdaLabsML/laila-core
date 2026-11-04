package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// atomic/types/atomic_dotmap.py -- thread-safe dot-notation attribute map.
//
// AtomicDotMap is the threaded sibling of DotMap: attribute access over an
// underlying dict where every read and write goes through an RLock, so
// concurrent producers and consumers never observe a half-written tree.
//
// Port note: ``m.a = 1`` / ``m.a`` / ``del m.a`` are Setattr / Getattr /
// Delattr; the same methods back py.Setattr / py.Getattr / py.Delattr for
// dynamic (RPC) access, mirroring __setattr__ / __getattr__ / __delattr__.

// INTERNAL_ATTRS_ are the attributes stored directly on the instance rather
// than in the user-visible ``_data`` mapping.
var INTERNAL_ATTRS_ = map[string]bool{"_data": true, "_lock": true, "_creation_timestamp": true}

// AtomicDotMap is a thread-safe mapping accessed via attribute syntax.
type AtomicDotMap struct {
	LAILA_LOCALLY_ATOMIC_OBJECT_
	Data_ *py.Dict  `py:"_data"`
	Lock_ *py.RLock `py:"_lock"`
}

// AtomicDotMap_class is the class descriptor of AtomicDotMap.
var AtomicDotMap_class = py.NewClass("AtomicDotMap", (*AtomicDotMap)(nil), LAILA_LOCALLY_ATOMIC_OBJECT_class)

// New_AtomicDotMap is ``AtomicDotMap()``.
func New_AtomicDotMap() *AtomicDotMap {
	o := AtomicDotMap_class.New().(*AtomicDotMap)
	o.Init_()
	return o
}

// Init_ initialises an empty data dict and a reentrant lock. Deliberately
// does not chain to the pydantic initialiser (the map has no declared
// fields), so the LAILA_OBJECT_ creation timestamp is stamped here.
func (m *AtomicDotMap) Init_() {
	m.Data_ = py.NewDict()
	m.Lock_ = py.NewRLock()
	ts := Now_creation_timestamp_()
	m.Creation_timestamp_ = &ts
}

// Getattr is ``__getattr__``: the value for *key*, or nil if absent.
func (m *AtomicDotMap) Getattr(key string) (any, error) {
	m.Lock_.Lock_()
	defer m.Lock_.Unlock()
	return m.Data_.GetDefault(key, nil), nil
}

// Get is ``m.<key>``: the value for *key*, or nil if absent.
func (m *AtomicDotMap) Get(key string) any {
	v, _ := m.Getattr(key)
	return v
}

// Setattr is ``__setattr__``: sets *key* to *value* (internal attrs bypass
// the data mapping and the lock).
func (m *AtomicDotMap) Setattr(key string, value any) error {
	if INTERNAL_ATTRS_[key] {
		return py.Setattr(m.Self(), key, value)
	}
	m.Lock_.Lock_()
	defer m.Lock_.Unlock()
	m.Data_.Set(key, value)
	return nil
}

// Set is ``m.<key> = value``.
func (m *AtomicDotMap) Set(key string, value any) { _ = m.Setattr(key, value) }

// Delattr is ``__delattr__``: deletes *key* (KeyError when absent, like
// ``del self._data[key]``).
func (m *AtomicDotMap) Delattr(key string) error {
	m.Lock_.Lock_()
	defer m.Lock_.Unlock()
	return m.Data_.Del(key)
}

// To_dict returns a shallow copy of the internal data as a plain dict.
func (m *AtomicDotMap) To_dict() *py.Dict {
	m.Lock_.Lock_()
	defer m.Lock_.Unlock()
	return m.Data_.Copy()
}

// Keys returns a snapshot list of keys.
func (m *AtomicDotMap) Keys() []any {
	m.Lock_.Lock_()
	defer m.Lock_.Unlock()
	return m.Data_.Keys()
}

// Values returns a snapshot list of values.
func (m *AtomicDotMap) Values() []any {
	m.Lock_.Lock_()
	defer m.Lock_.Unlock()
	return m.Data_.Values()
}

// Items returns a snapshot list of (key, value) pairs.
func (m *AtomicDotMap) Items() []py.Item {
	m.Lock_.Lock_()
	defer m.Lock_.Unlock()
	return m.Data_.Items()
}

// Repr is ``AtomicDotMap({...})``.
func (m *AtomicDotMap) Repr() string {
	m.Lock_.Lock_()
	defer m.Lock_.Unlock()
	return "AtomicDotMap(" + py.Repr(m.Data_) + ")"
}
