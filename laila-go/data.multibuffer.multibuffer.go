package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// data/multibuffer/multibuffer.py -- MultiBuffer: an integer-indexed ring
// of records with independent read/write heads.
//
// Where a pool is a *map* keyed by entry ``global_id``, a MultiBuffer is a
// *list*: a fixed number of slots addressed by integer index, plus two
// cursors that walk those slots modulo the capacity. It is the container
// a microcontroller puts in front of a device that produces data faster
// than it can be persisted -- the canonical example being a camera with a
// double (or triple) frame buffer.
//
// # Value contract (shared with pools)
//
//   - ``buf[i] = value`` (Setitem) stores a Record. A bare Entry is wrapped
//     as ``Record(entry=...)`` on the way in, exactly as central memory
//     does before a pool write; raw payloads are first lifted to a
//     constant entry.
//   - ``buf[i]`` (Getitem) returns the bare Entry (``record.entry``). Raw
//     slot contents that were never passed through Setitem -- bytes
//     deposited by hardware -- are wrapped into a constant entry on the
//     way out. Empty slots read as nil.
//
// # Heads
//
// Write targets ``_write_head`` and Read targets ``_read_head``; each call
// advances its own head by one, wrapping at ``capacity``. The heads are
// independent so a producer can run ahead of the consumer by up to
// ``capacity`` slots (the usual double-buffer hand-off), and both are only
// ever mutated under the instance's atomic lock.
//
// # Mapped mode
//
// When the buffer stands in for memory that *something else* fills -- a
// DMA engine, a camera driver, a shared byte buffer -- construct it with
// ``mapped=True`` and pass that memory as ``slots``. The list is used
// as-is (never copied) so external writes are visible immediately. In
// this mode Write does **not** go through Setitem: it only advances the
// write head (optionally depositing a raw value directly into the slot
// first). Read is unchanged -- it still goes through Getitem, which is
// where the raw bytes at the read head become an Entry.

// UNSET_ is the ``_UNSET`` sentinel of ``MultiBuffer.write``.
var UNSET_ = &struct{ name string }{"_UNSET"}

// MultiBuffer is a fixed-capacity ring of records with separate read and
// write heads.
//
//   - Capacity: number of slots. Defaults to 2 (a double buffer). When
//     ``slots`` is supplied explicitly this is overwritten with
//     ``len(slots)``.
//   - Mapped: true when ``slots`` is externally-owned memory that is
//     filled outside laila (see package docs). Changes only what Write
//     does; reads always go through the item protocol.
//   - Slots: the backing storage. Sized to ``capacity`` with nil when not
//     provided. When provided it is used by reference, so a caller (or
//     hardware) mutating the same object is observed by the buffer. Must
//     support ``len``, integer ``__getitem__`` and ``__setitem__``
//     (``*py.List`` or ``[]any``).
type MultiBuffer struct {
	LAILA_IDENTIFIABLE_DATA_CONTAINER_

	Capacity int  `py:"capacity,ge=1" default:"2"`
	Mapped   bool `py:"mapped" default:"false"`
	// Typed as Any (not list[Any]) so the caller's object is handed
	// through untouched -- copying would break mapped mode.
	Slots any `py:"slots,cli_exempt"`

	Read_head_  int `py:"_read_head"`
	Write_head_ int `py:"_write_head"`
}

// MultiBuffer_class is the class descriptor; ``_scopes`` defaults to
// ``[_MULTI_BUFFER_SCOPE]``.
var MultiBuffer_class = py.NewClass("MultiBuffer", (*MultiBuffer)(nil), LAILA_IDENTIFIABLE_DATA_CONTAINER_class).
	Set_private_default("_scopes", func() any { return []string{MULTI_BUFFER_SCOPE_} })

// New_MultiBuffer is ``MultiBuffer(**data)``.
func New_MultiBuffer(kw ...py.Kw) (*MultiBuffer, error) {
	b := MultiBuffer_class.New().(*MultiBuffer)
	if err := b.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return b, nil
}

// Model_post_init sizes the slot list (or adopts the supplied one).
func (b *MultiBuffer) Model_post_init(ctx any) error {
	if err := b.LAILA_IDENTIFIABLE_DATA_CONTAINER_.Model_post_init(ctx); err != nil {
		return err
	}
	if py.IsNone(b.Slots) {
		items := make([]any, b.Capacity)
		b.Slots = py.NewList(items...)
		return nil
	}
	n, err := py.Len(b.Slots)
	if err != nil {
		return py.TypeError.New("MultiBuffer.slots must be a sized, indexable sequence").From(err)
	}
	if n == 0 {
		return py.ValueError.New("MultiBuffer.slots must contain at least one slot")
	}
	b.Capacity = n
	return nil
}

// -------- Heads --------

// Read_head is the index of the slot the next Read will return.
func (b *MultiBuffer) Read_head() int { return b.Read_head_ }

// Write_head is the index of the slot the next Write will fill.
func (b *MultiBuffer) Write_head() int { return b.Write_head_ }

// Index_ is ``key % capacity`` (TypeError for a non-integer key).
func (b *MultiBuffer) Index_(key any) (int, error) {
	if _, isBool := key.(bool); isBool || !is_int_(key) {
		return 0, py.TypeError.Newf("MultiBuffer indices must be integers, not %s", py.Pytypename(key))
	}
	n, err := py.Int(key)
	if err != nil {
		return 0, err
	}
	// Python's ``%`` is floor-modulo: negative indices wrap around.
	m := n % b.Capacity
	if m < 0 {
		m += b.Capacity
	}
	return m, nil
}

// is_int_ is ``isinstance(v, int) and not isinstance(v, bool)`` over the
// Go integer kinds.
func is_int_(v any) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	}
	return false
}

// Len is ``len(buf)`` -> capacity.
func (b *MultiBuffer) Len() int { return b.Capacity }

// slot_get_ / slot_set_ are ``self.slots[i]`` / ``self.slots[i] = v`` on
// the (list-like) backing storage.
func (b *MultiBuffer) slot_get_(i int) (any, error) {
	switch s := b.Slots.(type) {
	case *py.List:
		return s.Get(i)
	case []any:
		return s[i], nil
	}
	return callm(b.Slots, "__getitem__", i)
}

func (b *MultiBuffer) slot_set_(i int, v any) error {
	switch s := b.Slots.(type) {
	case *py.List:
		return s.Set(i, v)
	case []any:
		s[i] = v
		return nil
	}
	_, err := callm(b.Slots, "__setitem__", i, v)
	return err
}

// -------- Item protocol --------

// Getitem returns the Entry at slot *key* (modulo capacity), or nil.
//
// A stored Record yields its ``entry``. Raw contents (anything that is
// neither a record nor an entry -- e.g. bytes written by mapped hardware)
// are wrapped into a constant entry.
func (b *MultiBuffer) Getitem(key any) (any, error) {
	idx, err := b.Index_(key)
	if err != nil {
		return nil, err
	}
	b.Lock()
	value, err := b.slot_get_(idx)
	b.Unlock()
	if err != nil {
		return nil, err
	}
	if py.IsNone(value) {
		return nil, nil
	}
	if r, ok := value.(*Record); ok {
		return r.Entry, nil
	}
	if As_entry_(value) != nil {
		return value, nil
	}
	e, err := Entry_constant(value)
	if err != nil {
		return nil, err
	}
	return e.Self(), nil
}

// Setitem stores a Record at slot *key* (modulo capacity).
//
// Accepts a Record (stored as-is), an Entry (wrapped in a fresh record),
// a raw payload (lifted to a constant entry, then wrapped), or nil to
// clear the slot.
func (b *MultiBuffer) Setitem(key any, value any) error {
	var record any
	switch {
	case py.IsNone(value):
		record = nil
	default:
		if r, ok := value.(*Record); ok {
			record = r
		} else if As_entry_(value) != nil {
			r, err := New_Record(py.Kw{"entry": value})
			if err != nil {
				return err
			}
			record = r
		} else {
			e, err := Entry_constant(value)
			if err != nil {
				return err
			}
			r, err := New_Record(py.Kw{"entry": e.Self()})
			if err != nil {
				return err
			}
			record = r
		}
	}
	idx, err := b.Index_(key)
	if err != nil {
		return err
	}
	b.Lock()
	defer b.Unlock()
	return b.slot_set_(idx, record)
}

// Empty clears every slot and rewinds both heads to slot 0.
func (b *MultiBuffer) Empty() error {
	b.Lock()
	defer b.Unlock()
	for i := 0; i < b.Capacity; i++ {
		if err := b.slot_set_(i, nil); err != nil {
			return err
		}
	}
	b.Read_head_ = 0
	b.Write_head_ = 0
	return nil
}

// -------- Verbs --------

// Write fills the slot at the write head, then advances the head.
//
// Unmapped: ``self[write_head] = value`` (so the slot ends up holding a
// Record); *value* is required (pass nothing -> TypeError).
//
// Mapped: Setitem is bypassed. If *value* is given it is deposited raw
// into the slot; if omitted the slot is assumed to have been filled
// externally and only the head moves.
//
// Returns the index of the slot that was written.
func (b *MultiBuffer) Write(value ...any) (int, error) {
	var v any = UNSET_
	if len(value) > 0 {
		v = value[0]
	}
	b.Lock()
	defer b.Unlock()
	idx := b.Write_head_
	if b.Mapped {
		if v != UNSET_ {
			if err := b.slot_set_(idx, v); err != nil {
				return 0, err
			}
		}
	} else {
		if v == UNSET_ {
			return 0, py.TypeError.New("MultiBuffer.write() requires a value when not mapped")
		}
		if err := b.Self().(interface{ Setitem(any, any) error }).Setitem(idx, v); err != nil {
			return 0, err
		}
	}
	b.Write_head_ = (idx + 1) % b.Capacity
	return idx, nil
}

// Read returns the Entry at the read head, then advances the head.
//
// Goes through Getitem, so in mapped mode this is where raw accumulated
// bytes are wrapped into an entry. An empty slot yields nil (the head
// still advances).
func (b *MultiBuffer) Read() (any, error) {
	b.Lock()
	defer b.Unlock()
	idx := b.Read_head_
	entry, err := b.Self().(interface{ Getitem(any) (any, error) }).Getitem(idx)
	if err != nil {
		return nil, err
	}
	b.Read_head_ = (idx + 1) % b.Capacity
	return entry, nil
}

// DefaultMultiBuffer is the class descriptor behind ``DefaultMultiBuffer``.
var DefaultMultiBuffer = MultiBuffer_class

func init() {
	py.Register_property("read_head", "write_head")
}
