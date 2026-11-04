package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// entry/entry_state.py -- enumeration of lifecycle states for an Entry.

// EntryState is the lifecycle state of an Entry.
//
//   - READY:   data is available and fully materialised.
//   - POOLED:  entry is stored in a pool.
//   - POOLING: entry is being transferred to a pool.
//   - STAGED:  entry is staged but data may not yet be materialised.
//   - STALE:   entry data is out of date.
//   - NA:      not applicable. Used by Entry subclasses whose lifecycle
//     states are meaningless (e.g. Manifest). Only Entry may hold
//     non-NA states.
type EntryState int

// EntryState members (``auto()`` values 1..6, in declaration order).
const (
	EntryState_READY EntryState = iota + 1
	EntryState_POOLED
	EntryState_POOLING
	EntryState_STAGED
	EntryState_STALE
	EntryState_NA
)

var entry_state_names_ = [...]string{"", "READY", "POOLED", "POOLING", "STAGED", "STALE", "NA"}

// EntryState_members_ is ``list(EntryState)``.
var EntryState_members_ = []EntryState{
	EntryState_READY, EntryState_POOLED, EntryState_POOLING,
	EntryState_STAGED, EntryState_STALE, EntryState_NA,
}

// Name is ``member.name``.
func (s EntryState) Name() string {
	if s >= 1 && int(s) < len(entry_state_names_) {
		return entry_state_names_[s]
	}
	return ""
}

// Value is ``member.value``.
func (s EntryState) Value() int { return int(s) }

// String is ``str(member)`` -> ``EntryState.READY``.
func (s EntryState) String() string { return "EntryState." + s.Name() }

// Repr is ``repr(member)`` -> ``<EntryState.READY: 1>``.
func (s EntryState) Repr() string {
	return "<EntryState." + s.Name() + ": " + py.Str(int(s)) + ">"
}

// Valid reports whether s is a declared member.
func (s EntryState) Valid() bool { return s >= EntryState_READY && s <= EntryState_NA }

// EntryState_by_name is ``EntryState[name]``; KeyError for unknown names.
func EntryState_by_name(name string) (EntryState, error) {
	for _, m := range EntryState_members_ {
		if m.Name() == name {
			return m, nil
		}
	}
	return 0, py.KeyError.New(name)
}

// ``EntryState.__reduce_ex__`` -> ``(EntryState, (value,))``.
func init() {
	py.Register_enum(EntryState_READY, "laila.entry.entry_state.EntryState",
		func(m any) any { return m.(EntryState).Value() },
		func(v any) (any, error) {
			n, err := py.Int(v)
			if err != nil {
				return nil, err
			}
			if s := EntryState(n); s.Valid() {
				return s, nil
			}
			return nil, py.ValueError.Newf("%s is not a valid EntryState", py.Repr(v))
		})
}
