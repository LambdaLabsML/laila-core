package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// entry/entry_metadata.py -- lightweight read-only views over Entry
// identity and state.

// EntryIdentityView is a read-only projection exposing only the identity
// and state of an Entry.
type EntryIdentityView struct {
	LAILA_IDENTIFIABLE_OBJECT_
	State_ EntryState `py:"_state"`
}

// EntryIdentityView_class is the class descriptor (default scope ENTRY).
var EntryIdentityView_class = py.NewClass("EntryIdentityView", (*EntryIdentityView)(nil), LAILA_IDENTIFIABLE_OBJECT_class).
	Set_classvar("_DEFAULT_SCOPES", []string{"ENTRY"})

// New_EntryIdentityView is ``EntryIdentityView(**kw)``.
func New_EntryIdentityView(kw ...py.Kw) (*EntryIdentityView, error) {
	v := EntryIdentityView_class.New().(*EntryIdentityView)
	if err := v.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return v, nil
}

// EntryHolisticView is a read-only projection exposing identity, state,
// and constitution of an Entry.
type EntryHolisticView struct {
	LAILA_IDENTIFIABLE_OBJECT_
	State_        EntryState   `py:"_state"`
	Constitution_ Constitution `py:"_constitution"`
}

// EntryHolisticView_class is the class descriptor (default scope ENTRY).
var EntryHolisticView_class = py.NewClass("EntryHolisticView", (*EntryHolisticView)(nil), LAILA_IDENTIFIABLE_OBJECT_class).
	Set_classvar("_DEFAULT_SCOPES", []string{"ENTRY"})

// New_EntryHolisticView is ``EntryHolisticView(**kw)``.
func New_EntryHolisticView(kw ...py.Kw) (*EntryHolisticView, error) {
	v := EntryHolisticView_class.New().(*EntryHolisticView)
	if err := v.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return v, nil
}
