package laila

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/entry.py -- core Entry class: the fundamental unit of data in
// LAILA.
//
// An Entry couples three concerns into one identifiable object:
//
//  1. Identity. A UUID (random, explicit, or nickname-derived), a list of
//     hierarchical scopes, and an optional *evolution* counter. Together
//     they form a canonical ``global_id`` (see LAILA_IDENTIFIABLE_OBJECT_).
//  2. Lifecycle state. An EntryState capturing whether the payload is
//     materialised (READY), staged for build (STAGED), in flight to a pool
//     (POOLING), pooled (POOLED), stale, or "not applicable" (NA).
//  3. Either a concrete payload (wrapped in a ComputationalData) or a
//     Constitution describing how to build the payload.
//
// Constants (``Entry_constant``) are immutable: ``evolution`` is nil and
// Evolve raises. Variables (``Entry_variable``) are evolvable: Evolve
// returns the next version with ``evolution + 1``.
//
// When created from a constitution an entry starts STAGED and must be
// materialised before its data can be read: ``laila.Build(entry).Wait()``
// or ``laila.Remember(entry_id)``.

// Entry_hooks are the overridable construction / build phases of Entry
// (Manifest overrides several of them).
type Entry_hooks interface {
	Initialize_identity_(data py.Kw) error
	Initialize_payload_(data py.Kw) error
	Initialize_constitution_(data py.Kw) error
	Initialize_state_(data py.Kw) error
	Post_build_(result any) error
}

// Entry is the fundamental unit of data in LAILA: identity + state +
// (payload XOR constitution). All mutators run under the entry's
// per-instance ``atomic`` lock.
type Entry struct {
	LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_
	State_        EntryState         `py:"_state"`
	Constitution_ Constitution       `py:"_constitution"`
	Payload_      Computational_data `py:"_payload"`
	// Locally_modified_ is the process-local "payload changed since the
	// last memorize" marker. Set by Set_data, cleared by Mark_memorized
	// and reset to false at the end of construction / deserialisation /
	// build so that only *user* mutations count. Never serialised.
	Locally_modified_ bool `py:"_locally_modified"`
	// Source_pool_gid_ is a process-local hint: the global_id of the pool
	// this entry was last read from or written to. ``Manifest.realized``
	// and constitution builds look there first. Never serialised.
	Source_pool_gid_ *string `py:"_source_pool_gid"`
}

// Entry_class is the class descriptor (``_ALLOWS_NON_NA_STATE = True``,
// default scope ENTRY).
var Entry_class = py.NewClass("Entry", (*Entry)(nil), LAILA_LOCALLY_ATOMIC_IDENTIFIABLE_OBJECT_class).
	Set_classvar("_ALLOWS_NON_NA_STATE", true).
	Set_classvar("_DEFAULT_SCOPES", []string{ENTRY_SCOPE_}).
	Set_private_default("_state", func() any { return EntryState_STAGED })

// New_Entry is ``Entry(**data)``: the raw constructor. Most callers should
// use Entry_constant / Entry_variable / Entry_contingent. Accepted keys:
// ``data`` / ``payload``, ``uuid``, ``evolution``, ``global_id``,
// ``nickname``, ``scopes``, ``constitution``, ``manifest``, ``state``.
func New_Entry(kw ...py.Kw) (*Entry, error) {
	e := Entry_class.New().(*Entry)
	if err := e.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return e, nil
}

// New_entry_of_ instantiates cls (Entry or a subclass) with kw.
func New_entry_of_(cls *py.Class, kw py.Kw) (*Entry, error) {
	inst := cls.New()
	if err := inst.(interface{ Init_(py.Kw) error }).Init_(kw); err != nil {
		return nil, err
	}
	return As_entry_(inst), nil
}

// As_entry_ returns the embedded *Entry of any Entry-derived instance
// (nil when v is not one).
func As_entry_(v any) *Entry {
	if e, ok := v.(*Entry); ok {
		return e
	}
	if b, ok := v.(interface{ Entry_base_() *Entry }); ok {
		return b.Entry_base_()
	}
	return nil
}

// Entry_base_ returns the embedded Entry (for subclasses).
func (e *Entry) Entry_base_() *Entry { return e }

// Init_ is ``Entry.__init__(**data)``: identity, payload, constitution,
// state; the initial payload is the baseline, not a change.
func (e *Entry) Init_(data py.Kw) error {
	h := e.Self().(Entry_hooks)
	if err := h.Initialize_identity_(data); err != nil {
		return err
	}
	if err := h.Initialize_payload_(data); err != nil {
		return err
	}
	if err := h.Initialize_constitution_(data); err != nil {
		return err
	}
	if err := h.Initialize_state_(data); err != nil {
		return err
	}
	e.Locally_modified_ = false
	return nil
}

// Initialize_identity_ parses identity fields from data and initialises
// the parent identity. ``global_id`` is preferred over the (uuid,
// evolution, scopes) triple when present.
func (e *Entry) Initialize_identity_(data py.Kw) error {
	identity_data := py.Kw{}
	global_id := data.Get("global_id", nil)
	uuid := data.Get("uuid", nil)
	evolution := data.Get("evolution", nil)
	nickname := data.Get("nickname", nil)
	scopes := data.Get("scopes", nil)

	if !py.IsNone(global_id) {
		identity_data, err := Process_global_id(global_id)
		if err != nil {
			return err
		}
		return e.LAILA_IDENTIFIABLE_OBJECT_.Init_(identity_data)
	}
	if !py.IsNone(uuid) {
		identity_data["uuid"] = uuid
	}
	if !py.IsNone(evolution) {
		identity_data["evolution"] = evolution
	}
	if !py.IsNone(nickname) {
		identity_data["nickname"] = nickname
	}
	if !py.IsNone(scopes) {
		identity_data["scopes"] = scopes
	}
	return e.LAILA_IDENTIFIABLE_OBJECT_.Init_(identity_data)
}

// Initialize_payload_ sets the payload from ``payload`` or ``data``.
func (e *Entry) Initialize_payload_(data py.Kw) error {
	return e.Set_data(data.Get("payload", data.Get("data", nil)))
}

// Initialize_constitution_ attaches a Constitution if one was provided: a
// live Constitution (a ``manifest`` kwarg is attached opportunistically to
// a ComplexConstitution without one), a ``[]string`` / list of strings
// (-> SimpleConstitution) or a single string (-> ComplexConstitution with
// the optional ``manifest``). TypeError otherwise.
func (e *Entry) Initialize_constitution_(data py.Kw) error {
	constitution := data.Get("constitution", nil)
	manifest := data.Get("manifest", nil)
	if py.IsNone(constitution) {
		return nil
	}
	if c, ok := constitution.(Constitution); ok {
		if cc, isComplex := c.(*ComplexConstitution); isComplex && !py.IsNone(manifest) {
			if cc.Manifest() == nil {
				if err := cc.Set_manifest(manifest); err != nil {
					return err
				}
			}
		}
		return e.Set_constitution(c)
	}
	switch x := constitution.(type) {
	case []string, *py.List, []any:
		c, err := New_SimpleConstitution(py.Kw{"codes": x})
		if err != nil {
			return err
		}
		return e.Set_constitution(c)
	case string:
		kwargs := py.Kw{"code": x}
		if !py.IsNone(manifest) {
			kwargs["manifest"] = manifest
		}
		c, err := New_ComplexConstitution(kwargs)
		if err != nil {
			return err
		}
		return e.Set_constitution(c)
	}
	return py.TypeError.New("constitution must be a Constitution, list[str], or str")
}

// Allows_non_na_state_ is ``type(self)._ALLOWS_NON_NA_STATE``.
func (e *Entry) Allows_non_na_state_() bool {
	if v, ok := e.Class_().Classvar("_ALLOWS_NON_NA_STATE"); ok {
		return py.Truthy(v)
	}
	return true
}

// Initialize_state_ picks the initial EntryState: NA for subclasses that
// disallow other states, STAGED when a constitution is attached (the
// explicit ``state`` is ignored), else the explicit ``state`` or STAGED.
func (e *Entry) Initialize_state_(data py.Kw) error {
	if !e.Allows_non_na_state_() {
		return e.Set_state(EntryState_NA)
	}
	if !py.IsNone(data.Get("constitution", nil)) {
		return e.Set_state(EntryState_STAGED)
	}
	return e.Set_state(data.Get("state", EntryState_STAGED))
}

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

// Data returns the unwrapped payload value (nil if unset). Read-only: it
// never triggers an implicit build. EntryNotBuiltError when a
// constitution is attached but the entry has not been built.
func (e *Entry) Data() (any, error) {
	e.Lock()
	defer e.Unlock()
	if e.Payload_ != nil {
		return e.Payload_.Base_().Data, nil
	}
	if e.Constitution_ != nil {
		return nil, EntryNotBuiltError.Newf(
			"Entry %s is not built. Use `laila.build(entry).wait()` (or "+
				"`await laila.build(entry, asynchronous=True)`) to materialize it before accessing .data.",
			e.Global_id())
	}
	return nil, nil
}

// Set_data replaces the payload. Raw values are auto-wrapped in the
// appropriate ComputationalData subclass; already-wrapped values pass
// through; nil clears the payload. Marks the entry locally modified.
func (e *Entry) Set_data(new_data any) error {
	e.Lock()
	defer e.Unlock()
	var wrapped Computational_data
	if !py.IsNone(new_data) {
		if cd, ok := new_data.(Computational_data); ok {
			wrapped = cd
		} else {
			var err error
			wrapped, err = New_ComputationalData(new_data)
			if err != nil {
				return err
			}
		}
	}
	e.Payload_ = wrapped
	e.Locally_modified_ = true
	return nil
}

// Locally_modified is true when the payload was re-assigned since the
// last memorize (process-local bookkeeping only).
func (e *Entry) Locally_modified() bool { return e.Locally_modified_ }

// Mark_memorized clears the locally-modified flag (called by central
// memory after a successful write).
func (e *Entry) Mark_memorized() { e.Locally_modified_ = false }

// Bump_evolution_if_locally_modified advances ``evolution`` in place (and
// re-stamps creation_timestamp) if the payload changed since the last
// memorize. Constants are never bumped. Returns true if incremented.
func (e *Entry) Bump_evolution_if_locally_modified() bool {
	e.Lock()
	defer e.Unlock()
	if e.Evolution_ == nil || !e.Locally_modified_ {
		return false
	}
	next := *e.Evolution_ + 1
	e.Evolution_ = &next
	ts := Now_creation_timestamp_()
	e.Creation_timestamp_ = &ts
	return true
}

// Constitution is the attached Constitution, or nil once built.
func (e *Entry) Constitution() Constitution {
	e.Lock()
	defer e.Unlock()
	return e.Constitution_
}

// Set_constitution attaches or clears (nil) the Constitution. TypeError
// for anything else.
func (e *Entry) Set_constitution(new_constitution any) error {
	e.Lock()
	defer e.Unlock()
	if py.IsNone(new_constitution) {
		e.Constitution_ = nil
		return nil
	}
	c, ok := new_constitution.(Constitution)
	if !ok {
		return py.TypeError.New("constitution must be a Constitution or None")
	}
	e.Constitution_ = c
	return nil
}

// State is the current EntryState.
func (e *Entry) State() EntryState {
	e.Lock()
	defer e.Unlock()
	return e.State_
}

// Set_state replaces the lifecycle state. ValueError if new_state is not
// an EntryState, or if this subclass disallows non-NA states.
func (e *Entry) Set_state(new_state any) error {
	e.Lock()
	defer e.Unlock()
	s, ok := new_state.(EntryState)
	if !ok || !s.Valid() {
		return py.ValueError.New("state must be an EntryState")
	}
	if !e.Allows_non_na_state_() && s != EntryState_NA {
		return py.ValueError.Newf("%s only accepts EntryState.NA", e.Class_().Name)
	}
	e.State_ = s
	return nil
}

// Metadata is not implemented for Entry (NotImplementedError).
func (e *Entry) Metadata() (any, error) {
	return nil, py.NotImplementedError.New("Metadata is not implemented for Entry")
}

// Source_pool_gid is the process-local source pool hint.
//
// Read under the entry lock: concurrent reads of one entry (the in-memory
// pool hands back the same object) stamp the hint from several
// goroutines at once.
func (e *Entry) Source_pool_gid() *string {
	e.Lock()
	defer e.Unlock()
	return e.Source_pool_gid_
}

// Set_source_pool_gid records the pool this entry was last read from /
// written to.
func (e *Entry) Set_source_pool_gid(gid *string) {
	e.Lock()
	e.Source_pool_gid_ = gid
	e.Unlock()
}

// ---------------------------------------------------------------------------
// Constitution Operations
// ---------------------------------------------------------------------------

// Build_sync_ synchronously materialises this entry by running its
// constitution on the calling goroutine. Idempotent on READY entries.
// RuntimeError if not built and no constitution is attached.
func (e *Entry) Build_sync_() (*Entry, error) {
	if e.State_ == EntryState_READY {
		return e, nil
	}
	if e.Constitution_ == nil {
		return nil, py.RuntimeError.New("Entry has no constitution attached.")
	}
	if err := e.Build_inplace_(); err != nil {
		return nil, err
	}
	return e, nil
}

// Current_slot_taskforce_ returns the taskforce owning the current parking
// slot (``_CURRENT_SLOT.get().tf``), or nil outside a taskforce. Installed
// by the command / parking module.
var Current_slot_taskforce_ = func() any { return nil }

// Build_async_ is the coroutine variant of Build_sync_. Keyword
// ``_process_taskforce_id`` routes the body of a complex constitution to
// that process pool (see ProcessConstitutionJob_). Simple constitutions
// run inline; complex ones resolve the manifest, await
// ``target.async_realized`` and run the body through the owning
// taskforce's ``run_sync`` (or a worker goroutine outside a taskforce).
func (e *Entry) Build_async_(kw ...py.Kw) (*Entry, error) {
	opts := py.MergeKw(kw...)
	if e.State_ == EntryState_READY {
		return e, nil
	}
	if e.Constitution_ == nil {
		return nil, py.RuntimeError.New("Entry has no constitution attached.")
	}
	if c, ok := e.Constitution_.(*ComplexConstitution); ok {
		if c.Code_ == nil {
			return nil, py.RuntimeError.New("no constitution code defined.")
		}
		target, err := c.Resolve_manifest_async_(e.Source_pool_gid())
		if err != nil {
			return nil, err
		}
		if py.IsNone(target) {
			return nil, py.RuntimeError.New("no manifest available to run constitution.")
		}
		realized, err := py.Getattr(target, "async_realized")
		if err != nil {
			return nil, err
		}
		if aw, ok := realized.(Awaitable); ok {
			if realized, err = aw.Await(); err != nil {
				return nil, err
			}
		}
		var result any
		if tfid := opts.Get("_process_taskforce_id", nil); !py.IsNone(tfid) {
			result, err = Run_body_in_process_(*c.Code_, target, realized, py.Str(tfid))
		} else {
			body, berr := Exec_one_fn_(*c.Code_)
			if berr != nil {
				return nil, berr
			}
			tf := Current_slot_taskforce_()
			if tf != nil && !py.IsNone(getattr(tf, "run_sync")) {
				var fut any
				fut, err = callm(tf, "run_sync", body, target)
				if err == nil {
					result, err = await_future_(fut)
				}
			} else {
				result, err = py.To_thread(func() (any, error) { return body(target) })
			}
		}
		if err != nil {
			return nil, err
		}
		if err := e.Self().(Entry_hooks).Post_build_(result); err != nil {
			return nil, err
		}
		if err := e.Set_constitution(nil); err != nil {
			return nil, err
		}
		if err := e.Set_state(EntryState_READY); err != nil {
			return nil, err
		}
		e.Locally_modified_ = false
		return e, nil
	}
	if err := e.Build_inplace_(); err != nil {
		return nil, err
	}
	return e, nil
}

// Manifest_iter_entries_ is ``Manifest._iter_entries(realized)``,
// installed by the manifest module.
var Manifest_iter_entries_ func(realized any) ([]*Entry, error)

// Run_body_in_process_ ships a realized manifest + constitution body to a
// process pool and awaits the result (``Entry._run_body_in_process``).
func Run_body_in_process_(code string, manifest any, realized any, taskforce_id string) (any, error) {
	if Manifest_iter_entries_ == nil {
		return nil, py.RuntimeError.New("laila.build on a process pool requires the manifest module")
	}
	ents, err := Manifest_iter_entries_(realized)
	if err != nil {
		return nil, err
	}
	entries := py.NewDict()
	for _, ent := range ents {
		d, err := ent.As_dict()
		if err != nil {
			return nil, err
		}
		entries.Set(ent.Global_id(), d)
	}
	mdata, err := py.Getattr(manifest, "data")
	if err != nil {
		return nil, err
	}
	job := &ProcessConstitutionJob_{
		Code:          code,
		Blueprint:     py.Deepcopy(mdata),
		Entries:       entries,
		Manifest_uuid: py.Str(getattr(manifest, "uuid")),
	}
	policy, err := Get_active_policy()
	if err != nil {
		return nil, err
	}
	command := getattr(getattr(policy, "central"), "command")
	fut, err := callm(command, "submit", py.NewList(job.Picklable_()), py.Kw{"taskforce_id": taskforce_id})
	if err != nil {
		if errors.Is(err, py.TypeError) {
			return nil, py.TypeError.New(
				"laila.build on a process pool requires the realized manifest payload " +
					"to be picklable (it is shipped to the worker process).").From(err)
		}
		return nil, err
	}
	defer func() { _, _ = callm(fut, "release") }()
	return await_future_(fut)
}

// Build_ dispatches to Build_async_ or Build_sync_ on ``asynchronous``.
func (e *Entry) Build_(kw ...py.Kw) (*Entry, error) {
	if py.Truthy(py.MergeKw(kw...).Get("asynchronous", false)) {
		return e.Build_async_()
	}
	return e.Build_sync_()
}

// Build_inplace_ runs the attached constitution synchronously, in place:
// the current payload (if any) is the ``payload_input``; the result goes
// through Post_build_; the constitution is detached and the state flips
// to READY. RuntimeError if no constitution is attached.
func (e *Entry) Build_inplace_() error {
	if e.Constitution_ == nil {
		return py.RuntimeError.New("Entry has no constitution attached.")
	}
	var payload_input any
	if e.Payload_ != nil {
		payload_input = e.Payload_.Base_().Data
	}
	result, err := e.Constitution_.Build(payload_input)
	if err != nil {
		return err
	}
	if err := e.Self().(Entry_hooks).Post_build_(result); err != nil {
		return err
	}
	if err := e.Set_constitution(nil); err != nil {
		return err
	}
	if err := e.Set_state(EntryState_READY); err != nil {
		return err
	}
	// Materialising stored bytes is not a user change.
	e.Locally_modified_ = false
	return nil
}

// Post_build_ places the build result into the payload slot (subclasses
// such as Manifest override this).
func (e *Entry) Post_build_(result any) error { return e.Set_data(result) }

// ---------------------------------------------------------------------------
// Variable
// ---------------------------------------------------------------------------

// Entry_variable is ``Entry.variable(data=None, *, uuid, evolution, state,
// constitution, manifest, global_id, nickname)``: a mutable (evolvable)
// entry. Pass either ``data`` (READY) or ``constitution`` + ``manifest``
// (STAGED), and at most one identity shape (``global_id`` / ``uuid`` [+
// ``evolution``] / ``nickname``). RuntimeError on conflicting identity
// arguments or constitution + data; ValueError when constitution and
// manifest are not given together.
func Entry_variable(data any, kw ...py.Kw) (*Entry, error) {
	return Entry_variable_of_(Entry_class, data, py.MergeKw(kw...))
}

// Entry_variable_of_ is Entry_variable with an explicit ``cls`` (for
// nickname derivation); the result is always a plain Entry.
func Entry_variable_of_(cls *py.Class, data any, opts py.Kw) (*Entry, error) {
	uuid := opts.Get("uuid", nil)
	evolution := opts.Get("evolution", nil)
	state := opts.Get("state", nil)
	constitution := opts.Get("constitution", nil)
	manifest := opts.Get("manifest", nil)
	global_id := opts.Get("global_id", nil)
	nickname := opts.Get("nickname", nil)

	if !py.IsNone(global_id) && (!py.IsNone(uuid) || !py.IsNone(evolution)) {
		return nil, py.RuntimeError.New("Cannot set both global_id and <uuid, evolution> at the same time.")
	}
	if !py.IsNone(constitution) && !py.IsNone(data) {
		return nil, py.RuntimeError.New("Cannot set both constitution and data.")
	}
	if py.IsNone(constitution) != py.IsNone(manifest) {
		return nil, py.ValueError.New("`constitution` and `manifest` must both be provided together.")
	}

	var scopes any
	if !py.IsNone(global_id) {
		identity_data, err := Process_global_id(global_id)
		if err != nil {
			return nil, err
		}
		uuid = identity_data["uuid"]
		evolution = identity_data["evolution"]
		if s := identity_data["scopes"].([]string); len(s) > 0 {
			scopes = s
		}
	}
	if py.IsNone(evolution) {
		evolution = 0
	}
	uuid = Merge_nickname_identity_(cls, uuid, nickname)

	identity_kwargs := py.Kw{"uuid": uuid, "evolution": evolution}
	if scopes != nil {
		identity_kwargs["scopes"] = scopes
	}
	if !py.IsNone(constitution) {
		identity_kwargs["constitution"] = constitution
		identity_kwargs["manifest"] = manifest
		return New_Entry(identity_kwargs)
	}
	if py.IsNone(state) {
		state = EntryState_READY
	}
	identity_kwargs["data"] = data
	identity_kwargs["state"] = state
	return New_Entry(identity_kwargs)
}

// Merge_nickname_identity_ combines an explicit ``uuid`` with a
// ``nickname``: the nickname (a deterministic uuid generator) takes
// precedence when both are given.
func Merge_nickname_identity_(cls *py.Class, uuid any, nickname any) any {
	if py.IsNone(nickname) {
		return uuid
	}
	return Generate_uuid_from_nickname(py.Str(nickname))
}

// Evolve returns a new Entry representing the next evolution: same uuid
// and scopes, ``evolution + 1``, the new payload, READY, and its own
// creation_timestamp. ``self`` is left untouched. RuntimeError on a
// constant; NotImplementedError while an unbuilt constitution is attached.
func (e *Entry) Evolve(data any) (*Entry, error) {
	if e.Evolution_ == nil {
		return nil, py.RuntimeError.New("Can't evolve a constant.")
	}
	if e.Constitution() != nil {
		return nil, py.NotImplementedError.New(
			"Entry has not been built yet, internal logic cannot change payload while constitution is not None.")
	}
	e.Lock()
	next_evolution := *e.Evolution_ + 1
	scopes_snapshot := append([]string{}, e.Scopes_...)
	uuid_snapshot := e.Uuid_
	e.Unlock()
	return Entry_contingent(py.Kw{
		"uuid":      uuid_snapshot,
		"scopes":    scopes_snapshot,
		"evolution": next_evolution,
		"data":      data,
		"state":     EntryState_READY,
	})
}

// ---------------------------------------------------------------------------
// Constant
// ---------------------------------------------------------------------------

// Entry_constant is ``Entry.constant(data, *, global_id, uuid, nickname)``:
// an immutable entry whose ``evolution`` is nil. RuntimeError if both
// ``global_id`` and ``uuid`` are supplied, or if ``global_id`` carries an
// evolution.
func Entry_constant(data any, kw ...py.Kw) (*Entry, error) {
	return Entry_constant_of_(Entry_class, data, py.MergeKw(kw...))
}

// Entry_constant_of_ is Entry_constant with an explicit ``cls``.
func Entry_constant_of_(cls *py.Class, data any, opts py.Kw) (*Entry, error) {
	global_id := opts.Get("global_id", nil)
	uuid := opts.Get("uuid", nil)
	nickname := opts.Get("nickname", nil)
	if !py.IsNone(global_id) && !py.IsNone(uuid) {
		return nil, py.RuntimeError.New("Cannot set both global_id and uuid at the same time.")
	}
	var scopes any
	if py.IsNone(uuid) && !py.IsNone(global_id) {
		identity_data, err := Process_global_id(global_id)
		if err != nil {
			return nil, err
		}
		uuid = identity_data["uuid"]
		if s := identity_data["scopes"].([]string); len(s) > 0 {
			scopes = s
		}
		if identity_data["evolution"].(*int) != nil {
			return nil, py.RuntimeError.New("Cannot have a constant with an evolution.")
		}
	}
	uuid = Merge_nickname_identity_(cls, uuid, nickname)
	identity_kwargs := py.Kw{"uuid": uuid, "evolution": nil, "data": data, "state": EntryState_READY}
	if scopes != nil {
		identity_kwargs["scopes"] = scopes
	}
	return New_Entry(identity_kwargs)
}

// ---------------------------------------------------------------------------
// Contingent
// ---------------------------------------------------------------------------

// Entry_contingent is ``Entry.contingent(**kwargs)``: raw construction
// bypassing the constant / variable consistency checks.
func Entry_contingent(kw ...py.Kw) (*Entry, error) { return New_Entry(kw...) }

// ---------------------------------------------------------------------------
// Serialize and Recovery
// ---------------------------------------------------------------------------

// As_dict is the unified serialised shape without any transformation:
// ``_uuid``, ``_evolution``, ``_scopes``, ``_state`` (member name),
// ``_creation_timestamp``, ``payload`` (raw value) and ``constitution``.
func (e *Entry) As_dict() (*py.Dict, error) {
	var constitution_dict any
	if e.Constitution_ != nil {
		d, err := e.Constitution_.As_dict()
		if err != nil {
			return nil, err
		}
		constitution_dict = d
	}
	var payload_value any
	if e.Payload_ != nil {
		payload_value = e.Payload_.Base_().Data
	}
	return py.DictOf(
		"_uuid", e.Uuid_,
		"_evolution", intPtrValue(e.Evolution_),
		"_scopes", py.NewList(stringsToAny(e.Scopes_)...),
		"_state", e.State_.Name(),
		"_creation_timestamp", strPtrValue(e.Creation_timestamp_),
		"payload", payload_value,
		"constitution", constitution_dict,
	), nil
}

// Serialize produces the pool-storage form. With transformations the
// payload is serialised to bytes (emitting its inverse code), run through
// the sequence (each step emitting its inverse) and the codes bundled
// into a SimpleConstitution under ``constitution``; the result is a
// *py.Dict. With nil transformations an independent Snapshot_ (*Entry) is
// returned for in-memory pools. RuntimeError unless the entry is READY.
func (e *Entry) Serialize(transformations *TransformationSequence) (any, error) {
	if e.State_ != EntryState_READY {
		return nil, py.RuntimeError.Newf(
			"Cannot serialize entry in state %s; only READY entries can be serialized.", py.Repr(e.State_.Name()))
	}
	if transformations == nil {
		// Virtual: subclasses (ComplexManifest) trim the stored copy.
		return e.Self().(interface{ Snapshot_() (any, error) }).Snapshot_()
	}
	var transformed_payload any
	codes := []string{}
	if e.Payload_ != nil {
		serialized_payload, payload_backward_code, err := e.Payload_.Serialize()
		if err != nil {
			return nil, err
		}
		tp, transformation_inverse_code, err := transformations.Forward(serialized_payload)
		if err != nil {
			return nil, err
		}
		transformed_payload = tp
		codes = append(append(codes, transformation_inverse_code...), payload_backward_code)
	}
	constitution, err := New_SimpleConstitution(py.Kw{"codes": codes})
	if err != nil {
		return nil, err
	}
	cd, err := constitution.As_dict()
	if err != nil {
		return nil, err
	}
	return py.DictOf(
		"_uuid", e.Uuid_,
		"_evolution", intPtrValue(e.Evolution_),
		"_scopes", py.NewList(stringsToAny(e.Scopes_)...),
		"_state", e.State_.Name(),
		"_creation_timestamp", strPtrValue(e.Creation_timestamp_),
		"payload", transformed_payload,
		"constitution", cd,
	), nil
}

// Snapshot_ is an independent copy with a deep-copied payload (own lock,
// not locally modified). Payloads that cannot be deep-copied are kept by
// reference, best effort, with a debug log.
func (e *Entry) Snapshot_() (any, error) {
	// ``self.model_copy()`` keeps the concrete subclass (Manifest, Future
	// ...): return the copy itself, not just its embedded Entry.
	copied := py.Model_copy(e.Self())
	clone := As_entry_(copied)
	clone.Local_lock_.Store(nil)
	if e.Payload_ != nil {
		copied, err := deepcopy_payload_(e.Payload_)
		if err != nil {
			if lg, lerr := Get_logger(); lerr == nil && lg != nil {
				lg.Debug(fmt.Sprintf("Entry %s: payload not deep-copyable (%s); stored by reference",
					e.Global_id(), py.Repr(err)))
			}
		} else {
			clone.Payload_ = copied
		}
	}
	clone.Locally_modified_ = false
	return copied, nil
}

// deepcopy_payload_ is ``copy.deepcopy(payload)`` with the raised error
// captured instead of propagating.
func deepcopy_payload_(p Computational_data) (out Computational_data, err error) {
	defer func() {
		if r := recover(); r != nil {
			if rerr, ok := r.(error); ok {
				err = rerr
			} else {
				err = py.RuntimeError.New(py.Str(r))
			}
		}
	}()
	cp, ok := py.Deepcopy(p).(Computational_data)
	if !ok {
		return nil, py.TypeError.New("payload deepcopy did not return a ComputationalData")
	}
	return cp, nil
}

// Entry_from_dict is ``Entry.from_dict(in_dict)``: hydrate an Entry from a
// serialised dict *without* running its constitution.
func Entry_from_dict(in_dict *py.Dict) (*Entry, error) {
	return Entry_from_dict_of_(Entry_class, in_dict)
}

// Entry_from_dict_of_ is Entry_from_dict for cls (Entry or a subclass):
// identity is restored and validated (ValueError on a malformed ``_uuid``
// / ``_evolution``), the stored payload attached as-is, the state and
// constitution re-attached, and ``_creation_timestamp`` restored (or
// freshly stamped). Not locally modified.
func Entry_from_dict_of_(cls *py.Class, in_dict *py.Dict) (*Entry, error) {
	raw_uuid, ok := in_dict.Get("_uuid")
	if !ok {
		return nil, py.KeyError.New("_uuid")
	}
	us, isStr := raw_uuid.(string)
	if !isStr || !UUID_RE_.MatchString(us) {
		return nil, py.ValueError.Newf("Entry.from_dict: invalid _uuid %s", py.Repr(raw_uuid))
	}
	raw_evolution := in_dict.GetDefault("_evolution", nil)
	if !py.IsNone(raw_evolution) && !valid_evolution_(raw_evolution) {
		return nil, py.ValueError.Newf("Entry.from_dict: invalid _evolution %s", py.Repr(raw_evolution))
	}
	inst := cls.New()
	entry := As_entry_(inst)
	if entry == nil {
		return nil, py.TypeError.Newf("%s is not an Entry class", cls.Name)
	}
	if err := entry.Entry_initialize_identity_(py.Kw{
		"uuid":      raw_uuid,
		"evolution": raw_evolution,
		"scopes":    in_dict.GetDefault("_scopes", nil),
	}); err != nil {
		return nil, err
	}
	// cls.New() bypasses LAILA_OBJECT_.Init_, so the creation_timestamp
	// must be restored (or freshly stamped) explicitly.
	ts := Now_creation_timestamp_()
	if v := in_dict.GetDefault("_creation_timestamp", nil); py.Truthy(v) {
		ts = py.Str(v)
	}
	entry.Creation_timestamp_ = &ts
	if err := entry.Set_data(in_dict.GetDefault("payload", nil)); err != nil {
		return nil, err
	}
	state, err := EntryState_by_name(py.Str(in_dict.GetDefault("_state", "STAGED")))
	if err != nil {
		return nil, err
	}
	if err := entry.Set_state(state); err != nil {
		return nil, err
	}
	constitution, err := Constitution_from_dict(in_dict.GetDefault("constitution", nil))
	if err != nil {
		return nil, err
	}
	if constitution != nil {
		if err := entry.Set_constitution(constitution); err != nil {
			return nil, err
		}
	} else if err := entry.Set_constitution(nil); err != nil {
		return nil, err
	}
	// A pool round-trip restores the memorized baseline: not locally modified.
	entry.Locally_modified_ = false
	return entry, nil
}

// Entry_initialize_identity_ is the explicitly non-virtual
// ``Entry._initialize_identity(entry, data)`` call used by from_dict.
func (e *Entry) Entry_initialize_identity_(data py.Kw) error { return e.Initialize_identity_(data) }

// valid_evolution_ is ``not bool and isinstance int and >= 0``.
func valid_evolution_(v any) bool {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int() >= 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

// Build_from_dict_input_ normalises the input of the ``_build_from_dict``
// family: JSON strings are parsed (ValueError on failure), live entries
// are passed through (ok=false, entry returned), anything but a dict is a
// RuntimeError.
func Build_from_dict_input_(in_dict any) (d *py.Dict, live any, err error) {
	local := in_dict
	if s, ok := local.(string); ok {
		parsed, perr := py.Json_loads(s)
		if perr != nil {
			return nil, nil, py.ValueError.New("Invalid JSON string").From(perr)
		}
		local = parsed
	}
	if py.Isinstance(local, Entry_class) {
		return nil, local, nil
	}
	if dd, ok := asDict(local); ok {
		return dd, nil, nil
	}
	return nil, nil, py.RuntimeError.New("Invalid input for entry build.")
}

// Entry_build_from_dict_sync_ synchronously hydrates an entry from a
// serialised dict (or JSON string; a live Entry is returned as-is) and
// runs its SimpleConstitution inline; ComplexConstitution entries are
// left STAGED.
func Entry_build_from_dict_sync_(in_dict any) (any, error) {
	return Entry_build_from_dict_of_(Entry_class, in_dict)
}

// Entry_build_from_dict_async_ is the async variant (same body: nothing
// blocks here).
func Entry_build_from_dict_async_(in_dict any) (any, error) {
	return Entry_build_from_dict_of_(Entry_class, in_dict)
}

// Entry_build_from_dict_of_ is the shared body for cls.
func Entry_build_from_dict_of_(cls *py.Class, in_dict any) (any, error) {
	d, live, err := Build_from_dict_input_(in_dict)
	if err != nil {
		return nil, err
	}
	if live != nil {
		return live, nil
	}
	entry, err := Entry_from_dict_of_(cls, d)
	if err != nil {
		return nil, err
	}
	if _, ok := entry.Constitution_.(*SimpleConstitution); ok {
		if err := entry.Build_inplace_(); err != nil {
			return nil, err
		}
	}
	return entry.Self(), nil
}

// Entry_build_from_dict_ routes to the async or sync builder.
func Entry_build_from_dict_(in_dict any, kw ...py.Kw) (any, error) {
	if py.Truthy(py.MergeKw(kw...).Get("asynchronous", false)) {
		return Entry_build_from_dict_async_(in_dict)
	}
	return Entry_build_from_dict_sync_(in_dict)
}

// ---------------------------------------------------------------------------
// String Representation
// ---------------------------------------------------------------------------

// String is ``str(entry)`` -> global_id.
func (e *Entry) String() string { return e.Global_id() }

// Repr is ``repr(entry)`` -> global_id.
func (e *Entry) Repr() string { return e.Global_id() }

// ---------------------------------------------------------------------------
// Process-pool constitution job
// ---------------------------------------------------------------------------

// ProcessConstitutionJob_ is a self-contained constitution build shipped
// to a process-pool worker: the constitution source, the manifest
// blueprint and every referenced entry in As_dict form. In the worker the
// entries are rebuilt, a manifest whose ``realized`` yields them is
// constructed and the body executed against it.
type ProcessConstitutionJob_ struct {
	Code          string
	Blueprint     any
	Entries       *py.Dict
	Manifest_uuid string
}

// Rebuild_realized_manifest_ is ``_RealizedManifest(data=blueprint,
// uuid=...)`` with ``_prebuilt = Manifest._rebuild_with_entries(blueprint,
// resolved)``, installed by the manifest module.
var Rebuild_realized_manifest_ func(blueprint any, manifest_uuid string, resolved *py.Dict) (any, error)

// Process_constitution_job_task_ is the registered process task behind
// ``_ProcessConstitutionJob.__call__``: what pickling the job instance
// amounts to in the Go port (the job's plain-data fields travel as the
// task's arguments; see Picklable_).
var Process_constitution_job_task_ = py.Register_process_task(
	"laila.entry.entry._ProcessConstitutionJob",
	func(code string, blueprint any, entries any, manifest_uuid string) (any, error) {
		ents, ok := asDict(entries)
		if !ok {
			return nil, py.TypeError.Newf("entries must be a dict, got %s", py.Pytypename(entries))
		}
		job := &ProcessConstitutionJob_{
			Code: code, Blueprint: blueprint, Entries: ents, Manifest_uuid: manifest_uuid,
		}
		return job.Call()
	},
)

// Picklable_ is the job as a process-pool submittable callable: a partial
// of Process_constitution_job_task_ over the job's fields.
func (j *ProcessConstitutionJob_) Picklable_() any {
	return py.Partial(Process_constitution_job_task_, j.Code, j.Blueprint, j.Entries, j.Manifest_uuid)
}

// Call is ``job()``: rebuild the entries and manifest and run the body.
func (j *ProcessConstitutionJob_) Call() (any, error) {
	if Rebuild_realized_manifest_ == nil {
		return nil, py.RuntimeError.New("process-pool constitution jobs require the manifest module")
	}
	resolved := py.NewDict()
	for _, it := range j.Entries.Items() {
		d, ok := asDict(it.Value)
		if !ok {
			return nil, py.TypeError.Newf("entry %s is not a dict", py.Repr(it.Key))
		}
		ent, err := Entry_from_dict(d)
		if err != nil {
			return nil, err
		}
		resolved.Set(it.Key, ent)
	}
	manifest, err := Rebuild_realized_manifest_(j.Blueprint, j.Manifest_uuid, resolved)
	if err != nil {
		return nil, err
	}
	body, err := Exec_one_fn_(j.Code)
	if err != nil {
		return nil, err
	}
	return body(manifest)
}

func intPtrValue(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

func strPtrValue(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

func init() {
	py.Register_property("data", "locally_modified", "constitution", "state", "metadata")
	Register_builder(ENTRY_SCOPE_,
		func(d *py.Dict, kw py.Kw) (any, error) { return Entry_build_from_dict_sync_(d) },
		func(d *py.Dict, kw py.Kw) (any, error) { return Entry_build_from_dict_async_(d) })
	// Pool index shards are plain entries whose payload is a dict;
	// registering them keeps a raw read of a shard from tripping
	// Build_by_scope.
	Register_builder(POOL_INDEX_SCOPE_,
		func(d *py.Dict, kw py.Kw) (any, error) { return Entry_build_from_dict_sync_(d) },
		func(d *py.Dict, kw py.Kw) (any, error) { return Entry_build_from_dict_async_(d) })
}
