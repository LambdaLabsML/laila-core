package laila

import (
	"github.com/amirzadehlambda/laila/laila-go/py"
)

// policy/central/memory/record/record.py -- Record model: decorates an
// Entry with provenance metadata for storage.
//
// A Record is what actually lands in a pool. It wraps an entry with three
// pieces of provenance:
//
//   - **recorder** -- the policy gid that originated the write (defaults
//     to the active policy at construction time);
//   - **borrower** -- the policy gid that requested the entry on behalf of
//     someone else (nil unless a caller sets it; reserved for future
//     attribution flows);
//   - **record_timestamp** -- ISO-8601 UTC timestamp of when the record
//     was constructed.
//
// Pools never see raw entries; they always see (and persist) a serialized
// Record. On read, Record_build_async_ / Record_build_sync_ strip the
// envelope and re-hydrate the inner entry via the registered
// scope-specific builder.

// Record is the immutable wrapper pairing an Entry with recorder/borrower
// metadata.
//
// Construct with ``Record(entry=...)``; the recorder defaults to the
// active policy's ``global_id`` and the timestamp is captured at
// construction time. Use Serialize to turn the record into the on-disk
// dict shape (which embeds the entry's serialized form), and
// Record_build_sync_ / Record_build_async_ to invert that on read.
type Record struct {
	py.Object

	Entry            any     `py:"entry,required"`
	Recorder         *string `py:"recorder"`
	Borrower         *string `py:"borrower"`
	Record_timestamp string  `py:"record_timestamp,factory=New_record_timestamp_"`
}

// Record_class is the class descriptor (``arbitrary_types_allowed``,
// ``extra="forbid"`` so a misspelled provenance kwarg (``creator=``) fails
// loudly instead of being silently dropped).
var Record_class = py.NewClass("Record", (*Record)(nil))

func init() {
	Record_class.Extra = "forbid"
}

// New_record_timestamp_ is ``datetime.now(UTC).isoformat(timespec="milliseconds")``.
func (r *Record) New_record_timestamp_() string { return py.Now_utc_iso_ms() }

// New_Record is ``Record(**data)``.
func New_Record(kw ...py.Kw) (*Record, error) {
	r := Record_class.New().(*Record)
	if err := py.Init(r, Record_class, py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return r, nil
}

// Model_post_init defaults the recorder to the active policy's gid when
// not provided.
//
// ``borrower`` is left exactly as given (nil unless a caller attributes
// the write to another policy).
func (r *Record) Model_post_init(ctx any) error {
	if r.Recorder == nil {
		policy, err := Get_active_policy()
		if err != nil {
			return err
		}
		gid := global_id_of_(policy)
		r.Recorder = &gid
	}
	return nil
}

// Serialize serializes the record into a pool-storable dict.
//
// The wrapped entry is run through Entry.Serialize with the provided
// *transformations* (which is also the pool's ``transformations``
// attribute) and the resulting dict replaces the ``entry`` slot in the
// record. The pool then persists this whole dict atomically.
func (r *Record) Serialize(transformations *TransformationSequence) (*py.Dict, error) {
	record_as_dict := r.As_dict()
	entry := As_entry_(r.Entry)
	if entry == nil {
		return nil, py.AttributeError.Newf("'%s' object has no attribute 'serialize'", py.Pytypename(r.Entry))
	}
	serialized, err := entry.Serialize(transformations)
	if err != nil {
		return nil, err
	}
	record_as_dict.Set("entry", serialized)
	return record_as_dict, nil
}

// Entry_id returns the global ID of the wrapped entry.
//
// TypeError when the entry neither exposes a ``global_id`` nor is a
// serialized entry mapping with ``_global_id``.
func (r *Record) Entry_id() (string, error) {
	if py.Hasattr(r.Entry, "global_id") {
		v, err := py.Getattr(r.Entry, "global_id")
		if err != nil {
			return "", err
		}
		return py.Str(v), nil
	}
	if d, ok := asDict(r.Entry); ok {
		v, ok := d.Get("_global_id")
		if !ok {
			return "", py.KeyError.New("_global_id")
		}
		return py.Str(v), nil
	}
	return "", py.TypeError.Newf(
		"Record.entry must expose a `global_id` or be a serialized entry mapping with "+
			"`_global_id`; got %s", py.Pytypename(r.Entry))
}

// As_dict returns a plain dict representation, preserving the raw entry
// object.
func (r *Record) As_dict() *py.Dict {
	data := py.Model_dump(r)
	// Since it model_dumps entry in a weird way
	data.Set("entry", r.Entry)
	return data
}

// Record_from_dict constructs a Record from the dict shape produced by
// As_dict.
//
// ``entry`` may be a live Entry or its ``to_dict`` / serialized-dict form,
// in which case it is rebuilt through Entry_from_dict. ``recorder``,
// ``borrower`` and ``record_timestamp`` are passed through when present.
//
// TypeError when *in_dict* is not a mapping; ValueError when it has no
// ``entry`` slot.
func Record_from_dict(in_dict any) (*Record, error) {
	d, ok := asDict(in_dict)
	if !ok {
		return nil, py.TypeError.Newf("Record.from_dict expects a mapping, got %s", py.Pytypename(in_dict))
	}
	if !d.Contains("entry") {
		return nil, py.ValueError.New("Record.from_dict requires an 'entry' slot")
	}
	entry := d.GetDefault("entry", nil)
	if ed, ok := asDict(entry); ok {
		built, err := Entry_from_dict(ed.Copy())
		if err != nil {
			return nil, err
		}
		entry = built.Self()
	}
	kwargs := py.Kw{"entry": entry}
	for _, name := range []string{"recorder", "borrower", "record_timestamp"} {
		if v := d.GetDefault(name, nil); !py.IsNone(v) {
			kwargs[name] = v
		}
	}
	return New_Record(kwargs)
}

// Record_build_sync_ synchronously hydrates a record dict from a JSON
// string or raw dict.
//
// Dispatches to the registered builder for the entry's scope via
// Build_by_scope (sync path), which returns a fully hydrated Entry.
// Returns a *shallow copy* of the record dict with the serialized
// ``entry`` slot replaced by the live entry: the input is never mutated,
// because in-memory pools hand out their stored dict by reference and
// rewriting it would turn every later read into the same already-built
// entry object (sharing state between readers and bypassing the
// constitution, e.g. decryption under the current key).
//
// Returning a dict (rather than a fresh Record) is intentional: callers
// in the read pipeline only need the inner entry; the metadata fields
// (recorder/borrower/timestamp) are read directly off the dict for
// logging and audit.
func Record_build_sync_(record any) (*py.Dict, error) {
	return record_build_(record, false)
}

// Record_build_async_ is the async variant of Record_build_sync_.
//
// Awaits the registered builder for the entry's scope (async path),
// allowing nested manifest fetches to yield the loop instead of blocking
// it.
func Record_build_async_(record any) (*py.Dict, error) {
	return record_build_(record, true)
}

func record_build_(record any, asynchronous bool) (*py.Dict, error) {
	if s, ok := record.(string); ok {
		parsed, err := py.Json_loads(s)
		if err != nil {
			return nil, err
		}
		record = parsed
	}
	if b, ok := record.([]byte); ok {
		parsed, err := py.Json_loads(string(b))
		if err != nil {
			return nil, err
		}
		record = parsed
	}
	d, ok := asDict(record)
	if !ok {
		return nil, py.TypeError.Newf("'%s' object is not a mapping", py.Pytypename(record))
	}
	raw_entry, ok := d.Get("entry")
	if !ok {
		return nil, py.KeyError.New("entry")
	}
	built := d.Copy()
	entry, err := Build_by_scope(raw_entry, py.Kw{"asynchronous": asynchronous})
	if err != nil {
		return nil, err
	}
	// Port note: both builders run to completion on the calling goroutine
	// (the async one yields through the loop internally), so there is no
	// coroutine object left to ``await`` here.
	built.Set("entry", entry)
	return built, nil
}

// Record_build_ is the router: dispatch to Record_build_async_ or
// Record_build_sync_.
func Record_build_(record any, kw ...py.Kw) (*py.Dict, error) {
	if py.Truthy(py.MergeKw(kw...).Get("asynchronous", false)) {
		return Record_build_async_(record)
	}
	return Record_build_sync_(record)
}

func init() {
	py.Register_property("entry_id", "as_dict")
}
