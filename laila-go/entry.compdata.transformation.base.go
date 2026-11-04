package laila

import (
	"regexp"
	"strings"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// entry/compdata/transformation/base.py -- abstract base classes for data
// transformations and the pipeline that chains them.
//
// A *transformation* is a reversible function on opaque data: an encoding
// (base64), a compression (zlib), an encryption (Fernet), a serialisation
// step (msgpack, pickle, numpy) or a wrapper into a JSON-friendly string.
// They are the building blocks SimpleConstitution strings together to
// recover an entry's payload from its serialised form.
//
// Two layers live in this file:
//
//   - Data_transformation_ -- the base every concrete transformation
//     embeds. Concrete classes register themselves in
//     Data_transformation_REGISTRY keyed by their ``name`` field, so the
//     constitution machinery can look a transformation up by name.
//   - TransformationSequence -- an ordered pipeline. Forward applies the
//     transformations in order on the way out (when serialising into a
//     pool); the inverse is replayed by the constitution from the
//     recovery-code snippets Forward collects.
//
// Port note -- recovery codes. ``backward_code`` is Python source text
// and is emitted byte-identically to the Python implementation so a
// Python peer can ``exec`` records written by this port. The port itself
// cannot execute Python; instead every registered class exposes its
// recovery-code *template* (the same regex machinery Python uses for the
// trust gate) and a Recover hook that rebuilds the native inverse from
// the template's captured slots. See Exec_one_fn_.

// SLOT_KWARGS_ is the sentinel ``backward_kwargs`` used to render a class's
// recovery-code template; its repr is located in the rendered code and
// replaced by a capture group (Backward_code_pattern_).
var SLOT_KWARGS_ = py.DictOf("__laila_slot__", 0)

// SLOT_KWARGS_REPR_ is ``repr(_SLOT_KWARGS)``.
var SLOT_KWARGS_REPR_ = py.Repr(SLOT_KWARGS_)

// Is_literal_dict_ is true when text is the repr of a plain literal dict.
func Is_literal_dict_(text string) bool {
	v, err := py.Literal_eval(text)
	if err != nil {
		return false
	}
	_, ok := v.(*py.Dict)
	return ok
}

// Data_transformer is the surface every concrete transformation
// implements (``_data_transformation`` instance protocol).
type Data_transformer interface {
	Forward(data any) (any, error)
	Backward(data any) (any, error)
	// Base_ exposes the embedded base (fields: name, kwargs, backward_code).
	Base_() *Data_transformation_
	Class_() *py.Class
}

// Backward_fn is a compiled recovery step (``backward(inp)``).
type Backward_fn func(data any) (any, error)

// Template_slot is one ``(sentinel_text, validator)`` pair of a class's
// recovery-code template.
type Template_slot struct {
	Text      string
	Validator func(group string) bool
}

// Transformation_hooks are the per-class classmethods of
// _data_transformation that the port cannot express as Go methods on a
// type (``_template_probe``, ``_template_slots``) plus Recover, the
// native replacement for ``exec``-ing a matched recovery code.
type Transformation_hooks struct {
	// Probe returns an instance whose backward_code embeds the sentinel
	// kwargs (``cls._template_probe``). nil means the default probe:
	// ``cls(backward_kwargs=dict(_SLOT_KWARGS))``.
	Probe func() (Data_transformer, error)
	// Slots is ``cls._template_slots``; nil means the default single
	// kwargs slot.
	Slots func() []Template_slot
	// Recover builds the native inverse from the groups captured by the
	// template (one per slot). nil means the default: construct the class
	// with ``backward_kwargs=literal_eval(groups[0])`` and use Backward.
	Recover func(groups []string) (Backward_fn, error)
}

// Data_transformation_ is the abstract base for a single reversible data
// transformation (``_data_transformation``).
//
// Concrete classes must set a stable, unique ``name`` (the registry key:
// renaming is a wire-format breaking change), implement Forward and
// Backward as mathematical inverses, and build ``backward_code`` -- the
// standalone Python snippet the constitution replays at recovery time.
type Data_transformation_ struct {
	py.Object
	Name            string   `py:"name" default:""`
	Forward_kwargs  *py.Dict `py:"forward_kwargs,factory=New_kwargs_dict_"`
	Backward_kwargs *py.Dict `py:"backward_kwargs,factory=New_kwargs_dict_"`
	Backward_code   string   `py:"backward_code" default:""`
}

// New_kwargs_dict_ is the ``Field(default_factory=dict)`` of the kwargs.
func (t *Data_transformation_) New_kwargs_dict_() *py.Dict { return py.NewDict() }

// Data_transformation_class is the class descriptor of
// _data_transformation (abstract).
var Data_transformation_class = func() *py.Class {
	c := py.NewClass("_data_transformation", (*Data_transformation_)(nil))
	c.Abstract = true
	return c
}()

// Base_ returns the embedded base.
func (t *Data_transformation_) Base_() *Data_transformation_ { return t }

// Init_ is ``_data_transformation.__init__`` (pydantic's constructor).
func (t *Data_transformation_) Init_(data py.Kw) error {
	return py.Init(t.Self(), t.Class_(), data)
}

// Set_backward_code is the ``__setattr__`` hook on ``backward_code``: every
// non-empty assignment lands in KNOWN_BACKWARD_CODES_ so exact codes
// generated in this process are trusted on the way back.
func (t *Data_transformation_) Set_backward_code(value string) {
	t.Backward_code = value
	if value != "" {
		TRUST_LOCK_.Lock()
		KNOWN_BACKWARD_CODES_.Add(value)
		TRUST_LOCK_.Unlock()
	}
}

// Repr is pydantic's default ``repr`` (``Base64(name='base64', ...)``).
func (t *Data_transformation_) Repr() string {
	parts := []string{}
	for _, it := range py.Model_dump(t.Self()).Items() {
		parts = append(parts, py.Str(it.Key)+"="+py.Repr(it.Value))
	}
	return t.Class_().Name + "(" + strings.Join(parts, ", ") + ")"
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

// Data_transformation_REGISTRY is ``_data_transformation.REGISTRY``:
// transformation name -> class.
var Data_transformation_REGISTRY = map[string]*py.Class{}

// Data_transformation_registry_order_ keeps registration order so trust
// checks iterate deterministically (``list(REGISTRY.values())``).
var Data_transformation_registry_order_ []string

// Transformation_hooks_ holds the per-class hooks (see Transformation_hooks).
var Transformation_hooks_ = map[*py.Class]*Transformation_hooks{}

// Register_transformation_ is ``__init_subclass__``: records cls under
// name in the registry (when name is non-empty), installs the class-level
// field default for ``name`` and the per-class hooks. Returns cls.
func Register_transformation_(cls *py.Class, name string, hooks *Transformation_hooks) *py.Class {
	if hooks == nil {
		hooks = &Transformation_hooks{}
	}
	Transformation_hooks_[cls] = hooks
	if name != "" {
		cls.Add_before(func(_ *py.Class, data py.Kw) (py.Kw, error) {
			if !data.Has("name") {
				data["name"] = name
			}
			return data, nil
		})
		TRUST_LOCK_.Lock()
		if _, dup := Data_transformation_REGISTRY[name]; !dup {
			Data_transformation_registry_order_ = append(Data_transformation_registry_order_, name)
		}
		Data_transformation_REGISTRY[name] = cls
		TRUST_LOCK_.Unlock()
	}
	return cls
}

// Registered_transformations_ is ``list(_data_transformation.REGISTRY.values())``.
func Registered_transformations_() []*py.Class {
	TRUST_LOCK_.Lock()
	defer TRUST_LOCK_.Unlock()
	out := make([]*py.Class, 0, len(Data_transformation_registry_order_))
	for _, n := range Data_transformation_registry_order_ {
		out = append(out, Data_transformation_REGISTRY[n])
	}
	return out
}

// New_transformation_ instantiates cls with keyword arguments
// (``cls(**kw)``), the generic constructor behind the default probe and
// Recover hooks.
func New_transformation_(cls *py.Class, kw py.Kw) (Data_transformer, error) {
	inst := cls.New()
	initer, ok := inst.(interface{ Init_(py.Kw) error })
	if !ok {
		return nil, py.TypeError.Newf("%s is not constructible", cls.Name)
	}
	if err := initer.Init_(kw); err != nil {
		return nil, err
	}
	t, ok := inst.(Data_transformer)
	if !ok {
		return nil, py.TypeError.Newf("%s is not a _data_transformation", cls.Name)
	}
	return t, nil
}

// ---------------------------------------------------------------------------
// Recovery-code trust
// ---------------------------------------------------------------------------
//
// A SimpleConstitution read back from a pool or a peer is a list of Python
// source snippets. Only snippets this process could itself have produced
// are allowed to run:
//
//   - Matches_backward_code_ -- template match against the class's own
//     recovery code (fixed text identical, the backward_kwargs slot a
//     literal dict). Records written with other kwargs by other processes
//     / hosts / ports still match.
//   - KNOWN_BACKWARD_CODES_ -- exact strings generated in this process
//     (every Set_backward_code lands here), the fallback for third-party
//     transformations whose template cannot be probed.

// KNOWN_BACKWARD_CODES_ is ``_data_transformation._KNOWN_BACKWARD_CODES``.
var KNOWN_BACKWARD_CODES_ = py.NewSet()

// Compiled_template is a compiled recovery-code template: the regex and
// one validator per capture group.
type Compiled_template struct {
	Pattern    *regexp.Regexp
	Validators []func(string) bool
}

// PATTERN_CACHE_ is ``_data_transformation._PATTERN_CACHE`` (class ->
// compiled template or nil).
var PATTERN_CACHE_ = map[*py.Class]*Compiled_template{}

// TRUST_LOCK_ is ``_data_transformation._TRUST_LOCK``.
var TRUST_LOCK_ sync.Mutex

// Template_probe_ is ``cls._template_probe()``: an instance whose
// backward_code embeds the sentinel kwargs. Classes needing extra
// constructor arguments (e.g. a key) override it through their hooks;
// an error means "no template" and only exact, locally generated codes
// are trusted for the class.
func Template_probe_(cls *py.Class) (Data_transformer, error) {
	if h := Transformation_hooks_[cls]; h != nil && h.Probe != nil {
		return h.Probe()
	}
	return New_transformation_(cls, py.Kw{"backward_kwargs": SLOT_KWARGS_.Copy()})
}

// Template_slots_ is ``cls._template_slots()``: the (sentinel_text,
// validator) pairs present in the probe's code.
func Template_slots_(cls *py.Class) []Template_slot {
	if h := Transformation_hooks_[cls]; h != nil && h.Slots != nil {
		return h.Slots()
	}
	return []Template_slot{{Text: SLOT_KWARGS_REPR_, Validator: Is_literal_dict_}}
}

// Backward_code_pattern_ is ``cls._backward_code_pattern()``: the compiled
// template for this class's recovery code (cached), or nil.
func Backward_code_pattern_(cls *py.Class) *Compiled_template {
	TRUST_LOCK_.Lock()
	if cached, ok := PATTERN_CACHE_[cls]; ok {
		TRUST_LOCK_.Unlock()
		return cached
	}
	TRUST_LOCK_.Unlock()
	code := ""
	func() {
		defer func() { _ = recover() }()
		if probe, err := Template_probe_(cls); err == nil {
			code = probe.Base_().Backward_code
		}
	}()
	var result *Compiled_template
	if code != "" {
		slots := Template_slots_(cls)
		tokens := make([]string, len(slots))
		for i, s := range slots {
			tokens[i] = regexp.QuoteMeta(s.Text)
		}
		token_re := regexp.MustCompile("(" + strings.Join(tokens, "|") + ")")
		var pattern strings.Builder
		pattern.WriteString("(?s)^")
		var validators []func(string) bool
		for _, part := range splitKeep(token_re, code) {
			var hit func(string) bool
			for _, s := range slots {
				if s.Text == part {
					hit = s.Validator
					break
				}
			}
			if hit != nil {
				pattern.WriteString("(.*?)")
				validators = append(validators, hit)
			} else {
				pattern.WriteString(regexp.QuoteMeta(part))
			}
		}
		pattern.WriteString("$")
		// A slot-less template (e.g. JsonString) is an exact match.
		result = &Compiled_template{Pattern: regexp.MustCompile(pattern.String()), Validators: validators}
	}
	TRUST_LOCK_.Lock()
	PATTERN_CACHE_[cls] = result
	TRUST_LOCK_.Unlock()
	return result
}

// splitKeep is ``re.split(f"({token_re})", code)``: the separators are kept.
func splitKeep(re *regexp.Regexp, s string) []string {
	var out []string
	last := 0
	for _, m := range re.FindAllStringIndex(s, -1) {
		out = append(out, s[last:m[0]], s[m[0]:m[1]])
		last = m[1]
	}
	return append(out, s[last:])
}

// Match_backward_code returns the template groups when code is an
// instance of cls's recovery template (``cls._matches_backward_code`` plus
// the captured slots), or (nil, false).
func Match_backward_code(cls *py.Class, code string) ([]string, bool) {
	compiled := Backward_code_pattern_(cls)
	if compiled == nil {
		return nil, false
	}
	m := compiled.Pattern.FindStringSubmatch(code)
	if m == nil {
		return nil, false
	}
	groups := m[1:]
	for i, v := range compiled.Validators {
		if i >= len(groups) || !v(groups[i]) {
			return nil, false
		}
	}
	return groups, true
}

// Matches_backward_code_ is ``cls._matches_backward_code(code)``.
func Matches_backward_code_(cls *py.Class, code string) bool {
	_, ok := Match_backward_code(cls, code)
	return ok
}

// Is_trusted_backward_code is ``_data_transformation.is_trusted_backward_code``:
// true when code may be executed as a recovery step -- generated in this
// process, or matching the recovery-code template of a loaded class.
func Is_trusted_backward_code(code any) bool {
	s, ok := code.(string)
	if !ok || s == "" {
		return false
	}
	TRUST_LOCK_.Lock()
	known := KNOWN_BACKWARD_CODES_.Contains(s)
	TRUST_LOCK_.Unlock()
	if known {
		return true
	}
	for _, sub := range Registered_transformations_() {
		if Matches_backward_code_(sub, s) {
			return true
		}
	}
	return false
}

// Compile_backward_code_ resolves a recovery-code snippet to its native
// inverse by template recognition (the port's replacement for ``exec``).
// Returns (nil, false) when no registered class recognises the code.
func Compile_backward_code_(code string) (Backward_fn, bool, error) {
	for _, cls := range Registered_transformations_() {
		groups, ok := Match_backward_code(cls, code)
		if !ok {
			continue
		}
		if h := Transformation_hooks_[cls]; h != nil && h.Recover != nil {
			fn, err := h.Recover(groups)
			return fn, true, err
		}
		kw := py.Kw{}
		if len(groups) > 0 {
			parsed, err := py.Literal_eval(groups[0])
			if err != nil {
				return nil, true, err
			}
			kw["backward_kwargs"] = parsed
		}
		inst, err := New_transformation_(cls, kw)
		if err != nil {
			return nil, true, err
		}
		return inst.Backward, true, nil
	}
	return nil, false, nil
}

// ---------------------------------------------------------------------------
// TransformationSequence
// ---------------------------------------------------------------------------

// TransformationSequence is a sequential transformation pipeline.
//
// Forward applies the transformations left-to-right and returns
// ``(transformed_data, inverse_codes_in_reverse)``; Append mutates the
// pipeline (one or many) returning the sequence for chaining. An empty
// pipeline is the identity: Forward(x) returns (x, []) and the
// constitution simply returns x on the way back.
type TransformationSequence struct {
	py.Object
	Transformations []Data_transformer `py:"transformations,factory=New_transformations_"`
}

// New_transformations_ is ``Field(default_factory=list)``.
func (s *TransformationSequence) New_transformations_() []Data_transformer {
	return []Data_transformer{}
}

// TransformationSequence_class is the class descriptor.
var TransformationSequence_class = py.NewClass("TransformationSequence", (*TransformationSequence)(nil))

// New_TransformationSequence is ``TransformationSequence(transformations=[...])``.
func New_TransformationSequence(kw ...py.Kw) (*TransformationSequence, error) {
	s := TransformationSequence_class.New().(*TransformationSequence)
	data := py.MergeKw(kw...)
	if raw, ok := data["transformations"]; ok {
		items, err := coerceTransformations(raw)
		if err != nil {
			return nil, err
		}
		data["transformations"] = items
	}
	if err := py.Init(s, TransformationSequence_class, data); err != nil {
		return nil, err
	}
	return s, nil
}

// Transformations_of is a convenience constructor:
// ``TransformationSequence(transformations=[t1, t2, ...])``.
func Transformations_of(ts ...Data_transformer) *TransformationSequence {
	return py.Must(New_TransformationSequence(py.Kw{"transformations": ts}))
}

func coerceTransformations(raw any) ([]Data_transformer, error) {
	switch x := raw.(type) {
	case nil:
		return []Data_transformer{}, nil
	case []Data_transformer:
		return x, nil
	}
	var out []Data_transformer
	for _, it := range py.Iter(raw) {
		t, ok := it.(Data_transformer)
		if !ok {
			return nil, py.Validation_error("TransformationSequence",
				"transformations\n  Input should be a valid _data_transformation [type=model_type, input_value="+py.Repr(it)+", input_type="+py.Pytypename(it)+"]")
		}
		out = append(out, t)
	}
	return out, nil
}

// Forward applies every contained transformation in order and collects
// their inverses: ``(transformed_value, inverse_code_snippets)`` where the
// second element is already reversed for replay.
func (s *TransformationSequence) Forward(data any) (any, []string, error) {
	current := data
	inverse_codes := []string{}
	for _, transformation := range s.Transformations {
		next, err := transformation.Forward(current)
		if err != nil {
			return nil, nil, err
		}
		current = next
		inverse_codes = append(inverse_codes, transformation.Base_().Backward_code)
	}
	for i, j := 0, len(inverse_codes)-1; i < j; i, j = i+1, j-1 {
		inverse_codes[i], inverse_codes[j] = inverse_codes[j], inverse_codes[i]
	}
	return current, inverse_codes, nil
}

// Iter is ``__iter__``: the contained transformations.
func (s *TransformationSequence) Iter() []Data_transformer {
	return append([]Data_transformer(nil), s.Transformations...)
}

// Append appends one transformation or a list of them (``[]Data_transformer``,
// ``*py.List`` or ``[]any`` of transformations) and returns the sequence.
// TypeError otherwise.
func (s *TransformationSequence) Append(t any) (*TransformationSequence, error) {
	switch x := t.(type) {
	case Data_transformer:
		s.Transformations = append(s.Transformations, x)
		return s, nil
	case []Data_transformer:
		s.Transformations = append(s.Transformations, x...)
		return s, nil
	case *py.List, []any:
		for _, it := range py.Iter(x) {
			dt, ok := it.(Data_transformer)
			if !ok {
				return nil, py.TypeError.New("All elements must be _data_transformation instances.")
			}
			s.Transformations = append(s.Transformations, dt)
		}
		return s, nil
	}
	return nil, py.TypeError.New("append expects a _data_transformation or list of them.")
}

// Repr is a human-readable pipeline summary.
func (s *TransformationSequence) Repr() string {
	if len(s.Transformations) == 0 {
		return s.Class_().Name + "(identity)"
	}
	names := make([]string, len(s.Transformations))
	for i, t := range s.Transformations {
		if n := t.Base_().Name; n != "" {
			names[i] = n
		} else {
			names[i] = t.Class_().Name
		}
	}
	return s.Class_().Name + "(" + strings.Join(names, " -> ") + ")"
}
