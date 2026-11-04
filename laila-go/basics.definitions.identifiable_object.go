package laila

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// basics/definitions/identifiable_object.py -- identifiable-object base
// class with UUID and global-ID support.
//
// The identity is the triple (uuid, scopes, evolution), combined into the
// canonical global ID ``LAILA:scope1:...:scopeN:<uuid>[@evolution=<n>]``.
// GLOBAL_ID_REGEX_PATTERN is the single source of truth for parsing global
// ids; all helpers funnel through it via Process_global_id.

// ATTRIBUTE_PAIR_ is a ``key=value`` attribute pair. Keys are identifiers;
// values may contain anything except ``,`` and ``@``.
const ATTRIBUTE_PAIR_ = `[A-Za-z_][A-Za-z0-9_]*=[^,@]*`

var GLOBAL_ID_REGEX_PATTERN = regexp.MustCompile(
	`^(?P<scopes>(?:[A-Za-z0-9_]+:)+)` +
		`(?P<uuid>[0-9a-fA-F-]{36})` +
		`(?:@(?P<attributes>` + ATTRIBUTE_PAIR_ + `(?:,` + ATTRIBUTE_PAIR_ + `)*))?$`,
)

var UUID_RE_ = regexp.MustCompile(`^[0-9a-fA-F-]{36}$`)

// UUID_LIKE_RE_: hex digits and dashes in the neighbourhood of a uuid's
// length -- what a truncated / corrupted uuid looks like.
var UUID_LIKE_RE_ = regexp.MustCompile(`^[0-9a-fA-F-]{30,48}$`)
var ATTRIBUTE_RE_ = regexp.MustCompile(`^(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^,@]*)$`)

// REFERENCE_EVOLUTION_RE_: evolution as written in a *reference*; negatives
// (``-1`` = latest) are search arguments resolved by laila.remember.
var REFERENCE_EVOLUTION_RE_ = regexp.MustCompile(`^-?\d+$`)

// EVOLUTION_ATTRIBUTE is the name of the single attribute that is part of
// an object's identity.
const EVOLUTION_ATTRIBUTE = "evolution"

// DEFAULT_REFERENCE_SCOPES_ are the scopes assumed for a scope-less
// *reference* when the resolving class is the generic base.
var DEFAULT_REFERENCE_SCOPES_ = []string{ENTRY_SCOPE_}

// INIT_PENDING_ is the thread-local identity staging area (see Init_).
var INIT_PENDING_ = py.NewLocal()

// Parse_global_id_attributes parses the ``@``-suffix of a global id into an
// ordered ``{key: value}`` dict. Raises ValueError on a malformed pair or a
// duplicated key.
func Parse_global_id_attributes(attributes string) (*py.Dict, error) {
	out := py.NewDict()
	if attributes == "" {
		return out, nil
	}
	for _, pair := range strings.Split(attributes, ",") {
		m := ATTRIBUTE_RE_.FindStringSubmatch(pair)
		if m == nil {
			return nil, py.ValueError.Newf("Invalid global id attribute: %s", py.Repr(pair))
		}
		key := m[1]
		if out.Contains(key) {
			return nil, py.ValueError.Newf("Duplicate global id attribute: %s", py.Repr(key))
		}
		out.Set(key, m[2])
	}
	return out, nil
}

// Format_global_id_attributes is the inverse of Parse_global_id_attributes
// (without the leading ``@``). nil values are skipped.
func Format_global_id_attributes(attributes *py.Dict) string {
	parts := []string{}
	for _, it := range attributes.Items() {
		if py.IsNone(it.Value) {
			continue
		}
		parts = append(parts, py.Str(it.Key)+"="+py.Str(it.Value))
	}
	return strings.Join(parts, ",")
}

// Split_global_id_attributes splits ``"<head>@<attributes>"`` into
// ``(head, {key: value})``.
func Split_global_id_attributes(ref string) (string, *py.Dict, error) {
	head, tail, sep := strings.Cut(ref, "@")
	if !sep {
		return ref, py.NewDict(), nil
	}
	if tail == "" {
		return "", nil, py.ValueError.Newf("Invalid GID format: %s", ref)
	}
	attrs, err := Parse_global_id_attributes(tail)
	if err != nil {
		return "", nil, err
	}
	return head, attrs, nil
}

// LAILA_IDENTIFIABLE_OBJECT_ is the pydantic base model providing
// UUID-based identity and global-ID encoding.
//
// Constructor keywords: ``uuid`` (explicit UUID), ``scopes`` (list of
// scope segments; defaults to the class's ``_DEFAULT_SCOPES``),
// ``evolution`` (nil = constant, int = variable), ``nickname`` (hashed to a
// deterministic UUID-5 under the active namespace).
type LAILA_IDENTIFIABLE_OBJECT_ struct {
	LAILA_OBJECT_
	Uuid_      string   `py:"_uuid"`
	Scopes_    []string `py:"_scopes"`
	Evolution_ *int     `py:"_evolution"`
}

// LAILA_IDENTIFIABLE_OBJECT_class is the class descriptor of
// _LAILA_IDENTIFIABLE_OBJECT.
var LAILA_IDENTIFIABLE_OBJECT_class = py.NewClass("_LAILA_IDENTIFIABLE_OBJECT", (*LAILA_IDENTIFIABLE_OBJECT_)(nil), LAILA_OBJECT_class).
	Set_classvar("_DEFAULT_SCOPES", []string{OBJECT_SCOPE_})

// New_LAILA_IDENTIFIABLE_OBJECT_ is ``_LAILA_IDENTIFIABLE_OBJECT(**data)``.
func New_LAILA_IDENTIFIABLE_OBJECT_(kw ...py.Kw) (*LAILA_IDENTIFIABLE_OBJECT_, error) {
	o := LAILA_IDENTIFIABLE_OBJECT_class.New().(*LAILA_IDENTIFIABLE_OBJECT_)
	if err := o.Init_(py.MergeKw(kw...)); err != nil {
		return nil, err
	}
	return o, nil
}

// Default_scopes returns ``cls._DEFAULT_SCOPES`` through the MRO.
func Default_scopes(cls *py.Class) []string {
	if v, ok := cls.Classvar("_DEFAULT_SCOPES"); ok {
		return append([]string(nil), toStrings(v)...)
	}
	return []string{OBJECT_SCOPE_}
}

// Init_ stashes identity fields in thread-local storage and delegates to
// pydantic. The effective identity lands on INIT_PENDING_ so it is already
// valid when any Model_post_init in the hierarchy runs; a fallback after
// the base Init_ covers subclasses whose Model_post_init does not chain.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Init_(data py.Kw) error {
	uuid_input := data.Pop("uuid", nil)
	scopes_input := data.Pop("scopes", nil)
	evolution_input := data.Pop("evolution", nil)
	nickname_input := data.Pop("nickname", nil)

	var pending_uuid string
	if !py.IsNone(nickname_input) {
		pending_uuid = Generate_uuid_from_nickname(py.Str(nickname_input))
	} else if !py.IsNone(uuid_input) {
		pending_uuid = py.Str(uuid_input)
	} else {
		pending_uuid = py.Uuid4().String()
	}

	scopes_explicit := !py.IsNone(scopes_input)
	var pending_scopes []string
	if scopes_explicit {
		pending_scopes = toStrings(scopes_input)
	} else {
		pending_scopes = Default_scopes(o.Class_())
	}
	pending_evolution, err := toIntPtr(evolution_input)
	if err != nil {
		return err
	}

	// Save the enclosing construction's staged identity (if any): a nested
	// identifiable object may be built while pydantic validates our fields.
	prev_uuid, _ := INIT_PENDING_.Get("uuid")
	prev_scopes, _ := INIT_PENDING_.Get("scopes")
	prev_evolution, _ := INIT_PENDING_.Get("evolution")
	prev_explicit := INIT_PENDING_.GetOr("scopes_explicit", false)
	INIT_PENDING_.Set("uuid", pending_uuid)
	INIT_PENDING_.Set("scopes", pending_scopes)
	INIT_PENDING_.Set("evolution", pending_evolution)
	INIT_PENDING_.Set("scopes_explicit", scopes_explicit)

	initErr := o.LAILA_OBJECT_.Init_(data)

	INIT_PENDING_.Set("uuid", prev_uuid)
	INIT_PENDING_.Set("scopes", prev_scopes)
	INIT_PENDING_.Set("evolution", prev_evolution)
	INIT_PENDING_.Set("scopes_explicit", prev_explicit)
	if initErr != nil {
		return initErr
	}

	// Fallback for subclasses whose Model_post_init does not chain to ours.
	if o.Uuid_ == "" {
		o.Uuid_ = pending_uuid
	}
	if o.Scopes_ == nil || (scopes_explicit && !eqStrings(o.Scopes_, pending_scopes)) {
		o.Scopes_ = pending_scopes
	}
	if pending_evolution != nil && o.Evolution_ == nil {
		o.Evolution_ = pending_evolution
	}
	return nil
}

// Model_post_init copies staged identity values from INIT_PENDING_ onto
// the private attrs. Subclasses that override it must call this first so
// Global_id() is valid for their own registration logic.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Model_post_init(ctx any) error {
	if err := o.LAILA_OBJECT_.Model_post_init(ctx); err != nil {
		return err
	}
	if v, ok := INIT_PENDING_.Get("uuid"); ok && !py.IsNone(v) {
		o.Uuid_ = v.(string)
	}
	if v, ok := INIT_PENDING_.Get("scopes"); ok && !py.IsNone(v) {
		// A subclass may still declare ``_scopes`` with its own
		// default_factory; an explicit ``scopes=`` argument always wins, but
		// a generated default must not clobber it.
		if o.Scopes_ == nil || py.Truthy(INIT_PENDING_.GetOr("scopes_explicit", false)) {
			o.Scopes_ = v.([]string)
		}
	}
	if v, ok := INIT_PENDING_.Get("evolution"); ok && !py.IsNone(v) {
		o.Evolution_ = v.(*int)
	}
	return nil
}

// From_global_id constructs an instance of cls from an encoded global ID
// string (``cls.from_global_id``).
func From_global_id(cls *py.Class, global_id string) (any, error) {
	identity_data, err := Process_global_id(global_id)
	if err != nil {
		return nil, err
	}
	inst := cls.New()
	if err := inst.(interface{ Init_(py.Kw) error }).Init_(identity_data); err != nil {
		return nil, err
	}
	return inst, nil
}

// LAILA_IDENTIFIABLE_OBJECT_from_global_id is
// ``_LAILA_IDENTIFIABLE_OBJECT.from_global_id``.
func LAILA_IDENTIFIABLE_OBJECT_from_global_id(global_id string) (*LAILA_IDENTIFIABLE_OBJECT_, error) {
	v, err := From_global_id(LAILA_IDENTIFIABLE_OBJECT_class, global_id)
	if err != nil {
		return nil, err
	}
	return v.(*LAILA_IDENTIFIABLE_OBJECT_), nil
}

// Resolve_global_id decodes a global-id *reference* into a full global ID
// (``cls.resolve_global_id``). Keyword options: ``evolution`` (int),
// ``prefix_scopes`` (list), ``default_scopes`` (list), ``parse_evolution``
// (bool, default true). Pass nil for cls to resolve on the generic base.
func Resolve_global_id(cls *py.Class, ref any, kw ...py.Kw) (string, error) {
	opts := py.MergeKw(kw...)
	if cls == nil {
		cls = LAILA_IDENTIFIABLE_OBJECT_class
	}
	refStr, ok := ref.(string)
	if !ok {
		return "", py.ValueError.Newf("global id reference must be a string, got %s", py.Pytypename(ref))
	}
	evolution, err := toIntPtr(opts.Get("evolution", nil))
	if err != nil {
		return "", err
	}
	var prefix []string
	if ps := opts.Get("prefix_scopes", nil); !py.IsNone(ps) {
		prefix = toStrings(ps)
	} else {
		prefix = []string{TOPMOST_SCOPE_}
	}
	parse_evolution := py.Truthy(opts.Get("parse_evolution", true))

	var head string
	var attrs *py.Dict
	if parse_evolution {
		head, attrs, err = Split_global_id_attributes(refStr)
		if err != nil {
			return "", err
		}
	} else {
		head, attrs = refStr, py.NewDict()
	}

	segs := strings.Split(head, ":")
	scopes, tail := segs[:len(segs)-1], segs[len(segs)-1]
	if tail == "" || !allNonEmpty(scopes) {
		return "", py.ValueError.Newf("Invalid GID format: %s", refStr)
	}

	// Already a full global id (well-formed *and* framed by the prefix)?
	if GLOBAL_ID_REGEX_PATTERN.MatchString(refStr) &&
		hasPrefix(scopes, prefix) &&
		len(scopes) > len(prefix) &&
		(evolution == nil || attrs.GetDefault(EVOLUTION_ATTRIBUTE, nil) == strconv.Itoa(*evolution)) {
		return refStr, nil
	}

	if evolution != nil {
		attrs.Set(EVOLUTION_ATTRIBUTE, strconv.Itoa(*evolution))
	} else if ev, ok := attrs.Get(EVOLUTION_ATTRIBUTE); ok && !REFERENCE_EVOLUTION_RE_.MatchString(ev.(string)) {
		return "", py.ValueError.Newf("Invalid evolution in GID: %s", refStr)
	}

	var uid string
	if UUID_RE_.MatchString(tail) {
		uid = tail
	} else if UUID_LIKE_RE_.MatchString(tail) {
		// A truncated or corrupted uuid: a malformed id, not a nickname.
		return "", py.ValueError.Newf("Invalid GID format: %s", refStr)
	} else {
		uid = Generate_uuid_from_nickname(tail)
	}

	// Tolerate a spelled-out prefix.
	if len(prefix) > 0 && hasPrefix(scopes, prefix) {
		scopes = scopes[len(prefix):]
	}
	if len(scopes) == 0 {
		if ds := opts.Get("default_scopes", nil); !py.IsNone(ds) {
			scopes = toStrings(ds)
		} else if eqStrings(Default_scopes(cls), []string{OBJECT_SCOPE_}) {
			// The base class has no domain of its own: a scope-less
			// *reference* means an entry.
			scopes = append([]string(nil), DEFAULT_REFERENCE_SCOPES_...)
		} else {
			scopes = Default_scopes(cls)
		}
	}

	gid := strings.Join(append(append([]string{}, prefix...), scopes...), ":") + ":" + uid
	suffix := Format_global_id_attributes(attrs)
	if suffix != "" {
		return gid + "@" + suffix, nil
	}
	return gid, nil
}

// To_global_id builds a global ID string from its constituent parts
// (``_LAILA_IDENTIFIABLE_OBJECT.to_global_id``). Keywords: ``uuid``,
// ``scopes``, ``evolution``, ``nickname``.
func To_global_id(kw ...py.Kw) string {
	opts := py.MergeKw(kw...)
	uuid := opts.Get("uuid", nil)
	scopes := toStrings(opts.Get("scopes", nil))
	// ``evolution`` may arrive as the port's *int optional (Evolution_,
	// Process_global_id output) or as a plain int.
	evolution := py.Deref_any(opts.Get("evolution", nil))
	if nick := opts.Get("nickname", nil); !py.IsNone(nick) {
		uuid = Generate_uuid_from_nickname(py.Str(nick))
	}
	if len(scopes) == 0 {
		scopes = []string{OBJECT_SCOPE_}
	}
	base := strings.Join(append([]string{TOPMOST_SCOPE_}, scopes...), ":") + ":" + py.Str(uuid)
	if py.IsNone(evolution) {
		return base
	}
	return base + "@" + EVOLUTION_ATTRIBUTE + "=" + py.Str(evolution)
}

// Is_laila_resource returns true if global_id parses as a laila global-ID
// string.
func Is_laila_resource(global_id string) bool {
	return GLOBAL_ID_REGEX_PATTERN.MatchString(global_id)
}

// Global_id is the fully-qualified global ID string for this instance.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Global_id() string {
	return To_global_id(py.Kw{"uuid": o.Uuid_, "scopes": o.Scopes_, "evolution": o.Evolution_})
}

// Set_global_id re-parses the string and re-assigns the underlying
// private attrs.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Set_global_id(value string) error {
	parsed, err := Process_global_id(value)
	if err != nil {
		return err
	}
	o.Uuid_ = parsed["uuid"].(string)
	o.Scopes_ = parsed["scopes"].([]string)
	o.Evolution_ = parsed["evolution"].(*int)
	return nil
}

// Uuid is the bare UUID string (no scopes, no evolution suffix).
func (o *LAILA_IDENTIFIABLE_OBJECT_) Uuid() string { return o.Uuid_ }

// Set_uuid is the ``uuid`` property setter.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Set_uuid(value string) { o.Uuid_ = value }

// Scopes are the hierarchical scope segments in global-id order.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Scopes() []string { return o.Scopes_ }

// Set_scopes is the ``scopes`` property setter.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Set_scopes(value []string) { o.Scopes_ = value }

// Evolution is the evolution counter, or nil for constant identities.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Evolution() *int { return o.Evolution_ }

// Set_evolution is the ``evolution`` property setter.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Set_evolution(value *int) { o.Evolution_ = value }

// Get_scopes_from_global_id extracts the scopes list without instantiating.
func Get_scopes_from_global_id(global_id string) ([]string, error) {
	p, err := Process_global_id(global_id)
	if err != nil {
		return nil, err
	}
	return p["scopes"].([]string), nil
}

// Get_uuid_from_global_id extracts the bare UUID without instantiating.
func Get_uuid_from_global_id(global_id string) (string, error) {
	p, err := Process_global_id(global_id)
	if err != nil {
		return "", err
	}
	return p["uuid"].(string), nil
}

// Get_evolution_from_global_id extracts the evolution counter without
// instantiating (nil for global ids without an ``evolution`` attribute).
func Get_evolution_from_global_id(global_id string) (*int, error) {
	p, err := Process_global_id(global_id)
	if err != nil {
		return nil, err
	}
	return p["evolution"].(*int), nil
}

// Get_attributes_from_global_id returns every ``@key=value`` attribute of
// global_id as raw strings, including search arguments.
func Get_attributes_from_global_id(global_id string) (*py.Dict, error) {
	resolved, err := Resolve_global_id(nil, global_id)
	if err != nil {
		return nil, err
	}
	m := GLOBAL_ID_REGEX_PATTERN.FindStringSubmatch(resolved)
	if m == nil {
		return nil, py.ValueError.Newf("Invalid GID format: %s", resolved)
	}
	return Parse_global_id_attributes(m[GLOBAL_ID_REGEX_PATTERN.SubexpIndex("attributes")])
}

// Strip_global_id_attributes returns global_id without its ``@...`` suffix.
func Strip_global_id_attributes(global_id string) string {
	head, _, _ := strings.Cut(global_id, "@")
	return head
}

// Has_evolution returns true if this object has a non-nil evolution.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Has_evolution() bool { return o.Evolution_ != nil }

// Process_global_id parses a global ID (or scoped shorthand) into its
// ``uuid`` (string), ``scopes`` ([]string) and ``evolution`` (*int) parts.
func Process_global_id(global_id any) (py.Kw, error) {
	s, ok := global_id.(string)
	if !ok || s == "" {
		return nil, py.ValueError.Newf("Invalid GID format: %s", py.Str(global_id))
	}
	// Framed full gids pass through unchanged; scoped shorthands are expanded.
	resolved, err := Resolve_global_id(nil, s)
	if err != nil {
		return nil, err
	}
	m := GLOBAL_ID_REGEX_PATTERN.FindStringSubmatch(resolved)
	if m == nil {
		return nil, py.ValueError.Newf("Invalid GID format: %s", resolved)
	}
	attrs, err := Parse_global_id_attributes(m[GLOBAL_ID_REGEX_PATTERN.SubexpIndex("attributes")])
	if err != nil {
		return nil, err
	}
	var evolution *int
	if raw, ok := attrs.Get(EVOLUTION_ATTRIBUTE); ok {
		rs := raw.(string)
		if !isdigit(rs) {
			return nil, py.ValueError.Newf("Invalid evolution in GID: %s", resolved)
		}
		n, _ := strconv.Atoi(rs)
		evolution = &n
	}
	// The scopes group is "LAILA:<mid...>:"; drop the leading TOPMOST and the
	// empty string after the final ':' so that To_global_id round-trips.
	segs := strings.Split(m[GLOBAL_ID_REGEX_PATTERN.SubexpIndex("scopes")], ":")
	return py.Kw{
		"uuid":      m[GLOBAL_ID_REGEX_PATTERN.SubexpIndex("uuid")],
		"scopes":    append([]string{}, segs[1:len(segs)-1]...),
		"evolution": evolution,
	}, nil
}

// LAILA_IDENTIFIABLE_OBJECT_type classifies a global id as ``"variable"``
// (has evolution) or ``"constant"`` (``_LAILA_IDENTIFIABLE_OBJECT.type``).
func LAILA_IDENTIFIABLE_OBJECT_type(x string) (string, error) {
	processed, err := Process_global_id(x)
	if err != nil {
		return "", err
	}
	if processed["evolution"].(*int) != nil {
		return "variable", nil
	}
	return "constant", nil
}

// String is ``str(obj)`` -> Global_id.
func (o *LAILA_IDENTIFIABLE_OBJECT_) String() string { return o.Global_id() }

// Repr is ``repr(obj)`` -> Global_id.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Repr() string { return o.Global_id() }

// Hash hashes by Global_id so instances are usable as set / dict keys.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Hash() uint64 { return py.Must(py.Hash(o.Global_id())) }

// Eq: same Global_id means the same object, regardless of payload.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Eq(other any) bool {
	other_ident, ok := other.(interface{ Global_id() string })
	if !ok || !py.Isinstance(other, LAILA_IDENTIFIABLE_OBJECT_class) {
		return false
	}
	return o.Global_id() == other_ident.Global_id()
}

// Ne is ``__ne__``.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Ne(other any) bool { return !o.Eq(other) }

// Identity returns a minimal dict describing this object's identity:
// always ``uuid``; ``scopes`` only when non-empty, ``evolution`` only when
// non-nil.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Identity() *py.Dict {
	identity := py.DictOf("uuid", o.Uuid())
	if len(o.Scopes()) > 0 {
		identity.Set("scopes", py.NewList(stringsToAny(o.Scopes())...))
	}
	if o.Evolution() != nil {
		identity.Set("evolution", *o.Evolution())
	}
	return identity
}

// Identity_as_json returns Identity serialized to a JSON string.
func (o *LAILA_IDENTIFIABLE_OBJECT_) Identity_as_json() string {
	return py.Must(py.Json_dumps(o.Identity()))
}

// Generate_uuid_from_nickname deterministically derives a UUID from a
// human-readable nickname with uuid5 under the active namespace.
func Generate_uuid_from_nickname(nickname string) string {
	return py.Uuid5(Get_active_namespace(), nickname).String()
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func toStrings(v any) []string {
	switch x := v.(type) {
	case nil:
		return nil
	case []string:
		return append([]string(nil), x...)
	case string:
		return []string{x}
	}
	items := py.Iter(v)
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = py.Str(it)
	}
	return out
}

func stringsToAny(s []string) []any {
	out := make([]any, len(s))
	for i, x := range s {
		out[i] = x
	}
	return out
}

func toIntPtr(v any) (*int, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case *int:
		return x, nil
	case int:
		return &x, nil
	case int64:
		n := int(x)
		return &n, nil
	case float64:
		n := int(x)
		return &n, nil
	case bool:
		n := 0
		if x {
			n = 1
		}
		return &n, nil
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(x))
		if err != nil {
			return nil, py.ValueError.Newf("invalid literal for int() with base 10: %s", py.Repr(x))
		}
		return &n, nil
	}
	return nil, py.TypeError.Newf("int() argument must be a string, a bytes-like object or a real number, not '%s'", py.Pytypename(v))
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func hasPrefix(s, prefix []string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return eqStrings(s[:len(prefix)], prefix)
}

func allNonEmpty(s []string) bool {
	for _, x := range s {
		if x == "" {
			return false
		}
	}
	return true
}

func isdigit(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func init() {
	py.Register_property("global_id", "uuid", "scopes", "evolution")
}
