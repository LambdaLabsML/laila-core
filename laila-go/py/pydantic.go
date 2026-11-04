package py

import (
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// ---------------------------------------------------------------------------
// naming rule (D1): Python name -> Go identifier
// ---------------------------------------------------------------------------

var dunderNames = map[string]string{
	"__init__": "Init_", "__str__": "String", "__repr__": "Repr", "__eq__": "Eq", "__ne__": "Ne",
	"__hash__": "Hash", "__len__": "Len", "__iter__": "Iter", "__next__": "Next", "__contains__": "Contains",
	"__getitem__": "Getitem", "__setitem__": "Setitem", "__delitem__": "Delitem", "__call__": "Call",
	"__enter__": "Enter", "__exit__": "Exit", "__bool__": "Bool", "__lt__": "Lt", "__le__": "Le",
	"__gt__": "Gt", "__ge__": "Ge", "__copy__": "Copy_", "__deepcopy__": "Deepcopy", "__await__": "Await",
	"__lshift__": "Lshift", "__rshift__": "Rshift", "__add__": "Add", "__sub__": "Sub", "__getattr__": "Getattr",
	"__setattr__": "Setattr", "__del__": "Del", "__new__": "New", "__reduce__": "Reduce",
}

// Goname applies the port's naming rule to a Python identifier:
// capitalise the first letter, move leading underscores to the end, map
// dunders to their fixed Go names.
func Goname(py string) string {
	if g, ok := dunderNames[py]; ok {
		return g
	}
	n := 0
	for n < len(py) && py[n] == '_' {
		n++
	}
	core := py[n:]
	if core == "" {
		return py
	}
	if n >= 2 && strings.HasSuffix(core, "__") && len(core) > 2 {
		core = strings.TrimSuffix(core, "__") // other dunders: strip both sides
		n = 0
	}
	r := []rune(core)
	r[0] = unicode.ToUpper(r[0])
	return string(r) + strings.Repeat("_", n)
}

// Pyname is the inverse of Goname for field names (Go struct field ->
// Python attribute): trailing underscores become leading ones and the
// first letter is lower-cased unless the name is an all-caps constant.
func Pyname(goName string) string {
	k := 0
	for k < len(goName) && goName[len(goName)-1-k] == '_' {
		k++
	}
	core := goName[:len(goName)-k]
	if core == "" {
		return goName
	}
	if strings.ToUpper(core) != core || len(core) == 1 {
		r := []rune(core)
		r[0] = unicode.ToLower(r[0])
		core = string(r)
	}
	return strings.Repeat("_", k) + core
}

// ---------------------------------------------------------------------------
// Class registry
// ---------------------------------------------------------------------------

// Class is the descriptor of a translated Python class.
type Class struct {
	Name   string
	Module string
	Bases  []*Class
	typ    reflect.Type // pointer-to-struct type of instances (nil for abstract mixins)
	// New allocates a zero instance (pointer) and binds it; set by NewClass.
	New func() any
	// Before validators: model_validator(mode="before") classmethods.
	Before []func(cls *Class, data Kw) (Kw, error)
	// After validators: model_validator(mode="after").
	After []func(self any) error
	// Field validators: field_validator("x") -> Validators["x"].
	Validators map[string][]func(self any, v any) (any, error)
	// Extra: "ignore" (pydantic default), "forbid", "allow".
	Extra string
	// Frozen makes Setattr raise.
	Frozen bool
	// Abstract marks ABCs (constructor raises TypeError).
	Abstract bool
	// ClassVars holds class attributes by Python name.
	ClassVars map[string]any
	// PrivateDefaults are PrivateAttr(default_factory=...) declarations by
	// Python name (cls.__private_attributes__[name].default_factory).
	PrivateDefaults map[string]func() any
	// FieldDefaults are subclass default overrides by Python name
	// (``x: int = Field(default=4096)`` redeclared in a subclass without
	// changing the field's storage) -- see Set_field_default.
	FieldDefaults map[string]any
	mu            sync.Mutex
	subs      []*Class
	fields    []*FieldInfo
	fieldsOK  bool
	mro       []*Class
}

var (
	classesMu sync.RWMutex
	classes   = map[string]*Class{}
)

// NewClass declares a class for the struct type of proto (a typed nil
// pointer such as (*S3Pool)(nil)); pass nil for stateless mixins.
func NewClass(name string, proto any, bases ...*Class) *Class {
	c := &Class{Name: name, Module: module_of_caller_(), Bases: bases, Validators: map[string][]func(any, any) (any, error){}, ClassVars: map[string]any{}, PrivateDefaults: map[string]func() any{}, FieldDefaults: map[string]any{}, Extra: "ignore"}
	if proto != nil {
		c.typ = reflect.TypeOf(proto)
		if c.typ.Kind() != reflect.Ptr || c.typ.Elem().Kind() != reflect.Struct {
			panic("py.NewClass: proto must be a pointer to struct")
		}
		t := c.typ
		c.New = func() any {
			v := reflect.New(t.Elem()).Interface()
			if o, ok := v.(objectBinder); ok {
				o.bind(c, v)
			}
			return v
		}
	}
	for _, b := range bases {
		b.mu.Lock()
		b.subs = append(b.subs, c)
		b.mu.Unlock()
	}
	classesMu.Lock()
	classes[name] = c
	classesMu.Unlock()
	return c
}

// module_of_caller_ derives the Python module of a class from the flat
// package file that declares it: ``laila-go/atomic.types.atomic_dict.go``
// is ``laila/atomic/types/atomic_dict.py`` so the class lives in
// ``laila.atomic.types.atomic_dict`` (``laila.go`` itself is ``laila``).
// Classes declared elsewhere (tests, examples) get the empty module and
// pickle under ``__main__``.
func module_of_caller_() string {
	_, file, _, ok := runtime.Caller(2)
	if !ok {
		return ""
	}
	dir, base := filepath.Split(file)
	if !strings.HasSuffix(dir, "/laila-go/") || !strings.HasSuffix(base, ".go") || strings.HasSuffix(base, "_test.go") {
		return ""
	}
	mod := strings.TrimSuffix(base, ".go")
	if mod == "laila" {
		return "laila"
	}
	return "laila." + mod
}

// Class_by_name looks a class up by its Python name.
func Class_by_name(name string) (*Class, bool) {
	classesMu.RLock()
	defer classesMu.RUnlock()
	c, ok := classes[name]
	return c, ok
}

// All_classes returns every registered class.
func All_classes() []*Class {
	classesMu.RLock()
	defer classesMu.RUnlock()
	out := make([]*Class, 0, len(classes))
	for _, c := range classes {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Subclasses is cls.__subclasses__() (direct subclasses, declaration order).
func (c *Class) Subclasses() []*Class {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*Class(nil), c.subs...)
}

// All_subclasses walks the subclass tree recursively (depth-first).
func (c *Class) All_subclasses() []*Class {
	var out []*Class
	for _, s := range c.Subclasses() {
		out = append(out, s)
		out = append(out, s.All_subclasses()...)
	}
	return out
}

// Mro returns the C3 linearisation of the class.
func (c *Class) Mro() []*Class {
	c.mu.Lock()
	if c.mro != nil {
		defer c.mu.Unlock()
		return c.mro
	}
	c.mu.Unlock()
	seqs := [][]*Class{}
	for _, b := range c.Bases {
		seqs = append(seqs, append([]*Class(nil), b.Mro()...))
	}
	seqs = append(seqs, append([]*Class(nil), c.Bases...))
	res := []*Class{c}
	for {
		nonEmpty := false
		for _, s := range seqs {
			if len(s) > 0 {
				nonEmpty = true
				break
			}
		}
		if !nonEmpty {
			break
		}
		var cand *Class
		for _, s := range seqs {
			if len(s) == 0 {
				continue
			}
			head := s[0]
			ok := true
			for _, o := range seqs {
				if len(o) == 0 {
					continue
				}
				for _, x := range o[1:] {
					if x == head {
						ok = false
						break
					}
				}
				if !ok {
					break
				}
			}
			if ok {
				cand = head
				break
			}
		}
		if cand == nil {
			panic(TypeError.Newf("Cannot create a consistent method resolution order (MRO) for %s", c.Name))
		}
		res = append(res, cand)
		for i, s := range seqs {
			if len(s) > 0 && s[0] == cand {
				seqs[i] = s[1:]
			}
		}
	}
	c.mu.Lock()
	c.mro = res
	c.mu.Unlock()
	return res
}

// Issubclass is issubclass(c, base).
func (c *Class) Issubclass(base *Class) bool {
	for _, m := range c.Mro() {
		if m == base {
			return true
		}
	}
	return false
}

// Type returns the Go pointer type of instances.
func (c *Class) Type() reflect.Type { return c.typ }

// Repr is <class 'module.Name'>.
func (c *Class) Repr() string {
	if c.Module != "" {
		return "<class '" + c.Module + "." + c.Name + "'>"
	}
	return "<class '" + c.Name + "'>"
}

// Add_before registers a model_validator(mode="before").
func (c *Class) Add_before(fn func(cls *Class, data Kw) (Kw, error)) *Class {
	c.Before = append(c.Before, fn)
	return c
}

// Add_after registers a model_validator(mode="after").
func (c *Class) Add_after(fn func(self any) error) *Class {
	c.After = append(c.After, fn)
	return c
}

// Add_field_validator registers a field_validator(name).
func (c *Class) Add_field_validator(field string, fn func(self any, v any) (any, error)) *Class {
	c.Validators[field] = append(c.Validators[field], fn)
	return c
}

// Set_classvar sets a class attribute.
func (c *Class) Set_classvar(name string, v any) *Class { c.ClassVars[name] = v; return c }

// Set_field_default is a subclass redeclaring an inherited field with a
// new default -- ``stream_chunk_bytes: int = Field(default=4096)`` on a
// class whose base declares ``Field(default=65536)``. The field keeps its
// single storage slot in the base struct (so base-class methods reading
// it directly see the subclass default); only the constructor default
// changes. Prefer this over redeclaring the Go field in the subclass
// struct, which would shadow the base slot.
func (c *Class) Set_field_default(name string, v any) *Class {
	c.mu.Lock()
	c.FieldDefaults[name] = v
	c.fieldsOK = false
	c.mu.Unlock()
	return c
}

// Classvar looks a class attribute up through the MRO.
func (c *Class) Classvar(name string) (any, bool) {
	for _, m := range c.Mro() {
		if v, ok := m.ClassVars[name]; ok {
			return v, true
		}
	}
	return nil, false
}

// Set_private_default declares `name: T = PrivateAttr(default_factory=fn)`
// for a private attribute whose Go field lives in a base struct (the
// factory wins over the base's own default, exactly like pydantic).
func (c *Class) Set_private_default(name string, fn func() any) *Class {
	c.PrivateDefaults[name] = fn
	return c
}

// Private_default is cls.__private_attributes__[name].default_factory
// looked up through the MRO.
func (c *Class) Private_default(name string) (func() any, bool) {
	for _, m := range c.Mro() {
		if fn, ok := m.PrivateDefaults[name]; ok {
			return fn, true
		}
	}
	return nil, false
}

// ---------------------------------------------------------------------------
// Object: the embedded header every translated class carries
// ---------------------------------------------------------------------------

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Object is embedded (once, in the root base) by every translated class.
// It stores the class descriptor and the "self" pointer used for virtual
// dispatch of hook methods from base-class code.
type Object struct {
	noCopy    noCopy
	cls       *Class
	self      any
	fieldsSet map[string]bool
	extra     *Dict
	privMu    sync.RWMutex // ``__dict__`` writes are GIL-atomic in CPython
	private   map[string]any
}

type objectBinder interface{ bind(cls *Class, self any) }

// copyHeaderFrom copies the class descriptor and bookkeeping of src into o
// without the self pointer (the copy rewires its own).
func (o *Object) copyHeaderFrom(src *Object) {
	o.cls = src.cls
	o.self = nil
	o.fieldsSet = make(map[string]bool, len(src.fieldsSet))
	for k, v := range src.fieldsSet {
		o.fieldsSet[k] = v
	}
	if src.extra != nil {
		o.extra = src.extra.Copy()
	}
	src.privMu.RLock()
	defer src.privMu.RUnlock()
	if src.private != nil {
		o.private = make(map[string]any, len(src.private))
		for k, v := range src.private {
			o.private[k] = v
		}
	}
}

func (o *Object) bind(cls *Class, self any) {
	o.cls, o.self = cls, self
	if o.fieldsSet == nil {
		o.fieldsSet = map[string]bool{}
	}
}

// Class_ returns type(self).
func (o *Object) Class_() *Class {
	if o.cls == nil {
		panic(RuntimeError.New("py.Object used before New_X()/py.Init() bound it"))
	}
	return o.cls
}

// Object_ returns the embedded py.Object itself (for weak references).
func (o *Object) Object_() *Object { return o }

// Self returns the outermost object (for virtual dispatch: self.(Hooks).X()).
func (o *Object) Self() any {
	if o.self == nil {
		panic(RuntimeError.New("py.Object used before New_X()/py.Init() bound it"))
	}
	return o.self
}

// Bound reports whether the object has been initialised.
func (o *Object) Bound() bool { return o.self != nil }

// Rewire_self_ re-binds self after a copy (used by Copy/Deepcopy/Model_copy).
func (o *Object) Rewire_self_(self any) { o.self = self }

// Fields_set is __pydantic_fields_set__.
func (o *Object) Fields_set() []string {
	out := make([]string, 0, len(o.fieldsSet))
	for k := range o.fieldsSet {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Mark_field_set records a field as explicitly provided.
func (o *Object) Mark_field_set(name string) {
	if o.fieldsSet == nil {
		o.fieldsSet = map[string]bool{}
	}
	o.fieldsSet[name] = true
}

// Extra is __pydantic_extra__ (nil unless extra="allow").
func (o *Object) Extra() *Dict { return o.extra }

// Private_get reads a dynamic private attribute (object.__setattr__ slots
// that are not struct fields).
func (o *Object) Private_get(name string) (any, bool) {
	o.privMu.RLock()
	v, ok := o.private[name]
	o.privMu.RUnlock()
	return v, ok
}

// Private_set writes a dynamic private attribute.
func (o *Object) Private_set(name string, v any) {
	o.privMu.Lock()
	if o.private == nil {
		o.private = map[string]any{}
	}
	o.private[name] = v
	o.privMu.Unlock()
}

// Private_del removes a dynamic private attribute; false when absent.
func (o *Object) Private_del(name string) bool {
	o.privMu.Lock()
	defer o.privMu.Unlock()
	if _, ok := o.private[name]; !ok {
		return false
	}
	delete(o.private, name)
	return true
}

// Isinstance is isinstance(x, cls) through the class table.
func Isinstance(x any, cls *Class) bool {
	if x == nil {
		return false
	}
	if o, ok := x.(interface{ Class_() *Class }); ok {
		defer func() { _ = recover() }()
		return o.Class_().Issubclass(cls)
	}
	return false
}

// Classof returns type(x) or nil.
func Classof(x any) *Class {
	if o, ok := x.(interface{ Class_() *Class }); ok {
		return o.Class_()
	}
	return nil
}

// ---------------------------------------------------------------------------
// fields
// ---------------------------------------------------------------------------

// FieldInfo describes one pydantic field.
type FieldInfo struct {
	Name       string // Python name
	GoName     string
	Alias      string
	Index      []int // reflect field index path
	Type       reflect.Type
	Required   bool
	HasDefault bool
	Default    any
	Factory    string // method name producing the default
	Exclude    bool
	Private    bool // PrivateAttr (leading underscore)
	ClassVar   bool
	Frozen     bool
	Ge, Gt     *float64
	Le, Lt     *float64
	MinLen     *int
	MaxLen     *int
	Owner      *Class
	// Shadowed lists the index paths of base-struct slots this field
	// redeclares (subclass override); Assign_fields writes them too.
	Shadowed [][]int
	// Json_schema_extra holds boolean markers from the tag (``cli_exempt``),
	// pydantic's Field(json_schema_extra={...}) extension point.
	Json_schema_extra map[string]any
}

func (c *Class) Fields() []*FieldInfo {
	c.mu.Lock()
	if c.fieldsOK {
		defer c.mu.Unlock()
		return c.fields
	}
	c.mu.Unlock()
	var fields []*FieldInfo
	if c.typ != nil {
		fields = dedupeFields(collectFields(c.typ.Elem(), nil, c))
	}
	// Subclass default overrides (Set_field_default), base-first so the
	// most-derived declaration wins.
	mro := c.Mro()
	for i := len(mro) - 1; i >= 0; i-- {
		m := mro[i]
		m.mu.Lock()
		overrides := m.FieldDefaults
		m.mu.Unlock()
		for name, v := range overrides {
			for _, f := range fields {
				if f.Name == name {
					f.HasDefault, f.Default, f.Factory, f.Required = true, v, "", false
				}
			}
		}
	}
	c.mu.Lock()
	c.fields, c.fieldsOK = fields, true
	c.mu.Unlock()
	return fields
}

var (
	objectType = reflect.TypeOf(Object{})
)

// dedupeFields applies pydantic's subclass-override rule: a field
// redeclared by a subclass (the outer struct, collected *after* the
// embedded base) replaces the base definition while keeping the base's
// position in field order.
func dedupeFields(fields []*FieldInfo) []*FieldInfo {
	pos := map[string]int{}
	out := make([]*FieldInfo, 0, len(fields))
	for _, f := range fields {
		if i, ok := pos[f.Name]; ok {
			// Keep the shadowed base slot(s) in sync on assignment so
			// base-class methods reading their own struct field see the
			// value the subclass declared.
			f.Shadowed = append(append([][]int{}, out[i].Shadowed...), out[i].Index)
			out[i] = f
			continue
		}
		pos[f.Name] = len(out)
		out = append(out, f)
	}
	return out
}

func collectFields(t reflect.Type, index []int, owner *Class) []*FieldInfo {
	var out []*FieldInfo
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		idx := append(append([]int(nil), index...), i)
		if f.Anonymous {
			if f.Type == objectType {
				continue
			}
			if f.Type.Kind() == reflect.Struct {
				out = append(out, collectFields(f.Type, idx, owner)...)
			}
			continue
		}
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("py")
		if tag == "-" {
			continue
		}
		fi := &FieldInfo{GoName: f.Name, Index: idx, Type: f.Type, Owner: owner}
		parts := strings.Split(tag, ",")
		if parts[0] != "" {
			fi.Name = parts[0]
		} else {
			fi.Name = Pyname(f.Name)
		}
		for _, p := range parts[1:] {
			switch {
			case p == "required":
				fi.Required = true
			case p == "exclude":
				fi.Exclude = true
			case p == "private":
				fi.Private = true
			case p == "classvar":
				fi.ClassVar = true
			case p == "frozen":
				fi.Frozen = true
			case strings.HasPrefix(p, "alias="):
				fi.Alias = p[6:]
			case strings.HasPrefix(p, "factory="):
				fi.Factory = p[8:]
			case strings.HasPrefix(p, "ge="):
				fi.Ge = parseF(p[3:])
			case strings.HasPrefix(p, "gt="):
				fi.Gt = parseF(p[3:])
			case strings.HasPrefix(p, "le="):
				fi.Le = parseF(p[3:])
			case strings.HasPrefix(p, "lt="):
				fi.Lt = parseF(p[3:])
			case strings.HasPrefix(p, "min_length="):
				fi.MinLen = parseI(p[11:])
			case strings.HasPrefix(p, "max_length="):
				fi.MaxLen = parseI(p[11:])
			case p != "":
				if fi.Json_schema_extra == nil {
					fi.Json_schema_extra = map[string]any{}
				}
				fi.Json_schema_extra[p] = true
			}
		}
		if strings.HasPrefix(fi.Name, "_") {
			fi.Private = true
		}
		if d, ok := f.Tag.Lookup("default"); ok {
			fi.HasDefault = true
			fi.Default = parseDefault(d, f.Type)
		}
		if !fi.Required && !fi.HasDefault && fi.Factory == "" && !fi.Private {
			// pydantic: a field without default is required... unless Optional
			// (pointer) which defaults to None, or a tag said otherwise.
			switch f.Type.Kind() {
			case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice:
				fi.HasDefault = true
				fi.Default = nil
			default:
				fi.Required = true
			}
		}
		if fi.Required && (fi.HasDefault || fi.Factory != "") {
			fi.Required = false
		}
		out = append(out, fi)
	}
	return out
}

func parseF(s string) *float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		panic("py: bad numeric constraint " + s)
	}
	return &f
}

func parseI(s string) *int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic("py: bad int constraint " + s)
	}
	return &n
}

// parseDefault interprets a `default:"..."` tag as a JSON literal when
// possible, else as a plain string.
func parseDefault(s string, t reflect.Type) any {
	if t.Kind() == reflect.String {
		return s
	}
	if v, err := Json_loads(s); err == nil {
		return v
	}
	return s
}

// Field_by_name finds a field by Python name or alias.
func (c *Class) Field_by_name(name string) *FieldInfo {
	for _, f := range c.Fields() {
		if f.Name == name || (f.Alias != "" && f.Alias == name) {
			return f
		}
	}
	return nil
}

// Model_fields is cls.model_fields: public (non-private) fields.
func (c *Class) Model_fields() []*FieldInfo {
	var out []*FieldInfo
	for _, f := range c.Fields() {
		if !f.Private && !f.ClassVar {
			out = append(out, f)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Init: the pydantic constructor
// ---------------------------------------------------------------------------

// Post_initer is implemented by classes with model_post_init.
type Post_initer interface{ Model_post_init(ctx any) error }

// Validation_error builds a pydantic-style ValidationError.
func Validation_error(model string, lines ...string) error {
	n := len(lines)
	msg := fmt.Sprintf("%d validation error%s for %s\n%s", n, plural(n), model, strings.Join(lines, "\n"))
	return ValidationError.New(msg).With("errors", lines)
}

// Init runs the pydantic constructor on a freshly allocated instance:
// binds the Object header, applies before-validators, assigns fields with
// coercion/defaults/constraints, field and after validators, then the
// Model_post_init chain. Call it from every New_X constructor.
func Init(self any, cls *Class, kws ...Kw) error {
	if cls.Abstract {
		return TypeError.Newf("Can't instantiate abstract class %s", cls.Name)
	}
	if b, ok := self.(objectBinder); ok {
		b.bind(cls, self)
	}
	data := Kw{}
	for _, k := range kws {
		for n, v := range k {
			data[n] = v
		}
	}
	// before validators, base-first
	mro := cls.Mro()
	for i := len(mro) - 1; i >= 0; i-- {
		for _, fn := range mro[i].Before {
			var err error
			data, err = fn(cls, data)
			if err != nil {
				return err
			}
		}
	}
	if err := Assign_fields(self, cls, data, true); err != nil {
		return err
	}
	for i := len(mro) - 1; i >= 0; i-- {
		for _, fn := range mro[i].After {
			if err := fn(self); err != nil {
				return err
			}
		}
	}
	if p, ok := self.(Post_initer); ok {
		if err := p.Model_post_init(nil); err != nil {
			return err
		}
	}
	return nil
}

// Assign_fields validates and assigns fields from data (model_validate /
// model_construct when validate is false).
func Assign_fields(self any, cls *Class, data Kw, validate bool) error {
	rv := reflect.ValueOf(self).Elem()
	var errs []string
	consumed := map[string]bool{}
	obj, _ := self.(interface {
		Mark_field_set(string)
		bind(*Class, any)
	})
	for _, f := range cls.Fields() {
		fv := rv.FieldByIndex(f.Index)
		if f.ClassVar {
			continue
		}
		if len(f.Shadowed) > 0 {
			// subclass redeclared a base field: mirror the final value
			// into the shadowed base slot(s) once this field is settled.
			defer func(f *FieldInfo, fv reflect.Value) {
				for _, idx := range f.Shadowed {
					bv := rv.FieldByIndex(idx)
					if bv.CanSet() && fv.Type().AssignableTo(bv.Type()) {
						bv.Set(fv)
					}
				}
			}(f, fv)
		}
		if f.Private {
			// PrivateAttr: default/factory only; never from data
			if fn, ok := cls.Private_default(f.Name); ok {
				assignValue(fv, fn())
				continue
			}
			if f.Factory != "" {
				if m := reflect.ValueOf(self).MethodByName(f.Factory); m.IsValid() {
					out := m.Call(nil)
					if len(out) > 0 {
						assignValue(fv, out[0].Interface())
					}
				}
			} else if f.HasDefault && f.Default != nil {
				assignValue(fv, f.Default)
			}
			continue
		}
		raw, ok := data[f.Name]
		if !ok && f.Alias != "" {
			raw, ok = data[f.Alias]
			if ok {
				consumed[f.Alias] = true
			}
		} else if ok {
			consumed[f.Name] = true
		}
		if !ok {
			if f.Factory != "" {
				if m := reflect.ValueOf(self).MethodByName(f.Factory); m.IsValid() {
					out := m.Call(nil)
					if len(out) > 0 {
						assignValue(fv, out[0].Interface())
					}
				} else {
					errs = append(errs, fmt.Sprintf("%s\n  default factory %s not found", f.Name, f.Factory))
				}
				continue
			}
			if f.HasDefault {
				if f.Default != nil {
					if v, err := Coerce(f.Default, f.Type); err == nil {
						fv.Set(v)
					} else {
						assignValue(fv, f.Default)
					}
				}
				continue
			}
			if f.Required {
				errs = append(errs, fmt.Sprintf("%s\n  Field required [type=missing, input_value=%s, input_type=dict]", f.Name, Repr(DictFromKw(data))))
			}
			continue
		}
		if obj != nil {
			obj.Mark_field_set(f.Name)
		}
		val := raw
		if validate {
			for _, m := range cls.Mro() {
				for _, vfn := range m.Validators[f.Name] {
					var err error
					val, err = vfn(self, val)
					if err != nil {
						errs = append(errs, fmt.Sprintf("%s\n  Value error, %s [type=value_error, input_value=%s, input_type=%s]", f.Name, err.Error(), Repr(raw), Pytypename(raw)))
						continue
					}
				}
			}
			cv, err := Coerce(val, f.Type)
			if err != nil {
				errs = append(errs, fmt.Sprintf("%s\n  %s [type=%s, input_value=%s, input_type=%s]", f.Name, err.Error(), coerceCode(f.Type), Repr(raw), Pytypename(raw)))
				continue
			}
			if msg := checkConstraints(f, cv); msg != "" {
				errs = append(errs, fmt.Sprintf("%s\n  %s [input_value=%s, input_type=%s]", f.Name, msg, Repr(raw), Pytypename(raw)))
				continue
			}
			fv.Set(cv)
		} else {
			assignValue(fv, val)
		}
	}
	// extra keys
	if validate && cls.Extra != "ignore" {
		var extras []string
		for k := range data {
			if !consumed[k] {
				extras = append(extras, k)
			}
		}
		sort.Strings(extras)
		if cls.Extra == "forbid" {
			for _, k := range extras {
				errs = append(errs, fmt.Sprintf("%s\n  Extra inputs are not permitted [type=extra_forbidden, input_value=%s, input_type=%s]", k, Repr(data[k]), Pytypename(data[k])))
			}
		} else if cls.Extra == "allow" {
			if o, ok := self.(interface{ setExtra(*Dict) }); ok {
				d := NewDict()
				for _, k := range extras {
					d.Set(k, data[k])
				}
				o.setExtra(d)
			}
		}
	}
	if len(errs) > 0 {
		return Validation_error(cls.Name, errs...)
	}
	return nil
}

func (o *Object) setExtra(d *Dict) { o.extra = d }

func assignValue(fv reflect.Value, v any) {
	if v == nil {
		fv.Set(reflect.Zero(fv.Type()))
		return
	}
	cv, err := Coerce(v, fv.Type())
	if err == nil {
		fv.Set(cv)
		return
	}
	rv := reflect.ValueOf(v)
	if rv.Type().AssignableTo(fv.Type()) {
		fv.Set(rv)
	} else if rv.Type().ConvertibleTo(fv.Type()) {
		fv.Set(rv.Convert(fv.Type()))
	}
}

func checkConstraints(f *FieldInfo, v reflect.Value) string {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	num := func() (float64, bool) {
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return float64(v.Int()), true
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return float64(v.Uint()), true
		case reflect.Float32, reflect.Float64:
			return v.Float(), true
		}
		return 0, false
	}
	if x, ok := num(); ok {
		if f.Ge != nil && x < *f.Ge {
			return fmt.Sprintf("Input should be greater than or equal to %s", Repr(*f.Ge))
		}
		if f.Gt != nil && x <= *f.Gt {
			return fmt.Sprintf("Input should be greater than %s", Repr(*f.Gt))
		}
		if f.Le != nil && x > *f.Le {
			return fmt.Sprintf("Input should be less than or equal to %s", Repr(*f.Le))
		}
		if f.Lt != nil && x >= *f.Lt {
			return fmt.Sprintf("Input should be less than %s", Repr(*f.Lt))
		}
	}
	if f.MinLen != nil || f.MaxLen != nil {
		if n, err := Len(v.Interface()); err == nil {
			if f.MinLen != nil && n < *f.MinLen {
				return fmt.Sprintf("Value should have at least %d item%s after validation, not %d", *f.MinLen, plural(*f.MinLen), n)
			}
			if f.MaxLen != nil && n > *f.MaxLen {
				return fmt.Sprintf("Value should have at most %d item%s after validation, not %d", *f.MaxLen, plural(*f.MaxLen), n)
			}
		}
	}
	return ""
}

func coerceCode(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "string_type"
	case reflect.Bool:
		return "bool_type"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int_type"
	case reflect.Float32, reflect.Float64:
		return "float_type"
	case reflect.Slice:
		return "list_type"
	case reflect.Map:
		return "dict_type"
	case reflect.Ptr:
		return "model_type"
	}
	return "type_error"
}

// ---------------------------------------------------------------------------
// Coerce: pydantic lax-mode conversion into a Go type
// ---------------------------------------------------------------------------

var (
	anyType    = reflect.TypeOf((*any)(nil)).Elem()
	dictType   = reflect.TypeOf((*Dict)(nil))
	listType   = reflect.TypeOf((*List)(nil))
	setType    = reflect.TypeOf((*Set)(nil))
	tupleType  = reflect.TypeOf(Tuple(nil))
	kwType     = reflect.TypeOf(Kw(nil))
	bytesType  = reflect.TypeOf([]byte(nil))
	bigIntType = reflect.TypeOf((*big.Int)(nil))
	errorType  = reflect.TypeOf((*error)(nil)).Elem()
)

// Coerce converts v into type t with pydantic's lax rules.
func Coerce(v any, t reflect.Type) (reflect.Value, error) {
	if t == anyType {
		return reflect.ValueOf(&v).Elem(), nil
	}
	if v == nil {
		switch t.Kind() {
		case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
			return reflect.Zero(t), nil
		}
		return reflect.Value{}, fmt.Errorf("Input should be a valid %s", pyTypeOfGo(t))
	}
	rv := reflect.ValueOf(v)
	if rv.Type() == t {
		return rv, nil
	}
	switch t.Kind() {
	case reflect.Ptr:
		if rv.Type().AssignableTo(t) {
			return rv, nil
		}
		if t == dictType || t == listType || t == setType || t == bigIntType {
			return coerceContainer(v, t)
		}
		if t.Elem().Kind() == reflect.Struct {
			// nested model from dict / Kw
			if cls := classForType(t); cls != nil {
				var kw Kw
				switch x := v.(type) {
				case *Dict:
					kw = x.ToKw()
				case Kw:
					kw = x
				case map[string]any:
					kw = Kw(x)
				default:
					return reflect.Value{}, fmt.Errorf("Input should be a valid dictionary or instance of %s", cls.Name)
				}
				inst := cls.New()
				if err := Init(inst, cls, kw); err != nil {
					return reflect.Value{}, err
				}
				return reflect.ValueOf(inst), nil
			}
		}
		if rv.Kind() != reflect.Ptr {
			ev, err := Coerce(v, t.Elem())
			if err != nil {
				return reflect.Value{}, err
			}
			p := reflect.New(t.Elem())
			p.Elem().Set(ev)
			return p, nil
		}
	case reflect.Interface:
		if rv.Type().Implements(t) {
			return rv.Convert(t), nil
		}
		return reflect.Value{}, fmt.Errorf("Input should be a valid %s", t.Name())
	case reflect.String:
		switch x := v.(type) {
		case string:
			return reflect.ValueOf(x).Convert(t), nil
		case []byte:
			return reflect.ValueOf(string(x)).Convert(t), nil
		case Stringer:
			if _, isEnum := v.(interface{ Pyvalue() any }); isEnum {
				return reflect.ValueOf(x.String()).Convert(t), nil
			}
		}
		if rv.Kind() == reflect.String {
			return rv.Convert(t), nil
		}
		return reflect.Value{}, fmt.Errorf("Input should be a valid string")
	case reflect.Bool:
		switch x := v.(type) {
		case bool:
			return reflect.ValueOf(x).Convert(t), nil
		case string:
			switch strings.ToLower(x) {
			case "true", "1", "yes", "y", "on", "t":
				return reflect.ValueOf(true).Convert(t), nil
			case "false", "0", "no", "n", "off", "f":
				return reflect.ValueOf(false).Convert(t), nil
			}
		}
		if k, err := keyOf(v); err == nil && k.kind == hkInt && (k.i == 0 || k.i == 1) {
			return reflect.ValueOf(k.i == 1).Convert(t), nil
		}
		return reflect.Value{}, fmt.Errorf("Input should be a valid boolean")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, ok := toInt64(v)
		if !ok {
			return reflect.Value{}, fmt.Errorf("Input should be a valid integer")
		}
		if t.Kind() != reflect.Int64 && reflect.Zero(t).OverflowInt(n) {
			return reflect.Value{}, fmt.Errorf("Input should be a valid integer")
		}
		return reflect.ValueOf(n).Convert(t), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, ok := toInt64(v)
		if !ok || n < 0 {
			if u, ok := v.(uint64); ok {
				return reflect.ValueOf(u).Convert(t), nil
			}
			return reflect.Value{}, fmt.Errorf("Input should be a valid integer")
		}
		return reflect.ValueOf(uint64(n)).Convert(t), nil
	case reflect.Float32, reflect.Float64:
		switch x := v.(type) {
		case float64:
			return reflect.ValueOf(x).Convert(t), nil
		case float32:
			return reflect.ValueOf(float64(x)).Convert(t), nil
		case string:
			f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
			if err == nil {
				return reflect.ValueOf(f).Convert(t), nil
			}
		}
		if n, ok := toInt64(v); ok {
			return reflect.ValueOf(float64(n)).Convert(t), nil
		}
		return reflect.Value{}, fmt.Errorf("Input should be a valid number")
	case reflect.Slice:
		if t == bytesType {
			switch x := v.(type) {
			case []byte:
				return reflect.ValueOf(x), nil
			case string:
				return reflect.ValueOf([]byte(x)), nil
			}
			return reflect.Value{}, fmt.Errorf("Input should be a valid bytes")
		}
		var items []any
		switch v.(type) {
		case *List, []any, Tuple, *Set, []string:
			items = Iter(v)
		default:
			if rv.Kind() == reflect.Slice {
				items = Iter(v)
			} else {
				return reflect.Value{}, fmt.Errorf("Input should be a valid list")
			}
		}
		out := reflect.MakeSlice(t, len(items), len(items))
		for i, it := range items {
			ev, err := Coerce(it, t.Elem())
			if err != nil {
				return reflect.Value{}, fmt.Errorf("%d\n  %s", i, err.Error())
			}
			out.Index(i).Set(ev)
		}
		return out, nil
	case reflect.Map:
		if t == kwType {
			switch x := v.(type) {
			case Kw:
				return reflect.ValueOf(x), nil
			case *Dict:
				return reflect.ValueOf(x.ToKw()), nil
			case map[string]any:
				return reflect.ValueOf(Kw(x)), nil
			}
		}
		if t.Key().Kind() == reflect.String {
			var items []Item
			switch x := v.(type) {
			case *Dict:
				items = x.Items()
			case Kw:
				items = DictFromKw(x).Items()
			case map[string]any:
				items = DictFromMap(x).Items()
			default:
				if rv.Kind() == reflect.Map {
					for _, k := range rv.MapKeys() {
						items = append(items, Item{k.Interface(), rv.MapIndex(k).Interface()})
					}
				} else {
					return reflect.Value{}, fmt.Errorf("Input should be a valid dictionary")
				}
			}
			out := reflect.MakeMapWithSize(t, len(items))
			for _, it := range items {
				ev, err := Coerce(it.Value, t.Elem())
				if err != nil {
					return reflect.Value{}, fmt.Errorf("%s\n  %s", Str(it.Key), err.Error())
				}
				out.SetMapIndex(reflect.ValueOf(Str(it.Key)).Convert(t.Key()), ev)
			}
			return out, nil
		}
	case reflect.Struct:
		if rv.Type().ConvertibleTo(t) {
			return rv.Convert(t), nil
		}
	case reflect.Func:
		if rv.Type().AssignableTo(t) {
			return rv, nil
		}
		if rv.Type().ConvertibleTo(t) {
			return rv.Convert(t), nil
		}
	}
	if rv.Type().AssignableTo(t) {
		return rv, nil
	}
	if rv.Type().ConvertibleTo(t) && rv.Kind() == t.Kind() {
		return rv.Convert(t), nil
	}
	return reflect.Value{}, fmt.Errorf("Input should be a valid %s", pyTypeOfGo(t))
}

func coerceContainer(v any, t reflect.Type) (reflect.Value, error) {
	switch t {
	case dictType:
		switch x := v.(type) {
		case *Dict:
			return reflect.ValueOf(x), nil
		case Kw:
			return reflect.ValueOf(DictFromKw(x)), nil
		case map[string]any:
			return reflect.ValueOf(DictFromMap(x)), nil
		case *DotMap:
			return reflect.ValueOf(x.ToDict()), nil
		}
		return reflect.Value{}, fmt.Errorf("Input should be a valid dictionary")
	case listType:
		switch x := v.(type) {
		case *List:
			return reflect.ValueOf(x), nil
		case []any, Tuple, *Set, []string:
			return reflect.ValueOf(NewList(Iter(x)...)), nil
		}
		if reflect.ValueOf(v).Kind() == reflect.Slice {
			return reflect.ValueOf(NewList(Iter(v)...)), nil
		}
		return reflect.Value{}, fmt.Errorf("Input should be a valid list")
	case setType:
		switch x := v.(type) {
		case *Set:
			return reflect.ValueOf(x), nil
		case *List, []any, Tuple, []string:
			return reflect.ValueOf(NewSet(Iter(x)...)), nil
		}
		return reflect.Value{}, fmt.Errorf("Input should be a valid set")
	case bigIntType:
		if n, ok := toInt64(v); ok {
			return reflect.ValueOf(big.NewInt(n)), nil
		}
		return reflect.Value{}, fmt.Errorf("Input should be a valid integer")
	}
	return reflect.Value{}, fmt.Errorf("Input should be a valid %s", t.String())
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			f, ferr := strconv.ParseFloat(strings.TrimSpace(x), 64)
			if ferr == nil && f == float64(int64(f)) {
				return int64(f), true
			}
			return 0, false
		}
		return n, true
	case float64:
		if x == float64(int64(x)) {
			return int64(x), true
		}
		return 0, false
	case float32:
		if float64(x) == float64(int64(x)) {
			return int64(x), true
		}
		return 0, false
	case *big.Int:
		if x.IsInt64() {
			return x.Int64(), true
		}
		return 0, false
	}
	if k, err := keyOf(v); err == nil && k.kind == hkInt {
		return k.i, true
	}
	return 0, false
}

func pyTypeOfGo(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Slice:
		return "list"
	case reflect.Map:
		return "dictionary"
	}
	if c := classForType(t); c != nil {
		return c.Name
	}
	return t.String()
}

func classForType(t reflect.Type) *Class {
	classesMu.RLock()
	defer classesMu.RUnlock()
	for _, c := range classes {
		if c.typ == t {
			return c
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// model_dump / model_copy / getattr / setattr
// ---------------------------------------------------------------------------

// Dumper is implemented by values with a custom serialisation in dumps
// (pydantic field_serializer / model_serializer).
type Dumper interface{ Model_dump(kw ...Kw) *Dict }

// Model_dump is self.model_dump(mode="python"|"json", exclude_none=...).
func Model_dump(self any, kw ...Kw) *Dict {
	opts := Kw{}
	for _, k := range kw {
		for n, v := range k {
			opts[n] = v
		}
	}
	mode, _ := opts.Get("mode", "python").(string)
	excludeNone, _ := opts.Get("exclude_none", false).(bool)
	cls := Classof(self)
	out := NewDict()
	if cls == nil {
		return out
	}
	rv := reflect.ValueOf(self).Elem()
	for _, f := range cls.Fields() {
		if f.Private || f.ClassVar || f.Exclude {
			continue
		}
		v := rv.FieldByIndex(f.Index).Interface()
		if excludeNone && IsNone(v) {
			continue
		}
		out.Set(f.Name, dumpValue(v, mode, opts))
	}
	if o, ok := self.(interface{ Extra() *Dict }); ok {
		if e := o.Extra(); e != nil {
			for _, it := range e.Items() {
				out.Set(it.Key, dumpValue(it.Value, mode, opts))
			}
		}
	}
	return out
}

func dumpValue(v any, mode string, opts Kw) any {
	if IsNone(v) {
		return nil
	}
	switch x := v.(type) {
	case Dumper:
		return x.Model_dump(Kw{"mode": mode})
	case interface{ Class_() *Class }:
		return Model_dump(v, Kw{"mode": mode})
	case *Dict:
		out := NewDict()
		for _, it := range x.Items() {
			k := it.Key
			if mode == "json" {
				k = Str(k)
			}
			out.Set(k, dumpValue(it.Value, mode, opts))
		}
		return out
	case *DotMap:
		return dumpValue(x.ToDict(), mode, opts)
	case Kw:
		return dumpValue(DictFromKw(x), mode, opts)
	case map[string]any:
		return dumpValue(DictFromMap(x), mode, opts)
	case *List:
		out := NewList()
		for _, e := range x.Items() {
			out.Append(dumpValue(e, mode, opts))
		}
		return out
	case []any:
		return dumpValue(NewList(x...), mode, opts)
	case Tuple:
		if mode == "json" {
			return dumpValue(NewList([]any(x)...), mode, opts)
		}
		out := make(Tuple, len(x))
		for i, e := range x {
			out[i] = dumpValue(e, mode, opts)
		}
		return out
	case *Set:
		if mode == "json" {
			return dumpValue(NewList(x.Items()...), mode, opts)
		}
		return x.Copy()
	case []byte:
		if mode == "json" {
			return string(x)
		}
		return x
	case interface{ Pyvalue() any }:
		if mode == "json" {
			return x.Pyvalue()
		}
		return v
	case string, bool, int, int64, float64, int32, uint64, uint32, int8, int16, uint8, uint16, uint, float32, *big.Int:
		return v
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr:
		// Optional[T] fields are *T in the port: dump the pointee.
		if rv.IsNil() {
			return nil
		}
		return dumpValue(rv.Elem().Interface(), mode, opts)
	case reflect.Slice, reflect.Array:
		return dumpValue(Iter(v), mode, opts)
	case reflect.Map:
		if rv.Type().Key().Kind() == reflect.String {
			d := NewDict()
			keys := rv.MapKeys()
			sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
			for _, k := range keys {
				d.Set(k.String(), dumpValue(rv.MapIndex(k).Interface(), mode, opts))
			}
			return d
		}
	case reflect.String:
		return rv.String()
	}
	if mode == "json" {
		return Str(v)
	}
	return v
}

// Model_copy is self.model_copy(update=..., deep=...): rebinds self.
func Model_copy(self any, kw ...Kw) any {
	opts := Kw{}
	for _, k := range kw {
		for n, v := range k {
			opts[n] = v
		}
	}
	deep, _ := opts.Get("deep", false).(bool)
	var cp any
	if deep {
		cp = Deepcopy(self)
	} else {
		rv := reflect.ValueOf(self).Elem()
		out := reflect.New(rv.Type())
		copyStruct(out.Elem(), rv, &Memo{m: map[uintptr]any{}})
		cp = out.Interface()
	}
	if o, ok := cp.(interface{ Rewire_self_(any) }); ok {
		o.Rewire_self_(cp)
	}
	if upd, ok := opts["update"]; ok && upd != nil {
		var u Kw
		switch x := upd.(type) {
		case Kw:
			u = x
		case *Dict:
			u = x.ToKw()
		case map[string]any:
			u = Kw(x)
		}
		for n, v := range u {
			_ = Setattr(cp, n, v)
		}
	}
	return cp
}

// Model_construct builds an instance without validation.
func Model_construct(cls *Class, kw ...Kw) (any, error) {
	inst := cls.New()
	data := Kw{}
	for _, k := range kw {
		for n, v := range k {
			data[n] = v
		}
	}
	if err := Assign_fields(inst, cls, data, false); err != nil {
		return nil, err
	}
	return inst, nil
}

// Model_validate is cls.model_validate(data).
func Model_validate(cls *Class, data any) (any, error) {
	var kw Kw
	switch x := data.(type) {
	case Kw:
		kw = x
	case *Dict:
		kw = x.ToKw()
	case map[string]any:
		kw = Kw(x)
	default:
		if Isinstance(data, cls) {
			return data, nil
		}
		return nil, Validation_error(cls.Name, fmt.Sprintf("  Input should be a valid dictionary or instance of %s [type=model_type, input_value=%s, input_type=%s]", cls.Name, Repr(data), Pytypename(data)))
	}
	inst := cls.New()
	if err := Init(inst, cls, kw); err != nil {
		return nil, err
	}
	return inst, nil
}

// Getattributer is implemented by classes overriding __getattribute__:
// after the normal lookup resolved attr, the hook may replace it (for
// example wrap a bound method in a gate) or raise.
type Getattributer interface {
	Getattribute(name string, attr any) (any, error)
}

// Getattr is getattr(self, name) by Python name: a field, a private attr,
// a zero-argument property getter, a bound method, or a class attribute;
// AttributeError otherwise.
func Getattr(self any, name string) (any, error) {
	attr, err := getattr(self, name)
	if err != nil {
		if g, ok := self.(Getattrer); ok && errors.Is(err, AttributeError) {
			return g.Getattr(name)
		}
		return nil, err
	}
	if g, ok := self.(Getattributer); ok {
		return g.Getattribute(name, attr)
	}
	return attr, nil
}

func getattr(self any, name string) (any, error) {
	if self == nil {
		return nil, AttributeError.Newf("'NoneType' object has no attribute '%s'", name)
	}
	rv := reflect.ValueOf(self)
	cls := Classof(self)
	if cls != nil {
		if f := cls.Field_by_name(name); f != nil {
			return rv.Elem().FieldByIndex(f.Index).Interface(), nil
		}
		if o, ok := self.(interface{ Private_get(string) (any, bool) }); ok {
			if v, ok := o.Private_get(name); ok {
				return v, nil
			}
		}
	}
	goName := Goname(name)
	// struct field by Go name (non-class structs)
	if rv.Kind() == reflect.Ptr && rv.Elem().Kind() == reflect.Struct {
		if fv := rv.Elem().FieldByName(goName); fv.IsValid() && fv.CanInterface() {
			return fv.Interface(), nil
		}
	}
	if m := rv.MethodByName(goName); m.IsValid() {
		mt := m.Type()
		// property: zero-arg method returning one value (or value, error)
		if mt.NumIn() == 0 && (mt.NumOut() == 1 || (mt.NumOut() == 2 && mt.Out(1) == errorType)) && isPropertyName(name) {
			out := m.Call(nil)
			if len(out) == 2 && !out[1].IsNil() {
				return nil, out[1].Interface().(error)
			}
			return out[0].Interface(), nil
		}
		return &BoundMethod{Name: name, Recv: self, fn: m}, nil
	}
	if cls != nil {
		if v, ok := cls.Classvar(name); ok {
			return v, nil
		}
	}
	// *Dict-like objects: attribute lookup falls through to keys for DotMap
	if dm, ok := self.(*DotMap); ok {
		if v, found := dm.Lookup(name); found {
			return v, nil
		}
		return dm.Get(name), nil
	}
	tn := Pytypename(self)
	return nil, AttributeError.Newf("'%s' object has no attribute '%s'", tn, name)
}

// isPropertyName decides whether a zero-arg method is a property (Python
// `@property`) or a method called with no arguments. The port marks
// properties by registering them; absent registration, lowercase Python
// names that do not look like verbs are treated as properties only when
// listed here by the translated classes.
func isPropertyName(name string) bool {
	propsMu.RLock()
	defer propsMu.RUnlock()
	return properties[name]
}

var (
	propsMu    sync.RWMutex
	properties = map[string]bool{}
)

// Register_property declares that Python attribute `name` is a @property
// everywhere in the port (so Getattr calls X() instead of returning a
// bound method).
func Register_property(names ...string) {
	propsMu.Lock()
	defer propsMu.Unlock()
	for _, n := range names {
		properties[n] = true
	}
}

// BoundMethod is a method retrieved through Getattr.
type BoundMethod struct {
	Name string
	Recv any
	fn   reflect.Value
}

// Call invokes the method with positional args and a trailing Kw when the
// method accepts variadic ...any (the port's convention), binding
// positional-only Go parameters by position otherwise.
//
// Go reflection carries no parameter names, so a Python caller passing a
// fixed parameter by keyword (``route(entries=[...])``, as RPC frames from
// a Python peer do) can only be bound when the method's parameter names
// were declared with Register_params; the registered names are then
// pulled out of kw into their positional slots before the call.
func (b *BoundMethod) Call(args []any, kw Kw) (any, error) {
	args, kw = bindRegisteredParams(b, args, kw)
	return Call_reflect(b.fn, args, kw)
}

var (
	paramsMu sync.RWMutex
	params   = map[string][]string{}
)

// Register_params declares the positional parameter names of Python
// method ``name`` (the fixed Go parameters, in order) so keyword calls
// bind like Python's. One declaration serves every class whose Go method
// of that name shares the signature; a class-specific declaration uses
// ``"ClassName.name"`` and wins over the bare name.
func Register_params(name string, param_names ...string) {
	paramsMu.Lock()
	params[name] = param_names
	paramsMu.Unlock()
}

func registeredParams(recv any, name string) []string {
	paramsMu.RLock()
	defer paramsMu.RUnlock()
	if cls := Classof(recv); cls != nil {
		for _, m := range cls.Mro() {
			if p, ok := params[m.Name+"."+name]; ok {
				return p
			}
		}
	}
	return params[name]
}

// bindRegisteredParams moves keyword arguments whose names are registered
// positional parameters into args (Python's call binding).
func bindRegisteredParams(b *BoundMethod, args []any, kw Kw) ([]any, Kw) {
	if len(kw) == 0 {
		return args, kw
	}
	names := registeredParams(b.Recv, b.Name)
	if len(names) == 0 || len(args) >= len(names) {
		return args, kw
	}
	hit := false
	for _, n := range names[len(args):] {
		if _, ok := kw[n]; ok {
			hit = true
			break
		}
	}
	if !hit {
		return args, kw
	}
	out := append([]any(nil), args...)
	rest := Kw{}
	for k, v := range kw {
		rest[k] = v
	}
	for i := len(args); i < len(names); i++ {
		v, ok := rest[names[i]]
		if !ok {
			break
		}
		out = append(out, v)
		delete(rest, names[i])
	}
	return out, rest
}

// Call_reflect calls fn with Python-style (args, kwargs). Fixed Go
// parameters are filled from args (coerced); a trailing variadic ...any
// receives the remaining args and the Kw (when non-empty). Results follow
// the port's conventions: (), (T), (error), (T, error).
func Call_reflect(fn reflect.Value, args []any, kw Kw) (any, error) {
	ft := fn.Type()
	nFixed := ft.NumIn()
	variadic := ft.IsVariadic()
	if variadic {
		nFixed--
	}
	in := make([]reflect.Value, 0, nFixed+len(args)+1)
	for i := 0; i < nFixed; i++ {
		if i >= len(args) {
			// try keyword by parameter position name? Go has no names; a
			// missing fixed arg is a TypeError like Python's.
			return nil, TypeError.Newf("%s() missing %d required positional argument(s)", ft.String(), nFixed-len(args))
		}
		cv, err := Coerce(args[i], ft.In(i))
		if err != nil {
			return nil, TypeError.Newf("argument %d: %s", i+1, err.Error())
		}
		in = append(in, cv)
	}
	rest := args[min(nFixed, len(args)):]
	if variadic {
		elem := ft.In(ft.NumIn() - 1).Elem()
		for _, a := range rest {
			cv, err := Coerce(a, elem)
			if err != nil {
				return nil, TypeError.New(err.Error())
			}
			in = append(in, cv)
		}
		if len(kw) > 0 {
			if elem == anyType || reflect.TypeOf(kw).AssignableTo(elem) {
				in = append(in, reflect.ValueOf(kw))
			} else {
				return nil, TypeError.Newf("got unexpected keyword arguments")
			}
		}
	} else {
		if len(rest) > 0 {
			return nil, TypeError.Newf("takes %d positional argument%s but %d were given", nFixed, plural(nFixed), len(args))
		}
		if len(kw) > 0 {
			name := ""
			for n := range kw {
				if name == "" || n < name {
					name = n
				}
			}
			return nil, TypeError.Newf("got an unexpected keyword argument '%s'", name)
		}
	}
	var out []reflect.Value
	var perr error
	func() {
		defer Recover(&perr)
		out = fn.Call(in)
	}()
	if perr != nil {
		return nil, perr
	}
	switch len(out) {
	case 0:
		return nil, nil
	case 1:
		if ft.Out(0) == errorType {
			if out[0].IsNil() {
				return nil, nil
			}
			return nil, out[0].Interface().(error)
		}
		return out[0].Interface(), nil
	default:
		var err error
		last := out[len(out)-1]
		if ft.Out(len(out)-1) == errorType && !last.IsNil() {
			err = last.Interface().(error)
		}
		if len(out) == 2 && ft.Out(1) == errorType {
			return out[0].Interface(), err
		}
		vals := make(Tuple, 0, len(out))
		for i, o := range out {
			if i == len(out)-1 && ft.Out(i) == errorType {
				break
			}
			vals = append(vals, o.Interface())
		}
		return vals, err
	}
}

// Getattrer is implemented by classes defining ``__getattr__``: consulted
// only when the normal lookup fails.
type Getattrer interface {
	Getattr(name string) (any, error)
}

// Setattrer is implemented by classes overriding ``__setattr__`` for
// dynamic (non-field) names; consulted before the generic fallbacks.
type Setattrer interface {
	Setattr(name string, value any) error
}

// Delattrer is implemented by classes defining ``__delattr__``.
type Delattrer interface {
	Delattr(name string) error
}

// Delattr is delattr(self, name).
func Delattr(self any, name string) error {
	if d, ok := self.(Delattrer); ok {
		return d.Delattr(name)
	}
	if o, ok := self.(interface{ Private_del(string) bool }); ok && o.Private_del(name) {
		return nil
	}
	return AttributeError.Newf("'%s' object has no attribute '%s'", Pytypename(self), name)
}

// Setattr is setattr(self, name, value) for fields (with coercion) and
// private attributes.
func Setattr(self any, name string, value any) error {
	cls := Classof(self)
	rv := reflect.ValueOf(self)
	if s, ok := self.(Setattrer); ok && (cls == nil || cls.Field_by_name(name) == nil) {
		return s.Setattr(name, value)
	}
	if cls != nil {
		if cls.Frozen {
			return ValidationError.Newf("1 validation error for %s\n%s\n  Instance is frozen [type=frozen_instance, input_value=%s, input_type=%s]", cls.Name, name, Repr(value), Pytypename(value))
		}
		if f := cls.Field_by_name(name); f != nil {
			fv := rv.Elem().FieldByIndex(f.Index)
			if f.Private {
				assignValue(fv, value)
				return nil
			}
			cv, err := Coerce(value, f.Type)
			if err != nil {
				assignValue(fv, value)
				return nil
			}
			fv.Set(cv)
			if o, ok := self.(interface{ Mark_field_set(string) }); ok {
				o.Mark_field_set(name)
			}
			return nil
		}
	}
	// property setter Set_x
	if m := rv.MethodByName("Set_" + strings.TrimPrefix(Goname(name), "")); m.IsValid() && m.Type().NumIn() == 1 {
		cv, err := Coerce(value, m.Type().In(0))
		if err != nil {
			return TypeError.New(err.Error())
		}
		out := m.Call([]reflect.Value{cv})
		if len(out) == 1 && !out[0].IsNil() {
			if e, ok := out[0].Interface().(error); ok {
				return e
			}
		}
		return nil
	}
	if rv.Kind() == reflect.Ptr && rv.Elem().Kind() == reflect.Struct {
		if fv := rv.Elem().FieldByName(Goname(name)); fv.IsValid() && fv.CanSet() {
			assignValue(fv, value)
			return nil
		}
	}
	if o, ok := self.(interface{ Private_set(string, any) }); ok && strings.HasPrefix(name, "_") {
		o.Private_set(name, value)
		return nil
	}
	return AttributeError.Newf("'%s' object has no attribute '%s'", Pytypename(self), name)
}

// Hasattr is hasattr(self, name).
func Hasattr(self any, name string) bool {
	_, err := Getattr(self, name)
	return err == nil
}

// Getattr_path walks a dotted attribute path (used by RPC dispatch).
func Getattr_path(self any, path []string) (any, error) {
	cur := self
	for _, seg := range path {
		v, err := Getattr(cur, seg)
		if err != nil {
			return nil, err
		}
		cur = v
	}
	return cur, nil
}

// New_instance is ``cls(**kwargs)`` for a registered class: allocate
// through the class descriptor and run the generated ``__init__``.
func New_instance(cls *Class, kwargs Kw) (any, error) {
	if cls == nil {
		return nil, RuntimeError.New("class not available")
	}
	inst := cls.New()
	if ctor, ok := inst.(interface{ Init_(Kw) error }); ok {
		if err := ctor.Init_(kwargs); err != nil {
			return nil, err
		}
		return inst, nil
	}
	if err := Init(inst, cls, kwargs); err != nil {
		return nil, err
	}
	return inst, nil
}

// Call is calling an arbitrary value with (args, kwargs): bound methods,
// Go funcs (through reflection) or objects with a Call method.
func Call(fn any, args []any, kw Kw) (any, error) {
	switch f := fn.(type) {
	case *BoundMethod:
		return f.Call(args, kw)
	case func() (any, error):
		return f()
	case func(args ...any) (any, error):
		if len(kw) > 0 {
			args = append(append([]any(nil), args...), kw)
		}
		return f(args...)
	case CoroutineFunc:
		if len(kw) > 0 {
			args = append(append([]any(nil), args...), kw)
		}
		return f(args...)
	case interface {
		Call(args []any, kw Kw) (any, error)
	}:
		return f.Call(args, kw)
	}
	rv := reflect.ValueOf(fn)
	if rv.Kind() == reflect.Func {
		return Call_reflect(rv, args, kw)
	}
	return nil, TypeError.Newf("'%s' object is not callable", Pytypename(fn))
}

// Callable is callable(v).
func Callable(v any) bool {
	switch v.(type) {
	case *BoundMethod, func() (any, error), func(args ...any) (any, error), CoroutineFunc:
		return true
	case interface {
		Call(args []any, kw Kw) (any, error)
	}:
		return true
	}
	return v != nil && reflect.ValueOf(v).Kind() == reflect.Func
}
