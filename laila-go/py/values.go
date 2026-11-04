package py

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Kw carries Python keyword arguments. By convention it is the last variadic
// argument of a translated function: laila.Memorize(e, py.Kw{"pool": "s3"}).
type Kw map[string]any

// Pop removes and returns kw[name] (or def when absent), like dict.pop.
func (k Kw) Pop(name string, def any) any {
	if v, ok := k[name]; ok {
		delete(k, name)
		return v
	}
	return def
}

// Get returns kw[name] or def.
func (k Kw) Get(name string, def any) any {
	if v, ok := k[name]; ok {
		return v
	}
	return def
}

// Has reports whether name was passed.
func (k Kw) Has(name string) bool { _, ok := k[name]; return ok }

// Ptr returns a pointer to v: the port spells ``T | None`` as *T, so
// Ptr("x") / Ptr(3) build the "present" case and nil is None.
func Ptr[T any](v T) *T { return &v }

// Deref returns *p, or def when p is nil (``p if p is not None else def``).
func Deref[T any](p *T, def T) T {
	if p == nil {
		return def
	}
	return *p
}

// Deref_any returns *p as any, or nil (None) when p is nil. It accepts the
// pointer-spelled optionals of the port (*string, *int, *float64, *bool,
// ...) so they can be handed to Repr / Str / Kw values verbatim.
func Deref_any(p any) any {
	if p == nil {
		return nil
	}
	rv := reflect.ValueOf(p)
	if rv.Kind() != reflect.Ptr {
		return p
	}
	if rv.IsNil() {
		return nil
	}
	return rv.Elem().Interface()
}

// Getitem is ``container[key]`` for any subscriptable value: *Dict, *List,
// Tuple, []any, Go maps keyed by string, Kw, and any type exposing a
// ``Getitem(key any) (any, error)`` method (AtomicDict, AtomicList, ...).
func Getitem(container any, key any) (any, error) {
	switch c := container.(type) {
	case *Dict:
		return c.Getitem(key)
	case Kw:
		if v, ok := c[Str(key)]; ok {
			return v, nil
		}
		return nil, KeyError.New(key)
	case map[string]any:
		if v, ok := c[Str(key)]; ok {
			return v, nil
		}
		return nil, KeyError.New(key)
	case *List:
		i, err := Int(key)
		if err != nil {
			return nil, TypeError.Newf("list indices must be integers or slices, not %s", Pytypename(key))
		}
		return c.Get(i)
	case Tuple:
		i, err := Int(key)
		if err != nil {
			return nil, TypeError.Newf("tuple indices must be integers or slices, not %s", Pytypename(key))
		}
		if i < 0 {
			i += len(c)
		}
		if i < 0 || i >= len(c) {
			return nil, IndexError.New("tuple index out of range")
		}
		return c[i], nil
	case []any:
		return Getitem(Tuple(c), key)
	case interface{ Getitem(key any) (any, error) }:
		return c.Getitem(key)
	}
	// Any other Go slice / array (``[]string``, ``[]int``, ...) is a Python
	// sequence: integer indexing with negative-index wraparound.
	if rv := reflect.ValueOf(container); rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
		i, err := Int(key)
		if err != nil {
			return nil, TypeError.Newf("list indices must be integers or slices, not %s", Pytypename(key))
		}
		if i < 0 {
			i += rv.Len()
		}
		if i < 0 || i >= rv.Len() {
			return nil, IndexError.New("list index out of range")
		}
		return rv.Index(i).Interface(), nil
	}
	return nil, TypeError.Newf("'%s' object is not subscriptable", Pytypename(container))
}

// MergeKw folds variadic keyword maps into one fresh map (later keys win),
// like Python's ``**data`` always binding a new dict; the constructors'
// `kw ...Kw` convention passes through here.
func MergeKw(kws ...Kw) Kw {
	out := Kw{}
	for _, k := range kws {
		for n, v := range k {
			out[n] = v
		}
	}
	return out
}

// Copy returns a shallow copy.
func (k Kw) Copy() Kw {
	out := make(Kw, len(k))
	for n, v := range k {
		out[n] = v
	}
	return out
}

// SplitKw separates a trailing Kw from a variadic ...any argument list,
// merging several Kw values if more than one was passed.
func SplitKw(args []any) ([]any, Kw) {
	var kw Kw
	pos := make([]any, 0, len(args))
	for _, a := range args {
		if k, ok := a.(Kw); ok {
			if kw == nil {
				kw = Kw{}
			}
			for n, v := range k {
				kw[n] = v
			}
			continue
		}
		pos = append(pos, a)
	}
	if kw == nil {
		kw = Kw{}
	}
	return pos, kw
}

// Tuple is an immutable, hashable Python tuple.
type Tuple []any

// Len returns len(t).
func (t Tuple) Len() int { return len(t) }

// Hashable is implemented by objects usable as dict keys / set members
// (Python __hash__/__eq__ on user classes).
type Hashable interface {
	Hash() uint64
	Eq(other any) bool
}

// Equaler is implemented by objects with a custom __eq__ only.
type Equaler interface{ Eq(other any) bool }

// ---------------------------------------------------------------------------
// hashing with Python key semantics
// ---------------------------------------------------------------------------

type hkind uint8

const (
	hkNone hkind = iota
	hkInt
	hkFloat
	hkStr
	hkBytes
	hkTuple
	hkObj
	hkBig
)

// hkey is the normalised, comparable form of a Python hashable value.
// Python guarantees hash(1) == hash(1.0) == hash(True), so all three
// normalise to hkInt; non-integral floats keep hkFloat.
type hkey struct {
	kind hkind
	i    int64
	u    uint64
	f    float64
	s    string
}

func keyOf(v any) (hkey, error) {
	switch x := v.(type) {
	case nil:
		return hkey{kind: hkNone}, nil
	case bool:
		if x {
			return hkey{kind: hkInt, i: 1}, nil
		}
		return hkey{kind: hkInt, i: 0}, nil
	case int:
		return hkey{kind: hkInt, i: int64(x)}, nil
	case int8:
		return hkey{kind: hkInt, i: int64(x)}, nil
	case int16:
		return hkey{kind: hkInt, i: int64(x)}, nil
	case int32:
		return hkey{kind: hkInt, i: int64(x)}, nil
	case int64:
		return hkey{kind: hkInt, i: x}, nil
	case uint:
		return keyOfUint(uint64(x)), nil
	case uint8:
		return hkey{kind: hkInt, i: int64(x)}, nil
	case uint16:
		return hkey{kind: hkInt, i: int64(x)}, nil
	case uint32:
		return hkey{kind: hkInt, i: int64(x)}, nil
	case uint64:
		return keyOfUint(x), nil
	case float32:
		return keyOfFloat(float64(x)), nil
	case float64:
		return keyOfFloat(x), nil
	case string:
		return hkey{kind: hkStr, s: x}, nil
	case []byte:
		return hkey{kind: hkBytes, s: string(x)}, nil
	case *big.Int:
		if x.IsInt64() {
			return hkey{kind: hkInt, i: x.Int64()}, nil
		}
		if x.IsUint64() {
			return keyOfUint(x.Uint64()), nil
		}
		return hkey{kind: hkBig, s: x.String()}, nil
	case Tuple:
		var sb strings.Builder
		for _, e := range x {
			k, err := keyOf(e)
			if err != nil {
				return hkey{}, err
			}
			sb.WriteString(fmt.Sprintf("%d:%d:%d:%v:%q;", k.kind, k.i, k.u, k.f, k.s))
		}
		return hkey{kind: hkTuple, s: sb.String()}, nil
	case Hashable:
		return hkey{kind: hkObj, u: x.Hash(), s: fmt.Sprintf("%T", x)}, nil
	case *List, *Dict, *Set:
		return hkey{}, TypeError.Newf("unhashable type: '%s'", Pytypename(v))
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		// identity hash for pointer objects without __hash__ (Python default)
		return hkey{kind: hkObj, u: uint64(rv.Pointer()), s: "ptr"}, nil
	}
	return hkey{}, TypeError.Newf("unhashable type: '%s'", Pytypename(v))
}

func keyOfUint(x uint64) hkey {
	if x <= math.MaxInt64 {
		return hkey{kind: hkInt, i: int64(x)}
	}
	return hkey{kind: hkBig, s: fmt.Sprint(x)}
}

func keyOfFloat(f float64) hkey {
	if f == math.Trunc(f) && !math.IsInf(f, 0) && math.Abs(f) < 1<<63 {
		return hkey{kind: hkInt, i: int64(f)}
	}
	if math.IsNaN(f) {
		return hkey{kind: hkFloat, f: 0, s: "nan"}
	}
	return hkey{kind: hkFloat, f: f}
}

// Hash returns a Python-compatible-in-spirit hash: equal values (1, 1.0,
// True) hash equal. Unhashable values return an error.
func Hash(v any) (uint64, error) {
	k, err := keyOf(v)
	if err != nil {
		return 0, err
	}
	h := uint64(14695981039346656037)
	mix := func(b byte) { h ^= uint64(b); h *= 1099511628211 }
	mix(byte(k.kind))
	for i := 0; i < 8; i++ {
		mix(byte(k.i >> (8 * i)))
		mix(byte(k.u >> (8 * i)))
	}
	bits := math.Float64bits(k.f)
	for i := 0; i < 8; i++ {
		mix(byte(bits >> (8 * i)))
	}
	for i := 0; i < len(k.s); i++ {
		mix(k.s[i])
	}
	return h, nil
}

// ---------------------------------------------------------------------------
// Dict: insertion-ordered, Python key semantics, per-operation atomicity
// ---------------------------------------------------------------------------

type dictEntry struct {
	key   any
	value any
	alive bool
}

// Dict is a Python dict. Every operation is atomic (the GIL guarantee the
// Python code relies on for shared dicts). Iteration helpers return
// snapshots, so callers may mutate while ranging over them.
type Dict struct {
	mu      sync.Mutex
	index   map[hkey][]int
	entries []dictEntry
	live    int
	head    int // every entry before head is dead (PopFirst cursor)
}

// NewDict creates an empty dict.
func NewDict() *Dict { return &Dict{index: map[hkey][]int{}} }

// DictOf builds a dict from alternating key, value pairs (insertion order kept).
func DictOf(kv ...any) *Dict {
	if len(kv)%2 != 0 {
		panic("py.DictOf: odd number of arguments")
	}
	d := NewDict()
	for i := 0; i < len(kv); i += 2 {
		d.Set(kv[i], kv[i+1])
	}
	return d
}

// DictFromMap converts a Go map into a Dict with keys in sorted order
// (Go maps are unordered; sorting keeps serialized bytes deterministic).
func DictFromMap(m map[string]any) *Dict {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	d := NewDict()
	for _, k := range keys {
		d.Set(k, m[k])
	}
	return d
}

// DictFromKw converts keyword arguments into a Dict with sorted keys.
func DictFromKw(k Kw) *Dict { return DictFromMap(map[string]any(k)) }

func (d *Dict) find(k hkey, key any) int {
	for _, i := range d.index[k] {
		e := &d.entries[i]
		if !e.alive {
			continue
		}
		if k.kind == hkObj {
			if h, ok := e.key.(Hashable); ok {
				if h.Eq(key) {
					return i
				}
				continue
			}
			if e.key == key {
				return i
			}
			continue
		}
		return i
	}
	return -1
}

func (d *Dict) compact() {
	if d.live*2 >= len(d.entries) || len(d.entries) < 32 {
		return
	}
	entries := make([]dictEntry, 0, d.live)
	index := make(map[hkey][]int, d.live)
	for _, e := range d.entries {
		if e.alive {
			k, _ := keyOf(e.key)
			index[k] = append(index[k], len(entries))
			entries = append(entries, e)
		}
	}
	d.entries, d.index = entries, index
	d.head = 0
}

// PopFirst removes and returns the *first* inserted (key, value) pair --
// ``k = next(iter(d)); v = d.pop(k)`` -- in amortised O(1): dead entries
// at the front are skipped once by the head cursor.
func (d *Dict) PopFirst() (any, any, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for d.head < len(d.entries) {
		i := d.head
		d.head++
		if d.entries[i].alive {
			e := d.entries[i]
			d.entries[i] = dictEntry{}
			d.live--
			d.compact()
			return e.key, e.value, nil
		}
	}
	return nil, nil, KeyError.New("popfirst(): dictionary is empty")
}

// Set is d[key] = value.
func (d *Dict) Set(key, value any) {
	k, err := keyOf(key)
	if err != nil {
		panic(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if i := d.find(k, key); i >= 0 {
		d.entries[i].value = value
		return
	}
	d.index[k] = append(d.index[k], len(d.entries))
	d.entries = append(d.entries, dictEntry{key: key, value: value, alive: true})
	d.live++
}

// Get returns (d[key], True) or (nil, False).
func (d *Dict) Get(key any) (any, bool) {
	k, err := keyOf(key)
	if err != nil {
		return nil, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if i := d.find(k, key); i >= 0 {
		return d.entries[i].value, true
	}
	return nil, false
}

// GetDefault is d.get(key, def).
func (d *Dict) GetDefault(key, def any) any {
	if v, ok := d.Get(key); ok {
		return v
	}
	return def
}

// Getitem is d[key], raising KeyError.
func (d *Dict) Getitem(key any) (any, error) {
	if v, ok := d.Get(key); ok {
		return v, nil
	}
	return nil, KeyError.New(key)
}

// Contains is `key in d`.
func (d *Dict) Contains(key any) bool { _, ok := d.Get(key); return ok }

// Del is `del d[key]`, raising KeyError.
func (d *Dict) Del(key any) error {
	_, err := d.Pop(key)
	return err
}

// Pop removes key and returns its value, raising KeyError.
func (d *Dict) Pop(key any) (any, error) {
	k, err := keyOf(key)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	i := d.find(k, key)
	if i < 0 {
		return nil, KeyError.New(key)
	}
	v := d.entries[i].value
	d.entries[i] = dictEntry{}
	d.live--
	d.compact()
	return v, nil
}

// PopDefault is d.pop(key, def).
func (d *Dict) PopDefault(key, def any) any {
	v, err := d.Pop(key)
	if err != nil {
		return def
	}
	return v
}

// Popitem removes and returns the last inserted (key, value) pair.
func (d *Dict) Popitem() (any, any, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := len(d.entries) - 1; i >= 0; i-- {
		if d.entries[i].alive {
			e := d.entries[i]
			d.entries[i] = dictEntry{}
			d.live--
			d.compact()
			return e.key, e.value, nil
		}
	}
	return nil, nil, KeyError.New("popitem(): dictionary is empty")
}

// SetDefault is d.setdefault(key, def).
func (d *Dict) SetDefault(key, def any) any {
	k, err := keyOf(key)
	if err != nil {
		panic(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if i := d.find(k, key); i >= 0 {
		return d.entries[i].value
	}
	d.index[k] = append(d.index[k], len(d.entries))
	d.entries = append(d.entries, dictEntry{key: key, value: def, alive: true})
	d.live++
	return def
}

// Len is len(d).
func (d *Dict) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.live
}

// Keys returns a snapshot of the keys in insertion order.
func (d *Dict) Keys() []any {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]any, 0, d.live)
	for _, e := range d.entries {
		if e.alive {
			out = append(out, e.key)
		}
	}
	return out
}

// StrKeys returns the keys as strings (non-string keys via Str).
func (d *Dict) StrKeys() []string {
	ks := d.Keys()
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = Str(k)
	}
	return out
}

// Values returns a snapshot of the values in insertion order.
func (d *Dict) Values() []any {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]any, 0, d.live)
	for _, e := range d.entries {
		if e.alive {
			out = append(out, e.value)
		}
	}
	return out
}

// Item is one (key, value) pair.
type Item struct{ Key, Value any }

// Items returns a snapshot of the (key, value) pairs in insertion order.
func (d *Dict) Items() []Item {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]Item, 0, d.live)
	for _, e := range d.entries {
		if e.alive {
			out = append(out, Item{e.key, e.value})
		}
	}
	return out
}

// Range calls fn for each pair of a snapshot until fn returns false.
func (d *Dict) Range(fn func(key, value any) bool) {
	for _, it := range d.Items() {
		if !fn(it.Key, it.Value) {
			return
		}
	}
}

// Clear removes everything.
func (d *Dict) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.index = map[hkey][]int{}
	d.entries = nil
	d.head = 0
	d.live = 0
}

// Update is d.update(other) for a Dict, Kw, map[string]any or []Item.
func (d *Dict) Update(other any) {
	switch o := other.(type) {
	case *Dict:
		for _, it := range o.Items() {
			d.Set(it.Key, it.Value)
		}
	case Kw:
		for _, it := range DictFromKw(o).Items() {
			d.Set(it.Key, it.Value)
		}
	case map[string]any:
		for _, it := range DictFromMap(o).Items() {
			d.Set(it.Key, it.Value)
		}
	case []Item:
		for _, it := range o {
			d.Set(it.Key, it.Value)
		}
	case nil:
	default:
		panic(TypeError.Newf("'%s' object is not iterable", Pytypename(other)))
	}
}

// Copy returns a shallow copy.
func (d *Dict) Copy() *Dict {
	out := NewDict()
	for _, it := range d.Items() {
		out.Set(it.Key, it.Value)
	}
	return out
}

// ToMap converts to a Go map with string keys (Str of each key).
func (d *Dict) ToMap() map[string]any {
	out := map[string]any{}
	for _, it := range d.Items() {
		out[Str(it.Key)] = it.Value
	}
	return out
}

// ToKw converts to keyword arguments.
func (d *Dict) ToKw() Kw { return Kw(d.ToMap()) }

// Eq implements Python dict equality.
func (d *Dict) Eq(other any) bool {
	o, ok := other.(*Dict)
	if !ok {
		if m, ok := other.(map[string]any); ok {
			o = DictFromMap(m)
		} else {
			return false
		}
	}
	if d == o {
		return true
	}
	a, b := d.Items(), o.Items()
	if len(a) != len(b) {
		return false
	}
	for _, it := range a {
		v, ok := o.Get(it.Key)
		if !ok || !Eq(it.Value, v) {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

// List is a Python list with per-operation atomicity.
type List struct {
	mu    sync.Mutex
	items []any
}

// NewList creates a list from items.
func NewList(items ...any) *List { return &List{items: append([]any(nil), items...)} }

// ListFrom creates a list from a slice (copied).
func ListFrom(items []any) *List { return NewList(items...) }

func (l *List) norm(i int, forInsert bool) (int, error) {
	n := len(l.items)
	if i < 0 {
		i += n
	}
	if forInsert {
		if i < 0 {
			i = 0
		}
		if i > n {
			i = n
		}
		return i, nil
	}
	if i < 0 || i >= n {
		return 0, IndexError.New("list index out of range")
	}
	return i, nil
}

// Append is l.append(v).
func (l *List) Append(v any) {
	l.mu.Lock()
	l.items = append(l.items, v)
	l.mu.Unlock()
}

// Extend is l.extend(iterable).
func (l *List) Extend(items any) {
	for _, v := range Iter(items) {
		l.Append(v)
	}
}

// Insert is l.insert(i, v).
func (l *List) Insert(i int, v any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	i, _ = l.norm(i, true)
	l.items = append(l.items, nil)
	copy(l.items[i+1:], l.items[i:])
	l.items[i] = v
}

// Get is l[i] with negative indices and IndexError.
func (l *List) Get(i int) (any, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	i, err := l.norm(i, false)
	if err != nil {
		return nil, err
	}
	return l.items[i], nil
}

// At is l[i], panicking on IndexError (for known-good indices).
func (l *List) At(i int) any { return Must(l.Get(i)) }

// Slice is a Python slice object: slice(start, stop, step) with None as nil.
type Slice struct{ Start, Stop, Step *int }

// NewSlice is slice(start, stop[, step]) with Go ints (no None).
func NewSlice(start, stop int, step ...int) Slice {
	s := Slice{Start: &start, Stop: &stop}
	if len(step) > 0 {
		st := step[0]
		s.Step = &st
	}
	return s
}

// Indices is slice.indices(n): the (start, stop, step) triple clamped to a
// sequence of length n; ValueError when step is 0.
func (s Slice) Indices(n int) (int, int, int, error) {
	step := 1
	if s.Step != nil {
		step = *s.Step
	}
	if step == 0 {
		return 0, 0, 0, ValueError.New("slice step cannot be zero")
	}
	var lower, upper int
	if step < 0 {
		lower, upper = -1, n-1
	} else {
		lower, upper = 0, n
	}
	clamp := func(v *int, def int) int {
		if v == nil {
			return def
		}
		x := *v
		if x < 0 {
			x += n
			if x < lower {
				x = lower
			}
		} else if x > upper {
			x = upper
		}
		return x
	}
	var start, stop int
	if step < 0 {
		start = clamp(s.Start, upper)
		stop = clamp(s.Stop, lower)
	} else {
		start = clamp(s.Start, lower)
		stop = clamp(s.Stop, upper)
	}
	return start, stop, step, nil
}

// Repr is slice(start, stop, step).
func (s Slice) Repr() string {
	p := func(v *int) string {
		if v == nil {
			return "None"
		}
		return strconv.Itoa(*v)
	}
	return "slice(" + p(s.Start) + ", " + p(s.Stop) + ", " + p(s.Step) + ")"
}

// indexes returns the positions selected by s over a sequence of length n.
func (s Slice) indexes(n int) ([]int, error) {
	start, stop, step, err := s.Indices(n)
	if err != nil {
		return nil, err
	}
	var out []int
	if step > 0 {
		for i := start; i < stop; i += step {
			out = append(out, i)
		}
	} else {
		for i := start; i > stop; i += step {
			out = append(out, i)
		}
	}
	return out, nil
}

// Getslice is l[s]: a new list (snapshot).
func (l *List) Getslice(s Slice) (*List, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	idx, err := s.indexes(len(l.items))
	if err != nil {
		return nil, err
	}
	out := make([]any, len(idx))
	for k, i := range idx {
		out[k] = l.items[i]
	}
	return &List{items: out}, nil
}

// Setslice is l[s] = items (simple slices splice; extended slices require
// an equal-length replacement, as in Python).
func (l *List) Setslice(s Slice, items any) error {
	vals := Iter(items)
	l.mu.Lock()
	defer l.mu.Unlock()
	n := len(l.items)
	if s.Step == nil || *s.Step == 1 {
		start, stop, _, err := s.Indices(n)
		if err != nil {
			return err
		}
		if stop < start {
			stop = start
		}
		out := make([]any, 0, n-(stop-start)+len(vals))
		out = append(out, l.items[:start]...)
		out = append(out, vals...)
		out = append(out, l.items[stop:]...)
		l.items = out
		return nil
	}
	idx, err := s.indexes(n)
	if err != nil {
		return err
	}
	if len(idx) != len(vals) {
		return ValueError.Newf("attempt to assign sequence of size %d to extended slice of size %d", len(vals), len(idx))
	}
	for k, i := range idx {
		l.items[i] = vals[k]
	}
	return nil
}

// Delslice is del l[s].
func (l *List) Delslice(s Slice) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	idx, err := s.indexes(len(l.items))
	if err != nil {
		return err
	}
	drop := make(map[int]bool, len(idx))
	for _, i := range idx {
		drop[i] = true
	}
	out := l.items[:0:0]
	for i, v := range l.items {
		if !drop[i] {
			out = append(out, v)
		}
	}
	l.items = out
	return nil
}

// Del is del l[i].
func (l *List) Del(i int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	i, err := l.norm(i, false)
	if err != nil {
		return IndexError.New("list assignment index out of range")
	}
	l.items = append(l.items[:i], l.items[i+1:]...)
	return nil
}

// Index_range is l.index(v, start[, stop]).
func (l *List) Index_range(v any, start int, stop ...int) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := len(l.items)
	if start < 0 {
		start += n
		if start < 0 {
			start = 0
		}
	}
	end := n
	if len(stop) > 0 {
		end = stop[0]
		if end < 0 {
			end += n
		}
		if end > n {
			end = n
		}
	}
	for i := start; i < end; i++ {
		if Eq(l.items[i], v) {
			return i, nil
		}
	}
	return 0, ValueError.Newf("%s is not in list", Repr(v))
}

// Set is l[i] = v.
func (l *List) Set(i int, v any) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	i, err := l.norm(i, false)
	if err != nil {
		return IndexError.New("list assignment index out of range")
	}
	l.items[i] = v
	return nil
}

// Pop is l.pop(i); i == -1 pops the last element.
func (l *List) Pop(i int) (any, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.items) == 0 {
		return nil, IndexError.New("pop from empty list")
	}
	i, err := l.norm(i, false)
	if err != nil {
		return nil, IndexError.New("pop index out of range")
	}
	v := l.items[i]
	l.items = append(l.items[:i], l.items[i+1:]...)
	return v, nil
}

// Remove is l.remove(v), raising ValueError when absent.
func (l *List) Remove(v any) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, x := range l.items {
		if Eq(x, v) {
			l.items = append(l.items[:i], l.items[i+1:]...)
			return nil
		}
	}
	return ValueError.New("list.remove(x): x not in list")
}

// Index is l.index(v).
func (l *List) Index(v any) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, x := range l.items {
		if Eq(x, v) {
			return i, nil
		}
	}
	return 0, ValueError.Newf("%s is not in list", Repr(v))
}

// Contains is `v in l`.
func (l *List) Contains(v any) bool { _, err := l.Index(v); return err == nil }

// Count is l.count(v).
func (l *List) Count(v any) int {
	n := 0
	for _, x := range l.Items() {
		if Eq(x, v) {
			n++
		}
	}
	return n
}

// Len is len(l).
func (l *List) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.items)
}

// Items returns a snapshot copy of the elements.
func (l *List) Items() []any {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]any(nil), l.items...)
}

// Slice is l[start:stop] with Python clamping and negative indices.
func (l *List) Slice(start, stop int) *List {
	items := l.Items()
	n := len(items)
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop > n {
		stop = n
	}
	if start >= stop {
		return NewList()
	}
	return NewList(items[start:stop]...)
}

// Clear removes everything.
func (l *List) Clear() {
	l.mu.Lock()
	l.items = nil
	l.mu.Unlock()
}

// Reverse reverses in place.
func (l *List) Reverse() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, j := 0, len(l.items)-1; i < j; i, j = i+1, j-1 {
		l.items[i], l.items[j] = l.items[j], l.items[i]
	}
}

// Replace swaps the whole content (l[:] = items).
func (l *List) Replace(items []any) {
	l.mu.Lock()
	l.items = append([]any(nil), items...)
	l.mu.Unlock()
}

// Copy returns a shallow copy.
func (l *List) Copy() *List { return NewList(l.Items()...) }

// Eq implements list equality.
func (l *List) Eq(other any) bool {
	var b []any
	switch o := other.(type) {
	case *List:
		if o == l {
			return true
		}
		b = o.Items()
	case []any:
		b = o
	default:
		return false
	}
	a := l.Items()
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !Eq(a[i], b[i]) {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Set
// ---------------------------------------------------------------------------

// Set is a Python set (iteration in insertion order for determinism).
type Set struct {
	d *Dict
}

// NewSet creates a set from items.
func NewSet(items ...any) *Set {
	s := &Set{d: NewDict()}
	for _, it := range items {
		s.Add(it)
	}
	return s
}

// Add is s.add(v).
func (s *Set) Add(v any) { s.d.Set(v, nil) }

// Discard is s.discard(v).
func (s *Set) Discard(v any) { _, _ = s.d.Pop(v) }

// Remove is s.remove(v), raising KeyError.
func (s *Set) Remove(v any) error { _, err := s.d.Pop(v); return err }

// Contains is `v in s`.
func (s *Set) Contains(v any) bool { return s.d.Contains(v) }

// Len is len(s).
func (s *Set) Len() int { return s.d.Len() }

// Items returns a snapshot.
func (s *Set) Items() []any { return s.d.Keys() }

// Clear empties the set.
func (s *Set) Clear() { s.d.Clear() }

// Update adds every element of iterable.
func (s *Set) Update(items any) {
	for _, v := range Iter(items) {
		s.Add(v)
	}
}

// Copy returns a shallow copy.
func (s *Set) Copy() *Set { return NewSet(s.Items()...) }

// Union is s | other.
func (s *Set) Union(other *Set) *Set {
	out := s.Copy()
	out.Update(other.Items())
	return out
}

// Intersection is s & other.
func (s *Set) Intersection(other *Set) *Set {
	out := NewSet()
	for _, v := range s.Items() {
		if other.Contains(v) {
			out.Add(v)
		}
	}
	return out
}

// Difference is s - other.
func (s *Set) Difference(other *Set) *Set {
	out := NewSet()
	for _, v := range s.Items() {
		if !other.Contains(v) {
			out.Add(v)
		}
	}
	return out
}

// Issubset is s <= other.
func (s *Set) Issubset(other *Set) bool {
	for _, v := range s.Items() {
		if !other.Contains(v) {
			return false
		}
	}
	return true
}

// Pop removes and returns an arbitrary element (KeyError when empty).
func (s *Set) Pop() (any, error) {
	k, _, err := s.d.Popitem()
	if err != nil {
		return nil, KeyError.New("pop from an empty set")
	}
	return k, nil
}

// Eq implements set equality.
func (s *Set) Eq(other any) bool {
	o, ok := other.(*Set)
	if !ok {
		return false
	}
	return s.Len() == o.Len() && s.Issubset(o)
}

// FrozenSet marks a set as frozen (hashable). Kept distinct for pickle.
type FrozenSet struct{ *Set }

// Eq compares with sets and frozensets alike (Python: frozenset({1}) == {1}).
func (f FrozenSet) Eq(other any) bool {
	switch o := other.(type) {
	case FrozenSet:
		return f.Set.Eq(o.Set)
	case *Set:
		return f.Set.Eq(o)
	}
	return false
}

// NaN is float("nan") with CPython's bit pattern (0x7ff8000000000000), so
// serialised NaNs are byte-identical; math.NaN() differs in the low bit.
var NaN = math.Float64frombits(0x7ff8000000000000)

// Hash makes FrozenSet usable as a key.
func (f FrozenSet) Hash() uint64 {
	var h uint64
	for _, v := range f.Items() {
		x, _ := Hash(v)
		h ^= x
	}
	return h
}

// ---------------------------------------------------------------------------
// generic helpers: Eq, Iter, Len, Truthy, Pytypename
// ---------------------------------------------------------------------------

// Eq is Python ==.
func Eq(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if ka, err := keyOf(a); err == nil && ka.kind != hkObj {
		if kb, err := keyOf(b); err == nil && kb.kind != hkObj {
			if ka.kind == hkFloat && ka.s == "nan" {
				return false // nan != nan
			}
			return ka == kb
		}
	}
	switch x := a.(type) {
	case Equaler:
		return x.Eq(b)
	case *List:
		return x.Eq(b)
	case []any:
		return NewList(x...).Eq(b)
	case *Dict:
		return x.Eq(b)
	case map[string]any:
		return DictFromMap(x).Eq(b)
	case *Set:
		return x.Eq(b)
	case Tuple:
		y, ok := b.(Tuple)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !Eq(x[i], y[i]) {
				return false
			}
		}
		return true
	}
	if e, ok := b.(Equaler); ok {
		return e.Eq(a)
	}
	if reflect.TypeOf(a).Comparable() && reflect.TypeOf(b).Comparable() {
		return a == b
	}
	return reflect.DeepEqual(a, b)
}

// Iter materialises any Python iterable into a slice.
func Iter(v any) []any {
	switch x := v.(type) {
	case nil:
		return nil
	case []any:
		return x
	case Tuple:
		return []any(x)
	case *List:
		return x.Items()
	case *Set:
		return x.Items()
	case FrozenSet:
		return x.Set.Items()
	case *Dict:
		return x.Keys()
	case []string:
		out := make([]any, len(x))
		for i, s := range x {
			out[i] = s
		}
		return out
	case string:
		out := make([]any, 0, len(x))
		for _, r := range x {
			out = append(out, string(r))
		}
		return out
	case Kw:
		return DictFromKw(x).Keys()
	case map[string]any:
		return DictFromMap(x).Keys()
	case []byte:
		out := make([]any, len(x))
		for i, b := range x {
			out[i] = int64(b)
		}
		return out
	case interface{ Iter() []any }:
		// ``__iter__`` protocol (Manifest yields its leaf global_ids).
		return x.Iter()
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		out := make([]any, rv.Len())
		for i := range out {
			out[i] = rv.Index(i).Interface()
		}
		return out
	}
	panic(TypeError.Newf("'%s' object is not iterable", Pytypename(v)))
}

// Len is len(v).
func Len(v any) (int, error) {
	switch x := v.(type) {
	case string:
		return len([]rune(x)), nil
	case []byte:
		return len(x), nil
	case []any:
		return len(x), nil
	case Tuple:
		return len(x), nil
	case *List:
		return x.Len(), nil
	case *Dict:
		return x.Len(), nil
	case *Set:
		return x.Len(), nil
	case Kw:
		return len(x), nil
	case map[string]any:
		return len(x), nil
	case interface{ Len() int }:
		return x.Len(), nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map, reflect.String:
		return rv.Len(), nil
	}
	return 0, TypeError.Newf("object of type '%s' has no len()", Pytypename(v))
}

// Int is int(v): ints and bools pass through, floats truncate toward
// zero, strings parse as decimal integers; TypeError / ValueError
// otherwise.
func Int(v any) (int, error) {
	switch x := v.(type) {
	case nil:
		return 0, TypeError.New("int() argument must be a string, a bytes-like object or a real number, not 'NoneType'")
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	case float64:
		if math.IsNaN(x) {
			return 0, ValueError.New("cannot convert float NaN to integer")
		}
		if math.IsInf(x, 0) {
			return 0, OverflowError.New("cannot convert float infinity to integer")
		}
		return int(x), nil
	case float32:
		return Int(float64(x))
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			return 0, ValueError.Newf("invalid literal for int() with base 10: %s", Repr(x))
		}
		return int(n), nil
	case Stringer:
		if _, isEnum := v.(interface{ Pyvalue() any }); !isEnum {
			break
		}
	}
	if k, err := keyOf(v); err == nil && k.kind == hkInt {
		return int(k.i), nil
	}
	return 0, TypeError.Newf("int() argument must be a string, a bytes-like object or a real number, not '%s'", Pytypename(v))
}

// Float is float(v).
func Float(v any) (float64, error) {
	switch x := v.(type) {
	case nil:
		return 0, TypeError.New("float() argument must be a string or a real number, not 'NoneType'")
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return 0, ValueError.Newf("could not convert string to float: %s", Repr(x))
		}
		return f, nil
	}
	if k, err := keyOf(v); err == nil && k.kind == hkInt {
		return float64(k.i), nil
	}
	return 0, TypeError.Newf("float() argument must be a string or a real number, not '%s'", Pytypename(v))
}

// Truthy is bool(v).
func Truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case []byte:
		return len(x) != 0
	case float64:
		return x != 0
	case float32:
		return x != 0
	case *big.Int:
		return x.Sign() != 0
	case interface{ Bool() bool }:
		return x.Bool()
	}
	if k, err := keyOf(v); err == nil && k.kind == hkInt {
		return k.i != 0
	}
	if n, err := Len(v); err == nil {
		return n != 0
	}
	return true
}

// Pytypename returns type(v).__name__ for the value model.
func Pytypename(v any) string {
	switch x := v.(type) {
	case nil:
		return "NoneType"
	case bool:
		return "bool"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, *big.Int:
		return "int"
	case float32, float64:
		return "float"
	case string:
		return "str"
	case []byte:
		return "bytes"
	case *List, []any:
		return "list"
	case Tuple:
		return "tuple"
	case *Dict, map[string]any, Kw:
		return "dict"
	case *Set:
		return "set"
	case FrozenSet:
		return "frozenset"
	case interface{ Pytype() string }:
		return x.Pytype()
	case interface{ Class_() *Class }:
		return x.Class_().Name
	}
	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

// Bytes_clone copies b into a fresh, never-nil slice. Python's ``b""`` is
// a value distinct from ``None``; because IsNone treats a nil slice as
// None, decoders that materialise bytes values must go through this
// helper so an empty payload survives as ``b""`` rather than ``None``.
func Bytes_clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// IsNone reports v is None.
func IsNone(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface, reflect.Func, reflect.Chan:
		return rv.IsNil()
	}
	return false
}
