package py

import "strings"

// DotMap is the dotmap.DotMap used by laila.args: an insertion-ordered
// mapping whose missing attributes auto-create empty child DotMaps
// (dynamic mode) and which converts nested dicts on the way in.
// laila.args.environment.policy.nickname  ->  args.Get("environment.policy.nickname")
type DotMap struct {
	d     *Dict
	OnSet func(key string, value any) // __setattr__ hook, fires after the store
	// Intercept is a pre-store __setattr__ hook: returning true swallows the
	// assignment (laila's _LailaArgs turns `args.environment = env` into a
	// full environment reload instead of a store).
	Intercept func(key string, value any) bool
	// OnRead fires before __getitem__ / get / items / toDict on the root
	// (laila's _LailaArgs flushes the environment mirror lazily).
	OnRead func(key string)
}

// NewDotMap creates an empty DotMap, optionally seeded from a dict/map.
func NewDotMap(seed ...any) *DotMap {
	m := &DotMap{d: NewDict()}
	for _, s := range seed {
		m.Update(s)
	}
	return m
}

func (m *DotMap) wrap(v any) any {
	switch x := v.(type) {
	case *Dict:
		child := &DotMap{d: NewDict()}
		for _, it := range x.Items() {
			child.d.Set(it.Key, child.wrap(it.Value))
		}
		return child
	case map[string]any:
		return m.wrap(DictFromMap(x))
	case Kw:
		return m.wrap(DictFromKw(x))
	}
	return v
}

// Get is attribute access with dotted-path support and auto-vivification:
// a missing leaf returns a new, empty child DotMap (dotmap dynamic mode).
func (m *DotMap) Get(path string) any {
	cur := m
	parts := strings.Split(path, ".")
	if m.OnRead != nil {
		m.OnRead(parts[0])
	}
	for i, p := range parts {
		v, ok := cur.d.Get(p)
		if !ok {
			child := &DotMap{d: NewDict()}
			cur.d.Set(p, child)
			v = child
		}
		if i == len(parts)-1 {
			return v
		}
		next, ok := v.(*DotMap)
		if !ok {
			return nil
		}
		cur = next
	}
	return cur
}

// Lookup is Get without auto-vivification: (value, found).
func (m *DotMap) Lookup(path string) (any, bool) {
	cur := m
	parts := strings.Split(path, ".")
	if m.OnRead != nil {
		m.OnRead(parts[0])
	}
	for i, p := range parts {
		v, ok := cur.d.Get(p)
		if !ok {
			return nil, false
		}
		if i == len(parts)-1 {
			return v, true
		}
		next, ok := v.(*DotMap)
		if !ok {
			return nil, false
		}
		cur = next
	}
	return cur, true
}

// Child returns the DotMap at path (auto-vivified) or nil when the value
// there is not a mapping.
func (m *DotMap) Child(path string) *DotMap {
	c, _ := m.Get(path).(*DotMap)
	return c
}

// Set is attribute assignment with dotted-path support; nested dicts are
// converted into DotMaps. The OnSet hook of the root fires with the first
// path segment, like _LailaArgs.__setattr__.
func (m *DotMap) Set(path string, value any) {
	parts := strings.Split(path, ".")
	if m.Intercept != nil && len(parts) == 1 && m.Intercept(parts[0], value) {
		return
	}
	cur := m
	for _, p := range parts[:len(parts)-1] {
		v, ok := cur.d.Get(p)
		next, isMap := v.(*DotMap)
		if !ok || !isMap {
			next = &DotMap{d: NewDict()}
			cur.d.Set(p, next)
		}
		cur = next
	}
	cur.d.Set(parts[len(parts)-1], m.wrap(value))
	if m.OnSet != nil {
		m.OnSet(parts[0], value)
	}
}

// Delete removes a key.
func (m *DotMap) Delete(path string) {
	parts := strings.Split(path, ".")
	cur := m
	for _, p := range parts[:len(parts)-1] {
		v, _ := cur.d.Get(p)
		next, ok := v.(*DotMap)
		if !ok {
			return
		}
		cur = next
	}
	_, _ = cur.d.Pop(parts[len(parts)-1])
}

// Has reports whether path exists (no auto-vivification).
func (m *DotMap) Has(path string) bool { _, ok := m.Lookup(path); return ok }

// Keys returns the top-level keys.
func (m *DotMap) Keys() []string { return m.d.StrKeys() }

// Len is len(m).
func (m *DotMap) Len() int { return m.d.Len() }

// Items returns the top-level items.
func (m *DotMap) Items() []Item {
	if m.OnRead != nil {
		m.OnRead("")
	}
	return m.d.Items()
}

// Pop removes and returns a top-level key (dict.pop(key, default)).
func (m *DotMap) Pop(key string, def any) any {
	return m.d.PopDefault(key, def)
}

// Update merges a dict/map/DotMap in.
func (m *DotMap) Update(other any) {
	switch o := other.(type) {
	case *DotMap:
		for _, it := range o.d.Items() {
			m.d.Set(it.Key, it.Value)
		}
	case nil:
	default:
		d := m.wrap(other).(*DotMap)
		for _, it := range d.d.Items() {
			m.d.Set(it.Key, it.Value)
		}
	}
}

// Clear empties the map.
func (m *DotMap) Clear() { m.d.Clear() }

// ToDict converts recursively back into plain *Dict values.
func (m *DotMap) ToDict() *Dict {
	if m.OnRead != nil {
		m.OnRead("")
	}
	out := NewDict()
	for _, it := range m.d.Items() {
		if c, ok := it.Value.(*DotMap); ok {
			out.Set(it.Key, c.ToDict())
		} else {
			out.Set(it.Key, it.Value)
		}
	}
	return out
}

// Eq compares as dicts.
func (m *DotMap) Eq(other any) bool {
	switch o := other.(type) {
	case *DotMap:
		return m.ToDict().Eq(o.ToDict())
	case *Dict:
		return m.ToDict().Eq(o)
	case map[string]any, Kw:
		return m.ToDict().Eq(o)
	}
	return false
}

// Repr is DotMap(...) like dotmap's repr.
func (m *DotMap) Repr() string { return "DotMap(" + Repr(m.ToDict()) + ")" }

// Copy returns a deep copy.
func (m *DotMap) Copy() *DotMap { return NewDotMap(Deepcopy(m.ToDict())) }

// Deepcopy implements the copy protocol.
func (m *DotMap) Deepcopy(memo *Memo) any { return m.Copy() }
