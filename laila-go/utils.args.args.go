package laila

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	toml "github.com/pelletier/go-toml/v2"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// utils/args/args.py -- runtime argument loading from multiple file
// formats and the terminal.
//
// ArgReader is the loader behind laila.Args. It accepts arguments from
// ``.json`` / ``.toml`` / ``.env`` / ``.xml`` files and from ``terminal``
// tokens and merges them into a single attribute-accessible mapping.
// Values are coerced from strings to the most specific scalar they fit
// (bool, None, int, float, JSON-shaped collection, quoted string, raw
// string) by Coerce_scalar_. Nested dicts are applied twice: flattened
// *one level* with underscore-joined keys (``s3_bucket_name``) and
// deep-merged into the nested tree (``laila.args.s3.bucket_name``).

// Looks_numeric_ is true for tokens such as ``-3`` / ``-0.5`` (values,
// not flags).
func Looks_numeric_(token string) bool {
	_, err := py.Float(token)
	return err == nil
}

// ArgReader is the loader of runtime args into an attribute-accessible
// target mapping. All Load / From_* methods mutate the target in place.
// The target defaults to laila.Args when nil.
type ArgReader struct {
	Target_ any `py:"_target"`
}

// New_ArgReader is ``ArgReader(target=None)``.
func New_ArgReader(kw ...py.Kw) (*ArgReader, error) {
	opts := py.MergeKw(kw...)
	return &ArgReader{Target_: opts.Get("target", nil)}, nil
}

// Target_map_ returns the target mapping, falling back to laila.Args.
func (r *ArgReader) Target_map_() any {
	if !py.IsNone(r.Target_) {
		return r.Target_
	}
	return Args
}

// ArgReader_coerce_scalar_ is ``ArgReader._coerce_scalar(value)``: coerces
// a string value to its most specific scalar type.
func ArgReader_coerce_scalar_(value any) any {
	s, ok := value.(string)
	if !ok {
		return value
	}
	stripped := strings.TrimSpace(s)
	lowered := strings.ToLower(stripped)
	if lowered == "true" || lowered == "false" {
		return lowered == "true"
	}
	if lowered == "none" || lowered == "null" {
		return nil
	}
	if n, err := strconv.ParseInt(stripped, 10, 64); err == nil {
		return int(n)
	}
	if f, err := py.Float(stripped); err == nil {
		return f
	}
	if (strings.HasPrefix(stripped, "{") && strings.HasSuffix(stripped, "}")) ||
		(strings.HasPrefix(stripped, "[") && strings.HasSuffix(stripped, "]")) {
		if v, err := py.Json_loads(stripped); err == nil {
			return v
		}
		return value
	}
	if len(stripped) >= 2 && ((strings.HasPrefix(stripped, `"`) && strings.HasSuffix(stripped, `"`)) ||
		(strings.HasPrefix(stripped, "'") && strings.HasSuffix(stripped, "'"))) {
		return stripped[1 : len(stripped)-1]
	}
	return value
}

// ArgReader_flatten_one_level_ flattens nested dicts one level, joining
// keys with ``_``.
func ArgReader_flatten_one_level_(payload *py.Dict) *py.Dict {
	out := py.NewDict()
	for _, it := range payload.Items() {
		key := py.Str(it.Key)
		if sub, ok := asDict(it.Value); ok {
			for _, s := range sub.Items() {
				out.Set(key+"_"+py.Str(s.Key), ArgReader_coerce_scalar_(s.Value))
			}
		} else {
			out.Set(key, ArgReader_coerce_scalar_(it.Value))
		}
	}
	return out
}

// ArgReader_coerce_tree_ coerces scalars recursively inside nested dicts
// and lists.
func ArgReader_coerce_tree_(value any) any {
	if d, ok := asDict(value); ok {
		out := py.NewDict()
		for _, it := range d.Items() {
			out.Set(it.Key, ArgReader_coerce_tree_(it.Value))
		}
		return out
	}
	switch x := value.(type) {
	case *py.List:
		out := py.NewList()
		for _, v := range x.Items() {
			out.Append(ArgReader_coerce_tree_(v))
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = ArgReader_coerce_tree_(v)
		}
		return out
	}
	return ArgReader_coerce_scalar_(value)
}

// ArgReader_deep_merge_ merges the nested dict *value* into
// ``target[key]`` leaf by leaf: an existing mapping under *key* (DotMap or
// dict) is updated in place, missing branches are created, non-mapping
// values at either side are replaced by the incoming leaf.
func ArgReader_deep_merge_(target any, key string, value *py.Dict) {
	lookup := func(container any, k string) any {
		switch c := container.(type) {
		case *py.DotMap:
			v, _ := c.Lookup(k)
			return v
		case *py.Dict:
			return c.GetDefault(k, nil)
		case py.Kw:
			return c.Get(k, nil)
		case map[string]any:
			return c[k]
		case *AtomicDotMap:
			return c.Get(k)
		}
		v, err := py.Getattr(container, k)
		if err != nil {
			return nil
		}
		return v
	}
	store := func(container any, k string, v any) {
		switch c := container.(type) {
		case *py.DotMap:
			if d, ok := asDict(v); ok {
				c.Set(k, py.NewDotMap(d))
			} else {
				c.Set(k, v)
			}
		case *py.Dict:
			c.Set(k, v)
		case py.Kw:
			c[k] = v
		case map[string]any:
			c[k] = v
		default:
			_ = py.Setattr(container, k, v)
		}
	}

	existing := lookup(target, key)
	var mapping_like bool
	switch existing.(type) {
	case *py.Dict, *py.DotMap, py.Kw, map[string]any:
		mapping_like = true
	default:
		mapping_like = existing != nil && py.Hasattr(existing, "__setitem__")
	}
	if !mapping_like {
		store(target, key, value)
		return
	}
	for _, it := range value.Items() {
		sub_key := py.Str(it.Key)
		if sub, ok := asDict(it.Value); ok {
			ArgReader_deep_merge_(existing, sub_key, sub)
		} else {
			switch e := existing.(type) {
			case *py.DotMap:
				e.Set(sub_key, it.Value)
			case *py.Dict:
				e.Set(sub_key, it.Value)
			case py.Kw:
				e[sub_key] = it.Value
			case map[string]any:
				e[sub_key] = it.Value
			default:
				_, _ = callm(existing, "__setitem__", sub_key, it.Value)
			}
		}
	}
}

// Apply_ applies *payload* to the target: flat ``a_b`` keys plus the
// nested tree. The ``environment`` key is applied last so an environment
// load (which calls Terminate and rebuilds the policy graph) sees every
// other key already in place.
func (r *ArgReader) Apply_(payload *py.Dict) {
	flat := ArgReader_flatten_one_level_(payload)
	target := r.Target_map_()
	for _, it := range flat.Items() {
		setattrTarget(target, py.Str(it.Key), it.Value)
	}
	var nested []py.Item
	for _, it := range payload.Items() {
		if _, ok := asDict(it.Value); ok {
			nested = append(nested, it)
		}
	}
	sort.SliceStable(nested, func(i, j int) bool {
		return py.Str(nested[i].Key) != "environment" && py.Str(nested[j].Key) == "environment"
	})
	for _, it := range nested {
		d, _ := asDict(ArgReader_coerce_tree_(it.Value))
		ArgReader_deep_merge_(target, py.Str(it.Key), d)
	}
}

// setattrTarget is ``setattr(target, key, value)`` for the mapping kinds a
// reader target can be.
func setattrTarget(target any, key string, value any) {
	switch t := target.(type) {
	case *py.DotMap:
		t.Set(key, value)
	case *AtomicDotMap:
		t.Set(key, value)
	case *py.Dict:
		t.Set(key, value)
	case py.Kw:
		t[key] = value
	case map[string]any:
		t[key] = value
	default:
		_ = py.Setattr(target, key, value)
	}
}

// Clear removes all keys from the target mapping.
func (r *ArgReader) Clear() {
	target := r.Target_map_()
	switch t := target.(type) {
	case *py.DotMap:
		for _, k := range t.Keys() {
			t.Delete(k)
		}
	case *AtomicDotMap:
		for _, k := range t.Keys() {
			_ = t.Delattr(py.Str(k))
		}
	case *py.Dict:
		t.Clear()
	case py.Kw:
		for k := range t {
			delete(t, k)
		}
	case map[string]any:
		for k := range t {
			delete(t, k)
		}
	default:
		if py.Hasattr(target, "keys") {
			if keys, err := callm(target, "keys"); err == nil {
				for _, k := range py.Iter(keys) {
					_ = py.Delattr(target, py.Str(k))
				}
			}
		}
	}
}

// From_json loads arguments from a JSON file (ValueError unless the top
// level is an object).
func (r *ArgReader) From_json(path string) error {
	text, err := py.Read_text(path)
	if err != nil {
		return err
	}
	data, err := py.Json_loads(text)
	if err != nil {
		return err
	}
	d, ok := asDict(data)
	if !ok {
		return py.ValueError.New("JSON args file must be a key/value object.")
	}
	r.Apply_(d)
	return nil
}

// From_toml loads arguments from a TOML file (ValueError unless the top
// level is a table; TOMLDecodeError on malformed input).
func (r *ArgReader) From_toml(path string) error {
	raw, err := py.Read_bytes(path)
	if err != nil {
		return err
	}
	var out map[string]any
	if err := toml.Unmarshal(raw, &out); err != nil {
		return TOMLDecodeError.New(err.Error())
	}
	r.Apply_(tomlToDict(out))
	return nil
}

// TOMLDecodeError is ``tomllib.TOMLDecodeError``.
var TOMLDecodeError = py.NewType("TOMLDecodeError", py.ValueError)

// tomlToDict converts go-toml's map tree into ordered *py.Dict values
// (keys sorted for determinism; Go maps are unordered).
func tomlToDict(m map[string]any) *py.Dict {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := py.NewDict()
	for _, k := range keys {
		out.Set(k, tomlValue(m[k]))
	}
	return out
}

func tomlValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return tomlToDict(x)
	case []any:
		items := make([]any, len(x))
		for i, e := range x {
			items[i] = tomlValue(e)
		}
		return py.NewList(items...)
	case []map[string]any:
		items := make([]any, len(x))
		for i, e := range x {
			items[i] = tomlToDict(e)
		}
		return py.NewList(items...)
	case int64:
		return int(x)
	}
	return v
}

// From_env loads arguments from a ``.env``-style file (``KEY=VALUE``
// lines, ``#`` comments allowed; malformed lines are skipped).
func (r *ArgReader) From_env(path string) error {
	text, err := py.Read_text(path)
	if err != nil {
		return err
	}
	parsed := py.NewDict()
	for _, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, "=") {
			continue
		}
		k, v, _ := strings.Cut(line, "=")
		parsed.Set(strings.TrimSpace(k), ArgReader_coerce_scalar_(strings.TrimSpace(v)))
	}
	r.Apply_(parsed)
	return nil
}

// xmlNode is the minimal element tree used by From_xml.
type xmlNode struct {
	XMLName  xml.Name
	Text     string    `xml:",chardata"`
	Children []xmlNode `xml:",any"`
}

// From_xml loads arguments from an XML file: one level of nesting allowed;
// siblings become nested dicts.
func (r *ArgReader) From_xml(path string) error {
	raw, err := py.Read_bytes(path)
	if err != nil {
		return err
	}
	var root xmlNode
	if err := xml.Unmarshal(raw, &root); err != nil {
		return ParseError.New(err.Error())
	}
	parsed := py.NewDict()
	for _, child := range root.Children {
		if len(child.Children) > 0 {
			sub := py.NewDict()
			for _, gc := range child.Children {
				sub.Set(gc.XMLName.Local, xmlText(gc))
			}
			parsed.Set(child.XMLName.Local, sub)
		} else {
			parsed.Set(child.XMLName.Local, xmlText(child))
		}
	}
	r.Apply_(parsed)
	return nil
}

// ParseError is ``xml.etree.ElementTree.ParseError``.
var ParseError = py.NewType("ParseError", py.SyntaxError)

// xmlText is ``element.text or ""``: the character data before the first
// child element.
func xmlText(n xmlNode) string {
	if len(n.Children) == 0 {
		return n.Text
	}
	// chardata of a mixed element concatenates every text run; Python's
	// ``.text`` is only the leading run, which is empty for the nested
	// shapes this reader accepts.
	return ""
}

// From_terminal loads arguments from command-line tokens
// (``from_terminal(args=None)``; nil means ``sys.argv[1:]``):
//
//   - ``key=value`` / ``--key=value`` / ``-key=value``
//   - ``--key value`` (the next token, unless it is itself a flag)
//   - ``--flag`` (no value) -> true
//
// Bare tokens without ``=`` or a leading dash are ignored.
func (r *ArgReader) From_terminal(args []string) {
	tokens := args
	if tokens == nil {
		tokens = os.Args[1:]
	}
	parsed := py.NewDict()
	i := 0
	for i < len(tokens) {
		token := tokens[i]
		i++
		dashed := strings.HasPrefix(token, "-") && !Looks_numeric_(token)
		if strings.Contains(token, "=") {
			k, v, _ := strings.Cut(token, "=")
			key := strings.TrimSpace(k)
			if dashed {
				key = strings.TrimLeft(key, "-")
			}
			if key == "" {
				continue
			}
			parsed.Set(key, ArgReader_coerce_scalar_(strings.TrimSpace(v)))
			continue
		}
		if !dashed {
			continue
		}
		key := strings.TrimLeft(strings.TrimSpace(token), "-")
		if key == "" {
			continue
		}
		if i < len(tokens) {
			nxt := tokens[i]
			if !strings.Contains(nxt, "=") && !(strings.HasPrefix(nxt, "-") && !Looks_numeric_(nxt)) {
				parsed.Set(key, ArgReader_coerce_scalar_(strings.TrimSpace(nxt)))
				i++
				continue
			}
		}
		parsed.Set(key, true)
	}
	r.Apply_(parsed)
}

// Load auto-detects the format from the file suffix (or the literal
// ``"terminal"``) and loads arguments (``load(source, *, terminal_args=None)``).
// ValueError if the suffix is not supported.
func (r *ArgReader) Load(source string, kw ...py.Kw) error {
	opts := py.MergeKw(kw...)
	name := filepath.Base(source)
	suffix := strings.ToLower(filepath.Ext(source))
	if strings.HasPrefix(name, ".") && !strings.Contains(name[1:], ".") {
		suffix = "" // ``Path(".env").suffix`` is ``""``
	}
	if suffix == "" && strings.ToLower(name) == ".env" {
		suffix = ".env"
	}
	switch suffix {
	case ".json":
		return r.From_json(source)
	case ".toml":
		return r.From_toml(source)
	case ".env":
		return r.From_env(source)
	case ".xml":
		return r.From_xml(source)
	}
	if strings.ToLower(source) == "terminal" {
		var args []string
		if ta := opts.Get("terminal_args", nil); !py.IsNone(ta) {
			args = []string{}
			for _, t := range py.Iter(ta) {
				args = append(args, py.Str(t))
			}
		}
		r.From_terminal(args)
		return nil
	}
	return py.ValueError.Newf("Unsupported args source: %s", source)
}
